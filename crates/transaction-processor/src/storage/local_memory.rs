use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::domain::{Account, Transaction, TxType};
use crate::ports::{Storage, TransactionError};

/// A Transaction that is guaranteed to be a Deposit or Withdrawal.
/// Lifecycle messages (Dispute/Resolve/Chargeback) are rejected.
#[derive(Debug, Clone)]
pub struct MonetaryTransaction(pub Transaction);

impl TryFrom<Transaction> for MonetaryTransaction {
    type Error = anyhow::Error;

    fn try_from(tx: Transaction) -> Result<Self, Self::Error> {
        match tx.tx_type {
            TxType::Deposit | TxType::Withdrawal => Ok(MonetaryTransaction(tx)),
            _ => Err(anyhow::anyhow!(
                TransactionError::InvalidTransactionStorageAttempt
            )),
        }
    }
}

/// In-memory storage adapter.
#[derive(Default)]
pub struct LocalMemoryStorage {
    accounts: Mutex<HashMap<u16, Account>>,
    transactions: Mutex<HashMap<u32, Transaction>>,
    disputed_transactions: Mutex<HashMap<u32, Transaction>>,
    processed_tx_ids: Mutex<HashSet<u32>>,
}

impl LocalMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&self, account: Account) {
        self.accounts
            .lock()
            .unwrap()
            .insert(account.client_id, account);
    }

    pub fn transfer_from_transactions_to_disputed(&self, tx_id: u32) -> anyhow::Result<()> {
        let tx = self
            .transactions
            .lock()
            .unwrap()
            .remove(&tx_id)
            .ok_or_else(|| anyhow::anyhow!(TransactionError::StoreCorruptionDetected))?;
        self.disputed_transactions.lock().unwrap().insert(tx_id, tx);
        Ok(())
    }

    pub fn transfer_from_disputed_to_transactions(&self, tx_id: u32) -> anyhow::Result<()> {
        let tx = self
            .disputed_transactions
            .lock()
            .unwrap()
            .remove(&tx_id)
            .ok_or_else(|| anyhow::anyhow!(TransactionError::StoreCorruptionDetected))?;
        self.transactions.lock().unwrap().insert(tx_id, tx);
        Ok(())
    }

    pub fn discard_disputed(&self, tx_id: u32) -> anyhow::Result<()> {
        self.disputed_transactions
            .lock()
            .unwrap()
            .remove(&tx_id)
            .ok_or_else(|| anyhow::anyhow!(TransactionError::StoreCorruptionDetected))?;
        Ok(())
    }

    pub fn get_or_create(&self, client_id: u16) -> Account {
        self.accounts
            .lock()
            .unwrap()
            .entry(client_id)
            .or_insert_with(|| Account {
                client_id,
                available: Decimal::ZERO,
                held: Decimal::ZERO,
                total: Decimal::ZERO,
                locked: false,
            })
            .clone()
    }

    pub fn add_transaction(&self, tx: MonetaryTransaction) {
        let tx = tx.0;
        let tx_id = tx.tx_id;
        self.transactions.lock().unwrap().insert(tx_id, tx);
        self.processed_tx_ids.lock().unwrap().insert(tx_id);
    }
}

#[async_trait]
impl Storage for LocalMemoryStorage {
    async fn update_account_for_withdrawal_or_deposit(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()> {
        self.update(account);
        self.add_transaction(MonetaryTransaction::try_from(tx)?);
        Ok(())
    }

    async fn update_account_for_dispute(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()> {
        self.update(account);
        self.transfer_from_transactions_to_disputed(tx.tx_id)?;
        Ok(())
    }

    async fn update_account_for_resolve(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()> {
        self.update(account);
        self.transfer_from_disputed_to_transactions(tx.tx_id)?;
        Ok(())
    }

    async fn update_account_for_chargeback(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()> {
        self.update(account);
        self.discard_disputed(tx.tx_id)?;
        Ok(())
    }

    async fn get_account(&self, client_id: u16) -> anyhow::Result<Account> {
        Ok(self.get_or_create(client_id))
    }

    async fn find_transaction(&self, tx_id: u32) -> anyhow::Result<Option<Transaction>> {
        Ok(self.transactions.lock().unwrap().get(&tx_id).cloned())
    }

    async fn find_disputed_transaction(&self, tx_id: u32) -> anyhow::Result<Option<Transaction>> {
        Ok(self
            .disputed_transactions
            .lock()
            .unwrap()
            .get(&tx_id)
            .cloned())
    }

    async fn has_transaction_been_processed(&self, tx_id: u32) -> anyhow::Result<bool> {
        Ok(self.processed_tx_ids.lock().unwrap().contains(&tx_id))
    }

    async fn all_accounts(&self, page: usize, page_size: usize) -> anyhow::Result<Vec<Account>> {
        let accounts = self.accounts.lock().unwrap();
        Ok(accounts
            .values()
            .skip(page * page_size)
            .take(page_size)
            .cloned()
            .collect())
    }
}
