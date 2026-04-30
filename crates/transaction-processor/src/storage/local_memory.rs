use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::Mutex;

use crate::{
    ports::{Account, Storage, TransactionError, TxType},
    Transaction,
};

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
    reverted_transactions: Mutex<HashMap<u32, Transaction>>,
}

impl LocalMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn len(&self) -> usize {
        self.accounts.lock().await.len()
    }

    pub async fn snapshot(&self) -> Vec<Account> {
        self.accounts.lock().await.values().cloned().collect()
    }

    pub async fn update(&self, account: Account) {
        let mut accounts = self.accounts.lock().await;
        accounts.insert(account.client_id, account);
    }

    pub async fn transfer_from_transactions_to_disputed(&self, tx_id: u32) -> anyhow::Result<()> {
        let mut transactions = self.transactions.lock().await;
        if let Some(tx) = transactions.remove(&tx_id) {
            let mut disputed_transactions = self.disputed_transactions.lock().await;
            disputed_transactions.insert(tx_id, tx);
            Ok(())
        } else {
            Err(anyhow::anyhow!(TransactionError::StoreCorruptionDetected))
        }
    }

    pub async fn transfer_from_disputed_to_transactions(&self, tx_id: u32) -> anyhow::Result<()> {
        let mut disputed_transactions = self.disputed_transactions.lock().await;
        if let Some(tx) = disputed_transactions.remove(&tx_id) {
            let mut transactions = self.transactions.lock().await;
            transactions.insert(tx_id, tx);
            Ok(())
        } else {
            Err(anyhow::anyhow!(TransactionError::StoreCorruptionDetected))
        }
    }

    pub async fn transfer_from_disputed_to_reverted(&self, tx_id: u32) -> anyhow::Result<()> {
        let mut disputed_transactions = self.disputed_transactions.lock().await;
        if let Some(tx) = disputed_transactions.remove(&tx_id) {
            let mut reverted_transactions = self.reverted_transactions.lock().await;
            reverted_transactions.insert(tx_id, tx);
            Ok(())
        } else {
            Err(anyhow::anyhow!(TransactionError::StoreCorruptionDetected))
        }
    }

    pub async fn get_or_create(&self, client_id: u16) -> Account {
        let mut accounts = self.accounts.lock().await;
        accounts
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

    pub async fn add_transaction(&self, tx: MonetaryTransaction) {
        let tx = tx.0;
        let mut transactions = self.transactions.lock().await;
        transactions.insert(tx.tx_id, tx);
    }
}

#[async_trait]
impl Storage for LocalMemoryStorage {
    async fn update_account_for_withdrawal_or_deposit(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()> {
        self.update(account).await;
        self.add_transaction(MonetaryTransaction::try_from(tx)?)
            .await;
        Ok(())
    }

    async fn update_account_for_dispute(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()> {
        self.update(account).await;
        self.transfer_from_transactions_to_disputed(tx.tx_id)
            .await?;
        Ok(())
    }

    async fn update_account_for_resolve(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()> {
        self.update(account).await;
        self.transfer_from_disputed_to_transactions(tx.tx_id)
            .await?;
        Ok(())
    }

    async fn update_account_for_chargeback(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()> {
        self.update(account).await;
        self.transfer_from_disputed_to_reverted(tx.tx_id).await?;
        Ok(())
    }

    async fn get_account(&self, client_id: u16) -> anyhow::Result<Account> {
        Ok(self.get_or_create(client_id).await)
    }

    async fn find_transaction(&self, tx_id: u32) -> anyhow::Result<Transaction> {
        let transactions = self.transactions.lock().await;
        transactions
            .get(&tx_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!(TransactionError::NotFound))
    }

    async fn find_disputed_transaction(&self, tx_id: u32) -> anyhow::Result<Transaction> {
        let disputed_transactions = self.disputed_transactions.lock().await;
        disputed_transactions
            .get(&tx_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!(TransactionError::NotFound))
    }

    async fn has_transaction_been_processed(&self, tx_id: u32) -> anyhow::Result<bool> {
        let transactions = self.transactions.lock().await;
        let disputed_transactions = self.disputed_transactions.lock().await;
        let reverted_transactions = self.reverted_transactions.lock().await;
        Ok(transactions.contains_key(&tx_id)
            || disputed_transactions.contains_key(&tx_id)
            || reverted_transactions.contains_key(&tx_id))
    }

    async fn all_accounts(&self, page: usize, page_size: usize) -> anyhow::Result<Vec<Account>> {
        let accounts = self.accounts.lock().await;
        let result = accounts
            .values()
            .skip(page * page_size)
            .take(page_size)
            .cloned()
            .collect();
        Ok(result)
    }
}
