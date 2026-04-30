use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::ports::{Account, Source, Storage, Transaction};
use crate::sources::csv_source::CsvSource;
use crate::storage::local_memory::LocalMemoryStorage;

pub const DEFAULT_CHANNEL_CAPACITY: usize = 512;

pub struct TransactionProcessorBuilder {
    parallelism: Option<usize>,
    channel_capacity: usize,
    storage: Option<Arc<dyn Storage>>,
}

impl Default for TransactionProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionProcessorBuilder {
    pub fn new() -> Self {
        Self {
            parallelism: None,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            storage: None,
        }
    }

    /// Override worker parallelism. Defaults to the number of logical CPUs.
    pub fn parallelism(mut self, n: usize) -> Self {
        self.parallelism = Some(n);
        self
    }

    /// Override the bounded-channel capacity (per channel). Defaults to 512.
    pub fn channel_capacity(mut self, n: usize) -> Self {
        self.channel_capacity = n;
        self
    }

    /// Override the storage adapter. Defaults to `LocalMemoryStorage`.
    pub fn storage(mut self, storage: Arc<dyn Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub async fn build(self) -> anyhow::Result<TransactionProcessor> {
        let parallelism = self.parallelism.unwrap_or_else(num_cpus::get).max(1);
        let storage = self
            .storage
            .unwrap_or_else(|| Arc::new(LocalMemoryStorage::new()));

        let mut senders = Vec::with_capacity(parallelism);
        let mut workers = Vec::with_capacity(parallelism);
        for worker_id in 0..parallelism {
            let (tx, rx) = mpsc::channel::<Transaction>(self.channel_capacity);
            senders.push(tx);
            let storage = Arc::clone(&storage);
            workers.push(tokio::spawn(worker_loop(worker_id, rx, storage)));
        }

        Ok(TransactionProcessor {
            parallelism,
            channel_capacity: self.channel_capacity,
            senders,
            workers,
            storage,
        })
    }
}

async fn worker_loop(
    worker_id: usize,
    mut rx: mpsc::Receiver<Transaction>,
    storage: Arc<dyn Storage>,
) {
    while let Some(tx) = rx.recv().await {
        tracing::debug!(
            worker = worker_id,
            tx_id = tx.tx_id,
            "processing transaction"
        );
        process_transaction(tx, Arc::clone(&storage))
            .await
            .unwrap_or_else(|err| {
                tracing::debug!(worker = worker_id, error = %err, "ignoring failed transaction");
            });
    }
}

/// Wrapper to catch all errors and conditionally ignore them based on type.
async fn process_transaction(tx: Transaction, storage: Arc<dyn Storage>) -> anyhow::Result<()> {
    match try_process_transaction(tx, storage).await {
        Ok(()) => Ok(()),
        Err(err) => match err.downcast_ref::<crate::ports::TransactionError>() {
            Some(crate::ports::TransactionError::NotFound)
            | Some(crate::ports::TransactionError::InsufficientFunds)
            | Some(crate::ports::TransactionError::InvalidTransactionAmount) => {
                tracing::debug!(error = %err, "ignoring non-fatal transaction error");
                Ok(())
            }
            Some(crate::ports::TransactionError::InvalidTransactionStorageAttempt)
            | Some(crate::ports::TransactionError::StoreCorruptionDetected) => {
                tracing::error!(error = %err, "transaction processing error");
                Err(err)
            }
            None => Err(err),
        },
    }
}

async fn try_process_transaction(tx: Transaction, storage: Arc<dyn Storage>) -> anyhow::Result<()> {
    // Skip monetary transactions that have already been processed. We are assuming that the lifecycle
    // types won't have duplicates where a a duplicate dispute comes in after a resolve. We need to
    // update the incoming Transaction model to be able to dedupe lifecycle events coming from the
    // source to know which lifecycle events are actually dupes.
    if tx.tx_type == crate::ports::TxType::Deposit || tx.tx_type == crate::ports::TxType::Withdrawal
    {
        if storage.has_transaction_been_processed(tx.tx_id).await? {
            tracing::info!(
                tx_id = tx.tx_id,
                "transaction has already been processed, skipping"
            );
            return Ok(());
        }
    }

    // First, get existing client account state from storage
    let account = storage.get_account(tx.client_id).await?;

    // If the account is locked, ignore withdrawals. Deposits and disputes/resolves/chargebacks on
    // locked accounts should still be processed to allow for dispute resolution and potential
    // unlocking in cases where the account has a negative balance due to a chargeback.
    if account.locked && tx.tx_type == crate::ports::TxType::Withdrawal {
        tracing::warn!(
            client_id = tx.client_id,
            tx_id = tx.tx_id,
            "ignoring withdrawal on locked account"
        );
        return Ok(());
    }

    // Apply the transaction to the account state
    match tx.tx_type {
        // A deposit is a credit to the client's asset account, meaning it should increase the available and
        // total funds of the client account
        crate::ports::TxType::Deposit => {
            if tx.amount < 0.0 {
                // Negative deposit amount, return an error
                return Err(anyhow::anyhow!(
                    crate::ports::TransactionError::InvalidTransactionAmount
                ));
            }
            let new_available = account.available + tx.amount;
            let new_total = account.total + tx.amount;
            let updated_account = Account {
                available: new_available,
                total: new_total,
                ..account
            };
            storage
                .update_account_for_withdrawal_or_deposit(tx, updated_account)
                .await?;
        }

        // A withdraw is a debit to the client's asset account, meaning it should decrease the available and
        // total funds of the client account
        crate::ports::TxType::Withdrawal => {
            if tx.amount < 0.0 {
                // Negative withdrawal amount, return an error
                return Err(anyhow::anyhow!(
                    crate::ports::TransactionError::InvalidTransactionAmount
                ));
            }
            if account.available < tx.amount {
                // Insufficient funds
                return Err(anyhow::anyhow!(
                    crate::ports::TransactionError::InsufficientFunds
                ));
            }
            let new_available = account.available - tx.amount;
            let new_total = account.total - tx.amount;
            let updated_account = Account {
                available: new_available,
                total: new_total,
                ..account
            };
            storage
                .update_account_for_withdrawal_or_deposit(tx, updated_account)
                .await?;
        }

        // A dispute represents a client's claim that a transaction was erroneous and should be reversed.
        // This is the beginning of a dispute lifecycle, which may later be resolved or chargebacked. This
        // is expected to move the disputed amount from available to held, keeping total the same. Allow
        // even if the account is locked, since disputes can be opened on already-locked accounts.
        crate::ports::TxType::Dispute => {
            let disputed_tx = storage.find_transaction(tx.tx_id).await?;
            if disputed_tx.client_id != tx.client_id {
                // Transaction client ID mismatch, ignore the dispute
                return Ok(());
            }
            if disputed_tx.tx_type != crate::ports::TxType::Deposit {
                // Only deposits can be disputed, ignore otherwise
                return Ok(());
            }
            let new_available = account.available - disputed_tx.amount;
            let new_held = account.held + disputed_tx.amount;
            let updated_account = Account {
                available: new_available,
                held: new_held,
                ..account
            };
            // These pairs of updates should be atomic. If there's a failure, we should revert
            storage
                .update_account_for_dispute(tx, updated_account)
                .await?;
        }

        // A resolve represents a resolution to a dispute, releasing the associated held funds. Funds that
        // were previously disputed are no longer disputed. This is expected to return the funds to the
        // client's available balance, decrease the held and keep total the same. This should be allowed
        // even if the account is locked.
        crate::ports::TxType::Resolve => {
            let disputed_tx = storage.find_disputed_transaction(tx.tx_id).await?;
            if disputed_tx.client_id != tx.client_id {
                // Transaction client ID mismatch, ignore the resolve
                return Ok(());
            }
            let new_available = account.available + disputed_tx.amount;
            let new_held = account.held - disputed_tx.amount;
            let updated_account = Account {
                available: new_available,
                held: new_held,
                ..account
            };
            storage
                .update_account_for_resolve(tx, updated_account)
                .await?;
        }

        // A chargeback is expected to be the final outcome of a dispute. This should remove the disputed
        // funds from held and total, and also lock the client's account to prevent further transactions.
        // If there's another chargeback on an already-locked account, we should still process it to
        // ensure the disputed funds are removed, but the account should remain locked.
        crate::ports::TxType::Chargeback => {
            let disputed_tx = storage.find_disputed_transaction(tx.tx_id).await?;
            if disputed_tx.client_id != tx.client_id {
                // Transaction client ID mismatch, ignore the chargeback
                return Ok(());
            }
            let new_held = account.held - disputed_tx.amount;
            let new_total = account.total - disputed_tx.amount;
            let updated_account = Account {
                held: new_held,
                total: new_total,
                locked: true,
                ..account
            };
            storage
                .update_account_for_chargeback(tx, updated_account)
                .await?;
        }
    }

    Ok(())
}

pub struct TransactionProcessor {
    parallelism: usize,
    channel_capacity: usize,
    senders: Vec<mpsc::Sender<Transaction>>,
    workers: Vec<JoinHandle<()>>,
    storage: Arc<dyn Storage>,
}

impl TransactionProcessor {
    pub fn builder() -> TransactionProcessorBuilder {
        TransactionProcessorBuilder::new()
    }

    pub fn parallelism(&self) -> usize {
        self.parallelism
    }

    pub fn channel_capacity(&self) -> usize {
        self.channel_capacity
    }

    pub fn storage(&self) -> Arc<dyn Storage> {
        Arc::clone(&self.storage)
    }

    /// Map a `u16` key onto a worker channel index using the consistent-hash
    /// routing function. The result is in `[0, parallelism)`.
    pub fn route(&self, key: u16) -> usize {
        crate::sources::route(key, self.parallelism)
    }

    /// Drive the CSV source against this processor's channels.
    pub async fn ingest_csv(&self, path: &Path) -> anyhow::Result<()> {
        let source = CsvSource::new(path.to_path_buf());
        source.run(self.senders.clone()).await
    }

    /// Get a paginated snapshot of all accounts from storage.
    pub async fn snapshot_accounts(
        &self,
        page: usize,
        page_size: usize,
    ) -> anyhow::Result<Vec<Account>> {
        self.storage.all_accounts(page, page_size).await
    }

    /// Drop all channel senders and await all worker tasks. Required for clean
    /// shutdown — workers exit once their channel is closed.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let TransactionProcessor {
            senders, workers, ..
        } = self;
        drop(senders);
        for handle in workers {
            let _ = handle.await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::process_transaction;
    use crate::ports::{Storage, Transaction, TxType};
    use crate::storage::local_memory::LocalMemoryStorage;

    fn make_tx(tx_type: TxType, client_id: u16, tx_id: u32, amount: f64) -> Transaction {
        Transaction {
            tx_type,
            client_id,
            tx_id,
            amount,
        }
    }

    /// Convenience wrapper: run a transaction against storage, panicking on unexpected errors.
    async fn run(storage: &Arc<LocalMemoryStorage>, transaction: Transaction) {
        process_transaction(
            transaction,
            Arc::clone(storage) as Arc<dyn crate::ports::Storage>,
        )
        .await
        .unwrap();
    }

    // -------------------------------------------------------------------------
    // Deposit
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn deposit_increases_available_and_total() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 100.0);
        assert_eq!(account.total, 100.0);
        assert_eq!(account.held, 0.0);
        assert!(!account.locked);
    }

    #[tokio::test]
    async fn deposit_negative_amount_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, -50.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 0.0);
        assert_eq!(account.total, 0.0);
    }

    #[tokio::test]
    async fn deposit_on_locked_account_still_increases_balance() {
        let storage = Arc::new(LocalMemoryStorage::new());
        // Lock the account via a chargeback
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;
        assert!(storage.get_account(1).await.unwrap().locked);

        // A deposit on a locked account must still be credited
        run(&storage, make_tx(TxType::Deposit, 1, 2, 50.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert!(account.locked);
        assert_eq!(account.available, 50.0);
        assert_eq!(account.total, 50.0);
    }

    // -------------------------------------------------------------------------
    // Withdrawal
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn withdrawal_decreases_available_and_total() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 40.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 60.0);
        assert_eq!(account.total, 60.0);
        assert_eq!(account.held, 0.0);
    }

    #[tokio::test]
    async fn withdrawal_negative_amount_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, -10.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 100.0);
        assert_eq!(account.total, 100.0);
    }

    #[tokio::test]
    async fn withdrawal_with_insufficient_funds_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 50.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 100.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 50.0);
        assert_eq!(account.total, 50.0);
    }

    #[tokio::test]
    async fn withdrawal_on_locked_account_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        // Two deposits so there are funds available after the chargeback removes one
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Deposit, 1, 2, 200.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;

        let before = storage.get_account(1).await.unwrap();
        assert!(before.locked);

        run(&storage, make_tx(TxType::Withdrawal, 1, 3, 50.0)).await;

        let after = storage.get_account(1).await.unwrap();
        assert_eq!(after.available, before.available);
        assert_eq!(after.total, before.total);
    }

    // -------------------------------------------------------------------------
    // Dispute
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn dispute_moves_amount_from_available_to_held() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 0.0);
        assert_eq!(account.held, 100.0);
        assert_eq!(account.total, 100.0);
        assert!(!account.locked);
    }

    #[tokio::test]
    async fn dispute_with_client_id_mismatch_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;

        // Client 2 tries to dispute a transaction belonging to client 1
        run(&storage, make_tx(TxType::Dispute, 2, 1, 0.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 100.0);
        assert_eq!(account.held, 0.0);
        assert_eq!(account.total, 100.0);
    }

    #[tokio::test]
    async fn dispute_on_non_deposit_transaction_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 40.0)).await;

        // Attempting to dispute the withdrawal (tx_id=2) — only deposits may be disputed
        run(&storage, make_tx(TxType::Dispute, 1, 2, 0.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 60.0);
        assert_eq!(account.held, 0.0);
        assert_eq!(account.total, 60.0);
    }

    #[tokio::test]
    async fn dispute_on_locked_account_is_processed() {
        let storage = Arc::new(LocalMemoryStorage::new());
        // Lock the account
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;

        // New deposit on the now-locked account
        run(&storage, make_tx(TxType::Deposit, 1, 2, 200.0)).await;
        let before = storage.get_account(1).await.unwrap();
        assert!(before.locked);

        // Dispute on a locked account must still move funds to held
        run(&storage, make_tx(TxType::Dispute, 1, 2, 0.0)).await;

        let after = storage.get_account(1).await.unwrap();
        assert_eq!(after.available, before.available - 200.0);
        assert_eq!(after.held, before.held + 200.0);
        assert_eq!(after.total, before.total);
        assert!(after.locked);
    }

    // -------------------------------------------------------------------------
    // Resolve
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn resolve_returns_held_funds_to_available() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Resolve, 1, 1, 0.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 100.0);
        assert_eq!(account.held, 0.0);
        assert_eq!(account.total, 100.0);
        assert!(!account.locked);
    }

    #[tokio::test]
    async fn resolve_with_client_id_mismatch_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;

        // Client 2 tries to resolve a dispute belonging to client 1
        run(&storage, make_tx(TxType::Resolve, 2, 1, 0.0)).await;

        let account = storage.get_account(1).await.unwrap();
        // Funds should remain held
        assert_eq!(account.available, 0.0);
        assert_eq!(account.held, 100.0);
        assert_eq!(account.total, 100.0);
    }

    #[tokio::test]
    async fn resolve_on_locked_account_is_processed() {
        let storage = Arc::new(LocalMemoryStorage::new());
        // Lock the account
        run(&storage, make_tx(TxType::Deposit, 1, 1, 50.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;

        // New deposit, then dispute on the locked account
        run(&storage, make_tx(TxType::Deposit, 1, 2, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 2, 0.0)).await;
        let before = storage.get_account(1).await.unwrap();
        assert!(before.locked);

        // Resolve on a locked account must still release held funds
        run(&storage, make_tx(TxType::Resolve, 1, 2, 0.0)).await;

        let after = storage.get_account(1).await.unwrap();
        assert_eq!(after.available, before.available + 100.0);
        assert_eq!(after.held, before.held - 100.0);
        assert_eq!(after.total, before.total);
        assert!(after.locked);
    }

    // -------------------------------------------------------------------------
    // Chargeback
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn chargeback_removes_held_funds_and_locks_account() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 0.0);
        assert_eq!(account.held, 0.0);
        assert_eq!(account.total, 0.0);
        assert!(account.locked);
    }

    #[tokio::test]
    async fn chargeback_with_client_id_mismatch_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;

        // Client 2 tries to chargeback a dispute belonging to client 1
        run(&storage, make_tx(TxType::Chargeback, 2, 1, 0.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.held, 100.0);
        assert_eq!(account.total, 100.0);
        assert!(!account.locked);
    }

    #[tokio::test]
    async fn chargeback_on_already_locked_account_still_removes_held_funds() {
        let storage = Arc::new(LocalMemoryStorage::new());
        // First chargeback to lock the account
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;
        assert!(storage.get_account(1).await.unwrap().locked);

        // Second deposit + dispute + chargeback on an already-locked account
        run(&storage, make_tx(TxType::Deposit, 1, 2, 200.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 2, 0.0)).await;
        let before = storage.get_account(1).await.unwrap();

        run(&storage, make_tx(TxType::Chargeback, 1, 2, 0.0)).await;

        let after = storage.get_account(1).await.unwrap();
        assert_eq!(after.held, before.held - 200.0);
        assert_eq!(after.total, before.total - 200.0);
        assert!(after.locked);
    }

    // -------------------------------------------------------------------------
    // Idempotency
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn duplicate_withdrawals_and_deposits_are_processed_idempotently() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 30.0)).await;
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 30.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 30.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, 70.0);
        assert_eq!(account.total, 70.0);
    }

    #[tokio::test]
    async fn duplicate_disputes_are_processed_idempotently() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;

        let after_first = storage.get_account(1).await.unwrap();
        assert_eq!(after_first.available, 0.0);
        assert_eq!(after_first.held, 100.0);
        assert_eq!(after_first.total, 100.0);

        // Second and third duplicate disputes — each errors at storage level and is swallowed
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;

        let after_duplicates = storage.get_account(1).await.unwrap();
        assert_eq!(after_duplicates.available, after_first.available);
        assert_eq!(after_duplicates.held, after_first.held);
        assert_eq!(after_duplicates.total, after_first.total);
    }

    #[tokio::test]
    async fn duplicate_resolves_are_processed_idempotently() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Resolve, 1, 1, 0.0)).await;

        let after_first = storage.get_account(1).await.unwrap();
        assert_eq!(after_first.available, 100.0);
        assert_eq!(after_first.held, 0.0);
        assert_eq!(after_first.total, 100.0);

        // Duplicate resolves — tx_id=1 is no longer in disputed_transactions
        run(&storage, make_tx(TxType::Resolve, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Resolve, 1, 1, 0.0)).await;

        let after_duplicates = storage.get_account(1).await.unwrap();
        assert_eq!(after_duplicates.available, after_first.available);
        assert_eq!(after_duplicates.held, after_first.held);
        assert_eq!(after_duplicates.total, after_first.total);
    }

    #[tokio::test]
    async fn duplicate_chargebacks_are_processed_idempotently() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;

        let after_first = storage.get_account(1).await.unwrap();
        assert_eq!(after_first.available, 0.0);
        assert_eq!(after_first.held, 0.0);
        assert_eq!(after_first.total, 0.0);
        assert!(after_first.locked);

        // Duplicate chargebacks — tx_id=1 is no longer in disputed_transactions
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;

        let after_duplicates = storage.get_account(1).await.unwrap();
        assert_eq!(after_duplicates.available, after_first.available);
        assert_eq!(after_duplicates.held, after_first.held);
        assert_eq!(after_duplicates.total, after_first.total);
        assert!(after_duplicates.locked);
    }
}
