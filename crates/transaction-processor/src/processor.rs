use std::path::Path;
use std::sync::Arc;

use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::ports::{Account, ProcessOutcome, Source, Storage, Transaction};
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

/// Main worker loop for each worker (count defined by `parallelism`). This waits on rx for incoming
/// transactions being pushed by the source. Each transaction is processed synchronously to completion
/// before the next one is pulled from the channel. This loop will only exit on channel closure, which
/// is triggered by the processor's `shutdown` method, or a fatal error (storage I/O failure or
/// violated invariant) that causes a panic.
async fn worker_loop(
    worker_id: usize,
    mut rx: mpsc::Receiver<Transaction>,
    storage: Arc<dyn Storage>,
) {
    while let Some(tx) = rx.recv().await {
        let tx_id = tx.tx_id;
        match process_transaction(tx, Arc::clone(&storage)).await {
            Ok(ProcessOutcome::Applied) => {
                tracing::trace!(worker = worker_id, tx_id, "applied transaction");
            }
            Ok(ProcessOutcome::SoftFailed(reason)) => {
                tracing::debug!(
                    worker = worker_id,
                    tx_id,
                    reason = reason.as_str(),
                    "skipping transaction"
                );
            }
            Err(err) => {
                tracing::error!(
                    worker = worker_id,
                    tx_id,
                    error = %err,
                    "unexpected failure processing transaction"
                );
                // Fail fast: anything reaching this branch is a storage I/O
                // failure or a violated internal invariant — not a recoverable
                // business condition. Business no-ops are `ProcessOutcome::SoftFailed`.
                panic!("worker {worker_id} encountered a fatal error: {err}");
            }
        }
    }
}

/// Apply a single transaction to storage.
/// `Ok(Applied)` - state was mutated.
/// `Ok(SoftFailed(reason))` - valid business no-op.
/// `Err(_)` - genuine fault, such as storage I/O failure or violated invariant.
async fn process_transaction(
    tx: Transaction,
    storage: Arc<dyn Storage>,
) -> anyhow::Result<ProcessOutcome> {
    use crate::ports::{ProcessOutcome::*, ProcessorSoftFailures::*, TxType::*};

    // Dedup monetary tx_ids. Lifecycle events are validated below by their
    // explicit lookup against the active or disputed maps, not via this gate.
    if matches!(tx.tx_type, Deposit | Withdrawal)
        && storage.has_transaction_been_processed(tx.tx_id).await?
    {
        return Ok(SoftFailed(AlreadyProcessed));
    }

    let account = storage.get_account(tx.client_id).await?;

    // Withdrawals on a locked account are silently ignored. Deposits and
    // dispute-lifecycle events on locked accounts must still be processed
    // (e.g. so an in-flight chargeback can complete on a previously locked
    // account).
    if account.locked && tx.tx_type == Withdrawal {
        return Ok(SoftFailed(AccountLocked));
    }

    match tx.tx_type {
        // Credit: increases available and total.
        Deposit => {
            if tx.amount < Decimal::ZERO {
                return Ok(SoftFailed(NegativeAmount));
            }
            let updated = Account {
                available: account.available + tx.amount,
                total: account.total + tx.amount,
                ..account
            };
            storage
                .update_account_for_withdrawal_or_deposit(tx, updated)
                .await?;
            Ok(Applied)
        }

        // Debit: decreases available and total. Rejected if it would overdraw.
        Withdrawal => {
            if tx.amount < Decimal::ZERO {
                return Ok(SoftFailed(NegativeAmount));
            }
            if account.available < tx.amount {
                return Ok(SoftFailed(InsufficientFunds));
            }
            let updated = Account {
                available: account.available - tx.amount,
                total: account.total - tx.amount,
                ..account
            };
            storage
                .update_account_for_withdrawal_or_deposit(tx, updated)
                .await?;
            Ok(Applied)
        }

        // Dispute opens a hold against an existing deposit, moving funds from
        // available → held. Allowed even on locked accounts so chargeback
        // lifecycles can be opened post-lock.
        Dispute => {
            let Some(disputed_tx) = storage.find_transaction(tx.tx_id).await? else {
                return Ok(SoftFailed(DisputedTransactionNotFound));
            };
            if disputed_tx.client_id != tx.client_id {
                return Ok(SoftFailed(ClientIdMismatch));
            }
            if disputed_tx.tx_type != Deposit {
                return Ok(SoftFailed(DisputeOnNonDeposit));
            }
            let updated = Account {
                available: account.available - disputed_tx.amount,
                held: account.held + disputed_tx.amount,
                ..account
            };
            storage.update_account_for_dispute(tx, updated).await?;
            Ok(Applied)
        }

        // Resolve releases held funds back to available; total unchanged.
        Resolve => {
            let Some(disputed_tx) = storage.find_disputed_transaction(tx.tx_id).await? else {
                return Ok(SoftFailed(DisputedTransactionNotFound));
            };
            if disputed_tx.client_id != tx.client_id {
                return Ok(SoftFailed(ClientIdMismatch));
            }
            let updated = Account {
                available: account.available + disputed_tx.amount,
                held: account.held - disputed_tx.amount,
                ..account
            };
            storage.update_account_for_resolve(tx, updated).await?;
            Ok(Applied)
        }

        // Chargeback removes held funds from total and locks the account.
        // A second chargeback for a tx_id no longer in the disputed map is
        // surfaced as `DisputedTransactionNotFound`, not an error.
        Chargeback => {
            let Some(disputed_tx) = storage.find_disputed_transaction(tx.tx_id).await? else {
                return Ok(SoftFailed(DisputedTransactionNotFound));
            };
            if disputed_tx.client_id != tx.client_id {
                return Ok(SoftFailed(ClientIdMismatch));
            }
            let updated = Account {
                held: account.held - disputed_tx.amount,
                total: account.total - disputed_tx.amount,
                locked: true,
                ..account
            };
            storage.update_account_for_chargeback(tx, updated).await?;
            Ok(Applied)
        }
    }
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

    /// Map a `u16` key onto a worker channel index using the
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

    use rust_decimal::Decimal;

    use super::process_transaction;
    use crate::ports::{
        ProcessOutcome, ProcessorSoftFailures, SkipReason, Storage, Transaction, TxType,
    };
    use crate::storage::local_memory::LocalMemoryStorage;

    fn make_tx(tx_type: TxType, client_id: u16, tx_id: u32, amount: f64) -> Transaction {
        Transaction {
            tx_type,
            client_id,
            tx_id,
            amount: Decimal::try_from(amount).unwrap(),
        }
    }

    fn d(v: f64) -> Decimal {
        Decimal::try_from(v).unwrap()
    }

    /// Run a transaction and return the outcome. Panics on fatal errors —
    /// reaching that branch would mean a storage fault or violated invariant,
    /// which a test should never silently accept.
    async fn run(storage: &Arc<LocalMemoryStorage>, transaction: Transaction) -> ProcessOutcome {
        process_transaction(transaction, Arc::clone(storage) as Arc<dyn Storage>)
            .await
            .expect("process_transaction returned a fatal error")
    }

    // -------------------------------------------------------------------------
    // Deposit
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn deposit_increases_available_and_total() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, d(100.0));
        assert_eq!(account.total, d(100.0));
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
    }

    #[tokio::test]
    async fn deposit_negative_amount_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        let outcome = run(&storage, make_tx(TxType::Deposit, 1, 1, -50.0)).await;
        assert_eq!(
            outcome,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::NegativeAmount)
        );

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.total, Decimal::ZERO);
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
        assert_eq!(account.available, d(50.0));
        assert_eq!(account.total, d(50.0));
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
        assert_eq!(account.available, d(60.0));
        assert_eq!(account.total, d(60.0));
        assert_eq!(account.held, Decimal::ZERO);
    }

    #[tokio::test]
    async fn withdrawal_negative_amount_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        let outcome = run(&storage, make_tx(TxType::Withdrawal, 1, 2, -10.0)).await;
        assert_eq!(
            outcome,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::NegativeAmount)
        );

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, d(100.0));
        assert_eq!(account.total, d(100.0));
    }

    #[tokio::test]
    async fn withdrawal_with_insufficient_funds_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 50.0)).await;
        let outcome = run(&storage, make_tx(TxType::Withdrawal, 1, 2, 100.0)).await;
        assert_eq!(
            outcome,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::InsufficientFunds)
        );

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, d(50.0));
        assert_eq!(account.total, d(50.0));
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

        let outcome = run(&storage, make_tx(TxType::Withdrawal, 1, 3, 50.0)).await;
        assert_eq!(
            outcome,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::AccountLocked)
        );

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
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, d(100.0));
        assert_eq!(account.total, d(100.0));
        assert!(!account.locked);
    }

    #[tokio::test]
    async fn dispute_with_client_id_mismatch_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;

        // Client 2 tries to dispute a transaction belonging to client 1
        let outcome = run(&storage, make_tx(TxType::Dispute, 2, 1, 0.0)).await;
        assert_eq!(
            outcome,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::ClientIdMismatch)
        );

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, d(100.0));
        assert_eq!(account.held, Decimal::ZERO);
        assert_eq!(account.total, d(100.0));
    }

    #[tokio::test]
    async fn dispute_on_non_deposit_transaction_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 40.0)).await;

        // Attempting to dispute the withdrawal (tx_id=2) — only deposits may be disputed
        let outcome = run(&storage, make_tx(TxType::Dispute, 1, 2, 0.0)).await;
        assert_eq!(
            outcome,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::DisputeOnNonDeposit)
        );

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, d(60.0));
        assert_eq!(account.held, Decimal::ZERO);
        assert_eq!(account.total, d(60.0));
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
        assert_eq!(after.available, before.available - d(200.0));
        assert_eq!(after.held, before.held + d(200.0));
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
        assert_eq!(account.available, d(100.0));
        assert_eq!(account.held, Decimal::ZERO);
        assert_eq!(account.total, d(100.0));
        assert!(!account.locked);
    }

    #[tokio::test]
    async fn resolve_with_client_id_mismatch_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;

        // Client 2 tries to resolve a dispute belonging to client 1
        let outcome = run(&storage, make_tx(TxType::Resolve, 2, 1, 0.0)).await;
        assert_eq!(
            outcome,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::ClientIdMismatch)
        );

        let account = storage.get_account(1).await.unwrap();
        // Funds should remain held
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, d(100.0));
        assert_eq!(account.total, d(100.0));
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
        assert_eq!(after.available, before.available + d(100.0));
        assert_eq!(after.held, before.held - d(100.0));
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
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, Decimal::ZERO);
        assert_eq!(account.total, Decimal::ZERO);
        assert!(account.locked);
    }

    #[tokio::test]
    async fn chargeback_with_client_id_mismatch_is_ignored() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;

        // Client 2 tries to chargeback a dispute belonging to client 1
        let outcome = run(&storage, make_tx(TxType::Chargeback, 2, 1, 0.0)).await;
        assert_eq!(
            outcome,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::ClientIdMismatch)
        );

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.held, d(100.0));
        assert_eq!(account.total, d(100.0));
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
        assert_eq!(after.held, before.held - d(200.0));
        assert_eq!(after.total, before.total - d(200.0));
        assert!(after.locked);
    }

    // -------------------------------------------------------------------------
    // Idempotency
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn duplicate_withdrawals_and_deposits_are_processed_idempotently() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;

        // The duplicate must surface as SoftFail(AlreadyProcessed) — this is
        // the central guarantee of `has_transaction_been_processed`.
        let dup = run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        assert_eq!(
            dup,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::AlreadyProcessed)
        );

        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 30.0)).await;
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 30.0)).await;
        run(&storage, make_tx(TxType::Withdrawal, 1, 2, 30.0)).await;

        let account = storage.get_account(1).await.unwrap();
        assert_eq!(account.available, d(70.0));
        assert_eq!(account.total, d(70.0));
    }

    #[tokio::test]
    async fn duplicate_disputes_are_processed_idempotently() {
        let storage = Arc::new(LocalMemoryStorage::new());
        run(&storage, make_tx(TxType::Deposit, 1, 1, 100.0)).await;
        run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;

        let after_first = storage.get_account(1).await.unwrap();
        assert_eq!(after_first.available, Decimal::ZERO);
        assert_eq!(after_first.held, d(100.0));
        assert_eq!(after_first.total, d(100.0));

        // Second dispute — tx_id=1 has already moved out of `transactions`,
        // so the outcome is SoftFailed(DisputedTransactionNotFound), not an error.
        let dup = run(&storage, make_tx(TxType::Dispute, 1, 1, 0.0)).await;
        assert_eq!(
            dup,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::DisputedTransactionNotFound)
        );
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
        assert_eq!(after_first.available, d(100.0));
        assert_eq!(after_first.held, Decimal::ZERO);
        assert_eq!(after_first.total, d(100.0));

        // Duplicate resolves — tx_id=1 is no longer in disputed_transactions
        let dup = run(&storage, make_tx(TxType::Resolve, 1, 1, 0.0)).await;
        assert_eq!(
            dup,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::DisputedTransactionNotFound)
        );
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
        assert_eq!(after_first.available, Decimal::ZERO);
        assert_eq!(after_first.held, Decimal::ZERO);
        assert_eq!(after_first.total, Decimal::ZERO);
        assert!(after_first.locked);

        // Duplicate chargebacks — tx_id=1 is no longer in disputed_transactions
        let dup = run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;
        assert_eq!(
            dup,
            ProcessOutcome::SoftFailed(ProcessorSoftFailures::DisputedTransactionNotFound)
        );
        run(&storage, make_tx(TxType::Chargeback, 1, 1, 0.0)).await;

        let after_duplicates = storage.get_account(1).await.unwrap();
        assert_eq!(after_duplicates.available, after_first.available);
        assert_eq!(after_duplicates.held, after_first.held);
        assert_eq!(after_duplicates.total, after_first.total);
        assert!(after_duplicates.locked);
    }
}
