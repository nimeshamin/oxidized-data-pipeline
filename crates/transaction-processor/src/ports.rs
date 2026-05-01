//! Hexagonal port definitions.
//!
//! Concrete adapters live in `sources::*` and `storage::*`. Domain types are
//! in `domain`; these traits are intentionally narrow so the processor core
//! has no knowledge of any specific I/O implementation.

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::domain::{Account, Transaction};

/// Genuine faults emitted by storage adapters. Business outcomes (negative
/// amounts, insufficient funds, lifecycle events referencing missing tx_ids,
/// etc.) are NOT errors — they're `ProcessOutcome::SoftFailed(reason)`. This
/// enum is reserved for cases where internal invariants have been violated
/// or storage I/O has failed.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("only Deposit and Withdrawal transactions can be stored as monetary transactions")]
    InvalidTransactionStorageAttempt,
    #[error("expected transaction for atomic update not found or invalid. Logic, data corruption or race condition.")]
    StoreCorruptionDetected,
}

/// Inbound port: an external producer that pushes transactions into the
/// processor's bounded channel.
#[async_trait]
pub trait Source: Send + Sync {
    async fn run(&self, sinks: Vec<mpsc::Sender<Transaction>>) -> anyhow::Result<()>;
}

/// Outbound port: persistence target invoked by each processor worker.
/// Instead of having granular methods for each operation, we want to ensure the ops
/// are atomic at the storage level, so we expose a more general API and let the storage
/// implementation handle the details.
#[async_trait]
pub trait Storage: Send + Sync {
    async fn update_account_for_withdrawal_or_deposit(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()>;
    async fn update_account_for_dispute(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()>;
    async fn update_account_for_resolve(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()>;
    async fn update_account_for_chargeback(
        &self,
        tx: Transaction,
        account: Account,
    ) -> anyhow::Result<()>;
    async fn get_account(&self, client_id: u16) -> anyhow::Result<Account>;
    /// Look up an active (non-disputed) transaction. Returns Ok(None) for a "not in map" result.
    /// Err() is reserved for storage faults that we should stop on.
    async fn find_transaction(&self, tx_id: u32) -> anyhow::Result<Option<Transaction>>;
    /// Look up a disputed transaction. Same Ok(None) semantics as find_transaction.
    async fn find_disputed_transaction(&self, tx_id: u32) -> anyhow::Result<Option<Transaction>>;
    async fn has_transaction_been_processed(&self, tx_id: u32) -> anyhow::Result<bool>;
    async fn all_accounts(&self, page: usize, page_size: usize) -> anyhow::Result<Vec<Account>>;
}
