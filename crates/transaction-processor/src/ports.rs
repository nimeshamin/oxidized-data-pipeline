//! Hexagonal port definitions.
//!
//! These are placeholder traits — concrete adapters live in `sources::*` and
//! `storage::*`. Keep the surface narrow so the processor core has no knowledge
//! of any specific I/O implementation.

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// Genuine faults emitted by storage adapters. Business outcomes (negative
/// amounts, insufficient funds, lifecycle events referencing missing tx_ids,
/// etc.) are NOT errors — they're `ProcessOutcome::Skipped(reason)`. This
/// enum is reserved for cases where internal invariants have been violated
/// or storage I/O has failed.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("only Deposit and Withdrawal transactions can be stored as monetary transactions")]
    InvalidTransactionStorageAttempt,
    #[error("expected transaction for atomic update not found or invalid. Logic, data corruption or race condition.")]
    StoreCorruptionDetected,
}

/// Soft failures that the processor can encounter, but continue processing. These are *expected*
/// business outcomes, that are encountered from things like malformed input, disputes on
/// non-deposit transactions, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorSoftFailures {
    /// Monetary tx_id has already been ingested in this run.
    AlreadyProcessed,
    /// Deposit/Withdrawal arrived with a negative amount.
    NegativeAmount,
    /// Withdrawal amount exceeded available funds.
    InsufficientFunds,
    /// Withdrawal targeted a locked account.
    AccountLocked,
    /// Lifecycle event (Dispute/Resolve/Chargeback) referenced a tx_id that
    /// is not currently in the active or disputed map.
    DisputedTransactionNotFound,
    /// Dispute referenced a non-deposit transaction.
    DisputeOnNonDeposit,
    /// Lifecycle event referenced a tx_id whose owner is a different client.
    ClientIdMismatch,
}

impl ProcessorSoftFailures {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyProcessed => "already_processed",
            Self::NegativeAmount => "negative_amount",
            Self::InsufficientFunds => "insufficient_funds",
            Self::AccountLocked => "account_locked",
            Self::DisputedTransactionNotFound => "disputed_transaction_not_found",
            Self::DisputeOnNonDeposit => "dispute_on_non_deposit",
            Self::ClientIdMismatch => "client_id_mismatch",
        }
    }
}

/// Result of processing a single transaction.
/// Applied => storage was mutated
/// SoftFailed => transaction was a valid business no-op
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    Applied,
    SoftFailed(ProcessorSoftFailures),
}

/// Type of tx coming in from the source.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// Internal transaction representation used for routing and processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub tx_type: TxType,
    pub client_id: u16,
    pub tx_id: u32,
    pub amount: Decimal,
}

/// Client account state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub client_id: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
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
