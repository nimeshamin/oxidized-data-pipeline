//! Hexagonal port definitions.
//!
//! These are placeholder traits — concrete adapters live in `sources::*` and
//! `storage::*`. Keep the surface narrow so the processor core has no knowledge
//! of any specific I/O implementation.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// Used by process_transaction to decide whether an error should be
/// silently swallowed (e.g. a lifecycle event referencing a non-existent or
/// already-processed transaction) or propagated to the caller.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    /// The referenced transaction could not be found in any storage bucket.
    #[error("transaction not found")]
    NotFound,
    #[error("insufficient available funds")]
    InsufficientFunds,
    #[error("invalid or negative amount specified for transaction")]
    InvalidTransactionAmount,
    #[error("only Deposit and Withdrawal transactions can be stored as monetary transactions")]
    InvalidTransactionStorageAttempt,
    #[error("expected transaction for ACID update not found or invalid. Logic, data corruption or race condition.")]
    StoreCorruptionDetected,
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
    pub amount: f64,
}

/// Client account state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub client_id: u16,
    pub available: f64,
    pub held: f64,
    pub total: f64,
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
    async fn find_transaction(&self, tx_id: u32) -> anyhow::Result<Transaction>;
    async fn find_disputed_transaction(&self, tx_id: u32) -> anyhow::Result<Transaction>;
    async fn has_transaction_been_processed(&self, tx_id: u32) -> anyhow::Result<bool>;
    async fn all_accounts(&self, page: usize, page_size: usize) -> anyhow::Result<Vec<Account>>;
}
