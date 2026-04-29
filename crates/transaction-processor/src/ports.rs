//! Hexagonal port definitions.
//!
//! These are placeholder traits — concrete adapters live in `sources::*` and
//! `storage::*`. Keep the surface narrow so the processor core has no knowledge
//! of any specific I/O implementation.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// Placeholder transaction record. Real fields are filled in later — `id` is
/// here so the routing/consistent-hash plumbing has something to key on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: u32,
}

/// Inbound port: an external producer that pushes transactions into the
/// processor's bounded channels.
#[async_trait]
pub trait Source: Send + Sync {
    async fn run(&self, sinks: Vec<mpsc::Sender<Transaction>>) -> anyhow::Result<()>;
}

/// Outbound port: persistence target invoked by each processor worker.
#[async_trait]
pub trait Storage: Send + Sync {
    async fn store(&self, tx: Transaction) -> anyhow::Result<()>;
}
