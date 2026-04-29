use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::ports::{Storage, Transaction};

/// Placeholder in-memory storage adapter. Useful for tests and as a worked
/// example of the `Storage` port — a real adapter would target a database.
#[derive(Default)]
pub struct LocalMemoryStorage {
    transactions: Mutex<Vec<Transaction>>,
}

impl LocalMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn len(&self) -> usize {
        self.transactions.lock().await.len()
    }

    pub async fn snapshot(&self) -> Vec<Transaction> {
        self.transactions.lock().await.clone()
    }
}

#[async_trait]
impl Storage for LocalMemoryStorage {
    async fn store(&self, tx: Transaction) -> anyhow::Result<()> {
        self.transactions.lock().await.push(tx);
        Ok(())
    }
}
