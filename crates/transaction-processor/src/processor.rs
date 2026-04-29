use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::ports::{Source, Storage, Transaction};
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
        if let Err(err) = storage.store(tx).await {
            tracing::error!(worker = worker_id, error = %err, "storage adapter failed");
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

    /// Map a `u32` key onto a worker channel index using the consistent-hash
    /// routing function. The result is in `[0, parallelism)`.
    pub fn route(&self, key: u32) -> usize {
        crate::sources::route(key, self.parallelism)
    }

    /// Drive the CSV source against this processor's channels.
    pub async fn ingest_csv(&self, path: &Path) -> anyhow::Result<()> {
        let source = CsvSource::new(path.to_path_buf());
        source.run(self.senders.clone()).await
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
