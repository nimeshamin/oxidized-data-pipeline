use std::path::PathBuf;

use async_trait::async_trait;
use csv_async::AsyncReaderBuilder;
use futures::StreamExt;
use tokio::fs::File;
use tokio::sync::mpsc;

use crate::ports::{Source, Transaction};

/// Single-instance CSV source. Reads rows asynchronously via `csv-async` and
/// fans them out across the supplied bounded channels using the consistent-hash
/// router.
pub struct CsvSource {
    path: PathBuf,
}

impl CsvSource {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl Source for CsvSource {
    async fn run(&self, sinks: Vec<mpsc::Sender<Transaction>>) -> anyhow::Result<()> {
        let parallelism = sinks.len().max(1);
        let file = File::open(&self.path).await?;
        let mut reader = AsyncReaderBuilder::new()
            .has_headers(true)
            .create_deserializer(file);
        let mut records = reader.deserialize::<Transaction>();

        while let Some(record) = records.next().await {
            let tx = record?;
            let idx = super::route(tx.id, parallelism);
            sinks[idx]
                .send(tx)
                .await
                .map_err(|e| anyhow::anyhow!("worker channel closed: {e}"))?;
        }
        Ok(())
    }
}
