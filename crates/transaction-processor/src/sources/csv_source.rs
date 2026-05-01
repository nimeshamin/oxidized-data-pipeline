use std::path::PathBuf;

use async_trait::async_trait;
use csv_async::{AsyncReaderBuilder, Error as CsvError, ErrorKind};
use futures::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::fs::File;
use tokio::sync::mpsc;

use crate::domain::{Transaction, TxType};
use crate::ports::Source;

/// Separate CsvTransaction struct for deserialization, since the CSV may have different field
/// names and types than the internal Transaction struct. This also prevents breaking changes to
/// the internal Transaction struct if we need to ignore or modify/process some types.
#[derive(Debug, Deserialize)]
struct CsvTransaction {
    #[serde(rename = "type")]
    tx_type: TxType,
    #[serde(rename = "client", default)]
    client_id: u16,
    #[serde(rename = "tx", default)]
    tx_id: u32,
    #[serde(default)]
    amount: Option<Decimal>,
}

impl CsvTransaction {
    fn into_transaction(self) -> Transaction {
        Transaction {
            tx_type: self.tx_type,
            client_id: self.client_id,
            tx_id: self.tx_id,
            amount: self.amount.unwrap_or(Decimal::ZERO),
        }
    }
}

/// Single-instance CSV source. Reads rows asynchronously via `csv-async` and
/// fans them out across the supplied bounded channels using the modulo-partition
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
        let mut records = reader.deserialize::<CsvTransaction>();

        while let Some(record) = records.next().await {
            match record {
                Ok(csv_tx) => {
                    let tx = csv_tx.into_transaction();
                    let idx = super::route(tx.client_id, parallelism);
                    sinks[idx]
                        .send(tx)
                        .await
                        .map_err(|e| anyhow::anyhow!("worker channel closed: {e}"))?;
                }
                Err(err) => {
                    if !is_ignorable_error(&err) {
                        return Err(err.into());
                    }
                }
            }
        }
        Ok(())
    }
}

/// Check to see if a `CsvError` is ignorable, specifically for invalid transaction types.
/// Returns `true` if the error is a deserialization error for an invalid transaction type,
/// and `false` otherwise
fn is_ignorable_error(err: &CsvError) -> bool {
    match err.kind() {
        ErrorKind::Deserialize { err: de_err, .. } => {
            let line = err.position().map(|pos| pos.line()).unwrap_or(0);
            let field = de_err.field();
            let kind = de_err.kind();
            tracing::debug!(line, field, kind = ?kind, "CSV row deserialization error");
            return true;
        }
        ErrorKind::UnequalLengths {
            pos,
            expected_len,
            len,
        } => {
            let row = pos.as_ref().map(|p| p.line()).unwrap_or(0);
            tracing::debug!(
                row,
                expected_len,
                len,
                "CSV row with unexpected number of fields"
            );
            return true;
        }
        // All other error kinds are not ignorable
        _ => false,
    }
}
