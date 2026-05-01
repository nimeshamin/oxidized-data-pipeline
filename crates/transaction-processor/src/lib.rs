//! Transaction processor with hexagonal architecture:
//!
//!   sources (port + adapters)  ->  processor core  ->  storage (port + adapters)
//!
//! The processor owns N bounded mpsc channels (N = parallelism). A single source
//! instance fans out into all channels using a modulo-partition routing function
//! keyed by `u16`. Each channel drains into a worker that delegates to the
//! configured `Storage` adapter.

pub mod ports;
pub mod processor;
pub mod sources;
pub mod storage;

pub use ports::{ProcessOutcome, ProcessorSoftFailures, Source, Storage, Transaction};
pub use processor::{TransactionProcessor, TransactionProcessorBuilder};
pub use sources::route;
