//! Test-harness crate. Integration tests live under `tests/`.
//!
//! Re-exports the `MetricsGuard` helper so individual integration test files
//! can report execution time and peak heap usage with a single line.

pub mod metrics;

pub use metrics::MetricsGuard;
