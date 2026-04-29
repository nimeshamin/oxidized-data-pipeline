use std::time::Instant;

use peak_alloc::PeakAlloc;

/// Global tracking allocator. Each integration-test binary installs this via
/// `#[global_allocator]`. Heap usage is process-global, so tests that need
/// isolated peaks should run with `--test-threads=1`.
pub static PEAK_ALLOC: PeakAlloc = PeakAlloc;

/// RAII guard: prints elapsed time and peak heap usage when dropped.
pub struct MetricsGuard {
    name: &'static str,
    start: Instant,
    baseline_peak_kb: f32,
}

impl MetricsGuard {
    pub fn new(name: &'static str) -> Self {
        let baseline_peak_kb = PEAK_ALLOC.peak_usage_as_kb();
        Self {
            name,
            start: Instant::now(),
            baseline_peak_kb,
        }
    }
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        let peak_kb = PEAK_ALLOC.peak_usage_as_kb();
        let delta_kb = (peak_kb - self.baseline_peak_kb).max(0.0);
        println!(
            "[metrics] test={name} elapsed={elapsed:?} peak_heap={peak_kb:.1}KB \
             delta_since_test_start={delta_kb:.1}KB",
            name = self.name,
            elapsed = elapsed,
            peak_kb = peak_kb,
            delta_kb = delta_kb,
        );
    }
}
