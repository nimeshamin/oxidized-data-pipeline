pub mod csv_source;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Consistent-hash router: maps a `u32` key to a channel index in
/// `[0, parallelism)`. The result depends on both `key` and `parallelism`,
/// matching the project requirement that the routing function be parameterised
/// by the configured worker count.
pub fn route(key: u32, parallelism: usize) -> usize {
    let n = parallelism.max(1);
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % n
}

#[cfg(test)]
mod tests {
    use super::route;

    #[test]
    fn route_is_in_range() {
        for parallelism in [1, 2, 4, 8, 16] {
            for key in 0..1024u32 {
                assert!(route(key, parallelism) < parallelism);
            }
        }
    }

    #[test]
    fn route_is_deterministic() {
        assert_eq!(route(42, 8), route(42, 8));
        assert_eq!(route(u32::MAX, 4), route(u32::MAX, 4));
    }
}
