pub mod csv_source;

/// Modulo-partition router: maps a `u16` key to a channel index in
/// `[0, parallelism)`. The result depends on both `key` and `parallelism`,
/// matching the project requirement that the routing function be parameterised
/// by the configured worker count.
pub fn route(key: u16, parallelism: usize) -> usize {
    key as usize % parallelism
}

#[cfg(test)]
mod tests {
    use super::route;

    #[test]
    fn route_is_in_range() {
        for parallelism in [1, 2, 4, 8, 16] {
            for key in 0..1024u16 {
                assert!(route(key, parallelism) < parallelism);
            }
        }
    }

    #[test]
    fn route_is_deterministic() {
        assert_eq!(route(42, 8), route(42, 8));
        assert_eq!(route(u16::MAX, 4), route(u16::MAX, 4));
    }
}
