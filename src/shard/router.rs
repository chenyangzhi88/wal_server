use xxhash_rust::xxh3::xxh3_64;

/// Routes keys to shards using xxh3 hash.
pub struct ShardRouter {
    num_shards: u16,
}

impl ShardRouter {
    pub fn new(num_shards: u16) -> Self {
        Self { num_shards }
    }

    /// Route a key to a shard index.
    #[inline]
    pub fn route(&self, key: &[u8]) -> u16 {
        (xxh3_64(key) % self.num_shards as u64) as u16
    }

    pub fn num_shards(&self) -> u16 {
        self.num_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_deterministic() {
        let router = ShardRouter::new(4);
        let shard1 = router.route(b"key1");
        let shard2 = router.route(b"key1");
        assert_eq!(shard1, shard2);
        assert!(shard1 < 4);
    }

    #[test]
    fn test_route_distribution() {
        let router = ShardRouter::new(8);
        let mut counts = [0u32; 8];
        for i in 0..10000u32 {
            let key = i.to_be_bytes();
            let shard = router.route(&key) as usize;
            counts[shard] += 1;
        }
        // Each shard should get at least some keys
        for count in &counts {
            assert!(*count > 500, "poor distribution: {counts:?}");
        }
    }
}
