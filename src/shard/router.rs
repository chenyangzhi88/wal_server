use xxhash_rust::xxh3::xxh3_64;

/// Routes streams to shards using a stable xxh3 hash of the stream id.
pub struct ShardRouter {
    num_shards: u16,
}

impl ShardRouter {
    pub fn new(num_shards: u16) -> Self {
        Self { num_shards }
    }

    /// Route a stream id to a shard index.
    #[inline]
    pub fn route_stream(&self, stream_id: u64) -> u16 {
        (xxh3_64(&stream_id.to_be_bytes()) % self.num_shards as u64) as u16
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
        let shard1 = router.route_stream(42);
        let shard2 = router.route_stream(42);
        assert_eq!(shard1, shard2);
        assert!(shard1 < 4);
    }

    #[test]
    fn test_route_distribution() {
        let router = ShardRouter::new(8);
        let mut counts = [0u32; 8];
        for i in 0..10000u32 {
            let shard = router.route_stream(i as u64) as usize;
            counts[shard] += 1;
        }
        // Each shard should get at least some keys
        for count in &counts {
            assert!(*count > 500, "poor distribution: {counts:?}");
        }
    }
}
