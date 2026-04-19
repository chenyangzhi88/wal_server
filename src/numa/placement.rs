use super::detection::NumaTopology;

/// Assignment of a shard to a CPU core.
#[derive(Debug, Clone)]
pub struct ShardAssignment {
    pub shard_id: u16,
    pub cpu_id: usize,
    pub numa_node: usize,
}

/// Complete placement plan for the server.
#[derive(Debug, Clone)]
pub struct ShardPlacement {
    /// One acceptor CPU per NUMA node.
    pub acceptor_cpus: Vec<usize>,
    /// Shard-to-CPU assignments.
    pub shard_assignments: Vec<ShardAssignment>,
}

impl ShardPlacement {
    /// Compute placement given a NUMA topology.
    ///
    /// Strategy:
    /// - Reserve the first CPU of each NUMA node for the acceptor.
    /// - Assign remaining CPUs as shard cores.
    /// - If `num_shards` is specified and less than available cores, use that many.
    pub fn compute(topology: &NumaTopology, num_shards: Option<usize>) -> Self {
        let mut acceptor_cpus = Vec::new();
        let mut available_cpus = Vec::new(); // (cpu_id, numa_node_id)

        for node in &topology.nodes {
            if node.cpu_ids.is_empty() {
                continue;
            }
            // Reserve first CPU for acceptor
            acceptor_cpus.push(node.cpu_ids[0]);
            // Rest are available for shards
            for &cpu_id in &node.cpu_ids[1..] {
                available_cpus.push((cpu_id, node.node_id));
            }
        }

        // If no CPUs available for shards (e.g., single-CPU machine),
        // share the acceptor CPU with shard 0
        if available_cpus.is_empty() {
            available_cpus.push((acceptor_cpus[0], topology.nodes[0].node_id));
        }

        let max_shards = available_cpus.len();
        let target_shards = num_shards.unwrap_or(max_shards).min(max_shards);

        let shard_assignments: Vec<ShardAssignment> = available_cpus[..target_shards]
            .iter()
            .enumerate()
            .map(|(i, &(cpu_id, numa_node))| ShardAssignment {
                shard_id: i as u16,
                cpu_id,
                numa_node,
            })
            .collect();

        ShardPlacement {
            acceptor_cpus,
            shard_assignments,
        }
    }
}

/// Bind the current thread to a specific CPU core.
pub fn bind_to_cpu(cpu_id: usize) {
    use nix::sched::{sched_setaffinity, CpuSet};
    use nix::unistd::Pid;

    let mut cpuset = CpuSet::new();
    if cpuset.set(cpu_id).is_err() {
        tracing::warn!(cpu_id, "failed to set CPU in cpuset");
        return;
    }
    if let Err(e) = sched_setaffinity(Pid::from_raw(0), &cpuset) {
        tracing::warn!(cpu_id, "sched_setaffinity failed: {e}");
    } else {
        tracing::debug!(cpu_id, "bound to CPU");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::numa::detection::{NumaNode, NumaTopology};

    #[test]
    fn test_placement_single_node() {
        let topo = NumaTopology {
            nodes: vec![NumaNode {
                node_id: 0,
                cpu_ids: vec![0, 1, 2, 3],
            }],
        };

        let placement = ShardPlacement::compute(&topo, None);
        assert_eq!(placement.acceptor_cpus, vec![0]);
        assert_eq!(placement.shard_assignments.len(), 3);
        assert_eq!(placement.shard_assignments[0].cpu_id, 1);
        assert_eq!(placement.shard_assignments[1].cpu_id, 2);
        assert_eq!(placement.shard_assignments[2].cpu_id, 3);
    }

    #[test]
    fn test_placement_multi_numa() {
        let topo = NumaTopology {
            nodes: vec![
                NumaNode {
                    node_id: 0,
                    cpu_ids: vec![0, 1, 2, 3],
                },
                NumaNode {
                    node_id: 1,
                    cpu_ids: vec![4, 5, 6, 7],
                },
            ],
        };

        let placement = ShardPlacement::compute(&topo, None);
        assert_eq!(placement.acceptor_cpus, vec![0, 4]);
        assert_eq!(placement.shard_assignments.len(), 6);
    }

    #[test]
    fn test_placement_with_limit() {
        let topo = NumaTopology {
            nodes: vec![NumaNode {
                node_id: 0,
                cpu_ids: vec![0, 1, 2, 3, 4, 5, 6, 7],
            }],
        };

        let placement = ShardPlacement::compute(&topo, Some(3));
        assert_eq!(placement.shard_assignments.len(), 3);
    }
}
