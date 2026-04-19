use std::fs;
use std::path::Path;

/// Represents a NUMA node with its CPU cores.
#[derive(Debug, Clone)]
pub struct NumaNode {
    pub node_id: usize,
    pub cpu_ids: Vec<usize>,
}

/// System NUMA topology.
#[derive(Debug, Clone)]
pub struct NumaTopology {
    pub nodes: Vec<NumaNode>,
}

impl NumaTopology {
    /// Detect NUMA topology from /sys/devices/system/node/.
    pub fn detect() -> std::io::Result<Self> {
        let node_dir = Path::new("/sys/devices/system/node");
        if !node_dir.exists() {
            return Ok(Self::single_node_fallback());
        }

        let mut nodes = Vec::new();
        for entry in fs::read_dir(node_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(node_id_str) = name_str.strip_prefix("node") {
                if let Ok(node_id) = node_id_str.parse::<usize>() {
                    let cpulist_path = entry.path().join("cpulist");
                    if cpulist_path.exists() {
                        let cpulist = fs::read_to_string(&cpulist_path)?;
                        let cpu_ids = parse_cpulist(cpulist.trim());
                        nodes.push(NumaNode { node_id, cpu_ids });
                    }
                }
            }
        }

        nodes.sort_by_key(|n| n.node_id);

        if nodes.is_empty() {
            return Ok(Self::single_node_fallback());
        }

        Ok(NumaTopology { nodes })
    }

    /// Fallback: single NUMA node with all online CPUs.
    pub fn single_node_fallback() -> Self {
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        NumaTopology {
            nodes: vec![NumaNode {
                node_id: 0,
                cpu_ids: (0..num_cpus).collect(),
            }],
        }
    }

    /// Total number of CPUs across all nodes.
    pub fn total_cpus(&self) -> usize {
        self.nodes.iter().map(|n| n.cpu_ids.len()).sum()
    }
}

/// Parse a Linux cpulist string like "0-3,5,7-9" into a Vec of CPU IDs.
fn parse_cpulist(s: &str) -> Vec<usize> {
    let mut result = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start_str, end_str)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) = (start_str.parse::<usize>(), end_str.parse::<usize>()) {
                result.extend(start..=end);
            }
        } else if let Ok(cpu) = part.parse::<usize>() {
            result.push(cpu);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpulist_range() {
        assert_eq!(parse_cpulist("0-3"), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_parse_cpulist_mixed() {
        assert_eq!(parse_cpulist("0-2,5,7-9"), vec![0, 1, 2, 5, 7, 8, 9]);
    }

    #[test]
    fn test_parse_cpulist_single() {
        assert_eq!(parse_cpulist("3"), vec![3]);
    }

    #[test]
    fn test_single_node_fallback() {
        let topo = NumaTopology::single_node_fallback();
        assert_eq!(topo.nodes.len(), 1);
        assert!(!topo.nodes[0].cpu_ids.is_empty());
    }
}
