# 角色定义
你是一个顶级的 C++/Rust 底层系统研发专家，精通分布式共识算法（Raft）、异步 I/O (io_uring/epoll)、零拷贝技术（Zero-copy）以及高性能存储引擎架构。

# 任务目标
请帮我实现一个**极度追求低延迟和高吞吐的分布式 WAL (Write-Ahead Log) Server**。它是为一个分布式数据库提供日志服务的底层基础设施。

# 整体系统上下文
1. **Meta Server (控制面)**：负责流 (Stream) 的分配、DB Writer 的选主以及颁发递增的 Epoch 纪元号。
2. **DB Primary Writer (生产者)**：向 WAL Server 写入日志，携带特定 Stream ID 和当前的 Epoch。
3. **DB Reader (消费者)**：从 WAL Server 读取日志，消费后立刻丢弃，不涉及长期存储。
4. **WAL Server (数据面，也就是你要编写的系统)**：基于 Raft 协议的多路复用日志节点。

# 核心架构设计与工程规范 (绝对遵守)

## 1. 线程模型与 I/O 架构：Per-core & 纯异步
* **架构要求**：采用 Thread-per-core 架构（Shared-nothing）。一个 Raft Group 严格绑定到一个 CPU Core 上运行。
* **多路复用 (Multi-Raft)**：一个 Raft Group 会负责处理多个独立的 Stream ID。
* **致命约束**：绝对不允许在该 Core 上发生任何阻塞 (Blocking) I/O 导致 Raft 心跳饿死。网络层和磁盘文件 I/O 必须全面采用纯异步事件驱动（建议封装 `io_uring` 或严格的 AIO）。

## 2. 写路径与防脑裂 (Epoch Fencing)
* DB Writer 发起的每条 Append 强求必须携带 `(StreamID, Epoch, Payload)`。
* WAL Server 收到写请求时，在 Raft Propose 前或 State Machine Apply 时，必须校验 Epoch：**如果请求的 Epoch 小于当前记录的最大 Epoch，直接拒绝写入（Reject）**。这是防止 DB 发生旧主脑裂双写的核心机制。

## 3. 状态机去负载化 (Bypass Payload / No Double Write)
* **致命约束**：绝对禁止传统的 Raft 双写机制（禁止把日志 payload 再存入 RocksDB/LevelDB 等存储引擎）。**Raft 的裸物理日志文件就是最终的数据存储。**
* **轻量级状态机**：Raft 的 State Machine Apply 时，只需要从物理日志中解析出 Header，在内存中构建稀疏索引：`{StreamID -> List<物理文件 Offset, Length>}`，同时记录各个 Stream 的最近写入位点和已消费位点。

## 4. 读路径分离与零拷贝 (Zero-copy)
* **热读 (Tailing Read)**：为刚 Append 完成的数据在内存中保留少量 Cache。绝大多数 DB Reader 的读取请求应直接命中此内存 Cache 返回。
* **冷读 (Catch-up Read)**：当 DB Reader 发生延迟，需要读取历史数据时：
  1. 通过内存的 Stream 索引查到物理日志的 FD 和对应的 Offset。
  2. **强制使用 Zero-copy**（如 Linux `sendfile` 或 `splice` API），直接将裸物理文件中的数据通过 OS Page Cache 发送到网卡（Socket），全程绝对不允许发生 CPU Payload 拷贝！

## 5. 日志清理 (Garbage Collection) 与轻量级 Snapshot
* WAL 数据生命周期极短，DB Reader 消费完毕即可丢弃。
* **水位线计算**：周期性计算当前 Raft Group 中所有活跃 Stream 的最小消费位点（Min Watermark）。
* **截断机制**：向底层的 Raft Log 存储引擎提交 `truncate_prefix`，一刀切地删除最小水位线之前的物理日志文件。
* **僵尸流剔除**：如果检测到某个 Stream 长时间未消费（Laggard），导致整体物理日志无法 Truncate（撑爆磁盘风险），要求能够强制封禁该 Stream 并跨越它的水位线。
* **Snapshot**：状态机打快照时，仅保存轻量的“索引信息、最大 Epoch 和消费水位线”至快照文件。宕机重启时，通过加载此轻量快照，并向后顺序扫描少量未 Commit 的裸物理日志来重建内存索引。

# 你的第一步交付物
在开始写具体代码之前，请先根据上述架构约束：
1. 帮我设计出核心的**内存数据结构**（如轻量级状态机、内存索引稀疏表、Epoch 状态表）。
2. 写出 **Write Path (写路径)** 和 **Read Path (热读/冷读零拷贝路径)** 的核心处理流（伪代码或高亮接口说明）。
3. 列出你认为在 C++/Rust 实现这个 io_uring + sendfile 组合时，最需要注意的底层陷阱。