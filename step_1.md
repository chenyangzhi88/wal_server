# 角色与背景
你是一个顶级的 C++/Rust 底层系统研发专家。我们正在开发一个高性能的分布式 WAL Server。为了降低工程复杂度，我们将分阶段进行。当前是 **Phase 1：底层单 Stream 的 I/O 与零拷贝验证**。
当前阶段我们不需要关心 Meta Server、Epoch 脑裂和多 Stream 复用，只需要把最核心的纯异步写和 Zero-copy 读跑通。

# Phase 1 核心任务
实现一个运行在单 CPU Core 上的 Event Loop（基于 io_uring 或 epoll+AIO），并在其上实现一个最基础的裸文件追加写与零拷贝读引擎。

# 核心约束 (绝对遵守)
1. **Thread-per-core**：整个生命周期只能有一个线程跑 Event Loop，绝对禁止任何阻塞 (Blocking) 系统调用。
2. **写路径 (Append)**：实现一个异步接收网络请求，将数据按顺序追加到本地裸文件 (`.log` 文件) 的功能。
3. **读路径 (Zero-copy)**：实现接收偏移量 (Offset) 和长度 (Length) 的读取请求，**必须使用 `sendfile` 或 `splice`** 将文件内容直接发送到 Socket，严禁将数据 Read 到用户态内存再 Send。

# 你的交付物
请给出 Phase 1 的核心代码框架（C++ 或 Rust）：
1. 封装好的底层异步 I/O 模块和网络收发模块的接口定义。
2. 一段能够展现从 `accept` -> 异步文件 `append` -> 异步 `sendfile` 返回闭环的核心事件处理伪代码/核心逻辑。