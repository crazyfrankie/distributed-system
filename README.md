# Distributed System -- From MIT6.824
Mit 6.824 [原课程网站](https://pdos.csail.mit.edu/6.824/schedule.html)

## 完成情况
- [x] [Lab1](#lab1)

## Lab1

### MapReduce 实现概述

本实验实现了一个简化版的 MapReduce 分布式计算框架，基于Google的 [MapReduce论文](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)。MapReduce是一种处理和生成大型数据集的编程模型，被设计用于在大型机器集群上并行化处理。

### 系统架构

本实现采用了 Master-Worker 架构：
- **Master 节点**：负责任务分配和状态跟踪
- **Worker 节点**：负责执行具体的 Map 和 Reduce  任务
- **通信机制**：基于 Go 的 RPC（远程过程调用）实现
- **容错机制**：支持 Worker 故障检测和任务重试

### 工作流程

1. **启动阶段**：
   - Master 启动并初始化Map任务
   - Worker 注册并获取唯一ID
   - Worker 开始发送心跳

2. **Map阶段**：
   - Worker 向 Master 请求任务
   - Master 分配 Map 任务
   - Worker 读取输入文件并执行 Map 函数
   - Worker 将中间结果按 Reduce 任务分区
   - Worker 将中间结果写入临时文件并原子重命名

3. **Reduce 阶段**：
   - 所有 Map 任务完成后，Master 切换到 Reduce 阶段
   - Worker 请求并执行 Reduce 任务
   - Worker 读取所有相关的中间文件并排序
   - Worker 执行 Reduce 函数并生成最终输出文件

4. **完成阶段**：
   - 所有 Reduce 任务完成后，Master 进入完成状态

### Master 实现细节

Master 节点负责全局协调，主要功能包括：

1. **任务状态管理**：
    ```
    type TaskState int
       
    const (
        TaskIdle TaskState = iota    // 任务尚未分配
        TaskInProgress               // 任务正在处理中
        TaskCompleted                // 任务已成功完成
        TaskFailed                   // 任务失败，需要重新分配
    )
    ```

2. **Worker 管理**：
   - 分配唯一 Worker ID
   - 跟踪 Worker 心跳时间
   - 检测超时的 Worker 并重新分配任务

3. **任务分配策略**：
   - 优先分配未处理任务
   - 其次分配失败但未超过重试上限的任务
   - 任务分配时记录开始时间和 Worker ID

4. **阶段转换**：
   - 当所有 Map 任务完成后，自动切换到 Reduce 阶段
   - 当所有 Reduce 任务完成后，Master 进入完成状态

5. **故障检测**：
   - 定期检查任务和 Worker 超时（默认10秒）
   - 将超时任务标记为失败并增加重试计数

### Worker实现细节

Worker节点负责具体计算，主要功能包括：

1. **Worker 生命周期**：
   - 注册获取ID
   - 启动心跳协程
   - 进入主循环（请求任务→执行→报告结果）

2. **Map 任务处理**：
   - 读取输入文件
   - 调用用户定义的 Map 函数
   - 根据键哈希分区
   - 写入 R 个中间文件（R 为 Reduce 任务数量）

3. **Reduce 任务处理**：
   - 读取所有相关中间文件
   - 对所有键值对排序
   - 对每个键调用 Reduce 函数
   - 输出结果到最终文件

4. **容错机制**：
   - 使用临时文件并原子重命名
   - 文件操作失败重试
   - RPC通信错误重试

5. **心跳机制**：
   - 定期向 Master 发送心跳（默认3秒一次）
   - 确保 Master 能检测到 Worker 状态

### 当前实现的不足与改进点

虽然当前实现已具备MapReduce框架的基本功能，但与原始Google MapReduce相比仍有多处不足：

1. **数据局部性缺失**：
   - 当前实现未考虑数据和计算的局部性
   - 改进：实现数据感知的任务分配策略，优先分配数据在本地的任务

2. **Master单点故障**：
   - Master故障会导致整个系统崩溃
   - 改进：实现Master状态持久化和故障恢复机制

