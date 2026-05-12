# Lab1 MapReduce 核心操作

Lab1 实现的是一个简化版 MapReduce 框架。应用只需要提供：

```text
mapf(filename, contents) -> []KeyValue
reducef(key, values) -> output
```

框架负责：

```text
分配 Map 任务 -> 生成中间文件 -> 分配 Reduce 任务 -> 生成最终输出
```

代码里使用 `Master` 命名，含义就是课程新版里常说的 `Coordinator`。

## 先抓重点

- Master 只负责分配任务、记录状态、处理超时，不亲自执行 Map/Reduce。
- Worker 主动找 Master 要任务，做完后再通知 Master。
- Map 任务输出 `mr-X-Y`，其中 `X` 是 Map 编号，`Y` 是 Reduce 编号。
- Reduce 任务读取所有 `mr-*-Y`，最后写出 `mr-out-Y`。
- 某个 Worker 卡住时，Master 会把执行中的任务重新变回可分配状态。

## 整体架构

```mermaid
flowchart LR
    APP["应用程序<br/>mapf / reducef"] --> W["Worker"]
    W -->|AllocateTask RPC| M["Master / Coordinator"]
    W -->|ReceiveFinishedMap RPC| M
    W -->|ReceiveFinishedReduce RPC| M

    subgraph FS["本地文件系统"]
        IN["输入文件"]
        MID["中间文件<br/>mr-X-Y"]
        OUT["输出文件<br/>mr-out-Y"]
    end

    W --> IN
    W --> MID
    W --> OUT
```

## 数据流总览

```mermaid
flowchart LR
    I0["input-0"] --> M0["Map 0"]
    I1["input-1"] --> M1["Map 1"]
    I2["input-2"] --> M2["Map 2"]

    M0 --> A0["mr-0-0"]
    M0 --> A1["mr-0-1"]
    M1 --> B0["mr-1-0"]
    M1 --> B1["mr-1-1"]
    M2 --> C0["mr-2-0"]
    M2 --> C1["mr-2-1"]

    A0 --> R0["Reduce 0"]
    B0 --> R0
    C0 --> R0
    A1 --> R1["Reduce 1"]
    B1 --> R1
    C1 --> R1

    R0 --> O0["mr-out-0"]
    R1 --> O1["mr-out-1"]
```

这个图里的 `mr-X-Y` 表示：

```text
X = Map task 编号
Y = Reduce task 编号
```

## 核心数据

| 数据 | 位置 | 含义 |
| --- | --- | --- |
| `files []string` | Master | 输入文件列表；每个输入文件对应一个 Map task。 |
| `nMap` | Master | Map task 数量，等于输入文件数量。 |
| `nReduce` | Master | Reduce task 数量。 |
| `maptasklog []int` | Master | Map task 状态：`0` 未分配，`1` 执行中，`2` 已完成。 |
| `reducetasklog []int` | Master | Reduce task 状态：`0` 未分配，`1` 执行中，`2` 已完成。 |
| `mapfinished` | Master | 已完成 Map task 数量。 |
| `reducefinished` | Master | 已完成 Reduce task 数量。 |
| `mu` | Master | 保护任务状态，避免多个 Worker 并发抢同一个任务。 |

## 任务状态机

Map task 和 Reduce task 都用同一套状态数字：

```mermaid
stateDiagram-v2
    [*] --> Unallocated: 0
    Unallocated --> Running: Worker 拿到任务
    Running --> Finished: Worker 汇报完成
    Running --> Unallocated: 10 秒超时未完成
    Finished --> [*]

    Unallocated: 0 未分配
    Running: 1 执行中
    Finished: 2 已完成
```

## 任务类型

| `Tasktype` | 含义 | Worker 行为 |
| --- | --- | --- |
| `0` | Map task | 读取输入文件，调用 `mapf`，写 `mr-X-Y`。 |
| `1` | Reduce task | 读取所有对应 `mr-X-Y`，调用 `reducef`，写 `mr-out-Y`。 |
| `2` | Waiting | 暂时没有可分配任务，Worker 等一会再问。 |
| `3` | Job finished | 全部任务结束，Worker 退出。 |

## 主流程

```mermaid
flowchart TD
    A["MakeMaster(files, nReduce)"] --> B["初始化 Map / Reduce 任务状态"]
    B --> C["启动 RPC server"]
    C --> D["Worker 循环调用 AllocateTask"]
    D --> E{"Map 是否全部完成?"}
    E -->|否| F["分配 Map task"]
    F --> G["Worker 执行 mapf<br/>写 mr-X-Y"]
    G --> H["ReceiveFinishedMap"]
    H --> D

    E -->|是| I{"Reduce 是否全部完成?"}
    I -->|否| J["分配 Reduce task"]
    J --> K["Worker 执行 reducef<br/>写 mr-out-Y"]
    K --> L["ReceiveFinishedReduce"]
    L --> D

    I -->|是| M["返回 Tasktype=3<br/>Worker 退出"]
```

重点：

```text
Reduce 阶段必须等所有 Map task 完成后才开始。
```

## Worker 拉任务循环

```mermaid
flowchart TD
    A["Worker 启动"] --> B["调用 AllocateTask"]
    B --> C{"Tasktype"}
    C -->|0 Map| D["执行 Map task"]
    C -->|1 Reduce| E["执行 Reduce task"]
    C -->|2 Waiting| F["sleep 1s"]
    C -->|3 Finished| G["退出"]

    D --> H["ReceiveFinishedMap"]
    E --> I["ReceiveFinishedReduce"]
    H --> F
    I --> F
    F --> B
```

## Map Task 流程

```mermaid
flowchart TD
    A["Worker 收到 Map task"] --> B["打开输入文件"]
    B --> C["读取文件内容"]
    C --> D["调用 mapf(filename, contents)"]
    D --> E["得到 []KeyValue"]
    E --> F["按 ihash(key) % NReduce 分桶"]
    F --> G["每个 reduce 分区写一个中间文件"]
    G --> H["mr-X-Y<br/>X=Map编号<br/>Y=Reduce编号"]
    H --> I["调用 ReceiveFinishedMap"]
```

中间文件命名：

```text
mr-X-Y

X = Map task 编号
Y = Reduce task 编号
```

同一个 key 一定会进入同一个 Reduce 分区：

```go
ihash(key) % NReduce
```

## 中间文件矩阵

假设有 `nMap = 3`、`nReduce = 2`：

```mermaid
flowchart TD
    subgraph MAP["Map task 输出"]
        M0["Map 0"] --> F00["mr-0-0"]
        M0 --> F01["mr-0-1"]
        M1["Map 1"] --> F10["mr-1-0"]
        M1 --> F11["mr-1-1"]
        M2["Map 2"] --> F20["mr-2-0"]
        M2 --> F21["mr-2-1"]
    end

    subgraph RED["Reduce task 读取"]
        F00 --> R0["Reduce 0"]
        F10 --> R0
        F20 --> R0
        F01 --> R1["Reduce 1"]
        F11 --> R1
        F21 --> R1
    end
```

所以：

```text
每个 Map task 会输出 nReduce 个中间文件。
每个 Reduce task 会读取所有 Map task 对应自己的那一列文件。
```

## Reduce Task 流程

```mermaid
flowchart TD
    A["Worker 收到 Reduce task Y"] --> B["读取所有 mr-X-Y"]
    B --> C["JSON decode KeyValue"]
    C --> D["按 key 排序"]
    D --> E["把相同 key 的 values 聚合"]
    E --> F["调用 reducef(key, values)"]
    F --> G["写入 mr-out-Y"]
    G --> H["删除对应中间文件"]
    H --> I["调用 ReceiveFinishedReduce"]
```

Reduce task `Y` 会读取：

```text
mr-0-Y
mr-1-Y
...
mr-(nMap-1)-Y
```

最终输出文件：

```text
mr-out-Y
```

## Reduce 内部处理顺序

```mermaid
flowchart LR
    A["读取 mr-*-Y"] --> B["得到 []KeyValue"]
    B --> C["按 key 排序"]
    C --> D["扫描相同 key 的连续区间"]
    D --> E["收集 values"]
    E --> F["reducef(key, values)"]
    F --> G["写一行到 mr-out-Y"]
```

## 超时重分配

Master 分配任务后，会把任务状态标成执行中：

```text
tasklog[i] = 1
```

然后启动一个 10 秒检查：

```mermaid
flowchart TD
    A["Master 分配 task i"] --> B["tasklog[i] = 1"]
    B --> C["启动 10 秒计时"]
    C --> D{"10 秒后 tasklog[i] 还是 1?"}
    D -->|是| E["认为 Worker 可能挂了<br/>tasklog[i] = 0<br/>允许重新分配"]
    D -->|否| F["任务已完成<br/>不处理"]
```

这个机制处理的是：

```text
Worker 拿到任务后崩溃 / 卡住 / 没汇报完成
```

注意：任务可能被重新执行，所以 Worker 写文件时使用临时文件再 `Rename` 成正式文件，减少半成品文件被其他任务看到的风险。

## RPC 表

| RPC | 谁调用 | 作用 |
| --- | --- | --- |
| `AllocateTask` | Worker -> Master | 请求一个新任务。 |
| `ReceiveFinishedMap` | Worker -> Master | 汇报 Map task 完成。 |
| `ReceiveFinishedReduce` | Worker -> Master | 汇报 Reduce task 完成。 |
| `Done` | 测试框架 / main | 判断整个 job 是否结束。 |

## Worker / Master RPC 时序

```mermaid
sequenceDiagram
    participant W as Worker
    participant M as Master

    W->>M: AllocateTask
    M-->>W: Map task
    W->>W: 执行 mapf, 写 mr-X-Y
    W->>M: ReceiveFinishedMap

    W->>M: AllocateTask
    M-->>W: Waiting 或 Reduce task
    W->>W: 执行 reducef, 写 mr-out-Y
    W->>M: ReceiveFinishedReduce

    W->>M: AllocateTask
    M-->>W: Job finished
    W->>W: 退出
```

## 代码入口

| 目标 | 文件/函数 |
| --- | --- |
| RPC 参数 | `src/mr/rpc.go` |
| Master 状态 | `src/mr/coordinator.go`: `Master` |
| 任务分配 | `src/mr/coordinator.go`: `AllocateTask` |
| 完成汇报 | `src/mr/coordinator.go`: `ReceiveFinishedMap`、`ReceiveFinishedReduce` |
| Job 完成判断 | `src/mr/coordinator.go`: `Done` |
| Worker 主循环 | `src/mr/worker.go`: `Worker` |
| Map 输出 | `src/mr/worker.go`: Map task 分支 |
| Reduce 输出 | `src/mr/worker.go`: Reduce task 分支 |
