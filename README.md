# MIT 6.5840 Labs 项目进度

本仓库是 MIT 6.5840 分布式系统课程实验实现。下面的进度根据当前代码、笔记目录和已保存的测试结果整理。

## 版本说明

这个仓库的代码骨架来自 `6.5840-golabs-2024` 模板，也就是更早一版 MIT 6.5840/6.824 实验骨架。它和 2026 课程网页不是同一套 starter code。

和 2026 版相比，主要差异在实验结构本身，而不只是日期：

- 2026 的 Lab 2 是带 `version` 的单机 KV server，当前仓库的 Lab 2 是 `Get/Put/Append` 的单机 KV 服务。
- 2026 的 Lab 4 先做通用 `rsm`，再做基于 `rsm` 的 KV 服务；当前仓库的 Lab 4 是直接在 `Raft` 上实现 `kvraft`。
- 2026 的 Lab 5 使用 `shardctrler`、`shardgrp`、`shardkv1` 这一套新骨架；当前仓库的 Lab 5 是经典的 `shardctrler + shardkv` 结构。

Lab 1 和 Lab 3 在大方向上仍然是 MapReduce 和 Raft，但具体目录名、测试入口、包拆分也和 2026 版不完全一样。
如果要严格对照课程提交，请以你当年的 schedule 和 lab 页面为准；这份 README 主要描述的是这个仓库自己的实现进度。

## Lab 总览

| Lab | 实验内容 | 当前进度 | 解法路径 |
| --- | --- | --- | --- |
| Lab 1: MapReduce | 实现一个简化版分布式 MapReduce 框架，由 Coordinator 分配 Map/Reduce 任务，Worker 执行任务、生成中间文件并输出结果。 | 已完成代码实现。包含任务分配、Map/Reduce 阶段切换、Worker 超时后的任务重试、中间文件 `mr-X-Y` 与最终输出 `mr-out-X`。仓库内未保存单独测试结果文件，可用 `src/main/test-mr.sh` 复测。 | `src/mr/`，核心文件为 `src/mr/coordinator.go`、`src/mr/worker.go`、`src/mr/rpc.go`；测试入口在 `src/main/test-mr.sh`。 |
| Lab 2: Key/Value Server | 实现单机 Key/Value 服务，支持 `Get`、`Put`、`Append`，并在 RPC 可能丢失时保证客户端重试不会导致重复执行。 | 已完成代码实现。服务端使用锁保护内存 map，并通过请求 ID 缓存处理重复请求；客户端失败后持续重试，完成后调用 `Finish` 清理请求记录。仓库内未保存单独测试结果文件。 | `src/kvsrv/`，核心文件为 `src/kvsrv/server.go`、`src/kvsrv/client.go`、`src/kvsrv/common.go`。 |
| Lab 3: Raft | 实现 Raft 共识算法，包括领导者选举、日志复制、持久化和快照，使上层服务能在故障、重启和网络分区下复制状态机命令。 | 已完成 Lab 3A-3D 的代码实现。`src/raft/test_result/test_2_500times.txt` 中保存了 `go test -run 3C` 的多轮通过记录；快照相关 3D 代码已实现，但仓库内未看到单独保存的 3D 测试结果。 | `src/raft/`，核心文件为 `src/raft/raft.go`、`src/raft/persister.go`、`src/raft/util.go`；循环测试脚本为 `src/raft/run_test.sh`。 |
| Lab 4: Fault-tolerant Key/Value Service | 基于 Lab 3 的 Raft 实现容错 Key/Value 服务，所有 `Get`、`Put`、`Append` 通过 Raft 达成一致，并支持客户端去重和快照压缩日志。 | 已完成代码实现。Lab 4A 已有 `go test -run 4A -race` 的 10 轮通过记录，保存在 `src/kvraft/test_result/result.txt`；Lab 4B 快照逻辑已写入代码，但仓库内未看到单独保存的 4B 测试结果。 | `src/kvraft/`，核心文件为 `src/kvraft/server.go`、`src/kvraft/client.go`、`src/kvraft/common.go`；循环测试脚本为 `src/kvraft/run_test.sh`。 |
| Lab 5: Sharded Key/Value Service | 实现分片 Key/Value 服务。ShardCtrler 负责管理配置变更和分片分配，ShardKV 负责多 Raft 组之间的分片读写、迁移、拒绝错误分片请求和故障恢复。 | 未完成。目前 `src/shardctrler/server.go`、`src/shardctrler/client.go`、`src/shardkv/server.go` 仍保留大量模板逻辑和空 RPC 处理函数，尚未形成可运行解法。 | 待实现路径为 `src/shardctrler/` 与 `src/shardkv/`；测试文件分别在 `src/shardctrler/test_test.go`、`src/shardkv/test_test.go`。 |

## 各 Lab 说明

### Lab 1: MapReduce

Lab 1 的目标是实现 MapReduce 运行框架，而不是具体的词频统计逻辑。应用只需要提供 `mapf` 和 `reducef`，框架负责读取输入文件、分配任务、生成中间文件、按 key 分区并执行 Reduce。

当前实现位于 `src/mr/`。`coordinator.go` 负责维护任务状态、向 Worker 分配任务，并在任务超过 10 秒未完成时重新分配；`worker.go` 负责执行 Map/Reduce 逻辑、写入 `mr-X-Y` 中间文件和 `mr-out-X` 输出文件。

### Lab 2: Key/Value Server

Lab 2 的目标是实现一个单机 KV 服务。它需要支持基本的 `Get`、`Put`、`Append` RPC，并处理网络不可靠时客户端重复发送请求的问题。

当前实现位于 `src/kvsrv/`。服务端用 `map[string]string` 保存数据，用请求 ID 记录已经处理过的 `Put/Append`，避免同一个请求被重复执行；客户端在 RPC 失败时持续重试。

### Lab 3: Raft

Lab 3 的目标是实现 Raft 共识模块，为后续容错 KV 服务提供复制日志能力。

- Lab 3A: 实现 leader election 和 heartbeat。
- Lab 3B: 实现日志复制、提交和应用。
- Lab 3C: 实现 `currentTerm`、`votedFor`、日志等 Raft 状态持久化。
- Lab 3D: 实现 snapshot 与 InstallSnapshot，避免日志无限增长。

当前实现位于 `src/raft/`。已有测试记录主要覆盖 3C，多轮结果保存在 `src/raft/test_result/test_2_500times.txt`。

### Lab 4: Fault-tolerant Key/Value Service

Lab 4 的目标是在 Raft 之上实现线性一致的容错 KV 服务。客户端请求先交给当前 leader，再通过 Raft 日志复制到多数节点，提交后由 KVServer 应用到本地状态机。

- Lab 4A: 不使用快照，实现基于 Raft 的容错 KV 服务。
- Lab 4B: 增加快照，当 Raft 日志过大时压缩 KV 状态，重启后可从快照恢复。

当前实现位于 `src/kvraft/`。`server.go` 中实现了请求提交、等待 Raft apply、重复请求过滤、状态机更新和快照保存/恢复；`client.go` 中实现了 leader 重试和请求 ID 生成。已有测试结果文件 `src/kvraft/test_result/result.txt` 记录了 4A 的 10 轮 race 测试通过。

### Lab 5: Sharded Key/Value Service

Lab 5 的目标是把 KV 服务扩展为分片系统。ShardCtrler 维护全局配置，决定每个 shard 属于哪个 replica group；ShardKV 则需要根据配置处理请求、迁移 shard，并在配置变更、故障、重启和网络不可靠时保持正确性。

当前 Lab 5 尚未完成。`src/shardctrler/` 和 `src/shardkv/` 主要还是课程骨架代码，其中服务端 RPC 处理和状态机逻辑尚未实现。

## 常用测试命令

```bash
# Lab 1
cd src/main
bash test-mr.sh

# Lab 2
cd src/kvsrv
go test

# Lab 3
cd src/raft
go test -run 3A
go test -run 3B
go test -run 3C
go test -run 3D

# Lab 4
cd src/kvraft
go test -run 4A -race
go test -run 4B -race

# Lab 5
cd src/shardctrler
go test
cd ../shardkv
go test
```
