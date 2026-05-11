# 6.5840 Lab 5: Sharded Key/Value Service 中文阅读导览

<style>
body { max-width: 45em; color: black; background-color: white; font-family: sans-serif; }
pre { overflow-x: auto; margin: 1em; border: 1px dashed #839496; padding: 1em; font-size: 100%; color: #839496; background: #002b36; }
code, tt { font-family: monospace; border-radius: 3px; font-size: 110%; color: #657b83; background-color: #fdf6e3; padding: 0 0.2em; word-wrap: break-word; }
.difficulty.easy { color: #00cc00; }
.difficulty.moderate { color: #0066ff; }
.difficulty.hard { color: #ff3300; }
</style>

来源页面: http://nil.csail.mit.edu/6.5840/2024/labs/lab-shard.html

说明: 这是对页面要求的中文导读和要点整理，不是逐句完整译文。

## 总体目标

这个实验要求你构建一个分片的 key/value 存储系统。系统会把 key 空间切成多个 shard，再把这些 shard 分配给多个 replica group。每个 replica group 内部用 Raft 复制自己的数据。

系统有两个主要部分:

- `shardctrler`: 分片控制器，负责维护“哪个 shard 由哪个 replica group 服务”的配置。
- `shardkv`: 真正提供 Get/Put/Append 的分片 key/value 服务。

分片的目的主要是提高吞吐量。不同 replica group 可以并行处理不同 shard 上的请求，因此系统总体处理能力能随 group 数量增加而提高。

## 关键概念

`configuration` 指 shard 到 replica group 的分配关系。它不是 Raft 的成员变更，不需要你实现 Raft 集群成员变更。

配置会随着 Join、Leave、Move 等操作变化。client 需要向 shardctrler 查询某个 key 应该找哪个 group；shardkv server 也要查询 shardctrler，知道自己当前应该服务哪些 shard。

一个 shard 在任意时刻最多只能被一个 replica group 对外服务。实验最重要的难点是: 当配置切换和客户端请求并发发生时，所有副本必须对请求与配置变更的先后顺序达成一致。

推荐思路是: replica group 的 Raft 日志里不仅记录 Put/Append/Get，也记录配置变更。这样同一个 group 的所有副本都会在相同日志位置切换配置。

<div style="margin:1em 0; border:1px dashed #4682B4; padding:1em; color:#4682B4;"><strong>Note:</strong> 只能通过 RPC 交互；这里的 configuration 指 shard 分配，不是 Raft 成员变更；Lab 5 的 sharded server、Lab 5 的 shard controller 和 Lab 4 的 kvraft 必须使用同一份 Raft 实现。</div>

## 起步

<div style="margin:1em 0; padding:1em; background-color:#990000; color:#fff;"><strong style="display:block; background:#550000; margin:-1em -1em 1em -1em; padding:0.7em 1em;">Important</strong>执行 `git pull` 获取最新 lab 软件。</div>

骨架代码和测试主要在:

- `src/shardctrler`
- `src/shardkv`

最终需要通过 `shardctrler` 和 `shardkv` 里的测试。

## 当前仓库实现状态

当前仓库使用的是经典 `shardctrler + shardkv` 骨架，不是 2026 版 `shardkv1/shardgrp` 骨架。代码已经完成 Lab 5A/5B：`shardctrler` 负责通过 Raft 复制配置变更并确定性 rebalance，`shardkv` 负责按配置服务 shard、拒绝错误 group 的请求、迁移 shard 数据和 client 去重状态，并在迁移完成后回收旧 shard。

当前 `shardkv` 的迁移主线是 `Pulling -> InsertShard -> GCing -> DeleteShards -> Serving`。也就是说，新 owner 先拉取旧 owner 的 shard 数据和 `LastRequestMap`，写入本组 Raft 后进入等待 GC 状态，再通知旧 owner 删除旧数据，最后恢复为可服务状态。实现中没有保留额外的 `monitorBePulling/CheckShards` 主动确认流程。

最近用于验证 Lab 5B 的命令是:

```sh
cd src/shardkv
go test -run 5B -count=1 -timeout 600s
```

## Part A: Controller And Static Sharding <span class="difficulty easy">(easy)</span>

Part A 分两块:

- 实现 `shardctrler/server.go` 和 `shardctrler/client.go`。
- 在 `shardkv` 中实现能处理静态配置的 key/value 服务，通过 `5A` 测试。

完成 Part A 后，目标是:

- `src/shardctrler` 下的所有测试通过。
- `src/shardkv` 下 `5A` 相关测试通过。

### shardctrler 要维护什么

shardctrler 维护一串有编号的配置。每个配置包含:

- 当前有哪些 replica group。
- 每个 shard 分配给哪个 group。

当分配关系变化时，shardctrler 创建一个新的配置。client 和 server 可以查询最新配置，也可以查询历史配置。

### shardctrler RPC

你需要支持 `shardctrler/common.go` 中定义的 RPC:

- `Join`
- `Leave`
- `Move`
- `Query`

`Join` 用来加入新的 replica group。参数是 GID 到 server name 列表的映射。新配置要尽量平均分配 shard，并且为了达到平均分配，移动尽量少的 shard。GID 只要不在当前配置中，就允许复用。

`Leave` 用来移除已有 replica group。被移除 group 的 shard 要重新分配给剩下的 group。同样要求尽量平均，且移动尽量少。

`Move` 指定某个 shard 移动到某个 GID。它主要用于测试。后续的 Join 或 Leave 可能因为重新均衡而覆盖 Move 的效果。

`Query` 查询某个配置编号。如果编号是 `-1`，或者大于当前最大配置编号，就返回最新配置。`Query(-1)` 必须反映它之前已经完成处理的 Join、Leave、Move。

### 初始配置

第一个配置编号是 `0`。它不包含任何 group，所有 shard 都分配给 GID `0`。GID `0` 是无效 GID。

第一次 Join 产生的配置编号是 `1`，之后递增。

通常 shard 数量会明显多于 group 数量，这样负载可以更细粒度地转移。

### Part A 实现提示

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>实现 shardctrler 的 Join/Leave/Move/Query，并用 Raft 容错，通过 shardctrler 测试。</div>

可以从 Lab 4 的 `kvraft` server 精简复制起步。

shardctrler 也要用 Raft 做容错复制。

虽然 shardctrler 的单元测试可能不直接测重复请求过滤，但后面的 shardkv 测试会在不可靠网络下使用它；建议实现重复 client 请求检测。

shard 重新均衡逻辑必须是确定性的。Go 的 map 遍历顺序不是确定的，所以不要让 map 随机遍历顺序影响最终配置。

Go 的 map 是引用类型。基于旧 Config 创建新 Config 时，要新建 map，并逐项复制 key/value。

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> 从 kvraft 精简起步，做重复请求检测，rebalance 要确定性，复制 Config 时深拷贝 map；`go test -race` 可以辅助找并发问题。</div>

### Part A 中 shardkv 的要求

在 `shardkv` 中先实现足够功能，过前两个测试。

第一个测试基本可以从 `kvraft` 迁移而来，因为提供的 client 会根据 controller 的配置把请求发到负责该 key 的 group。

第二个测试要求: 如果请求的 key 所属 shard 不归当前 group 管，server 必须拒绝请求，并返回 `ErrWrongGroup`。

server 需要周期性查询 controller 的最新配置，并在每次 Get/Put/Append 到达时检查该 key 对应的 shard 是否属于本 group。

用 `client.go` 中的 `key2shard()` 把 key 映射到 shard 编号。

shardkv server 不应该自己调用 shard controller 的 `Join()`；测试器会在适当时机调用。

## Part B: Shard Movement <span class="difficulty hard">(hard)</span>

<div style="margin:1em 0; padding:1em; background-color:#990000; color:#fff;"><strong style="display:block; background:#550000; margin:-1em -1em 1em -1em; padding:0.7em 1em;">Important</strong>执行 `git pull` 获取最新 lab 软件。</div>

Part B 的核心是: 当 controller 改变 shard 分配时，在 replica group 之间迁移 shard，同时保持客户端操作线性一致。

每个 shard 只要求在以下条件下继续前进:

- 该 shard 所在 Raft group 的多数 server 存活并能互通。
- 它们能和多数 shardctrler server 通信。

即使某些 group 中少数 server 死掉、暂时不可用或变慢，系统也应该继续服务并按需重新配置。

每个 shardkv server 只属于一个 replica group。一个 replica group 内的 server 集合不会变化。

### client 行为

提供的 client 会把每个 RPC 发给它认为负责该 key 的 group。如果 group 返回自己不负责，client 会查询 shardctrler 最新配置后重试。

你还需要像 kvraft lab 一样修改 `client.go`，支持重复 client RPC 的处理所需的 client id / sequence number 之类机制。

### migration 行为

server 要周期性观察配置变化。一旦发现新配置，就启动 shard migration。

如果某个 group 失去一个 shard，它必须立即停止服务该 shard 上的 key，并开始把这个 shard 的数据迁给新 owner。

如果某个 group 获得一个 shard，它必须等旧 owner 把该 shard 的旧数据传过来后，才能开始服务该 shard。

同一个 replica group 内的所有 server 必须在执行操作序列的同一点进行迁移，这样并发客户端请求会被一致地接受或拒绝。

建议先集中通过 `join then leave` 这个测试，再处理后续更复杂测试。

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>实现配置变更时的 shard migration；同一 group 内所有 server 必须在同一操作序列点迁移。</div>

### Part B 重要提示

<div style="margin:1em 0; border:1px dashed #4682B4; padding:1em; color:#4682B4;"><strong>Note:</strong> server 要大约每 100ms poll shardctrler；迁移 shard 时用 `make_end()` 把 server name 转成 `labrpc.ClientEnd`。</div>

配置变更要一次处理一个，并且按顺序处理。

server 需要周期性 poll shardctrler。测试期望大约每 100ms poll 一次；更频繁通常可以，明显更慢可能出问题。

group 之间迁移 shard 需要互相发 RPC。Config 里有 server name，但发 RPC 需要 `labrpc.ClientEnd`。可以用 `StartServer()` 传进来的 `make_end()` 把 server name 转成 ClientEnd。

如果测试失败，要检查 gob 注册错误。Go 不一定把 gob 错误当成 fatal，但实验里这种错误通常就是致命问题。

client 请求的 at-most-once 语义要跨 shard migration 保持。迁移 shard 数据时，也要迁移足够的去重状态，否则重复请求可能再次生效。

认真想清楚 `ErrWrongGroup` 和 client sequence number 的关系。比如 client 收到 `ErrWrongGroup` 后是否应该改变 sequence number，server 返回 `ErrWrongGroup` 时是否应该更新 client 去重状态。

server 移到新配置后，允许继续在内存里保留已经不归自己服务的 shard 数据。这在真实系统里浪费空间，但能简化基础实现。

RPC 回复中如果包含 server 状态里的 map，应该返回副本，而不是直接把内部 map 放进 reply。RPC 层会读这个 map，而 server 可能同时修改它，容易产生 race。

如果把 map 或 slice 放进 Raft log entry，apply 后保存到 server 状态时也要复制。否则 server 修改它时，Raft 持久化日志可能还在读它，会形成 race。

配置变更期间，两个 group 可能需要互相迁移 shard。如果看到死锁，要检查这种双向迁移场景。

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> 按顺序处理配置；迁移时带上去重状态；ErrWrongGroup 不应错误推进 client 序号；RPC 或 Raft log 里的 map/slice 都要复制，避免 race。</div>

## No-credit Challenges

这部分不计分，但是真实生产系统会需要。

### Challenge 1: 旧 shard 状态回收

<div style="margin:1em 0; border:1px solid #8B4513; padding:1em; color:#8B4513;"><strong style="float:right; background:#8B4513; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">CHALLENGE</strong>每个 replica group 保留旧 shard 的时间应尽可能短；方案必须能处理相关 group crash 后再恢复的情况。</div>

当 group 失去某个 shard 后，理想情况下应该删除这个 shard 的 key/value 数据。否则它会保存自己不再服务的数据，浪费空间。

难点是: 新 owner 可能还没拿到数据。如果旧 owner 过早删除，迁移就断了。你的方案还要能处理旧 owner 全部 crash 后再恢复的情况。

通过目标测试: `TestChallenge1Delete`。

### Challenge 2: 配置变更期间继续服务未受影响 shard

<div style="margin:1em 0; border:1px solid #8B4513; padding:1em; color:#8B4513;"><strong style="float:right; background:#8B4513; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">CHALLENGE</strong>配置变更期间，未受影响 shard 的 client operation 应继续执行。</div>

最简单的做法是在配置变更完成前暂停所有客户端操作。但真实系统不能这样，因为每次扩缩容都会让所有 client 长时间停顿。

优化目标是: 正在迁移的 shard 暂停，但未受影响的 shard 继续服务。

通过目标测试: `TestChallenge2Unaffected`。

更进一步，如果一个 group 在新配置中需要多个 shard，只要某个 shard 的数据已经到达，就应该立刻服务这个 shard，而不是等所有 shard 都迁完。

<div style="margin:1em 0; border:1px solid #8B4513; padding:1em; color:#8B4513;"><strong style="float:right; background:#8B4513; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">CHALLENGE</strong>每个 shard 一旦具备服务条件就立即服务，即使整个配置迁移还没结束。</div>

通过目标测试: `TestChallenge2Partial`。

## 提交前检查

<div style="margin:1em 0; padding:1em; background-color:#990000; color:#fff;"><strong style="display:block; background:#550000; margin:-1em -1em 1em -1em; padding:0.7em 1em;">Important</strong>提交前最后跑完整测试，并确认 Lab 5 sharded server、Lab 5 shard controller 和 Lab 4 kvraft 都使用同一份 Raft。</div>

```sh
go test ./raft
go test ./kvraft
go test ./shardctrler
go test ./shardkv
```

## 实现时的推荐顺序

1. 把 `kvraft` 的基本 Raft 状态机结构迁到 `shardctrler`。
2. 实现 Join/Leave/Move/Query，并确保 rebalance 确定、均衡、移动少。
3. 给 shardctrler 补上 client 去重。
4. 把 `kvraft` 的基本 Get/Put/Append 迁到 `shardkv`。
5. 实现静态配置下的 `ErrWrongGroup` 检查，过 `5A`。
6. 在 shardkv 中用 Raft 日志推进配置变更。
7. 实现 shard RPC 迁移，连同 key/value 数据和 client 去重表一起迁移。
8. 处理 snapshot、restart、不可靠网络和并发配置变更测试。
9. 有余力再做 challenge 的 GC 与部分可用优化。
