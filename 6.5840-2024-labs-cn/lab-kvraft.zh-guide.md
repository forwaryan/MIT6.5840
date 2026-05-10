# 6.5840 Lab 4: Fault-tolerant Key/Value Service 中文阅读导览

<style>
body { max-width: 45em; color: black; background-color: white; font-family: sans-serif; }
pre { overflow-x: auto; margin: 1em; border: 1px dashed #839496; padding: 1em; font-size: 100%; color: #839496; background: #002b36; }
code, tt { font-family: monospace; border-radius: 3px; font-size: 110%; color: #657b83; background-color: #fdf6e3; padding: 0 0.2em; word-wrap: break-word; }
.difficulty.easy { color: #00cc00; }
.difficulty.moderate { color: #0066ff; }
.difficulty.hard { color: #ff3300; }
</style>

来源页面: http://nil.csail.mit.edu/6.5840/2024/labs/lab-kvraft.html

说明: 这是对页面要求的中文导读和要点整理，不是逐句完整译文。

## 总体目标

这个实验要求你用 Lab 3 实现的 Raft 构建一个容错 key/value 服务。

系统由多个 kvserver 组成，每个 kvserver 都有一个对应的 Raft peer。所有 Put/Append/Get 操作都进入 Raft log，所有 server 按相同顺序 apply log，从而维护一致的 key/value 数据库。

只要多数 server 存活并能通信，即使其他 server 崩溃、网络分区或消息丢失，服务也应该继续处理 client 请求。

## API 语义

和 Lab 2 类似，client 通过 `Clerk` 调用:

- `Put(key, value)`: 替换 key 的值。
- `Append(key, arg)`: 追加到 key 的现有值后面；key 不存在时视为空字符串。
- `Get(key)`: 获取当前值；key 不存在时返回空字符串。

Lab 4 中 `Put` 和 `Append` 不向 client 返回 value。

服务必须保持线性一致。并发请求的结果必须等价于某个串行顺序；一个请求必须看见在它开始前已经完成的请求效果。

复制场景下更难，因为所有 server 必须选出同一执行顺序，不能用过期状态回复 client，并且 crash/restart 后必须保留已经确认给 client 的更新。

## 起步文件

骨架代码在:

- `src/kvraft`

主要修改:

- `kvraft/client.go`
- `kvraft/server.go`
- 可能还有 `kvraft/common.go`

Lab 4 分为两部分:

- Part A: 不使用 snapshot 的复制 key/value 服务。
- Part B: 使用 Lab 3D 的 snapshot，避免 Raft log 无限增长。

## Part A: Without Snapshots <span class="difficulty moderate">(moderate/hard)</span>

每个 client RPC 应该发给当前 Raft leader 对应的 kvserver。如果 Clerk 不知道谁是 leader，或者发错 server，或者 RPC 失败，就换 server 重试。

kvserver 收到 Put/Append/Get 后:

1. 构造一个 `Op`。
2. 调用 Raft `Start()` 把 Op 放入 log。
3. 等待这个 Op 被 commit 并从 `applyCh` 出现。
4. apply 到本地 key/value 状态机。
5. 对发起该 Op 的 RPC 返回结果。

kvserver 之间不应该直接通信，只能通过 Raft 产生一致顺序。

### 第一阶段目标

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>先实现无丢包、无 server failure 的复制 key/value 服务，并稳定通过 One client。</div>

可以从 Lab 2 的 client 代码迁移起步，但需要加入寻找 leader 的逻辑。

需要定义 `Op`，使它能描述 Put/Append/Get。

### applyCh 和等待机制

RPC handler 调用 `Start()` 后，要等待对应 log entry commit。

常见做法是:

- 用一个后台 apply goroutine 持续读取 `applyCh`。
- 用 map 记录某个 log index 对应的等待 channel。
- apply 到某 index 时通知等待这个 index 的 handler。
- handler 被唤醒后确认 apply 的 Op 确实是自己提交的 Op。

如果 index 上出现了别的 Op，说明自己可能失去 leader 或发生 term 变化，应返回错误让 Clerk 重试。

注意避免 kvserver 和 Raft 之间死锁。不要在持有 kvserver 锁时调用可能阻塞很久的逻辑。

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> Start 后等待 applyCh，Get 也进入 Raft log，尽早设计好锁和等待机制。</div>

### Get 也进 Raft

kvserver 如果不在多数派中，不能直接服务 Get，否则可能读到旧数据。

简单正确的做法是: Get 和 Put/Append 一样进入 Raft log。这样 Get 也在一致顺序中执行，不需要实现论文 Section 8 的只读优化。

## 失败和重复请求

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>处理 leader 变更、重试和重复 Clerk 请求，保证同一 Put/Append 只执行一次。</div>

Part A 后续要处理网络和 server failure。

典型问题:

- leader 把 client 的 Op commit 了，但回复 client 前崩溃。
- client 超时后把同一个请求发给新 leader。
- 如果没有去重，Put/Append 可能执行两次。

你需要像 Lab 2 一样实现 duplicate detection，但现在去重状态也要在所有 server 上一致。因此去重信息必须作为状态机状态的一部分，在 apply log 时更新。

每个 Clerk 请求应有唯一 client id 和 request id。server 记录每个 client 已处理的最新 request，以及必要的返回信息。

如果 leader 在调用 `Start()` 后失去 leadership，server 应让 client 重试。判断方式可以包括:

- Raft term 变化。
- 该 log index 最终 commit 的不是自己提交的 Op。
- 等待超时后发现自己不再是 leader。

Clerk 应记住上次成功的 leader，下次先发给它，减少寻找 leader 的开销。

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> server 要识别 Start 后失去 leadership 的情况，Clerk 要缓存上次 leader，去重表要能快速释放旧状态。</div>

完成 Part A 的目标:

```sh
go test -run 4A
```

## Part B: With Snapshots <span class="difficulty hard">(hard)</span>

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>当 Raft 持久化状态过大时生成 snapshot；重启时从 snapshot 恢复 kvserver 状态。</div>

如果不做 snapshot，重启 server 时必须回放完整 Raft log，log 也会无限增长。Part B 要让 kvserver 与 Raft 的 `Snapshot()` 配合，控制持久化 Raft 状态大小。

tester 会向 `StartKVServer()` 传入 `maxraftstate`。

- `maxraftstate` 表示允许的 Raft 持久化状态最大字节数，不含 snapshot。
- kvserver 应比较它和 `persister.RaftStateSize()`。
- 当 Raft 状态接近阈值时，kvserver 调用 Raft `Snapshot()`。
- 如果 `maxraftstate == -1`，则不需要 snapshot。

snapshot 内容应该足以恢复 kvserver 状态，至少包括:

- key/value 数据库。
- duplicate detection 表。
- 可能还包括和状态机执行进度相关的信息。

server 重启时要从 `persister.ReadSnapshot()` 读出 snapshot，并恢复状态。

如果 snapshot 中存结构体，字段名要大写，确保 labgob 能编码。

如果 Part B 暴露 Raft bug，修改 Raft 后要重新确认 Lab 3 测试全部通过。

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> snapshot 里必须包含 key/value 数据和去重状态，结构体字段要大写，Raft 也要继续通过 Lab 3。</div>

完成 Part B 的目标:

```sh
go test -run 4B
```

同时 Lab 4A 和 Lab 3 也应继续通过。

## 推荐实现顺序

1. 定义 Op，包括操作类型、key、value、client id、request id。
2. Clerk 支持 leader 记忆和重试。
3. server RPC handler 调用 Raft `Start()`。
4. 实现 apply goroutine，按 commit 顺序更新 key/value map。
5. 给 log index 建等待 channel，让 handler 等待自己的 Op commit。
6. Get 也进入 Raft log，避免 stale read。
7. 实现 duplicate detection，并确保重复 Put/Append 不再执行。
8. 让 handler 能识别 leadership 丢失或 index 被别的 Op 占用。
9. 通过 4A。
10. 实现 snapshot 编码/解码，包含数据库和去重状态。
11. 在 Raft 状态接近 `maxraftstate` 时调用 `Snapshot()`。
12. 通过 4B，并回归 Lab 3。
