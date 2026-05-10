# 6.5840 Lab 3: Raft 中文阅读导览

<style>
body { max-width: 45em; color: black; background-color: white; font-family: sans-serif; }
pre { overflow-x: auto; margin: 1em; border: 1px dashed #839496; padding: 1em; font-size: 100%; color: #839496; background: #002b36; }
code, tt { font-family: monospace; border-radius: 3px; font-size: 110%; color: #657b83; background-color: #fdf6e3; padding: 0 0.2em; word-wrap: break-word; }
.difficulty.easy { color: #00cc00; }
.difficulty.moderate { color: #0066ff; }
.difficulty.hard { color: #ff3300; }
</style>

来源页面: http://nil.csail.mit.edu/6.5840/2024/labs/lab-raft.html

说明: 这是对页面要求的中文导读和要点整理，不是逐句完整译文。

## 总体目标

这个实验要求你实现 Raft，一个 replicated state machine 协议。后面的 Lab 4 会在你的 Raft 上构建容错 key/value 服务，Lab 5 会进一步做分片。

复制服务通过在多个 server 上保存完整状态来容错。问题是网络分区、crash、消息丢失可能让副本状态不一致。Raft 通过复制一条统一的 log 来解决这个问题。

所有副本按相同顺序执行 log 中的 command，因此状态保持一致。只要多数 server 存活且能互相通信，Raft 就能继续前进；没有多数派时不会前进，但多数派恢复后能继续。

本实验实现的是 Go 中的 Raft 对象，供上层 service 调用。Raft peer 之间只能通过 RPC 交互，不能用共享变量或文件通信。

需要重点遵循 extended Raft paper，尤其 Figure 2。你不需要实现集群成员变更。

## 代码和接口

主要实现文件:

- `src/raft/raft.go`

测试文件:

- `src/raft/test_test.go`

上层服务和 tester 使用的接口包括:

```go
rf := Make(peers, me, persister, applyCh)
rf.Start(command interface{}) (index, term, isleader)
rf.GetState() (term, isLeader)
type ApplyMsg
```

`Make()` 创建 Raft peer。`peers` 是所有 peer 的 RPC 端点，`me` 是当前 peer 在数组中的下标。

`Start(command)` 请求 Raft 开始把 command 追加到复制日志中。它应该立即返回，不等待复制完成。

当某个 log entry commit 后，Raft 要通过 `applyCh` 给上层发送 `ApplyMsg`。

测试器会让 `labrpc` 延迟、重排、丢弃 RPC，以模拟网络问题。你可以临时改 `labrpc` 调试，但最终必须兼容原始 `labrpc`。

## Part 3A: Leader Election <span class="difficulty moderate">(moderate)</span>

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>实现 leader election 和空 AppendEntries heartbeat，通过 3A 测试。</div>

目标是实现:

- leader election。
- heartbeat，也就是没有 log entry 的 `AppendEntries`。
- 没有故障时保持一个 leader。
- leader 故障或网络不可达时选出新 leader。

测试命令:

```sh
go test -run 3A
```

### 3A 实现提示

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> 重点按 Figure 2 实现 RequestVote、heartbeat、选举超时、GetState 和 killed 检查。</div>

先按 Figure 2 补齐 leader election 相关状态，例如 current term、votedFor、角色、选举超时等。

实现 `RequestVoteArgs` 和 `RequestVoteReply`。

在 `Make()` 中启动后台 goroutine。它在一段时间没收到 leader 消息后发起选举。

实现 `RequestVote()` handler，让 server 能给候选人投票。

定义并实现 `AppendEntries` RPC，用于 leader 定期发送 heartbeat。

tester 要求 heartbeat 不能超过每秒 10 次。

tester 要求旧 leader 失败后，如果多数派仍能通信，要在 5 秒内选出新 leader。

论文中的 150-300ms election timeout 假设 heartbeat 很频繁。由于测试限制 heartbeat 频率，你需要把 election timeout 设得比论文更大，但不能大到超过 5 秒选主要求。

不要用 `time.Timer` 或 `time.Ticker`，页面建议用 goroutine 循环配合 `time.Sleep()`。

实现 `GetState()`。循环里检查 `rf.killed()`，避免已关闭实例继续打印日志或跑后台逻辑。

Go RPC 和 labgob 需要结构体字段首字母大写。

## Part 3B: Log Replication <span class="difficulty hard">(hard)</span>

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>实现 leader/follower 追加日志、复制日志、commit 和 apply，通过 3B 测试。</div>

测试命令:

```sh
go test -run 3B
```

第一目标是通过 `TestBasicAgree3B()`。

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> 先过 basic agreement，再处理 election restriction、等待循环性能和测试源码定位。</div>

建议顺序:

1. 实现 `Start()`，leader 把 command 加到本地 log。
2. leader 通过 `AppendEntries` 把 log entry 发给 follower。
3. follower 按 Figure 2 检查 prevLogIndex / prevLogTerm，接受或拒绝。
4. leader 维护每个 follower 的 `nextIndex` 和 `matchIndex`。
5. 当某 entry 被多数派复制后，leader 推进 `commitIndex`。
6. 每个 peer 按 commit 顺序通过 `applyCh` 发送 `ApplyMsg`。

必须实现论文 5.4.1 的 election restriction。也就是投票时要比较候选人的 log 是否至少和自己一样新，避免旧日志 candidate 当选。

如果代码里有循环等待事件，不要空转。用条件变量、channel，或者每轮 sleep 一小段时间。空转会让测试变慢甚至失败。

如果测试失败，读 `test_test.go` 和 `config.go`。`config.go` 也展示了 tester 如何使用你的 Raft API。

3B 对性能比较敏感。页面提醒: 如果 3B 测试用时明显超过一分钟，或 CPU 时间超过 5 秒，后面可能会出问题。

## Part 3C: Persistence <span class="difficulty hard">(hard)</span>

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>完成 persist/readPersist，保存并恢复 Raft 持久化状态，通过 3C 测试。</div>

目标是让 Raft server crash/restart 后能从持久化状态继续。

实验不写真实磁盘，而是通过 `Persister` 保存和恢复。

需要完成:

- `persist()`
- `readPersist()`

使用 `labgob` 把持久化状态编码成 bytes。每当持久化状态变化，就调用 `persist()`。

Figure 2 中标注为 persistent 的状态必须保存，典型包括:

- `currentTerm`
- `votedFor`
- log

在 3C 阶段，调用 `persister.Save()` 时 snapshot 参数先传 `nil`。

### 快速回退优化

3C 很可能需要实现 nextIndex 快速回退，而不是一次只退一个 log entry。

页面给出的思路是 follower 拒绝 AppendEntries 时返回:

- `XTerm`: 冲突 entry 的 term。
- `XIndex`: 该 term 第一个 entry 的 index。
- `XLen`: follower log 长度。

leader 据此更新 `nextIndex`:

- leader 没有 `XTerm`: `nextIndex = XIndex`。
- leader 有 `XTerm`: 回退到 leader 中该 term 的最后一个 entry。
- follower log 太短: `nextIndex = XLen`。

3C 测试比 3A/3B 更严格，失败可能来自早先部分的 bug。

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> 3C 常需要实现 nextIndex 快速回退，并回头修正 3A/3B 遗留问题。</div>

提交前建议多跑几轮:

```sh
for i in {0..10}; do go test; done
```

## Part 3D: Log Compaction <span class="difficulty hard">(hard)</span>

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>实现 Snapshot、InstallSnapshot，以及截断 log 后的 Raft 运行逻辑，通过 3D 和此前测试。</div>

目标是支持 snapshot 和日志压缩。长期运行时不能无限保存完整 Raft log；上层 service 会周期性保存自己的状态快照，Raft 可以丢弃快照之前的 log entry。

Raft 要提供:

```go
Snapshot(index int, snapshot []byte)
```

`index` 表示 snapshot 已经包含到哪个最高 log entry。Raft 应丢弃该点之前的 log。

3D 的 tester 会定期调用 `Snapshot()`；Lab 4 中则由你的 key/value server 自己调用。

你需要让 Raft 在只保存 log 后缀的情况下仍能运行。通常要维护一个逻辑起点，例如 snapshot 覆盖的最后 index 和 term。

### InstallSnapshot

如果 follower 落后太多，leader 已经丢弃了它需要的 log entry，那么 leader 必须发送 snapshot。你需要实现论文中的 `InstallSnapshot` RPC。

本实验可以一次性发送完整 snapshot，不需要实现 Figure 13 中按 offset 分块传输的机制。

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> 先让代码能只保存 log 后缀，再做 Snapshot 截断，最后让 leader 对过度落后的 follower 发送 InstallSnapshot。</div>

follower 收到 InstallSnapshot 后，可以通过 `applyCh` 发送带 snapshot 的 `ApplyMsg` 给上层。注意 snapshot 只能推进上层状态，不能让它倒退。

Raft crash 后必须同时恢复 Raft 持久化状态和对应 snapshot。`persister.Save()` 的第二个参数用于保存 snapshot。

## 推荐实现顺序

1. 先搭好状态结构，明确持久状态、易失状态、leader 专属状态。
2. 完成 3A 选举和 heartbeat，反复跑 `go test -run 3A`。
3. 实现 basic log replication 和 apply loop，先过 `TestBasicAgree3B`。
4. 加上 `nextIndex` / `matchIndex` / commit 推进。
5. 实现投票时的 log up-to-date 限制。
6. 清理锁粒度，避免 RPC 时持锁太久。
7. 实现 persist/readPersist，并在所有持久状态变化点调用。
8. 加快速回退优化。
9. 把 log index 抽象清楚，为 3D 的截断 log 做准备。
10. 实现 Snapshot、InstallSnapshot 和 snapshot 持久化。
11. 多次跑全量测试和 race 测试。
