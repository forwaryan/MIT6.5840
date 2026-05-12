# Lab3A Raft 选举与心跳

一句话主线：

```text
Follower 超时 -> Candidate 发 RequestVote -> 拿到多数票 -> Leader 周期性发 AppendEntries 心跳
```

Lab3A 先不处理客户端命令，重点是让一组 Raft peer 能在 leader 失联后重新选出一个 leader。上层服务只需要知道：只有 leader 才能接收 `Start(command)`。

## 三种角色

| 角色 | 行为 |
| --- | --- |
| `Follower` | 被动接收 `RequestVote` 和 `AppendEntries`；超时后转为 `Candidate`。 |
| `Candidate` | 增加 term，投票给自己，向其他 peer 拉票。 |
| `Leader` | 周期性发送 `AppendEntries`；没有日志时就是心跳。 |

当前代码还用 `Dead` 表示测试框架 `Kill()` 后的本地状态，方便后台 goroutine 退出。

```mermaid
flowchart LR
    subgraph Cluster["Raft peer 集群"]
        F1["Peer 0<br/>Follower"]
        F2["Peer 1<br/>Follower"]
        C["Peer 2<br/>Candidate"]
        F3["Peer 3<br/>Follower"]
        F4["Peer 4<br/>Follower"]
    end

    C -->|"RequestVote(term N)"| F1
    C -->|"RequestVote(term N)"| F2
    C -->|"投给自己"| C
    C -->|"RequestVote(term N)"| F3
    C -->|"RequestVote(term N)"| F4
    F1 -->|"VoteGranted"| C
    F3 -->|"VoteGranted"| C
    C -->|"多数票后 becomeLeader"| L["Leader"]
```

## 核心状态

| 字段 | 是否持久化 | 作用 |
| --- | --- | --- |
| `currentTerm` | 是 | 当前任期；任期越大，信息越新。 |
| `voteFor` | 是 | 当前 term 投给了谁；`-1` 表示还没投。 |
| `state` | 否 | 当前是 follower、candidate 还是 leader。 |
| `electionStartTime` | 否 | 最近一次收到有效心跳或投票的时间。 |

同一个 term 内，一个 peer 最多投一票。因为多数派之间一定有交集，这条规则保证同一个 term 不会出现两个合法 leader。

## 选举定时器

当前实现的选举超时是随机的：

```text
250ms 到 400ms
每 10ms 检查一次是否超时
```

```mermaid
flowchart TD
    A["runElectionTimer"] --> B{"仍是 Follower/Candidate?"}
    B -->|否| Z["退出旧 timer"]
    B -->|是| C{"term 没变?"}
    C -->|否| Z
    C -->|是| D{"已经超时?"}
    D -->|否| A
    D -->|是| E["startElection"]
```

代码里可能短暂存在多个 timer goroutine，所以它会检查启动 timer 时的 term 是否仍然等于当前 term。旧任期 timer 发现 term 变了就退出，避免旧 goroutine 误触发新选举。

## 发起选举

`startElection()` 做这些事：

```text
currentTerm += 1
state = Candidate
voteFor = me
electionStartTime = now
persist()
receivedVotes = 1
并发向其他 peer 发送 RequestVote
```

`RequestVoteArgs` 中除了 term 和 candidate id，还带上候选者最后一条日志：

| 字段 | 作用 |
| --- | --- |
| `LastLogIndex` | 候选者最后一条日志的 index。 |
| `LastLogTerm` | 候选者最后一条日志的 term。 |

虽然 Lab3A 的重点是选举，当前实现已经包含 Lab3B 所需的“日志新旧”投票规则。

```mermaid
sequenceDiagram
    participant P as Peer 超时
    participant A as Peer A
    participant B as Peer B
    participant C as Peer C

    P->>P: currentTerm++<br/>state=Candidate<br/>voteFor=me
    P->>A: RequestVote(term, lastLog)
    P->>B: RequestVote(term, lastLog)
    P->>C: RequestVote(term, lastLog)
    A-->>P: VoteGranted
    B-->>P: VoteGranted
    P->>P: 多数票成立
    P->>P: becomeLeader()
    P->>A: AppendEntries heartbeat
    P->>B: AppendEntries heartbeat
    P->>C: AppendEntries heartbeat
```

## 投票规则

Follower 收到 `RequestVote` 后：

| 条件 | 行为 |
| --- | --- |
| `args.Term > currentTerm` | 自己落后，转成 follower 并更新 term。 |
| `args.Term < currentTerm` | 候选者落后，拒绝。 |
| 当前 term 已投给别人 | 拒绝。 |
| 候选者日志不够新 | 拒绝。 |
| 其他情况 | 投票，重置选举计时。 |

日志新旧比较：

```text
候选者 LastLogTerm 更大
或 LastLogTerm 相同且 LastLogIndex >= 自己的 lastLogIndex
```

这能防止日志落后的节点成为 leader。

```mermaid
flowchart TD
    A["收到 RequestVote"] --> B{"args.Term < currentTerm?"}
    B -->|是| R["拒绝<br/>返回 currentTerm"]
    B -->|否| C{"args.Term > currentTerm?"}
    C -->|是| D["becomeFollower(args.Term)"]
    C -->|否| E["保持当前 term"]
    D --> F{"本 term 是否已投给别人?"}
    E --> F
    F -->|是| R
    F -->|否| G{"候选者日志足够新?"}
    G -->|否| R
    G -->|是| H["voteFor = CandidateId<br/>重置 electionStartTime<br/>返回 VoteGranted"]
```

## 成为 Leader

Candidate 只在自己仍是 candidate、term 也没变时处理投票回复。拿到多数票后调用 `becomeLeader()`。

成为 leader 后会初始化：

| 字段 | 初始值 | 含义 |
| --- | --- | --- |
| `nextIndex[i]` | `getNextIndex()` | 下次给 follower i 发送的日志 index。 |
| `matchIndex[i]` | `0` | 已知 follower i 复制成功的最大 index。 |

之后 leader 每 100ms 调用一次 `runHeartBeats()`。在没有新日志时，`AppendEntries.Entries` 为空，它就是心跳。

```mermaid
sequenceDiagram
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    loop 每 100ms
        L->>F1: AppendEntries(term, entries=空)
        L->>F2: AppendEntries(term, entries=空)
        F1->>F1: state=Follower<br/>electionStartTime=now
        F2->>F2: state=Follower<br/>electionStartTime=now
        F1-->>L: Success
        F2-->>L: Success
    end
```

## 状态转换

```mermaid
flowchart TD
    F["Follower"] -->|"election timeout"| C["Candidate"]
    C -->|"majority votes"| L["Leader"]
    C -->|"收到更大 term 或有效心跳"| F
    L -->|"收到更大 term"| F
    F -->|"收到有效心跳"| F
```

## 代码入口

| 目标 | 文件/函数 |
| --- | --- |
| 角色和字段 | `src/raft/raft.go`: `Raft`、`RuleState` |
| 选举定时器 | `src/raft/raft.go`: `runElectionTimer`、`ticker` |
| 发起选举 | `src/raft/raft.go`: `startElection` |
| 投票 RPC | `src/raft/raft.go`: `RequestVote`、`sendRequestVote` |
| 状态转换 | `src/raft/raft.go`: `becomeFollower`、`becomeLeader` |
| 心跳 | `src/raft/raft.go`: `heartBeatsTimer`、`runHeartBeats` |
