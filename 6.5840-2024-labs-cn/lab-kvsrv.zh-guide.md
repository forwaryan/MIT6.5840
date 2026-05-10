# 6.5840 Lab 2: Key/Value Server 中文阅读导览

<style>
body { max-width: 45em; color: black; background-color: white; font-family: sans-serif; }
pre { overflow-x: auto; margin: 1em; border: 1px dashed #839496; padding: 1em; font-size: 100%; color: #839496; background: #002b36; }
code, tt { font-family: monospace; border-radius: 3px; font-size: 110%; color: #657b83; background-color: #fdf6e3; padding: 0 0.2em; word-wrap: break-word; }
.difficulty.easy { color: #00cc00; }
.difficulty.moderate { color: #0066ff; }
.difficulty.hard { color: #ff3300; }
</style>

来源页面: http://nil.csail.mit.edu/6.5840/2024/labs/lab-kvsrv.html

说明: 这是对页面要求的中文导读和要点整理，不是逐句完整译文。

## 总体目标

这个实验要你实现一个单机 key/value server。它不处理 server crash，但要处理网络失败，例如 RPC 请求或回复丢失。

核心要求有两个:

- 每个客户端操作最多执行一次，不能因为重试被执行多次。
- Get/Put/Append 对外表现为线性一致。

后续 Lab 4 会把类似的 key/value server 放到 Raft 上复制，以处理 server crash。

## API 语义

client 通过 `Clerk` 调用三个方法:

- `Put(key, value)`: 设置或替换 key 的值。
- `Append(key, arg)`: 把 `arg` 追加到 key 的现有值后面，并返回旧值。
- `Get(key)`: 返回 key 当前值。

key 和 value 都是 string。

不存在的 key:

- `Get` 返回空字符串。
- `Append` 视为原值是空字符串。

server 内部维护一个内存 map。

## 线性一致性

如果请求不并发，后面的操作必须看见前面操作造成的状态变化。

如果请求并发，返回值和最终状态必须等价于这些请求按某个顺序一个个执行。

一个调用必须看见在它开始之前已经完成的所有调用的效果。

单机 server 中实现线性一致相对简单: 用锁保护 map，并在 RPC handler 中按互斥方式更新或读取状态即可。

## 起步文件

骨架代码在:

- `src/kvsrv`

你主要需要修改:

- `kvsrv/client.go`
- `kvsrv/server.go`
- `kvsrv/common.go`

## 第一阶段: 无网络失败 <span class="difficulty easy">(easy)</span>

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>先实现没有丢包时可工作的 key/value server。Clerk 的 Put/Append/Get 要发送 RPC，server 要实现 Put/Append/Get handler，并通过 one client 和 many clients。</div>

先实现没有丢包时可用的版本。

需要做:

- 在 `Clerk` 的 Put/Append/Get 中加入发 RPC 的代码。
- 在 `server.go` 中实现 Put、Append、Get RPC handler。

通过目标:

- `one client`
- `many clients`

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> 用 race detector 检查数据竞争。</div>

```sh
go test -race
```

## 第二阶段: RPC 丢失 <span class="difficulty easy">(easy)</span>

<div style="margin:1em 0; border:1px solid #B22222; padding:1em; color:#B22222;"><strong style="float:right; background:#B22222; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">TASK</strong>Clerk 要在收不到回复时重试；server 要过滤需要去重的重复操作。</div>

接着处理请求或回复丢失的情况。

`ck.server.Call()` 可能返回 `false`，表示在超时时间内没有收到回复。client 要重试，直到成功。

难点是: Put/Append 可能已经在 server 执行了，但回复丢了。client 重试时，server 不能再次执行同一个操作。

你需要:

- 给每个 client 操作一个唯一标识。
- 在 Clerk 收不到回复时重试。
- 在 server 中过滤需要去重的重复操作。

课程允许假设同一个 Clerk 一次只会发起一个调用。因此可以用 client id + 单调递增 request id 来标识操作。

## 去重状态怎么想

Put/Append 会改变状态，必须防重复执行。

Get 不改变 key/value map，但它也有线性一致性语义。通常可以直接执行 Get；如果你的设计让 Get 也带序列号并参与去重，也要确保返回值正确。

Append 在 Lab 2 中会返回旧值。因此如果重复 Append 到达，server 不仅要避免再次 append，还要能返回第一次执行时的旧值。

为了避免 server 内存无限增长，每个 RPC 可以隐含表示 client 已经收到它上一个 RPC 的回复。这样 server 只需保存每个 client 最近一次操作的信息。

<div style="margin:1em 0; border:1px dashed #50A02D; padding:1em; color:#50A02D;"><strong>Hint:</strong> 关键是唯一标识每个 client operation，并快速释放不再需要的去重状态。</div>

## 测试目标

最终 `go test` 应该通过:

- one client
- many clients
- unreliable net, many clients
- concurrent append to same key, unreliable
- memory use get
- memory use put
- memory use append
- memory use many puts
- memory use many gets

这些 memory 测试提醒你: 去重表不能无限保存所有历史 request。

## 推荐实现顺序

1. 定义 RPC args/reply，包含 key、value/arg、client id、request id。
2. Clerk 初始化唯一 client id 和递增 request id。
3. 实现无失败网络下的 Put/Append/Get。
4. 给 server map 和去重表加锁。
5. Clerk 在 Call 返回 false 时循环重试。
6. server 对 Put/Append 做重复检测，重复时返回缓存结果或确认。
7. 确认去重表只保留每个 client 必要的最近状态。
8. 跑 `go test` 和 `go test -race`。
