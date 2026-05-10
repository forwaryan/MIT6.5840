# 6.5840 Lab 1: MapReduce 中文阅读导览

<style>
body { max-width: 45em; color: black; background-color: white; font-family: sans-serif; }
pre { overflow-x: auto; margin: 1em; border: 1px dashed #839496; padding: 1em; font-size: 100%; color: #839496; background: #002b36; }
code, tt { font-family: monospace; border-radius: 3px; font-size: 110%; color: #657b83; background-color: #fdf6e3; padding: 0 0.2em; word-wrap: break-word; }
.difficulty.easy { color: #00cc00; }
.difficulty.moderate { color: #0066ff; }
.difficulty.hard { color: #ff3300; }
</style>

来源页面: http://nil.csail.mit.edu/6.5840/2024/labs/lab-mr.html

说明: 这是对页面要求的中文导读和要点整理，不是逐句完整译文。

## 总体目标

这个实验要求你实现一个分布式 MapReduce 系统。系统包含两个角色:

- coordinator: 负责任务分配、任务状态跟踪、失败 worker 的处理。
- worker: 向 coordinator 请求任务，执行 Map 或 Reduce，读写中间文件和最终输出。

实验中的所有进程都运行在同一台机器上，但设计上模拟多 worker 并行执行。worker 和 coordinator 之间通过 RPC 通信。

## 起步代码

课程提供了一个顺序版 MapReduce:

- `src/main/mrsequential.go`

它会在单个进程里按顺序执行所有 Map 和 Reduce。

课程还提供了示例应用:

- `mrapps/wc.go`: word count。
- `mrapps/indexer.go`: 文本索引。

你可以从 `mrsequential.go` 借用读文件、排序、输出格式等代码。

## 你需要改哪些文件

主要实现放在:

- `mr/coordinator.go`
- `mr/worker.go`
- `mr/rpc.go`

不要修改:

- `main/mrcoordinator.go`
- `main/mrworker.go`

可以为了临时测试改别的文件，但最终代码必须能和课程提供的原始文件配合。

## 你的任务与系统行为 <span class="difficulty moderate">(moderate/hard)</span>

整个 MapReduce job 的执行大致是:

1. coordinator 启动，输入文件列表作为参数。
2. 每个输入文件对应一个 Map task。
3. worker 反复向 coordinator 请求任务。
4. worker 执行 Map task 时读取输入文件，调用应用的 `Map` 函数。
5. Map 输出的中间 key/value 要按 reduce 分区写入中间文件。
6. 所有 Map task 完成后，Reduce task 才能开始。
7. worker 执行 Reduce task 时读取所有相关中间文件，调用应用的 `Reduce` 函数。
8. 每个 Reduce task 写出一个 `mr-out-X`。
9. 所有任务完成后 coordinator 的 `Done()` 返回 true，worker 也应该退出。

## 任务失败处理

coordinator 要能处理 worker 失败。

如果一个 worker 拿到任务后在合理时间内没有完成，coordinator 应该把同一个任务重新分配给其他 worker。本实验建议超时时间使用 10 秒。

coordinator 不可能可靠地区分 worker 是崩溃、卡住，还是只是很慢；实验要求采用超时重试策略。

## 输出和文件命名规则

Map 阶段要把中间 key 分到 `nReduce` 个 bucket。`nReduce` 是 `MakeCoordinator()` 收到的 reduce task 数量。

每个 Map task 应该为每个 Reduce task 创建一个中间文件。推荐命名:

- `mr-X-Y`

其中 `X` 是 Map task 编号，`Y` 是 Reduce task 编号。

第 `X` 个 Reduce task 的最终输出文件必须叫:

- `mr-out-X`

`mr-out-X` 中每一行对应一次 Reduce 输出，格式要接近:

```go
"%v %v"
```

key 和 value 用空格分隔。格式偏差太大测试会失败。

## 测试目标

测试脚本是:

```sh
cd ~/6.5840/src/main
bash test-mr.sh
```

最终应该通过这些测试:

- word count 输出正确。
- indexer 输出正确。
- Map task 能并行执行。
- Reduce task 能并行执行。
- job 数量合理。
- coordinator 能正确提前/最终退出。
- worker crash 后任务能恢复。

另有 `test-mr-many.sh` 可以重复运行 `test-mr.sh`，用来发现低概率并发 bug。

## 重要提示

应用的 Map 和 Reduce 函数以 Go plugin 形式运行，文件名通常是 `.so`。如果改了 `mr/` 里的代码，通常需要重新 build plugin。

Map task 可以用 `ihash(key)` 选择 key 属于哪个 Reduce task。

中间文件可以用 Go 的 `encoding/json` 编码和解码 key/value。

coordinator 是 RPC server，会并发处理请求。共享状态要加锁。

worker 有时需要等待，例如 Reduce 不能在所有 Map 完成前开始。可以让 worker 定期请求任务并 sleep，也可以让 coordinator 的 RPC handler 等待条件变化。

测试 crash 恢复可以使用 `mrapps/crash.go`，它会在 Map/Reduce 中随机退出。

为了避免别人看到部分写入的文件，可以先写临时文件，再用 `os.Rename()` 原子替换成目标文件。

`test-mr.sh` 会在 `mr-tmp` 子目录里运行。如果调试输出或中间文件，去那里看。

Go RPC 只会传输首字母大写的结构体字段。嵌套结构体字段也要大写。

RPC reply 参数最好使用全零值初始化，不要在调用前预先填字段。

建议用 race detector 检查:

```sh
go run -race ...
```

## 可选挑战

<div style="margin:1em 0; border:1px solid #8B4513; padding:1em; color:#8B4513;"><strong style="float:right; background:#8B4513; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">CHALLENGE</strong>实现你自己的 MapReduce 应用，例如 Distributed Grep。</div>

<div style="margin:1em 0; border:1px solid #8B4513; padding:1em; color:#8B4513;"><strong style="float:right; background:#8B4513; color:#fff; padding:0.25em 0.65em; margin:-1em -1em 0.5em 1em;">CHALLENGE</strong>让 MapReduce coordinator 和 worker 像真实环境一样运行在不同机器上。</div>

## 推荐实现顺序

1. 定义 RPC 参数和回复，包括任务类型、任务编号、文件名、`nReduce`、`nMap` 等。
2. coordinator 初始化所有 Map task 状态。
3. worker 能请求并执行单个 Map task。
4. Map 输出按 `ihash(key) % nReduce` 写入中间文件。
5. coordinator 追踪 Map 完成后再发 Reduce task。
6. worker 执行 Reduce task，读取所有 `mr-X-Y` 中属于自己的文件。
7. 写出 `mr-out-X`。
8. 增加任务超时和重发。
9. 处理 job 结束和 worker 退出。
10. 用 `test-mr-many.sh` 多跑几轮找并发问题。
