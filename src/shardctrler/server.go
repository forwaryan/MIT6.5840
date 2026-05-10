package shardctrler

import (
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

var debugMu sync.Mutex
var debugOnce sync.Once

const debugLogPath = "./tmp/shardctrler-debug.log"

func initDebugLog() {
	os.MkdirAll("./tmp", 0755)

	f, err := os.OpenFile(debugLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err == nil {
		f.Close()
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		debugOnce.Do(initDebugLog)

		debugMu.Lock()
		defer debugMu.Unlock()

		f, err := os.OpenFile(debugLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return
		}
		defer f.Close()

		fmt.Fprintf(f, format+"\n", a...)
	}
}

const ExecuteTimeout = 500 * time.Millisecond

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead           int32
	configs        []Config // indexed by config num
	LastRequestMap map[int64]int64
	waitChMap      map[int]chan *Op
}

type Op struct {
	// Your data here.
	ClientId  int64
	RequestId int64
	OpType    string
	Servers   map[int][]string //Join, gid - servers mappings
	GIDs      []int            //Leave
	Shard     int              //Move
	GID       int              //Move
	Num       int              //Query
	Configure Config
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	id := sc.me
	if sc.isInvalidRequest(args.ClientId, args.RequestId) {
		DPrintf("[%d] receive out of data request", id)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    Join,
		Servers:   args.Servers,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	DPrintf("[%d] send Join request, log index: [%d], log term: [%d], args[%v]", id, index, term, args)
	waitChan, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan *Op, 1)
		waitChan = sc.waitChMap[index]
	}
	sc.mu.Unlock()

	select {
	case res := <-waitChan:
		DPrintf("[%d] receive res from waitChan [%v]", id, res)
		reply.Err = OK
		currentTerm, stillLeader := sc.rf.GetState()
		if !stillLeader || currentTerm != term {
			DPrintf("[%d] hash accident, stillLeader [%t], term [%d], currentTerm [%d]", id, stillLeader, term, currentTerm)
			reply.WrongLeader = true
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("[%d] timeout!", id)
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.waitChMap, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	id := sc.me
	if sc.isInvalidRequest(args.ClientId, args.RequestId) {
		DPrintf("[%d] receive out of data request", id)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    Leave,
		GIDs:      args.GIDs,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	DPrintf("[%d] send Leave request, log index: [%d], log term: [%d], args[%v]", id, index, term, args)
	waitChan, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan *Op, 1)
		waitChan = sc.waitChMap[index]
	}
	sc.mu.Unlock()

	select {
	case res := <-waitChan:
		DPrintf("[%d] receive res from waitChan [%v]", id, res)
		reply.Err = OK
		currentTerm, stillLeader := sc.rf.GetState()
		if !stillLeader || currentTerm != term {
			DPrintf("[%d] hash accident, stillLeader [%t], term [%d], currentTerm [%d]", id, stillLeader, term, currentTerm)
			reply.WrongLeader = true
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("[%d] timeout!", id)
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.waitChMap, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	id := sc.me
	if sc.isInvalidRequest(args.ClientId, args.RequestId) {
		DPrintf("[%d] receive out of data request", id)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    Move,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	DPrintf("[%d] send Move request, log index: [%d], log term: [%d], args[%v]", id, index, term, args)
	waitChan, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan *Op, 1)
		waitChan = sc.waitChMap[index]
	}
	sc.mu.Unlock()

	select {
	case res := <-waitChan:
		DPrintf("[%d] receive res from waitChan [%v]", id, res)
		reply.Err = OK
		currentTerm, stillLeader := sc.rf.GetState()
		if !stillLeader || currentTerm != term {
			DPrintf("[%d] hash accident, stillLeader [%t], term [%d], currentTerm [%d]", id, stillLeader, term, currentTerm)
			reply.WrongLeader = true
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("[%d] timeout!", id)
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.waitChMap, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	id := sc.me
	if sc.isInvalidRequest(args.ClientId, args.RequestId) {
		DPrintf("[%d] receive out of data request", id)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    Query,
		Num:       args.Num,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	DPrintf("[%d] send Query request, log index: [%d], log term: [%d], args[%v]", id, index, term, args)
	waitChan, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan *Op, 1)
		waitChan = sc.waitChMap[index]
	}
	sc.mu.Unlock()

	select {
	case res := <-waitChan:
		DPrintf("[%d] receive res from waitChan [%v]", id, res)
		reply.Err = OK
		reply.Config = res.Configure
		currentTerm, stillLeader := sc.rf.GetState()
		if !stillLeader || currentTerm != term {
			DPrintf("[%d] hash accident, stillLeader [%t], term [%d], currentTerm [%d]", id, stillLeader, term, currentTerm)
			reply.WrongLeader = true
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("[%d] timeout!", id)
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.waitChMap, index)
	sc.mu.Unlock()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh
		sc.mu.Lock()
		DPrintf("[%d] receives applyMsg [%v]", sc.me, applyMsg)
		op := applyMsg.Command.(Op)
		sc.execute(&op)
		currentTerm, isLeader := sc.rf.GetState()
		if isLeader && applyMsg.CommandTerm == currentTerm {
			sc.notifyWaitCh(applyMsg.CommandIndex, &op)
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) execute(op *Op) {
	DPrintf("[%d] apply command [%+v] success", sc.me, op)
	if sc.isInvalidRequest(op.ClientId, op.RequestId) {
		return
	} else {
		switch op.OpType {
		case Join:
			sc.processJoin(op)
		case Leave:
			sc.processLeave(op)
		case Move:
			sc.processMove(op)
		case Query:
			sc.processQuery(op)
		}
		sc.UpdateLastRequest(op)
	}
}

func (sc *ShardCtrler) processJoin(op *Op) {
	n := len(sc.configs)
	newGroups, newShards := sc.copyGroups(), sc.configs[n-1].Shards
	for gid, servers := range op.Servers {
		newGroups[gid] = servers
	}
	sc.adjustShards(newGroups, &newShards)
	newConfig := Config{
		Num:    sc.configs[n-1].Num + 1,
		Groups: newGroups,
		Shards: newShards,
	}
	DPrintf("[%d] processJoin [%v] success", sc.me, newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) processLeave(op *Op) {
	n := len(sc.configs)
	newGroups, newShards := sc.copyGroups(), sc.configs[n-1].Shards
	for _, gid := range op.GIDs {
		for shardId, belongGid := range newShards {
			if gid == belongGid {
				newShards[shardId] = 0
			}
		}
		delete(newGroups, gid)
	}
	sc.adjustShards(newGroups, &newShards)
	newConfig := Config{
		Num:    sc.configs[n-1].Num + 1,
		Groups: newGroups,
		Shards: newShards,
	}
	DPrintf("[%d] processLeave [%v] success", sc.me, newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) processMove(op *Op) {
	n := len(sc.configs)
	Shard, GID := op.Shard, op.GID
	newGroups, newShards := sc.copyGroups(), sc.configs[n-1].Shards
	newShards[Shard] = GID
	newConfig := Config{
		Num:    sc.configs[n-1].Num + 1,
		Groups: newGroups,
		Shards: newShards,
	}
	DPrintf("[%d] processMove [%v] success", sc.me, newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) processQuery(op *Op) {
	num, n := op.Num, len(sc.configs)
	if num == -1 || num >= n {
		op.Configure = sc.configs[n-1]
		return
	}
	op.Configure = sc.configs[num]
}

func (sc *ShardCtrler) copyGroups() map[int][]string {
	n := len(sc.configs)
	newGroups := make(map[int][]string, len(sc.configs[n-1].Groups))
	for gid, servers := range sc.configs[n-1].Groups {
		copy_servers := make([]string, len(servers))
		copy(copy_servers, servers)
		newGroups[gid] = copy_servers
	}
	return newGroups
}

func (sc *ShardCtrler) adjustShards(newGroups map[int][]string, newShards *[NShards]int) {
	if len(newGroups) == 0 {
		return
	}
	DPrintf("[%d] newGroups [%v], newShard [%v]", sc.me, newGroups, newShards)
	cnt := map[int][]int{}
	for gid := range newGroups {
		cnt[gid] = make([]int, 0)
	}
	for shardId, gid := range newShards {
		cnt[gid] = append(cnt[gid], shardId)
	}
	DPrintf("[%d] cnt [%v]", sc.me, cnt)
	for {
		maxGid, mx, minGid, mn := sc.findMaxAndMinGid(cnt)
		if maxGid != 0 && mx-mn <= 1 {
			return
		}
		DPrintf("[%d] cnt [%v], newShards [%+v]", sc.me, cnt, newShards)
		shardId := cnt[maxGid][0]
		cnt[maxGid] = cnt[maxGid][1:]
		cnt[minGid] = append(cnt[minGid], shardId)
		newShards[shardId] = minGid
	}
}

// 如果 GID 0 有分片则优先处理这些分片，因为GID 0是特殊GID
func (sc *ShardCtrler) findMaxAndMinGid(cnt map[int][]int) (int, int, int, int) {
	maxGid, mx, minGid, mn := -1, math.MinInt, -1, math.MaxInt
	gids := []int{}
	// 如果直接遍历map 不同的ShardCtrler server 可能得到不同的结果 因为 map 的遍历是随机的
	for gid := range cnt {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	for _, gid := range gids {
		m := len(cnt[gid])
		if maxGid != 0 && ((gid == 0 && m > 0) || gid != 0 && m > mx) {
			maxGid, mx = gid, m
		}
		if gid != 0 && m < mn {
			minGid, mn = gid, m
		}
	}

	return maxGid, mx, minGid, mn
}

func (sc *ShardCtrler) notifyWaitCh(index int, op *Op) {
	DPrintf("[%d] notifyWaitCh, index [%d], op [%v]", sc.me, index, op)
	if waitCh, ok := sc.waitChMap[index]; ok {
		waitCh <- op
	}
}

func (sc *ShardCtrler) isInvalidRequest(clientdId int64, requestId int64) bool {
	if lastRequestId, ok := sc.LastRequestMap[clientdId]; ok {
		return requestId <= lastRequestId
	}
	return false
}

func (sc *ShardCtrler) UpdateLastRequest(op *Op) {
	lastRequestId, ok := sc.LastRequestMap[op.ClientId]
	if (ok && lastRequestId < op.RequestId) || !ok {
		sc.LastRequestMap[op.ClientId] = op.RequestId
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitChMap = make(map[int]chan *Op)
	sc.LastRequestMap = make(map[int64]int64)
	go sc.applier()

	return sc
}
