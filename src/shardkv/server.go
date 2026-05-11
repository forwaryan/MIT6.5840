package shardkv

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead              int32
	manager           *shardctrler.Clerk
	currentConfig     shardctrler.Config
	lastConfig        shardctrler.Config
	shards            map[int]*Shard
	waitChMap         map[int]chan *CommonReply
	LastRequestMap    map[int64]int64
	lastIncludedIndex int
	persister         *raft.Persister
}

type ShardState string

const (
	Serving   = "Serving"   // 当前分片正常服务中
	Pulling   = "Pulling"   // 当前分片正在从其它复制组中拉取信息
	BePulling = "BePulling" // 当前分片正在复制给其它复制组
	GCing     = "GCing"     // 当前分片正在等待清除（监视器检测到后需要从拥有这个分片的复制组中删除分片）
)

type Shard struct {
	ShardKVDB map[string]string
	State     ShardState
}

func (s *Shard) get(key string) string {
	return s.ShardKVDB[key]
}

func (s *Shard) put(key string, value string) {
	s.ShardKVDB[key] = value
}

func (s *Shard) append(key string, value string) {
	str := s.ShardKVDB[key]
	s.ShardKVDB[key] = str + value
}

func MakeShard(state ShardState) *Shard {
	return &Shard{
		ShardKVDB: make(map[string]string),
		State:     state,
	}
}

func (kv *ShardKV) Get(args *GetPutAppendArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if !kv.checkShardAndState(shardId) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	command := Command{
		CommandType: Get,
		Data:        *args,
	}
	response := &CommonReply{Err: OK}
	kv.StartCommand(command, response)
	reply.Value, reply.Err = response.Value, response.Err
}

func (kv *ShardKV) StartCommand(command Command, response *CommonReply) {
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	id, gid := kv.me, kv.gid
	if checkNotPutGetAppend(command.CommandType) {
		DPrintf("server [%d, %d], send command to leader, log index[%d], log term [%d], command [%+v]", id, gid, index, term, command)

	}
	waitChan, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan *CommonReply, 1)
		waitChan = kv.waitChMap[index]
	}
	kv.mu.Unlock()

	select {
	case res := <-waitChan:
		if checkNotPutGetAppend(command.CommandType) {
			DPrintf("server [%d, %d] receive res from waitChan [%+v]", id, gid, res)
		}
		response.Value, response.Err = res.Value, res.Err
		currentTerm, stillLeader := kv.rf.GetState()
		if !stillLeader || currentTerm != term {
			response.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		response.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.waitChMap, index)
	kv.mu.Unlock()
}

func checkNotPutGetAppend(cmdType string) bool {
	if cmdType != Get && cmdType != Put && cmdType != Append {
		return true
	}
	return false
}

func (kv *ShardKV) checkShardAndState(shardId int) bool {
	shard, ok := kv.shards[shardId]
	if ok && kv.currentConfig.Shards[shardId] == kv.gid &&
		(shard.State == Serving || shard.State == GCing) {
		return true
	}
	return false
}

func (kv *ShardKV) PutAppend(args *GetPutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if !kv.checkShardAndState(shardId) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.isInvalidRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	command := Command{
		CommandType: args.OpType,
		Data:        *args,
	}
	response := &CommonReply{Err: OK}
	kv.StartCommand(command, response)
	reply.Err = response.Err

}

func (kv *ShardKV) isInvalidRequest(clientId int64, requestId int64) bool {
	if lastRequestId, ok := kv.LastRequestMap[clientId]; ok {
		if requestId <= lastRequestId {
			return true
		}
	}
	return false
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		id, gid := kv.me, kv.gid
		if applyMsg.CommandValid {
			if applyMsg.CommandIndex <= kv.lastIncludedIndex {
				kv.mu.Unlock()
				continue
			}
			DPrintf("server [%d, %d] in applier() receive applyMsg [%+v]", id, gid, applyMsg)
			reply := kv.execute(applyMsg.Command)
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader && applyMsg.CommandTerm == currentTerm {
				kv.notifyWaitCh(applyMsg.CommandIndex, reply)
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.rf.Snapshot(applyMsg.CommandIndex, kv.encodeState())
			}
			kv.lastIncludedIndex = applyMsg.CommandIndex
		} else if applyMsg.SnapshotValid {
			if applyMsg.SnapshotIndex <= kv.lastIncludedIndex {
				kv.mu.Unlock()
				continue
			}
			kv.readPersist(applyMsg.Snapshot)
			kv.lastIncludedIndex = applyMsg.SnapshotIndex
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) notifyWaitCh(index int, reply *CommonReply) {
	if waitCh, ok := kv.waitChMap[index]; ok {
		waitCh <- reply
	}
}

func (kv *ShardKV) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.LastRequestMap)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	kvstate := w.Bytes()
	return kvstate
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	shards := map[int]*Shard{}
	lastRequestMap := map[int64]int64{}
	lastConfig := shardctrler.Config{}
	currentConfig := shardctrler.Config{}
	if d.Decode(&shards) != nil || d.Decode(&lastRequestMap) != nil ||
		d.Decode(&lastConfig) != nil || d.Decode(&currentConfig) != nil {
		return
	} else {
		kv.shards = shards
		kv.LastRequestMap = lastRequestMap
		kv.lastConfig = lastConfig
		kv.currentConfig = currentConfig
	}
}

func (kv *ShardKV) execute(cmd interface{}) *CommonReply {
	command := cmd.(Command)
	reply := &CommonReply{
		Err: OK,
	}
	// DPrintf("server [%d, %d] ready for execute command [%+v]", kv.me, kv.gid, command)
	switch command.CommandType {
	case Get, Put, Append:
		op := command.Data.(GetPutAppendArgs)
		kv.processGetPutAppend(&op, reply)
	case AddConfig:
		config := command.Data.(shardctrler.Config)
		kv.processAddConfig(&config, reply)
	case InsertShard:
		response := command.Data.(PullShardReply)
		kv.processInsertShard(&response, reply)
	case DeleteShard:
		args := command.Data.(RemoveShardArgs)
		kv.processDeleteShard(&args, reply)
	case AdjustShardState:
		args := command.Data.(AdjustShardArgs)
		kv.processAdjustGCingShard(&args, reply)
	}
	return reply
}

func (kv *ShardKV) processGetPutAppend(op *GetPutAppendArgs, reply *CommonReply) {
	shardId := key2shard(op.Key)
	if !kv.checkShardAndState(shardId) {
		reply.Err = ErrWrongGroup
		return
	}
	if op.OpType == Get || !kv.isInvalidRequest(op.ClientId, op.RequestId) {
		switch op.OpType {
		case Get:
			reply.Value = kv.shards[shardId].get(op.Key)
		case Put:
			kv.shards[shardId].put(op.Key, op.Value)
		case Append:
			kv.shards[shardId].append(op.Key, op.Value)
		}
		// DPrintf("server [%d, %d] processGetPutAppend OpType [%s] success!", kv.me, kv.gid, op.OpType)
		kv.UpdateLastRequest(op)
	}
}

func (kv *ShardKV) UpdateLastRequest(op *GetPutAppendArgs) {
	lastRequestId, ok := kv.LastRequestMap[op.ClientId]
	if (ok && lastRequestId < op.RequestId) || !ok {
		kv.LastRequestMap[op.ClientId] = op.RequestId
	}
}

func (kv *ShardKV) processAddConfig(newConfig *shardctrler.Config, reply *CommonReply) {
	DPrintf("server [%d, %d] processAddConfig, currentConfig [%+v], newConfig [%+v]", kv.me, kv.gid, kv.currentConfig, *newConfig)
	if newConfig.Num == kv.currentConfig.Num+1 {
		states := ""
		for i := 0; i < shardctrler.NShards; i++ {
			// 第 i 个分片从不由自己管理到由自己管理
			if newConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] != kv.gid {
				// 若当前该分片由其它组管理的话，需要从其它组那里拉去信息
				if kv.currentConfig.Shards[i] != 0 {
					kv.shards[i].State = Pulling
				}
			}
			// 第 i 个分片从由自己管理到不由自己管理
			if newConfig.Shards[i] != kv.gid && kv.currentConfig.Shards[i] == kv.gid {
				// 若当前分片需要给其它组的话，设置状态为待拉取
				if newConfig.Shards[i] != 0 {
					kv.shards[i].State = BePulling
				}
			}
			states += string(kv.shards[i].State) + ", "
		}
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *newConfig
		DPrintf("server [%d, %d] updates shards state and config over, shard state [%s]", kv.me, kv.gid, states)

		reply.Err = OK
	} else {
		reply.Err = OtherErr
	}
}

func (kv *ShardKV) processInsertShard(response *PullShardReply, reply *CommonReply) {
	DPrintf("server [%d, %d] processInsertShard, currentConfigNum [%d], response.ConfigNum [%d]", kv.me, kv.gid, kv.currentConfig.Num, response.ConfigNum)

	if response.ConfigNum == kv.currentConfig.Num {
		Shards := response.Shards
		for shardId, shard := range Shards {
			oldShard := kv.shards[shardId]
			if oldShard.State == Pulling {
				for key, value := range shard.ShardKVDB {
					oldShard.ShardKVDB[key] = value
				}
				oldShard.State = GCing
			}
		}
		DPrintf("server [%d, %d] updates shards [%+v], now shards [%s]", kv.me, kv.gid, Shards, ToString(kv.shards))
		LastRequestMap := response.LastRequestMap

		for clientId, requestId := range LastRequestMap {
			kv.LastRequestMap[clientId] = max(requestId, kv.LastRequestMap[clientId])
		}

		DPrintf("server [%d, %d] InsertShard over", kv.me, kv.gid)
	} else {
		reply.Err = OtherErr
	}
}

func (kv *ShardKV) processDeleteShard(args *RemoveShardArgs, reply *CommonReply) {
	DPrintf("server [%d, %d] processDeleteShard, currentConfigNum [%d], args.ConfigNum [%d]", kv.me, kv.gid, kv.currentConfig.Num, args.ConfigNum)

	if args.ConfigNum == kv.currentConfig.Num {
		DPrintf("server [%d, %d] original shards [%s]", kv.me, kv.gid, ToString(kv.shards))
		for _, shardId := range args.ShardIds {
			_, ok := kv.shards[shardId]
			if ok && kv.shards[shardId].State == BePulling {
				kv.shards[shardId] = MakeShard(Serving)
			}
		}
		DPrintf("server [%d, %d] updates shards [%s]", kv.me, kv.gid, ToString(kv.shards))
	} else {

		reply.Err = OtherErr
		if args.ConfigNum < kv.currentConfig.Num {
			reply.Err = OK
		}
		DPrintf("server [%d, %d] receive old delete shards request", kv.me, kv.gid)
	}
}

func (kv *ShardKV) processAdjustGCingShard(args *AdjustShardArgs, reply *CommonReply) {
	DPrintf("server [%d, %d] adjustGCingShard, currentConfigNum [%d], args.ConfigNum [%d]", kv.me, kv.gid, kv.currentConfig.Num, args.ConfigNum)

	if args.ConfigNum == kv.currentConfig.Num {
		DPrintf("server [%d, %d] original shards [%s]", kv.me, kv.gid, ToString(kv.shards))
		for _, shardId := range args.ShardIds {
			if _, ok := kv.shards[shardId]; ok {
				kv.shards[shardId].State = Serving
			}
		}
		DPrintf("server [%d, %d] updates shards [%s]", kv.me, kv.gid, ToString(kv.shards))
	} else {
		reply.Err = OtherErr
		if args.ConfigNum < kv.currentConfig.Num {
			reply.Err = OK
		}
		DPrintf("server [%d, %d] receive old delete shards request", kv.me, kv.gid)
	}
}

func ToString(shards map[int]*Shard) string {
	str := ""
	for i := 0; i < shardctrler.NShards; i++ {
		str += strconv.Itoa(i) + ": " + string(shards[i].State) + ", "
	}
	return str
}

func (kv *ShardKV) monitorRequestConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		id, gid := kv.me, kv.gid
		isProcessShardCommand := false
		for _, shard := range kv.shards {
			if shard.State != Serving {
				isProcessShardCommand = true
				break
			}
		}
		currentConfigNum := kv.currentConfig.Num
		DPrintf("server [%d, %d] in monitorRequestConfig() isProcessShardCommand, %t, currentConfig [%+v]",
			id, gid, isProcessShardCommand, kv.currentConfig)
		if isProcessShardCommand {
			DPrintf("server [%d, %d] shards [%s]", id, gid, ToString(kv.shards))
		}
		kv.mu.Unlock()

		if !isProcessShardCommand {
			newConfig := kv.manager.Query(currentConfigNum + 1)
			if newConfig.Num == currentConfigNum+1 {
				reply := &CommonReply{}
				DPrintf("server [%d, %d] receive new Config [%+v], process add config", id, gid, newConfig)
				command := Command{
					CommandType: AddConfig,
					Data:        newConfig,
				}
				kv.StartCommand(command, reply)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) monitorInsert() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		id, gid := kv.me, kv.gid
		groups := kv.getShardIdsWithSpecifiedState(Pulling)
		wg := &sync.WaitGroup{}
		wg.Add(len(groups))
		if len(groups) != 0 {
			DPrintf("server [%d, %d] in monitorInsert() requests pull shards in monitorInsert, groups [%+v]", id, gid, groups)
		}
		for oldGid, shardIds := range groups {
			configNum, servers := kv.currentConfig.Num, kv.lastConfig.Groups[oldGid]
			go func(oldGid int, configNum int, servers []string, shardIds []int) {
				defer wg.Done()
				DPrintf("server [%d, %d] send GetShards request to other servers, oldGid [%d], configNum [%d], servers [%+v], shardIds [%+v]",
					id, gid, oldGid, configNum, servers, shardIds)
				for _, server := range servers {
					args := &PullShardArgs{
						Gid:       oldGid,
						ShardIds:  shardIds,
						ConfigNum: configNum,
					}
					reply := &PullShardReply{}
					srv := kv.make_end(server)
					ok := srv.Call("ShardKV.GetShards", args, reply)
					if ok && reply.Err == OK {
						reply.ConfigNum = configNum
						command := Command{
							CommandType: InsertShard,
							Data:        *reply,
						}
						DPrintf("server [%d, %d] StartCommand [%+v]", id, gid, command)
						kv.StartCommand(command, &CommonReply{})
					}
				}
			}(oldGid, configNum, servers, shardIds)
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)

	}
}

func (kv *ShardKV) getShardIdsWithSpecifiedState(state ShardState) map[int][]int {
	tmp := make(map[int][]int)
	for shardId, shard := range kv.shards {
		if shard.State == state {
			gid := kv.lastConfig.Shards[shardId]
			if _, ok := tmp[gid]; !ok {
				tmp[gid] = make([]int, 0)
			}
			tmp[gid] = append(tmp[gid], shardId)
		}
	}
	return tmp
}

func (kv *ShardKV) GetShards(args *PullShardArgs, reply *PullShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum == kv.currentConfig.Num {
		shards := make(map[int]Shard)
		for _, shardId := range args.ShardIds {
			_, ok := kv.shards[shardId]
			if ok && kv.shards[shardId].State == BePulling {
				shards[shardId] = kv.shards[shardId].CopyShard()
			}
		}
		reply.Err, reply.Shards, reply.LastRequestMap = OK, shards, kv.copyLastRequestMap()
	} else {
		DPrintf("server [%d, %d] receives out of data GetShards request, currentConfigNum [%d], requestConfigNum [%d]", kv.me, kv.gid, kv.currentConfig.Num, args.ConfigNum)
		reply.Err = ErrWrongGroup
	}
}

func (s *Shard) CopyShard() Shard {
	newData := make(map[string]string, len(s.ShardKVDB))
	for k, v := range s.ShardKVDB {
		newData[k] = v
	}

	return Shard{
		ShardKVDB: newData,
		State:     Serving,
	}
}

func (kv *ShardKV) copyLastRequestMap() map[int64]int64 {
	lastRequestMap := make(map[int64]int64)
	for clientId, requestId := range kv.LastRequestMap {
		lastRequestMap[clientId] = requestId
	}
	return lastRequestMap
}

func (kv *ShardKV) monitorGC() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		id, gid := kv.me, kv.gid
		groups := kv.getShardIdsWithSpecifiedState(GCing)
		wg := &sync.WaitGroup{}
		wg.Add(len(groups))
		if len(groups) != 0 {
			DPrintf("server [%d, %d] in monitorGC() is requested pull shards in monitorGC, groups [%+v]", id, gid, groups)
		}
		for oldGid, shardIds := range groups {
			configNum, servers := kv.currentConfig.Num, kv.lastConfig.Groups[oldGid]
			go func(oldGid int, configNum int, servers []string, shardIds []int) {
				defer wg.Done()
				DPrintf("server [%d, %d] send DeleteShards request, oldGid [%d], configNum [%d], servers [%+v], shardIds [%+v]",
					id, gid, oldGid, configNum, servers, shardIds)
				for _, server := range servers {
					args := &RemoveShardArgs{
						ShardIds:  shardIds,
						ConfigNum: configNum,
					}
					reply := &RemoveShardReply{}
					srv := kv.make_end(server)
					ok := srv.Call("ShardKV.DeleteShards", args, reply)
					if ok && reply.Err == OK {
						adjargs := AdjustShardArgs{
							ShardIds:  shardIds,
							ConfigNum: configNum,
						}
						command := Command{
							CommandType: AdjustShardState,
							Data:        adjargs,
						}
						kv.StartCommand(command, &CommonReply{})
						DPrintf("server [%d, %d] adjust shard state over!!", id, gid)
					}
				}
			}(oldGid, configNum, servers, shardIds)
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) DeleteShards(args *RemoveShardArgs, reply *RemoveShardReply) {
	command := Command{
		CommandType: DeleteShard,
		Data:        *args,
	}
	response := &CommonReply{}
	DPrintf("server [%d, %d] StartCommand [%+v]", kv.me, kv.gid, command)
	kv.StartCommand(command, response)
	reply.Err = response.Err
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	labgob.Register(Command{})
	labgob.Register(GetPutAppendArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullShardReply{})
	labgob.Register(RemoveShardArgs{})
	labgob.Register(AdjustShardArgs{})
	labgob.Register(Shard{})

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.manager = shardctrler.MakeClerk(kv.ctrlers)
	kv.currentConfig, kv.lastConfig = shardctrler.Config{}, shardctrler.Config{}
	kv.shards = make(map[int]*Shard)
	kv.waitChMap = make(map[int]chan *CommonReply)
	kv.LastRequestMap = make(map[int64]int64)
	kv.persister = persister
	kv.readPersist(kv.persister.ReadSnapshot())

	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		if _, ok := kv.shards[shardId]; !ok {
			kv.shards[shardId] = MakeShard(Serving)
		}
	}

	go kv.applier()
	go kv.monitorRequestConfig()
	go kv.monitorInsert()
	go kv.monitorGC()

	return kv
}
