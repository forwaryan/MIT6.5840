package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	OtherErr       = "OtherErr"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64
	ClientId  int64
	Gid       int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	Gid       int
	ShardIds  []int
	ConfigNum int
}

type PullShardReply struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]Shard
	LastRequestMap map[int64]int64
}

type RemoveShardArgs struct {
	ShardIds  []int
	ConfigNum int
}

type RemoveShardReply struct {
	Err Err
}

type AdjustShardArgs struct {
	ShardIds  []int
	ConfigNum int
}

type Command struct {
	CommandType string
	Data        interface{}
}

const (
	Get              = "Get"
	Put              = "Put"
	Append           = "Append"
	AddConfig        = "AddConfig"
	InsertShard      = "InsertShard"
	DeleteShard      = "DeleteShard"
	AdjustShardState = "AdjustShardState"
)

type CommonReply struct {
	Err   Err
	Value string
}

type GetPutAppendArgs struct {
	Key       string
	Value     string
	OpType    string // "Put" or "Append" or "Get"
	RequestId int64
	ClientId  int64
	Gid       int
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
