package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

// Clerk 代表键值数据库中的一个客户端。
// 它需要与Leader节点和Follower节点进行通信。
type Clerk struct {

	// Servers 是所有的数据库服务器节点的 RPC 连接列表。
	// 这使得 Clerk 可以将请求发送给 Leader。
	Servers []*labrpc.ClientEnd

	// leaderId 是当前Leader节点的服务器ID。
	// Clerk需要知道向哪个节点发送请求。
	leaderId int

	// id 是这个 Clerk 客户端的唯一标识符。
	id int64

	// seq 是下一个操作的序列号。
	// Raft使用这个序列号来提供线性一致性。
	seq int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.Servers = servers
	ck.id = makeSeed()
	ck.seq = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	id := ck.leaderId
	args := GetArgs{key}
	reply := GetReply{}
	for {
		ok := ck.Servers[id].Call("KVServer.Get", &args, &reply)
		if ok && reply.IsLeader {
			ck.leaderId = id
			//fmt.Println("get value ",  reply.Value)
			return reply.Value
		}
		//		fmt.Println("Wrong leader !")

		id = (id + 1) % len(ck.Servers)
	}

	//return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, ck.id, ck.seq}
	ck.seq++
	id := ck.leaderId
	for {
		reply := PutAppendReply{}
		ok := ck.Servers[id].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.IsLeader {
			ck.leaderId = id
			//	fmt.Println("putappend", key , value)
			return
		}
		fmt.Print()
		id = (id + 1) % len(ck.Servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
