package main

import (
	"flag"
	"fmt"
	"log"
	"main/myraft"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	config "main/config"
	KV "main/grpc/mykv"
	Per "main/persister"
)

type KVServer struct {
	mu sync.Mutex
	me int
	rf *myraft.Raft
	// 定义管道
	applyCh chan int
	//applyCh chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	delay        int
	// Your definitions here.
	Seq     map[int64]int64
	db      map[string]string
	chMap   map[int]chan config.Op
	persist *Per.Persister
}

// PutAppend的作用是接收客户端请求，实现了处理客户端 Put 和 Append 操作的功能,通过 Raft 节点复制到集群中,并等待复制结果后返回。
func (kv *KVServer) PutAppend(ctx context.Context, args *KV.PutAppendArgs) (*KV.PutAppendReply, error) {
	// Your code here.
	time.Sleep(time.Millisecond * time.Duration(kv.delay+rand.Intn(25))) //延迟

	reply := &KV.PutAppendReply{}        //返回值
	_, reply.IsLeader = kv.rf.GetState() //判断是否为leader
	//reply.IsLeader = false;
	if !reply.IsLeader {
		return reply, nil
	} //不是leader直接返回
	oringalOp := config.Op{args.Op, args.Key, args.Value, args.Id, args.Seq} //构造操作
	index, _, isLeader := kv.rf.Start(oringalOp)                             //发送操作
	if !isLeader {
		fmt.Println("Leader Changed !")
		reply.IsLeader = false //不是leader直接返回
		return reply, nil
	}

	apply := <-kv.applyCh //等待操作完成，传输到applyCh
	//fmt.Println(apply)

	fmt.Println(index) //打印index
	if apply == 1 {

	}
	return reply, nil

}

// Get 函数用于接收客户端请求，实现了处理客户端 Get 操作的功能,并等待复制结果后返回。
func (kv *KVServer) Get(ctx context.Context, args *KV.GetArgs) (*KV.GetReply, error) {
	time.Sleep(time.Millisecond * time.Duration(kv.delay+rand.Intn(25))) //延迟

	reply := &KV.GetReply{}
	_, reply.IsLeader = kv.rf.GetState()
	//reply.IsLeader = false;
	if !reply.IsLeader {
		return reply, nil
	}

	//fmt.Println()

	oringalOp := config.Op{"Get", args.Key, "", 0, 0} //构造操作
	_, _, isLeader := kv.rf.Start(oringalOp)
	if !isLeader {
		return reply, nil
	}
	//fmt.Println(index)

	reply.IsLeader = true
	//kv.mu.Lock()
	//fmt.Println("Asdsada")
	reply.Value = string(kv.persist.Get(args.Key))
	//fmt.Println("Asdsada")

	//kv.mu.Unlock()
	return reply, nil
}

func (kv *KVServer) equal(a config.Op, b config.Op) bool {
	return a.Option == b.Option && a.Key == b.Key && a.Value == b.Value //比较两个操作是否相同
}

// RegisterServer 方法会在端口上启动 gRPC 服务,用于接收客户端请求
func (kv *KVServer) RegisterServer(address string) {
	// Register Server
	for {
		lis, err := net.Listen("tcp", address)
		fmt.Println(address)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		KV.RegisterKVServer(s, kv)
		// Register reflection service on gRPC server.
		reflection.Register(s)
		if err := s.Serve(lis); err != nil {
			fmt.Println("failed to serve: %v", err)
		}

	}

}

func main() {
	//定义服务器地址
	var add = flag.String("address", "", "Input Your address")
	//定义服务器成员
	var mems = flag.String("members", "", "Input Your follower")
	//定义election timeout
	var delays = flag.String("delay", "", "Input Your follower")
	//flag 用于解析命令行参数
	flag.Parse()

	// 定义服务器
	server := KVServer{}
	// Local address
	address := *add
	// Initialize Server
	persist := &Per.Persister{}
	persist.Init("../db/" + address)
	//for i := 0; i <= int (address[ len(address) - 1] - '0'); i++{
	// 创建一个缓冲区为100的channel并赋值给applyCh
	server.applyCh = make(chan int, 100)

	fmt.Println(server.applyCh)
	//  服务器持久化
	server.persist = persist
	// Members's address
	members := strings.Split(*mems, ",")
	delay, _ := strconv.Atoi(*delays)
	server.delay = delay
	if delay == 0 {
		fmt.Println("##########################################")
		fmt.Println("### Don't forget input delay's value ! ###")
		fmt.Println("##########################################")
	}
	fmt.Println(address, members, delay)
	go server.RegisterServer(address + "1")                                                   // 监听端口
	server.rf = myraft.MakeRaft(address, members, persist, &server.mu, server.applyCh, delay) // 创建raft节点
	time.Sleep(time.Second * 1200)                                                            //等待raft选举完成
}
