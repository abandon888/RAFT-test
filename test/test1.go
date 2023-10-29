package test

import (
	"fmt"
	"main/kvraft"
	"math/rand"
	"strconv"
	"time"
)

// 声明全局变量count
var count = 0

/** @fn request
 *  @brief 客户端请求
 *  @param num 客户端编号
 *  @return void
*   @note 客户端请求，每个客户端循环写入1000次，每次写入一个随机数，key和value都是int类型
*/
func request(num int) {
	// 定义数据库中的一个客户端信息
	ck := kvraft.Clerk{}
	// 创建一个长度为3的服务器列表
	ck.Servers = make([]string, 3)

	// 三个server ip需要配置
	// 设置三个server的ip和端口
	ck.Servers[0] = "ip:50011"
	ck.Servers[1] = "ip:50001"
	ck.Servers[2] = "ip:50021"
	// 循环写入1000次，每次写入一个随机数，key和value都是int类型
	for i := 0; i < 1000; i++ {
		// 设置随机数种子
		rand.Seed(time.Now().UnixNano())
		// 生成随机数在100000以内
		key := rand.Intn(100000)
		// 生成随机数在100000以内
		value := rand.Intn(100000)
		// 写操作，key和value都是int类型，需要转换成string类型
		ck.Put("key"+strconv.Itoa(key), "value"+strconv.Itoa(value))
		// 读操作，获取key对应的value
		fmt.Println(num, ck.Get("key1"))
		count++
	}
}

func main() {
	// 定义客户端数量
	clients := 10
	// 循环创建客户端
	for i := 0; i < clients; i++ {
		go request(i)
	}
	// 休眠一秒钟
	time.Sleep(time.Second)
	// 一秒钟的读写数
	fmt.Println(count)
	// 休眠20分钟
	time.Sleep(time.Second * 1200)

}
