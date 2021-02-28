package forest

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/labstack/gommon/log"
	"time"
)

const (
	JobNodePath      = "/forest/server/node/" // 这是普通的节点，不过都要注册进去
	JobNodeElectPath = "/forest/server/elect/leader" // 这个是直接写入好的,值是 ip值
	TTL              = 5
)

// job node
type JobNode struct {
	id           string
	registerPath string
	electPath    string
	etcd         *Etcd
	state        int
	apiAddress   string
	api          *JobAPi
	manager      *JobManager
	scheduler    *JobScheduler // 这是下面lister 的实例
	groupManager *JobGroupManager
	exec         *JobExecutor
	engine       *xorm.Engine
	collection   *JobCollection
	failOver     *JobSnapshotFailOver
	listeners    []NodeStateChangeListener
	close        chan bool
}

// node state change  listener
type NodeStateChangeListener interface {
	notify(int)
}


func NewJobNode(id string, etcd *Etcd, httpAddress, dbUrl string) (node *JobNode, err error) {
	// 这里只有ip是特殊的，每个job节点都不一样

	// 每个节点可以单独连接数据库
	engine, err := xorm.NewEngine("mysql", dbUrl)
	if err != nil {
		return
	}

	node = &JobNode{ // 所以jobNode 使用ip进行区分
		id:           id, // 这个id是ip    job就是分布式节点
		registerPath: fmt.Sprintf("%s%s", JobNodePath, id),
		electPath:    JobNodeElectPath,
		etcd:         etcd, // etcd基本都是一样的
		state:        NodeFollowerState,
		apiAddress:   httpAddress, // :2555
		close:        make(chan bool),
		engine:       engine, // 单独连接
		listeners:    make([]NodeStateChangeListener, 0),
	}

	node.failOver = NewJobSnapshotFailOver(node) // client故障转移

	node.collection = NewJobCollection(node) // 收集工作执行状态

	node.initNode()

	// create job executor
	node.exec = NewJobExecutor(node)
	// create  group manager
	node.groupManager = NewJobGroupManager(node)

	node.scheduler = NewJobScheduler(node)

	// create job manager
	node.manager = NewJobManager(node)

	// create a job http api
	node.api = NewJobAPi(node)

	return
}

func (node *JobNode) addListeners() {

	node.listeners = append(node.listeners, node.scheduler)

}

func (node *JobNode) changeState(state int) {

	node.state = state

	if len(node.listeners) == 0 {

		return
	}

	// notify all listener
	for _, listener := range node.listeners {

		listener.notify(state)
	}

}

// start register node
func (node *JobNode) initNode() {
	txResponse, err := node.registerJobNode() // 注册节点信息
	if err != nil {
		log.Fatalf("the job node:%s, fail register to :%s", node.id, node.registerPath)

	}
	if !txResponse.Success {
		log.Fatalf("the job node:%s, fail register to :%s,the job node id exist ", node.id, node.registerPath)
	}
	log.Printf("the job node:%s, success register to :%s", node.id, node.registerPath)
	node.watchRegisterJobNode() // 注视是监测事件
	node.watchElectPath() // 监测事件前面只是发生了选举和注册普通节点，一旦发生故障，还要有动作
	go node.loopStartElect()

}

// bootstrap
func (node *JobNode) Bootstrap() {

	go node.groupManager.loopLoadGroups()
	go node.manager.loopLoadJobConf()

	<-node.close
}

func (node *JobNode) Close() {

	node.close <- true
}

// watch the register job node
func (node *JobNode) watchRegisterJobNode() {

	keyChangeEventResponse := node.etcd.Watch(node.registerPath)

	go func() {

		for ch := range keyChangeEventResponse.Event {
			node.handleRegisterJobNodeChangeEvent(ch)
		}
	}()

}

// handle the register job node change event
func (node *JobNode) handleRegisterJobNodeChangeEvent(changeEvent *KeyChangeEvent) {

	switch changeEvent.Type {

	case KeyCreateChangeEvent:

	case KeyUpdateChangeEvent:

	case KeyDeleteChangeEvent:
		log.Printf("found the job node:%s register to path:%s has lose", node.id, node.registerPath)
		go node.loopRegisterJobNode() // 如果删除了就重新注册

	}
}

func (node *JobNode) registerJobNode() (txResponse *TxResponse, err error) {

	return node.etcd.TxKeepaliveWithTTL(node.registerPath, node.id, TTL)
}

// loop register the job node
func (node *JobNode) loopRegisterJobNode() {

RETRY:

	var (
		txResponse *TxResponse
		err        error
	)
	if txResponse, err = node.registerJobNode(); err != nil {
		log.Printf("the job node:%s, fail register to :%s", node.id, node.registerPath)
		time.Sleep(time.Second)
		goto RETRY
	}

	if txResponse.Success {
		log.Printf("the job node:%s, success register to :%s", node.id, node.registerPath)
	} else {

		v := txResponse.Value
		if v != node.id {
			time.Sleep(time.Second)
			log.Fatalf("the job node:%s,the other job node :%s has already  register to :%s", node.id, v, node.registerPath)
		}
		log.Printf("the job node:%s,has already success register to :%s", node.id, node.registerPath)
	}

}

// elect the leader
func (node *JobNode) elect() (txResponse *TxResponse, err error) {

	return node.etcd.TxKeepaliveWithTTL(node.electPath, node.id, TTL) // 这就算是leader了，之哟啊成功

}

// watch the job node elect path
func (node *JobNode) watchElectPath() {

	keyChangeEventResponse := node.etcd.Watch(node.electPath)

	go func() {

		for ch := range keyChangeEventResponse.Event {

			node.handleElectLeaderChangeEvent(ch)
		}
	}()

}

// handle the job node leader change event
func (node *JobNode) handleElectLeaderChangeEvent(changeEvent *KeyChangeEvent) {

	switch changeEvent.Type {

	case KeyDeleteChangeEvent:
		node.changeState(NodeFollowerState)
		node.loopStartElect()
	case KeyCreateChangeEvent:

	case KeyUpdateChangeEvent:

	}

}

// loop start elect
func (node *JobNode) loopStartElect() {

RETRY:
	var (
		txResponse *TxResponse
		err        error
	)
	if txResponse, err = node.elect(); err != nil {
		log.Printf("the job node:%s,elect  fail to :%s", node.id, node.electPath)
		time.Sleep(time.Second) // 1秒一次
		goto RETRY // 没选上就一直选 // 保证总会有一个leader节点
	}

	if txResponse.Success {
		node.changeState(NodeLeaderState) // 建立租约成功了，那么就要同步所有最新的配置信息
		log.Printf("the job node:%s,elect  success to :%s", node.id, node.electPath)
	} else {
		v := txResponse.Value
		if v != node.id {
			log.Printf("the job node:%s,give up elect request because the other job node：%s elect to:%s", node.id, v, node.electPath) // 这里还能返回其他当选的节点，厉害
			node.changeState(NodeFollowerState)
		} else { // 这里还是竞选成功的，感觉有问题啊，这个意思是之前就是leader节点了
			log.Printf("the job node:%s, has already elect  success to :%s", node.id, node.electPath)
			node.changeState(NodeLeaderState)
		}
	}

}
