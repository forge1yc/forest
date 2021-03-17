package autodispatcher

import (
	"context"
	"errors"
	"fmt"
	"github.com/busgo/forest/internal/app/ectd"
	"github.com/busgo/forest/internal/app/global"
	"github.com/coreos/etcd/clientv3"
	"github.com/labstack/gommon/log"
	"math/rand"
	"sync"
	"time"
)

//const (
//	GroupConfPath = "/forest/server/group/" // 这里一开始谁注册的，注册也是往这里注册
//	ClientPath    = "/forest/client/%s/clients/" // 这是工作节点的注册路径
//
//)

type JobGroupManager struct {
	node   *JobNode
	Groups map[string]*Group
	lk     *sync.RWMutex
}

func NewJobGroupManager(node *JobNode) (mgr *JobGroupManager) {

	mgr = &JobGroupManager{
		node:   node,
		Groups: make(map[string]*Group),
		lk:     &sync.RWMutex{},
	}

	go mgr.watchGroupPath()  // 内部线程阻塞监视

	go mgr.LoopLoadGroups()

	return

}

// watch the group path
func (mgr *JobGroupManager) watchGroupPath() { // 阻塞监视 // 这种阻塞监视的都需要用go起

	// 这里查看所有的
	keyChangeEventResponse := mgr.node.Etcd.WatchWithPrefixKey(global.GroupConfPath)
	// 下面可以用for + switch 判断
	for ch := range keyChangeEventResponse.Event {
		mgr.handleGroupChangeEvent(ch) // 阻塞一直等待事件的到来
	}

}

func (mgr *JobGroupManager) LoopLoadGroups() {

RETRY:
	var (
		keys   [][]byte
		values [][]byte
		err    error
	)
	if keys, values, err = mgr.node.Etcd.GetWithPrefixKey(global.GroupConfPath); err != nil { // 所以之前必须有一个etcd管理，来注册所有的节点信息，在前面应该有配置,node那里

		goto RETRY // 拿不到集群的就一直尝试，这里是拿所有集群，所以每个节点都要拿到所有的信息
	}

	if len(keys) == 0 {
		return
	}

	for i := 0; i < len(keys); i++ {
		path := string(keys[i]) // 这里对应的是集群地址 ip
		groupConf, err := UParkGroupConf(values[i])
		if err != nil {
			log.Warnf("upark the group conf error:%#v", err)
			continue
		}

		mgr.addGroup(groupConf.Name, path) // name 名字
	}

}

func (mgr *JobGroupManager) addGroup(name, path string) {

	mgr.lk.Lock()
	defer mgr.lk.Unlock()

	if _, ok := mgr.Groups[path]; ok {

		return // 已经存在就返回
	}
	group := NewGroup(name, path, mgr.node) // 添加一个group之后，后面就自动监控节点了，这里可以先写在define里面，之后看情况考虑是否添加到service.conf 里面
	mgr.Groups[path] = group
	log.Infof("add a new group:%s,for path:%s", name, path)

}

// delete a group  for path
func (mgr *JobGroupManager) deleteGroup(path string) {

	var (
		group *Group
		ok    bool
	)
	mgr.lk.Lock()
	defer mgr.lk.Unlock()

	if group, ok = mgr.Groups[path]; ok {

		return
	}

	// cancel watch the clients
	_ = group.watcher.Close()
	group.cancelFunc()
	delete(mgr.Groups, path)

	log.Infof("delete a  group:%s,for path:%s", group.name, path)
}

// handle the group change event
func (mgr *JobGroupManager) handleGroupChangeEvent(changeEvent *ectd.KeyChangeEvent) {

	switch changeEvent.Type {

	case global.KeyCreateChangeEvent:
		mgr.handleGroupCreateEvent(changeEvent)

	case global.KeyUpdateChangeEvent:
		// ignore
	case global.KeyDeleteChangeEvent:
		mgr.handleGroupDeleteEvent(changeEvent)
	}
}

func (mgr *JobGroupManager) handleGroupCreateEvent(changeEvent *ectd.KeyChangeEvent) {

	groupConf, err := UParkGroupConf(changeEvent.Value)
	if err != nil {
		log.Warnf("upark the group conf error:%#v", err)
		return
	}

	path := changeEvent.Key // path 都是key
	mgr.addGroup(groupConf.Name, path) // remark 含义是啥我还不太明白呢

}

func (mgr *JobGroupManager) handleGroupDeleteEvent(changeEvent *ectd.KeyChangeEvent) {

	path := changeEvent.Key

	mgr.deleteGroup(path)
}

func (mgr *JobGroupManager) SelectClient(name string) (client *Client, err error) {

	var (
		group *Group
		ok    bool
	)

	// 这里设置的是可以自选集群，其实挺好的，然后再集群里面找
	if group, ok = mgr.Groups[global.GroupConfPath+name]; !ok {
		err = errors.New(fmt.Sprintf("the group:%s not found", name))
		return
	}

	return group.selectClient()

}

type Group struct {
	path       string
	name       string
	node       *JobNode
	watchPath  string
	Clients    map[string]*Client
	watcher    clientv3.Watcher
	cancelFunc context.CancelFunc
	lk         *sync.RWMutex
}

// create a new group
func NewGroup(name, path string, node *JobNode) (group *Group) {

	group = &Group{
		name:      name,
		path:      path,
		node:      node,
		watchPath: fmt.Sprintf(global.ClientPath, name), // name is ip
		Clients:   make(map[string]*Client),
		lk:        &sync.RWMutex{},
	}

	go group.watchClientPath()

	go group.loopLoadAllClient()

	return
}

// watch the client path
func (group *Group) watchClientPath() { // 这里监控是否有集群下的节点变化

	keyChangeEventResponse := group.node.Etcd.WatchWithPrefixKey(group.watchPath)
	group.watcher = keyChangeEventResponse.Watcher
	group.cancelFunc = keyChangeEventResponse.CancelFunc
	for ch := range keyChangeEventResponse.Event {

		group.handleClientChangeEvent(ch)

	}

}

// loop load all client
func (group *Group) loopLoadAllClient() {

RETRY:
	var (
		keys   [][]byte
		values [][]byte
		err    error
	)

	// 这里能这样做是因为节点初始化的时候都注册进去了
	prefix := fmt.Sprintf(global.ClientPath, group.name)
	if keys, values, err = group.node.Etcd.GetWithPrefixKey(prefix); err != nil { // 这里面是所有的ip，也就是真实的主机

		time.Sleep(time.Second)
		goto RETRY
	}

	if len(keys) == 0 {
		return
	}

	for i := 0; i < len(keys); i++ {
		path := string(keys[i]) // etcd 长地址
		value := string(values[i]) // 单机ip    如果集群，这里对应的是名字
		if value == "" {
			log.Warnf("the client value is nil for path:%s", path)
			continue
		}

		group.addClient(value, path)
	}

}

// handle the client change event
func (group *Group) handleClientChangeEvent(changeEvent *ectd.KeyChangeEvent) {

	switch changeEvent.Type {

	case global.KeyCreateChangeEvent:
		path := changeEvent.Key // path 是 key value 代表 集群
		name := string(changeEvent.Value) // name 是ip
		group.addClient(name, path)

	case global.KeyUpdateChangeEvent:
		//ignore
	case global.KeyDeleteChangeEvent:
		path := changeEvent.Key
		group.deleteClient(path)
	}
}

// add  a new  client
func (group *Group) addClient(name, path string) {

	group.lk.Lock()
	defer group.lk.Unlock()
	if _, ok := group.Clients[path]; ok {
		log.Warnf("name:%s,path:%s,the client exist", name, path)
		return
	}

	client := &Client{
		Name: name,
		Path: path,
	}

	group.Clients[path] = client // 这个path 是
	log.Printf("add a new client for path:%s", path)

}

// delete a client for path
func (group *Group) deleteClient(path string) {

	var (
		client *Client
		ok     bool
	)
	group.lk.Lock()
	defer group.lk.Unlock()
	if client, ok = group.Clients[path]; !ok {
		log.Warnf("path:%s,the client not  exist", path)
		return
	}

	delete(group.Clients, path)
	log.Printf("delete a  client for path:%s", path)

	// fail over
	if group.node.State == global.NodeLeaderState {
		group.node.FailOver.DeleteClientEventChans <- &JobClientDeleteEvent{Group: group, Client: client}
	}

}

func (group *Group) selectClient() (client *Client, err error) {
	group.lk.RLock() // 锁一下怕 同时被选择吗,怕一个客户端同时
	defer group.lk.RUnlock()

	if len(group.Clients) == 0 { // 初始化的时候一个集群下面有多少都被添加进去了
		err = errors.New(fmt.Sprintf("the group:%s,has no client to select", group.name))
		return
	}

	num := len(group.Clients)

	pos := rand.Intn(num) //就是为了随机选择一个client，client 带ip

	index := 0

	for _, c := range group.Clients { // 但是为啥不用这里的索引呢

		if index == pos {

			client = c
			return
		}
		index++
	}

	return

}

// client
type Client struct {
	Name string
	Path string
}
