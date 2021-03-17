package app

import (
	"fmt"
	"github.com/busgo/forest/internal/app/global"
	"github.com/labstack/gommon/log"
)

//const (
//	JobSnapshotPath       = "/forest/client/snapshot/"
//	JobSnapshotGroupPath  = "/forest/client/snapshot/%s/"
//	JobClientSnapshotPath = "/forest/client/snapshot/%s/%s/"
//)

type JobExecutor struct {
	node      *JobNode
	snapshots chan *JobSnapshot
}

func NewJobExecutor(node *JobNode) (exec *JobExecutor) {

	exec = &JobExecutor{
		node:      node,
		snapshots: make(chan *JobSnapshot, 500),
	}
	go exec.lookup()

	return
}

func (exec *JobExecutor) lookup() {

	for snapshot := range exec.snapshots { // 这里接受快照，并执行

		exec.handleJobSnapshot(snapshot)
	}
}

// handle the job snapshot
func (exec *JobExecutor) handleJobSnapshot(snapshot *JobSnapshot) {
	var (
		err    error
		client *Client
	)
	group := snapshot.Group
	if client, err = exec.node.groupManager.selectClient(group); err != nil { // 这里是key，从对应的集群里面选择一个节点 还是没有找到在哪里执行
		log.Warnf("the group:%s,select a client error:%#v", group, err)
		return
	}

	clientName := client.name
	snapshot.Ip = clientName // name 对应的就是ip

	log.Printf("clientName:%#v", clientName)
	snapshotPath := fmt.Sprintf(global.JobClientSnapshotPath, group, clientName) // 往这个ip放快照，所以我到时候也需要这样，grindstone如果想用的话,我可以给这个＋ key

	log.Printf("snapshotPath:%#v", snapshotPath)
	value, err := ParkJobSnapshot(snapshot) // 这里就是一个json字符串了
	if err != nil {
		log.Warnf("uPark the snapshot  error:%#v", group, err)
		return
	}
	if err = exec.node.etcd.Put(snapshotPath+snapshot.Id, string(value)); err != nil { // 快照存储在etcd？？？   // value 里面带有ip，client端就自己从自己的配置里面拿取就行,如果grindstone也这样设计的话，就需要把上游的资源也一块给下游，就按照这个格式给，下游做完了就可以反馈说删除这个点，如果中途发生了什么，没有收到做完的信号，那么就可以把这个快照进行二次分配，这样看逻辑上还是行得通的
		log.Warnf("put  the snapshot  error:%#v", group, err) //所以这里放到etcd没有问题，我就改了他就行了，加上死活节点的检测
	}

}

// push a new job snapshot
func (exec *JobExecutor) pushSnapshot(snapshot *JobSnapshot) {

	exec.snapshots <- snapshot // 应该是同时也保存快照了
}
