package autodispatcher

import (
	"github.com/busgo/forest/internal/app/ectd"
	"github.com/busgo/forest/internal/app/global"
	"github.com/labstack/gommon/log"
	"sync"
	"time"
)

// collection job execute status

const (
	JobExecuteStatusCollectionPath = "/forest/client/execute/snapshot/"
	// 这里是为了收集执行状态
)

type JobCollection struct {
	node *JobNode
	lk   *sync.RWMutex
}

func NewJobCollection(node *JobNode) (c *JobCollection) {

	c = &JobCollection{
		node: node,
		lk:   &sync.RWMutex{},
	}

	c.watch()
	go c.loop()
	return
}

// watch
func (c *JobCollection) watch() {

	keyChangeEventResponse := c.node.Etcd.WatchWithPrefixKey(JobExecuteStatusCollectionPath) // 就能一直看着了 // 所以下面的都是上报目录，而不是快照目录
	log.Printf("the job collection success watch for path:%s ", JobExecuteStatusCollectionPath)
	go func() {

		for event := range keyChangeEventResponse.Event {

			c.handleJobExecuteStatusCollectionEvent(event)
		}

	}() // 这个函数执行完了，go 协程并不会结束，而是继续运行
}

// handle the job execute status
func (c *JobCollection) handleJobExecuteStatusCollectionEvent(event *ectd.KeyChangeEvent) {

	if c.node.State == global.NodeFollowerState {
		return
	}

	switch event.Type {

	case global.KeyCreateChangeEvent:

		if len(event.Value) == 0 {
			return
		}

		executeSnapshot, err := UParkJobExecuteSnapshot(event.Value)

		if err != nil {
			log.Warnf("UParkJobExecuteSnapshot:%s fail,err:%#v ", event.Value, err)
			_ = c.node.Etcd.Delete(event.Key)
			return
		}
		c.handleJobExecuteSnapshot(event.Key, executeSnapshot)

	case global.KeyUpdateChangeEvent:

		if len(event.Value) == 0 {
			return
		}

		executeSnapshot, err := UParkJobExecuteSnapshot(event.Value)

		if err != nil {
			log.Warnf("UParkJobExecuteSnapshot:%s fail,err:%#v ", event.Value, err)
			return
		}

		c.handleJobExecuteSnapshot(event.Key, executeSnapshot)

	case global.KeyDeleteChangeEvent:

	}
}

// handle job execute snapshot
func (c *JobCollection) handleJobExecuteSnapshot(path string, snapshot *JobExecuteSnapshot) {

	var (
		exist bool
		err   error
	)

	c.lk.Lock()
	defer c.lk.Unlock()
	if exist, err = c.checkExist(snapshot.Id); err != nil {
		log.Printf("check snapshot exist  error:%v", err)
		return
	}

	if exist {
		c.handleUpdateJobExecuteSnapshot(path, snapshot) // 存在就直接更新
	} else {
		c.handleCreateJobExecuteSnapshot(path, snapshot) // 不存在就创建快照
	}

}

// handle create job execute snapshot
func (c *JobCollection) handleCreateJobExecuteSnapshot(path string, snapshot *JobExecuteSnapshot) {

	if snapshot.Status == global.JobExecuteSnapshotUnkonwStatus || snapshot.Status == global.JobExecuteSnapshotErrorStatus || snapshot.Status == global.JobExecuteSnapshotSuccessStatus {
		_ = c.node.Etcd.Delete(path)
	}

	dateTime, err := ParseInLocation(snapshot.CreateTime)
	days := 0
	if err == nil {

		days = TimeSubDays(time.Now(), dateTime)

	}
	if snapshot.Status == global.JobExecuteSnapshotDoingStatus && days >= 3 {
		_ = c.node.Etcd.Delete(path)
	}  // 大于三天的为啥就删除了
	_, err = c.node.Engine.Insert(snapshot)
	if err != nil {
		log.Printf("err:%#v", err)
	}
}

// handle update job execute snapshot
func (c *JobCollection) handleUpdateJobExecuteSnapshot(path string, snapshot *JobExecuteSnapshot) {

	if snapshot.Status == global.JobExecuteSnapshotUnkonwStatus || snapshot.Status == global.JobExecuteSnapshotErrorStatus || snapshot.Status == global.JobExecuteSnapshotSuccessStatus { // 成功,失败,未知状态都删除？？？ 这里好像删除的上报的目录
		_ = c.node.Etcd.Delete(path)
	}

	dateTime, err := ParseInLocation(snapshot.CreateTime)
	days := 0
	if err == nil {

		days = TimeSubDays(time.Now(), dateTime)

	}
	if snapshot.Status == global.JobExecuteSnapshotDoingStatus && days >= 3 {
		_ = c.node.Etcd.Delete(path)
	}

	_, _ = c.node.Engine.Where("id=?", snapshot.Id).Cols("status", "finish_time", "times", "result").Update(snapshot) // 更新也是更新数据库里的,mysql数据库也就只有这一个表而已

}

// check the exist
func (c *JobCollection) checkExist(id string) (exist bool, err error) {

	var (
		snapshot *JobExecuteSnapshot
	)

	snapshot = new(JobExecuteSnapshot)

	if exist, err = c.node.Engine.Where("id=?", id).Get(snapshot); err != nil {
		return
	}

	return

}

func (c *JobCollection) loop() {

	timer := time.NewTimer(10 * time.Minute)

	for {

		key := JobExecuteStatusCollectionPath
		select {
		case <-timer.C:

			timer.Reset(10 * time.Second)
			keys, values, err := c.node.Etcd.GetWithPrefixKeyLimit(key, 1000)
			if err != nil {

				log.Warnf("collection loop err:%v ", err)
				continue
			}

			if len(keys) == 0 {

				continue

			}

			for pos := 0; pos < len(keys); pos++ {

				executeSnapshot, err := UParkJobExecuteSnapshot(values[pos])

				if err != nil {
					log.Warnf("UParkJobExecuteSnapshot:%s fail,err:%#v ", values[pos], err)
					_ = c.node.Etcd.Delete(string(keys[pos]))
					continue
				}

				if executeSnapshot.Status == global.JobExecuteSnapshotSuccessStatus || executeSnapshot.Status == global.JobExecuteSnapshotErrorStatus || executeSnapshot.Status == global.JobExecuteSnapshotUnkonwStatus {
					path := string(keys[pos])
					c.handleJobExecuteSnapshot(path, executeSnapshot)
				}

			}



		}
	}

}
