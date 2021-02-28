package forest

import (
	"github.com/labstack/gommon/log"
	"github.com/robfig/cron"
	"sync"
	"time"
)

// job scheduler
type JobScheduler struct {
	node          *JobNode
	eventChan     chan *JobChangeEvent
	schedulePlans map[string]*SchedulePlan
	lk            *sync.RWMutex
	syncStatus    bool
}

func NewJobScheduler(node *JobNode) (sch *JobScheduler) {

	sch = &JobScheduler{
		node:          node,
		eventChan:     make(chan *JobChangeEvent, 250),
		schedulePlans: make(map[string]*SchedulePlan),
		lk:            &sync.RWMutex{},
		syncStatus:    false,
	}
	go sch.loopSchedule()
	go sch.loopSync()

	return
}

// handle the job change event
func (sch *JobScheduler) handleJobChangeEvent(event *JobChangeEvent) {

	sch.lk.Lock()
	defer sch.lk.Unlock()
	switch event.Type {
	case JobCreateChangeEvent:
		sch.handleJobCreateEvent(event)
	case JobUpdateChangeEvent:
		sch.handleJobUpdateEvent(event)
	case JobDeleteChangeEvent:
		sch.handleJobDeleteEvent(event)
	}
}

// handle the job create event
func (sch *JobScheduler) handleJobCreateEvent(event *JobChangeEvent) {

	sch.createJobPlan(event)

}

// handle the job update event
func (sch *JobScheduler) handleJobUpdateEvent(event *JobChangeEvent) {

	var (
		err      error
		schedule cron.Schedule
		plan     *SchedulePlan
		ok       bool
	)

	jobConf := event.Conf

	if _, ok = sch.schedulePlans[jobConf.Id]; !ok {
		log.Warnf("the job conf:%#v not  exist", jobConf)
		log.Warnf("the job conf:%#v change job create event", jobConf)

		sch.createJobPlan(&JobChangeEvent{
			Type: JobCreateChangeEvent,
			Conf: jobConf,
		})
		return
	}

	// stop must delete from the job schedule plan list
	if jobConf.Status == JobStopStatus {

		log.Warnf("the job conf:%#v status is stop must delete from the schedule plan ", jobConf)
		delete(sch.schedulePlans, jobConf.Id)
		return
	}

	if schedule, err = cron.Parse(jobConf.Cron); err != nil {
		log.Errorf("the job conf:%#v  parse the cron error:%#v", jobConf, err)
		return
	}

	// build schedule plan
	plan = &SchedulePlan{
		Id:       jobConf.Id,
		Name:     jobConf.Name,
		Group:    jobConf.Group,
		Cron:     jobConf.Cron,
		Target:   jobConf.Target,
		Params:   jobConf.Params,
		Mobile:   jobConf.Mobile,
		Remark:   jobConf.Remark,
		schedule: schedule,
		Version:  jobConf.Version,
		NextTime: schedule.Next(time.Now()),
	}

	// update the schedule plan
	sch.schedulePlans[jobConf.Id] = plan
	log.Printf("the job conf:%#v update a new schedule plan:%#v", jobConf, plan)
}

// handle the job delete event
func (sch *JobScheduler) handleJobDeleteEvent(event *JobChangeEvent) {

	var (
		plan *SchedulePlan
		ok   bool
	)
	jobConf := event.Conf

	if plan, ok = sch.schedulePlans[jobConf.Id]; !ok {
		log.Printf("the job conf:%#v not  exist", jobConf)
		return
	}

	if plan.Version > jobConf.Version && jobConf.Version != -1 {
		log.Warnf("the job conf:%#v version:%d <  schedule plan:%#v,version:%d", jobConf, jobConf.Version, plan, plan.Version)
		return
	}
	log.Warnf("the job conf:%#v delete a  schedule plan:%#v", jobConf, plan)
	delete(sch.schedulePlans, jobConf.Id)

}

func (sch *JobScheduler) createJobPlan(event *JobChangeEvent) {

	var (
		err      error
		schedule cron.Schedule
	)

	jobConf := event.Conf

	if _, ok := sch.schedulePlans[jobConf.Id]; ok {
		log.Warnf("the job conf:%#v exist", jobConf)
		return
	}

	if jobConf.Status == JobStopStatus {

		log.Warnf("the job conf:%#v status is stop", jobConf)
		return
	}

	if schedule, err = cron.Parse(jobConf.Cron); err != nil {
		log.Errorf("the job conf:%#v cron is error exp ", jobConf)
		return
	}

	// build schedule plan
	plan := &SchedulePlan{
		Id:       jobConf.Id,
		Name:     jobConf.Name,
		Group:    jobConf.Group,
		Cron:     jobConf.Cron,
		Target:   jobConf.Target,
		Params:   jobConf.Params,
		Mobile:   jobConf.Mobile,
		Remark:   jobConf.Remark,
		Version:  jobConf.Version,
		schedule: schedule,
		NextTime: schedule.Next(time.Now()),
	}

	sch.schedulePlans[jobConf.Id] = plan

	log.Printf("the job conf:%#v create a new schedule plan:%#v", jobConf, plan)
}

// push a job change event
func (sch *JobScheduler) pushJobChangeEvent(event *JobChangeEvent) {

	sch.eventChan <- event
}

// loop schedule job
func (sch *JobScheduler) loopSchedule() {

	timer := time.NewTimer(time.Second)

	for { // 这类是无限调度了

		select {

		case <-timer.C: // 这里一秒调度一次？？如果有重复的怎么办

		case event := <-sch.eventChan: // 这里接上面的事件

			sch.handleJobChangeEvent(event)
		}

		// select 执行完之后到这里

		durationTime := sch.trySchedule()
		log.Infof("the durationTime :%d", durationTime)
		timer.Reset(durationTime)
	}

}

// try schedule the job
func (sch *JobScheduler) trySchedule() time.Duration {

	var (
		first bool
	)
	if len(sch.schedulePlans) == 0 {

		return time.Second
	}

	now := time.Now()
	leastTime := new(time.Time)
	first = true
	for _, plan := range sch.schedulePlans { // 如果有就一直调度,怎么还是单机啊，和我想的有点不一样

		scheduleTime := plan.NextTime
		if scheduleTime.Before(now) && sch.node.state == NodeLeaderState { // 只有master才可以调度
			log.Infof("schedule execute the plan:%#v", plan)

			snapshot := &JobSnapshot{
				Id:         GenerateSerialNo() + plan.Id,
				JobId:      plan.Id,
				Name:       plan.Name,
				Group:      plan.Group,
				Cron:       plan.Cron,
				Target:     plan.Target,
				Params:     plan.Params,
				Mobile:     plan.Mobile,
				Remark:     plan.Remark,
				CreateTime: ToDateString(now),
			}
			sch.node.exec.pushSnapshot(snapshot) // 执行计划了，去找对应的集群
		}
		nextTime := plan.schedule.Next(now) // 这里执行了吗
		plan.NextTime = nextTime
		plan.BeforeTime = scheduleTime

		// first
		if first {
			first = false
			leastTime = &nextTime
		}

		// check least time after next schedule  time
		if leastTime.After(nextTime) {

			leastTime = &nextTime
		}

	}

	if leastTime.Before(now) {

		return time.Second
	}

	return leastTime.Sub(now)

}

func (sch *JobScheduler) loopSync() {

	timer := time.NewTimer(1 * time.Minute)

	for {

		select {

		case <-timer.C:
			sch.trySync() // 这是定时同步 所有节点都会做的事情，而主节点当选之后需要特殊的事情
		}
		timer.Reset(1 * time.Minute) // 一分钟同步一次

	}

}

// 同步最新配置，但也只是leader节点
func (sch *JobScheduler) trySync() {

	var (
		jobConfs []*JobConf
		err      error
	)

	if sch.syncStatus == true {
		log.Warn("the sync event is syncing ....")
		return
	}

	now := time.Now()
	log.Warn("start sync the  schedule plan ....")

	sch.lk.Lock()
	defer sch.lk.Unlock()

	sch.syncStatus = true
	defer func() {
		sch.syncStatus = false
	}()

	// load all job conf list
	if jobConfs, err = sch.node.manager.jobList(); err != nil { // 这里是最新的jobConfs，目标是让所有的调度节点都同步最新的状态
		return
	}

	if len(jobConfs) == 0 {
		return
	}

	// sync job conf
	for _, conf := range jobConfs {

		sch.handleJobConfSync(conf)

	}

	// sync not receive the job conf delete event
	for id, plan := range sch.schedulePlans {

		if !sch.existPlan(id, jobConfs) { // 只有相符合的才有效
			log.Warnf("sync the schedule plan %v must delete", plan)
			delete(sch.schedulePlans, id)
		}
	}

	log.Infof("finish sync the  schedule plan use【%dms】....", time.Now().Sub(now)/time.Millisecond)

}

// check is old plan? // old need delete
func (sch *JobScheduler) existPlan(id string, jobConfs []*JobConf) bool {

	ok := false
	for _, conf := range jobConfs {

		if conf.Id == id {
			ok = true
			break
		}

	}

	return ok

}

// 转移给其他client
func (sch *JobScheduler) handleJobConfSync(conf *JobConf) {

	var (
		exist bool
		plan  *SchedulePlan
	)

	if plan, exist = sch.schedulePlans[conf.Id]; !exist {// follower do

		if conf.Status == JobRunningStatus { // 正在执行意味着删除了，所以从新创建？？？
			log.Warnf("sync the schedule plan the job conf: %v must create", conf)
			sch.handleJobCreateEvent(&JobChangeEvent{
				Type: JobCreateChangeEvent,
				Conf: conf,
			})
		} else {

			if plan.Version < conf.Version {
				log.Warnf("sync the schedule plan %v must update", plan)
				sch.handleJobUpdateEvent(&JobChangeEvent{
					Type: JobUpdateChangeEvent,
					Conf: conf, // 以最新的为主
				})
			}

		}

	}

}

// notify the node state change event
func (sch *JobScheduler) notify(state int) {
	log.Infof("found the job :{} state notify state:%d", sch.node, state)

	if state == NodeLeaderState { // 只有leader需要同步一下最新的配置
		log.Infof("found the job :{} state notify state:%d,must sync the job schedule plan", sch.node, state)
		sch.trySync() // 这里就同步了，把leader状态告诉所有人
	}
}
