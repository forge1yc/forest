package bus

type Job interface {
	execute() (interface{}, error)
	feedbackResult() error
}

type JobExecuteSnapshot struct {
	Id         string      `json:"id",xorm:"pk"`
	JobId      string      `json:"jobId",xorm:"job_id"`
	Name       string      `json:"name",xorm:"name"`
	Ip         string      `json:"ip",xorm:"ip"`
	Group      string      `json:"group",xorm:"group"`
	Cron       string      `json:"cron",xorm:"cron"`
	Target     string      `json:"target",xorm:"target"`
	Params     string      `json:"params",xorm:"params"`
	Mobile     string      `json:"mobile",xorm:"mobile"`
	Remark     string      `json:"remark",xorm:"remark"`
	CreateTime string      `json:"createTime",xorm:"create_time"`
	StartTime  string      `json:"startTime",xorm:"start_time"`
	FinishTime string      `json:"finishTime",xorm:"finish_time"`
	Times      int         `json:"times",xorm:"times"`
	Status     int         `json:"status",xorm:"status"`
	Result     interface{} `json:"result",xorm:"result"`
}

type JobConf struct {
	//Dispatch
	Id                 string `json:"id"`               // 作业唯一id
	JobName            string `json:"job_name"`             //调度作业名
	JobEnable          bool   `json:"job_enable"`           //作业开关, true为打开，false关闭
	JobDispatchCircle  int    `json:"job_dispatch_circle"`  //调度作业周期，单位为毫秒
	JobStep            int    `json:"job_step"`             //调度步长
	SmoothDispatchFlag bool   `json:"smooth_dispatch_flag"` //是否开启平滑调度(一个周期内按任务数切分时间片)
	StartTimeFlag      bool   `json:"start_time_flag"`      //是否开启作业开始时间标识
	StartTimeStr       string `json:"start_time_str"`       //指定作业开始时间(格式：hh:mm:ss)
	//Resource
	ResourceUrl      string `json:"resource_url"`       //上游取资源接口
	RTimeout         int    `json:"r_timeout"`          //取资源接口超时时间, 单位为毫秒
	REnableDowngrade bool   `json:"r_enable_downgrade"` //取资源是否降级
	RDowngradeFile   string `json:"r_downgrade_file"`   //降级资源文件
	//Scheduler
	STimeout    int               `json:"s_timeout"`     //调度下游接口超时时间, 单位为毫秒
	SPath       string            `json:"s_path"`        //调度任务接口路径
	SFixedParam map[string]string `json:"s_fixed_param"` //调度任务接口固定值参数列表
	SKey        []string          `json:"s_key"`         //调度任务接口非固定值参数, 其实只有一个
	DownStream  []string          `json:"down_stream"`   //下游节点集合
}



func (receiver *JobExecuteSnapshot) execute() (result interface{}, err error) {
	//TODO
	return
}

func (receiver *JobExecuteSnapshot) feedbackResult() (err error) {
	//TODO
	return
}
