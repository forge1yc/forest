package global

const (
	JobSnapshotPath       = "/forest/client/snapshot/"
	JobSnapshotGroupPath  = "/forest/client/snapshot/%s/"
	JobClientSnapshotPath = "/forest/client/snapshot/%s/%s/"
)

const (
	DefaultEndpoints   = "10.96.99.96:2379" // etcdServer需要写几个啊，一个不会挂掉吗
	DefaultHttpAddress = ":2856"
	DefaultDialTimeout = 5
	DefaultDbUrl       = "root:123456@tcp(127.0.0.1:3306)/forest?charset=utf8"
)

const (
	GroupConfPath = "/grindstone/server/group/" // 这里一开始谁注册的，注册也是往这里注册  所以在grindstone里面需要使用文件注册，不能使用apollo，不然无法正常使用，或者用开源的配置，比如协程的那个apollo 这个以后再改
	ClientPath    = "/grindstone/worker/%s/workers/" // 这是工作节点的注册路径
)

const (
	JobConfPath = "/forest/server/conf/"
)

const (
	JobNodePath      = "/forest/server/node/" // 这是普通的节点，不过都要注册进去
	JobNodeElectPath = "/forest/server/elect/leader" // 这个是直接写入好的,值是 ip值
	TTL              = 5
)

const (
	KeyCreateChangeEvent = iota
	KeyUpdateChangeEvent
	KeyDeleteChangeEvent
)

const (
	JobCreateChangeEvent = iota
	JobUpdateChangeEvent
	JobDeleteChangeEvent
)

const (
	JobRunningStatus = iota + 1
	JobStopStatus
)

const (
	NodeFollowerState = iota
	NodeLeaderState
)

const (
	JobExecuteSnapshotDoingStatus   = 1
	JobExecuteSnapshotSuccessStatus = 2
	JobExecuteSnapshotUnkonwStatus  = 3
	JobExecuteSnapshotErrorStatus   = -1
)

const ClientPathPrefix = "/forest/client/%s/clients/%s"
const (
	URegistryState = iota
	RegistryState
)

const JobSnapshotPrefix = "/forest/client/snapshot/%s/%s/"
const JobExecuteSnapshotPrefix = "/forest/client/execute/snapshot/%s/%s/"

const (
	// 执行中
	JobExecuteDoingStatus = 1
	// 执行成功
	JobExecuteSuccessStatus = 2
	// 未知
	JobExecuteUkonwStatus = 3
	// 执行失败
	JobExecuteErrorStatus = -1
)