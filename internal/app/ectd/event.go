package ectd

import (
	"context"
	"github.com/coreos/etcd/clientv3"
)

// 监听key 变化响应
type WatchKeyChangeResponse struct {
	Event      chan *KeyChangeEvent // 这个很重要，触发各种事件
	CancelFunc context.CancelFunc
	Watcher    clientv3.Watcher
}

// key 变化事件
type KeyChangeEvent struct {
	Type  int
	Key   string
	Value []byte
}

type TxResponse struct {
	Success bool
	LeaseID clientv3.LeaseID
	Lease   clientv3.Lease
	Key     string
	Value   string
	StateChan chan bool
}

//type WorkerTxResponse struct {
//	Success   bool
//	LeaseID   clientv3.LeaseID
//	Lease     clientv3.Lease
//	Key       string
//	Value     string
//	StateChan chan bool
//}