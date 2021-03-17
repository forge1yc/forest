package app

import (
	"context"
	"github.com/busgo/forest/internal/app/global"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"log"
	"time"
)

type Etcd struct {
	endpoints []string
	client    *clientv3.Client
	kv        clientv3.KV

	timeout time.Duration
}

// create a etcd
func NewEtcd(endpoints []string, timeout time.Duration) (etcd *Etcd, err error) {
	// 这个endpoints 都是127.0.0.1 还不太懂

	var (
		client *clientv3.Client
	)

	conf := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
	}
	if client, err = clientv3.New(conf); err != nil {
		return
	}

	etcd = &Etcd{

		endpoints: endpoints,
		client:    client,
		kv:        clientv3.NewKV(client),
		timeout:   timeout,
	}

	return
}

// get value  from a key
func (etcd *Etcd) Get(key string) (value []byte, err error) {

	var (
		getResponse *clientv3.GetResponse
	)
	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	if getResponse, err = etcd.kv.Get(ctx, key); err != nil {
		return
	}

	if len(getResponse.Kvs) == 0 {
		return
	}

	value = getResponse.Kvs[0].Value

	return

}

// get values from  prefixKey
func (etcd *Etcd) GetWithPrefixKey(prefixKey string) (keys [][]byte, values [][]byte, err error) {

	var (
		getResponse *clientv3.GetResponse
	)
	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	if getResponse, err = etcd.kv.Get(ctx, prefixKey, clientv3.WithPrefix()); err != nil {
		return
	}

	if len(getResponse.Kvs) == 0 {// 完了，这里为啥不是请求很多的机器，
		return
	}

	keys = make([][]byte, 0)
	values = make([][]byte, 0)

	for i := 0; i < len(getResponse.Kvs); i++ {
		keys = append(keys, getResponse.Kvs[i].Key)
		values = append(values, getResponse.Kvs[i].Value)
	}

	return

}

// get values from  prefixKey limit
func (etcd *Etcd) GetWithPrefixKeyLimit(prefixKey string, limit int64) (keys [][]byte, values [][]byte, err error) {

	var (
		getResponse *clientv3.GetResponse
	)
	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	if getResponse, err = etcd.kv.Get(ctx, prefixKey, clientv3.WithPrefix(), clientv3.WithLimit(limit)); err != nil {
		return
	}

	if len(getResponse.Kvs) == 0 {
		return
	}

	keys = make([][]byte, 0)
	values = make([][]byte, 0)

	for i := 0; i < len(getResponse.Kvs); i++ {
		keys = append(keys, getResponse.Kvs[i].Key)
		values = append(values, getResponse.Kvs[i].Value)
	}

	return

}

// put a key
func (etcd *Etcd) Put(key, value string) (err error) {

	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	if _, err = etcd.kv.Put(ctx, key, value); err != nil {
		return
	}

	return
}

// put a key not exist
func (etcd *Etcd) PutNotExist(key, value string) (success bool, oldValue []byte, err error) {

	var (
		txnResponse *clientv3.TxnResponse
	)
	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	txn := etcd.client.Txn(ctx)

	txnResponse, err = txn.If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, value)).
		Else(clientv3.OpGet(key)).
		Commit()

	if err != nil {
		return
	}

	if txnResponse.Succeeded {
		success = true
	} else {
		oldValue = make([]byte, 0)
		oldValue = txnResponse.Responses[0].GetResponseRange().Kvs[0].Value
	}

	return
}

func (etcd *Etcd) Update(key, value, oldValue string) (success bool, err error) {

	var (
		txnResponse *clientv3.TxnResponse
	)

	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	txn := etcd.client.Txn(ctx)

	txnResponse, err = txn.If(clientv3.Compare(clientv3.Value(key), "=", oldValue)).
		Then(clientv3.OpPut(key, value)).
		Commit()

	if err != nil {
		return
	}

	if txnResponse.Succeeded {
		success = true
	}

	return
}

func (etcd *Etcd) Delete(key string) (err error) {

	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	_, err = etcd.kv.Delete(ctx, key)

	return
}

// delete the keys  with prefix key
func (etcd *Etcd) DeleteWithPrefixKey(prefixKey string) (err error) {

	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	_, err = etcd.kv.Delete(ctx, prefixKey, clientv3.WithPrefix())

	return
}

// watch a key
func (etcd *Etcd) Watch(key string) (keyChangeEventResponse *WatchKeyChangeResponse) {

	watcher := clientv3.NewWatcher(etcd.client)
	watchChans := watcher.Watch(context.Background(), key)

	keyChangeEventResponse = &WatchKeyChangeResponse{
		Event:   make(chan *KeyChangeEvent, 250),
		Watcher: watcher,
	}

	go func() {

		for ch := range watchChans { // 这里也是一直监控的，这样一看这个keepAlive 作用是啥,锁吗 // 这里会阻塞，但是时间怎么控制的

			if ch.Canceled {

				goto End
			}
			// 这里循环查看，完了如果没有变动还会写回来，但是这个锁哪里管的，我还不知道
			for _, event := range ch.Events {
				// 这里秒啊，相当于封装了一层，先处理keyChangeEvent，然后判断类型，来定反应，因为有些反应的事件处理是一致的,相当于把事件做了一层映射，这里用map也能解决，不过函数看着更好，方便路径分析
				etcd.handleKeyChangeEvent(event, keyChangeEventResponse.Event)
			}
		}

	End:
		log.Println("the watcher lose for key:", key) // 这个默认就是锁
	}()

	return
}

// watch with prefix key
func (etcd *Etcd) WatchWithPrefixKey(prefixKey string) (keyChangeEventResponse *WatchKeyChangeResponse) {

	watcher := clientv3.NewWatcher(etcd.client)

	watchChans := watcher.Watch(context.Background(), prefixKey, clientv3.WithPrefix())

	keyChangeEventResponse = &WatchKeyChangeResponse{
		Event:   make(chan *KeyChangeEvent, 250),
		Watcher: watcher,
	}

	go func() {

		for ch := range watchChans {

			if ch.Canceled {
				goto End
			}
			for _, event := range ch.Events {
				etcd.handleKeyChangeEvent(event, keyChangeEventResponse.Event)
			}
		}

	End:
		log.Println("the watcher lose for prefixKey:", prefixKey)
	}()

	return // 附带了返回值
}

// handle the key change event
func (etcd *Etcd) handleKeyChangeEvent(event *clientv3.Event, events chan *KeyChangeEvent) { // 所以这个事件应该谁来决定呢

	changeEvent := &KeyChangeEvent{
		Key: string(event.Kv.Key),
	}
	switch event.Type {

	case mvccpb.PUT: // 只有这两种事件
		if event.IsCreate() {
			changeEvent.Type = global.KeyCreateChangeEvent
		} else {
			changeEvent.Type = global.KeyUpdateChangeEvent
		}
		changeEvent.Value = event.Kv.Value
	case mvccpb.DELETE:

		changeEvent.Type = global.KeyDeleteChangeEvent
	}
	events <- changeEvent // 把事件写入

}

// tx含义
func (etcd *Etcd) TxWithTTL(key, value string, ttl int64) (txResponse *TxResponse, err error) { // 好像懂了，有租约别的机器就不能来取值，那么他就是主节点，租约有一个过期时间，所以只要ttl内没有返回信息，租约就过期了，需要一直keepAlive来维持

	var (
		txnResponse *clientv3.TxnResponse
		leaseID     clientv3.LeaseID
		v           []byte
	)
	lease := clientv3.NewLease(etcd.client)

	grantResponse, err := lease.Grant(context.Background(), ttl)
	if grantResponse == nil {
		return
	}

	leaseID = grantResponse.ID

	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	txn := etcd.client.Txn(ctx)
	txnResponse, err = txn.If(
		clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(leaseID))).Commit()

	if err != nil {
		_ = lease.Close()
		return
	}

	txResponse = &TxResponse{
		LeaseID: leaseID,
		Lease:   lease,
	}
	if txnResponse.Succeeded {
		txResponse.Success = true
	} else {
		// close the lease
		_ = lease.Close()
		v, err = etcd.Get(key)
		if err != nil {
			return
		}
		txResponse.Success = false
		txResponse.Key = key
		txResponse.Value = string(v)
	}
	return
}

/*
value:ip
key: leaderPath or register path
 */
func (etcd *Etcd) TxKeepaliveWithTTL(key, value string, ttl int64) (txResponse *TxResponse, err error) { // 这个返回一定会判断状态的

	var (
		txnResponse    *clientv3.TxnResponse
		leaseID        clientv3.LeaseID
		aliveResponses <-chan *clientv3.LeaseKeepAliveResponse
		v              []byte
	)
	lease := clientv3.NewLease(etcd.client) // 建立租约

	grantResponse, err := lease.Grant(context.Background(), ttl)

	leaseID = grantResponse.ID

	if aliveResponses, err = lease.KeepAlive(context.Background(), leaseID); err != nil { // aliveResponses 是一个通道

		return // 不为空直接返回了
	}

	go func() {

		for ch := range aliveResponses {

			if ch == nil {
				goto End // 只有nil 才会到 end，否则一直阻塞，监控这个通道，就能起到连接心跳的作用
			}

		 } // 这个协程就一直开着了，除非有错误发生,有错误发生的话这里的函数也会继续进行下去，这是一个心跳，只是用来维持这个node的，没有了，就是没有了,后序在哪里处理的呢，我还没有找到

	End:
		log.Printf("the tx keepalive has lose key:%s", key)
		// 这里和bus不一样的处理方法是因为 我后面如果注册补上leader节点，就会一直尝试
	}()

	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc() // 这个函数退出就要关闭这个ctx起的协程,为了保险吗

	txn := etcd.client.Txn(ctx)
	txnResponse, err = txn.If( // 这是成功建立心跳连接之后就把自己的值写进去
		clientv3.Compare(clientv3.Version(key), "=", 0)). // 0 才放
		Then(clientv3.OpPut(key, value, clientv3.WithLease(leaseID))). // 0 就更新租约  // 并且记录id // 相当于建立一个长连接，以后每次访问 version 应该都会增加的
		Else( // 如果当前key的版本不为0，那么就进行get操作，每次通信都加1
			clientv3.OpGet(key), // 我只能拿到版本不为0的租约信息吗 ,这里是不是就相当于一次心跳
		).Commit() // 事务的返回信息

	if err != nil {
		_ = lease.Close()
		return
	}

	txResponse = &TxResponse{
		LeaseID: leaseID,
		Lease:   lease,
	}
	if txnResponse.Succeeded { // 这个对应txn // 只有新建立才是这个，否则还是false，感觉设计的并不科学，但是只能这样用了 ,所以后面还要比较id，有可能就是存在的，
		txResponse.Success = true
	} else {
		// close the lease
		_ = lease.Close() // 否则关闭租约，这个时候就不保持了
		txResponse.Success = false
		if v, err = etcd.Get(key); err != nil {
			return
		}
		txResponse.Key = key
		txResponse.Value = string(v)
	}
	return
}

// Transfer from  to with value
func (etcd *Etcd) Transfer(from string, to string, value string) (success bool, err error) {

	var (
		txnResponse *clientv3.TxnResponse
	)

	ctx, cancelFunc := context.WithTimeout(context.Background(), etcd.timeout)
	defer cancelFunc()

	txn := etcd.client.Txn(ctx)

	txnResponse, err = txn.If(
		clientv3.Compare(clientv3.Value(from), "=", value)).
		Then(
			clientv3.OpDelete(from),
			clientv3.OpPut(to, value),
		).Commit()

	if err != nil {
		return
	}

	success = txnResponse.Succeeded

	return

}
