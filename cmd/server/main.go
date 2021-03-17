package main

import (
	"flag"
	"github.com/busgo/forest/internal/app"
	"github.com/prometheus/common/log"
	"strings"
	"time"
)

const (
	DefaultEndpoints   = "10.96.99.96:2379"
	DefaultHttpAddress = ":2856"
	DefaultDialTimeout = 5
	DefaultDbUrl       = "root:123456@tcp(127.0.0.1:3306)/forest?charset=utf8"
)

func main() {

	ip := app.GetLocalIpAddress()
	log.Info("ip ",ip)
	if ip == "" {
		log.Fatal("has no get the ip address")

	}

	endpoints := flag.String("etcd-endpoints", DefaultEndpoints, "etcd endpoints")
	httpAddress := flag.String("http-address", DefaultHttpAddress, "http address")
	etcdDialTime := flag.Int64("etcd-dailtimeout", DefaultDialTimeout, "etcd dailtimeout")
	help := flag.String("help", "", "forest help")
	dbUrl := flag.String("db-url", DefaultDbUrl, "db-url for mysql")
	flag.Parse()
	flag.Usage()
	if *help != "" {
		flag.Usage()
		return
	}

	endpoint := strings.Split(*endpoints, ",")
	dialTime := time.Duration(*etcdDialTime) * time.Second

	etcd, err := app.NewEtcd(endpoint, dialTime)
	if err != nil {
		log.Fatal(err)
	}

	// 每一个节点具有所有的沟通能力，都被封装好了
	node, err := app.NewJobNode(ip, etcd, *httpAddress, *dbUrl)
	if err != nil {

		log.Fatal(err)
	}

	node.Bootstrap()
}
