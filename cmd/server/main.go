package main

import (
	"flag"
	"github.com/busgo/forest/internal/app/autodispatcher"
	"github.com/busgo/forest/internal/app/common/util"
	"github.com/busgo/forest/internal/app/ectd"
	"github.com/busgo/forest/internal/app/global"
	"github.com/prometheus/common/log"
	"strings"
	"time"
)



func main() {

	ip := util.GetLocalIpAddress()
	log.Info("ip ",ip)
	if ip == "" {
		log.Fatal("has no get the ip address")

	}

	endpoints := flag.String("etcd-endpoints", global.DefaultEndpoints, "etcd endpoints")
	httpAddress := flag.String("http-address", global.DefaultHttpAddress, "http address")
	etcdDialTime := flag.Int64("etcd-dailtimeout", global.DefaultDialTimeout, "etcd dailtimeout")
	help := flag.String("help", "", "forest help")
	dbUrl := flag.String("db-url", global.DefaultDbUrl, "db-url for mysql")
	flag.Parse()
	flag.Usage()
	if *help != "" {
		flag.Usage()
		return
	}

	endpoint := strings.Split(*endpoints, ",")
	dialTime := time.Duration(*etcdDialTime) * time.Second

	etcd, err := ectd.NewEtcd(endpoint, dialTime)
	if err != nil {
		log.Fatal(err)
	}

	// 每一个节点具有所有的沟通能力，都被封装好了
	node, err := autodispatcher.NewJobNode(ip, etcd, *httpAddress, *dbUrl)
	if err != nil {

		log.Fatal(err)
	}

	node.Bootstrap()
}
