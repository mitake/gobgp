package config

import (
	"io/ioutil"
	"os"
	"path/filepath"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/viper"
)

func WatchEtcd(etcdEndpoints string, etcdKey string, configCh chan BgpConfigSet) {
	log.Info("watching key: ", etcdKey)
	client, err := clientv3.NewFromURL(etcdEndpoints)
	if err != nil {
		log.Fatal("failed to create etcd client instance: %s", err)
		os.Exit(1)
	}

	watcher := clientv3.NewWatcher(client)
	watchRsp := watcher.Watch(context.TODO(), etcdKey, 1)

	cnt := 0

	for {
		rsp := <-watchRsp

		key := string(rsp.Events[0].Kv.Key)
		value := rsp.Events[0].Kv.Value
		log.Info("watched event from key: ", key)

		dir, derr := ioutil.TempDir("/tmp", "gobgp-")
		if derr != nil {
			log.Fatal("failed to create a temporal directory: ", derr)
			os.Exit(1)
		}

		tmpPath := dir + "/" + key
		werr := ioutil.WriteFile(tmpPath, value, 0600)
		if werr != nil {
			log.Fatal("failed to write a config file: ", werr)
			os.Exit(1)
		}

		b := Bgp{}
		p := RoutingPolicy{}
		v := viper.New()
		v.SetConfigFile(tmpPath)
		v.SetConfigType(filepath.Ext(tmpPath)[1:])
		err := v.ReadInConfig()
		if err != nil {
			log.Fatal("ReadInConfig() failed: ", err)
			os.Exit(1)
		}
		err = v.Unmarshal(&b)
		if err != nil {
			log.Fatal("Unmarshal() failed: ", err)
			os.Exit(1)
		}
		err = SetDefaultConfigValues(v, &b)
		if err != nil {
			log.Fatal("SetDefaultConfigValues() failed: ", err)
			os.Exit(1)
		}
		err = v.Unmarshal(&p)
		if err != nil {
			log.Fatal("Unmarshal() failed: ", err)
			os.Exit(1)
		}

		if cnt == 0 {
			log.Info("finished reading config from etcd")
		}
		cnt++
		configCh <- BgpConfigSet{Bgp: b, Policy: p}

		err = os.RemoveAll(dir)
		if err != nil {
			log.Fatal("failed to remove temporal directory: ", err)
			os.Exit(1)
		}
	}
}
