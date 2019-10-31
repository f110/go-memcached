package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/f110/go-memcached/client"
	"github.com/f110/go-memcached/cluster"
	"github.com/f110/go-memcached/proxy"
	"github.com/f110/go-memcached/server"
	"sigs.k8s.io/yaml"
)

type Server struct {
	Name  string `json:"name"`
	Host  string `json:"host"`
	Port  int    `json:"port"`
	Phase string `json:"phase"`
}

type Config struct {
	Primary   []Server `json:"primary"`
	Secondary []Server `json:"secondary"`
}

func connectServers(servers []Server) []*client.ServerWithMetaProtocol {
	res := make([]*client.ServerWithMetaProtocol, len(servers))
	for i := range servers {
		state := client.ServerStateNormal
		switch servers[i].Phase {
		case "delete_only":
			state = client.ServerStateDeleteOnly
		case "write_only":
			state = client.ServerStateWriteOnly
		}
		s, err := client.NewServerWithMetaProtocol(
			context.Background(),
			servers[i].Name,
			"tcp",
			fmt.Sprintf("%s:%d", servers[i].Host, servers[i].Port),
			state,
		)
		if err != nil {
			panic(err)
		}
		res[i] = s
	}

	return res
}

func main() {
	configFile := ""
	flag.StringVar(&configFile, "conf", configFile, "Config file path")
	flag.Parse()

	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		panic(err)
	}
	conf := &Config{}
	if err := yaml.Unmarshal(b, conf); err != nil {
		panic(err)
	}

	pool := cluster.NewReplicaPool(connectServers(conf.Primary), connectServers(conf.Secondary))
	handler := proxy.NewReplicaProxy(pool)
	serv := &server.Server{
		Addr:    ":8090",
		Handler: handler,
	}

	log.Fatal(serv.ListenAndServe())
}
