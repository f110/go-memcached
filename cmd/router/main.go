package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v3"

	"go.f110.dev/go-memcached/client"
	"go.f110.dev/go-memcached/cluster"
	"go.f110.dev/go-memcached/proxy"
	"go.f110.dev/go-memcached/server"
)

type Server struct {
	Name string `yaml:"name"`
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type Config struct {
	Primary   []Server `yaml:"primary"`
	Secondary []Server `yaml:"secondary"`
}

func connectServers(servers []Server) []*client.ServerWithMetaProtocol {
	res := make([]*client.ServerWithMetaProtocol, len(servers))
	for i := range servers {
		s, err := client.NewServerWithMetaProtocol(context.Background(), servers[i].Name, "tcp", fmt.Sprintf("%s:%d", servers[i].Host, servers[i].Port))
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
