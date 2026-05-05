package spec

import (
	"fmt"
	"strings"
	"time"
)

type PatroniConfig struct {
	Name        string
	NodeAddress NodeID
	EtcdHosts   []NodeID
}

func Patroni(name, image string, cfg PatroniConfig, configTemplate string) ContainerSpec {
	hosts := make([]string, 0, len(cfg.EtcdHosts))
	for _, host := range cfg.EtcdHosts {
		hosts = append(hosts, fmt.Sprintf("%s:2379", host))
	}

	return NewContainer(name, image).WithFile(
		"/etc/patroni/patroni.yml",
		strings.NewReplacer(
			"$CLUSTER_NAME", cfg.Name,
			"$NODE_ADDRESS", string(cfg.NodeAddress),
			"$ETCD3_HOSTS", fmt.Sprintf("[%s]", strings.Join(hosts, ",")),
		).Replace(configTemplate),
	)
}

type EtcdConfig struct {
	Name         string
	RunsOnHost   string
	ClusterHosts []string
}

func Etcd(name, image string, cfg EtcdConfig) ContainerSpec {
	cluster := make([]string, 0, len(cfg.ClusterHosts))
	for _, host := range cfg.ClusterHosts {
		cluster = append(cluster, fmt.Sprintf("%s=http://%s:2380", host, host))
	}

	return NewContainer(name, image).
		WithEnv("ETCD_NAME", cfg.Name).
		WithEnv("ETCD_INITIAL_CLUSTER", strings.Join(cluster, ",")).
		WithEnv("ETCD_INITIAL_CLUSTER_STATE", "new").
		WithEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380").
		WithEnv("ETCD_INITIAL_ADVERTISE_PEER_URLS", fmt.Sprintf("http://%s:2380", cfg.RunsOnHost)).
		WithEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379").
		WithEnv("ETCD_ADVERTISE_CLIENT_URLS", fmt.Sprintf("http://%s:2379", cfg.RunsOnHost)).
		WithReadiness(HTTPProbe{
			Method:  HTTPMethodGet,
			Path:    "/readyz",
			Port:    2379,
			Code:    200,
			Timeout: 5 * time.Second,
		})
}
