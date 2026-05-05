package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lmittmann/tint"
	"github.com/redis/go-redis/v9"
	redislogging "github.com/redis/go-redis/v9/logging"
	"github.com/sotnii/pakostii"
	"github.com/sotnii/pakostii/spec"
)

const (
	masterName = "mymaster"
	setKey     = "numbers"
)

var redisHosts = []string{"redis1", "redis2", "redis3", "redis4", "redis5"}

func main() {
	redislogging.Disable()

	cluster := spec.NewCluster("redis_lost_writes")

	for i, host := range redisHosts {
		az := "az2"
		if i < 2 {
			az = "az1"
		}

		cluster.AddNode(spec.
			NewNode(host).
			WithAZ(az).
			Runs(
				redisContainer("docker.io/bitnami/redis:latest", host),
				sentinelContainer("docker.io/bitnami/redis-sentinel:latest"),
			))
	}

	test := pakostii.NewTest(
		"redis_lost_writes",
		cluster,
		pakostii.WithLogger(slog.New(tint.NewHandler(os.Stdout, &tint.Options{
			Level: slog.LevelInfo,
		}))),
	)

	test.Run(context.Background(), func(t *pakostii.TestHandle) error {
		t.Logger.Info("waiting for redis and sentinel startup")
		time.Sleep(5 * time.Second)

		forwardedAddrs, err := forwardRedisPorts(t)
		if err != nil {
			return err
		}

		nodeIPs, err := redisNodeIPs(t)
		if err != nil {
			return err
		}
		t.Logger.Info("resolved redis node IPs", "nodes", len(nodeIPs))

		t.Logger.Info("setting up writer clients")
		dialer := forwardedDialer(forwardedAddrs, nodeIPs)
		client1 := newFailoverClient(sentinelAddrs(forwardedAddrs, redisHosts[:2]), dialer)
		defer client1.Close()
		client2 := newFailoverClient(sentinelAddrs(forwardedAddrs, redisHosts[2:]), dialer)
		defer client2.Close()

		writers := newNumberWriters(t.Ctx, setKey, client1, client2)
		writers.Start()

		t.Logger.Info("warming up writes before partition")
		time.Sleep(5 * time.Second)

		isolation := t.Network().Partition().IsolateAZ("redis az1 isolation", "az1").WithNetworkAccess()
		t.Logger.Info("applying redis az1 isolation")
		if err := isolation.Apply(); err != nil {
			writers.Stop()
			return err
		}

		t.Logger.Info("waiting for az2 sentinel failover")
		master, err := waitForAZ2Master(t.Ctx, forwardedAddrs, nodeIPs, t.Logger)
		if err != nil {
			writers.Stop()
			healErr := isolation.Heal()
			return errors.Join(err, healErr)
		}
		t.Logger.Info("az2 elected redis master", "master", master)

		t.Logger.Info("continuing writes during split brain")
		time.Sleep(5 * time.Second)

		t.Logger.Info("healing redis az1 isolation")
		if err := isolation.Heal(); err != nil {
			writers.Stop()
			return err
		}

		t.Logger.Info("waiting after heal before final comparison")
		time.Sleep(5 * time.Second)
		acknowledged := writers.Stop()
		t.Logger.Info("stopped redis writers", "acknowledged", len(acknowledged))

		final, err := readFinalSet(t.Ctx, client2)
		if err != nil {
			return err
		}
		t.Logger.Info("read final redis set", "values", len(final))

		if diff := compareSets(acknowledged, final); diff != "" {
			return fmt.Errorf("acknowledged writes do not match final redis set: %s", diff)
		}
		return nil
	})
}

func forwardRedisPorts(t *pakostii.TestHandle) (map[string]string, error) {
	forwardedAddrs := make(map[string]string, len(redisHosts)*2)

	for _, node := range redisHosts {
		for _, port := range []int{6379, 26379} {
			fw, err := t.Network().ForwardPort(node, port)
			if err != nil {
				return nil, err
			}
			if err := fw.Listen(t.Ctx); err != nil {
				return nil, err
			}

			forwardedAddrs[net.JoinHostPort(node, strconv.Itoa(port))] = net.JoinHostPort("localhost", strconv.Itoa(fw.Port()))
		}
	}

	return forwardedAddrs, nil
}

func sentinelAddrs(forwardedAddrs map[string]string, nodes []string) []string {
	addrs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		addrs = append(addrs, forwardedAddrs[net.JoinHostPort(node, "26379")])
	}
	return addrs
}

func redisNodeIPs(t *pakostii.TestHandle) (map[string]string, error) {
	ips := make(map[string]string, len(redisHosts))
	for _, node := range redisHosts {
		ip := t.Network().IpOf(node)
		if ip == nil {
			return nil, fmt.Errorf("could not resolve IP for %s", node)
		}
		ips[node] = ip.String()
	}
	return ips, nil
}

func forwardedDialer(forwardedAddrs map[string]string, nodeIPs map[string]string) func(context.Context, string, string) (net.Conn, error) {
	addrMap := make(map[string]string, len(forwardedAddrs)*2)
	for target, local := range forwardedAddrs {
		addrMap[target] = local

		host, port, err := net.SplitHostPort(target)
		if err != nil {
			continue
		}
		if ip := nodeIPs[host]; ip != "" {
			addrMap[net.JoinHostPort(ip, port)] = local
		}
	}

	netDialer := &net.Dialer{}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if mapped, ok := addrMap[addr]; ok {
			addr = mapped
		}
		return netDialer.DialContext(ctx, network, addr)
	}
}

func newFailoverClient(sentinelAddrs []string, dialer func(context.Context, string, string) (net.Conn, error)) *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs,
		Dialer:        dialer,
		DialTimeout:   time.Second,
		ReadTimeout:   time.Second,
		WriteTimeout:  time.Second,
		MaxRetries:    -1,
	})
}

func waitForAZ2Master(ctx context.Context, forwardedAddrs map[string]string, nodeIPs map[string]string, logger *slog.Logger) (string, error) {
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	sentinels := sentinelAddrs(forwardedAddrs, redisHosts[2:])
	hostByIP := make(map[string]string, len(nodeIPs))
	for host, ip := range nodeIPs {
		hostByIP[ip] = host
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		for _, addr := range sentinels {
			master, err := currentMaster(deadline, addr)
			if err != nil {
				logger.Debug("failed to query az2 sentinel", "sentinel", addr, "error", err)
				continue
			}

			masterHost := normalizeRedisHost(master, hostByIP)

			if masterHost == "redis3" || masterHost == "redis4" || masterHost == "redis5" {
				return masterHost, nil
			}
			logger.Debug("az2 sentinel has not elected an az2 master", "sentinel", addr, "master", master)
		}

		select {
		case <-deadline.Done():
			return "", fmt.Errorf("timed out waiting for az2 master change: %w", deadline.Err())
		case <-ticker.C:
		}
	}
}

func normalizeRedisHost(host string, hostByIP map[string]string) string {
	if mapped, ok := hostByIP[host]; ok {
		return mapped
	}
	return host
}

func currentMaster(ctx context.Context, sentinelAddr string) (string, error) {
	client := redis.NewSentinelClient(&redis.Options{
		Addr:         sentinelAddr,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		MaxRetries:   -1,
	})
	defer client.Close()

	addr, err := client.GetMasterAddrByName(ctx, masterName).Result()
	if err != nil {
		return "", err
	}
	if len(addr) == 0 {
		return "", fmt.Errorf("sentinel %s returned empty master address", sentinelAddr)
	}
	return addr[0], nil
}

func readFinalSet(ctx context.Context, client *redis.Client) (map[string]struct{}, error) {
	values, err := client.SMembers(ctx, setKey).Result()
	if err != nil {
		return nil, err
	}

	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out, nil
}

func compareSets(acknowledged, final map[string]struct{}) string {
	var missing []string
	for value := range acknowledged {
		if _, ok := final[value]; !ok {
			missing = append(missing, value)
		}
	}

	var extra []string
	for value := range final {
		if _, ok := acknowledged[value]; !ok {
			extra = append(extra, value)
		}
	}

	if len(missing) == 0 && len(extra) == 0 {
		return ""
	}

	var parts []string
	if len(missing) > 0 {
		parts = append(parts, fmt.Sprintf("missing %d acknowledged writes, sample=%v", len(missing), sample(missing, 10)))
	}
	if len(extra) > 0 {
		parts = append(parts, fmt.Sprintf("found %d unexpected values, sample=%v", len(extra), sample(extra, 10)))
	}
	return strings.Join(parts, "; ")
}

func sample(values []string, limit int) []string {
	if len(values) <= limit {
		return values
	}
	return values[:limit]
}
