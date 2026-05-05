package main

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"github.com/sotnii/pakostii"
	"github.com/sotnii/pakostii/spec"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	cluster := spec.NewCluster("etcd_stale_read")
	etcdHosts := []string{"etcd1", "etcd2", "etcd3"}

	for i, host := range etcdHosts {
		az := 1
		if i > 0 {
			az = 2
		}
		etcd := spec.Etcd(
			"etcd",
			// This image is based on quay.io/coreos/etcd:v3.3.0, but converted to a
			// newer image format using skopeo
			"ghcr.io/sotnii/etcd:3.3.0",
			spec.EtcdConfig{
				Name:         host,
				RunsOnHost:   etcdHosts[i],
				ClusterHosts: etcdHosts,
			},
		)
		cluster.AddNode(spec.
			NewNode(string(etcdHosts[i])).
			WithAZ(fmt.Sprintf("az%d", az)).
			Runs(etcd),
		)
	}

	test := pakostii.NewTest(
		"etcd_stale_read",
		cluster,
		pakostii.WithLogger(slog.New(tint.NewHandler(os.Stdout, &tint.Options{
			Level: slog.LevelInfo,
		}))),
	)

	testKey := "name"
	test.Run(context.Background(), func(t *pakostii.TestHandle) error {
		// TODO:
		// 	1. Move leader to etcd1
		// 	2. Set some value to "old"

		etcdEndpoints := make([]string, 0, len(etcdHosts))
		for _, host := range etcdHosts {
			fw, err := t.Network().ForwardPort(host, 2379)
			if err != nil {
				return err
			}
			err = fw.Listen(t.Ctx)
			if err != nil {
				return err
			}
			defer fw.Close()

			etcdEndpoints = append(etcdEndpoints, fmt.Sprintf("http://localhost:%d", fw.Port()))
		}

		etcdClient, err := createEtcdClient(etcdEndpoints)
		if err != nil {
			return err
		}
		defer etcdClient.Close()

		clientA, err := createEtcdClient(etcdEndpoints[:1])
		if err != nil {
			return err
		}
		defer clientA.Close()
		clientB, err := createEtcdClient(etcdEndpoints[1:])
		if err != nil {
			return err
		}
		defer clientB.Close()

		// Prepare the cluster: move leader to etcd1 and set test key to "old"
		err = moveEtcdLeader(t.Ctx, etcdEndpoints, etcdEndpoints[0], t.Logger)
		if err != nil {
			return err
		}
		_, err = etcdClient.Put(t.Ctx, testKey, "old")
		if err != nil {
			return err
		}

		// Create the isolation on az1, which contains etcd1
		isolation := t.Network().Partition().IsolateAZ("leader isolation", "az1").WithNetworkAccess()
		err = isolation.Apply()
		if err != nil {
			return err
		}

		time.Sleep(time.Second * 5)
		t.Logger.Info("updating test key to new value while leader is isolated")
		// Immediately update the key on etcd2 while the minority with the leader is isolated
		_, err = clientB.Put(t.Ctx, testKey, "new")
		if err != nil {
			return fmt.Errorf("failed to update key on the majority side: %w", err)
		}
		t.Logger.Info("updated test key to new value")

		getResp, err := clientA.Get(t.Ctx, testKey, clientv3.WithSerializable())
		if err == nil {
			val := string(getResp.Kvs[0].Value)
			if val != "new" {
				return fmt.Errorf("expected value 'new', got '%s'", val)
			}
		}

		return nil
	})
}

func moveEtcdLeader(ctx context.Context, endpoints []string, targetEndpoint string, logger *slog.Logger) error {
	statusClient, err := createEtcdClient(endpoints)
	if err != nil {
		return err
	}
	defer statusClient.Close()

	memberEndpointByID := make(map[uint64]string, len(endpoints))
	var leaderID uint64
	var targetID uint64

	for _, endpoint := range endpoints {
		resp, err := statusClient.Status(ctx, endpoint)
		if err != nil {
			return fmt.Errorf("get status for %s: %w", endpoint, err)
		}
		logger.Debug("got status", "endpoint", endpoint, "status", resp)

		memberID := resp.Header.MemberId
		memberEndpointByID[memberID] = endpoint
		leaderID = resp.Leader
		if endpoint == targetEndpoint {
			targetID = memberID
		}
	}

	if targetID == 0 {
		return fmt.Errorf("target endpoint %s was not found in etcd status responses", targetEndpoint)
	}
	if leaderID == 0 {
		return fmt.Errorf("etcd leader was not reported by status responses")
	}
	if leaderID == targetID {
		logger.Info("etcd leader is already on target", "leader", leaderID, "endpoint", targetEndpoint)
		return nil
	}

	leaderEndpoint, ok := memberEndpointByID[leaderID]
	if !ok {
		return fmt.Errorf("leader member %d was not found in etcd status responses", leaderID)
	}

	leaderClient, err := createEtcdClient([]string{leaderEndpoint})
	if err != nil {
		return err
	}
	defer leaderClient.Close()

	logger.Info("moving etcd leader", "from", leaderID, "from_endpoint", leaderEndpoint, "to", targetID, "to_endpoint", targetEndpoint)
	_, err = leaderClient.MoveLeader(ctx, targetID)
	if err != nil {
		return fmt.Errorf("move etcd leader from %s to %s: %w", leaderEndpoint, targetEndpoint, err)
	}
	return nil
}

func createEtcdClient(endpoints []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
}
