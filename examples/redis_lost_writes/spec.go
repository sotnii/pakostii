package main

import (
	"time"

	"github.com/sotnii/pakostii/spec"
)

func redisContainer(image, host string) spec.ContainerSpec {
	redis := spec.NewContainer("redis", image).
		WithEnv("ALLOW_EMPTY_PASSWORD", "yes").
		WithEnv("REDIS_AOF_ENABLED", "no")

	if host == "redis1" {
		return redis.WithEnv("REDIS_REPLICATION_MODE", "master")
	}

	return redis.
		WithEnv("REDIS_REPLICATION_MODE", "slave").
		WithEnv("REDIS_MASTER_HOST", "redis1").
		WithEnv("REDIS_MASTER_PORT_NUMBER", "6379")
}

func sentinelContainer(image string) spec.ContainerSpec {
	return spec.NewContainer("sentinel", image).
		WithStartDelay(2*time.Second).
		WithEnv("ALLOW_EMPTY_PASSWORD", "yes").
		WithEnv("REDIS_MASTER_HOST", "redis1").
		WithEnv("REDIS_MASTER_PORT_NUMBER", "6379").
		WithEnv("REDIS_SENTINEL_MASTER_NAME", masterName).
		WithEnv("REDIS_SENTINEL_QUORUM", "3").
		WithEnv("REDIS_SENTINEL_DOWN_AFTER_MILLISECONDS", "1000").
		WithEnv("REDIS_SENTINEL_FAILOVER_TIMEOUT", "5000")
}
