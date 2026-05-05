package main

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type numberWriters struct {
	ctx    context.Context
	cancel context.CancelFunc
	key    string

	clients []*redis.Client

	next uint64
	wg   sync.WaitGroup

	mu           sync.Mutex
	acknowledged map[string]struct{}
}

func newNumberWriters(ctx context.Context, key string, clients ...*redis.Client) *numberWriters {
	writerCtx, cancel := context.WithCancel(ctx)
	return &numberWriters{
		ctx:          writerCtx,
		cancel:       cancel,
		key:          key,
		clients:      clients,
		acknowledged: make(map[string]struct{}),
	}
}

func (w *numberWriters) Start() {
	ticker := time.NewTicker(20 * time.Millisecond)

	for _, client := range w.clients {
		w.wg.Add(1)
		go func(client *redis.Client) {
			defer w.wg.Done()
			for {
				select {
				case <-w.ctx.Done():
					return
				case <-ticker.C:
					n := atomic.AddUint64(&w.next, 1)
					value := strconv.FormatUint(n, 10)
					if err := client.SAdd(w.ctx, w.key, value).Err(); err == nil {
						w.mu.Lock()
						w.acknowledged[value] = struct{}{}
						w.mu.Unlock()
					}
				}
			}
		}(client)
	}

	go func() {
		<-w.ctx.Done()
		ticker.Stop()
	}()
}

func (w *numberWriters) Stop() map[string]struct{} {
	w.cancel()
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	out := make(map[string]struct{}, len(w.acknowledged))
	for value := range w.acknowledged {
		out[value] = struct{}{}
	}
	return out
}
