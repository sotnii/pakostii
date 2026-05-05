package pakostii

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/sotnii/pakostii/logging"
)

type Test struct {
	name        string
	clusterSpec ClusterSpec
	logger      *slog.Logger
	runtime     RuntimeFactory
}

func NewTest(name string, cluster *ClusterSpec, opts ...TestOption) *Test {
	if cluster == nil {
		panic("cluster spec cannot be nil")
	}
	t := &Test{
		name:        name,
		clusterSpec: *cluster,
		logger:      logging.NewDefaultLogger(),
		runtime:     NewTestRuntime,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *Test) Run(ctx context.Context, fn func(*TestHandle) error) {
	rt, err := t.runtime(t.name, t.clusterSpec, t.logger)
	if err != nil {
		t.logger.Error("error creating runtime", "error", err)
		panic(err)
	}
	result := rt.Run(ctx, fn)
	if result.Failed() {
		t.logger.Error("test failed", "error", result.ErrorMessage)
		if result.StackTrace != "" {
			fmt.Fprintln(os.Stderr, prettyStackTrace(result.StackTrace))
		}
		os.Exit(1)
	}
	t.logger.Info("test passed")
}

func prettyStackTrace(stackTrace string) string {
	lines := strings.Split(strings.TrimSpace(stackTrace), "\n")
	if len(lines) == 0 || lines[0] == "" {
		return ""
	}

	var b strings.Builder
	b.WriteString("panic stack trace:\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		b.WriteString("  ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}
