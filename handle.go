package pakostii

import (
	"context"
	"log/slog"

	"github.com/sotnii/pakostii/internal/runtime/agent"
	"github.com/sotnii/pakostii/internal/runtime/api"
	"github.com/sotnii/pakostii/internal/runtime/api/network"
	"github.com/sotnii/pakostii/internal/runtime/managers"
	"github.com/sotnii/pakostii/spec"
)

type TestHandle struct {
	Ctx     context.Context
	Logger  *slog.Logger
	exec    *api.Exec
	http    *api.Http
	network *network.Network
}

func newTestHandle(
	ctx context.Context,
	spec spec.ClusterSpec,
	containers *managers.ContainerManager,
	netManager *managers.NetworkManager,
	// TODO: Httpanget can be built from execAgent, so passing both is redundant.
	httpAgent agent.HttpAgent,
	execAgent agent.ClusterNetworkExecAgent,
	logger *slog.Logger,
) *TestHandle {
	return &TestHandle{
		Ctx:     ctx,
		Logger:  logger,
		exec:    api.NewExec(ctx, containers, logger.With("component", "exec_api")),
		http:    api.NewHttp(httpAgent),
		network: network.NewNetwork(spec, netManager, execAgent, logger.With("component", "network_api")),
	}
}

func (t *TestHandle) Cleanup() error {
	if t == nil || t.network == nil {
		return nil
	}
	return t.network.Cleanup()
}

func (t *TestHandle) Exec() *api.Exec {
	return t.exec
}

func (t *TestHandle) Http() *api.Http {
	return t.http
}

func (t *TestHandle) Network() *network.Network {
	return t.network
}
