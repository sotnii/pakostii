package api

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sotnii/pakostii/internal/containers"
	"github.com/sotnii/pakostii/spec"
)

type ContainerManager interface {
	FindContainer(id spec.NodeID, containerName string) *containers.RunningContainer
	ExecInContainer(ctx context.Context, containerID string, argv []string) (*containers.ExecResult, error)
}

type Exec struct {
	ctx        context.Context
	containers ContainerManager
	logger     *slog.Logger
}

func NewExec(ctx context.Context, containers ContainerManager, logger *slog.Logger) *Exec {
	return &Exec{ctx: ctx, containers: containers, logger: logger}
}

type ExecResult struct {
	ExitCode uint32
	Stdout   string
	Stderr   string
}

// InContainer executes a command in a container on a specific node
func (e *Exec) InContainer(nodeID, containerName string, argv ...string) (*ExecResult, error) {
	e.logger.Debug("exec requested", "node", nodeID, "service", containerName, "argv", argv)
	container := e.containers.FindContainer(spec.NodeID(nodeID), containerName)
	if container == nil {
		return nil, fmt.Errorf("container %q on node %q not found", containerName, nodeID)
	}
	res, err := e.containers.ExecInContainer(e.ctx, container.ID, argv)
	if err != nil {
		e.logger.Error("exec failed", "node", nodeID, "service", containerName, "container_id", container.ID, "error", err)
		return nil, err
	}
	e.logger.Debug("exec finished", "node", nodeID, "service", containerName, "container_id", container.ID, "exit_code", res.ExitCode, "stdout_len", len(res.Stdout), "stderr_len", len(res.Stderr))
	return convertContainersExecResult(*res), nil
}

func convertContainersExecResult(r containers.ExecResult) *ExecResult {
	return &ExecResult{
		ExitCode: r.ExitCode,
		Stdout:   r.Stdout,
		Stderr:   r.Stderr,
	}
}
