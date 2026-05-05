package managers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/sotnii/pakostii/internal/containers"
	"github.com/sotnii/pakostii/internal/network"
	"github.com/sotnii/pakostii/internal/runtime/util"
	"github.com/sotnii/pakostii/spec"
)

type NodeNetworkResolver interface {
	NodeIPs() map[spec.NodeID]net.IP
	HasNamespace(id spec.NodeID) bool
	GetNamespace(id spec.NodeID) network.Namespace
}

type ContainerManager struct {
	mu         sync.RWMutex
	containers map[spec.NodeID][]containers.RunningContainer
	runtime    containers.ContainerRuntimeManager
	logger     *slog.Logger
	status     containerManagerStatus

	// launchWG tracks all launch goroutines for this manager instance.
	// The manager is single-use: one Prepare, then one Teardown.
	launchWG sync.WaitGroup
}

type containerManagerStatus uint8

const (
	containerManagerStatusReady containerManagerStatus = iota
	containerManagerStatusPreparing
	containerManagerStatusPrepared
	containerManagerStatusClosing
	containerManagerStatusClosed
)

func (p *ContainerManager) FindContainer(id spec.NodeID, containerName string) *containers.RunningContainer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	nodeContainers, ok := p.containers[id]
	if !ok {
		return nil
	}
	for _, container := range nodeContainers {
		if containerName == container.Name {
			return &container
		}
	}
	return nil
}

func (p *ContainerManager) ExecInContainer(ctx context.Context, containerID string, argv []string) (*containers.ExecResult, error) {
	return p.runtime.ExecInContainer(ctx, containerID, argv)
}

func NewContainerManager(containerd containers.ContainerRuntimeManager, logger *slog.Logger) ContainerManager {
	return ContainerManager{
		runtime:    containerd,
		logger:     logger.With("component", "container_manager"),
		containers: map[spec.NodeID][]containers.RunningContainer{},
	}
}

func (p *ContainerManager) ContainersOf(id spec.NodeID) []containers.RunningContainer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := append([]containers.RunningContainer(nil), p.containers[id]...)
	return out
}

func (p *ContainerManager) Prepare(ctx context.Context, clusterSpec spec.ClusterSpec, testId string, nodeNet NodeNetworkResolver) error {
	p.mu.Lock()
	if p.status != containerManagerStatusReady {
		p.mu.Unlock()
		return fmt.Errorf("container manager prepare not allowed in status %s", p.status)
	}
	p.status = containerManagerStatusPreparing
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		if p.status == containerManagerStatusPreparing {
			p.status = containerManagerStatusPrepared
		}
		p.mu.Unlock()
	}()

	p.logger.Info("preparing containers")

	hostsFile, err := p.buildHostsFile(nodeNet.NodeIPs())
	if err != nil {
		return err
	}

	var (
		errMu   sync.Mutex
		runErrs []error
	)

	for _, nodeSpec := range clusterSpec.Nodes {
		if !nodeNet.HasNamespace(nodeSpec.ID) {
			return fmt.Errorf("could not find network namespace for node %s", nodeSpec.ID)
		}
		netns := nodeNet.GetNamespace(nodeSpec.ID)
		for _, containerSpec := range nodeSpec.Containers {
			p.launchWG.Add(1)
			go func(nodeSpec spec.NodeSpec, containerSpec spec.ContainerSpec, netns network.Namespace) {
				defer p.launchWG.Done()

				request := containers.LaunchRequest{
					ID:          fmt.Sprintf("pkst-%s-%s-%s-%s", testId, nodeSpec.ID, containerSpec.Name, util.NewResourceID()),
					Name:        containerSpec.Name,
					NodeID:      string(nodeSpec.ID),
					ImageRef:    containerSpec.ImageRef,
					NetNSPath:   netns.Path,
					Hostname:    string(nodeSpec.ID),
					Env:         util.CopyMap(containerSpec.Env),
					Files:       util.MergeMaps(containerSpec.Files, map[string]string{"/etc/hosts": hostsFile}),
					StartDelay:  containerSpec.StartDelay,
					Readiness:   containerSpec.ReadinessProbe,
					NamespaceIP: netns.AllocatedIP.String(),
				}
				p.logger.Debug("launching container", "node", nodeSpec.ID, "service", containerSpec.Name, "container_id", request.ID, "image", request.ImageRef, "netns", request.NetNSPath, "start_delay", request.StartDelay)

				running, err := p.runtime.RunContainer(ctx, request)
				if err != nil {
					p.logger.Error("container launch failed", "node", nodeSpec.ID, "service", containerSpec.Name, "container_id", request.ID, "error", err)
					errMu.Lock()
					runErrs = append(runErrs, err)
					errMu.Unlock()
					return
				}

				p.mu.Lock()
				p.containers[nodeSpec.ID] = append(p.containers[nodeSpec.ID], *running)
				p.mu.Unlock()
				p.logger.Info("container running", "node", nodeSpec.ID, "container_id", running.ID, "name", running.Name)
			}(nodeSpec, containerSpec, netns)
		}
	}

	p.launchWG.Wait()
	return errors.Join(runErrs...)
}

func (p *ContainerManager) Teardown(ctx context.Context) error {
	p.mu.Lock()
	switch p.status {
	case containerManagerStatusClosing, containerManagerStatusClosed:
		p.mu.Unlock()
		return nil
	default:
		p.status = containerManagerStatusClosing
	}
	p.mu.Unlock()

	p.launchWG.Wait()

	p.mu.Lock()
	defer func() {
		p.status = containerManagerStatusClosed
		p.mu.Unlock()
	}()
	var errs []error

	for nodeId, runningContainers := range p.containers {
		for _, container := range runningContainers {
			p.logger.Debug("teardown container", "node", nodeId, "container", container.ID)
			err := p.runtime.TeardownContainer(ctx, container)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	clear(p.containers)

	return errors.Join(errs...)
}

func (p *ContainerManager) buildHostsFile(nodeIPs map[spec.NodeID]net.IP) (string, error) {
	lines := []string{
		"127.0.0.1 localhost",
		"::1 localhost ip6-localhost ip6-loopback",
	}
	for nodeId, ip := range nodeIPs {
		p.logger.Debug("adding hosts entry", "node", nodeId, "ip", ip)
		lines = append(lines, fmt.Sprintf("%s %s", ip.String(), nodeId))
	}
	return strings.Join(lines, "\n"), nil
}

func (p *ContainerManager) Close() error {
	return p.runtime.Close()
}

func (p *ContainerManager) ObserveEvents(ctx context.Context) (<-chan containers.Event, <-chan error, error) {
	return p.runtime.ObserveEvents(ctx)
}

func (s containerManagerStatus) String() string {
	switch s {
	case containerManagerStatusReady:
		return "ready"
	case containerManagerStatusPreparing:
		return "preparing"
	case containerManagerStatusPrepared:
		return "prepared"
	case containerManagerStatusClosing:
		return "closing"
	case containerManagerStatusClosed:
		return "closed"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}
