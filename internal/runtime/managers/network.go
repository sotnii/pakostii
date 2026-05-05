package managers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/sotnii/pakostii/internal/network"
	"github.com/sotnii/pakostii/internal/runtime/util"
	"github.com/sotnii/pakostii/spec"
)

type NetworkManager struct {
	nsMgr          *network.NamespaceManager
	status         networkManagerStatus
	namespaces     map[spec.NodeID]*network.Namespace
	agentNamespace *network.Namespace
	logger         *slog.Logger
}

type networkManagerStatus uint8

const (
	networkManagerStatusReady networkManagerStatus = iota
	networkManagerStatusPreparing
	networkManagerStatusPrepared
	networkManagerStatusClosing
	networkManagerStatusClosed
)

func (m *NetworkManager) GetAgentNamespace() network.Namespace {
	if m.agentNamespace != nil {
		return *m.agentNamespace
	}
	panic("agent namespace is not initialized")
}

func (m *NetworkManager) HasNamespace(id spec.NodeID) bool {
	_, ok := m.namespaces[id]
	return ok
}

func (m *NetworkManager) GetNamespace(id spec.NodeID) network.Namespace {
	if _, ok := m.namespaces[id]; ok {
		return *m.namespaces[id]
	}

	panic("namespace " + id + " not found")
}

func NewNetworkManager(nsMgr *network.NamespaceManager, logger *slog.Logger) NetworkManager {
	return NetworkManager{
		nsMgr:      nsMgr,
		logger:     logger.With("component", "network_manager"),
		namespaces: map[spec.NodeID]*network.Namespace{},
	}
}

func (m *NetworkManager) AgentNamespace() *network.Namespace {
	return m.agentNamespace
}

func (m *NetworkManager) NodeIPs() map[spec.NodeID]net.IP {
	ips := make(map[spec.NodeID]net.IP)
	for id, ns := range m.namespaces {
		ips[id] = ns.AllocatedIP
	}
	return ips
}

func (m *NetworkManager) Prepare(ctx context.Context, spec spec.ClusterSpec) error {
	if m.status != networkManagerStatusReady {
		return fmt.Errorf("network manager prepare not allowed in status %s", m.status)
	}
	m.status = networkManagerStatusPreparing

	m.logger.Info("preparing network")
	if err := m.nsMgr.SetupBridge(ctx); err != nil {
		return err
	}

	agentResourceId := util.NewResourceID()
	agentNS, err := m.nsMgr.CreateNamespace(ctx, agentResourceId, "agent-"+agentResourceId)
	if err != nil {
		return err
	}
	m.agentNamespace = agentNS
	if err := m.nsMgr.SetupNamespace(ctx, agentNS); err != nil {
		return err
	}
	m.logger.Debug("agent namespace ready", "namespace", agentNS.Name, "path", agentNS.Path)

	for _, nodeSpec := range spec.Nodes {
		resourceId := util.NewResourceID()
		m.logger.Debug("preparing node namespace", "node", nodeSpec.ID, "resource_id", resourceId)
		ns, err := m.nsMgr.CreateNamespace(ctx, resourceId, fmt.Sprintf("node-%s", resourceId))
		if err != nil {
			return err
		}
		m.namespaces[nodeSpec.ID] = ns
		if err := m.nsMgr.SetupNamespace(ctx, ns); err != nil {
			return err
		}
		m.logger.Info(fmt.Sprintf("network ready for node %s", nodeSpec.ID), "namespace", ns.Name, "ip", ns.AllocatedIP)
	}

	m.status = networkManagerStatusPrepared
	return nil
}

func (m *NetworkManager) Teardown(ctx context.Context) error {
	switch m.status {
	case networkManagerStatusClosing, networkManagerStatusClosed:
		return nil
	default:
		m.status = networkManagerStatusClosing
	}

	var errs []error
	err := m.nsMgr.TeardownNamespace(ctx, m.agentNamespace)
	if err != nil {
		errs = append(errs, err)
	}

	m.agentNamespace = nil

	for _, ns := range m.namespaces {
		err := m.nsMgr.TeardownNamespace(ctx, ns)
		if err != nil {
			errs = append(errs, err)
		}
	}

	clear(m.namespaces)
	m.status = networkManagerStatusClosed

	return errors.Join(errs...)
}

func (s networkManagerStatus) String() string {
	switch s {
	case networkManagerStatusReady:
		return "ready"
	case networkManagerStatusPreparing:
		return "preparing"
	case networkManagerStatusPrepared:
		return "prepared"
	case networkManagerStatusClosing:
		return "closing"
	case networkManagerStatusClosed:
		return "closed"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}
