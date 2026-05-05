package network

import (
	"errors"
	"log/slog"
	"net"

	"github.com/sotnii/pakostii/internal/network"
	"github.com/sotnii/pakostii/internal/runtime/agent"
	"github.com/sotnii/pakostii/spec"
)

var (
	ErrNodeNetworkMissing = errors.New("node network is missing")
)

type NodeNetworkResolver interface {
	HasNamespace(id spec.NodeID) bool
	GetNamespace(id spec.NodeID) network.Namespace
	GetAgentNamespace() network.Namespace
}

type Network struct {
	partition *Partition
	portFw    *PortForward
}

func NewNetwork(spec spec.ClusterSpec, networkResolver NodeNetworkResolver, exec agent.ClusterNetworkExecAgent, logger *slog.Logger) *Network {
	return &Network{
		partition: &Partition{
			logger:          logger,
			spec:            spec,
			networkResolver: networkResolver,
			handles:         make([]*AZIsolationHandle, 0),
		},
		portFw: &PortForward{
			exec: exec,
		},
	}
}

func (n *Network) IpOf(node string) net.IP {
	if !n.partition.networkResolver.HasNamespace(spec.NodeID(node)) {
		return nil
	}
	return n.partition.networkResolver.GetNamespace(spec.NodeID(node)).AllocatedIP
}

func (n *Network) ForwardPort(node string, port int) (*PortForwardHandle, error) {
	ip := n.IpOf(node)
	if ip == nil {
		return nil, ErrNodeNetworkMissing
	}

	return n.portFw.ForwardTCP(ip, port), nil
}

func (n *Network) Partition() *Partition {
	return n.partition
}

func (n *Network) Cleanup() error {
	if n == nil {
		return nil
	}
	return n.partition.Cleanup()
}
