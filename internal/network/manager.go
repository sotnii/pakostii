package network

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
)

const defaultSubnet = "10.0.0.0/24"

type Namespace struct {
	ID          string
	Name        string
	Path        string
	Interface   string
	BridgePeer  string
	AllocatedIP net.IP
}

type NamespaceManager struct {
	prefix     string
	bridgeName string
	ip         *IPCmd
	alloc      *Allocator
	logger     *slog.Logger
}

func NewManager(prefix string, cmd Commander, logger *slog.Logger) (*NamespaceManager, error) {
	alloc, err := NewAllocator(defaultSubnet)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &NamespaceManager{
		prefix:     prefix,
		bridgeName: fmt.Sprintf("%s-bridge", prefix),
		ip:         NewIPCmd(cmd),
		alloc:      alloc,
		logger:     logger,
	}, nil
}

func (m *NamespaceManager) SetupBridge(ctx context.Context) error {
	m.logger.Debug("checking bridge", "bridge", m.bridgeName)
	exists, up, err := m.ip.LinkStatus(ctx, m.bridgeName)
	if err != nil {
		return fmt.Errorf("check bridge status: %w", err)
	}
	if !exists {
		m.logger.Debug("creating bridge", "bridge", m.bridgeName)
		if err := m.ip.AddBridge(ctx, m.bridgeName); err != nil {
			return fmt.Errorf("create bridge %s: %w", m.bridgeName, err)
		}
	}
	if !up {
		m.logger.Debug("bringing bridge up", "bridge", m.bridgeName)
		if err := m.ip.BringLinkUp(ctx, m.bridgeName); err != nil {
			return fmt.Errorf("bring bridge %s up: %w", m.bridgeName, err)
		}
	}
	m.logger.Debug("bridge ready", "bridge", m.bridgeName)
	return nil
}

func (m *NamespaceManager) CreateNamespace(ctx context.Context, id, logicalName string) (*Namespace, error) {
	name := fmt.Sprintf("%s-%s", m.prefix, logicalName)
	path := filepath.Join("/run/netns", name)
	m.logger.Debug("creating namespace", "namespace", name, "id", id, "path", path)

	if _, err := os.Stat(path); err == nil {
		m.logger.Warn("namespace already exists, deleting before recreate", "namespace", name)
		if err := m.ip.DeleteNamespace(ctx, name); err != nil {
			return nil, fmt.Errorf("delete existing namespace %s: %w", name, err)
		}
	}

	if err := m.ip.AddNamespace(ctx, name); err != nil {
		return nil, fmt.Errorf("create namespace %s: %w", name, err)
	}

	return &Namespace{ID: id, Name: name, Path: path}, nil
}

func (m *NamespaceManager) SetupNamespace(ctx context.Context, ns *Namespace) error {
	m.logger.Debug("setting up namespace", "namespace", ns.Name, "id", ns.ID)
	if err := m.ip.NamespaceLinkUp(ctx, ns.Name, "lo"); err != nil {
		return fmt.Errorf("bring loopback up in %s: %w", ns.Name, err)
	}

	veth := fmt.Sprintf("%s-%s-veth", m.prefix, ns.ID)
	bridgePeer := fmt.Sprintf("%s-%s-br", m.prefix, ns.ID)
	m.logger.Debug("creating veth pair", "namespace", ns.Name, "veth", veth, "bridge_peer", bridgePeer)
	if err := m.ip.AddVethPair(ctx, veth, bridgePeer); err != nil {
		return fmt.Errorf("create veth pair for %s: %w", ns.Name, err)
	}
	if err := m.ip.MoveToNamespace(ctx, veth, ns.Name); err != nil {
		return fmt.Errorf("move %s to namespace %s: %w", veth, ns.Name, err)
	}
	if err := m.ip.SetMaster(ctx, bridgePeer, m.bridgeName); err != nil {
		return fmt.Errorf("attach %s to bridge %s: %w", bridgePeer, m.bridgeName, err)
	}
	if err := m.ip.NamespaceLinkUp(ctx, ns.Name, veth); err != nil {
		return fmt.Errorf("bring %s up in namespace %s: %w", veth, ns.Name, err)
	}
	if err := m.ip.BringLinkUp(ctx, bridgePeer); err != nil {
		return fmt.Errorf("bring %s up: %w", bridgePeer, err)
	}

	ip, err := m.alloc.Allocate()
	if err != nil {
		return err
	}
	if err := m.ip.NamespaceAddrAdd(ctx, ns.Name, veth, fmt.Sprintf("%s/%d", ip.String(), m.alloc.Prefix())); err != nil {
		return fmt.Errorf("assign ip to %s: %w", veth, err)
	}

	ns.Interface = veth
	ns.BridgePeer = bridgePeer
	ns.AllocatedIP = ip
	m.logger.Debug("namespace ready", "namespace", ns.Name, "ip", ip.String(), "veth", veth, "bridge_peer", bridgePeer)
	return nil
}

func (m *NamespaceManager) TeardownNamespace(ctx context.Context, ns *Namespace) error {
	if ns == nil {
		return nil
	}
	if _, err := os.Stat(ns.Path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat namespace %s: %w", ns.Name, err)
	}
	m.logger.Debug("tearing down namespace", "namespace", ns.Name)
	if err := m.ip.DeleteNamespace(ctx, ns.Name); err != nil {
		return fmt.Errorf("delete namespace %s: %w", ns.Name, err)
	}
	return nil
}
