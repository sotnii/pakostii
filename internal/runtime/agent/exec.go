package agent

import (
	"github.com/sotnii/pakostii/internal/runtime/util"
	"github.com/vishvananda/netns"
)

type ClusterNetworkExecAgent interface {
	Exec(fn func() error) error
}

type NetNsExecAgent struct {
	netnsHandle netns.NsHandle
}

func NewNetNsExecAgent(netnsHandle netns.NsHandle) *NetNsExecAgent {
	return &NetNsExecAgent{
		netnsHandle: netnsHandle,
	}
}

func (e *NetNsExecAgent) Exec(fn func() error) error {
	return util.WithNetNSHandle(e.netnsHandle, fn)
}
