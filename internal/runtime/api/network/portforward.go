package network

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/sotnii/pakostii/internal/runtime/agent"
)

type PortForward struct {
	exec agent.ClusterNetworkExecAgent
}

type PortForwardHandle struct {
	exec       agent.ClusterNetworkExecAgent
	targetIP   net.IP
	targetPort int
	listener   net.Listener
}

func NewPortForward(exec agent.ClusterNetworkExecAgent) *PortForward {
	return &PortForward{
		exec: exec,
	}
}

func (p *PortForward) ForwardTCP(targetIP net.IP, targetPort int) *PortForwardHandle {
	return &PortForwardHandle{
		targetIP:   targetIP,
		targetPort: targetPort,
		exec:       p.exec,
	}
}

func (h *PortForwardHandle) Port() int {
	if h.listener == nil {
		return 0
	}
	return h.listener.Addr().(*net.TCPAddr).Port
}

func (h *PortForwardHandle) Listen(ctx context.Context) error {
	var err error
	h.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return err
	}

	go func() {
		defer h.Close()
		for {
			conn, err := h.listener.Accept()
			if err != nil {
				return
			}
			go h.handleConn(ctx, conn)
		}
	}()

	return nil
}

func (h *PortForwardHandle) handleConn(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()
	var targetConn net.Conn
	var err error
	err = h.exec.Exec(func() error {
		var d net.Dialer
		targetConn, err = d.DialContext(ctx, "tcp", net.JoinHostPort(h.targetIP.String(), strconv.Itoa(h.targetPort)))
		return err
	})
	if err != nil {
		return
	}
	defer targetConn.Close()

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = io.Copy(targetConn, clientConn)
	})
	wg.Go(func() {
		_, _ = io.Copy(clientConn, targetConn)
	})

	wg.Wait()
}

func (h *PortForwardHandle) Close() error {
	if h.listener == nil {
		return nil
	}
	err := h.listener.Close()
	h.listener = nil
	return err
}
