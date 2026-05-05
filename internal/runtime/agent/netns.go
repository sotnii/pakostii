package agent

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/sotnii/pakostii/spec"
)

type HttpResult struct {
	StatusCode int
	Body       []byte
}

type HttpAgent interface {
	Do(req *http.Request, timeout time.Duration) (*HttpResult, error)
}

type NetNsHttpAgent struct {
	execAgent ClusterNetworkExecAgent
	hosts     map[string]string
}

func NewNetNsHttpAgent(execAgent ClusterNetworkExecAgent, nodeIPs map[spec.NodeID]net.IP) (*NetNsHttpAgent, error) {
	hosts := make(map[string]string)
	for id, ip := range nodeIPs {
		hosts[string(id)] = ip.String()
	}

	return &NetNsHttpAgent{execAgent: execAgent, hosts: hosts}, nil
}

func (agent *NetNsHttpAgent) Do(req *http.Request, timeout time.Duration) (*HttpResult, error) {
	if req == nil {
		return nil, fmt.Errorf("request is nil")
	}
	if req.URL == nil {
		return nil, fmt.Errorf("request URL is nil")
	}
	if req.URL.Scheme != "http" {
		return nil, fmt.Errorf("only http is supported, got %q", req.URL.Scheme)
	}
	if req.URL.Host == "" {
		return nil, fmt.Errorf("request URL host is empty")
	}

	var res *HttpResult
	err := agent.execAgent.Exec(func() error {
		agentRes, err := agent.do(req, timeout)
		if err != nil {
			return err
		}
		res = agentRes
		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (agent *NetNsHttpAgent) do(req *http.Request, timeout time.Duration) (*HttpResult, error) {
	dialAddr, err := agent.resolveDialAddress(req.URL.Host)
	if err != nil {
		return nil, err
	}

	dialer := &net.Dialer{
		Timeout: timeout,
	}

	ctx := req.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	conn, err := dialer.DialContext(ctx, "tcp", dialAddr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", dialAddr, err)
	}
	defer conn.Close()

	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}

	// Clone the request so we do not mutate the caller's request.
	outReq := req.Clone(ctx)

	// Optional: if user did not explicitly set Host, preserve original URL host.
	if outReq.Host == "" {
		outReq.Host = req.URL.Host
	}

	if err := outReq.Write(conn); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, outReq)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return &HttpResult{
		StatusCode: resp.StatusCode,
		Body:       body,
	}, nil
}

func (agent *NetNsHttpAgent) resolveDialAddress(hostport string) (string, error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", err
	}

	if mapped, ok := agent.hosts[host]; ok {
		return net.JoinHostPort(mapped, port), nil
	}

	// If it's already an IP, use it directly.
	if ip := net.ParseIP(host); ip != nil {
		return net.JoinHostPort(ip.String(), port), nil
	}

	// No mapping found. Since we are avoiding DNS complexity here,
	// fail explicitly instead of silently doing a lookup.
	return "", fmt.Errorf("no host mapping for %q", host)
}
