package pakostii

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/sotnii/pakostii/internal/containers"
	"github.com/sotnii/pakostii/internal/network"
	"github.com/sotnii/pakostii/internal/runtime"
	"github.com/sotnii/pakostii/internal/runtime/agent"
	"github.com/sotnii/pakostii/internal/runtime/managers"
	"github.com/sotnii/pakostii/internal/runtime/util"
	"github.com/sotnii/pakostii/spec"
	"github.com/vishvananda/netns"
)

type TestResult struct {
	ErrorMessage string
	StackTrace   string
}

func (r TestResult) Failed() bool {
	return r.ErrorMessage != ""
}

type TestRuntime struct {
	id           string
	name         string
	spec         spec.ClusterSpec
	logger       *slog.Logger
	network      managers.NetworkManager
	containers   managers.ContainerManager
	workDir      string
	artifactsDir string
}

type signalState struct {
	mu  sync.Mutex
	sig os.Signal
}

func (s *signalState) set(sig os.Signal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sig == nil {
		s.sig = sig
	}
}

func (s *signalState) get() os.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sig
}

func NewTestRuntime(name string, cluster spec.ClusterSpec, logger *slog.Logger) (*TestRuntime, error) {
	workDir := filepath.Join(os.TempDir(), "pakostii")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, fmt.Errorf("create work dir: %w", err)
	}

	testId := util.NewResourceID()
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("test_id", testId)

	nsMgr, err := network.NewManager("pkst", network.ExecCommander{}, logger.With("component", "network"))
	if err != nil {
		return nil, err
	}

	containerRuntimeManager, err := containers.NewRuntimeManager(containers.Config{
		Socket:      containers.DefaultSocket,
		Namespace:   containers.DefaultNamespace,
		Snapshotter: containers.DefaultSnapshotter,
		WorkDir:     workDir,
		Logger:      logger.With("component", "container_runtime"),
	})
	if err != nil {
		return nil, err
	}

	return &TestRuntime{
		id:           testId,
		name:         name,
		spec:         cluster,
		logger:       logger,
		containers:   managers.NewContainerManager(containerRuntimeManager, logger),
		network:      managers.NewNetworkManager(nsMgr, logger),
		workDir:      workDir,
		artifactsDir: filepath.Join(".pakostii", fmt.Sprintf("%s-%s", name, testId)),
	}, nil
}

func (r *TestRuntime) Run(ctx context.Context, fn func(*TestHandle) error) (result TestResult) {
	ctx, cancel := context.WithCancel(ctx)
	var handle *TestHandle
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)

	sigState := &signalState{}
	go func() {
		select {
		case sig := <-signals:
			sigState.set(sig)
			r.logger.Warn("received termination signal", "signal", sig.String())
			cancel()
		case <-ctx.Done():
		}
	}()

	r.logger.Info(
		fmt.Sprintf("runtime for test %q starting", r.name),
		"nodes",
		len(r.spec.Nodes),
		"artifacts_dir",
		r.artifactsDir,
		"work_dir",
		r.workDir,
	)
	defer cancel()
	defer func() {
		if v := recover(); v != nil {
			result = TestResult{
				ErrorMessage: fmt.Sprintf("test runtime panicked: %v", v),
				StackTrace:   string(debug.Stack()),
			}
		}

		teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer teardownCancel()

		teardownErr := errors.Join(
			r.cleanupHandle(handle),
			r.teardown(teardownCtx),
		)
		if teardownErr != nil {
			if result.Failed() {
				result.ErrorMessage = errors.Join(errors.New(result.ErrorMessage), teardownErr).Error()
			} else {
				result.ErrorMessage = teardownErr.Error()
			}
		}
		_ = r.containers.Close()
		r.logger.Debug("test runtime finished", "error", result.ErrorMessage)
	}()

	if err := r.prepare(ctx); err != nil {
		return resultFromError(err, sigState)
	}

	agentNs := r.network.AgentNamespace()
	if agentNs == nil {
		return TestResult{ErrorMessage: "agent namespace not initialized, could not proceed the test"}
	}
	netnsHandle, err := netns.GetFromPath(agentNs.Path)
	if err != nil {
		return resultFromError(err, sigState)
	}
	netnsExecAgent := agent.NewNetNsExecAgent(netnsHandle)
	httpAgent, err := agent.NewNetNsHttpAgent(netnsExecAgent, r.network.NodeIPs())
	if err != nil {
		return resultFromError(err, sigState)
	}

	handle = newTestHandle(ctx, r.spec, &r.containers, &r.network, httpAgent, netnsExecAgent, r.logger)

	return r.runTest(ctx, cancel, sigState, handle, fn)
}

func (r *TestRuntime) prepare(ctx context.Context) error {
	r.logger.Info("preparing runtime")
	if err := r.network.Prepare(ctx, r.spec); err != nil {
		return err
	}
	if err := r.containers.Prepare(ctx, r.spec, r.id, &r.network); err != nil {
		return err
	}
	r.logger.Info("runtime ready")
	return nil
}

func (r *TestRuntime) runTest(ctx context.Context, cancel context.CancelFunc, sigState *signalState, handle *TestHandle, fn func(*TestHandle) error) TestResult {
	events, errs, err := r.containers.ObserveEvents(ctx)
	if err != nil {
		r.logger.Warn("container event subscription failed", "error", err)
	}

	testResult := make(chan TestResult, 1)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				testResult <- TestResult{
					ErrorMessage: fmt.Sprintf("test panicked: %v", v),
					StackTrace:   string(debug.Stack()),
				}
			}
		}()

		r.logger.Info(fmt.Sprintf("starting test %q", r.name))
		if err := fn(handle); err != nil {
			testResult <- TestResult{ErrorMessage: err.Error()}
			return
		}
		testResult <- TestResult{}
	}()

	var testFinished bool

	for {
		select {
		case result := <-testResult:
			testFinished = true
			r.logger.Debug("user test callback finished", "error", result.ErrorMessage)
			cancel()
			return result
		case event, ok := <-events:
			if ok {
				r.logger.Warn("container process exited", "container_id", event.ContainerID, "exec_id", event.ExecID, "exit_code", event.ExitCode)
			} else {
				events = nil
			}
		case err, ok := <-errs:
			if ok && err != nil {
				r.logger.Warn("container event stream failed", "error", err)
			} else {
				errs = nil
			}
		case <-ctx.Done():
			if testFinished {
				r.logger.Debug("runtime context canceled after normal completion")
				return TestResult{}
			}
			return resultFromError(ctx.Err(), sigState)
		}
	}
}

func resultFromError(err error, sigState *signalState) TestResult {
	if err == nil {
		return TestResult{}
	}
	if sig := sigState.get(); sig != nil {
		return TestResult{ErrorMessage: fmt.Errorf("%w: %s", runtime.ErrTestCancelled, sig.String()).Error()}
	}
	return TestResult{ErrorMessage: err.Error()}
}

func (r *TestRuntime) cleanupHandle(handle *TestHandle) (err error) {
	if handle == nil {
		return nil
	}

	defer func() {
		if v := recover(); v != nil {
			r.logger.Error("api handle cleanup panicked", "panic", v, "stack", string(debug.Stack()))
			err = fmt.Errorf("api handle cleanup panicked: %v", v)
		}
	}()

	r.logger.Debug("cleaning up api handles")
	return handle.Cleanup()
}

func (r *TestRuntime) teardown(ctx context.Context) error {
	r.logger.Info("tearing down runtime")
	var errs []error

	for _, node := range r.spec.Nodes {
		for _, container := range r.containers.ContainersOf(node.ID) {
			if err := r.collectContainerArtifacts(node.ID, container); err != nil {
				r.logger.Error("artifact collection failed", "node", node.ID, "service", container.Name, "error", err)
				errs = append(errs, err)
			}
		}
	}

	err := r.containers.Teardown(ctx)
	if err != nil {
		errs = append(errs, err)
	}

	err = r.network.Teardown(ctx)
	if err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (r *TestRuntime) collectContainerArtifacts(nodeID spec.NodeID, ctr containers.RunningContainer) error {
	dstDir := filepath.Join(r.artifactsDir, "logs", string(nodeID), ctr.Name)
	r.logger.Debug("collecting container artifacts", "node", nodeID, "service", ctr.Name, "src", ctr.IO.Dir, "dst", dstDir)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf("create artifact dir for %s/%s: %w", nodeID, ctr.Name, err)
	}

	if err := util.CopyFile(ctr.IO.Stdout, filepath.Join(dstDir, "stdout")); err != nil {
		return fmt.Errorf("copy stdout for %s/%s: %w", nodeID, ctr.Name, err)
	}
	if err := util.CopyFile(ctr.IO.Stderr, filepath.Join(dstDir, "stderr")); err != nil {
		return fmt.Errorf("copy stderr for %s/%s: %w", nodeID, ctr.Name, err)
	}

	r.logger.Info("container artifacts collected", "node", nodeID, "service", ctr.Name, "dst", dstDir)
	return nil
}
