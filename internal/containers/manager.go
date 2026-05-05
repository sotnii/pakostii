package containers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/containerd/containerd"
	containerevents "github.com/containerd/containerd/api/events"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sotnii/pakostii/internal/runtime/common"
)

type Config struct {
	Socket      string
	Namespace   string
	Snapshotter string
	WorkDir     string
	Logger      *slog.Logger
}

type manager struct {
	client      *containerd.Client
	namespace   string
	snapshotter string
	workDir     string
	logger      *slog.Logger
}

func NewRuntimeManager(cfg Config) (ContainerRuntimeManager, error) {
	if cfg.Logger != nil {
		cfg.Logger.Debug("connecting to containerd", "socket", cfg.Socket, "namespace", cfg.Namespace, "snapshotter", cfg.Snapshotter)
	}
	client, err := containerd.New(
		cfg.Socket,
		containerd.WithDefaultNamespace(cfg.Namespace),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to containerd: %w", err)
	}

	return &manager{
		client:      client,
		namespace:   cfg.Namespace,
		snapshotter: cfg.Snapshotter,
		workDir:     cfg.WorkDir,
		logger:      cfg.Logger,
	}, nil
}

func (m *manager) RunContainer(ctx context.Context, req LaunchRequest) (*RunningContainer, error) {
	ctx = namespaces.WithNamespace(ctx, m.namespace)
	m.logger.Debug("run container requested", "container_id", req.ID, "node", req.NodeID, "service", req.Name, "image", req.ImageRef, "netns", req.NetNSPath)

	if req.StartDelay > 0 {
		m.logger.Debug("waiting before container start", "container_id", req.ID, "delay", req.StartDelay)
		timer := time.NewTimer(req.StartDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	image, err := m.client.Pull(ctx, req.ImageRef, containerd.WithPullUnpack)
	if err != nil {
		return nil, fmt.Errorf("pull image %s: %w", req.ImageRef, err)
	}
	m.logger.Debug("image ready", "container_id", req.ID, "image", image.Name())

	var cleanup common.CleanupStack
	defer cleanup.Run()

	bundleDir, mounts, err := m.prepareFiles(req.ID, req.Files)
	if err != nil {
		return nil, err
	}
	cleanup.Push(func() {
		m.logger.Debug("cleanup bundle dir", "container_id", req.ID, "dir", bundleDir)
		_ = os.RemoveAll(bundleDir)
	})
	m.logger.Debug("prepared injected files", "container_id", req.ID, "bundle_dir", bundleDir, "mount_count", len(mounts))

	containerIO, stdoutWriter, stderrWriter, err := m.prepareContainerIO(req.ID)
	if err != nil {
		return nil, err
	}
	cleanup.Push(func() {
		if containerIO.Dir != "" {
			m.logger.Debug("cleanup container io dir", "container_id", req.ID, "dir", containerIO.Dir)
			_ = os.RemoveAll(containerIO.Dir)
		}
	})

	snapshotKey := req.ID + "-snapshot"
	opts := []containerd.NewContainerOpts{
		containerd.WithSnapshotter(m.snapshotter),
		containerd.WithNewSnapshot(snapshotKey, image),
		containerd.WithImage(image),
		containerd.WithContainerLabels(map[string]string{
			"pakostii.node": req.NodeID,
		}),
		containerd.WithNewSpec(
			oci.WithImageConfig(image),
			oci.WithEnv(envSlice(req.Env)),
			oci.WithHostname(req.Hostname),
			oci.WithLinuxNamespace(specs.LinuxNamespace{
				Type: specs.NetworkNamespace,
				Path: req.NetNSPath,
			}),
			oci.WithMounts(mounts),
		),
	}

	cleanupCtx, cleanupCancel := m.newCleanupContext()
	defer cleanupCancel()

	container, err := m.client.NewContainer(ctx, req.ID, opts...)
	if err != nil {
		return nil, fmt.Errorf("create container %s: %w", req.ID, err)
	}
	cleanup.Push(func() {
		m.logger.Debug("cleanup container metadata", "container_id", req.ID, "snapshot_key", snapshotKey)
		_ = container.Delete(cleanupCtx, containerd.WithSnapshotCleanup)
	})
	m.logger.Debug("container metadata created", "container_id", req.ID, "snapshot_key", snapshotKey)

	task, err := container.NewTask(
		ctx,
		cio.NewCreator(
			cio.WithStreams(nil, stdoutWriter, stderrWriter),
			cio.WithFIFODir(containerIO.Dir),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create task for %s: %w", req.ID, err)
	}
	cleanup.Push(func() {
		m.logger.Debug("cleanup container task", "container_id", req.ID)
		_, _ = task.Delete(cleanupCtx, containerd.WithProcessKill)
	})
	m.logger.Debug("task created", "container_id", req.ID)

	if err := task.Start(ctx); err != nil {
		return nil, fmt.Errorf("start task for %s: %w", req.ID, err)
	}

	m.logger.Debug("cleanup stack cleared after successful container start", "container_id", req.ID)
	cleanup.Clear()
	m.logger.Debug("container started", "container_id", req.ID, "image", req.ImageRef)
	return &RunningContainer{
		ID:          req.ID,
		Name:        req.Name,
		NodeID:      req.NodeID,
		SnapshotKey: snapshotKey,
		BundleDir:   bundleDir,
		IO:          containerIO,
	}, nil
}

func (m *manager) TeardownContainer(ctx context.Context, ctr RunningContainer) error {
	ctx = namespaces.WithNamespace(ctx, m.namespace)
	m.logger.Debug("tearing down container", "container_id", ctr.ID, "service", ctr.Name, "node", ctr.NodeID)
	container, err := m.client.LoadContainer(ctx, ctr.ID)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("load container %s: %w", ctr.ID, err)
	}

	task, err := container.Task(ctx, nil)
	if err == nil {
		m.logger.Debug("killing task", "container_id", ctr.ID)
		_ = task.Kill(ctx, syscall.SIGKILL)
		_, _ = task.Delete(ctx, containerd.WithProcessKill)
	}

	err = container.Delete(ctx, containerd.WithSnapshotCleanup)
	if removeErr := os.RemoveAll(ctr.BundleDir); removeErr != nil {
		err = errors.Join(err, removeErr)
	}
	if ctr.IO.Dir != "" {
		if removeErr := os.RemoveAll(ctr.IO.Dir); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
	}
	if err != nil {
		return fmt.Errorf("delete container %s: %w", ctr.ID, err)
	}
	return nil
}

func (m *manager) prepareContainerIO(containerID string) (ContainerIO, io.Writer, io.Writer, error) {
	root := filepath.Join(m.workDir, "logs", containerID)
	m.logger.Debug("preparing container io", "container_id", containerID, "dir", root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return ContainerIO{}, nil, nil, fmt.Errorf("create io dir for %s: %w", containerID, err)
	}

	stdoutPath := filepath.Join(root, "stdout")
	stderrPath := filepath.Join(root, "stderr")

	stdoutFile, err := os.OpenFile(stdoutPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return ContainerIO{}, nil, nil, fmt.Errorf("create stdout file for %s: %w", containerID, err)
	}
	if err := stdoutFile.Close(); err != nil {
		return ContainerIO{}, nil, nil, fmt.Errorf("close stdout file for %s: %w", containerID, err)
	}
	stderrFile, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return ContainerIO{}, nil, nil, fmt.Errorf("create stderr file for %s: %w", containerID, err)
	}
	if err := stderrFile.Close(); err != nil {
		return ContainerIO{}, nil, nil, fmt.Errorf("close stderr file for %s: %w", containerID, err)
	}

	return ContainerIO{
		Dir:    root,
		Stdout: stdoutPath,
		Stderr: stderrPath,
	}, fileAppender{path: stdoutPath}, fileAppender{path: stderrPath}, nil
}

func (m *manager) ExecInContainer(ctx context.Context, containerID string, argv []string) (*ExecResult, error) {
	ctx = namespaces.WithNamespace(ctx, m.namespace)
	m.logger.Debug("exec in container", "container_id", containerID, "argv", argv)

	container, err := m.client.LoadContainer(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("load container %s: %w", containerID, err)
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("load task for %s: %w", containerID, err)
	}
	taskSpec, err := task.Spec(ctx)
	if err != nil {
		return nil, fmt.Errorf("load task spec for %s: %w", containerID, err)
	}
	if taskSpec == nil || taskSpec.Process == nil {
		return nil, fmt.Errorf("task spec missing process for %s", containerID)
	}

	execID := "exec-" + filepath.Base(filepath.Clean(fmt.Sprintf("%d-%s", time.Now().UnixNano(), containerID)))
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	processSpec := *taskSpec.Process
	processSpec.Args = argv
	processSpec.Terminal = false
	processSpec.ConsoleSize = nil
	if processSpec.Cwd == "" {
		processSpec.Cwd = "/"
	}

	process, err := task.Exec(
		ctx,
		execID,
		&processSpec,
		cio.NewCreator(cio.WithStreams(nil, &stdout, &stderr)),
	)
	if err != nil {
		return nil, fmt.Errorf("create exec process in %s: %w", containerID, err)
	}
	m.logger.Debug("exec process created", "container_id", containerID, "exec_id", execID)

	statusC, err := process.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("wait on exec process %s: %w", execID, err)
	}

	if err := process.Start(ctx); err != nil {
		return nil, fmt.Errorf("start exec process %s: %w", execID, err)
	}
	m.logger.Debug("exec process started", "container_id", containerID, "exec_id", execID)

	select {
	case <-ctx.Done():
		m.logger.Warn("exec canceled", "container_id", containerID, "exec_id", execID)
		_ = process.Kill(context.Background(), syscall.SIGKILL)
		_, _ = process.Delete(context.Background(), containerd.WithProcessKill)
		return nil, ctx.Err()
	case status := <-statusC:
		exitCode, _, waitErr := status.Result()
		if _, err := process.Delete(ctx); err != nil && !errdefs.IsNotFound(err) {
			waitErr = errors.Join(waitErr, err)
		}
		if waitErr != nil {
			return nil, fmt.Errorf("exec process %s failed: %w", execID, waitErr)
		}
		m.logger.Debug("exec process completed", "container_id", containerID, "exec_id", execID, "exit_code", exitCode, "stdout_len", len(stdout.String()), "stderr_len", len(stderr.String()))
		return &ExecResult{
			ExitCode: exitCode,
			Stdout:   stdout.String(),
			Stderr:   stderr.String(),
		}, nil
	}
}

func (m *manager) ObserveEvents(ctx context.Context) (<-chan Event, <-chan error, error) {
	ctx = namespaces.WithNamespace(ctx, m.namespace)
	m.logger.Debug("subscribing to containerd events")
	stream, errs := m.client.Subscribe(ctx)
	out := make(chan Event, 16)
	errOut := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errOut)
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-errs:
				if !ok {
					return
				}
				if err != nil {
					m.logger.Warn("containerd event stream error", "error", err)
					errOut <- err
				}
			case envelope, ok := <-stream:
				if !ok {
					return
				}
				if envelope.Topic != "/tasks/exit" || envelope.Event == nil {
					continue
				}
				m.logger.Debug("received task exit envelope", "topic", envelope.Topic, "namespace", envelope.Namespace)
				payload, err := typeurl.UnmarshalAny(envelope.Event)
				if err != nil {
					errOut <- err
					continue
				}
				taskExit, ok := payload.(*containerevents.TaskExit)
				if !ok {
					continue
				}
				out <- Event{
					ContainerID: taskExit.ContainerID,
					ExecID:      taskExit.ID,
					ExitCode:    taskExit.ExitStatus,
				}
			}
		}
	}()

	return out, errOut, nil
}

func (m *manager) newCleanupContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	return namespaces.WithNamespace(ctx, m.namespace), cancel
}

func (m *manager) Close() error {
	return m.client.Close()
}

func (m *manager) prepareFiles(containerID string, files map[string]string) (string, []specs.Mount, error) {
	root := filepath.Join(m.workDir, "mounts", containerID)
	m.logger.Debug("preparing injected files", "container_id", containerID, "root", root, "file_count", len(files))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", nil, fmt.Errorf("create mount root for %s: %w", containerID, err)
	}

	mounts := make([]specs.Mount, 0, len(files))
	for target, content := range files {
		source := filepath.Join(root, sanitizeTarget(target))
		m.logger.Debug("writing injected file", "container_id", containerID, "target", target, "source", source, "size", len(content))
		if err := os.MkdirAll(filepath.Dir(source), 0o755); err != nil {
			return "", nil, fmt.Errorf("create host dir for %s: %w", target, err)
		}
		if err := os.WriteFile(source, []byte(content), 0o644); err != nil {
			return "", nil, fmt.Errorf("write injected file %s: %w", target, err)
		}

		mounts = append(mounts, specs.Mount{
			Destination: target,
			Type:        "bind",
			Source:      source,
			Options:     []string{"rbind", "ro"},
		})
	}

	return root, mounts, nil
}

func sanitizeTarget(target string) string {
	clean := filepath.Clean(target)
	if clean == "/" {
		return "root"
	}
	if clean[0] == '/' {
		clean = clean[1:]
	}
	return clean
}

func envSlice(env map[string]string) []string {
	out := make([]string, 0, len(env))
	for key, value := range env {
		out = append(out, fmt.Sprintf("%s=%s", key, value))
	}
	return out
}

type fileAppender struct {
	path string
}

func (w fileAppender) Write(p []byte) (int, error) {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.Write(p)
}
