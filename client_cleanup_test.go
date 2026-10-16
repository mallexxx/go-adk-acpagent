package acpagent

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientCleanupHelper runs only in subprocesses owned by cleanup tests.
func TestClientCleanupHelper(t *testing.T) {
	mode := os.Getenv("GO_ACP_CLEANUP_HELPER")
	if mode == "" {
		return
	}
	switch mode {
	case "cooperate":
		_, _ = os.Stdout.WriteString("ready")
		_, _ = io.Copy(io.Discard, os.Stdin)
	case "ignore EOF":
		_, _ = os.Stdout.WriteString("ready")
		_, _ = io.Copy(io.Discard, os.Stdin)
		time.Sleep(time.Minute)
	case "stderr":
		_, _ = os.Stdout.WriteString("ready")
		_, _ = os.Stderr.WriteString("ready")
	}
	os.Exit(0)
}

func TestClientCloseReapsChild(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"cooperate", "ignore EOF"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			client := newCleanupClient(t, mode, io.Discard)
			start := time.Now()
			if err := client.Close(); err != nil {
				t.Fatalf("Close() = %v", err)
			}
			if elapsed := time.Since(start); elapsed > 6200*time.Millisecond {
				t.Fatalf("Close took %v, want <=6.2s", elapsed)
			}
			select {
			case <-client.closed:
			default:
				t.Fatal("Close returned before reaping child")
			}
			if client.cmd.ProcessState == nil {
				t.Fatalf("child PID %d has no reaped process state", client.cmd.Process.Pid)
			}
			if mode == "cooperate" && !client.cmd.ProcessState.Success() {
				t.Fatalf("cooperating child was killed: %v", client.cmd.ProcessState)
			}
		})
	}
}

func TestClientCloseConcurrent(t *testing.T) {
	t.Parallel()
	client := newCleanupClient(t, "ignore EOF", io.Discard)
	stdin := &countedCleanupStdin{WriteCloser: client.stdin}
	client.stdin = stdin
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if err := client.Close(); err != nil {
				t.Errorf("Close() = %v", err)
			}
		})
	}
	wg.Wait()
	if err := client.Close(); err != nil {
		t.Fatalf("repeated Close() = %v", err)
	}
	if got := stdin.calls.Load(); got != 1 {
		t.Errorf("stdin closed %d times, want once", got)
	}
}

func TestClientCloseBoundsStalledWait(t *testing.T) {
	t.Parallel()
	stderr := &blockedCleanupWriter{started: make(chan struct{}), release: make(chan struct{})}
	client := newCleanupClient(t, "stderr", stderr)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(stderr.release) }) }
	defer release()
	select {
	case <-stderr.started:
	case <-time.After(3 * time.Second):
		t.Fatal("child did not write stderr")
	}
	result := make(chan error, 1)
	start := time.Now()
	go func() { result <- closeClientAfterError(client, context.DeadlineExceeded, "cleanup") }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "cleanup timeout") {
			t.Fatalf("Close() = %v, want cleanup timeout preserving deadline", err)
		}
		if elapsed := time.Since(start); elapsed < 5*time.Second || elapsed > 6200*time.Millisecond {
			t.Errorf("Close took %v, want 5s..6.2s", elapsed)
		}
	case <-time.After(6200 * time.Millisecond):
		t.Fatal("Close did not bound stalled Wait")
	}
	select {
	case <-client.closed:
		t.Fatal("stalled Wait falsely reported reaped")
	default:
	}
	// The original waitLoop must finish after the blocked copy is released.
	release()
	select {
	case <-client.closed:
	case <-time.After(time.Second):
		t.Fatal("original reaper did not finish")
	}
	if client.cmd.ProcessState == nil {
		t.Fatal("reaper lost child process state")
	}
}

type countedCleanupStdin struct {
	io.WriteCloser
	calls atomic.Int32
}

func (s *countedCleanupStdin) Close() error { s.calls.Add(1); return s.WriteCloser.Close() }

type cleanupReadyWriter chan struct{}

func (w cleanupReadyWriter) Write(p []byte) (int, error) {
	close(w)
	return len(p), nil
}

type blockedCleanupWriter struct {
	started chan struct{}
	release chan struct{}
}

func (w *blockedCleanupWriter) Write(p []byte) (int, error) {
	close(w.started)
	<-w.release
	return len(p), nil
}

func newCleanupClient(t *testing.T, mode string, stderr io.Writer) *Client {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestClientCleanupHelper$")
	cmd.Env = append(os.Environ(), "GO_ACP_CLEANUP_HELPER="+mode, "GORACE=atexit_sleep_ms=0")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = stderr
	ready := make(cleanupReadyWriter)
	cmd.Stdout = ready
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	dispatchDone := make(chan struct{})
	close(dispatchDone)
	client := &Client{ctx: t.Context(), cmd: cmd, stdin: stdin, logger: newLogger(nil, ""), closed: make(chan struct{}), dispatchDone: dispatchDone}
	go client.waitLoop()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-client.closed:
		case <-time.After(3 * time.Second):
			t.Error("cleanup reaper still pending")
		}
	})
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup helper did not become ready")
	}
	return client
}
