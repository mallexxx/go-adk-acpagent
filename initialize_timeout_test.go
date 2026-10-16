package acpagent

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func initializeConfig(t *testing.T, cfg Config, timeout time.Duration) Config {
	t.Helper()
	cfg.InitializeTimeout = timeout
	return cfg
}

func TestInitializeTimeoutCompatibility(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		setup, initialize time.Duration
		wantDeadline      bool
	}{
		{"zero inherits setup", 80 * time.Millisecond, 0, true},
		{"both zero preserve caller", 0, 0, false},
		{"positive overrides setup", 80 * time.Millisecond, time.Second, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, lazy := range []bool{false, true} {
				cfg := initializeConfig(t, Config{Command: helperCommandWithEnv(t, map[string]string{"GO_INITIALIZE_DELAY": "200ms"}), LazyInitialization: lazy, SetupTimeout: tc.setup}, tc.initialize)
				a, err := NewWithContext(t.Context(), cfg)
				if err == nil {
					defer closeTestCloser(t, a)
					err = a.ensureInitialized(t.Context())
				}
				if tc.wantDeadline {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("lazy=%v initialize=%v, want deadline", lazy, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("lazy=%v initialize=%v", lazy, err)
				}
			}
		})
	}
}

func TestInitializeTimeoutRejectsNegative(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		cfg := initializeConfig(t, Config{Command: []string{"/does/not/exist"}, LazyInitialization: lazy}, -time.Second)
		if a, err := NewWithContext(t.Context(), cfg); err == nil {
			closeTestCloser(t, a)
			t.Fatal("negative initialize timeout accepted")
		} else if !strings.Contains(err.Error(), "initialize timeout") {
			t.Fatalf("error=%v, want initialize timeout validation", err)
		}
	}
}

func TestInitializeLifetimeAndWarmReuse(t *testing.T) {
	t.Parallel()
	path := t.TempDir() + "/methods"
	cfg := initializeConfig(t, Config{Command: helperCommandWithEnv(t, map[string]string{"GO_METHOD_LOG": path}), LazyInitialization: true, SetupTimeout: 80 * time.Millisecond}, 200*time.Millisecond)
	a, err := NewWithContext(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestCloser(t, a)
	caller, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if err := a.ensureInitialized(caller); err != nil {
		t.Fatal(err)
	}
	client := a.client
	<-caller.Done()
	if err := a.ensureInitialized(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.client.NewSession(t.Context(), t.TempDir(), nil); err != nil {
		t.Fatalf("healthy child died after startup deadline: %v", err)
	}
	if a.client != client {
		t.Fatal("warm initialization replaced client")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), "initialize\n") != 1 {
		t.Fatalf("methods=%s", data)
	}
}

func TestInitializeBudgetIncludesGate(t *testing.T) {
	t.Parallel()
	cfg := initializeConfig(t, Config{Command: []string{"/does/not/exist"}, LazyInitialization: true}, 80*time.Millisecond)
	a, err := NewWithContext(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestCloser(t, a)
	a.initializationGate <- struct{}{}
	start := time.Now()
	err = a.ensureInitialized(t.Context())
	<-a.initializationGate
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued initialize=%v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("gate wait took %v", elapsed)
	}
	if a.client != nil {
		t.Fatal("gate timeout launched client")
	}
}

func TestInitializeCancellationReapsChild(t *testing.T) {
	for _, mode := range []string{"budget", "caller cancel", "caller deadline", "lifecycle", "shutdown", "after launch"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			methods := dir + "/methods"
			pidLog := dir + "/pid"
			env := map[string]string{"GO_HANG_METHOD": "initialize", "GO_METHOD_LOG": methods, "GO_PID_LOG": pidLog, "GO_IGNORE_STDIN_EOF": "1"}
			budget := 60 * time.Second
			if mode == "budget" {
				budget = 200 * time.Millisecond
			}
			life, cancelLife := context.WithCancel(t.Context())
			defer cancelLife()
			cfg := initializeConfig(t, Config{Command: helperCommandWithEnv(t, env), LazyInitialization: true, SetupTimeout: 10 * time.Second}, budget)
			a, err := NewWithContext(life, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestCloser(t, a)
			caller, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := context.Canceled
			if mode == "caller deadline" {
				caller, cancel = context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				want = context.DeadlineExceeded
			}
			if mode == "budget" {
				want = context.DeadlineExceeded
			}
			result := make(chan error, 1)
			go func() { result <- a.ensureInitialized(caller) }()
			if mode == "after launch" {
				waitInitializeFile(t, pidLog, "")
			} else {
				waitInitializeFile(t, methods, "initialize\n")
			}
			start := time.Now()
			switch mode {
			case "caller cancel", "after launch":
				cancel()
			case "lifecycle":
				cancelLife()
			case "shutdown":
				if err := a.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) || !strings.Contains(err.Error(), "initialize acp client") {
					t.Fatalf("initialize=%v, want %v", err, want)
				}
			case <-time.After(6200 * time.Millisecond):
				t.Fatal("initialization/cleanup exceeded bound")
			}
			if time.Since(start) > 6200*time.Millisecond {
				t.Fatal("cleanup exceeded bound")
			}
			if a.client != nil {
				t.Fatal("failed initialization published client")
			}
			data, err := os.ReadFile(pidLog)
			if err != nil {
				t.Fatal(err)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				t.Fatal(err)
			}
			process, err := os.FindProcess(pid)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = process.Release() }()
			if err := process.Signal(syscall.Signal(0)); err == nil {
				t.Fatalf("child PID%d still exists after cleanup", pid)
			}
		})
	}
}

func waitInitializeFile(t *testing.T, path, contains string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if data, err := os.ReadFile(path); err == nil && len(data) > 0 && strings.Contains(string(data), contains) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper did not rendezvous at %s", path)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestInitializeCancellationBeforePublish(t *testing.T) {
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := Config{Command: helperCommand(t), LazyInitialization: true, InitializeTimeout: time.Second,
		Logger: slog.New(slog.NewTextHandler(cancelInitializeWriter{cancel: cancel}, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	a, err := NewWithContext(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestCloser(t, a)
	if err := a.ensureInitialized(caller); !errors.Is(err, context.Canceled) {
		t.Fatalf("initialize=%v, want cancellation racing successful response", err)
	}
	if a.client != nil {
		t.Fatal("canceled successful response published client")
	}
}

type cancelInitializeWriter struct{ cancel context.CancelFunc }

func (w cancelInitializeWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "acp initialize succeeded") {
		w.cancel()
	}
	return len(p), nil
}

func TestInitializeQueuedCallerLifecycle(t *testing.T) {
	cfg := Config{Command: []string{"/does/not/exist"}, LazyInitialization: true, InitializeTimeout: 60 * time.Second}
	life, cancel := context.WithCancel(t.Context())
	defer cancel()
	a, err := NewWithContext(life, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestCloser(t, a)
	a.initializationGate <- struct{}{}
	done := make(chan error, 1)
	go func() { done <- a.ensureInitialized(t.Context()) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued initialize=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued caller ignored lifecycle cancellation")
	}
	<-a.initializationGate
}
