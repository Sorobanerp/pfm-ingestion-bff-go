package main

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnvOrDefault(t *testing.T) {
	getenv := func(key string) string {
		if key == "EXISTING" {
			return "value"
		}
		return ""
	}

	assert.Equal(t, "value", envOrDefault(getenv, "EXISTING", "fallback"))
	assert.Equal(t, "fallback", envOrDefault(getenv, "NON_EXISTING", "fallback"))
}

func TestRun_ShutdownGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	getenv := func(key string) string {
		if key == "PORT" || key == "SERVER_PORT" {
			return "0" // random ephemeral port
		}
		return ""
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, getenv)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("run timed out on shutdown")
	}
}

func TestMainFunction(t *testing.T) {
	t.Setenv("PORT", "0")
	t.Setenv("SERVER_PORT", "0")

	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	}()

	main()
}

func TestRun_ListenError(t *testing.T) {
	getenv := func(key string) string {
		if key == "PORT" || key == "SERVER_PORT" {
			return "not-a-port"
		}
		return ""
	}

	err := run(context.Background(), getenv)
	assert.Error(t, err)
}
