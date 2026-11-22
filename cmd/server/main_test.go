package main

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestLoadDotEnvLocalAndLocalRun(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), ".env.local")
	content := "# comment\n\nINVALID_LINE\nTEST_DOTENV_KEY=\"test_val\"\n"
	require.NoError(t, os.WriteFile(tmpFile, []byte(content), 0o600))
	loadDotEnvLocal(tmpFile)
	assert.Equal(t, "test_val", os.Getenv("TEST_DOTENV_KEY"))
	_ = os.Unsetenv("TEST_DOTENV_KEY")

	ctx, cancel := context.WithCancel(context.Background())
	getenv := func(key string) string {
		switch key {
		case "APP_ENV":
			return "local"
		case "PORT", "SERVER_PORT":
			return "0"
		default:
			return ""
		}
	}
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, getenv) }()
	time.Sleep(80 * time.Millisecond)
	cancel()
	assert.NoError(t, <-errCh)
}
