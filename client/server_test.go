package client

import (
	"context"
	"fmt"
	"testing"

	"go.f110.dev/go-memcached/testutil"
)

func TestServer_Reconnect(t *testing.T) {
	testFn := func(t *testing.T, process *testutil.MemcachedProcess, server Server) {
		_, err := server.Version()
		if err != nil {
			t.Fatal(err)
		}

		// Restarting memcached to close the connection.
		process.Restart(t)

		_, err = server.Version()
		if err == nil {
			t.Fatal("probably BUG: connection is not closed.")
		}

		err = server.Reconnect()
		if err != nil {
			t.Fatal(err)
		}
		_, err = server.Version()
		if err != nil {
			t.Fatal(err)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		server, err := NewServerWithTextProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", process.Port))
		if err != nil {
			t.Fatal(err)
		}
		testFn(t, process, server)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		server, err := NewServerWithMetaProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", process.Port))
		if err != nil {
			t.Fatal(err)
		}
		testFn(t, process, server)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		server, err := NewServerWithBinaryProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", process.Port))
		if err != nil {
			t.Fatal(err)
		}
		testFn(t, process, server)
	})
}
