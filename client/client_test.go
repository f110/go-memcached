package client

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	merrors "go.f110.dev/go-memcached/errors"
	"go.f110.dev/go-memcached/testutil"
)

func newClient(t *testing.T, server Server) *SinglePool {
	c, err := NewSinglePool(server)
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func newTextProtocolClient(t *testing.T, process *testutil.MemcachedProcess) *SinglePool {
	server, err := NewServerWithTextProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", process.Port))
	if err != nil {
		t.Fatal(err)
	}
	return newClient(t, server)
}

func newMetaProtocolClient(t *testing.T, process *testutil.MemcachedProcess) *SinglePool {
	server, err := NewServerWithMetaProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", process.Port))
	if err != nil {
		t.Fatal(err)
	}
	return newClient(t, server)
}

func newBinaryProtocolClient(t *testing.T, process *testutil.MemcachedProcess) *SinglePool {
	server, err := NewServerWithBinaryProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", process.Port))
	if err != nil {
		t.Fatal(err)
	}
	return newClient(t, server)
}

func TestClient_Get(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool, testExpiration bool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		if err := c.Set(&Item{Key: t.Name(), Value: []byte("FOOBAR"), Expiration: 90}); err != nil {
			t.Fatal(err)
		}
		item, err := c.Get(t.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item.Value, []byte("FOOBAR")) {
			t.Errorf("unexpected value: %v", item.Value)
		}
		if testExpiration && item.Expiration < 88 {
			t.Errorf("the expiration is too short: %d", item.Expiration)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process), false)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		testFn(t, newMetaProtocolClient(t, process), true)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process), false)
	})
}

func TestClient_Set(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		if err := c.Set(&Item{Key: t.Name(), Value: []byte("hoge")}); err != nil {
			t.Fatal(err)
		}
		item, err := c.Get(t.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item.Value, []byte("hoge")) {
			t.Errorf("stored unexpected valud")
		}
	}

	testCasFn := func(t *testing.T, c *SinglePool) {
		if err := c.Set(&Item{Key: t.Name(), Value: []byte("foo")}); err != nil {
			t.Fatal(err)
		}

		item, err := c.Get(t.Name())
		if err != nil {
			t.Fatal(err)
		}
		casUnique := item.Cas

		if err := c.Set(&Item{Key: t.Name(), Value: []byte("bar"), Cas: casUnique}); err != nil {
			t.Fatal(err)
		}
		item, err = c.Get(t.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item.Value, []byte("bar")) {
			t.Fatalf("unexpected value: %v", item.Value)
		}

		if err := c.Set(&Item{Key: t.Name(), Value: []byte("foo"), Cas: casUnique}); err == nil {
			t.Errorf("unexpected error: %v", err)
		} else if err != merrors.ItemExists {
			t.Errorf("expect item exists: %v", err)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process))
		testCasFn(t, newTextProtocolClient(t, process))
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		testFn(t, newMetaProtocolClient(t, process))
		testCasFn(t, newMetaProtocolClient(t, process))
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process))
		testCasFn(t, newBinaryProtocolClient(t, process))
	})
}

func TestClient_Add(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		if err := c.Delete(t.Name()); err != nil && err != merrors.ItemNotFound {
			t.Fatal(err)
		}

		err := c.Add(&Item{Key: t.Name(), Value: []byte("foobar")})
		if err != nil {
			t.Fatal(err)
		}

		err = c.Add(&Item{Key: t.Name(), Value: []byte("fail")})
		if err != merrors.ItemExists {
			t.Errorf("expect item exists error: %v", err)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process))
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		t.Skip("Not yet supported by meta command")
		testFn(t, newMetaProtocolClient(t, process))
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process))
	})
}

func TestClient_Replace(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		if err := c.Delete(t.Name()); err != nil && err != merrors.ItemNotFound {
			t.Fatal(err)
		}

		err := c.Replace(&Item{Key: t.Name(), Value: []byte("fail")})
		if err != merrors.ItemNotFound {
			t.Fatalf("expect item not found error: %v", err)
		}

		if err := c.Set(&Item{Key: t.Name(), Value: []byte("OK")}); err != nil {
			t.Fatal(err)
		}

		err = c.Replace(&Item{Key: t.Name(), Value: []byte("foobar")})
		if err != nil {
			t.Fatal(err)
		}

		item, err := c.Get(t.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item.Value, []byte("foobar")) {
			t.Errorf("Succeed replace, but not expected result: %v", item.Value)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process))
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		t.Skip("Not yet supported by meta command")
		testFn(t, newMetaProtocolClient(t, process))
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process))
	})
}

func TestClient_GetMulti(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool, testExpiration bool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		if err := c.Set(&Item{Key: "test", Value: []byte("OK1"), Expiration: 90}); err != nil {
			t.Fatal(err)
		}
		if err := c.Set(&Item{Key: "client", Value: []byte("OK2"), Expiration: 90}); err != nil {
			t.Fatal(err)
		}

		items, err := c.GetMulti("test", "client")
		if err != nil {
			t.Fatal(err)
		}

		if len(items) != 2 {
			t.Fatalf("expected returing 2 items: %d", len(items))
		}
		if testExpiration && (items[0].Expiration < 88 || items[1].Expiration < 88) {
			t.Errorf("the expiration is too short: %d %d", items[0].Expiration, items[1].Expiration)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process), false)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		testFn(t, newMetaProtocolClient(t, process), true)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process), false)
	})
}

func TestClient_Delete(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		if err := c.Set(&Item{Key: t.Name(), Value: []byte("YES")}); err != nil {
			t.Fatal(err)
		}

		if err := c.Delete(t.Name()); err != nil {
			t.Fatal(err)
		}

		_, err := c.Get(t.Name())
		if err != merrors.ItemNotFound {
			t.Fatal(err)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process))
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		testFn(t, newMetaProtocolClient(t, process))
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process))
	})
}

func TestClient_Touch(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		if err := c.Set(&Item{Key: t.Name(), Value: []byte("OK"), Expiration: 10}); err != nil {
			t.Fatal(err)
		}
		if err := c.Touch(t.Name(), 90); err != nil {
			t.Fatal(err)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process))
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		testFn(t, newMetaProtocolClient(t, process))
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process))
	})
}

func TestClient_Increment(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool) {
		if err := c.Set(&Item{Key: t.Name(), Value: []byte("1")}); err != nil {
			t.Fatal(err)
		}

		newValue, err := c.Increment(t.Name(), 1, 10)
		if err != nil {
			t.Fatal(err)
		}
		if newValue != 2 {
			t.Errorf("unexpected return value: %d", newValue)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process))
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		testFn(t, newMetaProtocolClient(t, process))
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process))
	})
}

func TestClient_Decrement(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		if err := c.Set(&Item{Key: t.Name(), Value: []byte("10")}); err != nil {
			t.Fatal()
		}

		newValue, err := c.Decrement(t.Name(), 1, 10)
		if err != nil {
			t.Fatal(err)
		}
		if newValue != 9 {
			t.Errorf("unexpected return value: %d", newValue)
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process))
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		testFn(t, newMetaProtocolClient(t, process))
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process))
	})
}

func TestClient_Flush(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		if err := c.Set(&Item{Key: t.Name(), Value: []byte("OK")}); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}

		_, err := c.Get(t.Name())
		if err == nil || err != merrors.ItemNotFound {
			t.Fatal("expect item not found")
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process))
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		testFn(t, newMetaProtocolClient(t, process))
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process))
	})
}

func TestClient_Version(t *testing.T) {
	testFn := func(t *testing.T, c *SinglePool) {
		defer func() {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		v, err := c.Version()
		if err != nil {
			t.Fatal(err)
		}

		if v["test"] == "" {
			t.Error("returning empty value")
		}
	}

	process := testutil.NewMemcachedProcess(t, nil)
	defer process.Stop(t)

	t.Run("TextProtocol", func(t *testing.T) {
		testFn(t, newTextProtocolClient(t, process))
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		testFn(t, newMetaProtocolClient(t, process))
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		testFn(t, newBinaryProtocolClient(t, process))
	})
}

func Benchmark_Get(b *testing.B) {
	benchFn := func(b *testing.B, c *SinglePool) {
		defer func() {
			if err := c.Close(); err != nil {
				b.Fatal(err)
			}
		}()

		if err := c.Set(&Item{Key: "bench", Value: []byte("OK")}); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < b.N; i++ {
			_, err := c.Get("bench")
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	process := testutil.NewMemcachedProcess(b, nil)
	defer process.Stop(b)

	b.Run("TextProtocol", func(b *testing.B) {
		server, err := NewServerWithTextProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", process.Port))
		if err != nil {
			b.Fatal(err)
		}
		c, err := NewSinglePool(server)
		if err != nil {
			b.Fatal(err)
		}

		benchFn(b, c)
	})

	b.Run("MetaProtocol", func(b *testing.B) {
		server, err := NewServerWithMetaProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", process.Port))
		if err != nil {
			b.Fatal(err)
		}
		c, err := NewSinglePool(server)
		if err != nil {
			b.Fatal(err)
		}

		benchFn(b, c)
	})

	b.Run("BinaryProtocol", func(b *testing.B) {
		server, err := NewServerWithBinaryProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", process.Port))
		if err != nil {
			b.Fatal(err)
		}
		c, err := NewSinglePool(server)
		if err != nil {
			b.Fatal(err)
		}

		benchFn(b, c)
	})
}
