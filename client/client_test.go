package client

import (
	"bytes"
	"context"
	"log"
	"testing"
)

func TestClient_Get(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
		if err := c.Set(&Item{Key: t.Name(), Value: []byte("OK")}); err != nil {
			t.Fatal(err)
		}
		item, err := c.Get(t.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item.Value, []byte("OK")) {
			t.Errorf("unexpected value: %v", item.Value)
		}
	}

	t.Run("TextProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolText)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11212", ProtocolMeta)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolBinary)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func TestClient_Set(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
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

	t.Run("TextProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolText)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11212", ProtocolMeta)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolBinary)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func TestClient_Add(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
		if err := c.Delete(t.Name()); err != nil && err != ItemNotFound {
			t.Fatal(err)
		}

		err := c.Add(&Item{Key: t.Name(), Value: []byte("foobar")})
		if err != nil {
			t.Fatal(err)
		}

		err = c.Add(&Item{Key: t.Name(), Value: []byte("fail")})
		if err != ItemExists {
			t.Errorf("expect item exists error: %v", err)
		}
	}

	t.Run("TextProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolText)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		t.Skip("Not yet supported by meta command")
		c, err := NewClient(context.Background(), "localhost:11212", ProtocolMeta)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolBinary)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func TestClient_Replace(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
		if err := c.Delete(t.Name()); err != nil && err != ItemNotFound {
			t.Fatal(err)
		}

		err := c.Replace(&Item{Key: t.Name(), Value: []byte("fail")})
		if err != ItemNotFound {
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

	t.Run("TextProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolText)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		t.Skip("Not yet supported by meta command")
		c, err := NewClient(context.Background(), "localhost:11212", ProtocolMeta)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolBinary)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func TestClient_GetMulti(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
		if err := c.Set(&Item{Key: "test", Value: []byte("OK1")}); err != nil {
			t.Fatal(err)
		}
		if err := c.Set(&Item{Key: "client", Value: []byte("OK2")}); err != nil {
			t.Fatal(err)
		}

		items, err := c.GetMulti("test", "client")
		if err != nil {
			t.Fatal(err)
		}

		if len(items) != 2 {
			t.Fatalf("expected returing 2 items: %d", len(items))
		}
		for _, v := range items {
			log.Print(v.Key)
		}
	}

	t.Run("TextProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolText)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11212", ProtocolMeta)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolBinary)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func TestClient_Delete(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
		if err := c.Set(&Item{Key: t.Name(), Value: []byte("YES")}); err != nil {
			t.Fatal(err)
		}

		if err := c.Delete(t.Name()); err != nil {
			t.Fatal(err)
		}

		_, err := c.Get(t.Name())
		if err != ItemNotFound {
			t.Fatal(err)
		}
	}

	t.Run("TextProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolText)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11212", ProtocolMeta)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolBinary)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func TestClient_Touch(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
		if err := c.Set(&Item{Key: t.Name(), Value: []byte("OK"), Expiration: 10}); err != nil {
			t.Fatal(err)
		}
		if err := c.Touch(t.Name(), 90); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("TextProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolText)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11212", ProtocolMeta)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func TestClient_Increment(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
		if err := c.Set(&Item{Key: t.Name(), Value: []byte("1")}); err != nil {
			t.Fatal(err)
		}

		newValue, err := c.Increment(t.Name(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if newValue != 2 {
			t.Errorf("unexpected return value: %d", newValue)
		}
	}

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolBinary)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func TestClient_Decrement(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
		if err := c.Set(&Item{Key: t.Name(), Value: []byte("10")}); err != nil {
			t.Fatal()
		}

		newValue, err := c.Decrement(t.Name(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if newValue != 9 {
			t.Errorf("unexpected return value: %d", newValue)
		}
	}

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolBinary)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func TestClient_Version(t *testing.T) {
	testFn := func(t *testing.T, c *Client) {
		v, err := c.Version()
		if err != nil {
			t.Fatal(err)
		}

		if v == "" {
			t.Error("returning empty value")
		}
	}

	t.Run("TextProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolText)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("MetaProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolMeta)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolBinary)
		if err != nil {
			t.Fatal(err)
		}

		testFn(t, c)
	})
}

func Benchmark_Get(b *testing.B) {
	benchFn := func(b *testing.B, c *Client) {
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

	b.Run("TextProtocol", func(b *testing.B) {
		c, err := NewClient(context.Background(), "localhost:11211", ProtocolText)
		if err != nil {
			b.Fatal(err)
		}

		benchFn(b, c)
	})

	b.Run("MetaProtocol", func(b *testing.B) {
		c, err := NewClient(context.Background(), "localhost:11212", ProtocolMeta)
		if err != nil {
			b.Fatal(err)
		}

		benchFn(b, c)
	})
}
