package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"go.f110.dev/go-memcached/client"
)

type testHandler struct {
	reqCh chan *Request
	res   [][]*Response
}

func (h *testHandler) ServeRequest(req *Request) ([]*Response, error) {
	h.reqCh <- req
	if h.res != nil {
		defer func() {
			h.res = h.res[1:]
		}()

		return h.res[0], nil
	}

	return []*Response{}, nil
}

func (h *testHandler) SetResponse(res []*Response) {
	h.res = append(h.res, res)
}

func matchRequest(t *testing.T, ch chan *Request, expect *Request) {
	var req *Request
	select {
	case req = <-ch:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Timeout")
	}

	if req.Opcode != expect.Opcode {
		t.Errorf("unexpect opcode: %v", req.Opcode)
	}
	if req.Key != expect.Key {
		t.Errorf("unexpected key: %s", req.Key)
	}
	if req.Keys != nil {
		if len(req.Keys) != len(expect.Keys) {
			t.Errorf("expect keys length is %d: %d", len(expect.Keys), len(req.Keys))
		}
		for i := 0; i < len(expect.Keys); i++ {
			if req.Keys[i] != expect.Keys[i] {
				t.Errorf("unexpected key: %s %s", expect.Keys[i], req.Keys[i])
			}
		}
	}
	if expect.Value != nil && !bytes.Equal(expect.Value, req.Value) {
		t.Errorf("Values is mismatch: %v", req.Value)
	}
	if expect.SetOpt != nil {
		if req.SetOpt == nil {
			t.Fatal("expect set SetOpt but not")
		}

		if expect.SetOpt.Expiration != req.SetOpt.Expiration {
			t.Errorf("mismatch expiration: %d", req.SetOpt.Expiration)
		}
	}
	if !bytes.Equal(expect.Flags, req.Flags) {
		t.Errorf("unexpected flags: %d", req.Flags)
	}
}

func matchItem(t *testing.T, res *client.Item, expect *client.Item) {
	if res.Key != expect.Key {
		t.Errorf("unexpected key: %s", res.Key)
	}
	if res.Expiration != expect.Expiration {
		t.Errorf("unexpected expiration: %d", res.Expiration)
	}
	if len(expect.Value) > 0 && len(expect.Value) != len(res.Value) {
		t.Error("mismatch value length")
	}
	if len(expect.Value) > 0 && !bytes.Equal(res.Value, expect.Value) {
		t.Error("unexpected value")
	}
	if len(expect.Flags) > 0 && len(expect.Flags) != len(res.Flags) {
		t.Error("mismatch flag length")
	}
	if len(expect.Flags) > 0 && !bytes.Equal(res.Flags, expect.Flags) {
		t.Errorf("unexpected flags: % 2x", res.Flags)
	}
}

func TestServer_Serve(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().(*net.TCPAddr)

	reqCh := make(chan *Request, 10)
	testServer := &testHandler{reqCh: reqCh}
	s := &Server{
		Handler: testServer,
	}
	go s.Serve(l)

	t.Run("TextProtocol", func(t *testing.T) {
		c, err := client.NewServerWithTextProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", addr.Port))
		if err != nil {
			t.Fatal(err)
		}
		m, err := client.NewServerWithMetaProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", addr.Port))
		if err != nil {
			t.Fatal(err)
		}

		t.Run("get", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Key: t.Name(),
				},
			})

			res, err := c.Get(t.Name())
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeGet, Keys: []string{t.Name()}})
			matchItem(t, res, &client.Item{Key: t.Name()})
		})

		t.Run("gets", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Key: "1",
				},
				{
					Key: "2",
				},
				{
					Key: "3",
				},
			})

			res, err := c.GetMulti("1", "2", "3")
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeGet, Keys: []string{"1", "2", "3"}})
			if len(res) != 3 {
				t.Fatalf("not have all result: %d", len(res))
			}
			matchItem(t, res[0], &client.Item{Key: "1"})
			matchItem(t, res[1], &client.Item{Key: "2"})
			matchItem(t, res[2], &client.Item{Key: "3"})
		})

		t.Run("set", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Set(&client.Item{
				Key:        t.Name(),
				Value:      []byte("OK"),
				Flags:      []byte{0x00, 0x00, 0x00, 0x01},
				Expiration: 2,
			})
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeSet,
					Key:    t.Name(),
					Value:  []byte("OK"),
					SetOpt: &SetOpt{
						Expiration: 2,
					},
					Flags: []byte{0x00, 0x00, 0x00, 0x01},
				},
			)
		})

		t.Run("delete", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{},
			})

			err := c.Delete(t.Name())
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeDelete,
					Key:    t.Name(),
				},
			)
		})

		t.Run("incr", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Value: []byte("1"),
				},
			})

			n, err := c.Increment(t.Name(), 1, 0)
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeIncrement,
					Key:    t.Name(),
				},
			)
			if n != 1 {
				t.Error("unexpected return value")
			}
		})

		t.Run("decr", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Value: []byte("9"),
				},
			})
			n, err := c.Decrement(t.Name(), 1, 0)
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeDecrement,
					Key:    t.Name(),
				},
			)
			if n != 9 {
				t.Error("unexpected return value")
			}
		})

		t.Run("touch", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{},
			})

			err := c.Touch(t.Name(), 2)
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeTouch,
					Key:    t.Name(),
				},
			)
		})

		t.Run("add", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Add(&client.Item{
				Key:        t.Name(),
				Value:      []byte("OK"),
				Flags:      []byte{0x00, 0x00, 0x00, 0x01},
				Expiration: 2,
			})
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeAdd,
					Key:    t.Name(),
					Value:  []byte("OK"),
					SetOpt: &SetOpt{
						Expiration: 2,
					},
					Flags: []byte{0x00, 0x00, 0x00, 0x01},
				},
			)
		})

		t.Run("replace", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Replace(&client.Item{
				Key:        t.Name(),
				Value:      []byte("OK"),
				Flags:      []byte{0x00, 0x00, 0x00, 0x01},
				Expiration: 2,
			})
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeReplace,
					Key:    t.Name(),
					Value:  []byte("OK"),
					SetOpt: &SetOpt{
						Expiration: 2,
					},
					Flags: []byte{0x00, 0x00, 0x00, 0x01},
				},
			)
		})

		t.Run("flush", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Flush()
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeFlush,
				},
			)
		})

		t.Run("meta get", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Key:   t.Name(),
					Value: []byte("meta get"),
				},
			})

			_, err := m.Get(t.Name())
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeGet,
					Key:    t.Name(),
				},
			)
		})

		t.Run("meta set", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := m.Set(&client.Item{
				Key:        t.Name(),
				Value:      []byte("OK"),
				Flags:      []byte{0x00, 0x00, 0x00, 0x01},
				Expiration: 10,
			})
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeSet,
					Key:    t.Name(),
					Value:  []byte("OK"),
					SetOpt: &SetOpt{
						Expiration: 10,
					},
				},
			)
		})

		t.Run("meta delete", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := m.Delete(t.Name())
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeDelete,
					Key:    t.Name(),
				},
			)
		})

		t.Run("version", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Value: []byte("VERSION test"),
				},
			})

			v, err := m.Version()
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeVersion,
				},
			)
			if v != "test" {
				t.Error("unexpected value")
			}
		})
	})

	t.Run("BinaryProtocol", func(t *testing.T) {
		c, err := client.NewServerWithBinaryProtocol(context.Background(), "test", "tcp", fmt.Sprintf("localhost:%d", addr.Port))
		if err != nil {
			t.Fatal(err)
		}

		t.Run("get", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Key:   t.Name(),
					Value: []byte("test"),
					Flags: []byte{0xde, 0xad, 0xbe, 0xef},
				},
			})

			res, err := c.Get(t.Name())
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeGet, Key: t.Name()})
			matchItem(t, res, &client.Item{Key: t.Name(), Flags: []byte{0xde, 0xad, 0xbe, 0xef}})
		})

		t.Run("get multi", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Key:   t.Name(),
					Value: []byte("OK"),
				},
			})
			testServer.SetResponse([]*Response{
				{
					Key:   t.Name() + "1",
					Value: []byte("OK2"),
				},
			})
			testServer.SetResponse([]*Response{{}})
			items, err := c.GetMulti(t.Name(), t.Name()+"1")
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeGet, Key: t.Name()})
			matchRequest(t, reqCh, &Request{Opcode: OpcodeGet, Key: t.Name() + "1"})
			matchRequest(t, reqCh, &Request{Opcode: OpcodeNoop})

			if len(items) != 2 {
				t.Fatalf("not return all result: %d", len(items))
			}
			matchItem(t, items[0], &client.Item{Key: t.Name(), Value: []byte("OK")})
			matchItem(t, items[1], &client.Item{Key: t.Name() + "1", Value: []byte("OK2")})
		})

		t.Run("set", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Set(&client.Item{
				Key:        t.Name(),
				Value:      []byte("OK"),
				Expiration: 2,
				Flags:      []byte{0x00, 0x00, 0x00, 0x01},
			})
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeSet,
					Key:    t.Name(),
					Value:  []byte("OK"),
					SetOpt: &SetOpt{
						Expiration: 2,
					},
					Flags: []byte{0x00, 0x00, 0x00, 0x01},
				},
			)
		})

		t.Run("add", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Add(&client.Item{
				Key:        t.Name(),
				Value:      []byte("OK"),
				Expiration: 10,
				Flags:      []byte{0x00, 0x00, 0x00, 0x01},
			})
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeAdd,
					Key:    t.Name(),
					Value:  []byte("OK"),
					SetOpt: &SetOpt{
						Expiration: 10,
					},
					Flags: []byte{0x00, 0x00, 0x00, 0x01},
				},
			)
		})

		t.Run("replace", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Replace(&client.Item{
				Key:        t.Name(),
				Value:      []byte("OK"),
				Expiration: 100,
				Flags:      []byte{0x00, 0x00, 0x00, 0x01},
			})
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(
				t,
				reqCh,
				&Request{
					Opcode: OpcodeReplace,
					Key:    t.Name(),
					Value:  []byte("OK"),
					SetOpt: &SetOpt{
						Expiration: 100,
					},
					Flags: []byte{0x00, 0x00, 0x00, 0x01},
				},
			)
		})

		t.Run("delete", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Delete(t.Name())
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeDelete, Key: t.Name()})
		})

		t.Run("incr", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Value: []byte{0, 0, 0, 0, 0, 0, 0, 0x01},
				},
			})

			v, err := c.Increment(t.Name(), 1, 10)
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeIncrement, Key: t.Name()})
			if v != 1 {
				t.Error("unexpected return value")
			}
		})

		t.Run("decr", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Value: []byte{0, 0, 0, 0, 0, 0, 0, 0x09},
				},
			})

			v, err := c.Decrement(t.Name(), 1, 10)
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeDecrement, Key: t.Name()})
			if v != 9 {
				t.Error("unexpected return value")
			}
		})

		t.Run("version", func(t *testing.T) {
			testServer.SetResponse([]*Response{
				{
					Value: []byte("VERSION test"),
				},
			})

			v, err := c.Version()
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeVersion})
			if v != "VERSION test" {
				t.Errorf("unexpected version string: %s", v)
			}
		})

		t.Run("flush", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Flush()
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeFlush})
		})

		t.Run("touch", func(t *testing.T) {
			testServer.SetResponse([]*Response{{}})

			err := c.Touch(t.Name(), 10)
			if err != nil {
				t.Fatal(err)
			}

			matchRequest(t, reqCh, &Request{Opcode: OpcodeTouch, Key: t.Name()})
		})
	})
}

func TestServer_WriteResponse(t *testing.T) {
	s := &Server{}

	t.Run("TextProtocol", func(t *testing.T) {
		t.Run("get", func(t *testing.T) {
			buf := new(bytes.Buffer)

			err := s.writeAsciiResponse(
				&conn{Protocol: ProtocolAscii, connBuf: bufio.NewReadWriter(bufio.NewReader(buf), bufio.NewWriter(buf))},
				&Request{
					Opcode: OpcodeGet,
					Key:    t.Name(),
				},
				[]*Response{
					{
						Flags: []byte{0x00, 0x00, 0x00, 0x00},
						Value: []byte("OK"),
					},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
		})
	})
}
