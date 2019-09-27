package memcached

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

var (
	crlf        = []byte("\r\n")
	msgStored   = []byte("STORED\r\n")
	msgDeleted  = []byte("DELETED\r\n")
	msgNotFound = []byte("NOT_FOUND\r\n")
	msgTouched  = []byte("TOUCHED\r\n")
	msgOk       = []byte("OK\r\n")
)

var (
	ItemNotFound = errors.New("mecached: not found")
)

type Item struct {
	Key        string
	Value      []byte
	Flags      int
	Expiration int
	Cas        int
}

type engine interface {
	Get(key string) (*Item, error)
	GetMulti(key ...string) ([]*Item, error)
	Set(item *Item) error
	Delete(key string) error
	Incr(key string, delta int) (int, error)
	Decr(key string, delta int) (int, error)
	Touch(key string, expiration int) error
	Flush() error
}

type Client struct {
	protocol engine
}

func NewClient(ctx context.Context, server string) (*Client, error) {
	engine, err := newTextProtocol(ctx, server)
	if err != nil {
		return nil, err
	}
	return &Client{
		protocol: engine,
	}, nil
}

func (c *Client) Get(key string) (*Item, error) {
	return c.protocol.Get(key)
}

func (c *Client) Set(item *Item) error {
	return c.protocol.Set(item)
}

func (c *Client) GetMulti(keys ...string) ([]*Item, error) {
	return c.protocol.GetMulti(keys...)
}

func (c *Client) Delete(key string) error {
	return c.protocol.Delete(key)
}

func (c *Client) Increment(key string, delta int) (int, error) {
	return c.protocol.Incr(key, delta)
}

func (c *Client) Decrement(key string, delta int) (int, error) {
	return c.protocol.Decr(key, delta)
}

func (c *Client) Flush() error {
	return c.protocol.Flush()
}

type textProtocol struct {
	server string
	conn   *bufio.ReadWriter
}

var _ engine = &textProtocol{}

func newTextProtocol(ctx context.Context, server string) (*textProtocol, error) {
	d := &net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", server)
	if err != nil {
		return nil, err
	}
	return &textProtocol{conn: bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))}, nil
}

func (t *textProtocol) Get(key string) (*Item, error) {
	t.conn.WriteString("get ")
	t.conn.WriteString(key)
	t.conn.Write(crlf)
	if err := t.conn.Flush(); err != nil {
		return nil, err
	}
	b, err := t.conn.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if bytes.Equal(b, []byte("END\r\n")) {
		return nil, ItemNotFound
	}
	if !bytes.HasPrefix(b, []byte("VALUE")) {
		return nil, errors.New("memcached: invalid response")
	}
	item := &Item{}
	size := 0
	p := "VALUE %s %d %d %d"
	scan := []interface{}{&item.Key, &item.Flags, &size, &item.Cas}
	if bytes.Count(b, []byte(" ")) == 3 {
		p = "VALUE %s %d %d"
		scan = scan[:3]
	}
	if _, err := fmt.Fscanf(bytes.NewReader(b), p, scan...); err != nil {
		return nil, err
	}
	buf := make([]byte, size+2)

	if _, err := io.ReadFull(t.conn, buf); err != nil {
		return nil, err
	}
	item.Value = buf[:size]

	return item, nil
}

func (t *textProtocol) GetMulti(keys ...string) ([]*Item, error) {
	t.conn.WriteString("gets ")
	t.conn.WriteString(strings.Join(keys, " "))
	t.conn.Write(crlf)
	if err := t.conn.Flush(); err != nil {
		return nil, err
	}
	r := bufio.NewReader(t.conn)
	return t.parseGetResponse(r)
}

func (t *textProtocol) Delete(key string) error {
	if _, err := fmt.Fprintf(t.conn, "delete %s\r\n", key); err != nil {
		return err
	}
	if err := t.conn.Flush(); err != nil {
		return err
	}
	r := bufio.NewReader(t.conn)
	b, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	case bytes.Equal(b, msgDeleted):
		return nil
	case bytes.Equal(b, msgNotFound):
		return ItemNotFound
	}
	return nil
}

func (t *textProtocol) Incr(key string, delta int) (int, error) {
	return t.incrOrDecr("incr", key, delta)
}

func (t *textProtocol) Decr(key string, delta int) (int, error) {
	return t.incrOrDecr("decr", key, delta)
}

func (t *textProtocol) incrOrDecr(op, key string, delta int) (int, error) {
	if _, err := fmt.Fprintf(t.conn, "%s %s %d\r\n", op, key, delta); err != nil {
		return 0, err
	}
	if err := t.conn.Flush(); err != nil {
		return 0, err
	}
	r := bufio.NewReader(t.conn)
	b, err := r.ReadSlice('\n')
	if err != nil {
		return 0, err
	}

	switch {
	case bytes.Equal(b, msgNotFound):
		return 0, ItemNotFound
	default:
		i, err := strconv.Atoi(string(b[:len(b)-2]))
		if err != nil {
			return 0, err
		}
		return i, nil
	}
}

func (t *textProtocol) Touch(key string, expiration int) error {
	if _, err := fmt.Fprintf(t.conn, "touch %s %d\r\n", key, expiration); err != nil {
		return err
	}
	r := bufio.NewReader(t.conn)
	b, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	case bytes.Equal(b, msgTouched):
		return nil
	case bytes.Equal(b, msgNotFound):
		return ItemNotFound
	}

	return nil
}

func (t *textProtocol) parseGetResponse(r *bufio.Reader) ([]*Item, error) {
	res := make([]*Item, 0)
	for {
		b, err := r.ReadSlice('\n')
		if err != nil {
			return nil, err
		}
		if bytes.Equal(b, []byte("END\r\n")) {
			break
		}
		s := bytes.Split(b, []byte(" "))
		if !bytes.Equal(s[0], []byte("VALUE")) {
			return nil, errors.New("memcached: invalid response")
		}
		f, err := strconv.Atoi(string(s[2]))
		if err != nil {
			return nil, err
		}
		var size int
		if len(s) == 4 {
			size, err = strconv.Atoi(string(s[3][:len(s[3])-2]))
		} else {
			size, err = strconv.Atoi(string(s[3]))
		}
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		item := &Item{
			Key:   string(s[1]),
			Flags: f,
		}
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		item.Value = buf[:size]
		res = append(res, item)
	}

	return res, nil
}

func (t *textProtocol) Set(item *Item) error {
	if _, err := fmt.Fprintf(t.conn, "set %s %d %d %d\r\n", item.Key, item.Flags, item.Expiration, len(item.Value)); err != nil {
		return err
	}
	if _, err := t.conn.Write(append(item.Value, crlf...)); err != nil {
		return err
	}
	if err := t.conn.Flush(); err != nil {
		return err
	}
	buf, err := t.conn.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	case bytes.Equal(buf, msgStored):
		return nil
	default:
		return fmt.Errorf("memcached: failed set: %s", string(buf))
	}
}

func (t *textProtocol) Flush() error {
	if _, err := fmt.Fprint(t.conn, "flush_all"); err != nil {
		return err
	}
	r := bufio.NewReader(t.conn)
	b, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	case bytes.Equal(b, msgOk):
		return nil
	default:
		return fmt.Errorf("memcached: %s", string(b[:len(b)-2]))
	}
}
