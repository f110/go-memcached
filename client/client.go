package client

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

const (
	ProtocolText   = "text"
	ProtocolBinary = "binary"
	ProtocolMeta   = "meta"
)

var (
	crlf                 = []byte("\r\n")
	msgTextProtoStored   = []byte("STORED\r\n")
	msgTextProtoDeleted  = []byte("DELETED\r\n")
	msgTextProtoNotFound = []byte("NOT_FOUND\r\n")
	msgTextProtoTouched  = []byte("TOUCHED\r\n")
	msgTextProtoOk       = []byte("OK\r\n")
	msgTextProtoEnd      = []byte("END\r\n")

	valueFormatWithCas = "VALUE %s %d %d %d"
	valueFormat        = "VALUE %s %d %d"
)

var (
	msgMetaProtoEnd     = []byte("EN\r\n")
	msgMetaProtoStored  = []byte("ST ")
	msgMetaProtoDeleted = []byte("DE ")
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

func NewClient(ctx context.Context, server, protocol string) (*Client, error) {
	var e engine

	switch protocol {
	case ProtocolText:
		engine, err := newTextProtocol(ctx, server)
		if err != nil {
			return nil, err
		}
		e = engine
	case ProtocolMeta:
		engine, err := newMetaProtocol(ctx, server)
		if err != nil {
			return nil, err
		}
		e = engine
	}
	return &Client{
		protocol: e,
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

func (c *Client) Touch(key string, expiration int) error {
	return c.protocol.Touch(key, expiration)
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
	if _, err := fmt.Fprintf(t.conn, "get %s\r\n", key); err != nil {
		return nil, err
	}
	if err := t.conn.Flush(); err != nil {
		return nil, err
	}
	b, err := t.conn.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if bytes.Equal(b, msgTextProtoEnd) {
		return nil, ItemNotFound
	}
	if !bytes.HasPrefix(b, []byte("VALUE")) {
		return nil, errors.New("memcached: invalid response")
	}
	item := &Item{}
	size := 0
	p := valueFormatWithCas
	scan := []interface{}{&item.Key, &item.Flags, &size, &item.Cas}
	if bytes.Count(b, []byte(" ")) == 3 {
		p = valueFormat
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

	b, err = t.conn.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(b, msgTextProtoEnd) {
		return nil, errors.New("memcached: invalid response")
	}

	return item, nil
}

func (t *textProtocol) GetMulti(keys ...string) ([]*Item, error) {
	if _, err := fmt.Fprintf(t.conn, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
		return nil, err
	}
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
	b, err := t.conn.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	case bytes.Equal(b, msgTextProtoDeleted):
		return nil
	case bytes.Equal(b, msgTextProtoNotFound):
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
	case bytes.Equal(b, msgTextProtoNotFound):
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
	case bytes.Equal(b, msgTextProtoTouched):
		return nil
	case bytes.Equal(b, msgTextProtoNotFound):
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
	case bytes.Equal(buf, msgTextProtoStored):
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
	case bytes.Equal(b, msgTextProtoOk):
		return nil
	default:
		return fmt.Errorf("memcached: %s", string(b[:len(b)-2]))
	}
}

type metaProtocol struct {
	server string
	conn   *bufio.ReadWriter
}

var _ engine = &metaProtocol{}

func newMetaProtocol(ctx context.Context, server string) (*metaProtocol, error) {
	d := &net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", server)
	if err != nil {
		return nil, err
	}
	return &metaProtocol{conn: bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))}, nil
}

func (m *metaProtocol) GetMulti(key ...string) ([]*Item, error) {
	panic("implement me")
}

func (m *metaProtocol) Get(key string) (*Item, error) {
	if _, err := fmt.Fprintf(m.conn, "mg %s svf\r\n", key); err != nil {
		return nil, err
	}
	if err := m.conn.Flush(); err != nil {
		return nil, err
	}
	b, err := m.conn.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if bytes.Equal(b, msgMetaProtoEnd) {
		return nil, ItemNotFound
	}
	if !bytes.HasPrefix(b, []byte("VA")) {
		return nil, errors.New("memcached: invalid response")
	}

	item := &Item{}
	size := 0
	scan := []interface{}{&size, &item.Flags}
	if _, err := fmt.Fscanf(bytes.NewReader(b), "VA svf %d %d", scan...); err != nil {
		return nil, err
	}

	buf := make([]byte, size+2)
	if _, err := io.ReadFull(m.conn, buf); err != nil {
		return nil, err
	}
	item.Value = buf[:size]

	b, err = m.conn.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(b, msgMetaProtoEnd) {
		return nil, errors.New("memcached: invalid response")
	}
	return item, nil
}

func (m *metaProtocol) Set(item *Item) error {
	if _, err := fmt.Fprintf(m.conn, "ms %s S %d\r\n", item.Key, len(item.Value)); err != nil {
		return err
	}
	if _, err := m.conn.Write(append(item.Value, crlf...)); err != nil {
		return err
	}
	if err := m.conn.Flush(); err != nil {
		return err
	}
	b, err := m.conn.ReadSlice('\n')
	if err != nil {
		return err
	}
	if bytes.HasPrefix(b, msgMetaProtoStored) {
		return nil
	}

	return errors.New("memcached: failed set")
}

func (m *metaProtocol) Delete(key string) error {
	if _, err := fmt.Fprintf(m.conn, "md %s\r\n", key); err != nil {
		return err
	}
	if err := m.conn.Flush(); err != nil {
		return err
	}
	b, err := m.conn.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	case bytes.HasPrefix(b, msgMetaProtoDeleted):
		return nil
	default:
		return errors.New("memcached: failed delete")
	}
}

// Incr is increment value when if exist key.
// implement is same as textProtocol.Incr.
func (m *metaProtocol) Incr(key string, delta int) (int, error) {
	return m.incrOrDecr("incr", key, delta)
}

// Decr is decrement value when if exist key.
// implement is same as textProtocol.Decr.
func (m *metaProtocol) Decr(key string, delta int) (int, error) {
	return m.incrOrDecr("decr", key, delta)
}

func (m *metaProtocol) incrOrDecr(op, key string, delta int) (int, error) {
	if _, err := fmt.Fprintf(m.conn, "%s %s %d\r\n", op, key, delta); err != nil {
		return 0, err
	}
	if err := m.conn.Flush(); err != nil {
		return 0, err
	}
	b, err := m.conn.ReadSlice('\n')
	if err != nil {
		return 0, err
	}

	switch {
	case bytes.Equal(b, msgTextProtoNotFound):
		return 0, ItemNotFound
	default:
		i, err := strconv.Atoi(string(b[:len(b)-2]))
		if err != nil {
			return 0, err
		}
		return i, nil
	}
}

func (m *metaProtocol) Touch(key string, expiration int) error {
	if _, err := fmt.Fprintf(m.conn, "md %s IT %d\r\n", key, expiration); err != nil {
		return err
	}
	if err := m.conn.Flush(); err != nil {
		return err
	}

	b, err := m.conn.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	case bytes.HasPrefix(b, msgMetaProtoDeleted):
		return nil
	default:
		return errors.New("memcached: failed touch")
	}
}

func (m *metaProtocol) Flush() error {
	if _, err := fmt.Fprint(m.conn, "flush_all"); err != nil {
		return err
	}
	r := bufio.NewReader(m.conn)
	b, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	case bytes.Equal(b, msgTextProtoOk):
		return nil
	default:
		return fmt.Errorf("memcached: %s", string(b[:len(b)-2]))
	}
}
