package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
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
	magicRequest  byte = 0x80
	magicResponse byte = 0x81
)

const (
	binaryOpcodeGet byte = iota
	binaryOpcodeSet
	binaryOpcodeAdd
	binaryOpcodeReplace
	binaryOpcodeDelete
	binaryOpcodeIncrement
	binaryOpcodeDecrement
	binaryOpcodeQuit
	binaryOpcodeFlush
	binaryOpcodeGetQ
	binaryOpcodeNoop
	binaryOpcodeVersion
	binaryOpcodeGetK
	binaryOpcodeGetKQ
	binaryOpcodeAppend
	binaryOpcodePrepend
	binaryOpcodeTouch byte = 28 // 0x1c
)

var binaryStatus = map[uint16]string{
	1:   "key not found",
	2:   "key exists",
	3:   "value too large",
	4:   "invalid arguments",
	6:   "non-numeric value",
	129: "unknown command",
	130: "out of memory",
}

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
	GetMulti(keys ...string) ([]*Item, error)
	Set(item *Item) error
	Delete(key string) error
	Incr(key string, delta int) (int64, error)
	Decr(key string, delta int) (int64, error)
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
	case ProtocolBinary:
		engine, err := newBinaryProtocol(ctx, server)
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

func (c *Client) Increment(key string, delta int) (int64, error) {
	return c.protocol.Incr(key, delta)
}

func (c *Client) Decrement(key string, delta int) (int64, error) {
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
	return t.parseGetResponse(t.conn.Reader)
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

func (t *textProtocol) Incr(key string, delta int) (int64, error) {
	return t.incrOrDecr("incr", key, delta)
}

func (t *textProtocol) Decr(key string, delta int) (int64, error) {
	return t.incrOrDecr("decr", key, delta)
}

func (t *textProtocol) incrOrDecr(op, key string, delta int) (int64, error) {
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
		i, err := strconv.ParseInt(string(b[:len(b)-2]), 10, 64)
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
	if err := t.conn.Flush(); err != nil {
		return err
	}
	b, err := t.conn.ReadSlice('\n')
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

func (m *metaProtocol) Get(key string) (*Item, error) {
	if _, err := fmt.Fprintf(m.conn, "mg %s svf\r\n", key); err != nil {
		return nil, err
	}
	if err := m.conn.Flush(); err != nil {
		return nil, err
	}

	return m.parseGetResponse("svf")
}

func (m *metaProtocol) GetMulti(keys ...string) ([]*Item, error) {
	for _, key := range keys {
		if _, err := fmt.Fprintf(m.conn, "mg %s svfk\r\n", key); err != nil {
			return nil, err
		}
	}
	m.conn.WriteString("mn\r\n")
	if err := m.conn.Flush(); err != nil {
		return nil, err
	}

	items := make([]*Item, 0)
MultiRead:
	for {
		item, err := m.parseGetResponse("svfk")
		if err != nil {
			switch err {
			case ItemNotFound:
				break MultiRead
			default:
				return nil, err
			}
		}
		items = append(items, item)
	}

	return items, nil
}

func (m *metaProtocol) parseGetResponse(flags string) (*Item, error) {
	b, err := m.conn.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if bytes.Equal(b, msgMetaProtoEnd) {
		return nil, ItemNotFound
	}
	if !bytes.HasPrefix(b, []byte("VA")) {
		return nil, errors.New("memcached: invalid get response")
	}
	item := &Item{}
	size := 0
	scan := make([]interface{}, 0, len(flags))
	format := ""
	for _, t := range flags {
		switch t {
		case 's':
			scan = append(scan, &size)
			format += " %d"
		case 'f':
			scan = append(scan, &item.Flags)
			format += " %d"
		case 'k':
			scan = append(scan, &item.Key)
			format += " %s"
		}
	}
	if _, err := fmt.Fscanf(bytes.NewReader(b), "VA "+flags+format, scan...); err != nil {
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
		return nil, errors.New("memcached: invalid get response")
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
func (m *metaProtocol) Incr(key string, delta int) (int64, error) {
	return m.incrOrDecr("incr", key, delta)
}

// Decr is decrement value when if exist key.
// implement is same as textProtocol.Decr.
func (m *metaProtocol) Decr(key string, delta int) (int64, error) {
	return m.incrOrDecr("decr", key, delta)
}

func (m *metaProtocol) incrOrDecr(op, key string, delta int) (int64, error) {
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
		i, err := strconv.ParseInt(string(b[:len(b)-2]), 10, 64)
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
	b, err := m.conn.ReadSlice('\n')
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

type binaryProtocol struct {
	server string
	conn   *bufio.ReadWriter

	reqHeaderPool *sync.Pool
	resHeaderPool *sync.Pool
}

var _ engine = &binaryProtocol{}

func newBinaryProtocol(ctx context.Context, server string) (*binaryProtocol, error) {
	d := &net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", server)
	if err != nil {
		return nil, err
	}
	return &binaryProtocol{
		conn: bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		reqHeaderPool: &sync.Pool{New: func() interface{} {
			return newBinaryRequestHeader()
		}},
		resHeaderPool: &sync.Pool{New: func() interface{} {
			return newBinaryResponseHeader()
		}},
	}, nil
}

func (b *binaryProtocol) Get(key string) (*Item, error) {
	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	defer b.reqHeaderPool.Put(header)
	header.Reset()

	header.Opcode = binaryOpcodeGet
	header.KeyLength = uint16(len(key))
	header.TotalBodyLength = uint32(len(key))
	if err := header.EncodeTo(b.conn); err != nil {
		return nil, err
	}
	b.conn.Write([]byte(key))
	if err := b.conn.Flush(); err != nil {
		return nil, err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)

	if err := resHeader.Read(b.conn); err != nil {
		return nil, err
	}

	buf := make([]byte, resHeader.TotalBodyLength)
	if _, err := io.ReadFull(b.conn, buf); err != nil {
		return nil, err
	}

	if resHeader.Status != 0 {
		if v, ok := binaryStatus[resHeader.Status]; ok {
			switch resHeader.Status {
			case 1: // key not found
				return nil, ItemNotFound
			default:
				return nil, fmt.Errorf("memcached: error %s", v)
			}
		}
		return nil, fmt.Errorf("memcached: unknown error %d", resHeader.Status)
	}

	return &Item{
		Key:   key,
		Value: buf[uint16(resHeader.ExtraLength)+resHeader.KeyLength:],
		Flags: int(binary.BigEndian.Uint32(buf[:resHeader.ExtraLength])),
	}, nil
}

func (b *binaryProtocol) GetMulti(keys ...string) ([]*Item, error) {
	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	for _, key := range keys {
		header.Reset()
		header.Opcode = binaryOpcodeGetKQ
		header.KeyLength = uint16(len(key))
		header.TotalBodyLength = uint32(len(key))
		if err := header.EncodeTo(b.conn); err != nil {
			return nil, err
		}
		b.conn.Write([]byte(key))
	}
	b.reqHeaderPool.Put(header)

	header = b.reqHeaderPool.Get().(*binaryRequestHeader)
	header.Reset()
	header.Opcode = binaryOpcodeNoop
	if err := header.EncodeTo(b.conn); err != nil {
		return nil, err
	}
	b.reqHeaderPool.Put(header)
	if err := b.conn.Flush(); err != nil {
		return nil, err
	}

	items := make([]*Item, 0)
	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	for {
		if err := resHeader.Read(b.conn); err != nil {
			return nil, err
		}
		if resHeader.Opcode == binaryOpcodeNoop {
			break
		}

		if resHeader.Status != 0 {
			if v, ok := binaryStatus[resHeader.Status]; ok {
				return nil, fmt.Errorf("memcached: error %s", v)
			}
			return nil, fmt.Errorf("memcached: unknown error %d", resHeader.Status)
		}

		buf := make([]byte, resHeader.TotalBodyLength)
		if _, err := io.ReadFull(b.conn, buf); err != nil {
			return nil, err
		}

		flags := 0
		if resHeader.ExtraLength > 0 {
			flags = int(binary.BigEndian.Uint32(buf[:resHeader.ExtraLength]))
		}
		items = append(items, &Item{
			Key:   string(buf[resHeader.ExtraLength : uint16(resHeader.ExtraLength)+resHeader.KeyLength]),
			Value: buf[uint16(resHeader.ExtraLength)+resHeader.KeyLength:],
			Flags: flags,
		})
	}

	return items, nil
}

func (b *binaryProtocol) Set(item *Item) error {
	extra := make([]byte, 8)
	binary.BigEndian.PutUint32(extra[4:8], uint32(item.Expiration))
	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	defer b.reqHeaderPool.Put(header)
	header.Reset()

	header.Opcode = binaryOpcodeSet
	header.KeyLength = uint16(len(item.Key))
	header.ExtraLength = uint8(len(extra))
	header.TotalBodyLength = uint32(len(item.Key) + len(item.Value) + len(extra))
	if err := header.EncodeTo(b.conn); err != nil {
		return err
	}
	b.conn.Write(extra)
	b.conn.Write([]byte(item.Key))
	b.conn.Write(item.Value)
	if err := b.conn.Flush(); err != nil {
		return err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)
	if err := resHeader.Read(b.conn); err != nil {
		return err
	}
	if resHeader.Opcode != binaryOpcodeSet {
		return errors.New("memcached: invalid response")
	}

	if resHeader.Status != 0 {
		if v, ok := binaryStatus[resHeader.Status]; ok {
			return fmt.Errorf("memcached: error %s", v)
		} else {
			return fmt.Errorf("memcached: unknown error %d", resHeader.Status)
		}
	}

	return nil
}

func (b *binaryProtocol) Delete(key string) error {
	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	defer b.reqHeaderPool.Put(header)
	header.Reset()

	header.Opcode = binaryOpcodeDelete
	header.KeyLength = uint16(len(key))
	header.TotalBodyLength = uint32(len(key))
	if err := header.EncodeTo(b.conn); err != nil {
		return err
	}
	b.conn.Write([]byte(key))
	if err := b.conn.Flush(); err != nil {
		return err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)
	if err := resHeader.Read(b.conn); err != nil {
		return err
	}
	if resHeader.Opcode != binaryOpcodeDelete {
		return errors.New("memcached: invalid response")
	}

	if resHeader.Status != 0 {
		if v, ok := binaryStatus[resHeader.Status]; ok {
			return fmt.Errorf("memcached: error %s", v)
		} else {
			return fmt.Errorf("memcached: unknown error %d", resHeader.Status)
		}
	}

	return nil
}

func (b *binaryProtocol) Incr(key string, delta int) (int64, error) {
	return b.incrOrDecr(binaryOpcodeIncrement, key, delta)
}

func (b *binaryProtocol) Decr(key string, delta int) (int64, error) {
	return b.incrOrDecr(binaryOpcodeDecrement, key, delta)
}

func (b *binaryProtocol) incrOrDecr(opcode byte, key string, delta int) (int64, error) {
	extra := make([]byte, 20)
	binary.BigEndian.PutUint64(extra[:8], uint64(delta))
	binary.BigEndian.PutUint64(extra[8:16], 1)
	binary.BigEndian.PutUint32(extra[16:20], 600)
	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	defer b.reqHeaderPool.Put(header)

	header.Opcode = opcode
	header.KeyLength = uint16(len(key))
	header.ExtraLength = uint8(len(extra))
	header.TotalBodyLength = uint32(len(key) + len(extra))

	if err := header.EncodeTo(b.conn); err != nil {
		return 0, err
	}
	b.conn.Write(extra)
	b.conn.Write([]byte(key))
	if err := b.conn.Flush(); err != nil {
		return 0, err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)

	if err := resHeader.Read(b.conn); err != nil {
		return 0, err
	}
	if resHeader.Opcode != opcode {
		return 0, errors.New("memcached: invalid response")
	}

	var body []byte
	if resHeader.TotalBodyLength > 0 {
		buf := make([]byte, resHeader.TotalBodyLength)
		if _, err := io.ReadFull(b.conn, buf); err != nil {
			return 0, err
		}
		body = buf
	}

	if resHeader.Status != 0 {
		if v, ok := binaryStatus[resHeader.Status]; ok {
			additional := ""
			if len(body) > 0 {
				additional = " (" + string(body) + ")"
			}
			return 0, fmt.Errorf("memcached: error %s%s", v, additional)
		} else {
			return 0, fmt.Errorf("memcached: unknown error %d (%s)", resHeader.Status, string(body))
		}
	}

	return int64(binary.BigEndian.Uint64(body)), nil
}

func (b *binaryProtocol) Touch(key string, expiration int) error {
	extra := make([]byte, 4)
	binary.BigEndian.PutUint32(extra, uint32(expiration))
	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	defer b.reqHeaderPool.Put(header)
	header.Reset()

	header.Opcode = binaryOpcodeTouch
	header.ExtraLength = uint8(len(extra))
	header.KeyLength = uint16(len(key))
	header.TotalBodyLength = uint32(len(key) + len(extra))
	if err := header.EncodeTo(b.conn); err != nil {
		return err
	}
	b.conn.Write(extra)
	b.conn.Write([]byte(key))
	if err := b.conn.Flush(); err != nil {
		return err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)

	if err := resHeader.Read(b.conn); err != nil {
		return err
	}

	if resHeader.Status != 0 {
		if v, ok := binaryStatus[resHeader.Status]; ok {
			return fmt.Errorf("memcached: error %s", v)
		} else {
			return fmt.Errorf("memcached: unknown error %d", resHeader.Status)
		}
	}

	return nil
}

func (b *binaryProtocol) Flush() error {
	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	defer b.reqHeaderPool.Put(header)
	header.Reset()

	header.Opcode = binaryOpcodeFlush
	if err := header.EncodeTo(b.conn); err != nil {
		return err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)

	if err := resHeader.Read(b.conn); err != nil {
		return err
	}
	if resHeader.Opcode != binaryOpcodeFlush {
		return errors.New("memcached: invalid response")
	}

	return nil
}

type binaryRequestHeader struct {
	Opcode          byte
	KeyLength       uint16
	ExtraLength     uint8
	DataType        byte
	VBucketId       uint16
	TotalBodyLength uint32
	Opaque          uint32
	CAS             uint64

	buf []byte
}

func newBinaryRequestHeader() *binaryRequestHeader {
	return &binaryRequestHeader{buf: make([]byte, 24)}
}

func (h *binaryRequestHeader) EncodeTo(w io.Writer) error {
	h.buf[0] = magicRequest
	h.buf[1] = h.Opcode
	binary.BigEndian.PutUint16(h.buf[2:4], h.KeyLength)        // ken len
	h.buf[4] = h.ExtraLength                                   // extra len
	h.buf[5] = h.DataType                                      // data type
	binary.BigEndian.PutUint16(h.buf[6:8], 0)                  // vbucket id
	binary.BigEndian.PutUint32(h.buf[8:12], h.TotalBodyLength) // total body len
	binary.BigEndian.PutUint32(h.buf[12:16], 0)                // opaque
	binary.BigEndian.PutUint64(h.buf[16:24], 0)                //cas

	if _, err := w.Write(h.buf); err != nil {
		return err
	}
	return nil
}

func (h *binaryRequestHeader) Reset() {
	h.Opcode = 0
	h.KeyLength = 0
	h.ExtraLength = 0
	h.DataType = 0
	h.VBucketId = 0
	h.TotalBodyLength = 0
	h.Opaque = 0
	h.CAS = 0
}

type binaryResponseHeader struct {
	Opcode          byte
	KeyLength       uint16
	ExtraLength     uint8
	DataType        byte
	Status          uint16
	TotalBodyLength uint32
	Opaque          uint32
	CAS             uint64

	buf []byte
}

func newBinaryResponseHeader() *binaryResponseHeader {
	return &binaryResponseHeader{buf: make([]byte, 24)}
}

func (h *binaryResponseHeader) Read(r io.Reader) error {
	if _, err := io.ReadFull(r, h.buf); err != nil {
		return err
	}
	if h.buf[0] != magicResponse {
		return errors.New("memcached: invalid response")
	}
	h.Opcode = h.buf[1]
	h.KeyLength = binary.BigEndian.Uint16(h.buf[2:4])
	h.ExtraLength = h.buf[4]
	h.DataType = h.buf[5]
	h.Status = binary.BigEndian.Uint16(h.buf[6:8])
	h.TotalBodyLength = binary.BigEndian.Uint32(h.buf[8:12])
	h.Opaque = binary.BigEndian.Uint32(h.buf[12:16])
	h.CAS = binary.BigEndian.Uint64(h.buf[16:24])

	return nil
}
