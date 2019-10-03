package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	ItemNotFound = errors.New("memcached: not found")
	ItemExists   = errors.New("memcached: item exists")
)

type Item struct {
	Key        string
	Value      []byte
	Flags      int
	Expiration int
	Cas        int
}

func (i *Item) marshalBinaryRequestHeader(h *binaryRequestHeader) []byte {
	extra := make([]byte, 8)
	binary.BigEndian.PutUint32(extra[4:8], uint32(i.Expiration))
	h.KeyLength = uint16(len(i.Key))
	h.ExtraLength = uint8(len(extra))
	h.TotalBodyLength = uint32(len(i.Key) + len(i.Value) + len(extra))
	return extra
}

type engine interface {
	Get(key string) (*Item, error)
	GetMulti(keys ...string) ([]*Item, error)
	Set(item *Item) error
	Add(item *Item) error
	Replace(item *Item) error
	Delete(key string) error
	Incr(key string, delta int) (int64, error)
	Decr(key string, delta int) (int64, error)
	Touch(key string, expiration int) error
	Flush() error
	Version() (map[string]string, error)
}

type Client struct {
	protocol engine
}

func NewClient(ctx context.Context, protocol string, servers ...*Server) (*Client, error) {
	var e engine

	ring := NewRing(servers...)

	switch protocol {
	case ProtocolText:
		e = newTextProtocol(ring)
	case ProtocolMeta:
		e = newMetaProtocol(ring)
	case ProtocolBinary:
		e = newBinaryProtocol(ring)
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

func (c *Client) Add(item *Item) error {
	return c.protocol.Add(item)
}

func (c *Client) Replace(item *Item) error {
	return c.protocol.Replace(item)
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

func (c *Client) Version() (map[string]string, error) {
	return c.protocol.Version()
}

type textProtocol struct {
	ring *Ring
}

var _ engine = &textProtocol{}

func newTextProtocol(ring *Ring) *textProtocol {
	return &textProtocol{ring: ring}
}

func (t *textProtocol) Get(key string) (*Item, error) {
	s := t.ring.Pick(key)
	if _, err := fmt.Fprintf(s.conn, "get %s\r\n", key); err != nil {
		return nil, err
	}
	if err := s.conn.Flush(); err != nil {
		return nil, err
	}
	b, err := s.conn.ReadSlice('\n')
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

	if _, err := io.ReadFull(s.conn, buf); err != nil {
		return nil, err
	}
	item.Value = buf[:size]

	b, err = s.conn.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(b, msgTextProtoEnd) {
		return nil, errors.New("memcached: invalid response")
	}

	return item, nil
}

func (t *textProtocol) GetMulti(keys ...string) ([]*Item, error) {
	keyMap := make(map[string][]string)
	for _, key := range keys {
		s := t.ring.Pick(key)
		if _, ok := keyMap[s.Name]; !ok {
			keyMap[s.Name] = make([]string, 0)
		}
		keyMap[s.Name] = append(keyMap[s.Name], key)
	}

	items := make([]*Item, 0, len(keys))
	for serverName, keys := range keyMap {
		s := t.ring.Find(serverName)
		if _, err := fmt.Fprintf(s.conn, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
			return nil, err
		}
		if err := s.conn.Flush(); err != nil {
			return nil, err
		}
		i, err := t.parseGetResponse(s.conn.Reader)
		if err != nil {
			return nil, err
		}
		items = append(items, i...)
	}

	return items, nil
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

func (t *textProtocol) Delete(key string) error {
	s := t.ring.Pick(key)
	if _, err := fmt.Fprintf(s.conn, "delete %s\r\n", key); err != nil {
		return err
	}
	if err := s.conn.Flush(); err != nil {
		return err
	}
	b, err := s.conn.ReadSlice('\n')
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
	s := t.ring.Pick(key)
	if _, err := fmt.Fprintf(s.conn, "%s %s %d\r\n", op, key, delta); err != nil {
		return 0, err
	}
	if err := s.conn.Flush(); err != nil {
		return 0, err
	}
	b, err := s.conn.ReadSlice('\n')
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
	s := t.ring.Pick(key)
	if _, err := fmt.Fprintf(s.conn, "touch %s %d\r\n", key, expiration); err != nil {
		return err
	}
	if err := s.conn.Flush(); err != nil {
		return err
	}
	b, err := s.conn.ReadSlice('\n')
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

func (t *textProtocol) Set(item *Item) error {
	s := t.ring.Pick(item.Key)
	if _, err := fmt.Fprintf(s.conn, "set %s %d %d %d\r\n", item.Key, item.Flags, item.Expiration, len(item.Value)); err != nil {
		return err
	}
	if _, err := s.conn.Write(append(item.Value, crlf...)); err != nil {
		return err
	}
	if err := s.conn.Flush(); err != nil {
		return err
	}
	buf, err := s.conn.ReadSlice('\n')
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

func (t *textProtocol) Add(item *Item) error {
	s := t.ring.Pick(item.Key)
	if _, err := fmt.Fprintf(s.conn, "add %s %d %d %d\r\n", item.Key, item.Flags, item.Expiration, len(item.Value)); err != nil {
		return err
	}
	s.conn.Write(item.Value)
	s.conn.Write(crlf)
	if err := s.conn.Flush(); err != nil {
		return err
	}

	b, err := s.conn.ReadSlice('\n')
	if err != nil {
		return err
	}
	if bytes.Equal(b, msgTextProtoStored) {
		return nil
	}

	return ItemExists
}

func (t *textProtocol) Replace(item *Item) error {
	s := t.ring.Pick(item.Key)
	if _, err := fmt.Fprintf(s.conn, "replace %s %d %d %d\r\n", item.Key, item.Flags, item.Expiration, len(item.Value)); err != nil {
		return err
	}
	s.conn.Write(item.Value)
	s.conn.Write(crlf)
	if err := s.conn.Flush(); err != nil {
		return err
	}

	b, err := s.conn.ReadSlice('\n')
	if err != nil {
		return err
	}
	if bytes.Equal(b, msgTextProtoStored) {
		return nil
	}

	return ItemNotFound
}

func (t *textProtocol) Flush() error {
	return t.ring.Each(func(s *Server) error {
		if _, err := fmt.Fprint(s.conn, "flush_all"); err != nil {
			return err
		}
		if err := s.conn.Flush(); err != nil {
			return err
		}
		b, err := s.conn.ReadSlice('\n')
		if err != nil {
			return err
		}

		switch {
		case bytes.Equal(b, msgTextProtoOk):
			return nil
		default:
			return fmt.Errorf("memcached: %s", string(b[:len(b)-2]))
		}
	})
}

func (t *textProtocol) Version() (map[string]string, error) {
	result := make(map[string]string)

	err := t.ring.Each(func(s *Server) error {
		if _, err := s.conn.WriteString("version\r\n"); err != nil {
			return err
		}
		if err := s.conn.Flush(); err != nil {
			return err
		}

		b, err := s.conn.ReadSlice('\n')
		if err != nil {
			return err
		}

		if len(b) > 0 {
			result[s.Name] = strings.TrimPrefix(string(b), "VERSION ")
		}
		return nil
	})

	return result, err
}

type metaProtocol struct {
	ring *Ring
}

var _ engine = &metaProtocol{}

func newMetaProtocol(ring *Ring) *metaProtocol {
	return &metaProtocol{ring: ring}
}

func (m *metaProtocol) Get(key string) (*Item, error) {
	s := m.ring.Pick(key)
	if _, err := fmt.Fprintf(s.conn, "mg %s svf\r\n", key); err != nil {
		return nil, err
	}
	if err := s.conn.Flush(); err != nil {
		return nil, err
	}

	return m.parseGetResponse(s.conn, "svf")
}

func (m *metaProtocol) GetMulti(keys ...string) ([]*Item, error) {
	keyMap := make(map[string][]string)
	for _, key := range keys {
		s := m.ring.Pick(key)
		if _, ok := keyMap[s.Name]; !ok {
			keyMap[s.Name] = make([]string, 0)
		}
		keyMap[s.Name] = append(keyMap[s.Name], key)
	}

	items := make([]*Item, 0)
	for serverName, keys := range keyMap {
		s := m.ring.Find(serverName)
		for _, key := range keys {
			if _, err := fmt.Fprintf(s.conn, "mg %s svfk\r\n", key); err != nil {
				return nil, err
			}
		}
		s.conn.WriteString("mn\r\n")
		if err := s.conn.Flush(); err != nil {
			return nil, err
		}

	MultiRead:
		for {
			item, err := m.parseGetResponse(s.conn, "svfk")
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
	}

	return items, nil
}

func (m *metaProtocol) parseGetResponse(conn *bufio.ReadWriter, flags string) (*Item, error) {
	b, err := conn.ReadSlice('\n')
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
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	item.Value = buf[:size]

	b, err = conn.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(b, msgMetaProtoEnd) {
		return nil, errors.New("memcached: invalid get response")
	}

	return item, nil
}

func (m *metaProtocol) Set(item *Item) error {
	s := m.ring.Pick(item.Key)
	if _, err := fmt.Fprintf(s.conn, "ms %s S %d\r\n", item.Key, len(item.Value)); err != nil {
		return err
	}
	if _, err := s.conn.Write(append(item.Value, crlf...)); err != nil {
		return err
	}
	if err := s.conn.Flush(); err != nil {
		return err
	}
	b, err := s.conn.ReadSlice('\n')
	if err != nil {
		return err
	}
	if bytes.HasPrefix(b, msgMetaProtoStored) {
		return nil
	}

	return errors.New("memcached: failed set")
}

func (m *metaProtocol) Add(item *Item) error {
	panic("Currently not supported meta command. Use text or binary protocol.")
}

func (m *metaProtocol) Replace(item *Item) error {
	panic("Currently not supported meta command. Use text or binary protocol")
}

func (m *metaProtocol) Delete(key string) error {
	s := m.ring.Pick(key)
	if _, err := fmt.Fprintf(s.conn, "md %s\r\n", key); err != nil {
		return err
	}
	if err := s.conn.Flush(); err != nil {
		return err
	}
	b, err := s.conn.ReadSlice('\n')
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
	s := m.ring.Pick(key)
	if _, err := fmt.Fprintf(s.conn, "%s %s %d\r\n", op, key, delta); err != nil {
		return 0, err
	}
	if err := s.conn.Flush(); err != nil {
		return 0, err
	}
	b, err := s.conn.ReadSlice('\n')
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
	s := m.ring.Pick(key)
	if _, err := fmt.Fprintf(s.conn, "md %s IT %d\r\n", key, expiration); err != nil {
		return err
	}
	if err := s.conn.Flush(); err != nil {
		return err
	}

	b, err := s.conn.ReadSlice('\n')
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
	return m.ring.Each(func(s *Server) error {
		if _, err := fmt.Fprint(s.conn, "flush_all"); err != nil {
			return err
		}
		b, err := s.conn.ReadSlice('\n')
		if err != nil {
			return err
		}

		switch {
		case bytes.Equal(b, msgTextProtoOk):
			return nil
		default:
			return fmt.Errorf("memcached: %s", string(b[:len(b)-2]))
		}
	})
}

func (m *metaProtocol) Version() (map[string]string, error) {
	result := make(map[string]string)
	err := m.ring.Each(func(s *Server) error {
		if _, err := s.conn.WriteString("version\r\n"); err != nil {
			return err
		}
		if err := s.conn.Flush(); err != nil {
			return err
		}

		b, err := s.conn.ReadSlice('\n')
		if err != nil {
			return err
		}
		if len(b) > 0 {
			result[s.Name] = strings.TrimPrefix(string(b), "VERSION ")
		}
		return nil
	})

	return result, err
}

type binaryProtocol struct {
	ring *Ring

	reqHeaderPool *sync.Pool
	resHeaderPool *sync.Pool
}

var _ engine = &binaryProtocol{}

func newBinaryProtocol(ring *Ring) *binaryProtocol {
	return &binaryProtocol{
		ring: ring,
		reqHeaderPool: &sync.Pool{New: func() interface{} {
			return newBinaryRequestHeader()
		}},
		resHeaderPool: &sync.Pool{New: func() interface{} {
			return newBinaryResponseHeader()
		}},
	}
}

func (b *binaryProtocol) Get(key string) (*Item, error) {
	s := b.ring.Pick(key)

	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	defer b.reqHeaderPool.Put(header)
	header.Reset()

	header.Opcode = binaryOpcodeGet
	header.KeyLength = uint16(len(key))
	header.TotalBodyLength = uint32(len(key))
	if err := header.EncodeTo(s.conn); err != nil {
		return nil, err
	}
	s.conn.Write([]byte(key))
	if err := s.conn.Flush(); err != nil {
		return nil, err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)

	if err := resHeader.Read(s.conn); err != nil {
		return nil, err
	}

	buf := make([]byte, resHeader.TotalBodyLength)
	if _, err := io.ReadFull(s.conn, buf); err != nil {
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
	keyMap := make(map[string][]string)
	for _, key := range keys {
		s := b.ring.Pick(key)
		if _, ok := keyMap[s.Name]; !ok {
			keyMap[s.Name] = make([]string, 0)
		}
		keyMap[s.Name] = append(keyMap[s.Name], key)
	}

	items := make([]*Item, 0, len(keys))
	for serverName, keys := range keyMap {
		s := b.ring.Find(serverName)

		header := b.reqHeaderPool.Get().(*binaryRequestHeader)
		for _, key := range keys {
			header.Reset()
			header.Opcode = binaryOpcodeGetKQ
			header.KeyLength = uint16(len(key))
			header.TotalBodyLength = uint32(len(key))
			if err := header.EncodeTo(s.conn); err != nil {
				return nil, err
			}
			s.conn.Write([]byte(key))
		}
		b.reqHeaderPool.Put(header)

		header = b.reqHeaderPool.Get().(*binaryRequestHeader)
		header.Reset()
		header.Opcode = binaryOpcodeNoop
		if err := header.EncodeTo(s.conn); err != nil {
			return nil, err
		}
		b.reqHeaderPool.Put(header)
		if err := s.conn.Flush(); err != nil {
			return nil, err
		}

		resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
		for {
			if err := resHeader.Read(s.conn); err != nil {
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
			if _, err := io.ReadFull(s.conn, buf); err != nil {
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
	}

	return items, nil
}

func (b *binaryProtocol) Set(item *Item) error {
	s := b.ring.Pick(item.Key)

	header := b.getReqHeader()
	defer b.putReqHeader(header)

	header.Opcode = binaryOpcodeSet
	extra := item.marshalBinaryRequestHeader(header)
	if err := header.EncodeTo(s.conn); err != nil {
		return err
	}
	s.conn.Write(extra)
	s.conn.Write([]byte(item.Key))
	s.conn.Write(item.Value)
	if err := s.conn.Flush(); err != nil {
		return err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)
	if err := resHeader.Read(s.conn); err != nil {
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

func (b *binaryProtocol) Add(item *Item) error {
	s := b.ring.Pick(item.Key)

	header := b.getReqHeader()
	defer b.putReqHeader(header)

	header.Opcode = binaryOpcodeAdd
	extra := item.marshalBinaryRequestHeader(header)
	if err := header.EncodeTo(s.conn); err != nil {
		return err
	}
	s.conn.Write(extra)
	s.conn.Write([]byte(item.Key))
	s.conn.Write(item.Value)
	if err := s.conn.Flush(); err != nil {
		return err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)
	if err := resHeader.Read(s.conn); err != nil {
		return err
	}
	if resHeader.Opcode != binaryOpcodeAdd {
		return errors.New("memcached: invalid response")
	}

	if resHeader.Status != 0 {
		switch resHeader.Status {
		case 2:
			return ItemExists
		default:
			if v, ok := binaryStatus[resHeader.Status]; ok {
				return fmt.Errorf("memcached: error %s", v)
			} else {
				return fmt.Errorf("memcached: unknown error %d", resHeader.Status)
			}
		}
	}

	return nil
}

func (b *binaryProtocol) Replace(item *Item) error {
	s := b.ring.Pick(item.Key)
	header := b.getReqHeader()
	defer b.putReqHeader(header)

	header.Opcode = binaryOpcodeReplace
	extra := item.marshalBinaryRequestHeader(header)
	if err := header.EncodeTo(s.conn); err != nil {
		return err
	}
	s.conn.Write(extra)
	s.conn.Write([]byte(item.Key))
	s.conn.Write(item.Value)
	if err := s.conn.Flush(); err != nil {
		return err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)
	if err := resHeader.Read(s.conn); err != nil {
		return err
	}
	if resHeader.Opcode != binaryOpcodeReplace {
		return errors.New("memcached: invalid response")
	}

	buf := make([]byte, resHeader.TotalBodyLength)
	if _, err := io.ReadFull(s.conn, buf); err != nil {
		return err
	}

	if resHeader.Status != 0 {
		switch resHeader.Status {
		case 1:
			return ItemNotFound
		default:
			if v, ok := binaryStatus[resHeader.Status]; ok {
				return fmt.Errorf("memcached: error %s", v)
			} else {
				return fmt.Errorf("memcached: unknown error %d", resHeader.Status)
			}
		}
	}

	return nil
}

func (b *binaryProtocol) Delete(key string) error {
	s := b.ring.Pick(key)
	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	defer b.reqHeaderPool.Put(header)
	header.Reset()

	header.Opcode = binaryOpcodeDelete
	header.KeyLength = uint16(len(key))
	header.TotalBodyLength = uint32(len(key))
	if err := header.EncodeTo(s.conn); err != nil {
		return err
	}
	s.conn.Write([]byte(key))
	if err := s.conn.Flush(); err != nil {
		return err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)
	if err := resHeader.Read(s.conn); err != nil {
		return err
	}
	if resHeader.Opcode != binaryOpcodeDelete {
		return errors.New("memcached: invalid response")
	}

	buf := make([]byte, resHeader.TotalBodyLength)
	if _, err := io.ReadFull(s.conn, buf); err != nil {
		return err
	}

	if resHeader.Status != 0 {
		switch resHeader.Status {
		case 1: // key not found
			return ItemNotFound
		default:
			if v, ok := binaryStatus[resHeader.Status]; ok {
				return fmt.Errorf("memcached: error %s", v)
			} else {
				return fmt.Errorf("memcached: unknown error %d", resHeader.Status)
			}
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
	s := b.ring.Pick(key)
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

	if err := header.EncodeTo(s.conn); err != nil {
		return 0, err
	}
	s.conn.Write(extra)
	s.conn.Write([]byte(key))
	if err := s.conn.Flush(); err != nil {
		return 0, err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)

	if err := resHeader.Read(s.conn); err != nil {
		return 0, err
	}
	if resHeader.Opcode != opcode {
		return 0, errors.New("memcached: invalid response")
	}

	var body []byte
	if resHeader.TotalBodyLength > 0 {
		buf := make([]byte, resHeader.TotalBodyLength)
		if _, err := io.ReadFull(s.conn, buf); err != nil {
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
	s := b.ring.Pick(key)
	extra := make([]byte, 4)
	binary.BigEndian.PutUint32(extra, uint32(expiration))
	header := b.reqHeaderPool.Get().(*binaryRequestHeader)
	defer b.reqHeaderPool.Put(header)
	header.Reset()

	header.Opcode = binaryOpcodeTouch
	header.ExtraLength = uint8(len(extra))
	header.KeyLength = uint16(len(key))
	header.TotalBodyLength = uint32(len(key) + len(extra))
	if err := header.EncodeTo(s.conn); err != nil {
		return err
	}
	s.conn.Write(extra)
	s.conn.Write([]byte(key))
	if err := s.conn.Flush(); err != nil {
		return err
	}

	resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
	defer b.resHeaderPool.Put(resHeader)

	if err := resHeader.Read(s.conn); err != nil {
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
	return b.ring.Each(func(s *Server) error {
		header := b.getReqHeader()
		defer b.putReqHeader(header)

		header.Opcode = binaryOpcodeFlush
		if err := header.EncodeTo(s.conn); err != nil {
			return err
		}
		if err := s.conn.Flush(); err != nil {
			return err
		}

		resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
		defer b.resHeaderPool.Put(resHeader)

		if err := resHeader.Read(s.conn); err != nil {
			return err
		}
		if resHeader.Opcode != binaryOpcodeFlush {
			return errors.New("memcached: invalid response")
		}

		return nil
	})
}

func (b *binaryProtocol) Version() (map[string]string, error) {
	result := make(map[string]string)
	err := b.ring.Each(func(s *Server) error {
		header := b.getReqHeader()
		defer b.putReqHeader(header)

		header.Opcode = binaryOpcodeVersion
		if err := header.EncodeTo(s.conn); err != nil {
			return err
		}
		if err := s.conn.Flush(); err != nil {
			return err
		}

		resHeader := b.resHeaderPool.Get().(*binaryResponseHeader)
		defer b.resHeaderPool.Put(resHeader)
		if err := resHeader.Read(s.conn); err != nil {
			return err
		}

		if resHeader.Status != 0 {
			if v, ok := binaryStatus[resHeader.Status]; ok {
				return fmt.Errorf("memcached: error %s", v)
			}
			return fmt.Errorf("memcached: unknown error %d", resHeader.Status)
		}

		buf := make([]byte, resHeader.TotalBodyLength)
		if _, err := io.ReadFull(s.conn, buf); err != nil {
			return err
		}

		result[s.Name] = string(buf)
		return nil
	})

	return result, err
}

func (b *binaryProtocol) getReqHeader() *binaryRequestHeader {
	h := b.reqHeaderPool.Get().(*binaryRequestHeader)
	h.Reset()
	return h
}

func (b *binaryProtocol) putReqHeader(h *binaryRequestHeader) {
	b.reqHeaderPool.Put(h)
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
	binary.BigEndian.PutUint64(h.buf[16:24], 0)                // cas

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
