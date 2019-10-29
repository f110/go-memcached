package server

import (
	"bufio"
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
	ProtocolAscii  Protocol = "ascii"
	ProtocolBinary Protocol = "binary"
)

var (
	magicRequest  byte = 0x80
	magicResponse byte = 0x81
)

const (
	OpcodeGet Opcode = iota
	OpcodeSet
	OpcodeAdd
	OpcodeReplace
	OpcodeDelete
	OpcodeIncrement
	OpcodeDecrement
	OpcodeQuit
	OpcodeFlush
	binaryOpcodeGetQ
	OpcodeNoop
	OpcodeVersion
	binaryOpcodeGetK
	OpcodeGetKQ
	binaryOpcodeAppend
	binaryOpcodePrepend
	OpcodeTouch Opcode = 0x1c
)

var (
	textProtoEnd     = []byte("END\r\n")
	textProtoOk      = []byte("OK\r\n")
	textProtoStored  = []byte("STORED\r\n")
	textProtoDeleted = []byte("DELETED\r\n")
	textProtoTouched = []byte("TOUCHED\r\n")
)

var (
	metaProtoEnd     = []byte("EN\r\n")
	metaProtoStored  = []byte("ST ")
	metaProtoDeleted = []byte("DE ")
)

var crlf = []byte("\r\n")

type Protocol string

type conn struct {
	Protocol Protocol

	connBuf *bufio.ReadWriter
	raw     net.Conn

	closeOnce sync.Once
}

func newConn(c net.Conn) *conn {
	return &conn{
		connBuf: bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c)),
		raw:     c,
	}
}

func (c *conn) detectProtocol() error {
	b, err := c.connBuf.Peek(1)
	if err != nil {
		return err
	}
	if b[0] == magicRequest {
		c.Protocol = ProtocolBinary
		return nil
	}

	c.Protocol = ProtocolAscii
	return nil
}

func (c *conn) close() {
	c.closeOnce.Do(func() {
		c.raw.Close()
	})
}

func (c *conn) readRequest() (*Request, error) {
	if c.Protocol == "" {
		if err := c.detectProtocol(); err != nil {
			return nil, err
		}
	}

	switch c.Protocol {
	case ProtocolAscii:
		return c.readAsciiRequest()
	case ProtocolBinary:
		return c.readBinaryRequest()
	}

	return nil, nil
}

func (c *conn) readAsciiRequest() (*Request, error) {
	line, err := c.connBuf.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	s := strings.Split(strings.TrimSuffix(string(line), "\r\n"), " ")
	if len(s) == 0 {
		return nil, errors.New("server: invalid request")
	}

	switch s[0] {
	case "get":
		if len(s) != 2 {
			return nil, errors.New("server: invalid request")
		}
		return &Request{Opcode: OpcodeGet, Key: s[1]}, nil
	case "gets":
		return &Request{Opcode: OpcodeGet, Keys: s[1:]}, nil
	case "set", "add", "replace":
		if len(s) != 5 {
			return nil, errors.New("server: invalid request")
		}
		f, err := strconv.ParseInt(s[2], 10, 32)
		if err != nil {
			return nil, err
		}
		flags := make([]byte, 4)
		binary.BigEndian.PutUint32(flags, uint32(f))
		expiration, err := strconv.ParseInt(s[3], 10, 32)
		if err != nil {
			return nil, err
		}

		bufLen, err := strconv.ParseInt(s[4], 10, 32)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, bufLen+2)
		if _, err := io.ReadFull(c.connBuf, buf); err != nil {
			return nil, err
		}
		buf = buf[:bufLen]

		opcode := OpcodeSet
		switch s[0] {
		case "add":
			opcode = OpcodeAdd
		case "replace":
			opcode = OpcodeReplace
		}

		return &Request{Opcode: opcode, Key: s[1], Flags: flags, Value: buf, SetOpt: &SetOpt{Expiration: int(expiration)}}, nil
	case "delete", "md":
		if len(s) != 2 {
			return nil, errors.New("server: invalid request")
		}
		opt := &DelOpt{}
		if s[0] == "md" {
			opt.Meta = true
		}
		return &Request{Opcode: OpcodeDelete, Key: s[1], DelOpt: opt}, nil
	case "incr", "decr", "touch":
		if len(s) != 3 {
			return nil, errors.New("server: invalid request")
		}
		d, err := strconv.ParseInt(s[2], 10, 32)
		if err != nil {
			return nil, err
		}
		opcode := OpcodeIncrement
		switch s[0] {
		case "decr":
			opcode = OpcodeDecrement
		case "touch":
			opcode = OpcodeTouch
		}
		return &Request{Opcode: opcode, Key: s[1], Delta: int64(d)}, nil
	case "mg":
		if len(s) < 3 {
			return nil, errors.New("server: invalid request")
		}
		opt := &GetOpt{Meta: true, RequestFlag: s[2]}
		if len(s[2]) > 0 {
			for _, f := range s[2] {
				switch f {
				case 'c':
					opt.Cas = true
				case 'f':
					opt.Flag = true
				case 'h':
					opt.Hit = true
				case 'k':
					opt.Key = true
				case 'l':
					opt.LastAccess = true
				case 'o':
					opt.WithOpaque = true
				case 'q':
					opt.NoReply = true
				case 's':
					opt.Size = true
				case 't':
					opt.TTL = true
				case 'v':
					opt.Value = true
				}
			}
		}
		return &Request{Opcode: OpcodeGet, Key: s[1], GetOpt: opt}, nil
	case "ms":
		if len(s) < 3 {
			return nil, errors.New("server: invalid request")
		}
		tokens := s[3:]
		opt := &SetOpt{Meta: true, RequestFlag: s[2]}
		if len(s[2]) > 0 {
			for _, f := range s[2] {
				switch f {
				case 'C':
					opt.Cas = true
					v, err := strconv.ParseUint(tokens[0], 10, 64)
					if err != nil {
						return nil, err
					}
					opt.CasValue = v
					tokens = tokens[1:]
				case 'F':
					v, err := strconv.ParseInt(tokens[0], 10, 32)
					if err != nil {
						return nil, err
					}
					opt.Flag = int32(v)
					tokens = tokens[1:]
				case 'I':
					opt.Invalidate = true
				case 'k':
					opt.Key = true
				case 'O':
					v, err := strconv.ParseInt(tokens[0], 10, 64)
					if err != nil {
						return nil, err
					}
					opt.Opaque = v
					tokens = tokens[1:]
				case 'q':
					opt.NoReply = true
				case 'S':
					v, err := strconv.ParseInt(tokens[0], 10, 32)
					if err != nil {
						return nil, err
					}
					opt.Size = int(v)
					tokens = tokens[1:]
				case 'T':
					v, err := strconv.ParseInt(tokens[0], 10, 32)
					if err != nil {
						return nil, err
					}
					opt.Expiration = int(v)
					tokens = tokens[1:]
				}
			}
		}

		buf := make([]byte, opt.Size+2)
		if _, err := io.ReadFull(c.connBuf, buf); err != nil {
			return nil, err
		}
		buf = buf[:opt.Size]

		return &Request{Opcode: OpcodeSet, Key: s[1], Value: buf, SetOpt: opt}, nil
	case "flush_all":
		return &Request{Opcode: OpcodeFlush}, nil
	case "version":
		return &Request{Opcode: OpcodeVersion}, nil
	}

	return nil, errors.New("server: invalid request")
}

func (c *conn) readBinaryRequest() (*Request, error) {
	header := make([]byte, 24)
	if _, err := io.ReadFull(c.connBuf, header); err != nil {
		return nil, err
	}
	if header[0] != magicRequest {
		return nil, errors.New("server: invalid request")
	}

	switch Opcode(header[1]) {
	case OpcodeGet:
		KeyLength := binary.BigEndian.Uint16(header[2:4])
		keyBuf := make([]byte, KeyLength)
		if _, err := io.ReadFull(c.connBuf, keyBuf); err != nil {
			return nil, err
		}

		return &Request{Opcode: OpcodeGet, Key: string(keyBuf)}, nil
	case OpcodeGetKQ:
		KeyLength := binary.BigEndian.Uint16(header[2:4])
		keyBuf := make([]byte, KeyLength)
		if _, err := io.ReadFull(c.connBuf, keyBuf); err != nil {
			return nil, err
		}

		return &Request{Opcode: OpcodeGet, Key: string(keyBuf), GetOpt: &GetOpt{NoReply: true, Key: true}}, nil
	case OpcodeSet, OpcodeAdd, OpcodeReplace:
		extra := make([]byte, header[4])
		if _, err := io.ReadFull(c.connBuf, extra); err != nil {
			return nil, err
		}
		expiration := binary.BigEndian.Uint32(extra[4:8])

		KeyLength := binary.BigEndian.Uint16(header[2:4])
		keyBuf := make([]byte, KeyLength)
		if _, err := io.ReadFull(c.connBuf, keyBuf); err != nil {
			return nil, err
		}

		totalBodyLen := binary.BigEndian.Uint32(header[8:12])
		buf := make([]byte, int(totalBodyLen)-len(extra)-int(KeyLength))
		if _, err := io.ReadFull(c.connBuf, buf); err != nil {
			return nil, err
		}

		return &Request{
			Opcode: Opcode(header[1]),
			Key:    string(keyBuf),
			Value:  buf,
			SetOpt: &SetOpt{Expiration: int(expiration)},
			Flags:  extra[:4],
		}, nil
	case OpcodeDelete:
		KeyLength := binary.BigEndian.Uint16(header[2:4])
		keyBuf := make([]byte, KeyLength)
		if _, err := io.ReadFull(c.connBuf, keyBuf); err != nil {
			return nil, err
		}

		return &Request{Opcode: OpcodeDelete, Key: string(keyBuf)}, nil
	case OpcodeIncrement, OpcodeDecrement:
		extra := make([]byte, header[4])
		if _, err := io.ReadFull(c.connBuf, extra); err != nil {
			return nil, err
		}
		delta := binary.BigEndian.Uint64(extra[:8])
		expiration := binary.BigEndian.Uint32(extra[16:20])

		KeyLength := binary.BigEndian.Uint16(header[2:4])
		keyBuf := make([]byte, KeyLength)
		if _, err := io.ReadFull(c.connBuf, keyBuf); err != nil {
			return nil, err
		}

		return &Request{
			Opcode: Opcode(header[1]),
			Key:    string(keyBuf),
			Delta:  int64(delta),
			SetOpt: &SetOpt{Expiration: int(expiration)},
		}, nil
	case OpcodeTouch:
		extra := make([]byte, header[4])
		if _, err := io.ReadFull(c.connBuf, extra); err != nil {
			return nil, err
		}
		expiration := binary.BigEndian.Uint32(extra[:4])

		KeyLength := binary.BigEndian.Uint16(header[2:4])
		keyBuf := make([]byte, KeyLength)
		if _, err := io.ReadFull(c.connBuf, keyBuf); err != nil {
			return nil, err
		}

		return &Request{Opcode: OpcodeTouch, Key: string(keyBuf), SetOpt: &SetOpt{Expiration: int(expiration)}}, nil
	case OpcodeNoop, OpcodeVersion, OpcodeFlush:
		return &Request{Opcode: Opcode(header[1])}, nil
	}

	return nil, errors.New("server: invalid request")
}

type Opcode byte

type Handler interface {
	ServeRequest(*Request) ([]*Response, error)
}

type Request struct {
	Opcode Opcode
	Key    string
	Keys   []string
	Flags  []byte
	Value  []byte
	Delta  int64
	GetOpt *GetOpt
	SetOpt *SetOpt
	DelOpt *DelOpt
}

type Response struct {
	Key   string
	Error error
	Flags []byte
	Value []byte
	Cas   int64
	TTL   int64
}

type GetOpt struct {
	Meta        bool
	RequestFlag string
	Cas         bool
	Flag        bool
	Key         bool
	LastAccess  bool
	Hit         bool
	NoReply     bool
	Size        bool
	TTL         bool
	WithOpaque  bool
	Opaque      int
	Value       bool
}

type SetOpt struct {
	Meta        bool
	RequestFlag string
	Cas         bool
	CasValue    uint64
	Flag        int32
	Invalidate  bool
	Key         bool
	Opaque      int64
	NoReply     bool
	Size        int
	Expiration  int
}

type DelOpt struct {
	Meta bool
}

type MetaFlag struct {
	Flag  rune
	Token string
}

type Server struct {
	Addr    string
	Handler Handler
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}

	return s.Serve(l)
}

func (s *Server) Serve(l net.Listener) error {
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		go func() {
			c := newConn(conn)
			defer c.close()

			_ = s.serve(c)
		}()
	}
}

func (s *Server) serve(conn *conn) error {
	for {
		req, err := conn.readRequest()
		if err != nil {
			break
		}

		if err := s.serveRequest(req, conn); err != nil {
			break
		}
	}

	return nil
}

func (s *Server) serveRequest(req *Request, conn *conn) error {
	res, err := s.Handler.ServeRequest(req)
	if err != nil {
		return err
	}

	return s.writeResponse(conn, req, res)
}

func (s *Server) writeResponse(conn *conn, req *Request, res []*Response) error {
	switch conn.Protocol {
	case ProtocolAscii:
		return s.writeAsciiResponse(conn, req, res)
	case ProtocolBinary:
		return s.writeBinaryResponse(conn, req, res)
	}

	return nil
}

func (s *Server) writeAsciiResponse(conn *conn, req *Request, res []*Response) error {
	switch req.Opcode {
	case OpcodeGet:
		if req.GetOpt == nil {
			for _, r := range res {
				if err := s.writeAsciiResponseGet(conn, r); err != nil {
					return err
				}
			}
			conn.connBuf.Write(textProtoEnd)
			if err := conn.connBuf.Flush(); err != nil {
				return err
			}
		} else {
			for _, r := range res {
				if err := s.writeAsciiResponseMetaGet(conn, req, r); err != nil {
					return err
				}
			}
			conn.connBuf.Write(metaProtoEnd)
			if err := conn.connBuf.Flush(); err != nil {
				return err
			}
		}
	case OpcodeSet, OpcodeAdd, OpcodeReplace:
		if res[0].Error == nil {
			if req.SetOpt.Meta {
				conn.connBuf.Write(metaProtoStored)
				conn.connBuf.Write(crlf)
				if err := conn.connBuf.Flush(); err != nil {
					return err
				}
			} else {
				conn.connBuf.Write(textProtoStored)
				if err := conn.connBuf.Flush(); err != nil {
					return err
				}
			}
		}
	case OpcodeDelete:
		if res[0].Error == nil {
			if req.DelOpt.Meta {
				conn.connBuf.Write(metaProtoDeleted)
				conn.connBuf.Write(crlf)
				if err := conn.connBuf.Flush(); err != nil {
					return err
				}
			} else {
				conn.connBuf.Write(textProtoDeleted)
				if err := conn.connBuf.Flush(); err != nil {
					return err
				}
			}
		}
	case OpcodeIncrement, OpcodeDecrement:
		if res[0].Error == nil {
			conn.connBuf.Write(res[0].Value)
			conn.connBuf.Write(crlf)
			if err := conn.connBuf.Flush(); err != nil {
				return err
			}
		}
	case OpcodeVersion:
		if res[0].Error == nil {
			conn.connBuf.Write(res[0].Value)
			conn.connBuf.Write(crlf)
			if err := conn.connBuf.Flush(); err != nil {
				return err
			}
		}
	case OpcodeTouch:
		if res[0].Error == nil {
			conn.connBuf.Write(textProtoTouched)
			if err := conn.connBuf.Flush(); err != nil {
				return err
			}
		}
	case OpcodeFlush:
		if res[0].Error == nil {
			conn.connBuf.Write(textProtoOk)
			if err := conn.connBuf.Flush(); err != nil {
				return err
			}
		}
	default:
		return errors.New("memcached: unknown opcode")
	}

	return nil
}

func (s *Server) writeAsciiResponseGet(conn *conn, r *Response) error {
	flag := uint32(0)
	if len(r.Flags) > 0 {
		flag = binary.BigEndian.Uint32(r.Flags)
	}
	_, err := conn.connBuf.WriteString(
		fmt.Sprintf("VALUE %s %d %d\r\n",
			r.Key,
			flag,
			len(r.Value),
		),
	)
	if err != nil {
		return err
	}
	if _, err := conn.connBuf.Write(r.Value); err != nil {
		return err
	}
	if _, err := conn.connBuf.Write(crlf); err != nil {
		return err
	}

	return nil
}
func (s *Server) writeAsciiResponseMetaGet(conn *conn, req *Request, r *Response) error {
	flag := uint32(0)
	if len(r.Flags) > 0 {
		flag = binary.BigEndian.Uint32(r.Flags)
	}
	_, err := conn.connBuf.WriteString(
		fmt.Sprintf("VA %s",
			req.GetOpt.RequestFlag,
		),
	)
	if err != nil {
		return err
	}
	for _, v := range req.GetOpt.RequestFlag {
		switch v {
		case 'c':
			conn.connBuf.WriteString(" " + strconv.FormatInt(r.Cas, 10))
		case 'f':
			conn.connBuf.WriteString(" " + strconv.FormatInt(int64(flag), 10))
		case 'k':
			conn.connBuf.WriteString(" " + req.Key)
		case 's':
			conn.connBuf.WriteString(" " + strconv.FormatInt(int64(len(r.Value)), 10))
		case 't':
			conn.connBuf.WriteString(" " + strconv.FormatInt(r.TTL, 10))
		}
	}
	conn.connBuf.Write(crlf)
	if req.GetOpt.Value {
		if _, err := conn.connBuf.Write(r.Value); err != nil {
			return err
		}
	}
	if _, err := conn.connBuf.Write(crlf); err != nil {
		return err
	}

	return nil
}

func (s *Server) writeBinaryResponse(conn *conn, req *Request, res []*Response) error {
	switch req.Opcode {
	case OpcodeGet:
		for _, v := range res {
			if err := v.writeAsBinaryProtocol(conn, req); err != nil {
				return err
			}
		}
		if len(res) > 1 {
			r := &Response{}
			if err := r.writeAsBinaryProtocol(conn, &Request{Opcode: OpcodeNoop}); err != nil {
				return err
			}
		}
	case OpcodeSet, OpcodeAdd, OpcodeReplace, OpcodeDelete, OpcodeIncrement, OpcodeDecrement,
		OpcodeVersion, OpcodeFlush, OpcodeTouch, OpcodeNoop:
		if res[0].Error == nil {
			return res[0].writeAsBinaryProtocol(conn, req)
		}
	default:
		return errors.New("memcached: unknown opcode")
	}

	return nil
}

func (r *Response) writeAsBinaryProtocol(conn *conn, req *Request) error {
	header := make([]byte, 24)
	header[0] = magicResponse
	header[1] = byte(req.Opcode)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(r.Key)))
	header[4] = uint8(len(r.Flags))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(r.Key)+len(r.Flags)+len(r.Value)))

	conn.connBuf.Write(header)
	if len(r.Flags) > 0 {
		conn.connBuf.Write(r.Flags)
	}
	if len(r.Key) > 0 {
		conn.connBuf.Write([]byte(r.Key))
	}
	if len(r.Value) > 0 {
		conn.connBuf.Write(r.Value)
	}
	if err := conn.connBuf.Flush(); err != nil {
		return err
	}

	return nil
}
