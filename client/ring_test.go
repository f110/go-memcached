package client

import (
	"testing"
)

func TestRing_Pick(t *testing.T) {
	// crc32.ChecksumIEEE("test1") = 2326977762
	// crc32.ChecksumIEEE("test4") = 4208470125
	r := &Ring{
		nodes: []*Node{
			{hash: 1, Server: &ServerWithTextProtocol{name: "first"}},
			{hash: 2326977761, Server: &ServerWithTextProtocol{name: "second"}},
			{hash: 2326977763, Server: &ServerWithTextProtocol{name: "third"}},
			{hash: 3200000000, Server: &ServerWithTextProtocol{name: "fourth"}},
		},
	}
	s := r.Pick("test1")
	if s.Name() != "third" {
		t.Errorf("expect pick third server: %s", s.Name())
	}

	s = r.Pick("test4")
	if s.Name() != "first" {
		t.Errorf("expect pick first server: %s", s.Name())
	}
}

func TestRing_Next(t *testing.T) {
	// crc32.ChecksumIEEE("before-test1") = 21363832
	// crc32.ChecksumIEEE("test1") = 2326977762
	// crc32.ChecksumIEEE("test4") = 4208470125
	r := &Ring{
		nodes: []*Node{
			{hash: 2000000000, Server: &ServerWithTextProtocol{name: "first"}},
			{hash: 2326977761, Server: &ServerWithTextProtocol{name: "second"}},
			{hash: 2326977763, Server: &ServerWithTextProtocol{name: "third"}},
			{hash: 3200000000, Server: &ServerWithTextProtocol{name: "fourth"}},
		},
	}

	s := r.Next("before-test1")
	if s.Name() != "second" {
		t.Errorf("expect pick second server: %s", s.Name())
	}
	s = r.Next("test1")
	if s.Name() != "fourth" {
		t.Errorf("expect pick fourth server: %s", s.Name())
	}

	s = r.Next("test4")
	if s.Name() != "second" {
		t.Errorf("expect pick second server: %s", s.Name())
	}
}
