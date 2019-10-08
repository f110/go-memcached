package testutil

import (
	"flag"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"
)

var memcachedBinaryPath = ""

func init() {
	flag.StringVar(&memcachedBinaryPath, "memcached-binary", memcachedBinaryPath, "memcached program path")
}

var portFounder = &portGovernor{used: make(map[int]struct{})}

type portGovernor struct {
	mu   sync.Mutex
	used map[int]struct{}
}

func (p *portGovernor) Find() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	addr := l.Addr().(*net.TCPAddr)
	port := addr.Port
	p.used[addr.Port] = struct{}{}
	if err := l.Close(); err != nil {
		return 0, err
	}

	return port, nil
}

type MemcachedProcess struct {
	Port int
	cmd  *exec.Cmd
}

func NewMemcachedProcess(t *testing.T, args []string) *MemcachedProcess {
	if memcachedBinaryPath == "" {
		p, err := exec.LookPath("memcached")
		if err != nil {
			t.Fatal(err)
		}
		memcachedBinaryPath = p
	}

	port, err := portFounder.Find()
	if err != nil {
		t.Fatal(err)
	}
	arg := []string{"-p", strconv.Itoa(port)}
	if args != nil {
		arg = append(arg, args...)
	}
	cmd := exec.Command(memcachedBinaryPath, arg...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		<-ticker.C
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			continue
		}
		conn.Close()
		break
	}
	ticker.Stop()

	return &MemcachedProcess{Port: port, cmd: cmd}
}

func (m *MemcachedProcess) Stop(t *testing.T) {
	if err := m.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, err := m.cmd.Process.Wait()
	if err != nil {
		t.Fatal(err)
	}
}
