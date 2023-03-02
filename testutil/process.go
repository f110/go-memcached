package testutil

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	args []string
}

type testContext interface {
	Fatal(...interface{})
	Fatalf(string, ...interface{})
	Log(...interface{})
}

func NewMemcachedProcess(t testContext, args []string) *MemcachedProcess {
	if memcachedBinaryPath == "" {
		if os.Getenv("RUN_UNDER_RUNFILES") != "" {
			// Running under the sandbox of Bazel
			filepath.Walk(os.Getenv("RUNFILES_DIR"), func(path string, info fs.FileInfo, err error) error {
				if strings.HasSuffix(path, "bin/memcached") {
					memcachedBinaryPath = path
					return errors.New("found")
				}
				return nil
			})
		}
	}
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

	p := &MemcachedProcess{Port: port, args: args}
	p.start(t)
	return p
}

func (m *MemcachedProcess) start(t testContext) {
	arg := []string{"-p", strconv.Itoa(m.Port)}
	if m.args != nil {
		arg = append(arg, m.args...)
	}
	cmd := exec.Command(memcachedBinaryPath, arg...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("%s: %v", memcachedBinaryPath, err)
	}
	m.cmd = cmd

	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		<-ticker.C
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", m.Port))
		if err != nil {
			continue
		}
		conn.Close()
		break
	}
	ticker.Stop()
}

func (m *MemcachedProcess) Stop(t testContext) {
	if err := m.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, err := m.cmd.Process.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func (m *MemcachedProcess) Restart(t testContext) {
	if err := m.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	_, err := m.cmd.Process.Wait()
	if err != nil {
		t.Fatal(err)
	}
	m.start(t)
}
