package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"go.f110.dev/go-memcached/client"
	"go.f110.dev/go-memcached/testutil"
)

type testReplicaPool struct {
	primaryProcess   *testutil.MemcachedProcess
	secondaryProcess *testutil.MemcachedProcess
	replicaPool      *ReplicaPool
}

func newTestReplicaPool(t *testing.T) *testReplicaPool {
	primaryProcess := testutil.NewMemcachedProcess(t, nil)
	secondaryProcess := testutil.NewMemcachedProcess(t, nil)

	primaryServer, err := client.NewServerWithMetaProtocol(context.Background(), "primary", "tcp", fmt.Sprintf("localhost:%d", primaryProcess.Port))
	if err != nil {
		t.Fatal(err)
	}
	secondaryServer, err := client.NewServerWithMetaProtocol(context.Background(), "secondary", "tcp", fmt.Sprintf("localhost:%d", secondaryProcess.Port))
	if err != nil {
		t.Fatal(err)
	}

	return &testReplicaPool{
		primaryProcess:   primaryProcess,
		secondaryProcess: secondaryProcess,
		replicaPool:      NewReplicaPool([]*client.ServerWithMetaProtocol{primaryServer}, []*client.ServerWithMetaProtocol{secondaryServer}),
	}
}

func (r *testReplicaPool) Stop(t *testing.T) {
	r.primaryProcess.Stop(t)
	r.secondaryProcess.Stop(t)
}

func TestReplicaPool_Get(t *testing.T) {
	r := newTestReplicaPool(t)
	defer r.Stop(t)
	replicaPool := r.replicaPool

	if err := replicaPool.Set(&client.Item{Key: t.Name(), Value: []byte("OK")}); err != nil {
		t.Fatal(err)
	}
	item, err := replicaPool.Get(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if item.Key != t.Name() {
		t.Errorf("key is %s", item.Key)
	}
	if !bytes.Equal(item.Value, []byte("OK")) {
		t.Errorf("unexpected value: % 2x", item.Value)
	}
}

func TestReplicaPool_GetMulti(t *testing.T) {
	r := newTestReplicaPool(t)
	defer r.Stop(t)
	replicaPool := r.replicaPool

	if err := replicaPool.Set(&client.Item{Key: t.Name(), Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := replicaPool.Set(&client.Item{Key: t.Name() + "2", Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	items, err := replicaPool.GetMulti(t.Name(), t.Name()+"2")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Errorf("expect got 2 items: %d", len(items))
	}
}

func TestReplicaPool_Add(t *testing.T) {
	r := newTestReplicaPool(t)
	defer r.Stop(t)
	replicaPool := r.replicaPool

	// Cleanup before Add
	if err := replicaPool.Delete(t.Name()); err != nil {
		t.Fatal(err)
	}
	if err := replicaPool.Add(&client.Item{Key: t.Name(), Value: []byte("OK")}); err != nil {
		t.Fatal(err)
	}
}

func TestReplicaPool_Replace(t *testing.T) {
	r := newTestReplicaPool(t)
	defer r.Stop(t)
	replicaPool := r.replicaPool

	if err := replicaPool.Set(&client.Item{Key: t.Name(), Value: []byte("OK")}); err != nil {
		t.Fatal(err)
	}
	if err := replicaPool.Replace(&client.Item{Key: t.Name(), Value: []byte("FOOBAR")}); err != nil {
		t.Fatal(err)
	}
}

func TestReplicaPool_Increment(t *testing.T) {
	r := newTestReplicaPool(t)
	defer r.Stop(t)
	replicaPool := r.replicaPool

	flag := make([]byte, 4)
	binary.BigEndian.PutUint32(flag, 1)
	if err := replicaPool.Set(&client.Item{Key: t.Name(), Value: []byte("1"), Expiration: 10, Flags: flag}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	if n, err := replicaPool.Increment(t.Name(), 1, 10); err != nil {
		t.Fatal(err)
	} else {
		if n != 2 {
			t.Error("unexpected value")
		}
	}
	item, err := replicaPool.secondary.Pick(t.Name()).Get(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if item.Value[0] != '2' {
		t.Error("unexpected value")
	}
	if item.Expiration != 9 {
		t.Errorf("expiration not copied: %d", item.Expiration)
	}
	if !bytes.Equal(item.Flags, flag) {
		t.Errorf("unexpected flag: % 2x", item.Flags)
	}
}

func TestReplicaPool_Decrement(t *testing.T) {
	r := newTestReplicaPool(t)
	defer r.Stop(t)
	replicaPool := r.replicaPool

	flag := make([]byte, 4)
	binary.BigEndian.PutUint32(flag, 1)
	if err := replicaPool.Set(&client.Item{Key: t.Name(), Value: []byte("10"), Expiration: 10, Flags: flag}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	if n, err := replicaPool.Decrement(t.Name(), 1, 10); err != nil {
		t.Fatal(err)
	} else {
		if n != 9 {
			t.Errorf("unexpected value: %d", n)
		}
	}
	item, err := replicaPool.secondary.Pick(t.Name()).Get(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if item.Value[0] != '9' {
		t.Error("unexpected value")
	}
	if item.Expiration != 9 {
		t.Errorf("expiration not copied: %d", item.Expiration)
	}
	if !bytes.Equal(item.Flags, flag) {
		t.Errorf("unexpected flag: % 2x", item.Flags)
	}
}
