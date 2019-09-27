package memcached

import (
	"context"
	"log"
	"testing"
)

func TestClient_Get(t *testing.T) {
	c, err := NewClient(context.Background(), "localhost:11211")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Set(&Item{Key: "test", Value: []byte("OK")}); err != nil {
		t.Fatal(err)
	}
	item, err := c.Get("test")
	if err != nil {
		t.Fatal(err)
	}
	_ = item
}

func TestClient_Set(t *testing.T) {
	c, err := NewClient(context.Background(), "localhost:11211")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Set(&Item{Key: "client", Value: []byte("hoge")}); err != nil {
		t.Fatal(err)
	}
	item, err := c.Get("client")
	if err != nil {
		t.Fatal(err)
	}
	_ = item
}

func TestClient_GetMulti(t *testing.T) {
	c, err := NewClient(context.Background(), "localhost:11211")
	if err != nil {
		t.Fatal(err)
	}

	items, err := c.GetMulti("test", "client")
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 2 {
		t.Fatalf("expected returing 2 items: %d", len(items))
	}
	for _, v := range items {
		log.Print(v.Key)
	}
}

func TestClient_Delete(t *testing.T) {
	c, err := NewClient(context.Background(), "localhost:11211")
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Set(&Item{Key: t.Name(), Value: []byte("YES")}); err != nil {
		t.Fatal(err)
	}

	if err := c.Delete(t.Name()); err != nil {
		t.Fatal(err)
	}

	_, err = c.Get(t.Name())
	if err != ItemNotFound {
		t.Fatal(err)
	}
}

func Benchmark_Get(b *testing.B) {
	c, err := NewClient(context.Background(), "localhost:11211")
	if err != nil {
		b.Fatal(err)
	}
	if err := c.Set(&Item{Key: "bench", Value: []byte("OK")}); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := c.Get("bench")
		if err != nil {
			b.Fatal(err)
		}
	}
}
