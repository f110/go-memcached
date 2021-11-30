package proxy

import (
	"errors"

	"go.f110.dev/go-memcached/client"
	"go.f110.dev/go-memcached/cluster"
	merrors "go.f110.dev/go-memcached/errors"
	"go.f110.dev/go-memcached/server"
)

type ReplicaProxy struct {
	pool *cluster.ReplicaPool
}

func NewReplicaProxy(pool *cluster.ReplicaPool) *ReplicaProxy {
	return &ReplicaProxy{pool: pool}
}

func (p *ReplicaProxy) ServeRequest(req *server.Request) ([]*server.Response, error) {
	switch req.Opcode {
	case server.OpcodeGet:
		res, err := p.pool.Get(req.Key)
		if err != nil {
			if _, ok := err.(*merrors.MemcachedError); ok {
				return []*server.Response{{Error: err}}, nil
			}
			return nil, err
		}
		return []*server.Response{{Key: res.Key, Value: res.Value}}, nil
	case server.OpcodeSet:
		err := p.pool.Set(&client.Item{
			Key:        req.Key,
			Value:      req.Value,
			Flags:      req.Flags,
			Expiration: req.SetOpt.Expiration,
			Cas:        req.SetOpt.CasValue,
		})
		if err != nil {
			if _, ok := err.(*merrors.MemcachedError); ok {
				return []*server.Response{{Error: err}}, nil
			}
			return nil, err
		}
		return []*server.Response{{}}, nil
	default:
		return nil, errors.New("proxy: unknown opcode")
	}
}
