package cluster

import (
	"github.com/f110/go-memcached/client"
)

type ReplicaPool struct {
	primary   *client.Ring
	secondary *client.Ring
}

func NewReplicaPool(primary, secondary []client.Server) *ReplicaPool {
	primaryRing := client.NewRing(primary...)
	secondaryRing := client.NewRing(secondary...)
	return &ReplicaPool{
		primary:   primaryRing,
		secondary: secondaryRing,
	}
}

func (p *ReplicaPool) Get(key string) (*client.Item, error) {
	return p.primary.Pick(key).Get(key)
}

func (p *ReplicaPool) GetMulti(keys ...string) ([]*client.Item, error) {
	keyMap := make(map[string][]string)
	for _, key := range keys {
		s := p.primary.Pick(key)
		if _, ok := keyMap[s.Name()]; !ok {
			keyMap[s.Name()] = make([]string, 0)
		}
		keyMap[s.Name()] = append(keyMap[s.Name()], key)
	}

	result := make([]*client.Item, 0, len(keys))
	for serverName, keys := range keyMap {
		s := p.primary.Find(serverName)
		items, err := s.GetMulti(keys...)
		if err != nil {
			return nil, err
		}
		result = append(result, items...)
	}
	return result, nil
}

func (p *ReplicaPool) Set(item *client.Item) error {
	if err := p.primary.Pick(item.Key).Set(item); err != nil {
		return err
	}

	return p.secondary.Pick(item.Key).Set(item)
}

func (p *ReplicaPool) Add(item *client.Item) error {
	if err := p.primary.Pick(item.Key).Add(item); err != nil {
		return err
	}

	return p.secondary.Pick(item.Key).Set(item)
}

func (p *ReplicaPool) Replace(item *client.Item) error {
	if err := p.primary.Pick(item.Key).Replace(item); err != nil {
		return err
	}

	return p.secondary.Pick(item.Key).Set(item)
}

func (p *ReplicaPool) Delete(key string) error {
	if err := p.primary.Pick(key).Delete(key); err != nil {
		return err
	}

	return p.secondary.Pick(key).Delete(key)
}

func (p *ReplicaPool) Increment(key string, delta int) (int64, error) {
	s := p.primary.Pick(key)
	n, err := s.Increment(key, delta)
	if err != nil {
		return 0, err
	}
	item, err := s.Get(key)
	if err != nil {
		return n, err
	}
	err = p.secondary.Pick(key).Set(&client.Item{
		Key:        key,
		Value:      item.Value,
		Expiration: item.Expiration,
		Flags:      item.Flags,
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (p *ReplicaPool) Decrement(key string, delta int) (int64, error) {
	s := p.primary.Pick(key)
	n, err := s.Decrement(key, delta)
	if err != nil {
		return 0, err
	}
	item, err := s.Get(key)
	if err != nil {
		return n, err
	}
	err = p.secondary.Pick(key).Set(&client.Item{
		Key:        key,
		Value:      item.Value,
		Expiration: item.Expiration,
		Flags:      item.Flags,
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}
