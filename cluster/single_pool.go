package cluster

import "github.com/f110/go-memcached/client"

type SinglePool struct {
	fullRing   *client.Ring
	normalRing *client.Ring
}

func NewSinglePool(servers []*client.ServerWithMetaProtocol) *SinglePool {
	normal := make([]client.Server, 0, len(servers))
	for _, v := range servers {
		if v.State() != client.ServerStateNormal {
			continue
		}
		normal = append(normal, v)
	}

	primaries := make([]client.Server, len(servers))
	for i := 0; i < len(servers); i++ {
		primaries[i] = servers[i]
	}
	primaryRing := client.NewRing(primaries...)

	return &SinglePool{fullRing: primaryRing}
}

func (p *SinglePool) Get(key string) (*client.Item, error) {
	s := p.fullRing.Pick(key)
	switch s.State() {
	case client.ServerStateDeleteOnly, client.ServerStateWriteOnly:
		s = p.normalRing.Pick(key)
	}

	return s.Get(key)
}

func (p *SinglePool) GetMulti(keys ...string) ([]*client.Item, error) {
	keyMap := make(map[string][]string)
	for _, key := range keys {
		s := p.fullRing.Pick(key)
		switch s.State() {
		case client.ServerStateDeleteOnly, client.ServerStateWriteOnly:
			s = p.normalRing.Next(key)
		}

		if _, ok := keyMap[s.Name()]; !ok {
			keyMap[s.Name()] = make([]string, 0)
		}
		keyMap[s.Name()] = append(keyMap[s.Name()], key)
	}

	result := make([]*client.Item, 0, len(keys))
	for serverName, keys := range keyMap {
		s := p.fullRing.Find(serverName)
		items, err := s.GetMulti(keys...)
		if err != nil {
			return nil, err
		}
		result = append(result, items...)
	}
	return result, nil
}

func (p *SinglePool) Set(item *client.Item) error {
	s := p.fullRing.Pick(item.Key)
	switch s.State() {
	case client.ServerStateDeleteOnly:
		s = p.normalRing.Pick(item.Key)
	case client.ServerStateWriteOnly:
		if err := s.Set(item); err != nil {
			return err
		}

		s = p.normalRing.Pick(item.Key)
	}

	return s.Set(item)
}

func (p *SinglePool) Add(item *client.Item) error {
	s := p.fullRing.Pick(item.Key)
	switch s.State() {
	case client.ServerStateDeleteOnly:
		s = p.normalRing.Pick(item.Key)
	case client.ServerStateWriteOnly:
		if err := s.Add(item); err != nil {
			return err
		}

		s = p.normalRing.Pick(item.Key)
	}

	return s.Add(item)
}

func (p *SinglePool) Replace(item *client.Item) error {
	s := p.fullRing.Pick(item.Key)
	switch s.State() {
	case client.ServerStateDeleteOnly:
		s = p.normalRing.Pick(item.Key)
	case client.ServerStateWriteOnly:
		if err := s.Replace(item); err != nil {
			return err
		}

		s = p.normalRing.Pick(item.Key)
	}

	return s.Replace(item)
}

func (p *SinglePool) Delete(key string) error {
	s := p.fullRing.Pick(key)
	switch s.State() {
	case client.ServerStateDeleteOnly, client.ServerStateWriteOnly:
		if err := s.Delete(key); err != nil {
			return err
		}
		s = p.normalRing.Pick(key)
	}

	return s.Delete(key)
}

func (p *SinglePool) Increment(key string, delta, expiration int) (int64, error) {
	s := p.fullRing.Pick(key)
	switch s.State() {
	case client.ServerStateDeleteOnly:
		s = p.normalRing.Pick(key)
	case client.ServerStateWriteOnly:
		_, err := s.Increment(key, delta, expiration)
		if err != nil {
			return 0, err
		}

		s = p.normalRing.Pick(key)
	}

	return s.Increment(key, delta, expiration)
}

func (p *SinglePool) Decrement(key string, delta, expiration int) (int64, error) {
	s := p.fullRing.Pick(key)
	switch s.State() {
	case client.ServerStateDeleteOnly:
		s = p.normalRing.Pick(key)
	case client.ServerStateWriteOnly:
		_, err := s.Decrement(key, delta, expiration)
		if err != nil {
			return 0, err
		}

		s = p.normalRing.Pick(key)
	}

	return s.Decrement(key, delta, expiration)
}
