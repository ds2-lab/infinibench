package benchclient

import (
	"context"
	"fmt"
	"log"
	"math"

	"github.com/go-redis/redis/v8"
	infinicache "github.com/mason-leap-lab/infinicache/client"
)

var (
	GenElasticCacheCluster = func(client *Redis, addrPattern string, nodes int, numSlots int) RedisClusterSlotsProvider {
		return func(ctx context.Context) ([]redis.ClusterSlot, error) {
			client.ctx = ctx
			if numSlots == 0 {
				numSlots = 16384
			}
			slots := make([]redis.ClusterSlot, nodes)
			slotStep := int(math.Floor(float64(numSlots) / float64(nodes)))
			remainder := numSlots - slotStep*nodes
			next := 0
			for i := 0; i < nodes; i++ {
				bonus := 0
				if remainder > 0 {
					bonus = 1
					remainder--
				}
				slots[i].Start = next
				slots[i].End = slots[i].Start + slotStep + bonus - 1
				next = slots[i].End + 1
				slots[i].Nodes = []redis.ClusterNode{{
					Addr: fmt.Sprintf(addrPattern, i+1),
				}}
			}
			log.Println(slots)
			return slots, nil
		}
	}
)

type RedisClusterSlotsProvider func(context.Context) ([]redis.ClusterSlot, error)

type Redis struct {
	*defaultClient
	backend redis.UniversalClient
	ctx     context.Context
}

func NewRedis(addr string) *Redis {
	backend := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
	})
	return NewRedisWithBackend(backend)
}

func NewRedisWithBackend(backend redis.UniversalClient) *Redis {
	//client := newSession(addr)
	client := &Redis{
		defaultClient: newDefaultClient("Redis: "),
		backend:       backend,
		ctx:           context.Background(),
	}
	client.setter = client.set
	client.getter = client.get
	return client
}

func NewElasticCache(addrPattern string, nodes int, numSlots int) *Redis {
	client := &Redis{
		defaultClient: newDefaultClient("Redis: "),
	}
	client.backend = redis.NewClusterClient(&redis.ClusterOptions{
		ClusterSlots:  GenElasticCacheCluster(client, addrPattern, nodes, numSlots),
		RouteRandomly: true,
	})
	client.setter = client.set
	client.getter = client.get
	return client
}

func (r *Redis) set(key string, val []byte) (err error) {
	return r.backend.Set(r.ctx, key, val, 0).Err()
}

func (r *Redis) get(key string) (infinicache.ReadAllCloser, error) {
	val, err := r.backend.Get(r.ctx, key).Bytes()
	if err == redis.Nil {
		return nil, infinicache.ErrNotFound
	} else if err != nil {
		return nil, err
	} else {
		return NewByteReader(val), nil
	}
}

func (r *Redis) Close() {
	if r.backend != nil {
		r.backend.Close()
		r.backend = nil
	}
}
