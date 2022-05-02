package benchclient

import (
	"context"
	"time"

	infinicache "github.com/mason-leap-lab/infinicache/client"
	"github.com/zhangjyr/hashmap"
)

var (
	sizemap *hashmap.HashMap
)

func ResetDummySizeRegistry() {
	sizemap = hashmap.New(10000)
}

type Dummy struct {
	*defaultClient
	ctx       context.Context
	bandwidth int64
}

// NewDummy returns a new dummy client.
// bandwidth defined the bandwidth of the dummy client in B/s, 0 for unlimited.
func NewDummy(bandwidth int64) *Dummy {
	//client := newSession(addr)
	client := &Dummy{
		defaultClient: newDefaultClient("Dummy: "),
		ctx:           context.Background(),
		bandwidth:     bandwidth,
	}
	client.setter = client.set
	client.getter = client.get
	return client
}

func (d *Dummy) set(key string, val []byte) (err error) {
	sizemap.Set(key, len(val))

	if d.bandwidth == 0 {
		return nil
	}

	time.Sleep(d.sizeToDuration(len(val)))
	return nil
}

func (d *Dummy) get(key string) (infinicache.ReadAllCloser, error) {
	size, ok := sizemap.Get(key)
	if !ok {
		return nil, infinicache.ErrNotFound
	}

	if d.bandwidth == 0 {
		return &DummyReadAllCloser{size: size.(int)}, nil
	}
	time.Sleep(d.sizeToDuration(size.(int)))
	return &DummyReadAllCloser{size: size.(int)}, nil
}

func (d *Dummy) sizeToDuration(size int) time.Duration {
	return time.Duration(float64(size) / float64(d.bandwidth) * float64(time.Second))
}

type DummyReadAllCloser struct {
	size int
}

func (r *DummyReadAllCloser) Len() int {
	return r.size
}

func (r *DummyReadAllCloser) Read(p []byte) (n int, err error) {
	return n, ErrNotSupported
}

func (r *DummyReadAllCloser) ReadAll() ([]byte, error) {
	return nil, ErrNotSupported
}

func (r *DummyReadAllCloser) Close() error {
	return nil
}
