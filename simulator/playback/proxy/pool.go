package proxy

import "sync"

var (
	PoolForPerformance       = PoolPerformanceOption(0)
	PoolForStrictConcurrency = PoolPerformanceOption(1)
)

type PoolPerformanceOption int

type Pool interface {
	Get() interface{}
	Release(i interface{})
	Put(i interface{})
	Close()
}

type ConcurrencyPool struct {
	New      func() interface{}
	Finalize func(interface{})

	capacity  int
	allocated int
	pooled    chan interface{}
	opt       PoolPerformanceOption

	mu   sync.Mutex
	cond *sync.Cond
}

func NewPool(cap int, opt PoolPerformanceOption) Pool {
	if cap == 0 {
		return &NilPool{}
	}
	return (&ConcurrencyPool{}).init(cap, opt)
}

func InitPool(p *ConcurrencyPool, cap int, opt PoolPerformanceOption) Pool {
	if cap == 0 {
		return &NilPool{
			New:      p.New,
			Finalize: p.Finalize,
		}
	}
	return p.init(cap, opt)
}

func (p *ConcurrencyPool) init(cap int, opt PoolPerformanceOption) *ConcurrencyPool {
	p.capacity = cap
	p.pooled = make(chan interface{}, p.capacity)
	p.opt = opt
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *ConcurrencyPool) Get() interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		select {
		case i := <-p.pooled:
			return i
		default:
			if p.allocated < p.capacity || p.opt&PoolForStrictConcurrency == 0 {
				p.allocated++
				if p.New == nil {
					return nil
				} else {
					return p.New()
				}
			}

			p.cond.Wait()
		}
	}
}

func (p *ConcurrencyPool) Release(i interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.allocated--
	if p.Finalize != nil {
		p.Finalize(i)
	}
}

func (p *ConcurrencyPool) Put(i interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case p.pooled <- i:
		p.cond.Signal()
	default:
		// Abandon. This is unlikely, but just in case.
		if p.Finalize != nil {
			p.Finalize(i)
		}
	}
}

func (p *ConcurrencyPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	finalize := p.Finalize
	if finalize == nil {
		finalize = p.defaultFinalizer
	}
	for len(p.pooled) > 0 {
		finalize(<-p.pooled)
	}
}

func (p *ConcurrencyPool) defaultFinalizer(i interface{}) {
}

type NilPool struct {
	New      func() interface{}
	Finalize func(interface{})
}

func (p *NilPool) Get() interface{} {
	if p.New == nil {
		return nil
	} else {
		return p.New()
	}
}

func (p *NilPool) Release(i interface{}) {
	if p.Finalize != nil {
		p.Finalize(i)
	}
}

func (p *NilPool) Put(i interface{}) {
	if p.Finalize != nil {
		p.Finalize(i)
	}
}

func (p *NilPool) Close() {
}
