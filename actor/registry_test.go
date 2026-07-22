package actor

import (
	"fmt"
	"sync"
	"testing"
)

type registryProcesser struct {
	pid *PID
}

func (p *registryProcesser) Start()               {}
func (p *registryProcesser) PID() *PID            { return p.pid }
func (p *registryProcesser) Send(*PID, any, *PID) {}
func (p *registryProcesser) Invoke([]Envelope)    {}
func (p *registryProcesser) Shutdown()            {}

func TestRegistryAddGetRemove(t *testing.T) {
	r := newRegistry(&Engine{})
	proc := &registryProcesser{pid: NewPID(localAddress, "one")}

	if !r.add(proc) {
		t.Fatal("add returned false for a new process")
	}
	if got := r.get(proc.pid); got != proc {
		t.Fatalf("get returned %p, want %p", got, proc)
	}

	r.remove(proc.pid)
	if got := r.get(proc.pid); got != nil {
		t.Fatalf("get after remove returned %p, want nil", got)
	}
}

func TestRegistryCollision(t *testing.T) {
	r := newRegistry(&Engine{})
	first := &registryProcesser{pid: NewPID(localAddress, "same")}
	second := &registryProcesser{pid: NewPID(localAddress, "same")}

	if !r.add(first) {
		t.Fatal("add returned false for the first process")
	}
	if r.add(second) {
		t.Fatal("add returned true for a duplicate ID")
	}
	if got := r.get(first.pid); got != first {
		t.Fatalf("collision replaced the first process: got %p, want %p", got, first)
	}
}

func TestRegistryGetMissing(t *testing.T) {
	r := newRegistry(&Engine{})

	if got := r.get(NewPID(localAddress, "missing")); got != nil {
		t.Fatalf("get returned %p for an unknown PID, want nil", got)
	}
	if got := r.get(nil); got != nil {
		t.Fatalf("get(nil) returned %p, want nil", got)
	}
}

func TestRegistryConcurrent(t *testing.T) {
	const (
		goroutines = 100
		operations = 100
		poolSize   = 32
	)

	r := newRegistry(&Engine{})
	procs := make([]*registryProcesser, poolSize)
	for i := range procs {
		procs[i] = &registryProcesser{pid: NewPID(localAddress, fmt.Sprintf("process-%d", i))}
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for worker := 0; worker < goroutines; worker++ {
		go func(worker int) {
			defer wg.Done()
			for operation := 0; operation < operations; operation++ {
				proc := procs[(worker+operation)%poolSize]
				switch operation % 3 {
				case 0:
					r.add(proc)
				case 1:
					r.get(proc.pid)
				case 2:
					r.remove(proc.pid)
				}
			}
		}(worker)
	}
	wg.Wait()

	present := 0
	for _, proc := range procs {
		if r.get(proc.pid) != nil {
			present++
		}
	}
	if got := r.len(); got != present {
		t.Fatalf("len returned %d, counted %d present processes", got, present)
	}
}

func BenchmarkRegistryGet(b *testing.B) {
	r := newRegistry(&Engine{})
	pid := NewPID(localAddress, "benchmark")
	r.add(&registryProcesser{pid: pid})

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if r.get(pid) == nil {
				b.Fatal("registered process not found")
			}
		}
	})
}
