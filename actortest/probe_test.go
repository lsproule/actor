package actortest

import (
	"testing"
	"time"

	"github.com/lsproule/actor"
)

type dummyMsg struct{ val int }

func TestProbeExpect(t *testing.T) {
	e := NewEngine(t)
	p := NewProbe(t, e)

	e.Send(p.PID(), "hello")
	p.Expect("hello")
}

func TestProbeExpectType(t *testing.T) {
	e := NewEngine(t)
	p := NewProbe(t, e)

	e.Send(p.PID(), dummyMsg{val: 42})
	res := p.ExpectType(dummyMsg{})
	
	if res.(dummyMsg).val != 42 {
		t.Errorf("expected val 42, got %v", res)
	}
}

func TestProbeExpectNoMessage(t *testing.T) {
	e := NewEngine(t)
	p := NewProbe(t, e)
	
	p.ExpectNoMessage(50 * time.Millisecond)
}

func TestProbeReceivedSnapshot(t *testing.T) {
	e := NewEngine(t)
	p := NewProbe(t, e)

	e.Send(p.PID(), 1)
	e.Send(p.PID(), 2)
	e.Send(p.PID(), 3)

	p.Expect(3) // Wait until the 3rd message is processed

	snap := p.Received()
	if len(snap) < 3 {
		t.Fatalf("expected at least 3 messages, got %d", len(snap))
	}

	// Mutate slice to ensure isolation
	snap[0] = 99
	
	snap2 := p.Received()
	if snap2[0] == 99 {
		t.Fatal("mutating snapshot affected probe's internal state")
	}
}

// Fake TB para probar el fallo sin tumbar la suite principal
type mockTB struct {
	testing.TB
	fatalCalled bool
	lastMsg     string
}

func (m *mockTB) Fatalf(format string, args ...any) {
	m.fatalCalled = true
}
func (m *mockTB) Helper() {}

func TestProbeTimeoutMessage(t *testing.T) {
	e := NewEngine(t)
	mock := &mockTB{}
	
	p := NewProbe(mock, e).WithTimeout(10 * time.Millisecond)
	e.Send(p.PID(), "wrong_message")
	
	p.Expect("right_message")

	if !mock.fatalCalled {
		t.Fatal("expected probe to fail on timeout, but it didn't")
	}
}
