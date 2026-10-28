package shed_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/internal/obstest"
	"github.com/anuar-fest/tools/resilience/shed"
)

func TestNew_Validates(t *testing.T) {
	if _, err := shed.New(shed.Config{}); err == nil {
		t.Fatal("MaxInFlight 0 must be rejected")
	}
	if _, err := shed.New(shed.Config{MaxInFlight: 10, NormalAbove: 0.5, BackgroundAbove: 0.8}); err == nil {
		t.Fatal("BackgroundAbove > NormalAbove must be rejected")
	}
}

func TestShedder_ClassesShedInOrder(t *testing.T) {
	rec := &obstest.Recorder{}
	s, err := shed.New(shed.Config{Name: "api", MaxInFlight: 10, Observer: rec})
	if err != nil {
		t.Fatal(err)
	}
	if s.Limit(shed.Critical) != 10 || s.Limit(shed.Normal) != 9 || s.Limit(shed.Background) != 7 {
		t.Fatalf("limits = %d/%d/%d", s.Limit(shed.Critical), s.Limit(shed.Normal), s.Limit(shed.Background))
	}
	var releases []func()
	admit := func(c shed.Class) bool {
		r, ok := s.Admit(context.Background(), c)
		if ok {
			releases = append(releases, r)
		}
		return ok
	}
	for range 7 {
		if !admit(shed.Background) {
			t.Fatal("background should be admitted below 70%")
		}
	}
	if admit(shed.Background) {
		t.Fatal("8th background must be shed")
	}
	for range 2 {
		if !admit(shed.Normal) {
			t.Fatal("normal admitted up to 9")
		}
	}
	if admit(shed.Normal) {
		t.Fatal("10th normal must be shed")
	}
	if !admit(shed.Critical) {
		t.Fatal("critical admitted up to 10")
	}
	if admit(shed.Critical) {
		t.Fatal("11th must be shed")
	}
	if rec.Count(resilience.DecisionShed) != 3 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
	for _, r := range releases {
		r()
		r() // idempotent
	}
	if s.InFlight() != 0 {
		t.Fatalf("inflight = %d after release", s.InFlight())
	}
}

func TestShedder_NeverExceedsCapUnderContention(t *testing.T) {
	s, _ := shed.New(shed.Config{Name: "api", MaxInFlight: 8})
	var peak, cur atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				release, ok := s.Admit(context.Background(), shed.Critical)
				if !ok {
					continue
				}
				n := cur.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				cur.Add(-1)
				release()
			}
		}()
	}
	wg.Wait()
	if peak.Load() > 8 {
		t.Fatalf("peak %d exceeded cap", peak.Load())
	}
	if s.InFlight() != 0 {
		t.Fatalf("leaked %d", s.InFlight())
	}
}
