package retry

import (
	"sync"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// BudgetConfig bounds retries as a fraction of recent requests, so a dependency
// that fails every call sees at most (1 + Ratio) × the normal load instead of
// MaxAttempts × it.
type BudgetConfig struct {
	// Window over which requests and retries are counted (default 10s).
	Window time.Duration
	// Ratio of retries allowed per request in the window (default 0.2 = 20%).
	Ratio float64
	// MinRetriesPerWindow keeps retries possible at low request rates (default 10).
	MinRetriesPerWindow int
	Clock               resilience.Clock
}

// Budget is a sliding-window retry budget shared by every call to one
// dependency. Safe for concurrent use.
type Budget struct {
	cfg   BudgetConfig
	mu    sync.Mutex
	slots []budgetSlot
}

type budgetSlot struct {
	sec      int64
	requests int
	retries  int
}

// NewBudget builds a budget; zero fields take the documented defaults.
func NewBudget(cfg BudgetConfig) *Budget {
	if cfg.Window <= 0 {
		cfg.Window = 10 * time.Second
	}
	if cfg.Ratio <= 0 {
		cfg.Ratio = 0.2
	}
	if cfg.MinRetriesPerWindow <= 0 {
		cfg.MinRetriesPerWindow = 10
	}
	cfg.Clock = resilience.OrSystem(cfg.Clock)
	n := int(cfg.Window / time.Second)
	if n < 1 {
		n = 1
	}
	return &Budget{cfg: cfg, slots: make([]budgetSlot, n)}
}

// Deposit records one request (first attempt). Policies call it; callers that
// use Withdraw directly must call Deposit per logical request.
func (b *Budget) Deposit() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.slot(b.now()).requests++
}

// Withdraw asks for permission to retry once. It returns false when the budget
// is exhausted; nothing is consumed in that case.
func (b *Budget) Withdraw() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	var reqs, retries int
	for i := range b.slots {
		if b.slots[i].sec > now-int64(len(b.slots)) {
			reqs += b.slots[i].requests
			retries += b.slots[i].retries
		}
	}
	allowed := int(b.cfg.Ratio*float64(reqs)) + b.cfg.MinRetriesPerWindow
	if retries+1 > allowed {
		return false
	}
	b.slot(now).retries++
	return true
}

func (b *Budget) now() int64 { return b.cfg.Clock.Now().Unix() }

// slot returns the bucket for sec, resetting it if it held an older second.
func (b *Budget) slot(sec int64) *budgetSlot {
	s := &b.slots[int(sec%int64(len(b.slots)))]
	if s.sec != sec {
		*s = budgetSlot{sec: sec}
	}
	return s
}
