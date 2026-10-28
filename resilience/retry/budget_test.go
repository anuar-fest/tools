package retry_test

import (
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience/internal/clock"
	"github.com/anuar-fest/tools/resilience/retry"
)

func TestBudget_RatioPlusMinimum(t *testing.T) {
	fc := clock.NewFake()
	b := retry.NewBudget(retry.BudgetConfig{Window: 10 * time.Second, Ratio: 0.2, MinRetriesPerWindow: 2, Clock: fc})

	// 10 requests → 2 (ratio) + 2 (min) = 4 retries allowed.
	for range 10 {
		b.Deposit()
	}
	for i := range 4 {
		if !b.Withdraw() {
			t.Fatalf("withdraw %d should be allowed", i)
		}
	}
	if b.Withdraw() {
		t.Fatal("5th withdraw should be refused")
	}
}

func TestBudget_WindowSlides(t *testing.T) {
	fc := clock.NewFake()
	b := retry.NewBudget(retry.BudgetConfig{Window: 3 * time.Second, Ratio: 1, MinRetriesPerWindow: 1, Clock: fc})
	b.Deposit()
	for i := range 2 {
		if !b.Withdraw() {
			t.Fatalf("1 request: 1 (ratio) + 1 (min) retries expected, refused at %d", i)
		}
	}
	if b.Withdraw() {
		t.Fatal("budget should be spent")
	}
	fc.Advance(3 * time.Second) // everything falls out of the window
	if !b.Withdraw() {
		t.Fatal("minimum should be available again after the window slides")
	}
}

func TestBudget_ZeroConfigHasDefaults(t *testing.T) {
	b := retry.NewBudget(retry.BudgetConfig{})
	for i := range 10 {
		if !b.Withdraw() {
			t.Fatalf("default minimum of 10 retries, refused at %d", i)
		}
	}
	if b.Withdraw() {
		t.Fatal("11th retry without requests should be refused")
	}
}
