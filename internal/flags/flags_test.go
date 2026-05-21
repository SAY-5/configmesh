package flags_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/SAY-5/configmesh/internal/flags"
)

func TestParse_ValidatesPercentage(t *testing.T) {
	_, err := flags.Parse([]byte(`{"percentage":150}`))
	if err == nil {
		t.Fatal("expected error for percentage>100")
	}
	_, err = flags.Parse([]byte(`{"percentage":-1}`))
	if err == nil {
		t.Fatal("expected error for negative percentage")
	}
}

func TestParse_RejectsOverlappingOverrides(t *testing.T) {
	_, err := flags.Parse([]byte(`{"percentage":50,"on_for":["x"],"off_for":["x"]}`))
	if err == nil {
		t.Fatal("expected error for overlap")
	}
}

func TestEvaluate_Overrides(t *testing.T) {
	f := flags.Flag{Percentage: 0, OnFor: []string{"alice"}, OffFor: []string{"bob"}}
	if d := flags.Evaluate("flag.x", f, "alice"); !d.Enabled || d.Reason != "override-on" {
		t.Fatalf("alice: %+v", d)
	}
	if d := flags.Evaluate("flag.x", f, "bob"); d.Enabled || d.Reason != "override-off" {
		t.Fatalf("bob: %+v", d)
	}
}

func TestEvaluate_ZeroAndFull(t *testing.T) {
	if d := flags.Evaluate("k", flags.Flag{Percentage: 0}, "u"); d.Enabled || d.Reason != "rollout-zero" {
		t.Fatalf("zero: %+v", d)
	}
	if d := flags.Evaluate("k", flags.Flag{Percentage: 100}, "u"); !d.Enabled || d.Reason != "rollout-full" {
		t.Fatalf("full: %+v", d)
	}
}

func TestEvaluate_StableUnderRepetition(t *testing.T) {
	f := flags.Flag{Percentage: 50}
	got := flags.Evaluate("flag.stable", f, "alice").Enabled
	for i := 0; i < 1000; i++ {
		if flags.Evaluate("flag.stable", f, "alice").Enabled != got {
			t.Fatalf("instability at iter %d", i)
		}
	}
}

// TestEvaluate_DistributionAtP50 asserts that at percentage=50 the
// bucketing is approximately uniform: out of N random users, the share
// enabled lands in [0.45, 0.55]. With N=10000 and a uniform-ish FNV
// hash this margin is comfortable; the test is here to catch a
// regression that, say, accidentally hashes only the user_id and
// produces identical answers across all flags.
func TestEvaluate_DistributionAtP50(t *testing.T) {
	const n = 10_000
	const p = 50
	f := flags.Flag{Percentage: p}
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test seed
	enabled := 0
	for i := 0; i < n; i++ {
		uid := fmt.Sprintf("user-%d-%d", rng.Int63(), i)
		if flags.Evaluate("flag.dist", f, uid).Enabled {
			enabled++
		}
	}
	ratio := float64(enabled) / float64(n)
	if ratio < 0.45 || ratio > 0.55 {
		t.Fatalf("distribution off: enabled %d / %d = %.3f, want 0.45..0.55", enabled, n, ratio)
	}
}

// TestEvaluate_DifferentFlagsBucketDifferently asserts that two flags
// at the same percentage do not bucket the same user set. If the hash
// did not include the flag_key, all flags would flip together for the
// same users.
func TestEvaluate_DifferentFlagsBucketDifferently(t *testing.T) {
	const n = 2000
	const p = 30
	f := flags.Flag{Percentage: p}
	matches := 0
	for i := 0; i < n; i++ {
		uid := fmt.Sprintf("user-%d", i)
		a := flags.Evaluate("flag.a", f, uid).Enabled
		b := flags.Evaluate("flag.b", f, uid).Enabled
		if a == b {
			matches++
		}
	}
	// If flags bucketed identically, matches would be ~n. If they were
	// independent, ~58% (p*p + (1-p)*(1-p) = .09+.49 = .58 for p=.3).
	// We assert <80% match, well below the identical-bucketing case
	// and above the perfectly-independent case to avoid flakes.
	if matches >= n*8/10 {
		t.Fatalf("flags appear correlated: %d/%d match", matches, n)
	}
}

func TestEvaluate_MonotonicInPercentage(t *testing.T) {
	// Property: for a fixed user, if percentage p1 < p2 and the rule
	// branch is rollout-bucket for both, enabled(p1) implies enabled(p2).
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test seed
	for i := 0; i < 500; i++ {
		uid := fmt.Sprintf("u%d", rng.Int())
		p1 := rng.Intn(99) + 1 // 1..99
		p2 := p1 + rng.Intn(100-p1)
		if p2 == p1 {
			continue
		}
		e1 := flags.Evaluate("flag.mono", flags.Flag{Percentage: p1}, uid).Enabled
		e2 := flags.Evaluate("flag.mono", flags.Flag{Percentage: p2}, uid).Enabled
		if e1 && !e2 {
			t.Fatalf("monotonicity violated: p1=%d enabled, p2=%d disabled, uid=%s", p1, p2, uid)
		}
	}
}
