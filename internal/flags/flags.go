// Package flags is the feature-flag evaluator.
//
// A flag is a JSON document stored as a config value:
//
//	{
//	  "percentage": 25,
//	  "on_for":  ["alice","bob"],
//	  "off_for": ["carol"]
//	}
//
// Evaluation rules, in order:
//
//  1. user_id is in off_for -> false ("override-off")
//  2. user_id is in on_for  -> true  ("override-on")
//  3. percentage == 0       -> false ("rollout-zero")
//  4. percentage >= 100     -> true  ("rollout-full")
//  5. Otherwise compute bucket = FNV-1a(flag_key + ":" + user_id) % 100.
//     enabled = bucket < percentage. ("rollout-bucket")
//
// FNV-1a is used (not a crypto hash) because we need stability,
// uniformity, and speed; not unpredictability. With the flag_key
// included in the hash input, two flags at the same percentage will
// bucket different user sets, which avoids the "all your flags flip
// for the same 10% of users" failure mode.
package flags

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
)

// Flag is the parsed flag document.
type Flag struct {
	Percentage int      `json:"percentage"`
	OnFor      []string `json:"on_for,omitempty"`
	OffFor     []string `json:"off_for,omitempty"`
}

// Decision is the result of Evaluate.
type Decision struct {
	Enabled bool
	// Reason is a short machine-readable token: "override-off",
	// "override-on", "rollout-zero", "rollout-full", "rollout-bucket",
	// "default-off" (flag missing or malformed -- caller handles).
	Reason string
	Bucket int // 0..99 or -1 if not bucketed
}

// Parse decodes a JSON flag document. Validates that percentage is
// 0..100 and that override lists do not overlap.
func Parse(raw []byte) (Flag, error) {
	var f Flag
	if len(raw) == 0 {
		return f, errors.New("flags: empty document")
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return f, fmt.Errorf("flags: parse: %w", err)
	}
	if f.Percentage < 0 || f.Percentage > 100 {
		return f, fmt.Errorf("flags: percentage out of range: %d", f.Percentage)
	}
	off := make(map[string]struct{}, len(f.OffFor))
	for _, u := range f.OffFor {
		off[u] = struct{}{}
	}
	for _, u := range f.OnFor {
		if _, dup := off[u]; dup {
			return f, fmt.Errorf("flags: user %q appears in both on_for and off_for", u)
		}
	}
	return f, nil
}

// Evaluate runs the rules above against flag for userID.
func Evaluate(flagKey string, f Flag, userID string) Decision {
	for _, u := range f.OffFor {
		if u == userID {
			return Decision{Enabled: false, Reason: "override-off", Bucket: -1}
		}
	}
	for _, u := range f.OnFor {
		if u == userID {
			return Decision{Enabled: true, Reason: "override-on", Bucket: -1}
		}
	}
	if f.Percentage <= 0 {
		return Decision{Enabled: false, Reason: "rollout-zero", Bucket: -1}
	}
	if f.Percentage >= 100 {
		return Decision{Enabled: true, Reason: "rollout-full", Bucket: -1}
	}
	b := bucket(flagKey, userID)
	return Decision{Enabled: b < f.Percentage, Reason: "rollout-bucket", Bucket: b}
}

func bucket(flagKey, userID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(flagKey))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(userID))
	return int(h.Sum32() % 100)
}
