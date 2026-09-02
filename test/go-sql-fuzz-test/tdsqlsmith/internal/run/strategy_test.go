package run

import (
	"testing"

	"tdsqlsmith/internal/random"
)

func TestPickGenerationStrategyNoGapsReturnsQueryRandom(t *testing.T) {
	rng := random.New(1)
	for i := 0; i < 256; i++ {
		got := pickGenerationStrategy(rng, nil)
		if got != strategyQueryRandom {
			t.Fatalf("expected query_random without missing rules, got: %s", got)
		}
	}
}

func TestPickGenerationStrategyWithMissingRulesMayPickRuleSeed(t *testing.T) {
	rng := random.New(3)
	missingRules := []string{"query_specification", "joined_table", "window_offset_clause"}
	sawRuleSeed := false
	for i := 0; i < 256; i++ {
		got := pickGenerationStrategy(rng, missingRules)
		if got != strategyRuleSeed && got != strategyQueryRandom {
			t.Fatalf("unexpected strategy: %s", got)
		}
		if got == strategyRuleSeed {
			sawRuleSeed = true
		}
	}
	if !sawRuleSeed {
		t.Fatalf("expected rule_seed to be picked at least once with missing rules")
	}
}

func TestQueryRuleComplexGapScore(t *testing.T) {
	score := queryRuleComplexGapScore([]string{
		"joined_table",
		"fill_opt",
		"query_simple_or_subquery",
		"function_expression",
	})
	if score < 7 {
		t.Fatalf("unexpected complex gap score: %d", score)
	}
}

func TestAppendUniqueStrategies(t *testing.T) {
	got := appendUniqueStrategies(
		[]generationStrategy{strategyQueryRandom, strategyQueryRandom},
		strategyRuleSeed,
		strategyQueryRandom,
		strategyRuleSeed,
	)
	want := []generationStrategy{
		strategyQueryRandom,
		strategyRuleSeed,
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected len: got=%d want=%d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected order at %d: got=%v want=%v (all=%v)", i, got[i], want[i], got)
		}
	}
}
