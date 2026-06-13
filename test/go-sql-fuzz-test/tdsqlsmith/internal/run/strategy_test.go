package run

import (
	"testing"

	"tdsqlsmith/internal/random"
)

func TestPickGenerationStrategyNoGapsSkipsCoverageStrategies(t *testing.T) {
	rng := random.New(1)
	for i := 0; i < 256; i++ {
		got := pickGenerationStrategy(rng, 0, nil)
		if got == strategyBranchCase || got == strategyRuleSeed {
			t.Fatalf("unexpected strategy without gaps: %s", got)
		}
	}
}

func TestPickGenerationStrategyBranchOnlySkipsRuleSeed(t *testing.T) {
	rng := random.New(2)
	for i := 0; i < 256; i++ {
		got := pickGenerationStrategy(rng, 12, nil)
		if got == strategyRuleSeed {
			t.Fatalf("unexpected rule-seed strategy with no missing rules")
		}
	}
}

func TestPickGenerationStrategyRuleOnlySkipsBranchCase(t *testing.T) {
	rng := random.New(3)
	missingRules := []string{"query_specification", "joined_table", "window_offset_clause"}
	for i := 0; i < 256; i++ {
		got := pickGenerationStrategy(rng, 0, missingRules)
		if got == strategyBranchCase {
			t.Fatalf("unexpected branch strategy with no missing branch cases")
		}
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
		[]generationStrategy{strategyQueryRandom, strategyQueryRandom, strategyBranchCase},
		strategyBranchCase,
		strategyRuleSeed,
		strategyWorkload,
		strategyRuleSeed,
	)
	want := []generationStrategy{
		strategyQueryRandom,
		strategyBranchCase,
		strategyRuleSeed,
		strategyWorkload,
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
