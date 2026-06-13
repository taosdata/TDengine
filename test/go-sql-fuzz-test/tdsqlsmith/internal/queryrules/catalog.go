package queryrules

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type Catalog struct {
	required    []string
	requiredSet map[string]struct{}
	production  []string // 1-based index: production id -> lhs
}

func LoadCatalog(sqlparseRoot string) (*Catalog, error) {
	if strings.TrimSpace(sqlparseRoot) == "" {
		return nil, fmt.Errorf("empty sqlparse root")
	}
	scriptPath := filepath.Join(sqlparseRoot, "tool", "migrate", "query_rule_diff.sh")
	preferred, err := loadQueryRuleList(scriptPath)
	if err != nil {
		preferred = append([]string(nil), defaultQueryRules...)
	}
	if len(preferred) == 0 {
		return nil, fmt.Errorf("empty query rule list")
	}

	grammarPath := filepath.Join(sqlparseRoot, "td_sql.y")
	production, rules, err := loadProductionAndRules(grammarPath)
	if err != nil {
		return nil, err
	}
	return buildCatalog(preferred, production, rules)
}

func LoadCatalogFromContent(queryRuleScript string, grammar string) (*Catalog, error) {
	preferred, err := loadQueryRuleListFromContent(queryRuleScript)
	if err != nil {
		preferred = append([]string(nil), defaultQueryRules...)
	}
	if len(preferred) == 0 {
		return nil, fmt.Errorf("empty query rule list")
	}

	production, rules, err := loadProductionAndRulesContent(grammar, "embedded/td_sql.y")
	if err != nil {
		return nil, err
	}
	return buildCatalog(preferred, production, rules)
}

func buildCatalog(preferred []string, production []string, rules map[string][]string) (*Catalog, error) {
	reachable := deriveReachableQueryRules(rules)
	if len(reachable) == 0 {
		return nil, fmt.Errorf("empty reachable query rules from grammar")
	}
	required := mergeRequiredRuleOrder(preferred, reachable)

	reqSet := make(map[string]struct{}, len(required))
	for _, r := range required {
		reqSet[r] = struct{}{}
	}

	return &Catalog{
		required:    required,
		requiredSet: reqSet,
		production:  production,
	}, nil
}

func (c *Catalog) RequiredRules() []string {
	if c == nil {
		return nil
	}
	out := make([]string, len(c.required))
	copy(out, c.required)
	return out
}

func (c *Catalog) RuleFromReduction(id int) string {
	if c == nil || id <= 0 || id >= len(c.production) {
		return ""
	}
	return c.production[id]
}

func (c *Catalog) QueryRulesFromReductions(ids []int) []string {
	if c == nil || len(ids) == 0 {
		return nil
	}
	set := make(map[string]struct{}, 8)
	for _, id := range ids {
		lhs := c.RuleFromReduction(id)
		if lhs == "" {
			continue
		}
		if _, ok := c.requiredSet[lhs]; !ok {
			continue
		}
		set[lhs] = struct{}{}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	sortStrings(out)
	return out
}

func (c *Catalog) IsQueryRule(rule string) bool {
	if c == nil {
		return false
	}
	_, ok := c.requiredSet[rule]
	return ok
}

func loadQueryRuleList(scriptPath string) ([]string, error) {
	b, err := os.ReadFile(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("read query rule script: %w", err)
	}
	return loadQueryRuleListFromContent(string(b))
}

func loadQueryRuleListFromContent(s string) ([]string, error) {
	start := strings.Index(s, "query_rules=(")
	if start < 0 {
		return nil, fmt.Errorf("query_rules block not found")
	}
	rest := s[start+len("query_rules=("):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return nil, fmt.Errorf("query_rules block not terminated")
	}
	block := rest[:end]
	re := regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
	all := re.FindAllString(block, -1)
	if len(all) == 0 {
		return nil, fmt.Errorf("empty query_rules block")
	}
	return uniquePreserveOrder(all), nil
}

func sortStrings(in []string) {
	sort.Strings(in)
}

func deriveReachableQueryRules(rules map[string][]string) []string {
	if len(rules) == 0 {
		return nil
	}
	lhsSet := make(map[string]struct{}, len(rules))
	for lhs := range rules {
		lhsSet[lhs] = struct{}{}
	}

	adj := make(map[string]map[string]struct{}, len(rules))
	refRE := regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
	for lhs, alts := range rules {
		for _, alt := range alts {
			rhs := stripActionAndPrecedence(alt)
			for _, tok := range refRE.FindAllString(rhs, -1) {
				if _, ok := lhsSet[tok]; !ok {
					continue
				}
				if adj[lhs] == nil {
					adj[lhs] = map[string]struct{}{}
				}
				adj[lhs][tok] = struct{}{}
			}
		}
	}

	q := make([]string, 0, len(queryRoots))
	seen := make(map[string]struct{}, len(rules))
	for _, r := range queryRoots {
		if _, ok := lhsSet[r]; !ok {
			continue
		}
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		q = append(q, r)
	}
	for i := 0; i < len(q); i++ {
		cur := q[i]
		next := make([]string, 0, len(adj[cur]))
		for n := range adj[cur] {
			next = append(next, n)
		}
		sortStrings(next)
		for _, n := range next {
			if _, ok := seen[n]; ok {
				continue
			}
			seen[n] = struct{}{}
			q = append(q, n)
		}
	}

	out := make([]string, 0, len(seen))
	for r := range seen {
		out = append(out, r)
	}
	sortStrings(out)
	return out
}

func stripActionAndPrecedence(in string) string {
	s := in
	if i := strings.Index(s, "{"); i >= 0 {
		s = s[:i]
	}
	if i := strings.Index(s, "%prec"); i >= 0 {
		s = s[:i]
	}
	s = strings.ReplaceAll(s, "/* empty */", " ")
	s = strings.ReplaceAll(s, "/*empty*/", " ")
	return s
}

func mergeRequiredRuleOrder(preferred []string, reachable []string) []string {
	reachSet := make(map[string]struct{}, len(reachable))
	for _, r := range reachable {
		reachSet[r] = struct{}{}
	}
	out := make([]string, 0, len(reachable))
	seen := make(map[string]struct{}, len(reachable))
	for _, r := range preferred {
		if _, ok := reachSet[r]; !ok {
			continue
		}
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	extra := make([]string, 0, len(reachable)-len(out))
	for _, r := range reachable {
		if _, ok := seen[r]; ok {
			continue
		}
		extra = append(extra, r)
	}
	sortStrings(extra)
	out = append(out, extra...)
	return out
}

var queryRoots = []string{
	"query_expression",
	"query_simple",
	"query_specification",
	"query_simple_or_subquery",
	"query_or_subquery",
	"union_query_expression",
	"subquery",
	"insert_query",
}

func uniquePreserveOrder(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

var defaultQueryRules = []string{
	"query_expression",
	"query_simple",
	"union_query_expression",
	"query_simple_or_subquery",
	"query_or_subquery",
	"query_specification",
	"set_quantifier_opt",
	"select_list",
	"select_item",
	"from_clause_opt",
	"table_reference_list",
	"table_reference",
	"table_primary",
	"alias_opt",
	"joined_table",
	"inner_joined",
	"outer_joined",
	"semi_joined",
	"anti_joined",
	"asof_joined",
	"win_joined",
	"join_on_clause_opt",
	"join_on_clause",
	"window_offset_clause",
	"window_offset_literal",
	"jlimit_clause_opt",
	"where_clause_opt",
	"group_by_clause_opt",
	"group_by_list",
	"having_clause_opt",
	"order_by_clause_opt",
	"sort_specification_list",
	"sort_specification",
	"ordering_specification_opt",
	"null_ordering_opt",
	"limit_clause_opt",
	"slimit_clause_opt",
	"subquery",
	"common_expression",
	"expr_or_subquery",
	"expression",
	"expression_list",
	"column_reference",
	"column_alias",
	"table_alias",
	"pseudo_column",
	"function_expression",
	"function_name",
	"star_func",
	"if_expression",
	"case_when_expression",
	"when_then_list",
	"case_when_else_opt",
	"boolean_value_expression",
	"boolean_primary",
	"predicate",
	"compare_op",
	"in_op",
	"search_condition",
	"partition_by_clause_opt",
	"partition_list",
	"partition_item",
	"range_opt",
	"every_opt",
	"interp_fill_opt",
	"tag_mode_opt",
	"twindow_clause_opt",
	"count_window_args",
	"interval_sliding_duration_literal",
	"sliding_opt",
	"extend_literal",
	"zeroth_literal",
	"state_window_opt",
	"true_for_opt",
	"fill_opt",
	"fill_value",
	"fill_mode",
	"fill_position_mode",
	"fill_position_mode_extension",
	"interp_fill_mode",
	"parenthesized_joined_table",
	"type_name",
	"trim_specification_type",
}
