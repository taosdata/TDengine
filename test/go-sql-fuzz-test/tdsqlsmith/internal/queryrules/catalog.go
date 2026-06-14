// Package queryrules parses the TDengine grammar and query-rule catalog,
// builds seed SQL pools, and tracks which query grammar rules were exercised.
//
// queryrules 包解析 TDengine 语法和查询规则目录,
// 构建种子 SQL 池,并跟踪哪些查询语法规则被覆盖。
package queryrules

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Catalog holds the set of query grammar rules that must be covered together
// with the grammar production table used to map parser reductions back to rules.
//
// Catalog 保存必须被覆盖的查询语法规则集合,以及用于将解析器归约
// 映射回规则的语法产生式表。
type Catalog struct {
	required    []string            // ordered list of required query rule names / 必需查询规则名的有序列表
	requiredSet map[string]struct{} // membership set for fast lookup of required rules / 用于快速查找必需规则的成员集合
	production  []string            // 1-based index: production id -> left-hand-side rule name / 下标从 1 开始:产生式 id -> 左部规则名
}

// LoadCatalog builds a Catalog by reading the preferred query-rule list from the
// migration script and the grammar productions/rules from td_sql.y under sqlparseRoot.
// It falls back to the built-in defaultQueryRules if the rule list cannot be read.
//
// LoadCatalog 通过从迁移脚本读取优先查询规则列表,并从 sqlparseRoot 下的
// td_sql.y 读取语法产生式/规则来构建 Catalog。如果无法读取规则列表,
// 则回退到内置的 defaultQueryRules。
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

// buildCatalog assembles a Catalog from the preferred rule order, the production
// table, and the grammar rules. It restricts the required set to rules reachable
// from the query roots and orders them according to the preferred list.
//
// buildCatalog 根据优先规则顺序、产生式表和语法规则组装一个 Catalog。
// 它将必需集合限制为从查询根可达的规则,并按优先列表对其排序。
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

// RequiredRules returns a copy of the ordered list of required query rule names.
//
// RequiredRules 返回必需查询规则名有序列表的一份副本。
func (c *Catalog) RequiredRules() []string {
	if c == nil {
		return nil
	}
	out := make([]string, len(c.required))
	copy(out, c.required)
	return out
}

// RuleFromReduction maps a parser production/reduction id to its left-hand-side
// rule name, or returns "" if the id is out of range.
//
// RuleFromReduction 将解析器产生式/归约 id 映射到其左部规则名,
// 如果 id 越界则返回 ""。
func (c *Catalog) RuleFromReduction(id int) string {
	if c == nil || id <= 0 || id >= len(c.production) {
		return ""
	}
	return c.production[id]
}

// QueryRulesFromReductions converts a list of reduction ids into the sorted set
// of distinct required query rule names they correspond to.
//
// QueryRulesFromReductions 将一组归约 id 转换为它们所对应的、
// 去重并排序后的必需查询规则名集合。
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

// IsQueryRule reports whether rule is one of the required query rules.
//
// IsQueryRule 报告 rule 是否为必需查询规则之一。
func (c *Catalog) IsQueryRule(rule string) bool {
	if c == nil {
		return false
	}
	_, ok := c.requiredSet[rule]
	return ok
}

// loadQueryRuleList reads the migration script at scriptPath and extracts the
// preferred query-rule names from its query_rules block.
//
// loadQueryRuleList 读取 scriptPath 处的迁移脚本,并从其 query_rules 块中
// 提取优先查询规则名。
func loadQueryRuleList(scriptPath string) ([]string, error) {
	b, err := os.ReadFile(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("read query rule script: %w", err)
	}
	return loadQueryRuleListFromContent(string(b))
}

// loadQueryRuleListFromContent parses the query_rules=( ... ) shell-array block
// out of s and returns the contained identifiers in their original order.
//
// loadQueryRuleListFromContent 从 s 中解析出 query_rules=( ... ) shell 数组块,
// 并按原始顺序返回其中包含的标识符。
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

// sortStrings sorts in in ascending lexical order in place.
//
// sortStrings 原地按字典升序对 in 排序。
func sortStrings(in []string) {
	sort.Strings(in)
}

// deriveReachableQueryRules returns the sorted set of grammar rule names
// reachable from the query roots by following non-terminal references in the
// right-hand sides of the grammar rules.
//
// deriveReachableQueryRules 通过沿语法规则右部中的非终结符引用进行遍历,
// 返回从查询根可达的、排序后的语法规则名集合。
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

// stripActionAndPrecedence removes the semantic action block, %prec directive,
// and empty-alternative markers from a grammar alternative, leaving the symbols.
//
// stripActionAndPrecedence 从语法备选项中移除语义动作块、%prec 指令
// 和空备选项标记,只留下符号。
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

// mergeRequiredRuleOrder returns the reachable rules ordered first by their
// appearance in preferred, with any remaining reachable rules appended sorted.
//
// mergeRequiredRuleOrder 返回可达规则的排序:先按其在 preferred 中的出现顺序排列,
// 再将剩余的可达规则排序后追加在后面。
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

// queryRoots lists the grammar non-terminals used as starting points when
// computing the set of query rules reachable in the grammar.
//
// queryRoots 列出在计算语法中可达查询规则集合时作为起点使用的语法非终结符。
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

// uniquePreserveOrder returns in with duplicate strings removed, keeping the
// first occurrence of each value.
//
// uniquePreserveOrder 返回去除重复字符串后的 in,保留每个值的首次出现。
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

// defaultQueryRules is the built-in fallback ordering of query rule names used
// when the preferred list cannot be loaded from the migration script.
//
// defaultQueryRules 是内置的查询规则名回退顺序,当无法从迁移脚本加载
// 优先列表时使用。
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
