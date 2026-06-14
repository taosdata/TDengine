package queryrules

// grammar.go parses the bison-style td_sql.y grammar file into a production
// table and a map of rule name to its alternatives.
//
// grammar.go 将 bison 风格的 td_sql.y 语法文件解析为产生式表,
// 以及从规则名到其备选项的映射。

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// ruleHeaderRE matches a grammar rule header line of the form "rule_name :".
//
// ruleHeaderRE 匹配形如 "rule_name :" 的语法规则头部行。
var ruleHeaderRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*$`)

// loadProductionAndRules reads the grammar file at path and parses it into a
// production table and a rule->alternatives map.
//
// loadProductionAndRules 读取 path 处的语法文件,并将其解析为产生式表
// 以及从规则到备选项的映射。
func loadProductionAndRules(path string) ([]string, map[string][]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read grammar %s: %w", path, err)
	}
	return loadProductionAndRulesContent(string(b), path)
}

// loadProductionAndRulesContent parses grammar content (the text between the two
// %% markers), returning the production table (1-based, index 0 left empty to
// match generated-parser numbering) and a map of rule name to its alternatives.
// It skips comments and semantic action blocks tracked via brace depth.
//
// loadProductionAndRulesContent 解析语法内容(两个 %% 标记之间的文本),
// 返回产生式表(下标从 1 开始,下标 0 留空以匹配生成的解析器编号)
// 以及从规则名到其备选项的映射。它会跳过注释和通过花括号深度跟踪的语义动作块。
func loadProductionAndRulesContent(content string, source string) ([]string, map[string][]string, error) {
	lines := strings.Split(content, "\n")
	inGrammar := false
	currentRule := ""
	expectFirstAlt := false
	actionDepth := 0

	// yyn starts from 1 in generated parser; keep index 0 empty.
	// 生成的解析器中 yyn 从 1 开始;保持下标 0 为空。
	production := make([]string, 1, 1300)
	rules := make(map[string][]string, 320)

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if !inGrammar {
			if line == "%%" {
				inGrammar = true
			}
			continue
		}
		if line == "%%" {
			break
		}

		if idx := strings.Index(line, "//"); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if line == "" {
			continue
		}

		if actionDepth > 0 {
			actionDepth += braceDelta(line)
			if actionDepth < 0 {
				actionDepth = 0
			}
			continue
		}

		if m := ruleHeaderRE.FindStringSubmatch(line); len(m) == 2 {
			currentRule = m[1]
			expectFirstAlt = true
			if _, ok := rules[currentRule]; !ok {
				rules[currentRule] = nil
			}
			continue
		}

		if currentRule == "" {
			continue
		}

		if strings.HasPrefix(line, ";") {
			currentRule = ""
			expectFirstAlt = false
			continue
		}

		if strings.HasPrefix(line, "|") {
			alt := strings.TrimSpace(line[1:])
			rules[currentRule] = append(rules[currentRule], alt)
			production = append(production, currentRule)
			expectFirstAlt = false
			actionDepth += braceDelta(line)
			if actionDepth < 0 {
				actionDepth = 0
			}
			continue
		}

		if expectFirstAlt {
			rules[currentRule] = append(rules[currentRule], line)
			production = append(production, currentRule)
			expectFirstAlt = false
			actionDepth += braceDelta(line)
			if actionDepth < 0 {
				actionDepth = 0
			}
			continue
		}

		if strings.Contains(line, "{") {
			actionDepth += braceDelta(line)
			if actionDepth < 0 {
				actionDepth = 0
			}
		}
	}

	if len(production) <= 1 {
		return nil, nil, fmt.Errorf("no grammar productions parsed from %s", source)
	}
	return production, rules, nil
}

// braceDelta returns the net change in brace nesting for line, counting '{' as
// +1 and '}' as -1.
//
// braceDelta 返回 line 中花括号嵌套层级的净变化,'{' 计为 +1,'}' 计为 -1。
func braceDelta(line string) int {
	delta := 0
	for _, ch := range line {
		switch ch {
		case '{':
			delta++
		case '}':
			delta--
		}
	}
	return delta
}
