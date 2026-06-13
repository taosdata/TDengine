package queryrules

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

var ruleHeaderRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*$`)

func loadProductionLHS(path string) ([]string, error) {
	prod, _, err := loadProductionAndRules(path)
	if err != nil {
		return nil, err
	}
	return prod, nil
}

func loadProductionAndRules(path string) ([]string, map[string][]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read grammar %s: %w", path, err)
	}
	return loadProductionAndRulesContent(string(b), path)
}

func loadProductionAndRulesContent(content string, source string) ([]string, map[string][]string, error) {
	lines := strings.Split(content, "\n")
	inGrammar := false
	currentRule := ""
	expectFirstAlt := false
	actionDepth := 0

	// yyn starts from 1 in generated parser; keep index 0 empty.
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
