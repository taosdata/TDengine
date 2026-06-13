package corpusdata

import "fmt"

type Embedded struct {
	SelectBranchMatrix    string
	SelectNestedMatrix    string
	SelectBranchNegative  string
	WriteSQLCases         string
	ValidSQLCases         string
	StatementBranchMatrix string
	Grammar               string
	QueryRuleScript       string
}

func Load() (Embedded, error) {
	return Embedded{}, fmt.Errorf("corpus data disabled: runtime is AST-only")
}
