package workload

import (
	"fmt"
	"strings"

	"tdsqlsmith/internal/generator"
	"tdsqlsmith/internal/random"
)

type Prepared struct {
	QueryCases []string
	Pools      map[string][]string
}

func BuildPools(writeSQL []string) Prepared {
	p := Prepared{Pools: map[string][]string{}}
	for _, raw := range writeSQL {
		sqlText := strings.TrimSpace(raw)
		if sqlText == "" {
			continue
		}
		lower := strings.ToLower(sqlText)
		switch {
		case strings.HasPrefix(lower, "create table"):
			p.Pools[DDLCreateTable] = append(p.Pools[DDLCreateTable], sqlText)
		case strings.HasPrefix(lower, "alter table"):
			p.Pools[DDLAlterTable] = append(p.Pools[DDLAlterTable], sqlText)
		case strings.HasPrefix(lower, "create index"):
			p.Pools[DDLCreateIndex] = append(p.Pools[DDLCreateIndex], sqlText)
		case strings.HasPrefix(lower, "delete"):
			p.Pools[DMLDelete] = append(p.Pools[DMLDelete], sqlText)
		case strings.HasPrefix(lower, "update"):
			p.Pools[DMLUpdate] = append(p.Pools[DMLUpdate], sqlText)
		case strings.HasPrefix(lower, "insert"):
			p.Pools[DMLInsert] = append(p.Pools[DMLInsert], sqlText)
		}
	}
	return p
}

type Generator struct {
	sampler *Sampler
	query   *generator.Generator
	pools   map[string][]string
}

func New(cfg Config, queryGen *generator.Generator, prepared Prepared) (*Generator, error) {
	s, err := NewSampler(cfg)
	if err != nil {
		return nil, err
	}
	if queryGen == nil {
		return nil, fmt.Errorf("nil query generator")
	}
	return &Generator{sampler: s, query: queryGen, pools: prepared.Pools}, nil
}

func (g *Generator) Next(r *random.RNG, missingQueryCases []string) (generator.Generated, string, error) {
	if r == nil {
		return generator.Generated{}, "", fmt.Errorf("nil random")
	}

	family := g.sampler.Pick(r)
	if len(missingQueryCases) > 0 && r.Intn(100) < 75 {
		family = DMLSelect
	}

	switch family {
	case TxnBegin:
		return generator.Generated{Rule: "txn_begin", SQL: "begin", Kind: "write"}, family, nil
	case TxnCommit:
		return generator.Generated{Rule: "txn_commit", SQL: "commit", Kind: "write"}, family, nil
	case TxnRollback:
		return generator.Generated{Rule: "txn_rollback", SQL: "rollback", Kind: "write"}, family, nil
	case DMLSelect:
		q, err := g.query.Next(r, missingQueryCases)
		if err != nil {
			return generator.Generated{}, family, err
		}
		q.Kind = "query"
		if q.Rule == "" {
			q.Rule = "query_select"
		}
		return q, family, nil
	case DMLSelectForUpdate:
		// Keep it simple and stable for TDengine parser/executor.
		return generator.Generated{Rule: "query_select_for_update", SQL: "select v from t1 where v > 1 for update", Kind: "query"}, family, nil
	default:
		if arr := g.pools[family]; len(arr) > 0 {
			sqlText := arr[r.Intn(len(arr))]
			return generator.Generated{Rule: strings.ToLower(family), SQL: sqlText, Kind: "write"}, family, nil
		}
		// fallback to query generation if selected family has empty pool.
		q, err := g.query.Next(r, missingQueryCases)
		if err != nil {
			return generator.Generated{}, family, err
		}
		q.Kind = "query"
		return q, DMLSelect, nil
	}
}
