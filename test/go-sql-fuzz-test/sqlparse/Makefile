.PHONY: validate-parity baseline-parity query-diff statement-diff lemon-write-audit query-coverage write-sql-gate insert-sql-gate statement-branch-gate statement-roundtrip-gate parser-hard-gate clean-reports gen-valid-corpus test

GOYACC ?= /tmp/bin/goyacc

validate-parity:
	bash tool/migrate/validate_full.sh

baseline-parity:
	bash tool/migrate/gen_baseline_reports.sh

query-diff:
	bash tool/migrate/query_rule_diff.sh

statement-diff:
	bash tool/migrate/statement_rule_diff.sh

lemon-write-audit:
	bash tool/migrate/audit_lemon_write_semantics.sh

query-coverage:
	bash tool/migrate/query_coverage.sh

write-sql-gate:
	GOCACHE=/tmp/gocache GOMODCACHE=/tmp/gomodcache go test ./... -run 'TestWriteSQLCorpus_ParseAll|TestWriteSQLCorpus_PerTypeRepresentatives' -count=1

insert-sql-gate:
	GOCACHE=/tmp/gocache GOMODCACHE=/tmp/gomodcache go test ./... -run 'TestInsertSQLCorpus_ParseAll|TestInsertSQLCorpus_RoundTripSample|TestInsertParse_DBTableColumnsNowInt' -count=1

statement-branch-gate:
	GOCACHE=/tmp/gocache GOMODCACHE=/tmp/gomodcache go test ./... -run 'TestStatementBranchMatrix_RoundTrip|TestStatementBranchNegativeMatrix|TestStatementTypeCoverage_All|TestSelectBranchMatrix_RoundTrip|TestSelectBranchNegativeMatrix|TestSelectNestedMatrix_RoundTrip' -count=1

statement-roundtrip-gate:
	GOCACHE=/tmp/gocache GOMODCACHE=/tmp/gomodcache go test ./... -run 'TestStatementRoundTrip_CommandSuites|TestStatementRoundTrip_ValidCorpus|TestStatementRoundTrip_PassReport|TestStatementRoundTrip_GlobalAllSyntax' -count=1

parser-hard-gate:
	$(GOYACC) -o sql.go -v y.output td_sql.y
	GOCACHE=/tmp/gocache GOMODCACHE=/tmp/gomodcache go test ./... -count=1
	$(MAKE) validate-parity
	$(MAKE) query-coverage
	$(MAKE) write-sql-gate
	$(MAKE) insert-sql-gate
	$(MAKE) statement-diff
	$(MAKE) statement-branch-gate
	$(MAKE) statement-roundtrip-gate

clean-reports:
	bash tool/migrate/clean_reports.sh 2026-02-13

gen-valid-corpus:
	GOCACHE=/tmp/gocache GOMODCACHE=/tmp/gomodcache go run -mod=readonly tool/migrate/cmd/gen_valid_sql_corpus/main.go

test:
	GOCACHE=/tmp/go-build go test ./... -count=1
