# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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
