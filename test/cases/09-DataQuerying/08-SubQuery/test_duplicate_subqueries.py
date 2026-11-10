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

import re
from pathlib import Path

from new_test_framework.utils import tdCom, tdLog


class TestDuplicateSubqueries:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def normalize_distributed_analyze(text):
        # Parallel vnode tasks can appear in either order in ANALYZE output.
        # Results and static plans verify rows, types and producer identities.
        counters = re.compile(
            r"\b(rows|fetch_rows|check_rows|total_blocks|file_load_blocks|"
            r"stt_load_blocks|mem_load_blocks|sma_load_blocks|composed_blocks)="
            r"[0-9]+(?:\.[0-9]+)?(?:\([0-9]+\))?"
        )
        distributed_analyze = False
        lines = []
        for line in text.splitlines(keepends=True):
            if line.startswith("taos> "):
                sql = line[6:].lower()
                distributed_analyze = sql.startswith("explain analyze ") and "distributed_" in sql
            if distributed_analyze and line.startswith("QUERY_PLAN:"):
                line = counters.sub(r"\1=<runtime>", line)
            lines.append(line)
        return "".join(lines)

    def do_compare_results(self, case_name):
        case_dir = Path(__file__).resolve().parent
        tdCom.compare_testcase_result(
            str(case_dir / "in" / f"{case_name}.in"),
            str(case_dir / "ans" / f"{case_name}.ans"),
            case_name,
            result_transform=self.normalize_distributed_analyze,
        )

    def test_duplicate_subqueries(self):
        """Reuse identical non-correlated expression subqueries

        1. Check scalar, IN, EXISTS, UNION and nested dependency results.
        2. Check executed InitPlan counts for repeated references.
        3. Preserve NULL, cardinality errors and incompatible consumer types.
        4. Keep cache results valid after constant folding and across consumers.
        5. Share renamed table/output bindings while preserving self-join roles.

        Catalog:
            - Querying

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-13 Joey Sima Add duplicate subquery regression coverage
            - 2026-10-27 Joey Sima Cover scope-aware alias equivalence
            - 2026-11-09 Joey Sima Move SQL and expected output to in/ans files

        """
        self.do_compare_results("duplicate_subqueries")

    def test_duplicate_positions(self):
        """Shared scalar and predicate subqueries across operator positions

        1. Check SELECT, WHERE, HAVING, ORDER/GROUP/PARTITION BY, CASE and JOIN ON.
        2. Check aggregates, derived inputs, UNION branches and interval output.
        3. Assert query results and both static and executed EXPLAIN plans.

        Catalog:
            - Querying

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Joey Sima Cover duplicate subquery position matrix
            - 2026-11-09 Joey Sima Move SQL and expected output to in/ans files

        """
        self.do_compare_results("duplicate_positions")

    def test_duplicate_mixed_consumers(self):
        """Mixed scalar, non-scalar, correlated and ordinary subqueries

        1. Share compatible ordinary calls while keeping correlated calls independent.
        2. Keep incompatible result consumers and nested plan ownership separate.
        3. Check results, InitPlan identities and correlated EXPLAIN templates.

        Catalog:
            - Querying

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Joey Sima Cover mixed subquery consumers and correlation
            - 2026-11-09 Joey Sima Move SQL and expected output to in/ans files

        """
        self.do_compare_results("duplicate_mixed_consumers")

    def test_duplicate_correlated_positions(self):
        """Repeated correlated scalar calls preserve per-row semantics

        1. Exercise projection, filter, grouping, having, sort, partition, CASE and JOIN.
        2. Cover repeated/NULL outer parameters and empty/NULL inner results.
        3. Verify independent correlated templates in both EXPLAIN modes.

        Catalog:
            - Querying

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Joey Sima Cover correlated subquery position matrix
            - 2026-11-09 Joey Sima Move SQL and expected output to in/ans files

        """
        self.do_compare_results("duplicate_correlated_positions")

    def test_duplicate_unshareable(self):
        """Different semantics, empty results and rejected subquery shapes

        1. Keep distinct sources, filters, functions, types, order, limits and windows separate.
        2. Check empty/NULL sets and scalar cardinality errors after deduplication.
        3. Assert query and EXPLAIN rejection codes for unsupported correlated forms.

        Catalog:
            - Querying

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Joey Sima Cover unshareable semantics and error paths
            - 2026-11-09 Joey Sima Move SQL and expected output to in/ans files

        """
        self.do_compare_results("duplicate_unshareable")

    def test_duplicate_equivalent_variants(self):
        """Equivalent SQL spellings and the limits of structural sharing

        1. Share renamed bindings, qualifiers, projections and parser-normalized forms.
        2. Record semantic rewrites that the structural comparator does not recognize.
        3. Keep changed output positions, self-join roles and correlated templates separate.
        4. Assert query results, static EXPLAIN and EXPLAIN ANALYZE for each pair.

        Catalog:
            - Querying

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Joey Sima Cover differently written equivalent subqueries
            - 2026-11-09 Joey Sima Move SQL and expected output to in/ans files

        """
        self.do_compare_results("duplicate_equivalent_variants")

    def test_duplicate_review_regressions(self):
        """UNION branches, nested external windows and distributed sharing

        1. Keep different UNION operands independent and verify distinct results.
        2. Exclude external windows inside derived tables and UNION branches.
        3. Share results across scan and projection consumers on two vnode groups.
        4. Check NULL membership, query results, EXPLAIN and EXPLAIN ANALYZE.

        Catalog:
            - Querying

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-03 Joey Sima Cover duplicate subquery review regressions
            - 2026-11-09 Joey Sima Move SQL and expected output to in/ans files

        """
        self.do_compare_results("duplicate_review_regressions")
