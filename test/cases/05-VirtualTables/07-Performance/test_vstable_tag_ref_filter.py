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

from new_test_framework.utils import tdLog, tdSql, etool, tdCom, tdDnodes

import os


class TestVstableTagRefFilter:
    updatecfgDict = {"disableStream": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.source_db = "vt_tag_filter_source"
        cls.virtual_db = "vt_tag_filter_idmp"
        # The CLI runs in a separate process from the pytest connection.
        tdDnodes.sim.cfg("queryPolicy", cls.query_policy)
        with etool.data_write_log():
            if os.getenv("VTB_TAG_FILTER_SCALE_ONLY") == "1":
                cls.prepare_scale()
            else:
                cls.prepare()

    @classmethod
    def prepare_scale(cls):
        cls.prepare(
            children=int(os.getenv("VTB_TAG_FILTER_CHILDREN", "128")),
            sources=int(os.getenv("VTB_TAG_FILTER_SOURCES", "16")),
            match_every=int(os.getenv("VTB_TAG_FILTER_MATCH_EVERY", "4")),
        )

    @classmethod
    def prepare(cls, children=128, sources=16, match_every=4):
        cls.children = children
        cls.sources = sources
        cls.expected = {}
        cls.phones = {
            i: None if i == 14 else "" if i == 15 else f"{'1380' if i % match_every == 0 else '1390'}{i:04d}"
            for i in range(sources)
        }
        for db in (cls.virtual_db, cls.source_db):
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdSql.execute(f"CREATE DATABASE {db} VGROUPS 2 KEEP 36500 BUFFER 16")
        cols = ", ".join(f"v{i} INT" for i in range(25))
        tdSql.execute(
            f"CREATE STABLE {cls.source_db}.src (ts TIMESTAMP, {cols}, payload VARCHAR(16384)) "
            "TAGS (phone VARCHAR(32), kind INT)"
        )
        for table in ("data_left", "data_right", "data_empty"):
            tdSql.execute(f"CREATE TABLE {cls.source_db}.{table} USING {cls.source_db}.src TAGS ('data', 0)")
        values = ", ".join(str(i) for i in range(25))
        for offset, table in enumerate(("data_left", "data_right")):
            tdSql.execute(
                f"INSERT INTO {cls.source_db}.{table} VALUES "
                f"({1700000000000 + offset * 1000}, {values}, '{'x' * 16000}')"
            )
        for i, phone in cls.phones.items():
            literal = "NULL" if phone is None else f"'{phone}'"
            tdSql.execute(f"CREATE TABLE {cls.source_db}.tag_{i} USING {cls.source_db}.src TAGS ({literal}, {i})")
        extra_tags = ", ".join(f"t{i} INT" for i in range(34))
        tdSql.execute(
            f"CREATE STABLE {cls.virtual_db}.vst (ts TIMESTAMP, {cols}, payload VARCHAR(16384)) "
            f"TAGS (`手机` VARCHAR(32), local_tag INT, {extra_tags}) VIRTUAL 1"
        )
        tdSql.execute(f"USE {cls.virtual_db}")
        for i in range(cls.children):
            empty = i == 0 or i % 64 == 63
            left = "data_empty" if empty else "data_left"
            right = "data_empty" if empty else "data_right"
            refs = ", ".join(f"v{j} FROM {cls.source_db}.{left}.v{j}" for j in range(25))
            tag_refs = ", ".join(f"t{j} FROM {cls.source_db}.tag_{i % cls.sources}.kind" for j in range(34))
            name = f"vt_{i:06d}"
            local_tag = 10000 if i == 0 else i % 2
            tdSql.execute(
                f"CREATE VTABLE {name} ({refs}, payload FROM {cls.source_db}.{right}.payload) "
                f"USING vst TAGS (`手机` FROM {cls.source_db}.tag_{i % cls.sources}.phone, {local_tag}, {tag_refs})"
            )
            cls.expected[name] = (cls.phones[i % cls.sources], local_tag, not empty)
            if (i + 1) % 10000 == 0:
                tdLog.info(f"Created {i + 1}/{cls.children} virtual children")

    def do_scale_report(self):
        report = os.getenv("VTB_TAG_FILTER_REPORT")
        if not report:
            return
        if os.getenv("VTB_TAG_FILTER_SCALE_ONLY") != "1":
            with etool.data_write_log():
                self.prepare_scale()
        import json
        import subprocess
        import threading
        import time

        prefix = os.path.join(os.environ["TAOS_BIN_PATH"], "taosd") + " -c "
        config = os.path.join(os.environ["WORK_DIR"], "dnode1", "cfg")
        processes = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
        pids = [row.strip().split(None, 1)[0] for row in processes.splitlines()
                if len(row.strip().split(None, 1)) == 2
                and row.strip().split(None, 1)[1] == prefix + config]
        assert len(pids) == 1, f"Expected one local test taosd, got {pids}"

        def rss_mb():
            return int(subprocess.check_output(["ps", "-o", "rss=", "-p", pids[0]], text=True).strip()) / 1024

        queries = {
            "having_last": "SELECT tbname FROM vst GROUP BY tbname HAVING last(`手机`) LIKE '1380%'",
            "distinct_tbname": "SELECT DISTINCT tbname FROM vst WHERE `手机` LIKE '1380%'",
            "ins_tags": f"SELECT table_name FROM information_schema.ins_tags WHERE db_name='{self.virtual_db}' "
                        "AND stable_name='vst' AND tag_name='手机' AND tag_value LIKE '1380%'",
        }
        result = {"virtual_children": self.children, "physical_tag_sources": self.sources,
                  "columns": 27, "tags": 36, "rss_sampling_interval_s": 0.05, "queries": {}}
        for name, sql in queries.items():
            stop = threading.Event()
            baseline = rss_mb()
            peak = [baseline]

            def sample():
                while not stop.wait(0.05):
                    peak[0] = max(peak[0], rss_mb())

            sampler = threading.Thread(target=sample)
            sampler.start()
            started = time.perf_counter()
            try:
                tdSql.query(sql)
            finally:
                elapsed = time.perf_counter() - started
                peak[0] = max(peak[0], rss_mb())
                stop.set()
                sampler.join()
            result["queries"][name] = {"seconds": elapsed, "rows": tdSql.queryRows,
                                       "baseline_rss_mb": baseline, "sampled_peak_rss_mb": peak[0]}
            expected = {
                table for table, (phone, local_tag, has_rows) in self.expected.items()
                if (phone or "").startswith("1380") and (has_rows or name != "having_last")
            }
            assert {tdSql.getData(i, 0) for i in range(tdSql.queryRows)} == expected
            assert tdSql.queryRows == len(expected)
            assert elapsed < float(os.getenv("VTB_TAG_FILTER_MAX_QUERY_S", "60")), result["queries"][name]
        with open(report, "w", encoding="utf-8") as output:
            json.dump(result, output, indent=2)
        print(f"virtual tag scale report: {report}")

    def do_correctness_matrix(self):
        if os.getenv("VTB_TAG_FILTER_SCALE_ONLY") == "1":
            return
        case_dir = os.path.dirname(__file__)
        for group in ("metadata", "rows", "projection_refresh", "json", "plans", "references"):
            case = f"test_vstable_tag_ref_filter_{group}"
            answer = f"{case}_q3" if group == "plans" and self.query_policy == 3 else case
            tdCom.compare_testcase_result(
                os.path.join(case_dir, "in", f"{case}.in"),
                os.path.join(case_dir, "ans", f"{answer}.ans"),
                case,
            )

    def test_tag_ref_filter(self):
        """Filter virtual children by referenced tags before data scans

        1. Check exact, prefix, NULL, empty, mixed and negated metadata predicates
        2. Preserve timestamp unions and exclude empty children from last(tag)
        3. Resolve multi-hop and dangling tags and observe source-tag changes
        4. Preserve tag-only DISTINCT row semantics, name restrictions and JSON tags with streams disabled

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-03 Joey Sima Created
            - 2026-11-10 Joey Sima converted correctness coverage to in/ans fixtures

        """
        self.do_correctness_matrix()
        self.do_scale_report()

    def teardown_class(cls):
        for db in (cls.virtual_db, cls.source_db):
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
