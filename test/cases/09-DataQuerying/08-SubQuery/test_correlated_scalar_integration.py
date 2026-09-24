from new_test_framework.utils import tdCom, tdLog, tdSql

import os
import re
import subprocess
import sys
import tempfile
import uuid


class TestCorrelatedScalarIntegration:
    SCALAR_POLICY = 0x26A9

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def check_result(self, sql, expected):
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(len(expected))
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def do_query_policies(self):
        scalar = (
            "select r.v, (select max(h.v) from history h "
            "where h.device = r.device) from readings r order by r.ts"
        )
        for policy in (1, 2, 3):
            tdSql.execute(f'alter local "queryPolicy" "{policy}"')
            self.check_result(scalar, [(1, 11), (2, 22)])
            self.check_result(
                "select r.v, (select sum(h.v) from history_st h "
                "where h.device = r.device) from readings r order by r.ts",
                [(1, 25), (2, 20)],
            )
            self.check_result(
                "select r.v, (select h.v from history_st h "
                "where h.device = r.device order by h.ts desc limit 1) "
                "from readings r order by r.ts",
                [(1, 9), (2, 8)],
            )
            print(f"query policy {policy} ....................... [ passed ]")
        tdSql.execute('alter local "queryPolicy" "4"')
        tdSql.error(scalar, expectedErrno=self.SCALAR_POLICY)
        tdSql.execute('alter local "queryPolicy" "1"')
        print("query policies 1/2/3/4 ................. [ passed ]")

    def do_mnode_fallback(self):
        tdSql.execute('alter local "queryPolicy" "3"')
        try:
            self.check_result(
                "select r.id, (select count(*) from history h where h.v > r.id) "
                "from information_schema.ins_dnodes r where r.id = 1",
                [(1, 2)],
            )
        finally:
            tdSql.execute('alter local "queryPolicy" "1"')
        print("mnode policy fallback ................... [ passed ]")

    def prepare_multi_vnode_stable(self, database):
        tdSql.execute("create stable history_st (ts timestamp, v int) tags (device varchar(16))")
        for index in range(9):
            tdSql.execute(
                f"create table hist_{index} using history_st tags ('d{index % 2 + 1}')"
            )
            tdSql.execute(
                f"insert into hist_{index} values ('2026-09-11 00:00:{index + 10:02d}', {index + 1})"
            )
        tdSql.query(
            "select distinct vgroup_id from information_schema.ins_tables "
            f"where db_name = '{database}' and table_name like 'hist_%'",
            queryTimes=1,
        )
        history_vgroups = {row[0] for row in tdSql.queryResult}
        assert len(history_vgroups) >= 2, history_vgroups
        tdSql.query("show dnodes", queryTimes=1)
        if tdSql.queryRows >= 3:
            tdSql.query("show vgroups", queryTimes=1)
            history_dnodes = {row[3] for row in tdSql.queryResult if row[0] in history_vgroups}
            assert len(history_dnodes) >= 2, history_dnodes

    def do_existing_subquery(self):
        self.check_result(
            "select r.v, (select max(h.v) from history h "
            "where h.device = r.device and h.v > "
            "(select min(v) from history)) "
            "from readings r order by r.ts",
            [(1, None), (2, 22)],
        )
        self.check_result(
            "select r.v, (select max(h.v) from history h "
            "where h.device = r.device and h.v in "
            "(select v from history where v = 11)) "
            "from readings r order by r.ts",
            [(1, 11), (2, None)],
        )
        self.check_result(
            "select r.v, (select max(h.v) from history h "
            "where h.device = r.device and exists "
            "(select v from history where v = 11)) "
            "from readings r order by r.ts",
            [(1, 11), (2, 22)],
        )
        self.check_result(
            "select r.v from readings r where r.v in "
            "(select v from readings where v = 1) order by r.ts",
            [(1,)],
        )
        self.check_result(
            "select r.v from readings r where r.v > any "
            "(select v from readings where v = 1) order by r.ts",
            [(2,)],
        )
        tdSql.error(
            "select (select max(h.v) from history h "
            "where h.device = r.device and h.v > "
            "(select v from history)) from readings r",
            expectedErrno=0x26A5,
        )
        print("correlated and noncorrelated nesting ..... [ passed ]")

    def do_cross_node_numeric_params(self):
        tdSql.execute("create table numeric_readings (ts timestamp, device_id int)")
        tdSql.execute(
            "insert into numeric_readings values "
            "('2026-09-11 00:00:01', 101) "
            "('2026-09-11 00:00:02', 202) "
            "('2026-09-11 00:00:03', 101) "
            "('2026-09-11 00:00:04', 202)"
        )
        sql = (
            "select r.device_id, (select sum(h.v) from history_st h where "
            "(r.device_id = 101 and h.device = 'd1') or "
            "(r.device_id = 202 and h.device = 'd2')) "
            "from numeric_readings r order by r.ts"
        )
        try:
            for policy in (1, 3):
                tdSql.execute(f'alter local "queryPolicy" "{policy}"')
                self.check_result(sql, [(101, 25), (202, 20), (101, 25), (202, 20)])
        finally:
            tdSql.execute('alter local "queryPolicy" "1"')
        print("101/202 parameter rows .................. [ passed ]")

    def do_noncorrelated_controls(self):
        self.check_result(
            "select r.v, h.v, (select max(v) from history) "
            "from readings r join history h on h.ts = r.ts and h.device = r.device order by r.ts",
            [(1, 11, 22), (2, 22, 22)],
        )
        tdSql.execute("create view plain_scalar_view as select r.ts, r.v, "
                      "(select max(v) from history) as latest from readings r")
        self.check_result("select v, latest from plain_scalar_view order by ts", [(1, 22), (2, 22)])
        tdSql.execute("create table plain_scalar_result (ts timestamp, v int)")
        tdSql.execute("insert into plain_scalar_result "
                      "select r.ts, (select max(v) from history) from readings r")
        self.check_result("select v from plain_scalar_result order by ts", [(22,), (22,)])
        stmt = self.conn.statement2(
            "select r.v, (select max(v) from history) from readings r where r.v = ?"
        )
        try:
            for value in (1, 2, 1):
                stmt.bind_param(None, None, [[[value]]])
                stmt.execute()
                assert stmt.result().fetch_all() == [(value, 22)]
        finally:
            stmt.close()
        print("noncorrelated JOIN/view/INSERT/STMT ...... [ passed ]")

    def do_insert_select(self):
        tdSql.execute(
            "insert into result_rows select r.ts, (select max(h.v) from history h "
            "where h.device = r.device) from readings r"
        )
        self.check_result("select v from result_rows order by ts", [(11,), (22,)])
        print("INSERT SELECT ............................ [ passed ]")

    def do_view_and_union(self):
        tdSql.execute(
            "create view correlated_view as "
            "select r.ts, r.v, (select max(h.v) from history h "
            "where h.device = r.device) as latest from readings r"
        )
        self.check_result(
            "select v, latest from correlated_view order by ts",
            [(1, 11), (2, 22)],
        )
        self.check_result(
            "select r.v, (select max(h.v) from history h "
            "where h.device = r.device) from readings r where r.device = 'd1' "
            "union all select r.v, (select max(h.v) from history h "
            "where h.device = r.device) from readings r where r.device = 'd2' "
            "order by 1",
            [(1, 11), (2, 22)],
        )
        print("view and UNION ALL ....................... [ passed ]")

    def do_stmt_repeated(self):
        stmt = self.conn.statement2(
            "select r.v, (select max(h.v) from history h "
            "where h.device = r.device) from readings r where r.v = ?"
        )
        try:
            for value in (1, 2, 1):
                stmt.bind_param(None, None, [[[value]]])
                stmt.execute()
                assert stmt.result().fetch_all() == [(value, value * 11)]
        finally:
            stmt.close()
        print("repeated STMT2 execution ............... [ passed ]")

    def do_charset_inheritance(self, database):
        tdSql.execute("create table charset_outer (ts timestamp, label nchar(16))")
        tdSql.execute("create table charset_inner (ts timestamp, label nchar(16), v int)")
        tdSql.execute(
            "insert into charset_outer values "
            "('2026-09-11 00:00:01', '中文测试') "
            "('2026-09-11 00:00:02', '你好世界')"
        )
        tdSql.execute(
            "insert into charset_inner values "
            "('2026-09-11 00:00:01', '中文测试', 11) "
            "('2026-09-11 00:00:02', '你好世界', 22)"
        )
        sql = (
            f"select count(*) from {database}.charset_outer r where "
            f"(select max(h.v) from {database}.charset_inner h "
            "where h.label = r.label and h.label like '中文%') = 11"
        )
        control_sql = (
            f"select count(*) from {database}.charset_inner h "
            "where h.label like '中文%'"
        )
        bin_dir = os.environ.get("TAOS_BIN_PATH")
        if bin_dir is None:
            bin_dir = os.path.join(tdCom.getBuildPath(), "build", "bin")
        with open(os.path.join(self.cfg_path, "taos.cfg")) as config_file:
            config_lines = [
                line for line in config_file
                if not line.strip().lower().startswith(("charset", "locale"))
            ]
        with tempfile.TemporaryDirectory(prefix="p14_charset_") as temp_dir:
            # The macOS CLI also rejects the ordinary GBK LIKE control query.
            charsets = ("UTF-8",) if sys.platform == "darwin" else ("UTF-8", "GBK")
            for charset in charsets:
                config_dir = os.path.join(temp_dir, charset)
                os.mkdir(config_dir)
                with open(os.path.join(config_dir, "taos.cfg"), "w") as config_file:
                    config_file.writelines(config_lines)
                    config_file.write(f"charset {charset}\n")
                for query in (control_sql, sql):
                    sql_file = os.path.join(temp_dir, "query.sql")
                    with open(sql_file, "wb") as query_file:
                        query_file.write((query + ";\n").encode(charset))
                    process = subprocess.run(
                        [os.path.join(bin_dir, "taos"), "-c", config_dir, "-h", self.host,
                         "-P", str(self.port), "-f", sql_file],
                        capture_output=True,
                        timeout=30,
                    )
                    output = process.stdout.decode(charset, "replace")
                    assert process.returncode == 0 and "DB error" not in output, output
                    match = re.search(r"^\s+(\d+)\s+\|", output, re.M)
                    assert match is not None and int(match.group(1)) == 1, output
        print(f"correlated {'/'.join(charsets)} NCHAR LIKE ........ [ passed ]")

    def do_session_inheritance(self, original_timezone):
        tdSql.query("select first_day_of_week()", queryTimes=1)
        original_first_day = tdSql.queryResult[0][0]
        try:
            for timezone in ("UTC", "Asia/Shanghai"):
                tdSql.execute(f'alter local "timezone" "{timezone}"')
                tdSql.query("select timezone() from readings order by ts", queryTimes=1)
                expected = tdSql.queryResult
                self.check_result(
                    "select (select timezone() from history h "
                    "where h.device = r.device) from readings r order by r.ts",
                    expected,
                )
                self.check_result(
                    "select (select (select timezone() from history h2 "
                    "where h2.device = h.device) from history h "
                    "where h.device = r.device) from readings r order by r.ts",
                    expected,
                )
            for first_day in (0, 1):
                tdSql.execute(f"set first_day_of_week {first_day}")
                tdSql.query("select first_day_of_week() from readings order by ts", queryTimes=1)
                expected = tdSql.queryResult
                self.check_result(
                    "select (select first_day_of_week() from history h "
                    "where h.device = r.device) from readings r order by r.ts",
                    expected,
                )
                self.check_result(
                    "select (select (select first_day_of_week() from history h2 "
                    "where h2.device = h.device) from history h "
                    "where h.device = r.device) from readings r order by r.ts",
                    expected,
                )
        finally:
            try:
                tdSql.execute(f"set first_day_of_week {original_first_day}")
            finally:
                tdSql.execute(f'alter local "timezone" "{original_timezone}"')
        print("correlated session time settings ........ [ passed ]")

    def do_remote_late_error(self):
        tdSql.execute("create table late_history (ts timestamp, device varchar(16), v int)")
        tdSql.execute(
            "insert into late_history values "
            "('2026-09-11 00:00:01', 'd1', 11) "
            "('2026-09-11 00:00:02', 'd2', 22) "
            "('2026-09-11 00:00:03', 'd2', 23)"
        )
        tdSql.execute('alter local "queryPolicy" "3"')
        try:
            tdSql.error(
                "select r.v, (select h.v from late_history h "
                "where h.device = r.device) from readings r order by r.ts",
                expectedErrno=0x26A5,
            )
            self.check_result(
                "select r.v, (select max(h.v) from late_history h "
                "where h.device = r.device) from readings r order by r.ts",
                [(1, 11), (2, 23)],
            )
        finally:
            tdSql.execute('alter local "queryPolicy" "1"')
        print("remote later-row error recovery ........ [ passed ]")

    def do_dependency_routes(self, database):
        sql = (
            "select r.v, (select max(h.v) from history h "
            "where h.device = r.device and h.v > "
            "(select min(v) from history)) "
            "from readings r order by r.ts"
        )
        distributed_sql = (
            "select r.v, (select max(h.v) from history_st h "
            "where h.device = r.device and h.v > "
            "(select min(v) from history_st)) "
            "from readings r order by r.ts"
        )
        tdSql.query("show dnodes", queryTimes=1)
        remote_sql = None
        remote_expected = None
        if tdSql.queryRows >= 3:
            tdSql.query("show vgroups", queryTimes=1)
            vgroup_dnodes = {row[0]: str(row[3]) for row in tdSql.queryResult}
            tdSql.query(
                "select vgroup_id from information_schema.ins_tables "
                f"where db_name = '{database}' and table_name = 'readings'",
                queryTimes=1,
            )
            assert tdSql.queryRows == 1
            outer_dnode = vgroup_dnodes[tdSql.queryResult[0][0]]
            tdSql.query(
                "select table_name, vgroup_id from information_schema.ins_tables "
                f"where db_name = '{database}' and table_name like 'hist_%'",
                queryTimes=1,
            )
            remote_histories = sorted(
                (int(row[0][5:]), row[0])
                for row in tdSql.queryResult
                if row[0].startswith("hist_") and row[0][5:].isdigit()
                and vgroup_dnodes[row[1]] != outer_dnode
            )
            assert remote_histories, vgroup_dnodes
            index, table_name = remote_histories[0]
            remote_value = index + 1
            remote_sql = (
                "select r.v, (select max(h.v) from history_st h "
                "where h.device = r.device and h.v > "
                f"(select min(v) from {table_name})) "
                "from readings r order by r.ts"
            )
            remote_expected = [(1, 9 if remote_value < 9 else None), (2, 8 if remote_value < 8 else None)]
        try:
            for policy in (1, 3):
                tdSql.execute(f'alter local "queryPolicy" "{policy}"')
                self.check_result(sql, [(1, None), (2, 22)])
                self.check_result(distributed_sql, [(1, 9), (2, 8)])
                if remote_sql is not None:
                    self.check_result(remote_sql, remote_expected)
        finally:
            tdSql.execute('alter local "queryPolicy" "1"')
        print("local and qnode dependency routes ....... [ passed ]")

    def do_integration(self):
        tdSql.query("show local variables")
        settings = {row[0]: row[1] for row in tdSql.queryResult}
        database = f"correlated_scalar_e4_{uuid.uuid4().hex[:8]}"
        tdSql.query("show qnodes")
        create_qnode = tdSql.queryRows == 0
        created_qnode = False
        try:
            tdSql.execute(f"create database {database} vgroups 3")
            try:
                tdSql.execute(f"use {database}")
                tdSql.execute("create table readings (ts timestamp, device varchar(16), v int)")
                tdSql.execute("create table history (ts timestamp, device varchar(16), v int)")
                tdSql.execute("create table result_rows (ts timestamp, v int)")
                tdSql.execute(
                    "insert into readings values "
                    "('2026-09-11 00:00:01', 'd1', 1) "
                    "('2026-09-11 00:00:02', 'd2', 2)"
                )
                tdSql.execute(
                    "insert into history values "
                    "('2026-09-11 00:00:01', 'd1', 11) "
                    "('2026-09-11 00:00:02', 'd2', 22)"
                )
                self.prepare_multi_vnode_stable(database)
                if create_qnode:
                    self.do_mnode_fallback()
                    tdSql.execute("create qnode on dnode 1")
                    created_qnode = True
                self.do_query_policies()
                self.do_cross_node_numeric_params()
                self.do_existing_subquery()
                self.do_noncorrelated_controls()
                self.do_insert_select()
                self.do_view_and_union()
                self.do_stmt_repeated()
                self.do_charset_inheritance(database)
                self.do_session_inheritance(settings["timezone"].split(" (", 1)[0])
                self.do_remote_late_error()
                self.do_dependency_routes(database)
            finally:
                tdSql.execute(f"drop database {database}")
        finally:
            try:
                tdSql.execute(f'alter local "queryPolicy" "{settings["queryPolicy"]}"')
            finally:
                if created_qnode:
                    tdSql.execute("drop qnode on dnode 1")

    def test_correlated_integration(self):
        """Correlated calls integrate with policies and existing subqueries

        1. Policies 1/2/3 return equal values and 4 rejects them.
        2. Ordinary IN/ANY and noncorrelated scalar inside a correlated one keep their results.
        3. STMT2 repeats, INSERT SELECT, view and UNION ALL preserve each binding.
        4. One- and two-level correlated templates inherit the current
           session timezone and first day of week.
        5. A later-row scalar error from a qnode route reaches the root.
        6. Existing noncorrelated dependencies inside the template return
           the same result through vnode, qnode and cross-dnode routes.

        Catalog:
            - Query:SubQuery

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-17 Codex Add policy and SQL entry integration checks.
            - 2026-09-17 Codex Cover a remote ordinary subquery producer.

        """
        self.do_integration()
