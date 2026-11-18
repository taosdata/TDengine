# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool
from new_test_framework.utils import tdCom

import os
import subprocess
import sys
import tempfile
import time

import taos


SCHEMA_WORKER = r"""
import os
import sys
import time

import taos


def write_signal(path):
    temp_path = f"{path}.{os.getpid()}.tmp"
    with open(temp_path, "w", encoding="utf-8") as output:
        output.write("ready")
    os.replace(temp_path, path)


def wait_for_signal(path):
    deadline = time.monotonic() + 60
    while not os.path.exists(path):
        if time.monotonic() >= deadline:
            raise TimeoutError("timed out waiting for parent process")
        time.sleep(0.02)


(config, db, target, target_kind, column_type, function, ready_path,
 alter_path, done_path, stop_path) = sys.argv[1:]
conn = taos.connect(config=config)
cur = conn.cursor()
try:
    write_signal(ready_path)
    wait_for_signal(alter_path)
    cur.execute(
        f"ALTER {target_kind.upper()} {db}.{target} "
        f"MODIFY COLUMN v {column_type}(4096)"
    )
    value = ("a" if function == "MIN" else "x") * 3113
    start = 1699999999000 if function == "FIRST" else 1700000000001
    count = 3 if function == "MODE" else 1
    rows = " ".join(f"({start + i}, '{value}')" for i in range(count))
    insert_target = target + "a" if target_kind == "stable" else target
    cur.execute(f"INSERT INTO {db}.{insert_target} VALUES {rows}")
    write_signal(done_path)
    wait_for_signal(stop_path)
finally:
    cur.close()
    conn.close()
"""


class TestLastCacheSchemaRefresh:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def start_schema_worker(config, db, target, target_kind, column_type,
                            function, work_dir):
        ready_path = os.path.join(work_dir, "ready")
        alter_path = os.path.join(work_dir, "alter")
        done_path = os.path.join(work_dir, "done")
        stop_path = os.path.join(work_dir, "stop")
        worker_env = os.environ.copy()
        # Python exit leaks must not fail a successful schema worker.
        for key in ("ASAN_OPTIONS", "LSAN_OPTIONS"):
            options = worker_env.get(key, "").split(":")
            options = [option for option in options
                       if option and not option.startswith("detect_leaks=")]
            worker_env[key] = ":".join(options + ["detect_leaks=0"])
        proc = subprocess.Popen(
            [sys.executable, "-c", SCHEMA_WORKER, config, db, target,
             target_kind, column_type, function, ready_path, alter_path,
             done_path, stop_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=worker_env,
        )
        return proc, ready_path, alter_path, done_path, stop_path

    @staticmethod
    def wait_worker_signal(proc, path, label, timeout=60):
        deadline = time.monotonic() + timeout
        while not os.path.exists(path):
            if proc.poll() is not None:
                stdout, stderr = proc.communicate()
                raise AssertionError(
                    f"worker exited before {label}: code={proc.returncode}, "
                    f"stdout={stdout!r}, stderr={stderr!r}"
                )
            if time.monotonic() >= deadline:
                proc.terminate()
                try:
                    stdout, stderr = proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    stdout, stderr = proc.communicate()
                raise TimeoutError(
                    f"worker timed out before {label}: stdout={stdout!r}, "
                    f"stderr={stderr!r}"
                )
            time.sleep(0.02)

    @staticmethod
    def signal_worker(path):
        temp_path = f"{path}.{os.getpid()}.tmp"
        with open(temp_path, "w", encoding="utf-8") as output:
            output.write("go")
        os.replace(temp_path, path)

    @staticmethod
    def finish_worker(proc):
        try:
            stdout, stderr = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired as error:
            proc.terminate()
            try:
                stdout, stderr = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
            raise TimeoutError(
                f"worker did not exit: stdout={stdout!r}, "
                f"stderr={stderr!r}"
            ) from error
        if stderr:
            sys.stderr.write(stderr)
            sys.stderr.flush()
        if proc.returncode != 0:
            raise AssertionError(
                f"worker exited with code {proc.returncode}: "
                f"stdout={stdout!r}, stderr={stderr!r}"
            )

    @staticmethod
    def stop_worker(proc):
        if proc.poll() is not None:
            return
        proc.terminate()
        try:
            _, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            _, stderr = proc.communicate()
        if stderr:
            sys.stderr.write(stderr)
            sys.stderr.flush()

    def do_cache_schema_refresh(self, config=None):
        config = config or tdCom.getClientCfgPath()
        conn = taos.connect(config=config)
        cursor = conn.cursor()
        db = "last_cache_schema_refresh"
        original = "o" * 2048
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            cursor.execute(f"CREATE DATABASE {db} VGROUPS 1 "
                           "BUFFER 16 CACHEMODEL 'both'")
            index = 0
            for kind in ("VARCHAR", "NCHAR"):
                for function in ("LAST_ROW", "LAST", "FIRST", "MIN", "MAX",
                                 "MODE"):
                    value = ("a" if function == "MIN" else "x") * 3113
                    for clause in ("", " PARTITION BY tbname",
                                   " PARTITION BY id"):
                        stable = f"s{index}"
                        index += 1
                        cursor.execute(f"CREATE STABLE {db}.{stable} "
                                       f"(ts TIMESTAMP, v {kind}(2048)) "
                                       "TAGS(id INT)")
                        for suffix in ("a", "b"):
                            cursor.execute(f"CREATE TABLE {db}.{stable}"
                                           f"{suffix} USING {db}.{stable} "
                                           "TAGS(1)")
                            cursor.execute(f"INSERT INTO {db}.{stable}"
                                           f"{suffix} VALUES "
                                           f"(1700000000000, '{original}')")
                        sql = (f"SELECT {function}(v) FROM {db}.{stable}"
                               + clause)
                        cursor.execute("EXPLAIN " + sql)
                        plan = cursor.fetchall()
                        if function in ("LAST_ROW", "LAST"):
                            assert any("Last Row Scan" in str(row)
                                       for row in plan), sql
                        with tempfile.TemporaryDirectory(
                                prefix="last-cache-stable-") as work_dir:
                            worker = self.start_schema_worker(
                                config, db, stable, "stable", kind, function,
                                work_dir)
                            proc, ready, alter, done, stop = worker
                            try:
                                self.wait_worker_signal(proc, ready, "ready")
                                cursor.execute(sql)
                                old_rows = (
                                    2 if clause == " PARTITION BY tbname"
                                    else 1
                                )
                                assert cursor.fetchall() == (
                                    [(original,)] * old_rows
                                )
                                self.signal_worker(alter)
                                self.wait_worker_signal(proc, done, "done")
                                # Require success without application retry.
                                cursor.execute(sql)
                                rows = cursor.fetchall()
                                expected = (
                                    [(value,), (original,)]
                                    if clause == " PARTITION BY tbname"
                                    else [(value,)]
                                )
                                assert sorted(rows) == sorted(expected), sql
                            except BaseException:
                                self.stop_worker(proc)
                                raise
                            else:
                                try:
                                    self.signal_worker(stop)
                                    self.finish_worker(proc)
                                finally:
                                    self.stop_worker(proc)
            print("cache schema refresh ......................... [ passed ]")
        finally:
            try:
                cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            finally:
                cursor.close()
                conn.close()

    def do_normal_table_schema_refresh(self, config=None):
        config = config or tdCom.getClientCfgPath()
        conn = taos.connect(config=config)
        cursor = conn.cursor()
        db = "normal_table_schema_refresh"
        original = "o" * 2048
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            cursor.execute(f"CREATE DATABASE {db} VGROUPS 1 "
                           "BUFFER 16 CACHEMODEL 'both'")
            for index, function in enumerate(
                    ("LAST_ROW", "MIN", "MAX", "MODE")):
                table = f"t{index}"
                value = ("a" if function == "MIN" else "x") * 3113
                cursor.execute(f"CREATE TABLE {db}.{table} "
                               "(ts TIMESTAMP, v VARCHAR(2048))")
                cursor.execute(f"INSERT INTO {db}.{table} VALUES "
                               f"(1700000000000, '{original}')")
                sql = f"SELECT {function}(v) FROM {db}.{table}"
                if function == "LAST_ROW":
                    cursor.execute("EXPLAIN " + sql)
                    assert any("Last Row Scan" in str(row)
                               for row in cursor.fetchall()), sql
                with tempfile.TemporaryDirectory(
                        prefix="last-cache-normal-") as work_dir:
                    worker = self.start_schema_worker(
                        config, db, table, "table", "VARCHAR", function,
                        work_dir)
                    proc, ready, alter, done, stop = worker
                    try:
                        self.wait_worker_signal(proc, ready, "ready")
                        cursor.execute(sql)
                        assert cursor.fetchall() == [(original,)]
                        self.signal_worker(alter)
                        self.wait_worker_signal(proc, done, "done")
                        cursor.execute(sql)
                        assert cursor.fetchall() == [(value,)], sql
                    except BaseException:
                        self.stop_worker(proc)
                        raise
                    else:
                        try:
                            self.signal_worker(stop)
                            self.finish_worker(proc)
                        finally:
                            self.stop_worker(proc)
            print("normal table schema refresh .................. [ passed ]")
        finally:
            try:
                cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            finally:
                cursor.close()
                conn.close()

    def do_virtual_aggregate_width_errors(self, config=None):
        config = config or tdCom.getClientCfgPath()
        conn = taos.connect(config=config)
        cursor = conn.cursor()
        db = "last_cache_virtual_width"
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            cursor.execute(f"CREATE DATABASE {db} VGROUPS 2 BUFFER 16")
            cursor.execute(f"CREATE TABLE {db}.src "
                           "(ts TIMESTAMP, v VARCHAR(2048))")
            cursor.execute(f"INSERT INTO {db}.src VALUES "
                           f"(1700000000000, '{'x' * 1200}')")
            cursor.execute(f"CREATE STABLE {db}.vst "
                           "(ts TIMESTAMP, v VARCHAR(4)) "
                           "TAGS(id INT) VIRTUAL 1")
            cursor.execute(f"CREATE VTABLE {db}.vc(v FROM {db}.src.v) "
                           f"USING {db}.vst TAGS(1)")
            expressions = ("FIRST(v)", "MIN(v)", "MAX(v)", "MODE(v)",
                           "LAST(v)", "LAST_ROW(v)", "SAMPLE(v, 1)",
                           "VALUE_CHANGES(v)")
            queries = [f"SELECT {expr} FROM {db}.vst" for expr in expressions]
            queries[-1] += " PARTITION BY tbname"
            queries.append(f"SELECT v_a FROM TABLE(TIME_ALIGN("
                           f"TABLE {db}.vst, 'v:PREV:_a'))")
            for sql in queries:
                try:
                    cursor.execute(sql)
                    cursor.fetchall()
                except taos.Error as error:
                    assert error.errno & 0xFFFF == 0x070B, (sql, str(error))
                else:
                    raise AssertionError(f"expected buffer error: {sql}")
            cursor.execute(f"SELECT TAIL(v, 1) FROM {db}.vst")
            assert cursor.fetchall() == [("x" * 1200,)]
            cursor.execute(f"CREATE VTABLE {db}.vflat "
                           f"(ts TIMESTAMP, v VARCHAR(4) FROM {db}.src.v)")
            cursor.execute(f"SELECT SAMPLE(v, 1) FROM {db}.vflat")
            assert cursor.fetchall() == [("x" * 1200,)]
            print("virtual aggregate widths ..................... [ passed ]")
        finally:
            try:
                cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            finally:
                cursor.close()
                conn.close()

    def do_primary_key_aggregate_widths(self, config=None):
        config = config or tdCom.getClientCfgPath()
        conn = taos.connect(config=config)
        cursor = conn.cursor()
        db = "last_cache_pk_width"
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            cursor.execute(f"CREATE DATABASE {db} VGROUPS 2 "
                           "BUFFER 16 CACHEMODEL 'both'")
            schema = "(ts TIMESTAMP, pk VARCHAR(16) PRIMARY KEY, v INT)"
            cursor.execute(f"CREATE TABLE {db}.ntb {schema}")
            cursor.execute(f"CREATE STABLE {db}.st {schema} TAGS(id INT)")
            cursor.execute(f"CREATE TABLE {db}.ctb USING {db}.st TAGS(1)")
            for table in ("ntb", "ctb"):
                cursor.execute(f"INSERT INTO {db}.{table} VALUES "
                               "(1700000000000, 'a', 10) "
                               "(1700000001000, 'b', 20)")
            for table in ("ntb", "ctb", "st"):
                for function, expected in (("FIRST", 10), ("LAST", 20),
                                           ("LAST_ROW", 20), ("IRATE", 10.0)):
                    sql = f"SELECT {function}(v) FROM {db}.{table}"
                    try:
                        cursor.execute(sql)
                    except taos.Error as error:
                        raise AssertionError((sql, str(error))) from error
                    assert cursor.fetchall() == [(expected,)], (table,
                                                               function)

            cursor.execute(f"CREATE STABLE {db}.merge_st {schema} "
                           "TAGS(id INT)")
            child_names = [f"merge_ct{i}" for i in range(16)]
            for index, child in enumerate(child_names):
                cursor.execute(f"CREATE TABLE {db}.{child} "
                               f"USING {db}.merge_st TAGS({index})")
            cursor.execute(
                "SELECT table_name, vgroup_id "
                "FROM information_schema.ins_tables "
                f"WHERE db_name = '{db}' AND stable_name = 'merge_st'"
            )
            by_vgroup = {}
            for child, vgroup in cursor.fetchall():
                by_vgroup.setdefault(vgroup, child)
            assert len(by_vgroup) >= 2, by_vgroup
            left, right = list(by_vgroup.values())[:2]
            cursor.execute(f"INSERT INTO {db}.{left} VALUES "
                           "(1700000000000, 'a', 10) "
                           "(1700000003000, 'ccc', 30)")
            cursor.execute(f"INSERT INTO {db}.{right} VALUES "
                           "(1700000001000, 'bb', 20) "
                           "(1700000004000, 'dddd', 40)")
            cursor.execute(f"SELECT IRATE(v) FROM {db}.merge_st")
            assert cursor.fetchall() == [(10.0,)]
            print("primary key aggregate widths ................. [ passed ]")
        finally:
            try:
                cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            finally:
                cursor.close()
                conn.close()

    def test_cache_schema_refresh(self):
        """Aggregates refresh stale column widths transparently

        Validate the first query after another Native process widens a column,
        for FIRST/LAST/LAST_ROW/MIN/MAX/MODE, VARCHAR/NCHAR and three grouping
        forms. Cached LAST queries exercise both cache retrieval modes.

        Catalog:
            - Function

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Verify schema refresh on the original connection
        """
        self.do_cache_schema_refresh()

    def test_normal_table_schema_refresh(self):
        """Normal tables refresh stale column widths transparently

        Validate the original Native connection succeeds on its first query
        after another process widens a normal-table column. LAST_ROW, MIN,
        MAX and MODE cover cache, extreme-value and stored-value paths.

        Catalog:
            - Function

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-16 Cover normal-table metadata refresh
        """
        self.do_normal_table_schema_refresh()

    def test_virtual_aggregate_width_errors(self):
        """Virtual supertable wide values retain buffer errors

        Verify selection, stored-value and grouping functions over one virtual
        child retain buffer errors for legitimate source-width values, including
        TIME_ALIGN synthesized columns. TAIL continues returning the full source
        value when its plan permits it.

        Catalog:
            - Function

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-13 Preserve virtual supertable width error classification
        """
        self.do_virtual_aggregate_width_errors()

    def test_primary_key_aggregate_widths(self):
        """String primary keys retain aggregate slot semantics

        Validate FIRST/LAST/LAST_ROW/IRATE on normal, child and supertables with
        a VARCHAR primary key. An unpartitioned two-vgroup IRATE query verifies
        variable-length primary-key states cross the partial/merge path.

        Catalog:
            - Function

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-16 Verify planned primary-key slot widths
        """
        self.do_primary_key_aggregate_widths()
