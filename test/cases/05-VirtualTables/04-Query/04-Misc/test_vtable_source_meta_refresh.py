###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

import glob
import os
import subprocess
import tempfile
import time


class TestVtableSourceMetaRefresh:
    updatecfgDict = {
        "clientCfg": {
            "enableQueryHb": 0,
            "cDebugFlag": 135,
            "qDebugFlag": 135,
        }
    }
    SOURCE_DB_A = "vtable_meta_src_a"
    SOURCE_DB_B = "vtable_meta_src_b"
    VIRTUAL_DB = "vtable_meta_query"
    CHURN_CYCLES_PER_BATCH = 20
    MAX_CHURN_BATCHES = 5
    QUERY_COUNT_PER_BATCH = 80
    STABLE_QUERY_TIMES = 20

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        for db in (cls.VIRTUAL_DB, cls.SOURCE_DB_A, cls.SOURCE_DB_B):
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

        for db, value in ((cls.SOURCE_DB_A, 101), (cls.SOURCE_DB_B, 202)):
            tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
            tdSql.execute(f"CREATE TABLE {db}.src (ts TIMESTAMP, value INT)")
            tdSql.execute(f"INSERT INTO {db}.src VALUES (now, {value})")

        tdSql.execute(f"CREATE DATABASE {cls.VIRTUAL_DB} VGROUPS 1")
        tdSql.execute(
            f"CREATE STABLE {cls.VIRTUAL_DB}.vstb "
            "(ts TIMESTAMP, value INT) TAGS (device_id INT) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE {cls.VIRTUAL_DB}.vt_0 "
            f"(value FROM {cls.SOURCE_DB_A}.src.value) "
            f"USING {cls.VIRTUAL_DB}.vstb TAGS (0)"
        )

    def teardown_class(cls):
        for db in (cls.VIRTUAL_DB, cls.SOURCE_DB_A, cls.SOURCE_DB_B):
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

    def _query_sql(self):
        return (
            f"SELECT AVG(value), device_id FROM {self.VIRTUAL_DB}.vstb "
            "WHERE ts >= now - 12h AND ts <= now PARTITION BY device_id"
        )

    def _build_churn_sql(self):
        statements = []
        for _ in range(self.CHURN_CYCLES_PER_BATCH):
            statements.extend(
                [
                    f"ALTER VTABLE {self.VIRTUAL_DB}.vt_0 ALTER COLUMN value "
                    f"SET {self.SOURCE_DB_B}.src.value",
                    f"DROP TABLE {self.SOURCE_DB_A}.src",
                    f"CREATE TABLE {self.SOURCE_DB_A}.src "
                    "(ts TIMESTAMP, value INT)",
                    f"INSERT INTO {self.SOURCE_DB_A}.src VALUES (now, 101)",
                    f"ALTER VTABLE {self.VIRTUAL_DB}.vt_0 ALTER COLUMN value "
                    f"SET {self.SOURCE_DB_A}.src.value",
                    f"DROP TABLE {self.SOURCE_DB_B}.src",
                    f"CREATE TABLE {self.SOURCE_DB_B}.src "
                    "(ts TIMESTAMP, value INT)",
                    f"INSERT INTO {self.SOURCE_DB_B}.src VALUES (now, 202)",
                ]
            )
        return ";".join(statements) + ";"

    def _copy_client_cfg(self, cfg_dir, log_dir):
        source_cfg = os.path.join(tdCom.getClientCfgPath(), "taos.cfg")
        os.mkdir(log_dir)
        with open(source_cfg, "r", encoding="utf-8") as source_file:
            cfg_lines = [
                line
                for line in source_file
                if not line.lstrip().startswith("logDir ")
            ]
        with open(
            os.path.join(cfg_dir, "taos.cfg"), "w", encoding="utf-8"
        ) as cfg_file:
            cfg_file.writelines(cfg_lines)
            cfg_file.write(f"\nlogDir {log_dir}\n")

    def _read_client_logs(self, log_dir):
        chunks = []
        for path in glob.glob(os.path.join(log_dir, "taoslog*")):
            with open(path, "r", encoding="utf-8", errors="ignore") as log_file:
                chunks.append(log_file.read())
        return "".join(chunks)

    def _run_client(self, command, output):
        return subprocess.Popen(
            command,
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def _wait_client(self, process, timeout):
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise TimeoutError(f"client process did not finish in {timeout} seconds")

    def _run_churn_batch(self, query_sql):
        with tempfile.TemporaryDirectory(prefix="vtable_meta_refresh_") as root:
            query_cfg = os.path.join(root, "query_cfg")
            query_log = os.path.join(root, "query_log")
            ddl_cfg = os.path.join(root, "ddl_cfg")
            ddl_log = os.path.join(root, "ddl_log")
            os.mkdir(query_cfg)
            os.mkdir(ddl_cfg)
            self._copy_client_cfg(query_cfg, query_log)
            self._copy_client_cfg(ddl_cfg, ddl_log)

            repeated_query = (query_sql + ";") * self.QUERY_COUNT_PER_BATCH
            query_command = [
                etool.taosFile(),
                "-c",
                query_cfg,
                "-s",
                repeated_query,
            ]
            ddl_command = [
                etool.taosFile(),
                "-c",
                ddl_cfg,
                "-s",
                self._build_churn_sql(),
            ]

            with tempfile.TemporaryFile(mode="w+") as query_output_file:
                with tempfile.TemporaryFile(mode="w+") as ddl_output_file:
                    query_process = self._run_client(
                        query_command, query_output_file
                    )
                    time.sleep(0.3)
                    ddl_process = self._run_client(ddl_command, ddl_output_file)
                    ddl_return_code = self._wait_client(ddl_process, 120)
                    query_return_code = self._wait_client(query_process, 120)

                    query_output_file.seek(0)
                    query_output = query_output_file.read()
                    ddl_output_file.seek(0)
                    ddl_output = ddl_output_file.read()

            if ddl_return_code != 0 or "DB error:" in ddl_output:
                raise AssertionError(
                    f"metadata churn failed, return code: {ddl_return_code}\n"
                    f"{ddl_output}"
                )
            if query_return_code != 0 and "No valid epSet" not in query_output:
                raise AssertionError(
                    f"query client failed unexpectedly, return code: "
                    f"{query_return_code}\n{query_output}"
                )

            query_logs = self._read_client_logs(query_log)
            error_message = (
                "No valid epSet for virtual super table data source node"
            )
            retry_seen = (
                "client retry to handle the error" in query_logs
                and error_message in query_logs
            )
            source_db_removed = any(
                "start to remove db from cache" in line
                and (self.SOURCE_DB_A in line or self.SOURCE_DB_B in line)
                for line in query_logs.splitlines()
            )
            return retry_seen, source_db_removed

    def _check_source_metadata_refresh(self, query_sql):
        for batch in range(self.MAX_CHURN_BATCHES):
            retry_seen, source_db_removed = self._run_churn_batch(query_sql)
            if not retry_seen:
                continue
            if not source_db_removed:
                raise AssertionError(
                    "endpoint-miss retry did not clear referenced source DB metadata"
                )
            tdLog.info(f"source DB metadata refreshed in churn batch {batch + 1}")
            return
        raise AssertionError("metadata churn did not trigger endpoint-miss retry")

    def _check_queries_after_churn(self, query_sql):
        for attempt in range(self.STABLE_QUERY_TIMES):
            try:
                tdSql.cursor.execute(query_sql)
                rows = tdSql.cursor.fetchall()
            except Exception as error:
                raise AssertionError(
                    f"query {attempt + 1} still used stale virtual source metadata: "
                    f"{error!r}"
                ) from error

            if len(rows) != 1 or rows[0][0] != 101 or rows[0][1] != 0:
                raise AssertionError(f"unexpected result rows after churn: {rows}")

    def do_source_metadata_refresh(self):
        query_sql = self._query_sql()
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 101)

        self._check_source_metadata_refresh(query_sql)
        self._check_queries_after_churn(query_sql)
        print("virtual table source metadata refresh [ passed ]")

    def test_vtable_source_meta_refresh(self):
        """Query: refresh source metadata when retrying a virtual table query

        1. Warm a long-lived client's virtual table and source metadata caches
        2. Change references and recreate non-current source tables from another client
        3. Verify endpoint-miss retry clears source DB caches and the query recovers

        Catalog:
            - VirtualTable

        Since: v3.4.2.4

        Labels: common,ci,virtual,metadata,retry,regression,integration,functional

        Jira: None

        History:
            - 2026-08-21 Joey Sima Created
        """
        self.do_source_metadata_refresh()
