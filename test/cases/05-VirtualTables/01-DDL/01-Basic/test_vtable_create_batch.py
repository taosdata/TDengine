import taos
from new_test_framework.utils import TDSql, tdLog, tdSql


class TestVtableCreateBatch:
    DB_A = "test_vtable_create_batch_a"
    DB_B = "test_vtable_create_batch_b"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        for db in (cls.DB_A, cls.DB_B):
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

        cls._prepare_database(cls.DB_A, 10)
        cls._prepare_database(cls.DB_B, 30)

    def teardown_class(cls):
        for db in (cls.DB_A, cls.DB_B):
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

    # --- util ---

    @classmethod
    def _prepare_database(cls, db, value_base):
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
        tdSql.execute(f"USE {db}")
        tdSql.execute(
            "CREATE STABLE src_stb (ts TIMESTAMP, value INT) "
            "TAGS (site INT, src_code INT)"
        )
        tdSql.execute(
            f"CREATE TABLE src_0 USING src_stb "
            f"TAGS ({value_base}, {value_base * 10})"
        )
        tdSql.execute(
            f"CREATE TABLE src_1 USING src_stb "
            f"TAGS ({value_base + 10}, {(value_base + 10) * 10})"
        )
        tdSql.execute(
            f"INSERT INTO src_0 VALUES "
            f"('2026-08-19 00:00:00.000', {value_base + 1})"
        )
        tdSql.execute(
            f"INSERT INTO src_0 VALUES "
            f"('2026-08-19 00:00:00.001', {value_base + 2})"
        )
        tdSql.execute(
            f"INSERT INTO src_1 VALUES "
            f"('2026-08-19 00:00:00.000', {value_base + 11})"
        )
        tdSql.execute(
            f"INSERT INTO src_1 VALUES "
            f"('2026-08-19 00:00:00.001', {value_base + 12})"
        )
        for vst in ("vst_main", "vst_alt"):
            tdSql.execute(
                f"CREATE STABLE {vst} (ts TIMESTAMP, value INT) "
                "TAGS (site INT, src_code INT) VIRTUAL 1"
            )

    def _check_exists(self, db, table, expected):
        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{db}' AND table_name='{table}'"
        )
        tdSql.checkData(0, 0, expected)

    def _check_values(self, table, first, second):
        tdSql.query(f"SELECT value FROM {table} ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, first)
        tdSql.checkData(1, 0, second)

    # --- impl ---

    def do_basic_column_reference_forms(self):
        db = self.DB_A
        tdSql.execute(f"USE {db}")
        tdSql.execute(
            "CREATE VTABLE "
            "v_explicit (value FROM src_0.value) "
            "USING vst_main TAGS (1, 101) "
            "v_positional (src_1.value) "
            "USING vst_main TAGS (2, 202) "
            "v_without_ref USING vst_main TAGS (3, 303)"
        )

        tdSql.query(f"SHOW {db}.VTABLES")
        tdSql.checkRows(3)
        tdSql.query(f"SHOW CHILD {db}.VTABLES")
        tdSql.checkRows(3)
        tdSql.query(f"SHOW CREATE VTABLE {db}.v_explicit")
        tdSql.checkRows(1)
        self._check_values(f"{db}.v_explicit", 11, 12)
        self._check_values(f"{db}.v_positional", 21, 22)
        tdSql.query(f"SELECT * FROM {db}.v_without_ref")
        tdSql.checkRows(0)

        tdLog.info("basic batch column references passed")

    def do_multiple_vst_database_and_vgroup(self):
        db_a = self.DB_A
        db_b = self.DB_B
        tdSql.execute(f"USE {db_a}")
        tdSql.execute(
            "CREATE VTABLE "
            f"{db_a}.v_multi_main (value FROM {db_a}.src_0.value) "
            f"USING {db_a}.vst_main TAGS (4, 404) "
            f"{db_a}.v_multi_alt ({db_a}.src_1.value) "
            f"USING {db_a}.vst_alt TAGS (5, 505) "
            f"{db_b}.v_multi_cross (value FROM {db_b}.src_0.value) "
            f"USING {db_b}.vst_main TAGS ("
            f"site FROM {db_a}.src_0.site, "
            f"src_code FROM {db_a}.src_0.src_code)"
        )

        for db, table in (
            (db_a, "v_multi_main"),
            (db_a, "v_multi_alt"),
            (db_b, "v_multi_cross"),
        ):
            self._check_exists(db, table, 1)

        tdSql.query(
            "SELECT count(DISTINCT vgroup_id) "
            "FROM information_schema.ins_tables WHERE "
            f"(db_name='{db_a}' AND table_name IN "
            "('v_multi_main', 'v_multi_alt')) OR "
            f"(db_name='{db_b}' AND table_name='v_multi_cross')"
        )
        tdSql.checkData(0, 0, 2)
        self._check_values(f"{db_b}.v_multi_cross", 31, 32)
        tdSql.query(
            f"SELECT site, src_code FROM {db_b}.v_multi_cross LIMIT 1"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 100)

        tdLog.info("multiple VST database and vgroup passed")

    def do_tag_references_and_if_not_exists(self):
        db = self.DB_A
        tdSql.execute(f"USE {db}")
        tdSql.execute(
            "CREATE VTABLE "
            "v_tag_named (src_0.value) USING vst_main TAGS ("
            "site FROM src_0.site, src_code FROM src_0.src_code) "
            "v_tag_positional (src_1.value) USING vst_main TAGS ("
            "src_1.site, src_1.src_code)"
        )
        tdSql.query(
            "SELECT value, site, src_code FROM v_tag_named ORDER BY ts"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 100)
        tdSql.query(
            "SELECT value, site, src_code FROM v_tag_positional ORDER BY ts"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 21)
        tdSql.checkData(0, 1, 20)
        tdSql.checkData(0, 2, 200)

        tdSql.execute(
            "CREATE VTABLE v_ine_existing (src_0.value) "
            "USING vst_main TAGS (7, 707)"
        )
        batch_sql = (
            "CREATE VTABLE "
            "IF NOT EXISTS v_ine_existing (src_1.value) "
            "USING vst_main TAGS (70, 7070) "
            "v_ine_new (src_1.value) USING vst_main TAGS (8, 808)"
        )
        tdSql.execute(batch_sql)
        tdSql.error(batch_sql)
        self._check_exists(db, "v_ine_existing", 1)
        self._check_exists(db, "v_ine_new", 1)
        self._check_values("v_ine_existing", 11, 12)

        tdLog.info("tag references and per-clause IF NOT EXISTS passed")

    def do_prevalidation_failures(self):
        db = self.DB_A
        tdSql.execute(f"USE {db}")
        tdSql.error(
            "CREATE VTABLE "
            "v_precheck_ok (src_0.value) USING vst_main TAGS (9, 909) "
            "v_precheck_bad (missing_source.value) "
            "USING vst_main TAGS (10, 1010)"
        )
        self._check_exists(db, "v_precheck_ok", 0)
        self._check_exists(db, "v_precheck_bad", 0)

        tdLog.info("prevalidation failure creates no tables")

    def do_permission_prevalidation(self):
        db = self.DB_A
        user = "batch_vtable_user"
        tdSql.execute(f"USE {db}")
        for index in range(2):
            tdSql.execute(
                f"CREATE STABLE auth_src_stb_{index} "
                "(ts TIMESTAMP, value INT) TAGS (site INT, src_code INT)"
            )
            tdSql.execute(
                f"CREATE TABLE auth_src_{index} USING auth_src_stb_{index} "
                f"TAGS ({index + 1}, {(index + 1) * 100})"
            )
        tdSql.execute(f"DROP USER IF EXISTS {user}")
        tdSql.execute(f"CREATE USER {user} PASS 'batch12@#*'")
        tdSql.execute(f"GRANT USE ON DATABASE {db} TO {user}")
        tdSql.execute(f"GRANT CREATE TABLE ON DATABASE {db} TO {user}")
        tdSql.execute(f"GRANT SELECT ON {db}.auth_src_stb_0 TO {user}")
        tdSql.execute("RESET QUERY CACHE")

        connection = taos.connect(user=user, password="batch12@#*")
        user_sql = TDSql()
        user_sql.init(connection.cursor())
        user_sql.execute(f"USE {db}")
        user_sql.execute(
            "CREATE VTABLE v_auth_allowed (auth_src_0.value) "
            "USING vst_main TAGS ("
            "site FROM auth_src_0.site, src_code FROM auth_src_0.src_code)"
        )
        user_sql.error(
            "CREATE VTABLE "
            "v_auth_first (auth_src_0.value) USING vst_main TAGS ("
            "site FROM auth_src_0.site, src_code FROM auth_src_0.src_code) "
            "v_auth_second (auth_src_0.value) USING vst_main TAGS ("
            "site FROM auth_src_1.site, src_code FROM auth_src_1.src_code)",
            expectErrInfo="Permission denied to select from table or view",
        )
        connection.close()

        self._check_exists(db, "v_auth_allowed", 1)
        self._check_exists(db, "v_auth_first", 0)
        self._check_exists(db, "v_auth_second", 0)
        tdSql.execute(f"DROP VTABLE {db}.v_auth_allowed")
        tdSql.execute(f"DROP USER {user}")

        tdLog.info("permission failure creates no tables")

    def do_reject_in_batch_reference(self):
        db = self.DB_A
        tdSql.execute(f"USE {db}")
        tdSql.error(
            "CREATE VTABLE "
            "v_chain_source (src_0.value) USING vst_main TAGS (11, 1111) "
            "v_chain_target (value FROM v_chain_source.value) "
            "USING vst_main TAGS (12, 1212)"
        )
        self._check_exists(db, "v_chain_source", 0)
        self._check_exists(db, "v_chain_target", 0)

        tdLog.info("in-batch reference is rejected")

    def do_partial_failure_and_retry(self):
        db = self.DB_A
        tdSql.execute(f"USE {db}")
        tdSql.error(
            "CREATE VTABLE "
            "v_partial (src_0.value) USING vst_alt TAGS (13, 1313) "
            "v_partial (src_1.value) USING vst_alt TAGS (14, 1414)"
        )
        self._check_exists(db, "v_partial", 1)
        self._check_values("v_partial", 11, 12)

        tdSql.execute(
            "CREATE VTABLE "
            "IF NOT EXISTS v_partial (src_0.value) "
            "USING vst_alt TAGS (13, 1313) "
            "IF NOT EXISTS v_partial (src_1.value) "
            "USING vst_alt TAGS (14, 1414)"
        )
        self._check_exists(db, "v_partial", 1)
        self._check_values("v_partial", 11, 12)

        tdLog.success(f"{__file__} successfully executed")
        tdLog.info("partial failure IF NOT EXISTS retry passed")

    # --- main ---

    def test_vtable_create_batch(self):
        """Batch-create virtual child tables with validation and retry semantics

        1. Create a batch with explicit, positional, and empty column references
        2. Create across virtual stables, databases, and vgroups
        3. Verify tag references and per-clause IF NOT EXISTS
        4. Verify metadata validation failure creates no tables
        5. Verify tag-reference permission failure creates no tables
        6. Reject references to another virtual table in the same batch
        7. Verify duplicate-target partial success and idempotent retry

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,virtual,ddl,batch

        Jira: None

        History:
            - 2026-08-19 Created

        """
        self.do_basic_column_reference_forms()
        self.do_multiple_vst_database_and_vgroup()
        self.do_tag_references_and_if_not_exists()
        self.do_prevalidation_failures()
        self.do_permission_prevalidation()
        self.do_reject_in_batch_reference()
        self.do_partial_failure_and_retry()
