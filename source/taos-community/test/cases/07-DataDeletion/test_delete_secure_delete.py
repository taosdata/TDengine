import time

from new_test_framework.utils import tdLog, tdSql


class TestDeleteSecureDelete:

    def _prepare_database(self, db_name: str, secure_delete: int):
        tdSql.execute(f"DROP DATABASE IF EXISTS {db_name}")
        tdSql.execute(
            f"CREATE DATABASE {db_name} KEEP 365 DURATION 10 SECURE_DELETE {secure_delete}"
        )
        tdSql.execute(f"USE {db_name}")

    def _prepare_stable(self, stb_name: str, secure_delete: int = None):
        option = f" SECURE_DELETE {secure_delete}" if secure_delete is not None else ""
        tdSql.execute(
            f"CREATE STABLE {stb_name} (ts TIMESTAMP, v INT) TAGS (t INT){option}"
        )
        tdSql.execute(f"CREATE TABLE {stb_name}_1 USING {stb_name} TAGS(1)")
        tdSql.execute(f"INSERT INTO {stb_name}_1 VALUES (now-10s, 10)")
        tdSql.execute(f"INSERT INTO {stb_name}_1 VALUES (now-5s, 20)")
        tdSql.execute(f"INSERT INTO {stb_name}_1 VALUES (now, 30)")

    def _check_show_create_database_secure_delete(self, db_name: str, expected: int, timeout: int = 10):
        deadline = time.time() + timeout
        while True:
            tdSql.query(f"SHOW CREATE DATABASE {db_name}")
            create_sql = tdSql.getData(0, 1)
            tdLog.info(f"SHOW CREATE DATABASE output: {create_sql}")
            if f"SECURE_DELETE {expected}" in create_sql:
                return
            if time.time() >= deadline:
                assert 0, f"SECURE_DELETE {expected} not found in: {create_sql}"
            time.sleep(1)

    def _check_show_create_stable_secure_delete(self, stb_name: str, expected: int):
        tdSql.query(f"SHOW CREATE TABLE {stb_name}")
        create_sql = tdSql.getData(0, 1)
        tdSql.checkEqual(f"SECURE_DELETE {expected}" in create_sql, True)

    def test_database_secure_delete_enable(self):
        """Database secure delete enable

        1. Create database with `SECURE_DELETE 1`
        2. Verify database option via SHOW CREATE DATABASE

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Codex Created
        """
        tdLog.info("=== test database secure_delete enable ===")
        self._prepare_database("test_db_secure_delete_on", 1)
        self._check_show_create_database_secure_delete("test_db_secure_delete_on", 1)

    def test_database_secure_delete_disable(self):
        """Database secure delete disable

        1. Create database with `SECURE_DELETE 1`
        2. Alter database to `SECURE_DELETE 0`
        3. Verify database option via SHOW CREATE DATABASE

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Codex Created
        """
        tdLog.info("=== test database secure_delete disable ===")
        db_name = "test_db_secure_delete_off"
        self._prepare_database(db_name, 1)
        tdSql.execute(f"ALTER DATABASE {db_name} SECURE_DELETE 0")
        self._check_show_create_database_secure_delete(db_name, 0)

    def test_stable_secure_delete_enable(self):
        """Super table secure delete enable

        1. Create database with `SECURE_DELETE 0`
        2. Create super table with `SECURE_DELETE 1`
        3. Verify super table option via SHOW CREATE TABLE

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Codex Created
        """
        tdLog.info("=== test stable secure_delete enable ===")
        self._prepare_database("test_stb_secure_delete_on", 0)
        self._prepare_stable("stb_secure_delete_on", secure_delete=1)
        self._check_show_create_stable_secure_delete("stb_secure_delete_on", 1)

    def test_stable_secure_delete_disable(self):
        """Super table secure delete disable

        1. Create database with `SECURE_DELETE 0`
        2. Create super table with `SECURE_DELETE 1`
        3. Alter super table to `SECURE_DELETE 0`
        4. Verify super table option via SHOW CREATE TABLE

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Codex Created
        """
        tdLog.info("=== test stable secure_delete disable ===")
        self._prepare_database("test_stb_secure_delete_off", 0)
        self._prepare_stable("stb_secure_delete_off", secure_delete=1)
        tdSql.execute("ALTER STABLE stb_secure_delete_off SECURE_DELETE 0")
        self._check_show_create_stable_secure_delete("stb_secure_delete_off", 0)

    def test_delete_statement_with_secure_delete_option(self):
        """Delete statement secure delete option

        1. Create database and super table with secure delete disabled
        2. Execute DELETE statement with explicit `SECURE_DELETE`
        3. Verify delete effect by row count

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Codex Created
        """
        tdLog.info("=== test delete statement with SECURE_DELETE option ===")
        self._prepare_database("test_delete_stmt_secure_delete", 0)
        self._prepare_stable("stb_delete_stmt_secure_delete", secure_delete=0)

        tdSql.query("SELECT COUNT(*) FROM stb_delete_stmt_secure_delete")
        tdSql.checkData(0, 0, 3)

        tdSql.execute(
            "DELETE FROM stb_delete_stmt_secure_delete WHERE ts <= now-5s SECURE_DELETE"
        )
        tdSql.query("SELECT COUNT(*) FROM stb_delete_stmt_secure_delete")
        tdSql.checkData(0, 0, 1)
