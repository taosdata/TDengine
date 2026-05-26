
from new_test_framework.utils import tdLog, tdSql, etool

# Add extra imports only when needed:
# from new_test_framework.utils import sc, clusterComCheck
# import time


class TestCaseTemplate:
    """
    Template for TDengine Python test cases.

    Rules:
      - One class per file; class name is PascalCase of the file name.
      - Only test_* methods are pytest entry points.
      - test_* body calls do_* methods only — no inline SQL or assertions.
      - setup_class handles one-time initialization.
      - All comments and docstrings in English.
      - SQL via tdSql global; shell ops via etool.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # one-time setup: e.g. create shared databases, users, config

    #
    # --- util ---
    #

    def check_something(self, expected, actual):
        """Example helper — keep helpers small and focused."""
        if expected != actual:
            raise Exception(f"expected {expected}, got {actual}")

    #
    # --- impl ---
    #

    def prepare(self):
        # create prerequisite objects shared across do_* methods
        tdSql.execute("CREATE DATABASE IF NOT EXISTS testdb")
        tdSql.execute("USE testdb")

    def do_create(self):
        # normal path
        tdSql.execute("CREATE TABLE t1 (ts TIMESTAMP, val INT)")
        tdSql.query("SELECT * FROM information_schema.ins_tables WHERE db_name='testdb'")
        tdSql.checkRows(1)

        # exception path
        tdSql.error("CREATE TABLE t1 (ts TIMESTAMP, val INT)")  # duplicate

        tdLog.info("create ............................. [ passed ]")

    def do_query(self):
        tdSql.execute("INSERT INTO testdb.t1 VALUES (NOW(), 42)")
        tdSql.query("SELECT val FROM testdb.t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)

        tdLog.info("query .............................. [ passed ]")

    def do_drop(self):
        tdSql.execute("DROP TABLE IF EXISTS testdb.t1")
        tdSql.query("SELECT * FROM information_schema.ins_tables WHERE db_name='testdb'")
        tdSql.checkRows(0)

        tdSql.error("DROP TABLE testdb.t1")  # already dropped

        tdLog.info("drop ............................... [ passed ]")

    #
    # --- main ---
    #

    def test_case_template(self):
        """One-line description of what this test covers

        1. Create: normal creation and duplicate error
        2. Query: insert and verify data
        3. Drop: normal drop, IF EXISTS, and error on re-drop

        Catalog:
            - CategoryName

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-01 Author Created

        """
        self.prepare()
        self.do_create()
        self.do_query()
        self.do_drop()
