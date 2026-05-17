from new_test_framework.utils import tdLog, tdSql, tdStream


class TestShowCreateStream:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_create_stream(self):
        """Show create stream

        1. Create a database with super table and child tables
        2. Create streams with different window types (state_window, session)
        3. Verify SHOW CREATE STREAM returns correct stream name and CREATE STREAM SQL
        4. Verify the returned SQL is idempotent (drop stream, re-execute SQL, re-check)
        5. Verify SHOW CREATE STREAM on a non-existent stream returns an error

        Catalog:
            - Streams:ShowCommands

        Since: v3.4.1.8

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-16 Created

        """

        # ---- setup ----
        tdStream.createSnode()
        tdSql.execute("drop database if exists test_scs")
        tdSql.execute("create database test_scs vgroups 1")
        tdSql.execute("use test_scs")
        tdSql.execute(
            "create stable stb (ts timestamp, c1 int, c2 float) tags(t1 int)"
        )
        tdSql.execute("create table t1 using stb tags(1)")
        tdSql.execute("create table t2 using stb tags(2)")

        # ---- case 1: state_window with partition by tbname ----
        tdLog.info("case 1: state_window stream")
        sql_state = (
            "create stream s_state state_window(c1) from stb "
            "partition by tbname into out_state as select * from %%tbname"
        )
        tdSql.execute(sql_state)

        tdSql.query("show create stream s_state")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`test_scs`.`s_state`")
        create_sql_state = tdSql.queryResult[0][1].strip()
        tdLog.info(f"s_state create SQL: {create_sql_state}")
        assert "state_window(c1)" in create_sql_state, \
            f"Expected 'state_window(c1)' in create SQL, got: {create_sql_state}"
        assert "partition by tbname" in create_sql_state, \
            f"Expected 'partition by tbname' in create SQL, got: {create_sql_state}"
        assert "%%tbname" in create_sql_state, \
            f"Expected '%%tbname' in create SQL, got: {create_sql_state}"

        # ---- case 2: session window ----
        tdLog.info("case 2: session window stream")
        sql_session = (
            "create stream s_session session(ts, 1s) from stb "
            "into out_session as select _twstart, sum(c1) from stb"
        )
        tdSql.execute(sql_session)

        tdSql.query("show create stream s_session")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`test_scs`.`s_session`")
        create_sql_session = tdSql.queryResult[0][1].strip()
        tdLog.info(f"s_session create SQL: {create_sql_session}")
        assert "session(ts, 1s)" in create_sql_session, \
            f"Expected 'session(ts, 1s)' in create SQL, got: {create_sql_session}"
        assert "s_session" in create_sql_session, \
            f"Expected stream name 's_session' in create SQL, got: {create_sql_session}"

        # ---- case 3: result is re-executable (idempotency) ----
        tdLog.info("case 3: drop and recreate s_state from returned SQL")
        tdSql.execute("drop stream if exists s_state")
        tdSql.execute(create_sql_state)

        tdSql.query("show create stream s_state")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`test_scs`.`s_state`")
        recreated_sql = tdSql.queryResult[0][1].strip()
        assert "state_window(c1)" in recreated_sql, \
            f"Recreated stream SQL mismatch: {recreated_sql}"

        # ---- case 4: non-existent stream returns error ----
        tdLog.info("case 4: show create stream on non-existent stream")
        tdSql.error("show create stream no_such_stream")

        # ---- case 5: show streams lists both streams ----
        tdLog.info("case 5: show streams lists created streams")
        tdSql.query("show streams")
        stream_names = [row[0] for row in tdSql.queryResult]
        assert "s_state" in stream_names, \
            f"Expected 's_state' in show streams output: {stream_names}"
        assert "s_session" in stream_names, \
            f"Expected 's_session' in show streams output: {stream_names}"

        # ---- teardown ----
        tdSql.execute("drop stream if exists s_state")
        tdSql.execute("drop stream if exists s_session")
        tdSql.execute("drop database if exists test_scs")
        tdLog.info("test_show_create_stream passed")
