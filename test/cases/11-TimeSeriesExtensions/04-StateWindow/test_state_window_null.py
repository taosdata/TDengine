from new_test_framework.utils import tdLog, tdSql

class TestStateWindowNull:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_state_window_null(self):
        """test state window with null state column

        1. data start or end with null
        2. window contains null
        3. null between windows

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.3.6.14

        Labels: common,ci

        Jira: TD-38389

        History:
            - 2025-10-24 Tony Zhang created

        """
        self.prepare_data()
        self.check_extend_normal_table()
        self.check_extend_super_table()

    def prepare_data(self):
        tdSql.execute("drop database if exists testdb")
        tdSql.execute("create database if not exists testdb", show=True)
        tdSql.execute("use testdb")
        values = """
                 ('2025-09-01 10:00:00', null,    20), \
                 ('2025-09-01 10:00:01', 'a',     23.5), \
                 ('2025-09-01 10:00:02', 'a',     25.9), \
                 ('2025-09-01 10:02:15', null,    26), \
                 ('2025-09-01 10:02:45', 'a',     28), \
                 ('2025-09-01 10:04:00', null,    24.3), \
                 ('2025-09-01 10:05:00', null,    null), \
                 ('2025-09-01 11:01:10', 'b',     18), \
                 ('2025-09-01 12:03:22', 'b',     14.4), \
                 ('2025-09-01 12:20:19', 'a',     17.7), \
                 ('2025-09-01 13:00:00', 'a',     null), \
                 ('2025-09-01 14:00:00', null,    22.3), \
                 ('2025-09-01 18:18:18', 'b',     18.18), \
                 ('2025-09-01 20:00:00', 'b',     19.5), \
                 ('2025-09-02 08:00:00', null,    9.9)
                 """
        # normal table
        tdSql.execute("create table ntb (ts timestamp, s varchar(10), v double)", show=True)
        tdSql.execute(f"insert into ntb values {values}", show=True)

        # super table
        tdSql.execute("create table stb (ts timestamp, s varchar(10), v double) tags (gid int)", show=True)
        tdSql.execute("create table ctb1 using stb tags (1)", show=True)
        tdSql.execute("create table ctb2 using stb tags (2)", show=True)
        tdSql.execute(f"insert into ctb1 values {values}", show=True)
        tdSql.execute(f"insert into ctb2 values {values}", show=True)

    def check_extend_normal_table(self):
        tdSql.execute("use testdb")
        # no extend, default 0
        tdSql.query("select _wstart, _wduration, _wend, count(*), count(s), count(v), \
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts) \
                    from ntb state_window(s)", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-09-01 10:00:00.000")
        tdSql.checkData(0, 1, 165000)
        tdSql.checkData(0, 2, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 3, 5)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 24.68)
        tdSql.checkData(0, 7, 20)
        tdSql.checkData(0, 8, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 9, "2025-09-01 10:02:45.000")
        tdSql.checkData(1, 0, "2025-09-01 11:01:10.000")
        tdSql.checkData(1, 1, 3732000)
        tdSql.checkData(1, 2, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, 2)
        tdSql.checkData(1, 6, 16.2)
        tdSql.checkData(1, 7, 18)
        tdSql.checkData(1, 8, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 9, "2025-09-01 12:03:22.000")
        tdSql.checkData(2, 0, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 1, 2381000)
        tdSql.checkData(2, 2, "2025-09-01 13:00:00.000")
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 2)
        tdSql.checkData(2, 5, 1)
        tdSql.checkData(2, 6, 17.7)
        tdSql.checkData(2, 7, 17.7)
        tdSql.checkData(2, 8, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 9, "2025-09-01 13:00:00.000")
        tdSql.checkData(3, 0, "2025-09-01 18:18:18.000")
        tdSql.checkData(3, 1, 49302000)
        tdSql.checkData(3, 2, "2025-09-02 08:00:00.000")
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 2)
        tdSql.checkData(3, 5, 3)
        tdSql.checkData(3, 6, 15.86)
        tdSql.checkData(3, 7, 18.18)
        tdSql.checkData(3, 8, "2025-09-02 08:00:00.000")
        tdSql.checkData(3, 9, "2025-09-02 08:00:00.000")

    def check_extend_super_table(self):
        tdSql.execute("use testdb")
        # no extend, default 0
        tdSql.query("select _wstart, _wduration, _wend, count(*), count(s), count(v), \
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts) \
                    from stb state_window(s)", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-09-01 10:00:00.000")
        tdSql.checkData(0, 1, 165000)
        tdSql.checkData(0, 2, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 3, 10)
        tdSql.checkData(0, 4, 6)
        tdSql.checkData(0, 5, 10)
        tdSql.checkData(0, 6, 24.68)
        tdSql.checkData(0, 7, 20)
        tdSql.checkData(0, 8, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 9, "2025-09-01 10:02:45.000")
        tdSql.checkData(1, 0, "2025-09-01 11:01:10.000")
        tdSql.checkData(1, 1, 3732000)
        tdSql.checkData(1, 2, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 3, 4)
        tdSql.checkData(1, 4, 4)
        tdSql.checkData(1, 5, 4)
        tdSql.checkData(1, 6, 16.2)
        tdSql.checkData(1, 7, 18)
        tdSql.checkData(1, 8, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 9, "2025-09-01 12:03:22.000")
        tdSql.checkData(2, 0, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 1, 2381000)
        tdSql.checkData(2, 2, "2025-09-01 13:00:00.000")
        tdSql.checkData(2, 3, 4)
        tdSql.checkData(2, 4, 4)
        tdSql.checkData(2, 5, 2)
        tdSql.checkData(2, 6, 17.7)
        tdSql.checkData(2, 7, 17.7)
        tdSql.checkData(2, 8, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 9, "2025-09-01 13:00:00.000")
        tdSql.checkData(3, 0, "2025-09-01 18:18:18.000")
        tdSql.checkData(3, 1, 49302000)
        tdSql.checkData(3, 2, "2025-09-02 08:00:00.000")
        tdSql.checkData(3, 3, 6)
        tdSql.checkData(3, 4, 4)
        tdSql.checkData(3, 5, 6)
        tdSql.checkData(3, 6, 15.86)
        tdSql.checkData(3, 7, 18.18)
        tdSql.checkData(3, 8, "2025-09-02 08:00:00.000")
        tdSql.checkData(3, 9, "2025-09-02 08:00:00.000")
