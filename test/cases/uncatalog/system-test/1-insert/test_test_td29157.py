from new_test_framework.utils import tdLog, tdSql

class TestTd29157:
    """Verify td-29157
    """
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.db_name = "td29157"

    def test_td29157(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
        self.conn.execute(f"drop database if exists {self.db_name}")
        self.conn.execute(f"CREATE DATABASE {self.db_name}")
        self.conn.execute(f"USE {self.db_name}")

        tdSql.execute("create table stb1 (ts timestamp, c0 varbinary(10)) tags(t0 varbinary(10));")
        tdSql.execute("insert into ctb11 using stb1 tags(\"0x11\") values(now,\"0x01\");")
        tdSql.execute("insert into ctb12 using stb1 tags(\"0x22\") values(now,\"0x02\");")
        tdSql.query("show tags from ctb11;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 3, 't0')
        tdSql.checkData(0, 4, 'VARBINARY(10)')
        tdSql.checkData(0, 5, '\\x30783131')
        
        tdSql.execute("create table stb2 (ts timestamp, c0 geometry(500)) tags(t0 geometry(100));")
        tdSql.execute("insert into ctb2 using stb2 tags('LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)') values(now,'POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0))');")
        tdSql.query("show tags from ctb2;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 3, 't0')
        tdSql.checkData(0, 4, 'GEOMETRY(100)')
        tdSql.checkData(0, 5, 'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)')

        tdSql.execute("create table stb3 (ts timestamp, c0 bigint, c1 varchar(10)) tags(t0 geometry(100), t1 varbinary(10));")
        tdSql.execute("insert into ctb3 using stb3 tags('POLYGON EMPTY', \"0x03\") values(now,100, \"abc\");")
        tdSql.query("show tags from ctb3;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, 't0')
        tdSql.checkData(0, 4, 'GEOMETRY(100)')
        tdSql.checkData(0, 5, 'POLYGON EMPTY')
        tdSql.checkData(1, 3, 't1')
        tdSql.checkData(1, 4, 'VARBINARY(10)')
        tdSql.checkData(1, 5, '\\x30783033')
        tdSql.execute("drop database if exists %s" % self.db_name)
        tdLog.success("%s successfully executed" % __file__)
