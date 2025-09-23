from new_test_framework.utils import tdLog, tdSql

class TestTd27388:
    """Verify the insert with format exception for task TD-27388
    """
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql))
        cls.dbname = 'db'
        cls.stbname = 'st'
        tdSql.execute("create database {};".format(cls.dbname))
        tdSql.execute("use {};".format(cls.dbname))
        tdSql.execute("create table st (ts timestamp, col1 int, col2 varchar(64)) tags (t1 int, t2 varchar(32));")

    def check_half_quotes(self):
        sql_list = [
            "insert into t1 using st tags(1, 'tag1) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, tag1') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, test msg');",
            "insert into t1 using st tags(1, 'tag1' values(now, 1, test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg)';",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2) values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, tag2') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1) values(now, 1, 'test msg') t2 using st tags(2, 'tag2) values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg) t2 using st tags(2, 'tag2') values(now, 2, test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, 'test msg);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, 'test msg);"
        ]
        for sql in sql_list:
            tdLog.debug("execute harlf quotes sql: %s" % sql)
            tdSql.error(sql)

    def check_esc(self):
        sql_list = [
            "insert into t1 using st tags(1, 'tag1\\') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, \\'tag1') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg\\');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, \\'test msg');",
            "insert into t1 using st tags(1, \\'tag1\\') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, \\'test msg\\');",
            "insert into t1 using st tags(1, \\'tag1\\') values(now, 1, \\'test msg\\');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2\\') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, \\'tag2') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, \\'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, 'test msg\\');",
            "insert into t1 using st tags(1, \\'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2\\') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2') values(now, 2, \\'test msg\\');"
        ]
        for sql in sql_list:
            tdLog.debug("execute escape character sql: %s" % sql)
            tdSql.error(sql)

    def check_specific_character(self):
        sql_list = [
            "insert into t1 using st tags(1, 'tag1$) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1,) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1'') values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1() values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1*) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1+) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1,) values(now, 1, 'test msg');",
            "isnert into t1 using st tags(1, 'tag1-) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1.) values(now, 1, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg$);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg,);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg+%+-.);",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2$) values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2,) values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2'') values(now, 2, 'test msg');",
            "insert into t1 using st tags(1, 'tag1') values(now, 1, 'test msg') t2 using st tags(2, 'tag2() values(now, 2, 'test msg');"
        ]
        for sql in sql_list:
            tdLog.debug("execute specific character sql: %s" % sql)
            tdSql.error(sql)

    def test_td27388(self):
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
        self.check_half_quotes()
        self.check_esc()
        self.check_specific_character()
        tdLog.success(f"{__file__} successfully executed")
