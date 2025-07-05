import random
import string
from util.log import *
from util.cases import *
from util.sql import *
from util.sqlset import *
from util import constant
from util.common import *


class TDTestCase:
    """Verify the insert with format exception for task TD-27388
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db'
        self.stbname = 'st'
        tdSql.execute("create database {};".format(self.dbname))
        tdSql.execute("use {};".format(self.dbname))
        tdSql.execute("create table st (ts timestamp, col1 int, col2 varchar(64)) tags (t1 int, t2 varchar(32));")

    def test_half_quotes(self):
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

    def test_esc(self):
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

    def test_specific_character(self):
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

    def run(self):
        self.test_half_quotes()
        self.test_esc()
        self.test_specific_character()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
