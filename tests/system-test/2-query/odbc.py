import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *
from util.common import tdCom

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def check_ins_cols(self):
        tdSql.execute("create database if not exists db")
        tdSql.execute("create table db.ntb (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned, c10 float, c11 double, c12 varchar(100), c13 nchar(100))")
        tdSql.execute("create table db.stb (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned, c10 float, c11 double, c12 varchar(100), c13 nchar(100)) tags(t int)")
        tdSql.execute("insert into db.ctb using db.stb tags(1) (ts, c1) values (now, 1)")

        tdSql.execute("select count(*) from information_schema.ins_columns")

        tdSql.query("select * from information_schema.ins_columns where table_name = 'ntb'")
        tdSql.checkRows(14)
        tdSql.checkData(0, 2, "NORMAL_TABLE")


        tdSql.query("select * from information_schema.ins_columns where table_name = 'stb'")
        tdSql.checkRows(14)
        tdSql.checkData(0, 2, "SUPER_TABLE")


        tdSql.query("select db_name,table_type,col_name,col_type,col_length from information_schema.ins_columns where table_name = 'ctb'")
        tdSql.checkRows(14)
        tdSql.checkData(0, 0, "db")
        tdSql.checkData(1, 1, "CHILD_TABLE")
        tdSql.checkData(3, 2, "c3")
        tdSql.checkData(4, 3, "INT")
        tdSql.checkData(5, 4, 8)

        tdSql.query("desc information_schema.ins_columns")
        tdSql.checkRows(9)
        tdSql.checkData(0, 0, "table_name")
        tdSql.checkData(5, 0, "col_length")
        tdSql.checkData(1, 2, 64)

    def check_get_db_name(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/get_db_name_test'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("sml_test get_db_name_test != 0")

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare(replica = self.replicaVar)

        tdLog.printNoPrefix("==========start check_ins_cols run ...............")
        self.check_ins_cols()
        tdLog.printNoPrefix("==========end check_ins_cols run ...............")

        tdLog.printNoPrefix("==========start check_get_db_name run ...............")
        self.check_get_db_name()
        tdLog.printNoPrefix("==========end check_get_db_name run ...............")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
