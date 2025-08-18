import os
from new_test_framework.utils import tdLog, tdSql, tdCom
import platform

class TestOdbc:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

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
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "table_name")
        tdSql.checkData(5, 0, "col_length")
        tdSql.checkData(1, 2, 64)

    def check_get_db_name(self):
        buildPath = tdCom.getBuildPath()
        exe_file = "get_db_name_test" if platform.system() != "Windows" else "get_db_name_test.exe"
        cmdStr = os.path.join(buildPath, "build", "bin", exe_file)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("sml_test get_db_name_test != 0")

    def test_odbc(self):
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
  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare(replica = self.replicaVar)

        tdLog.printNoPrefix("==========start check_ins_cols run ...............")
        self.check_ins_cols()
        tdLog.printNoPrefix("==========end check_ins_cols run ...............")

        tdLog.printNoPrefix("==========start check_get_db_name run ...............")
        self.check_get_db_name()
        tdLog.printNoPrefix("==========end check_get_db_name run ...............")

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
