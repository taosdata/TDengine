from new_test_framework.utils import tdLog, tdSql

class TestCountalwaysreturnvalue:
    updatecfgDict = {"countAlwaysReturnValue":0}

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

    def prepare_data(self, dbname="db"):
        tdSql.execute(
            f"create database if not exists {dbname} keep 3650 duration 100")
        tdSql.execute(f"use {dbname} ")
        tdSql.execute(
            f"create table {dbname}.tb (ts timestamp, c0 int)"
        )
        tdSql.execute(
            f"create table {dbname}.stb (ts timestamp, c0 int) tags (t0 int)"
        )
        tdSql.execute(
            f"create table {dbname}.ctb1 using {dbname}.stb tags (1)"
        )
        tdSql.execute(
            f"create table {dbname}.ctb2 using {dbname}.stb tags (2)"
        )

        tdSql.execute(
            f"create table {dbname}.tb_empty (ts timestamp, c0 int)"
        )
        tdSql.execute(
            f"create table {dbname}.stb_empty (ts timestamp, c0 int) tags (t0 int)"
        )
        tdSql.execute(
            f"create table {dbname}.ctb1_empty using {dbname}.stb tags (1)"
        )
        tdSql.execute(
            f"create table {dbname}.ctb2_empty using {dbname}.stb tags (2)"
        )

        tdSql.execute(
            f"insert into {dbname}.tb values (now(), NULL)")

        tdSql.execute(
            f"insert into {dbname}.ctb1 values (now(), NULL)")

        tdSql.execute(
            f"insert into {dbname}.ctb2 values (now() + 1s, NULL)")

    def check_results(self, dbname="db"):

        # count
        tdSql.query(f"select count(c0) from {dbname}.tb")
        tdSql.checkRows(0)

        tdSql.query(f"select count(NULL) from {dbname}.tb")
        tdSql.checkRows(0)

        tdSql.query(f"select c0,count(c0) from {dbname}.tb group by c0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select count(c0) from {dbname}.stb")
        tdSql.checkRows(0)

        tdSql.query(f"select count(NULL) from {dbname}.stb")
        tdSql.checkRows(0)

        tdSql.query(f"select c0,count(c0) from {dbname}.stb group by c0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select count(NULL)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        # hyperloglog
        tdSql.query(f"select hyperloglog(c0) from {dbname}.tb")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(NULL) from {dbname}.tb")
        tdSql.checkRows(0)

        tdSql.query(f"select c0,hyperloglog(c0) from {dbname}.tb group by c0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select hyperloglog(c0) from {dbname}.stb")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(NULL) from {dbname}.stb")
        tdSql.checkRows(0)

        tdSql.query(f"select c0,hyperloglog(c0) from {dbname}.stb group by c0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select hyperloglog(NULL)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        # test empty table/input
        tdSql.query(f"select count(*) from {dbname}.tb where ts > now + 1h")
        tdSql.checkRows(0)

        tdSql.query(f"select count(ts) from {dbname}.stb where ts > now + 1h")
        tdSql.checkRows(0)

        tdSql.query(f"select count(c0) from {dbname}.ctb1 where ts > now + 1h")
        tdSql.checkRows(0)

        tdSql.query(f"select count(1) from {dbname}.ctb2 where ts > now + 1h")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from {dbname}.tb_empty")
        tdSql.checkRows(0)

        tdSql.query(f"select count(ts) from {dbname}.stb_empty")
        tdSql.checkRows(0)

        tdSql.query(f"select count(c0) from {dbname}.ctb1_empty")
        tdSql.checkRows(0)

        tdSql.query(f"select count(1) from {dbname}.ctb2_empty")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(c0) from {dbname}.tb where ts > now + 1h")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(ts) from {dbname}.stb where ts > now + 1h")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(1) from {dbname}.ctb1 where ts > now + 1h")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(1) from {dbname}.ctb2 where ts > now + 1h")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(c0) from {dbname}.tb_empty")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(ts) from {dbname}.stb_empty")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(1) from {dbname}.ctb1_empty")
        tdSql.checkRows(0)

        tdSql.query(f"select hyperloglog(1) from {dbname}.ctb2_empty")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*), hyperloglog(c0), sum(1), max(c0) from {dbname}.tb where ts > now + 1h")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*), hyperloglog(c0), sum(1), max(c0) from {dbname}.tb_empty")
        tdSql.checkRows(0)

    def test_countAlwaysReturnValue(self):
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

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:prepare data ==============")

        self.prepare_data()
        tdSql.execute('alter local "countAlwaysReturnValue" "0"')

        tdLog.printNoPrefix("==========step2:test results ==============")

        self.check_results()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
