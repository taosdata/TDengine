from wsgiref.headers import tspecials
from new_test_framework.utils import tdLog, tdSql

class TestPartitionExpr:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.rowNum = 10
        cls.batchNum = 5
        cls.ts = 1537146000000

    def test_partition_expr(self):
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

        dbname = "db"
        tdSql.prepare()

        tdSql.execute(f'''create table sta(ts timestamp, f int, col2 bigint) tags(tg1 int, tg2 binary(20))''')
        tdSql.execute(f"create table sta1 using sta tags(1, 'a')")
        tdSql.execute(f"insert into sta1 values(1537146000001, 11, 110)")
        tdSql.execute(f"insert into sta1 values(1537146000002, 12, 120)")
        tdSql.execute(f"insert into sta1 values(1537146000003, 13, 130)")

        tdSql.query("select _wstart, f+100, count(*) from db.sta partition by f+100 session(ts, 1a) order by _wstart");
        tdSql.checkData(0, 1, 111.0)
        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
