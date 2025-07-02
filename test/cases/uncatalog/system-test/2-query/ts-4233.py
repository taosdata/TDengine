import os
from new_test_framework.utils import tdLog, tdSql

class TestTs4233:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def checksql(self, sql):
        result = os.popen(f"taos -s \"{sql}\" ")
        res = result.read()
        print(res)
        if ("Query OK" in res):
            tdLog.info(f"checkEqual success")
        else :
            tdLog.exit(f"checkEqual error")

    def check(self):
        sql = "select 'a;b' as x"
        tdSql.query(f"%s" %sql)
        tdSql.checkRows(1)

        self.checksql('select \\\"a;b\\\" as x\G')
        self.checksql('select \\\"a;b\\\" as x >> temp.txt')

    def test_ts_4233(self):
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
        self.check()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
