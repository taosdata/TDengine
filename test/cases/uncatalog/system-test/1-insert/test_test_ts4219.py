from new_test_framework.utils import tdLog, tdSql

class TestTs4219:
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

    def prepare_data(self):
        tdSql.execute("create database db;")
        tdSql.execute("use db;")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float) tags(groupname binary(32));")

    def test_ts4219(self):
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
        tdSql.error("insert into ct1 using st tags('group name 1') values(now, 1, 1.1)(now+1s, 2, 2.2)  ct1 using st tags('group 1) values(now+2s, 3, 3.3); ")
        tdLog.success(f"{__file__} successfully executed")
