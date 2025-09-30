from new_test_framework.utils import tdLog, tdSql

'''
Test case for TS-5150
'''
class TestStddevTest:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.ts = 1537146000000

    def initdabase(self):
        tdSql.execute('create database if not exists db_test vgroups 2  buffer 10')
        tdSql.execute('use db_test')
        tdSql.execute('create stable stb(ts timestamp, delay int) tags(groupid int)')
        tdSql.execute('create table t1 using stb tags(1)')
        tdSql.execute('create table t2 using stb tags(2)')
        tdSql.execute('create table t3 using stb tags(3)')
        tdSql.execute('create table t4 using stb tags(4)')
        tdSql.execute('create table t5 using stb tags(5)')
        tdSql.execute('create table t6 using stb tags(6)')
    def insert_data(self):
        for i in range(5000):
            tdSql.execute(f"insert into t1 values({self.ts + i * 1000}, {i%5})")
            tdSql.execute(f"insert into t2 values({self.ts + i * 1000}, {i%5})")
            tdSql.execute(f"insert into t3 values({self.ts + i * 1000}, {i%5})")

    def verify_stddev(self):
        for i in range(20):
            tdSql.query(f'SELECT MAX(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS maxDelay,\
                        MIN(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS minDelay,\
                        AVG(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS avgDelay,\
                        STDDEV(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS jitter,\
                        COUNT(CASE WHEN delay = 0 THEN 1 ELSE NULL END) AS timeoutCount,\
                        COUNT(*) AS totalCount from stb where ts between {1537146000000 + i * 1000} and {1537146000000 + (i+10) * 1000}')
            res = tdSql.queryResult[0][3]
            assert res > 0.8
    def test_stddev_test(self):
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

        self.initdabase()
        self.insert_data()
        self.verify_stddev()
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
