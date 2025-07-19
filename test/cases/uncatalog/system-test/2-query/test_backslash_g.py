from new_test_framework.utils import tdLog, tdSql
import taos
import os
class TestBackslashG:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

    def checksql(self, sql):
        result = os.popen(f"taos -s \"{sql}\" ")
        res = result.read()
        print(res)
        if ("Query OK" in res):
            tdLog.info(f"checkEqual success")
        else :
            tdLog.exit(f"checkEqual error")

    def td_28164(self):
        tdSql.execute("drop database if exists td_28164;")
        tdSql.execute("create database td_28164;")
        tdSql.execute("create table td_28164.test (ts timestamp, name varchar(10));")
        tdSql.execute("insert into td_28164.test values (now(), 'ac\\\\G') (now() + 1s, 'ac\\\\G') (now()+2s, 'ac\\G') (now()+3s, 'acG') (now()+4s, 'acK') ;")

        tdSql.query(f"select * from td_28164.test;")
        tdSql.checkRows(5)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\\\\\G';")
        tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\\\G';")
        tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\G';")
        tdSql.checkRows(2)

        # tdSql.query(f"select * from td_28164.test where name like 'ac\\\g';")
        # tdSql.checkRows(0)
        #
        # tdSql.query(f"select * from td_28164.test where name like 'ac\\g';")
        # tdSql.checkRows(0)

        self.checksql(f'select * from td_28164.test where name like \'ac\\G\'\G;')
        # tdSql.checkRows(2)

        self.checksql(f"select * from td_28164.test where name like \'ac\\G\'   \G;")
        # tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac/\\G';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from td_28164.test where name like 'ac/G';")
        tdSql.checkRows(0)

    def test_backslash_g(self):
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

        # tdSql.prepare()
        self.td_28164()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
