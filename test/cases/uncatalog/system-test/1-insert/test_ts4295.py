from new_test_framework.utils import tdLog, tdSql

class TestTs4295:
    """Verify inserting varbinary type data of ts-4295
    """
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.conn = conn
        self.db_name = "db"
        self.stable_name = "st"

    def test_ts4295(self):
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
        tdSql.execute("create database if not exists %s" % self.db_name)
        tdSql.execute("use %s" % self.db_name)
        # create super table
        tdSql.execute("create table %s (ts timestamp, c1 varbinary(32)) tags (t1 int)" % self.stable_name)
        # create child table
        child_table_list = []
        for i in range(10):
            child_table_name = "ct_" + str(i+1)
            child_table_list.append(child_table_name)
            tdSql.execute("create table %s using st tags(%s);" % (child_table_name, str(i+1)))
            tdLog.info("create table %s successfully" % child_table_name)
        # insert data
        for i in range(100):
            sql = "insert into table_name values"
            for j in range(10000):
                sql += "(now+%ss, '0x7661726331')," % str(j+1)
            for child_table in child_table_list:
                tdSql.execute(sql.replace("table_name", child_table))
                tdLog.info("Insert data into %s successfully" % child_table)
            tdLog.info("Insert data round %s successfully" % str(i+1))
        tdSql.execute("flush database %s" % self.db_name)
        
        tdLog.success(f"{__file__} successfully executed")
