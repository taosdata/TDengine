from new_test_framework.utils import tdLog, tdSql

from wsgiref.headers import tspecials

class TestProjectiondesc:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.rowNum = 10
        cls.batchNum = 5
        cls.ts = 1537146000000

    def test_projectionDesc(self):
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

        intData = []
        floatData = []

        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags('beijing')")
        for n in range(self.batchNum):
            for i in range(self.rowNum):
                tdSql.execute(f"insert into {dbname}.stb_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                            % (self.ts, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
                intData.append(i + 1)
                floatData.append(i + 0.1)
            tdSql.execute(f"flush database {dbname}")

        tdSql.query(f"select * from {dbname}.stb_1 order by ts desc")
        tdSql.checkRows(1)
        #tdSql.checkData(0,0,1537146000000)
        tdSql.checkData(0,1,10)

        tdSql.query(f"select * from {dbname}.stb_1 order by 'aaa' desc")

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
