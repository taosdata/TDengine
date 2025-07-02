import taos
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSmlTd19291:
    updatecfgDict = {'clientCfg': {'smlChildTableName': 'dataModelName', 'smlAutoChildTableNameDelimiter': '-', 'fqdn': 'localhost', 'smlDot2Underline': 1}, 'fqdn': 'localhost'}
    print("===================: ", updatecfgDict)
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def check(self):
        conn = taos.connect()
        dbname = "td19291"
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s precision 'us'" % dbname)
        conn.select_db(dbname)

        lines = [
            'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t1=3i64,t2=4f64,dataModelName=ttt c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t1=3i64,t2=4f.64 c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t1=ioiooo3i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i64uuuuuuuuuuuuuuuuuuuuuuuuuu4 c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t2=q,t1=iooo3i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i64uuuuuuuuuuuuuuuuuuuuuuuuuu4 c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t2=a,t1=ooo3i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i64uuuuuuuuuuuuuuuuuuuuuuuuuu4 c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
        ]
        conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED)
        print("inserted")

        tdSql.query("select table_name from information_schema.ins_tables where type = 'CHILD_TABLE' order by table_name")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "3i64-4f64-\"t3\"")
        tdSql.checkData(1, 0, "3i64-4f64-ttt")
        tdSql.checkData(2, 0, "3i64-4f_64")
        tdSql.checkData(3, 0, "a-ooo3i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i64uuuuuuuuuuuuuuuuuuuuuuuuuu4")
        tdSql.checkData(4, 0, "t_418c134a0f00c7f536886e132d5fbfff")
        tdSql.checkData(5, 0, "t_cb0dbf4ee9c9052815c17fc6483b0139")
        # tdSql.query(f"select * from td24559.stb order by _ts")
        # tdSql.checkRows(4)
        # tdSql.checkData(0, 2, "POINT (4.343000 89.342000)")
        # tdSql.checkData(3, 2, "GEOMETRYCOLLECTION (MULTIPOINT ((0.000000 0.000000), (1.000000 1.000000)), POINT (3.000000 4.000000), LINESTRING (2.000000 3.000000, 3.000000 4.000000))")
        return

    def test_sml_TD19291(self):
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
