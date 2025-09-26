from new_test_framework.utils import tdLog, tdSql, etool,cluster
import os




class TestTLSDemo:
    updateCfgDict = {
       "enableTls"        : "1", 
       "tlsCliKeyPath"         : "/tmp/server.crt", 
       "tlsCliCertPath"         : "/tmp/server.crt", 
       "tlsSvrKeyPath"         : "/tmp/server.crt", 
       "tlsSvrCertPath"         : "/tmp/server.crt", 
       "tlsCaPath":             "/tmp/ca.crt",  
    }
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

        tdSql.prepare(dbname="test", drop=True, replica=cls.replicaVar)
        
    def initEnv(self):
        os.system("")

    def stop_and_restart(self):   
        dnodes = cluster.dnodes;
        dnodes[0].stoptaosd() 
    def test_tls_demo(self):
        """测试TLS demo

        展示基本TLS
        操作写法

        Since: v3.3.0.0

        Labels: demo

        Jira: None

        History:
            - 2025-09-20 dengyihao Created
        """
        # 查询
        tdSql.query("select * from test.meters")
        tdSql.checkRows(100)

        tdSql.query("select count(*) from test.meters", row_tag=True)
        tdSql.checkData(0, 0, 100)

        # 插入
        tdSql.execute("insert into test.d0 values (now, 1, 2, 1)")
        tdSql.query("select * from test.meters")
        tdSql.checkRows(101)

        tdLog.info(f"{__file__} successfully executed")
