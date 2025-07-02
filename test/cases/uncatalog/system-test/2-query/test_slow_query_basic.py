from new_test_framework.utils import tdLog, tdSql

import string
import platform
import os

class checkSlowQueryBasic:
    updatecfgDict = {'slowLogThresholdTest': ''}
    updatecfgDict["slowLogThresholdTest"]  = 0
    
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        if (platform.system().lower() == 'windows'):
            tool = tool + ".exe"
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    def taosBenchmark(self, param):
        binPath = self.getPath()
        cmd = f"{binPath} {param}"
        tdLog.info(cmd)
        os.system(cmd)
        
    def checkSlowQuery(self):
        self.taosBenchmark(" -d db -t 2 -v 2 -n 1000000 -y")
        sql = "select count(*) from db.meters"
        for i in range(10):            
            tdSql.query(sql)
            tdSql.checkData(0, 0, 2 * 1000000)

    def test_slow_query_basic(self):
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

        self.checkSlowQuery()

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
