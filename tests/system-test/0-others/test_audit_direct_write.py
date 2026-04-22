from util.log import *
from util.cases import *
from util.sql import *

import time

from ..common.basic import BasicFun


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to execute {__file__}")
        self.replicaVar = int(replicaVar)
        self.Fun = BasicFun()

    def run(self):
        self.Fun.config_cluster(1)

        for dnode in self.Fun.TDDnodes.dnodes:
            dnode.addExtraCfg("audit", "1")
            dnode.addExtraCfg("auditInterval", "500")
            dnode.addExtraCfg("auditLevel", "5")
            dnode.addExtraCfg("enableAuditDelete", "1")
            dnode.addExtraCfg("enableAuditSelect", "1")
            dnode.addExtraCfg("enableAuditInsert", "1")
            dnode.addExtraCfg("auditDirectWrite", "1")
            dnode.addExtraCfg("auditUseToken", "0")
            dnode.addExtraCfg("monitorFqdn", "127.0.0.1")
            dnode.addExtraCfg("monitorPort", "1")

        self.Fun.deploy_start_cluster()
        self.Fun.connect()

        tdSql.execute("drop database if exists audit_case")
        tdSql.execute("create database audit_case")
        tdSql.execute("use audit_case")
        tdSql.execute("create table if not exists t(ts timestamp, c1 int)")
        tdSql.execute("insert into t values(now, 1)")
        tdSql.query("select * from t")
        tdSql.checkRows(1)

        # auditRecord is sync for these statements, but wait briefly for robustness.
        count = 0
        for _ in range(20):
            tdSql.query("select count(*) from audit_case.audit_events")
            count = tdSql.queryResult[0][0]
            if count > 0:
                break
            time.sleep(1)

        tdSql.checkGreater(count, 0)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
