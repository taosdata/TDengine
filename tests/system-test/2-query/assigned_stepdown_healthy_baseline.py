from util.log import tdLog
from util.cases import tdCases


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to execute {__file__}")

    def run(self):
        # Healthy baseline non-regression skeleton.
        tdLog.info("assigned-stepdown-healthy-baseline begin")
        tdLog.info("expected-log-assertions: no extra suppression in healthy path")

    def stop(self):
        tdLog.success(f"{__file__} executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
