from util.log import tdLog
from util.cases import tdCases


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to execute {__file__}")

    def run(self):
        # Recovery loop skeleton for repeated crash/restart validation.
        # The loop harness drives repeated executions via loop.sh.
        for i in range(3):
            tdLog.info(f"assigned-stepdown-recovery-loop iteration={i} replicaVar={self.replicaVar}")

    def stop(self):
        tdLog.success(f"{__file__} executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
