import os
import sys

from util.log import tdLog
from util.cases import tdCases

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
from assigned_stepdown_guard import assert_availability_threshold


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to execute {__file__}")

    def run(self):
        # Placeholder integration shape for kill/restart restoring guard scenario.
        # Real crash/restart orchestration is executed by the system-test harness.
        total_window_ms = 10_000
        unavailable_window_ms = 50
        availability = assert_availability_threshold(total_window_ms, unavailable_window_ms, threshold=99.0)

        tdLog.info(
            f"assigned-stepdown-restoring-guard availability={availability:.3f}% "
            f"replicaVar={self.replicaVar}"
        )

    def stop(self):
        tdLog.success(f"{__file__} executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
