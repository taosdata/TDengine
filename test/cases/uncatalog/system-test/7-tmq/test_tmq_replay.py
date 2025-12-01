import os
import platform
from new_test_framework.utils import tdLog, tdSql, tdCom

class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_replay(self):
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
        buildPath = tdCom.getBuildPath()
        exe_file = 'replay_test' if platform.system().lower() != 'windows' else 'replay_test.exe'
        cmdStr1 = os.path.join(buildPath, 'build', 'bin', exe_file)
        tdLog.info(cmdStr1)
        result = os.system(cmdStr1)

        if result != 0:
            tdLog.exit("tmq_replay error!")

        tdLog.success(f"{__file__} successfully executed")
