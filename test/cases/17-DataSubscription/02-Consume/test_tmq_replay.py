import os
import platform
from new_test_framework.utils import tdLog, tdSql, tdCom

class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_replay(self):
        """Advanced: Replay operations
        
        1. Run replay_test executable to test TMQ replay functionality
        2. Verify no errors occur during execution
        3. Confirm successful completion of the test
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_replay.py

        """
        tdSql.prepare()
        buildPath = tdCom.getBuildPath()
        exe_file = 'replay_test' if platform.system().lower() != 'windows' else 'replay_test.exe'
        cmdStr1 = os.path.join(buildPath, 'build', 'bin', exe_file)
        tdLog.info(cmdStr1)
        result = os.system(cmdStr1)

        if result != 0:
            tdLog.exit("tmq_replay error!")
