import os

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

        cmdStr1 = '%s/build/bin/replay_test'%(buildPath)
        tdLog.info(cmdStr1)
        result = os.system(cmdStr1)

        if result != 0:
            tdLog.exit("tmq_replay error!")

        tdLog.success(f"{__file__} successfully executed")
