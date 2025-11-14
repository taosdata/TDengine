from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDnodeAlterDebugFlag:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_dnode_alter_debugflag(self):
        """Dnode alter

        1. Start only one dnode
        2. Modify the monitor and debugflag parameters of the online dnode
        3. Modify parameters of an offline dnode (error expected)

        Catalog:
            - ManageNodes:Dnode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/alter/dnode.sim

        """

        tdLog.info(f"======== step1")
        tdSql.execute(f"alter dnode 1 'resetlog'")
        tdSql.execute(f"alter all dnodes 'monitor' '1'")
        tdSql.execute(f"alter all dnodes 'monitor' '0'")
        tdSql.execute(f"alter all dnodes 'monitor 1'")
        tdSql.execute(f"alter all dnodes 'monitor 0'")

        tdLog.info(f"======== step2")
        tdSql.error(f"alter dnode 1 'resetquerycache'")
        tdSql.execute(f"alter dnode 1 'debugFlag 135'")
        tdSql.execute(f"alter dnode 1 'dDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'vDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'mDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'wDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'sDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'tsdbDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'tqDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'fsDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'udfDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'smaDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'idxDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'tdbDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'tmrDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'uDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'smaDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'rpcDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'qDebugFlag 131'")
        tdSql.execute(f"alter dnode 1 'metaDebugFlag 131'")

        tdSql.execute(f"alter dnode 1 'debugFlag' '135'")
        tdSql.execute(f"alter dnode 1 'dDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'vDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'mDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'wDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'sDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'tsdbDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'tqDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'fsDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'udfDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'smaDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'idxDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'tdbDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'tmrDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'uDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'smaDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'rpcDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'qDebugFlag' '131'")
        tdSql.execute(f"alter dnode 1 'metaDebugFlag' '131'")

        tdSql.error(f"alter dnode 2 'wDebugFlag 135'")
        tdSql.error(f"alter dnode 2 'tmrDebugFlag 135'")
        tdSql.error(f"alter dnode 1 'monDebugFlag 131'")
        tdSql.error(f"alter dnode 1 'cqDebugFlag 131'")
        tdSql.error(f"alter dnode 1 'httpDebugFlag 131'")
        tdSql.error(f"alter dnode 1 'mqttDebugFlag 131'")
        tdSql.error(f"alter dnode 1 'qDebugFlaga 131'")
        tdSql.error(f"alter all dnodes 'qDebugFlaga 131'")

        tdSql.error(f"alter dnode 2 'wDebugFlag' '135'")
        tdSql.error(f"alter dnode 2 'tmrDebugFlag' '135'")
        tdSql.error(f"alter dnode 1 'monDebugFlag' '131'")
        tdSql.error(f"alter dnode 1 'cqDebugFlag' '131'")
        tdSql.error(f"alter dnode 1 'httpDebugFlag' '131'")
        tdSql.error(f"alter dnode 1 'mqttDebugFlag' '131'")
        tdSql.error(f"alter dnode 1 'qDebugFlaga' '131'")
        tdSql.error(f"alter all dnodes 'qDebugFlaga' '131'")

        hostname1 = "localhost"
        hostname2 = "localhost"
        tdLog.info(f"======== step3")
        tdSql.error(f"alter {hostname1} debugFlag 135")
        tdSql.error(f"alter {hostname1} monDebugFlag 135")
        tdSql.error(f"alter {hostname1} vDebugFlag 135")
        tdSql.error(f"alter {hostname1} mDebugFlag 135")
        tdSql.error(f"alter dnode {hostname2} debugFlag 135")
        tdSql.error(f"alter dnode {hostname2} monDebugFlag 135")
        tdSql.error(f"alter dnode {hostname2} vDebugFlag 135")
        tdSql.error(f"alter dnode {hostname2} mDebugFlag 135")
        tdSql.error(f"alter dnode {hostname1} debugFlag 135")
        tdSql.error(f"alter dnode {hostname1} monDebugFlag 135")
        tdSql.error(f"alter dnode {hostname1} vDebugFlag 135")
        tdSql.error(f"alter dnode {hostname1} tmrDebugFlag 131")

        tdLog.info(f"======== step4")
        tdSql.error(f"sql alter dnode 1 balance 0")
        tdSql.error(f"sql alter dnode 1 balance vnode:1-dnode:1")
        tdSql.error(f'sql alter dnode 1 balance "vnode:1"')
        tdSql.error(f'sql alter dnode 1 balance "vnode:1-dnode:1"')
        tdSql.error(f'sql alter dnode 1 balance "dnode:1-vnode:1"')
        tdSql.error(f'sql alter dnode 1 balance "dnode:1-"')
        tdSql.error(f'sql alter dnode 1 balance "vnode:2-dnod"')
        tdSql.error(f'alter dnode 1 balance "vnode:2-dnode:1"')

        tdLog.info(f"======= over")
