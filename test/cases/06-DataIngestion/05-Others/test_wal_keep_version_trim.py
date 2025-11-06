import os
import time
import json
from new_test_framework.utils import tdLog, tdSql, sc, tdDnodes,clusterComCheck


class TestWalKeepVersionTrim:
    """Test WAL keep version and trim functionality
    
    This test verifies:
    1. prepare data
    2. set keep version 0
    3. check wal keep version
    4. trim database wal
    5. check wal log dropped after trim

    Catalog:
        - Database:WAL

    Since: v3.3.6.31

    Labels: common,ci

    Jira: TS-7567

    History:
        - 2025-11-06 Simon Guan Migrated from tsim/wal/keep.sim
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")


    def test_wal_keep_version_and_trim(self):
        clusterComCheck.checkDnodes(3)

        os.system("taosBenchmark -t 100 -n 100000 -a 3 -y")

        tdSql.execute("alter database test WAL_RETENTION_PERIOD 0")

        # set keep version 0
        tdSql.execute("alter vnode 2 set keep version 0")
        tdSql.execute("flush database test")

        # wait for flush finished
        time.sleep(10)

        # check wal keep version 
        ver = self.get_wal_file_first_version(1, 2)
        tdLog.info(f"dnode1 vg2 firstVer: {ver}")
        assert ver == 0
        ver = self.get_wal_file_first_version(2, 2)
        tdLog.info(f"dnode2 vg2 firstVer: {ver}")
        assert ver == 0
        ver = self.get_wal_file_first_version(3, 2)
        tdLog.info(f"dnode3 vg2 firstVer: {ver}")
        assert ver == 0

        ver = self.get_wal_file_first_version(1, 3)
        tdLog.info(f"dnode1 vg3 firstVer: {ver}")
        assert ver > 0
        ver = self.get_wal_file_first_version(2, 3)
        tdLog.info(f"dnode2 vg3 firstVer: {ver}")
        assert ver > 0
        ver = self.get_wal_file_first_version(3, 3)
        tdLog.info(f"dnode3 vg3 firstVer: {ver}")
        assert ver > 0

        # trim database wal
        tdSql.execute("trim database test wal")
        time.sleep(10)

        # check wal log dropped after trim
        ver = self.get_wal_file_first_version(1, 2)
        tdLog.info(f"dnode1 vg2 firstVer: {ver}")
        assert ver > 0
        ver = self.get_wal_file_first_version(2, 2)
        tdLog.info(f"dnode2 vg2 firstVer: {ver}")
        assert ver > 0
        ver = self.get_wal_file_first_version(3, 2)
        tdLog.info(f"dnode3 vg2 firstVer: {ver}")
        assert ver > 0

        ver = self.get_wal_file_first_version(1, 3)
        tdLog.info(f"dnode1 vg3 firstVer: {ver}")
        assert ver > 0
        ver = self.get_wal_file_first_version(2, 3)
        tdLog.info(f"dnode2 vg3 firstVer: {ver}")
        assert ver > 0
        ver = self.get_wal_file_first_version(3, 3)
        tdLog.info(f"dnode3 vg3 firstVer: {ver}")
        assert ver > 0

    def get_wal_file_first_version(self, dnode_id, vgId):
        """Get firstVer from WAL meta-ver file
        
        Args:
            dnode_id: dnode ID
            vgId: vgroup ID
            
        Returns:
            int: firstVer value from meta-ver file
        """
        rootDir = tdDnodes.getDnodeDir(dnode_id)
        walPath = os.path.join(rootDir, "data", "vnode", f"vnode{vgId}", "wal")
        
        tdLog.info(f"walPath: {walPath}")
        
        # Find meta-ver file (exclude .tmp files, match pattern meta-ver or meta-ver*)
        meta_ver_file = None
        if os.path.exists(walPath):
            for filename in os.listdir(walPath):
                # Match meta-ver or meta-ver<number>, but not meta-ver.tmp
                if filename.startswith("meta-ver") and not filename.endswith(".tmp"):
                    meta_ver_file = os.path.join(walPath, filename)
                    tdLog.info(f"Found meta-ver file: {filename}")
                    break
        
        if not meta_ver_file:
            tdLog.exit(f"meta-ver file not found in {walPath}")
        

        with open(meta_ver_file, 'r') as f:
            meta_data = json.load(f)
            first_ver = int(meta_data['meta']['firstVer'])
            tdLog.info(f"firstVer from {os.path.basename(meta_ver_file)}: {first_ver}")
            return first_ver
