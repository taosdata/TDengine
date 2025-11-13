import os
import time
import json
import subprocess

from new_test_framework.utils import tdLog, tdSql, sc, tdDnodes,clusterComCheck


class TestWalKeepVersionTrim:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")


    def test_wal_keep_version_and_trim(self):
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
            - 2025-11-06 beryl bao created
        """
        clusterComCheck.checkDnodes(3)

        subprocess.run("taosBenchmark -t 100 -n 100000 -a 3 -y", shell=True, check=True)

        tdSql.execute("alter database test WAL_RETENTION_PERIOD 0")

        # set keep version 0
        tdSql.execute("alter vgroup 2 set keep 0")
        tdSql.execute("flush database test")

        tdSql.query("show test.vgroups")
        tdSql.checkData(0, 19, 0)
        tdSql.checkData(1, 19, -1)

        max_retry = 240
        # check wal vgId 3 firstVer is greater than 0 means flush finished
        for dnode_id in [1,2,3]:
            check_ver = False
            for _ in range(max_retry):
                ver = self.get_wal_file_first_version(dnode_id, 3)
                tdLog.info(f"dnode{dnode_id} vg3 firstVer: {ver}")
                if ver > 0:
                    check_ver = True
                    break
                time.sleep(1)

            assert check_ver, f"dnode{dnode_id} vg3 firstVer is not greater than 0 after {max_retry} seconds"
        
        # check wal vgId 2 firstVer is 0
        for dnode_id in [1,2,3]:
            check_ver = False
            for _ in range(max_retry):
                ver = self.get_wal_file_first_version(dnode_id, 2)
                tdLog.info(f"dnode{dnode_id} vg2 firstVer: {ver}")
                if ver == 0:
                    check_ver = True
                    break
                time.sleep(1)
            assert check_ver, f"dnode{dnode_id} vg2 firstVer is not greater than 0 after {max_retry} seconds"
        

        # trim database wal
        tdSql.execute("trim database test wal")

        # check wal vgId 2 firstVer is greater than 0 after trim
        for dnode_id in [1,2,3]:
            check_ver = False
            for _ in range(max_retry):
                ver = self.get_wal_file_first_version(dnode_id, 2)
                tdLog.info(f"dnode{dnode_id} vg2 firstVer: {ver}")
                if ver > 0:
                    check_ver = True
                    break
                time.sleep(1)
            assert check_ver, f"dnode{dnode_id} vg2 firstVer is not greater than 0 after {max_retry} seconds after trim"


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
