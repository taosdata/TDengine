from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes
import os
import subprocess
import time


class TestBasic:
    updatecfgDict = {'dDebugFlag': 131}
    
    encryptConfig = {
        "svrKey": "oldsvr123",
        "dbKey": "olddb45678",
        "dataKey": "olddata123",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0

    def test_key_update(self):
        """ Test key update features

        Tests from TS-7230 Section 6.2:
        1. Update SVR_KEY through taosk
        2. Update DB_KEY through taosk (regenerates derived keys)
        3. Update both keys simultaneously
        4. Update keys through SQL commands

        Since: v3.4.0.0

        Labels: common,ci

        Jira: TS-7230

        History:
            - 2025-01-26 Created based on TS-7230

        """
        cfg_path = tdDnodes.dnodes[0].cfgDir
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            data_dir = data_dir[0]
        
        key_dir = os.path.join(data_dir, "dnode", "config")
        master_file = os.path.join(key_dir, "master.bin")
        derived_file = os.path.join(key_dir, "derived.bin")
        
        tdLog.info(f"Config: {cfg_path}, Keys: {key_dir}")

        # Verify pre-generated keys
        assert os.path.exists(master_file), f"master.bin missing at {master_file}"
        assert os.path.exists(derived_file), f"derived.bin missing at {derived_file}"
        tdLog.info("Initial keys verified")

        # Test 1: Update SVR_KEY
        tdLog.info("=" * 80)
        tdLog.info("Test 1: Update SVR_KEY")
        tdLog.info("=" * 80)
        
        initial_mtime = os.path.getmtime(master_file)
        time.sleep(0.1)
        
        cmd = ['taosk', '-c', cfg_path, '--update-svrkey', 'newsvr123']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
        tdLog.info(f"Output: {result.stdout}")
        
        updated_mtime = os.path.getmtime(master_file)
        assert updated_mtime > initial_mtime, f"master.bin not updated: {initial_mtime} -> {updated_mtime}"
        tdLog.info("SVR_KEY updated")

        # Test 2: Update DB_KEY
        tdLog.info("=" * 80)
        tdLog.info("Test 2: Update DB_KEY")
        tdLog.info("=" * 80)
        
        initial_derived_mtime = os.path.getmtime(derived_file)
        time.sleep(0.1)
        
        cmd = ['taosk', '-c', cfg_path, '--update-dbkey', 'newdb45678']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
        tdLog.info(f"Output: {result.stdout}")
        
        updated_derived_mtime = os.path.getmtime(derived_file)
        assert updated_derived_mtime > initial_derived_mtime, "derived.bin not regenerated"
        tdLog.info("DB_KEY updated, derived keys regenerated")

        # Test 3: Update both keys
        tdLog.info("=" * 80)
        tdLog.info("Test 3: Update both SVR_KEY and DB_KEY")
        tdLog.info("=" * 80)
        
        initial_master = os.path.getmtime(master_file)
        initial_derived = os.path.getmtime(derived_file)
        time.sleep(0.1)
        
        cmd = ['taosk', '-c', cfg_path, '--update-svrkey', 'bothsvr123', '--update-dbkey', 'bothdb45678']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
        tdLog.info(f"Output: {result.stdout}")
        
        assert os.path.getmtime(master_file) > initial_master, "master.bin not updated"
        assert os.path.getmtime(derived_file) > initial_derived, "derived.bin not updated"
        tdLog.info("Both keys updated")

        # Test 4: SQL update
        tdLog.info("=" * 80)
        tdLog.info("Test 4: Update keys via SQL")
        tdLog.info("=" * 80)
        
        tdSql.execute("ALTER SYSTEM SET svr_key 'sqlsvr123'")
        tdLog.info("SVR_KEY updated via SQL")
        
        tdSql.execute("ALTER SYSTEM SET db_key 'sqldb45678'")
        tdLog.info("DB_KEY updated via SQL")
        
        tdSql.query("SELECT * FROM information_schema.ins_encrypt_status")
        if tdSql.queryRows > 0:
            tdLog.info(f"Encryption status: {tdSql.queryRows} rows")

        tdLog.success(f"{__file__} successfully executed")
