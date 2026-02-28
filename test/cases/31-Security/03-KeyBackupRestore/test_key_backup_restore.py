from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes
import os
import subprocess
import shutil
import time


class TestBasic:
    updatecfgDict = {'dDebugFlag': 131}
    
    # Pre-generate encryption keys
    encryptConfig = {
        "svrKey": "backupsvr12",
        "dbKey": "backupdb456",
        "dataKey": "backupdata1",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0

    def test_key_backup_restore(self):
        """ Test key backup and restore across machines

        1. Backup keys with SVR_KEY verification
        2. Test backup with wrong SVR_KEY (should fail)
        3. Test backup without SVR_KEY (should fail)
        4. Restore keys to simulate new machine
        5. Test restore with wrong SVR_KEY (should fail)

        Since: v3.4.0.0

        Labels: common,ci

        Jira: TS-7230

        History:
            - 2025-01-26 Created based on TS-7230

        """
        # Get config and data directories
        cfg_path = tdDnodes.dnodes[0].cfgDir
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            data_dir = data_dir[0]
        
        key_dir = os.path.join(data_dir, "dnode", "config")
        master_file = os.path.join(key_dir, "master.bin")
        
        tdLog.info(f"Config directory: {cfg_path}")
        tdLog.info(f"Data directory: {data_dir}")
        
        # Verify initial keys exist
        assert os.path.exists(master_file), f"master.bin should exist at {master_file}"

        # Test 1: Backup keys with correct SVR_KEY
        tdLog.info("=" * 80)
        tdLog.info("Test 1: Backing up keys with correct SVR_KEY")
        tdLog.info("=" * 80)
        
        cmd_backup = ['taosk', '-c', cfg_path, '--backup', '--svr-key', 'backupsvr12']
        result_backup = subprocess.run(cmd_backup, capture_output=True, text=True, timeout=30, check=True)
        tdLog.info(f"Backup output: {result_backup.stdout}")
        
        # Check if backup file was created
        backup_files = [f for f in os.listdir(key_dir) if f.startswith('master.bin.backup')]
        assert len(backup_files) > 0, "Backup file should be created"
        backup_file = os.path.join(key_dir, backup_files[0])
        tdLog.info(f"Backup file created: {backup_file}")

        # Test 2: Backup with wrong SVR_KEY (should fail)
        tdLog.info("=" * 80)
        tdLog.info("Test 2: Testing backup with wrong SVR_KEY")
        tdLog.info("=" * 80)
        
        cmd_wrong = ['taosk', '-c', cfg_path, '--backup', '--svr-key', 'wrongkey123']
        result_wrong = subprocess.run(cmd_wrong, capture_output=True, text=True, timeout=30)
        assert result_wrong.returncode != 0, "Backup with wrong SVR_KEY should fail"
        tdLog.info(f"Backup failed as expected with wrong key: {result_wrong.stderr}")

        # Test 3: Backup without SVR_KEY (should fail)
        tdLog.info("=" * 80)
        tdLog.info("Test 3: Testing backup without SVR_KEY")
        tdLog.info("=" * 80)
        
        cmd_no_key = ['taosk', '-c', cfg_path, '--backup']
        result_no_key = subprocess.run(cmd_no_key, capture_output=True, text=True, timeout=30)
        assert result_no_key.returncode != 0, "Backup without SVR_KEY should fail"
        tdLog.info(f"Backup failed as expected without key: {result_no_key.stderr}")

        # Test 4: Restore keys (simulate to new machine)
        tdLog.info("=" * 80)
        tdLog.info("Test 4: Restoring keys from backup")
        tdLog.info("=" * 80)
        
        # Remove master.bin to simulate new machine
        if os.path.exists(master_file):
            master_backup = master_file + ".original"
            shutil.copy2(master_file, master_backup)
            os.remove(master_file)
        
        cmd_restore = ['taosk', '-c', cfg_path, '--restore', 
                       '--machine-code', backup_file, 
                       '--svr-key', 'backupsvr12']
        result_restore = subprocess.run(cmd_restore, capture_output=True, text=True, timeout=30, check=True)
        tdLog.info(f"Restore output: {result_restore.stdout}")
        
        # Verify master.bin was restored
        assert os.path.exists(master_file), f"master.bin should be restored at {master_file}"
        tdLog.info("Keys restored successfully")
        
        # Restore original file
        if os.path.exists(master_backup):
            shutil.copy2(master_backup, master_file)
            os.remove(master_backup)

        # Test 5: Restore with wrong SVR_KEY (should fail)
        tdLog.info("=" * 80)
        tdLog.info("Test 5: Testing restore with wrong SVR_KEY")
        tdLog.info("=" * 80)
        
        cmd_restore_wrong = ['taosk', '-c', cfg_path, '--restore', 
                             '--machine-code', backup_file, 
                             '--svr-key', 'wrongkey123']
        result_restore_wrong = subprocess.run(cmd_restore_wrong, capture_output=True, text=True, timeout=30)
        assert result_restore_wrong.returncode != 0, "Restore with wrong SVR_KEY should fail"
        tdLog.info(f"Restore failed as expected with wrong key: {result_restore_wrong.stderr}")

        tdLog.success(f"{__file__} successfully executed")
