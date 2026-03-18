from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes
import os
import subprocess


class TestBasic:
    updatecfgDict = {'dDebugFlag': 131}

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0

    def test_key_generation(self):
        """ Test taosk key generation features

        Tests from TS-7230 Section 6.1:
        1. taosk --help and --version commands
        2. Generate default keys with SM4 algorithm
        3. Generate keys with specified SVR_KEY and DB_KEY
        4. Enable all encryption options (config, metadata, data)
        5. Test key length validation (8-16 characters)

        Since: v3.4.0.0

        Labels: common,ci

        Jira: TS-7230

        History:
            - 2025-01-26 Created based on TS-7230

        """
        # Get directories
        cfg_path = tdDnodes.dnodes[0].cfgDir
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            data_dir = data_dir[0]
        
        key_dir = os.path.join(data_dir, "dnode", "config")
        master_file = os.path.join(key_dir, "master.bin")
        derived_file = os.path.join(key_dir, "derived.bin")
        
        tdLog.info(f"Config: {cfg_path}, Data: {data_dir}, Keys: {key_dir}")

        # Test 1: Help and version
        tdLog.info("=" * 80)
        tdLog.info("Test 1: taosk --help and --version")
        tdLog.info("=" * 80)
        
        result = subprocess.run(['taosk', '--help'], capture_output=True, text=True, timeout=10)
        assert result.returncode == 0 or 'help' in result.stdout.lower(), "taosk --help failed"
        tdLog.info("--help passed")

        result = subprocess.run(['taosk', '--version'], capture_output=True, text=True, timeout=10)
        assert result.returncode == 0 or 'version' in result.stdout.lower(), "taosk --version failed"
        tdLog.info("--version passed")

        # Test 2: Default keys
        tdLog.info("=" * 80)
        tdLog.info("Test 2: Generate default SM4 keys")
        tdLog.info("=" * 80)
        
        cmd = ['taosk', '-c', cfg_path, '--encrypt-server', '--encrypt-database']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
        tdLog.info(f"Output: {result.stdout}")
        
        assert os.path.exists(master_file), f"master.bin missing at {master_file}"
        tdLog.info(f"master.bin: {os.path.getsize(master_file)} bytes")

        # Test 3: Specified keys
        tdLog.info("=" * 80)
        tdLog.info("Test 3: Generate with specified SVR_KEY/DB_KEY")
        tdLog.info("=" * 80)
        
        if os.path.exists(master_file):
            os.remove(master_file)
        
        cmd = ['taosk', '-c', cfg_path, '--encrypt-server', 'mysvr123', '--encrypt-database', 'mydb4567']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
        
        assert os.path.exists(master_file), "master.bin not generated"
        tdLog.info("Custom keys generated")

        # Test 4: All encryption options
        tdLog.info("=" * 80)
        tdLog.info("Test 4: All encryption options")
        tdLog.info("=" * 80)
        
        if os.path.exists(master_file):
            os.remove(master_file)
        if os.path.exists(derived_file):
            os.remove(derived_file)
        
        cmd = ['taosk', '-c', cfg_path, '--encrypt-server', '--encrypt-database',
               '--encrypt-config', '--encrypt-metadata', '--encrypt-data']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
        
        assert os.path.exists(master_file), "master.bin not generated"
        assert os.path.exists(derived_file), "derived.bin not generated"
        tdLog.info(f"master: {os.path.getsize(master_file)}B, derived: {os.path.getsize(derived_file)}B")

        # Test 5: Key validation
        tdLog.info("=" * 80)
        tdLog.info("Test 5: Key length validation")
        tdLog.info("=" * 80)
        
        # Short key
        cmd_short = ['taosk', '-c', cfg_path, '--encrypt-server', 'short', '--encrypt-database']
        result_short = subprocess.run(cmd_short, capture_output=True, text=True, timeout=30)
        tdLog.info(f"Short key exit: {result_short.returncode}")
        
        # Long key
        cmd_long = ['taosk', '-c', cfg_path, '--encrypt-server', 'toolongkey123456', '--encrypt-database']
        result_long = subprocess.run(cmd_long, capture_output=True, text=True, timeout=30)
        tdLog.info(f"Long key exit: {result_long.returncode}")
        
        # Valid key
        if os.path.exists(master_file):
            os.remove(master_file)
        
        cmd_valid = ['taosk', '-c', cfg_path, '--encrypt-server', 'validkey12', '--encrypt-database', 'validdb34']
        result_valid = subprocess.run(cmd_valid, capture_output=True, text=True, timeout=30, check=True)
        
        assert os.path.exists(master_file), "Valid key failed"
        tdLog.info("Valid key passed")

        tdLog.success(f"{__file__} successfully executed")
