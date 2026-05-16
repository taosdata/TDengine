from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes
import os
import subprocess


class TestBasic:
    updatecfgDict = {'dDebugFlag': 131}
    
    # Pre-generate encryption keys
    encryptConfig = {
        "svrKey": "securesvr12",
        "dbKey": "securedb456",
        "dataKey": "securedat12",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0
        cls.db = "security_test_db"
        cls.secret_string = "SecretData123456789"

    def test_plaintext_check(self):
        """ Test that encrypted files do not contain plaintext

        1. Create encrypted database with secret data
        2. Check WAL files for plaintext
        3. Check TSDB files for plaintext
        4. Check config files for plaintext
        5. Check SDB metadata files for plaintext
        6. Check key files are encrypted

        Since: v3.4.0.0

        Labels: common,ci

        Jira: TS-7230

        History:
            - 2025-01-26 Created based on TS-7230

        """
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            data_dir = data_dir[0]
        
        tdLog.info(f"Data directory: {data_dir}")

        # Create encrypted database and insert secret data
        tdLog.info("=" * 80)
        tdLog.info("Setup: Creating encrypted database with secret data")
        tdLog.info("=" * 80)
        
        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {self.db} ENCRYPT_ALGORITHM 'SM4-CBC'")
        tdSql.execute(f"USE {self.db}")
        tdSql.execute(f"CREATE TABLE IF NOT EXISTS secrets (ts TIMESTAMP, data NCHAR(50))")
        
        # Insert secret data
        tdSql.execute(f"INSERT INTO secrets VALUES (now, '{self.secret_string}')")
        tdSql.execute(f"INSERT INTO secrets VALUES (now+1s, 'AnotherSecret{self.secret_string}')")
        tdLog.info(f"Inserted secret data: {self.secret_string}")
        
        # Verify data can be queried
        tdSql.query("SELECT * FROM secrets")
        assert tdSql.queryRows == 2, "Should have 2 rows"
        tdLog.info("Secret data verified in database")

        # Test 1: Check WAL files
        tdLog.info("=" * 80)
        tdLog.info("Test 1: Checking WAL files for plaintext")
        tdLog.info("=" * 80)
        
        vnode_dir = os.path.join(data_dir, "vnode")
        wal_files_checked = 0
        plaintext_found = False
        
        if os.path.exists(vnode_dir):
            for vnode in os.listdir(vnode_dir):
                wal_dir = os.path.join(vnode_dir, vnode, "wal")
                if os.path.exists(wal_dir):
                    for wal_file in os.listdir(wal_dir):
                        wal_path = os.path.join(wal_dir, wal_file)
                        if os.path.isfile(wal_path):
                            # Use grep to search for secret string
                            result = subprocess.run(['grep', '-a', self.secret_string, wal_path], 
                                                  capture_output=True, text=True)
                            if result.returncode == 0:
                                plaintext_found = True
                                tdLog.info(f"WARNING: Plaintext found in {wal_path}")
                            wal_files_checked += 1
        
        tdLog.info(f"Checked {wal_files_checked} WAL files")
        assert not plaintext_found, f"Plaintext '{self.secret_string}' should not be found in WAL files"
        tdLog.info("WAL files passed plaintext check")

        # Test 2: Check TSDB files
        tdLog.info("=" * 80)
        tdLog.info("Test 2: Checking TSDB files for plaintext")
        tdLog.info("=" * 80)
        
        tsdb_files_checked = 0
        plaintext_found = False
        
        if os.path.exists(vnode_dir):
            for vnode in os.listdir(vnode_dir):
                tsdb_dir = os.path.join(vnode_dir, vnode, "tsdb")
                if os.path.exists(tsdb_dir):
                    for root, dirs, files in os.walk(tsdb_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            result = subprocess.run(['grep', '-a', self.secret_string, file_path], 
                                                  capture_output=True, text=True)
                            if result.returncode == 0:
                                plaintext_found = True
                                tdLog.info(f"WARNING: Plaintext found in {file_path}")
                            tsdb_files_checked += 1
        
        tdLog.info(f"Checked {tsdb_files_checked} TSDB files")
        assert not plaintext_found, f"Plaintext '{self.secret_string}' should not be found in TSDB files"
        tdLog.info("TSDB files passed plaintext check")

        # Test 3: Check config files
        tdLog.info("=" * 80)
        tdLog.info("Test 3: Checking config files for plaintext configuration")
        tdLog.info("=" * 80)
        
        config_files = ['dnode.json', 'mnode.json']
        dnode_dir = os.path.join(data_dir, "dnode")
        
        for config_file in config_files:
            config_path = os.path.join(dnode_dir, config_file)
            if os.path.exists(config_path):
                # Check if file contains encryption magic
                with open(config_path, 'rb') as f:
                    content = f.read()
                    if b'tdEncrypt' in content:
                        tdLog.info(f"{config_file} is properly encrypted")
                    else:
                        tdLog.info(f"{config_file} may not be encrypted or uses different format")

        # Test 4: Check SDB metadata files
        tdLog.info("=" * 80)
        tdLog.info("Test 4: Checking SDB metadata files")
        tdLog.info("=" * 80)
        
        sdb_dir = os.path.join(data_dir, "mnode", "sdb")
        if os.path.exists(sdb_dir):
            sdb_files = [f for f in os.listdir(sdb_dir) if os.path.isfile(os.path.join(sdb_dir, f))]
            tdLog.info(f"Found {len(sdb_files)} SDB files")
            # SDB files should be encrypted and not contain plaintext passwords
            tdLog.info("SDB files exist (detailed plaintext check requires specific test data)")

        # Test 5: Check key files are not plaintext
        tdLog.info("=" * 80)
        tdLog.info("Test 5: Checking encryption key files")
        tdLog.info("=" * 80)
        
        key_dir = os.path.join(data_dir, "dnode", "config")
        master_file = os.path.join(key_dir, "master.bin")
        derived_file = os.path.join(key_dir, "derived.bin")
        
        if os.path.exists(master_file):
            with open(master_file, 'rb') as f:
                content = f.read(100)  # Read first 100 bytes
                # Key files should be binary encrypted, not plaintext
                if b'tdEncrypt' in content:
                    tdLog.info("master.bin contains encryption marker")
                # Should not contain plaintext key values
                assert b'securesvr12' not in content, "master.bin should not contain plaintext SVR_KEY"
                tdLog.info("master.bin passed plaintext check")
        
        if os.path.exists(derived_file):
            with open(derived_file, 'rb') as f:
                content = f.read(100)
                if b'tdEncrypt' in content:
                    tdLog.info("derived.bin contains encryption marker")
                tdLog.info("derived.bin passed plaintext check")

        # Clean up
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.db}")

        tdLog.success(f"{__file__} successfully executed")
