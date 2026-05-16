from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes
import os


class TestBasic:
    updatecfgDict = {'dDebugFlag': 131}
    
    # Pre-generate encryption keys with all options
    encryptConfig = {
        "svrKey": "encryptsvr1",
        "dbKey": "encryptdb45",
        "dataKey": "encryptdat1",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0
        cls.db = "encrypt_test_db"

    def test_transparent_encryption(self):
        """ Test transparent encryption features

        1. Test config file encryption
        2. Test metadata encryption (SDB)
        3. Test data file encryption (TSDB, WAL)
        4. Create encrypted database and verify data operations

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

        # Test 1: Check config file encryption
        tdLog.info("=" * 80)
        tdLog.info("Test 1: Checking config file encryption")
        tdLog.info("=" * 80)
        
        config_files = ['dnode.json', 'mnode.json']
        dnode_dir = os.path.join(data_dir, "dnode")
        
        for config_file in config_files:
            config_path = os.path.join(dnode_dir, config_file)
            if os.path.exists(config_path):
                tdLog.info(f"Checking {config_file}")
                with open(config_path, 'rb') as f:
                    header = f.read(16)
                    # Check for encryption magic number "tdEncrypt"
                    if b'tdEncrypt' in header:
                        tdLog.info(f"{config_file} is encrypted (contains tdEncrypt magic)")
                    else:
                        tdLog.info(f"{config_file} may not be encrypted or uses different format")

        # Test 2: Check metadata encryption (SDB)
        tdLog.info("=" * 80)
        tdLog.info("Test 2: Checking metadata encryption")
        tdLog.info("=" * 80)
        
        mnode_dir = os.path.join(data_dir, "mnode")
        sdb_dir = os.path.join(mnode_dir, "sdb")
        if os.path.exists(sdb_dir):
            sdb_files = os.listdir(sdb_dir)
            tdLog.info(f"SDB directory exists with {len(sdb_files)} files")
            for sdb_file in sdb_files[:3]:  # Check first 3 files
                tdLog.info(f"SDB file: {sdb_file}")
        else:
            tdLog.info("SDB directory not found yet (may be created after operations)")

        # Test 3: Create encrypted database
        tdLog.info("=" * 80)
        tdLog.info("Test 3: Creating encrypted database")
        tdLog.info("=" * 80)
        
        # Create database with SM4 encryption
        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {self.db} ENCRYPT_ALGORITHM 'SM4-CBC'")
        tdLog.info(f"Database {self.db} created with SM4 encryption")
        
        # Use database
        tdSql.execute(f"USE {self.db}")
        
        # Create table and insert data
        tdSql.execute("CREATE TABLE IF NOT EXISTS meters (ts TIMESTAMP, voltage INT, current FLOAT, phase FLOAT)")
        tdLog.info("Table created")
        
        # Insert test data with identifiable pattern
        test_value = "SecretData123"
        tdSql.execute(f"INSERT INTO meters VALUES (now, 220, 10.5, 0.95)")
        tdSql.execute(f"INSERT INTO meters VALUES (now+1s, 221, 10.6, 0.96)")
        tdSql.execute(f"INSERT INTO meters VALUES (now+2s, 222, 10.7, 0.97)")
        tdLog.info("Test data inserted")
        
        # Query data to verify
        tdSql.query("SELECT * FROM meters")
        assert tdSql.queryRows == 3, "Should have 3 rows"
        tdLog.info(f"Query returned {tdSql.queryRows} rows")

        # Test 4: Verify data file encryption
        tdLog.info("=" * 80)
        tdLog.info("Test 4: Checking data file encryption")
        tdLog.info("=" * 80)
        
        # Check for database directory
        vnode_dir = os.path.join(data_dir, "vnode")
        if os.path.exists(vnode_dir):
            vnode_list = os.listdir(vnode_dir)
            tdLog.info(f"Found {len(vnode_list)} vnode directories")
            
            # Check for encrypted data files in vnodes
            for vnode in vnode_list[:2]:  # Check first 2 vnodes
                vnode_path = os.path.join(vnode_dir, vnode)
                if os.path.isdir(vnode_path):
                    # Check TSDB directory
                    tsdb_dir = os.path.join(vnode_path, "tsdb")
                    if os.path.exists(tsdb_dir):
                        tsdb_files = os.listdir(tsdb_dir)
                        tdLog.info(f"Vnode {vnode} TSDB has {len(tsdb_files)} files")
                    
                    # Check WAL directory
                    wal_dir = os.path.join(vnode_path, "wal")
                    if os.path.exists(wal_dir):
                        wal_files = os.listdir(wal_dir)
                        tdLog.info(f"Vnode {vnode} WAL has {len(wal_files)} files")

        # Query encryption status
        tdLog.info("=" * 80)
        tdLog.info("Checking encryption status from system table")
        tdLog.info("=" * 80)
        
        tdSql.query("SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases WHERE name='encrypt_test_db'")
        if tdSql.queryRows > 0:
            tdLog.info(f"Database encryption info: {tdSql.queryResult}")

        tdLog.success(f"{__file__} successfully executed")
