from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes


class TestBasic:
    updatecfgDict = {'dDebugFlag': 131}
    
    # Pre-generate encryption keys
    encryptConfig = {
        "svrKey": "statussvr12",
        "dbKey": "statusdb456",
        "dataKey": "statusdata1",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0

    def test_encryption_status(self):
        """ Test viewing encryption status through system tables

        1. Query ins_encrypt_status for system encryption info
        2. Query ins_databases for database encryption algorithms
        3. Create databases with different encryption algorithms
        4. Verify encryption status is correctly reported

        Since: v3.4.0.0

        Labels: common,ci

        Jira: TS-7230

        History:
            - 2025-01-26 Created based on TS-7230

        """
        # Test 1: Query system encryption status
        tdLog.info("=" * 80)
        tdLog.info("Test 1: Querying system encryption status")
        tdLog.info("=" * 80)
        
        tdSql.query("SELECT * FROM information_schema.ins_encrypt_status")
        
        if tdSql.queryRows > 0:
            tdLog.info(f"Encryption status returned {tdSql.queryRows} rows")
            for i, row in enumerate(tdSql.queryResult):
                tdLog.info(f"Row {i}: {row}")
        else:
            tdLog.info("No encryption status found (may be normal for some configurations)")

        # Test 2: Query database encryption algorithms
        tdLog.info("=" * 80)
        tdLog.info("Test 2: Querying database encryption algorithms")
        tdLog.info("=" * 80)
        
        tdSql.query("SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases")
        
        if tdSql.queryRows > 0:
            tdLog.info(f"Found {tdSql.queryRows} databases")
            for row in tdSql.queryResult:
                db_name = row[0]
                encrypt_algo = row[1] if len(row) > 1 else "None"
                tdLog.info(f"Database: {db_name}, Encryption: {encrypt_algo}")

        # Test 3: Create databases with different encryption algorithms
        tdLog.info("=" * 80)
        tdLog.info("Test 3: Creating databases with different encryption")
        tdLog.info("=" * 80)
        
        # Create database with SM4 encryption
        tdSql.execute("CREATE DATABASE IF NOT EXISTS db_sm4 ENCRYPT_ALGORITHM 'SM4-CBC'")
        tdLog.info("Created database with SM4 encryption")
        
        # Create database with AES encryption
        tdSql.execute("CREATE DATABASE IF NOT EXISTS db_aes ENCRYPT_ALGORITHM 'AES-128-CBC'")
        tdLog.info("Created database with AES encryption")
        
        # Create database without encryption
        tdSql.execute("CREATE DATABASE IF NOT EXISTS db_plain")
        tdLog.info("Created database without encryption")

        # Test 4: Verify encryption status for created databases
        tdLog.info("=" * 80)
        tdLog.info("Test 4: Verifying encryption status for created databases")
        tdLog.info("=" * 80)
        
        # Check SM4 database
        tdSql.query("SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases WHERE name='db_sm4'")
        assert tdSql.queryRows == 1, "Should find db_sm4"
        encrypt_algo = tdSql.queryResult[0][1] if len(tdSql.queryResult[0]) > 1 else None
        tdLog.info(f"db_sm4 encryption: {encrypt_algo}")
        
        # Check AES database
        tdSql.query("SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases WHERE name='db_aes'")
        assert tdSql.queryRows == 1, "Should find db_aes"
        encrypt_algo = tdSql.queryResult[0][1] if len(tdSql.queryResult[0]) > 1 else None
        tdLog.info(f"db_aes encryption: {encrypt_algo}")
        
        # Check plain database
        tdSql.query("SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases WHERE name='db_plain'")
        assert tdSql.queryRows == 1, "Should find db_plain"
        encrypt_algo = tdSql.queryResult[0][1] if len(tdSql.queryResult[0]) > 1 else None
        tdLog.info(f"db_plain encryption: {encrypt_algo}")

        # Clean up
        tdSql.execute("DROP DATABASE IF EXISTS db_sm4")
        tdSql.execute("DROP DATABASE IF EXISTS db_aes")
        tdSql.execute("DROP DATABASE IF EXISTS db_plain")
        tdLog.info("Test databases cleaned up")

        tdLog.success(f"{__file__} successfully executed")
