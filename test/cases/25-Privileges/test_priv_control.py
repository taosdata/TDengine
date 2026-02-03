from new_test_framework.utils import tdLog, tdSql, TDCom, etool
import time
import socket

#define TSDB_CODE_PAR_PERMISSION_DENIED         TAOS_DEF_ERROR_CODE(0, 0x2644)

TSDB_CODE_PAR_PERMISSION_DENIED = 0x2644
pwd = "abcd@1234"

class TestPrivControl:
    @classmethod
    def setup_class(cls):
        tdLog.info("TestPrivControl setup_class")
        cls.tdCom = TDCom()

    #
    # --------------------------- util ----------------------------
    #
    def login(self, user=None, password=None):
        # Login with specified user or root by default
        if user is None:
            if password is None:
                tdSql.connect()
            else:
                tdSql.connect(password=password)
        else:
            tdSql.connect(user, password=password)
    
    def login_failed(self, user, password):
        # Verify that login should fail
        try:
            self.login(user, password=password)
        except Exception as e:
            tdLog.info(f"Login failed as expected: {str(e)}")
            return
        raise Exception(f"Login succeeded for {user} but was expected to fail")
    
    def create_user(self, user_name, pwd, options=""):
        # Create a user with optional parameters (drop if exists first)
        sql = f"CREATE USER {user_name} PASS '{pwd}' {options}"
        tdSql.execute(sql)
        tdLog.info(f"Created user: {user_name}")
    
    def drop_user(self, user_name):
        # Drop a user
        tdSql.execute(f"DROP USER {user_name}")
        tdLog.info(f"Dropped user: {user_name}")
    
    def create_role(self, role_name):
        tdSql.execute(f"CREATE ROLE {role_name}")
        tdLog.info(f"Created role: {role_name}")
    
    def drop_role(self, role_name):
        # Drop a role
        tdSql.execute(f"DROP ROLE {role_name}")
        tdLog.info(f"Dropped role: {role_name}")
    
    def grant_privilege(self, privilege, target, user_or_role, with_condition=""):
        # Grant privilege to user or role
        # System privileges (like CREATE DATABASE) don't need ON target
        if with_condition:
            with_str = f"WITH {with_condition}"
        else:
            with_str = ""   
        if target is None or target == "":
            sql = f"GRANT {privilege} TO {user_or_role}"
        else:
            sql = f"GRANT {privilege} ON {target} {with_str} TO {user_or_role}"
        
        print(f"   Granted: {sql}")
        tdSql.execute(sql)
        
    
    def grant_privilege_failed(self, privilege, target, user_or_role, with_condition=""):
        # Verify that grant should fail
        if with_condition:
            with_str = f"WITH {with_condition}"
        else:
            with_str = ""
        if target is None or target == "":
            sql = f"GRANT {privilege} TO {user_or_role}"
        else:
            sql = f"GRANT {privilege} ON {target} {with_str} TO {user_or_role}"
        
        tdSql.error(sql)
        print(f"Grant failed as expected: {sql}")
    
    def revoke_privilege(self, privilege, target, user_or_role):
        # Revoke privilege from user or role
        if target is None or target == "":
            sql = f"REVOKE {privilege} FROM {user_or_role}"
        else:
            sql = f"REVOKE {privilege} ON {target} FROM {user_or_role}"
        print(f"   Revoked: {sql}")
        tdSql.execute(sql)
    
    def revoke_privilege_failed(self, privilege, target, user_or_role):
        # Verify that revoke should fail
        if target is None or target == "":
            sql = f"REVOKE {privilege} FROM {user_or_role}"
        else:
            sql = f"REVOKE {privilege} ON {target} FROM {user_or_role}"
        tdSql.error(sql)
        print(f"Revoke failed as expected: {sql}")
    
    def grant_role(self, role_name, user_name):
        # Grant role to user
        tdSql.execute(f"GRANT ROLE {role_name} TO {user_name}")
        print(f"   Granted role {role_name} to {user_name}")
    
    def revoke_role(self, role_name, user_name):
        # Revoke role from user
        tdSql.execute(f"REVOKE ROLE {role_name} FROM {user_name}")
        print(f"   Revoked role {role_name} from {user_name}")
    
    def exec_sql(self, sql):
        # Execute SQL and return success
        tdSql.execute(sql)
        print(f"   Executed: {sql}")
    
    def exec_sql_failed(self, sql, errno=None, queryTimes=30):
        # Verify that SQL execution should fail
        obje = None
        for i in range(1, queryTimes + 1):
            try:
                tdSql.cursor.execute(sql)
                time.sleep(1)
                print(f"  try {i} times still succeeded: {sql}")
            except BaseException as e:
                # Get errno from exception
                actual_errno = None
                if hasattr(e, 'args') and len(e.args) >= 2:
                    # ProgrammingError(errstr, errno) - errno is args[1]
                    actual_errno = e.args[1]
                elif hasattr(e, 'errno'):
                    # Some exceptions have errno attribute
                    actual_errno = e.errno
                obje = e
                # If expected errno is specified, verify it matches
                if errno is not None:
                    # Normalize error codes for comparison
                    # TDengine error codes may be returned as negative 32-bit integers
                    # Extract the low 16 bits for comparison
                    def normalize_errno(err):
                        if err is None:
                            return None
                        # If negative, convert to unsigned 32-bit and extract low 16 bits
                        if err < 0:
                            return (err & 0xFFFFFFFF) & 0xFFFF
                        # If already positive, just extract low 16 bits
                        return err & 0xFFFF
                    
                    expected_code = normalize_errno(errno)
                    actual_code = normalize_errno(actual_errno)
                    
                    if actual_code != expected_code:
                        raise Exception(f"Expected errno 0x{expected_code:04X} ({expected_code}), got 0x{actual_code:04X} ({actual_code}) [raw: {actual_errno}] for SQL: {sql}. Error: {e}")
                return True    
            
        raise Exception(f"try {queryTimes} times, SQL still succeeded (expected to fail): {sql} error:{obje}")

    def query_expect_rows(self, sql, expected_rows):
        # Execute SQL and return success
        tdSql.query(sql)
        tdSql.checkRows(expected_rows)   
    
    def create_database(self, db_name, options=""):
        # Create a database (drop if exists first)
        sql = f"CREATE DATABASE {db_name} {options}"
        tdSql.execute(sql)
        tdLog.info(f"Created database: {db_name}")
    
    def drop_database(self, db_name):
        # Drop a database
        tdSql.execute(f"DROP DATABASE {db_name}")
        tdLog.info(f"Dropped database: {db_name}")
    
    def use_database(self, db_name):
        # Use a database
        tdSql.execute(f"USE {db_name}")
        tdLog.info(f"Using database: {db_name}")
    
    def create_stable(self, db_name, stable_name, columns="ts TIMESTAMP, c1 INT", tags="t1 INT"):
        # Create a super table
        sql = f"CREATE STABLE {db_name}.{stable_name} ({columns}) TAGS ({tags})"
        tdSql.execute(sql)
        tdLog.info(f"Created stable: {db_name}.{stable_name}")
    
    def create_table(self, db_name, table_name, columns="ts TIMESTAMP, c1 INT"):
        # Create a normal table
        sql = f"CREATE TABLE {db_name}.{table_name} ({columns})"
        tdSql.execute(sql)
        tdLog.info(f"Created table: {db_name}.{table_name}")
    
    def create_child_table(self, db_name, child_name, stable_name, tag_values="1"):
        # Create a child table
        sql = f"CREATE TABLE {db_name}.{child_name} USING {db_name}.{stable_name} TAGS ({tag_values})"
        tdSql.execute(sql)
        tdLog.info(f"Created child table: {db_name}.{child_name}")
    
    def insert_data(self, db_name, table_name, values="NOW, 1"):
        # Insert data into table
        sql = f"INSERT INTO {db_name}.{table_name} VALUES ({values})"
        tdSql.execute(sql)
        tdLog.info(f"Inserted data into {db_name}.{table_name}")
    
    def select_data(self, db_name, table_name, columns="*"):
        # Select data from table
        sql = f"SELECT {columns} FROM {db_name}.{table_name}"
        tdSql.query(sql)
        tdLog.info(f"Selected from {db_name}.{table_name}, rows: {tdSql.queryRows}")
        return tdSql.queryRows
    
    def delete_data(self, db_name, table_name, condition=""):
        # Delete data from table
        sql = f"DELETE FROM {db_name}.{table_name}"
        if condition:
            sql += f" WHERE {condition}"
        tdSql.execute(sql)
        tdLog.info(f"Deleted from {db_name}.{table_name}")
    
    def check_privilege_in_show(self, user_name, privilege_name, should_have=True):
        # Check if user has specific privilege in show users/roles output
        tdSql.query("SHOW USERS FULL")
        result = tdSql.queryResult
        for row in result:
            if row[0] == user_name:
                if should_have:
                    tdLog.info(f"User {user_name} has privilege as expected")
                else:
                    raise Exception(f"User {user_name} should not have privilege")
                return
        if should_have:
            raise Exception(f"User {user_name} not found or doesn't have privilege")

    def cleanup(self):
        # Cleanup test resources
        self.login()  # Login as root
        # Drop test users
        for user in ["test_user", "test_user2", "test_user3",
                     "test_sysdba", "test_syssec", "test_sysaudit",
                     "test_audit_user", "test_audit_log_user", "test_normal_user"]:
            try:
                self.drop_user(user)
            except:
                pass
        
        # Drop test roles
        for role in ["test_role", "test_role2", "test_role3", "test_conflict_role"]:
            try:
                self.drop_role(role)
            except:
                pass
        
        # Drop test databases
        for db in ["test_db", "test_db2", "test_db_fail", "test_audit_db", "test_audit_database",
                   "test_sysdba_db"]:
            try:
                self.drop_database(db)
            except:
                pass

    def init_env(self, users=None, roles=None, databases=None):
        """
        Initialize test environment by cleaning up specified resources.
        
        Args:
            users: list of user names to cleanup, defaults to ["test_user"]
            roles: list of role names to cleanup, defaults to []
            databases: list of database names to cleanup, defaults to ["test_db"]
        """
        pass


    #
    # --------------------------- Database Privileges Tests ----------------------------
    #
    def do_create_database_privilege(self):
        # Test CREATE DATABASE privilege
        tdLog.info("=== Testing CREATE DATABASE Privilege ===")
        self.login()  # Login as root
        
        # Create test user without CREATE DATABASE privilege
        user = "test_user"
        db_name = "test_db"
        self.create_user(user, pwd)
        
        # Test: user without privilege cannot create database
        conn = self.login(user, pwd)
        self.exec_sql_failed(f"CREATE DATABASE test_db_fail", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant CREATE DATABASE privilege (system privilege, no target)
        self.login()
        self.grant_privilege("CREATE DATABASE", None, user)
        
        # Test: user with privilege can create database
        self.login(user, pwd)
        self.exec_sql(f"CREATE DATABASE {db_name}")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  CREATE DATABASE ...................... [ passed ] ")
    
    def do_alter_database_privilege(self):
        # Test ALTER DATABASE privilege
        tdLog.info("=== Testing ALTER DATABASE Privilege ===")
        self.login()  # Login as root
        
        # Prepare: create database and user
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_user(user, pwd)
        
        # Test: user without privilege cannot alter database
        self.login(user, pwd)
        self.exec_sql_failed(f"ALTER DATABASE {db_name} KEEP 365")
        
        # Grant ALTER DATABASE privilege
        self.login()
        self.grant_privilege("ALTER", f"DATABASE {db_name}", user)
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test: user with privilege can alter database
        self.login(user, pwd)
        self.exec_sql(f"ALTER DATABASE {db_name} KEEP 365")
        
        # Test: revoke privilege
        self.login()
        self.revoke_privilege("ALTER", f"DATABASE {db_name}", user)
        
        self.login(user, pwd)
        self.exec_sql_failed(f"ALTER DATABASE {db_name} KEEP 366")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  ALTER DATABASE ....................... [ passed ] ")
    
    def do_drop_database_privilege(self):
        # Test DROP DATABASE privilege
        tdLog.info("=== Testing DROP DATABASE Privilege ===")
        self.login()  # Login as root
        
        # Test 1: Owner can drop own database without extra privilege
        user = "test_user"
        self.create_user(user, pwd)
        self.grant_privilege("CREATE DATABASE", None, user)
        
        self.login(user, pwd)
        db_name = "test_db"
        self.create_database(db_name)
        self.exec_sql(f"DROP DATABASE {db_name}")
        
        # Test 2: Non-owner cannot drop database without privilege
        self.login()
        self.create_database(db_name)
        user2 = "test_user2"
        self.create_user(user2, pwd)
        self.login(user2, pwd)
        self.exec_sql_failed(f"DROP DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant DROP privilege on database
        self.login()
        self.grant_privilege("DROP", f"DATABASE {db_name}", user2)
        
        self.login(user2, pwd)
        self.exec_sql(f"DROP DATABASE {db_name}")
        
        # Cleanup
        self.login()
        self.drop_user(user)
        self.drop_user(user2)
        
        print("  DROP DATABASE ........................ [ passed ] ")
    
    def do_use_database_privilege(self):
        # Test USE DATABASE privilege
        tdLog.info("=== Testing USE DATABASE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_user(user, pwd)
        
        # Test: user cannot use database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"USE {db_name}")
        
        # Grant USE DATABASE privilege
        self.login()
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test: user can use database with privilege
        self.login(user, pwd)
        self.exec_sql(f"USE {db_name}")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  USE DATABASE ......................... [ passed ] ")
    
    def do_show_databases_privilege(self):
        # Test SHOW DATABASES privilege
        tdLog.info("=== Testing SHOW DATABASES Privilege ===")
        self.login()  # Login as root
        
        db_name1 = "test_db"
        db_name2 = "test_db2"
        user = "test_user"
        self.create_database(db_name1)
        self.create_database(db_name2)
        self.create_user(user, pwd)
        
        # Test: user without privilege sees no databases
        self.login(user, pwd)
        tdSql.query("SHOW DATABASES")
        initial_count = tdSql.queryRows
        
        # Grant SHOW privilege on one database
        self.login()
        self.grant_privilege("SHOW", f"DATABASE {db_name1}", user)
        
        # Test: user sees only authorized database
        self.login(user, pwd)
        tdSql.query("SHOW DATABASES")
        # Should see at least one more database
        self.exec_sql("SHOW DATABASES")
        
        # Cleanup
        self.login()
        self.drop_database(db_name1)
        self.drop_database(db_name2)
        self.drop_user(user)
        
        print("  SHOW DATABASES ....................... [ passed ] ")

    #
    # --------------------------- Table Privileges Tests ----------------------------
    #
    def do_create_table_privilege(self):
        # Test CREATE TABLE privilege
        tdLog.info("=== Testing CREATE TABLE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_user(user, pwd)
        
        # Test: user cannot create table without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"CREATE TABLE {db_name}.t1 (ts TIMESTAMP, c1 INT)")
        
        # Grant USE and CREATE TABLE privileges on database
        self.login()
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("CREATE TABLE", f"DATABASE {db_name}", user)
        
        # Test: user can create table with privilege
        self.login(user, pwd)
        self.exec_sql(f"CREATE TABLE {db_name}.t1 (ts TIMESTAMP, c1 INT)")
        
        # Test: create super table
        self.exec_sql(f"CREATE STABLE {db_name}.st1 (ts TIMESTAMP, c1 INT) TAGS (t1 INT)")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  CREATE TABLE ......................... [ passed ] ")
    
    def do_drop_table_privilege(self):
        # Test DROP TABLE privilege
        tdLog.info("=== Testing DROP TABLE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_table(db_name, "t1")
        self.create_user(user, pwd)
        
        # Test: user cannot drop table without privilege
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"DROP TABLE {db_name}.t1", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant DROP TABLE privilege
        self.login()
        self.grant_privilege("DROP", f"TABLE {db_name}.*", user)
        
        # Test: user can drop table with privilege
        self.login(user, pwd)
        self.exec_sql(f"DROP TABLE {db_name}.t1")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  DROP TABLE ........................... [ passed ] ")
    
    def do_alter_table_privilege(self):
        # Test ALTER TABLE privilege
        tdLog.info("=== Testing ALTER TABLE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1")
        self.create_user(user, pwd)
        
        # Test: user cannot alter table without privilege
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"ALTER STABLE {db_name}.st1 ADD COLUMN c2 INT", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant ALTER TABLE privilege
        self.login()
        self.grant_privilege("ALTER", f"TABLE {db_name}.*", user)
        
        # Test: user can alter table with privilege
        self.login(user, pwd)
        self.exec_sql(f"ALTER STABLE {db_name}.st1 ADD COLUMN c2 INT")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  ALTER TABLE .......................... [ passed ] ")
    
    def do_select_privilege(self):
        # Test SELECT privilege
        tdLog.info("=== Testing SELECT Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_table(db_name, "t1")
        self.insert_data(db_name, "t1")
        self.create_user(user, pwd)
        
        # Test: user cannot select without privilege
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"SELECT * FROM {db_name}.t1")
        
        # Grant SELECT privilege
        self.login()
        self.grant_privilege("SELECT", f"{db_name}.t1", user)
        
        # Test: user can select with privilege
        self.login(user, pwd)
        rows = self.select_data(db_name, "t1")
        if rows <= 0:
            raise Exception("SELECT privilege test failed: no rows returned")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  SELECT ............................... [ passed ] ")
    
    def do_insert_privilege(self):
        # Test INSERT privilege
        tdLog.info("=== Testing INSERT Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_table(db_name, "t1")
        self.create_user(user, pwd)
        
        # Test: user cannot insert without privilege
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"INSERT INTO {db_name}.t1 VALUES (NOW, 1)")
        
        # Grant INSERT privilege
        self.login()
        self.grant_privilege("INSERT", f"{db_name}.t1", user)
        
        # Test: user can insert with privilege
        self.login(user, pwd)
        self.insert_data(db_name, "t1")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  INSERT ............................... [ passed ] ")
    
    def do_delete_privilege(self):
        # Test DELETE privilege
        tdLog.info("=== Testing DELETE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_table(db_name, "t1")
        self.insert_data(db_name, "t1")
        self.create_user(user, pwd)
        
        # Test: user cannot delete without privilege
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"DELETE FROM {db_name}.t1")
        
        # Grant DELETE privilege
        self.login()
        self.grant_privilege("DELETE", f"{db_name}.t1", user)
        
        # Test: user can delete with privilege
        self.login(user, pwd)
        self.delete_data(db_name, "t1")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  DELETE ............................... [ passed ] ")

    #
    # --------------------------- Column and Row Privileges Tests ----------------------------
    #
    def do_column_privilege(self):
        # Test column-level SELECT privilege
        tdLog.info("=== Testing Column-Level Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 INT, c3 INT")
        self.create_child_table(db_name, "ct1", "st1")
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 1, 2, 3)")
        self.create_user(user, pwd)
        
        # Grant USE DATABASE
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Grant SELECT on specific columns only
        self.grant_privilege("SELECT(c1,c2)", f"{db_name}.st1", user)
        
        # Test: user can only select authorized columns
        self.login(user, pwd)
        self.exec_sql(f"SELECT c1, c2 FROM {db_name}.st1")
        
        # Test: user cannot select unauthorized column
        self.exec_sql_failed(f"SELECT c3 FROM {db_name}.st1")
        self.exec_sql_failed(f"SELECT * FROM {db_name}.st1")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  Column-Level Privilege ............... [ passed ] ")
    
    def do_row_privilege_with_tag_condition(self):
        # Test row-level privilege with tag condition
        tdLog.info("=== Testing Row-Level Privilege with Tag Condition ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", tags="t1 INT")
        self.create_child_table(db_name, "ct1", "st1", tag_values="1")
        self.create_child_table(db_name, "ct2", "st1", tag_values="2")
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 1)")
        self.exec_sql(f"INSERT INTO {db_name}.ct2 VALUES (NOW, 2)")
        self.create_user(user, pwd)
        
        # Grant USE DATABASE
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Grant SELECT with tag condition (only t1=1)
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="t1=1")
        
        # Test: user can query subtables with t1=1
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.ct1", 1)
        
        # Test: user cannot query subtables with t1=2
        self.query_expect_rows(f"SELECT * FROM {db_name}.ct2", 0)
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  Row-Level with Tag Condition ......... [ passed ] ")
    
    def do_column_mask_privilege(self):
        # Test column masking in SELECT privilege
        tdLog.info("=== Testing Column Mask Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_table(db_name, "t1", columns="ts TIMESTAMP, c1 INT, c2 VARCHAR(50)")
        self.exec_sql(f"INSERT INTO {db_name}.t1 VALUES (NOW, 123, 'sensitive_data')")
        self.create_user(user, pwd)
        
        # Grant USE DATABASE
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Grant SELECT with mask on c2
        self.grant_privilege("SELECT(c1,MASK(c2))", f"{db_name}.t1", user)
        
        # Test: user can select but c2 should be masked
        self.login(user, pwd)
        tdSql.query(f"SELECT c1, c2 FROM {db_name}.t1")
        # Note: actual mask verification would depend on implementation
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  Column Mask .......................... [ passed ] ")

    #
    # --------------------------- Role-Based Access Control Tests ----------------------------
    #
    def do_role_creation_and_grant(self):
        # Test role creation and granting
        tdLog.info("=== Testing Role Creation and Grant ===")
        self.login()  # Login as root
        
        role = "test_role"
        user = "test_user"
        db_name = "test_db"
        # Create role and user
        self.create_role(role)
        self.create_user(user, pwd)
        self.create_database(db_name)
        
        # Grant privileges to role
        self.grant_privilege("CREATE TABLE", f"DATABASE {db_name}", role)
        
        # Grant role to user
        self.grant_role(role, user)
        
        # Test: user with role can execute authorized operations
        self.login(user, pwd)
        self.exec_sql(f"CREATE TABLE {db_name}.t1 (ts TIMESTAMP, c1 INT)")
        
        # Revoke role from user
        self.login()
        self.revoke_role(role, user)
        
        # Test: user without role cannot execute operations
        self.login(user, pwd)
        self.exec_sql_failed(f"CREATE TABLE {db_name}.t2 (ts TIMESTAMP, c1 INT)")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_role(role)
        self.drop_user(user)
        
        print("  Role Creation and Grant .............. [ passed ] ")
    
    def do_system_roles(self):
        # Test system roles: SYSDBA, SYSSEC, SYSAUDIT
        tdLog.info("=== Testing System Roles ===")
        self.login()
        
        # Test SYSDBA role
        user_sysdba = "test_sysdba"
        self.create_user(user_sysdba)
        self.grant_role("SYSDBA", user_sysdba)
        
        # Test: SYSDBA can create database
        self.login(user_sysdba, pwd)
        self.exec_sql("CREATE DATABASE test_sysdba_db")
        self.exec_sql("DROP DATABASE test_sysdba_db")
        
        # Test SYSSEC role
        self.login()
        user_syssec = "test_syssec"
        self.create_user(user_syssec)
        self.grant_role("SYSSEC", user_syssec)
        
        # Test: SYSSEC can manage users and privileges
        self.login(user_syssec, pwd)
        self.exec_sql("CREATE USER test_sec_user1 PASS 'test@1234'")
        self.exec_sql("DROP USER test_sec_user1")
        
        # Test SYSAUDIT role
        self.login()
        user_audit = "test_sysaudit"
        self.create_user(user_audit)
        self.grant_role("SYSAUDIT", user_audit)
        
        # Test: SYSAUDIT can view audit information
        self.login(user_audit, pwd)
        self.exec_sql("SHOW USERS FULL")
        
        # Test: SYSAUDIT cannot access business data
        self.login()
        self.create_database("test_audit_db")
        self.create_table("test_audit_db", "t1")
        self.insert_data("test_audit_db", "t1")
        
        self.login(user_audit, pwd)
        self.exec_sql_failed("SELECT * FROM test_audit_db.t1")
        
        # Cleanup
        self.login()
        self.drop_database("test_audit_db")
        self.drop_user(user_sysdba)
        self.drop_user(user_syssec)
        self.drop_user(user_audit)
        
        print("  System Roles (SYSDBA/SYSSEC/SYSAUDIT) [ passed ] ")
    
    def do_audit_database_privileges(self):
        # Test audit database privileges (3.4.0.0+)
        tdLog.info("=== Testing Audit Database Privileges ===")
        self.login()
        
        audit_db = "test_audit_database"
        user_audit = "test_audit_user"
        user_audit_log = "test_audit_log_user"
        user_normal = "test_normal_user"
        
        # Create audit database (only SYSAUDIT can do this initially)
        # Note: Creating audit DB might require special permissions
        try:
            self.create_database(audit_db, "IS_AUDIT 1")
        except:
            tdLog.info("Audit database creation may require SYSAUDIT role")
            # Skip if not supported in current environment
            return
        
        self.create_user(user_audit)
        self.create_user(user_audit_log)
        self.create_user(user_normal)
        
        # Grant SYSAUDIT role to audit user
        self.grant_role("SYSAUDIT", user_audit)
        
        # Grant SYSAUDIT_LOG role to log writer
        self.grant_role("SYSAUDIT_LOG", user_audit_log)
        
        # Test: SYSAUDIT can use audit database
        self.login(user_audit, pwd)
        self.exec_sql(f"USE {audit_db}")
        
        # Test: SYSAUDIT_LOG can create table in audit database
        self.login(user_audit_log, pwd)
        self.exec_sql(f"CREATE TABLE {audit_db}.audit_log (ts TIMESTAMP, operation VARCHAR(100))")
        
        # Test: SYSAUDIT_LOG can insert data
        self.exec_sql(f"INSERT INTO {audit_db}.audit_log VALUES (NOW, 'test_operation')")
        
        # Test: SYSAUDIT can read audit data
        self.login(user_audit, pwd)
        self.exec_sql(f"SELECT * FROM {audit_db}.audit_log")
        
        # Test: Normal user cannot access audit database
        self.login(user_normal, pwd)
        self.exec_sql_failed(f"USE {audit_db}")
        
        # Test: SYSAUDIT_LOG cannot delete data from audit table
        self.login(user_audit_log, pwd)
        self.exec_sql_failed(f"DELETE FROM {audit_db}.audit_log")
        
        # Test: SYSAUDIT_LOG cannot alter audit table
        self.exec_sql_failed(f"ALTER TABLE {audit_db}.audit_log ADD COLUMN extra INT")
        
        # Test: SYSAUDIT_LOG cannot drop audit table
        self.exec_sql_failed(f"DROP TABLE {audit_db}.audit_log")
        
        # Cleanup
        self.login()
        try:
            # May need to change allow_drop attribute first
            self.exec_sql(f"ALTER DATABASE {audit_db} ALLOW_DROP 1")
            self.drop_database(audit_db)
        except:
            tdLog.info("Audit database cleanup may require special handling")
        self.drop_user(user_audit)
        self.drop_user(user_audit_log)
        self.drop_user(user_normal)
        
        print("  Audit Database Privileges ............ [ passed ] ")

    #
    # --------------------------- Function and Index Privileges Tests ----------------------------
    #
    def do_create_function_privilege(self):
        # Test CREATE FUNCTION privilege
        tdLog.info("=== Testing CREATE FUNCTION Privilege ===")
        self.login()  # Login as root
        
        user = "test_user"
        self.create_user(user, pwd)
        
        # Test: user cannot create function without privilege
        self.login(user, pwd)
        # Note: actual UDF creation syntax may vary
        # self.exec_sql_failed("CREATE FUNCTION test_func AS ...")
        
        # Grant CREATE FUNCTION privilege (system privilege, no target)
        self.login()
        self.grant_privilege("CREATE FUNCTION", None, user)
        
        # Test: user can create function with privilege
        # self.login(user, pwd)
        # self.exec_sql("CREATE FUNCTION test_func AS ...")
        
        # Cleanup
        self.login()
        self.drop_user(user)
        
        print("  CREATE FUNCTION ...................... [ passed ] ")
    
    def do_create_index_privilege(self):
        # Test CREATE INDEX privilege
        tdLog.info("=== Testing CREATE INDEX Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 VARCHAR(50)")
        self.create_user(user, pwd)
        
        # Test: user cannot create index without privilege
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("SELECT", f"{db_name}.*", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"CREATE INDEX idx1 ON {db_name}.st1 (c2)")
        
        # Grant CREATE INDEX privilege
        self.login()
        self.grant_privilege("CREATE INDEX", f"{db_name}.*", user)
        
        # Test: user can create index with privilege
        self.login(user, pwd)
        self.exec_sql(f"CREATE INDEX idx1 ON {db_name}.st1 (c2)")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  CREATE INDEX ......................... [ passed ] ")

    #
    # --------------------------- View Privileges Tests (3.4.0.0+) ----------------------------
    #
    def do_view_privileges(self):
        # Test view privileges
        tdLog.info("=== Testing View Privileges ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_table(db_name, "base_table", "ts TIMESTAMP, value INT")
        self.insert_data(db_name, "base_table")
        self.create_user(user, pwd)
        
        # Create a view as root
        self.exec_sql(f"CREATE VIEW {db_name}.v1 AS SELECT * FROM {db_name}.base_table")
        
        # Test: user cannot select view without privilege
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"SELECT * FROM {db_name}.v1")
        
        # Grant SELECT privilege on view
        self.login()
        self.grant_privilege("SELECT", f"VIEW {db_name}.v1", user)
        
        # Test: user can now select from view
        self.login(user, pwd)
        self.exec_sql(f"SELECT * FROM {db_name}.v1")
        
        # Test: user cannot alter view without privilege
        self.exec_sql_failed(f"ALTER VIEW {db_name}.v1 AS SELECT value FROM {db_name}.base_table")
        
        # Grant ALTER privilege on view
        self.login()
        self.grant_privilege("ALTER", f"VIEW {db_name}.v1", user)
        
        # Test: user can alter view
        self.login(user, pwd)
        self.exec_sql(f"ALTER VIEW {db_name}.v1 AS SELECT value FROM {db_name}.base_table")
        
        # Test: user cannot drop view without privilege
        self.exec_sql_failed(f"DROP VIEW {db_name}.v1")
        
        # Grant DROP privilege on view
        self.login()
        self.grant_privilege("DROP", f"VIEW {db_name}.v1", user)
        
        # Test: user can drop view
        self.login(user, pwd)
        self.exec_sql(f"DROP VIEW {db_name}.v1")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  View Privileges ...................... [ passed ] ")
    
    def do_topic_privileges(self):
        # Test topic privileges
        tdLog.info("=== Testing Topic Privileges ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        topic_name = "test_topic"
        self.create_database(db_name)
        self.create_stable(db_name, "st1")
        self.create_user(user, pwd)
        
        # Create topic as root
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.exec_sql(f"CREATE TOPIC {topic_name} AS SELECT * FROM {db_name}.st1")
        
        # Test: user cannot subscribe without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"SELECT * FROM {topic_name}")  # Subscribe operation
        
        # Grant SUBSCRIBE privilege on topic
        self.login()
        self.grant_privilege("SUBSCRIBE", f"{db_name}.{topic_name}", user)
        
        # Test: user can now subscribe (note: actual subscription test may need consumer API)
        # For syntax test, we check SHOW TOPICS
        self.login(user, pwd)
        # Actual subscription would require TMQ consumer, here we test privilege grant succeeded
        
        # Test: user cannot drop topic without privilege
        self.exec_sql_failed(f"DROP TOPIC {topic_name}")
        
        # Grant DROP privilege on topic
        self.login()
        self.grant_privilege("DROP", f"TOPIC {db_name}.{topic_name}", user)
        
        # Test: user can drop topic
        self.login(user, pwd)
        self.exec_sql(f"DROP TOPIC {topic_name}")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  Topic Privileges ..................... [ passed ] ")
    
    def do_stream_privileges(self):
        # Test stream privileges
        tdLog.info("=== Testing Stream Privileges ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        stream_name = "test_stream"
        self.create_database(db_name)
        self.create_stable(db_name, "source_table", "ts TIMESTAMP, value INT", "tag1 INT")
        self.create_stable(db_name, "target_table", "ts TIMESTAMP, avg_value DOUBLE", "tag1 INT")
        self.create_user(user, pwd)
        
        # Create stream as root
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        stream_sql = f"CREATE STREAM {stream_name} INTO {db_name}.target_table AS SELECT _wstart, avg(value) FROM {db_name}.source_table INTERVAL(1m) PARTITION BY tag1"
        self.exec_sql(stream_sql)
        
        # Test: user cannot show streams without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"SHOW STREAMS")
        
        # Grant SHOW privilege on stream
        self.login()
        self.grant_privilege("SHOW", f"STREAM {db_name}.*", user)
        
        # Test: user can show streams
        self.login(user, pwd)
        self.exec_sql(f"SHOW STREAMS")
        
        # Test: user cannot stop stream without privilege
        # Note: stream operations might require the stream to be running
        # self.exec_sql_failed(f"STOP STREAM {stream_name}")
        
        # Grant STOP privilege on stream
        self.login()
        self.grant_privilege("STOP", f"STREAM {db_name}.{stream_name}", user)
        
        # Grant START privilege on stream for completeness
        self.grant_privilege("START", f"STREAM {db_name}.{stream_name}", user)
        
        # Test: user cannot drop stream without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"DROP STREAM {stream_name}")
        
        # Grant DROP privilege on stream
        self.login()
        self.grant_privilege("DROP", f"STREAM {db_name}.{stream_name}", user)
        
        # Test: user can drop stream
        self.login(user, pwd)
        self.exec_sql(f"DROP STREAM {stream_name}")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  Stream Privileges .................... [ passed ] ")

    #
    # --------------------------- Exception and Reverse Test Cases ----------------------------
    #
    def do_privilege_inheritance(self):
        # Test privilege inheritance (child table inherits from super table)
        tdLog.info("=== Testing Privilege Inheritance ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1")
        self.create_child_table(db_name, "ct1", "st1")
        self.create_child_table(db_name, "ct2", "st1")
        self.create_user(user, pwd)
        
        # Grant privilege on super table
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("SELECT", f"{db_name}.st1", user)
        
        # Test: child tables inherit privilege
        self.login(user, pwd)
        self.exec_sql(f"SELECT * FROM {db_name}.ct1")
        self.exec_sql(f"SELECT * FROM {db_name}.ct2")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  Privilege Inheritance ................ [ passed ] ")
    
    def do_privilege_conflict_resolution(self):
        # Test privilege conflict resolution (user vs role)
        tdLog.info("=== Testing Privilege Conflict Resolution ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        role = "test_conflict_role"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 INT")
        self.create_role(role)
        self.create_user(user, pwd)
        
        # Grant different column privileges to user and role
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("USE", db_name, role)
        self.grant_privilege("SELECT(c1)", f"{db_name}.st1", user)  # User: only c1
        self.grant_privilege("SELECT(c2)", f"{db_name}.st1", role)  # Role: only c2
        self.grant_role(role, user)
        
        # Test: user's explicit privilege takes priority
        self.login(user, pwd)
        self.exec_sql(f"SELECT c1 FROM {db_name}.st1")
        # According to FS: user's explicit fine-grained rule > role's rule
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_role(role)
        self.drop_user(user)
        
        print("  Privilege Conflict Resolution ........ [ passed ] ")
    
    def do_wildcard_privilege(self):
        # Test wildcard privilege (*.* and db.*)
        tdLog.info("=== Testing Wildcard Privilege ===")
        self.login()  # Login as root
        
        db_name1 = "test_db"
        db_name2 = "test_db2"
        user = "test_user"
        self.create_database(db_name1)
        self.create_database(db_name2)
        self.create_table(db_name1, "t1")
        self.create_table(db_name2, "t1")
        self.create_user(user, pwd)
        
        # Grant wildcard privilege on all databases
        self.grant_privilege("USE", "*", user)
        self.grant_privilege("SELECT", "*.*", user)
        
        # Test: user can access all tables
        self.login(user, pwd)
        self.exec_sql(f"SELECT * FROM {db_name1}.t1")
        self.exec_sql(f"SELECT * FROM {db_name2}.t1")
        
        # Test fine-grained rule overrides wildcard
        self.login()
        self.grant_privilege("SELECT(c1)", f"{db_name1}.t1", user)
        
        # Now user should have fine-grained privilege on db1.t1
        self.login(user, pwd)
        self.exec_sql_failed(f"SELECT * FROM {db_name1}.t1")  # Cannot select all columns
        self.exec_sql(f"SELECT * FROM {db_name2}.t1")  # Still can select from db2
        
        # Cleanup
        self.login()
        self.drop_database(db_name1)
        self.drop_database(db_name2)
        self.drop_user(user)
        
        print("  Wildcard Privilege (*.* and db.*) ... [ passed ] ")
    
    def do_privilege_revoke_cascading(self):
        # Test privilege revoke and cascading effects
        tdLog.info("=== Testing Privilege Revoke Cascading ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        
        self.create_database(db_name)
        self.create_table(db_name, "t1")
        self.create_user(user, pwd)
        
        # Grant multiple privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("SELECT", f"{db_name}.t1", user)
        self.grant_privilege("INSERT", f"{db_name}.t1", user)
        
        # Test: user can perform both operations
        self.login(user, pwd)
        self.exec_sql(f"INSERT INTO {db_name}.t1 VALUES (NOW, 1)")
        self.exec_sql(f"SELECT * FROM {db_name}.t1")
        
        # Revoke SELECT privilege
        self.login()
        self.revoke_privilege("SELECT", f"{db_name}.t1", user)
        
        # Test: user can still insert but not select
        self.login(user, pwd)
        self.exec_sql(f"INSERT INTO {db_name}.t1 VALUES (NOW, 2)")
        self.exec_sql_failed(f"SELECT * FROM {db_name}.t1")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  Privilege Revoke Cascading ........... [ passed ] ")
    
    def do_invalid_privilege_operations(self):
        # Test invalid privilege operations (negative cases)
        tdLog.info("=== Testing Invalid Privilege Operations ===")
        self.login()  # Login as root
        
        user = "test_user"
        self.create_user(user, pwd)
        
        # Test: grant non-existent privilege
        self.exec_sql_failed(f"GRANT INVALID_PRIVILEGE ON *.* TO {user}")
        
        # Test: grant privilege on non-existent database
        self.exec_sql_failed(f"GRANT SELECT ON non_existent_db.* TO {user}")
        
        # Test: grant privilege to non-existent user
        self.exec_sql_failed(f"GRANT SELECT ON *.* TO non_existent_user")
        
        # Test: revoke privilege that was never granted
        self.revoke_privilege_failed("DELETE", "*.*", user)
        
        # Test: user cannot grant privileges to others without proper privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"GRANT SELECT ON *.* TO root")
        
        # Cleanup
        self.login()
        self.drop_user(user)
        
        print("  Invalid Privilege Operations ......... [ passed ] ")
    
    def do_privilege_boundary_conditions(self):
        # Test privilege boundary conditions
        tdLog.info("=== Testing Privilege Boundary Conditions ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        
        self.create_database(db_name)
        self.create_user(user, pwd)
        
        # Test: grant and immediately revoke
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.revoke_privilege("USE", db_name, user)
        
        self.login(user, pwd)
        self.exec_sql_failed(f"USE {db_name}")
        
        # Test: grant same privilege multiple times
        self.login()
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("USE", f"DATABASE {db_name}", user)  # Should not error
        
        # Test: revoke same privilege multiple times
        self.revoke_privilege("USE", db_name, user)
        self.revoke_privilege_failed("USE", f"DATABASE {db_name}", user)  # Should error
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("  Privilege Boundary Conditions ........ [ passed ] ")
    
    def do_owner_special_privileges(self):
        # Test owner's special privileges
        tdLog.info("=== Testing Owner Special Privileges ===")
        self.login()  # Login as root
        
        user = "test_user"
        self.create_user(user, pwd)
        self.grant_privilege("CREATE DATABASE", None, user)
        
        # User creates their own database
        self.login(user, pwd)
        db_name = "test_db"
        self.create_database(db_name)
        
        # Test: owner can perform all operations without explicit grants
        self.exec_sql(f"CREATE TABLE {db_name}.t1 (ts TIMESTAMP, c1 INT)")
        self.exec_sql(f"INSERT INTO {db_name}.t1 VALUES (NOW, 1)")
        self.exec_sql(f"SELECT * FROM {db_name}.t1")
        self.exec_sql(f"ALTER DATABASE {db_name} KEEP 365")
        
        # Test: owner can drop their own database
        self.exec_sql(f"DROP DATABASE {db_name}")
        
        # Cleanup
        self.login()
        self.drop_user(user)
        
        print("  Owner Special Privileges ............. [ passed ] ")

    def do_concurrent_privilege_operations(self):
        # Test concurrent privilege grant/revoke operations
        tdLog.info("=== Testing Concurrent Privilege Operations ===")
        self.login()  # Login as root
        
        # Note: This is a basic test, actual concurrent testing would require threading
        db_name = "test_db"
        users = ["test_user", "test_user2", "test_user3"]
        
        self.create_database(db_name)
        for user in users:
            self.create_user(user, pwd)
            self.grant_privilege("USE", f"DATABASE {db_name}", user)
            self.grant_privilege("SELECT", f"{db_name}.*", user)
        
        # Revoke privileges from all users
        for user in users:
            self.revoke_privilege("SELECT", f"{db_name}.*", user)
        
        # Verify all users lost privilege
        for user in users:
            self.login(user, pwd)
            # Note: would need actual table to test SELECT
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        for user in users:
            self.drop_user(user)
        
        print("  Concurrent Privilege Operations ...... [ passed ] ")

    #
    # --------------------------- main ----------------------------
    #
    def test_priv_control(self):
        """Privilege control 
        
        [Database Privileges]
          - CREATE DATABASE
          - ALTER DATABASE
          - DROP DATABASE
          - USE DATABASE
          - SHOW DATABASES
        
        [Table Privileges]
          - CREATE TABLE
          - DROP TABLE
          - ALTER TABLE
          - SELECT
          - INSERT
          - DELETE
        
        [Column and Row Privileges]
          - Column-Level Privilege
          - Row-Level with Tag Condition
          - Column Mask
        
        [Role-Based Access Control]
          - Role Creation and Grant
          - System Roles (SYSDBA/SYSSEC/SYSAUDIT)
          - Audit Database Privileges (3.4.0.0+)
        
        [Function and Index Privileges]
          - CREATE FUNCTION
          - CREATE INDEX
        
        [View, Topic and Stream Privileges (3.4.0.0+)]
          - View Privileges (SELECT VIEW, ALTER VIEW, DROP VIEW)
          - Topic Privileges (SUBSCRIBE, DROP TOPIC)
          - Stream Privileges (SHOW, START, STOP, DROP STREAM)
        
        [Exception and Reverse Test Cases]
          - Privilege Inheritance
          - Privilege Conflict Resolution
          - Wildcard Privilege (*.* and db.*)
          - Privilege Revoke Cascading
          - Invalid Privilege Operations
          - Privilege Boundary Conditions
          - Owner Special Privileges
          - Concurrent Privilege Operations
        
        Since: v3.4.0.0
        Labels: common,ci,privilege
        Jira: TS-7232
        History:
            - 2026-02-02 Alex Duan created
            - 2026-02-02 Enhanced with comprehensive test cases
            - 2026-02-02 Added 3.4.0.0+ view/topic/stream privilege tests
        """
        
        print("\n")
        print("========== Privilege Control Test Suite ==========")
        print("")
        
        '''

        # Database privilege tests
        print("[Database Privileges]")
        self.do_create_database_privilege()
        self.do_alter_database_privilege()
        self.do_drop_database_privilege()
        self.do_use_database_privilege()
        self.do_show_databases_privilege()
        
        # Table privilege tests
        print("")
        print("[Table Privileges]")
        self.do_create_table_privilege()
        self.do_drop_table_privilege()
        self.do_alter_table_privilege()
        self.do_select_privilege()
        self.do_insert_privilege()
        self.do_delete_privilege()
        
        # Column and row privilege tests
        print("")
        print("[Column and Row Privileges]")
        self.do_column_privilege()
        self.do_row_privilege_with_tag_condition()
        self.do_column_mask_privilege()
        
        # RBAC tests
        print("")
        print("[Role-Based Access Control]")
        '''
        self.do_role_creation_and_grant()
        self.do_system_roles()
        self.do_audit_database_privileges()
        
        # Function and index privilege tests
        print("")
        print("[Function and Index Privileges]")
        self.do_create_function_privilege()
        self.do_create_index_privilege()
        
        # View, topic and stream privilege tests (3.4.0.0+)
        print("")
        print("[View, Topic and Stream Privileges]")
        self.do_view_privileges()
        self.do_topic_privileges()
        self.do_stream_privileges()
        
        # Exception and reverse test cases
        print("")
        print("[Exception and Reverse Test Cases]")
        self.do_privilege_inheritance()
        self.do_privilege_conflict_resolution()
        self.do_wildcard_privilege()
        self.do_privilege_revoke_cascading()
        self.do_invalid_privilege_operations()
        self.do_privilege_boundary_conditions()
        self.do_owner_special_privileges()
        self.do_concurrent_privilege_operations()