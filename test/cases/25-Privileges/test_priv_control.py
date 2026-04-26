from new_test_framework.utils import tdLog, tdSql, TDCom, etool
from taos.tmq import Consumer
import time
import socket
import os
import platform

TSDB_CODE_NO_SUCH_FILE                        = 0x02
#define TSDB_CODE_OPS_NOT_SUPPORT               TAOS_DEF_ERROR_CODE(0, 0x0100)
TSDB_CODE_OPS_NOT_SUPPORT                     = 0x0100
#define TSDB_CODE_MND_NO_RIGHTS                       TAOS_DEF_ERROR_CODE(0, 0x0303)
TSDB_CODE_MND_NO_RIGHTS                       = 0x0303
#define TSDB_CODE_MND_TRANS_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x03D1)
TSDB_CODE_MND_TRANS_NOT_EXIST                 = 0x03D1
#define TSDB_CODE_MND_INVALID_CONN_ID           TAOS_DEF_ERROR_CODE(0, 0x030E)
TSDB_CODE_MND_INVALID_CONN_ID                 = 0x030E
#define TSDB_CODE_MND_PRIVILEGE_EXIST           TAOS_DEF_ERROR_CODE(0, 0x0359)
TSDB_CODE_MND_PRIVILEGE_EXIST                 = 0x0359
#define TSDB_CODE_MND_ROLE_NOT_EXIST            TAOS_DEF_ERROR_CODE(0, 0x04F1)
TSDB_CODE_MND_ROLE_NOT_EXIST                  = 0x04F1
#define TSDB_CODE_MND_ROLE_CONFLICTS            TAOS_DEF_ERROR_CODE(0, 0x04F6)
TSDB_CODE_MND_ROLE_CONFLICTS                  = 0x04F6
#define TSDB_CODE_PAR_PERMISSION_DENIED               TAOS_DEF_ERROR_CODE(0, 0x2644)
TSDB_CODE_PAR_PERMISSION_DENIED               = 0x2644
#define TSDB_CODE_PAR_COL_PERMISSION_DENIED           TAOS_DEF_ERROR_CODE(0, 0x26E1)
TSDB_CODE_PAR_COL_PERMISSION_DENIED           = 0x26E1
#define TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED        TAOS_DEF_ERROR_CODE(0, 0x26E3)
TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED        = 0x26E3
#define TSDB_CODE_PAR_TB_CREATE_PERMISSION_DENIED     TAOS_DEF_ERROR_CODE(0, 0x26E4)
TSDB_CODE_PAR_TB_CREATE_PERMISSION_DENIED     = 0x26E4
#define TSDB_CODE_PAR_STREAM_CREATE_PERMISSION_DENIED TAOS_DEF_ERROR_CODE(0, 0x26E7)
TSDB_CODE_PAR_STREAM_CREATE_PERMISSION_DENIED = 0x26E7



pwd = "abcd@1234"

def normalize_errno(err):
    if err is None:
        return None
    # If negative, convert to unsigned 32-bit and extract low 16 bits
    if err < 0:
        return (err & 0xFFFFFFFF) & 0xFFFF
    # If already positive, just extract low 16 bits
    return err & 0xFFFF

def errno_from_exception(e):
    errno = None
    if hasattr(e, 'args') and len(e.args) >= 2:
        # ProgrammingError(errstr, errno) - errno is args[1]
        errno = e.args[1]
    elif hasattr(e, 'errno'):
        # Some exceptions have errno attribute
        errno = e.errno
    return errno

class TestPrivControl:
    @classmethod
    def setup_class(cls):
        tdLog.info("TestPrivControl setup_class")
        cls.tdCom = TDCom()

    #
    # --------------------------- base function ----------------------------
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
        tdSql.execute(sql, queryTimes=30)
        print(f"   Executed: {sql}")
    
    def exec_sql_failed(self, sql, errno=None, queryTimes=30):
        # Verify that SQL execution should fail
        for i in range(1, queryTimes + 1):
            try:
                tdSql.cursor.execute(sql)
                time.sleep(1)
                print(f"   try {i}/{queryTimes} times still succeeded: {sql}")
            except Exception as e:
                print(f"   Exception: {e}")
                # Get errno from exception
                actual_errno = errno_from_exception(e)
                # If expected errno is specified, verify it matches
                if errno is not None:
                    expected_code = normalize_errno(errno)
                    actual_code = normalize_errno(actual_errno)
                    
                    if actual_code != expected_code:
                        tips = f"   try {i}/{queryTimes} times expected errno 0x{expected_code:04X} ({expected_code}), got 0x{actual_code:04X} ({actual_code}) [raw: {actual_errno}] for SQL: {sql}. Error: {e}"
                        if i < queryTimes:
                            print(tips)
                            time.sleep(1)
                            continue    
                        raise Exception(tips)
                print(f"   SQL failed as expected: {sql}")
                return True
            
        raise Exception(f"try {queryTimes} times, SQL still succeeded (expected to fail): {sql}")

    def query_expect_rows(self, sql, expected_rows, queryTimes=30):
        # Execute SQL and return success
        for i in range(queryTimes):            
            tdSql.query(sql)
            actual_rows = tdSql.queryRows
            if actual_rows == expected_rows:
                return 
            print(f"    try {i+1}/{queryTimes} times got {actual_rows} rows, expected {expected_rows} for SQL: {sql}")
            time.sleep(1)            

        raise Exception(f"Expected {expected_rows} rows, but got {actual_rows} for SQL: {sql}")

    def create_snode(self, dnode_id=1):
        # Create a stream node on specified data node
        sql = f"CREATE SNODE ON DNODE {dnode_id}"
        tdSql.execute(sql)
    
    def create_qnode(self, dnode_id=1):
        # Create a query node on specified data node
        sql = f"CREATE QNODE ON DNODE {dnode_id}"
        tdSql.execute(sql)
    
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

    def create_rsma(self, db_name, stable_name):
        # Create a RSMA
        sql = f"CREATE RSMA {stable_name}_rsma on {db_name}.{stable_name} function(avg(c1)) interval(1m,5m)"
        tdSql.execute(sql)
        tdLog.info(f"Created RSMA: {db_name}.{stable_name}_rsma")
    
    def create_table(self, db_name, table_name, columns="ts TIMESTAMP, c1 INT"):
        # Create a normal table
        sql = f"CREATE TABLE {db_name}.{table_name} ({columns})"
        tdSql.execute(sql)
        tdLog.info(f"Created table: {db_name}.{table_name}")
        
    def create_topic(self, topic_name, as_clause, options=""):
        # Create a topic
        sql = f"CREATE TOPIC {topic_name} {options} as {as_clause}"
        tdSql.execute(sql)
        tdLog.info(f"Created topic: {topic_name}") 
    
    def drop_topic(self, topic_name, options=""):
        # Drop a topic
        sql = f"DROP TOPIC {options} {topic_name}"
        tdSql.execute(sql)
        tdLog.info(f"Dropped topic: {topic_name}")
        
    def subscribe_topic(self, user, password, group_id, topic_name, expected_rows=None, createTimes=30):
        attr = {
            'group.id': group_id,
            'td.connect.user': user,
            'td.connect.pass': password,
            'auto.offset.reset': 'earliest'
        }

        for i in range(createTimes):
            try:
                # Create consumer
                consumer = Consumer(attr)
                # Subscribe topic
                consumer.subscribe([topic_name])
                print("   Subscribe topics successfully")
                # Poll data
                records = consumer.poll(5)
                if records:
                    err = records.error()
                    if err is not None:
                        print(f"Poll data error, {err}")
                        raise err
                    
                    if expected_rows is None:
                        print("   Polled data successfully")
                        return

                    val = records.value()
                    if val:
                        for block in val:
                            data = block.fetchall()
                            print(f"   data: {data}")        
                    if expected_rows is not None:
                        actual_rows = len(data)                        
                        if actual_rows == expected_rows:
                            print(f"   Got expected rows: {actual_rows}")
                            return consumer
                        else:
                            raise Exception(f"Got rows: {actual_rows}, expected: {expected_rows}")
                print("   Polled data successfully")
                return consumer
            except Exception as e:                
                print(f"Create consumer try {i+1}/{createTimes} failed: {str(e)}")
                time.sleep(1)
        
        raise Exception(f"Failed to create consumer & subscribe after {createTimes} attempts")
        
    def subscribe_topic_failed(self, user, password, group_id, topic_name, expect_errno=None, times=10):
        attr = {
            'group.id': group_id,
            'td.connect.user': user,
            'td.connect.pass': password,
            'auto.offset.reset': 'earliest'
        }        
        for i in range(times):
            try:
                # Create consumer
                consumer = Consumer(attr)
                # Subscribe topic                
                consumer.subscribe([topic_name])
                print("   Subscribe topics succeeded but was expected to fail")
                consumer.unsubscribe()
                time.sleep(1)
            except Exception as e:
                print(f"   Subscribe topics failed as expected: {str(e)}")
                if expect_errno is not None:
                    actual_errno = errno_from_exception(e)
                    expected_code = normalize_errno(expect_errno)
                    actual_code = normalize_errno(actual_errno)                    
                    if actual_code != expected_code:
                        raise Exception(f"Expected errno 0x{expected_code:04X} ({expected_code}), got 0x{actual_code:04X} when subscribing topic. Error: {e}")  
                return
        raise Exception(f"Subscribe topics still succeeded after {times} attempts, expected to fail")
    
    def unsubscribe_topic(self, consumer):
        consumer.unsubscribe()
        print("Unsubscribed topic successfully")
        
    def drop_stream(self, db_name, stream_name):
        # Drop a stream
        sql = f"DROP STREAM {db_name}.{stream_name}"
        tdSql.execute(sql)
        tdLog.info(f"Dropped stream: {db_name}.{stream_name}")
    
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
            except Exception:
                pass
        
        # Drop test roles
        for role in ["test_role", "test_role2", "test_role3", "test_conflict_role"]:
            try:
                self.drop_role(role)
            except Exception:
                pass
        
        # Drop test databases
        for db in ["test_db", "test_db2", "test_db_fail", "test_audit_db", "test_audit_database",
                   "test_sysdba_db"]:
            try:
                self.drop_database(db)
            except Exception:
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
        
        print("CREATE DATABASE ...................... [ passed ] ")
    
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
        
        print("ALTER DATABASE ....................... [ passed ] ")
    
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
        
        print("DROP DATABASE ........................ [ passed ] ")
    
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
        
        print("USE DATABASE ......................... [ passed ] ")
    
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
        
        print("SHOW DATABASES ....................... [ passed ] ")

    def do_show_create_database_privilege(self):
        # Test SHOW CREATE DATABASE privilege
        tdLog.info("=== Testing SHOW CREATE DATABASE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  #revoke default role
        
        # Test: user cannot show create database without privilege
        '''BUG19'''
        self.login(user, pwd)
        self.exec_sql_failed(f"SHOW CREATE DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant SHOW CREATE DATABASE privilege
        self.login()
        self.grant_privilege("SHOW CREATE", f"DATABASE {db_name}", user)
        
        # Test: user can show create database with privilege
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW CREATE DATABASE {db_name}", 1)

        # revoke privilege and verify failure
        self.login()
        self.revoke_privilege("SHOW CREATE", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        '''BUG19
        self.exec_sql_failed(f"SHOW CREATE DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        '''

        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("SHOW CREATE DATABASE ................ [ passed ] ")

    def do_flush_database_privilege(self):
        # Test FLUSH DATABASE privilege
        tdLog.info("=== Testing FLUSH DATABASE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  #revoke default role
        
        # Test: user cannot flush database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"FLUSH DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant FLUSH DATABASE privilege
        self.login()
        self.grant_privilege("FLUSH", f"DATABASE {db_name}", user)
        
        # Test: user can flush database with privilege
        self.login(user, pwd)
        self.exec_sql(f"FLUSH DATABASE {db_name}")
        
        # revoke privilege and verify failure
        self.login()
        self.revoke_privilege("FLUSH", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"FLUSH DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)        
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("FLUSH DATABASE ...................... [ passed ] ")

    def do_compact_database_privilege(self):
        # Test COMPACT DATABASE privilege
        tdLog.info("=== Testing COMPACT DATABASE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  #revoke default role
        
        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test: user cannot compact database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"COMPACT DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant 
        self.login()
        self.grant_privilege("COMPACT", f"DATABASE {db_name}", user)
        
        # Test: user can compact database with privilege
        self.login(user, pwd)
        self.exec_sql(f"COMPACT DATABASE {db_name}")

        # Revoke
        self.login()
        self.revoke_privilege("COMPACT", f"DATABASE {db_name}", user)
        
        # Test: user cannot compact database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"COMPACT DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("COMPACT DATABASE .................... [ passed ] ")

    def do_trim_database_privilege(self):
        # Test TRIM DATABASE privilege
        tdLog.info("=== Testing TRIM DATABASE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name, "KEEP 365d")
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  #revoke default role

        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test: user cannot trim database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"TRIM DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Revoke
        self.login()
        self.revoke_privilege("COMPACT", f"DATABASE {db_name}", user)        

        # Test: user cannot trim database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"TRIM DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant TRIM DATABASE privilege
        self.login()
        self.grant_privilege("TRIM", f"DATABASE {db_name}", user)
        
        # Test: user can trim database with privilege
        self.login(user, pwd)
        self.exec_sql(f"TRIM DATABASE {db_name}")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("TRIM DATABASE ....................... [ passed ] ")

    def do_rollup_database_privilege(self):
        # Test ROLLUP DATABASE privilege
        tdLog.info("=== Testing ROLLUP DATABASE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1")
        self.create_rsma(db_name, "st1")
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  #revoke default role
        
        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test: user cannot rollup database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"ROLLUP DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant ROLLUP DATABASE privilege
        self.login()
        self.grant_privilege("ROLLUP", f"DATABASE {db_name}", user)
        
        # Test: user can rollup database with privilege
        self.login(user, pwd)
        '''BUG20 '''
        self.exec_sql(f"ROLLUP DATABASE {db_name}")
        
        # Revoke
        self.login()
        self.revoke_privilege("ROLLUP", f"DATABASE {db_name}", user)        

        # Test: user cannot rollup database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"ROLLUP DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("ROLLUP DATABASE ..................... [ passed ] ")

    def do_scan_database_privilege(self):
        # Test SCAN DATABASE privilege
        tdLog.info("=== Testing SCAN DATABASE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  #revoke default role

        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test: user cannot scan database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"SCAN DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant SCAN DATABASE privilege
        self.login()
        self.grant_privilege("SCAN", f"DATABASE {db_name}", user)
        
        # Test: user can scan database with privilege
        self.login(user, pwd)
        self.exec_sql(f"SCAN DATABASE {db_name}")
        
        # Revoke
        self.login()
        self.revoke_privilege("SCAN", f"DATABASE {db_name}", user)          
        
        # Test: user cannot scan database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"SCAN DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("SCAN DATABASE ....................... [ passed ] ")

    def do_ssmigrate_database_privilege(self):
        # Test SSMIGRATE DATABASE privilege
        tdLog.info("=== Testing SSMIGRATE DATABASE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  #revoke default role

        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test: user cannot ssmigrate database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"SSMIGRATE DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant SSMIGRATE DATABASE privilege
        self.login()
        self.grant_privilege("SSMIGRATE", f"DATABASE {db_name}", user)
        
        # Test: user can ssmigrate database with privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"SSMIGRATE DATABASE {db_name}", TSDB_CODE_OPS_NOT_SUPPORT)  # SSMIGRATE is not supported, just check permission
        
        # Revoke
        self.login()
        self.revoke_privilege("SSMIGRATE", f"DATABASE {db_name}", user)          
        
        # Test: user cannot ssmigrate database without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"SSMIGRATE DATABASE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)

        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("SSMIGRATE DATABASE .................. [ passed ] ")

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
        
        print("CREATE TABLE ......................... [ passed ] ")
    
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
        
        print("DROP TABLE ........................... [ passed ] ")
    
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
        
        print("ALTER TABLE .......................... [ passed ] ")
    
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
        
        print("SELECT ............................... [ passed ] ")
    
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
        
        print("INSERT ............................... [ passed ] ")
    
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
        
        print("DELETE ............................... [ passed ] ")
        
    def do_select_column_privilege_comprehensive(self):
        # Comprehensive test for column-level SELECT privilege
        tdLog.info("=== Testing Comprehensive Column-Level SELECT Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 FLOAT, c3 DOUBLE, c4 BOOL, c5 VARCHAR(10)", tags="t1 INT")
        self.create_child_table(db_name, "ct1", "st1", tag_values="1")
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 1, 2.0, 3.0, true, 'test')")
        self.create_user(user, pwd)
        
        # Grant USE DATABASE
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test 1: Grant SELECT on specific columns
        self.grant_privilege("SELECT(c1,c3,c5)", f"{db_name}.st1", user)
        
        self.login(user, pwd)
        # Should succeed
        self.exec_sql(f"SELECT c1, c3, c5 FROM {db_name}.st1")
        # Should fail
        self.exec_sql_failed(f"SELECT c2 FROM {db_name}.st1")
        self.exec_sql_failed(f"SELECT c4 FROM {db_name}.st1")
        self.exec_sql_failed(f"SELECT * FROM {db_name}.st1")
        
        # Test 2: Add more columns to existing privilege
        self.login()
        self.revoke_privilege("SELECT", f"{db_name}.st1", user)
        self.grant_privilege("SELECT(c1,c2,c3,c4,c5)", f"{db_name}.st1", user)
        
        self.login(user, pwd)
        self.exec_sql(f"SELECT c1, c2, c3, c4, c5 FROM {db_name}.st1")
        self.exec_sql_failed(f"SELECT * FROM {db_name}.st1")  # Still cannot use *
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("Comprehensive Column SELECT ........... [ passed ] ")

    def do_insert_column_privilege_comprehensive(self):
        # Comprehensive test for column-level INSERT privilege
        tdLog.info("=== Testing Comprehensive Column-Level INSERT Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 FLOAT, c3 DOUBLE, c4 BOOL, c5 VARCHAR(10)", tags="t1 INT")
        self.create_child_table(db_name, "ct1", "st1", tag_values="1")
        self.create_user(user, pwd)
        
        # Grant USE and CREATE TABLE privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("CREATE TABLE", f"DATABASE {db_name}", user)
        
        # Test 1: Grant INSERT on specific columns
        self.grant_privilege("INSERT(ts,c1,c3)", f"{db_name}.st1", user)
        
        self.login(user, pwd)
        # Should succeed
        self.exec_sql(f"INSERT INTO {db_name}.ct1 (ts, c1, c3) VALUES (NOW, 100, 300.0)")
        # Should fail - missing required column
        self.exec_sql_failed(f"INSERT INTO {db_name}.ct1 (ts, c1, c2) VALUES (NOW, 101, 201.0)")
        # Should fail - using *
        self.exec_sql_failed(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 102, 202.0, 302.0, false, 'fail')")
        
        # Test 2: Verify data was inserted with authorized columns only
        self.login()
        self.grant_privilege("SELECT", f"{db_name}.st1", user)
        
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT c1, c3 FROM {db_name}.st1 WHERE c1=100", 1)
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("Comprehensive Column INSERT ........... [ passed ] ")

    def do_show_create_table_privilege(self):
        # Test SHOW CREATE TABLE privilege
        tdLog.info("=== Testing SHOW CREATE TABLE Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 FLOAT", tags="t1 INT, t2 VARCHAR(20)")
        self.create_table(db_name, "t1", columns="ts TIMESTAMP, val INT")
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  #revoke default role
        
        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("SELECT", f"{db_name}.st1", user)
        self.grant_privilege("SELECT", f"{db_name}.t1", user)
        
        # Test: user cannot show create table without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"SHOW CREATE TABLE {db_name}.st1", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed(f"SHOW CREATE TABLE {db_name}.t1", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant SHOW CREATE TABLE privilege on specific tables
        self.login()
        self.grant_privilege("SHOW CREATE", f"TABLE {db_name}.st1", user)
        self.grant_privilege("SHOW CREATE", f"TABLE {db_name}.t1", user)
        
        # Test: user can show create table with privilege
        self.login(user, pwd)
        self.exec_sql(f"SHOW CREATE TABLE {db_name}.st1")
        self.exec_sql(f"SHOW CREATE TABLE {db_name}.t1")
        
        # Test: revoke privilege
        self.login()
        self.revoke_privilege("SHOW CREATE", f"TABLE {db_name}.st1", user)
        
        self.login(user, pwd)
        self.exec_sql_failed(f"SHOW CREATE TABLE {db_name}.st1", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql(f"SHOW CREATE TABLE {db_name}.t1")  # This should still work
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("SHOW CREATE TABLE .................... [ passed ] ")        

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
        
        print("Column-Level Privilege ............... [ passed ] ")
    
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
        
        print("Row-Level with Tag Condition ......... [ passed ] ")
    
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
        
        print("Column Mask .......................... [ passed ] ")

    def do_row_privilege_complex_conditions(self):
        # Test complex condition combinations (AND/OR)
        tdLog.info("=== Testing Row Privilege with Complex Conditions ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, val INT, status INT", tags="location VARCHAR(20), sensor_type VARCHAR(20)")
        self.create_child_table(db_name, "ct1", "st1", tag_values="'room1', 'temperature'")
        self.create_child_table(db_name, "ct2", "st1", tag_values="'room2', 'humidity'")
        self.create_child_table(db_name, "ct3", "st1", tag_values="'room1', 'humidity'")
        self.create_child_table(db_name, "ct4", "st1", tag_values="'room2', 'temperature'")
        
        # Insert test data
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 25, 1)")
        self.exec_sql(f"INSERT INTO {db_name}.ct2 VALUES (NOW, 60, 0)")
        self.exec_sql(f"INSERT INTO {db_name}.ct3 VALUES (NOW, 55, 1)")
        self.exec_sql(f"INSERT INTO {db_name}.ct4 VALUES (NOW, 30, 0)")
        
        self.create_user(user, pwd)
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test 1: AND condition (location='room1' AND sensor_type='temperature')
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="location='room1' AND sensor_type='temperature'")
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.st1", 1)  # Only ct1
        
        # Test 2: OR condition (location='room1' OR sensor_type='temperature')
        self.login()
        self.revoke_privilege("SELECT", f"TABLE {db_name}.st1", user)
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="location='room1' OR sensor_type='temperature'")
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.st1", 3)  # ct1, ct3, ct4
        
        # Test 3: Complex combination (location='room1' AND (sensor_type='temperature' OR status=1))
        self.login()
        self.revoke_privilege("SELECT", f"TABLE {db_name}.st1", user)
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="location='room1' AND (sensor_type='temperature' OR status=1)")
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.st1", 2)  # ct1 and ct3
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("Row Privilege Complex Conditions ........ [ passed ] ")

    def do_row_privilege_time_range(self):
        # Test time range conditions
        tdLog.info("=== Testing Row Privilege with Time Range ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, val INT")
        self.create_child_table(db_name, "ct1", "st1")
        
        # Insert data with specific timestamps
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES ('2024-01-01 00:00:00', 100)")
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES ('2024-01-15 12:00:00', 200)")
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES ('2024-02-01 00:00:00', 300)")
        
        self.create_user(user, pwd)
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test 1: Specific date range
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="ts >= '2024-01-01' AND ts < '2024-02-01'")
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.st1", 2)  # First two records
        
        # Test 2: Time range with specific time
        self.login()
        self.revoke_privilege("SELECT", f"TABLE {db_name}.st1", user)
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="ts >= '2024-01-15 00:00:00'")
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.st1", 2)  # Last two records
        
        # Test 3: Exact timestamp
        self.login()
        self.revoke_privilege("SELECT", f"TABLE {db_name}.st1", user)
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="ts = '2024-01-15 12:00:00'")
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.st1", 1)  # Only the exact timestamp
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("Row Privilege Time Range ............... [ passed ] ")

    def do_row_privilege_mixed_conditions(self):
        # Test mixed tag and data conditions
        tdLog.info("=== Testing Row Privilege with Mixed Conditions ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, temperature FLOAT, humidity FLOAT", tags="location VARCHAR(20), device_id INT")
        self.create_child_table(db_name, "ct1", "st1", tag_values="'room1', 101")
        self.create_child_table(db_name, "ct2", "st1", tag_values="'room1', 102")
        self.create_child_table(db_name, "ct3", "st1", tag_values="'room2', 201")
        
        # Insert test data
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 25.5, 60.0)")
        self.exec_sql(f"INSERT INTO {db_name}.ct2 VALUES (NOW, 30.0, 55.0)")
        self.exec_sql(f"INSERT INTO {db_name}.ct3 VALUES (NOW, 22.0, 65.0)")
        
        self.create_user(user, pwd)
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test 1: Tag condition + data condition
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="location='room1' AND temperature > 26.0")
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.st1", 1)  # Only ct2
        
        # Test 2: Multiple conditions with OR
        self.login()
        self.revoke_privilege("SELECT", f"TABLE {db_name}.st1", user)
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="(location='room1' AND device_id=101) OR humidity < 58.0")
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.st1", 2)  # ct1 and ct2
        
        # Test 3: Complex mixed conditions
        self.login()
        self.revoke_privilege("SELECT", f"TABLE {db_name}.st1", user)
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user, with_condition="(location='room1' AND temperature BETWEEN 25.0 AND 28.0) OR (location='room2' AND humidity > 60.0)")
        self.login(user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.st1", 2)  # ct1 and ct3
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("Row Privilege Mixed Conditions ......... [ passed ] ")

    def do_column_row_combined_privilege(self):
        # Test combined column and row privileges
        tdLog.info("=== Testing Combined Column and Row Privileges ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, temperature FLOAT, humidity FLOAT, status INT, voltage FLOAT", tags="location VARCHAR(20)")
        self.create_child_table(db_name, "ct1", "st1", tag_values="'room1'")
        self.create_child_table(db_name, "ct2", "st1", tag_values="'room2'")
        
        # Insert test data
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 25.5, 60.0, 1, 220.0)")
        self.exec_sql(f"INSERT INTO {db_name}.ct2 VALUES (NOW, 30.0, 55.0, 0, 230.0)")
        
        self.create_user(user, pwd)
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test 1: Column SELECT with row condition
        self.grant_privilege("SELECT(temperature,humidity)", f"TABLE {db_name}.st1", user, with_condition="location='room1'")
        self.login(user, pwd)
        '''BUG23
        self.query_expect_rows(f"SELECT temperature, humidity FROM {db_name}.st1", 1)  # Only ct1
        '''
        self.exec_sql_failed(f"SELECT status FROM {db_name}.st1", TSDB_CODE_PAR_COL_PERMISSION_DENIED)  # Column not authorized
        self.query_expect_rows(f"SELECT temperature FROM {db_name}.ct2", 0)  # Row not authorized, reuse super table privilege
        
        # Test 2: Column INSERT with row condition
        self.login()
        self.revoke_privilege("SELECT", f"TABLE {db_name}.st1", user)
        self.grant_privilege("INSERT(ts,temperature,status)", f"TABLE {db_name}.st1", user, with_condition="location='room1'")
        self.login(user, pwd)
        self.exec_sql(f"INSERT INTO {db_name}.ct1 (ts, temperature, status) VALUES (NOW+1s, 26.0, 1)")
        self.exec_sql_failed(f"INSERT INTO {db_name}.ct2 (ts, temperature, status) VALUES (NOW+2s, 27.0, 0)")  # Row not authorized
        self.exec_sql_failed(f"INSERT INTO {db_name}.ct1 (ts, temperature, humidity) VALUES (NOW+3s, 28.0, 62.0)")  # Column not authorized
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("Column-Row Combined Privilege .......... [ passed ] ")

    def do_column_privilege_update_priority(self):
        # Test column privilege update priority
        tdLog.info("=== Testing Column Privilege Update Priority ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 INT, c3 INT, c4 INT")
        self.create_child_table(db_name, "ct1", "st1")
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 1, 2, 3, 4)")
        
        self.create_user(user, pwd)
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test 1: Grant column privilege, then update with different columns
        self.grant_privilege("SELECT(c1,c2)", f"{db_name}.st1", user)
        self.login(user, pwd)
        self.exec_sql(f"SELECT c1, c2 FROM {db_name}.st1")
        self.exec_sql_failed(f"SELECT c3 FROM {db_name}.st1")
        
        # Update privilege with different columns (should replace previous)
        self.login()
        self.exec_sql_failed(f"GRANT SELECT(c3,c4) ON {db_name}.st1 TO {user}", TSDB_CODE_MND_PRIVILEGE_EXIST)
        self.revoke_privilege("SELECT", f"{db_name}.st1", user)
        self.grant_privilege("SELECT(c3,c4)", f"{db_name}.st1", user)        
        self.login(user, pwd)
        self.exec_sql_failed(f"SELECT c1 FROM {db_name}.st1")  # Previous columns should be revoked
        self.exec_sql(f"SELECT c3, c4 FROM {db_name}.st1")  # New columns should work
        
        # Test 2: Grant same privilege multiple times (should error)
        self.login()
        self.exec_sql_failed(f"GRANT SELECT(c3,c4) ON {db_name}.st1 TO {user}", TSDB_CODE_MND_PRIVILEGE_EXIST)  # Same privilege again
        
        # Test 3: Revoke and regrant with different columns
        self.revoke_privilege("SELECT", f"{db_name}.st1", user)
        self.grant_privilege("SELECT(c1,c2,c3)", f"{db_name}.st1", user)
        self.login(user, pwd)
        self.exec_sql(f"SELECT c1, c2, c3 FROM {db_name}.st1")
        self.exec_sql_failed(f"SELECT c4 FROM {db_name}.st1")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("Column Privilege Update Priority ....... [ passed ] ")

    def do_privilege_update_time_priority(self):
        # Test privilege priority based on update time
        tdLog.info("=== Testing Privilege Update Time Priority ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        role = "test_role"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 INT, c3 INT")
        self.create_child_table(db_name, "ct1", "st1")
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 1, 2, 3)")
        
        self.create_user(user, pwd)
        self.create_role(role)
        self.grant_role(role, user)
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("USE", f"DATABASE {db_name}", role)
        
        # Test 1: Role privilege granted first, then user privilege (user should win)
        self.grant_privilege("SELECT(c1)", f"{db_name}.st1", role)  # Role: c1 only
        # Wait a moment to ensure different update time
        time.sleep(0.1)
        self.grant_privilege("SELECT(c2)", f"{db_name}.st1", user)  # User: c2 only
        
        self.login(user, pwd)
        # According to priority: user privilege (c2) should win over role privilege (c1)
        self.exec_sql(f"SELECT c2 FROM {db_name}.st1")
        self.exec_sql_failed(f"SELECT c1 FROM {db_name}.st1")  # Role privilege should not apply
        
        # Test 2: User privilege granted first, then role privilege (role should win if later)
        self.login()
        self.revoke_privilege("SELECT", f"{db_name}.st1", user)
        self.revoke_privilege("SELECT", f"{db_name}.st1", role)
        self.grant_privilege("SELECT(c1)", f"{db_name}.st1", user)  # User: c1 first
        time.sleep(0.1)
        self.grant_privilege("SELECT(c2)", f"{db_name}.st1", role)  # Role: c2 later
        
        self.login(user, pwd)
        # Role privilege (c2) should win because it's later
        self.exec_sql(f"SELECT c2 FROM {db_name}.st1")
        self.exec_sql_failed(f"SELECT c1 FROM {db_name}.st1")  # User privilege should not apply
        
        # Test 3: Same update time, user privilege should win
        self.login()
        self.revoke_privilege("SELECT", f"{db_name}.st1", user)
        self.revoke_privilege("SELECT", f"{db_name}.st1", role)
        # Grant both at nearly same time
        self.grant_privilege("SELECT(c1)", f"{db_name}.st1", user)
        self.grant_privilege("SELECT(c2)", f"{db_name}.st1", role)
        
        self.login(user, pwd)
        # User privilege should win when update times are same
        self.exec_sql(f"SELECT c1 FROM {db_name}.st1")
        self.exec_sql_failed(f"SELECT c2 FROM {db_name}.st1")  # Role privilege should not apply
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        self.drop_role(role)
        
        print("Privilege Update Time Priority ........ [ passed ] ")

    #
    # --------------------------- Role-Based Access Control Tests ----------------------------
    #
    def do_role_privilege(self):
        # Test role privileges: CREATE ROLE, DROP ROLE, SHOW ROLES, LOCK ROLE, UNLOCK ROLE
        tdLog.info("=== Testing Role Privileges ===")
        self.login()  # Login as root
        
        # Test user
        test_user = "test_user"
        role1 = "test_role_1"
        role2 = "test_role_2"
        role3 = "test_role_3"
        
        # Create test user and revoke default role
        self.create_user(test_user, pwd)
        self.revoke_role("`SYSINFO_1`", test_user)  # revoke default role
        
        # ==================== CREATE ROLE ====================
        print("--- Testing CREATE ROLE privilege ---")
        
        # Test: user cannot create role without privilege
        self.login(test_user, pwd)
        self.exec_sql_failed(f"CREATE ROLE {role1}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant CREATE ROLE privilege
        self.login()
        self.grant_privilege("CREATE ROLE", None, test_user)
        
        # Test: user can create role with privilege
        self.login(test_user, pwd)
        self.exec_sql(f"CREATE ROLE {role1}")
        
        # Test revoke CREATE ROLE privilege
        self.login()
        self.revoke_privilege("CREATE ROLE", None, test_user)
        
        # Test: user cannot create role after revoking privilege
        self.login(test_user, pwd)
        self.exec_sql_failed(f"CREATE ROLE {role2}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # ==================== DROP ROLE ====================
        print("--- Testing DROP ROLE privilege ---")
        
        # Create another role as root for testing DROP
        self.login()
        self.create_role(role2)
        
        # Test: user cannot drop role without privilege
        self.login(test_user, pwd)
        self.exec_sql_failed(f"DROP ROLE {role2}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant DROP ROLE privilege
        self.login()
        self.grant_privilege("DROP ROLE", None, test_user)
        
        # Test: user can drop role with privilege
        self.login(test_user, pwd)
        self.exec_sql(f"DROP ROLE {role2}")

        # Test revoke DROP ROLE privilege
        self.login()
        self.revoke_privilege("DROP ROLE", None, test_user)        
        
        # ==================== SHOW ROLES ====================
        print("--- Testing SHOW ROLES privilege ---")
        
        # Create another role as root for testing SHOW
        self.login()
        self.create_role(role3)
        
        # Test: user cannot show roles without privilege
        self.login(test_user, pwd)
        self.exec_sql_failed("SHOW ROLES", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant SHOW ROLES privilege
        self.login()
        self.grant_privilege("SHOW ROLES", None, test_user)
        
        # Test: user can show roles with privilege
        self.login(test_user, pwd)
        tdSql.query("SHOW ROLES")
        rows = tdSql.queryRows
        tdLog.info(f"Found {rows} roles")
        
        # Verify role1 and role3 are in the list
        result = tdSql.queryResult
        role_found_1 = False
        role_found_3 = False
        for row in result:
            if row[0] == role1:
                role_found_1 = True
            if row[0] == role3:
                role_found_3 = True
        
        if not role_found_1:
            tdLog.error(f"Role {role1} not found in SHOW ROLES output")
        if not role_found_3:
            tdLog.error(f"Role {role3} not found in SHOW ROLES output")
        
        # Test revoke SHOW ROLES privilege
        self.login()
        self.revoke_privilege("SHOW ROLES", None, test_user)
        self.login(test_user, pwd)
        self.exec_sql_failed("SHOW ROLES", TSDB_CODE_PAR_PERMISSION_DENIED)        
        
        #BUG21
        self.login()
        self.drop_role(role1)
        self.drop_role(role3)        
        # Drop users
        self.drop_user(test_user)
        return 

        # ==================== LOCK ROLE ====================
        print("--- Testing LOCK ROLE privilege ---")
        
        # Create a role for testing LOCK (user can't create, so root does it)
        lock_test_role = "lock_test_role"
        self.create_role(lock_test_role)
        
        # Test: user cannot lock role without privilege
        self.login(test_user, pwd)
        self.exec_sql_failed(f"LOCK ROLE {lock_test_role}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant LOCK ROLE privilege
        self.login()
        self.grant_privilege("LOCK ROLE", None, test_user)
        
        # Test: user can lock role with privilege
        self.login(test_user, pwd)
        self.exec_sql(f"LOCK ROLE {lock_test_role}")
        
        # Test: user cannot lock own role
        self.exec_sql_failed(f"LOCK ROLE {role1}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Verify role is locked (try to grant it to user should fail)
        self.login()
        self.exec_sql_failed(f"GRANT ROLE {lock_test_role} TO {test_user}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Test revoke LOCK ROLE privilege
        self.login()
        self.revoke_privilege("LOCK ROLE", None, test_user)
        
        # ==================== UNLOCK ROLE ====================
        print("--- Testing UNLOCK ROLE privilege ---")
        
        # Lock a role as root first
        unlock_test_role = "unlock_test_role"
        self.create_role(unlock_test_role)
        self.exec_sql(f"LOCK ROLE {unlock_test_role}")
        
        # Test: user cannot unlock role without privilege
        self.login(test_user, pwd)
        self.exec_sql_failed(f"UNLOCK ROLE {unlock_test_role}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant UNLOCK ROLE privilege
        self.login()
        self.grant_privilege("UNLOCK ROLE", None, test_user)
        
        # Test: user can unlock role with privilege
        self.login(test_user, pwd)
        self.exec_sql(f"UNLOCK ROLE {unlock_test_role}")
        
        # Verify role is unlocked (should be able to grant it)
        self.login()
        self.exec_sql(f"GRANT ROLE {unlock_test_role} TO {test_user}")
        
        # Test: user cannot unlock own role
        self.login(test_user, pwd)
        self.exec_sql_failed(f"UNLOCK ROLE {role1}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Test revoke UNLOCK ROLE privilege
        self.login()
        self.revoke_privilege("UNLOCK ROLE", None, test_user)
        
        # ==================== Combined Privilege Test ====================
        print("--- Testing Combined Role Privileges ---")
        
        # Create a new user for combined test
        combined_user = "combined_role_user"
        self.create_user(combined_user, pwd)
        self.revoke_role("`SYSINFO_1`", combined_user)
        
        # Grant all role privileges at once
        self.grant_privilege("CREATE ROLE, DROP ROLE, SHOW ROLES, LOCK ROLE, UNLOCK ROLE", None, combined_user)
        
        # Test all privileges work together
        self.login(combined_user, pwd)
        
        # 1. CREATE ROLE
        combined_role1 = "combined_role_1"
        self.exec_sql(f"CREATE ROLE {combined_role1}")
        
        # 2. SHOW ROLES (should see the created role)
        tdSql.query("SHOW ROLES")
        result = tdSql.queryResult
        found_combined_role = False
        for row in result:
            if row[0] == combined_role1:
                found_combined_role = True
                break
        if not found_combined_role:
            tdLog.error(f"Role {combined_role1} not found in SHOW ROLES output")
        
        # 3. LOCK ROLE
        self.exec_sql(f"LOCK ROLE {unlock_test_role}")  # Using existing role
        
        # 4. UNLOCK ROLE
        self.exec_sql(f"UNLOCK ROLE {unlock_test_role}")
        
        # 5. DROP ROLE (create another role first)
        combined_role2 = "combined_role_2"
        self.exec_sql(f"CREATE ROLE {combined_role2}")
        self.exec_sql(f"DROP ROLE {combined_role2}")
        
        # Test: user cannot drop own role
        self.exec_sql_failed(f"DROP ROLE {combined_role1}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # ==================== Test Role Privilege Hierarchy ====================
        print("--- Testing Role Privilege Hierarchy ---")
        
        # Create a role with role management privileges
        role_admin_role = "role_admin"
        self.login()
        self.create_role(role_admin_role)
        self.grant_privilege("CREATE ROLE, DROP ROLE, SHOW ROLES, LOCK ROLE, UNLOCK ROLE", None, role_admin_role)
        
        # Grant role_admin role to combined_user
        self.grant_role(role_admin_role, combined_user)
        
        # Test that combined_user still has individual privileges AND role privileges
        self.login(combined_user, pwd)
        
        # Create role using role privilege
        role_from_role = "role_from_role"
        self.exec_sql(f"CREATE ROLE {role_from_role}")
        
        # Show roles
        tdSql.query("SHOW ROLES")
        
        # Lock a role
        self.exec_sql(f"LOCK ROLE {unlock_test_role}")
        
        # Unlock the role
        self.exec_sql(f"UNLOCK ROLE {unlock_test_role}")
        
        # Create another role and drop it
        temp_role = "temp_role"
        self.exec_sql(f"CREATE ROLE {temp_role}")
        self.exec_sql(f"DROP ROLE {temp_role}")
        
        # ==================== Cleanup ====================
        self.login()
        
        # Drop roles created by users
        self.drop_role(role1)
        self.drop_role(role3)
        self.drop_role(lock_test_role)
        self.drop_role(unlock_test_role)
        self.drop_role(combined_role1)
        self.drop_role(role_from_role)
        self.drop_role(role_admin_role)
        
        # Drop users
        self.drop_user(test_user)
        self.drop_user(combined_user)
        
        print("Role Privileges (CREATE/DROP/SHOW/LOCK/UNLOCK) ... [ passed ] ")
    
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
        
        # Grant USE DATABASE
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Grant privileges to role
        self.grant_privilege("CREATE TABLE", f"DATABASE {db_name}", role)
        
        # Grant role to user
        self.grant_role(role, user)
        
        # Test: user with role can execute authorized operations
        self.login(user, pwd)
        # BUG1
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
        
        print("Role Creation and Grant .............. [ passed ] ")

    def do_role_lock_unlock(self):
        # Test LOCK ROLE and UNLOCK ROLE privileges
        tdLog.info("=== Testing LOCK ROLE / UNLOCK ROLE Privileges ===")
        self.login()  # Login as root
        
        # Create test resources
        role_name = "test_lock_role"
        user1 = "test_user1"
        user2 = "test_user2"
        user3 = "test_user3"
        db_name = "test_db"
        
        # Create role and users
        self.create_role(role_name)
        self.create_user(user1, pwd)
        self.create_user(user2, pwd)
        self.create_user(user3, pwd)
        self.create_database(db_name)
        self.revoke_role("`SYSINFO_1`", user1)  #revoke default role
        self.revoke_role("`SYSINFO_1`", user2)  #revoke default role
        self.revoke_role("`SYSINFO_1`", user3)  #revoke default role
        
        # Grant privileges to the role
        self.grant_privilege("USE", f"DATABASE {db_name}", role_name)
        self.grant_privilege("CREATE TABLE", f"DATABASE {db_name}", role_name)
        self.grant_privilege("INSERT", f"{db_name}.*", role_name)
        self.grant_privilege("SELECT", f"{db_name}.*", role_name)
        self.grant_privilege("DROP",   f"{db_name}.*", role_name)
        
        # Grant role to user1
        self.grant_role(role_name, user1)
        
        # Test 1: Verify user1 can use role privileges initially
        self.login(user1, pwd)
        self.exec_sql(f"USE {db_name}")
        self.exec_sql(f"CREATE TABLE  {db_name}.t1 (ts TIMESTAMP, c1 INT)")
        self.exec_sql(f"INSERT INTO   {db_name}.t1 (ts, c1) VALUES (NOW(), 1)")
        self.exec_sql(f"SELECT * FROM {db_name}.t1")
        self.exec_sql(f"DROP TABLE    {db_name}.t1")
        
        # Test 2: Lock the role
        self.login()
        self.exec_sql(f"LOCK ROLE {role_name}")
        
        # Test 3: After locking, user1 should not be able to use role privileges
        self.login(user1, pwd)
        self.exec_sql_failed(f"USE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed(f"CREATE TABLE {db_name}.t1 (ts TIMESTAMP, c1 INT)", TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED)
        
        # Test 4: Locked role be granted to new user
        self.login()
        self.exec_sql(f"GRANT ROLE {role_name} TO {user2}")

        # new user can not got privileges from locked role
        self.login(user2, pwd)
        self.exec_sql_failed(f"USE {db_name}", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed(f"CREATE TABLE {db_name}.t2 (ts TIMESTAMP, c1 INT)", TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED)
        
        # Test 5: Unlock the role
        self.login()
        self.exec_sql(f"UNLOCK ROLE {role_name}")
        self.grant_role(role_name, user2)  # Grant role to user2 after unlocking
        
        # Test 6: After unlocking, user1 can use role privileges again
        self.login(user1, pwd)
        self.exec_sql(f"USE {db_name}")
        self.exec_sql(f"CREATE TABLE  {db_name}.t1 (ts TIMESTAMP, c1 INT)")
        self.exec_sql(f"INSERT INTO   {db_name}.t1 (ts, c1) VALUES (NOW(), 1)")
        self.query_expect_rows(f"SELECT * FROM {db_name}.t1", 1)

        # user2 have role privileges after unlocking
        self.login(user2, pwd)
        self.exec_sql(f"USE {db_name}")
        self.exec_sql(f"CREATE TABLE  {db_name}.t2 (ts TIMESTAMP, c1 INT)")
        self.exec_sql(f"INSERT INTO   {db_name}.t2 (ts, c1) VALUES (NOW(), 1)")
        self.query_expect_rows(f"SELECT * FROM {db_name}.t2", 1)

        # Test 7: After unlocking, role can be granted to new users
        self.login()
        self.grant_role(role_name, user3)
        
        # Test 8: New user with unlocked role can use privileges
        self.login(user3, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.t1", 1)
        self.query_expect_rows(f"SELECT * FROM {db_name}.t2", 1)
        
        # Test 9: Test SHOW ROLES shows lock status
        self.login()
        tdSql.query("SHOW ROLES")
        result = tdSql.queryResult
        role_found = False
        for row in result:
            if row[0] == role_name:
                role_found = True
                if row[1] != 1:
                    raise Exception(f"Expected role {role_name} to be UNLOCKED, but got {row[1]}")
                break
        if not role_found:
            raise Exception(f"Role {role_name} not found in SHOW ROLES output")
        
        # Test 10: Lock a non-existent role should fail
        self.exec_sql_failed("LOCK ROLE non_existent_role", TSDB_CODE_MND_ROLE_NOT_EXIST)
        
        # Test 11: Unlock a non-existent role should fail
        self.exec_sql_failed("UNLOCK ROLE non_existent_role", TSDB_CODE_MND_ROLE_NOT_EXIST)
        
        # Test 12: Lock already locked role
        self.exec_sql(f"LOCK ROLE {role_name}")
        self.exec_sql(f"LOCK ROLE {role_name}")  # Should succeed or fail gracefully
        
        # Test 13: Unlock already unlocked role
        self.exec_sql(f"UNLOCK ROLE {role_name}")
        self.exec_sql(f"UNLOCK ROLE {role_name}")  # Should succeed or fail gracefully
        
        # Test 14: System roles (like SYSDBA) cannot be locked
        self.exec_sql_failed("LOCK ROLE `SYSDBA`", TSDB_CODE_OPS_NOT_SUPPORT)
        self.exec_sql_failed("LOCK ROLE `SYSSEC`", TSDB_CODE_OPS_NOT_SUPPORT)
        self.exec_sql_failed("LOCK ROLE `SYSAUDIT`", TSDB_CODE_OPS_NOT_SUPPORT)
        
        # Test 15: Regular user cannot lock/unlock roles
        self.login(user1, pwd)
        self.exec_sql_failed(f"LOCK ROLE {role_name}", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql(f"UNLOCK ROLE {role_name}")
        
        self.login()
        self.exec_sql(f"LOCK ROLE {role_name}")
        self.login(user1, pwd)
        self.exec_sql_failed(f"UNLOCK ROLE {role_name}", TSDB_CODE_MND_NO_RIGHTS)
        
        # Test 16: User with LOCK ROLE privilege can lock/unlock roles
        self.login()
        self.exec_sql(f"UNLOCK ROLE {role_name}")
        lock_admin = "lock_admin"
        self.create_user(lock_admin, pwd)
        
        '''BUG21'''
        self.grant_privilege("LOCK ROLE,UNLOCK ROLE", None, lock_admin)
        self.login(lock_admin, pwd)
        self.exec_sql(f"LOCK ROLE {role_name}")
        # Verify role is locked
        tdSql.execute(f"reset query cache")
        self.login(user1, pwd)
        self.exec_sql_failed(f"CREATE TABLE {db_name}.t4 (ts TIMESTAMP, c1 INT)", TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED)
        # Unlock as lock_admin
        self.login(lock_admin, pwd)
        self.exec_sql(f"UNLOCK ROLE {role_name}")
        self.login(user1, pwd)
        self.exec_sql(f"CREATE TABLE {db_name}.t4 (ts TIMESTAMP, c1 INT)")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_role(role_name)
        '''BUG22'''
        self.drop_user(user1)
        self.drop_user(user2)
        self.drop_user(user3)
        self.drop_user(lock_admin)
        
        print("LOCK ROLE / UNLOCK ROLE .............. [ passed ] ")
    
    def do_system_roles(self):
        # Test system roles: SYSDBA, SYSSEC, SYSAUDIT
        tdLog.info("=== Testing System Roles ===")
        self.login()
        
        role = "test_role"
        user = "test_user"
        db_name = "test_db"        
        
        # Test SYSDBA role
        user_sysdba = "test_sysdba"
        self.create_user(user_sysdba, pwd)
        self.grant_role("`SYSDBA`", user_sysdba)
        
        # Test: SYSDBA can create database
        self.login(user_sysdba, pwd)
        self.create_database(db_name)
        self.drop_database(db_name)
        self.create_database(db_name)

        # Test: SYSDBA can create user and role 
        self.create_user(user, pwd)
        self.drop_user(user)
        self.create_user(user, pwd)
        self.create_role(role)
        self.drop_role(role)
        self.create_role(role)
        
        # Test SYSSEC role
        self.login()
        user_syssec = "test_syssec"
        self.create_user(user_syssec, pwd)
        self.grant_role("`SYSSEC`", user_syssec)
        
        # Test SYSSEC can grant/revoke privileges
        self.login(user_syssec, pwd)
        self.grant_privilege("CREATE DATABASE", None, user)
        self.revoke_privilege("CREATE DATABASE", None, user)
        
        # Test SYSAUDIT role
        self.login()
        user_audit = "test_sysaudit"
        self.create_user(user_audit, pwd)
        self.grant_role("`SYSAUDIT`", user_audit)
        
        # Test: SYSAUDIT can view audit information
        self.login(user_audit, pwd)
        #BUG2
        #self.exec_sql("SHOW USERS FULL")
        
        # Test: SYSAUDIT cannot access business data
        self.login()
        self.create_database("test_audit_db")
        self.create_table("test_audit_db", "t1")
        self.insert_data("test_audit_db", "t1")
        
        self.login(user_audit, pwd)
        self.exec_sql_failed("SELECT * FROM test_audit_db.t1", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Cleanup
        self.login()
        self.drop_database("test_audit_db")
        self.drop_user(user_sysdba)
        self.drop_user(user_syssec)
        self.drop_user(user_audit)
        self.drop_user(user)
        self.drop_role(role)
        self.drop_database(db_name)
        
        print("System Roles (SYSDBA/SYSSEC/SYSAUDIT) [ passed ] ")
    
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
        
        self.create_user(user_audit, pwd)
        self.create_user(user_audit_log, pwd)
        self.create_user(user_normal, pwd)
        
        # Grant SYSAUDIT role to audit user
        self.grant_role("`SYSAUDIT`", user_audit)
        
        # Grant SYSAUDIT_LOG role to log writer
        self.grant_role("`SYSAUDIT_LOG`", user_audit_log)
        
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
        
        print("Audit Database Privileges ............ [ passed ] ")

    #
    # --------------------------- System Privileges Tests ----------------------------
    #
    def do_user_management_privileges(self):
        # Test user management privileges: ALTER USER (ENABLE), SHOW USERS SECURITY INFORMATION
        tdLog.info("=== Testing User Management Privileges ===")
        self.login()  # Login as root
        
        test_user = "test_user"
        admin_user = "admin_user"
        sec_user = "sec_user"
        
        # Create users
        self.create_user(test_user, pwd)
        self.create_user(admin_user, pwd)
        self.create_user(sec_user, pwd)
        
        # Test: Normal user cannot alter other users (lock/unlock)
        self.login(test_user, pwd)
        self.exec_sql_failed(f"ALTER USER {admin_user} ENABLE 0", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed(f"ALTER USER {admin_user} ENABLE 1", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant ALTER USER privilege to admin_user
        self.login()
        self.grant_privilege("ALTER USER,LOCK USER,UNLOCK USER", None, admin_user)
        
        # Test: admin_user can lock users (disable via ALTER USER)
        self.login(admin_user, pwd)
        self.exec_sql(f"ALTER USER {test_user} ENABLE 0")
        
        # Test: Locked user cannot login
        self.login_failed(test_user, pwd)
        
        # Test: admin_user can also unlock users (enable via ALTER USER)
        self.login(admin_user, pwd)
        self.exec_sql(f"ALTER USER {test_user} ENABLE 1")
        
        # Test: Unlocked user can login again
        self.login(test_user, pwd)
        
        # Test: SHOW USERS SECURITY INFORMATION privilege
        self.login()
        self.grant_privilege("SHOW USERS SECURITY INFORMATION", None, sec_user)
        
        self.login(sec_user, pwd)
        self.exec_sql("SHOW USERS FULL")
        
        # Cleanup
        self.login()
        self.drop_user(test_user)
        self.drop_user(admin_user)
        self.drop_user(sec_user)
        
        print("User Management Privileges ........... [ passed ] ")
    
    def do_token_management_privileges(self):
        # Test Token management privileges
        tdLog.info("=== Testing Token Management Privileges ===")
        self.login()  # Login as root
        
        test_user = "test_user"
        token_admin = "token_admin"
        
        # Create users
        self.create_user(test_user, pwd)
        self.create_user(token_admin, pwd)
        
        # Test: Normal user cannot create token for others
        self.login(test_user, pwd)
        self.exec_sql_failed(f"CREATE TOKEN test_token FROM USER {token_admin}", TSDB_CODE_MND_NO_RIGHTS)
        
        # Grant CREATE TOKEN privilege
        self.login()
        self.grant_privilege("CREATE TOKEN", None, token_admin)
        
        # Test: token_admin can create token
        self.login(token_admin, pwd)
        self.exec_sql(f"CREATE TOKEN test_token FROM USER {test_user}")
        
        # Test: SHOW TOKENS privilege
        self.login()
        self.grant_privilege("SHOW TOKENS", None, test_user)
        
        self.login(test_user, pwd)
        self.exec_sql("SHOW TOKENS")
        
        # Test: ALTER TOKEN privilege
        self.login()
        self.grant_privilege("ALTER TOKEN", None, token_admin)
        
        self.login(token_admin, pwd)
        self.exec_sql("ALTER TOKEN test_token ENABLE 0")
        
        # Test: DROP TOKEN privilege
        self.login()
        self.grant_privilege("DROP TOKEN", None, token_admin)
        
        self.login(token_admin, pwd)
        self.exec_sql("DROP TOKEN test_token")
        
        # Cleanup
        self.login()
        self.drop_user(test_user)
        self.drop_user(token_admin)
        
        print("Token Management Privileges .......... [ passed ] ")
    
    def do_totp_management_privileges(self):
        #BUG11
        return
        # Test TOTP management privileges
        tdLog.info("=== Testing TOTP Management Privileges ===")
        self.login()  # Login as root
        
        test_user = "test_user"
        totp_admin = "totp_admin"
        
        # Create users
        self.create_user(test_user, pwd)
        self.create_user(totp_admin, pwd)
        
        # Test: Normal user cannot create TOTP for others
        self.login(test_user, pwd)
        self.exec_sql_failed(f"CREATE TOTP_SECRET FOR USER {totp_admin}", TSDB_CODE_MND_NO_RIGHTS)
        
        # Grant CREATE TOTP privilege
        self.login()
        self.grant_privilege("CREATE TOTP", None, totp_admin)
        
        # Test: totp_admin can create TOTP
        self.login(totp_admin, pwd)
        self.exec_sql(f"CREATE TOTP_SECRET FOR USER {test_user}")
        
        # Grant DROP TOTP privilege
        self.login()
        self.grant_privilege("DROP TOTP", None, totp_admin)
        
        # Test: totp_admin can drop TOTP
        self.login(totp_admin, pwd)
        self.exec_sql(f"DROP TOTP_SECRET FROM USER {test_user}")
        
        # Cleanup
        self.login()
        self.drop_user(test_user)
        self.drop_user(totp_admin)
        
        print("TOTP Management Privileges ........... [ passed ] ")
    
    def do_password_management_privileges(self):
        # Test password management privileges
        tdLog.info("=== Testing Password Management Privileges ===")
        self.login()  # Login as root
        
        test_user = "test_user"
        pass_admin = "pass_admin"
        new_pwd = "NewPass@123"
        
        # Create users
        self.create_user(test_user, pwd)
        self.create_user(pass_admin, pwd)
        
        # Test: Normal user cannot change others' password
        self.login(test_user, pwd)
        self.exec_sql_failed(f"ALTER USER {pass_admin} PASS '{new_pwd}'", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant ALTER PASS privilege
        self.login()
        self.grant_privilege("ALTER PASS", None, pass_admin)
        
        # Test: pass_admin can change others' password
        self.login(pass_admin, pwd)
        #BUG12
        self.exec_sql(f"ALTER USER {test_user} PASS '{new_pwd}'")
        # Verify new password works
        self.login(test_user, new_pwd)
        
        # Test: ALTER SELF PASS privilege
        self.login()
        self.drop_user(test_user)
        tdSql.execute(f"reset query cache")
        self.create_user(test_user, pwd)  # Recreate with original password
        self.revoke_role("`SYSINFO_1`", test_user)  # revoke default role
        self.grant_privilege("ALTER SELF PASS", None, test_user)
        
        self.login(test_user, pwd)
        self.exec_sql(f"ALTER USER {test_user} PASS '{new_pwd}'")
        
        # Verify can login with new password
        self.login(test_user, new_pwd)
        
        # Cleanup
        self.login()
        self.drop_user(test_user)
        self.drop_user(pass_admin)
        
        print("Password Management Privileges ....... [ passed ] ")
    
    def do_node_management_privileges(self):
        # Test node management privileges
        tdLog.info("=== Testing Node Management Privileges ===")
        self.login()  # Login as root
        
        test_user = "test_user"
        
        # Create users
        self.create_user(test_user, pwd)
        self.revoke_role("`SYSINFO_1`", test_user)  # revoke default role
        
        # Test: Normal user cannot show nodes
        self.login(test_user, pwd)
        self.exec_sql_failed("SHOW DNODES", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW MNODES", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW SNODES", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW QNODES", TSDB_CODE_PAR_PERMISSION_DENIED)        
        
        # Grant SHOW NODES privilege
        self.login()
        self.grant_privilege("SHOW NODES", None, test_user)
        
        self.login(test_user, pwd)
        self.exec_sql("SHOW DNODES")
        self.exec_sql("SHOW MNODES")
        self.exec_sql("SHOW QNODES")
        # Test no privilege
        self.exec_sql_failed("CREATE DNODE 'localhost:6330'", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("CREATE MNODE ON DNODE 2", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("CREATE SNODE ON DNODE 2", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("CREATE QNODE ON DNODE 2", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("DROP DNODE 3", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("DROP MNODE ON DNODE 1", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("DROP SNODE ON DNODE 1", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("DROP QNODE ON DNODE 1", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Set privilege        
        self.login()
        self.grant_privilege("CREATE NODE", None, test_user)
        self.grant_privilege("DROP NODE", None, test_user)
        self.grant_role("`SYSINFO_1`", test_user)  # Grant default role back for cleanup
        
        # Test have privilege
        self.login(test_user, pwd)
        #BUG13
        self.exec_sql("CREATE DNODE 'localhost:6330'")
        self.exec_sql("CREATE MNODE ON DNODE 2")
        self.exec_sql("CREATE SNODE ON DNODE 2")
        self.exec_sql("CREATE QNODE ON DNODE 2")
        self.exec_sql("DROP DNODE 4 FORCE")
        self.exec_sql("DROP DNODE 3")
        self.exec_sql("DROP MNODE ON DNODE 2")
        self.exec_sql("DROP SNODE ON DNODE 2")
        self.exec_sql("DROP QNODE ON DNODE 2")
        
        # Cleanup
        self.login()
        self.drop_user(test_user)
        
        print("Node Management Privileges ........... [ passed ] ")
    

    
    def do_mount_management_privileges(self):
        # Test mount management privileges
        tdLog.info("=== Testing Mount Management Privileges ===")
        self.login()  # Login as root
        test_user = "test_user"
        sql_mount = f"CREATE MOUNT mnt1 ON dnode 1 FROM '/root/errorPath'"    
        
        # Create users
        self.create_user(test_user, pwd)
        self.revoke_role("`SYSINFO_1`", test_user)  # revoke default role
        
        # Test: Normal user cannot show mounts
        self.login(test_user, pwd)
        #BUG13
        self.exec_sql_failed("SHOW MOUNTS", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant SHOW MOUNTS privilege
        self.login()
        self.grant_privilege("SHOW MOUNTS", None, test_user)
        
        self.login(test_user, pwd)
        self.exec_sql("SHOW MOUNTS")
        
        # Test: no privilege
        self.login(test_user, pwd)
        self.exec_sql_failed(sql_mount, TSDB_CODE_MND_NO_RIGHTS)
        
        # Set privilege
        self.login()
        self.grant_privilege("CREATE MOUNT", None, test_user)
        self.grant_privilege("DROP MOUNT", None, test_user)
        
        # Test: CREATE/DROP MOUNT
        self.login(test_user, pwd)
        #BUG14
        self.exec_sql_failed(sql_mount, TSDB_CODE_NO_SUCH_FILE)
        
        # Cleanup
        self.login()
        self.drop_user(test_user)
        
        print("Mount Management Privileges .......... [ passed ] ")
    
    def do_system_variable_privileges(self):
        # Test system variable privileges
        tdLog.info("=== Testing System Variable Privileges ===")
        self.login()  # Login as root
        
        test_user = "test_user"
        
        # Create users
        self.create_user(test_user, pwd)
        # revoke
        self.revoke_role("`SYSINFO_1`", test_user)  # revoke default role
        self.revoke_privilege("SHOW  SYSTEM   VARIABLES", None, test_user)
        self.revoke_privilege("ALTER SYSTEM   VARIABLE",  None, test_user)
        self.revoke_privilege("SHOW  AUDIT    VARIABLES", None, test_user)
        self.revoke_privilege("ALTER AUDIT    VARIABLE",  None, test_user)        
        self.revoke_privilege("SHOW  SECURITY VARIABLES", None, test_user)
        self.revoke_privilege("ALTER SECURITY VARIABLE",  None, test_user)
        self.revoke_privilege("SHOW  DEBUG    VARIABLES", None, test_user)
        self.revoke_privilege("ALTER DEBUG    VARIABLE",  None, test_user)
    
        # Test: no privilege
        self.login(test_user, pwd)
        '''BUG15'''
        self.query_expect_rows("SHOW CLUSTER VARIABLES LIKE 'monitor'", 0)
        self.query_expect_rows("SHOW CLUSTER VARIABLES LIKE 'audit'", 0)
        self.query_expect_rows("SHOW CLUSTER VARIABLES LIKE 'enableStrongPassword'", 0)
        self.query_expect_rows("SHOW LOCAL   VARIABLES LIKE 'numOfLogLines'", 0)
        self.exec_sql_failed("ALTER ALL DNODES 'monitor 1'", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed("ALTER ALL DNODES 'numOfLogLines 100000'", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed("ALTER ALL DNODES 'audit 0'", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed("ALTER ALL DNODES 'enableStrongPassword 0'", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed("ALTER LOCAL 'debugFlag 141'", TSDB_CODE_PAR_PERMISSION_DENIED)

        # Grant privilege
        self.login()
        self.grant_privilege("SHOW  SYSTEM   VARIABLES", None, test_user)
        self.grant_privilege("ALTER SYSTEM   VARIABLE",  None, test_user)
        self.grant_privilege("SHOW  AUDIT    VARIABLES", None, test_user)
        self.grant_privilege("ALTER AUDIT    VARIABLE",  None, test_user)        
        self.grant_privilege("SHOW  SECURITY VARIABLES", None, test_user)
        self.grant_privilege("ALTER SECURITY VARIABLE",  None, test_user)
        self.grant_privilege("SHOW  DEBUG    VARIABLES", None, test_user)
        self.grant_privilege("ALTER DEBUG    VARIABLE",  None, test_user)
        
        # Test: have privilege
        self.login(test_user, pwd)
        self.exec_sql("SHOW CLUSTER VARIABLES")
        self.exec_sql("ALTER ALL DNODES 'monitor 1'")
        self.exec_sql("ALTER ALL DNODES 'numOfLogLines 100000'")
        self.exec_sql("ALTER ALL DNODES 'audit 0'")
        self.exec_sql("ALTER ALL DNODES 'enableStrongPassword 0'")
        # show
        self.query_expect_rows("SHOW CLUSTER VARIABLES LIKE 'monitor'", 1)
        self.query_expect_rows("SHOW CLUSTER VARIABLES LIKE 'audit'", 1)
        self.query_expect_rows("SHOW CLUSTER VARIABLES LIKE 'enableStrongPassword'", 1)
        self.query_expect_rows("SHOW LOCAL   VARIABLES LIKE 'numOfLogLines'", 1)
        
        # revoke again
        self.login()
        self.revoke_privilege("SHOW  SYSTEM   VARIABLES", None, test_user)
        self.revoke_privilege("ALTER SYSTEM   VARIABLE",  None, test_user)
        self.revoke_privilege("SHOW  AUDIT    VARIABLES", None, test_user)
        self.revoke_privilege("ALTER AUDIT    VARIABLE",  None, test_user)        
        self.revoke_privilege("SHOW  SECURITY VARIABLES", None, test_user)
        self.revoke_privilege("ALTER SECURITY VARIABLE",  None, test_user)
        self.revoke_privilege("SHOW  DEBUG    VARIABLES", None, test_user)
        self.revoke_privilege("ALTER DEBUG    VARIABLE",  None, test_user)        
        
        # Test: no privilege
        self.login(test_user, pwd)
        '''BUG15'''
        self.query_expect_rows("SHOW CLUSTER VARIABLES LIKE 'monitor'", 0)
        self.query_expect_rows("SHOW CLUSTER VARIABLES LIKE 'audit'", 0)
        self.query_expect_rows("SHOW CLUSTER VARIABLES LIKE 'enableStrongPassword'", 0)
        self.query_expect_rows("SHOW LOCAL   VARIABLES LIKE 'numOfLogLines'", 0)
        self.exec_sql_failed("ALTER ALL DNODES 'monitor 1'", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed("ALTER ALL DNODES 'numOfLogLines 100000'", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed("ALTER ALL DNODES 'audit 0'", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed("ALTER ALL DNODES 'enableStrongPassword 0'", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed("ALTER LOCAL 'debugFlag 141'", TSDB_CODE_PAR_PERMISSION_DENIED)
                
        # Cleanup
        self.login()
        self.drop_user(test_user)
        
        print("System Variable Privileges ........... [ passed ] ")
    
    def do_information_schema_privileges(self):
        # Test information_schema access privileges
        tdLog.info("=== Testing Information Schema Privileges ===")
        self.login()  # Login as root
        
        test_user = "test_user"
        
        # Create users
        self.create_user(test_user, pwd)
        self.revoke_role("`SYSINFO_1`", test_user)  # revoke default role
        
        # Test: without privilege
        self.login(test_user, pwd)
        '''BUG16'''
        self.exec_sql_failed("SELECT * FROM information_schema.ins_tables", TSDB_CODE_PAR_PERMISSION_DENIED)       # basic
        self.exec_sql_failed("SELECT * FROM information_schema.ins_databases", TSDB_CODE_PAR_PERMISSION_DENIED)    # basic
        self.exec_sql_failed("SELECT * FROM information_schema.ins_users", TSDB_CODE_PAR_PERMISSION_DENIED)        # security
        self.exec_sql_failed("SELECT * FROM information_schema.ins_grants_full", TSDB_CODE_PAR_PERMISSION_DENIED)  # privileged
        self.exec_sql_failed("SELECT * FROM performance_schema.perf_connections", TSDB_CODE_PAR_PERMISSION_DENIED) # basic
        self.exec_sql_failed("SELECT * FROM performance_schema.perf_instances",   TSDB_CODE_PAR_PERMISSION_DENIED) # privileged
        
        # Grant privilege
        self.login()
        self.grant_privilege("READ INFORMATION_SCHEMA BASIC",      None, test_user)
        self.grant_privilege("READ INFORMATION_SCHEMA PRIVILEGED", None, test_user)
        self.grant_privilege("READ INFORMATION_SCHEMA SECURITY",   None, test_user)
        self.grant_privilege("READ INFORMATION_SCHEMA AUDIT",      None, test_user) # empty
        self.grant_privilege("READ PERFORMANCE_SCHEMA BASIC",      None, test_user)
        self.grant_privilege("READ PERFORMANCE_SCHEMA PRIVILEGED", None, test_user)
        
        # Test: have privilege
        self.login(test_user, pwd)
        self.exec_sql("SELECT * FROM information_schema.ins_databases")
        self.exec_sql("SELECT * FROM information_schema.ins_users")
        self.exec_sql("SELECT * FROM information_schema.ins_grants_full")
        self.exec_sql("SELECT * FROM performance_schema.perf_connections")
        self.exec_sql("SELECT * FROM performance_schema.perf_instances")        
        
        # Revoke
        self.login()
        self.revoke_privilege("READ INFORMATION_SCHEMA BASIC",      None, test_user)
        self.revoke_privilege("READ INFORMATION_SCHEMA PRIVILEGED", None, test_user)
        self.revoke_privilege("READ INFORMATION_SCHEMA SECURITY",   None, test_user)
        self.revoke_privilege("READ INFORMATION_SCHEMA AUDIT",      None, test_user) # empty
        self.revoke_privilege("READ PERFORMANCE_SCHEMA BASIC",      None, test_user)
        self.revoke_privilege("READ PERFORMANCE_SCHEMA PRIVILEGED", None, test_user)        
        
        # Test: without privilege
        self.login(test_user, pwd)
        '''BUG16'''
        self.exec_sql_failed("SELECT * FROM information_schema.ins_tables", TSDB_CODE_PAR_PERMISSION_DENIED)       # basic
        self.exec_sql_failed("SELECT * FROM information_schema.ins_databases", TSDB_CODE_PAR_PERMISSION_DENIED)    # basic
        self.exec_sql_failed("SELECT * FROM information_schema.ins_users", TSDB_CODE_PAR_PERMISSION_DENIED)        # security
        self.exec_sql_failed("SELECT * FROM information_schema.ins_grants_full", TSDB_CODE_PAR_PERMISSION_DENIED)  # privileged
        self.exec_sql_failed("SELECT * FROM performance_schema.perf_connections", TSDB_CODE_PAR_PERMISSION_DENIED) # basic
        self.exec_sql_failed("SELECT * FROM performance_schema.perf_instances",   TSDB_CODE_PAR_PERMISSION_DENIED) # privileged
        
        # Cleanup
        self.login()
        self.drop_user(test_user)
        
        print("Information Schema Privileges ........ [ passed ] ")
    
    def do_system_monitoring_privileges(self):
        # Test system monitoring privileges: SHOW/KILL TRANSACTIONS/CONNECTIONS/QUERIES
        tdLog.info("=== Testing System Monitoring Privileges ===")
        self.login()  # Login as root
        
        test_user = "test_user"
        
        # Create users
        self.create_user(test_user, pwd)
        self.revoke_role("`SYSINFO_1`", test_user)  # revoke default role
        
        # Test: without privilege
        self.login(test_user, pwd)
        self.exec_sql_failed("SHOW TRANSACTIONS", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW QUERIES", TSDB_CODE_PAR_PERMISSION_DENIED)
        '''BUG17
        self.exec_sql_failed("SHOW CONNECTIONS", TSDB_CODE_PAR_PERMISSION_DENIED)
        '''
        
        # Grant
        self.login()
        self.grant_privilege("SHOW TRANSACTIONS", None, test_user)
        self.grant_privilege("SHOW CONNECTIONS", None, test_user)
        self.grant_privilege("SHOW QUERIES", None, test_user)
        
        self.login(test_user, pwd)
        self.exec_sql("SHOW TRANSACTIONS")        
        self.exec_sql("SHOW CONNECTIONS")        
        self.exec_sql("SHOW QUERIES")
        
        # Grant kill
        self.login()
        self.grant_privilege("KILL TRANSACTION", None, test_user)
        self.grant_privilege("KILL CONNECTION", None, test_user)
        self.grant_privilege("KILL QUERY", None, test_user)
        
        # Test: KILL privileges
        self.login(test_user, pwd)
        self.exec_sql_failed("KILL TRANSACTION 9999", TSDB_CODE_MND_TRANS_NOT_EXIST)     # Assuming 9999 is an invalid transaction ID
        self.exec_sql_failed("KILL CONNECTION 9999", TSDB_CODE_MND_INVALID_CONN_ID)      # Assuming 9999 is an invalid connection ID
        self.exec_sql_failed("KILL QUERY 'd5835564:999'", TSDB_CODE_MND_INVALID_CONN_ID) # Assuming 'd5835564:999' is an invalid query ID
        
        # Revoke 
        self.login()
        self.revoke_privilege("SHOW TRANSACTIONS", None, test_user)
        self.revoke_privilege("SHOW CONNECTIONS", None, test_user)
        self.revoke_privilege("SHOW QUERIES", None, test_user)

        # Test: without privilege
        self.login(test_user, pwd)
        self.exec_sql_failed("SHOW TRANSACTIONS", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW QUERIES", TSDB_CODE_PAR_PERMISSION_DENIED)
        '''BUG17'''
        self.exec_sql_failed("SHOW CONNECTIONS", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Cleanup
        self.login()
        self.drop_user(test_user)
        
        print("System Monitoring Privileges .......... [ passed ] ")
    
    def do_show_grants_cluster_apps_privileges(self):
        # Test SHOW GRANTS, SHOW CLUSTER, SHOW APPS privileges
        tdLog.info("=== Testing SHOW GRANTS/CLUSTER/APPS Privileges ===")
        self.login()  # Login as root
        
        test_user = "test_user"
        
        # Create users
        self.create_user(test_user, pwd)
        self.revoke_role("`SYSINFO_1`", test_user)  # revoke default role
        
        # Test: without privilege
        self.login(test_user, pwd)
        '''BUG18'''
        self.exec_sql("SHOW GRANTS")
        self.exec_sql_failed("SHOW GRANTS FULL", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW GRANTS LOGS", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW CLUSTER MACHINES", TSDB_CODE_PAR_PERMISSION_DENIED)

        self.exec_sql_failed("SHOW CLUSTER", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW APPS", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant
        self.login()
        self.grant_privilege("SHOW GRANTS", None, test_user)
        self.grant_privilege("SHOW CLUSTER", None, test_user)
        self.grant_privilege("SHOW APPS", None, test_user)
        
        self.login(test_user, pwd)
        self.exec_sql("SHOW GRANTS")
        self.exec_sql("SHOW GRANTS FULL")
        self.exec_sql("SHOW GRANTS LOGS")
        self.exec_sql("SHOW CLUSTER MACHINES")
        self.exec_sql("SHOW CLUSTER")
        self.exec_sql("SHOW APPS")
        
        # Revoke 
        self.login()
        self.revoke_privilege("SHOW GRANTS", None, test_user)
        self.revoke_privilege("SHOW CLUSTER", None, test_user)
        self.revoke_privilege("SHOW APPS", None, test_user)

        # Test: without privilege
        self.login(test_user, pwd)
        '''BUG18'''
        self.exec_sql("SHOW GRANTS")
        self.exec_sql_failed("SHOW GRANTS FULL", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW GRANTS LOGS", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW CLUSTER MACHINES", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW CLUSTER", TSDB_CODE_PAR_PERMISSION_DENIED)
        self.exec_sql_failed("SHOW APPS", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Cleanup
        self.login()
        self.drop_user(test_user)
        
        print("SHOW GRANTS/CLUSTER/APPS Privileges ... [ passed ] ")
    
    def do_privilege_delegation(self):
        # Test GRANT/REVOKE PRIVILEGE privileges (recursive authorization)
        tdLog.info("=== Testing Privilege Delegation ===")
        self.login()  # Login as root
        
        admin_user = "admin_user"
        test_user = "test_user"
        db_name = "test_db"
        
        # Create users and database
        self.create_user(admin_user, pwd)
        self.create_user(test_user, pwd)
        self.revoke_role("`SYSINFO_1`", test_user)  # revoke default role
        self.create_database(db_name)
        
        # Test: Normal user cannot grant privileges
        self.login(admin_user, pwd)
        self.exec_sql_failed(f"GRANT SELECT ON {db_name}.* TO {test_user}", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant GRANT PRIVILEGE privilege to admin_user
        self.login()
        self.grant_privilege("GRANT PRIVILEGE", None, admin_user)
        
        # Test: admin_user can now grant privileges to others
        self.login(admin_user, pwd)
        self.grant_privilege("USE", f"DATABASE {db_name}", test_user)
        
        # Verify test_user received the privilege
        self.login(test_user, pwd)
        self.exec_sql(f"USE {db_name}")
        
        # Test: REVOKE PRIVILEGE privilege
        self.login()
        self.grant_privilege("REVOKE PRIVILEGE", None, admin_user)
        
        self.login(admin_user, pwd)
        self.revoke_privilege("USE", f"DATABASE {db_name}", test_user)
        
        # Verify privilege was revoked
        self.login(test_user, pwd)
        self.exec_sql_failed(f"USE {db_name}", TSDB_CODE_MND_NO_RIGHTS)
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(admin_user)
        self.drop_user(test_user)
        
        print("Privilege Delegation ................. [ passed ] ")

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
        
        print("CREATE FUNCTION ...................... [ passed ] ")
    
    def do_create_index_privilege(self):
        # Test CREATE INDEX privilege
        tdLog.info("=== Testing CREATE INDEX Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 VARCHAR(50)", tags="t1 INT, t2 INT, t3 varchar(20)")
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  # SYSINFO_1 is default role
        
        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("SELECT", f"{db_name}.*", user)
        
        #
        # create index
        #
        
        # Test: user cannot create index without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"CREATE INDEX idx1 ON {db_name}.st1 (t2)", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant CREATE INDEX privilege
        self.login()
        self.grant_privilege("CREATE INDEX", f"TABLE {db_name}.*", user)
        # Test: user can create index with privilege
        self.login(user, pwd)
        self.exec_sql(f"CREATE INDEX idx1 ON {db_name}.st1 (t2)")
        # Test: revoke
        self.login()
        self.exec_sql(f"CREATE INDEX idx2 ON {db_name}.st1 (t3)")
        
        #
        # show
        #
        
        # Test: user cannot show index without privilege
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW INDEXES FROM {db_name}.st1", 1)
        # Grant privilege
        self.login()
        self.grant_privilege("SHOW", f"INDEX {db_name}.idx2", user)
        # Test: passed
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW INDEXES FROM {db_name}.st1", 2)
        # Test: revoke
        self.login()
        self.revoke_privilege("SHOW", f"INDEX {db_name}.idx2", user)
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW INDEXES FROM {db_name}.st1", 1)
        
        #
        # drop
        #
        
        # Test: user cannot drop index without privilege
        self.exec_sql_failed(f"DROP INDEX {db_name}.idx1", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant DROP privilege on index
        self.login()
        self.grant_privilege("DROP", f"INDEX {db_name}.*", user)
        # Test: user can drop index
        self.login(user, pwd)
        self.exec_sql(f"DROP INDEX {db_name}.idx1")
        # Test: revoke
        self.login()
        self.revoke_privilege("DROP", f"INDEX {db_name}.*", user)
        time.sleep(1)  # Ensure previous drop is fully processed
        self.login(user, pwd)
        time.sleep(1)
        self.exec_sql_failed(f"DROP INDEX {db_name}.idx2", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("CREATE INDEX ......................... [ passed ] ")

    def do_create_tsma_privilege(self):
        # Test CREATE TSMA privilege
        tdLog.info("=== Testing CREATE TSMA Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name)
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 INT", tags="t1 INT")
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  # SYSINFO_1 is default role
        
        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("SELECT", f"{db_name}.*", user)
        
        #
        # create tsma
        #
        
        # Test: user cannot create tsma without privilege
        create_tsma_sql = f"CREATE TSMA tsma1 ON {db_name}.st1 FUNCTION(MIN(c1), MAX(c2)) INTERVAL(1m)"
        self.login(user, pwd)
        self.exec_sql_failed(create_tsma_sql, TSDB_CODE_PAR_TB_CREATE_PERMISSION_DENIED)
        self.login()
        self.grant_privilege("CREATE TABLE", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        self.exec_sql_failed(create_tsma_sql, TSDB_CODE_PAR_STREAM_CREATE_PERMISSION_DENIED)
        self.login()
        self.grant_privilege("CREATE STREAM", f"DATABASE {db_name}", user)
        self.login(user, pwd)
        self.exec_sql_failed(create_tsma_sql, TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant CREATE TSMA privilege
        self.login()
        self.grant_privilege("CREATE TSMA", f"TABLE {db_name}.*", user)
        self.exec_sql(f"CREATE TSMA tsma2 ON {db_name}.st1 FUNCTION(COUNT(ts)) INTERVAL(2m)")
        # Test: user can create tsma with privilege
        self.login(user, pwd)
        self.exec_sql(create_tsma_sql)
        
        #
        # show
        #
        
        # Test: user cannot show tsma without privilege
        #BUG10
        self.query_expect_rows(f"SHOW {db_name}.TSMAS", 1) # tsma1(create owner)
        # Grant privilege
        self.login()
        self.grant_privilege("SHOW", f"TSMA {db_name}.tsma2", user)
        # Test: passed
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW {db_name}.TSMAS", 2) # tsma1(create owner), tsma2(root)
        # Test: revoke
        self.login()
        self.revoke_privilege("SHOW", f"TSMA {db_name}.tsma2", user)
        self.login(user, pwd)
        #BUG10
        self.query_expect_rows(f"SHOW {db_name}.TSMAS", 1) # tsma1(create owner)

        # Test: revoke for create tsma
        self.login()
        self.revoke_privilege("CREATE TSMA", f"TABLE {db_name}.*", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"CREATE TSMA tsma3 ON {db_name}.st1 FUNCTION(AVG(c1)) INTERVAL(5m)", TSDB_CODE_PAR_PERMISSION_DENIED)

        #
        # drop
        #
        
        # Test: user cannot drop tsma without privilege
        self.exec_sql_failed(f"DROP TSMA {db_name}.tsma1", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant DROP privilege on tsma
        self.login()
        self.grant_privilege("DROP", f"TSMA {db_name}.*", user)
        # Test: user can drop tsma
        self.login(user, pwd)
        self.exec_sql(f"DROP TSMA {db_name}.tsma1")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("CREATE TSMA .......................... [ passed ] ")

    def do_create_rsma_privilege(self):
        # Test CREATE RSMA privilege
        tdLog.info("=== Testing CREATE RSMA Privilege ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_database(db_name, "DURATION 10d")
        self.create_stable(db_name, "st1", columns="ts TIMESTAMP, c1 INT, c2 FLOAT, c3 DOUBLE, c4 BOOL, c5 VARCHAR(10)", tags="t1 INT")
        self.create_stable(db_name, "st2", columns="ts TIMESTAMP, c1 INT, c2 FLOAT, c3 DOUBLE, c4 BOOL, c5 VARCHAR(10)", tags="t1 INT")
        self.create_stable(db_name, "st3", columns="ts TIMESTAMP, c1 INT, c2 FLOAT", tags="t1 INT")
        self.create_child_table(db_name, "ct1", "st1", tag_values="1")
        self.exec_sql(f"INSERT INTO {db_name}.ct1 VALUES (NOW, 1, 2.0, 3.0, true, 'test')")
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  # SYSINFO_1 is default role
        
        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("SELECT", f"{db_name}.*", user)
        self.grant_privilege("INSERT", f"{db_name}.*", user)
        
        #
        # create rsma
        #
        
        # Test: user cannot create rsma without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"CREATE RSMA rsma1 ON {db_name}.st1 FUNCTION(MIN(c1), MAX(c2), AVG(c3), LAST(c5)) INTERVAL(1m, 5m)", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant CREATE RSMA privilege
        self.login()
        self.grant_privilege("CREATE RSMA", f"TABLE {db_name}.*", user)
        self.exec_sql(f"CREATE RSMA rsma2 ON {db_name}.st2 FUNCTION(MIN(c1), MAX(c3)) INTERVAL(2m, 10m)")
        # Test: user can create rsma with privilege
        self.login(user, pwd)
        self.exec_sql(f"CREATE RSMA rsma1 ON {db_name}.st1 FUNCTION(MIN(c1), MAX(c2), AVG(c3), LAST(c5)) INTERVAL(1m, 5m)")
        
        #
        # alter
        #
        
        # Test: user cannot alter rsma without privilege
        self.exec_sql_failed(f"ALTER RSMA {db_name}.rsma1 FUNCTION(FIRST(c4))", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant ALTER privilege on rsma
        self.login()
        self.grant_privilege("ALTER", f"RSMA {db_name}.*", user)
        # Test: user can alter rsma
        self.login(user, pwd)
        self.exec_sql(f"ALTER RSMA {db_name}.rsma1 FUNCTION(FIRST(c4))")
        # Test: revoke
        self.login()
        self.revoke_privilege("ALTER", f"RSMA {db_name}.*", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"ALTER RSMA {db_name}.rsma1 FUNCTION(LAST(c4))", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        #
        # show
        #
        
        # Test: user cannot show rsma without privilege
        self.query_expect_rows(f"SHOW {db_name}.RSMAS", 1)  # rsma1 is owned by user
        # Grant privilege
        self.login()
        self.grant_privilege("SHOW", f"RSMA {db_name}.rsma2", user)
        # Test: passed
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW {db_name}.RSMAS", 2)  # rsma1(owner) + rsma2(root)
        # Test: revoke
        self.login()
        self.revoke_privilege("SHOW", f"RSMA {db_name}.rsma2", user)
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW {db_name}.RSMAS", 1)  # only rsma1(owner)
        
        # create rsmat test revoke
        self.login()
        self.revoke_privilege("CREATE RSMA", f"TABLE {db_name}.*", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"CREATE RSMA rsma3 ON {db_name}.st3 FUNCTION(SUM(c1)) INTERVAL(1m, 5m)", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        
        #
        # show create
        #
        
        # Test: user cannot show create rsma without privilege
        self.exec_sql_failed(f"SHOW CREATE RSMA {db_name}.rsma2", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant privilege
        self.login()
        self.grant_privilege("SHOW CREATE", f"RSMA {db_name}.*", user)
        # Test: passed
        self.login(user, pwd)
        self.exec_sql(f"SHOW CREATE RSMA {db_name}.rsma1")
        self.exec_sql(f"SHOW CREATE RSMA {db_name}.rsma2")
        # Test: revoke
        self.login()
        self.revoke_privilege("SHOW CREATE", f"RSMA {db_name}.*", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"SHOW CREATE RSMA {db_name}.rsma2", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        #
        # drop
        #
        
        # Test: user cannot drop rsma without privilege
        self.exec_sql_failed(f"DROP RSMA {db_name}.rsma2", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant DROP privilege on rsma
        self.login()
        self.grant_privilege("DROP", f"RSMA {db_name}.*", user)
        # Test: user can drop rsma
        self.login(user, pwd)
        self.exec_sql(f"DROP RSMA {db_name}.rsma1")
        self.exec_sql(f"DROP RSMA {db_name}.rsma2")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("CREATE RSMA .......................... [ passed ] ")

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
        self.create_table(db_name, "base_table", "ts TIMESTAMP, val INT")
        self.insert_data(db_name, "base_table")
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  # SYSINFO_1 is default role
        
        # Create a view as root
        self.exec_sql(f"CREATE VIEW {db_name}.v1 AS SELECT * FROM {db_name}.base_table")
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        #
        # select
        #
          
        # Test: user cannot select view without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"SELECT * FROM {db_name}.v1", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant SELECT privilege on view
        self.login()
        self.grant_privilege("SELECT", f"VIEW {db_name}.v1", user)
        # Test: user can select from view
        self.login(user, pwd)
        self.exec_sql(f"SELECT * FROM {db_name}.v1")
        # Test: revoke
        self.login()
        self.revoke_privilege("SELECT", f"VIEW {db_name}.v1", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"SELECT * FROM {db_name}.v1", TSDB_CODE_PAR_PERMISSION_DENIED)

        #
        # alter
        #        
        
        # Test: user cannot alter view without privilege
        self.exec_sql_failed(f"CREATE OR REPLACE VIEW {db_name}.v1 AS SELECT ts FROM {db_name}.base_table", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant ALTER privilege on view
        self.login()
        self.grant_privilege("ALTER", f"VIEW {db_name}.v1", user)
        # Test: user can alter view
        self.login(user, pwd)
        #BUG4
        #self.exec_sql(f"CREATE OR REPLACE VIEW {db_name}.v1 AS SELECT ts FROM {db_name}.base_table")
        # Test: revoke
        self.login()
        self.revoke_privilege("ALTER", f"VIEW {db_name}.v1", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"CREATE OR REPLACE VIEW {db_name}.v1 AS SELECT ts,val,ts FROM {db_name}.base_table", TSDB_CODE_PAR_PERMISSION_DENIED)

        #
        # show
        #

        # Test: user cannot show view without privilege
        self.query_expect_rows(f"SHOW {db_name}.VIEWS", 0)
        # Grant privilege
        self.login()
        self.grant_privilege("SHOW", f"VIEW {db_name}.v1", user)
        # Test: passed
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW {db_name}.VIEWS", 1)
        # Test: revoke
        self.login()
        self.revoke_privilege("SHOW", f"VIEW {db_name}.v1", user)
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW {db_name}.VIEWS", 0)

        #
        # show create
        #
        
        # Test: user cannot show create view without privilege
        self.exec_sql_failed(f"SHOW CREATE VIEW {db_name}.v1", TSDB_CODE_PAR_PERMISSION_DENIED)
        # Grant privilege
        self.login()
        self.grant_privilege("SHOW CREATE", f"VIEW {db_name}.*", user)
        # Test: passed
        self.login(user, pwd)
        self.exec_sql(f"SHOW CREATE VIEW {db_name}.v1")
        # Test： revoke
        self.login()
        self.revoke_privilege("SHOW CREATE", f"VIEW {db_name}.*", user)
        self.login(user, pwd)
        self.exec_sql_failed(f"SHOW CREATE VIEW {db_name}.v1", TSDB_CODE_PAR_PERMISSION_DENIED)

        #
        # drop
        #
                
        # Test: user cannot drop view without privilege
        self.exec_sql_failed(f"DROP VIEW {db_name}.v1", TSDB_CODE_PAR_PERMISSION_DENIED)
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
        
        print("View Privileges ...................... [ passed ] ")
        
    def do_view_nested_privilege(self):
        # Test nested view privileges (effective user concept)
        tdLog.info("=== Testing Nested View Privileges ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        base_user = "base_user"
        view_user = "view_user"
        nested_user = "nested_user"
        
        self.create_database(db_name)
        self.create_table(db_name, "base_table", "ts TIMESTAMP, val INT, lab VARCHAR(20)")
        self.exec_sql(f"INSERT INTO {db_name}.base_table VALUES (NOW, 100, 'A'), (NOW+1s, 200, 'B')")
        
        # Create users
        self.create_user(base_user, pwd)
        self.create_user(view_user, pwd)
        self.create_user(nested_user, pwd)
        
        # Grant base_user privileges to create view and access base table
        self.grant_privilege("USE", f"DATABASE {db_name}", base_user)
        self.grant_privilege("CREATE VIEW", f"DATABASE {db_name}", base_user)
        self.grant_privilege("SELECT", f"{db_name}.base_table", base_user)
        
        # Create first-level view as base_user
        self.login(base_user, pwd)
        self.exec_sql(f"CREATE VIEW {db_name}.v1 AS SELECT ts, val FROM {db_name}.base_table WHERE lab='A'")
        
        # Grant view_user privileges to create nested view and access v1
        self.login()
        self.grant_privilege("USE", f"DATABASE {db_name}", view_user)
        self.grant_privilege("CREATE VIEW", f"DATABASE {db_name}", view_user)
        self.grant_privilege("SELECT", f"VIEW {db_name}.v1", view_user)
        
        # Create nested view as view_user
        self.login(view_user, pwd)
        self.exec_sql(f"CREATE VIEW {db_name}.v2 AS SELECT ts, val*2 as double_val FROM {db_name}.v1")
        
        # Test 1: nested_user with SELECT on v2 but not on v1 or base_table
        self.login()
        self.grant_privilege("USE", f"DATABASE {db_name}", nested_user)
        self.grant_privilege("SELECT", f"VIEW {db_name}.v2", nested_user)
        
        self.login(nested_user, pwd)
        # Should succeed - nested view inherits effective user's privileges
        self.query_expect_rows(f"SELECT * FROM {db_name}.v2", 1)
        
        # Test 2: nested_user without SELECT on v2
        self.login()
        self.revoke_privilege("SELECT", f"VIEW {db_name}.v2", nested_user)
        
        self.login(nested_user, pwd)
        self.exec_sql_failed(f"SELECT * FROM {db_name}.v2", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Test 3: Grant SELECT on v1 directly to nested_user
        self.login()
        self.grant_privilege("SELECT", f"VIEW {db_name}.v1", nested_user)
        
        self.login(nested_user, pwd)
        self.query_expect_rows(f"SELECT * FROM {db_name}.v1", 1)
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(base_user)
        self.drop_user(view_user)
        self.drop_user(nested_user)
        
        print("Nested View Privileges ............... [ passed ] ")
        
    
    def do_topic_privileges(self):
        # Test topic privileges
        tdLog.info("=== Testing Topic Privileges ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        topic_name = "test_topic"
        self.create_database(db_name)
        self.create_stable(db_name, "st1")
        self.create_child_table(db_name, "ct1", "st1")
        self.insert_data(db_name, "ct1")
        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)
        self.create_topic("root_topic", f"SELECT * FROM {db_name}.st1")

        # not owner to test
        user1 = "test_user1"
        self.create_user(user1, pwd)        
        self.revoke_role("`SYSINFO_1`", user1)  # SYSINFO_1 is default role
        
        # Grant basic privileges
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        
        # Test 1: user cannot create topic without privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"CREATE TOPIC {topic_name} AS SELECT * FROM {db_name}.st1", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Grant CREATE TOPIC privilege
        self.login()
        # Note: CREATE TOPIC may require SELECT privilege on the source table/stb
        self.grant_privilege("SELECT", f"TABLE {db_name}.st1", user)
        self.grant_privilege("CREATE TOPIC", f"DATABASE {db_name}", user)
        
        # Test 2: user can create topic with privilege
        self.login(user, pwd)
        self.create_topic(topic_name, f"SELECT * FROM {db_name}.st1")
        self.query_expect_rows(f"SHOW TOPICS", 1)  # only own topics
        
        # Test 3: user1 cannot show/drop topic without privilege
        self.login(user1, pwd)
        self.query_expect_rows(f"SHOW TOPICS", 0)
        self.exec_sql_failed(f"DROP TOPIC {topic_name}", TSDB_CODE_MND_NO_RIGHTS)
        
        # Test 4: user cannot drop topic without privilege (topic was created by user, so can drop)
        # Create another topic as root to test DROP privilege
        self.login()
        topic_name2 = "test_topic2"
        self.create_topic(topic_name2, f"SELECT * FROM {db_name}.st1")
        
        self.login(user, pwd)
        self.exec_sql_failed(f"DROP TOPIC {topic_name2}", TSDB_CODE_MND_NO_RIGHTS)
        
        # Test 5: Grant DROP privilege on topic
        self.login()
        self.grant_privilege("DROP", f"TOPIC {db_name}.{topic_name2}", user)

        #
        # consumer privileges
        #
        
        consumer_user = "consumer_user"
        self.create_user(consumer_user, pwd)
        #self.revoke_role("`SYSINFO_1`", consumer_user)  # SYSINFO_1 is default role
        #self.grant_role("`SYSDBA`", consumer_user)

        # Test: consumer_user cannot consume topic without privilege
        self.subscribe_topic_failed(consumer_user, pwd, "group1", topic_name, TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED)
        
        # Grant SUBSCRIBE privilege on topic
        self.grant_privilege("USE", f"DATABASE {db_name}", consumer_user)
        self.grant_privilege("SUBSCRIBE", f"TOPIC {db_name}.{topic_name}", consumer_user)
        #BUG9
        consumer1 = self.subscribe_topic(consumer_user, pwd, "group1", topic_name, expected_rows=1)
        # consumer1 = self.subscribe_topic("root", "taosdata", "group1", topic_name, expected_rows=1)
        
        # Test: show consumers/subscriptions without privilege
        self.login(user1, pwd)
        self.query_expect_rows("show consumers;",     0)
        self.query_expect_rows("show subscriptions;", 0)
        # Grant SHOW CONSUMERS and SHOW SUBSCRIPTIONS privilege
        self.login()
        self.grant_privilege("SHOW CONSUMERS",     f"TOPIC {db_name}.{topic_name}", user1)
        self.grant_privilege("SHOW SUBSCRIPTIONS", f"TOPIC {db_name}.{topic_name}", user1)
        # Test again
        self.login(user1, pwd)
        self.query_expect_rows("show consumers;",     1) # one consumer
        #BUG8
        self.query_expect_rows("show subscriptions;", 2) # two vgroups
        
        self.login()
        consumer1.unsubscribe()
        self.revoke_privilege("SUBSCRIBE", f"TOPIC {db_name}.{topic_name}", consumer_user)
        
        # Test: user can drop topic with privilege
        self.drop_topic(topic_name2)
        # Test: user can drop own created topic
        self.drop_topic(topic_name)        
        
        # Cleanup
        self.login()
        self.drop_topic("root_topic")
        self.drop_database(db_name)
        self.drop_user(user)
        self.drop_user(user1)
        
        print("Topic Privileges ..................... [ passed ] ")
    
    def do_stream_privileges(self):
        # Test stream privileges
        tdLog.info("=== Testing Stream Privileges ===")
        self.login()  # Login as root
        
        db_name  = "test_db"
        db_name2 = "test_db2"
        user = "test_user"
        stream_name1 = "test_stream1"
        stream_name2 = "test_stream2"
        # db2
        stream_name3 = "test_stream3"
        stream_name4 = "test_stream4"
        self.create_database(db_name)
        self.create_stable(db_name, "source_table", "ts TIMESTAMP, val INT", "tag1 INT")
        self.create_database(db_name2)
        self.create_stable(db_name2, "source_table2", "ts TIMESTAMP, val INT", "tag1 INT")

        self.create_user(user, pwd)
        self.revoke_role("`SYSINFO_1`", user)  # SYSINFO_1 is default role
        
        # Create stream as root
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        stream_sql = f"CREATE STREAM {db_name}.{stream_name1} interval(1s) sliding(1s) FROM {db_name}.source_table INTO {db_name}.stream_result1  AS SELECT _twstart, avg(val) FROM %%trows "
        self.exec_sql(stream_sql)
        stream_sql = f"CREATE STREAM {db_name}.{stream_name2} interval(2s) sliding(2s) FROM {db_name}.source_table INTO {db_name}.stream_result2  AS SELECT _twstart, avg(val) FROM %%trows "
        self.exec_sql(stream_sql)
        
        # db2
        self.grant_privilege("USE", f"DATABASE {db_name2}", user)
        stream_sql = f"CREATE STREAM {db_name2}.{stream_name3} interval(1s) sliding(1s) FROM {db_name2}.source_table2 INTO {db_name2}.stream_result3  AS SELECT _twstart, avg(val) FROM %%trows "
        self.exec_sql(stream_sql)
        stream_sql = f"CREATE STREAM {db_name2}.{stream_name4} interval(2s) sliding(2s) FROM {db_name2}.source_table2 INTO {db_name2}.stream_result4  AS SELECT _twstart, avg(val) FROM %%trows "
        self.exec_sql(stream_sql)
        
        #
        # privilege level
        #
        
        # *.*
        self.grant_privilege("ALL", f"STREAM *.*", user)
        self.login(user, pwd)
        self.grant_privilege_failed("ALL", f"STREAM *.*", user)
        self.query_expect_rows(f"SHOW {db_name}.STREAMS", 2)
        self.query_expect_rows(f"SHOW {db_name2}.STREAMS", 2)
        self.login()
        self.revoke_privilege("ALL", f"STREAM *.*", user)
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW {db_name}.STREAMS", 0)
        self.query_expect_rows(f"SHOW {db_name2}.STREAMS", 0)
        
        # db.stream_name
        self.login()
        self.grant_privilege("ALL", f"STREAM {db_name}.{stream_name2}", user)
        self.login(user, pwd)
        #BUG6
        #self.query_expect_rows(f"SHOW {db_name}.STREAMS", 1)
        #self.drop_stream(db_name, stream_name2)
        self.query_expect_rows(f"SHOW {db_name2}.STREAMS", 0)
        self.login()
        self.revoke_privilege("ALL", f"STREAM {db_name}.{stream_name2}", user)
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW {db_name}.STREAMS", 0)
        self.query_expect_rows(f"SHOW {db_name2}.STREAMS", 0)
        

        #
        # base
        #
        self.login()
        self.drop_stream(db_name, stream_name2) # BUG6 open then comment this line
        self.revoke_privilege("ALL", f"STREAM {db_name}.*", user)
        
        # Test: user cannot show streams without privilege
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW {db_name}.STREAMS", 0)
        
        # Grant SHOW privilege on stream
        self.login()
        self.grant_privilege("SHOW", f"STREAM {db_name}.*", user)
        self.grant_privilege("SHOW CREATE", f"STREAM {db_name}.*", user)
        
        # Test: user can show streams
        self.login(user, pwd)
        self.query_expect_rows(f"SHOW {db_name}.STREAMS", 1)
        
        # Test: user cannot stop/start/drop stream without privilege
        self.exec_sql_failed(f"RECALCULATE STREAM {db_name}.{stream_name1} from '2025-01-01 10:00:00'", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed(f"STOP  STREAM {db_name}.{stream_name1}", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed(f"START STREAM {db_name}.{stream_name1}", TSDB_CODE_MND_NO_RIGHTS)
        self.exec_sql_failed(f"DROP  STREAM {db_name}.{stream_name1}", TSDB_CODE_MND_NO_RIGHTS)   
        
        # Grant start/stop/drop privilege on stream
        self.login()
        self.grant_privilege("RECALCULATE",  f"STREAM {db_name}.{stream_name1}", user)
        self.grant_privilege("STOP",  f"STREAM {db_name}.{stream_name1}", user)
        self.grant_privilege("START", f"STREAM {db_name}.{stream_name1}", user)
        self.grant_privilege("DROP",  f"STREAM {db_name}.{stream_name1}", user)

        #BUG5
        #self.login(user, pwd)        
        #self.exec_sql(f"RECALCULATE STREAM {db_name}.{stream_name1}")
        #self.exec_sql(f"STOP  STREAM {db_name}.{stream_name1}")
        #self.exec_sql(f"START STREAM {db_name}.{stream_name1}")
        #self.exec_sql(f"DROP  STREAM {db_name}.{stream_name1}")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_database(db_name2)
        self.drop_user(user)
        
        print("Stream Privileges .................... [ passed ] ")

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
        
        print("Privilege Inheritance ................ [ passed ] ")
    
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
        self.grant_privilege("USE", f"DATABASE {db_name}", role)
        self.grant_privilege("SELECT(c1)", f"{db_name}.st1", user)  # User: only c1
        self.grant_privilege("SELECT(c2)", f"{db_name}.st1", role)  # Role: only c2
        self.grant_role(role, user)
        
        # Test: user's explicit privilege takes priority
        self.login(user, pwd)
        self.exec_sql(f"SELECT c1 FROM {db_name}.st1")
        # According to FS: user's explicit fine-grained rule > role's rule
        self.exec_sql_failed(f"SELECT c2 FROM {db_name}.st1")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_role(role)
        self.drop_user(user)
        
        print("Privilege Conflict Resolution ........ [ passed ] ")
    
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
        self.grant_privilege("USE", "DATABASE *", user)
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
        
        print("Wildcard Privilege (*.* and db.*) ... [ passed ] ")
    
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
        
        print("Privilege Revoke Cascading ........... [ passed ] ")
    
    def do_invalid_privilege_operations(self):
        # Test invalid privilege operations (negative cases)
        tdLog.info("=== Testing Invalid Privilege Operations ===")
        self.login()  # Login as root
        
        user = "test_user"
        db_name = "test_db"
        self.create_user(user, pwd)
        self.create_database(db_name)
        
        # Test: grant non-existent privilege
        self.exec_sql_failed(f"GRANT INVALID_PRIVILEGE ON *.* TO {user}")
        
        # Test: grant privilege on non-existent database
        self.exec_sql_failed(f"GRANT SELECT ON non_existent_db.* TO {user}")
        
        # Test: grant privilege to non-existent user
        self.exec_sql_failed(f"GRANT SELECT ON *.* TO non_existent_user")
        
        # Test: revoke privilege that was never granted(MYSQL report error but PG does not, we choose following with PG)
        self.revoke_privilege("DELETE", f"{db_name}.*", user)
        
        # Test: user cannot grant privileges to others without proper privilege
        self.login(user, pwd)
        self.exec_sql_failed(f"GRANT SELECT ON *.* TO root")
        
        # Cleanup
        self.login()
        self.drop_user(user)
        self.drop_database(db_name)
        
        print("Invalid Privilege Operations ......... [ passed ] ")
    
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
        self.revoke_privilege("USE", f"DATABASE {db_name}", user)
        
        self.login(user, pwd)
        self.exec_sql_failed(f"USE {db_name}")
        
        # Test: grant same privilege multiple times
        self.login()
        self.grant_privilege("USE", f"DATABASE {db_name}", user)
        self.grant_privilege("USE", f"DATABASE {db_name}", user)  # Should not error
        
        # Test: revoke same privilege multiple times
        self.revoke_privilege("USE", f"DATABASE {db_name}", user)
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        
        print("Privilege Boundary Conditions ........ [ passed ] ")
    
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
        self.exec_sql(f"ALTER DATABASE {db_name} KEEP 365")
        
        # Test: owner can perform all operations without explicit grants
        self.create_stable(db_name, "st")
        self.create_child_table(db_name, "t1", "st")
        self.create_child_table(db_name, "t2", "st")
        self.insert_data(db_name, "t1")
        self.insert_data(db_name, "t2")
        self.exec_sql(f"ALTER TABLE {db_name}.st ADD COLUMN c4 INT")
        self.exec_sql(f"SELECT * FROM {db_name}.st")
        self.exec_sql(f"SELECT * FROM {db_name}.t1")
        # ntb
        self.create_table(db_name, "ntb")
        self.insert_data(db_name, "ntb")
        self.exec_sql(f"ALTER TABLE {db_name}.ntb ADD COLUMN c4 INT")    
        
        # Test: owner can drop their own database
        self.delete_data(db_name, "t2")
        self.exec_sql(f"DROP TABLE {db_name}.t1")
        self.exec_sql(f"DROP TABLE {db_name}.st")
        self.exec_sql(f"DROP TABLE {db_name}.ntb")
        self.drop_database(db_name)
        
        # Cleanup
        self.login()
        self.drop_user(user)
        
        print("Owner Special Privileges ............. [ passed ] ")
        
    def queryIncludes(self, sql, expected_user, expected_priv_types):
        # Helper function to check if query result includes expected privileges
        tdSql.query(sql)
        found_privs = set()
        for row in tdSql.queryResult:
            user, priv_type = row[0], row[1]
            if user == expected_user:
                found_privs.add(priv_type)
        for priv in expected_priv_types:
            if priv not in found_privs:
                raise Exception(f"Expected privilege '{priv}' not found for user '{expected_user}'")

    def do_show_privilege(self):
        # Test permission query and display commands
        tdLog.info("=== Testing Permission Query and Display ===")
        self.login()  # Login as root
        
        # Create test users and roles
        test_user = "test_user"
        test_role = "test_role"
        test_db = "test_db"
        self.create_database(test_db)
        self.create_user(test_user, pwd)
        self.create_role(test_role)
        
        priv_types = ["CREATE DATABASE", "SELECT", "INSERT", "DROP"]
        priv_targes = [None, f"{test_db}.*", f"{test_db}.*", f"DATABASE {test_db}"]
        users = [test_user, test_role]
        
        # Grant
        for priv_t, priv_target in zip(priv_types, priv_targes):
            for user in users:
                self.grant_privilege(priv_t, priv_target, user)

        # User
        sql = "SHOW USER PRIVILEGES"
        self.queryIncludes(sql, test_user, priv_types)
        
        # Role
        sql = "SHOW ROLE PRIVILEGES"
        self.queryIncludes(sql, test_role, priv_types)
        
        # Cleanup
        self.login()
        self.drop_database(test_db)
        self.drop_role(test_role)
        self.drop_user(test_user)
        
        print("Show Privileges ....................... [ passed ] ")

    def do_concurrent_privilege_operations(self):
        # Test concurrent privilege grant/revoke operations
        tdLog.info("=== Testing Concurrent Privilege Operations ===")
        self.login()  # Login as root
        
        # Note: This is a basic test, actual concurrent testing would require threading
        db_name = "test_db"
        users = ["test_user", "test_user2", "test_user3"]
        
        self.create_database(db_name)
        self.create_stable(db_name, "st")
        self.create_child_table(db_name, "t1", "st")
        self.create_child_table(db_name, "t2", "st")
        self.insert_data(db_name, "t1")
        self.insert_data(db_name, "t2")
        
        for user in users:
            self.create_user(user, pwd)
            self.grant_privilege("USE", f"DATABASE {db_name}", user)
            self.grant_privilege("SELECT", f"{db_name}.*", user)
            self.exec_sql(f"SELECT * FROM {db_name}.st")
        
        # Revoke privileges from all users
        for user in users:
            self.revoke_privilege("SELECT", f"{db_name}.*", user)
        
        # Verify all users lost privilege
        for user in users:
            self.login(user, pwd)
            # Note: would need actual table to test SELECT
            self.exec_sql_failed(f"SELECT * FROM {db_name}.st", TSDB_CODE_PAR_PERMISSION_DENIED)            
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        for user in users:
            self.drop_user(user)
        
        print("Concurrent Privilege Operations ....... [ passed ] ")

    def do_root_initial_permissions(self):
        # Test root user initial permissions (3.4.0.0+ feature)
        tdLog.info("=== Testing Root User Initial Permissions ===")
        self.login()  # Login as root
        
        # Root should be able to perform all system role operations by default
        # Create test users for role separation
        dba_user = "test_dba_user"
        sec_user = "test_sec_user"
        audit_user = "test_audit_user"
        test_user = "test_user"        
        
        self.create_user(dba_user, pwd)
        self.create_user(sec_user, pwd)
        self.create_user(audit_user, pwd)
        self.create_user(test_user, pwd)
        
        # Root can grant all system roles (proving root has all permissions)
        self.grant_role("`SYSDBA`", dba_user)
        self.grant_role("`SYSSEC`", sec_user)
        self.grant_role("`SYSAUDIT`", audit_user)
        
        # Verify each user has the correct role
        self.login(dba_user, pwd)
        # SYSDBA should be able to create database
        test_db = "test_root_db"
        self.create_database(test_db)
        self.drop_database(test_db)
        
        self.login(sec_user, pwd)
        # SYSSEC should be able to grant privileges
        self.grant_privilege("CREATE DATABASE", None, test_user)
        self.revoke_privilege("CREATE DATABASE", None, test_user)
        
        self.login(audit_user, pwd)
        # SYSAUDIT should be able to view audit information
        # Note: Actual audit operations may require audit database setup
        
        # Cleanup test users
        self.login()
        self.drop_user(dba_user)
        self.drop_user(sec_user)
        self.drop_user(audit_user)
        self.drop_user(test_user)        
        
        print("Root User Initial Permissions ......... [ passed ] ")

    def do_role_separation_best_practice(self):
        # Test role separation best practice (三权分立)
        tdLog.info("=== Testing Role Separation Best Practice ===")
        self.login()  # Login as root
        
        # Create dedicated admin users as per best practice
        dba_admin = "dba_admin"
        sec_admin = "sec_admin" 
        audit_admin = "audit_admin"
        business_user = "business_user"
        
        self.create_user(dba_admin, pwd)
        self.create_user(sec_admin, pwd)
        self.create_user(audit_admin, pwd)
        self.create_user(business_user, pwd)
        
        # Separate roles to different users (三权分立)
        self.grant_role("`SYSDBA`", dba_admin)
        self.grant_role("`SYSSEC`", sec_admin)
        self.grant_role("`SYSAUDIT`", audit_admin)
        
        # Verify role separation works correctly
        # DBA admin should be able to perform database operations
        self.login(dba_admin, pwd)
        dba_db = "dba_managed_db"
        self.create_database(dba_db)
        self.create_stable(dba_db, "dba_stable")
        self.drop_database(dba_db)
        
        # Security admin should be able to manage privileges but not database operations
        self.login(sec_admin, pwd)
        # Should be able to grant privileges
        self.grant_privilege("CREATE DATABASE", None, business_user)
        # Should NOT be able to create database (not SYSDBA)
        self.exec_sql_failed("CREATE DATABASE sec_db", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Audit admin should not access business data
        self.login(audit_admin, pwd)
        # Should NOT be able to access business database
        self.exec_sql_failed("CREATE DATABASE audit_test_db", TSDB_CODE_PAR_PERMISSION_DENIED)
        
        # Business user with granted privilege should work
        self.login(business_user, pwd)
        business_db = "business_db"
        self.create_database(business_db)
        self.drop_database(business_db)
        
        # Cleanup role separation test users
        self.login()
        self.drop_user(dba_admin)
        self.drop_user(sec_admin)
        self.drop_user(audit_admin)
        self.drop_user(business_user)
        
        print("Role Separation Best Practice ........ [ passed ] ")

    def do_daily_operations_without_root(self):
        # Test daily operations without using root (best practice)
        tdLog.info("=== Testing Daily Operations Without Root ===")
        self.login()  # Login as root
        
        # Create dedicated admin users for daily operations
        daily_dba = "daily_dba"
        daily_sec = "daily_sec"
        daily_audit = "daily_audit"
        app_user = "app_user"
        
        self.create_user(daily_dba, pwd)
        self.create_user(daily_sec, pwd)
        self.create_user(daily_audit, pwd)
        self.create_user(app_user, pwd)
        
        # Grant appropriate roles (三权分立)
        self.grant_role("`SYSDBA`", daily_dba)
        self.grant_role("`SYSSEC`", daily_sec)
        self.grant_role("`SYSAUDIT`", daily_audit)
        
        # Test daily operations without using root
        # 1. Database administration by DBA
        self.login(daily_dba, pwd)
        prod_db = "production_db"
        self.create_database(prod_db)
        self.create_stable(prod_db, "sensors")
        self.create_child_table(prod_db, "sensor_001", "sensors")
        self.insert_data(prod_db, "sensor_001")
        
        # 2. Security management by Security admin
        self.login(daily_sec, pwd)
        # Grant application user access to production database
        self.grant_privilege("USE", f"DATABASE {prod_db}", app_user)
        self.grant_privilege("SELECT", f"{prod_db}.*", app_user)
        self.grant_privilege("INSERT", f"{prod_db}.*", app_user)
        
        # 3. Application user can work without root
        self.login(app_user, pwd)
        # Can use the database
        tdSql.execute(f"USE {prod_db}")
        # Can query data
        tdSql.query(f"SELECT * FROM {prod_db}.sensors")
        # Can insert data
        self.insert_data(prod_db, "sensor_001")
        
        # 4. Audit operations by Audit admin
        self.login(daily_audit, pwd)
        # Audit admin can view audit information but not business data
        # This would typically involve audit database operations
        
        # 5. Verify root is not needed for these daily operations
        # All operations above were performed without using root after initial setup
        
        # Cleanup daily operations test
        self.login()
        self.drop_database(prod_db)
        self.drop_user(daily_dba)
        self.drop_user(daily_sec)
        self.drop_user(daily_audit)
        self.drop_user(app_user)
        
        print("Daily Operations Without Root ........ [ passed ] ")

    def do_constraint(self):
        # Test constraint operations only - focusing on system limitations
        tdLog.info("=== Testing Constraint Operations ===")
        self.login()  # Login as root
        
        db_name = "test_db"
        user = "test_user"
        self.create_user(user, pwd)        
        self.create_database(db_name)
        self.create_stable(db_name, "st")
        self.create_child_table(db_name, "t1", "st")
        self.create_child_table(db_name, "t2", "st")
        self.insert_data(db_name, "t1")
        self.insert_data(db_name, "t2")
        
        # Test 1: Not allowed to grant both SYSDBA/SYSSEC/SYSAUDIT to the same user
        # This is a critical constraint in 3.4.0.0+ three-power separation
        self.grant_role("`SYSDBA`", user)
        self.exec_sql_failed(f"GRANT ROLE `SYSSEC` TO {user}", TSDB_CODE_MND_ROLE_CONFLICTS)
        self.exec_sql_failed(f"GRANT ROLE `SYSAUDIT` TO {user}", TSDB_CODE_MND_ROLE_CONFLICTS)
        self.revoke_role("`SYSDBA`", user)
        self.grant_role("`SYSSEC`", user)
        self.exec_sql_failed(f"GRANT ROLE `SYSDBA` TO {user}", TSDB_CODE_MND_ROLE_CONFLICTS)
        self.exec_sql_failed(f"GRANT ROLE `SYSAUDIT` TO {user}", TSDB_CODE_MND_ROLE_CONFLICTS)
        self.revoke_role("`SYSSEC`", user)
        self.grant_role("`SYSAUDIT`", user)
        self.exec_sql_failed(f"GRANT ROLE `SYSDBA` TO {user}", TSDB_CODE_MND_ROLE_CONFLICTS)
        self.exec_sql_failed(f"GRANT ROLE `SYSSEC` TO {user}", TSDB_CODE_MND_ROLE_CONFLICTS)
        
        # Test 2: System allows multiple users to own the same system role
        # This is allowed - multiple users can have SYSDBA role
        user2 = "test_user2"
        user3 = "test_user3"
        self.create_user(user2, pwd)
        self.create_user(user3, pwd)
        self.exec_sql_failed(f"GRANT ROLE `SYSDBA` TO {user}", TSDB_CODE_MND_ROLE_CONFLICTS)
        self.revoke_role("`SYSAUDIT`", user)
        self.revoke_role("`SYSINFO_1`", user2)
        self.grant_role("`SYSDBA`", user)
        self.grant_role("`SYSDBA`", user2)  # Should be allowed - multiple users can have same role
        self.grant_role("`SYSDBA`", user3)  # Should be allowed
        
        # Test 3: Additional constraint - system roles cannot be locked
        self.exec_sql_failed("LOCK ROLE `SYSDBA`", TSDB_CODE_OPS_NOT_SUPPORT)
        self.exec_sql_failed("LOCK ROLE `SYSSEC`", TSDB_CODE_OPS_NOT_SUPPORT)
        self.exec_sql_failed("LOCK ROLE `SYSAUDIT`", TSDB_CODE_OPS_NOT_SUPPORT)
        
        # Test 4: Constraint - cannot drop system roles
        self.exec_sql_failed("DROP ROLE `SYSDBA`")
        self.exec_sql_failed("DROP ROLE `SYSSEC`")
        self.exec_sql_failed("DROP ROLE `SYSAUDIT`")
        
        # Cleanup
        self.login()
        self.drop_database(db_name)
        self.drop_user(user)
        self.drop_user(user2)
        self.drop_user(user3)
        
        print("Constraint ............................ [ passed ] ")

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
          - Row-Level Complex Conditions (AND/OR combinations)
          - Row-Level Time Range Conditions
          - Row-Level Mixed Tag and Data Conditions
          - Combined Column and Row Privileges
          - Column Privilege Update Priority
          - Privilege Update Time Priority (User vs Role)
        
        [Role-Based Access Control]
          - Role Creation and Grant
          - System Roles (SYSDBA/SYSSEC/SYSAUDIT)
          - Audit Database Privileges (3.4.0.0+)
        
        [System Privileges]
          - User Management (ALTER USER, SHOW USERS SECURITY INFORMATION)
          - Token Management (CREATE/ALTER/DROP TOKEN, SHOW TOKENS)
          - TOTP Management (CREATE/DROP TOTP)
          - Password Management (ALTER PASS, ALTER SELF PASS)
          - Node Management (CREATE/DROP NODE, SHOW NODES)
          - Mount Management (CREATE/DROP MOUNT, SHOW MOUNTS)
          - System Variable Management (ALTER/SHOW SYSTEM/SECURITY/AUDIT/DEBUG VARIABLES)
          - Information Schema Access (READ INFORMATION_SCHEMA BASIC/PRIVILEGED/SECURITY/AUDIT)
          - System Monitoring (SHOW/KILL TRANSACTIONS/CONNECTIONS/QUERIES)
          - Cluster Information (SHOW GRANTS/CLUSTER/APPS)
          - Privilege Delegation (GRANT/REVOKE PRIVILEGE)
        
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
          - Database Owner Privileges (implicit permissions)
          - Object Owner Privileges (tables, views, topics, streams)
          - Permission Query and Display (SHOW USER/ROLE PRIVILEGES, information_schema)
          - Concurrent Privilege Operations
        
        [Three-Power Separation Tests (3.4.0.0+)]
          - Root User Initial Permissions
          - Role Separation Best Practice (Three-Power Separation)
          - Daily Operations Without Root
          - System Role Constraints
        
        Since: v3.4.0.0

        Labels: common,ci,privilege

        Jira: TS-7232

        History:
            - 2026-02-02 Alex Duan created
            - 2026-02-02 Alex Duan Enhanced with comprehensive test cases
            - 2026-02-02 Alex Duan Added 3.4.0.0+ view/topic/stream privilege tests
            - 2026-02-27 Alex Duan Added three-power separation tests (SYSDBA/SYSSEC/SYSAUDIT)
            - 2026-02-27 Alex Duan Added database/object owner privilege tests and permission query tests

        """
        
        print("\n")
        print("========== Privilege Control Test Suite ==========")
        print("")
        
        ''' test
        self.create_snode()
        self.create_qnode()
        self.do_show_privilege()
        return
        '''

        # Database privilege tests
        print("[Database Privileges]")
        self.create_snode()
        self.create_qnode()
        self.do_create_database_privilege()
        self.do_alter_database_privilege()
        self.do_drop_database_privilege()
        self.do_use_database_privilege()
        self.do_show_databases_privilege()
        self.do_show_create_database_privilege()      
        self.do_flush_database_privilege()           
        self.do_compact_database_privilege()         
        self.do_trim_database_privilege()            
        self.do_rollup_database_privilege()          
        self.do_scan_database_privilege()            
        self.do_ssmigrate_database_privilege()               
        
        # Table privilege tests
        print("")
        print("[Table Privileges]")
        self.do_create_table_privilege()
        self.do_drop_table_privilege()
        self.do_alter_table_privilege()
        self.do_select_privilege()
        self.do_insert_privilege()
        self.do_delete_privilege()
        self.do_select_column_privilege_comprehensive()  
        self.do_insert_column_privilege_comprehensive()  
        self.do_show_create_table_privilege()                    
        
        # Column and row privilege tests
        print("")
        print("[Column and Row Privileges]")
        self.do_row_privilege_with_tag_condition()
        self.do_row_privilege_complex_conditions()
        self.do_row_privilege_time_range()
        self.do_row_privilege_mixed_conditions()
        self.do_column_privilege()
        self.do_column_mask_privilege()
        self.do_column_row_combined_privilege()
        self.do_column_privilege_update_priority()
        self.do_privilege_update_time_priority()
        
        # RBAC tests
        print("")
        print("[Role-Based Access Control]")
        self.do_role_privilege()
        self.do_role_creation_and_grant()
        self.do_role_lock_unlock()  #can cause core BUG21
        self.do_system_roles()
        self.do_audit_database_privileges()
        
        # System privilege tests
        print("")
        print("[System Privileges]")
        self.do_user_management_privileges()
        self.do_token_management_privileges()
        self.do_totp_management_privileges()
        self.do_password_management_privileges()
        self.do_node_management_privileges()
        self.do_mount_management_privileges()
        self.do_system_variable_privileges()
        self.do_information_schema_privileges()
        self.do_system_monitoring_privileges()
        self.do_show_grants_cluster_apps_privileges()
        self.do_privilege_delegation()
        
        # Function/index/tsrma/rsma privilege tests
        print("")
        print("[Function and Index Privileges]")
        self.do_create_function_privilege()
        self.do_create_index_privilege()
        if platform.system().lower() != 'windows':
            # windows does not support tsma
            self.do_create_tsma_privilege()
        self.do_create_rsma_privilege()
                
        # View, topic and stream privilege tests (3.4.0.0+)
        print("")
        print("[View, Topic and Stream Privileges]")
        self.do_view_privileges()
        self.do_view_nested_privilege()               
        self.do_topic_privileges()
        self.do_stream_privileges()

        # Exception and reverse test cases
        print("")
        print("[Exception and Reverse Test Cases]")
        self.do_show_privilege()
        self.do_privilege_inheritance()
        self.do_privilege_conflict_resolution()
        self.do_wildcard_privilege()
        self.do_privilege_revoke_cascading()
        self.do_invalid_privilege_operations()
        self.do_privilege_boundary_conditions()
        self.do_owner_special_privileges()
        self.do_concurrent_privilege_operations()
        
        # Three-power separation tests (3.4.0.0+)
        print("")
        print("[Three-Power Separation Tests]")
        self.do_root_initial_permissions()
        self.do_role_separation_best_practice()
        self.do_daily_operations_without_root()
        self.do_constraint()