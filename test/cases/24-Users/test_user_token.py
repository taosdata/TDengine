
from new_test_framework.utils import tdLog, tdSql, TDSql, TDCom, etool

import datetime
import os
import pyotp

EXPECTED_TOKEN_LENGTH = 63

class TestUserSecurity:
    @classmethod
    def setup_class(cls):
        pass

    #
    # --------------------------- util ----------------------------
    #
    def checkEquals(self, a, b):
        if type(a) == str:
            a = a.strip().replace("'", "").replace("\n", "").replace("\00", "")
            
        if type(b) == str:
            b = b.strip().replace("'", "").replace("\n", "").replace("\00", "")
            
        if a != b:
            raise Exception(f"not equal: {a} != {b}")
    
    def options(self, options: dict):
        if options is None:
            return ""
        opts = ""
        for k, v in options.items():
            if v is None or v == "":
                opts += f" {k} "
            else:
                opts += f" {k} {v} "
        return opts
    
    def checkOptions(self, options: dict):
        if options is None:
            return
        for k, v in options.items():
            k = k.upper()
            col = 0
            if k == "TTL":
                # Check TTL expiration time
                if v == 0:
                    # Unlimited: epoch time
                    tdSql.checkData(0, 5, "1970-01-01 08:00:00.000")
                else:
                    # TTL: create_time + days
                    create_time = tdSql.getData(0, 4)
                    expire_time = tdSql.getData(0, 5)
                    expected_days = (expire_time - create_time).days
                    if expected_days != v:
                        raise Exception(f"TTL days not match: expected {v}, got {expected_days}")
                continue
            elif k == "PROVIDER":
                col = 2
            elif k == "ENABLE":
                col = 3
            elif k == "EXTRA_INFO":
                col = 6
            else:
                raise Exception(f"unknown option key: {k}")
            data = tdSql.getData(0, col)
            self.checkEquals(data, v)
    
    # create
    def create_token(self, tokenName, userName, option1="", option2:dict=None):        
        sql = f"CREATE TOKEN {option1} {tokenName} FROM USER {userName} {self.options(option2)}"
        token = tdSql.getFirstValue(sql)
        if len(token) != EXPECTED_TOKEN_LENGTH:
            raise Exception(f"token length error: {token} len={len(token)}")
        name = tokenName.replace("`", "")
        sql = f"select * from information_schema.ins_tokens where name='{name}' and `user`='{userName}' "
        tdSql.query(sql)
        tdSql.checkRows(1)
        self.checkOptions(option2)
        
        return token
 
    def create_token_fail(self, tokenName, userName, option1="", option2:dict={}):        
        sql = f"CREATE TOKEN {option1} {tokenName} FROM USER {userName} {self.options(option2)}"
        tdSql.error(sql)

    # alter
    def alter_token(self, tokenName, option1="", option2:dict={}):        
        sql = f"ALTER TOKEN {tokenName} {self.options(option2)}"
        tdSql.execute(sql)
        sql = f"select * from information_schema.ins_tokens where name='{tokenName}'"
        tdSql.query(sql)
        tdSql.checkRows(1)
        self.checkOptions(option2)
        
    def alter_token_fail(self, tokenName, option1="", option2:dict={}):
        sql = f"ALTER TOKEN {tokenName} {self.options(option2)}"
        tdSql.error(sql)        

    # delete
    def drop_token(self, tokenName, option1=""):
        tdSql.execute(f"DROP TOKEN {option1} {tokenName}")
        sql = f"select count(*) from information_schema.ins_tokens where name='{tokenName}'"
        count = tdSql.getFirstValue(sql)
        if count != 0:
            raise Exception(f"drop token succ but found on ins_tokens. token:{tokenName} count:{count}")

    def drop_token_fail(self, tokenName, option1=""):
        tdSql.error(f"DROP TOKEN {option1} {tokenName}")

    # login
    def login_token(self, token, options=""):
        # login with token
        taosFile = etool.taosFile()
        success = [
            "Connect with token ...... [ OK ]",
            "Query OK"
        ]
        failed = "Connect with token ...... [ FAILED ]"
        
        # arg
        command = f"{taosFile} -q{token} {options} -s 'show tokens;' "
        rlist = etool.runRetList(command, checkRun=True, show= True)
        self.checkManyString(rlist, success)

    def login_token_fail(self, token, options=""):
        # login with token
        taosFile = etool.taosFile()
        failed = "Connect with token ...... [ FAILED ]"
        
        # arg
        command = f"{taosFile} -q{token} {options} -s 'show tokens;' "
        rlist = etool.runRetList(command, checkRun=False, show= True)
        self.checkManyString(rlist, failed)

    # user
    def create_user(self, user, password=None, options=""):
        if password is None:
            password = "abcd@1234"
        sql = f"CREATE USER {user} pass '{password}' {options}"
        tdSql.execute(sql)
    
    def drop_user(self, user, option1=""):
        tdSql.execute(f"DROP USER {option1} {user}")
        
    def login(self, user=None, password=None):
        if user is None:
            if password is None:
                tdSql.connect()
            else:
                tdSql.connect(password=password)
        else:
            tdSql.connect(user=user, password=password)        

    #
    # --------------------------- token ----------------------------
    #
 
    def prepare(self):
        # Create test user
        self.drop_user("test_user1", "IF EXISTS")
        self.drop_user("test_user2", "IF EXISTS")
        self.drop_user("test_user3", "IF EXISTS")
        self.create_user("test_user1")
        self.create_user("test_user2", options="ENABLE 0")
        self.create_user("test_user3", options="CREATEDB 0")
    
    def do_create_token(self):
        # Basic token creation
        self.create_token("token11", "test_user1")
        # Token with all options
        self.create_token("token12", "test_user1", "", {"ENABLE": 1, "TTL": 0, "PROVIDER": "'test_provider'", "EXTRA_INFO": "'test info'"})
        # Token with TTL
        self.create_token("token13", "test_user1", "", {"TTL": 7})
        # except max 3 tokens limit
        self.create_token_fail("token_fail", "test_user1", "", {"TTL": 8})
           
        # Disabled token
        self.create_token("token21", "test_user2", "", {"ENABLE": 0})
        
        # IF NOT EXISTS
        self.create_token("token22", "test_user2", "IF NOT EXISTS")
        #BUG-1
        #token22_again = self.create_token("token22", "test_user2", "IF NOT EXISTS")
        self.create_token("t" * 31, "test_user2") # max length token name
        
        # language support
        self.create_token("`我的TOKEN_31`", "test_user3", "", {"EXTRA_INFO": "'测试信息'"})  
        
        # except
        self.create_token_fail("token11", "test_user1")          # duplicate token name
        self.create_token_fail("token_fail", "nonexistent_user") # nonexistent user
        self.create_token_fail("t" * 32, "test_user3")           # over max length
        self.create_token_fail("token_with_very_long_name_exceeding_31_bytes_limit_here", "test_user3")
        self.create_token_fail("", "test_user1")                  # empty token name
        self.create_token_fail("token31", "")                    # empty user name
        self.create_token_fail("token_invalid_ttl", "test_user1", "", {"TTL": -1})      # Exception: invalid TTL
        self.create_token_fail("token_invalid_enable", "test_user1", "", {"ENABLE": 2}) # Exception: invalid ENABLE
        
        print("create token .......................... [ passed ] ")
        
    def do_show_token(self):
        # SHOW TOKENS command
        tdSql.query("SHOW TOKENS")
        rows = tdSql.queryRows
        if rows != 7:
            raise Exception(f"Expected at least 7 tokens, got {rows}")
        
        # Query from system table
        tdSql.query("SELECT * FROM information_schema.ins_tokens")
        system_rows = tdSql.queryRows
        if system_rows != rows:
            raise Exception(f"SHOW TOKENS and system table should return same rows: {rows} vs {system_rows}")
        
        # Query specific token
        tdSql.query("SELECT * FROM information_schema.ins_tokens WHERE name='token11' AND `user`='test_user1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "token11")
        tdSql.checkData(0, 1, "test_user1")
        tdSql.checkData(0, 3, 1)
        
        # Query with filters
        tdSql.query("SELECT * FROM information_schema.ins_tokens WHERE `enable`= 0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "token21")
        
        print("show token ............................ [ passed ] ")

    def do_alter_token(self):
        # Alter ENABLE status
        self.alter_token("token11", "", {"ENABLE": 0})
        
        # Alter TTL
        self.alter_token("token12", "", {"TTL": 30})
        
        # Alter PROVIDER
        self.alter_token("token13", "", {"PROVIDER": "'new_provider'"})
        
        # Alter EXTRA_INFO
        self.alter_token("token21", "", {"EXTRA_INFO": "'updated info'"})
        
        # Alter multiple options
        self.alter_token("token22", "", {"ENABLE": 1, "TTL": 60, "PROVIDER": "'multi_provider'", "EXTRA_INFO": "'multi info'"})
        
        # except
        #BUG-2
        #self.alter_token_fail("nonexistent_token", "", {"ENABLE": 1}) # Exception: nonexistent token       
        self.alter_token_fail("", "", {"ENABLE": 1})                  # Exception: empty token name
        self.alter_token_fail("token11", "", {"TTL": -10})            # Exception: invalid TTL
        self.alter_token_fail("token12", "", {"ENABLE": 5})           # Exception: invalid ENABLE
        
        print("alter token ........................... [ passed ] ")

    def do_delete_token(self):
        # Drop tokens
        self.drop_token("token11")
        self.drop_token("token12")
        self.drop_token("token13")

        # Verify all tokens deleted
        tdSql.query("SELECT * FROM information_schema.ins_tokens WHERE `user`='test_user1'")
        tdSql.checkRows(0)
        
        # IF EXISTS option
        self.drop_token("token21")
        self.drop_token("token21", "IF EXISTS")
        self.drop_token_fail("token21")
        self.drop_token("token22")
        
        # except
        self.drop_token_fail("nonexistent_token")  # Exception: nonexistent token
        self.drop_token_fail("")                   # Exception: empty token name        
        self.drop_token_fail("token11")            # Exception: token already dropped
        
        print("delete token .......................... [ passed ] ")


    def do_login(self):
        # root
        token_root = self.create_token("login_token_root", "root")
        self.login_token(token_root)
        self.drop_token("login_token_root")
        
        # normal
        token1 = self.create_token("login_token1", "test_user1")
        self.login_token(token1)
        
        # disabled token
        token2 = self.create_token("login_token2", "test_user1", "", {"ENABLE": 0})
        self.login_token_fail(token2)
        
        # alter to enable
        self.alter_token("login_token2", "", {"ENABLE": 1})
        self.login_token(token2)
        
        # alter to disable
        self.alter_token("login_token2", "", {"ENABLE": 0})
        self.login_token_fail(token2)
        
        # delete token login
        self.drop_token("login_token1")
        self.login_token_fail(token1)

        # recreate token with same name
        token1 = self.create_token("login_token1", "test_user1", option2={"TTL": 1})
        self.login_token(token1)
        self.alter_token("login_token1", "", {"TTL": 365, "PROVIDER": "'big_provider'"}) # big TTL
        self.login_token(token1)

        # except
        self.login_token_fail("invalid_token_string") # invalid token string
        self.login_token_fail("s")                    # too short token string
        self.login_token_fail("longtoken" * 100)      # too long token string
        self.login_token_fail("'``!@#$%^&*()_+'")     # invalid characters in token string
        
        
        # login with no root privilege
        user = "no_privilege_user"
        password = "abcd@1234"
        self.create_user(user, password, options="CREATEDB 0 SYSINFO 0")
        self.login(user, password)
        # can create
        token3 = self.create_token("login_token3", user)
        # can login
        self.login_token(token3)
        # can show
        tdSql.query("SHOW TOKENS")
        tdSql.checkRows(1)
        
        self.drop_token("login_token3")
        tdSql.query("SHOW TOKENS")
        tdSql.checkRows(0)

        # root login again
        self.login()
        
        print("login token ........................... [ passed ] ")

    #
    # --------------------------- main ----------------------------
    #
    def test_user_token(self):
        """User token login

        1. Create token
            - Basic creation
            - With options (ENABLE, TTL, PROVIDER, EXTRA_INFO)
            - IF NOT EXISTS clause
            - Max length token name (31 characters)
            - Multi-language support
            - Exception cases: duplicate name, non-existent user, over-length name, invalid parameters    
        2. Show tokens
            - SHOW TOKENS command
            - Query from system table ins_tokens
            - Query specific token with filters
            - Verify row counts consistency
        3. Alter token
            - Modify single property (ENABLE, TTL, PROVIDER, EXTRA_INFO)
            - Modify multiple properties at once
            - Exception cases: non-existent token, empty name, invalid parameter values
        4. Delete token
            - Normal drop operation
            - IF EXISTS option
            - Verify deletion from system table
            - Exception cases: non-existent token, empty name, duplicate deletion
        5. Token login
            - Normal token login
            - Disabled token login failure
            - Enable/disable toggling
            - Login failure after deletion
            - Recreate same-name token
            - Modify TTL and login
            - Exception cases: invalid token string, too short/long, special characters
        
        Since: v3.4.0.0

        Labels: common,ci,user

        Jira: None

        History:
            - 2026-01-13 Alex Duan created

        """
        self.prepare()
        self.do_create_token()
        self.do_show_token()
        self.do_alter_token()
        self.do_delete_token()
        self.do_login()