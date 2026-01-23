
from new_test_framework.utils import tdLog, tdSql, TDSql, TDCom, etool

import datetime
import os
import pyotp
import time

class TestUserSecurity:
    @classmethod
    def setup_class(cls):
        cls.tdCom = TDCom()

    #
    # --------------------------- util ----------------------------
    #
    
    # create totp
    def create_security_key(self, userName):
        securityKey = tdSql.getFirstValue(f"create totp_secret for user {userName}")
        if len(securityKey) != 52:
            raise Exception(f"totp code length error: {securityKey} len={len(securityKey)}")
        created = tdSql.getFirstValue(f"select totp from information_schema.ins_users where name='{userName}' ")
        if created != 1:
            raise Exception(f"totp not created for user {userName} totp={created}")
        return securityKey
    
    def drop_security_key(self, userName):
        print(f"drop totp_secret from user {userName}")
        tdSql.execute(f"drop totp_secret from user {userName}")
        dropped = tdSql.getFirstValue(f"select totp from information_schema.ins_users where name='{userName}' ")
        if dropped != 0:
            raise Exception(f"totp not dropped for user {userName} totp={dropped}")

    def drop_security_key_failed(self, userName):
        try:
            self.drop_security_key(userName)
        except Exception as e:
            tdLog.info(f"drop totp fail as expected: {e} user:{userName}")
            return
        raise Exception(f"drop totp success, but expected to fail. user:{userName}")
    
    def create_security_key_fail(self, userName):
        tdSql.error(f"create totp_secret for user {userName}")
    
    def check_login(self, userName, password, totpCode):
        # TODO: use connect_with_totp api after add the support
        taosFile = etool.taosFile()
        command = f"echo '{totpCode}' | {taosFile} -u{userName} -p{password} -s 'show databases;' "
        rlist = etool.runRetList(command, checkRun=True, show= True)
        self.checkListString(rlist, "Query OK,")
        
    def check_login_fail(self, userName, password, totpCode):
        try:
            self.check_login(userName, password, totpCode)
        except Exception as e:
            tdLog.info(f"login fail as expected: {e} user:{userName}")
            return
        raise Exception(f"login success, but expected to fail. user:{userName}")
    
    def get_totp_code(self, secretKey, interval = 30):
        totp = pyotp.TOTP(secretKey, interval=interval)
        totpCode = totp.now()
        timeRemaining = interval - (int(time.time()) % interval)
        
        tdLog.info(f"TOTP code: {totpCode}")
        tdLog.info(f"Remain: {timeRemaining} s")
        tdLog.info(f"Generate time: {datetime.datetime.now().strftime('%H:%M:%S')}")
        return totpCode
    
    def create_user(self, user, password=None, options=""):
        if password is None:
            password = "abcd@1234" # default password
        sql = f"CREATE USER {user} pass '{password}' {options}"
        tdSql.execute(sql)

    #
    # --------------------------- totp ----------------------------
    #
    def login_with_key(self, userName, password, secretKey):
        totpCode = self.get_totp_code(secretKey)
        self.check_login(userName, password, totpCode)
    
    def login_with_key_fail(self, userName, password, secretKey):
        try:
            self.login_with_key(userName, password, secretKey)
        except Exception as e:
            tdLog.info(f"login fail as expected: {e} user:{userName}")
            return
        raise Exception(f"login success, but expected to fail. user:{userName}")
  
    def login_with_user(self, userName, password):
        secretKey = self.create_security_key(userName)
        totpCode = self.get_totp_code(secretKey)
        self.check_login(userName, password, totpCode)    
    
    def check_duplicate_key(self, userName):
        keys = []
        for i in range(100):
            key = self.create_security_key(userName)
            if key in keys:
                raise Exception(f"duplicate totp key found: {key} at {i} time")
            keys.append(key)
    
    # prepare
    def prepare(self):
        pass

    # create
    def do_create_totp(self):
        password = "abcd@1234"
        
        # default user
        user = "user_default"
        self.create_user(user, password)
        key = self.create_security_key(user)
        self.login_with_key(user, password, key)

        # enable 0
        user = "user_enable_0"
        self.create_user(user, password, "enable 0")
        key = self.create_security_key(user)
        self.login_with_key_fail(user, password, key)
        
        # sysinfo 0
        user = "user_sysinfo_0"
        self.create_user(user, password, "sysinfo 0")
        self.login_with_user(user, password)
        
        # duplicate create
        self.check_duplicate_key("user_default")
        
        # except
        self.create_security_key_fail("non_exist_user") # non-exist user
        self.create_security_key_fail("")               # empty  
        self.create_security_key_fail("select")         # keyword
        self.create_security_key_fail("user_with_long_name_exceeding_limit_123456") # exceed length limit
        
        print("create totp ........................... [ passed ] ")
        

    # alter
    def do_alter_totp(self):
        user = "user_default"
        password = "abcd@1234"
        
        old_key = self.create_security_key(user)
        self.login_with_key(user, password, old_key)
        # alter
        new_key = self.create_security_key(user)
        if old_key == new_key:
            raise Exception(f"alter totp get same key: {old_key}")
        # login with new key
        self.login_with_key(user, password, new_key)
        # login with old key fail
        self.login_with_key_fail(user, password, old_key)
        
        print("alter totp ............................ [ passed ] ")

    # delete
    def do_delete_totp(self):
        
        key = self.create_security_key("user_default")
        print(f"delete totp key: {key}")
        
        # drop basic
        self.drop_security_key_failed("root")
        self.drop_security_key("user_enable_0")
        # duplicate drop
        self.drop_security_key("user_default")
        self.drop_security_key_failed("user_default")
       
        # except
        print(f"delete totp except")
        self.drop_security_key_failed("non_exist_user") # non-exist user
        self.drop_security_key_failed("")               # empty
        self.drop_security_key_failed("`root`")         # with quotes
        self.drop_security_key_failed("select")         # keyword
        self.drop_security_key_failed("user_with_long_name_exceeding_limit_123456") # exceed length limit
        
        print("delete totp ........................... [ passed ] ")


    # login
    def do_login(self):
        #
        # normal login
        #
        self.login_with_user("root", "taosdata")
        
        #
        # except
        #
        user = "user1"
        self.create_user(user, "abcd@1234", "createdb 1")
        security_key = self.create_security_key(user)
        totp = pyotp.TOTP(security_key, interval=30)

        code = totp.now()
        self.check_login_fail("non_exist_user", "abcd@1234", "123456")    # non-exist user
        self.check_login_fail("user1", "wrong_password", code)            # wrong password
        self.check_login_fail("", "abcd@1234", "123456")                  # empty user
        self.check_login_fail("user1", "", code)                          # empty password
        self.check_login_fail("user1", "abcd@1234", "")                   # empty totp
        self.check_login_fail("user1", "abcd@1234", "123456")             # wrong totp
        
        #
        # fault tolerance test
        #
        # if we are at the last 10 second of the current interval , wait until we are into
        # the new interval, otherwise, the following tests may be flaky.
        #
        if (time.time() % totp.interval) > (totp.interval - 10):
            time_to_wait = totp.interval - time.time() % totp.interval + 1
            print(f"wait {time_to_wait:.1f}s to cross interval boundary")
            time.sleep(time_to_wait)

        # totp code of current interval should work
        code = totp.now()
        self.check_login(user, "abcd@1234", code)

        # totp code of previous interval should work
        code = totp.at(int(time.time()) - totp.interval)
        self.check_login(user, "abcd@1234", code)

        # totp code before the previous interval should fail
        code = totp.at(int(time.time()) - 2 * totp.interval)
        self.check_login_fail(user, "abcd@1234", code)

        # totp code of next interval should work
        code = totp.at(int(time.time()) + totp.interval)
        self.check_login(user, "abcd@1234", code)

        # totp code after the next interval should fail
        code = totp.at(int(time.time()) + 2 * totp.interval)
        self.check_login_fail(user, "abcd@1234", code)
        
        print("login totp ............................ [ passed ] ")

    #
    # --------------------------- main ----------------------------
    #
    def test_user_totp(self):
        """User totp login

        1. Create TOTP
            - super root account create key
            - default user account create key
            - disabled user account create key
            - sysinfo disabled user account create key
            - check duplicate create key
            - exception cases
        2. Alter TOTP
            - alter totp for default user
            - login with new totp
            - login with old totp fail
        3. Delete TOTP
            - drop totp for super root account
            - drop totp for disabled user account
            - duplicate drop totp
            - login fail after drop totp
            - exception cases
        4. Login with TOTP
            - exception cases: 
                - non-exist user
                - wrong password
                - empty user
                - empty password
                - empty totp code
                - wrong totp code
            - totp code expired (30s)
        
        Since: v3.4.0.0

        Labels: common,ci,user

        Jira: None

        History:
            - 2026-01-12 Alex Duan created

        """
        self.prepare()
        self.do_create_totp()
        self.do_alter_totp()
        self.do_delete_totp()
        self.do_login()
