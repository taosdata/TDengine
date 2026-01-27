from new_test_framework.utils import tdLog, tdSql, TDCom, etool
import time
import threading
import socket

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 不会真的发包；只是让系统选择默认出口网卡
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()

def get_tomorrow():
    t = time.localtime(time.time() + 86400)
    return time.strftime("%Y-%m-%d", t)

def get_tomorrow_weekday_short():
    t = time.localtime(time.time() + 86400)
    return time.strftime("%a", t).upper()

class TestUserSecurity:
    @classmethod
    def setup_class(cls):
        cls.tdCom = TDCom()

    #
    # --------------------------- util ----------------------------
    #
    def login_failed(self, user=None, password=None):
        try:
            self.login(user, password)
        except Exception as e:
            tdLog.info(f"Login failed as expected: {e}")
            return
        raise Exception("Login succeeded but was expected to fail")
    
    def login(self, user=None, password=None):
        if user is None:
            if password is None:
                tdSql.connect()
            else:
                tdSql.connect(password=password)
        else:
            tdSql.connect(user=user, password=password)
    
    def create_user(self, user_name, password=None, options="", login=False):
        if password is None:
            password = "abcd@1234" # default password
        sql = f"CREATE USER {user_name} pass '{password}' {options}"
        tdSql.execute(sql)
        
        if login:
            self.login(user=user_name, password=password)

    def alter_user(self, user_name, options):
        tdSql.execute(f"ALTER USER {user_name} {options}")
            
    def drop_user(self, user_name):
        tdSql.execute(f"DROP USER {user_name}")

    def drop_user_failed(self, user_name):
        tdSql.error(f"DROP USER {user_name}")    
    
    def alter_password(self, user_name, new_password):
        tdSql.execute(f"ALTER USER {user_name} PASS '{new_password}'")

    def alter_password_failed(self, user_name, new_password):
        tdSql.error(f"ALTER USER {user_name} PASS '{new_password}'")
    
    def except_create_user(self, option, min, max):
        self.login()
        old_pass = "abcd@1234"
        tdSql.error(f"create user except_user pass '{old_pass}' {option}  abc")
        tdSql.error(f"create user except_user pass '{old_pass}' {option} 'abc'")
        tdSql.error(f"create user except_user pass '{old_pass}' {option} '1'")
        tdSql.error(f"create user except_user pass '{old_pass}' {option}  -1")
        if max is not None:
            tdSql.error(f"create user except_user pass '{old_pass}' {option} {max+1}")
        if min is not None:
            tdSql.error(f"create user except_user pass '{old_pass}' {option} {min-1}")
        tdSql.error(f"create user except_user pass '{old_pass}' a{option}  0")
        tdSql.error(f"create user except_user pass '{old_pass}' {option}b  0")
            
    def create_session(self, user_name, password, num):
        conns = []
        for i in range(num):
            conn = self.tdCom.newTdSql()
            conn.connect(user=user_name, password=password)
            conn.execute("show users")
            conns.append(conn)
        
        for conn in conns:
            conn.close()
            
    def create_session_failed(self, user_name, password, num):
        try:
            self.create_session(user_name, password, num)
        except Exception as e:
            print(f" max sessions({num}) reached as expected: {e}")
            return            

        raise Exception("Exceeded max sessions but was expected to fail")
        
    def check_user_option(self, user_name, option_name, expected_value, find=False):
        res = tdSql.getResult(f"show users full")
        idx = tdSql.fieldIndex(option_name)
        for row in res:
            if row[0] == user_name:
                actual_value = row[idx]
                if isinstance(actual_value, str):
                    actual_value = actual_value.strip().strip('\x00')
                if isinstance(expected_value, str):
                    expected_value = expected_value.strip().strip('\x00')
                # partial match
                if find:
                    if str(actual_value).find(str(expected_value)) < 0:
                        raise Exception(f"User option value mismatch for {option_name}: expected to find {expected_value}, got {actual_value}")
                    return
                # exact match
                if actual_value != expected_value:
                    raise Exception(f"User option value mismatch for {option_name}: expected {expected_value}, got {actual_value}")
                return
        raise Exception(f"User {user_name} not found in show users output")
    
    def thread_exec_sql(self, user_name, password, sql, results, index):
        try:
            tdCom = TDCom()
            conn = tdCom.newTdSql()
            conn.connect(user=user_name, password=password)
            conn.execute(sql)
            results[index] = True
            conn.close()
        except Exception as e:
            results[index] = False
            print(f" Thread {index} failed to execute SQL: {sql} error:{e}")
    
    def create_concurrent_threads(self, user_name, password, sql, num):
        threads = []
        results = [False] * num
        for i in range(num):
            t = threading.Thread(target=self.thread_exec_sql, args=(user_name, password, sql, results, i))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()

        print(f" threads results:{results}")            
        if not all(results):
            raise Exception("Not all sessions executed the SQL successfully")
    
    def check_in_max_succ_login(self, user_name, password, max_attempts):
        for i in range(max_attempts - 1):            
            self.login_failed(user=user_name, password="wrong_password")
        self.login(user=user_name, password=password)            

    def check_over_max_failed_login(self, user_name, password, max_attempts):
        for i in range(max_attempts):
            self.login_failed(user=user_name, password="wrong_password")
        
        # Next attempt should lock the user
        try:
            self.login(user=user_name, password=password)
        except Exception as e:
            print(f" User {user_name} locked as expected after {max_attempts} failed attempts: {e}")
            return
        
        raise Exception(f"User {user_name} was not locked after {max_attempts} failed attempts")
    
    def create_token(self, user_name, token_num, start=0):
        for i in range(start, start + token_num):
            tdSql.execute(f"create token tk_{user_name}{i} from user {user_name}")
        
    def create_token_failed(self, user_name, token_num, start=0):
        try:
            self.create_token(user_name, token_num, start)
        except Exception as e:
            print(f" max tokens({token_num}) reached as expected: {e}")
            return            

        raise Exception("Exceeded max tokens but was expected to fail")
    
    #
    # --------------------------- create user ----------------------------
    #
    
    # option CHANGEPASS
    def options_changepass(self):
        # init
        old_pass = "abcd@1234"
        new_pass = "newpass@1234"

        # default check (2-modify)
        user     = "user_changepass2"
        self.create_user(user, password=old_pass, login=True)
        tdSql.execute(f"alter user {user} pass '{new_pass}'")
        self.login(user=user, password=new_pass)
        
        # 1-must modify
        self.login()
        user = "user_changepass1"
        self.create_user(user, password=old_pass, options="CHANGEPASS 1")
        self.login(user, password=old_pass)
        tdSql.error("show users")
        tdSql.execute(f"alter user {user} pass '{new_pass}'")
        tdSql.execute("show users")
        self.login(user=user, password=new_pass)
        
        # 0-must not modify
        self.login()
        user = "user_changepass0"
        self.create_user(user, password=old_pass, options="CHANGEPASS 0")
        self.login(user=user, password=old_pass)
        tdSql.error(f"alter user {user} pass '{new_pass}'")
        
        # except
        self.except_create_user("CHANGEPASS", 0, 2)
        
        # root login
        self.login()
        print("option CHANGEPASS ..................... [ passed ] ")

    # option SESSION_PER_USER
    def options_session_per_user(self):
        # init
        password = "abcd@1234"

        # default check (UNLIMITED sessions)
        user = "user_session1"        
        self.create_user(user, password=password)
        self.create_session(user, password, 200)

        # min check (1 session)
        user = "user_session2"
        self.create_user(user, password=password, options="SESSION_PER_USER 1")
        self.create_session(user, password, 1)
        self.create_session_failed(user, password, 1+1)

        # big check (100 sessions)
        user = "user_session3"
        self.create_user(user, password=password, options="SESSION_PER_USER 100")
        self.create_session(user, password, 100)
        self.create_session_failed(user, password, 100+1)

        # unlimited check (500 sessions)
        user = "user_session4"
        self.create_user(user, password=password, options="SESSION_PER_USER UNLIMITED")
        self.create_session(user, password, 500)

        print("option SESSION_PER_USER ............... [ passed ] ")

    # option CONNECT_TIME
    def options_connect_time(self):
        # defalut check (-1, unlimited)
        user = "user_connect_time1"
        self.create_user(user)
        self.check_user_option(user, "CONNECT_TIME", -1)

        # min check (1 minute)
        user = "user_connect_time2"
        # check option value
        self.login()
        self.create_user(user, options="CONNECT_TIME 1", login=True)
        target_seconds = 57
        start_time = time.monotonic()
        while True:
            tdSql.execute("show databases")
            elapsed = time.monotonic() - start_time
            if elapsed >= target_seconds:
                break
            time.sleep(min(1.0, target_seconds - elapsed))

        time.sleep(5)
        tdSql.error("show databases")
        print(f" user {user} disconnected as expected after CONNECT_TIME 1 minute")

        # check option value
        self.login()
        self.check_user_option(user, "CONNECT_TIME", 60)

        print("option CONNECT_TIME ................... [ passed ] ")

    # option CONNECT_IDLE_TIME
    def options_connect_idle_time(self):
        # defalut check (-1, unlimited)
        user = "user_connect_idle_time1"
        self.create_user(user)
        self.check_user_option(user, "CONNECT_IDLE_TIMEOUT", -1)

        # min check (1 minute)
        user = "user_connect_idle_time2"
        # check option value
        self.login()
        self.create_user(user, options="CONNECT_IDLE_TIME 1", login=True)
        time.sleep(61)
        tdSql.error("show databases")
        print(f" user {user} disconnected as expected after CONNECT_IDLE_TIME 1 minute")

        # check option value
        self.login()
        self.check_user_option(user, "CONNECT_IDLE_TIMEOUT", 60)

        print("option CONNECT_IDLE_TIME .............. [ passed ] ")


    # option CALL_PER_SESSION
    def options_call_per_session(self):
        password = "abcd@1234"
        self.login()        
        # defalut check (-1, unlimited)
        user = "user_call_per_session1"
        self.create_user(user, password=password)
        self.check_user_option(user, "CALL_PER_SESSION", -1)
        self.create_concurrent_threads(user, password, "show databases", 10)

        # min check (1 minute)
        user = "user_call_per_session2"
        # check option value
        self.create_user(user, options="CALL_PER_SESSION 5")
        self.check_user_option(user, "CALL_PER_SESSION", 5)
        self.create_concurrent_threads(user, password, "show databases", 5)

        print("option CALL_PER_SESSION ............... [ passed ] ")

    # option VNODE_PER_CALL
    def options_vnode_per_call(self):
        password = "abcd@1234"
        self.login()
        succ = False
        
        # defalut check (-1, unlimited)
        user = "user_vnode_per_call1"
        self.create_user(user, password=password)
        self.check_user_option(user, "VNODE_PER_CALL", -1)
        tdSql.execute(f"grant all on test.* to {user}")
        tdSql.execute(f"grant all on  database test to {user}")
        self.login(user, password)
        for i in range(20):
            time.sleep(1)
            try:
                tdSql.checkFirstValue(f"select count(*) from test.meters", 10000)
                print("  default vnode per call unlimited works fine.")
                succ = True
                break
            except:
                print(f"  try {i+1} times ...")   
        if not succ:
            raise Exception("Default vnode per call unlimited failed")        

        # min
        user = "user_vnode_per_call2"
        succ = False
        self.login()        
        self.create_user(user, password=password, options="VNODE_PER_CALL 1")
        self.check_user_option(user, "VNODE_PER_CALL", 1)
        tdSql.execute(f"grant all on test.* to {user}")
        tdSql.execute(f"grant all on  database test to {user}")
        self.login(user, password)
        for i in range(20):
            time.sleep(1)
            try:
                tdSql.checkFirstValue(f"select count(*) from test.meters", 10000)
            except Exception as e:                
                if str(e).find("[0x023d]") >= 0: # TSDB_CODE_TSC_SESS_MAX_CALL_VNODE_LIMIT
                    print(f"  hit max vnode per call limit as expected.")
                    succ = True
                    break
                print(f"  try {i+1} times ... error:{e}")
        if not succ:
            raise Exception("Min vnode per call 1 vgroup failed")

        # equal db vgroups 16
        user = "user_vnode_per_call3"
        succ = False
        self.login()
        self.create_user(user, password=password, options="VNODE_PER_CALL 16")
        self.check_user_option(user, "VNODE_PER_CALL", 16)
        tdSql.execute(f"grant all on test.* to {user}")
        tdSql.execute(f"grant all on  database test to {user}")
        self.login(user, password)
        for i in range(20):
            time.sleep(1)
            try:
                tdSql.checkFirstValue(f"select count(*) from test.meters", 10000)
                print("  vnode per call 16 vgroups works fine.")
                succ = True
                break
            except Exception as e:
                print(f"  try {i+1} times ... error:{e}")
        if not succ:
            raise Exception("vnode per call 16 vgroups failed")
        
        # except
        self.except_create_user("VNODE_PER_CALL", 1, None)

        print("option VNODE_PER_CALL ................. [ passed ] ")

    # option FAILED_LOGIN_ATTEMPTS
    def options_failed_login_attempts(self):
        password = "abcd@1234"
        # defalut check (3)
        user = "user_failed_login1"
        self.create_user(user, password=password)
        self.check_user_option(user, "FAILED_LOGIN_ATTEMPTS", 3)
        self.check_in_max_succ_login(user, password, 3)
        self.check_over_max_failed_login(user, password, 3)

        # min check (1)
        user = "user_failed_login2"
        # check option value
        self.login()
        self.create_user(user, options="FAILED_LOGIN_ATTEMPTS 1")
        self.check_user_option(user, "FAILED_LOGIN_ATTEMPTS", 1)
        self.check_over_max_failed_login(user, password, 1)

        # big check (10)
        user = "user_failed_login3"
        # check option value
        self.login()
        self.create_user(user, options="FAILED_LOGIN_ATTEMPTS 10")
        self.check_user_option(user, "FAILED_LOGIN_ATTEMPTS", 10)
        self.check_in_max_succ_login(user, password, 10)
        self.check_over_max_failed_login(user, password, 10)
        
        # except
        self.except_create_user("FAILED_LOGIN_ATTEMPTS", 1, None)

        print("option FAILED_LOGIN_ATTEMPTS .......... [ passed ] ")

    # option PASSWORD_LOCK_TIME
    def options_password_lock_time(self):
        password = "abcd@1234"
        # defalut check (1440m)
        user = "user_password_lock1"
        self.create_user(user, password=password)
        self.check_user_option(user, "PASSWORD_LOCK_TIME", 86400)

        # min check (1)
        user = "user_password_lock2"
        # check option value
        self.create_user(user, options="PASSWORD_LOCK_TIME 1")
        self.check_user_option(user, "PASSWORD_LOCK_TIME", 60)
        # lock
        self.check_over_max_failed_login(user, password, 3)
        time.sleep(30)
        self.login_failed(user, password)
        time.sleep(31)  # wait for 1 minute lock time        
        # unlock
        self.login(user, password)

        # big check (UNLIMITED)
        user = "user_password_lock3"
        # check option value
        self.login()
        self.create_user(user, options="PASSWORD_LOCK_TIME UNLIMITED")
        self.check_user_option(user, "PASSWORD_LOCK_TIME", -1)
        
        # except
        self.except_create_user("PASSWORD_LOCK_TIME", 1, None)

        print("option PASSWORD_LOCK_TIME ............. [ passed ] ")

    # option PASSWORD_LIFE_TIME
    def options_password_life_time(self):
        password = "abcd@1234"
        self.login()        
        # defalut check (90 days)
        user = "user_password_life1"
        self.create_user(user, password=password)
        self.check_user_option(user, "PASSWORD_LIFE_TIME", 90*24*3600)

        # min check (1 minute)
        user = "user_password_life2"
        self.create_user(user, options="PASSWORD_LIFE_TIME 1")
        self.check_user_option(user, "PASSWORD_LIFE_TIME", 1*24*3600)

        # max check (1 minute)
        user = "user_password_life3"
        self.create_user(user, options="PASSWORD_LIFE_TIME UNLIMITED")
        self.check_user_option(user, "PASSWORD_LIFE_TIME", -1)
        
        # except
        self.except_create_user("PASSWORD_LIFE_TIME", 1, None)

        print("option PASSWORD_LIFE_TIME ............... [ passed ] ")

    # option PASSWORD_GRACE_TIME
    def options_password_grace_time(self):
        password = "abcd@1234"
        self.login()        
        # defalut check (7 days)
        user = "user_password_grace1"
        self.create_user(user, password=password)
        self.check_user_option(user, "PASSWORD_GRACE_TIME", 7*24*3600)

        # min check (0 days)
        user = "user_password_grace2"
        self.create_user(user, options="PASSWORD_GRACE_TIME 0")
        self.check_user_option(user, "PASSWORD_GRACE_TIME", 0)

        # max check (UNLIMITED)
        user = "user_password_grace3"
        self.create_user(user, options="PASSWORD_GRACE_TIME UNLIMITED")
        self.check_user_option(user, "PASSWORD_GRACE_TIME", -1)
        
        # except
        self.except_create_user("PASSWORD_GRACE_TIME", None, None)

        print("option PASSWORD_GRACE_TIME ............ [ passed ] ")

    # option PASSWORD_REUSE_TIME
    def options_password_reuse_time(self):
        password1 = "abcd@1234"
        password2 = "abcd@1235"
        self.login()        

        # defalut check (30 days) 
        self.check_user_option(self.user_default, "PASSWORD_REUSE_TIME", 30*24*3600)

        # min check (0 days)
        user = "user_password_reuse1"
        self.create_user(user, password1, options="PASSWORD_REUSE_TIME 0 PASSWORD_REUSE_MAX 0")
        self.check_user_option(user, "PASSWORD_REUSE_TIME", 0)
        # fun check
        self.alter_password(user, password2)
        self.alter_password(user, password1) # should be ok reused

        # max check (365 days)
        user = "user_password_reuse2"
        self.create_user(user, password1, options="PASSWORD_REUSE_TIME 365")
        self.check_user_option(user, "PASSWORD_REUSE_TIME", 365*24*3600)
        # fun check
        self.alter_password(user, password2)
        self.alter_password_failed(user, password1) # old password should not be reused
        
        # except
        self.except_create_user("PASSWORD_REUSE_TIME", None, 365)

        print("option PASSWORD_REUSE_TIME ............ [ passed ] ")

    # option PASSWORD_REUSE_MAX
    def options_password_reuse_max(self):
        password1 = "abcd@1234"
        password2 = "abcd@1235"
        self.login()        

        # defalut check (5) 
        self.check_user_option(self.user_default, "PASSWORD_REUSE_MAX", 5)

        # min check (0)
        user = "user_pwd_reuse_max1"
        self.create_user(user, password1, options="PASSWORD_REUSE_MAX 0 PASSWORD_REUSE_TIME 0")
        self.check_user_option(user, "PASSWORD_REUSE_MAX", 0)

        # fun check
        self.alter_password(user, password2)
        self.alter_password(user, password1) # should be ok reused

        # max check (100)
        user = "user_pwd_reuse_max2"
        self.create_user(user, password1, options="PASSWORD_REUSE_MAX 100 PASSWORD_REUSE_TIME 0")
        self.check_user_option(user, "PASSWORD_REUSE_MAX", 100)
        # fun check
        for i in range(99):
            new_pass = f"abcd@1111{i:02d}"
            self.alter_password(user, new_pass)
        
        self.alter_password_failed(user, password1) # 99th old password should not be reused
        self.alter_password(user, password2) # 100th
        self.alter_password(user, password1) # 101th old password can be reused
        
        # except
        self.except_create_user("PASSWORD_REUSE_MAX", None, 100)

        print("option PASSWORD_REUSE_MAX ............. [ passed ] ")
    
    # option INACTIVE_ACCOUNT_TIME
    def options_inactive_account_time(self):
        password = "abcd@1234"
        self.login()        
        # defalut check (90 days)
        self.check_user_option(self.user_default, "INACTIVE_ACCOUNT_TIME", 90*24*3600)

        # min check (1 days)
        user = "user_inactive_account1"
        self.create_user(user, password, options="INACTIVE_ACCOUNT_TIME 1")
        self.check_user_option(user, "INACTIVE_ACCOUNT_TIME", 1*24*3600)

        # max check (UNLIMITED)
        user = "user_inactive_account2"
        self.create_user(user, password, options="INACTIVE_ACCOUNT_TIME UNLIMITED")
        self.check_user_option(user, "INACTIVE_ACCOUNT_TIME", -1)
        self.login(user, password)
        
        # except
        self.login()
        self.except_create_user("INACTIVE_ACCOUNT_TIME", 1, None)

        print("option INACTIVE_ACCOUNT_TIME ........... [ passed ] ")

    # option ALLOW_TOKEN_NUM
    def options_allow_token_num(self):
        password = "abcd@1234"
        self.login()        
        
        # defalut check (3)
        user = "user_allow1"
        self.create_user(user, password)
        self.check_user_option(self.user_default, "ALLOW_TOKEN_NUM", 3)
        self.create_token(user, 3)
        self.create_token_failed(user, 1, start=3)

        # min check (0)
        '''BUG4
        user = "user_allow2"
        self.create_user(user, password, options="ALLOW_TOKEN_NUM 0")
        self.check_user_option(user, "ALLOW_TOKEN_NUM", 0)
        self.create_token_failed(user, 1)
        '''

        # max check (UNLIMITED)
        user = "user_allow3"
        self.create_user(user, password, options="ALLOW_TOKEN_NUM UNLIMITED")
        self.check_user_option(user, "ALLOW_TOKEN_NUM", -1)
        self.create_token(user, 100)
        
        # except
        self.except_create_user("ALLOW_TOKEN_NUM", 0, None)

        print("option ALLOW_TOKEN_NUM ................ [ passed ] ")

    # option HOST white list
    def options_host(self):
        password = "abcd@1234"
        self.login()

        # single ip
        user = "user_host1"
        self.create_user(user, password, options="HOST '192.168.99.200'")
        self.check_user_option(user, "allowed_host", "+127.0.0.1/32, +192.168.99.200/32, +::1/128")

        # ip range
        user = "user_host2"
        self.create_user(user, password, options="HOST '192.168.99.1/16'")
        self.check_user_option(user, "allowed_host", "+127.0.0.1/32, +192.168.99.1/16, +::1/128")

        # single ip + ip range
        user = "user_host3"
        self.create_user(user, password, options="HOST '192.168.99.1', '192.168.100.1/16'")
        self.check_user_option(user, "allowed_host", "+127.0.0.1/32, +192.168.99.1/32, +192.168.100.1/16, +::1/128")

        # except
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' HOST '192.148.1.11.11.2'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' HOST '192.148.1'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' HOST 192.148.1.100")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' HOST 192.148.1.100/23")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' HOST '192.148.1.100/400'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' HOST  ")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' HOST  123")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' HOST  19212.3")
        print("option HOST ........................... [ passed ] ")

    # option NOT_ALLOW_HOST black list
    def options_not_allow_host(self):
        password = "abcd@1234"
        self.login()   

        # single ip
        user = "user_not_allow_host1"
        self.create_user(user, password, options="NOT_ALLOW_HOST '192.168.99.200'")
        self.check_user_option(user, "allowed_host", "-192.168.99.200/32")

        # ip range
        user = "user_not_allow_host2"
        self.create_user(user, password, options="NOT_ALLOW_HOST '192.168.99.1/16'")
        self.check_user_option(user, "allowed_host", "-192.168.99.1/16")

        # single ip + ip range
        user = "user_not_allow_host3"
        self.create_user(user, password, options="NOT_ALLOW_HOST '192.168.99.1', '192.168.100.1/16'")
        self.check_user_option(user, "allowed_host", "-192.168.99.1/32, -192.168.100.1/16")

        # except
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_HOST '192.148.1.11.11.2'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_HOST '192.148.1'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_HOST 192.148.1.100")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_HOST 192.148.1.100/23")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_HOST '192.148.1.100/400'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_HOST  ")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_HOST  123")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_HOST  19212.3")
        print("option NOT_ALLOW_HOST ................. [ passed ] ")

    # option ALLOW_DATETIME
    def options_allow_datetime(self):
        password = "abcd@1234"
        self.login()
        
        # default
        self.check_user_option(self.user_default, "allowed_datetime", "+ALL")

        # ignore passed time
        user = "user_allow_datetime1"
        val = "2025-12-01 10:00 10"
        self.create_user(user, password, options=f"ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "+ALL")
        
        # forbid login
        user = "user_allow_datetime2"
        val = get_tomorrow() + " 10:00 10"
        self.create_user(user, password, options=f"ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "+" + val, find=True)
        self.login_failed(user, password)
        self.login()

        # allow login current time
        user = "user_allow_datetime3"
        val = time.strftime("%Y-%m-%d %H:%M", time.localtime()) + " 90"
        self.create_user(user, password, options=f"ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "+" + val, find=True)
        self.login(user, password)
        self.login()

        # week date, forbid login
        user = "user_allow_datetime4"
        val = get_tomorrow_weekday_short() + " 02:00 20"
        self.create_user(user, password, options=f"ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "+" + val, find=True)
        self.login_failed(user, password)
        self.login()

        # week date, allow login
        user = "user_allow_datetime5"
        val = time.strftime("%a %H:%M 20", time.localtime()).upper()
        self.create_user(user, password, options=f"ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "+" + val, find=True)
        self.login(user, password)
        self.login()

        # multiple allow date time, allow login
        user = "user_allow_datetime6"
        val2 = get_tomorrow() + " 10:00 20"
        self.create_user(user, password, options=f"ALLOW_DATETIME '{val}', '{val2}'")
        self.check_user_option(user, "allowed_datetime", "+" + val, find=True)
        self.check_user_option(user, "allowed_datetime", "+" + val2, find=True)
        self.login(user, password)
        self.login()

        # except
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME '2023_12_01 10:00:20'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIMEB '2028-12-01 10:00:20 30'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME '10:00:00'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME 2026-13-01 10:00")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME '2028-15-65 25:61 200'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME '192.148.1.100/400'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME  ")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME  123")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME  19212.3")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME  'abcd")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' ALLOW_DATETIME  'now'")

        print("option ALLOW_DATETIME ................. [ passed ] ")


    # option NOT_ALLOW_DATETIME
    def options_not_allow_datetime(self):
        password = "abcd@1234"
        self.login()

        # ignore passed time
        user = "user_not_allow_d1"
        val = "2025-12-01 10:00 10"
        self.create_user(user, password, options=f"NOT_ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "+ALL")
        self.login(user, password)
        self.login()
        
        # allow login
        user = "user_not_allow_d2"
        val = get_tomorrow() + " 10:00 10"
        self.create_user(user, password, options=f"NOT_ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "-" + val, find=True)
        self.login(user, password)
        self.login()

        # forbid login
        user = "user_not_allow_d3"
        val = time.strftime("%Y-%m-%d %H:%M", time.localtime()) + " 120"
        self.create_user(user, password, options=f"NOT_ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "-" + val, find=True)
        self.login_failed(user, password)
        self.login()

        # week date, allow login
        user = "user_not_allow_d4"
        val = get_tomorrow_weekday_short() + " 02:00 15"
        self.create_user(user, password, options=f"NOT_ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "-" + val, find=True)
        self.login(user, password)
        self.login()

        # week date, forbid login
        user = "user_not_allow_d5"
        val = time.strftime("%a %H:%M 20", time.localtime()).upper()
        self.create_user(user, password, options=f"NOT_ALLOW_DATETIME '{val}'")
        self.check_user_option(user, "allowed_datetime", "-" + val, find=True)
        self.login_failed(user, password)
        self.login()

        # multiple not allow date time, forbid login
        user = "user_not_allow_d6"
        val2 = get_tomorrow() + " 10:00 20"
        self.create_user(user, password, options=f"NOT_ALLOW_DATETIME '{val}', '{val2}'")
        self.check_user_option(user, "allowed_datetime", "-" + val, find=True)
        self.check_user_option(user, "allowed_datetime", "-" + val2, find=True)
        self.login_failed(user, password)
        self.login()

        # except
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME '2023_12_01 10:00:20'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIMEB '2028-12-01 10:00:20 30'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME '10:00:00'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME 2026-13-01 10:00")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME '2028-15-65 25:61 200'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME '192.148.1.100/400'")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME  ")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME  123")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME  19212.3")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME  'abcd")
        tdSql.error("create user except_user1 pass  'aaa@aaaaa122' NOT_ALLOW_DATETIME  'now'")

        print("option NOT_ALLOW_DATETIME ............. [ passed ] ")

    # option combine
    def options_combine(self):
        password = "abcd@1234"
        tomorrow = get_tomorrow()
        self.login()   

        # create
        user = "user_combine1"
        options  = "SESSION_PER_USER      10 "
        options += "CONNECT_TIME          20 "
        options += "CONNECT_IDLE_TIME     30 "
        options += "CALL_PER_SESSION      40 "
        options += "VNODE_PER_CALL        50 "
        options += "FAILED_LOGIN_ATTEMPTS 60 "
        options += "PASSWORD_LOCK_TIME    70 "
        options += "PASSWORD_LIFE_TIME    80 "
        options += "PASSWORD_GRACE_TIME   90 "
        options += "PASSWORD_REUSE_TIME   100 "
        options += "PASSWORD_REUSE_MAX    15 "
        options += "INACTIVE_ACCOUNT_TIME 120 "
        options += "ALLOW_TOKEN_NUM       130 "
        options += "HOST                  '192.168.5.1' " 
        options += "NOT_ALLOW_HOST        '192.168.6.1' "
        options += f"ALLOW_DATETIME       '{tomorrow} 10:00 60' "
        options += f"NOT_ALLOW_DATETIME   '{tomorrow} 11:00 60' "
             
        self.create_user(user, password, options)

        # check options
        self.check_user_option(user, "SESSION_PER_USER",      10)
        self.check_user_option(user, "CONNECT_TIME",          20*60)
        self.check_user_option(user, "connect_idle_timeout",  30*60)
        self.check_user_option(user, "CALL_PER_SESSION",      40)
        self.check_user_option(user, "VNODE_PER_CALL",        50)
        self.check_user_option(user, "FAILED_LOGIN_ATTEMPTS", 60)
        self.check_user_option(user, "PASSWORD_LOCK_TIME",    70*60)
        self.check_user_option(user, "PASSWORD_LIFE_TIME",    80*24*3600)
        self.check_user_option(user, "PASSWORD_GRACE_TIME",   90*24*3600)
        self.check_user_option(user, "PASSWORD_REUSE_TIME",   100*24*3600)
        self.check_user_option(user, "PASSWORD_REUSE_MAX",    15)
        self.check_user_option(user, "INACTIVE_ACCOUNT_TIME", 120*24*3600)
        self.check_user_option(user, "ALLOW_TOKEN_NUM",       130)
        self.check_user_option(user, "ALLOWED_HOST",          "+192.168.5.1/32", find=True)
        self.check_user_option(user, "ALLOWED_HOST",          "-192.168.6.1/32", find=True)
        self.check_user_option(user, "ALLOWED_DATETIME",      f"+{tomorrow} 10:00 60", find=True)
        self.check_user_option(user, "ALLOWED_DATETIME",      f"-{tomorrow} 11:00 60", find=True)

        # login should fail as NOT_ALLOW_DATETIME is set and current time is not in ALLOW_DATETIME
        self.login_failed(user, password)

        # except
        self.login()
        tdSql.error("create user except_user1 pass 'aaa@aaaaa122' VNODE_PER_CALL 23 NOT_ALLOW_HOST '192.148.1.11.11.2'")
        tdSql.error("create user except_user1 pass 'aaa@aaaaa122' PASSWORD_LIFE_TIME  NOT_ALLOW_HOST '192.148.1'")
        tdSql.error("create user except_user1 pass 'aaa@aaaaa122' INACTIVE_ACCOUNT_TIME 192.148.1.100")
        tdSql.error("create user except_user1 pass 'aaa@aaaaa122' CALL_PER_SESSION '23'  CONNECT_TIME 100")
        tdSql.error("create user except_user1 pass 'aaa@aaaaa122' VNODE_PER_CALL ab INACTIVE_ACCOUNT_TIME '192'")
        tdSql.error("create user except_user1 pass 'aaa@aaaaa122' PASSWORD_REUSE_TIME 2  PASSWORD_REUSE_TIME FAILED_LOGIN_ATTEMPTS -1  ")

        print("option combine ........................ [ passed ] ")
    
    def do_create_user(self):
        print("\n")
        self.user_default = "user_default"
        self.create_user(self.user_default)

        self.options_changepass()
        self.options_session_per_user()
        self.options_connect_time()
        self.options_connect_idle_time()
        self.options_call_per_session()
        self.options_vnode_per_call()
        self.options_failed_login_attempts()
        self.options_password_lock_time()
        self.options_password_life_time()
        self.options_password_grace_time()
        self.options_password_reuse_time()
        self.options_password_reuse_max()
        self.options_inactive_account_time()
        self.options_allow_token_num()
        self.options_host()
        self.options_not_allow_host()
        self.options_allow_datetime()
        self.options_not_allow_datetime()
        self.options_combine()

    #
    # --------------------------- show user ----------------------------
    #
    def do_show_user(self):
        def checkData():
            tdSql.checkRows(57)
            tdSql.checkData(0,  0, "user_not_allow_d2")   # name
            tdSql.checkData(1,  0, "user_allow3") 
            tdSql.checkData(0,  2, 1)               # enable
            tdSql.checkData(0,  6, 0)               # totp
            tdSql.checkData(0,  9, "SYSINFO_1")     # roles

        # show users
        tdSql.query("show users")
        checkData()
        # query ins_users
        tdSql.query("select * from information_schema.ins_users")
        checkData()        

    #
    # --------------------------- drop user ----------------------------
    #
    def do_drop_user(self):
        # normal
        self.login()
        self.drop_user("user_session1")
        tdSql.checkFirstValue("select count(*) from information_schema.ins_users where name='user_session1'", 0)
        
        # except
        self.drop_user_failed("non_exist_user")        

    #
    # --------------------------- alter user ----------------------------
    #
    def check_alter_option_value(self):        
        user = "user_alter_option1"
        password = "abcd@1234"
        date = get_tomorrow()
        allow_time = f'{date} 10:00 30'
        not_allow_time = f'{date} 18:00 60'

        # create
        self.create_user(user, password)
        self.check_user_option(user, "allowed_host", "+127.0.0.1/32, +::1/128")
        
        # alter options
        options  = "SESSION_PER_USER      10 "
        options += "CONNECT_TIME          20 "
        options += "CONNECT_IDLE_TIME     30 "
        options += "CALL_PER_SESSION      40 "
        options += "VNODE_PER_CALL        50 "
        options += "FAILED_LOGIN_ATTEMPTS 60 "
        options += "PASSWORD_LOCK_TIME    70 "
        options += "PASSWORD_LIFE_TIME    80 "
        options += "PASSWORD_GRACE_TIME   90 "
        options += "PASSWORD_REUSE_TIME   100 "
        options += "PASSWORD_REUSE_MAX    15 "
        options += "INACTIVE_ACCOUNT_TIME 120 "
        options += "ALLOW_TOKEN_NUM       130 "
        options += "ADD HOST              '192.168.0.1' " 
        options += "ADD NOT_ALLOW_HOST    '192.169.0.1' " 
        options += f"ADD ALLOW_DATETIME     '{allow_time}' " 
        options += f"ADD NOT_ALLOW_DATETIME '{not_allow_time}' " 
        self.alter_user(user, options)
        
        # check options
        self.check_user_option(user, "SESSION_PER_USER",      10)
        self.check_user_option(user, "CONNECT_TIME",          20*60)
        self.check_user_option(user, "connect_idle_timeout",  30*60)
        self.check_user_option(user, "CALL_PER_SESSION",      40)
        self.check_user_option(user, "VNODE_PER_CALL",        50)
        self.check_user_option(user, "FAILED_LOGIN_ATTEMPTS", 60)
        self.check_user_option(user, "PASSWORD_LOCK_TIME",    70*60)
        self.check_user_option(user, "PASSWORD_LIFE_TIME",    80*24*3600)
        self.check_user_option(user, "PASSWORD_GRACE_TIME",   90*24*3600)
        self.check_user_option(user, "PASSWORD_REUSE_TIME",   100*24*3600)
        self.check_user_option(user, "PASSWORD_REUSE_MAX",    15)
        self.check_user_option(user, "INACTIVE_ACCOUNT_TIME", 120*24*3600)
        self.check_user_option(user, "ALLOW_TOKEN_NUM",       130)        
        self.check_user_option(user, "allowed_host",          "+192.168.0.1", find=True)
        self.check_user_option(user, "allowed_host",          "-192.169.0.1", find=True)
        self.check_user_option(user, "allowed_datetime",      f"+{allow_time}", find=True)
        self.check_user_option(user, "allowed_datetime",      f"-{not_allow_time}", find=True)

        # login should fail as NOT_ALLOW_DATETIME is set and current time is not in ALLOW_DATETIME
        self.login_failed(user, password)

        # alter user to add current time to ALLOW_DATETIME and drop previous ALLOW_DATETIME
        self.login()
        options = f"DROP ALLOW_DATETIME '{allow_time}' "
        allow_time = time.strftime("%Y-%m-%d %H:00 180", time.localtime())
        options += f"ADD ALLOW_DATETIME '{allow_time}' "
        self.alter_user(user, options)
        # login should succeed as current time is in ALLOW_DATETIME and not in NOT_ALLOW_DATETIME
        self.login(user, password)

        # alter user to add current time to NOT_ALLOW_DATETIME and drop previous NOT_ALLOW_DATETIME
        self.login()
        options = f"DROP NOT_ALLOW_DATETIME '{not_allow_time}' "
        not_allow_time = time.strftime("%Y-%m-%d %H:00 60", time.localtime())
        options += f"ADD NOT_ALLOW_DATETIME '{not_allow_time}' "
        self.alter_user(user, options)
        # login should fail as current time is both in ALLOW_DATETIME and NOT_ALLOW_DATETIME
        self.login_failed(user, password)

        self.login()
        # drop HOST
        options = ""
        options += "DROP HOST              '192.168.0.1' " 
        options += "DROP NOT_ALLOW_HOST    '192.169.0.1' " 
        self.alter_user(user, options)
        self.check_user_option(user, "allowed_host",          "+127.0.0.1/32, +::1/128")

        # drop DATETIME
        options = ""
        options += f"DROP ALLOW_DATETIME     '{allow_time}' "
        options += f"DROP NOT_ALLOW_DATETIME '{not_allow_time}' " 
        self.alter_user(user, options)
        self.check_user_option(user, "allowed_datetime",      "+ALL")

    def check_alter_fun_work(self):
        #
        # immediately work
        #
        user = "user_alter_fun1"
        password = "abcd@1234"
        wrong_pwd = "wrong@1234"
        
        # enable
        self.create_user(user, password, options="ENABLE 0 ")
        self.login_failed(user, password)
        self.login()
        self.alter_user(user, "ENABLE 1")
        self.login(user, password)
        self.login()

        #
        # next work
        #
        
        # SESSION_PER_USER
        user = "user_alter_fun2"
        self.create_user(user, password, options="SESSION_PER_USER 2")
        self.alter_user(user, "SESSION_PER_USER 1")
        self.create_session(user, password, 1)
        self.create_session_failed(user, password, 2)

        # CALL_PER_SESSION
        user = "user_alter_fun3"
        self.create_user(user, password, options="CALL_PER_SESSION 1")
        self.alter_user(user, "CALL_PER_SESSION 3")
        self.create_concurrent_threads(user, password, "show databases", 3)
        self.alter_user(user, "CALL_PER_SESSION 5")
        self.create_concurrent_threads(user, password, "show databases", 5)
        
        # PASSWORD_REUSE_MAX & PASSWORD_REUSE_TIME
        self.alter_user(user, "PASSWORD_REUSE_MAX 0 PASSWORD_REUSE_TIME 0")
        self.alter_password(user, "newpass@1234")
        self.alter_password(user, password) # should be ok reused
        self.alter_user(user, "PASSWORD_REUSE_MAX 5 PASSWORD_REUSE_TIME 365")
        self.alter_password(user, "newpass@1234")
        self.alter_password_failed(user, password) # old password should not be reused
        
        # ALLOW_TOKEN_NUM
        self.alter_user(user, "ALLOW_TOKEN_NUM 2")
        self.create_token(user, 2)
        self.create_token_failed(user, 1, start=2)
        
        # VNODE_PER_CALL
        user = "user_alter_fun4"
        self.create_user(user, password, options="VNODE_PER_CALL 128")
        self.alter_user(user, "ENABLE 1 VNODE_PER_CALL 1")
        tdSql.execute(f"grant all on test.* to {user}")
        tdSql.execute(f"grant all on  database test to {user}")
        self.login(user, password)
        succ = False
        for i in range(20):
            time.sleep(1)
            try:
                tdSql.checkFirstValue(f"select count(*) from test.meters", 10000)
            except Exception as e:                
                if str(e).find("[0x023d]") >= 0: # TSDB_CODE_TSC_SESS_MAX_CALL_VNODE_LIMIT
                    print(f"  hit max vnode per call limit as expected.")
                    succ = True
                    break
                print(f"  try {i+1} times ... error:{e}")
        if not succ:
            raise Exception("Alter VNODE_PER_CALL 1 not work on next")
        self.login()
        
        # FAILED_LOGIN_ATTEMPTS
        self.alter_user(user, "FAILED_LOGIN_ATTEMPTS 1")
        self.login_failed(user, wrong_pwd)
        self.login_failed(user, password)
        self.login()
        self.alter_user(user, "FAILED_LOGIN_ATTEMPTS 2")
        self.login(user, password)
    
    def do_alter_user(self):
        self.check_alter_option_value()
        self.check_alter_fun_work()
    
    # prepare data
    def prepare_data(self):
        command = f"-v 16 -t 100 -n 100 -y"
        etool.benchMark(command)

    #
    # --------------------------- main ----------------------------
    #
    def test_user_manager(self):
        """User manager

        1. create user with variant options
            - SESSION_PER_USER
            - CONNECT_TIME
            - CONNECT_IDLE_TIME
            - CALL_PER_SESSION
            - VNODE_PER_CALL
            - FAILED_LOGIN_ATTEMPTS
            - PASSWORD_LOCK_TIME
            - PASSWORD_LIFE_TIME
            - PASSWORD_GRACE_TIME
            - PASSWORD_REUSE_TIME
            - PASSWORD_REUSE_MAX
            - INACTIVE_ACCOUNT_TIME
            - ALLOW_TOKEN_NUM
            - HOST white list
            - NOT_ALLOW_HOST black list
            - ALLOW_DATETIME
            - NOT_ALLOW_DATETIME
            - combine options
            - check created options work fine
        2. show user
            - show user command
            - query information_schema.ins_users
        3. alter user options
            - alter user to change options
            - check altered options work fine
        4. drop user
        5. exception on create/alter/drop user
        
        Since: v3.4.0.0

        Labels: common,ci,user

        Jira: None

        History:
            - 2026-01-06 Alex Duan created
            - 2026-01-09 Alex Duan finished

        """
        self.prepare_data()
        self.do_create_user()
        self.do_show_user()
        self.do_alter_user()
        self.do_drop_user()