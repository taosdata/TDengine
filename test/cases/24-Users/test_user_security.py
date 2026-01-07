from new_test_framework.utils import tdLog, tdSql, TDSql, TDCom, etool
import os
import platform
import time
import threading

class TestUserSecurity:
    def setup_class(cls):
        pass

    #
    # --------------------------- util ----------------------------
    #
    def login_failed(self, user=None, password=None):
        try:
            if user is None:
                if password is None:
                    tdSql.connect()
                else:
                    tdSql.connect(password=password)
            else:
                tdSql.connect(user=user, password=password)
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
    
    def create_user(self, user_name, options = "", password = None, login=False):
        if password is None:
            password = "abcd@1234" # default password
        sql = f"CREATE USER {user_name} pass '{password}' {options}"
        tdSql.execute(sql)
        
        if login:
            self.login(user=user_name, password=password)
            
    def drop_user(self, user_name):
        tdSql.execute(f"DROP USER {user_name}")
        
    
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
        tdCom = TDCom()
        for i in range(num):
            conn = tdCom.newTdSql()
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
    
    def check_user_option(self, user_name, option_name, expected_value):
        res = tdSql.getResult(f"show users full")
        idx = tdSql.fieldIndex(option_name)
        for row in res:
            if row[0] == user_name:
                actual_value = row[idx]
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
    
    def create_multiple_sessions(self, user_name, password, sql, num):
        
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
        for i in range(60):
            tdSql.execute("show databases")
            time.sleep(1)

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
        time.sleep(65)
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
        self.create_multiple_sessions(user, password, "show databases", 10)

        # min check (1 minute)
        user = "user_call_per_session2"
        # check option value
        self.create_user(user, options="CALL_PER_SESSION 5")
        self.check_user_option(user, "CALL_PER_SESSION", 5)
        self.create_multiple_sessions(user, password, "show databases", 5)

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
        self.login(user=user, password=password)
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
        self.login(user=user, password=password)
        for i in range(20):
            time.sleep(1)
            try:
                tdSql.checkFirstValue(f"select count(*) from test.meters", 10000)
                break
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
        self.login(user=user, password=password)
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
        
    
    def do_create_user(self):
        print("\n")
        #'''
        self.options_changepass()
        self.options_session_per_user()
        self.options_connect_time()
        self.options_connect_idle_time()
        self.options_call_per_session()
        self.options_vnode_per_call()
        #'''
        self.options_failed_login_attempts()        

    #
    # --------------------------- show user ----------------------------
    #
    def do_show_user(self):
        pass

    #
    # --------------------------- drop user ----------------------------
    #
    def do_drop_user(self):
        pass

    #
    # --------------------------- alter user ----------------------------
    #
    def do_alter_user(self):
        pass

    #
    # --------------------------- totp ----------------------------
    #
    def do_totp(self):
        pass

    #
    # --------------------------- token ----------------------------
    #
    def do_token(self):
        pass
    
    # prepare data
    def prepare_data(self):
        command = f"-v 16 -t 100 -n 100 -y"
        etool.benchMark(command)

    #
    # --------------------------- main ----------------------------
    #
    def test_user_security(self):
        """User security

        1. create user
        2. show user
        3. alter user
        4. drop user
        5. TOTP management
        6. Token management
        
        Since: v3.4.0.0

        Labels: common,ci,user

        Jira: None

        History:
            - 2026-01-06 Alex Duan created

        """
        self.prepare_data()
        self.do_create_user()
        self.do_show_user()
        self.do_alter_user()
        self.do_drop_user()
        self.do_totp()
        self.do_token()