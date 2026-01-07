from new_test_framework.utils import tdLog, tdSql, TDSql, TDCom
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
        self.login()
        tdSql.error(f"create user except_user pass '{old_pass}' CHANGEPASS abc")
        tdSql.error(f"create user except_user pass '{old_pass}' CHANGEPASS '1'")
        tdSql.error(f"create user except_user pass '{old_pass}' CHANGEPASS -1")
        tdSql.error(f"create user except_user pass '{old_pass}' CHANGEPASS 3")
        tdSql.error(f"create user except_user pass '{old_pass}' CHANGEPAS  0")
        tdSql.error(f"create user except_user pass '{old_pass}' ACHANGEPASS  0")
        
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

        print("option CALL_PER_SESSION .............. [ passed ] ")
        
    
    def do_create_user(self):
        print("\n")
        self.options_changepass()
        self.options_session_per_user()
        self.options_connect_time()
        self.options_connect_idle_time()
        self.options_call_per_session()

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
        self.do_create_user()
        self.do_show_user()
        self.do_alter_user()
        self.do_drop_user()
        self.do_totp()
        self.do_token()