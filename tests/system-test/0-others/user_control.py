import taos
import sys
import inspect
import traceback

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


PRIVILEGES_ALL      = "ALL"
PRIVILEGES_READ     = "READ"
PRIVILEGES_WRITE    = "WRITE"

class TDconnect:
    def __init__(self,
                 host       = None,
                 port       = None,
                 user       = None,
                 password   = None,
                 database   = None,
                 config     = None,
        ) -> None:
        self._conn      = None
        self._host      = host
        self._user      = user
        self._password  = password
        self._database  = database
        self._port      = port
        self._config    = config

    def __enter__(self):
        self._conn = taos.connect(
            host    =self._host,
            port    =self._port,
            user    =self._user,
            password=self._password,
            database=self._database,
            config  =self._config
        )

        self.cursor = self._conn.cursor()
        return self

    def error(self, sql):
        expectErrNotOccured = True
        try:
            self.cursor.execute(sql)
        except BaseException:
            expectErrNotOccured = False

        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured" )
        else:
            self.queryRows = 0
            self.queryCols = 0
            self.queryResult = None
            tdLog.info(f"sql:{sql}, expect error occured")

    def query(self, sql, row_tag=None):
        # sourcery skip: raise-from-previous-error, raise-specific-error
        self.sql = sql
        try:
            self.cursor.execute(sql)
            self.queryResult = self.cursor.fetchall()
            self.queryRows = len(self.queryResult)
            self.queryCols = len(self.cursor.description)
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.notice(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, {repr(e)}")
            traceback.print_exc()
            raise Exception(repr(e))
        if row_tag:
            return self.queryResult
        return self.queryRows

    def __exit__(self, types, values, trace):
        if self._conn:
            self.cursor.close()
            self._conn.close()

def taos_connect(
    host    = "127.0.0.1",
    port    = 6030,
    user    = "root",
    passwd  = "taosdata",
    database= None,
    config  = None
):
    return TDconnect(
        host = host,
        port=port,
        user=user,
        password=passwd,
        database=database,
        config=config
    )

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    @property
    def __user_list(self):
        return  [f"user_test{i}" for i in range(self.users_count) ]

    @property
    def __passwd_list(self):
        return  [f"taosdata{i}" for i in range(self.users_count) ]

    @property
    def __privilege(self):
        return [ PRIVILEGES_ALL, PRIVILEGES_READ, PRIVILEGES_WRITE ]

    def __priv_level(self, dbname=None):
        return f"{dbname}.*" if dbname else "*.*"


    def create_user_current(self):
        users  = self.__user_list
        passwds = self.__passwd_list
        for i in range(self.users_count):
            tdSql.execute(f"create user {users[i]} pass '{passwds[i]}' ")

        tdSql.query("show users")
        tdSql.checkRows(self.users_count + 1)

    def create_user_err(self):
        sqls = [
            "create users u1 pass 'u1passwd' ",
            "create user '' pass 'u1passwd' ",
            "create user  pass 'u1passwd' ",
            "create user u1 pass u1passwd ",
            "create user u1 password 'u1passwd' ",
            "create user u1 pass u1passwd ",
            "create user u1 pass '' ",
            "create user u1 pass '   ' ",
            "create user u1 pass  ",
            "create user u1 u2 pass 'u1passwd' 'u2passwd' ",
            "create user u1 u2 pass 'u1passwd', 'u2passwd' ",
            "create user u1, u2 pass 'u1passwd', 'u2passwd' ",
            "create user u1, u2 pass 'u1passwd'  'u2passwd' ",
            # length of user_name must <= 23
            "create user u12345678901234567890123 pass 'u1passwd' " ,
            # length of passwd must <= 128
            "create user u1 pass 'u12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678' " ,
            # password must have not " ' ~ ` \
            "create user u1 pass 'u1passwd\\' " ,
            "create user u1 pass 'u1passwd~' " ,
            "create user u1 pass 'u1passwd\"' " ,
            "create user u1 pass 'u1passwd\'' " ,
            "create user u1 pass 'u1passwd`' " ,
            # must after create a user named u1
            "create user u1 pass 'u1passwd' " ,
        ]

        tdSql.execute("create user u1 pass 'u1passwd' ")
        for sql in sqls:
            tdSql.error(sql)

        tdSql.execute("DROP USER u1")

    def __alter_pass_sql(self, user, passwd):
        return f'''ALTER USER {user} PASS '{passwd}' '''

    def alter_pass_current(self):
        self.__init_pass = True
        for count, i in enumerate(range(self.users_count)):
            if self.__init_pass:
                tdSql.query(self.__alter_pass_sql(self.__user_list[i], f"new{self.__passwd_list[i]}"))
                self.__init_pass = count != self.users_count - 1
            else:
                tdSql.query(self.__alter_pass_sql(self.__user_list[i], self.__passwd_list[i] ) )
                self.__init_pass = count == self.users_count - 1

    def alter_pass_err(self):  # sourcery skip: remove-redundant-fstring
        sqls = [
            f"alter users {self.__user_list[0]} pass 'newpass' " ,
            f"alter user {self.__user_list[0]} pass '' " ,
            f"alter user {self.__user_list[0]} pass '  ' " ,
            f"alter user anyuser pass 'newpass' " ,
            f"alter user {self.__user_list[0]} pass  " ,
            f"alter user {self.__user_list[0]} password 'newpass'  " ,
        ]
        for sql in sqls:
            tdSql.error(sql)


    def grant_user_privileges(self, privilege,  dbname=None, user_name="root"):
        return f"GRANT {privilege} ON {self.__priv_level(dbname)} TO {user_name} "

    def test_user_create(self):
        self.create_user_current()
        self.create_user_err()

    def test_alter_pass(self):
        self.alter_pass_current()
        self.alter_pass_err()

    def user_login(self, user, passwd):
        login_except = False
        try:
            with taos_connect(user=user, passwd=passwd) as conn:
                cursor = conn.cursor
        except BaseException:
            login_except = True
            cursor = None
        return login_except, cursor

    def login_currrent(self, user, passwd):
        login_except, _ = self.user_login(user, passwd)
        if login_except:
            tdLog.exit(f"connect failed, user: {user} and pass: {passwd} do not match!")
        else:
            tdLog.info("connect successfully, user and pass matched!")


    def login_err(self, user, passwd):
        login_except, _ = self.user_login(user, passwd)
        if login_except:
            tdLog.info("connect failed, except error occured!")
        else:
            tdLog.exit("connect successfully, except error not occrued!")

    def __drop_user(self, user):
        return f"DROP USER {user}"

    def drop_user_current(self):
        for user in self.__user_list:
            tdSql.query(self.__drop_user(user))

    def drop_user_error(self):
        sqls = [
            f"DROP {self.__user_list[0]}",
            f"DROP user {self.__user_list[0]}  {self.__user_list[1]}",
            f"DROP user {self.__user_list[0]} , {self.__user_list[1]}",
            f"DROP users {self.__user_list[0]}  {self.__user_list[1]}",
            f"DROP users {self.__user_list[0]} , {self.__user_list[1]}",
            # "DROP user root",
            "DROP user abcde",
            "DROP user ALL",
        ]

        for sql in sqls:
            tdSql.error(sql)

    def test_drop_user(self):
        # must drop err first
        self.drop_user_error()
        self.drop_user_current()

    def run(self):

        # 默认只有 root 用户
        tdLog.printNoPrefix("==========step0: init, user list only has root account")
        tdSql.query("show users")
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 1, "super")

        # root用户权限
        # 创建用户测试
        tdLog.printNoPrefix("==========step1: create user test")
        self.users_count = 5
        self.test_user_create()

        # 查看用户
        tdLog.printNoPrefix("==========step2: show user test")
        tdSql.query("show users")
        tdSql.checkRows(self.users_count + 1)

        # 密码登录认证
        self.login_currrent(self.__user_list[0], self.__passwd_list[0])
        self.login_err(self.__user_list[0], f"new{self.__passwd_list[0]}")

        # 修改密码
        tdLog.printNoPrefix("==========step3: alter user pass test")
        self.test_alter_pass()

        # 密码修改后的登录认证
        tdLog.printNoPrefix("==========step4: check login test")
        self.login_err(self.__user_list[0], self.__passwd_list[0])
        self.login_currrent(self.__user_list[0], f"new{self.__passwd_list[0]}")

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.query("show users")
        tdSql.checkRows(self.users_count + 1)

        # 普通用户权限
        # 密码登录
        # _, user = self.user_login(self.__user_list[0], f"new{self.__passwd_list[0]}")
        with taos_connect(user=self.__user_list[0], passwd=f"new{self.__passwd_list[0]}") as user:
            # user = conn
            # 不能创建用户
            tdLog.printNoPrefix("==========step5: normal user can not create user")
            user.error("create use utest1 pass 'utest1pass'")
            # 可以查看用户
            tdLog.printNoPrefix("==========step6: normal user can show user")
            user.query("show users")
            assert user.queryRows == self.users_count + 1
            # 不可以修改其他用户的密码
            tdLog.printNoPrefix("==========step7: normal user can not alter other user pass")
            user.error(self.__alter_pass_sql(self.__user_list[1], self.__passwd_list[1] ))
            user.error(self.__alter_pass_sql("root", "taosdata_root" ))
            # 可以修改自己的密码
            tdLog.printNoPrefix("==========step8: normal user can alter owner pass")
            user.query(self.__alter_pass_sql(self.__user_list[0], self.__passwd_list[0]))
            # 不可以删除用户，包括自己
            tdLog.printNoPrefix("==========step9: normal user can not drop any user ")
            user.error(f"drop user {self.__user_list[0]}")
            user.error(f"drop user {self.__user_list[1]}")
            user.error("drop user root")

        # root删除用户测试
        tdLog.printNoPrefix("==========step10: super user drop normal user")
        self.test_drop_user()

        tdSql.query("show users")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 1, "super")

        tdDnodes.stop(1)
        tdDnodes.start(1)

        # 删除后无法登录
        self.login_err(self.__user_list[0], self.__passwd_list[0])
        self.login_err(self.__user_list[0], f"new{self.__passwd_list[0]}")
        self.login_err(self.__user_list[1], self.__passwd_list[1])
        self.login_err(self.__user_list[1], f"new{self.__passwd_list[1]}")

        tdSql.query("show users")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 1, "super")


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
