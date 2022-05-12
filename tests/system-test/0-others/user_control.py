import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *


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
        return self.cursor



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
            # length of passwd must <= 15
            "create user u1 pass 'u123456789012345' " ,
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

    @property
    def __alter_pass_sql(self):
        return [f'''ALTER USER {self.__user_list[0]} PASS 'new{self.__passwd_list[0]}' ''', f'''ALTER USER {self.__user_list[0]} PASS '{self.__passwd_list[0]}' ''']

    def alter_pass_current(self):
        self.__init_pass = True
        tdSql.execute(self.__alter_pass_sql[0]) if self.__init_pass else tdSql.execute(self.__alter_pass_sql[1] )
        if self.__init_pass:
            tdSql.query(self.__alter_pass_sql[0])
            self.__init_pass = False
        else:
            tdSql.query(self.__alter_pass_sql[1] )
            self.__init_pass = True

    def alter_pass_err(self):
        sqls = [
            f"alter users {self.__user_list[0]} pass 'newpass' "
            f"alter user {self.__user_list[0]} pass '' "
            f"alter user {self.__user_list[0]} pass '  ' "
            f"alter user anyuser pass 'newpass' "
            f"alter user {self.__user_list[0]} pass  "
            f"alter user {self.__user_list[0]} password 'newpass'  "
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
                cursor = conn.cursor()
        except BaseException:
            login_except = True
            cursor = None

        return login_except, cursor

    def login_currrent(self, user, passwd):
        login_except, _ = self.user_login(user, passwd)
        if login_except:
            tdLog.info("connect successfully, user and pass matched!")
        else:
            tdLog.exit("connect failed, user and pass do not match!")

    def login_err(self, user, passwd):
        login_except, _ = self.user_login(user, passwd)
        if login_except:
            tdLog.exit("connect successfully, except error not occrued!")
        else:
            tdLog.info("connect failed, except error occured!")



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
        tdSql.checkRows(self.users_count + 2)

        # 修改密码
        tdLog.printNoPrefix("==========step3: alter user pass test")
        self.test_alter_pass()


        # 密码登录认证
        tdLog.printNoPrefix("==========step3: alter user pass test")
        self.login_err("err1", "passwd1")
        self.login_currrent(self.__user_list[0], self.__passwd_list[0])



        # 删除用户测试
        tdLog.printNoPrefix("==========step2: drop user")







    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
