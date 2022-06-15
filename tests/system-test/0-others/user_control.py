from tabnanny import check
import taos
import time
import inspect
import traceback
import socket
from dataclasses  import dataclass

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

PRIVILEGES_ALL      = "ALL"
PRIVILEGES_READ     = "READ"
PRIVILEGES_WRITE    = "WRITE"

WEIGHT_ALL      = 5
WEIGHT_READ     = 2
WEIGHT_WRITE    = 3

PRIMARY_COL = "ts"

INT_COL     = "c1"
BINT_COL    = "c2"
SINT_COL    = "c3"
TINT_COL    = "c4"
FLOAT_COL   = "c5"
DOUBLE_COL  = "c6"
BOOL_COL    = "c7"

BINARY_COL  = "c8"
NCHAR_COL   = "c9"
TS_COL      = "c10"

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
    host    = socket.gethostname(),
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


@dataclass
class User:
    name        : str   = None
    passwd      : str   = None
    db_set      : set   = None
    priv        : str   = None
    priv_weight : int   = 0

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    @property
    def __user_list(self):
        return  [f"user_test{i}" for i in range(self.users_count) ]

    def __users(self):
        self.users = []
        self.root_user = User()
        self.root_user.name = "root"
        self.root_user.passwd = "taosdata"
        self.root_user.db_set = set("*")
        self.root_user.priv = PRIVILEGES_ALL
        self.root_user.priv_weight = WEIGHT_ALL
        for i in range(self.users_count):
            user = User()
            user.name = f"user_test{i}"
            user.passwd = f"taosdata{i}"
            user.db_set = set()
            self.users.append(user)
        return self.users

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

    def __grant_user_privileges(self, privilege,  dbname=None, user_name="root"):
        return f"GRANT {privilege} ON {self.__priv_level(dbname)} TO {user_name} "

    def __revoke_user_privileges(self, privilege,  dbname=None, user_name="root"):
        return f"REVOKE {privilege} ON {self.__priv_level(dbname)} FROM {user_name} "

    def __user_check(self, user:User=None, check_priv=PRIVILEGES_ALL):
        if user is None:
            user = self.root_user
        with taos_connect(user=user.name, passwd=user.passwd) as use:
            time.sleep(2)
            if check_priv == PRIVILEGES_ALL:
                use.query("use db")
                use.query("show tables")
                use.query("select * from ct1")
                use.query("insert into t1 (ts) values (now())")
            elif check_priv == PRIVILEGES_READ:
                use.query("use db")
                use.query("show tables")
                use.query("select * from ct1")
                use.error("insert into t1 (ts) values (now())")
            elif check_priv == PRIVILEGES_WRITE:
                use.query("use db")
                use.query("show tables")
                use.error("select * from ct1")
                use.query("insert into t1 (ts) values (now())")
            elif check_priv is None:
                use.error("use db")
                use.error("show tables")
                use.error("select * from db.ct1")
                use.error("insert into db.t1 (ts) values (now())")

    def __change_user_priv(self, user: User, pre_priv, invoke=False):
        if user.priv == pre_priv and invoke :
            return
        if user.name == "root":
            return

        if pre_priv.upper() == PRIVILEGES_ALL:
            pre_weight = -5 if invoke else 5
        elif pre_priv.upper() == PRIVILEGES_READ:
            pre_weight = -2 if invoke else 2
        elif pre_priv.upper() == PRIVILEGES_WRITE:
            pre_weight = -3 if invoke else 3
        else:
            return
        pre_weight += user.priv_weight

        if pre_weight >= 5:
            user.priv = PRIVILEGES_ALL
            user.priv_weight = 5
        elif pre_weight == 3:
            user.priv = PRIVILEGES_WRITE
            user.priv_weight = pre_weight
        elif pre_weight == 2:
            user.priv_weight = pre_weight
            user.priv = PRIVILEGES_READ
        elif pre_weight in [1, -1]:
            return
        elif pre_weight <= 0:
            user.priv_weight = 0
            user.priv = ""

        return user

    def grant_user(self, user: User = None, priv=PRIVILEGES_ALL, dbname=None):
        if not user:
            user = self.root_user
        sql = self.__grant_user_privileges(privilege=priv, dbname=dbname, user_name=user.name)
        tdLog.info(sql)
        if (user not in self.users and user.name != "root") or priv not in (PRIVILEGES_ALL, PRIVILEGES_READ, PRIVILEGES_WRITE):
            tdSql.error(sql)
        tdSql.query(sql)
        self.__change_user_priv(user=user, pre_priv=priv)
        user.db_set.add(dbname)
        time.sleep(1)

    def revoke_user(self, user: User = None, priv=PRIVILEGES_ALL, dbname=None):
        sql = self.__revoke_user_privileges(privilege=priv, dbname=dbname, user_name=user.name)
        tdLog.info(sql)
        if user is None or priv not in (PRIVILEGES_ALL, PRIVILEGES_READ, PRIVILEGES_WRITE):
            tdSql.error(sql)
        tdSql.query(sql)
        self.__change_user_priv(user=user, pre_priv=priv, invoke=True)
        if user.name != "root":
            user.db_set.discard(dbname) if dbname else user.db_set.clear()
        time.sleep(1)

    def test_priv_change_current(self):
        tdLog.printNoPrefix("==========step 1.0: if do not grant, can not read/write")
        self.__user_check(user=self.root_user)
        self.__user_check(user=self.users[0], check_priv=None)

        tdLog.printNoPrefix("==========step 1.1: grant read, can read, can not write")
        self.grant_user(user=self.users[0], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_READ)

        tdLog.printNoPrefix("==========step 1.2: grant write, can write")
        self.grant_user(user=self.users[1], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_WRITE)

        tdLog.printNoPrefix("==========step 1.3: grant all, can write and read")
        self.grant_user(user=self.users[2])
        self.__user_check(user=self.users[2], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.4:  grant read to write = all ")
        self.grant_user(user=self.users[0], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.5:  revoke write from all = read ")
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_READ)

        tdLog.printNoPrefix("==========step 1.6: grant write to read = all")
        self.grant_user(user=self.users[1], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.7:  revoke read from all = write ")
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_WRITE)

        tdLog.printNoPrefix("==========step 1.8: grant read to all = all")
        self.grant_user(user=self.users[0], priv=PRIVILEGES_ALL)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.9: grant write to all = all")
        self.grant_user(user=self.users[1], priv=PRIVILEGES_ALL)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.10: grant all to read = all")
        self.grant_user(user=self.users[0], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.11: grant all to write = all")
        self.grant_user(user=self.users[1], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_ALL)

        ### init user
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_WRITE)
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_READ)

        tdLog.printNoPrefix("==========step 1.12: revoke read from write = no change")
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_WRITE)

        tdLog.printNoPrefix("==========step 1.13: revoke write from read = no change")
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_READ)

        tdLog.printNoPrefix("==========step 1.14: revoke read from read = nothing")
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[0], check_priv=None)

        tdLog.printNoPrefix("==========step 1.15: revoke write from write = nothing")
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[1], check_priv=None)

        ### init user
        self.grant_user(user=self.users[0], priv=PRIVILEGES_READ)
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_WRITE)

        tdLog.printNoPrefix("==========step 1.16: revoke all from write = nothing")
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_ALL)
        self.__user_check(user=self.users[1], check_priv=None)

        tdLog.printNoPrefix("==========step 1.17: revoke all from read = nothing")
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_ALL)
        self.__user_check(user=self.users[0], check_priv=None)

        tdLog.printNoPrefix("==========step 1.18: revoke all from all = nothing")
        self.revoke_user(user=self.users[2], priv=PRIVILEGES_ALL)
        time.sleep(3)
        self.__user_check(user=self.users[2], check_priv=None)

    def __grant_err(self):
        return [
            self.__grant_user_privileges(privilege=self.__privilege[0], user_name="") ,
            self.__grant_user_privileges(privilege=self.__privilege[0], user_name="*") ,
            self.__grant_user_privileges(privilege=self.__privilege[1], dbname="not_exist_db", user_name=self.__user_list[0]),
            self.__grant_user_privileges(privilege="any_priv", user_name=self.__user_list[0]),
            self.__grant_user_privileges(privilege="", dbname="db", user_name=self.__user_list[0]) ,
            self.__grant_user_privileges(privilege=" ".join(self.__privilege), user_name=self.__user_list[0]) ,
            f"GRANT {self.__privilege[0]} ON * TO {self.__user_list[0]}" ,
            f"GRANT {self.__privilege[0]} ON db.t1 TO {self.__user_list[0]}" ,
        ]

    def __revoke_err(self):
        return [
            self.__revoke_user_privileges(privilege=self.__privilege[0], user_name="") ,
            self.__revoke_user_privileges(privilege=self.__privilege[0], user_name="*") ,
            self.__revoke_user_privileges(privilege=self.__privilege[1], dbname="not_exist_db", user_name=self.__user_list[0]),
            self.__revoke_user_privileges(privilege="any_priv", user_name=self.__user_list[0]),
            self.__revoke_user_privileges(privilege="", dbname="db", user_name=self.__user_list[0]) ,
            self.__revoke_user_privileges(privilege=" ".join(self.__privilege), user_name=self.__user_list[0]) ,
            f"REVOKE {self.__privilege[0]} ON * FROM {self.__user_list[0]}" ,
            f"REVOKE {self.__privilege[0]} ON db.t1 FROM {self.__user_list[0]}" ,
        ]

    def test_grant_err(self):
        for sql in self.__grant_err():
            tdSql.error(sql)

    def test_revoke_err(self):
        for sql in self.__revoke_err():
            tdSql.error(sql)

    def test_change_priv(self):
        self.test_grant_err()
        self.test_revoke_err()
        self.test_priv_change_current()

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

    def __create_tb(self):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table t1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')
            { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2}

    def __insert_data(self, rows):
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(rows):
            tdSql.execute(
                f"insert into ct1 values ( { now_time - i * 1000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into ct4 values ( { now_time - i * 7776000000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into ct2 values ( { now_time - i * 7776000000 }, {-i},  {-11111 * i}, {-111 * i % 32767 }, {-11 * i % 127}, {-1.11*i}, {-1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
        tdSql.execute(
            f'''insert into ct1 values
            ( { now_time - rows * 5 }, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar_测试_0', { now_time + 8 } )
            ( { now_time + 10000 }, { rows }, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar_测试_9', { now_time + 9 } )
            '''
        )

        tdSql.execute(
            f'''insert into ct4 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time +  7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000}, {pow(2,31)-pow(2,15)}, {pow(2,63)-pow(2,30)}, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000}
                )
            (
                { now_time + 2592000000 }, {pow(2,31)-pow(2,16)}, {pow(2,63)-pow(2,31)}, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000}
                )
            '''
        )

        tdSql.execute(
            f'''insert into ct2 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000 }, { -1 * pow(2,31) + pow(2,15) }, { -1 * pow(2,63) + pow(2,30) }, -32766, -126,
                { -1 * 3.2 * pow(10,38) }, { -1.2 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 2592000000 }, { -1 * pow(2,31) + pow(2,16) }, { -1 * pow(2,63) + pow(2,31) }, -32767, -127,
                { - 3.3 * pow(10,38) }, { -1.3 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }
                )
            '''
        )

        for i in range(rows):
            insert_data = f'''insert into t1 values
                ( { now_time - i * 3600000 }, {i}, {i * 11111}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_测试_{i}", { now_time - 1000 * i } )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into t1 values
            ( { now_time + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - (( rows // 2 ) * 60 + 30) * 60000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3600000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7200000 }, { pow(2,31) - pow(2,15) }, { pow(2,63) - pow(2,30) }, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 },
                "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 3600000 } , { pow(2,31) - pow(2,16) }, { pow(2,63) - pow(2,31) }, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 },
                "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }
                )
            '''
        )

    def run(self):
        tdSql.prepare()
        self.__create_tb()
        self.rows = 10
        self.users_count = 5
        self.__insert_data(self.rows)
        self.users = self.__users()

        tdDnodes.stop(1)
        tdDnodes.start(1)

        # 默认只有 root 用户
        tdLog.printNoPrefix("==========step0: init, user list only has root account")
        tdSql.query("show users")
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 1, "1")

        # root用户权限
        # 创建用户测试
        tdLog.printNoPrefix("==========step1: create user test")
        self.test_user_create()

        # 查看用户
        tdLog.printNoPrefix("==========step2: show user test")
        tdSql.query("show users")
        tdSql.checkRows(self.users_count + 1)

        # 密码登录认证
        self.login_currrent(self.__user_list[0], self.__passwd_list[0])
        self.login_err(self.__user_list[0], f"new{self.__passwd_list[0]}")

        # 用户权限设置
        self.test_change_priv()

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
        tdSql.checkData(0, 1, "1")

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
        tdSql.checkData(0, 1, "1")


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
