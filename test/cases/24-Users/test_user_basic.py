
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck
from new_test_framework.utils.sqlset import TDSetSql
from itertools import product
from taos.tmq import *
import taos
import time
import random

class TestUserBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_user_basic(self):
        tdLog.info(f"=============== step0")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkKeyData("root", 1, 1)
        tdSql.checkKeyData("root", 2, 1)
        tdSql.checkKeyData("root", 3, 1)
        tdSql.checkKeyData("root", 4, 1)

        tdSql.execute(f"alter user root pass 'taosdata'")

        tdSql.error(f"ALTER USER root SYSINFO 0")
        tdSql.error(f"ALTER USER root SYSINFO 1")
        tdSql.error(f"ALTER USER root enable 0")
        tdSql.error(f"ALTER USER root enable 1")
        tdSql.error(f"ALTER USER root createdb 0")
        tdSql.error(f"ALTER USER root createdb 1")

        # sql_error create database db vgroups 1;
        tdSql.error(f"GRANT read ON db.* to root;")
        tdSql.error(f"GRANT read ON *.* to root;")
        tdSql.error(f"REVOKE read ON db.* from root;")
        tdSql.error(f"REVOKE read ON *.* from root;")
        tdSql.error(f"GRANT write ON db.* to root;")
        tdSql.error(f"GRANT write ON *.* to root;")
        tdSql.error(f"REVOKE write ON db.* from root;")
        tdSql.error(f"REVOKE write ON *.* from root;")
        tdSql.error(f"REVOKE write ON *.* from root;")

        tdSql.error(f"GRANT all ON *.* to root;")
        tdSql.error(f"REVOKE all ON *.* from root;")
        tdSql.error(f"GRANT read,write ON *.* to root;")
        tdSql.error(f"REVOKE read,write ON *.* from root;")

        tdLog.info(f"=============== step1: sysinfo create")
        tdSql.execute(f"CREATE USER u1 PASS 'taosdata' SYSINFO 0;")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdSql.checkKeyData("u1", 1, 0)
        tdSql.checkKeyData("u1", 2, 1)
        tdSql.checkKeyData("u1", 3, 0)
        tdSql.checkKeyData("u1", 4, 0)

        tdSql.execute(f"CREATE USER u2 PASS 'taosdata' SYSINFO 1;")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(3)

        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdLog.info(f"=============== step2: sysinfo alter")
        tdSql.execute(f"ALTER USER u1 SYSINFO 1")
        tdSql.query(f"select * from information_schema.ins_users")

        tdSql.checkKeyData("u1", 1, 0)
        tdSql.checkKeyData("u1", 2, 1)
        tdSql.checkKeyData("u1", 3, 1)
        tdSql.checkKeyData("u1", 4, 0)

        tdSql.execute(f"ALTER USER u1 SYSINFO 0")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkKeyData("u1", 1, 0)
        tdSql.checkKeyData("u1", 2, 1)
        tdSql.checkKeyData("u1", 3, 0)
        tdSql.checkKeyData("u1", 4, 0)

        tdSql.execute(f"ALTER USER u1 SYSINFO 0")
        tdSql.execute(f"ALTER USER u1 SYSINFO 0")

        tdSql.execute(f"drop user u1")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step3: enable alter")
        tdSql.execute(f"ALTER USER u2 enable 0")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 0)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdSql.execute(f"ALTER USER u2 enable 1")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdSql.execute(f"ALTER USER u2 enable 1")
        tdSql.execute(f"ALTER USER u2 enable 1")

        tdLog.info(f"=============== step4: createdb alter")
        tdSql.execute(f"ALTER USER u2 createdb 1")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)
        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 1)

        tdSql.execute(f"ALTER USER u2 createdb 0")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdLog.info(f"=============== restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step5: enable privilege")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdSql.error(f"CREATE USER u100 PASS 'taosdata' SYSINFO -1;")
        tdSql.error(f"CREATE USER u101 PASS 'taosdata' SYSINFO 2;")
        tdSql.error(f"CREATE USER u102 PASS 'taosdata' SYSINFO 20000;")
        tdSql.error(f"CREATE USER u103 PASS 'taosdata' SYSINFO 1000;")
        tdSql.error(f"ALTER USER u1 enable -1")
        tdSql.error(f"ALTER USER u1 enable 2")
        tdSql.error(f"ALTER USER u1 enable 1000")
        tdSql.error(f"ALTER USER u1 enable 10000")
        tdSql.error(f"ALTER USER u1 sysinfo -1")
        tdSql.error(f"ALTER USER u1 sysinfo 2")
        tdSql.error(f"ALTER USER u1 sysinfo 1000")
        tdSql.error(f"ALTER USER u1 sysinfo -20000")
        tdSql.error(f"ALTER USER u1 createdb -1")
        tdSql.error(f"ALTER USER u1 createdb 3")
        tdSql.error(f"ALTER USER u1 createdb 1000")
        tdSql.error(f"ALTER USER u1 createdb 100000")

        print("do create user basic .................. [passed]")

    #
    # ------------------- test_user_privilege_multi_users.py ----------------
    #
    def initVar(self):
        self.setsql = TDSetSql()
        # user info
        self.userNum = 100
        self.basic_username = "user"
        self.password = "test123@#$"

        # db info
        self.dbname = "user_privilege_multi_users"
        self.stbname = 'stb'
        self.ctbname_num = 100
        self.column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
        }
        self.tag_dict = {
            'ctbname': 'binary(10)'
        }
        
        self.privilege_list = []

    def prepare_data1(self):
        """Create the db and data for test
        """
        # create datebase
        tdSql.execute(f"create database {self.dbname}")
        tdLog.debug("sql:" + f"create database {self.dbname}")
        tdSql.execute(f"use {self.dbname}")
        tdLog.debug("sql:" + f"use {self.dbname}")

        # create super table
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
        tdLog.debug("Create stable {} successfully".format(self.stbname))
        for ctbIndex in range(self.ctbname_num):
            ctname = f"ctb{ctbIndex}"
            tdSql.execute(f"create table {ctname} using {self.stbname} tags('{ctname}')")
            tdLog.debug("sql:" + f"create table {ctname} using {self.stbname} tags('{ctname}')")

    def create_multiusers(self):
        """Create the user for test
        """
        for userIndex in range(self.userNum):
            username = f"{self.basic_username}{userIndex}"
            tdSql.execute(f'create user {username} pass "{self.password}"')
            tdLog.debug("sql:" + f'create user {username} pass "{self.password}"')

    def grant_privilege(self):
        """Add the privilege for the users
        """
        try:
            for userIndex in range(self.userNum):
                username = f"{self.basic_username}{userIndex}"
                privilege = random.choice(["read", "write", "all"])
                condition = f"ctbname='ctb{userIndex}'"
                self.privilege_list.append({
                    "username": username,
                    "privilege": privilege,
                    "condition": condition
                })
                tdSql.execute(f'grant {privilege} on {self.dbname}.{self.stbname} with {condition} to {username}')
                tdLog.debug("sql:" + f'grant {privilege} on {self.dbname}.{self.stbname} with {condition} to {username}')
        except Exception as ex:
            tdLog.exit(ex)

    def remove_privilege(self):
        """Remove the privilege for the users
        """
        try:
            for item in self.privilege_list:
                username = item["username"]
                privilege = item["privilege"]
                condition = item["condition"]
                tdSql.execute(f'revoke {privilege} on {self.dbname}.{self.stbname} with {condition} from {username}')
                tdLog.debug("sql:" + f'revoke {privilege} on {self.dbname}.{self.stbname} with {condition} from {username}')
        except Exception as ex:
            tdLog.exit(ex)

    def do_user_privilege_multi_users(self):
        self.initVar()
        self.create_multiusers()
        self.prepare_data1()
        # grant privilege to users
        self.grant_privilege()
        # check information_schema.ins_user_privileges
        tdSql.query("select * from information_schema.ins_user_privileges;")
        tdLog.debug("Current information_schema.ins_user_privileges values: {}".format(tdSql.queryResult))
        if len(tdSql.queryResult) >= self.userNum:
            tdLog.debug("case passed")
        else:
            tdLog.exit("The privilege number in information_schema.ins_user_privileges is incorrect")
        tdSql.query("select * from information_schema.ins_columns where db_name='{self.dbname}';")

        # remove the privilege
        self.remove_privilege()
        # clear env
        tdSql.execute(f"drop database {self.dbname}")
        # remove the users
        for userIndex in range(self.userNum):
            username = f"{self.basic_username}{userIndex}"
            tdSql.execute(f'drop user {username}')  
        # close the connection
        tdLog.success("%s successfully executed" % __file__)

        print("do multi user privileges ............. [passed]")

    #
    # ------------------- test_user_manager.py ----------------
    #
    def initData(self):
        self.setsql = TDSetSql()
        self.stbname = 'stb'
        self.binary_length = 20  # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict
        self.column_dict = {
            'ts': 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': f'binary({self.binary_length})',
            'col13': f'nchar({self.nchar_length})'
        }
        self.tag_dict = {
            'ts_tag': 'timestamp',
            't1': 'tinyint',
            't2': 'smallint',
            't3': 'int',
            't4': 'bigint',
            't5': 'tinyint unsigned',
            't6': 'smallint unsigned',
            't7': 'int unsigned',
            't8': 'bigint unsigned',
            't9': 'float',
            't10': 'double',
            't11': 'bool',
            't12': f'binary({self.binary_length})',
            't13': f'nchar({self.nchar_length})'
        }
        self.tag_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        self.values_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        self.tbnum = 1

    def prepare_data2(self):
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
            for j in self.values_list:
                tdSql.execute(f'insert into {self.stbname}_{i} values({j})')

    def create_user(self):
        for user_name in ['jiacy1_all', 'jiacy1_read', 'jiacy1_write', 'jiacy1_none', 'jiacy0_all', 'jiacy0_read',
                          'jiacy0_write', 'jiacy0_none']:
            if 'jiacy1' in user_name.lower():
                tdSql.execute(f'create user {user_name} pass "123abc!@#" sysinfo 1')
            elif 'jiacy0' in user_name.lower():
                tdSql.execute(f'create user {user_name} pass "123abc!@#" sysinfo 0')
        for user_name in ['jiacy1_all', 'jiacy1_read', 'jiacy0_all', 'jiacy0_read']:
            tdSql.execute(f'grant read on db to {user_name}')
        for user_name in ['jiacy1_all', 'jiacy1_write', 'jiacy0_all', 'jiacy0_write']:
            tdSql.execute(f'grant write on db to {user_name}')

    def user_privilege_check(self):
        jiacy1_read_conn = taos.connect(user='jiacy1_read', password='123abc!@#')
        sql = "create table ntb (ts timestamp,c0 int)"
        expectErrNotOccured = True
        try:
            jiacy1_read_conn.execute(sql)
        except BaseException:
            expectErrNotOccured = False
        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        else:
            self.queryRows = 0
            self.queryCols = 0
            self.queryResult = None
            tdLog.info(f"sql:{sql}, expect error occured")
        pass

    def drop_topic(self):
        jiacy1_all_conn = taos.connect(user='jiacy1_all', password='123abc!@#')
        jiacy1_read_conn = taos.connect(user='jiacy1_read', password='123abc!@#')
        jiacy1_write_conn = taos.connect(user='jiacy1_write', password='123abc!@#')
        jiacy1_none_conn = taos.connect(user='jiacy1_none', password='123abc!@#')
        jiacy0_all_conn = taos.connect(user='jiacy0_all', password='123abc!@#')
        jiacy0_read_conn = taos.connect(user='jiacy0_read', password='123abc!@#')
        jiacy0_write_conn = taos.connect(user='jiacy0_write', password='123abc!@#')
        jiacy0_none_conn = taos.connect(user='jiacy0_none', password='123abc!@#')
        tdSql.execute('create topic root_db as select * from db.stb')
        for user in [jiacy1_all_conn, jiacy1_read_conn, jiacy0_all_conn, jiacy0_read_conn]:
            user.execute(f'create topic db_jiacy as select * from db.stb')
            user.execute('drop topic db_jiacy')
        for user in [jiacy1_write_conn, jiacy1_none_conn, jiacy0_write_conn, jiacy0_none_conn, jiacy1_all_conn,
                     jiacy1_read_conn, jiacy0_all_conn, jiacy0_read_conn]:
            sql_list = []
            if user in [jiacy1_all_conn, jiacy1_read_conn, jiacy0_all_conn, jiacy0_read_conn]:
                sql_list = ['drop topic root_db']
            elif user in [jiacy1_write_conn, jiacy1_none_conn, jiacy0_write_conn, jiacy0_none_conn]:
                sql_list = ['drop topic root_db', 'create topic db_jiacy as select * from db.stb']
            for sql in sql_list:
                expectErrNotOccured = True
                try:
                    user.execute(sql)
                except BaseException:
                    expectErrNotOccured = False
                if expectErrNotOccured:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
                else:
                    self.queryRows = 0
                    self.queryCols = 0
                    self.queryResult = None
                    tdLog.info(f"sql:{sql}, expect error occured")

    def tmq_commit_cb_print(tmq, resp, param=None):
        print(f"commit: {resp}, tmq: {tmq}, param: {param}")

    def subscribe_topic(self):
        print("create topic")
        tdSql.execute('create topic db_topic as select * from db.stb')
        tdSql.execute('grant subscribe on db_topic to jiacy1_all')
        print("build consumer")
        tmq = Consumer({"group.id": "tg2", "td.connect.user": "jiacy1_all", "td.connect.pass": "123abc!@#",
                        "enable.auto.commit": "true"})
        print("build topic list")
        tmq.subscribe(["db_topic"])
        print("basic consume loop")
        c = 0
        l = 0
        for i in range(10):
            if c > 10:
                break
            res = tmq.poll(10)
            print(f"loop {l}")
            l += 1
            if not res:
                print(f"received empty message at loop {l} (committed {c})")
                continue
            if res.error():
                print(f"consumer error at loop {l} (committed {c}) {res.error()}")
                continue

            c += 1
            topic = res.topic()
            db = res.database()
            print(f"topic: {topic}\ndb: {db}")

            for row in res:
                print(row.fetchall())
            print("* committed")
            tmq.commit(res)

    def do_user_manage(self):
        self.initData()
        tdSql.prepare()
        self.create_user()
        self.prepare_data2()
        self.drop_topic()
        self.user_privilege_check()
        self.subscribe_topic()

        print("do user manager ............. [passed]")
    
    #
    # ------------------- main ----------------
    #
    def test_user_basic(self):
        """User: basic test

        1. Verifies root user default privileges and restrictions on privilege modification attempts
        2. Tests creation of users with different SYSINFO privilege levels (0/1)
        3. Validates privilege alteration for enable/createdb/SYSINFO flags
        4. Check system persistence after dnode restart
        5. Ensures proper error handling for invalid privilege values
        6. Check create multi users and grant/revoke privilege for them
        7. Subscribe topic with different user privileges

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/basic.sim
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_user_privilege_multi_users.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_user_manager.py

        """
        self.do_user_basic()
        self.do_user_privilege_multi_users()
        self.do_user_manage()