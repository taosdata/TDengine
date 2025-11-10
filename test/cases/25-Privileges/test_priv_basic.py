from new_test_framework.utils import tdLog, tdSql, etool, TDSql, TDSetSql
from itertools import product

from taos.tmq import *
import inspect
import taos
import time

class TestPrivBasic:
    """Add test case to cover the basic privilege test
    """
    def setup_class(self):
        pass

    #
    # ------------------- test_auth_basic.py ----------------
    #
    def prepare_data1(self):
        # database
        tdSql.execute("create database db;")
        tdSql.execute("use db;")
        
        # create super table
        tdSql.execute("create table stb(ts timestamp, col1 float, col2 int, col3 varchar(16)) tags(t1 int, t2 binary(20));")
        
        # create child table
        tdSql.execute("create table ct1 using stb tags(1, 'beijing');")
        # insert data to child table
        tdSql.execute("insert into ct1 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        
        # create reguler table
        tdSql.execute("create table rt1(ts timestamp, col1 float, col2 int, col3 varchar(16));")
        # insert data to reguler table
        tdSql.execute("insert into rt1 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")

    def create_user(self, user_name, passwd):
        tdSql.execute(f"create user {user_name} pass '{passwd}';")

    def delete_user(self, user_name):
        tdSql.execute(f"drop user {user_name};")

    def run_test_common_user_privileges(self):
        self.prepare_data1()
        # create user
        self.create_user("test", "test12@#*")
        # check user 'test' privileges
        testconn = taos.connect(user='test', password='test12@#*')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)
        # create db privilege
        testSql.error("create database db1;", expectErrInfo="Insufficient privilege for operation", fullMatched=False)
        # modify db、super table、child table、reguler table privilege
        testSql.error("alter database db buffer 512;")
        testSql.error("alter stable db.stb add column col4 float;")
        testSql.error("alter stable db.stb drop column col3;")
        testSql.error("alter stable db.stb modify column col3 varchar(32);")
        testSql.error("alter stable db.stb add tag t3 int;")
        testSql.error("alter stable db.stb drop tag t2;")
        testSql.error("alter stable db.stb rename tag t2 t21;")
        testSql.error("alter table ct1 add column col4 double;")
        testSql.error("alter table ct1 drop column col3;")
        testSql.error("alter table ct1 modify column col3 varchar(32);")
        testSql.error("alter table ct1 rename column col3 col4;")
        testSql.error("alter table ct1 set tag t1=11;")
        testSql.error("alter table rt1 add column col4 double;")
        testSql.error("alter table rt1 drop column col3;")
        testSql.error("alter table rt1 modify column col3 varchar(32);")
        testSql.error("alter table rt1 rename column col3 col4;")
        # create super table、child table、reguler table privilege
        testSql.error("create table stb2 (ts timestamp, col1 int) tags(t1 int, t2 varchar(16));")
        testSql.error("create table ct2 using db.stb tags(2, 'shanghai');")
        testSql.error("create table rt2 (ts timestamp, col1 int);")
        # query super table、child table、reguler table privilege
        testSql.error("select * from db.stb;")
        testSql.error("select * from db.ct1;")
        testSql.error("select * from db.rt1;")
        # query system table privilege
        testSql.query("select * from information_schema.ins_users;")
        testSql.checkRows(2)
        # drop db privilege
        testSql.error("drop database db;", expectErrInfo="Insufficient privilege for operation", fullMatched=False)
        # delete user
        self.delete_user("test")
        # drop db
        tdSql.execute("drop database db;")
        tdLog.info("test_common_user_privileges successfully executed")

    def run_test_common_user_with_createdb_privileges(self):
        self.prepare_data1()
        # create user
        self.create_user("test", "test12@#*")
        # check user 'test' privileges
        testconn = taos.connect(user='test', password='test12@#*')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)
        # grant create db privilege
        tdSql.execute("alter user test createdb 1;")
        # check user 'test' create db、super table、child table、reguler table、view privileges
        testSql.execute("create database db1;")
        testSql.execute("use db1;")
        testSql.execute("create stable stb1 (ts timestamp, col1 float, col2 int, col3 varchar(16)) tags(t1 int, t2 binary(20));")
        testSql.execute("create table ct1 using stb1 tags(1, 'beijing');")
        testSql.execute("insert into ct1 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        testSql.query("select * from stb1;")
        testSql.checkRows(2)
        testSql.query("select * from ct1;")
        testSql.checkRows(2)
        testSql.execute("create table rt1(ts timestamp, col1 float, col2 int, col3 varchar(16));")
        testSql.execute("insert into rt1 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        testSql.query("select * from rt1;")
        testSql.checkRows(2)
        testSql.execute("create view v1 as select * from stb1;")
        testSql.query("select * from v1;")
        testSql.checkRows(2)
        # check user 'test' alter db、super table、child table、reguler table privileges
        testSql.execute("alter database db1 buffer 512;")
        testSql.execute("alter stable db1.stb1 add column col4 varchar(16);")
        testSql.execute("alter stable db1.stb1 drop column col3;")
        testSql.execute("alter stable db1.stb1 modify column col4 varchar(32);")
        testSql.execute("alter stable db1.stb1 add tag t3 binary(16);")
        testSql.execute("alter stable db1.stb1 drop tag t2;")
        testSql.execute("alter table ct1 set tag t1=11;")
        testSql.execute("alter table rt1 add column col4 varchar(16);")
        testSql.execute("alter table rt1 drop column col3;")
        testSql.execute("alter table rt1 modify column col4 varchar(32);")
        testSql.execute("alter table rt1 rename column col4 col5;")
        # check user 'test' query privileges
        testSql.query("select * from db1.stb1;")
        testSql.checkRows(2)
        testSql.query("select * from db1.ct1;")
        testSql.checkRows(2)
        testSql.query("select * from db1.rt1;")
        testSql.checkRows(2)

        # create another user 'test1'
        self.create_user("test1", "test12@#*^%")
        test1conn = taos.connect(user='test1', password='test12@#*^%')
        cursor1 = test1conn.cursor()
        test1Sql = TDSql()
        test1Sql.init(cursor1)
        # check user 'test' doesn't privilege to grant create db privilege to another user
        testSql.error("alter user test1 createdb 1;", expectErrInfo="Insufficient privilege for operation", fullMatched=False)
        testSql.error("grant all on db1.stb1 to test1;", expectErrInfo="Insufficient privilege for operation", fullMatched=False)

        # grant read、write privilege to user 'test' and check user 'test' privileges
        tdSql.execute("grant all on db.* to test;")
        testSql.query("select * from db.stb;")
        testSql.checkRows(2)
        testSql.query("select * from db.ct1;")
        testSql.checkRows(2)
        testSql.query("select * from db.rt1;")
        testSql.checkRows(2)
        testSql.execute("create stable db.stb2 (ts timestamp, col1 float, col2 int, col3 varchar(16)) tags(t1 int, t2 binary(20));")
        testSql.execute("create table db.ct2 using db.stb2 tags(2, 'shenzhen');")
        testSql.execute("insert into db.ct2 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        testSql.query("select * from db.stb2;")
        testSql.checkRows(2)
        testSql.query("select * from db.ct2;")
        testSql.checkRows(2)
        # grant all privilege to user 'test1' and check user 'test1' privileges
        tdSql.execute("grant all on db1.* to test1;")
        test1Sql.query("select * from db1.stb1;")
        test1Sql.checkRows(2)
        test1Sql.query("select * from db1.ct1;")
        test1Sql.checkRows(2)
        test1Sql.query("select * from db1.rt1;")
        test1Sql.checkRows(2)
        test1Sql.execute("create stable db1.stb2 (ts timestamp, col1 float, col2 int, col3 varchar(16)) tags(t1 int, t2 binary(20));")
        test1Sql.execute("create table db1.ct2 using db1.stb2 tags(3, 'guangzhou');")
        test1Sql.execute("insert into db1.ct2 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        test1Sql.query("select * from db1.stb2;")
        test1Sql.checkRows(2)
        
        # revoke create db privilege from user 'test'
        tdSql.execute("alter user test createdb 0;")
        testSql.error("create database db2;", expectErrInfo="Insufficient privilege for operation", fullMatched=False)
        # check other privileges of user 'test'
        testSql.query("select * from db.stb;")
        testSql.checkRows(2)
        testSql.query("select * from db1.stb1;")
        testSql.checkRows(2)
        tdSql.execute("alter user test createdb 1;")
        testSql.execute("create database db2;")
        testSql.query("show databases;")
        tdLog.info(testSql.queryResult)
        testSql.checkRows(5)
        testSql.execute("drop view v1;")
        testSql.execute("drop table db1.rt1;")
        testSql.execute("drop table db1.ct1;")
        testSql.execute("drop table db1.stb1;")
        testSql.execute("drop database db1;")
        testSql.execute("drop database db2;")
        tdSql.execute("drop database db;")
        self.delete_user("test")
        self.delete_user("test1")

    def do_common_user_privileges(self):
        self.run_test_common_user_privileges()
        self.run_test_common_user_with_createdb_privileges()
        print("\n")
        print("do common user privileges ............. [passed]")
    
    #
    # ------------------- test_user_privilege.py ----------------
    #
    def setup_class2(self):
        self.setsql = TDSetSql()
        self.stbname = 'stb'
        self.user_name = 'test'
        self.binary_length = 20  # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict
        self.dbnames = ['db1', 'db2']
        self.column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
            'col3': 'float',
        }
        self.tag_dict = {
            't1': 'int',
            't2': f'binary({self.binary_length})'
        }
        self.tag_list = [
            f'1, "Beijing"',
            f'2, "Shanghai"',
            f'3, "Guangzhou"',
            f'4, "Shenzhen"'
        ]
        self.values_list = [
            f'now, 9.1, 200, 0.3'            
        ]
        self.tbnum = 4
        self.stbnum_grant = 200

    def grant_user(self):
        tdSql.execute(f'grant read on {self.dbnames[0]}.{self.stbname} with t2 = "Beijing" to {self.user_name}')
        tdSql.execute(f'grant write on {self.dbnames[1]}.{self.stbname} with t1 = 2 to {self.user_name}')
                
    def prepare_data2(self):
        for db in self.dbnames:
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"use {db}")
            tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
                for j in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values({j})')
            for i in range(self.stbnum_grant):
                tdSql.execute(f'create table {self.stbname}_grant_{i} (ts timestamp, c0 int) tags(t0 int)')
    
    def user_read_privilege_check(self, dbname):
        testconn = taos.connect(user='test', password='test123@#$')        
        expectErrNotOccured = False
        
        try:
            sql = f"select count(*) from {dbname}.stb where t2 = 'Beijing'"
            res = testconn.query(sql)
            data = res.fetch_all()
            count = data[0][0]            
        except BaseException:
            expectErrNotOccured = True
        
        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        elif count != 1:
            tdLog.exit(f"{sql}, expect result doesn't match")
        pass
    
    def user_write_privilege_check(self, dbname):
        testconn = taos.connect(user='test', password='test123@#$')        
        expectErrNotOccured = False
        
        try:            
            sql = f"insert into {dbname}.stb_1 values(now, 1.1, 200, 0.3)"
            testconn.execute(sql)            
        except BaseException:
            expectErrNotOccured = True
        
        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        else:
            pass
    
    def user_privilege_error_check(self):
        testconn = taos.connect(user='test', password='test123@#$')        
        expectErrNotOccured = False
        
        sql_list = [f"alter talbe {self.dbnames[0]}.stb_1 set t2 = 'Wuhan'", 
                    f"insert into {self.dbnames[0]}.stb_1 values(now, 1.1, 200, 0.3)", 
                    f"drop table {self.dbnames[0]}.stb_1", 
                    f"select count(*) from {self.dbnames[1]}.stb"]
        
        for sql in sql_list:
            try:
                res = testconn.execute(sql)                        
            except BaseException:
                expectErrNotOccured = True
            
            if expectErrNotOccured:
                pass
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        pass

    def user_privilege_grant_check(self):
        for db in self.dbnames:
            tdSql.execute(f"use {db}")
            for i in range(self.stbnum_grant):
                tdSql.execute(f'grant read on {db}.{self.stbname}_grant_{i} to {self.user_name}')
                tdSql.execute(f'grant write on {db}.{self.stbname}_grant_{i} to {self.user_name}')

    def do_grant_multi_tables(self):
        self.setup_class2()
        self.prepare_data2()
        self.create_user(self.user_name, "test123@#$")
        self.grant_user()
        self.user_read_privilege_check(self.dbnames[0])
        self.user_write_privilege_check(self.dbnames[1])
        self.user_privilege_error_check()
        self.user_privilege_grant_check()

        print("do grant multi tables ................. [passed]")
    
    #
    # ------------------- test_user_privilege_show.py ----------------
    #
    def setup_class3(self):
        tdLog.debug("start to execute %s" % __file__)
        self.setsql = TDSetSql()
        # user info
        self.username = 'test1'
        self.password = 'test123@#$'
        # db info
        self.dbname = "user_privilege_show"
        self.stbname = 'stb1'
        self.common_tbname = "tb1"
        self.ctbname_list = ["ct1", "ct2"]
        self.column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
        }
        self.tag_dict = {
            'ctbname': 'binary(10)'
        }

        # privilege check scenario info
        self.privilege_check_dic = {}
        self.senario_type = ["stable", "table", "ctable"]
        self.priv_type = ["read", "write", "all", "none"]
        # stable senarios
        # include the show stable xxx command test senarios and expect result, true as have privilege, false as no privilege
        # the list element is (db_privilege, stable_privilege, expect_res)
        st_senarios_list = []
        for senario in list(product(self.priv_type, repeat=2)):
            expect_res = True
            if senario == ("write", "write") or senario == ("none", "none") or senario == ("none", "write") or senario == ("write", "none"):
                expect_res = False
            st_senarios_list.append(senario + (expect_res,))
        # self.privilege_check_dic["stable"] = st_senarios_list

        # table senarios
        # the list element is (db_privilege, table_privilege, expect_res)
        self.privilege_check_dic["table"] = st_senarios_list
        
        # child table senarios
        # the list element is (db_privilege, stable_privilege, ctable_privilege, expect_res)
        ct_senarios_list = []
        for senario in list(product(self.priv_type, repeat=3)):
            expect_res = True
            if senario[2] == "write" or (senario[2] == "none" and senario[1] == "write") or (senario[2] == "none" and senario[1] == "none" and senario[0] == "write"):
                expect_res = False
            ct_senarios_list.append(senario + (expect_res,))
        self.privilege_check_dic["ctable"] = ct_senarios_list

    def prepare_data3(self, senario_type):
        """Create the db and data for test
        """
        if senario_type == "stable":
            # db name
            self.dbname = self.dbname + '_stable'
        elif senario_type == "table":
            # db name
            self.dbname = self.dbname + '_table'
        else:
            # db name
            self.dbname = self.dbname + '_ctable'

        # create datebase
        tdSql.execute(f"create database {self.dbname}")
        tdLog.debug("sql:" + f"create database {self.dbname}")
        tdSql.execute(f"use {self.dbname}")
        tdLog.debug("sql:" + f"use {self.dbname}")
        
        # create tables
        if "_stable" in self.dbname:
            # create stable
            tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
            tdLog.debug("Create stable {} successfully".format(self.stbname))
        elif "_table" in self.dbname:
            # create common table
            tdSql.execute(f"create table {self.common_tbname}(ts timestamp, col1 float, col2 int)")
            tdLog.debug("sql:" + f"create table {self.common_tbname}(ts timestamp, col1 float, col2 int)")
        else:
            # create stable and child table
            tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
            tdLog.debug("Create stable {} successfully".format(self.stbname))
            for ctname in self.ctbname_list:
                tdSql.execute(f"create table {ctname} using {self.stbname} tags('{ctname}')")
                tdLog.debug("sql:" + f"create table {ctname} using {self.stbname} tags('{ctname}')")

    def grant_privilege1(self, username, privilege, privilege_obj, ctable_include=False, tag_condition=None):
        """Add the privilege for the user
        """
        try:
            if ctable_include and tag_condition:
                tdSql.execute(f'grant {privilege} on {self.dbname}.{privilege_obj} with {tag_condition} to {username}')
                tdLog.debug("sql:" + f'grant {privilege} on {self.dbname}.{privilege_obj} with {tag_condition} to {username}')
            else:
                tdSql.execute(f'grant {privilege} on {self.dbname}.{privilege_obj} to {username}')
                tdLog.debug("sql:" + f'grant {privilege} on {self.dbname}.{privilege_obj} to {username}')
        except Exception as ex:
            tdLog.exit(ex)

    def remove_privilege(self, username, privilege, privilege_obj, ctable_include=False, tag_condition=None):
        """Remove the privilege for the user
        """
        try:
            if ctable_include and tag_condition:
                tdSql.execute(f'revoke {privilege} on {self.dbname}.{privilege_obj} with {tag_condition} from {username}')
                tdLog.debug("sql:" + f'revoke {privilege} on {self.dbname}.{privilege_obj} with {tag_condition} from {username}')
            else:
                tdSql.execute(f'revoke {privilege} on {self.dbname}.{privilege_obj} from {username}')
                tdLog.debug("sql:" + f'revoke {privilege} on {self.dbname}.{privilege_obj} from {username}')
        except Exception as ex:
            tdLog.exit(ex)

    def do_revoke_privilege(self):
        self.setup_class3()

        self.create_user(self.username, self.password)

        # temp solution only for the db read privilege verification
        self.prepare_data3("table")
        # grant db read privilege
        self.grant_privilege1(self.username, "read", "*")
        # create the taos connection with -utest -ptest
        testconn = taos.connect(user=self.username, password=self.password)
        testconn.execute("use %s;" % self.dbname)
        # show the user privileges
        res = testconn.query("select * from information_schema.ins_user_privileges;")
        tdLog.debug("Current information_schema.ins_user_privileges values: {}".format(res.fetch_all()))
        # query execution
        sql = "show create table " + self.common_tbname + ";"
        tdLog.debug("sql: %s" % sql)
        res = testconn.query(sql)
        # query result
        tdLog.debug("sql res:" + str(res.fetch_all()))
        # remove the privilege
        self.remove_privilege(self.username, "read", "*")
        # clear env
        testconn.close()
        tdSql.execute(f"drop database {self.dbname}")

        # remove the user
        tdSql.execute(f'drop user {self.username}')

        print("do user privileges show ............... [passed]")
    
    #
    # ------------------- test_user_privilege_all.py ----------------
    #
    def initAll(self):
        self.setsql = TDSetSql()
        # user info
        self.test_user = 'test_user'
        self.password = 'test123@#$'
        # db info
        self.dbname = "user_privilege_all_db"
        self.stbname = 'stb'
        self.common_tbname = "tb"
        self.ctbname_list = ["ct1", "ct2"]
        self.common_table_dict = {
            'ts':'timestamp',
            'col1':'float',
            'col2':'int'
        }
        self.stable_column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
        }
        self.tag_dict = {
            'ctbname': 'binary(10)'
        }

        # case list
        self.cases = {
            "test_db_table_both_no_permission": {
                "db_privilege": "none",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct1 using stb tags('ct1') values(now, 1.1, 1)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 3.3, 3);",
                    "select * from tb;"],
                "res": [False, False, False, False, False, False]
            },
            "test_db_no_permission_table_read": {
                "db_privilege": "none",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "read",
                "sql": ["insert into ct1 using stb tags('ct1') values(now, 1.1, 1)", 
                        "select * from stb;", 
                        "select * from ct1;", 
                        "select * from ct2;",
                        "insert into tb values(now, 3.3, 3);",
                        "select * from tb;"],
                "res": [False, False, False, False, False, True]
            },
            "test_db_no_permission_childtable_read": {
                "db_privilege": "none",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "read",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct1 using stb tags('ct1') values(now, 1.1, 1)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 3.3, 3);",
                    "select * from tb;"],
                "res": [False, True, True, False, False, False]
            },
            "test_db_no_permission_table_write": {
                "db_privilege": "none",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "write",
                "sql": ["insert into ct1 using stb tags('ct1') values(now, 1.1, 1)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 3.3, 3);",
                    "select * from tb;"],
                "res": [False, False, False, False, True, False]
            },
            "test_db_no_permission_childtable_write": {
                "db_privilege": "none",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "write",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 1.1, 1)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 3.3, 3);",
                    "select * from tb;"],
                "res": [True, False, False, False, False, False]
            },
            "test_db_read_table_no_permission": {
                "db_privilege": "read",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 1.1, 1)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 3.3, 3);",
                    "select * from tb;"],
                "res": [False, True, True, True, False, True]
            },
            "test_db_read_table_read": {
                "db_privilege": "read",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "read",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 1.1, 1)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 3.3, 3);",
                    "select * from tb;"],
                "res": [False, True, True, True, False, True]
            },
            "test_db_read_childtable_read": {
                "db_privilege": "read",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "read",
                "child_table_ct2_privilege": "read",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 1.1, 1)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 3.3, 3);",
                    "select * from tb;"],
                "res": [False, True, True, True, False, True]
            },
            "test_db_read_table_write": {
                "db_privilege": "read",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "write",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 1.1, 1)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 4.4, 4);",
                    "select * from tb;"],
                "res": [False, True, True, True, True, True]
            },
            "test_db_read_childtable_write": {
                "db_privilege": "read",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "write",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 1.1, 1)", 
                    "insert into ct1 using stb tags('ct1') values(now, 5.5, 5)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 4.4, 4);",
                    "select * from tb;"],
                "res": [False, True, True, True, True, False, True]
            },
            "test_db_write_table_no_permission": {
                "db_privilege": "write",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 6.6, 6)", 
                    "insert into ct1 using stb tags('ct1') values(now, 7.7, 7)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 8.8, 8);",
                    "select * from tb;"],
                "res": [True, True, False, False, False, True, False]
            },
            "test_db_write_table_write": {
                "db_privilege": "write",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 9.9, 9)", 
                    "insert into ct1 using stb tags('ct1') values(now, 10.0, 10)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 11.1, 11);",
                    "select * from tb;"],
                "res": [True, True, False, False, False, True, False]
            },
            "test_db_write_childtable_write": {
                "db_privilege": "write",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 12.2, 12)", 
                    "insert into ct1 using stb tags('ct1') values(now, 13.3, 13)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 14.4, 14);",
                    "select * from tb;"],
                "res": [True, True, False, False, False, True, False]
            },
            "test_db_write_table_read": {
                "db_privilege": "write",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "read",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 15.5, 15)", 
                    "insert into ct1 using stb tags('ct1') values(now, 16.6, 16)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 17.7, 17);",
                    "select * from tb;"],
                "res": [True, True, False, False, False, True, True]
            },
            "test_db_write_childtable_read": {
                "db_privilege": "write",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "read",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 18.8, 18)", 
                    "insert into ct1 using stb tags('ct1') values(now, 19.9, 19)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 20.0, 20);",
                    "select * from tb;"],
                "res": [True, True, True, True, False, True, False]
            },
            "test_db_all_childtable_none": {
                "db_privilege": "all",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 20.2, 20)", 
                    "insert into ct1 using stb tags('ct1') values(now, 21.21, 21)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 22.22, 22);",
                    "select * from tb;"],
                "res": [True, True, True, True, True, True, True]
            },
            "test_db_none_stable_all_childtable_none": {
                "db_privilege": "none",
                "stable_priviege": "all",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 23.23, 23)", 
                    "insert into ct1 using stb tags('ct1') values(now, 24.24, 24)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 25.25, 25);",
                    "select * from tb;"],
                "res": [True, True, True, True, True, False, False]
            },
            "test_db_no_permission_childtable_all": {
                "db_privilege": "none",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "all",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "none",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 26.26, 26)", 
                    "insert into ct1 using stb tags('ct1') values(now, 27.27, 27)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 28.28, 28);",
                    "select * from tb;"],
                "res": [False, True, True, True, False, False, False]
            },
            "test_db_none_stable_none_table_all": {
                "db_privilege": "none",
                "stable_priviege": "none",
                "child_table_ct1_privilege": "none",
                "child_table_ct2_privilege": "none",
                "table_tb_privilege": "all",
                "sql": ["insert into ct2 using stb tags('ct2') values(now, 26.26, 26)", 
                    "insert into ct1 using stb tags('ct1') values(now, 27.27, 27)", 
                    "select * from stb;", 
                    "select * from ct1;", 
                    "select * from ct2;", 
                    "insert into tb values(now, 29.29, 29);",
                    "select * from tb;"],
                "res": [False, False, False, False, False, True, True]
            }
        }

    def prepare_data4(self):
        """Create the db and data for test
        """
        tdLog.debug("Start to prepare the data for test")
        # create datebase
        tdSql.execute(f"create database {self.dbname}")
        tdSql.execute(f"use {self.dbname}")

        # create stable
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.stable_column_dict, self.tag_dict))
        tdLog.debug("Create stable {} successfully".format(self.stbname))

        # insert data into child table
        for ctname in self.ctbname_list:
            tdSql.execute(f"insert into {ctname} using {self.stbname} tags('{ctname}') values(now, 1.1, 1)")
            tdSql.execute(f"insert into {ctname} using {self.stbname} tags('{ctname}') values(now, 2.1, 2)")

        # create common table
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.common_tbname, self.common_table_dict))
        tdLog.debug("Create common table {} successfully".format(self.common_tbname))

        # insert data into common table 
        tdSql.execute(f"insert into {self.common_tbname} values(now, 1.1, 1)")
        tdSql.execute(f"insert into {self.common_tbname} values(now, 2.2, 2)")
        tdLog.debug("Finish to prepare the data")


    def grant_privilege2(self, test_user, privilege, table, tag_condition=None):
        """Add the privilege for the user
        """
        try:
            if tag_condition:
                tdSql.execute(f'grant {privilege} on {self.dbname}.{table} with {tag_condition} to {test_user}')
            else:
                tdSql.execute(f'grant {privilege} on {self.dbname}.{table} to {test_user}')
            time.sleep(2)
            tdLog.debug("Grant {} privilege on {}.{} with condition {} to {} successfully".format(privilege, self.dbname, table, tag_condition, test_user))
        except Exception as ex:
            tdLog.exit(ex)

    def remove_privilege(self, test_user, privilege, table, tag_condition=None):
        """Remove the privilege for the user
        """
        try:
            if tag_condition:
                tdSql.execute(f'revoke {privilege} on {self.dbname}.{table} with {tag_condition} from {test_user}')
            else:
                tdSql.execute(f'revoke {privilege} on {self.dbname}.{table} from {test_user}')
            tdLog.debug("Revoke {} privilege on {}.{} with condition {} from {} successfully".format(privilege, self.dbname, table, tag_condition, test_user))
        except Exception as ex:
            tdLog.exit(ex)

    def do_user_privilege_all(self):
        self.initAll()
        self.create_user(self.test_user, self.password)
        # prepare the test data
        self.prepare_data4()

        for case_name in self.cases.keys():
            tdLog.debug("Execute the case {} with params {}".format(case_name, str(self.cases[case_name])))
            # grant privilege for user test if case need
            if self.cases[case_name]["db_privilege"] != "none":
                self.grant_privilege2(self.test_user, self.cases[case_name]["db_privilege"], "*")
            if self.cases[case_name]["stable_priviege"] != "none":
                self.grant_privilege2(self.test_user, self.cases[case_name]["stable_priviege"], self.stbname)
            if self.cases[case_name]["child_table_ct1_privilege"] != "none" and self.cases[case_name]["child_table_ct2_privilege"] != "none":
                self.grant_privilege2(self.test_user, self.cases[case_name]["child_table_ct1_privilege"], self.stbname, "ctbname='ct1' or ctbname='ct2'")
            elif self.cases[case_name]["child_table_ct1_privilege"] != "none":
                self.grant_privilege2(self.test_user, self.cases[case_name]["child_table_ct1_privilege"], self.stbname, "ctbname='ct1'")
            elif self.cases[case_name]["child_table_ct2_privilege"] != "none":
                self.grant_privilege2(self.test_user, self.cases[case_name]["child_table_ct2_privilege"], self.stbname, "ctbname='ct2'")
            if self.cases[case_name]["table_tb_privilege"] != "none":
                self.grant_privilege2(self.test_user, self.cases[case_name]["table_tb_privilege"], self.common_tbname)
            # connect db with user test
            testconn = taos.connect(user=self.test_user, password=self.password)
            if case_name != "test_db_table_both_no_permission":
                testconn.execute("use %s;" % self.dbname)
            # check privilege of user test from ins_user_privileges table
            res = testconn.query("select * from information_schema.ins_user_privileges;")
            tdLog.debug("Current information_schema.ins_user_privileges values: {}".format(res.fetch_all()))
            # check privilege of user test by executing sql query
            for index in range(len(self.cases[case_name]["sql"])):
                tdLog.debug("Execute sql: {}".format(self.cases[case_name]["sql"][index]))
                try:
                    # for write privilege
                    if "insert " in self.cases[case_name]["sql"][index]:
                        testconn.execute(self.cases[case_name]["sql"][index])
                        # check the expected result
                        if self.cases[case_name]["res"][index]:
                            tdLog.debug("Write data with sql {} successfully".format(self.cases[case_name]["sql"][index]))
                    # for read privilege
                    elif "select " in self.cases[case_name]["sql"][index]:
                        res = testconn.query(self.cases[case_name]["sql"][index])
                        data = res.fetch_all()
                        tdLog.debug("query result: {}".format(data))
                        # check query results by cases
                        if case_name in ["test_db_no_permission_childtable_read", "test_db_write_childtable_read", "test_db_no_permission_childtable_all"] and self.cases[case_name]["sql"][index] == "select * from ct2;":
                            if not self.cases[case_name]["res"][index]:
                                if  0 == len(data):
                                    tdLog.debug("Query with sql {} successfully as expected with empty result".format(self.cases[case_name]["sql"][index]))
                                    continue
                                else:
                                    tdLog.exit("Query with sql {} failed with result {}".format(self.cases[case_name]["sql"][index], data))
                        # check the expected result
                        if self.cases[case_name]["res"][index]:
                            if len(data) > 0:
                                tdLog.debug("Query with sql {} successfully".format(self.cases[case_name]["sql"][index]))
                            else:
                                tdLog.exit("Query with sql {} failed with result {}".format(self.cases[case_name]["sql"][index], data))
                        else:
                            tdLog.exit("Execute query sql {} successfully, but expected failed".format(self.cases[case_name]["sql"][index]))
                except BaseException as ex:
                    # check the expect false result
                    if not self.cases[case_name]["res"][index]:
                        tdLog.debug("Execute sql {} failed with {} as expected".format(self.cases[case_name]["sql"][index], str(ex)))
                        continue
                    # unexpected exception
                    else:
                        tdLog.exit(ex)
            # remove the privilege
            if self.cases[case_name]["db_privilege"] != "none":
                self.remove_privilege(self.test_user, self.cases[case_name]["db_privilege"], "*")
            if self.cases[case_name]["stable_priviege"] != "none":
                self.remove_privilege(self.test_user, self.cases[case_name]["stable_priviege"], self.stbname)
            if self.cases[case_name]["child_table_ct1_privilege"] != "none":
                self.remove_privilege(self.test_user, self.cases[case_name]["child_table_ct1_privilege"], self.stbname, "ctbname='ct1'")
            if self.cases[case_name]["child_table_ct2_privilege"] != "none":
                self.remove_privilege(self.test_user, self.cases[case_name]["child_table_ct2_privilege"], self.stbname, "ctbname='ct2'")
            if self.cases[case_name]["table_tb_privilege"] != "none":
                self.remove_privilege(self.test_user, self.cases[case_name]["table_tb_privilege"], self.common_tbname)
            # close the connection of user test
            testconn.close()

        # remove the user
        tdSql.execute(f'drop user {self.test_user}')

        print("do user privileges all ................ [passed]") 

    #
    # ------------------- main ----------------
    #
    def test_priv_basic(self):
        """Privileges basic
        
        1. Test common user privileges
        2. Test common user with create database privilege
        3. Test grant read and write privileges with condition
        4. Test grant privilege on multiple tables
        5. Test grant privilege error cases
        6. Test revoke privilege
        7. Test grant on super/child/normal table
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-22 Alex Duan Migrated from uncatalog/army/authorith/test_auth_basic.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_user_privilege.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_user_privilege_show.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_user_privilege_all.py
        """
        self.do_common_user_privileges()
        self.do_grant_multi_tables()
        self.do_revoke_privilege()
        self.do_user_privilege_all()
