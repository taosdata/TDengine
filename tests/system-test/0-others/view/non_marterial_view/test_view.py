
import taos
import os
import sys
import time
from pathlib import Path
sys.path.append(os.path.dirname(Path(__file__).resolve().parent.parent.parent) + "/7-tmq")

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from util.sqlset import *
from tmqCommon import *

class TDTestCase:
    """This test case is used to veirfy the tmq consume data from non marterial view
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        
        # db info
        self.dbname = "view_db"
        self.stbname = 'stb'
        self.ctbname_list = ["ct1", "ct2"]
        self.stable_column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
        }
        self.tag_dict = {
            'ctbname': 'binary(10)'
        }

    def prepare_data(self, conn=None):
        """Create the db and data for test
        """
        tdLog.debug("Start to prepare the data")
        if not conn:
            conn = tdSql
        # create datebase
        conn.execute(f"create database {self.dbname}")
        conn.execute(f"use {self.dbname}")
        time.sleep(2)

        # create stable
        conn.execute(self.setsql.set_create_stable_sql(self.stbname, self.stable_column_dict, self.tag_dict))
        tdLog.debug("Create stable {} successfully".format(self.stbname))

        # create child tables
        for ctname in self.ctbname_list:
            conn.execute(f"create table {ctname} using {self.stbname} tags('{ctname}');")
            tdLog.debug("Create child table {} successfully".format(ctname))

            # insert data into child tables
            conn.execute(f"insert into {ctname} values(now, 1.1, 1)(now+1s, 2.2, 2)(now+2s, 3.3, 3)(now+3s, 4.4, 4)(now+4s, 5.5, 5)(now+5s, 6.6, 6)(now+6s, 7.7, 7)(now+7s, 8.8, 8)(now+8s, 9.9, 9)(now+9s, 10.1, 10);)")
            tdLog.debug(f"Insert into data to {ctname} successfully")

    def prepare_tmq_data(self, para_dic):
        tdLog.debug("Start to prepare the tmq data")
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, para_dic["dbName"], para_dic["dropFlag"], vgroups=para_dic["vgroups"], replica=1)
        tdLog.info("create stb")
        tdCom.create_stable(tdSql, dbname=para_dic["dbName"], stbname=para_dic["stbName"], column_elm_list=para_dic['colSchema'], tag_elm_list=para_dic['tagSchema'])
        tdLog.info("create ctb")
        tdCom.create_ctable(tdSql, dbname=para_dic["dbName"], stbname=para_dic["stbName"],tag_elm_list=para_dic['tagSchema'], count=para_dic["ctbNum"], default_ctbname_prefix=para_dic['ctbPrefix'])
        tdLog.info("insert data")
        tmqCom.insert_data(tdSql, para_dic["dbName"], para_dic["ctbPrefix"], para_dic["ctbNum"], para_dic["rowsPerTbl"], para_dic["batchNum"], para_dic["startTs"])
        tdLog.debug("Finish to prepare the tmq data")
        
    def check_view_num(self, num):
        tdSql.query("show views;")
        rows = tdSql.queryRows
        assert(rows == num)
        tdLog.debug(f"Verify the view number successfully")
        
    def create_user(self, username, password):
        tdSql.execute(f"create user {username} pass '{password}';")
        tdLog.debug("Create user {} with password {} successfully".format(username, password))
        
    def check_permissions(self, username, db_name, permission_dict, view_name=None):
        """
        :param permission_dict: {'db': ["read", "write], 'view': ["read", "write", "alter"]}
        """
        tdSql.query("select * from information_schema.ins_user_privileges;")
        for item in permission_dict.keys():
            if item == "db":
                for permission in permission_dict[item]:
                    assert((username, permission, db_name, "", "", "") in tdSql.queryResult)
                    tdLog.debug(f"Verify the {item} {db_name} {permission} permission successfully")
            elif item == "view":
                for permission in permission_dict[item]:
                    assert((username, permission, db_name, view_name, "", "view") in tdSql.queryResult)
                    tdLog.debug(f"Verify the {item} {db_name} {view_name} {permission} permission successfully")
            else:
                raise Exception(f"Invalid permission type: {item}")

    def test_create_view_from_one_database(self):
        """This test case is used to verify the create view from one database
        """
        self.prepare_data()
        tdSql.execute(f"create view v1 as select * from {self.stbname};")
        self.check_view_num(1)
        tdSql.error(f'create view v1 as select * from {self.stbname};', expectErrInfo='view already exists in db')
        tdSql.error(f'create view db2.v2 as select * from {self.stbname};', expectErrInfo='Fail to get table info, error: Database not exist')
        tdSql.error(f'create view v2 as select c2 from {self.stbname};', expectErrInfo='Invalid column name: c2')
        tdSql.error(f'create view v2 as select ts, col1 from tt1;', expectErrInfo='Fail to get table info, error: Table does not exist')

        tdSql.execute(f"drop database {self.dbname}")
        tdLog.debug("Finish test case 'test_create_view_from_one_database'")

    def test_create_view_from_multi_database(self):
        """This test case is used to verify the create view from multi database
        """
        self.prepare_data()
        tdSql.execute(f"create view v1 as select * from view_db.{self.stbname};")
        self.check_view_num(1)

        self.dbname = "view_db2"
        self.prepare_data()
        tdSql.execute(f"create view v1 as select * from view_db2.{self.stbname};")
        tdSql.execute(f"create view v2 as select * from view_db.v1;")
        self.check_view_num(2)

        self.dbname = "view_db"
        tdSql.execute(f"drop database view_db;")
        tdSql.execute(f"drop database view_db2;")
        tdLog.debug("Finish test case 'test_create_view_from_multi_database'")

    def test_create_view_name_params(self):
        """This test case is used to verify the create view with different view name params
        """
        self.prepare_data()
        tdSql.execute(f"create view v1 as select * from {self.stbname};")
        self.check_view_num(1)
        tdSql.error(f"create view v/2 as select * from {self.stbname};", expectErrInfo='syntax error near "/2 as select * from stb;"')
        tdSql.execute(f"create view v2 as select ts, col1 from {self.stbname};")
        self.check_view_num(2)
        view_name_192_characters = "rzuoxoIXilAGgzNjYActiQwgzZK7PZYpDuaOe1lSJMFMVYXaexh1OfMmk3LvJcQbTeXXW7uGJY8IHuweHF73VHgoZgf0waO33YpZiTKfDQbdWtN4YmR2eWjL84ZtkfjM4huCP6lCysbDMj8YNwWksTdUq70LIyNhHp2V8HhhxyYSkREYFLJ1kOE78v61MQT6"
        tdSql.execute(f"create view {view_name_192_characters} as select * from {self.stbname};")
        self.check_view_num(3)
        tdSql.error(f"create view {view_name_192_characters}1 as select * from {self.stbname};", expectErrInfo='Invalid identifier name: rzuoxoixilaggznjyactiqwgzzk7pzypduaoe1lsjmfmvyxaexh1ofmmk3lvjcqbtexxw7ugjy8ihuwehf73vhgozgf0wao33ypzitkfdqbdwtn4ymr2ewjl84ztkfjm4hucp6lcysbdmj8ynwwkstduq70liynhhp2v8hhhxyyskreyflj1koe78v61mqt61 as select * from stb;')
        tdSql.execute(f"drop database {self.dbname}")
        tdLog.debug("Finish test case 'test_create_view_name_params'")

    def test_create_view_query(self):
        """This test case is used to verify the create view with different data type in query
        """
        self.prepare_data()
        # add different data type table
        tdSql.execute(f"create table tb (ts timestamp, c1 int, c2 int unsigned, c3 bigint, c4 bigint unsigned, c5 float, c6 double, c7 binary(16), c8 smallint, c9 smallint unsigned, c10 tinyint, c11 tinyint unsigned, c12 bool, c13 varchar(16), c14 nchar(8), c15 geometry(21), c16 varbinary(16));")
        tdSql.execute(f"create view v1 as select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16 from tb;")
        # check data type in create view sql
        tdSql.query("desc v1;")
        res = tdSql.queryResult
        data_type_list = [res[index][1] for index in range(len(res))]
        tdLog.debug(data_type_list)
        assert('TIMESTAMP' in data_type_list and 'INT' in data_type_list and 'INT UNSIGNED' in data_type_list and 'BIGINT' in data_type_list and 'BIGINT UNSIGNED' in data_type_list and 'FLOAT' in data_type_list and 'DOUBLE' in data_type_list and 'VARCHAR' in data_type_list and 'SMALLINT' in data_type_list and 'SMALLINT UNSIGNED' in data_type_list and 'TINYINT' in data_type_list and 'TINYINT UNSIGNED' in data_type_list and 'BOOL' in data_type_list and 'VARCHAR' in data_type_list and 'NCHAR' in data_type_list and 'GEOMETRY' in data_type_list and 'VARBINARY' in data_type_list)
        tdSql.execute("create view v2 as select * from tb where c1 >5 and c7 like '%ab%';")
        self.check_view_num(2)
        tdSql.error("create view v3 as select * from tb where c1 like '%ab%';", expectErrInfo='Invalid operation')
        tdSql.execute("create view v3 as select first(ts), sum(c1) from tb group by c2 having avg(c4) > 0;")
        tdSql.execute("create view v4 as select _wstart,sum(c6) from tb interval(10s);")
        tdSql.execute("create view v5 as select * from tb join v2 on tb.ts = v2.ts;")
        tdSql.execute("create view v6 as select * from (select ts, c1, c2 from (select * from v2));")
        self.check_view_num(6)
        for v in ['v1', 'v2', 'v3', 'v4', 'v5', 'v6']:
            tdSql.execute(f"drop view {v};")
        tdSql.execute(f"drop database {self.dbname}")
        tdLog.debug("Finish test case 'test_create_view_query'")

    def test_show_view(self):
        """This test case is used to verify the show view
        """
        self.prepare_data()
        tdSql.execute(f"create view v1 as select * from {self.ctbname_list[0]};")

        # query from show sql
        tdSql.query("show views;")
        res = tdSql.queryResult
        assert(res[0][0] == 'v1' and res[0][1] == 'view_db' and res[0][2] == 'root' and res[0][4] == 'NORMAL' and res[0][5] == 'select * from ct1;')

        # show create sql
        tdSql.query("show create view v1;")
        res = tdSql.queryResult
        assert(res[0][1] == 'CREATE VIEW `view_db`.`v1` AS select * from ct1;')

        # query from desc results
        tdSql.query("desc view_db.v1;")
        res = tdSql.queryResult
        assert(res[0][1] == 'TIMESTAMP' and res[1][1] == 'FLOAT' and res[2][1] == 'INT')

        # query from system table
        tdSql.query("select * from information_schema.ins_views;")
        res = tdSql.queryResult
        assert(res[0][0] == 'v1' and res[0][1] == 'view_db' and res[0][2] == 'root' and res[0][4] == 'NORMAL' and res[0][5] == 'select * from ct1;')
        tdSql.error("show db3.views;", expectErrInfo='Database not exist')
        tdSql.error("desc viewx;", expectErrInfo='Table does not exist')
        tdSql.error(f"show create view {self.dbname}.viewx;", expectErrInfo='view not exists in db')
        tdSql.execute(f"drop database {self.dbname}")
        tdSql.error("show views;", expectErrInfo='Database not exist')
        tdLog.debug("Finish test case 'test_show_view'")

    def test_drop_view(self):
        """This test case is used to verify the drop view
        """
        self.prepare_data()
        self.dbname = "view_db2"
        self.prepare_data()
        tdSql.execute("create view view_db.v1 as select * from view_db.stb;")
        tdSql.execute("create view view_db2.v1 as select * from view_db2.stb;")
        # delete view without database name
        tdSql.execute("drop view v1;")
        # delete view with database name
        tdSql.execute("drop view view_db.v1;")
        # delete non exist view
        tdSql.error("drop view view_db.v11;", expectErrInfo='view not exists in db')
        tdSql.execute("drop database view_db")
        tdSql.execute("drop database view_db2;")
        self.dbname = "view_db"
        tdLog.debug("Finish test case 'test_drop_view'")

    def test_view_permission_db_all_view_all(self):
        """This test case is used to verify the view permission with db all and view all,
        the time sleep to wait the permission take effect
        """
        self.prepare_data()
        username = "view_test"
        password = "test"
        self.create_user(username, password)
        # grant all db permission to user
        tdSql.execute("grant all on view_db.* to view_test;")
        
        conn = taos.connect(user=username, password=password)
        conn.execute(f"use {self.dbname};")
        conn.execute("create view v1 as select * from stb;")
        res = conn.query("show views;")
        assert(len(res.fetch_all()) == 1)
        tdLog.debug(f"Verify the show view permission of user '{username}' with db all and view all successfully")
        self.check_permissions("view_test", "view_db", {"db": ["read", "write"], "view": ["read", "write", "alter"]}, "v1")
        tdLog.debug(f"Verify the view permission from system table successfully")
        time.sleep(2)
        conn.execute("drop view v1;")
        tdSql.execute("revoke all on view_db.* from view_test;")
        tdSql.execute(f"drop database {self.dbname};")
        time.sleep(1)

        # prepare data by user 'view_test'
        self.prepare_data(conn)

        conn.execute("create view v1 as select * from stb;")
        res = conn.query("show views;")
        assert(len(res.fetch_all()) == 1)
        tdLog.debug(f"Verify the view permission of user '{username}' with db all and view all successfully")
        self.check_permissions("view_test", "view_db", {"db": ["read", "write"], "view": ["read", "write", "alter"]}, "v1")
        tdLog.debug(f"Verify the view permission from system table successfully")
        time.sleep(2)
        conn.execute("drop view v1;")
        tdSql.execute("revoke all on view_db.* from view_test;")
        tdSql.execute("revoke all on view_db.v1 from view_test;")
        tdSql.execute(f"drop database {self.dbname}")
        tdSql.execute("drop user view_test;")
        tdLog.debug("Finish test case 'test_view_permission_db_all_view_all'")

    def test_view_permission_db_write_view_all(self):
        """This test case is used to verify the view permission with db write and view all
        """
        username = "view_test"
        password = "test"
        self.create_user(username, password)
        conn = taos.connect(user=username, password=password)
        self.prepare_data(conn)
        conn.execute("create view v1 as select * from stb;")
        tdSql.execute("revoke read on view_db.* from view_test;")
        self.check_permissions("view_test", "view_db", {"db": ["write"], "view": ["read", "write", "alter"]}, "v1")
        # create view permission error
        try:
            conn.execute("create view v2 as select * from v1;")
        except Exception as ex:
            assert("[0x2644]: Permission denied or target object not exist" in str(ex))
        # query from view permission error
        try:
            conn.query("select * from v1;")
        except Exception as ex:
            assert("[0x2644]: Permission denied or target object not exist" in str(ex))
        # view query permission
        res = conn.query("show views;")
        assert(len(res.fetch_all()) == 1)
        time.sleep(2)
        conn.execute("drop view v1;")
        tdSql.execute("revoke write on view_db.* from view_test;")
        tdSql.execute(f"drop database {self.dbname}")
        tdSql.execute("drop user view_test;")
        tdLog.debug("Finish test case 'test_view_permission_db_write_view_all'")

    def test_view_permission_db_write_view_read(self):
        """This test case is used to verify the view permission with db write and view read
        """
        username = "view_test"
        password = "test"
        self.create_user(username, password)
        conn = taos.connect(user=username, password=password)
        self.prepare_data()
        
        tdSql.execute("create view v1 as select * from stb;")
        tdSql.execute("grant write on view_db.* to view_test;")
        tdSql.execute("grant read on view_db.v1 to view_test;")
        
        conn.execute(f"use {self.dbname};")
        time.sleep(2)
        res = conn.query("select * from v1;")
        assert(len(res.fetch_all()) == 20)
        
        conn.execute("create view v2 as select * from v1;")
        # create view from super table of database
        try:
            conn.execute("create view v3 as select * from stb;")
        except Exception as ex:
            assert("[0x2644]: Permission denied or target object not exist" in str(ex))
        time.sleep(2)
        conn.execute("drop view v2;")
        try:
            conn.execute("drop view v1;")
        except Exception as ex:
            assert("[0x2644]: Permission denied or target object not exist" in str(ex))
        tdSql.execute("revoke read on view_db.v1 from view_test;")
        tdSql.execute("revoke write on view_db.* from view_test;")
        tdSql.execute(f"drop database {self.dbname}")
        tdSql.execute("drop user view_test;")
        tdLog.debug("Finish test case 'test_view_permission_db_write_view_read'")

    def test_view_permission_db_write_view_alter(self):
        """This test case is used to verify the view permission with db write and view alter
        """
        username = "view_test"
        password = "test"
        self.create_user(username, password)
        conn = taos.connect(user=username, password=password)
        self.prepare_data()
        
        tdSql.execute("create view v1 as select * from stb;")
        tdSql.execute("grant write on view_db.* to view_test;")
        tdSql.execute("grant alter on view_db.v1 to view_test;")
        try:
            conn.execute(f"use {self.dbname};")
            conn.execute("select * from v1;")
        except Exception as ex:
            assert("[0x2644]: Permission denied or target object not exist" in str(ex))
        time.sleep(2)
        conn.execute("drop view v1;")
        tdSql.execute("revoke write on view_db.* from view_test;")
        tdSql.execute(f"drop database {self.dbname}")
        tdSql.execute("drop user view_test;")
        tdLog.debug("Finish test case 'test_view_permission_db_write_view_alter'")

    def test_view_permission_db_read_view_all(self):
        """This test case is used to verify the view permission with db read and view all
        """
        username = "view_test"
        password = "test"
        self.create_user(username, password)
        conn = taos.connect(user=username, password=password)
        self.prepare_data()
        
        tdSql.execute("create view v1 as select * from stb;")
        tdSql.execute("grant read on view_db.* to view_test;")
        tdSql.execute("grant all on view_db.v1 to view_test;")
        try:
            conn.execute(f"use {self.dbname};")
            conn.execute("create view v2 as select * from v1;")
        except Exception as ex:
            assert("[0x2644]: Permission denied or target object not exist" in str(ex))
        time.sleep(2)
        res = conn.query("select * from v1;")
        assert(len(res.fetch_all()) == 20)
        conn.execute("drop view v1;")
        tdSql.execute("revoke read on view_db.* from view_test;")
        tdSql.execute(f"drop database {self.dbname}")
        tdSql.execute("drop user view_test;")
        tdLog.debug("Finish test case 'test_view_permission_db_read_view_all'")

    def test_view_permission_db_read_view_alter(self):
        """This test case is used to verify the view permission with db read and view alter
        """
        username = "view_test"
        password = "test"
        self.create_user(username, password)
        conn = taos.connect(user=username, password=password)
        self.prepare_data()
        
        tdSql.execute("create view v1 as select * from stb;")
        tdSql.execute("grant read on view_db.* to view_test;")
        tdSql.execute("grant alter on view_db.v1 to view_test;")
        try:
            conn.execute(f"use {self.dbname};")
            conn.execute("select * from v1;")
        except Exception as ex:
            assert("[0x2644]: Permission denied or target object not exist" in str(ex))
            
        time.sleep(2)
        conn.execute("drop view v1;")
        tdSql.execute("revoke read on view_db.* from view_test;")
        tdSql.execute(f"drop database {self.dbname}")
        tdSql.execute("drop user view_test;")
        tdLog.debug("Finish test case 'test_view_permission_db_read_view_alter'")

    def test_view_permission_db_read_view_read(self):
        """This test case is used to verify the view permission with db read and view read
        """
        username = "view_test"
        password = "test"
        self.create_user(username, password)
        conn = taos.connect(user=username, password=password)
        self.prepare_data()
        
        tdSql.execute("create view v1 as select * from stb;")
        tdSql.execute("grant read on view_db.* to view_test;")
        tdSql.execute("grant read on view_db.v1 to view_test;")
        conn.execute(f"use {self.dbname};")
        time.sleep(2)
        res = conn.query("select * from v1;")
        assert(len(res.fetch_all()) == 20)
        try:
            conn.execute("drop view v1;")
        except Exception as ex:
            assert("[0x2644]: Permission denied or target object not exist" in str(ex))
        tdSql.execute("revoke read on view_db.* from view_test;")
        tdSql.execute("revoke read on view_db.v1 from view_test;")
        tdSql.execute(f"drop database {self.dbname}")
        tdSql.execute("drop user view_test;")
        tdLog.debug("Finish test case 'test_view_permission_db_read_view_read'")

    def test_query_from_view(self):
        """This test case is used to verify the query from view
        """
        self.prepare_data()
        view_name_list = []

        # common query from super table
        tdSql.execute(f"create view v1 as select * from {self.stbname};")
        tdSql.query(f"select * from v1;")
        rows = tdSql.queryRows
        assert(rows == 20)
        view_name_list.append("v1")
        tdLog.debug("Verify the query from super table successfully")

        # common query from child table
        tdSql.execute(f"create view v2 as select * from {self.ctbname_list[0]};")
        tdSql.query(f"select * from v2;")
        rows = tdSql.queryRows
        assert(rows == 10)
        view_name_list.append("v2")
        tdLog.debug("Verify the query from child table successfully")

        # join query
        tdSql.execute(f"create view v3 as select * from {self.stbname} join {self.ctbname_list[1]} on {self.ctbname_list[1]}.ts = {self.stbname}.ts;")
        tdSql.query(f"select * from v3;")
        rows = tdSql.queryRows
        assert(rows == 10)
        view_name_list.append("v3")
        tdLog.debug("Verify the join query successfully")

        # group by query
        tdSql.execute(f"create view v4 as select count(*) from {self.stbname} group by tbname;")
        tdSql.query(f"select * from v4;")
        rows = tdSql.queryRows
        assert(rows == 2)
        res = tdSql.queryResult
        assert(res[0][0] == 10)
        view_name_list.append("v4")
        tdLog.debug("Verify the group by query successfully")

        # partition by query
        tdSql.execute(f"create view v5 as select sum(col1) from {self.stbname} where col2 > 4 partition by tbname interval(3s);")
        tdSql.query(f"select * from v5;")
        rows = tdSql.queryRows
        assert(rows >= 4)
        view_name_list.append("v5")
        tdLog.debug("Verify the partition by query successfully")

        # query from nested view
        tdSql.execute(f"create view v6 as select * from v5;")
        tdSql.query(f"select * from v6;")
        rows = tdSql.queryRows
        assert(rows >= 4)
        view_name_list.append("v6")
        tdLog.debug("Verify the query from nested view successfully")

        # delete view
        for view in view_name_list:
            tdSql.execute(f"drop view {view};")
            tdLog.debug(f"Drop view {view} successfully")
        tdSql.execute(f"drop database {self.dbname}")
        tdLog.debug("Finish test case 'test_query_from_view'")

    def test_tmq_from_view(self):
        """This test case is used to verify the tmq consume data from view
        """
        # params for db
        paraDict = {'dbName':     'view_db',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1}
        # topic info
        topic_name_list = ['topic1']
        view_name_list = ['view1']
        expectRowsList = []
        
        self.prepare_tmq_data(paraDict)
        
        # init consume info, and start tmq_sim, then check consume result
        tmqCom.initConsumerTable()
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        tdSql.execute(f"create view {view_name_list[0]} as {queryString}")
        sqlString = "create topic %s as %s" %(topic_name_list[0], "select * from %s"%view_name_list[0])
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())

        consumerId   = 1
        topicList    = topic_name_list[0]
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        keyList      = 'group.id:cgrp1, enable.auto.commit:false, auto.commit.interval.ms:6000, auto.offset.reset:earliest'
        ifcheckdata  = 1
        ifManualCommit = 1
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt, topicList, keyList, ifcheckdata, ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'], paraDict["dbName"], paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("wait the consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        if expectRowsList[0] != resultList[0]:
            tdLog.info("expect consume rows: %d, act consume rows: %d"%(expectRowsList[0], resultList[0]))
            tdLog.exit("1 tmq consume rows error!")

        tmqCom.checkFileContent(consumerId, queryString)

        time.sleep(10)
        for i in range(len(topic_name_list)):
            tdSql.query("drop topic %s"%topic_name_list[i])
        for i in range(len(view_name_list)):
            tdSql.query("drop view %s"%view_name_list[i])

        # drop database
        tdSql.execute(f"drop database {paraDict['dbName']}")
        tdSql.execute("drop database cdb;")
        tdLog.debug("Finish test case 'test_tmq_from_view'")

    def run(self):
        self.test_create_view_from_one_database()
        self.test_create_view_from_multi_database()
        self.test_create_view_name_params()
        self.test_create_view_query()
        self.test_show_view()
        self.test_drop_view()
        self.test_view_permission_db_all_view_all()
        self.test_view_permission_db_write_view_all()
        self.test_view_permission_db_write_view_read()
        self.test_view_permission_db_write_view_alter()
        self.test_view_permission_db_read_view_all()
        self.test_view_permission_db_read_view_alter()
        self.test_view_permission_db_read_view_read()
        self.test_query_from_view()
        self.test_tmq_from_view()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
