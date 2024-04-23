from itertools import product
import taos
import time
from taos.tmq import *
from util.cases import *
from util.common import *
from util.log import *
from util.sql import *
from util.sqlset import *


class TDTestCase:
    """This test case is used to veirfy the user privilege for insert and select operation on 
    stable、child table and table
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        # init the tdsql
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        # user info
        self.username = 'test'
        self.password = 'test'
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

    def prepare_data(self):
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

    def create_user(self):
        """Create the user for test
        """
        tdSql.execute(f'create user {self.username} pass "{self.password}"')
        tdLog.debug("sql:" + f'create user {self.username} pass "{self.password}" successfully')

    def grant_privilege(self, username, privilege, table, tag_condition=None):
        """Add the privilege for the user
        """
        try:
            if tag_condition:
                tdSql.execute(f'grant {privilege} on {self.dbname}.{table} with {tag_condition} to {username}')
            else:
                tdSql.execute(f'grant {privilege} on {self.dbname}.{table} to {username}')
            time.sleep(2)
            tdLog.debug("Grant {} privilege on {}.{} with condition {} to {} successfully".format(privilege, self.dbname, table, tag_condition, username))
        except Exception as ex:
            tdLog.exit(ex)

    def remove_privilege(self, username, privilege, table, tag_condition=None):
        """Remove the privilege for the user
        """
        try:
            if tag_condition:
                tdSql.execute(f'revoke {privilege} on {self.dbname}.{table} with {tag_condition} from {username}')
            else:
                tdSql.execute(f'revoke {privilege} on {self.dbname}.{table} from {username}')
            tdLog.debug("Revoke {} privilege on {}.{} with condition {} from {} successfully".format(privilege, self.dbname, table, tag_condition, username))
        except Exception as ex:
            tdLog.exit(ex)

    def run(self):
        self.create_user()
        # prepare the test data
        self.prepare_data()

        for case_name in self.cases.keys():
            tdLog.debug("Execute the case {} with params {}".format(case_name, str(self.cases[case_name])))
            # grant privilege for user test if case need
            if self.cases[case_name]["db_privilege"] != "none":
                self.grant_privilege(self.username, self.cases[case_name]["db_privilege"], "*")
            if self.cases[case_name]["stable_priviege"] != "none":
                self.grant_privilege(self.username, self.cases[case_name]["stable_priviege"], self.stbname)
            if self.cases[case_name]["child_table_ct1_privilege"] != "none" and self.cases[case_name]["child_table_ct2_privilege"] != "none":
                self.grant_privilege(self.username, self.cases[case_name]["child_table_ct1_privilege"], self.stbname, "ctbname='ct1' or ctbname='ct2'")
            elif self.cases[case_name]["child_table_ct1_privilege"] != "none":
                self.grant_privilege(self.username, self.cases[case_name]["child_table_ct1_privilege"], self.stbname, "ctbname='ct1'")
            elif self.cases[case_name]["child_table_ct2_privilege"] != "none":
                self.grant_privilege(self.username, self.cases[case_name]["child_table_ct2_privilege"], self.stbname, "ctbname='ct2'")
            if self.cases[case_name]["table_tb_privilege"] != "none":
                self.grant_privilege(self.username, self.cases[case_name]["table_tb_privilege"], self.common_tbname)
            # connect db with user test
            testconn = taos.connect(user=self.username, password=self.password)
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
                self.remove_privilege(self.username, self.cases[case_name]["db_privilege"], "*")
            if self.cases[case_name]["stable_priviege"] != "none":
                self.remove_privilege(self.username, self.cases[case_name]["stable_priviege"], self.stbname)
            if self.cases[case_name]["child_table_ct1_privilege"] != "none":
                self.remove_privilege(self.username, self.cases[case_name]["child_table_ct1_privilege"], self.stbname, "ctbname='ct1'")
            if self.cases[case_name]["child_table_ct2_privilege"] != "none":
                self.remove_privilege(self.username, self.cases[case_name]["child_table_ct2_privilege"], self.stbname, "ctbname='ct2'")
            if self.cases[case_name]["table_tb_privilege"] != "none":
                self.remove_privilege(self.username, self.cases[case_name]["table_tb_privilege"], self.common_tbname)
            # close the connection of user test
            testconn.close()

    def stop(self):
        # remove the user
        tdSql.execute(f'drop user {self.username}')
        # close the connection
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
