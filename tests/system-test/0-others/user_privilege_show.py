from itertools import product
import taos
from taos.tmq import *
from util.cases import *
from util.common import *
from util.log import *
from util.sql import *
from util.sqlset import *


class TDTestCase:
    """This test case is used to veirfy the show create stable/table command for
        the different user privilege(TS-3469)
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
        self.dbname = "user_privilege_show"
        self.stbname = 'stb'
        self.common_tbname = "tb"
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

    def prepare_data(self, senario_type):
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

    def create_user(self):
        """Create the user for test
        """
        tdSql.execute(f'create user {self.username} pass "{self.password}"')
        tdLog.debug("sql:" + f'create user {self.username} pass "{self.password}"')

    def grant_privilege(self, username, privilege, privilege_obj, ctable_include=False, tag_condition=None):
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

    def run(self):
        """Currently, the test case can't be executed for all of the privilege combinations cause
        the table privilege isn't finished by dev team, only left one senario:
        db read privilege for user and show create table command; will udpate the test case once
        the table privilege function is finished
        """
        self.create_user()

        # temp solution only for the db read privilege verification
        self.prepare_data("table")
        # grant db read privilege
        self.grant_privilege(self.username, "read", "*")
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

        """
        for senario_type in self.privilege_check_dic.keys():
            tdLog.debug(f"---------check the {senario_type} privilege----------")
            self.prepare_data(senario_type)
            for senario in self.privilege_check_dic[senario_type]:
                # grant db privilege
                if senario[0] != "none":
                    self.grant_privilege(self.username, senario[0], "*")
                # grant stable privilege
                if senario[1] != "none":
                    self.grant_privilege(self.username, senario[1], self.stbname if senario_type == "stable" or senario_type == "ctable" else self.common_tbname)
                if senario_type == "stable" or senario_type == "table":
                    tdLog.debug(f"check the db privilege: {senario[0]}, (s)table privilege: {senario[1]}")
                else:
                    if senario[2] != "none":
                        # grant child table privilege
                        self.grant_privilege(self.username, senario[2], self.stbname, True, "ctbname='ct1'")
                    tdLog.debug(f"check the db privilege: {senario[0]}, (s)table privilege: {senario[1]}, ctable privilege: {senario[2]}")
                testconn = taos.connect(user=self.username, password=self.password)
                tdLog.debug("Create taos connection with user: {}, password: {}".format(self.username, self.password))
                try:
                    testconn.execute("use %s;" % self.dbname)
                except BaseException as ex:
                    if (senario_type in ["stable", "table"] and senario[0] == "none" and senario[1] == "none") or (senario_type == "ctable" and senario[0] == "none" and senario[1] == "none" and senario[2] == "none"):
                        continue
                    else:
                        tdLog.exit(ex)

                # query privileges for user
                res = testconn.query("select * from information_schema.ins_user_privileges;")
                tdLog.debug("Current information_schema.ins_user_privileges values: {}".format(res.fetch_all()))

                if senario_type == "stable" or senario_type == "table":
                    sql = "show create " + (("stable " + self.stbname) if senario_type == "stable" else (f"table {self.dbname}." + self.common_tbname + ";"))
                    if senario[2]:
                        tdLog.debug("sql: %s" % sql)
                        tdLog.debug(f"expected result: {senario[2]}")
                        res = testconn.query(sql)
                        tdLog.debug("sql res:" + res.fetch_all())
                    else:
                        exception_flag = False
                        try:
                            tdLog.debug("sql: %s" % sql)
                            tdLog.debug(f"expected result: {senario[2]}")
                            res = testconn.query(sql)
                            tdLog.debug("sql res:" + res.fetch_all())
                        except BaseException:
                            exception_flag = True
                            caller = inspect.getframeinfo(inspect.stack()[1][0])
                            tdLog.debug(f"{caller.filename}({caller.lineno}) failed to check the db privilege {senario[0]} and stable privilege {senario[1]} failed as expected")
                        if not exception_flag:
                            pass
                            # tdLog.exit("The expected exception isn't occurred")
                else:
                    sql = f"show create table {self.dbname}.{self.ctbname_list[0]};"
                    if senario[3]:
                        tdLog.debug("sql: %s" % sql)
                        tdLog.debug(f"expected result: {senario[3]}")
                        res = testconn.query(sql)
                        tdLog.debug(res.fetch_all())
                    else:
                        exception_flag = False
                        try:
                            tdLog.debug("sql: %s" % sql)
                            tdLog.debug(f"expected result: {senario[3]}")
                            res = testconn.query(sql)
                            tdLog.debug(res.fetch_all())
                        except BaseException:
                            exception_flag = True
                            caller = inspect.getframeinfo(inspect.stack()[1][0])
                            tdLog.debug(f"{caller.filename}({caller.lineno}) failed to check the db privilege {senario[0]}, stable privilege {senario[1]} and ctable privilege {senario[2]} failed as expected")
                        if not exception_flag:
                            pass
                            # tdLog.exit("The expected exception isn't occurred")

                # remove db privilege
                if senario[0] != "none":
                    self.remove_privilege(self.username, senario[0], "*")
                # remove stable privilege
                if senario[1] != "none":
                    self.remove_privilege(self.username, senario[1], self.stbname if senario_type == "stable" else self.common_tbname)
                # remove child table privilege
                if senario_type == "ctable":
                    if senario[2] != "none":
                        self.remove_privilege(self.username, senario[2], self.ctbname_list[0], True, "ctbname='ct1'")
                testconn.close()

            # remove the database
            tdSql.execute(f"drop database {self.dbname}")
            # reset the dbname
            self.dbname = "user_privilege_show"
        """

    def stop(self):
        # remove the user
        tdSql.execute(f'drop user {self.username}')
        # close the connection
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())  
