from itertools import product
import taos
import random
from taos.tmq import *
from util.cases import *
from util.common import *
from util.log import *
from util.sql import *
from util.sqlset import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        # init the tdsql
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        # user info
        self.userNum = 100
        self.basic_username = "user"
        self.password = "pwd"

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

    def prepare_data(self):
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

    def run(self):
        """
        Check the information from information_schema.ins_user_privileges
        """
        self.create_multiusers()
        self.prepare_data()
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

    def stop(self):
        # remove the privilege
        self.remove_privilege()
        # clear env
        tdSql.execute(f"drop database {self.dbname}")
        # remove the users
        for userIndex in range(self.userNum):
            username = f"{self.basic_username}{userIndex}"
            tdSql.execute(f'drop user {username}')  
        # close the connection
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
