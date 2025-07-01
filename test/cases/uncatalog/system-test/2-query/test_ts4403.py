import random
import string
from util.log import *
from util.cases import *
from util.sql import *
from util.sqlset import *
from util import constant
from util.common import *


class TDTestCase:
    """Verify the jira TS-4403
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db'
        self.stbname = 'st'

    def prepareData(self):
        # db
        tdSql.execute("create database {};".format(self.dbname))
        tdSql.execute("use {};".format(self.dbname))
        tdLog.debug("Create database %s" % self.dbname)

        # super table
        tdSql.execute("create table {} (ts timestamp, col1 int) tags (t1 int, t2 binary(16), t3 nchar(16));".format(self.stbname))
        tdLog.debug("Create super table %s" % self.stbname)

        # create index for all tags
        tdSql.execute("create index t2_st on {} (t2);".format(self.stbname))
        tdSql.execute("create index t3_st on {} (t3);".format(self.stbname))

    def run(self):
        self.prepareData()
        # check index number
        tdSql.query("show indexes from {};".format(self.stbname))
        assert(3 == len(tdSql.queryResult))
        tdLog.debug("All tags of super table have index successfully")

        # drop default first tag index
        tdSql.execute("drop index t1_st;")
        tdSql.query("show indexes from {};".format(self.stbname))
        assert(2 == len(tdSql.queryResult))
        tdLog.debug("Delete the default index of first tag successfully")

        # create index for first tag
        tdSql.execute("create index t1_st on {} (t1);".format(self.stbname))
        tdSql.query("show indexes from {};".format(self.stbname))
        assert(3 == len(tdSql.queryResult))
        tdLog.debug("Create index for first tag successfully")

        # null as index value to create child table
        tdSql.execute("create table ct1 using {} tags(null, null, null);".format(self.stbname))
        tdSql.query("show tables;")
        assert(1 == len(tdSql.queryResult))
        tdLog.debug("Create child table with tags value as 'null' successfully")

        # redundant index with different index name for some tag
        tdSql.error("create index t2_ct1 on st (t2);")
        tdLog.debug("Verify redundant index with different index name for some tag successfully")

        # same index name for different tag
        tdSql.error("create index t2_st on st (t3);")
        tdLog.debug("Verify same index name for some tag successfully")

        # add index for multiple tags(TD-28078)
        tdSql.error("create index tt on {} (t2, t3);".format(self.stbname))
        tdLog.debug("Verify add index for multiple tags successfully")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
