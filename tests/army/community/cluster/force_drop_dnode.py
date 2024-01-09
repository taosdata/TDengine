import taos
import sys

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *

class TDTestCase(TBase):

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):
        id = 0
        tdSql.execute('create dnode \'u1_176:6239\';')
        sql = 'show dnodes'
        param_list = self.query(sql, row_tag=True)
        for param in param_list:
            if param[2] == 0:
                id = param[0]
                tdLog.debug("drop dnode id %d"%(id))

        tdSql.execute(f''' drop dnode {id} force; ''')

        sql = 'show dnodes'
        param_list = self.query(sql, row_tag=True)
        for param in param_list:
            if param[0] == id:
                tdLog.("drop dnode id %d"%(id))
                tdLog.exit("drop failed: id:%d" % id)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
