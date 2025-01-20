import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    """Verify the concat function
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor())
        self.dbname = 'db'
        self.tbname = f'{self.dbname}.tbconcat'
    def checkConcat(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table if not exists {self.tbname}(ts timestamp, name varchar(20), name2 nchar(20))')
        tdSql.execute(f'insert into {self.tbname} values(now(),"abcdefg","你好")')
        tdSql.execute(f'insert into {self.tbname} values(now(),"abcdefgh","我好")')
        tdSql.execute(f'insert into {self.tbname} values(now(),"abcdefg", "")')
        tdSql.execute(f'select concat("你好",name2) from {self.tbname}')
        tdSql.execute(f'select concat(name2,"你好") from {self.tbname}')
        tdSql.execute(f'select concat(name2,"") from {self.tbname}')
        tdSql.execute(f'select concat("", name2) from {self.tbname}')
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        # check concat function
        self.checkConcat()
        tdLog.success(f"{__file__} successfully executed")
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())