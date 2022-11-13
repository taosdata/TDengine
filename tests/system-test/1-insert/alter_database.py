import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.buffer_boundary = [3, 4097, 8193, 12289, 16384]
        self.buffer_error = [self.buffer_boundary[0] -
                             1, self.buffer_boundary[-1]+1, 12289, 256]
        # pages_boundary >= 64
        self.pages_boundary = [64, 128, 512]
        self.pages_error = [self.pages_boundary[0]-1]

    def alter_buffer(self):
        tdSql.execute('create database db')
        for buffer in self.buffer_boundary:
            tdSql.execute(f'alter database db buffer {buffer}')
            tdSql.query(
                'select * from information_schema.ins_databases where name = "db"')
            tdSql.checkEqual(tdSql.queryResult[0][8], buffer)
        tdSql.execute('drop database db')
        tdSql.execute('create database db vgroups 10')
        for buffer in self.buffer_error:
            tdSql.error(f'alter database db buffer {buffer}')
        tdSql.execute('drop database db')

    def alter_pages(self):
        tdSql.execute('create database db')
        for pages in self.pages_boundary:
            tdSql.execute(f'alter database db pages {pages}')
            tdSql.query(
                'select * from information_schema.ins_databases where name = "db"')
            tdSql.checkEqual(tdSql.queryResult[0][10], pages)
        tdSql.execute('drop database db')
        tdSql.execute('create database db')
        tdSql.query(
            'select * from information_schema.ins_databases where name = "db"')
        self.pages_error.append(tdSql.queryResult[0][10])
        for pages in self.pages_error:
            tdSql.error(f'alter database db pages {pages}')
        tdSql.execute('drop database db')

    def run(self):
        tdSql.error('create database db1 vgroups 10 buffer 12289')
        self.alter_buffer()
        self.alter_pages()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
