import taos
import sys
import time
import socket
import os
import threading
import psutil
import platform
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
        # remove the value > free_memory, 70% is the weight to calculate the max value
        # if platform.system() == "Linux" and platform.machine() == "aarch64":
            # mem = psutil.virtual_memory()
            # free_memory = mem.free * 0.7 / 1024 / 1024
            # for item in self.buffer_boundary:
            #     if item > free_memory:
            #         self.buffer_boundary.remove(item)

        self.buffer_error = [self.buffer_boundary[0] -
                             1, self.buffer_boundary[-1]+1]
        # pages_boundary >= 64
        self.pages_boundary = [64, 128, 512]
        self.pages_error = [self.pages_boundary[0]-1]

    def alter_buffer(self):
        tdSql.execute('create database db')
        if platform.system() == "Linux" and platform.machine() == "aarch64":
            tdLog.debug("Skip check points for Linux aarch64 due to environment settings")
        else:
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
        # self.pages_error.append(tdSql.queryResult[0][10])
        for pages in self.pages_error:
            tdSql.error(f'alter database db pages {pages}')
        tdSql.execute('drop database db')

    def alter_same_options(self):
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db')
        tdSql.query('select * from information_schema.ins_databases where name = "db"')
        
        db_options_items = ["replica","keep","buffer","pages","minrows","cachemodel","cachesize","wal_level","wal_fsync_period",
                      "wal_retention_period","wal_retention_size","stt_trigger"]
        db_options_result_idx = [4,7,8,10,11,18,19,20,21,22,23,24]
        
        self.option_result = []
        for idx in db_options_result_idx:
            self.option_result.append(tdSql.queryResult[0][idx])
        
        index = 0
        for option in db_options_items:
            if option == "cachemodel":
                option_sql = "alter database db %s '%s'" % (option, self.option_result[index] )
            else:
                option_sql = "alter database db %s %s" % (option, self.option_result[index] )
            tdLog.debug(option_sql)
            tdSql.query(option_sql)
            index += 1
        tdSql.execute('drop database db')
        
    def run(self):
        
        self.alter_buffer()
        self.alter_pages()
        self.alter_same_options()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
