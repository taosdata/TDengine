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
        if platform.system().lower() == 'windows':
            self.buffer_boundary = [3, 4097]
        else:
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

    def alter_encrypt_alrogithm(self):
        tdSql.execute('create database db')
        tdSql.checkEqual("Encryption is not allowed to be changed after database is created", tdSql.error('alter database db encrypt_algorithm \'sM4\''))
        tdSql.checkEqual("Encryption is not allowed to be changed after database is created", tdSql.error('alter database db encrypt_algorithm \'noNe\''))
        tdSql.checkEqual("Encryption is not allowed to be changed after database is created", tdSql.error('alter database db encrypt_algorithm \'\''))
        tdSql.checkEqual("Invalid option encrypt_algorithm: none ", tdSql.error('alter database db encrypt_algorithm \'none \''))
        tdSql.execute('drop database db')

    def alter_compact(self):
        tdSql.execute('create database db')
        tdSql.execute('alter database db compact_time_offset 2')
        tdSql.execute('alter database db compact_time_offset 3h')
        tdSql.error('create database db1 compact_time_range 60,61', expectErrInfo="Invalid option compact_time_range: 86400m, start_time should be in range: [-5256000m, -14400m]", fullMatched=False)
        tdSql.error('create database db1 compact_time_offset -1', expectErrInfo="syntax error near", fullMatched=False)
        tdSql.error('create database d3 compact_interval 1m; ', expectErrInfo="Invalid option compact_interval: 1m, valid range: [10m, 5256000m]", fullMatched=False)
        tdSql.error('alter database db compact_time_offset -1', expectErrInfo="syntax error near", fullMatched=False)
        tdSql.error('alter database db compact_time_offset 24', expectErrInfo="Invalid option compact_time_offset: 24h, valid range: [0h, 23h]", fullMatched=False)
        tdSql.error('alter database db compact_time_offset 24h', expectErrInfo="Invalid option compact_time_offset: 24h, valid range: [0h, 23h]", fullMatched=False)
        tdSql.error('alter database db compact_time_offset 1d', expectErrInfo="Invalid option compact_time_offset unit: d, only h allowed", fullMatched=False)



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
        self.alter_encrypt_alrogithm()
        self.alter_same_options()
        self.alter_compact()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
