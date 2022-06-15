import random
import string
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'

    def get_long_name(self, length, mode="mixed"):
        """
        generate long name
        mode could be numbers/letters/letters_mixed/mixed
        """
        if mode == "numbers":
            population = string.digits
        elif mode == "letters":
            population = string.ascii_letters.lower()
        elif mode == "letters_mixed":
            population = string.ascii_letters.upper() + string.ascii_letters.lower()
        else:
            population = string.ascii_letters.lower() + string.digits
        return "".join(random.choices(population, k=length))
    def last_check_stb_tb_base(self):
        tdSql.prepare()
        tdSql.execute('''create table stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned, 
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(loc nchar(20))''')
        tdSql.execute("create table stb_1 using stb tags('beijing')")
        tdSql.execute("insert into stb_1(ts) values(%d)" % (self.ts - 1))
        
        for i in ['stb_1','db.stb_1','stb_1','db.stb_1']:
            tdSql.query(f"select last(*) from {i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)
        #!bug TD-16561
        # for i in ['stb','db.stb','stb','db.stb']:
        #     tdSql.query(f"select last(*) from {i}")
        #     tdSql.checkRows(1)
        #     tdSql.checkData(0, 1, None)
        for i in range(1, 14):
            for j in ['stb_1','db.stb_1','stb_1','db.stb_1']:
                tdSql.query(f"select last(col{i}) from {j}")
                tdSql.checkRows(0)
        tdSql.query("select last(col1) from stb_1 group by col7")
        tdSql.checkRows(1)
        for i in range(self.rowNum):
            tdSql.execute(f"insert into stb_1 values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
        for i in ['stb_1', 'db.stb_1', 'stb', 'db.stb']:
            tdSql.query(f"select last(*) from {i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 10)
        for i in range(1, 14):
            for j in ['stb_1', 'db.stb_1', 'stb', 'db.stb']:
                tdSql.query(f"select last(col{i}) from {j}")
                tdSql.checkRows(1)
                # tinyint,smallint,int,bigint,tinyint unsigned,smallint unsigned,int unsigned,bigint unsigned
                if i >=1 and i<9:
                    tdSql.checkData(0, 0, 10)
                # float,double
                elif i>=9 and i<11:
                    tdSql.checkData(0, 0, 9.1)
                # bool
                elif i == 11:
                    tdSql.checkData(0, 0, True)
                # binary
                elif i == 12:
                    tdSql.checkData(0, 0, f'{self.binary_str}{self.rowNum}')
                # nchar
                elif i == 13:
                    tdSql.checkData(0, 0, f'{self.nchar_str}{self.rowNum}')
        tdSql.query("select last(col1,col2,col3) from stb_1")
        tdSql.checkData(0, 2, 10)

        tdSql.error("select col1 from stb where last(col13)='涛思数据10'")
        tdSql.error("select col1 from stb_1 where last(col13)='涛思数据10'")
        tdSql.execute('drop database db')
    
    def last_check_ntb_base(self):
        tdSql.prepare()
        tdSql.execute('''create table ntb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned, 
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20))''')
        tdSql.execute("insert into ntb(ts) values(%d)" % (self.ts - 1))  
        tdSql.query("select last(*) from ntb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.query("select last(*) from db.ntb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        for i in range(1,14):
            for j in['ntb','db.ntb']:
                tdSql.query(f"select last(col{i}) from {j}")
                tdSql.checkRows(0)
        for i in range(self.rowNum):
            tdSql.execute(f"insert into ntb values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
        tdSql.query("select last(*) from ntb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.query("select last(*) from db.ntb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        for i in range(1, 9):
            for j in ['ntb', 'db.ntb']:
                tdSql.query(f"select last(col{i}) from {j}")
                tdSql.checkRows(1)
                # tinyint,smallint,int,bigint,tinyint unsigned,smallint unsigned,int unsigned,bigint unsigned
                if i >=1 and i<9:
                    tdSql.checkData(0, 0, 10)
                # float,double
                elif i>=9 and i<11:
                    tdSql.checkData(0, 0, 9.1)
                # bool
                elif i == 11:
                    tdSql.checkData(0, 0, True)
                # binary
                elif i == 12:
                    tdSql.checkData(0, 0, f'{self.binary_str}{self.rowNum}')
                # nchar
                elif i == 13:
                    tdSql.checkData(0, 0, f'{self.nchar_str}{self.rowNum}')

        tdSql.error("select col1 from ntb where last(col9)='涛思数据10'")

    def last_check_stb_distribute(self):
        # prepare data for vgroup 5
        dbname = self.get_long_name(length=10, mode="letters")
        stbname = self.get_long_name(length=5, mode="letters")
        tdSql.execute(f"create database if not exists {dbname} vgroups 4")
        tdSql.execute(f'use {dbname}')
        # build 20 child tables,every table insert 10 rows
        tdSql.execute(f'''create table {stbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned, 
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(loc nchar(20))''')
        for i in range(1,21):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags('beijing')")
            tdSql.execute(f"insert into {stbname}_{i}(ts) values(%d)" % (self.ts - 1-i))
        # for i in [f'{stbname}', f'{dbname}.{stbname}']:
        #     tdSql.query(f"select last(*) from {i}")
        #     tdSql.checkRows(1)
        #     tdSql.checkData(0, 1, None)
        tdSql.query('show tables')
        vgroup_list = []
        for i in range(len(tdSql.queryResult)):
            vgroup_list.append(tdSql.queryResult[i][6])
        vgroup_list_set = set(vgroup_list)
        print(vgroup_list_set)
        print(vgroup_list)
        for i in vgroup_list_set:
            vgroups_num = vgroup_list.count(i)
            if vgroups_num >=2:
                tdLog.info(f'This scene with {vgroups_num} vgroups is ok!')
                continue
            else:
                tdLog.info('This scene does not meet the requirements!\n')
                tdLog.exit(1)
        
        for i in range(1,21):
            for j in range(self.rowNum):
                tdSql.execute(f"insert into {stbname}_{i} values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + j + i, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 0.1, j + 0.1, j % 2, j + 1, j + 1))
        for i in [f'{stbname}', f'{dbname}.{stbname}']:
            tdSql.query(f"select last(*) from {i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 10)
        for i in range(1, 14):
            for j in [f'{stbname}', f'{dbname}.{stbname}']:
                tdSql.query(f"select last(col{i}) from {j}")
                tdSql.checkRows(1)
                # tinyint,smallint,int,bigint,tinyint unsigned,smallint unsigned,int unsigned,bigint unsigned
                if i >=1 and i<9:
                    tdSql.checkData(0, 0, 10)
                # float,double
                elif i>=9 and i<11:
                    tdSql.checkData(0, 0, 9.1)
                # bool
                elif i == 11:
                    tdSql.checkData(0, 0, True)
                # binary
                elif i == 12:
                    tdSql.checkData(0, 0, f'{self.binary_str}{self.rowNum}')
                # nchar
                elif i == 13:
                    tdSql.checkData(0, 0, f'{self.nchar_str}{self.rowNum}')
        tdSql.execute(f'drop database {dbname}')
    def run(self):
        self.last_check_stb_tb_base()
        self.last_check_ntb_base()
        self.last_check_stb_distribute()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
