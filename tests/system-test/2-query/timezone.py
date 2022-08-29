
from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
import platform
import os
if platform.system().lower() == 'windows':
    import tzlocal


class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.arithmetic_operators = ['+','-','*','/']
        self.arithmetic_values = [0,1,100,15.5]
        self.dbname = 'db'
        # name of normal table
        self.ntbname = f'{self.dbname}.ntb'
        # name of stable
        self.stbname = f'{self.dbname}.stb'
        # structure of column
        self.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'double'
        }
        # structure of tag
        self.tag_dict = {
            't0':'int'
        }
        # number of child tables
        self.tbnum = 2
        # values of tag,the number of values should equal to tbnum
        self.tag_values = [
            f'10',
            f'100'
        ]
        # values of rows, structure should be same as column
        self.values_list = [
            f'now,10,99.99,11.111111',
            f'today(),100,11.111,22.222222'

        ]
        self.error_param = [1,'now()']
    def get_system_timezone(self):
        if platform.system().lower() == 'windows':
            time_zone_1 = tzlocal.get_localzone_name()
            time_zone_2 = time.strftime('(UTC, %z)')
            time_zone = time_zone_1 + " " + time_zone_2
        else:
            time_zone_arr = os.popen('timedatectl | grep zone').read().strip().split(':')
            if len(time_zone_arr) > 1:
                time_zone = time_zone_arr[1].lstrip()
            else:
                # possibly in a docker container
                time_zone_1 = os.popen('ls -l /etc/localtime|awk -F/ \'{print $(NF-1) "/" $NF}\'').read().strip()
                time_zone_2 = os.popen('date "+(%Z, %z)"').read().strip()
                time_zone = time_zone_1 + " " + time_zone_2
        return time_zone

    def tb_type_check(self,tb_type):
        if tb_type in ['normal_table','child_table']:
            tdSql.checkRows(len(self.values_list))
        elif tb_type == 'stable':
            tdSql.checkRows(len(self.values_list*self.tbnum))
    def data_check(self,timezone,tbname,tb_type):
        tdSql.query(f"select timezone() from {tbname}")
        self.tb_type_check(tb_type)
        tdSql.checkData(0,0,timezone)
        for symbol in self.arithmetic_operators:
            tdSql.query(f"select timezone(){symbol}null from {tbname}")
            self.tb_type_check(tb_type)
            tdSql.checkData(0,0,None)
        for i in self.arithmetic_values:
            for symbol in self.arithmetic_operators:
                tdSql.query(f"select timezone(){symbol}{i} from {tbname}")
                self.tb_type_check(tb_type)
                if symbol == '+':
                    tdSql.checkData(0,0,i)
                elif symbol == '-':
                    tdSql.checkData(0,0,-i)
                elif symbol in ['*','/','%']:
                    if i == 0 and symbol == '/':
                        tdSql.checkData(0,0,None)
                    else:
                        tdSql.checkData(0,0,0)
        for param in self.error_param:
            tdSql.error(f'select timezone({param}) from {tbname}')
        tdSql.query(f"select * from {tbname} where timezone()='{timezone}'")
        self.tb_type_check(tb_type)
    def timezone_check_ntb(self,timezone):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        for value in self.values_list:
            tdSql.execute(
                f'insert into {self.ntbname} values({value})')
        self.data_check(timezone,self.ntbname,'normal_table')
        tdSql.execute('drop database db')
    def timezone_check_stb(self,timezone):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table if not exists {self.stbname}_{i} using {self.stbname} tags({self.tag_values[i]})')
            for j in self.values_list:
                tdSql.execute(f'insert into {self.stbname}_{i} values({j})')
        self.data_check(timezone,self.stbname,'stable')
        for i in range(self.tbnum):
            self.data_check(timezone,f'{self.stbname}_{i}','child_table')
    def run(self):  # sourcery skip: extract-duplicate-method
        timezone = self.get_system_timezone()
        self.timezone_check_ntb(timezone)
        self.timezone_check_stb(timezone)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
