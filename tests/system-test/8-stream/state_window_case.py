import sys
from util.log import *
from util.sql import *

from util.cases import *
from util.common import *


class TDTestCase:
    updatecfgDict = {'debugFlag':135,}
    def init(self, conn, logSql, replicaVar = 1):
        self.replicaVar = replicaVar
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tdCom = tdCom
    def init_case(self):
        tdLog.debug("==========init case==========")
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("CREATE STABLE `st_variable_data` (`load_time` TIMESTAMP, `collect_time` TIMESTAMP, `var_value` NCHAR(300)) TAGS (`factory_id` NCHAR(30), `device_code` NCHAR(80), `var_name` NCHAR(120), `var_type` NCHAR(30), `var_address` NCHAR(100), `var_attribute` NCHAR(30), `device_name` NCHAR(150), `var_desc` NCHAR(200), `trigger_value` NCHAR(50), `var_category` NCHAR(50), `var_category_desc` NCHAR(200));")
        tdSql.execute('CREATE TABLE aaa using `st_variable_data` tags("a1","a2", "a3","a4","a5","a6","a7","a8","a9","a10","a11")')
        time.sleep(2)

    def create_stream(self):
        tdLog.debug("==========create stream==========")
        tdSql.execute("CREATE STREAM stream_device_alarm TRIGGER AT_ONCE DELETE_MARK 30d INTO st_device_alarm tags(factory_id varchar(20), device_code varchar(80), var_name varchar(200))\
                    as select _wstart start_time, last(load_time) end_time, first(var_value) var_value, (case when lower(var_value)=lower(trigger_value) then '1' else '0' end) state_flag from st_variable_data\
                    PARTITION BY tbname tname, factory_id, device_code, var_name STATE_WINDOW(case when lower(var_value)=lower(trigger_value) then '1' else '0' end)")
        time.sleep(2)
        tdSql.execute("CREATE STREAM stream_device_alarm2 TRIGGER AT_ONCE DELETE_MARK 30d INTO st_device_alarm2 tags(factory_id varchar(20), device_code varchar(80), var_name varchar(200))\
                    as select _wstart start_time, last(load_time) end_time, first(var_value) var_value, 1 state_flag from st_variable_data\
                    PARTITION BY tbname tname, factory_id, device_code, var_name STATE_WINDOW(case when lower(var_value)=lower(trigger_value) then '1' else '0' end)")
        time.sleep(5)
    
    def insert_data(self):
        try:
            tdSql.execute("insert into aaa values('2024-07-15 14:00:00', '2024-07-15 14:00:00', 'a8')", queryTimes=5, show=True)
            time.sleep(0.01)
            tdSql.execute("insert into aaa values('2024-07-15 14:10:00', '2024-07-15 14:10:00', 'a9')", queryTimes=5, show=True)
            time.sleep(5)
        except Exception as error:
            tdLog.exit(f"insert data failed {error}")
    
    def run(self):
        self.init_case()
        self.create_stream()
        self.insert_data()
        tdSql.query("select state_flag from st_device_alarm")
        tdSql.checkData(0, 0, 0, show=True)
        tdSql.checkData(1, 0, 1, show=True)
        tdSql.query("select state_flag from st_device_alarm2")
        tdSql.checkData(0, 0, 1, show=True)
        tdSql.checkData(1, 0, 1, show=True)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())