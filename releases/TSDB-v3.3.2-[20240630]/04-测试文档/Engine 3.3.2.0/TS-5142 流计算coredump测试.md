# TS-5142 流计算coredump测试

## 1. 测试目标

测试在`STATE_WINDOW`语句中使用`case when`语句时taosd能否正常运行。
现场情况： 使用下列语句创造流之后出现`coredump`:

```sql
REATE STREAM stream_device_alarm TRIGGER AT_ONCE DELETE_MARK 30d
 INTO tt.st_device_alarm tags(factory_id varchar(20), device_code varchar(80), var_name varchar(200))
 as select _wstart start_time, last(load_time) end_time, first(var_value) var_value, (case when lower(var_value)=lower(trigger_value) then '1' else '0' end) state_flag
 from st_variable_data WHERE var_attribute = 'deviceAlarm' PARTITION BY tbname tname, factory_id, device_code, var_name
 STATE_WINDOW(case when lower(var_value)=lower(trigger_value) then '1' else '0' end);
```

测试过程中发现，当想要得到`state_flag`这种通过`case when`生成的列的时候，必须再`STATE_WINDOW`中包含着相同的表达式才能符合语法。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024/07/15 | 0.1 | @张志鹏 |  |

## 3. 测试结论

在旧版本下，在创造流的过程中使用`STATE_WINDOW`以及`case when`语句会出现coredump。报错信息为:
```sql
2024-07-15 15:54:53.124635 insert data failed ProgrammingError('some vnode/qnode/mnode(s) out of service', -2147483616)
```

在新版本下，运行正常。

`STATE_WINDOW`以及`case when`的结合在单纯的`SELECT`语句可以正常运行。

## 4. 已知问题和限制

无

## 5. 测试环境

Ubuntu 22.04 版本， gcc版本13.2
测试的旧版本为: 3e320ebdf11639c9e184e885dbd88c8637df9e26
新版本为：9f4f4f7f9fc19865bc5cfab307ebd5dd01eac58b

## 6. 测试范围及方法

测试的内容与出现bug的语句类似，主要考虑了`STATE_WINDOW`与`case when`语句同时使用的情况。
查看是否能正常插入数据。并检测流的结果是否正确。
在测试中我们考虑了两种情况：
1. `state_flag`为固定值1
2. `state_flag`从`case when`语句中得到
两种情况下的测试结论一致。

## 7. 测试数据

无

## 8. 测试步骤

```sql
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
        time.sleep(2)
    
    def insert_data(self):
        try:
            tdSql.execute("insert into aaa values('2024-07-15 14:00:00', '2024-07-15 14:00:00', 'a8')", queryTimes=5, show=True)
            time.sleep(0.01)
            tdSql.execute("insert into aaa values('2024-07-15 14:10:00', '2024-07-15 14:10:00', 'a9')", queryTimes=5, show=True)
            time.sleep(1)
        except Exception as error:
            tdLog.error(f"insert data failed {error}")
    
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
```

### 8.1 旧版本复现

```bash
$ python3 test.py -f 8-stream/state_window_case.py 
2024-07-15 15:53:48.838421 Procedures for tdengine deployed in ubuntu

2024-07-15 15:53:48.849569 stop all dnodes, asan:0

2024-07-15 15:53:48.989136 psim is deployed and configured by /home/zhipz/projects/TDEnterprise/TDinternal/sim/psim/cfg/taos.cfg
2024-07-15 15:53:49.004287 dnode:1 is deployed and configured by /home/zhipz/projects/TDEnterprise/TDinternal/sim/dnode1/cfg/taos.cfg
2024-07-15 15:53:49.014531 taosd found: /home/zhipz/projects/TDEnterprise/TDinternal/debug/build/bin/taosd

2024-07-15 15:53:49.015942 dnode:1 is running with nohup /home/zhipz/projects/TDEnterprise/TDinternal/debug/build/bin/taosd -c /home/zhipz/projects/TDEnterprise/TDinternal/sim/dnode1/cfg > /dev/null 2>&1 &  
2024-07-15 15:53:50.063054 the dnode:1 has been started.
2024-07-15 15:53:50.063092 Procedures for testing self-deployment

2024-07-15 15:53:50.067550 start to execute /home/zhipz/projects/TDEnterprise/TDinternal/community/tests/system-test/8-stream/state_window_case.py
sqllog is :True
2024-07-15 15:53:50.086587 ==========init case==========
2024-07-15 15:53:52.370686 ==========create stream==========
2024-07-15 15:53:56.406877 insert into aaa values('2024-07-15 14:00:00', '2024-07-15 14:00:00', 'a8')

2024-07-15 15:53:56.418185 insert into aaa values('2024-07-15 14:10:00', '2024-07-15 14:10:00', 'a9')

2024-07-15 15:54:07.992128 Try to execute sql again, execute times: 1 
2024-07-15 15:54:19.276277 Try to execute sql again, execute times: 2 
2024-07-15 15:54:30.559737 Try to execute sql again, execute times: 3 
2024-07-15 15:54:41.842657 Try to execute sql again, execute times: 4 
2024-07-15 15:54:53.124000 Try to execute sql again, execute times: 5 
2024-07-15 15:54:53.124621 /home/zhipz/projects/TDEnterprise/TDinternal/community/tests/system-test/8-stream/state_window_case.py(39) failed: sql:insert into aaa values('2024-07-15 14:10:00', '2024-07-15 14:10:00', 'a9'), ProgrammingError('some vnode/qnode/mnode(s) out of service', -2147483616)
2024-07-15 15:54:53.124635 insert data failed ProgrammingError('some vnode/qnode/mnode(s) out of service', -2147483616)

```

### 8.2 新版本下结果

```bash
$ python3 test.py -f 8-stream/state_window_case.py
2024-07-15 16:02:44.846820 Procedures for tdengine deployed in ubuntu

2024-07-15 16:02:44.856611 stop all dnodes, asan:0

2024-07-15 16:02:44.990421 psim is deployed and configured by /home/zhipz/projects/TDEnterprise/TDinternal/sim/psim/cfg/taos.cfg
2024-07-15 16:02:45.004331 dnode:1 is deployed and configured by /home/zhipz/projects/TDEnterprise/TDinternal/sim/dnode1/cfg/taos.cfg
2024-07-15 16:02:45.013762 taosd found: /home/zhipz/projects/TDEnterprise/TDinternal/debug/build/bin/taosd

2024-07-15 16:02:45.015238 dnode:1 is running with nohup /home/zhipz/projects/TDEnterprise/TDinternal/debug/build/bin/taosd -c /home/zhipz/projects/TDEnterprise/TDinternal/sim/dnode1/cfg > /dev/null 2>&1 &  
2024-07-15 16:02:46.088563 the dnode:1 has been started.
2024-07-15 16:02:46.088604 Procedures for testing self-deployment

2024-07-15 16:02:46.093071 start to execute /home/zhipz/projects/TDEnterprise/TDinternal/community/tests/system-test/8-stream/state_window_case.py
sqllog is :True
2024-07-15 16:02:46.111334 ==========init case==========
2024-07-15 16:02:48.400587 ==========create stream==========
2024-07-15 16:02:52.435846 insert into aaa values('2024-07-15 14:00:00', '2024-07-15 14:00:00', 'a8')

2024-07-15 16:02:52.447103 insert into aaa values('2024-07-15 14:10:00', '2024-07-15 14:10:00', 'a9')

2024-07-15 16:02:53.452344 check successfully

2024-07-15 16:02:53.452908 check successfully

2024-07-15 16:02:53.455824 check successfully

2024-07-15 16:02:53.456378 check successfully

2024-07-15 16:02:53.456613 /home/zhipz/projects/TDEnterprise/TDinternal/community/tests/system-test/8-stream/state_window_case.py successfully executed
```
