# TS-5184 [威士顿卷烟厂] insert写入异常

## 1. 测试目标

测试的行为为在复合`insert`语句中，反复切换绑定列和不绑定列的形式，查看是否能够正常进行插入。在旧版本中，不不绑定列的情况下会自动加载前一个绑定列的结果导致插入错误。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024/07/22 | 0.1 | @张志鹏 |  |

## 3. 测试结论

在修复前对该问题进行了复现。`insert`语句会报错误，这是由于错误的将值绑定到了不同的列上。
修复后该问题被解决。`insert`语句现在可以同时处理未绑定列以及绑定列的结果。
修复前使用`taosBenchmark`测试插入性能，用时：27s
修复后使用`taosBenchmark`测试插入性能，用时：25.733s
插入性能没有下降

## 4. 测试环境

Ubuntu 22.04 版本， gcc版本13.2
3.0
测试的旧版本为:0a16d1bc56535ecfb4dff59a80dae20896569eeb (存在问题）
新版本为：651077866e7ce7f08a8ced5613ee2c0ab35b7415 (问题修复）
3.1
测试的旧版本为：f65945dcd3a3a642faac6a7a6230d8676e8cfe76 （存在问题）
测试的新版本为: 17e0035beea714f27b53c2aa6b6057b4c17f820f （问题修复）
Main
测试的旧版本为：dbc84b5b400538dc6980d48809833b00e87aea5b（存在问题)
测试的新版本为: 139aab7f3bc9552ce1585e6970bb64fc1c31eae5(问题修复)

## 5. 测试范围

测试了在单条`insert`语句中反复使用绑定列和未绑定列两种形式插入，查看插入是否异常。

## 6. 测试程序：

```bash
from util.sql import *
from util.common import *
import taos
taos.taos_connect
class TDTestCase:
    def init(self, conn, logSql, replicaVar = 1):
        self.replicaVar = replicaVar
        tdLog.debug(f"start to excute {__file__}")
        self.conn = conn
        tdSql.init(conn.cursor(), logSql)
    def initdb(self):
        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database d0")
        tdSql.execute("use d0")
        tdSql.execute("create stable stb0 (ts timestamp, w_ts timestamp, opc nchar(100), quality int) tags(t0 int)")
        tdSql.execute("create table t0 using stb0 tags(1)")
        tdSql.execute("create table t1 using stb0 tags(2)")
    def multi_insert(self):
        for i in range(10):
            tdSql.execute(f"insert into t1 values(1721265436000, now() + {i + 1}s, '0', 12) t1(opc, quality, ts) values ('opc2', 192, now()+ {i + 2}s) t1(ts, opc, quality) values(now() + {i + 3}s, 'opc4', 10) t1 values(1721265436000, now() + {i + 4}s, '1', 191) t1(opc, quality, ts) values('opc5', 192, now() + {i + 5}s) t1 values(1721265486000, now() + {i + 6}s, '2', 192)")
            tdSql.execute("insert into t0 values(1721265436000,now(),'0',192) t0(quality,w_ts,ts) values(192,now(),1721265326000) t0(quality,w_t\
s,ts) values(190,now()+1s,1721265326000) t0 values(1721265436000,now()+2s,'1',191) t0(quality,w_ts,ts) values(192,now()+3s,\
1721265326002) t0(ts,w_ts,opc,quality) values(1721265436003,now()+4s,'3',193) t0 values(172126543700, now() + 4s , '2', 192)")
    def run(self):
        self.initdb()
        self.multi_insert()
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
```
