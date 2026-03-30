# TS-5150 STDDEV 函数计算结果随机出现结果为0

## 1. 测试目标

在一个超级表中创建多个子表，并且保证存在空的子表，这个时候使用聚合计算得到`stddev`的值时会随机出现结果为0的情况。测试该bug是否被修复。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024/07/20 | 0.1 | @张志鹏 |  |

## 3. 测试结论

在修复前确实复现了相应的问题，在`stddev`函数与`where between`函数一起使用时，会随机出现结果为0的情况。测试下来10次约有5次出现相应的情况。经过测试使用`interval`函数并不会出现该问题，仅在`where ts between`中出现相应的问题。
经过修复后问题被解决。

## 4. 测试环境

Ubuntu 22.04 版本， gcc版本13.2
3.0
测试的旧版本为: dccbd4be99f2373cc657f4c90836a67c3c7b1eaf (存在问题）
新版本为：c10106a4da4f8833784554a6b57f8f6d11082ba7 (问题修复）
3.1
测试的旧版本为：f65945dcd3a3a642faac6a7a6230d8676e8cfe76 （存在问题）
测试的新版本为: 17e0035beea714f27b53c2aa6b6057b4c17f820f （问题修复）
Main
测试的旧版本为：98674d80c23d09a0767d2e2ad5051418377d5e69 （存在问题)
测试的新版本为: e50a504178f522078becc1c9b0928e4c4b6b3084 (问题修复)

## 5. 测试范围

测试了包含空子表情况下使用`stddev`函数以及`where ts between`进行聚合计算的结果。

## 6. 测试方法以及用例

整个测试的内容包括：
1. 首先创建超级表并创建了六个子表
2. 向其中三个子表中插入数据
3. 使用`select`语句调用`stddev`函数以及`where ts between`函数
4. 验证其计算的结果是否正确
```bash
import numpy as np
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *

'''
Test case for TS-5150
'''
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.ts = 1537146000000
    def initdabase(self):
        tdSql.execute('create database if not exists db_test vgroups 2  buffer 10')
        tdSql.execute('use db_test')
        tdSql.execute('create stable stb(ts timestamp, delay int) tags(groupid int)')
        tdSql.execute('create table t1 using stb tags(1)')
        tdSql.execute('create table t2 using stb tags(2)')
        tdSql.execute('create table t3 using stb tags(3)')
        tdSql.execute('create table t4 using stb tags(4)')
        tdSql.execute('create table t5 using stb tags(5)')
        tdSql.execute('create table t6 using stb tags(6)')
    def insert_data(self):
        for i in range(5000):
            tdSql.execute(f"insert into t1 values({self.ts + i * 1000}, {i%5})")
            tdSql.execute(f"insert into t2 values({self.ts + i * 1000}, {i%5})")
            tdSql.execute(f"insert into t3 values({self.ts + i * 1000}, {i%5})")
    
    def verify_stddev(self):
        for i in range(20):
            tdSql.query(f'SELECT MAX(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS maxDelay,\
                        MIN(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS minDelay,\
                        AVG(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS avgDelay,\
                        STDDEV(CASE WHEN delay != 0 THEN delay ELSE NULL END) AS jitter,\
                        COUNT(CASE WHEN delay = 0 THEN 1 ELSE NULL END) AS timeoutCount,\
                        COUNT(*) AS totalCount from stb where ts between {1537146000000 + i * 1000} and {1537146000000 + (i+10) * 1000}')
            res = tdSql.queryResult[0][3]
            assert res > 0.8
    def run(self):
        self.initdabase()
        self.insert_data()
        self.verify_stddev()
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())

        
```
