# tests/test_new/xxx/xxx/test_xxx.py
# import ...
# ./pytest.sh python3 ./test.py -f storage/compact/test_compact_meta.py

import random
import taos
import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *


class TestCompactMeta:
    def init(self):
        tdLog.debug("start to execute %s" % __file__)

    def test_template(self):
        """用例目标，必填，用一行简短总结
        <空行>
        用例详细描述，必填，允许多行
        <空行>
        Since: 用例开始支持的TDengine版本，新增用例必填
        <空行>
        Labels: 筛选标签，选填，多个标签用英文逗号分隔
        <空行>
        Jira: 相关jira任务id，选填
        <空行>
        History: 用例变更历史，选填，每行一次变更信息
            - 日期1 变更人1 变更原因1
            - 日期2 变更人2 变更原因2
        """

    def test_demo(self):
        """测试超级表插入各种数据类型

        使用多种数据类型创建超级表，向超级表插入数据，
        包括：常规数据，空数据，边界值等，插入均执行成功

        Since: v3.3.0.0

        Labels: stable, data_type

        Jira: TD-12345, TS-1234

        History:
            - 2024-2-6 Feng Chao Created
            - 2024-2-7 Huo Hong updated for feature TD-23456
        """

    def test_case1(self):
        dbname = 'db'
        super_table = "stb"
        child_table_prefix = "ctb"
        num_of_child_tables = 10000
        max_alter_times = 100

        # Drop database
        sql = f'drop database if exists {dbname}'
        print(sql)
        tdSql.execute(sql)

        # Create database
        sql = f'create database {dbname}'
        print(sql)
        tdSql.query(sql)

        # # Create super table
        # sql = f'create {dbname}.{super_table} (ts timestamp, c1 int) tags (tag1 int)'
        # tdSql.query(sql)

        # # Create child tables
        # for i in range(num_of_child_tables):
        #     sql = f'create {dbname}.{child_table_prefix}{i} using {dbname}.{super_table} tags ({i})'
        #     tdSql.query(sql)

        # # Alter child tables
        # for i in range(num_of_child_tables):
        #     for j in range(random.randint(1, max_alter_times)):
        #         sql = f'alter table {dbname}.{child_table_prefix}{i} set tag1 = {i + j}'
        #         tdSql.query(sql)

        # # Alter super tables
        # for i in range(random.randint(1, max_alter_times)):
        #     sql = f'alter table {dbname}.{super_table} add column c{i+1} int'
        #     tdSql.query(sql)

        # # Compact meta
        # sql = f'compact database {dbname}'
        # tdSql.query(sql)

    def run(self):
        self.test_template()
        self.test_demo()
        self.test_case1()

    def stop(self):
        tdLog.success("%s successfully executed" % __file__)


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
