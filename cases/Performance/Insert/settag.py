import os
import time
from taostest.util.file import read_yaml
from datetime import datetime
from typing import List
from taostest import TDCase
from taostest.performance.perfor_basic import InsertFile
from taostest.performance.result_reduction import Perf_Base_func
from taostest.util.remote import Remote
from taostest.util.common import TDCom

class InsertTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.firstEP = []
        self.taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        self.test_root = os.environ['TEST_ROOT']
        self.dbname = ""
        pass
    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def alter_set_tag(self):
        avg_set_tag = []
        self.remote.cmd(self.taosBenchmark_fqdn[0],f"taosBenchmark -f {self.test_root}/cases/Performance/Insert/basic.json")
        self.tdSql.query('show databases')
        print(self.tdSql.query_data)
        for dbname in self.tdSql.query_data:
            print(dbname[0])
            if dbname[0].lower() != "information_schema" and dbname[0].lower() != "performance_schema":
                self.dbname = dbname[0]
        self.tdSql.execute(f'use {self.dbname}')
        self.tdSql.query('show tables')
        print(f"tbnum = {len(self.tdSql.query_data)}")
        for tbname in self.tdSql.query_data:
            tag_value = self.tdCom.get_long_name(10)
            start_time = time.time()
            self.tdSql.execute(f'alter table {tbname[0]} set tag location = "{tag_value}"')
            end_time = time.time()
            avg_set_tag.append(end_time-start_time)
        
        sum = 0
        for i in avg_set_tag:
            sum += i
        
        avg_time = sum / len(avg_set_tag)
        print(f"=========================================\n")
        print(f"avg_delay = {int(avg_time*1000)} ms")
        print(f'max_delay = {int(max(avg_set_tag)*1000)} ms')
        print(f'min_delay = {int(min(avg_set_tag)*1000)} ms')

        
    def run(self):
        self.alter_set_tag()
        