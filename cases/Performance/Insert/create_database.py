# utf-8
import os
import time
from taostest.util.file import read_yaml
from datetime import datetime
from typing import List
from taostest import TDCase
from taostest.util.common import TDCom
from taostest.performance.result_reduction import Perf_Base_func
from prettytable import PrettyTable
class CreateDatabase(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.vgroups_list = [2,10,20,40]
        self.resultfile = Perf_Base_func(self.logger, self.run_log_dir)
        
        self.create_result = []
        self.test_num = 10
        self.avg_time = []
    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass
    def create_database_time(self):
        for vgroups in self.vgroups_list:
            test_result = []
            for i in range(self.test_num):
                dbname = self.tdCom.get_long_name(5)
                start_time = time.time()
                self.tdSql.execute(f'create database {dbname} replica 3 vgroups {vgroups}')
                end_time = time.time()
                test_result.append(end_time-start_time)
                self.tdSql.execute(f'drop database {dbname}')
            sum = 0
            for i in test_result:
                sum += i
            self.avg_time.append(round(sum / len(test_result),3))
        data_list = []
        for i in range(len(self.vgroups_list)):
            data_list.append([self.vgroups_list[i],self.avg_time[i]])
        file_name = self.run_log_dir + f'/create_database_perfreport_{self.mnode_num}mnode.txt'
        tb = PrettyTable()
        tb.field_names = ['vgroups','create_time(s)']
        for i in data_list:
            tb.add_row(i)
        print(tb)
        file = open(file_name, 'a')
        file.write(f'"\n**********************Create Database Performance Test Result({self.mnode_num} mnode)*********************\n"')
        file.write(str(tb) + '\n')
        file.close()
        
    def run(self):
        self.tdSql.query('show mnodes')
        self.mnode_num = self.tdSql.query_row
        self.create_database_time()

