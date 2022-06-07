import os,sys
from datetime import datetime
from taostest import TDCase
from taostest.util.common import TDCom

class QueryTest(TDCase):

    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def desc(self):
        pass

    def author(self):
        pass

    def cleanup(self):
        pass

    def tags(self):
        pass

    def run(self):
        print(" start run case for Version 3.0 ")
        # prepare data for basic query data of large amount of records 
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')

        case_path = os.path.realpath(__file__)
        len_case = len(case_path.split("/")[-1])
        case_dir = case_path[:len(case_path)-len_case]
        print(case_dir)
        ret = os.system(f"taosBenchmark -f  {case_dir}pre_datas_insert.json")

        if ret !=0:
            print("prepare data done ! ")
            sys.exit(ret)

        for _ in range(10):
            # basic aggregate query for 3.0 branch 
            ret = os.system(f"taosBenchmark -f  {case_dir}basic_agg_query.json")

            if ret !=0:
                print("basic aggregate done ! ")
                sys.exit(ret)

            # basic long query for 3.0 branch 

            ret = os.system(f"taosBenchmark -f  {case_dir}basic_long_query.json")

            if ret !=0:
                print("basic long done ! ")
                sys.exit(ret)
        
        
