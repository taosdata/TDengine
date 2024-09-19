from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
import json
import random


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)


    def checksql(self, sql):
        result = os.popen(f"taos -s \"{sql}\" ")
        res = result.read()
        print(res)
        if ("Query OK" in res):
            tdLog.info(f"checkEqual success")
        else :
            tdLog.exit(f"checkEqual error")
    def generate_random_str(self,randomlength=32):
        """
        生成一个指定长度的随机字符串
        """
        random_str = ''
        base_str = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz1234567890'
        #base_str = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz'
        length = len(base_str) - 1
        count = 0
        for i in range(randomlength):
            count = count + 1 
            random_str += base_str[random.randint(0, length)]
        return random_str
    def check(self):
        # tdSql.execute("create database db" )
        # tdSql.execute("create table db.jtable (ts timestamp, c1 VARCHAR(64000))",queryTimes=2)
        # with open('./1-insert/temp.json', 'r') as f:
        # data = json.load(f)
        # json_str=json.dumps(data)
        # print(data,type(data),type(json_str))
        # json_str=json_str.replace('"','\\"')
        # # sql = f"insert into db.jtable values(now,\"{json_str}\") "
        # # os.system(f"taos -s {sql} ")
        # rowNum = 100
        # step = 1000
        # self.ts = 1537146000000
        # for j in range(1000):
        # sql = "insert into db.jtable values"
        # for k in range(rowNum):
        # self.ts += step
        # sql += f"({self.ts},\"{json_str}\") "
        # tdSql.execute(sql,queryTimes=2)
        # tdSql.execute("flush database db",queryTimes=2)

        tdSql.execute("create database db1" )
        tdSql.execute("create table db1.jtable (ts timestamp, c1 VARCHAR(6400) compress 'zstd')",queryTimes=2)
        # with open('./1-insert/seedStr.json', 'r') as f:
        # data = f.read()
        # json_str=str(data)
        # print(data,type(data),type(json_str))
        # json_str=json_str.replace('"','\\"')


        rowNum = 100
        step = 1000
        self.ts = 1657146000000
        f=self.generate_random_str(5750)
        json_str=f.replace('"','\\"')
        for j in range(1000):
            sql = "insert into db1.jtable values"
        # f=self.generate_random_str(5750)
        # json_str=f.replace('"','\\"')
            for k in range(rowNum):
                self.ts += step
                f=self.generate_random_str(5750)
                json_str=f.replace('"','\\"')
                sql += f"({self.ts},\"{json_str}\") "
                #print(sql)
            tdSql.execute(sql,queryTimes=2)
        tdSql.execute("flush database db1",queryTimes=2)


    def run(self):
        self.check()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")



tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
