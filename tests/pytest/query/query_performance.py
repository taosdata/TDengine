import time
import taos
import csv
import numpy as np
import random
import os
import requests
import json
import sys

"""
需要第三方库： taos,requests,numpy
当前机器已经启动taosd服务
使用方法见底部示例
"""



class Ding:
    """
    发送消息到钉钉,
    urls: 钉钉群的token组成的list，可以发多个钉钉群，需要提前加白名单或其他放行策略
    at_mobiles： 需要@的人的手机号组成的list
    msg： 要发送的str
    """
    def __init__(self, url_list, at_mobiles):
        self.urls = url_list
        self.at_mobiles = at_mobiles

    def send_message(self, msg):
        data1 = {
             "msgtype": "text",
             "text": {
                 "content": msg
             },
             "at": {
                 "atMobiles": self.at_mobiles,
                 "isAtAll": False
             }
         }

        header = {'Content-Type': 'application/json; charset=utf-8'}

        for url in self.urls:
            requests.post(url=url, data=json.dumps(data1), headers=header)




class TDConn:
    def __init__(self, config:dict):
        self.host = config['host']
        self.user = config['user']
        self.password = config['password']
        self.config = config['config']
        self.conn = None
        self.cursor = None

    def connect(self):
        conn = taos.connect(host=self.host, user=self.user, password=self.password, config=self.config)
        cursor = conn.cursor()
        self.conn = conn
        self.cursor = cursor
        print('connect ...')
        return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()
        print('close ... ')


class Tool:
    """
    可能有用
    """
    @staticmethod
    def str_gen(num):
        return ''.join(random.sample('abcdefghijklmnopqrstuvwxyz', num))

    @staticmethod
    def float_gen(n, m):
        return random.uniform(n, m)

    @staticmethod
    def int_gen(n, m):
        return random.randint(n, m)

class Demo:
    def __init__(self, engine):
        self.engine =  engine['engine'](engine['config'])
        self.cursor = self.engine.connect()


    def date_gen(self, db, number_per_table, type_of_cols, num_of_cols_per_record, num_of_tables):
        """
        :目前都是 taosdemo 的参数
        :return:
        """
        sql = 'yes | sudo taosdemo  -d {db} -n {number_per_table} -b {type_of_cols} -l {num_of_cols_per_record} ' \
              '-t {num_of_tables}'.format(db=db, number_per_table=number_per_table, type_of_cols=type_of_cols,
                                          num_of_cols_per_record=num_of_cols_per_record, num_of_tables=num_of_tables)
        os.system(sql)
        print('insert data completed')


    # def main(self, db, circle, csv_name, case_func, result_csv, nums, ding_flag):
    def main(self, every_num_per_table, result_csv, all_result_csv, values):
        db = values['db_name']
        number_per_table = every_num_per_table
        type_of_cols = values['col_type']
        num_of_cols_per_record = values['col_num']
        num_of_tables = values['table_num']
        self.date_gen(db=db, number_per_table=number_per_table, type_of_cols=type_of_cols,
                      num_of_cols_per_record=num_of_cols_per_record, num_of_tables=num_of_tables)

        circle = values['circle']
        # print(every_num_per_table, result_csv, values)
        csv_name = result_csv
        case_func = values['sql_func']
        nums = num_of_tables * number_per_table
        ding_flag = values['ding_flag']

        _data = []
        f = open(csv_name,'w',encoding='utf-8')
        f1 = open(all_result_csv,'a',encoding='utf-8')
        csv_writer = csv.writer(f)
        csv_writer1 = csv.writer(f1)
        csv_writer.writerow(["number", "elapse", 'sql'])
        self.cursor.execute('use {db};'.format(db=db))


        for i in range(circle):
            self.cursor.execute('reset query cache;')
            sql = case_func()
            start = time.time()
            self.cursor.execute(sql)
            self.cursor.fetchall()
            end = time.time()
            _data.append(end-start)
            elapse = '%.4f' %(end -start)
            print(sql, i, elapse, '\n')
            csv_writer.writerow([i+1, elapse, sql])

            # time.sleep(1)
        _list = [nums, np.mean(_data)]
        _str = '总数据: %s 条 , table数: %s , 每个table数据数: %s , 数据类型: %s \n' % \
               (nums, num_of_tables, number_per_table, type_of_cols)
        # print('avg : ', np.mean(_data), '\n')
        _str += '平均值 : %.4f 秒\n' % np.mean(_data)
        for each in (50, 80, 90, 95):
            _list.append(np.percentile(_data,each))
            _str += ' %d 分位数 : %.4f 秒\n' % (each , np.percentile(_data,each))
           
        print(_str)   
        if ding_flag:
            ding = Ding(values['ding_config']['urls'], values['ding_config']['at_mobiles'])
            ding.send_message(_str)
        csv_writer1.writerow(_list)
        f.close()
        f1.close()
        self.engine.close()


def run(engine, test_cases: dict, result_dir):
    for each_case, values in test_cases.items():
        for every_num_per_table in values['number_per_table']:
            result_csv = result_dir + '{case}_table{table_num}_{number_per_table}.csv'.\
                format(case=each_case, table_num=values['table_num'], number_per_table=every_num_per_table)
            all_result_csv = result_dir + '{case_all}_result.csv'.format(case_all=each_case)
            d = Demo(engine)
            # print(each_case, result_csv)
            d.main(every_num_per_table, result_csv, all_result_csv, values)



if __name__ == '__main__':
    """
    测试用例在test_cases中添加。
    result_dir： 报告生成目录，会生成每次测试结果，和具体某一用例的统计结果.需注意目录权限需要执行用户可写。
    case1、case2 : 具体用例名称
    engine: 数据库引擎，目前只有taosd。使用时需开启taosd服务。
    table_num: 造数据时的table数目
    circle: 循环测试次数，求平均值
    number_per_table：需要传list，多个数值代表会按照list内的数值逐个测试
    col_num：table col的数目
    col_type： 表中数据类型
    db_name： 造数据的db名，默认用test
    sql_func： 当前测试的sql方法，需要自己定义
    ding_flag： 如果需要钉钉发送数据，flag设置真值，
    ding_config： 如ding_flag 设置为真值，此项才有意义。ding_flag为假时此项可以为空。urls传入一list，内容为要发送的群的token，
    需提前设置白名单，at_mobiles传入一list，内容为在群内需要@的人的手机号
    """
    engine_dict = {
        'taosd': {'engine': TDConn, 'config':
            {'host': '127.0.0.1', 'user': 'root', 'password': 'taosdata', 'config':'/etc/taos'}}
    }

    def case1():
        return 'select * from meters where f1 = {n};'.format(n=random.randint(1,30))

    def case2():
        return 'select * from meters where f1 = %.4f;' %random.uniform(1,30)


    result_dir = '/usr/local/demo/benchmarktestdata/'
    test_cases = {
        'case1': {'engine':'taosd', 'table_num': 10, 'circle': 100, 'number_per_table':[10, 100], 'col_num': 5,
                  'col_type': 'INT', 'db_name': 'test', 'sql_func':  case1, 'ding_flag': True,
                  'ding_config':
                      {'urls': [r'https://oapi.dingtalk.com/robot/send?access_token=58fe61c28ee7142a90d2b1e7b9a8b9d99f502f248739c12a7fb62579edc0cd93'],
                       'at_mobiles':[17080138990,],}},
        'case2': {'engine':'taosd', 'table_num': 10, 'circle': 50, 'number_per_table':[10, 100], 'col_num': 5,
                  'col_type': 'FLOAT', 'db_name': 'test', 'sql_func':  case2, 'ding_flag': False,
                  'ding_config': None
                  }
    }

    run(engine_dict['taosd'], test_cases, result_dir)
