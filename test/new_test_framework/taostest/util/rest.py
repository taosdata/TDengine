import time
import requests
from ..logger import get_basic_logger
from ..errors import TestException
import threading
import random


class TDRest:
    def __init__(self, logger=None,
                 default_server="localhost:6041",
                 default_path="/rest/sql",
                 default_auth_header={'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='},
                 env_setting=None):
        if logger:
            self._logger = logger
        else:
            self._logger = get_basic_logger()
        if env_setting is None:
            self._default_server = default_server
        else:
            for settings in env_setting["settings"]:
                if settings["name"].lower() == "taosadapter":
                    taosadapter_fqdn = random.choice(settings["fqdn"])
                    taosadapter_port = settings["spec"]["adapter_config"]["port"]
                    self._default_server = f'{taosadapter_fqdn}:{taosadapter_port}'
        self._default_path = default_path
        self._default_url = "http://" + self._default_server + self._default_path
        self._default_header = default_auth_header
        self.resp = None
        self.url = None
        self.white_list = ["statsd", "node_exporter", "collectd", "icinga2", "tcollector", "information_schema", "performance_schema"]

    def request(self, data, flag = None,url=None, header=None,requestTimes=10):
        """
        返回格式：
        {
            "status": "succ",
            "head": ["ts","current", …],
            "column_meta": [["ts",9,8],["current",6,4], …],
            "data": [
                ["2018-10-03 14:38:05.000", 10.3, …],
                ["2018-10-03 14:38:15.000", 12.6, …]
            ],
            "rows": 2
        }
        如果是更新操作，则head字段的值为["affected_rows"].
        参考：https://www.taosdata.com/docs/cn/v2.0/connector#restful
        """
        if not header:
            header = self._default_header
        if not url:
            url = self._default_url
        if flag == None:
            i=1
            resp = requests.post(url, data.encode("utf-8"), headers=header)
            resp.encoding = "utf-8"
            self.resp = resp.json()
            resp_code = self.resp["code"]
            if resp_code != 0:
                self._logger.error("error request url=%s, resp=%s", url, resp.text)
                while i <= requestTimes:
                    self._logger.error("Try to execute sql again, execute times: %d "%i)
                    resp = requests.post(url, data.encode("utf-8"), headers=header)
                    resp.encoding = "utf-8"
                    self.resp = resp.json()
                    resp_code = self.resp["code"]
                    time.sleep(0.5)
                    i+=1
                    if resp_code == 0:
                        return
                     
                else:
                    self._logger.debug("test case failed: request:%s, " % resp.text)

            else:
                return 
        elif flag == 'error':
            resp = requests.post(url, data.encode("utf-8"), headers=header)
            resp.encoding = "utf-8"
            self.resp = resp.json()
            resp_code = self.resp["code"]
            if resp_code != "0":
                self._logger.error("error request url=%s, resp=%s", url, resp.text)
                return 
            else:
                return 

    def query(self, sql):
        self.request(sql)

    def error(self, sql) -> bool:
        """
        @param throw: 失败是否抛异常
        @param sql: 期望执行错误的sql
        @return: 如果报错返回True， 如果没报错返回False
        """
        try:
            self.request(sql,'error')
        except BaseException as e:
            self._logger.info(f"error occur as expected: {e}")
            return True

    def drop_all_db(self, url=None, header=None):
        self.request("show databases", url, header)
        if len(self.resp["data"]) > 0:
            for db_info_list in self.resp["data"]:
                if db_info_list[0] not in self.white_list or "telegraf" not in db_info_list[0]:
                    self.request(f"drop database if exists {db_info_list[0]}", url, header)

    def getOneRow(self, location, containElm) -> list:
        """
        用途：用于query并筛选出指定location含有containElm的tuple/list并将结果加入新列表
        输入：期望指定location包含的元素
        返回：正常，失败
        """
        res_list = list()
        if 0 <= location <= len(self.resp["data"])and len(self.resp["data"]) >=1:
            for row in self.resp["data"]:
                if row[location] == containElm:
                    res_list.append(row)
            return res_list
        else:
            raise TestException(f'getOneRow out of range: row_index={location} row_count={len(self.resp["data"])}')

    def getColNameList(self, col_tag=None):
        '''
            get col name/type list
        '''
        try:
            col_name_list = list()
            col_type_list = list()
            length_list = list()
            note_list = list()
            for query_col in self.resp["data"]:
                col_name_list.append(query_col[0])
                col_type_list.append(query_col[1])
                length_list.append(query_col[2])
                note_list.append(query_col[3])
            if col_tag:
                return col_name_list, col_type_list, length_list, note_list
            return col_name_list
        except Exception as e:
            raise Exception(repr(e))

    def genMultiThreadSeq(self, sql_list):
        tlist = list()
        for insert_sql in sql_list:
            t = threading.Thread(target=self.request, args=(insert_sql,))
            tlist.append(t)
        return tlist

    def multiThreadRun(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    def pre_define(self):
        header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        sql_url = f"http://{self._default_server}/rest/sql"
        sqlt_url = f"http://{self._default_server}/rest/sqlt"
        sqlutc_url = f"http://{self._default_server}/rest/sqlutc"
        influx_url = f"http://{self._default_server}/influxdb/v1/write"
        telnet_url = f"http://{self._default_server}/opentsdb/v1/put/telnet"
        json_url = f"http://{self._default_server}/opentsdb/v1/put/json"
        return header, sql_url, sqlt_url, sqlutc_url, influx_url, telnet_url, json_url

    def restApiPost(self, sql):
        requests.post(self.pre_define()[1], sql.encode("utf-8"), headers = self.pre_define()[0])

    def gen_url(self, url_type, dbname, precision):
        if url_type == "influxdb":
            if precision is None:
                self.url = self.pre_define()[4] + "?" + "db=" + dbname
            else:
                self.url = self.pre_define()[4] + "?" + "db=" + dbname + "&precision=" + precision
        elif url_type == "telnet":
            self.url = self.pre_define()[5] + "/" + dbname
        elif url_type == "json":
            self.url = self.pre_define()[6] + "/" + dbname
        else:
            self.url = self.pre_define()[1]

    def schemalessApiPost(self, sql, url_type="influxdb", dbname="test", precision=None):
        if url_type == "influxdb":
            self.gen_url(url_type, dbname, precision)
            res = requests.post(self.url, sql.encode("utf-8"), headers = self.pre_define()[0])
        elif url_type == "telnet":
            self.gen_url(url_type, dbname, precision)
            res = requests.post(self.url, sql.encode("utf-8"), headers = self.pre_define()[0])
        elif url_type == "json":
            self.gen_url(url_type, dbname, precision)
            res = requests.post(self.url, sql, headers = self.pre_define()[0])
        return res

    def get_rest_db_field(self,resp,test_param,dbname):
        for i in range(len(resp['column_meta'])):
            if resp['column_meta'][i][0] == f'{test_param}':
                for j in range(len(resp['data'])):
                    if resp['data'][j][0] == f'{dbname}':
                        return resp['data'][j][i]