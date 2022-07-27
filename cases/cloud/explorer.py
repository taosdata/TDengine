"""
压力测试：模拟 Explorer执行 SQL 的高并发压力测试
"""
import random
import threading

from taostest import TDCase, T
from taostest.util import caseutil
from cloud_api import CloudApi
from threading import Thread


class Explorer(TDCase):
    def init(self):
        params = caseutil.parse_param(self.case_param)
        self.cloud_api = CloudApi()
        self.thread_count = int(params["thread"])
        self.limit = int(params["limit"])
        self.threads = []

    def cleanup(self):
        pass

    def query(self, sql, app_id):
        resp = self.cloud_api.sql(sql, app_id)
        status = resp.status_code
        thread_name = threading.current_thread().name
        if status != 200:
            self.logger.info(f"{thread_name} status: {resp.status_code}")
        else:
            data = resp.json()
            code = data["code"]
            if code != 21200:
                self.logger.info(f"{thread_name} {data}")
            else:
                self.logger.info(f"{thread_name} {code}")

    def run(self):
        self.cloud_api.get_auth_token()
        self.logger.info("get token %s", self.cloud_api.token)
        sql = "select * from test.meters limit " + str(self.limit)
        app_ids = ["1552169702935015424", "1549749987899842560", "1552171949022556160", "1552173418803146752"]
        for i in range(self.thread_count):
            app_id = random.choice(app_ids)
            t = Thread(target=self.query, args=(sql, app_id), name="thread-" + str(i))
            t.start()
            self.logger.info("start thread-" + str(i) + " app_id " + app_id)
            self.threads.append(t)
        for t in self.threads:
            t.join()

    def desc(self) -> str:
        return "模拟 Explorer 执行 SQL 的高并发压力测试"

    def author(self) -> str:
        return "DingBo"

    def tags(self):
        return T.Cloud
