# 环境依赖
# 1. node.js
# 2. 按照官网 emqx 文档的步骤，配置好的 emqx 容器镜像，配置会持久化到 /opt/emqx 目录
# 3. taosAdapter
# 本用例假设以上环境均已具备， 只执行以下操作：
# 1. 创建容器
# 2. 写入模拟数据
# 3. 检查数据库中的模拟数据
# 4. 销毁容器
# 目前测试只能在虚拟机： 192.168.0.41 上执行。这个环境中已经有 TDengine 和 taosAdapter 每天都会更新。
# 如果迁移测试机器，需要重新从 WEB 界面配置，不能将原来的配置直接复制过去使用，因为配置和主机名及IP是相关的。

from taostest import TDCase
from taostest.util import Remote
import time
import os


class EMQXTest(TDCase):
    def init(self):
        self.remote = Remote(self.logger)
        self.target_host = self.env_setting["target_host"]
        self.tdSql.execute("create database IF NOT EXISTS test")
        self.tdSql.execute("DROP TABLE IF EXISTS test.sensor_data")
        self.tdSql.execute("CREATE TABLE test.sensor_data (ts timestamp, temperature float, humidity float, volume float, PM10 float, pm25 float, SO2 float, NO2 float, CO float, sensor_id NCHAR(255), area TINYINT, coll_time timestamp)")

    def cleanup(self):
        self.remote.cmd(self.target_host, ["docker stop emqxtest", "docker rm emqxtest"])

    def copy_files(self, file_names):
        for file_name in file_names:
            file_path = os.path.join(self.get_case_folder(), file_name)
            self.remote.put(self.target_host, file_path, "/tmp/emqxtest")

    def run(self):
        self.remote.cmd(self.target_host, "docker run -d --name emqxtest --network host  -v /opt/emqx/data:/opt/emqx/data -v /opt/emqx/log:/opt/emqx/log emqx/emqx:latest")
        time.sleep(15)
        self.copy_files(["mock.js", "package.json"])
        self.remote.cmd(self.target_host, ["cd /tmp/emqxtest", "npm install", "node --unhandled-rejections=strict  mock.js"])
        time.sleep(1)
        self.tdSql.query("select count(*) from test.sensor_data")
        assert self.tdSql.query_data[0][0] >= 10

    def desc(self) -> str:
        return "Test EMQX"

    def author(self) -> str:
        return "BoDing"

    def tags(self):
        return "Third-Party, EMQX"
