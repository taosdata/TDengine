"""
测试到云服务的连通性，假设目标机器上已经安装好 taos 命令行
如果进程异常退出，或超过 10 秒没退出，都认为失败
启动命令： tt --case=cloud/cli.py --use=cloud_test.yaml
"""

import os

from taostest import TDCase, T
from subprocess import run


class TDengineCLI(TDCase):
    def init(self):
        os.environ.update(self.env_setting["env"])

    def cleanup(self):
        pass

    def run(self):
        run(["taos", "-s", "show databases"], check=True, timeout=10)

    def desc(self) -> str:
        return "Test connect to cloud using CLI"

    def author(self) -> str:
        return "DingBo"

    def tags(self):
        return T.Cloud.Toos.TDengineCLI
