"""
测试订阅和流计算
1. 页面上创建库和表
2. 页面上创建 topic
3. 页面上创建 stream
4. 启动写数据进程
5. 启动订阅进程
"""

from taostest import TDCase, T
from subprocess import Popen, PIPE, STDOUT
from .cloud_case import CloudCase
import os
import time


class StreamTest(CloudCase):

    def run(self):
        pass

    def desc(self) -> str:
        pass

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud

    def init(self):
        pass

    def cleanup(self):
        pass
