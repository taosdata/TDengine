"""
taosBenchMark 功能性测试。 因为是测试 taosBenchMark 工具本身，所以尽量不给云服务实例性能压力
1. 测试与云服务的连通性
2. 建表和插入数据
启动命令： tt --case=cloud/benchmark.py --use=cloud_test.yaml
"""

from subprocess import run

from taostest import TDCase, T
from taostest.util.cloud_sql import CloudSql

from .cloud_case import CloudCase


class TaosBenchMark(TDCase, CloudCase):
    def init(self):
        self.set_env()
        self.cql = CloudSql()

    def run(self):
        # 创建 1 张表，写 100 条数据
        run(['taosBenchmark', '--database', 'benchmark', '-T', '1', '-t', '1', '--records', '10', '-y'], check=True, timeout=20)
        count = self.cql.count("benchmark", "meters")
        assert count == 100

    def cleanup(self):
        pass

    def desc(self) -> str:
        return "Test basic function of taosBenchmark"

    def author(self) -> str:
        return "DingBo"

    def tags(self):
        return T.Cloud.Toos.TaosBenchmark
