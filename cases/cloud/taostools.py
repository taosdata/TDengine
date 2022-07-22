"""
taosBenchmark 和 taosdump 功能性测试。 因为是测试工具本身，所以尽量不给云服务实例性能压力
1. 测试与云服务的连通性
2. 建表和插入数据
3. 导出数据测试
启动命令： tt --case=cloud/taostools.py --use=cloud_test.yaml
"""

from subprocess import run

from taostest import TDCase, T
from taostest.util.cloud_sql import CloudSql

from .cloud_case import CloudCase
import os
import shutil


class TaosTools(TDCase, CloudCase):
    def init(self):
        self.cwd = self.env_setting["work_dir"] + '/tools'
        self.dump_dir = os.path.join(self.cwd, "dumped_data")
        self.set_env()
        self.cql = CloudSql()

    def run(self):
        self.test_taos_benchmark()
        self.test_taos_dump()

    def test_taos_benchmark(self):
        self.logger.info("-------------------Test taosBenchmark---------------------------")
        # 创建 1 张表，写 records 条数据
        records = 10
        run(['taosBenchmark', '--database', 'benchmark', '-T', '1', '-t', '1', '--records', str(records), '-y'],
            cwd=self.cwd, check=True, timeout=20)
        count = self.cql.count("benchmark", "meters")
        assert count == records

    def test_taos_dump(self):
        self.logger.info("-------------------Test taosdump---------------------------")
        try:
            os.mkdir(self.dump_dir)
        except:
            self.cleanup()
            os.mkdir(self.dump_dir)

        run(['taosdump', '-D', 'benchmark', '-o', 'dumped_data'], check=True, timeout=20, cwd=self.cwd)

    def cleanup(self):
        shutil.rmtree(self.dump_dir, ignore_errors=True)

    def desc(self) -> str:
        return "Test basic function of taosBenchmark"

    def author(self) -> str:
        return "DingBo"

    def tags(self):
        return T.Cloud.Toos.TaosBenchmark, T.Cloud.Toos.TaosDump
