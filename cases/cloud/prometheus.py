"""
修改配置文件
测试 remote write 的方法：
启动 prometheus， 检查输出是否包括： Server is ready to receive web requests
测试 remote read 的方法：
启动 promql-cli 执行查询，不抛异常则成功
"""

from taostest import TDCase, T
from subprocess import Popen, PIPE, STDOUT
import os
import time


class Prometheus(TDCase):

    def config(self):
        pass

    def run(self):
        os.environ.update(self.env_setting["env"])
        cwd = self.env_setting["work_dir"] + "/prometheus/prometheus"
        self.config()
        prom_process = Popen('prometheus --config.file prometheus.yml', cwd=cwd, stdout=PIPE, stderr=STDOUT)
        remote_write_success = False
        for _ in range(50):
            ret_status = prom_process.poll()
            if ret_status is not None:
                self.set_error_msg(f"process exit unexpectedly with code {ret_status}")
                return False
            line = prom_process.stdout.readline()
            self.logger.info(line)
            if b'Server is ready to receive web requests' in line:
                remote_write_success = True
                break
        if not remote_write_success:
            self.set_error_msg("remote write failed")
            return False
        time.sleep(3)
        try:
            self.lcmd.run('promql-cli "rate(prometheus_tsdb_head_chunks_created_total[1m])"', cwd=cwd)
        except BaseException as e:
            self.set_error_msg("remote read failed:" + str(e))
            prom_process.kill()
            return False

    def desc(self) -> str:
        return "TDengine Cloud integrate with Prometheus"

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud.ThirdParty

    def init(self):
        pass

    def cleanup(self):
        pass
