"""
修改配置文件
测试 remote write 的方法：
启动 prometheus， 检查输出是否包括： Server is ready to receive web requests
测试 remote read 的方法：
启动 promql-cli 执行查询，不抛异常则成功
启动命令： tt --case=cloud/prometheus.py --use=cloud_test.yaml
"""

from taostest import TDCase, T
from subprocess import Popen, PIPE, STDOUT
from .cloud_case import CloudCase
import os
import time


class Prometheus(TDCase, CloudCase):

    def init(self):
        self.set_env()
        self.cwd = self.env_setting["work_dir"] + "/prometheus/prometheus"
        self.init_config()

    def init_config(self):
        cloud_url = os.environ["TDENGINE_CLOUD_URL"]
        cloud_token = os.environ["TDENGINE_CLOUD_TOKEN"]
        in_path = os.path.join(self.cwd, "config_template.yml")
        out_path = os.path.join(self.cwd, "prometheus.yml")
        with open(in_path, "rt") as f1:
            with open(out_path, "wt") as f2:
                for line in f1:
                    if '<cloud_url>' in line:
                        line = line.replace('<cloud_url>', cloud_url)
                    if '<cloud_token>' in line:
                        line = line.replace('<cloud_token>', cloud_token)
                    f2.write(line)

    def run(self):
        prom_process = Popen(['./prometheus', '--config.file', 'prometheus.yml'], cwd=self.cwd, stdout=PIPE, stderr=STDOUT)
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
        else:
            self.logger.info("remote write success")
        time.sleep(15)
        try:
            self.lcmd.run('promql-cli count(prometheus_target_interval_length_seconds)', cwd=self.cwd)
        finally:
            prom_process.kill()

    def desc(self) -> str:
        return "TDengine Cloud integrate with Prometheus"

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud.ThirdParty

    def cleanup(self):
        pass
