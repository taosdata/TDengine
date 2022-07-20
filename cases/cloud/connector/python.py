"""
测试云服务 Python 连接器示例代码
启动命令：tt --case=cloud/connector/python.py --use=cloud_test.yaml
"""
import os

from taostest import TDCase, T


class Python(TDCase):
    def init(self):
        os.system("pip3 uninstall -y taospy")
        os.system('pip3 install "taospy<=3.0.0"')

    def cleanup(self):
        pass

    def run(self):
        cwd = self.env_setting["work_dir"] + "/docs-cloud/docs/examples/python"
        self.lcmd.run("python3 develop_tutorial.py", cwd=cwd, env=self.env_setting["env"])
        # self.lcmd.run("python3  reference_connection.py", cmw=cwd, env=self.env_setting["env"])
        # self.lcmd.run("python3  reference_cursor.py", cmw=cwd, env=self.env_setting["env"])
        # self.lcmd.run("python3  reference_rest_client.py", cmw=cwd, env=self.env_setting["env"])

    def desc(self) -> str:
        return "Python connector for cloud"

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud.Connector.Python
