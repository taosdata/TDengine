"""
到指定测试服务器目录，执行 mvn test
启动命令： tt --case=cloud/connector/java.py --use=cloud_test.yaml
"""

from taostest import TDCase, T


class Java(TDCase):
    def init(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        cwd = self.env_setting["work_dir"] + "/docs-cloud/docs/examples/java"
        self.lcmd.run("mvn test", cwd=cwd, env=self.env_setting["env"])

    def desc(self) -> str:
        return """Java connector for cloud """

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud.Connector.Java
