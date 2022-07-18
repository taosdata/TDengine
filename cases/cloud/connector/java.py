"""
到指定测试服务器目录，更新 Java 测试代码，执行 mvn test
启动命令： tt --case=cloud/connector/java.py --use=cloud_test.yaml
"""
import subprocess as sp

from taostest import TDCase, T


class Java(TDCase):
    def init(self):
        pass

    def run(self):
        cwd = self.env_setting["work_dir"] + "/docs-cloud/docs/examples/java"
        sp.run(["git", "pull"], cwd=cwd)
        sp.run(["mvn", "test"], cwd=cwd, check=True, env=self.env_setting["env"])

    def desc(self) -> str:
        return """Test Code Examples for Java Connector """

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        T.Cloud.Connector.Java
