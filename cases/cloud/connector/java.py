import subprocess as sp

from taostest import TDCase, T


class Java(TDCase):
    def init(self):
        pass

    def run(self):
        cwd = self.env_setting["work_dir"] + "/docs-cloud/docs/examples/java"
        sp.run("git pull", cwd=cwd)
        cp = sp.run("mvn test", cwd=cwd, check=True, env=self.env_setting["env"], capture_output=True, text=True)
        print(cp.stdout)

    def desc(self) -> str:
        return """Test Code Examples for Java Connector """

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        T.Cloud.Connector.Java
