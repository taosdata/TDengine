import os.path

from taostest import TDCase
from taostest.util import Remote


class HiveMQTest(TDCase):
    def install_mosquitto(self):
        if not self.remote.command_exists(self.target_host, "mosquitto"):
            self.remote.cmd(self.target_host, "apt-get install -y mosquitto")

    def install_maven(self):
        if not self.remote.command_exists(self.target_host, "mvn"):
            self.remote.cmd(self.target_host, "apt-get install -y maven")

    def install_openjdk_11(self):
        self.remote.cmd(self.target_host, "apt-get install openjdk-11-jdk -y")

    def compile_plugin(self):
        compile_script = os.path.join(self.get_case_folder(), "compile_plugin.sh")
        suc = self.remote.put(self.target_host, compile_script, "/tmp")
        if not suc:
            raise Exception("failed to put compile_script")
        result = self.remote.cmd(self.target_host, "bash /tmp/compile_plugin.sh")
        if result is None:
            raise Exception("failed to compile_plugin")

    def run_container(self):
        pass

    def start_container(self):
        pass

    def init(self):
        self.remote = Remote(self.logger)
        self.target_host = self.env_setting["target_host"]

    def cleanup(self):
        pass

    def run(self):
        # self.install_mosquitto()
        # self.install_openjdk_11()
        # self.install_maven()
        self.compile_plugin()

    def desc(self) -> str:
        return "Test HiveMQ TDengine Extension"

    def author(self) -> str:
        return "BoDing"

    def tags(self):
        return "Third-Party, HiveMQ"
