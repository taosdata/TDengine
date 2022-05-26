import os.path

from taostest import TDCase
from taostest.util import Remote


class HiveMQTest(TDCase):
    def prepare(self):
        script_path = os.path.join(self.get_case_folder(), "prepare.sh")
        suc = self.remote.put(self.target_host, script_path, "/tmp")
        if not suc:
            raise Exception("failed to put prepare.sh")
        result = self.remote.cmd(self.target_host, "bash /tmp/prepare.sh")
        if result is None:
            raise Exception("failed to run prepare.sh")

    def compile_plugin(self):
        compile_script = os.path.join(self.get_case_folder(), "compile_plugin.sh")
        suc = self.remote.put(self.target_host, compile_script, "/tmp")
        if not suc:
            raise Exception("failed to put compile_script")
        result = self.remote.cmd(self.target_host, "bash /tmp/compile_plugin.sh")
        if result is None:
            raise Exception("failed to compile_plugin")

    def config_rest(self):
        result = self.remote.cmd(self.target_host, 'sed -i "s/127.0.0.1/192.168.0.41/" /root/hivemq-tdengine-extension/tdengine.xml')
        if result is None:
            raise Exception("failed to config plugin for rest connection")

    def config_native(self):
        result = self.remote.cmd(self.target_host, ['sed -i "s/6041/6030/" /root/hivemq-tdengine-extension/tdengine.xml',
                                                    'sed -i "s/>http</>sdk</" /root/hivemq-tdengine-extension/tdengine.xml'])
        if result is None:
            raise Exception("failed to config plugin for native connection")

    def run_container(self):
        script = os.path.join(self.get_case_folder(), "run_container.sh")
        suc = self.remote.put(self.target_host, script, "/tmp")
        if not suc:
            raise Exception("failed to run run_container.sh")
        result = self.remote.cmd(self.target_host, "bash /tmp/run_container.sh")
        if result is None:
            raise Exception("failed to run container")

    def send_test_data(self, n):
        cmd = f"mosquitto_pub -t taostest-hivemq -m 'test message xxx' --repeat {n}"
        result = self.remote.cmd(self.target_host, cmd)
        if result is None:
            raise Exception("failed to send test data")

    def init(self):
        self.remote = Remote(self.logger)
        self.target_host = self.env_setting["target_host"]

    def cleanup(self):
        pass

    def run(self):
        # self.prepare()
        self.compile_plugin()
        self.tdSql.execute("drop database if exists hivemq")
        self.config_rest()
        self.run_container()
        self.send_test_data(3)
        # self.config_native()
        # kill and start container to make new configuration take effect
        # self.run_container()
        # self.send_test_data(3)
        self.tdSql.query("select count(*) from hivemq.mqtt_payload")
        self.tdSql.checkData(1, 1, 3)

    def desc(self) -> str:
        return "Test HiveMQ TDengine Extension"

    def author(self) -> str:
        return "BoDing"

    def tags(self):
        return "Third-Party, HiveMQ"
