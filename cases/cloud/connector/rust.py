"""
测试云服务 Rust 连接器示例代码
启动命令: tt --case=cloud/connector/python.py --use=cloud_test.yaml
"""
from taostest import TDCase, T


class Rust(TDCase):
    def run(self):
        cwd = self.env_setting["work_dir"] + "/docs-cloud/docs/examples/rust/cloud-example"
        self.lcmd.run("cargo run", cwd=cwd, env=self.env_setting["env"])

    def desc(self) -> str:
        return "rust connector for cloud"

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud.Connector.Rust

    def init(self):
        pass
