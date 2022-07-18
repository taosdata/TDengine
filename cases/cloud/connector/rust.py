"""
测试云服务 Rust 连接器示例代码
启动命令: tt --case=cloud/connector/rust.py --use=cloud_test.yaml
"""
import os

from taostest import TDCase, T


class Rust(TDCase):
    def run(self):
        cwd = self.env_setting["work_dir"] + "/docs-cloud/docs/examples/rust/cloud-example"
        os.environ.update(self.env_setting["env"])
        print("---------------------------------------------")
        print(os.environ["PATH"])
        print("----------------------------------------------")
        self.lcmd.run("cargo run", cwd=cwd, env=os.environ)

    def desc(self) -> str:
        return "rust connector for cloud"

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud.Connector.Rust

    def init(self):
        pass
