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
        self.lcmd.run("cargo run", cwd=cwd)

    def desc(self) -> str:
        return "Rust connector for cloud"

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud.Connector.Rust

    def init(self):
        pass

    def cleanup(self):
        pass
