"""
测试云服务 Go 连接器示例代码
启动命令: tt --case=cloud/connector/rust.py --use=cloud_test.yaml
"""
import os

from taostest import TDCase, T


class Rust(TDCase):
    def run(self):
        cwd = self.env_setting["work_dir"] + "/docs-cloud/docs/examples/go/connectexample"
        os.environ.update(self.env_setting["env"])
        self.lcmd.run("go mod tidy", cwd=cwd)
        self.lcmd.run("go run main.go", cwd=cwd)

    def desc(self) -> str:
        return "Go connector for cloud"

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud.Connector.Go

    def init(self):
        pass

    def cleanup(self):
        pass
