"""
测试云服务 Node.js 连接器示例代码
启动命令: tt --case=cloud/connector/node.py --use=cloud_test.yaml
"""
import os

from taostest import TDCase, T


class Node(TDCase):
    def run(self):
        cwd = self.env_setting["work_dir"] + "/docs-cloud/docs/examples/node"
        os.environ.update(self.env_setting["env"])
        self.lcmd.run("rm -rf node_modules", cwd=cwd)
        self.lcmd.run("npm i td2.0-rest-connector", cwd=cwd)
        self.lcmd.run("node connect.js", cwd=cwd)

    def desc(self) -> str:
        return "Node.js connector for cloud"

    def author(self) -> str:
        return "Ding Bo"

    def tags(self):
        return T.Cloud.Connector.Node

    def init(self):
        pass
