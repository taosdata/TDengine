from ..util.remote import Remote
from ..util.common import TDCom

class CreateTableTool:
    def __init__(self, remote: Remote):
        self._remote = remote

    def install(self, host, version):
        """
        1. check if installed correct version.
        2. if not, install new one.
        3. else uninstall old one and install new one.
        """
        installFlag = False
        result = self._remote.cmd(host, ["which create_table"])
        if result == "":
            installFlag = True
            TDCom.download_pkg(version, self._remote, host)
        return

    def install_pkg(self, host, pkg):
        TDCom.install_with_pkg(self._remote, host, pkg)

    def setup(self, nodeDict):
        hosts = nodeDict["fqdn"]
        version = nodeDict["spec"]["version"]
        pkg = nodeDict["server_pkg"]
        for host in hosts:
            if pkg is None:
                self.install(host, version)
            else:
                # if package specified, install the package without checking version
                self.install_pkg(host, pkg)