"""
Prepare machines for Test Framework
为测试框架准备环境
"""

import os
import platform
from fabric2 import Connection
from paramiko import AuthenticationException
from ..errors import UnknownOSException
from ..util.remote import Remote
from ..logger import Logger

CentOS = "CentOS"
Ubuntu = "Ubuntu"
Debian = "Debian"


class ResourceManager:

    def __init__(self, logger, env_root):
        """
        res_file: 机器资源列表
        """
        self._env_root = env_root
        self._logger: Logger = logger
        self._remote = Remote(self._logger)
        self.host_pass = dict()
        self._local_host = platform.node()

    def _read_host_pass(self, res_file):
        # 如果参数中有逗号，则为主机名+口令
        if "," in res_file:
            self._parse_res(res_file)
        else:
            res_path = os.path.join(self._env_root, res_file)
            with open(res_path) as f:
                for line in f.readlines():
                    line = line.strip()
                    self._parse_res(line)
        self._logger.info("got resource list: %s", self.host_pass)

    def _parse_res(self, line: str):
        # 口令中可能包含“，”，避免使用split
        pos = line.find(",")
        print("find:", pos)
        if pos > 0:
            host = line[0:pos]
            pwd = line[pos+1:]
        else:
            host = line
            pwd = ""
        if len(host) == 0:
            self._logger.error("wrong line: %s", line)
        else:
            self.host_pass[host] = pwd

    def prepare(self, res_file):
        """
        准备测试用的机器，输入是机器列表文件。
        机器列表文件有两列，第一列是 hostname， 第二列是 root 密码。
        如果已经手动配置了无密码访问，第二列可以省略。
        """
        self._logger.info("prepare test cluster specified by: %s", res_file)
        self._read_host_pass(res_file)
        pvt_key_path_rsa = os.path.expanduser("~") + "/.ssh/id_rsa"
        pvt_key_path_ed25519 = os.path.expanduser("~") + "/.ssh/id_ed25519"
        if os.path.exists(pvt_key_path_rsa):
            pub_key_path = pvt_key_path_rsa + ".pub"
        elif os.path.exists(pvt_key_path_ed25519):
            pub_key_path = pvt_key_path_ed25519 + ".pub"
        else:
            self._ssh_keygen(pvt_key_path_rsa)
        for host, pwd in self.host_pass.items():
            if not self._check_no_pass_ssh(host):
                self._copy_pubkey(host, pub_key_path, password=pwd)
        for host in self.host_pass.keys():
            self._install_all(host)

    def _install_all(self, host):
        """
        在 host 上安装框架依赖的所有命令
        """
        self._logger.info("prepare host %s", host)
        with Connection(host, user="root") as c:
            cmd = "cat /etc/os-release  | grep ^NAME"
            self._logger.debug(cmd)
            out = c.run(cmd).stdout
            if CentOS in out:
                os_name = CentOS
            elif Ubuntu in out:
                os_name = Ubuntu
            elif Debian in out:
                os_name = Ubuntu
            else:
                self._logger.error("can't determine what OS it is: %s", out)
                return
            self._logger.info("os_name=%s", os_name)
            self._install_wget(c, os_name)
            self._install_iptables(c, os_name)
            self._install_screen(c, os_name)
            self._install_docker(c, os_name)
            self._install_compose(c)
            self._install_ntpdate(c, os_name)
            self._install_valgrind(c, os_name)

    def _check_no_pass_ssh(self, host):
        """
        检查是否可以无密码访问
        """
        with Connection(host, user="root") as c:
            try:
                c.run("")
                return True
            except AuthenticationException:
                return False

    def _ssh_keygen(self, id_file):
        """
        使用ssh-keygen命令，在当前机器生成.ssh/id_rsa.pub文件,需要同时支持linux、windows、mac
        ssh-keygen -f ~/.ssh/id_rsa -N ""
        """
        self._logger.info("generate local ssh public key")
        ret = os.system(f"ssh-keygen -f {id_file} -N ''")
        if ret != 0:
            self._logger.error("generate ssh public key error")

    def _command_exists(self, c: Connection, prog_name):
        """
        检查一个命令行程序是否可用
        """
        cmd = "command -v " + prog_name
        self._logger.debug("%s", cmd)
        result = c.run(cmd, warn=True)
        self._logger.debug("%s exists: %s", prog_name, result.ok)
        return result.ok

    def _install_wget(self, c: Connection, os_name: str):
        if self._command_exists(c, "wget"):
            return
        if os_name == Ubuntu:
            cmd = "apt-get install -y wget"
        elif os_name == CentOS:
            cmd = "yum -y install wget"
        else:
            raise UnknownOSException(os_name)
        self._logger.info("cmd: %s", cmd)
        c.run(cmd)

    def _install_iptables(self, c: Connection, os_name: str):
        if self._command_exists(c, "iptables"):
            return
        if os_name == Ubuntu:
            cmd = "apt-get install -y iptables"
        elif os_name == CentOS:
            cmd = "yum -y install iptables"
        else:
            raise UnknownOSException(os_name)
        self._logger.info("cmd: %s", cmd)
        c.run(cmd)

    def _install_screen(self, c: Connection, os_name: str):
        """
        1. 检查screen命令是否安装
        2. 如果没有安装则安装
        ubuntu: apt-get install screen
        centos: yum install screen
        """
        if self._command_exists(c, "screen"):
            return
        if os_name == Ubuntu:
            cmd = "apt-get install -y screen"
        elif os_name == CentOS:
            cmd = "yum install -y screen"
        else:
            raise UnknownOSException(os_name)
        self._logger.info("cmd: %s", cmd)
        c.run(cmd)

    def _install_docker(self, c: Connection, os_name: str):
        """
        1. 检查docker engine是否安装
        2. 如果没有安装则安装
        安装教程：https://docs.docker.com/engine/install/
        """
        if self._command_exists(c, "docker"):
            return
        if os_name == Ubuntu:
            cmd = ""
        elif os_name == CentOS:
            self._install_docker_on_centos(c)
        else:
            raise UnknownOSException(os_name)

    def _install_docker_on_ubuntu(self, c: Connection):
        cmd = "apt-get install -y -q \
            ca-certificates \
            curl \
            gnupg \
            lsb-release"
        self._logger.info(cmd)
        c.run(cmd, warn=True)
        cmd = "[ ! -f /usr/share/keyrings/docker-archive-keyring.gpgx ] && curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg"
        self._logger.info(cmd)
        c.run(cmd, warn=True)
        cmd = """echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu  $(lsb_release -cs) stable" |  tee /etc/apt/sources.list.d/docker.list > /dev/null"""
        self._logger.info(cmd)
        c.run(cmd, warn=True)
        cmd = "apt-get install -y  docker-ce docker-ce-cli containerd.io"
        self._logger.info(cmd)
        c.run(cmd)
        if not self._command_exists(c, "docker"):
            self._logger.error("install docker failed")

    def _install_docker_on_centos(self, c: Connection):
        cmd = "yum install -y yum-utils"
        self._logger.info(cmd)
        c.run(cmd, warn=True)
        cmd = """yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo"""
        self._logger.info(cmd)
        c.run(cmd, warn=True)
        cmd = "yum install -y docker-ce docker-ce-cli containerd.io"
        self._logger.info(cmd)
        c.run(cmd, warn=True)
        if not self._command_exists(c, "docker"):
            self._logger.error("install docker failed")

    def _install_compose(self, c: Connection):
        """
        1. 检查docker-compose命令是否存在
        2. 如果不存在则安装
        安装命令： sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        安装之后添加可执行权限 chmod 755 /usr/local/bin/docker-compose
        """
        if self._command_exists(c, "docker-compose"):
            return
        cmd = """curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose"""
        self._logger.info(cmd)
        c.run(cmd, warn=True)
        cmd = "chmod 755 /usr/local/bin/docker-compose"
        self._logger.info(cmd)
        c.run(cmd)
        if not self._command_exists(c, "docker-compose"):
            self._logger.error("install docker-dompose failed")

    def _install_ntpdate(self, c: Connection, os_name: str):
        """
        1. 检查ntpdate命令是否安装
        2. 如果没有安装则安装
        ubuntu: apt-get install ntpdate
        centos: yum install -y ntpdate
        """
        if self._command_exists(c, "ntpdate"):
            return
        if os_name == Ubuntu:
            cmd = "apt-get install -y ntpdate"
        elif os_name == CentOS:
            cmd = "yum install -y ntpdate"
        else:
            raise UnknownOSException(os_name)
        self._logger.info("cmd: %s", cmd)
        c.run(cmd)

    def _install_valgrind(self, c: Connection, os_name: str):
        """
        1. 检查ntpdate命令是否安装
        2. 如果没有安装则安装
        ubuntu: apt-get install valgrind
        centos: yum install -y https://rpmfind.net/linux/centos/7.9.2009/os/x86_64/Packages/valgrind-3.15.0-11.el7.x86_64.rpm
        """
        if self._command_exists(c, "valgrind"):
            return
        if os_name == Ubuntu:
            cmd = "apt-get install -y valgrind"
        elif os_name == CentOS:
            cmd = "yum install -y https://rpmfind.net/linux/centos/7.9.2009/os/x86_64/Packages/valgrind-3.15.0-11.el7.x86_64.rpm"
        else:
            raise UnknownOSException(os_name)
        self._logger.info("cmd: %s", cmd)
        c.run(cmd)

    def _copy_pubkey(self, host, local_pub_key_path, remote_tmp_file="./tmp_pub_key", password=""):
        """
        1. 检查无密码访问是否成功
        2. 如果不成功则添加本地id_rsa.pub到root@host:~/.ssh/authorized_keys
        """
        if host == self._local_host:
            return
        self._logger.info("copy pubkey to %s", host)
        suc = self._remote.put(host, local_pub_key_path, remote_tmp_file, password)
        if not suc:
            self._logger.error("copy pubkey failed to %s", host)
            return

        result = self._remote.cmd2(host, [
            f"cat {remote_tmp_file}/id_rsa.pub >> .ssh/authorized_keys",
            "chmod 600 .ssh/authorized_keys",
            "rm -f " + remote_tmp_file
        ], password=password)

        if result.failed:
            self._logger.error("copy pubkey failed to %s", host)
        else:
            self._logger.info("copy pubkey success to %s", host)
