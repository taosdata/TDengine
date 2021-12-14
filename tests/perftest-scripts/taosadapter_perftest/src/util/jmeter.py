import sys
sys.path.append("../../")
from config.env_init import *
from src.util.RemoteModule import RemoteModule
from src.common.common import Common

class Jmeter:
    def __init__(self):
        self.Com = Common()
        self.jmeter_ip = config["jmeter"]["ip"]
        self.jmeter_port = config["jmeter"]["port"]
        self.jmeter_username = config["jmeter"]["username"]
        self.jmeter_password = config["jmeter"]["password"]
        self.jmeter_conn = RemoteModule(self.jmeter_ip, self.jmeter_port, self.jmeter_username, self.jmeter_password)
        self.tar_file_name = config["jmeter"]["jmeter_addr"].split("/")[-1]
        self.tar_file_dir = self.tar_file_name.replace(".tgz", "")
        if self.jmeter_username == "root":
            self.home_dir = "/root"
        else:
            self.home_dir = f"/home/{self.jmeter_username}"

    def installPkg(self, pkg_name):
        if bool(int(self.jmeter_conn.exec_cmd('cat /etc/os-release | grep ubuntu >> /dev/null && echo 1 || echo 0'))):
            if not bool(int(self.jmeter_conn.exec_cmd(f'sudo dpkg -s {pkg_name} >> /dev/null && echo 1 || echo 0'))):
                self.jmeter_conn.exec_cmd(f'sudo apt-get install -y {pkg_name}')
        elif bool(int(self.jmeter_conn.exec_cmd(f'cat /etc/os-release | grep centos >> /dev/null && echo 1 || echo 0'))):
            if not bool(int(self.jmeter_conn.exec_cmd(f'sudo rpm -qa | grep {pkg_name} >> /dev/null && echo 1 || echo 0'))):
                self.jmeter_conn.exec_cmd(f'sudo yum install -y {pkg_name}')
        else:
            pass

    def installJava(self):
        self.installPkg("openjdk-8-jdk")

    def downloadJmeter(self):
        logger.info(f'{self.jmeter_ip}: downloading jmeter from {config["jmeter"]["jmeter_addr"]}')
        if not bool(int(self.jmeter_conn.exec_cmd(f'[ -e ~/{self.tar_file_name} ] && echo 1 || echo 0'))):
            self.jmeter_conn.exec_cmd(f'wget -P ~ {config["jmeter"]["jmeter_addr"]}')

    def deployJmeter(self):
        logger.info(f'{self.jmeter_ip}: deploying jmeter')
        self.downloadJmeter()
        self.installJava()
        if not bool(int(self.jmeter_conn.exec_cmd(f'ls ~/{self.tar_file_dir} >> /dev/null && echo 1 || echo 0'))):
            self.jmeter_conn.exec_cmd(f'cd ~ && tar -xvf {self.tar_file_name}')
        if not bool(int(self.jmeter_conn.exec_cmd(f'grep "^jmeter.reportgenerator.overall_granularity"  ~/{self.tar_file_dir}/bin/user.properties >> /dev/null && echo 1 || echo 0'))):
            self.jmeter_conn.exec_cmd(f'echo "jmeter.reportgenerator.overall_granularity=300000" >> ~/{self.tar_file_dir}/bin/user.properties')
        if not bool(int(self.jmeter_conn.exec_cmd(f'ls /usr/local/{self.tar_file_dir} >> /dev/null && echo 1 || echo 0'))):
            self.jmeter_conn.exec_cmd(f'mv ~/{self.tar_file_dir} /usr/local')
        if not bool(int(self.jmeter_conn.exec_cmd(f'grep "jmeter" ~/.bashrc >> /dev/null && echo 1 || echo 0'))):
            self.jmeter_conn.exec_cmd(f'echo "export PATH=$PATH:/usr/local/{self.tar_file_dir}/bin" >> ~/.bashrc')
        # if bool(int(self.jmeter_conn.exec_cmd(f'jmeter -v >> /dev/null && echo 1 || echo 0'))):
        #     logger.success('deploy jmeter successful')
        # else:
        #     logger.error('deploy jmeter failed')
        #     sys.exit(1)
        return f"/usr/local/{self.tar_file_dir}/bin/jmeter"

if __name__ == '__main__':
    deploy = Jmeter()
    deploy.deployJmeter()


