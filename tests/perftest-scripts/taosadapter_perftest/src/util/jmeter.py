import sys
import shutil
sys.path.append("../../")
from config.env_init import *
from src.util.RemoteModule import RemoteModule

class Jmeter:
    def __init__(self):
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
        self.installPkg("java")

    def downloadJmeter(self):
        logger.info(f'{self.jmeter_ip}: downloading jmeter from {config["jmeter"]["jmeter_addr"]}')
        if not bool(int(self.jmeter_conn.exec_cmd(f'[ -e ~/{self.tar_file_name} ] && echo 1 || echo 0'))):
            self.jmeter_conn.exec_cmd(f'wget -P ~ {config["jmeter"]["jmeter_addr"]}')

    def deployJmeter(self):
        logger.info(f'{self.jmeter_ip}: deploying jmeter')
        self.downloadJmeter()
        self.installJava()
        self.jmeter_conn.exec_cmd(f'cd ~ && tar -xvf {self.tar_file_name}')
        if not bool(int(self.jmeter_conn.exec_cmd(f'grep "^jmeter.reportgenerator.overall_granularity"  ~/{self.tar_file_dir}/bin/user.properties >> /dev/null && echo 1 || echo 0'))):
            self.jmeter_conn.exec_cmd(f'echo "jmeter.reportgenerator.overall_granularity=300000" >> ~/{self.tar_file_dir}/bin/user.properties')
        self.jmeter_conn.exec_cmd(f'mv ~/{self.tar_file_dir} /usr/local')
        if not bool(int(self.jmeter_conn.exec_cmd(f'grep "jmeter" ~/.bashrc >> /dev/null && echo 1 || echo 0'))):
            self.jmeter_conn.exec_cmd(f'echo "export PATH=$PATH:/usr/local/{self.tar_file_dir}/bin" >> ~/.bashrc')
        if bool(int(self.jmeter_conn.exec_cmd(f'jmeter -v >> /dev/null && echo 1 || echo 0'))):
            logger.success('deploy jmeter successful')
        else:
            logger.error('deploy jmeter failed')
            sys.exit(1)

    def genJmx(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        des_jmx_file_list = list()
        base_jmx_file = os.path.join(current_dir, '../../config/taosadapter_performance_test.jmx')
        if config["taosadapter_separate_deploy"]:
            for key in config:
                if "taosd_dnode" in str(key):
                    des_jmx_file = os.path.join(current_dir, f'../../config/{key}.jmx')
                    # self.ip_list.append(config[key]["ip"])
                    shutil.copyfile(base_jmx_file, des_jmx_file)
                    des_jmx_file_list.append(des_jmx_file)
                    # TODO 
                    #replace restful_ip port
        else:
            des_jmx_file = os.path.join(current_dir, f'../../config/taosd_dnode1.jmx')
            # self.ip_list.append(config[key]["ip"])
            shutil.copyfile(base_jmx_file, des_jmx_file)
            des_jmx_file_list.append(des_jmx_file)
            # TODO 
            #replace restful_ip port
        return des_jmx_file_list

if __name__ == '__main__':
    deploy = Jmeter()
    deploy.deployJmeter()


