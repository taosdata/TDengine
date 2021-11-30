import sys
sys.path.append("../../")
from config.env_init import *
from RemoteModule import RemoteModule
class Deploy():
    def __init__(self):
        self.install_package = config["install_package"]
        self.dnode1_ip = config["taosd_dnode1"]["ip"]
        self.dnode1_port = config["taosd_dnode1"]["port"]
        self.dnode1_username = config["taosd_dnode1"]["username"]
        self.dnode1_password = config["taosd_dnode1"]["password"]
        self.RM_dnode1 = RemoteModule(self.dnode1_ip, self.dnode1_port, self.dnode1_username, self.dnode1_password)

    def checkStatus(self,  process):
        process_count = self.RM_dnode1.exec_cmd(f'ps -ef | grep -w {process} | grep -v grep | wc -l')
        if int(process_count.strip()) > 0:
            return True
        else:
            return False


    def deployTaosd(self):
        if self.dnode1_username == "root":
            remote_dir = "/root"
        else:
            remote_dir = f"/home/{self.dnode1_username}"
        self.RM_dnode1.upload_file(remote_dir, self.install_package)
        if config["clean_env"]:
            self.RM_dnode1.exec_cmd('rm -rf /var/lib/taos/* /var/log/taos/* /etc/taos/taos.cfg')
        package_name = self.install_package.split("/")[-1]
        package_dir = '-'.join(package_name.split("-", 3)[0:3])
        logger.info('kill taosd')
        logger.info(self.RM_dnode1.exec_cmd('systemctl stop taosd && ps -ef | grep -w taosd | grep -v grep | awk \'{print $2}\' | xargs kill -9'))
        logger.info(self.RM_dnode1.exec_cmd(f'cd {remote_dir} && tar -xvf {remote_dir}/{package_name} && cd {package_dir} && ./install.sh'))
        logger.info(self.RM_dnode1.exec_cmd('systemctl start taosd'))
        if self.checkStatus("taosd"):
            logger.success('taosd deploy success')
        else:
            logger.error('taosd deploy failed, please check by manual')
            sys.exit(1)
        print("should not reach here")


if __name__ == '__main__':   
    deploy = Deploy()
    deploy.deployTaosd()

    