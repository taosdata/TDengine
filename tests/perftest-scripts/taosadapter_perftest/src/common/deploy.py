import sys
sys.path.append("../../")
from config.env_init import *
from src.util.RemoteModule import RemoteModule
class Dnode:
    def __init__(self, index, dnode_ip, dnode_port, dnode_username, dnode_password):
        self.install_package = config["install_package"]
        self.hostname_prefix = config["hostname_prefix"]
        self.ip_suffix = dnode_ip.split('.')[-1]
        self.dnode_name = self.hostname_prefix + self.ip_suffix
        self.index = index
        self.dnode_dict = config[f'taosd_dnode{self.index}']
        self.dnode_ip = dnode_ip
        self.dnode_port = dnode_port
        self.dnode_username = dnode_username
        self.dnode_password = dnode_password
        self.dnode_conn = RemoteModule(self.dnode_ip, self.dnode_port, self.dnode_username, self.dnode_password)

        # self.dnode_ip = config["taosd_dnode1"]["ip"]
        # self.dnode_port = config["taosd_dnode1"]["port"]
        # self.dnode_username = config["taosd_dnode1"]["username"]
        # self.dnode_password = config["taosd_dnode1"]["password"]
        # self.RM_dnode_conn = RemoteModule(self.dnode1_ip, self.dnode1_port, self.dnode1_username, self.dnode1_password)
        # self.RM_dnode1 = RemoteModule(self.dnode1_ip, self.dnode1_port, self.dnode1_username, self.dnode1_password)

    def startTaosd(self):
        logger.info(f'starting {self.dnode_name}-taosd')
        self.dnode_conn.exec_cmd("sudo systemctl start taosd")
        
    def stopTaosd(self):
        logger.info(f'stopping {self.dnode_name}-taosd')
        self.dnode_conn.exec_cmd("sudo systemctl stop taosd")

    def killTaosd(self):
        logger.info(f'killing {self.dnode_name}-taosd')
        self.dnode_conn.exec_cmd("ps -ef | grep -w taosd | grep -v grep | awk \'{print $2}\' | sudo xargs kill -9")
    
    def restartTaosd(self):
        logger.info(f'restarting {self.dnode_name}-taosd')
        self.dnode_conn.exec_cmd("sudo systemctl restart taosd")
    
    def rmTaosd(self):
        logger.info(f'removing {self.dnode_name}-taosd')
        self.dnode_conn.exec_cmd("rmtaos")

    def rmTaosdLog(self):
        logger.info(f'removing {self.dnode_name}-taosd\'s log')
        if self.dnode_dict["modify_cfg"]:
            self.dnode_conn.exec_cmd(f'sudo rm -rf {self.dnode_dict["cfg"]["logDir"]}/*')
        else:
            self.dnode_conn.exec_cmd("sudo rm -rf /var/log/taos/*")

    def rmTaosdData(self):
        logger.info(f'removing {self.dnode_name}-taosd\'s data')
        if self.dnode_dict["modify_cfg"]:
            self.dnode_conn.exec_cmd(f'sudo rm -rf {self.dnode_dict["cfg"]["dataDir"]}/*')
        else:
            self.dnode_conn.exec_cmd("sudo rm -rf /var/lib/taos/*")

    def rmTaosCfg(self):
        logger.info(f'removing taos-{self.dnode_name}\'s cfg')
        self.dnode_conn.exec_cmd("sudo rm -rf /etc/taos/taos.cfg")

    def configHostname(self):
        logger.info(f'config {self.dnode_name}-taosd\'s hostname: {self.dnode_name}')
        self.dnode_conn.exec_cmd(f"sudo hostnamectl set-hostname {self.dnode_name}")

    def createRemoteDir(self, dir):
        '''
            if exist: echo 1
            else: echo 0
        '''
        res = bool(int(self.dnode_conn.exec_cmd(f'[ -e {dir} ] && echo 1 || echo 0')))
        if not res:
            self.dnode_conn.exec_cmd(f'sudo mkdir -p {dir}')

    def modifyTaosCfg(self):
        if self.dnode_dict["modify_cfg"]:
            logger.info('modify /etc/taos/taos.cfg')
            for key, value in self.dnode_dict['cfg'].items():
                self.createRemoteDir(value)
                self.dnode_conn.exec_cmd(f'echo {key}   {value} >> /etc/taos/taos.cfg')

    def hostsIsExist(self, ip):
        ip_suffix = ip.split('.')[-1]
        host_count = int(self.dnode_conn.exec_cmd(f'grep "^{ip}.*.{self.hostname_prefix}{ip_suffix}" /etc/hosts | wc -l'))
        if host_count > 0:
            logger.info(f'check {self.dnode_name}-taosd /etc/hosts: {ip} {self.hostname_prefix}{ip_suffix} existed')
            return True
        else:
            logger.info(f'check {self.dnode_name}-taosd /etc/hosts: {ip} {self.hostname_prefix}{ip_suffix} not exist')
            return False

    def configHosts(self, ip):
        if not self.hostsIsExist(ip):
            ip_suffix = ip.split('.')[-1]
            logger.info(f'config {self.dnode_name}-taosd /etc/hosts: {ip} {self.hostname_prefix}{ip_suffix}')
            self.dnode_conn.exec_cmd(f'sudo echo "{ip} {self.hostname_prefix}{ip_suffix}" >> /etc/hosts')

    def checkStatus(self,  process):
        process_count = self.dnode_conn.exec_cmd(f'ps -ef | grep -w {process} | grep -v grep | wc -l')
        if int(process_count.strip()) > 0:
            logger.info(f'check {self.dnode_name}-taosd {process} existed')
            return True
        else:
            logger.info(f'check {self.dnode_name}-taosd {process} not exist')
            return False

    def deployTaosd(self):
        if self.dnode_username == "root":
            remote_dir = "/root"
        else:
            remote_dir = f"/home/{self.dnode_username}"
        self.dnode_conn.upload_file(remote_dir, self.install_package)
        if config["clean_env"]:
            self.rmTaosCfg()
            self.rmTaosdLog()
            self.rmTaosdData()
        package_name = self.install_package.split("/")[-1]
        package_dir = '-'.join(package_name.split("-", 3)[0:3])
        self.stopTaosd()
        self.killTaosd()
        logger.info(f'install {self.dnode_name}-taosd')
        logger.info(self.dnode_conn.exec_cmd(f'cd {remote_dir} && tar -xvf {remote_dir}/{package_name} && cd {package_dir} && ./install.sh'))
        self.modifyTaosCfg()
        self.dnode_conn.exec_cmd('systemctl start taosd')
        if self.checkStatus("taosd"):
            logger.success(f'{self.dnode_name}-taosd deploy success')
        else:
            logger.error('taosd deploy failed, please check by manual')
            sys.exit(1)

class Cluster:
    def __init__(self):
        self.dnodes = list()
        self.ip_list = list()
        index = 1
        for key in config:
            if "taosd_dnode" in str(key):
                self.dnodes.append(Dnode(index, config[key]["ip"], config[key]["port"], config[key]["username"], config[key]["password"]))
                self.ip_list.append(config[key]["ip"])
                index += 1

    def rmDnodeTaosd(self, index):
        self.dnodes[index - 1].rmTaosd()

    def rmDnodeTaosdLog(self, index):
        self.dnodes[index - 1].rmTaosdLog()

    def rmDnodeTaosdData(self, index):
        self.dnodes[index - 1].rmTaosdData()

    def rmDnodeTaosCfg(self, index):
        self.dnodes[index - 1].rmTaosCfg()

    def modifyDnodeTaosCfg(self, index):
        self.dnodes[index - 1].modifyTaosCfg()
        
    def configDnodesHostname(self):
        for i in range(len(self.dnodes)):
            self.dnodes[i].configHostname()

    def configDnodesHosts(self):
        for index in range(len(self.dnodes)):
            for ip in self.ip_list:
                self.dnodes[index].configHosts(ip)

    def stopDnodeTaosd(self, index):
        self.dnodes[index - 1].stopTaosd()

    def killDnodeTaosd(self, index):
        self.dnodes[index - 1].killTaosd()

    def startDnodeTaosd(self, index):
        self.dnodes[index - 1].startTaosd()

    def stopAllTaosd(self):
        for i in range(len(self.dnodes)):
            self.dnodes[i].stopTaosd()
    
    def killAllTaosd(self):
        for i in range(len(self.dnodes)):
            self.dnodes[i].stopTaosd()
    
    def startAllTaosd(self):
        for i in range(len(self.dnodes)):
            self.dnodes[i].startTaosd()
    
    def deployClusters(self):
        if config["taosd_cluster"]:
            self.configDnodesHostname()
            self.configDnodesHosts()
            # TODO
            # for i in range(len(self.dnodes)):
            #     if i == 0:
                    
            #     if i > 0:
            #         self.modifyDnodeTaosCfg(i + 2)
            #     self.dnodes[i].deployTaosd()



        

if __name__ == '__main__':   
    # dnode_ip = config["taosd_dnode1"]["ip"]
    # dnode_port = config["taosd_dnode1"]["port"]
    # dnode_username = config["taosd_dnode1"]["username"]
    # dnode_password = config["taosd_dnode1"]["password"]
    # deploy = Dnode(1, dnode_ip, dnode_port, dnode_username, dnode_password)
    # deploy.deployTaosd()
    # dnode_ip = config["taosd_dnode2"]["ip"]
    # dnode_port = config["taosd_dnode2"]["port"]
    # dnode_username = config["taosd_dnode2"]["username"]
    # dnode_password = config["taosd_dnode2"]["password"]
    # deploy = Dnode(2, dnode_ip, dnode_port, dnode_username, dnode_password)
    # deploy.deployTaosd()

    deploy_cluster = Cluster()
    deploy_cluster.deployClusters()

    