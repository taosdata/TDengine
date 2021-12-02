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
        logger.info(f'starting {self.dnode_ip}-taosd')
        self.dnode_conn.exec_cmd("sudo systemctl start taosd")
        
    def stopTaosd(self):
        logger.info(f'stopping {self.dnode_ip}-taosd')
        self.dnode_conn.exec_cmd("sudo systemctl stop taosd")

    def killTaosd(self):
        logger.info(f'killing {self.dnode_ip}-taosd')
        self.dnode_conn.exec_cmd("ps -ef | grep -w taosd | grep -v grep | awk \'{print $2}\' | sudo xargs kill -9")
    
    def restartTaosd(self):
        logger.info(f'restarting {self.dnode_ip}-taosd')
        self.dnode_conn.exec_cmd("sudo systemctl restart taosd")

    def startTaosadapter(self):
        logger.info(f'starting {self.dnode_ip}-Taosadapter')
        self.dnode_conn.exec_cmd("sudo systemctl start taosadapter")
        
    def stopTaosadapter(self):
        logger.info(f'stopping {self.dnode_ip}-Taosdapter')
        self.dnode_conn.exec_cmd("sudo systemctl stop taosdapter")

    def killTaosadapter(self):
        logger.info(f'killing {self.dnode_ip}-Taosdapter')
        self.dnode_conn.exec_cmd("ps -ef | grep -w taosdapter | grep -v grep | awk \'{print $2}\' | sudo xargs kill -9")
    
    def restartTaosadapter(self):
        logger.info(f'restarting {self.dnode_ip}-Taosdapter')
        self.dnode_conn.exec_cmd("sudo systemctl restart taosdapter")
    
    def rmTaosd(self):
        logger.info(f'removing {self.dnode_ip}-taosd')
        self.dnode_conn.exec_cmd("rmtaos")

    def rmTaosdLog(self):
        logger.info(f'removing {self.dnode_ip}-taosd\'s log')
        if self.dnode_dict["modify_cfg"]:
            self.dnode_conn.exec_cmd(f'sudo rm -rf {self.dnode_dict["cfg"]["logDir"]}/*')
        else:
            self.dnode_conn.exec_cmd("sudo rm -rf /var/log/taos/*")

    def rmTaosdData(self):
        logger.info(f'removing {self.dnode_ip}-taosd\'s data')
        if self.dnode_dict["modify_cfg"]:
            self.dnode_conn.exec_cmd(f'sudo rm -rf {self.dnode_dict["cfg"]["dataDir"]}/*')
        else:
            self.dnode_conn.exec_cmd("sudo rm -rf /var/lib/taos/*")

    def rmTaosCfg(self):
        logger.info(f'removing taos-{self.dnode_ip}\'s cfg')
        self.dnode_conn.exec_cmd("sudo rm -rf /etc/taos/taos.cfg")

    def modifyTaosCfg(self, firstEp=None):
        hostname = self.configHostname()
        if self.dnode_dict["modify_cfg"]:
            logger.info('modify /etc/taos/taos.cfg')
            for key, value in self.dnode_dict['cfg'].items():
                self.createRemoteDir(value)
                self.dnode_conn.exec_cmd(f'echo {key}   {value} >> /etc/taos/taos.cfg')
        if firstEp is not None:
            self.dnode_conn.exec_cmd(f'echo "firstEp   {firstEp}" >> /etc/taos/taos.cfg')
            self.dnode_conn.exec_cmd(f'echo "fqdn   {hostname}" >> /etc/taos/taos.cfg')

    def createRemoteDir(self, dir):
        '''
            if exist: echo 1
            else: echo 0
        '''
        res = bool(int(self.dnode_conn.exec_cmd(f'[ -e {dir} ] && echo 1 || echo 0')))
        if not res:
            self.dnode_conn.exec_cmd(f'sudo mkdir -p {dir}')

    def getHostname(self, ip=None):
        if ip == self.dnode_ip:
            return self.dnode_conn.exec_cmd('hostname').strip()
        else:
            return False

    def configHostname(self):
        logger.info(f'config {self.dnode_ip}-taosd\'s hostname')
        ori_hostname = self.dnode_conn.exec_cmd('hostname').strip()
        if "localhost" in str(ori_hostname).lower():
            self.dnode_conn.exec_cmd(f"sudo hostnamectl set-hostname {self.dnode_name}")
            return self.dnode_name
        return ori_hostname

    def hostsIsExist(self, ip, hostname):
        host_count = int(self.dnode_conn.exec_cmd(f'grep "^{ip}.*.{hostname}" /etc/hosts | wc -l'))
        if host_count > 0:
            logger.info(f'check {self.dnode_ip} /etc/hosts: {ip} {hostname} existed')
            return True
        else:
            logger.info(f'check {self.dnode_ip} /etc/hosts: {ip} {hostname} not exist')
            return False

    def configHosts(self, ip, hostname):
        if not self.hostsIsExist(ip, hostname):
            logger.info(f'config {self.dnode_ip}-taosd /etc/hosts: {ip} {hostname}')
            self.dnode_conn.exec_cmd(f'sudo echo "{ip} {hostname}" >> /etc/hosts')

    def checkStatus(self,  process):
        process_count = self.dnode_conn.exec_cmd(f'ps -ef | grep -w {process} | grep -v grep | wc -l')
        if int(process_count.strip()) > 0:
            logger.info(f'check {self.dnode_ip} {process} existed')
            return True
        else:
            logger.info(f'check {self.dnode_ip} {process} not exist')
            return False

    def taoscCreateDnodes(self):
        firstEp = f'{self.configHostname()}:6030'
        self.dnode_conn.exec_cmd(f'sudo taos -s "create dnode \'{firstEp}\'"')
        ready_count = self.dnode_conn.exec_cmd(f'taos -s "show dnodes" | grep {firstEp} | grep ready | wc -l')
        ready_flag = 0
        if int(ready_count) == 1:
            logger.success(f'deploy dnode {firstEp} success')
        while int(ready_count) != 1:
            if ready_flag < config["timeout"]:
                ready_flag += 1
            else:
                logger.error(f'deploy cluster {firstEp} failed, please check by manual')
            time.sleep(1)
            ready_count = self.dnode_conn.exec_cmd(f'taos -s "show dnodes" | grep {firstEp} | grep ready | wc -l')
            if int(ready_count) == 1:
                logger.success(f'deploy dnode {firstEp} success')
        
    def deployTaosd(self, firstEp=None, deploy_type="taosd"):
        '''
            deploy_type = taosd/taosadapter
        '''
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
        logger.info(f'install {self.dnode_ip}-taosd')
        logger.info(self.dnode_conn.exec_cmd(f'cd {remote_dir} && tar -xvf {remote_dir}/{package_name} && cd {package_dir} && ./install.sh'))
        self.modifyTaosCfg(firstEp)
        if deploy_type == "taosd":
            self.startTaosd()
        elif deploy_type == "taosadapter":
            self.startTaosadapter()
        if self.checkStatus(deploy_type):
            logger.success(f'{self.dnode_ip}-{deploy_type} deploy success')
        else:
            logger.error(f'{deploy_type} deploy failed, please check by manual')
            sys.exit(1)

class Dnodes:
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

    def modifyDnodeTaosCfg(self, index, taosCfgKey=None, taosCfgValue=None):
        self.dnodes[index - 1].modifyTaosCfg(taosCfgKey, taosCfgValue)
        
    def configDnodesHostname(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].configHostname()

    def configDnodesHosts(self):
        for index in range(len(self.dnodes)):
            for ip in self.ip_list:
                self.dnodes[index].configHosts(ip)

    def startDnodeTaosd(self, index):
        self.dnodes[index - 1].startTaosd()

    def stopDnodeTaosd(self, index):
        self.dnodes[index - 1].stopTaosd()

    def killDnodeTaosd(self, index):
        self.dnodes[index - 1].killTaosd()
    
    def restartDnodeTaosd(self, index):
        self.dnodes[index - 1].restartTaosd()

    def startAllTaosd(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].startTaosd()

    def stopAllTaosd(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].stopTaosd()
    
    def killAllTaosd(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].stopTaosd()

    def restartAllTaosd(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].restartTaosd()

    def startNodeTaosadapter(self, index):
        self.dnodes[index - 1].startTaosadapter()

    def stopNodeTaosadapter(self, index):
        self.dnodes[index - 1].stopTaosadapter()

    def killNodeTaosadapter(self, index):
        self.dnodes[index - 1].killTaosadapter()
    
    def restartNodeTaosadapter(self, index):
        self.dnodes[index - 1].restartTaosadapter()

    def startAllTaosadapters(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].startTaosadapter()

    def stopAllTaosadapters(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].stopTaosadapter()
    
    def killAllTaosadapters(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].killTaosadapter()

    def restartAllTaosadapters(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].restartTaosadapter()

    def configDnodesHostname(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].configHostname()

    def configDnodesHosts(self):
        ip_hostname_dict = dict()
        for index in range(len(self.dnodes)):
            for ip in self.ip_list:
                hostname = self.dnodes[index].getHostname(ip)
                if hostname is not False:
                    ip_hostname_dict[ip] = hostname
        for index in range(len(self.dnodes)):
            for ip, hostname in ip_hostname_dict.items():
                self.dnodes[index].configHosts(ip, hostname)

    def deployNodes(self):
        self.configDnodesHostname()
        self.configDnodesHosts()
        firstEp = f'{self.dnodes[0].configHostname()}:6030'
        if not config["taosadapter_separate_deploy"] and not config["taosd_cluster"]:
            self.dnodes[0].deployTaosd()
        elif config["taosadapter_separate_deploy"] and not config["taosd_cluster"]:
            for index in range(len(self.dnodes)):
                if index == 0:
                    self.dnodes[index].deployTaosd(firstEp, "taosd")
                else:
                    self.dnodes[index].deployTaosd(firstEp, "taosadapter")
        else:
            for index in range(len(self.dnodes)):
                self.dnodes[index].deployTaosd(firstEp)
            for index in range(len(self.dnodes)):
                if index != 0:
                    self.dnodes[index].taoscCreateDnodes()


    # def deployTaosadapters(self, firstEp):
    #     if config["taosadapter_separate_deploy"]:
    #         self.configDnodesHostname()
    #         self.configDnodesHosts()
    #         for i in range(len(self.dnodes)):
    #             self.dnodes[i].deployTaosd(firstEp, "taosadapter")

    # def deployClusters(self):
    #     if config["taosd_cluster"]:
    #         self.configDnodesHostname()
    #         self.configDnodesHosts()
    #         firstEp = f'{self.dnodes[0].dnode_name}:6030'
    #         for index in range(len(self.dnodes)):
    #             self.dnodes[index].deployTaosd(firstEp)
    #         for index in range(len(self.dnodes)):
    #             if index != 0:
    #                 self.dnodes[index].taoscCreateDnodes()
# class TaosAdapter:
#     def __init__(self):
#         self.dnodes = list()
#         self.ip_list = list()
#         index = 1
#         for key in config:
#             if "taosadapter_node" in str(key):
#                 self.dnodes.append(Dnode(index, config[key]["ip"], config[key]["port"], config[key]["username"], config[key]["password"], "taosadapter"))
#                 self.ip_list.append(config[key]["ip"])
#                 index += 1

#     def startNodeTaosadapter(self, index):
#         self.dnodes[index - 1].startTaosadapter()

#     def stopNodeTaosadapter(self, index):
#         self.dnodes[index - 1].stopTaosadapter()

#     def killNodeTaosadapter(self, index):
#         self.dnodes[index - 1].killTaosadapter()
    
#     def restartNodeTaosadapter(self, index):
#         self.dnodes[index - 1].restartTaosadapter()

#     def startAllTaosadapters(self):
#         for index in range(len(self.dnodes)):
#             self.dnodes[index].startTaosadapter()

#     def stopAllTaosadapters(self):
#         for index in range(len(self.dnodes)):
#             self.dnodes[index].stopTaosadapter()
    
#     def killAllTaosadapters(self):
#         for index in range(len(self.dnodes)):
#             self.dnodes[index].killTaosadapter()

#     def restartAllTaosadapters(self):
#         for index in range(len(self.dnodes)):
#             self.dnodes[index].restartTaosadapter()

#     def configDnodesHostname(self):
#         for index in range(len(self.dnodes)):
#             self.dnodes[index].configHostname()

#     def configDnodesHosts(self):
#         for index in range(len(self.dnodes)):
#             for ip in self.ip_list:
#                 self.dnodes[index].configHosts(ip)

#     def deployTaosadapters(self, firstEp):
#         if config["taosadapter_separate_deploy"]:
#             self.configDnodesHostname()
#             self.configDnodesHosts()
#             for i in range(len(self.dnodes)):
#                 self.dnodes[i].deployTaosd(firstEp, "taosadapter")

if __name__ == '__main__':
    # dnode_ip = config["taosd_dnode1"]["ip"]
    # dnode_port = config["taosd_dnode1"]["port"]
    # dnode_username = config["taosd_dnode1"]["username"]
    # dnode_password = config["taosd_dnode1"]["password"]
    # deploy = Dnode(1, dnode_ip, dnode_port, dnode_username, dnode_password)
    # deploy.deployTaosd()
    # firstEp = f'{deploy.dnode_name}:6030'

    # deploy_cluster = Cluster()
    # deploy_cluster.deployClusters()

    # deploy_taosadapter = TaosAdapter()
    # deploy_taosadapter.deployTaosadapters(firstEp)
    deploy = Dnodes()
    deploy.deployNodes()

    