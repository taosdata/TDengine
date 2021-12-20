import sys
import json
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

        if self.dnode_username == "root":
            self.home_dir = "/root"
        else:
            self.home_dir = f"/home/{self.dnode_username}"

    def installPackage(self):
        if bool(int(self.dnode_conn.exec_cmd(f'cat /etc/os-release | grep ubuntu >> /dev/null && echo 1 || echo 0'))):
            package_list = ["wget", "screen"]
            for package in package_list:
                if not bool(int(self.dnode_conn.exec_cmd(f'sudo dpkg -s {package} >> /dev/null && echo 1 || echo 0'))):
                    self.dnode_conn.exec_cmd(f'apt update -y && apt install -y {package}')
        elif bool(int(self.dnode_conn.exec_cmd(f'cat /etc/os-release | grep centos >> /dev/null && echo 1 || echo 0'))):
            package_list = ["wget", "screen"]
            for package in package_list:
                if not bool(int(self.dnode_conn.exec_cmd(f'sudo rpm -qa | grep {package} >> /dev/null && echo 1 || echo 0'))):
                    self.dnode_conn.exec_cmd(f'yum update -y && yum install -y {package}')
        else:
            pass

    def startTaosd(self):
        logger.info(f'{self.dnode_ip}: starting taosd')
        self.dnode_conn.exec_cmd("sudo systemctl start taosd")
        
    def stopTaosd(self):
        logger.info(f'{self.dnode_ip}: stopping taosd')
        self.dnode_conn.exec_cmd("sudo systemctl stop taosd")

    def killTaosd(self):
        logger.info(f'{self.dnode_ip}: killing taosd')
        self.dnode_conn.exec_cmd("ps -ef | grep -w taosd | grep -v grep | awk \'{print $2}\' | sudo xargs kill -9")
    
    def restartTaosd(self):
        logger.info(f'{self.dnode_ip}: restarting taosd')
        self.dnode_conn.exec_cmd("sudo systemctl restart taosd")

    def startTaosadapter(self):
        logger.info(f'{self.dnode_ip}: starting taosadapter')
        self.dnode_conn.exec_cmd("sudo systemctl start taosadapter")
        
    def stopTaosadapter(self):
        logger.info(f'{self.dnode_ip}: stopping taosadapter')
        self.dnode_conn.exec_cmd("sudo systemctl stop taosadapter")

    def killTaosadapter(self):
        logger.info(f'{self.dnode_ip}: killing taosadapter')
        self.dnode_conn.exec_cmd("ps -ef | grep -w taosadapter | grep -v grep | awk \'{print $2}\' | sudo xargs kill -9")
    
    def restartTaosadapter(self):
        logger.info(f'{self.dnode_ip}: restarting taosadapter')
        self.dnode_conn.exec_cmd("sudo systemctl restart taosadapter")
    
    def rmTaosd(self):
        logger.info(f'{self.dnode_ip}: removing taosd')
        self.dnode_conn.exec_cmd("rmtaos")

    def rmTaosdLog(self):
        logger.info(f'{self.dnode_ip}: removing taosd log')
        if self.dnode_dict["modify_cfg"]:
            self.dnode_conn.exec_cmd(f'sudo rm -rf {self.dnode_dict["cfg"]["logDir"]}/*')
        else:
            self.dnode_conn.exec_cmd("sudo rm -rf /var/log/taos/*")

    def rmTaosdData(self):
        logger.info(f'{self.dnode_ip}: removing taosd data')
        if self.dnode_dict["modify_cfg"]:
            self.dnode_conn.exec_cmd(f'sudo rm -rf {self.dnode_dict["cfg"]["dataDir"]}/*')
        else:
            self.dnode_conn.exec_cmd("sudo rm -rf /var/lib/taos/*")

    def rmTaosCfg(self):
        logger.info(f'{self.dnode_ip}: removing taos.cfg')
        self.dnode_conn.exec_cmd("sudo rm -rf /etc/taos/taos.cfg")

    def modifyTaosCfg(self, firstEp=None):
        hostname = self.configHostname()
        if self.dnode_dict["modify_cfg"]:
            logger.info(f'{self.dnode_ip}: modify /etc/taos/taos.cfg')
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
        logger.info(f'{self.dnode_ip}: config dnode hostname')
        ori_hostname = self.dnode_conn.exec_cmd('hostname').strip()
        if "localhost" in str(ori_hostname).lower():
            self.dnode_conn.exec_cmd(f"sudo hostnamectl set-hostname {self.dnode_name}")
            return self.dnode_name
        return ori_hostname

    def hostsIsExist(self, ip, hostname):
        host_count = int(self.dnode_conn.exec_cmd(f'grep "^{ip}.*.{hostname}" /etc/hosts | wc -l'))
        if host_count > 0:
            logger.info(f'{self.dnode_ip}: check /etc/hosts: {ip} {hostname} existed')
            return True
        else:
            logger.info(f'{self.dnode_ip}: check /etc/hosts: {ip} {hostname} not exist')
            return False

    def configHosts(self, ip, hostname):
        if not self.hostsIsExist(ip, hostname):
            logger.info(f'{self.dnode_ip}: config dnode /etc/hosts: {ip} {hostname}')
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

    def downloadNodeExporter(self):
        logger.info(f'{self.dnode_ip}: downloading node_exporter from {config["prometheus"]["node_exporter_addr"]}')
        tar_file_name = config["prometheus"]["node_exporter_addr"].split("/")[-1]
        if not bool(int(self.dnode_conn.exec_cmd(f'[ -e ~/{tar_file_name} ] && echo 1 || echo 0'))):
            self.dnode_conn.exec_cmd(f'wget -P ~ {config["prometheus"]["node_exporter_addr"]}')

    def configNodeExporterService(self):
        logger.info(f'{self.dnode_ip}: configing /lib/systemd/system/node_exporter.service')
        if not bool(int(self.dnode_conn.exec_cmd(f'[ -e /lib/systemd/system/node_exporter.service ] && echo 1 || echo 0'))):
            self.dnode_conn.exec_cmd(f'sudo echo -e [Service]\n\
                                    User=prometheus\n\
                                    Group=prometheus\n\
                                    ExecStart=/usr/local/bin/node_exporter\n\
                                    [Install]\n\
                                    WantedBy=multi-user.target\n\
                                    [Unit]\n\
                                    Description=node_exporter\n\
                                    After=network.target \
                                    >> /lib/systemd/system/node_exporter.service')

    def killNodeExporter(self):
        logger.info(f'{self.dnode_ip}: killing node_exporter')
        self.dnode_conn.exec_cmd("ps -ef | grep -w node_exporter | grep -v grep | awk \'{print $2}\' | sudo xargs kill -9")

    def deployNodeExporter(self):
        logger.info(f'{self.dnode_ip}: deploying node_exporter')
        self.killNodeExporter()
        self.downloadNodeExporter()
        tar_file_name = config["prometheus"]["node_exporter_addr"].split("/")[-1]
        tar_file_dir = tar_file_name.replace(".tar.gz", "")
        self.dnode_conn.exec_cmd(f'cd ~ && tar -xvf {tar_file_name} && cd {tar_file_dir} && cp -rf node_exporter /usr/local/bin')
        self.configNodeExporterService()
        self.dnode_conn.exec_cmd('sudo groupadd -r prometheus')
        self.dnode_conn.exec_cmd('sudo useradd -r -g prometheus -s /sbin/nologin -M -c "prometheus Daemons" prometheus')
        self.dnode_conn.exec_cmd('systemctl start node_exporter && systemctl enable node_exporter && systemctl status node_exporter')
        
    def downloadProcessExporter(self):
        tar_file_name = config["prometheus"]["process_exporter_addr"].split("/")[-1]
        logger.info(f'{self.dnode_ip}: downloading process_exporter from {config["prometheus"]["process_exporter_addr"]}')
        if not bool(int(self.dnode_conn.exec_cmd(f'[ -e ~/{tar_file_name} ] && echo 1 || echo 0'))):
            self.dnode_conn.exec_cmd(f'wget -P ~ {config["prometheus"]["process_exporter_addr"]}')

    def killProcessExporter(self):
        logger.info(f'{self.dnode_ip}: killing process_exporter')
        self.dnode_conn.exec_cmd("ps -ef | grep -w process_exporter | grep -v grep | awk \'{print $2}\' | sudo xargs kill -9")

    def uploadProcessExporterYml(self, process_list):
        logger.info(f'{self.dnode_ip}: generating process_exporter yml')
        sub_list = list()
        for process in process_list:
            sub_list.append({'name':'{{.Comm}}', 'cmdline': [process]})
        djson = {'process_names': sub_list}
        dstr=json.dumps(djson)
        dyml=yaml.load(dstr, Loader=yaml.FullLoader)
        stream = open('process_name.yml', 'w')
        yaml.safe_dump(dyml, stream, default_flow_style=False)
        self.dnode_conn.upload_file(self.home_dir, 'process_name.yml')

    def deployProcessExporter(self, process_list):
        logger.info(f'{self.dnode_ip}: deploying process_exporter')
        self.killProcessExporter()
        self.downloadProcessExporter()
        self.uploadProcessExporterYml(process_list)
        tar_file_name = config["prometheus"]["process_exporter_addr"].split("/")[-1]
        tar_file_dir = tar_file_name.replace(".tar.gz", "")
        self.dnode_conn.exec_cmd(f'cd ~ && tar -xvf {tar_file_name} && mv -f ~/process_name.yml ~/{tar_file_dir}')
        self.dnode_conn.exec_cmd(f'screen -d -m ~/{tar_file_dir}/process-exporter --config.path ~/{tar_file_dir}/process_name.yml')

    def deployTaosd(self, firstEp=None, deploy_type="taosd"):
        '''
            deploy_type = taosd/taosadapter
        '''
        self.dnode_conn.upload_file(self.home_dir, self.install_package)
        if config["clean_env"]:
            self.rmTaosCfg()
            self.rmTaosdLog()
            self.rmTaosdData()
        package_name = self.install_package.split("/")[-1]
        package_dir = '-'.join(package_name.split("-", 3)[0:3])
        self.stopTaosd()
        self.killTaosd()
        logger.info(f'{self.dnode_ip}: installing taosd')
        logger.info(self.dnode_conn.exec_cmd(f'cd {self.home_dir} && tar -xvf {self.home_dir}/{package_name} && cd {package_dir} && yes|./install.sh'))
        self.modifyTaosCfg(firstEp)
        if deploy_type == "taosd":
            self.startTaosd()
        elif deploy_type == "taosadapter":
            self.startTaosadapter()
        if self.checkStatus(deploy_type):
            logger.success(f'{self.dnode_ip}: {deploy_type} deploy success')
        else:
            logger.error(f'{self.dnode_ip}: {deploy_type} deploy failed, please check by manual')
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

    def installDnodesPackage(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].installPackage()

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

if __name__ == '__main__':
    deploy = Dnodes()
    deploy.deployNodes()
    
