import sys
import json
sys.path.append("../../")
from config.env_init import *
from src.util.RemoteModule import RemoteModule
from src.common.dnodes import Dnodes, Dnode

class Monitor:
    def __init__(self):
        self.monitor_ip = config["prometheus"]["ip"]
        self.monitor_port = config["prometheus"]["port"]
        self.monitor_username = config["prometheus"]["username"]
        self.monitor_password = config["prometheus"]["password"]
        self.monitor_conn = RemoteModule(self.monitor_ip, self.monitor_port, self.monitor_username, self.monitor_password)
        self.dnodes = list()
        index = 1
        for key in config:
            if "taosd_dnode" in str(key):
                self.dnodes.append(Dnode(index, config[key]["ip"], config[key]["port"], config[key]["username"], config[key]["password"]))
                index += 1

        if self.monitor_username == "root":
            self.home_dir = "/root"
        else:
            self.home_dir = f"/home/{self.monitor_username}"

    def installDnodesPackage(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].installPackage()

    def deployAllNodeExporters(self):
        for index in range(len(self.dnodes)):
            self.dnodes[index].deployNodeExporter()

    def deployAllProcessExporters(self):
        for index in range(len(self.dnodes)):
            if index == 0:
                self.dnodes[index].deployProcessExporter(['taosd', 'taosadapter'])
            else:
                if config['taosd_cluster'] and config['taosadapter_separate_deploy']:
                    self.dnodes[index].deployProcessExporter(['taosd', 'taosadapter'])
                elif config['taosd_cluster'] and not config['taosadapter_separate_deploy']:
                    self.dnodes[index].deployProcessExporter(['taosd'])
                elif not config['taosd_cluster'] and config['taosadapter_separate_deploy']:
                    self.dnodes[index].deployProcessExporter(['taosadapter'])
                else:
                    pass

    def downloadPrometheus(self):
        logger.info(f'{self.monitor_ip}: downloading prometheus from {config["prometheus"]["prometheus_addr"]}')
        tar_file_name = config["prometheus"]["prometheus_addr"].split("/")[-1]
        if not bool(int(self.monitor_conn.exec_cmd(f'[ -e ~/{tar_file_name} ] && echo 1 || echo 0'))):
            self.monitor_conn.exec_cmd(f'wget -P ~ {config["prometheus"]["prometheus_addr"]}')

    def killPrometheus(self):
        logger.info(f'{self.monitor_ip}: killing prometheus')
        self.monitor_conn.exec_cmd("ps -ef | grep -w prometheus | grep -v grep | awk \'{print $2}\' | sudo xargs kill -9")

    def uploadPrometheusYml(self):
        logger.info('generating prometheus yml')
        scrape_configs = [{'job_name': 'prometheus', 'static_configs': [{'targets': ['localhost:9090']}]}]
        for index in range(len(self.dnodes)):
            if not config['taosd_cluster'] and not config['taosadapter_separate_deploy']:
                pass
            else:
                scrape_configs.append({'job_name': f'{self.dnodes[index].dnode_ip}_sys', 'static_configs': [{'targets': [f'{self.dnodes[index].dnode_ip}:9100'], 'labels': {'instance': f'{self.dnodes[index].dnode_ip}_sys'}}]})
                scrape_configs.append({'job_name': f'{self.dnodes[index].dnode_ip}', 'static_configs': [{'targets': [f'{self.dnodes[index].dnode_ip}:9256'], 'labels': {'instance': f'{self.dnodes[index].dnode_ip}'}}]})
        djson = {'global': {'scrape_interval': config["prometheus"]["scrape_interval"], 'evaluation_interval': config["prometheus"]["evaluation_interval"], 'scrape_timeout': config["prometheus"]["scrape_timeout"]}, 'alerting': {'alertmanagers': [{'static_configs': [{'targets': None}]}]}, 'rule_files': None, 'scrape_configs': scrape_configs}
        dstr=json.dumps(djson)
        dyml=yaml.load(dstr, Loader=yaml.FullLoader)
        stream = open('prometheus.yml', 'w')
        yaml.safe_dump(dyml, stream, default_flow_style=False)
        self.monitor_conn.upload_file(self.home_dir, 'prometheus.yml')

    def deployPrometheus(self):
        logger.info(f'{self.monitor_ip}: deploying prometheus')
        self.installDnodesPackage()
        self.killPrometheus()
        self.downloadPrometheus()
        self.uploadPrometheusYml()
        tar_file_name = config["prometheus"]["prometheus_addr"].split("/")[-1]
        tar_file_dir = tar_file_name.replace(".tar.gz", "")
        self.monitor_conn.exec_cmd(f'cd ~ && tar -xvf {tar_file_name} && mv ~/prometheus.yml ~/{tar_file_dir}')
        self.monitor_conn.exec_cmd(f'screen -d -m ~/{tar_file_dir}/prometheus --config.file={self.home_dir}/{tar_file_dir}/prometheus.yml')

    def installGrafana(self):
        logger.info(f'{self.monitor_ip}: installing grafana')
        if bool(int(self.monitor_conn.exec_cmd(f'cat /etc/os-release | grep ubuntu >> /dev/null && echo 1 || echo 0'))):
            if not bool(int(self.monitor_conn.exec_cmd(f'sudo dpkg -s grafana >> /dev/null && echo 1 || echo 0'))):
                self.monitor_conn.exec_cmd('sudo apt-get install -y apt-transport-https')
                self.monitor_conn.exec_cmd('sudo apt-get install -y software-properties-common wget')
                self.monitor_conn.exec_cmd('wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -')
                self.monitor_conn.exec_cmd('echo "deb https://packages.grafana.com/oss/deb stable main" | sudo tee -a /etc/apt/sources.list.d/grafana.list')
                self.monitor_conn.exec_cmd('apt-get update')
                self.monitor_conn.exec_cmd('sudo apt-get -y install grafana')
        elif bool(int(self.monitor_conn.exec_cmd(f'cat /etc/os-release | grep centos >> /dev/null && echo 1 || echo 0'))):
            if not bool(int(self.monitor_conn.exec_cmd(f'sudo rpm -qa | grep grafana >> /dev/null && echo 1 || echo 0'))):
                self.monitor_conn.exec_cmd('rm -rf /etc/yum.repos.d/grafana.repo')
                self.monitor_conn.exec_cmd('sudo echo -e "[grafana]\nname=grafana\nbaseurl=https://packages.grafana.com/oss/rpm\nrepo_gpgcheck=1\nenabled=1\ngpgcheck=1\ngpgkey=https://packages.grafana.com/gpg.key\nsslverify=1\nsslcacert=/etc/pki/tls/certs/ca-bundle.crt" \
                                            >> /etc/yum.repos.d/grafana.repo')
                self.monitor_conn.exec_cmd('yum install -y grafana')
        else:
            pass

    def deployGrafana(self):
        self.installGrafana()
        self.monitor_conn.exec_cmd('systemctl daemon-reload')
        self.monitor_conn.exec_cmd('systemctl start grafana-server')
        self.monitor_conn.exec_cmd('systemctl enable grafana-server.service')
        self.monitor_conn.exec_cmd('systemctl status grafana-server')

if __name__ == '__main__':
    deploy = Dnodes()
    deploy.deployNodes()
    monitor = Monitor()
    monitor.deployAllNodeExporters()
    monitor.deployAllProcessExporters()
    monitor.deployPrometheus()
    monitor.deployGrafana()
