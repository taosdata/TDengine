"""
container components
容器组件
"""
import os

from ..util.remote import Remote


class Container:
    """
    以下都可以封装的操作，根据需要封装
      build              Build or rebuild services
      config             Validate and view the Compose file
      create             Create services
      down               Stop and remove resources
      events             Receive real time events from containers
      exec               Execute a command in a running container
      help               Get help on a command
      images             List images
      kill               Kill containers
      logs               View output from containers
      pause              Pause services
      port               Print the public port for a port binding
      ps                 List containers
      pull               Pull service images
      push               Push service images
      restart            Restart services
      rm                 Remove stopped containers
      run                Run a one-off command
      scale              Set number of containers for a service
      start              Start services
      stop               Stop services
      top                Display the running processes
      unpause            Unpause services
      up                 Create and start containers
    """

    def __init__(self, remote: Remote):
        self.remote = remote
        self.remote_compose_root = ".taostest/compose"
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")

    def setup(self, config):
        """
        configure and up
        """
        host = config["fqdn"][0]
        compose_file_name = config["spec"]["compose_file"]
        compose_file = os.path.join(self.env_root, compose_file_name)
        suc = self.remote.put(host, compose_file, self.remote_compose_root)
        if suc:
            remote_compose_file = self.remote_compose_root + '/' + compose_file_name
            self.remote.cmd(host, [f"docker-compose -f {remote_compose_file} up -d"])

    def destroy(self, config):
        """
        delete compose file on remote machine
        """
        self.down(config)

    def reset(self, config):
        self.down(config)

    def start(self, config):
        host, remote_compose_file = self._get_host_and_compose_file(config)
        self.remote.cmd(host, [f"docker-compose -f {remote_compose_file} start"])

    def stop(self, config):
        host, remote_compose_file = self._get_host_and_compose_file(config)
        self.remote.cmd(host, [f"docker-compose -f {remote_compose_file} stop"])

    def up(self, config):
        """
        Create and start containers
        """
        host, remote_compose_file = self._get_host_and_compose_file(config)
        self.remote.cmd(host, [f"docker-compose -f {remote_compose_file} up"])

    def down(self, config):
        """
        Stop and remove resources
        """
        host, remote_compose_file = self._get_host_and_compose_file(config)
        self.remote.cmd(host, [f"docker-compose -f {remote_compose_file} down"])

    def _get_host_and_compose_file(self, config):
        host = config["fqdn"][0]
        compose_file_name = config["spec"]["compose_file"]
        remote_compose_file = self.remote_compose_root + '/' + compose_file_name
        return host, remote_compose_file

    def destroy_docker_net(self, config):
        self.remote.cmd(config["fqdn"][0], [f'docker network rm {config["net_name"]}'])

    def create_docker_net(self, config):
        self.destroy_docker_net(config)
        self.remote.cmd(config["fqdn"][0], [f'docker network create --subnet={config["subnet"]} {config["net_name"]}'])

    def pull_image(self, config):
        self.remote.cmd(config["fqdn"][0], [f'docker pull {config["image"]}'])

    def run_container(self, config, container_name, host, net_ip):
        self.stop_container(config, container_name)
        self.rm_container(config, container_name)
        self.remote.cmd(config["fqdn"][0], [f'docker run -itd --privileged=true --name {container_name} -h {host} --net {config["net_name"]} --ip {net_ip} {config["image"]} /bin/bash'])

    def start_container(self, config, container_name):
        self.remote.cmd(config["fqdn"][0], [f'docker start {container_name}'])

    def restart_container(self, config, container_name):
        self.remote.cmd(config["fqdn"][0], [f'docker restart {container_name}'])

    def stop_container(self, config, container_name):
        self.remote.cmd(config["fqdn"][0], [f'docker stop {container_name}'])

    def rm_container(self, config, container_name):
        self.remote.cmd(config["fqdn"][0], [f'docker rm {container_name}'])

    def gen_host_setting_dict(self, config, container_count):
        ip_prefix = ".".join(config["subnet"].split(".")[:3])
        ip_suffix = int(config["subnet"].split(".")[-1].split("/")[0])
        host_setting_dict = dict()
        for i in range(container_count):
            host_setting_dict[f'{config["net_name"]}_host_{i}'] = f'{ip_prefix}.{ip_suffix+i+1}'
        return host_setting_dict

    def config_container(self, config, container_name, host_setting_dict):
        ip_prefix = ".".join(config["subnet"].split(".")[:3])
        ip_suffix = int(config["subnet"].split(".")[-1].split("/")[0])
        for host, ip in host_setting_dict.items():
            if ip == f'{ip_prefix}.{ip_suffix+1}':
                firstEp = f'{host}:6030'
            self.remote.cmd(config["fqdn"][0], [f'docker exec -d {container_name} sh -c \'echo "{ip}\t{host}" >> /etc/hosts && sort -u /etc/hosts -o /etc/hosts && sed -i "s/# firstEp                   hostname:6030/firstEp {firstEp}/g" /etc/taos/taos.cfg && sed -i "s/# fqdn                      hostname/fqdn {container_name}/g" /etc/taos/taos.cfg\''])

    def config_bashrc(self, config, container_name):
        self.remote.cmd(config["fqdn"][0], [f'docker exec -d {container_name} sh -c \'echo "if [ \`ps -ef | grep taosd | grep -v grep | wc -l\` -eq 0 ];then nohup taosd -c /etc/taos >/dev/null 2>&1 & fi" >> ~/.bashrc\''])

    def run_container_taosd(self, config, container_name):
        self.remote.cmd(config["fqdn"][0], [f'docker exec -d {container_name} sh -c \'taosd\''])

    def add_dnodes(self, config, container_name, dnode_info):
        self.remote.cmd(config["fqdn"][0], [f'docker exec -d {container_name} sh -c \'taos -s \"create dnode \\"{dnode_info}\\"\"\''])
