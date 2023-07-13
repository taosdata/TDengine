###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import time
import threading
from fabric2 import Connection
import os
import platform
import sys

class Common:
    def __init__(self):
        pass
    def multi_thread_run(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    def checkIn(self, contain_elm, elm):
        if contain_elm in elm:
            print(f"check in success, {contain_elm} in {elm}")
            return True
        else:
            raise AssertionError(f"checkIn error, {contain_elm} not in {elm}")

    def checkEqual(self, elm, expect_elm):
        if elm == expect_elm:
            print(f"checkEqual success, elm={elm} expect_elm={expect_elm}")
            return True
        else:
            raise AssertionError(f"checkEqual error, elm={elm} expect_elm={expect_elm}")

class Remote:
    def __init__(self):
        self._local_host = platform.node()

    def cmd(self, host, cmd_list, password=""):
        if isinstance(cmd_list, list):
            cmd_line = " && ".join(cmd_list)
        else:
            cmd_line = cmd_list
        print(cmd_line)
        if host == self._local_host:
            return os.popen(cmd_line).read().strip()
        try:
            with Connection(host, user="root", connect_kwargs={"password": password}) as c:
                result = c.run(cmd_line, warn=True)
                return result.stdout.strip()
        except Exception as e:
            print(f"Exception occur---{e}")
            return None

class Container:
    def __init__(self, remote: Remote):
        self.remote = remote

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


class XTYTest:
    def __init__(self, container_config):
        self.common = Common()
        self._remote = Remote()
        self.container = Container(self._remote)
        self.container_setting = container_config
        self.dbname = "test"
        self.stbname = "stb"
        self.ctbname = "ctb"
        self.time_out = 300
        self.host_setting_dict = self.container.gen_host_setting_dict(self.container_setting, self.container_setting["dnode_count"])
        self.start_ts = time.time()
        self.end_ts = time.time()

    def start_container_dnodes(self):
        print('==========  starting dnodes ==========')
        print('==========  creating docker net-bridge ==========')
        self.container.create_docker_net(self.container_setting)
        run_container_tlist = list()
        config_container_tlist = list()
        run_taosd_tlist = list()
        add_dnodes_tlist = list()
        for host, ip in self.host_setting_dict.items():
            host_info = host
            run_container_tlist.append(threading.Thread(target=self.container.run_container,args=(self.container_setting, host, host, ip)))
            config_container_tlist.append(threading.Thread(target=self.container.config_container,args=(self.container_setting, host, self.host_setting_dict)))
            run_taosd_tlist.append(threading.Thread(target=self.container.run_container_taosd,args=(self.container_setting, host)))
            add_dnodes_tlist.append(threading.Thread(target=self.container.add_dnodes,args=(self.container_setting, host, f"{host}:6030")))
        print('==========  running containers ==========')
        self.common.multi_thread_run(run_container_tlist)
        print('==========  configuring containers ==========')
        self.common.multi_thread_run(config_container_tlist)
        print('==========  running taosd ==========')
        self.common.multi_thread_run(run_taosd_tlist)
        print('==========  adding dnodes to cluster ==========')
        self.common.multi_thread_run(add_dnodes_tlist)
        # self.common.multi_thread_run(config_bashrc_tlist)
        res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"show dnodes\"\''])
        ready_flag = 0

        while f'{self.container_setting["dnode_count"]} row(s) in set' not in res or 'offline' in res:
            res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"show dnodes\"\''])
            if ready_flag < self.time_out:
                ready_flag += 5
                time.sleep(5)
            else:
                return False
        self.common.checkIn(f'{self.container_setting["dnode_count"]} row(s) in set', res)
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"create database {self.dbname};\"\''])
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"create stable {self.dbname}.{self.stbname} (ts timestamp, c1 int) tags (t1 int);\"\''])
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"create table {self.dbname}.{self.ctbname} using {self.dbname}.{self.stbname} tags (1);\"\''])
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"insert into  {self.dbname}.{self.ctbname} values (now, 1);\"\''])
        res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"select * from {self.dbname}.{self.stbname};\"\''])
        self.common.checkIn(f'1 row(s) in set', res)
        print(f'\033[1;32m********  start {self.container_setting["dnode_count"]} dnodes cluster successful ********\033[0m')

    def restart_container_dnodes(self):
        print('==========  restarting dnodes ==========')
        restart_container_tlist = list()
        run_taosd_tlist = list()
        for host, _ in self.host_setting_dict.items():
            host_info = host
            restart_container_tlist.append(threading.Thread(target=self.container.restart_container,args=(self.container_setting, host)))
            run_taosd_tlist.append(threading.Thread(target=self.container.run_container_taosd,args=(self.container_setting, host)))
        print('==========  restarting containers ==========')
        self.common.multi_thread_run(restart_container_tlist)
        self.start_ts = time.time()
        print('==========  restarting cluster ==========')
        self.common.multi_thread_run(run_taosd_tlist)
        res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"select distinct(status) from information_schema.ins_dnodes;\"\''])
        ready_flag = 0

        while f'Query OK, 1 row(s) in set' not in res or "ready" not in res:
            res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"select distinct(status) from information_schema.ins_dnodes;\"\''])
            if ready_flag < self.time_out:
                ready_flag += 1
                time.sleep(1)
            else:
                return False
        self.common.checkEqual("Query OK, 1 row(s) in set" in res and "ready" in res, True)
        self.end_ts = time.time()
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"insert into  {self.dbname}.{self.ctbname} values (now, 1);\"\''])
        res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"select * from {self.dbname}.{self.stbname};\"\''])
        self.common.checkIn(f'2 row(s) in set', res)
        print('==========  checking restart time ==========')
        self.common.checkEqual(self.end_ts-self.start_ts <= 100, True)
        print(f'\033[1;32m********  restart {self.container_setting["dnode_count"]} dnodes cluster use {self.end_ts-self.start_ts}s ********\033[0m')

if  __name__ == '__main__':
    container_config = {"fqdn": ["u1-60"], "net_name":"taostest_net", "subnet": "172.12.0.1/16", "image": "tdengine/tdengine", "dnode_count": 100}
    XTYTest = XTYTest(container_config)
    for argv in sys.argv[1:]:
        if argv in dir(XTYTest):
            exec(f'XTYTest.{argv}()')
    # XTYTest.start_container_dnodes()
    # XTYTest.restart_container_dnodes()