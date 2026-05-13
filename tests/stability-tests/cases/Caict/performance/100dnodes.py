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

from taostest.util.common import TDCom
from taostest import TDCase
from taostest.components.container import Container
from taostest.util.remote import Remote
import time
import threading

class Dnodes_100(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.container = Container(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.container_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "docker"
        )
        self.dbname = "test"
        self.stbname = "stb"
        self.ctbname = "ctb"
        self.time_out = 300
        self.host_setting_dict = self.container.gen_host_setting_dict(self.container_setting, self.container_setting["dnode_count"])
        self.start_ts = time.time()
        self.end_ts = time.time()

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def start_dnodes(self):
        self._remote._logger.info('==========  starting dnodes ==========')
        self._remote._logger.info('creating docker net-bridge')
        self.container.create_docker_net(self.container_setting)
        run_container_tlist = list()
        config_container_tlist = list()
        run_taosd_tlist = list()
        add_dnodes_tlist = list()
        config_bashrc_tlist = list()
        for host, ip in self.host_setting_dict.items():
            host_info = host
            run_container_tlist.append(threading.Thread(target=self.container.run_container,args=(self.container_setting, host, host, ip)))
            config_container_tlist.append(threading.Thread(target=self.container.config_container,args=(self.container_setting, host, self.host_setting_dict)))
            run_taosd_tlist.append(threading.Thread(target=self.container.run_container_taosd,args=(self.container_setting, host)))
            add_dnodes_tlist.append(threading.Thread(target=self.container.add_dnodes,args=(self.container_setting, host, f"{host}:6030")))
            # config_bashrc_tlist.append(threading.Thread(target=self.container.config_bashrc,args=(self.container_setting, host)))
          # self.container.run_container(self.container_setting, host, host, ip)
          # self.container.config_container(self.container_setting, host, host_setting_dict)
          # self.container.run_container_taosd(self.container_setting, host)
          # self.container.add_dnodes(self.container_setting, host, f"{host}:6030")
        self._remote._logger.info('running containers')
        self.tdCom.multi_thread_run(run_container_tlist)
        self._remote._logger.info('configuring containers')
        self.tdCom.multi_thread_run(config_container_tlist)
        self._remote._logger.info('running taosd')
        self.tdCom.multi_thread_run(run_taosd_tlist)
        self._remote._logger.info('adding dnodes to cluster')
        self.tdCom.multi_thread_run(add_dnodes_tlist)
        # self.tdCom.multi_thread_run(config_bashrc_tlist)
        res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"show dnodes\"\''])
        ready_flag = 0

        while f'{self.container_setting["dnode_count"]} row(s) in set' not in res or 'offline' in res:
            res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"show dnodes\"\''])
            if ready_flag < self.time_out:
                ready_flag += 5
                time.sleep(5)
            else:
                return False
        self.tdSql.checkIn(f'{self.container_setting["dnode_count"]} row(s) in set', res)
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"create database {self.dbname};\"\''])
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"create stable {self.dbname}.{self.stbname} (ts timestamp, c1 int) tags (t1 int);\"\''])
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"create table {self.dbname}.{self.ctbname} using {self.dbname}.{self.stbname} tags (1);\"\''])
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"insert into  {self.dbname}.{self.ctbname} values (now, 1);\"\''])
        res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"select * from {self.dbname}.{self.stbname};\"\''])
        self.tdSql.checkIn(f'1 row(s) in set', res)
        self._remote._logger.info(f'start {self.container_setting["dnode_count"]} dnodes cluster successful')

    def restart_dnodes(self):
        self._remote._logger.info('==========  restarting dnodes ==========')
        restart_container_tlist = list()
        run_taosd_tlist = list()
        for host, _ in self.host_setting_dict.items():
            host_info = host
            restart_container_tlist.append(threading.Thread(target=self.container.restart_container,args=(self.container_setting, host)))
            run_taosd_tlist.append(threading.Thread(target=self.container.run_container_taosd,args=(self.container_setting, host)))
        self._remote._logger.info('restarting containers')
        self.tdCom.multi_thread_run(restart_container_tlist)
        self.start_ts = time.time()
        self._remote._logger.info('restarting cluster')
        self.tdCom.multi_thread_run(run_taosd_tlist)
        res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"select distinct(status) from information_schema.ins_dnodes;\"\''])
        ready_flag = 0

        while f'Query OK, 1 row(s) in set' not in res or "ready" not in res:
            res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"select distinct(status) from information_schema.ins_dnodes;\"\''])
            if ready_flag < self.time_out:
                ready_flag += 1
                time.sleep(1)
            else:
                return False
        self.tdSql.checkEqual("Query OK, 1 row(s) in set" in res and "ready" in res, True)
        self.end_ts = time.time()
        self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"insert into  {self.dbname}.{self.ctbname} values (now, 1);\"\''])
        res = self._remote.cmd(self.container_setting["fqdn"][0], [f'docker exec -ti {host_info} sh -c \'taos -s \"select * from {self.dbname}.{self.stbname};\"\''])
        self.tdSql.checkIn(f'2 row(s) in set', res)
        self.tdSql.checkEqual(self.end_ts-self.start_ts <= 100, True)
        self._remote._logger.info(f'restart {self.container_setting["dnode_count"]} dnodes cluster use {self.end_ts-self.start_ts}s')

    def run(self):
        self.start_dnodes()
        self.restart_dnodes()