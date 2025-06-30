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
## taostest --setup=pocs/gyrx/demo_multi.yaml --case=customer_scenarios/gyrx/cluster_sync_test.py --keep

import os
from taostest.util.common import TDCom
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from taostest.util.taosx_util import TaosxUtil
import taos

class ClusterSyncTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.reserve_dnode_list = self.taosd_setting["spec"]["reserve_dnodes"]
        self.dbname = "functiontest"
        self.stbname = "stb"
        self.ctbname = "ctb"

        # Initialize TaosX for sync tasks
        taosx_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosx")
        taosadapter_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosadapter")

        if taosx_setting and taosadapter_setting:
            self.taosx_host = taosx_setting.get("fqdn", [])[0] if taosx_setting.get("fqdn") else "localhost"
            self.taosx_port = taosx_setting.get("spec", {}).get("port", 6050)
            self.taosadapter_host = taosadapter_setting.get("fqdn", [])[0] if taosadapter_setting.get("fqdn") else "localhost"
            self.taosadapter_port = taosadapter_setting.get("spec", {}).get("adapter_config", {}).get("port", 6041)

            self.taosx_util = TaosxUtil(
                taosx_host=self.taosx_host,
                taosx_port=self.taosx_port,
                taosadapter_host=self.taosadapter_host,
                taosadapter_port=self.taosadapter_port,
                logger=self._remote._logger
            )
        else:
            self.taosx_util = None

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def create_sync_task(self):
        """Create a sync task to replicate data to the target node"""
        if not self.taosx_util:
            self._remote._logger.warning("TaosX not configured, skipping sync task creation")
            return

        try:
            # Check TaosX service availability
            if not self.taosx_util.check_taosx_status():
                self._remote._logger.warning("TaosX service not available, skipping sync task")
                return

            # Get target node from reserve_dnode_list[0]
            target_endpoint = self.reserve_dnode_list[0]["endpoint"]

            # Get cluster ID automatically
            cluster_id = self.taosx_util.get_cluster_id()

            # Create sync task payload as specified
            sync_payload = {
                "labels": ["type::replication", f"cluster-id::{cluster_id}"],
                "to": f"taos://{target_endpoint}/{self.dbname}",
                "from": f"sync+http://127.0.0.1:6041/{self.dbname}?timeout=never"
            }

            self._remote._logger.info(f"Creating sync task with payload: {sync_payload}")

            # Create the sync task using TaosX API
            task_info = self.taosx_util.create_task(sync_payload)
            task_id = task_info.get("id")

            self._remote._logger.info(f"Sync task created successfully with ID: {task_id}")

            # Start the sync task
            if task_id:
                start_success = self.taosx_util.start_task(task_id)
                if start_success:
                    self._remote._logger.info(f"Sync task {task_id} started successfully")

                    # Get task metrics without waiting for completion (metrics may not be available immediately)
                    try:
                        import time
                        time.sleep(5)  # Give task a moment to initialize
                        metrics = self.taosx_util.get_task_metrics(task_id)
                        self._remote._logger.info(f"Sync task metrics: {metrics}")
                    except Exception as e:
                        self._remote._logger.info(f"Task metrics not available yet (task may still be initializing): {e}")

                else:
                    self._remote._logger.warning(f"Failed to start sync task {task_id}")

        except Exception as e:
            self._remote._logger.error(f"Failed to create sync task: {e}")
            raise

    def cleanup(self):
        pass

    def run(self):
        self.taosd.add_reserve_dnodes(self._tmp_dir, self.taosd_setting, self.reserve_dnode_list)
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdSql.execute(f'create database {self.dbname}')
        self.tdSql.execute(f'use {self.dbname}')
        self.tdSql.execute(f'create stable {self.stbname} (ts timestamp, v_int int) tags (t_int int);')
        self.tdSql.execute(f'insert into {self.ctbname} using {self.stbname} tags (1) values (now, 1);')
        self.tdSql.query(f'select * from {self.stbname};')
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        self.tdSql.checkEqual(self.tdSql.query_data[0][1], 1)

        # Create sync task to replicate data to target node
        self.create_sync_task()
        target_host = self.reserve_dnode_list[0]["endpoint"].split(":")[0]
        target_port = int(self.reserve_dnode_list[0]["endpoint"].split(":")[1])
        conn = taos.connect(host=target_host, port=target_port)
        conn.execute(f'use {self.dbname}')
        result = conn.query(f'select * from {self.stbname};')
        self.tdSql.checkEqual(len(result.fetch_all()), 1)
        conn.close()

