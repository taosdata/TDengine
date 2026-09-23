


# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from taostest.util.remote import Remote

class GetJson:
    def __init__(self, logger, run_log_dir,env_setting=None):
        self.logger = logger
        self.run_log_dir = run_log_dir
        self.env_setting = env_setting
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
        
    def get_vnode_json(self,db_vnode_kv_dict):
        remote = Remote(self.logger)
        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"/vnode/vnode{db_vnode_kv_dict[0][0]}"
        remote.get(self.fqdn,f'{self.vnode_dir}/vnode.json',f'{self.run_log_dir}/vnode.json')
        file = open(f'{self.run_log_dir}/vnode.json')
        return file