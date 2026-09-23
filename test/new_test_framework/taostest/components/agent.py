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

from typing import Dict
from ..util.remote import Remote
import os

class Agent:
    def __init__(self, remote: Remote):
        self._remote: Remote = remote
        self._fqdn: str = None
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.dockerfile_path = os.path.join(self.current_dir, "../stability/agent_dockerfile")
        self.build_items: list = list()
        self.remote_path = "/opt"

    def get_build_items_list(self, config: Dict):
        for key, _ in config["spec"].items():
            self.build_items.append(key)

    def put_dockerfiles(self):
        """
        upload dockerfiles to remote
        """
        self._remote.put(self._fqdn, self.dockerfile_path, self.remote_path)

    def build_images(self, config: Dict):
        """
        docker build images
        """
        self.get_build_items_list(config)
        self.put_dockerfiles()
        for build_item in self.build_items:
            self._remote.cmd(self._fqdn, [f'cd {self.remote_path}/agent_dockerfile/{build_item}', f'docker build -t "{build_item}:v1" .'])

    def rm_images(self, config: Dict):
        """
        docker rm images
        """
        self.get_build_items_list(config)
        for build_item in self.build_items:
            self._remote.cmd(self._fqdn, [f'docker rmi {build_item}:v1'])

    def setup(self, config: Dict):
        for fqdn in config["fqdn"]:
            self._fqdn = fqdn
            self.build_images(config)

    def destroy(self, config: Dict):
        for fqdn in config["fqdn"]:
            self._fqdn = fqdn
            self.rm_images(config)
