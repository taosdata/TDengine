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

"""perform the build release package and install and then test the restful service"""

import unittest


class ForecastTest(unittest.TestCase):

    def test_release(self):
        """test the package"""
        pass

        # print("build install package")
        # os.system("../../script/release.sh")
        # print("build completed")
        #
        # self.assertEqual(os.path.exists("../../release/TDengine-enterprise-anode-1.0.0.tar.gz"), 1)

    def test_install(self):
        """test"""
        pass

        # print("start to install package")
        # os.system("tar zxvf ../../release/TDengine-enterprise-anode-1.0.0.tar.gz")
        # os.chdir("../../release/TDengine-enterprise-anode-1.0.0/")
        #
        # os.system("./install.sh")
