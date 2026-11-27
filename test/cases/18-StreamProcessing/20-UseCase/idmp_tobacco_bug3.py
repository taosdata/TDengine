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

from test_idmp_tobacco import IdmpScene


class TestIdmpTobaccoBug3:

    def test_idmp_tobacco_bug3(self):
        """IDMP tobacco scene

        1. Initialize IDMP tobacco scene
        2. Run stream processing job
        3. Verify results
        

        Since: v3.3.7.0

        Labels: common,ci,skip

        Jira:
            - https://jira.taosdata.com:18080/browse/TD-36699

        History:
            - 2025-7-18 zyyang90 Created
        """
        tobac = IdmpScene()
        tobac.init(
            "tobacco",
            "idmp_sample_tobacco",
            "idmp",
            "cases/41-StreamProcessing/20-UseCase/tobacco_data/idmp_sample_tobacco",
            "cases/41-StreamProcessing/20-UseCase/tobacco_data/idmp/vstb.sql",
            "cases/41-StreamProcessing/20-UseCase/tobacco_data/idmp/vtb.sql",
            "cases/41-StreamProcessing/20-UseCase/tobacco_data/idmp/stream.json",
        )
        tobac.stream_ids = [3]
        tobac.assert_retry = 60
        tobac.run()
