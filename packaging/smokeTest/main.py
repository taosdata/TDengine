# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

import pytest

# python3 -m pytest test_server.py -v --html=/var/www/html/report.html --json-report --json-report-file="/var/www/html/report.json" --timeout=60

# pytest.main(["-s", "-v"])
import pytest

import subprocess


# define cmd function




def main():
    pytest.main(['--html=report.html'])


if __name__ == '__main__':
    main()
