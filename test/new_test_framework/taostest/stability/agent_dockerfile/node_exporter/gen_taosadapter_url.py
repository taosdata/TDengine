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

import sys
## eg: python3 gen_taosadapter_url.py 172.26.10.87 10000:10050

#TaosadapterIp = "172.26.10.87"
TaosadapterIp = sys.argv[1]
#TaosadapterPort = "10000:10050"
TaosadapterPort = sys.argv[2]
start_port = int(TaosadapterPort.split(":")[0])
end_port = int(TaosadapterPort.split(":")[1])
urls_list = []
for port in range(start_port, end_port+1):
	urls_list.append(f"http://{TaosadapterIp}:{port}")
print(urls_list)
	

