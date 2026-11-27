#!/bin/bash

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

sed -i 's/TaosadapterIp/'$TaosadapterIp'/g;s/TaosadapterPort/'$TaosadapterPort'/g;' /root/statsd/config.js
nohup node /root/statsd/stats.js /root/statsd/config.js &
sleep 10
for i in `seq 1 100`;
do
	echo "${HOSTNAME}.count${i}:55|c" | nc -w 1 -u 127.0.0.1 8125
done
tail -f /dev/null
