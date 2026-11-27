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

./collectd_docker/run_collectd.sh rm collectd_agent*
./icinga2_docker/run_icinga2.sh rm icinga2_agent*
./statsd_docker/run_statsd.sh rm statsd_agent*
./tcollector_docker/run_tcollector.sh rm tcollector_agent*
./telegraf_docker/run_telegraf.sh rm telegraf_agent*
./node_exporter_docker/run_node_exporter.sh rm node_exporter_agent*
