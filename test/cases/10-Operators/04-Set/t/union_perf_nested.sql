-- Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This program is free software: you can use, redistribute, and/or modify
-- it under the terms of the GNU Affero General Public License, version 3
-- or later ("AGPL"), as published by the Free Software Foundation.
--
-- This program is distributed in the hope that it will be useful, but WITHOUT
-- ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
-- FITNESS FOR A PARTICULAR PURPOSE.
--
-- You should have received a copy of the GNU Affero General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.

# nested_union_all_explain
use union_perf_repro_336;
explain verbose true SELECT * FROM (SELECT _wstart AS bucket, \
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`, \
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`, \
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`, \
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`, \
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`, \
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`, \
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`, \
       '3天' as type, \
       host_name, \
       host_group, \
       ip_address, \
       if_name \
FROM host_linux_1m_net_metrics \
WHERE ip_address = '10.12.180.40' \
  and ts >= 1788105600000 \
  AND ts < 1788191940000 \
AND timediff(1788191940000,1788105600000,1a) <= 259200000 \
partition by host_name,host_group,ip_address,if_name \
INTERVAL(1m) FILL(NONE) \
UNION ALL \
SELECT _wstart AS bucket, \
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`, \
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`, \
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`, \
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`, \
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`, \
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`, \
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`, \
       '3-7天' as type, \
       host_name, \
       host_group, \
       ip_address, \
       if_name \
FROM host_linux_1m_net_metrics \
WHERE ip_address = '10.12.180.40' \
  and ts >= 1788105600000 \
  AND ts < 1788191940000 \
AND timediff(1788191940000,1788105600000,1a) > 259200000 \
  AND timediff(1788191940000,1788105600000,1a) <= 604800000 \
partition by host_name,host_group,ip_address,if_name \
INTERVAL(2m) FILL(NONE) \
UNION ALL \
SELECT _wstart AS bucket, \
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`, \
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`, \
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`, \
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`, \
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`, \
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`, \
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`, \
       '7-15天' as type, \
       host_name, \
       host_group, \
       ip_address, \
       if_name \
FROM host_linux_1m_net_metrics \
WHERE ip_address = '10.12.180.40' \
  and ts >= 1788105600000 \
  AND ts < 1788191940000 \
AND timediff(1788191940000,1788105600000,1a) > 604800000 \
  AND timediff(1788191940000,1788105600000,1a) <= 1296000000 \
partition by host_name,host_group,ip_address,if_name \
INTERVAL(5m) FILL(NONE) \
UNION ALL \
SELECT _wstart AS bucket, \
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`, \
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`, \
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`, \
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`, \
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`, \
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`, \
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`, \
       '15-30天' as type, \
       host_name, \
       host_group, \
       ip_address, \
       if_name \
FROM host_linux_1m_net_metrics \
WHERE ip_address = '10.12.180.40' \
  and ts >= 1788105600000 \
  AND ts < 1788191940000 \
AND timediff(1788191940000,1788105600000,1a) > 1296000000 \
  AND timediff(1788191940000,1788105600000,1a) <= 2592000000 \
partition by host_name,host_group,ip_address,if_name \
INTERVAL(10m) FILL(NONE) \
UNION ALL \
SELECT _wstart AS bucket, \
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`, \
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`, \
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`, \
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`, \
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`, \
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`, \
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`, \
       '30-90天' as type, \
       host_name, \
       host_group, \
       ip_address, \
       if_name \
FROM host_linux_1m_net_metrics \
WHERE ip_address = '10.12.180.40' \
  and ts >= 1788105600000 \
  AND ts < 1788191940000 \
AND timediff(1788191940000,1788105600000,1a) > 2592000000 \
  AND timediff(1788191940000,1788105600000,1a) <= 7776000000 \
partition by host_name,host_group,ip_address,if_name \
INTERVAL(30m) FILL(NONE) \
UNION ALL \
SELECT _wstart AS bucket, \
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`, \
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`, \
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`, \
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`, \
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`, \
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`, \
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`, \
       '90天-180天' as type, \
       host_name, \
       host_group, \
       ip_address, \
       if_name \
FROM host_linux_1m_net_metrics \
WHERE ip_address = '10.12.180.40' \
  and ts >= 1788105600000 \
  AND ts < 1788191940000 \
AND timediff(1788191940000,1788105600000,1a) > 7776000000 \
  AND timediff(1788191940000,1788105600000,1a) <= 15552000000 \
partition by host_name,host_group,ip_address,if_name \
INTERVAL(1h) FILL(NONE) \
UNION ALL \
SELECT _wstart AS bucket, \
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`, \
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`, \
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`, \
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`, \
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`, \
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`, \
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`, \
       '180天-1年' as type, \
       host_name, \
       host_group, \
       ip_address, \
       if_name \
FROM host_linux_1m_net_metrics \
WHERE ip_address = '10.12.180.40' \
  and ts >= 1788105600000 \
  AND ts < 1788191940000 \
AND timediff(1788191940000,1788105600000,1a) > 15552000000 \
  AND timediff(1788191940000,1788105600000,1a) <= 31536000000 \
partition by host_name,host_group,ip_address,if_name \
INTERVAL(2h) FILL(NONE) \
UNION ALL \
SELECT _wstart AS bucket, \
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`, \
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`, \
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`, \
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`, \
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`, \
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`, \
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`, \
       '1年-3年' as type, \
       host_name, \
       host_group, \
       ip_address, \
       if_name \
FROM host_linux_1m_net_metrics \
WHERE ip_address = '10.12.180.40' \
  and ts >= 1788105600000 \
  AND ts < 1788191940000 \
AND timediff(1788191940000,1788105600000,1a) > 31536000000 \
  AND timediff(1788191940000,1788105600000,1a) <= 94608000000 \
partition by host_name,host_group,ip_address,if_name \
INTERVAL(5h) FILL(NONE) \
UNION ALL \
SELECT _wstart AS bucket, \
       AVG(CAST(traffic_in AS FLOAT)) AS `入站流量`, \
       AVG(CAST(traffic_out AS FLOAT)) AS `出站流量`, \
       AVG(CAST(packet_errors_in AS FLOAT)) AS `入站错误包数`, \
       AVG(CAST(packet_errors_out AS FLOAT)) AS `出站错误包数`, \
       AVG(CAST(packet_drops_in AS FLOAT)) AS `入站丢包数`, \
       AVG(CAST(packet_drops_out AS FLOAT)) AS `出站丢包数`, \
       AVG(CAST(link_status AS FLOAT)) AS `网卡链路状态`, \
       '3年以上' as type, \
       host_name, \
       host_group, \
       ip_address, \
       if_name \
FROM host_linux_1m_net_metrics \
WHERE ip_address = '10.12.180.40' \
  and ts >= 1788105600000 \
  AND ts < 1788191940000 \
AND timediff(1788191940000,1788105600000,1a) > 94608000000 \
partition by host_name,host_group,ip_address,if_name \
INTERVAL(1d) FILL(NONE)) t_a_0 \G;
