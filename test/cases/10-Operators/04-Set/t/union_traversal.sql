# keep_left
use union_perf_repro_336;
select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts \G;
explain verbose true select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts \G;

# keep_left_multiple_levels
use union_perf_repro_336;
select value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u) v order by 1 \G;
explain verbose true select value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u) v order by 1 \G;

# keep_right
use union_perf_repro_336;
select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as value from host_linux_1m_net_metrics) u order by ts \G;
explain verbose true select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as value from host_linux_1m_net_metrics) u order by ts \G;

# keep_right_multiple_levels
use union_perf_repro_336;
select value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as value from host_linux_1m_net_metrics) u) v order by 1 \G;
explain verbose true select value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as value from host_linux_1m_net_metrics) u) v order by 1 \G;

# join_left
use union_perf_repro_336;
select l.ts, l.value as left_value, r.value as right_value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts) l join (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics) v order by ts) r on l.ts=r.ts order by l.ts, l.value, r.value \G;
explain verbose true select l.ts, l.value as left_value, r.value as right_value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts) l join (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics) v order by ts) r on l.ts=r.ts order by l.ts, l.value, r.value \G;

# join_right
use union_perf_repro_336;
select l.ts, l.value as left_value, r.value as right_value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics) u order by ts) l join (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) v order by ts) r on l.ts=r.ts order by l.ts, l.value, r.value \G;
explain verbose true select l.ts, l.value as left_value, r.value as right_value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics) u order by ts) l join (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) v order by ts) r on l.ts=r.ts order by l.ts, l.value, r.value \G;

# join_both
use union_perf_repro_336;
select l.ts, l.value as left_value, r.value as right_value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts) l join (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) v order by ts) r on l.ts=r.ts order by l.ts, l.value, r.value \G;
explain verbose true select l.ts, l.value as left_value, r.value as right_value from (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts) l join (select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) v order by ts) r on l.ts=r.ts order by l.ts, l.value, r.value \G;

# all_empty
use union_perf_repro_336;
select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u \G;
explain verbose true select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u \G;

# nested_aggregate
use union_perf_repro_336;
select * from ( \
            select to_char(t.f_ts, 'YYYY-MM-DD HH24:MI') as f_ax_0, \
                   t.f_if as f_ax_1, round(cast(avg(cast(t.f_err as double)) as double), 8) as f_ay_0 \
            from (select bucket as f_ts, if_name as f_if, `出站错误包数` as f_err \
                  from (SELECT _wstart AS bucket, \
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
INTERVAL(1d) FILL(NONE)) u) t \
            group by to_char(t.f_ts, 'YYYY-MM-DD HH24:MI'), t.f_if \
        ) result order by f_ax_0, f_ax_1 \G;
explain verbose true select * from ( \
            select to_char(t.f_ts, 'YYYY-MM-DD HH24:MI') as f_ax_0, \
                   t.f_if as f_ax_1, round(cast(avg(cast(t.f_err as double)) as double), 8) as f_ay_0 \
            from (select bucket as f_ts, if_name as f_if, `出站错误包数` as f_err \
                  from (SELECT _wstart AS bucket, \
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
INTERVAL(1d) FILL(NONE)) u) t \
            group by to_char(t.f_ts, 'YYYY-MM-DD HH24:MI'), t.f_if \
        ) result order by f_ax_0, f_ax_1 \G;

# sort_keep_left
use union_perf_repro_336;
select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts, value \G;
explain verbose true select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts, value \G;

# sort_keep_right
use union_perf_repro_336;
select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as value from host_linux_1m_net_metrics) u order by ts, value \G;
explain verbose true select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as value from host_linux_1m_net_metrics) u order by ts, value \G;

# sort_value
use union_perf_repro_336;
select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by value, ts \G;
explain verbose true select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by value, ts \G;

# sort_value_only
use union_perf_repro_336;
select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by value \G;
explain verbose true select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by value \G;

# sort_surviving_set
use union_perf_repro_336;
select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts, value \G;
explain verbose true select * from (select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics union all select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0) u order by ts, value \G;

# sort_different_alias
use union_perf_repro_336;
select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as other from host_linux_1m_net_metrics) u order by ts, value \G;
explain verbose true select * from (select ts, traffic_in as value from host_linux_1m_net_metrics where 1=0 union all select ts, traffic_in as other from host_linux_1m_net_metrics) u order by ts, value \G;
