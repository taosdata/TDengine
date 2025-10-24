###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool


class TestTaosinspect:

    def check_column(self, result: list, column_name: str, stable_name: str):
        get_column = False

        for row in result:
            if column_name.lower() == row[0].lower():
                get_column = True
                break
        
        assert get_column == True, f"Cannot find {column_name} in {stable_name}"
                
    def run_system_table_schema_verification(self):        
        tdLog.info("===== Testing scenario: Verify definition of system tables for inspect tools =====")
        # 1. server_version
        tdSql.query("select server_version()")
        tdSql.checkRows(1)
        server_version = tdSql.getData(0, 0)

        # 2. information_schema.ins_stables
        result = tdSql.getResult("desc information_schema.ins_stables")
        self.check_column(result, "stable_name", "information_schema.ins_stables")
        self.check_column(result, "db_name", "information_schema.ins_stables")
        # tdSql.query("select * from information_schema.ins_stables where stable_name='taosd_dnodes_info' and db_name='log'")
        # tdSql.query("select db_name, stable_name from information_schema.ins_stables where db_name not in ('information_schema', 'performance_schema','audit','log') group by db_name, stable_name")
        # tdSql.query("select stable_name from information_schema.ins_stables where db_name='{db_name}'")

        # 3. log.taosd_dnodes_info
        # tdSql.query("select _wstart, _wend, count(*), dnode_id, TIMEDIFF(_wend, _wstart,1m) from log.taosd_dnodes_info where _ts > '{start_data.strftime('%Y-%m-%d')}' partition by dnode_id event_window start with cpu_system >{cpu_percent} end with cpu_system<{cpu_percent}")
        
        # 4. information_schema.ins_dnode_variables
        result = tdSql.getResult("desc information_schema.ins_dnode_variables")
        self.check_column(result, "name", "information_schema.ins_dnode_variables")
        self.check_column(result, "value", "information_schema.ins_dnode_variables")
        self.check_column(result, "scope", "information_schema.ins_dnode_variables")
        # tdSql.query("select name,`value`,scope from information_schema.ins_dnode_variables")

        # 5. information_schema.ins_dnodes
        result = tdSql.getResult("desc information_schema.ins_dnodes")
        self.check_column(result, "id", "information_schema.ins_dnodes")
        self.check_column(result, "endpoint", "information_schema.ins_dnodes")
        self.check_column(result, "vnodes", "information_schema.ins_dnodes")
        self.check_column(result, "status", "information_schema.ins_dnodes")
        self.check_column(result, "create_time", "information_schema.ins_dnodes")
        self.check_column(result, "reboot_time", "information_schema.ins_dnodes")
        # tdSql.query("select id, endpoint,`vnodes`, status, create_time, reboot_time from information_schema.ins_dnodes")

        # 6. information_schema.ins_mnodes
        result = tdSql.getResult("desc information_schema.ins_mnodes")
        self.check_column(result, "id", "information_schema.ins_mnodes")
        self.check_column(result, "endpoint", "information_schema.ins_mnodes")
        self.check_column(result, "role", "information_schema.ins_mnodes")
        self.check_column(result, "status", "information_schema.ins_mnodes")
        self.check_column(result, "create_time", "information_schema.ins_mnodes")
        self.check_column(result, "role_time", "information_schema.ins_mnodes")
        # tdSql.query("select id,endpoint,role,status,create_time,role_time from information_schema.ins_mnodes")

        # 7. information_schema.ins_vnodes
        result = tdSql.getResult("desc information_schema.ins_vnodes")
        self.check_column(result, "dnode_id", "information_schema.ins_vnodes")
        self.check_column(result, "vgroup_id", "information_schema.ins_vnodes")
        self.check_column(result, "db_name", "information_schema.ins_vnodes")
        self.check_column(result, "status", "information_schema.ins_vnodes")
        self.check_column(result, "start_time", "information_schema.ins_vnodes")
        self.check_column(result, "restored", "information_schema.ins_vnodes")
        self.check_column(result, "role_time", "information_schema.ins_vnodes")
        # tdSql.query("select dnode_id, vgroup_id,db_name,status,start_time,restored from information_schema.ins_vnodes order by dnode_id, vgroup_id")
        # tdSql.query("select dnode_id,count(*) from information_schema.ins_vnodes where db_name='{db_name}' and status='leader' group by dnode_id order by dnode_id")
        # tdSql.query("select dnode_id,count(*) from information_schema.ins_vnodes where db_name='{db_name}' and status='follower' group by dnode_id order by dnode_id")
        # tdSql.query("select vgroup_id, dnode_id, status, role_time, start_time, restored from information_schema.ins_vnodes where db_name='{db_name}' order by vgroup_id asc, status desc")

        # 8. information_schema.ins_users
        result = tdSql.getResult("desc information_schema.ins_users")
        self.check_column(result, "name", "information_schema.ins_users")
        self.check_column(result, "super", "information_schema.ins_users")
        self.check_column(result, "enable", "information_schema.ins_users")
        self.check_column(result, "sysinfo", "information_schema.ins_users")
        self.check_column(result, "create_time", "information_schema.ins_users")
        if server_version.startswith("3.2") or server_version.startswith("3.3"):
            self.check_column(result, "allowed_host", "information_schema.ins_users")
        # tdSql.query("select * from information_schema.ins_users")
        # tdSql.query("select name, super, `enable`, `sysinfo`, create_time, allowed_host from information_schema.ins_users")

        # 9. information_schema.ins_user_privileges
        result = tdSql.getResult("desc information_schema.ins_user_privileges")
        self.check_column(result, "user_name", "information_schema.ins_user_privileges")
        self.check_column(result, "privilege", "information_schema.ins_user_privileges")
        self.check_column(result, "db_name", "information_schema.ins_user_privileges")
        self.check_column(result, "table_name", "information_schema.ins_user_privileges")
        # tdSql.query("select user_name, privilege, db_name, table_name from information_schema.ins_user_privileges")

        # 10. information_schema.ins_grants
        if server_version.startswith("3.1"):
            result = tdSql.getResult("desc information_schema.ins_grants")
            # todo 
        if server_version.startswith("3.2") or server_version.startswith("3.3"):
            result = tdSql.getResult("desc information_schema.ins_grants_full")
            self.check_column(result, "display_name", "information_schema.ins_grants_full")
            self.check_column(result, "expire", "information_schema.ins_grants_full")
            self.check_column(result, "limits", "information_schema.ins_grants_full")
        # tdSql.query("select * from information_schema.ins_grants")   # 3.1
        # tdSql.query("select display_name,expire,limits from information_schema.ins_grants_full")   # 3.2+

        # 11. log.taos_slow_sql_detail
        # tdSql.query("select to_char(_wstart, 'yyyy-mm-dd'),count(*), min(query_time), max(query_time) from log.taos_slow_sql_detail where start_ts > '{start_data.strftime('%Y-%m-%d')}' and `type` =1 interval(1d)")
        
        # 12. information_schema.ins_databases
        result = tdSql.getResult("desc information_schema.ins_databases")
        self.check_column(result, "name", "information_schema.ins_databases")
        self.check_column(result, "replica", "information_schema.ins_databases")
        self.check_column(result, "create_time", "information_schema.ins_databases")
        # tdSql.query("select name from information_schema.ins_databases where name not in ('information_schema', 'performance_schema','audit','log') order by create_time  desc")
        # tdSql.query("select name, `replica` from information_schema.ins_databases where name not in ('information_schema', 'performance_schema') order by name")
        # tdSql.query("select name from information_schema.ins_databases where name not in ('information_schema', 'performance_schema','audit','log') order by create_time  desc")

        # 13. information_schema.ins_tables
        result = tdSql.getResult("desc information_schema.ins_tables")
        self.check_column(result, "stable_name", "information_schema.ins_tables")
        self.check_column(result, "db_name", "information_schema.ins_tables")
        self.check_column(result, "create_time", "information_schema.ins_tables")
        # tdSql.query("select count(*) as ctable_count from information_schema.ins_tables where stable_name is not null and db_name not in ('information_schema', 'performance_schema','audit','log')")
        # tdSql.query("select count(*) as ntable_count from information_schema.ins_tables where stable_name is null and db_name not in ('information_schema', 'performance_schema','audit','log')")
        # tdSql.query("select db_name,sum(columns-1) from information_schema.ins_tables group by db_name")
        # tdSql.query("select stable_name, count(*) as ctable_count from information_schema.ins_tables where db_name='{db_name}' and stable_name is not null group by stable_name order by stable_name")

        # 14. information_schema.ins_streams
        result = tdSql.getResult("desc information_schema.ins_streams")
        self.check_column(result, "stream_name", "information_schema.ins_streams")
        self.check_column(result, "db_name", "information_schema.ins_streams")
        self.check_column(result, "create_time", "information_schema.ins_streams")
        self.check_column(result, "stream_id", "information_schema.ins_streams")
        self.check_column(result, "sql", "information_schema.ins_streams")
        self.check_column(result, "status", "information_schema.ins_streams")
        self.check_column(result, "snodeLeader", "information_schema.ins_streams")
        self.check_column(result, "snodeReplica", "information_schema.ins_streams")
        self.check_column(result, "message", "information_schema.ins_streams")
        # tdSql.query("select count(*) as stream_count from information_schema.ins_streams")
        # tdSql.query("select stream_name, create_time, "--" as stream_id, sql, status, `watermark`, `trigger`, "--" as sink_quota from information_schema.ins_streams") # 3.1
        # tdSql.query("select stream_name, create_time, stream_id, sql, status, `watermark`, `trigger`, sink_quota from information_schema.ins_streams") # 3.2+

        # 15. information_schema.ins_topics
        result = tdSql.getResult("desc information_schema.ins_topics")
        self.check_column(result, "topic_name", "information_schema.ins_topics")
        self.check_column(result, "db_name", "information_schema.ins_topics")
        self.check_column(result, "create_time", "information_schema.ins_topics")
        self.check_column(result, "sql", "information_schema.ins_topics")
        self.check_column(result, "meta", "information_schema.ins_topics")
        self.check_column(result, "type", "information_schema.ins_topics")
        # tdSql.query("select count(*) as topic_count from information_schema.ins_topics")
        # tdSql.query("select topic_name, db_name, create_time, sql, `meta`, `type` from information_schema.ins_topics order by db_name, topic_name")

        # 16. information_schema.ins_subscriptions
        result = tdSql.getResult("desc information_schema.ins_subscriptions")
        self.check_column(result, "topic_name", "information_schema.ins_subscriptions")
        self.check_column(result, "consumer_group", "information_schema.ins_subscriptions")
        self.check_column(result, "vgroup_id", "information_schema.ins_subscriptions")
        self.check_column(result, "consumer_id", "information_schema.ins_subscriptions")
        self.check_column(result, "offset", "information_schema.ins_subscriptions")
        self.check_column(result, "rows", "information_schema.ins_subscriptions")
        # tdSql.query("select count(*) from information_schema.ins_subscriptions")
        # tdSql.query("select topic_name, consumer_group, vgroup_id, consumer_id, `offset`, `rows` from information_schema.ins_subscriptions order by topic_name")

        # 17. information_schema.ins_vgroups
        result = tdSql.getResult("desc information_schema.ins_vgroups")
        self.check_column(result, "db_name", "information_schema.ins_vgroups")
        self.check_column(result, "vgroup_id", "information_schema.ins_vgroups")
        self.check_column(result, "tables", "information_schema.ins_vgroups")
        self.check_column(result, "v1_dnode", "information_schema.ins_vgroups")
        self.check_column(result, "v1_status", "information_schema.ins_vgroups")
        self.check_column(result, "v2_dnode", "information_schema.ins_vgroups")
        self.check_column(result, "v2_status", "information_schema.ins_vgroups")
        self.check_column(result, "v3_dnode", "information_schema.ins_vgroups")
        self.check_column(result, "v3_status", "information_schema.ins_vgroups")
        # tdSql.query("select db_name, count(*) from information_schema.ins_vgroups group by db_name order by db_name")
        # tdSql.query("select v1_dnode,  count(*) as vg_count from information_schema.ins_vgroups group by v1_dnode order by v1_dnode")
        # tdSql.query("select vgroup_id, `tables`, v1_dnode, v1_status, v2_dnode, v2_status, v3_dnode, v3_status from information_schema.ins_vgroups where db_name='{db_name}'")
        
        # 18. information_schema.ins_stream_tasks
        result = tdSql.getResult("desc information_schema.ins_stream_tasks")
        self.check_column(result, "stream_name", "information_schema.ins_stream_tasks")
        self.check_column(result, "stream_id", "information_schema.ins_stream_tasks")
        self.check_column(result, "task_id", "information_schema.ins_stream_tasks")
        self.check_column(result, "type", "information_schema.ins_stream_tasks")
        self.check_column(result, "serious_id", "information_schema.ins_stream_tasks")
        self.check_column(result, "deploy_id", "information_schema.ins_stream_tasks")
        self.check_column(result, "node_type", "information_schema.ins_stream_tasks")
        self.check_column(result, "node_id", "information_schema.ins_stream_tasks")
        self.check_column(result, "task_idx", "information_schema.ins_stream_tasks")
        self.check_column(result, "status", "information_schema.ins_stream_tasks")
        self.check_column(result, "start_time", "information_schema.ins_stream_tasks")
        self.check_column(result, "last_update", "information_schema.ins_stream_tasks")
        self.check_column(result, "extra_info", "information_schema.ins_stream_tasks")
        self.check_column(result, "message", "information_schema.ins_stream_tasks")
        # tdSql.query("select stream_name,task_id,node_type,node_id,level,status from information_schema.ins_stream_tasks where stream_name='{stream[0]}'") # 3.1
        # tdSql.query("select stream_name,task_id,start_time,node_type,node_id,level,status,in_queue,process_total,process_throughput,out_total,out_throughput,info from information_schema.ins_stream_tasks where stream_name='{stream[0]}'")  # 3.2+

        tdLog.info("Verify definition of system tables for inspect tools is done")

    # Run tests
    def test_taosinspect(self):
        """Tool system tables inspect

        1. Check columns completeness on information_schema tables:
        2. information_schema.ins_stables
        3. information_schema.ins_dnode_variables
        4. information_schema.ins_dnodes
        5. information_schema.ins_mnodes
        6. information_schema.ins_vnodes
        7. information_schema.ins_users
        8. information_schema.ins_user_privileges
        9. information_schema.ins_grants
        10. information_schema.ins_databases
        11. information_schema.ins_tables
        12. information_schema.ins_streams
        13. information_schema.ins_topics
        14. information_schema.ins_subscriptions
        15. information_schema.ins_vgroups
        16. information_schema.ins_stream_tasks
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-24 Alex Duan Migrated from uncatalog/army/inspect-tools/test_taosinspect.py

        """
        tdLog.debug(f"Start to execute {__file__}")

        # Prepare test data
        self.run_system_table_schema_verification()

        tdLog.success(f"{__file__} successfully executed")

