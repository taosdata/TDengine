###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

# -*- coding: utf-8 -*-

import os
import json
from new_test_framework.utils import tdLog, tdSql


def _get_sql_rules_path():
    """Get absolute path to sql_rules.json for test (client reads this file)."""
    test_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(test_dir))))
    default_path = os.path.join(project_root, "sql_rules_blacklist.json")
    if os.path.exists(default_path):
        return default_path
    rules_path = os.path.join(test_dir, "sql_rules_blacklist.json")
    rules = {
        "version": "1.0",
        "rules": [
            {"ruleId": 1, "ruleName": "UNION_SELECT", "action": "DENY", "priority": "HIGH",
             "pattern": "union[[:space:]]+select", "enabled": True},
            {"ruleId": 2, "ruleName": "DROP_TABLE", "action": "DENY", "priority": "HIGH",
             "pattern": "drop[[:space:]]+table", "enabled": True},
        ],
    }
    with open(rules_path, "w") as f:
        json.dump(rules, f, indent=2)
    return rules_path


class TestSqlFirewall:
    """SQL Firewall blacklist tests per 行为说明.md.

    Config must be in clientCfg so sim writes to client's taos.cfg (psim/cfg).
    SQL security runs on the native client; use without -R (no REST).
    """

    _rules_path = _get_sql_rules_path()
    _client_cfg = {
        "sqlSecurity": 1,
        "sqlSecurityWhitelistMode": 2,
        "sqlSecurityStringCheck": 1,
        "sqlSecurityASTCheck": 1,
        "sqlSecurityRuleFile": _rules_path,
    }
    updatecfgDict = {"clientCfg": _client_cfg}

    @classmethod
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def union_select_denied(self):
        """Blacklist: UNION SELECT denied (string layer)."""
        tdLog.info("union_select_denied start")
        tdSql.prepare(dbname="db_fw", drop=True)
        tdSql.execute("use db_fw")
        tdSql.execute("create table t1 (ts timestamp, c1 int)")
        tdSql.execute("insert into t1 values (now, 1)")
        tdSql.query("select * from t1")
        tdSql.checkRows(1)
        tdSql.error("select * from t1 union select * from t1")
        tdSql.error("SELECT * FROM t1 UNION SELECT * FROM t1")
        tdSql.execute("drop database db_fw")
        tdLog.info("union_select_denied success")

    def drop_table_denied(self):
        """Blacklist: DROP TABLE denied (string layer from rule file)."""
        tdLog.info("drop_table_denied start")
        tdSql.prepare(dbname="db_fw", drop=True)
        tdSql.execute("use db_fw")
        tdSql.execute("create table t1 (ts timestamp, c1 int)")
        tdSql.error("drop table t1")
        tdSql.error("DROP TABLE t1")
        tdSql.execute("drop database db_fw")
        tdLog.info("drop_table_denied success")

    def other_sql_allowed(self):
        """Blacklist: normal SQL not matching rules is allowed."""
        tdLog.info("other_sql_allowed start")
        tdSql.prepare(dbname="db_fw", drop=True)
        tdSql.execute("use db_fw")
        tdSql.execute("create table sensors (ts timestamp, temp float)")
        tdSql.execute("insert into sensors values (now, 25.5)")
        tdSql.query("select * from sensors where ts > now - 1h")
        tdSql.checkRows(1)
        tdSql.query("select avg(temp) from sensors")
        tdSql.execute("drop database db_fw")
        tdLog.info("other_sql_allowed success")

    def test_sql_blacklist(self):
        """ Test sql blacklist features

        1. UNION SELECT denied (string layer)
        2. DROP TABLE denied (rule file)
        3. Normal SQL allowed

        Since: v3.4.1.0

        Labels: common,ci

        Jira: 6670404791

        History:
            - 2025-03-16 Created based on 6670404791
        """

        self.union_select_denied()
        self.drop_table_denied()
        self.other_sql_allowed()
        tdLog.success("%s successfully executed" % __file__)
