###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

# -*- coding: utf-8 -*-

import os
import json
import time
from new_test_framework.utils import tdLog, tdSql


def _get_sql_rules_path():
    """Get absolute path to sql_rules.json for test."""
    test_dir = os.path.dirname(os.path.abspath(__file__))
    rules_path = os.path.join(test_dir, "sql_rules_whitelist.json")

    # Start with minimal rules
    rules = {
        "version": "1.0",
        "rules": [],
    }

    with open(rules_path, "w") as f:
        json.dump(rules, f, indent=2)

    return rules_path


class TestWhitelistLearning:
    """Test whitelist learning mode functionality.

    This test demonstrates the automatic learning of SQL patterns:
    1. Enable learning mode
    2. Execute SQL statements multiple times
    3. Patterns that exceed threshold are learned
    4. Export learned rules to file
    5. Enable whitelist mode and verify learned patterns work
    """

    _rules_path = _get_sql_rules_path()
    _client_cfg = {
        "sqlSecurity": 1,
        "sqlSecurityWhitelistMode": 0,  # Start disabled
        "sqlSecurityStringCheck": 1,
        "sqlSecurityASTCheck": 1,
        "sqlSecurityRuleFile": _rules_path,
        "whitelistLearning": 1,  # Enable learning
        "whitelistLearningPeriod": 7,
        "whitelistLearningThreshold": 3,  # Low threshold for testing
    }
    updatecfgDict = {"clientCfg": _client_cfg}

    @classmethod
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def test_whitelist_learning(self):
        """ Test sql whitelist learning features

        1. Basic learning - patterns are recorded
        2. Pattern generalization - different values map to same pattern
        3. Threshold behavior - only frequent patterns are learned
        4. Complete workflow - learn, export, protect

        Since: v3.4.1.0

        Labels: common,ci

        Jira: 6670404791

        History:
            - 2025-03-16 Created based on 6670404791
        """

        self.do_prepare()
        self.basic_check()
        self.different_values_check()
        self.threshold_check()

        tdLog.success("%s successfully executed" % __file__)

    def do_prepare(self):
        tdLog.info("Preparing test environment...")
        # delete the rules_path file
        if os.path.exists(self._rules_path):
            os.remove(self._rules_path)
        tdLog.info("Rules path deleted")
        tdSql.execute("alter local 'sqlSecurityWhitelistMode' '0'")
        tdSql.execute("alter local 'whitelistLearning' '1'")
        tdSql.execute("alter local 'whitelistLearningThreshold' '3'")

        # Create test database and table
        tdSql.prepare(dbname="db_learn", drop=True)
        tdSql.execute("use db_learn")
        tdSql.execute("create table sensors (ts timestamp, temp float, humidity int)")
        tdSql.execute("show tables")

        # Execute same pattern multiple times (should be learned)
        tdLog.info("Executing pattern multiple times to trigger learning...")
        for i in range(5):
            tdSql.execute("alter local 'whitelistLearning' '1'")
            tdSql.execute("alter local 'sqlSecurityWhitelistMode' '0'")
            tdSql.execute(f"insert into sensors values (now + {i}s, 25.5, 60)")
            tdSql.query(f"select * from sensors where temp > 20")
            tdSql.query(f"select avg(temp) from sensors")

        #检查_rules_path是否生成
        i = 0
        while not os.path.exists(self._rules_path) and i < 60:
            i += 1
            time.sleep(1)
        tdLog.info("Rules path created")
        if i >= 60:
            tdLog.error("Rules path not created")
            raise Exception("Rules path not created")
        tdLog.info("Waiting for learning to complete...")

    def basic_check(self):
        """Test basic learning functionality."""
        tdLog.info("=== Test 1: Basic Learning ===")
        tdSql.execute("alter local 'whitelistLearning' '0'")
        tdSql.execute("alter local 'sqlSecurityWhitelistMode' '1'")
        time.sleep(5)
        # should be allowed
        tdSql.query("select * from sensors where temp > 20")
        tdSql.query(f"select avg(temp) from sensors")
        # should be denied
        tdSql.error("select count(*) from sensors")
        tdSql.error("drop database db_learn")
        tdLog.success("Basic learning test passed")

    def different_values_check(self):
        """Test that different literal values map to same pattern."""
        tdLog.info("=== Test 2: Learning with different values ===")
        # Execute similar queries with different values
        # These should all map to the same pattern
        tdSql.execute(f"insert into sensors values (now + 15s, 26.5, 100)")
        tdSql.execute(f"insert into sensors values (now + 125s, 27.5, 200)")
        tdSql.execute(f"insert into sensors values (now + 1135s, 28.5, 300)")

        tdSql.query("select * from sensors where temp > 26.5")
        tdSql.query("select * from sensors where temp > 27.5")
        tdSql.query("select * from sensors where temp > 28.5")

        tdSql.error(f"insert into sensors values (now, 29.5, 400)")
        tdSql.error("select * from sensors where temp < 29.5")

        tdLog.success("Learning with different values test passed")

    def threshold_check(self):
        """Test that only frequent patterns are learned."""
        tdLog.info("=== Test 3: Learning threshold ===")
        # these sql only executed once, should not be learned since threshold is 3
        tdSql.error(f"create table sensors (ts timestamp, temp float, humidity int)")
        tdSql.error(f"use db_learn")
        tdSql.error(f"show tables")
