/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

class ParserShowToUseTest : public ParserDdlTest {};

// todo SHOW accounts
// todo SHOW apps
// todo SHOW connections

TEST_F(ParserShowToUseTest, showCluster) {
  useDb("root", "test");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_SELECT_STMT); });

  run("SHOW CLUSTER");
}

TEST_F(ParserShowToUseTest, showConsumers) {
  useDb("root", "test");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_SELECT_STMT); });

  run("SHOW CONSUMERS");
}

TEST_F(ParserShowToUseTest, showCreateDatabase) {
  useDb("root", "test");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_SHOW_CREATE_DATABASE_STMT);
    ASSERT_EQ(pQuery->execMode, QUERY_EXEC_MODE_LOCAL);
    ASSERT_TRUE(pQuery->haveResultSet);
    ASSERT_NE(((SShowCreateDatabaseStmt*)pQuery->pRoot)->pCfg, nullptr);
  });

  run("SHOW CREATE DATABASE test");
}

TEST_F(ParserShowToUseTest, showCreateSTable) {
  useDb("root", "test");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_SHOW_CREATE_STABLE_STMT);
    ASSERT_EQ(pQuery->execMode, QUERY_EXEC_MODE_LOCAL);
    ASSERT_TRUE(pQuery->haveResultSet);
    ASSERT_NE(((SShowCreateTableStmt*)pQuery->pRoot)->pDbCfg, nullptr);
    ASSERT_NE(((SShowCreateTableStmt*)pQuery->pRoot)->pTableCfg, nullptr);
  });

  run("SHOW CREATE STABLE st1");
}

TEST_F(ParserShowToUseTest, showCreateTable) {
  useDb("root", "test");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_SHOW_CREATE_TABLE_STMT);
    ASSERT_EQ(pQuery->execMode, QUERY_EXEC_MODE_LOCAL);
    ASSERT_TRUE(pQuery->haveResultSet);
    ASSERT_NE(((SShowCreateTableStmt*)pQuery->pRoot)->pDbCfg, nullptr);
    ASSERT_NE(((SShowCreateTableStmt*)pQuery->pRoot)->pTableCfg, nullptr);
  });

  run("SHOW CREATE TABLE t1");
}

TEST_F(ParserShowToUseTest, showDatabases) {
  useDb("root", "test");

  run("SHOW databases");
}

TEST_F(ParserShowToUseTest, showDnodes) {
  useDb("root", "test");

  run("SHOW dnodes");
}

TEST_F(ParserShowToUseTest, showDnodeVariables) {
  useDb("root", "test");

  run("SHOW DNODE 1 VARIABLES");

  run("SHOW DNODE 1 VARIABLES LIKE '%debug%'");
}

TEST_F(ParserShowToUseTest, showFunctions) {
  useDb("root", "test");

  run("SHOW functions");
}

// todo SHOW licence

TEST_F(ParserShowToUseTest, showLocalVariables) {
  useDb("root", "test");

  run("SHOW LOCAL VARIABLES");
}

TEST_F(ParserShowToUseTest, showIndexes) {
  useDb("root", "test");

  run("SHOW indexes from t1");

  run("SHOW indexes from t1 from test");
}

TEST_F(ParserShowToUseTest, showMnodes) {
  useDb("root", "test");

  run("SHOW mnodes");
}

TEST_F(ParserShowToUseTest, showQnodes) {
  useDb("root", "test");

  run("SHOW qnodes");
}

// todo SHOW queries
// todo SHOW scores

TEST_F(ParserShowToUseTest, showStables) {
  useDb("root", "test");

  run("SHOW stables");

  run("SHOW test.stables");

  run("SHOW stables like 'c%'");

  run("SHOW test.stables like 'c%'");
}

TEST_F(ParserShowToUseTest, showStreams) {
  useDb("root", "test");

  run("SHOW streams");
}

TEST_F(ParserShowToUseTest, showSubscriptions) {
  useDb("root", "test");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_SELECT_STMT); });

  run("SHOW SUBSCRIPTIONS");
}

TEST_F(ParserShowToUseTest, showTransactions) {
  useDb("root", "test");

  run("SHOW TRANSACTIONS");
}

TEST_F(ParserShowToUseTest, showTables) {
  useDb("root", "test");

  run("SHOW tables");

  run("SHOW test.tables");

  run("SHOW tables like 'c%'");

  run("SHOW test.tables like 'c%'");
}

TEST_F(ParserShowToUseTest, showTableDistributed) {
  useDb("root", "test");

  run("SHOW TABLE DISTRIBUTED st1");
}

TEST_F(ParserShowToUseTest, showTableTags) {
  useDb("root", "test");

  run("SHOW TABLE TAGS FROM st1");

  run("SHOW TABLE TAGS tag1, tag2 FROM st1");

  run("SHOW TABLE TAGS TBNAME, _TAGS, tag3 FROM st1");
}

TEST_F(ParserShowToUseTest, showTags) {
  useDb("root", "test");

  run("SHOW TAGS FROM st1s1");
}

// todo SHOW topics

TEST_F(ParserShowToUseTest, showUsers) {
  useDb("root", "test");

  run("SHOW USERS");
}

TEST_F(ParserShowToUseTest, showUserPrivileges) {
  useDb("root", "test");

  run("SHOW USER PRIVILEGES");
}

TEST_F(ParserShowToUseTest, showVariables) {
  useDb("root", "test");

  run("SHOW VARIABLES");
}

TEST_F(ParserShowToUseTest, showVgroups) {
  useDb("root", "test");

  run("SHOW VGROUPS");

  run("SHOW test.VGROUPS");
}

TEST_F(ParserShowToUseTest, showVnodes) {
  useDb("root", "test");

  run("SHOW VNODES ON DNODE 1");

  run("SHOW VNODES");
}

TEST_F(ParserShowToUseTest, splitVgroup) {
  useDb("root", "test");

  SSplitVgroupReq expect = {0};

  auto setSplitVgroupReqFunc = [&](int32_t vgId) { expect.vgId = vgId; };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_SPLIT_VGROUP_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_SPLIT_VGROUP);
    SSplitVgroupReq req = {0};
    ASSERT_EQ(tDeserializeSSplitVgroupReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(req.vgId, expect.vgId);
  });

  setSplitVgroupReqFunc(15);
  run("SPLIT VGROUP 15");
}

TEST_F(ParserShowToUseTest, trimDatabase) {
  useDb("root", "test");

  STrimDbReq expect = {0};

  auto setTrimDbReq = [&](const char* pDb, int32_t maxSpeed = 0) {
    snprintf(expect.db, sizeof(expect.db), "0.%s", pDb);
    expect.maxSpeed = maxSpeed;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_TRIM_DATABASE_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_TRIM_DB);
    STrimDbReq req = {0};
    ASSERT_EQ(tDeserializeSTrimDbReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(std::string(req.db), std::string(expect.db));
    ASSERT_EQ(req.maxSpeed, expect.maxSpeed);
  });

  setTrimDbReq("wxy_db");
  run("TRIM DATABASE wxy_db");

  setTrimDbReq("wxy_db", 100);
  run("TRIM DATABASE wxy_db BWLIMIT 100");
}

TEST_F(ParserShowToUseTest, useDatabase) {
  useDb("root", "test");

  run("use wxy_db");
}

}  // namespace ParserTest
