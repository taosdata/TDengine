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

class ParserInitialDTest : public ParserDdlTest {};

// DELETE FROM table_name [WHERE condition]
TEST_F(ParserInitialDTest, delete) {
  useDb("root", "test");

  run("DELETE FROM t1");

  run("DELETE FROM t1 WHERE ts > now - 2d and ts < now - 1d");

  run("DELETE FROM st1");

  run("DELETE FROM st1 WHERE ts > now - 2d and ts < now - 1d AND tag1 = 10");
}

TEST_F(ParserInitialDTest, deleteSemanticCheck) {
  useDb("root", "test");

  run("DELETE FROM t1 WHERE c1 > 10", TSDB_CODE_PAR_INVALID_DELETE_WHERE, PARSER_STAGE_TRANSLATE);
}

// DESC table_name
TEST_F(ParserInitialDTest, describe) {
  useDb("root", "test");

  run("DESC t1");

  run("DESCRIBE st1");
}

// todo describe
// todo DROP account

TEST_F(ParserInitialDTest, dropBnode) {
  useDb("root", "test");

  run("DROP BNODE ON DNODE 1");
}

// DROP CONSUMER GROUP [ IF EXISTS ] cgroup_name ON topic_name
TEST_F(ParserInitialDTest, dropConsumerGroup) {
  useDb("root", "test");

  SMDropCgroupReq expect = {0};

  auto setDropCgroupReqFunc = [&](const char* pTopicName, const char* pCGroupName, int8_t igNotExists = 0) {
    memset(&expect, 0, sizeof(SMDropCgroupReq));
    snprintf(expect.topic, sizeof(expect.topic), "0.%s", pTopicName);
    strcpy(expect.cgroup, pCGroupName);
    expect.igNotExists = igNotExists;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_DROP_CGROUP_STMT);
    SMDropCgroupReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSMDropCgroupReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.topic), std::string(expect.topic));
    ASSERT_EQ(std::string(req.cgroup), std::string(expect.cgroup));
    ASSERT_EQ(req.igNotExists, expect.igNotExists);
  });

  setDropCgroupReqFunc("tp1", "cg1");
  run("DROP CONSUMER GROUP cg1 ON tp1");

  setDropCgroupReqFunc("tp1", "cg1", 1);
  run("DROP CONSUMER GROUP IF EXISTS cg1 ON tp1");
}

// todo DROP database
// todo DROP dnode
// todo DROP function

TEST_F(ParserInitialDTest, dropIndex) {
  useDb("root", "test");

  run("DROP index index1 on t1");
}

TEST_F(ParserInitialDTest, dropMnode) {
  useDb("root", "test");

  run("DROP mnode on dnode 1");
}

TEST_F(ParserInitialDTest, dropQnode) {
  useDb("root", "test");

  run("DROP qnode on dnode 1");
}

TEST_F(ParserInitialDTest, dropSnode) {
  useDb("root", "test");

  run("DROP snode on dnode 1");
}

TEST_F(ParserInitialDTest, dropSTable) {
  useDb("root", "test");

  run("DROP STABLE st1");
}

// todo DROP stream

TEST_F(ParserInitialDTest, dropTable) {
  useDb("root", "test");

  run("DROP TABLE t1");
}

TEST_F(ParserInitialDTest, dropTopic) {
  useDb("root", "test");

  run("DROP topic tp1");

  run("DROP topic if exists tp1");
}

TEST_F(ParserInitialDTest, dropUser) {
  login("root");
  useDb("root", "test");

  run("DROP user wxy");
}

}  // namespace ParserTest
