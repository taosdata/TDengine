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

// todo delete
// todo desc
// todo describe
// todo drop account

TEST_F(ParserInitialDTest, dropBnode) {
  useDb("root", "test");

  run("DROP BNODE ON DNODE 1");
}

// DROP CGROUP [ IF EXISTS ] cgroup_name ON topic_name
TEST_F(ParserInitialDTest, dropCGroup) {
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
  run("DROP CGROUP cg1 ON tp1");

  setDropCgroupReqFunc("tp1", "cg1", 1);
  run("DROP CGROUP IF EXISTS cg1 ON tp1");
}

// todo drop database
// todo drop dnode
// todo drop function

TEST_F(ParserInitialDTest, dropIndex) {
  useDb("root", "test");

  run("drop index index1 on t1");
}

TEST_F(ParserInitialDTest, dropMnode) {
  useDb("root", "test");

  run("drop mnode on dnode 1");
}

TEST_F(ParserInitialDTest, dropQnode) {
  useDb("root", "test");

  run("drop qnode on dnode 1");
}

TEST_F(ParserInitialDTest, dropSnode) {
  useDb("root", "test");

  run("drop snode on dnode 1");
}

// todo drop stable
// todo drop stream
// todo drop table

TEST_F(ParserInitialDTest, dropTopic) {
  useDb("root", "test");

  run("drop topic tp1");

  run("drop topic if exists tp1");
}

TEST_F(ParserInitialDTest, dropUser) {
  login("root");
  useDb("root", "test");

  run("drop user wxy");
}

}  // namespace ParserTest
