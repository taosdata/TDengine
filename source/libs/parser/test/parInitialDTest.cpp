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

// DROP CONSUMER GROUP [ IF EXISTS ] cgroup_name ON topic_name
TEST_F(ParserInitialDTest, dropConsumerGroup) {
  useDb("root", "test");

  SMDropCgroupReq expect = {0};

  auto clearDropCgroupReq = [&]() { memset(&expect, 0, sizeof(SMDropCgroupReq)); };

  auto setDropCgroupReq = [&](const char* pTopicName, const char* pCGroupName, int8_t igNotExists = 0) {
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

  setDropCgroupReq("tp1", "cg1");
  run("DROP CONSUMER GROUP cg1 ON tp1");
  clearDropCgroupReq();

  setDropCgroupReq("tp1", "cg1", 1);
  run("DROP CONSUMER GROUP IF EXISTS cg1 ON tp1");
  clearDropCgroupReq();
}

// todo DROP database

TEST_F(ParserInitialDTest, dropDnode) {
  useDb("root", "test");

  SDropDnodeReq expect = {0};

  auto clearDropDnodeReq = [&]() { memset(&expect, 0, sizeof(SDropDnodeReq)); };

  auto setDropDnodeReqById = [&](int32_t dnodeId, bool force = false) {
    expect.dnodeId = dnodeId;
    expect.force = force;
  };

  auto setDropDnodeReqByEndpoint = [&](const char* pFqdn, int32_t port, bool force = false) {
    strcpy(expect.fqdn, pFqdn);
    expect.port = port;
    expect.force = force;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_DROP_DNODE_STMT);
    SDropDnodeReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSDropDnodeReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(req.dnodeId, expect.dnodeId);
    ASSERT_EQ(std::string(req.fqdn), std::string(expect.fqdn));
    ASSERT_EQ(req.port, expect.port);
    ASSERT_EQ(req.force, expect.force);
  });

  setDropDnodeReqById(1);
  run("DROP DNODE 1");
  clearDropDnodeReq();

  setDropDnodeReqById(2, true);
  run("DROP DNODE 2 FORCE");
  clearDropDnodeReq();

  setDropDnodeReqByEndpoint("host1", 7030);
  run("DROP DNODE 'host1:7030'");
  clearDropDnodeReq();

  setDropDnodeReqByEndpoint("host2", 8030, true);
  run("DROP DNODE 'host2:8030' FORCE");
  clearDropDnodeReq();
}

// todo DROP function

TEST_F(ParserInitialDTest, dropIndex) {
  useDb("root", "test");

  SMDropSmaReq expect = {0};

  auto clearDropSmaReq = [&]() { memset(&expect, 0, sizeof(SMDropSmaReq)); };

  auto setDropSmaReq = [&](const char* pName, int8_t igNotExists = 0) {
    sprintf(expect.name, "0.test.%s", pName);
    expect.igNotExists = igNotExists;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_DROP_INDEX_STMT);
    SMDropSmaReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSMDropSmaReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.name), std::string(expect.name));
    ASSERT_EQ(req.igNotExists, expect.igNotExists);
  });

  setDropSmaReq("index1");
  run("DROP INDEX index1");
  clearDropSmaReq();

  setDropSmaReq("index2", 1);
  run("DROP INDEX IF EXISTS index2");
  clearDropSmaReq();
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

TEST_F(ParserInitialDTest, dropStream) {
  useDb("root", "test");

  SMDropStreamReq expect = {0};

  auto clearDropStreamReq = [&]() { memset(&expect, 0, sizeof(SMDropStreamReq)); };

  auto setDropStreamReq = [&](const char* pStream, int8_t igNotExists = 0) {
    sprintf(expect.name, "0.%s", pStream);
    expect.igNotExists = igNotExists;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_DROP_STREAM_STMT);
    SMDropStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSMDropStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.name), std::string(expect.name));
    ASSERT_EQ(req.igNotExists, expect.igNotExists);
  });

  setDropStreamReq("s1");
  run("DROP STREAM s1");
  clearDropStreamReq();

  setDropStreamReq("s2", 1);
  run("DROP STREAM IF EXISTS s2");
  clearDropStreamReq();
}

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
