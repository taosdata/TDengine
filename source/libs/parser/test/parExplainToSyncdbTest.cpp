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

class ParserExplainToSyncdbTest : public ParserDdlTest {};

TEST_F(ParserExplainToSyncdbTest, explain) {
  useDb("root", "test");

  run("EXPLAIN SELECT * FROM t1");

  run("EXPLAIN ANALYZE SELECT * FROM t1");

  run("EXPLAIN ANALYZE VERBOSE true RATIO 0.01 SELECT * FROM t1");
}

TEST_F(ParserExplainToSyncdbTest, grant) {
  useDb("root", "test");

  SAlterUserReq expect = {0};

  auto setAlterUserReq = [&](int8_t alterType, const string& user, const string& obj) {
    expect.alterType = alterType;
    snprintf(expect.user, sizeof(expect.user), "%s", user.c_str());
    snprintf(expect.objname, sizeof(expect.objname), "%s", obj.c_str());
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_GRANT_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_ALTER_USER);
    SAlterUserReq req = {0};
    ASSERT_EQ(tDeserializeSAlterUserReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(req.alterType, expect.alterType);
    ASSERT_EQ(string(req.user), string(expect.user));
    ASSERT_EQ(string(req.objname), string(expect.objname));
  });

  setAlterUserReq(TSDB_ALTER_USER_ADD_ALL_DB, "wxy", "0.test");
  run("GRANT ALL ON test.* TO wxy");

  setAlterUserReq(TSDB_ALTER_USER_ADD_READ_DB, "wxy", "0.test");
  run("GRANT READ ON test.* TO wxy");

  setAlterUserReq(TSDB_ALTER_USER_ADD_WRITE_DB, "wxy", "0.test");
  run("GRANT WRITE ON test.* TO wxy");

  setAlterUserReq(TSDB_ALTER_USER_ADD_ALL_DB, "wxy", "0.test");
  run("GRANT READ, WRITE ON test.* TO wxy");

  setAlterUserReq(TSDB_ALTER_USER_ADD_SUBSCRIBE_TOPIC, "wxy", "0.tp1");
  run("GRANT SUBSCRIBE ON tp1 TO wxy");
}

TEST_F(ParserExplainToSyncdbTest, insert) {
  useDb("root", "test");

  run("INSERT INTO t1 SELECT * FROM t1");
}

// todo kill connection
// todo kill query
// todo kill stream

TEST_F(ParserExplainToSyncdbTest, mergeVgroup) {
  useDb("root", "test");

  SMergeVgroupReq expect = {0};

  auto setMergeVgroupReqFunc = [&](int32_t vgId1, int32_t vgId2) {
    expect.vgId1 = vgId1;
    expect.vgId2 = vgId2;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_MERGE_VGROUP_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_MERGE_VGROUP);
    SMergeVgroupReq req = {0};
    ASSERT_EQ(tDeserializeSMergeVgroupReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(req.vgId1, expect.vgId1);
    ASSERT_EQ(req.vgId2, expect.vgId2);
  });

  setMergeVgroupReqFunc(1, 2);
  run("MERGE VGROUP 1 2");
}

TEST_F(ParserExplainToSyncdbTest, redistributeVgroup) {
  useDb("root", "test");

  SRedistributeVgroupReq expect = {0};

  auto setRedistributeVgroupReqFunc = [&](int32_t vgId, int32_t dnodeId1, int32_t dnodeId2 = -1,
                                          int32_t dnodeId3 = -1) {
    expect.vgId = vgId;
    expect.dnodeId1 = dnodeId1;
    expect.dnodeId2 = dnodeId2;
    expect.dnodeId3 = dnodeId3;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_REDISTRIBUTE_VGROUP_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_REDISTRIBUTE_VGROUP);
    SRedistributeVgroupReq req = {0};
    ASSERT_EQ(tDeserializeSRedistributeVgroupReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(req.vgId, expect.vgId);
    ASSERT_EQ(req.dnodeId1, expect.dnodeId1);
    ASSERT_EQ(req.dnodeId2, expect.dnodeId2);
    ASSERT_EQ(req.dnodeId3, expect.dnodeId3);
  });

  setRedistributeVgroupReqFunc(3, 1);
  run("REDISTRIBUTE VGROUP 3 DNODE 1");

  setRedistributeVgroupReqFunc(5, 10, 20, 30);
  run("REDISTRIBUTE VGROUP 5 DNODE 10 DNODE 20 DNODE 30");
}

// todo reset query cache

TEST_F(ParserExplainToSyncdbTest, revoke) {
  useDb("root", "test");

  run("REVOKE ALL ON test.* FROM wxy");
  run("REVOKE READ ON test.* FROM wxy");
  run("REVOKE WRITE ON test.* FROM wxy");
  run("REVOKE READ, WRITE ON test.* FROM wxy");
}

// todo syncdb

}  // namespace ParserTest
