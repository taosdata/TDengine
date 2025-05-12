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

  auto setAlterUserReq = [&](int8_t alterType, int64_t privileges, const string& user, const string& obj) {
    expect.alterType = alterType;
    expect.privileges = privileges;
    expect.tabName[0] = 0;
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
    tFreeSAlterUserReq(&req);
  });

  setAlterUserReq(TSDB_ALTER_USER_ADD_PRIVILEGES, PRIVILEGE_TYPE_ALL, "wxy", "0.*");
  run("GRANT ALL ON *.* TO wxy");

  setAlterUserReq(TSDB_ALTER_USER_ADD_PRIVILEGES, PRIVILEGE_TYPE_READ, "wxy", "0.test");
  run("GRANT READ ON test.* TO wxy");

  setAlterUserReq(TSDB_ALTER_USER_ADD_PRIVILEGES, PRIVILEGE_TYPE_WRITE, "wxy", "0.test");
  run("GRANT WRITE ON test.* TO wxy");

  setAlterUserReq(TSDB_ALTER_USER_ADD_PRIVILEGES, PRIVILEGE_TYPE_ALTER, "wxy", "0.test");
  run("GRANT ALTER ON test.* TO wxy");

  setAlterUserReq(TSDB_ALTER_USER_ADD_PRIVILEGES, PRIVILEGE_TYPE_READ | PRIVILEGE_TYPE_WRITE, "wxy", "0.test");
  run("GRANT READ, WRITE ON test.* TO wxy");

  setAlterUserReq(TSDB_ALTER_USER_ADD_PRIVILEGES, PRIVILEGE_TYPE_SUBSCRIBE, "wxy", "0.tp1");
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

TEST_F(ParserExplainToSyncdbTest, pauseStreamStmt) {
  useDb("root", "test");

  SMPauseStreamReq expect = {0};

  auto setMPauseStreamReq = [&](const string& name, bool igNotExists = false) {
    snprintf(expect.name, sizeof(expect.name), "0.%s", name.c_str());
    expect.igNotExists = igNotExists;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_PAUSE_STREAM_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_PAUSE_STREAM);
    SMPauseStreamReq req = {0};
    ASSERT_EQ(tDeserializeSMPauseStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(string(req.name), string(expect.name));
    ASSERT_EQ(req.igNotExists, expect.igNotExists);
  });

  setMPauseStreamReq("str1");
  run("PAUSE STREAM str1");

  setMPauseStreamReq("str2", true);
  run("PAUSE STREAM IF EXISTS str2");
}

TEST_F(ParserExplainToSyncdbTest, resumeStreamStmt) {
  useDb("root", "test");

  SMResumeStreamReq expect = {0};

  auto setMResumeStreamReq = [&](const string& name, bool igNotExists = false, bool igUntreated = false) {
    snprintf(expect.name, sizeof(expect.name), "0.%s", name.c_str());
    expect.igNotExists = igNotExists;
    expect.igUntreated = igUntreated;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_RESUME_STREAM_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_RESUME_STREAM);
    SMResumeStreamReq req = {0};
    ASSERT_EQ(tDeserializeSMResumeStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(string(req.name), string(expect.name));
    ASSERT_EQ(req.igNotExists, expect.igNotExists);
    ASSERT_EQ(req.igUntreated, expect.igUntreated);
  });

  setMResumeStreamReq("str1");
  run("RESUME STREAM str1");

  setMResumeStreamReq("str2", true, true);
  run("RESUME STREAM IF EXISTS IGNORE UNTREATED str2");
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
    tFreeSRedistributeVgroupReq(&req);
  });

  setRedistributeVgroupReqFunc(3, 1);
  run("REDISTRIBUTE VGROUP 3 DNODE 1");

  setRedistributeVgroupReqFunc(5, 10, 20, 30);
  run("REDISTRIBUTE VGROUP 5 DNODE 10 DNODE 20 DNODE 30");
}

TEST_F(ParserExplainToSyncdbTest, restoreDnode) {
  useDb("root", "test");

  SRestoreDnodeReq expect = {0};

  auto clearRestoreDnodeReq = [&]() { memset(&expect, 0, sizeof(SRestoreDnodeReq)); };

  auto setRestoreDnodeReq = [&](int32_t dnodeId, int8_t type) {
    expect.dnodeId = dnodeId;
    expect.restoreType = type;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    int32_t expectNodeType = 0;
    switch (expect.restoreType) {
      case RESTORE_TYPE__ALL:
        expectNodeType = QUERY_NODE_RESTORE_DNODE_STMT;
        break;
      case RESTORE_TYPE__MNODE:
        expectNodeType = QUERY_NODE_RESTORE_MNODE_STMT;
        break;
      case RESTORE_TYPE__VNODE:
        expectNodeType = QUERY_NODE_RESTORE_VNODE_STMT;
        break;
      case RESTORE_TYPE__QNODE:
        expectNodeType = QUERY_NODE_RESTORE_QNODE_STMT;
        break;
      default:
        break;
    }
    ASSERT_EQ(nodeType(pQuery->pRoot), expectNodeType);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_RESTORE_DNODE);
    SRestoreDnodeReq req = {0};
    ASSERT_EQ(tDeserializeSRestoreDnodeReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(req.dnodeId, expect.dnodeId);
    ASSERT_EQ(req.restoreType, expect.restoreType);
    tFreeSRestoreDnodeReq(&req);
  });

  setRestoreDnodeReq(1, RESTORE_TYPE__ALL);
  run("RESTORE DNODE 1");
  clearRestoreDnodeReq();

  setRestoreDnodeReq(2, RESTORE_TYPE__MNODE);
  run("RESTORE MNODE ON DNODE 2");
  clearRestoreDnodeReq();

  setRestoreDnodeReq(1, RESTORE_TYPE__VNODE);
  run("RESTORE VNODE ON DNODE 1");
  clearRestoreDnodeReq();

  setRestoreDnodeReq(2, RESTORE_TYPE__QNODE);
  run("RESTORE QNODE ON DNODE 2");
  clearRestoreDnodeReq();
}



// todo reset query cache

TEST_F(ParserExplainToSyncdbTest, revoke) {
  useDb("root", "test");

  SAlterUserReq expect = {0};

  auto setAlterUserReq = [&](int8_t alterType, int64_t privileges, const string& user, const string& obj) {
    expect.alterType = alterType;
    expect.privileges = privileges;
    expect.tabName[0] = 0;
    snprintf(expect.user, sizeof(expect.user), "%s", user.c_str());
    snprintf(expect.objname, sizeof(expect.objname), "%s", obj.c_str());
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_REVOKE_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_ALTER_USER);
    SAlterUserReq req = {0};
    ASSERT_EQ(tDeserializeSAlterUserReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(req.alterType, expect.alterType);
    ASSERT_EQ(string(req.user), string(expect.user));
    ASSERT_EQ(string(req.objname), string(expect.objname));
    tFreeSAlterUserReq(&req);
  });

  setAlterUserReq(TSDB_ALTER_USER_DEL_PRIVILEGES, PRIVILEGE_TYPE_ALL, "wxy", "0.*");
  run("REVOKE ALL ON *.* FROM wxy");

  setAlterUserReq(TSDB_ALTER_USER_DEL_PRIVILEGES, PRIVILEGE_TYPE_READ, "wxy", "0.test");
  run("REVOKE READ ON test.* FROM wxy");

  setAlterUserReq(TSDB_ALTER_USER_DEL_PRIVILEGES, PRIVILEGE_TYPE_WRITE, "wxy", "0.test");
  run("REVOKE WRITE ON test.* FROM wxy");

  setAlterUserReq(TSDB_ALTER_USER_DEL_PRIVILEGES, PRIVILEGE_TYPE_ALTER, "wxy", "0.test");
  run("REVOKE ALTER ON test.* FROM wxy");

  setAlterUserReq(TSDB_ALTER_USER_DEL_PRIVILEGES, PRIVILEGE_TYPE_READ|PRIVILEGE_TYPE_WRITE, "wxy", "0.test");
  run("REVOKE READ, WRITE ON test.* FROM wxy");

  setAlterUserReq(TSDB_ALTER_USER_DEL_PRIVILEGES, PRIVILEGE_TYPE_SUBSCRIBE, "wxy", "0.tp1");
  run("REVOKE SUBSCRIBE ON tp1 FROM wxy");
}

// todo syncdb

}  // namespace ParserTest
