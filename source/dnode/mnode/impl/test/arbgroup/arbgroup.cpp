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

// #include <iostream>
#include <string>
#include <gtest/gtest.h>
// #include "nodes.h"
// #include "planner.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

// #include <libs/transport/trpc.h>
#include "../../inc/mndArbGroup.h"

namespace {

void generateArbToken(int32_t nodeId, int32_t vgId, char* buf) {
  memset(buf, 0, TSDB_ARB_TOKEN_SIZE);
  int32_t randVal = taosSafeRand() % 1000;
  int64_t currentMs = taosGetTimestampMs();
  snprintf(buf, TSDB_ARB_TOKEN_SIZE, "d%d#g%d#%" PRId64 "#%d", nodeId, vgId, currentMs, randVal);
}

}  // namespace

class ArbgroupTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::cout << "setup env for arbgroupTest suite" << std::endl;
  }

  static void TearDownTestSuite() { std::cout << "tearDown env for arbgroupTest suite" << std::endl; }

  virtual void SetUp() override {}

  virtual void TearDown() override {}

};

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST_F(ArbgroupTest, 01_encode_decode_sdb) {
  SArbGroup group = {0};
  group.vgId = 5;
  group.dbUid = 1234;
  group.members[0].info.dnodeId = 1;
  generateArbToken(1, 5, group.members[0].state.token);
  group.members[1].info.dnodeId = 2;
  generateArbToken(2, 5, group.members[1].state.token);
  group.isSync = 1;
  group.assignedLeader.dnodeId = 1;
  generateArbToken(1, 5, group.assignedLeader.token);
  group.version = 2234;

  // --------------------------------------------------------------------------------
  SSdbRaw* pRaw = mndArbGroupActionEncode(&group);
  ASSERT_NE(pRaw, nullptr);

  SSdbRow* pRow = mndArbGroupActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SArbGroup* pNewGroup = (SArbGroup*)sdbGetRowObj(pRow);

  EXPECT_EQ(group.vgId, pNewGroup->vgId);
  EXPECT_EQ(group.dbUid, pNewGroup->dbUid);
  EXPECT_EQ(group.members[0].info.dnodeId, pNewGroup->members[0].info.dnodeId);
  EXPECT_EQ(group.members[1].info.dnodeId, pNewGroup->members[1].info.dnodeId);
  EXPECT_EQ(group.isSync, pNewGroup->isSync);
  EXPECT_EQ(group.assignedLeader.dnodeId, pNewGroup->assignedLeader.dnodeId);

  EXPECT_EQ(std::string(group.members[0].state.token), std::string(pNewGroup->members[0].state.token));
  EXPECT_EQ(std::string(group.members[1].state.token), std::string(pNewGroup->members[1].state.token));
  EXPECT_EQ(std::string(group.assignedLeader.token), std::string(pNewGroup->assignedLeader.token));
  EXPECT_EQ(group.version, pNewGroup->version);

  taosMemoryFree(pRow);
  taosMemoryFree(pRaw);
}

TEST_F(ArbgroupTest, 02_process_heart_beat_rsp) {
  const int32_t dnodeId = 1;
  const int32_t vgId = 5;

  SArbGroup group = {0};
  group.vgId = vgId;
  group.dbUid = 1234;
  group.members[0].info.dnodeId = dnodeId;
  generateArbToken(dnodeId, vgId, group.members[0].state.token);
  group.members[0].state.lastHbMs = 1000;
  group.members[0].state.responsedHbSeq = 100;
  group.members[0].state.nextHbSeq = 102;

  group.members[1].info.dnodeId = 2;
  generateArbToken(2, vgId, group.members[1].state.token);

  group.isSync = 1;
  group.assignedLeader.dnodeId = dnodeId;
  strncpy(group.assignedLeader.token, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);

  taosThreadMutexInit(&group.mutex, NULL);

  // --------------------------------------------------------------------------------
  {  // expired hb => skip
    SVArbHbRspMember rspMember = {0};
    rspMember.vgId = vgId;
    rspMember.hbSeq = group.members[0].state.responsedHbSeq - 1;
    strncpy(rspMember.memberToken, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);
    int32_t nowMs = group.members[0].state.lastHbMs + 10;

    SArbGroup newGroup = {0};
    bool      updateToken = mndUpdateArbGroupByHeartBeat(&group, &rspMember, nowMs, dnodeId, &newGroup);

    EXPECT_FALSE(updateToken);
    EXPECT_NE(group.members[0].state.responsedHbSeq, rspMember.hbSeq);
    EXPECT_NE(group.members[0].state.lastHbMs, nowMs);
  }

  {  // old token
    SVArbHbRspMember rspMember = {0};
    rspMember.vgId = vgId;
    rspMember.hbSeq = group.members[0].state.responsedHbSeq + 1;
    strncpy(rspMember.memberToken, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);
    int32_t nowMs = group.members[0].state.lastHbMs + 10;

    SArbGroup newGroup = {0};
    bool      updateToken = mndUpdateArbGroupByHeartBeat(&group, &rspMember, nowMs, dnodeId, &newGroup);

    EXPECT_FALSE(updateToken);
    EXPECT_EQ(group.members[0].state.responsedHbSeq, rspMember.hbSeq);
    EXPECT_EQ(group.members[0].state.lastHbMs, nowMs);
  }

  {  // new token
    SVArbHbRspMember rspMember = {0};
    rspMember.vgId = vgId;
    rspMember.hbSeq = group.members[0].state.responsedHbSeq + 1;
    generateArbToken(dnodeId, vgId, rspMember.memberToken);
    int32_t nowMs = group.members[0].state.lastHbMs + 10;

    SArbGroup newGroup = {0};
    bool      updateToken = mndUpdateArbGroupByHeartBeat(&group, &rspMember, nowMs, dnodeId, &newGroup);

    EXPECT_TRUE(updateToken);
    EXPECT_EQ(group.members[0].state.responsedHbSeq, rspMember.hbSeq);
    EXPECT_EQ(group.members[0].state.lastHbMs, nowMs);

    EXPECT_EQ(std::string(newGroup.members[0].state.token), std::string(rspMember.memberToken));
    EXPECT_FALSE(newGroup.isSync);
    EXPECT_EQ(newGroup.assignedLeader.dnodeId, 0);
    EXPECT_EQ(std::string(newGroup.assignedLeader.token).size(), 0);
  }

  taosThreadMutexDestroy(&group.mutex);
}

TEST_F(ArbgroupTest, 03_process_check_sync_rsp) {
  const int32_t dnodeId = 1;
  const int32_t vgId = 5;

  SArbGroup group = {0};
  group.vgId = vgId;
  group.dbUid = 1234;
  group.members[0].info.dnodeId = dnodeId;
  generateArbToken(dnodeId, vgId, group.members[0].state.token);
  group.members[0].state.lastHbMs = 1000;
  group.members[0].state.responsedHbSeq = 100;
  group.members[0].state.nextHbSeq = 102;

  group.members[1].info.dnodeId = 2;
  generateArbToken(2, vgId, group.members[1].state.token);

  group.isSync = 0;

  taosThreadMutexInit(&group.mutex, NULL);

  // --------------------------------------------------------------------------------
  {  // token mismatch => skip
    char member0Token[TSDB_ARB_TOKEN_SIZE] = {0};
    strncpy(member0Token, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);
    char member1Token[TSDB_ARB_TOKEN_SIZE] = {0};
    generateArbToken(2, 5, member1Token);
    bool newIsSync = false;

    SArbGroup newGroup = {0};
    bool      updateIsSync = mndUpdateArbGroupByCheckSync(&group, vgId, member0Token, member1Token, newIsSync, &newGroup);

    EXPECT_FALSE(updateIsSync);
  }

  {  // newIsSync
    char member0Token[TSDB_ARB_TOKEN_SIZE] = {0};
    strncpy(member0Token, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);
    char member1Token[TSDB_ARB_TOKEN_SIZE] = {0};
    strncpy(member1Token, group.members[1].state.token, TSDB_ARB_TOKEN_SIZE);
    bool newIsSync = true;

    SArbGroup newGroup = {0};
    bool      updateIsSync = mndUpdateArbGroupByCheckSync(&group, vgId, member0Token, member1Token, newIsSync, &newGroup);

    EXPECT_TRUE(updateIsSync);
    EXPECT_TRUE(newGroup.isSync);
  }

  taosThreadMutexDestroy(&group.mutex);
}

TEST_F(ArbgroupTest, 04_process_set_assigned_leader){
  const int32_t dnodeId = 1;
  const int32_t vgId = 5;

  SArbGroup group = {0};
  group.vgId = vgId;
  group.dbUid = 1234;
  group.members[0].info.dnodeId = dnodeId;
  generateArbToken(dnodeId, vgId, group.members[0].state.token);
  group.members[0].state.lastHbMs = 1000;
  group.members[0].state.responsedHbSeq = 100;
  group.members[0].state.nextHbSeq = 102;

  group.members[1].info.dnodeId = 2;
  generateArbToken(2, vgId, group.members[1].state.token);

  group.isSync = 1;
  group.assignedLeader.dnodeId = dnodeId;
  strncpy(group.assignedLeader.token, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);

  taosThreadMutexInit(&group.mutex, NULL);

  // --------------------------------------------------------------------------------
  {  // token mismatch => skip
    char memberToken[TSDB_ARB_TOKEN_SIZE] = {0};
    generateArbToken(dnodeId, vgId, memberToken);
    int32_t errcode = TSDB_CODE_SUCCESS;

    SArbGroup newGroup = {0};
    bool      updateAssigned = mndUpdateArbGroupBySetAssignedLeader(&group, vgId, memberToken, errcode, &newGroup);

    EXPECT_FALSE(updateAssigned);
  }

  {  // errcode != TSDB_CODE_SUCCESS
    char memberToken[TSDB_ARB_TOKEN_SIZE] = {0};
    strncpy(memberToken, group.assignedLeader.token, TSDB_ARB_TOKEN_SIZE);
    int32_t errcode = TSDB_CODE_MND_ARB_TOKEN_MISMATCH;

    SArbGroup newGroup = {0};
    bool      updateAssigned = mndUpdateArbGroupBySetAssignedLeader(&group, vgId, memberToken, errcode, &newGroup);

    EXPECT_FALSE(updateAssigned);
  }

  {  // errcode == TSDB_CODE_SUCCESS
    char memberToken[TSDB_ARB_TOKEN_SIZE] = {0};
    strncpy(memberToken, group.assignedLeader.token, TSDB_ARB_TOKEN_SIZE);
    int32_t errcode = TSDB_CODE_SUCCESS;

    SArbGroup newGroup = {0};
    bool      updateAssigned = mndUpdateArbGroupBySetAssignedLeader(&group, vgId, memberToken, errcode, &newGroup);

    EXPECT_TRUE(updateAssigned);
    EXPECT_FALSE(newGroup.isSync);
  }

  taosThreadMutexDestroy(&group.mutex);
}

#pragma GCC diagnostic pop
