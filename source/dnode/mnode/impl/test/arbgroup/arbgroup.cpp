/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

struct ScopedMutex {
  TdThreadMutex* m;
  ScopedMutex(TdThreadMutex* m) : m(m) {}
  ~ScopedMutex() {
    if (m) taosThreadMutexDestroy(m);
  }
};

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
  group.assignedLeader.assignedDnodeId = 1;
  generateArbToken(1, 5, group.assignedLeader.token);
  group.version = 2234;

  // --------------------------------------------------------------------------------
  SSdbRaw* pRaw = mndArbGroupActionEncode(&group);
  ASSERT_NE(pRaw, nullptr);

  SSdbRow* pRow = mndArbGroupActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SArbGroup* pNewGroup = (SArbGroup*)sdbGetRowObj(pRow);

  ASSERT_EQ(group.vgId, pNewGroup->vgId);
  ASSERT_EQ(group.dbUid, pNewGroup->dbUid);
  ASSERT_EQ(group.members[0].info.dnodeId, pNewGroup->members[0].info.dnodeId);
  ASSERT_EQ(group.members[1].info.dnodeId, pNewGroup->members[1].info.dnodeId);
  ASSERT_EQ(group.isSync, pNewGroup->isSync);
  ASSERT_EQ(group.assignedLeader.assignedDnodeId, pNewGroup->assignedLeader.assignedDnodeId);

  ASSERT_EQ(std::string(group.members[0].state.token), std::string(pNewGroup->members[0].state.token));
  ASSERT_EQ(std::string(group.members[1].state.token), std::string(pNewGroup->members[1].state.token));
  ASSERT_EQ(std::string(group.assignedLeader.token), std::string(pNewGroup->assignedLeader.token));
  ASSERT_EQ(group.version, pNewGroup->version);

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
  group.assignedLeader.assignedDnodeId = dnodeId;
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
    bool      updateToken = mndArbIsNeedUpdateTokenByHeartBeat(&group, &rspMember, nowMs, dnodeId, &newGroup);

    ASSERT_EQ(updateToken, false);
    ASSERT_NE(group.members[0].state.responsedHbSeq, rspMember.hbSeq);
    ASSERT_NE(group.members[0].state.lastHbMs, nowMs);
  }

  {  // old token
    SVArbHbRspMember rspMember = {0};
    rspMember.vgId = vgId;
    rspMember.hbSeq = group.members[0].state.responsedHbSeq + 1;
    strncpy(rspMember.memberToken, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);
    int32_t nowMs = group.members[0].state.lastHbMs + 10;

    SArbGroup newGroup = {0};
    bool      updateToken = mndArbIsNeedUpdateTokenByHeartBeat(&group, &rspMember, nowMs, dnodeId, &newGroup);

    ASSERT_EQ(updateToken, false);
    ASSERT_EQ(group.members[0].state.responsedHbSeq, rspMember.hbSeq);
    ASSERT_EQ(group.members[0].state.lastHbMs, nowMs);
  }

  {  // new token
    SVArbHbRspMember rspMember = {0};
    rspMember.vgId = vgId;
    rspMember.hbSeq = group.members[0].state.responsedHbSeq + 1;
    generateArbToken(dnodeId, vgId, rspMember.memberToken);
    int32_t nowMs = group.members[0].state.lastHbMs + 10;

    SArbGroup newGroup = {0};
    bool      updateToken = mndArbIsNeedUpdateTokenByHeartBeat(&group, &rspMember, nowMs, dnodeId, &newGroup);

    ASSERT_EQ(updateToken, true);
    ASSERT_EQ(group.members[0].state.responsedHbSeq, rspMember.hbSeq);
    ASSERT_EQ(group.members[0].state.lastHbMs, nowMs);

    ASSERT_EQ(std::string(newGroup.members[0].state.token), std::string(rspMember.memberToken));
    ASSERT_EQ(newGroup.isSync, false);
    ASSERT_EQ(newGroup.assignedLeader.assignedDnodeId, 0);
    ASSERT_EQ(std::string(newGroup.assignedLeader.token).size(), 0);
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
    bool updateIsSync = mndArbIsNeedUpdateSyncStatusByCheckSync(&group, vgId, member0Token, member1Token, newIsSync, &newGroup, 0);

    ASSERT_EQ(updateIsSync, false);
  }

  {  // newIsSync
    char member0Token[TSDB_ARB_TOKEN_SIZE] = {0};
    strncpy(member0Token, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);
    char member1Token[TSDB_ARB_TOKEN_SIZE] = {0};
    strncpy(member1Token, group.members[1].state.token, TSDB_ARB_TOKEN_SIZE);
    bool newIsSync = true;

    SArbGroup newGroup = {0};
    bool updateIsSync = mndArbIsNeedUpdateSyncStatusByCheckSync(&group, vgId, member0Token, member1Token, newIsSync, &newGroup, 0);

    ASSERT_EQ(updateIsSync, true);
    ASSERT_EQ(newGroup.isSync, true);
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
  group.assignedLeader.assignedDnodeId = dnodeId;
  strncpy(group.assignedLeader.token, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);

  taosThreadMutexInit(&group.mutex, NULL);

  // --------------------------------------------------------------------------------
  {  // token mismatch => skip
    char memberToken[TSDB_ARB_TOKEN_SIZE] = {0};
    generateArbToken(dnodeId, vgId, memberToken);
    int32_t errcode = TSDB_CODE_SUCCESS;

    SArbGroup newGroup = {0};
    bool      updateAssigned = mndArbIsNeedUpdateAssignedBySetAssignedLeader(&group, vgId, memberToken, errcode, &newGroup);

    ASSERT_EQ(updateAssigned, false);
  }

  {  // errcode != TSDB_CODE_SUCCESS
    char memberToken[TSDB_ARB_TOKEN_SIZE] = {0};
    strncpy(memberToken, group.assignedLeader.token, TSDB_ARB_TOKEN_SIZE);
    int32_t errcode = TSDB_CODE_MND_ARB_TOKEN_MISMATCH;

    SArbGroup newGroup = {0};
    bool      updateAssigned = mndArbIsNeedUpdateAssignedBySetAssignedLeader(&group, vgId, memberToken, errcode, &newGroup);

    ASSERT_EQ(updateAssigned, false);
  }

  {  // errcode == TSDB_CODE_SUCCESS
    char memberToken[TSDB_ARB_TOKEN_SIZE] = {0};
    strncpy(memberToken, group.assignedLeader.token, TSDB_ARB_TOKEN_SIZE);
    int32_t errcode = TSDB_CODE_SUCCESS;

    SArbGroup newGroup = {0};
    bool      updateAssigned = mndArbIsNeedUpdateAssignedBySetAssignedLeader(&group, vgId, memberToken, errcode, &newGroup);

    ASSERT_EQ(updateAssigned, true);
    ASSERT_EQ(newGroup.isSync, false);
  }

  taosThreadMutexDestroy(&group.mutex);
}

TEST_F(ArbgroupTest, 05_check_sync_timer) {
  const int32_t assgndDnodeId = 1;
  const int32_t vgId = 5;
  const int64_t nowMs = 173044838300;

  SArbGroup group = {0};
  group.vgId = vgId;
  group.dbUid = 1234;
  group.members[0].info.dnodeId = assgndDnodeId;
  group.members[0].state.lastHbMs = nowMs - 10;

  group.members[1].info.dnodeId = 2;
  group.members[1].state.lastHbMs = nowMs - 10;

  group.isSync = 1;
  taosThreadMutexInit(&group.mutex, NULL);

  SArbAssignedLeader assgnedLeader = {0};
  assgnedLeader.assignedDnodeId = assgndDnodeId;
  assgnedLeader.assignAcked = false;
  strncpy(assgnedLeader.token, group.members[0].state.token, TSDB_ARB_TOKEN_SIZE);

  SArbAssignedLeader noneAsgndLeader = {0};
  noneAsgndLeader.assignedDnodeId = 0;
  noneAsgndLeader.assignAcked = false;

  ECheckSyncOp op = CHECK_SYNC_NONE;
  SArbGroup    newGroup = {0};

  // 1. asgnd,sync,noAck --> send set assigned
  group.assignedLeader = assgnedLeader;
  group.assignedLeader.assignAcked = false;
  group.isSync = true;
  mndArbCheckSync(&group, nowMs, &op, &newGroup);

  ASSERT_EQ(op, CHECK_SYNC_SET_ASSIGNED_LEADER);

  // 2. asgnd,notSync,noAck --> send set assgnd
  newGroup = {0};
  group.assignedLeader = assgnedLeader;
  group.isSync = false;
  group.assignedLeader.assignAcked = false;
  mndArbCheckSync(&group, nowMs, &op, &newGroup);

  ASSERT_EQ(op, CHECK_SYNC_SET_ASSIGNED_LEADER);

  // 3. noAsgnd,notSync,noAck(init) --> check sync
  newGroup = {0};
  group.assignedLeader = noneAsgndLeader;
  group.isSync = false;
  group.assignedLeader.assignAcked = false;
  mndArbCheckSync(&group, nowMs, &op, &newGroup);

  ASSERT_EQ(op, CHECK_SYNC_CHECK_SYNC);

  // 4. noAsgnd,sync,noAck,one timeout--> update arbgroup (asgnd,sync,noAck)
  newGroup = {0};
  group.assignedLeader = noneAsgndLeader;
  group.isSync = true;
  group.assignedLeader.assignAcked = false;
  group.members[1].state.lastHbMs = nowMs - 2 * tsArbSetAssignedTimeoutSec * 1000; // member1 timeout
  mndArbCheckSync(&group, nowMs, &op, &newGroup);

  ASSERT_EQ(op, CHECK_SYNC_UPDATE);
  ASSERT_EQ(newGroup.assignedLeader.assignedDnodeId, assgndDnodeId);
  ASSERT_EQ(std::string(newGroup.assignedLeader.token), std::string(group.members[0].state.token));
  ASSERT_EQ(newGroup.isSync, true);
  ASSERT_EQ(newGroup.assignedLeader.assignAcked, false);
}

TEST_F(ArbgroupTest, 06_update_dnode_by_vgobj) {
  const int32_t vgId = 5;

  // --------------------------------------------------------------------------------
  // Case A: leader vnode (dnode 1) is migrated away to dnode 3; follower (dnode 2) stays.
  {
    SArbGroup old = {0};
    old.vgId = vgId;
    old.dbUid = 1234;
    old.members[0].info.dnodeId = 1;
    generateArbToken(1, vgId, old.members[0].state.token);
    old.members[1].info.dnodeId = 2;
    generateArbToken(2, vgId, old.members[1].state.token);
    old.isSync = 1;
    old.assignedLeader.assignedDnodeId = 1;  // leader was on dnode 1
    strncpy(old.assignedLeader.token, old.members[0].state.token, TSDB_ARB_TOKEN_SIZE);

    SVgObj vg = {0};
    vg.vgId = vgId;
    vg.replica = 2;
    vg.vnodeGid[0].dnodeId = 3;  // leader migrated 1 -> 3
    vg.vnodeGid[1].dnodeId = 2;  // follower unchanged

    SArbGroup newGroup = {0};
    int32_t   code = mndArbGroupUpdateDnodeByVgObj(&old, &vg, &newGroup);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    ASSERT_EQ(newGroup.members[0].info.dnodeId, 3);
    ASSERT_EQ(newGroup.members[1].info.dnodeId, 2);
    // token must be preserved (heartbeat self-heals the fresh vnode token)
    ASSERT_EQ(std::string(newGroup.members[0].state.token), std::string(old.members[0].state.token));
    ASSERT_EQ(std::string(newGroup.members[1].state.token), std::string(old.members[1].state.token));
    // assigned leader (dnode 1) no longer in vgroup -> reset
    ASSERT_EQ(newGroup.assignedLeader.assignedDnodeId, 0);
    ASSERT_EQ(std::string(newGroup.assignedLeader.token).size(), 0);
  }

  // --------------------------------------------------------------------------------
  // Case B: only the follower (dnode 2) is migrated to dnode 4; leader dnode 1 stays.
  {
    SArbGroup old = {0};
    old.vgId = vgId;
    old.dbUid = 1234;
    old.members[0].info.dnodeId = 1;
    generateArbToken(1, vgId, old.members[0].state.token);
    old.members[1].info.dnodeId = 2;
    generateArbToken(2, vgId, old.members[1].state.token);
    old.isSync = 1;
    old.assignedLeader.assignedDnodeId = 1;  // leader on dnode 1, unchanged
    strncpy(old.assignedLeader.token, old.members[0].state.token, TSDB_ARB_TOKEN_SIZE);

    SVgObj vg = {0};
    vg.vgId = vgId;
    vg.replica = 2;
    vg.vnodeGid[0].dnodeId = 1;  // leader unchanged
    vg.vnodeGid[1].dnodeId = 4;  // follower migrated 2 -> 4

    SArbGroup newGroup = {0};
    int32_t   code = mndArbGroupUpdateDnodeByVgObj(&old, &vg, &newGroup);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    ASSERT_EQ(newGroup.members[0].info.dnodeId, 1);
    ASSERT_EQ(newGroup.members[1].info.dnodeId, 4);
    // leader dnode still present -> assigned leader must be preserved
    ASSERT_EQ(newGroup.assignedLeader.assignedDnodeId, 1);
    ASSERT_EQ(std::string(newGroup.assignedLeader.token), std::string(old.assignedLeader.token));
  }

  // --------------------------------------------------------------------------------
  // Case C: replica != 2 must be rejected.
  {
    SArbGroup old = {0};
    old.vgId = vgId;
    old.members[0].info.dnodeId = 1;
    old.members[1].info.dnodeId = 2;

    SVgObj vg = {0};
    vg.vgId = vgId;
    vg.replica = 3;
    vg.vnodeGid[0].dnodeId = 1;
    vg.vnodeGid[1].dnodeId = 2;
    vg.vnodeGid[2].dnodeId = 3;

    SArbGroup newGroup = {0};
    int32_t   code = mndArbGroupUpdateDnodeByVgObj(&old, &vg, &newGroup);
    ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);
  }
}

TEST_F(ArbgroupTest, 07_redistribute_realigns_arbgroup_dnode) {
  const int32_t vgId = 5;

  // Simulate the live arbgroup currently stored in SDB (this is what "show arbgroups" reads).
  SArbGroup old = {0};
  old.vgId = vgId;
  old.dbUid = 1234;
  old.members[0].info.dnodeId = 1;
  generateArbToken(1, vgId, old.members[0].state.token);
  old.members[1].info.dnodeId = 2;
  generateArbToken(2, vgId, old.members[1].state.token);
  old.isSync = 1;
  old.assignedLeader.assignedDnodeId = 1;  // leader currently on dnode 1
  strncpy(old.assignedLeader.token, old.members[0].state.token, TSDB_ARB_TOKEN_SIZE);
  old.version = 10;
  taosThreadMutexInit(&old.mutex, NULL);
  ScopedMutex guard(&old.mutex);  // ensures destroy even if an ASSERT fails

  // Member tokens must be preserved by the realign helper (the fresh vnode self-heals its token via
  // heartbeat), so capture them before redistribution to assert they are not overwritten.
  const std::string oldTok0 = old.members[0].state.token;
  const std::string oldTok1 = old.members[1].state.token;

  // --------------------------------------------------------------------------------
  // Case A: redistribute migrates the LEADER vnode (dnode 1 -> dnode 3); follower (dnode 2) stays.
  {
    SVgObj vg = {0};
    vg.vgId = vgId;
    vg.replica = 2;
    vg.vnodeGid[0].dnodeId = 3;  // leader migrated 1 -> 3
    vg.vnodeGid[1].dnodeId = 2;  // follower unchanged

    SArbGroup newGroup = {0};
    ASSERT_EQ(mndArbGroupUpdateDnodeByVgObj(&old, &vg, &newGroup), TSDB_CODE_SUCCESS);

    // The pure function must compute the assigned-leader reset: the leader dnode is no longer part of
    // the vgroup, so it must be cleared in the new arbgroup state (applied under the version gate by
    // mndArbGroupActionUpdate, NOT by the unconditional realign helper).
    ASSERT_EQ(newGroup.assignedLeader.assignedDnodeId, 0);

    // Apply the commit log the same way SDB does. This is exactly the path that previously dropped
    // members[i].info.dnodeId, leaving "show arbgroups" stale after redistribute.
    mndArbGroupApplyDnodeRealign(&old, &newGroup);

    // "show arbgroups" must now report the redistributed member mapping.
    ASSERT_EQ(old.members[0].info.dnodeId, 3);
    ASSERT_EQ(old.members[1].info.dnodeId, 2);
    // The realign helper only touches members[i].info.dnodeId; assignedLeader stays intact here and is
    // updated (from newGroup) only under the version gate in mndArbGroupActionUpdate.
    ASSERT_EQ(old.assignedLeader.assignedDnodeId, 1);
    // Member tokens are preserved (not copied from newGroup).
    ASSERT_EQ(std::string(old.members[0].state.token), oldTok0);
    ASSERT_EQ(std::string(old.members[1].state.token), oldTok1);
  }

  // Reset the live object back to the pre-redistribute layout for Case B.
  old.members[0].info.dnodeId = 1;
  old.members[1].info.dnodeId = 2;
  old.assignedLeader.assignedDnodeId = 1;

  // --------------------------------------------------------------------------------
  // Case B: redistribute migrates only the FOLLOWER (dnode 2 -> dnode 4); leader dnode 1 stays.
  {
    SVgObj vg = {0};
    vg.vgId = vgId;
    vg.replica = 2;
    vg.vnodeGid[0].dnodeId = 1;  // leader unchanged
    vg.vnodeGid[1].dnodeId = 4;  // follower migrated 2 -> 4

    SArbGroup newGroup = {0};
    ASSERT_EQ(mndArbGroupUpdateDnodeByVgObj(&old, &vg, &newGroup), TSDB_CODE_SUCCESS);

    // The leader dnode is still present -> assigned leader must be preserved in the new state.
    ASSERT_EQ(newGroup.assignedLeader.assignedDnodeId, 1);

    mndArbGroupApplyDnodeRealign(&old, &newGroup);

    ASSERT_EQ(old.members[0].info.dnodeId, 1);
    ASSERT_EQ(old.members[1].info.dnodeId, 4);
    // Leader dnode is still present -> assigned leader must be preserved by the helper.
    ASSERT_EQ(old.assignedLeader.assignedDnodeId, 1);
    ASSERT_EQ(std::string(old.members[0].state.token), oldTok0);
    ASSERT_EQ(std::string(old.members[1].state.token), oldTok1);
  }

  // Reset the live object back to a layout where the assigned leader is ALREADY cleared.
  old.members[0].info.dnodeId = 1;
  old.members[1].info.dnodeId = 2;
  old.assignedLeader.assignedDnodeId = 0;  // already cleared (e.g. previous redistribute)
  memset(old.assignedLeader.token, 0, TSDB_ARB_TOKEN_SIZE);

  // --------------------------------------------------------------------------------
  // Case C: leader vnode (dnode 1 -> dnode 3) migrated while assigned leader is already 0.
  {
    SVgObj vg = {0};
    vg.vgId = vgId;
    vg.replica = 2;
    vg.vnodeGid[0].dnodeId = 3;
    vg.vnodeGid[1].dnodeId = 2;

    SArbGroup newGroup = {0};
    ASSERT_EQ(mndArbGroupUpdateDnodeByVgObj(&old, &vg, &newGroup), TSDB_CODE_SUCCESS);
    // assigned leader already 0 and still 0 after the update (boundary branch).
    ASSERT_EQ(newGroup.assignedLeader.assignedDnodeId, 0);

    mndArbGroupApplyDnodeRealign(&old, &newGroup);
    ASSERT_EQ(old.members[0].info.dnodeId, 3);
    ASSERT_EQ(old.members[1].info.dnodeId, 2);
    ASSERT_EQ(old.assignedLeader.assignedDnodeId, 0);  // unchanged by helper
    ASSERT_EQ(std::string(old.members[0].state.token), oldTok0);
    ASSERT_EQ(std::string(old.members[1].state.token), oldTok1);
  }

  // Reset back to the pre-redistribute layout for Case D.
  old.members[0].info.dnodeId = 1;
  old.members[1].info.dnodeId = 2;
  old.assignedLeader.assignedDnodeId = 1;

  // --------------------------------------------------------------------------------
  // Case D (idempotent): vnodeGid unchanged -> realign must be a no-op on dnodeId.
  {
    SVgObj vg = {0};
    vg.vgId = vgId;
    vg.replica = 2;
    vg.vnodeGid[0].dnodeId = 1;  // unchanged
    vg.vnodeGid[1].dnodeId = 2;  // unchanged

    SArbGroup newGroup = {0};
    ASSERT_EQ(mndArbGroupUpdateDnodeByVgObj(&old, &vg, &newGroup), TSDB_CODE_SUCCESS);
    ASSERT_EQ(newGroup.assignedLeader.assignedDnodeId, 1);  // leader still present

    mndArbGroupApplyDnodeRealign(&old, &newGroup);
    ASSERT_EQ(old.members[0].info.dnodeId, 1);
    ASSERT_EQ(old.members[1].info.dnodeId, 2);
    ASSERT_EQ(old.assignedLeader.assignedDnodeId, 1);
    ASSERT_EQ(std::string(old.members[0].state.token), oldTok0);
    ASSERT_EQ(std::string(old.members[1].state.token), oldTok1);
  }
}
#pragma GCC diagnostic pop
