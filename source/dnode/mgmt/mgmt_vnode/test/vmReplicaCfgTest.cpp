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

/*
 * Regression tests for TD-SEC-2026-034 / GHSA-c6c6-fm4m-fj5w:
 * SCreateVnodeReq.replica (an int8 read straight off the wire) was used as a loop bound when filling
 * SSyncCfg.nodeInfo[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA], with no upper-bound check, so a
 * replica value above the array capacity overflowed it (a stack array in vmProcessCreateVnodeReq).
 * The same counters/indexes are reused by the mount and alter-replica handlers.
 */

#include <gtest/gtest.h>

#include <limits>

extern "C" {
#include "tmsg.h"
#include "vmInt.h"
}

namespace {

constexpr int32_t kNodeInfoCapacity = TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA;

// the four counters/indexes as they reach vmIsReplicaCfgValid from a decoded request
struct ReplicaCfg {
  int32_t replica;
  int32_t selfIndex;
  int32_t learnerReplica;
  int32_t learnerSelfIndex;
};

bool IsValid(const ReplicaCfg &cfg) {
  return vmIsReplicaCfgValid(1, cfg.replica, cfg.selfIndex, cfg.learnerReplica, cfg.learnerSelfIndex);
}

// a voter-only config that the mnode would legitimately produce
ReplicaCfg VoterCfg(int32_t replica, int32_t selfIndex) { return ReplicaCfg{replica, selfIndex, 0, -1}; }

}  // namespace

// The bound enforced by the validator is only meaningful while it matches the capacity of the array the
// counters are used to fill. If someone raises TSDB_MAX_REPLICA without growing SSyncCfg.nodeInfo, the
// overflow comes back - fail here instead.
TEST(vmReplicaCfg, nodeInfoCapacityMatchesReplicaLimits) {
  SSyncCfg cfg = {};
  EXPECT_EQ(sizeof(cfg.nodeInfo) / sizeof(cfg.nodeInfo[0]), static_cast<size_t>(kNodeInfoCapacity));
  EXPECT_EQ(sizeof(((SCreateVnodeReq *)nullptr)->replicas) / sizeof(SReplica), static_cast<size_t>(TSDB_MAX_REPLICA));
  EXPECT_EQ(sizeof(((SCreateVnodeReq *)nullptr)->learnerReplicas) / sizeof(SReplica),
            static_cast<size_t>(TSDB_MAX_LEARNER_REPLICA));
}

TEST(vmReplicaCfg, acceptsConfigsProducedByMnode) {
  for (int32_t replica = 1; replica <= TSDB_MAX_REPLICA; ++replica) {
    for (int32_t selfIndex = 0; selfIndex < replica; ++selfIndex) {
      EXPECT_TRUE(IsValid(VoterCfg(replica, selfIndex))) << "replica:" << replica << " selfIndex:" << selfIndex;
    }
  }

  // this dnode is a learner of the vgroup: selfIndex is -1 and learnerSelfIndex points at the learner slot
  for (int32_t learnerReplica = 1; learnerReplica <= TSDB_MAX_LEARNER_REPLICA; ++learnerReplica) {
    for (int32_t learnerSelfIndex = 0; learnerSelfIndex < learnerReplica; ++learnerSelfIndex) {
      EXPECT_TRUE(IsValid(ReplicaCfg{1, -1, learnerReplica, learnerSelfIndex}))
          << "learnerReplica:" << learnerReplica << " learnerSelfIndex:" << learnerSelfIndex;
    }
  }

  // a fully populated vgroup: voters and learners together exactly fill nodeInfo
  EXPECT_TRUE(IsValid(ReplicaCfg{TSDB_MAX_REPLICA, TSDB_MAX_REPLICA - 1, TSDB_MAX_LEARNER_REPLICA, -1}));
  EXPECT_TRUE(IsValid(ReplicaCfg{TSDB_MAX_REPLICA, -1, TSDB_MAX_LEARNER_REPLICA, TSDB_MAX_LEARNER_REPLICA - 1}));
}

// the finding itself: replica above the nodeInfo capacity must be rejected
TEST(vmReplicaCfg, rejectsReplicaAboveArrayCapacity) {
  const int32_t oversized[] = {TSDB_MAX_REPLICA + 1, kNodeInfoCapacity, kNodeInfoCapacity + 1, 100,
                               std::numeric_limits<int8_t>::max()};
  for (int32_t replica : oversized) {
    EXPECT_FALSE(IsValid(VoterCfg(replica, 0))) << "replica:" << replica;
    EXPECT_FALSE(IsValid(ReplicaCfg{replica, -1, 1, 0})) << "replica:" << replica;
  }
}

TEST(vmReplicaCfg, rejectsNonPositiveReplica) {
  EXPECT_FALSE(IsValid(VoterCfg(0, -1)));
  EXPECT_FALSE(IsValid(ReplicaCfg{0, -1, 1, 0}));
  EXPECT_FALSE(IsValid(VoterCfg(-1, 0)));
  EXPECT_FALSE(IsValid(VoterCfg(std::numeric_limits<int8_t>::min(), 0)));
}

// learnerReplica is the bound of the second nodeInfo fill loop, so it needs the same treatment
TEST(vmReplicaCfg, rejectsLearnerReplicaOutOfRange) {
  const int32_t oversized[] = {TSDB_MAX_LEARNER_REPLICA + 1, kNodeInfoCapacity, 100,
                               std::numeric_limits<int8_t>::max()};
  for (int32_t learnerReplica : oversized) {
    EXPECT_FALSE(IsValid(ReplicaCfg{1, 0, learnerReplica, -1})) << "learnerReplica:" << learnerReplica;
  }
  EXPECT_FALSE(IsValid(ReplicaCfg{1, 0, -1, -1}));
  EXPECT_FALSE(IsValid(ReplicaCfg{1, 0, std::numeric_limits<int8_t>::min(), -1}));
}

// selfIndex indexes replicas[] directly
TEST(vmReplicaCfg, rejectsSelfIndexOutOfRange) {
  EXPECT_FALSE(IsValid(VoterCfg(1, 1)));
  EXPECT_FALSE(IsValid(VoterCfg(3, 3)));
  EXPECT_FALSE(IsValid(VoterCfg(TSDB_MAX_REPLICA, TSDB_MAX_REPLICA)));
  EXPECT_FALSE(IsValid(VoterCfg(1, std::numeric_limits<int8_t>::max())));
  EXPECT_FALSE(IsValid(VoterCfg(1, -2)));
  EXPECT_FALSE(IsValid(VoterCfg(1, std::numeric_limits<int8_t>::min())));
}

// learnerSelfIndex indexes learnerReplicas[] directly
TEST(vmReplicaCfg, rejectsLearnerSelfIndexOutOfRange) {
  EXPECT_FALSE(IsValid(ReplicaCfg{1, -1, 1, 1}));
  EXPECT_FALSE(IsValid(ReplicaCfg{1, -1, TSDB_MAX_LEARNER_REPLICA, TSDB_MAX_LEARNER_REPLICA}));
  EXPECT_FALSE(IsValid(ReplicaCfg{1, -1, 1, std::numeric_limits<int8_t>::max()}));
  EXPECT_FALSE(IsValid(ReplicaCfg{1, -1, 1, -2}));
  // no learners at all: learnerSelfIndex must stay unset
  EXPECT_FALSE(IsValid(ReplicaCfg{1, 0, 0, 0}));
}

// with both indexes unset the handlers would fall through to learnerReplicas[-1]
TEST(vmReplicaCfg, rejectsRequestThatRefersToNoLocalReplica) {
  EXPECT_FALSE(IsValid(ReplicaCfg{1, -1, 0, -1}));
  EXPECT_FALSE(IsValid(ReplicaCfg{TSDB_MAX_REPLICA, -1, TSDB_MAX_LEARNER_REPLICA, -1}));
}

// The decoder reads a fixed TSDB_MAX_REPLICA entries and never compares them against the counter, so an
// oversized replica survives the round trip untouched: the handlers are the only place that can stop it.
TEST(vmReplicaCfg, oversizedReplicaSurvivesCreateVnodeReqWire) {
  SCreateVnodeReq req = {};
  req.vgId = 2;
  req.replica = std::numeric_limits<int8_t>::max();
  req.selfIndex = 0;
  req.learnerReplica = std::numeric_limits<int8_t>::max();
  req.learnerSelfIndex = -1;

  int32_t contLen = tSerializeSCreateVnodeReq(NULL, 0, &req);
  ASSERT_GT(contLen, 0);
  void *pCont = taosMemoryCalloc(1, contLen);
  ASSERT_NE(pCont, nullptr);
  ASSERT_GT(tSerializeSCreateVnodeReq(pCont, contLen, &req), 0);

  SCreateVnodeReq decoded = {};
  ASSERT_EQ(tDeserializeSCreateVnodeReq(pCont, contLen, &decoded), 0);
  EXPECT_EQ(static_cast<int32_t>(decoded.replica), std::numeric_limits<int8_t>::max());
  EXPECT_EQ(static_cast<int32_t>(decoded.learnerReplica), std::numeric_limits<int8_t>::max());

  EXPECT_FALSE(vmIsReplicaCfgValid(decoded.vgId, decoded.replica, decoded.selfIndex, decoded.learnerReplica,
                                   decoded.learnerSelfIndex));

  (void)tFreeSCreateVnodeReq(&decoded);
  taosMemoryFree(pCont);
}

TEST(vmReplicaCfg, oversizedReplicaSurvivesAlterVnodeReplicaReqWire) {
  SAlterVnodeReplicaReq req = {};
  req.vgId = 2;
  req.replica = std::numeric_limits<int8_t>::max();
  req.selfIndex = TSDB_MAX_REPLICA;  // past replicas[], but below the forged replica count
  req.learnerReplica = 0;
  req.learnerSelfIndex = -1;

  int32_t contLen = tSerializeSAlterVnodeReplicaReq(NULL, 0, &req);
  ASSERT_GT(contLen, 0);
  void *pCont = taosMemoryCalloc(1, contLen);
  ASSERT_NE(pCont, nullptr);
  ASSERT_GT(tSerializeSAlterVnodeReplicaReq(pCont, contLen, &req), 0);

  SAlterVnodeReplicaReq decoded = {};
  ASSERT_EQ(tDeserializeSAlterVnodeReplicaReq(pCont, contLen, &decoded), 0);
  EXPECT_EQ(static_cast<int32_t>(decoded.replica), std::numeric_limits<int8_t>::max());
  EXPECT_EQ(static_cast<int32_t>(decoded.selfIndex), TSDB_MAX_REPLICA);

  EXPECT_FALSE(vmIsReplicaCfgValid(decoded.vgId, decoded.replica, decoded.selfIndex, decoded.learnerReplica,
                                   decoded.learnerSelfIndex));

  taosMemoryFree(pCont);
}

// vnodeAlterReplica() fills nodeInfo with the same unchecked counters; it must refuse an oversized request
// before it even reads the on-disk vnode config.
TEST(vmReplicaCfg, vnodeAlterReplicaRejectsOversizedReplica) {
  SAlterVnodeReplicaReq req = {};
  req.vgId = 2;
  req.replica = std::numeric_limits<int8_t>::max();
  req.selfIndex = 0;

  terrno = 0;
  EXPECT_LT(vnodeAlterReplica("vnode/vnode2", &req, 0, NULL), 0);
  EXPECT_EQ(terrno, TSDB_CODE_INVALID_MSG);

  req.replica = 1;
  req.learnerReplica = std::numeric_limits<int8_t>::max();
  terrno = 0;
  EXPECT_LT(vnodeAlterReplica("vnode/vnode2", &req, 0, NULL), 0);
  EXPECT_EQ(terrno, TSDB_CODE_INVALID_MSG);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
