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

// Input-validation tests for wire-supplied data handled by the vnode:
// alter-replica counts, sync config node arrays and blob submit descriptors.

#include <cstdint>
#include <cstring>

#include "gtest/gtest.h"

#include "os.h"

extern "C" {
#include "monitor.h"
#include "taoserror.h"
#include "tdef.h"
#include "tjson.h"
#include "tmsg.h"
#include "vnd.h"
#include "vnodeInt.h"
}

SDmNotifyHandle dmNotifyHdl = {};

TEST(VnodeAlterReplicaValidation, OutOfRangeCountsRejectedBeforeAnyUse) {
  // path and tfs are NULL on purpose: validation must fire before either is touched
  SAlterVnodeReplicaReq req = {};
  req.vgId = 2;
  req.replica = 127;
  req.learnerReplica = 0;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeAlterReplica(NULL, &req, 0, NULL));

  req.replica = 3;
  req.learnerReplica = 127;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeAlterReplica(NULL, &req, 0, NULL));

  req.replica = TSDB_MAX_REPLICA;
  req.learnerReplica = TSDB_MAX_LEARNER_REPLICA + 1;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeAlterReplica(NULL, &req, 0, NULL));

  req.replica = 0;
  req.learnerReplica = 0;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeAlterReplica(NULL, &req, 0, NULL));

  req.replica = -1;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeAlterReplica(NULL, &req, 0, NULL));

  req.replica = 3;
  req.learnerReplica = -5;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeAlterReplica(NULL, &req, 0, NULL));

  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeAlterReplica(NULL, NULL, 0, NULL));
}

namespace {

void fillCfg(SVnodeCfg* pCfg, int32_t nNodes) {
  memset(pCfg, 0, sizeof(*pCfg));
  pCfg->vgId = 2;
  tstrncpy(pCfg->dbname, "db", sizeof(pCfg->dbname));
  pCfg->syncCfg.replicaNum = 3;
  pCfg->syncCfg.myIndex = 0;
  pCfg->syncCfg.totalReplicaNum = nNodes;
  for (int32_t i = 0; i < nNodes; ++i) {
    pCfg->syncCfg.nodeInfo[i].nodePort = (uint16_t)(6030 + i);
    pCfg->syncCfg.nodeInfo[i].nodeId = i + 1;
    tstrncpy(pCfg->syncCfg.nodeInfo[i].nodeFqdn, "localhost", sizeof(pCfg->syncCfg.nodeInfo[i].nodeFqdn));
  }
}

}  // namespace

TEST(VnodeDecodeConfigValidation, NodeInfoArrayLargerThanFixedArrayRejected) {
  const int32_t kMax = TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA;

  SVnodeCfg cfg;
  fillCfg(&cfg, kMax);

  SJson* pRoot = tjsonCreateObject();
  ASSERT_NE(nullptr, pRoot);
  ASSERT_EQ(0, tjsonAddObject(pRoot, "config", vnodeEncodeConfig, &cfg));

  // a config that fills the array exactly is not rejected by the size check
  {
    SVnodeCfg out;
    memset(&out, 0, sizeof(out));
    EXPECT_NE(TSDB_CODE_INVALID_MSG, tjsonToObject(pRoot, "config", vnodeDecodeConfig, &out));
    EXPECT_EQ(kMax, out.syncCfg.totalReplicaNum);
  }

  // one node more than the fixed array can hold (the 16th entry)
  SJson* pCfgObj = tjsonGetObjectItem(pRoot, "config");
  ASSERT_NE(nullptr, pCfgObj);
  SJson* pNodes = tjsonGetObjectItem(pCfgObj, "syncCfg.nodeInfo");
  ASSERT_NE(nullptr, pNodes);
  SJson* pExtra = tjsonCreateObject();
  ASSERT_NE(nullptr, pExtra);
  ASSERT_EQ(0, tjsonAddItemToArray(pNodes, pExtra));
  ASSERT_EQ(kMax + 1, tjsonGetArraySize(pNodes));

  SVnodeCfg out;
  memset(&out, 0, sizeof(out));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tjsonToObject(pRoot, "config", vnodeDecodeConfig, &out));

  tjsonDelete(pRoot);
}

TEST(VnodeDecodeConfigValidation, BadReplicaNumAndMyIndexRejected) {
  SVnodeCfg cfg;
  fillCfg(&cfg, 3);
  cfg.syncCfg.replicaNum = 127;
  SJson* pRoot = tjsonCreateObject();
  ASSERT_NE(nullptr, pRoot);
  ASSERT_EQ(0, tjsonAddObject(pRoot, "config", vnodeEncodeConfig, &cfg));
  SVnodeCfg out;
  memset(&out, 0, sizeof(out));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tjsonToObject(pRoot, "config", vnodeDecodeConfig, &out));
  tjsonDelete(pRoot);

  fillCfg(&cfg, 3);
  cfg.syncCfg.myIndex = 100;
  pRoot = tjsonCreateObject();
  ASSERT_NE(nullptr, pRoot);
  ASSERT_EQ(0, tjsonAddObject(pRoot, "config", vnodeEncodeConfig, &cfg));
  memset(&out, 0, sizeof(out));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tjsonToObject(pRoot, "config", vnodeDecodeConfig, &out));
  tjsonDelete(pRoot);
}

TEST(VnodeBlobValidation, SequenceSlotMustFitInsideRow) {
  const int64_t seqSize = (int64_t)sizeof(uint64_t);

  EXPECT_EQ(0, vnodeCheckBlobSeqSlot(seqSize, 0));
  EXPECT_EQ(0, vnodeCheckBlobSeqSlot(100, 100 - seqSize));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeCheckBlobSeqSlot(100, 100 - seqSize + 1));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeCheckBlobSeqSlot(100, 100));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeCheckBlobSeqSlot(seqSize - 1, 0));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeCheckBlobSeqSlot(0, 0));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeCheckBlobSeqSlot(-1, 0));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeCheckBlobSeqSlot(100, UINT64_MAX));
}

extern "C" int32_t syncSnapCheckPrepData(const void *data, uint32_t dataLen);

TEST(VnodeRefillMsgBody, GrowsBufferWhenRebuiltBodyIsLarger) {
  const int32_t hdr = (int32_t)sizeof(SMsgHead);
  SRpcMsg       msg = {};
  msg.contLen = hdr + 4;
  msg.pCont = rpcMallocCont(msg.contLen);
  ASSERT_NE(msg.pCont, nullptr);
  SMsgHead *pHead = (SMsgHead *)msg.pCont;
  pHead->vgId = 7;
  pHead->contLen = 99;
  memset(POINTER_SHIFT(msg.pCont, hdr), 0x11, 4);

  uint8_t body[32];
  for (int32_t i = 0; i < (int32_t)sizeof(body); i++) body[i] = (uint8_t)i;
  ASSERT_EQ(0, vnodeRefillMsgBody(&msg, body, sizeof(body)));
  EXPECT_EQ(msg.contLen, hdr + (int32_t)sizeof(body));
  EXPECT_EQ(((SMsgHead *)msg.pCont)->vgId, 7);  // header is carried over
  EXPECT_EQ(0, memcmp(POINTER_SHIFT(msg.pCont, hdr), body, sizeof(body)));
  rpcFreeCont(msg.pCont);
}

TEST(VnodeRefillMsgBody, ShrinksInPlaceWhenRebuiltBodyIsSmaller) {
  const int32_t hdr = (int32_t)sizeof(SMsgHead);
  SRpcMsg       msg = {};
  msg.contLen = hdr + 16;
  msg.pCont = rpcMallocCont(msg.contLen);
  ASSERT_NE(msg.pCont, nullptr);
  void *before = msg.pCont;

  uint8_t body[4] = {1, 2, 3, 4};
  ASSERT_EQ(0, vnodeRefillMsgBody(&msg, body, sizeof(body)));
  EXPECT_EQ(msg.pCont, before);
  EXPECT_EQ(msg.contLen, hdr + (int32_t)sizeof(body));
  EXPECT_EQ(0, memcmp(POINTER_SHIFT(msg.pCont, hdr), body, sizeof(body)));

  ASSERT_EQ(0, vnodeRefillMsgBody(&msg, NULL, 0));  // empty rebuilt body is valid
  EXPECT_EQ(msg.contLen, hdr);
  rpcFreeCont(msg.pCont);
}

TEST(VnodeRefillMsgBody, InvalidArgsRejected) {
  SRpcMsg msg = {};
  uint8_t body[4] = {};
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeRefillMsgBody(NULL, body, sizeof(body)));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeRefillMsgBody(&msg, body, sizeof(body)));  // no content buffer
  msg.contLen = (int32_t)sizeof(SMsgHead) + 4;
  msg.pCont = rpcMallocCont(msg.contLen);
  ASSERT_NE(msg.pCont, nullptr);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeRefillMsgBody(&msg, body, -1));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeRefillMsgBody(&msg, NULL, 4));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeRefillMsgBody(&msg, body, INT32_MAX));
  rpcFreeCont(msg.pCont);
}

TEST(SyncSnapshotPrepData, TlvHeaderMustMatchPayload) {
  // SSyncTLV is {int32 typ; int32 len; char val[]}, build the 8 byte header by hand (flexible array member)
  uint8_t buf[64] = {0};
  struct {
    int32_t typ;
    int32_t len;
  } head = {1, 0};
  static_assert(sizeof(head) == sizeof(SSyncTLV), "SSyncTLV header layout changed");

  EXPECT_EQ(TSDB_CODE_INVALID_DATA_FMT, syncSnapCheckPrepData(NULL, 16));
  EXPECT_EQ(TSDB_CODE_INVALID_DATA_FMT, syncSnapCheckPrepData(buf, 0));
  EXPECT_EQ(TSDB_CODE_INVALID_DATA_FMT, syncSnapCheckPrepData(buf, sizeof(SSyncTLV) - 1));

  head.len = 0;
  memcpy(buf, &head, sizeof(head));
  EXPECT_EQ(0, syncSnapCheckPrepData(buf, sizeof(SSyncTLV)));

  head.len = 16;
  memcpy(buf, &head, sizeof(head));
  EXPECT_EQ(0, syncSnapCheckPrepData(buf, sizeof(SSyncTLV) + 16));
  EXPECT_EQ(TSDB_CODE_INVALID_DATA_FMT, syncSnapCheckPrepData(buf, sizeof(SSyncTLV) + 15));

  head.len = -1;
  memcpy(buf, &head, sizeof(head));
  EXPECT_EQ(TSDB_CODE_INVALID_DATA_FMT, syncSnapCheckPrepData(buf, sizeof(buf)));
  head.len = INT32_MAX;
  memcpy(buf, &head, sizeof(head));
  EXPECT_EQ(TSDB_CODE_INVALID_DATA_FMT, syncSnapCheckPrepData(buf, sizeof(buf)));

  // the payload may sit at an odd address
  head.len = 8;
  memcpy(buf + 1, &head, sizeof(head));
  EXPECT_EQ(0, syncSnapCheckPrepData(buf + 1, sizeof(SSyncTLV) + 8));
}
