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

/**
 * Regression test for the batch-meta-txn super-table ALTER path.
 *
 * Bug: metaAlterSuperTable() correctly preserved the ORIGINAL pre-txn version in
 * entry.txnOrigVer and wrote it to the B+ tree entry, but the txn.idx upsert was
 * fed pEntry->version (the intermediate in-txn version) instead. On a REPEATED
 * ALTER of the same STB within one txn, txn.idx therefore recorded an intermediate
 * version. The snapshot reader (metaSnapshotReaderOpen -> metaScanTxnEntries) treats
 * txn.idx's txnOrigVer as authoritative for lowering `sver` and building the
 * prev-version rescue map, so a follower catching up via snapshot during a pending
 * txn would get the wrong/missing pre-version rows.
 *
 * This test drives the REAL metaAlterSuperTable twice under one txnId and asserts
 * that metaScanTxnEntries reports the STB's txnOrigVer as the ORIGINAL pre-txn
 * version (V0), not the intermediate. It fails on the pre-fix code and passes after.
 */

#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <string>

#include "gtest/gtest.h"
#include "stub.h"

extern "C" {
#include "meta.h"
#include "taoserror.h"
#include "tarray.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "vnd.h"
#include "vnodeInt.h"
}

// Referenced by the meta audit-notify path (dmNotifyHdl.state stays 0 => no-op).
SDmNotifyHandle dmNotifyHdl = {};

namespace {

const char* kTestDir = "/tmp/td_meta_txn_alter_stb_test";
const int64_t kSuid = 700001;
const char* kStbName = "stb_txn_alter";
const int64_t kTxnId = 42;

SRpcMsg gDirectMetaRsp = {};
int32_t gDirectMetaRspCount = 0;

void captureDirectMetaRsp(SRpcMsg* pMsg) {
  gDirectMetaRsp = *pMsg;
  ++gDirectMetaRspCount;
}

// Build a minimal 2-column (ts, v) row schema; nCols columns total.
void fillRowSchema(SSchemaWrapper* sw, int32_t nCols, int32_t schemaVer) {
  sw->nCols = nCols;
  sw->version = schemaVer;
  sw->pRsma = nullptr;
  sw->pSchema = (SSchema*)taosMemoryCalloc(nCols, sizeof(SSchema));
  // col 0: ts (timestamp, primary key)
  sw->pSchema[0].type = TSDB_DATA_TYPE_TIMESTAMP;
  sw->pSchema[0].flags = 0;
  sw->pSchema[0].colId = 1;
  sw->pSchema[0].bytes = 8;
  tstrncpy(sw->pSchema[0].name, "ts", TSDB_COL_NAME_LEN);
  for (int32_t i = 1; i < nCols; i++) {
    sw->pSchema[i].type = TSDB_DATA_TYPE_INT;
    sw->pSchema[i].flags = 0;
    sw->pSchema[i].colId = (col_id_t)(i + 1);
    sw->pSchema[i].bytes = 4;
    snprintf(sw->pSchema[i].name, TSDB_COL_NAME_LEN, "c%d", i);
  }
}

void fillTagSchema(SSchemaWrapper* sw, int32_t schemaVer) {
  sw->nCols = 1;
  sw->version = schemaVer;
  sw->pRsma = nullptr;
  sw->pSchema = (SSchema*)taosMemoryCalloc(1, sizeof(SSchema));
  sw->pSchema[0].type = TSDB_DATA_TYPE_INT;
  sw->pSchema[0].flags = 0;
  sw->pSchema[0].colId = 100;
  sw->pSchema[0].bytes = 4;
  tstrncpy(sw->pSchema[0].name, "t1", TSDB_COL_NAME_LEN);
}

void fillColCmpr(SColCmprWrapper* cw, const SSchemaWrapper* row) {
  cw->nCols = row->nCols;
  cw->version = row->version;
  cw->pColCmpr = (SColCmpr*)taosMemoryCalloc(row->nCols, sizeof(SColCmpr));
  for (int32_t i = 0; i < row->nCols; i++) {
    cw->pColCmpr[i].id = row->pSchema[i].colId;
    cw->pColCmpr[i].alg = createDefaultColCmprByType(row->pSchema[i].type);
  }
}

// Build an SVCreateStbReq with `nRowCols` row columns (ts + nRowCols-1 data cols).
void buildStbReq(SVCreateStbReq* req, int32_t nRowCols, int32_t schemaVer, txn_id_t txnId) {
  memset(req, 0, sizeof(*req));
  req->name = (char*)kStbName;
  req->suid = kSuid;
  req->colCmpred = 1;
  req->txnId = txnId;
  fillRowSchema(&req->schemaRow, nRowCols, schemaVer);
  fillTagSchema(&req->schemaTag, schemaVer);
  fillColCmpr(&req->colCmpr, &req->schemaRow);
}

void freeStbReq(SVCreateStbReq* req) {
  taosMemoryFreeClear(req->schemaRow.pSchema);
  taosMemoryFreeClear(req->schemaTag.pSchema);
  taosMemoryFreeClear(req->colCmpr.pColCmpr);
}

// Look up the scanned txn.idx entry for kSuid; returns txnOrigVer or INT64_MIN if absent.
int64_t scanStbTxnOrigVer(SMeta* pMeta, int8_t* pStatusOut) {
  SArray* pArr = nullptr;
  int32_t code = metaScanTxnEntries(pMeta, &pArr);
  if (code != 0 || pArr == nullptr) return INT64_MIN;
  int64_t found = INT64_MIN;
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pArr); i++) {
    SMetaTxnScanEntry* e = (SMetaTxnScanEntry*)taosArrayGet(pArr, i);
    if (e->uid == kSuid) {
      found = e->txnOrigVer;
      if (pStatusOut) *pStatusOut = e->txnStatus;
      break;
    }
  }
  taosArrayDestroy(pArr);
  return found;
}

}  // namespace

TEST(VnodeTableMeta, DirectRequestUsesResponseMessageType) {
  SVnode vnode = {};
  vnode.config.vgId = 1;
  vnode.config.szPage = 4096;
  vnode.config.szCache = 256;
  vnode.path = const_cast<char*>(kTestDir);

  taosRemoveDir(kTestDir);
  taosMkDir(kTestDir);

  SMeta* pMeta = nullptr;
  ASSERT_EQ(0, metaOpen(&vnode, &pMeta, 0));
  ASSERT_NE(nullptr, pMeta);
  vnode.pMeta = pMeta;
  ASSERT_EQ(0, metaBegin(pMeta, META_BEGIN_HEAP_OS));
  SVCreateStbReq createReq = {};
  buildStbReq(&createReq, 2, 0, 0);
  int32_t code = metaCreateSuperTable(pMeta, 100, &createReq);
  freeStbReq(&createReq);
  ASSERT_EQ(0, code);
  ASSERT_EQ(0, metaCommit(pMeta, pMeta->txn));

  STableInfoReq infoReq = {};
  infoReq.option = REQ_OPT_TBUID;
  snprintf(infoReq.tbName, sizeof(infoReq.tbName), "%" PRId64, kSuid);
  int32_t reqLen = tSerializeSTableInfoReq(nullptr, 0, &infoReq);
  ASSERT_GT(reqLen, 0);
  void* pReq = taosMemoryMalloc(reqLen);
  ASSERT_NE(nullptr, pReq);
  ASSERT_EQ(reqLen, tSerializeSTableInfoReq(pReq, reqLen, &infoReq));
  SRpcMsg msg = {};
  msg.msgType = TDMT_VND_TABLE_META;
  msg.pCont = pReq;
  msg.contLen = reqLen;

  gDirectMetaRsp = {};
  gDirectMetaRspCount = 0;
  {
    Stub stub;
    stub.set(tmsgSendRsp, captureDirectMetaRsp);
    EXPECT_EQ(TSDB_CODE_SUCCESS, vnodeGetTableMeta(&vnode, &msg, true));
  }
  ASSERT_EQ(1, gDirectMetaRspCount);
  EXPECT_EQ(TDMT_VND_TABLE_META_RSP, gDirectMetaRsp.msgType);
  EXPECT_EQ(TSDB_CODE_SUCCESS, gDirectMetaRsp.code);
  ASSERT_NE(nullptr, gDirectMetaRsp.pCont);

  STableMetaRsp metaRsp = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTableMetaRsp(gDirectMetaRsp.pCont, gDirectMetaRsp.contLen, &metaRsp));
  EXPECT_EQ(kSuid, metaRsp.suid);
  EXPECT_EQ(TSDB_SUPER_TABLE, metaRsp.tableType);
  EXPECT_STREQ(kStbName, metaRsp.tbName);
  tFreeSTableMetaRsp(&metaRsp);
  rpcFreeCont(gDirectMetaRsp.pCont);
  taosMemoryFree(pReq);
  metaClose(&pMeta);
  taosRemoveDir(kTestDir);
}

// ============================================================================
// Repeated STB ALTER within one txn must record the ORIGINAL pre-txn version in
// txn.idx (the snapshot reader's authoritative source), not the intermediate one.
// ============================================================================
TEST(MetaTxnAlterStb, RepeatedAlterRecordsOriginalPrevVerInTxnIdx) {
  // Minimal SVnode: metaOpenImpl needs config.{szPage,szCache,tdbEncryptData};
  // the STB handlers need config.vgId and cacheLast==0 (skip tsdb cache path).
  SVnode vnode;
  memset(&vnode, 0, sizeof(vnode));
  vnode.config.vgId = 1;
  vnode.config.szPage = 4096;
  vnode.config.szCache = 256;
  vnode.config.cacheLast = 0;  // TSDB_CACHE_NO => STB-update skips pTsdb deref
  memset(&vnode.config.tdbEncryptData, 0, sizeof(vnode.config.tdbEncryptData));

  taosRemoveDir(kTestDir);
  taosMkDir(kTestDir);
  vnode.path = (char*)kTestDir;

  SMeta* pMeta = nullptr;
  ASSERT_EQ(metaOpen(&vnode, &pMeta, 0), 0);
  ASSERT_NE(pMeta, nullptr);
  vnode.pMeta = pMeta;

  // ── V100: CREATE the super table (committed baseline, no txn marker) ──
  const int64_t V0 = 100;
  {
    ASSERT_EQ(metaBegin(pMeta, META_BEGIN_HEAP_OS), 0);
    SVCreateStbReq req;
    buildStbReq(&req, /*nRowCols=*/2, /*schemaVer=*/0, /*txnId=*/0);
    int32_t code = metaCreateSuperTable(pMeta, V0, &req);
    freeStbReq(&req);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(metaCommit(pMeta, pMeta->txn), 0);
  }

  // ── V101: first in-txn ALTER (add column c2) → PRE_ALTER, txnOrigVer should be V0 ──
  const int64_t V1 = 101;
  {
    ASSERT_EQ(metaBegin(pMeta, META_BEGIN_HEAP_OS), 0);
    SVCreateStbReq req;
    buildStbReq(&req, /*nRowCols=*/3, /*schemaVer=*/1, /*txnId=*/kTxnId);
    int32_t code = metaAlterSuperTable(pMeta, V1, &req);
    freeStbReq(&req);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(metaCommit(pMeta, pMeta->txn), 0);
  }

  int8_t  st1 = -1;
  int64_t origAfter1 = scanStbTxnOrigVer(pMeta, &st1);
  EXPECT_EQ(st1, META_TXN_PRE_ALTER);
  EXPECT_EQ(origAfter1, V0) << "after 1st ALTER, txn.idx txnOrigVer must be the pre-txn version";

  // ── V102: second in-txn ALTER (add column c3) → still same txn ──
  // pEntry->version is now V1 (intermediate). The buggy code fed V1 to txn.idx;
  // the fix feeds the preserved entry.txnOrigVer (== V0).
  const int64_t V2 = 102;
  {
    ASSERT_EQ(metaBegin(pMeta, META_BEGIN_HEAP_OS), 0);
    SVCreateStbReq req;
    buildStbReq(&req, /*nRowCols=*/4, /*schemaVer=*/2, /*txnId=*/kTxnId);
    int32_t code = metaAlterSuperTable(pMeta, V2, &req);
    freeStbReq(&req);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(metaCommit(pMeta, pMeta->txn), 0);
  }

  int8_t  st2 = -1;
  int64_t origAfter2 = scanStbTxnOrigVer(pMeta, &st2);
  EXPECT_EQ(st2, META_TXN_PRE_ALTER);
  // The core assertion: repeated ALTER must NOT overwrite txn.idx with the
  // intermediate version V1. It must stay the original pre-txn version V0.
  EXPECT_EQ(origAfter2, V0)
      << "REGRESSION: repeated STB ALTER recorded intermediate version in txn.idx "
         "instead of the original pre-txn version (snapshot sync would ship the wrong prev-version)";
  EXPECT_NE(origAfter2, V1) << "txn.idx must not hold the intermediate in-txn version";

  metaClose(&pMeta);
  taosRemoveDir(kTestDir);
}

// ============================================================================
// Security hardening: wire-supplied schemas / suid / colRef strings must be
// rejected instead of being persisted or copied into fixed-size buffers.
// ============================================================================
extern "C" int meteDecodeColRefEntry(SDecoder* pDecoder, SMetaEntry* pME);

namespace {

class MetaStbHardeningTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memset(&vnode_, 0, sizeof(vnode_));
    vnode_.config.vgId = 1;
    vnode_.config.szPage = 4096;
    vnode_.config.szCache = 256;
    vnode_.config.cacheLast = 0;
    taosRemoveDir(kTestDir);
    taosMkDir(kTestDir);
    vnode_.path = (char*)kTestDir;
    ASSERT_EQ(0, metaOpen(&vnode_, &pMeta_, 0));
    ASSERT_NE(nullptr, pMeta_);
    vnode_.pMeta = pMeta_;
    ASSERT_EQ(0, metaBegin(pMeta_, META_BEGIN_HEAP_OS));
  }

  void TearDown() override {
    metaClose(&pMeta_);
    taosRemoveDir(kTestDir);
  }

  int32_t createStb(int32_t nRowCols, int64_t version) {
    SVCreateStbReq req;
    buildStbReq(&req, nRowCols, 0, 0);
    int32_t code = metaCreateSuperTable(pMeta_, version, &req);
    freeStbReq(&req);
    return code;
  }

  SVnode vnode_;
  SMeta* pMeta_ = nullptr;
};

}  // namespace

TEST_F(MetaStbHardeningTest, AlterWithForeignSuidRejected) {
  ASSERT_EQ(0, createStb(2, 100));

  SVCreateStbReq req;
  buildStbReq(&req, 3, 1, 0);
  req.suid = kSuid + 1;  // names the existing table but carries another table's suid
  int32_t code = metaAlterSuperTable(pMeta_, 101, &req);
  freeStbReq(&req);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, code);

  // the matching suid is still accepted
  buildStbReq(&req, 3, 1, 0);
  code = metaAlterSuperTable(pMeta_, 102, &req);
  freeStbReq(&req);
  EXPECT_EQ(0, code);
}

TEST_F(MetaStbHardeningTest, CreateWithInvalidColumnTypeRejected) {
  const int8_t badTypes[] = {(int8_t)-1, (int8_t)TSDB_DATA_TYPE_MAX, (int8_t)100, (int8_t)-128};
  int64_t      version = 100;
  for (size_t i = 0; i < sizeof(badTypes) / sizeof(badTypes[0]); ++i) {
    SVCreateStbReq req;
    buildStbReq(&req, 3, 0, 0);
    req.schemaRow.pSchema[1].type = badTypes[i];
    EXPECT_EQ(TSDB_CODE_INVALID_MSG, metaCreateSuperTable(pMeta_, version++, &req)) << "row type " << (int)badTypes[i];
    freeStbReq(&req);

    buildStbReq(&req, 3, 0, 0);
    req.schemaTag.pSchema[0].type = badTypes[i];
    EXPECT_EQ(TSDB_CODE_INVALID_MSG, metaCreateSuperTable(pMeta_, version++, &req)) << "tag type " << (int)badTypes[i];
    freeStbReq(&req);
  }

  // an empty row schema is rejected as well
  SVCreateStbReq req;
  buildStbReq(&req, 3, 0, 0);
  req.schemaRow.nCols = 0;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, metaCreateSuperTable(pMeta_, version++, &req));
  req.schemaRow.nCols = 3;
  freeStbReq(&req);

  // nothing was created: a valid request still succeeds afterwards
  EXPECT_EQ(0, createStb(3, version));
}

TEST_F(MetaStbHardeningTest, AlterWithInvalidColumnTypeRejected) {
  ASSERT_EQ(0, createStb(2, 100));
  SVCreateStbReq req;
  buildStbReq(&req, 3, 1, 0);
  req.schemaRow.pSchema[2].type = 100;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, metaAlterSuperTable(pMeta_, 101, &req));
  freeStbReq(&req);
}

TEST(MetaColRefDecode, OversizedColRefStringRejected) {
  const int32_t kLen = 600;  // far larger than refDbName[TSDB_DB_NAME_LEN]
  std::string   longStr(kLen, 'x');

  for (int field = 0; field < 3; ++field) {
    uint8_t  buf[4096] = {0};
    SEncoder enc;
    tEncoderInit(&enc, buf, sizeof(buf));
    ASSERT_EQ(0, tEncodeI32v(&enc, 1));  // nCols
    ASSERT_EQ(0, tEncodeI32v(&enc, 1));  // version
    ASSERT_EQ(0, tEncodeI8(&enc, 1));    // hasRef
    ASSERT_EQ(0, tEncodeI16v(&enc, 1));  // id
    ASSERT_EQ(0, tEncodeCStr(&enc, field == 0 ? longStr.c_str() : "db"));
    ASSERT_EQ(0, tEncodeCStr(&enc, field == 1 ? longStr.c_str() : "tb"));
    ASSERT_EQ(0, tEncodeCStr(&enc, field == 2 ? longStr.c_str() : "col"));
    uint32_t len = enc.pos;
    tEncoderClear(&enc);

    SDecoder   dec;
    SMetaEntry entry = {};
    tDecoderInit(&dec, buf, len);
    EXPECT_NE(0, meteDecodeColRefEntry(&dec, &entry)) << "field " << field;
    tDecoderClear(&dec);
  }

  // well-formed strings still decode
  uint8_t  buf[256] = {0};
  SEncoder enc;
  tEncoderInit(&enc, buf, sizeof(buf));
  ASSERT_EQ(0, tEncodeI32v(&enc, 1));
  ASSERT_EQ(0, tEncodeI32v(&enc, 1));
  ASSERT_EQ(0, tEncodeI8(&enc, 1));
  ASSERT_EQ(0, tEncodeI16v(&enc, 1));
  ASSERT_EQ(0, tEncodeCStr(&enc, "db"));
  ASSERT_EQ(0, tEncodeCStr(&enc, "tb"));
  ASSERT_EQ(0, tEncodeCStr(&enc, "col"));
  uint32_t len = enc.pos;
  tEncoderClear(&enc);
  SDecoder   dec;
  SMetaEntry entry = {};
  tDecoderInit(&dec, buf, len);
  EXPECT_EQ(0, meteDecodeColRefEntry(&dec, &entry));
  tDecoderClear(&dec);
}

TEST(MetaColRefDecode, InvalidColRefCountRejected) {
  uint8_t  buf[64] = {0};
  SEncoder enc;
  tEncoderInit(&enc, buf, sizeof(buf));
  ASSERT_EQ(0, tEncodeI32v(&enc, 1 << 30));  // nCols far beyond TSDB_MAX_COLUMNS
  ASSERT_EQ(0, tEncodeI32v(&enc, 1));
  uint32_t len = enc.pos;
  tEncoderClear(&enc);

  SDecoder   dec;
  SMetaEntry entry = {};
  tDecoderInit(&dec, buf, len);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, meteDecodeColRefEntry(&dec, &entry));
  tDecoderClear(&dec);
}
