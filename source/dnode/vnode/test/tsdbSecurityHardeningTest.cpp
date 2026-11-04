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

// Crafted-buffer regression tests for the untrusted-length validation added to the tsdb layer
// (snapshot / on-disk descriptor / cache / TLV parsers).

#include <gtest/gtest.h>

#ifdef LINUX

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include <vnodeInt.h>
#include <cstring>
#include <vector>
#include "cJSON.h"
#include "tsdbDataFileRAW.h"
#include "tsdbFSet2.h"
#include "tsdbFile2.h"

#pragma GCC diagnostic pop

// Required stub for builds that reference this symbol via dmRepair linkage.
extern "C" SDmNotifyHandle dmNotifyHdl = {};

#define TEST_SNAP_MSG_VER 1

// ---------------------------------------------------------------------------
// SDiskDataHdr / tBlockDataDecompress
// ---------------------------------------------------------------------------
class DiskHdrTest : public ::testing::Test {
 protected:
  SBuffer buf_;
  void    SetUp() override { tBufferInit(&buf_); }
  void    TearDown() override { tBufferDestroy(&buf_); }

  static SDiskDataHdr MakeHdr() {
    SDiskDataHdr hdr = {};
    hdr.delimiter = TSDB_FILE_DLMT;
    hdr.fmtVer = 1;
    hdr.suid = 1;
    hdr.uid = 2;
    hdr.szUid = 0;
    hdr.szVer = 8;
    hdr.szKey = 8;
    hdr.szBlkCol = 0;
    hdr.nRow = 4;
    hdr.cmprAlg = 0;
    hdr.numOfPKs = 0;
    return hdr;
  }

  int32_t Get(const SDiskDataHdr &in, SDiskDataHdr *out) {
    tBufferClear(&buf_);
    EXPECT_EQ(tPutDiskDataHdr(&buf_, &in), 0);
    SBufferReader br = BUFFER_READER_INITIALIZER(0, &buf_);
    return tGetDiskDataHdr(&br, out);
  }
};

TEST_F(DiskHdrTest, ValidHeaderAccepted) {
  SDiskDataHdr in = MakeHdr(), out = {};
  EXPECT_EQ(Get(in, &out), 0);
  EXPECT_EQ(out.nRow, 4);
}

TEST_F(DiskHdrTest, RejectNonPositiveOrHugeRowCount) {
  SDiskDataHdr in = MakeHdr(), out = {};
  in.nRow = 0;
  EXPECT_EQ(Get(in, &out), TSDB_CODE_FILE_CORRUPTED);
  in.nRow = -5;
  EXPECT_EQ(Get(in, &out), TSDB_CODE_FILE_CORRUPTED);
  in.nRow = TSDB_MAX_MAXROWS_FBLOCK + 1;
  EXPECT_EQ(Get(in, &out), TSDB_CODE_FILE_CORRUPTED);
  in.nRow = 0x0fffffff;
  EXPECT_EQ(Get(in, &out), TSDB_CODE_FILE_CORRUPTED);
}

TEST_F(DiskHdrTest, RejectNegativeSizes) {
  SDiskDataHdr in = MakeHdr(), out = {};
  in.szKey = -1;
  EXPECT_EQ(Get(in, &out), TSDB_CODE_FILE_CORRUPTED);
  in = MakeHdr();
  in.szVer = -1;
  EXPECT_EQ(Get(in, &out), TSDB_CODE_FILE_CORRUPTED);
  in = MakeHdr();
  in.szBlkCol = -100;
  EXPECT_EQ(Get(in, &out), TSDB_CODE_FILE_CORRUPTED);
}

TEST_F(DiskHdrTest, RejectNumOfPKsAboveMax) {
  SDiskDataHdr in = MakeHdr(), out = {};
  // numOfPKs is the last byte of a header that carries zero primary-key columns
  for (int8_t bad : {(int8_t)(TD_MAX_PK_COLS + 1), (int8_t)3, (int8_t)127, (int8_t)-1, (int8_t)-128}) {
    tBufferClear(&buf_);
    ASSERT_EQ(tPutDiskDataHdr(&buf_, &in), 0);
    ((uint8_t *)tBufferGetData(&buf_))[tBufferGetSize(&buf_) - 1] = (uint8_t)bad;
    SBufferReader br = BUFFER_READER_INITIALIZER(0, &buf_);
    EXPECT_EQ(tGetDiskDataHdr(&br, &out), TSDB_CODE_FILE_CORRUPTED) << "numOfPKs=" << (int)bad;
  }
}

TEST_F(DiskHdrTest, DecompressRejectsSizesBeyondPayload) {
  SDiskDataHdr in = MakeHdr();
  in.szVer = 0x7ffffff0;
  in.szKey = 0x7ffffff0;  // sum overflows int32, must not be trusted
  tBufferClear(&buf_);
  ASSERT_EQ(tPutDiskDataHdr(&buf_, &in), 0);
  SBufferReader br = BUFFER_READER_INITIALIZER(0, &buf_);
  SBlockData    bd = {};
  SBuffer       assist;
  tBufferInit(&assist);
  EXPECT_EQ(tBlockDataDecompress(&br, &bd, &assist), TSDB_CODE_FILE_CORRUPTED);
  tBlockDataDestroy(&bd);
  tBufferDestroy(&assist);
}

TEST_F(DiskHdrTest, DecompressRejectsMissingKeyAndVersion) {
  // nRow > 0 but no compressed key / version payload at all
  SDiskDataHdr in = MakeHdr();
  in.szKey = 0;
  in.szVer = 0;
  tBufferClear(&buf_);
  ASSERT_EQ(tPutDiskDataHdr(&buf_, &in), 0);
  SBufferReader br = BUFFER_READER_INITIALIZER(0, &buf_);
  SBlockData    bd = {};
  SBuffer       assist;
  tBufferInit(&assist);
  EXPECT_EQ(tBlockDataDecompress(&br, &bd, &assist), TSDB_CODE_FILE_CORRUPTED);
  tBlockDataDestroy(&bd);
  tBufferDestroy(&assist);
}

TEST_F(DiskHdrTest, DecompressRejectsTruncatedPayload) {
  // header claims 64 bytes of key data but only 8 follow
  SDiskDataHdr in = MakeHdr();
  in.szKey = 64;
  in.szVer = 8;
  tBufferClear(&buf_);
  ASSERT_EQ(tPutDiskDataHdr(&buf_, &in), 0);
  uint8_t pad[16] = {0};
  ASSERT_EQ(tBufferPut(&buf_, pad, sizeof(pad)), 0);
  SBufferReader br = BUFFER_READER_INITIALIZER(0, &buf_);
  SBlockData    bd = {};
  SBuffer       assist;
  tBufferInit(&assist);
  EXPECT_EQ(tBlockDataDecompress(&br, &bd, &assist), TSDB_CODE_FILE_CORRUPTED);
  tBlockDataDestroy(&bd);
  tBufferDestroy(&assist);
}

// ---------------------------------------------------------------------------
// brin / statis / tomb descriptor validators
// ---------------------------------------------------------------------------
TEST(BlkDescSizesTest, Validation) {
  int32_t size[3] = {10, 20, 30};
  EXPECT_EQ(tsdbCheckBlkDescSizes(size, 3, 100, 60), 0);
  EXPECT_EQ(tsdbCheckBlkDescSizes(size, 3, 100, 59), TSDB_CODE_FILE_CORRUPTED);  // beyond loaded bytes
  EXPECT_EQ(tsdbCheckBlkDescSizes(size, 3, -1, 60), TSDB_CODE_FILE_CORRUPTED);   // negative numRec
  EXPECT_EQ(tsdbCheckBlkDescSizes(size, 3, INT32_MAX / 8 + 1, 60), TSDB_CODE_FILE_CORRUPTED);
  int32_t neg[2] = {10, -1};
  EXPECT_EQ(tsdbCheckBlkDescSizes(neg, 2, 1, 1000), TSDB_CODE_FILE_CORRUPTED);
  int32_t big[2] = {INT32_MAX, INT32_MAX};  // sum overflows int32
  EXPECT_EQ(tsdbCheckBlkDescSizes(big, 2, 1, 1000), TSDB_CODE_FILE_CORRUPTED);
}

TEST(ValueColInfoTest, Validation) {
  SValueColumnCompressInfo info = {};
  info.type = TSDB_DATA_TYPE_BIGINT;
  info.dataOriginalSize = 80;
  info.dataCompressedSize = 40;
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 10), 0);
  info.dataCompressedSize = -1;
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 10), TSDB_CODE_FILE_CORRUPTED);
  info.dataCompressedSize = 8;
  info.type = 100;  // outside tDataTypes[]
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 10), TSDB_CODE_FILE_CORRUPTED);
  info.type = -3;
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 10), TSDB_CODE_FILE_CORRUPTED);
  info.type = TSDB_DATA_TYPE_VARCHAR;
  info.dataCompressedSize = 8;
  info.offsetCompressedSize = -1;
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 10), TSDB_CODE_FILE_CORRUPTED);
}

// The original sizes drive the output allocation, so they must match the record count of the block.
TEST(ValueColInfoTest, OriginalSizeBoundByNumRec) {
  SValueColumnCompressInfo info = {};
  info.type = TSDB_DATA_TYPE_BIGINT;
  info.dataCompressedSize = 1;
  info.dataOriginalSize = 80;
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 10), 0);
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 9), TSDB_CODE_FILE_CORRUPTED);
  info.dataOriginalSize = INT32_MAX;  // 1 compressed byte claiming 2 GiB
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 10), TSDB_CODE_FILE_CORRUPTED);

  info.type = TSDB_DATA_TYPE_VARCHAR;
  info.offsetCompressedSize = 1;
  info.offsetOriginalSize = 10 * sizeof(int32_t);
  info.dataOriginalSize = 100;
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 10), 0);
  info.offsetOriginalSize = 11 * sizeof(int32_t);  // one offset per record
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 10), TSDB_CODE_FILE_CORRUPTED);
  info.offsetOriginalSize = 10 * sizeof(int32_t);
  info.dataOriginalSize = INT32_MAX;
  EXPECT_EQ(tsdbCheckValueColInfo(&info, 1), TSDB_CODE_FILE_CORRUPTED);
  EXPECT_EQ(tsdbCheckValueColInfo(&info, -1), TSDB_CODE_FILE_CORRUPTED);
}

// ---------------------------------------------------------------------------
// tDeserializeTsdbFSetPartList
// ---------------------------------------------------------------------------
static int32_t EncodePartList(uint8_t *buf, int32_t cap, int32_t nPart, int32_t typMax, int32_t iLen) {
  SEncoder enc = {};
  tEncoderInit(&enc, buf, cap);
  EXPECT_EQ(tStartEncode(&enc), 0);
  EXPECT_EQ(tEncodeI8(&enc, TEST_SNAP_MSG_VER), 0);
  EXPECT_EQ(tEncodeI32(&enc, nPart), 0);
  for (int32_t u = 0; u < nPart; u++) {
    EXPECT_EQ(tEncodeI64(&enc, 100 + u), 0);
    EXPECT_EQ(tEncodeI8(&enc, 0), 0);
    EXPECT_EQ(tEncodeI8(&enc, 0), 0);
    EXPECT_EQ(tEncodeI16(&enc, 0), 0);
    EXPECT_EQ(tEncodeI32(&enc, typMax), 0);
    for (int32_t i = 0; i < typMax && i < 16; i++) {
      EXPECT_EQ(tEncodeI32(&enc, iLen), 0);
      for (int32_t j = 0; j < iLen && j < 4; j++) {
        EXPECT_EQ(tEncodeI64(&enc, j), 0);
        EXPECT_EQ(tEncodeI64(&enc, j + 1), 0);
        EXPECT_EQ(tEncodeI64(&enc, 0), 0);
      }
    }
  }
  tEndEncode(&enc);
  int32_t len = enc.pos;
  tEncoderClear(&enc);
  return len;
}

TEST(FSetPartListTest, TypMaxOutOfRangeRejected) {
  uint8_t           buf[4096] = {0};
  STsdbFSetPartList *pList = tsdbFSetPartListCreate();
  ASSERT_NE(pList, nullptr);

  int32_t len = EncodePartList(buf, sizeof(buf), 1, 100, 0);
  EXPECT_EQ(tDeserializeTsdbFSetPartList(buf, len, pList), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(TARRAY2_SIZE(pList), 0);

  len = EncodePartList(buf, sizeof(buf), 1, -1, 0);
  EXPECT_EQ(tDeserializeTsdbFSetPartList(buf, len, pList), TSDB_CODE_INVALID_MSG);

  len = EncodePartList(buf, sizeof(buf), 1, TSDB_FSET_RANGE_TYP_MAX + 1, 0);
  EXPECT_EQ(tDeserializeTsdbFSetPartList(buf, len, pList), TSDB_CODE_INVALID_MSG);
  tsdbFSetPartListDestroy(&pList);
}

TEST(FSetPartListTest, LenAndRangeCountRejected) {
  uint8_t           buf[4096] = {0};
  STsdbFSetPartList *pList = tsdbFSetPartListCreate();
  ASSERT_NE(pList, nullptr);

  // partition count far larger than the payload can hold
  SEncoder enc = {};
  tEncoderInit(&enc, buf, sizeof(buf));
  ASSERT_EQ(tStartEncode(&enc), 0);
  ASSERT_EQ(tEncodeI8(&enc, TEST_SNAP_MSG_VER), 0);
  ASSERT_EQ(tEncodeI32(&enc, 0x7fffffff), 0);
  tEndEncode(&enc);
  int32_t len = enc.pos;
  tEncoderClear(&enc);
  EXPECT_EQ(tDeserializeTsdbFSetPartList(buf, len, pList), TSDB_CODE_INVALID_MSG);

  // negative / oversized per-type range count
  len = EncodePartList(buf, sizeof(buf), 1, 1, -7);
  EXPECT_EQ(tDeserializeTsdbFSetPartList(buf, len, pList), TSDB_CODE_INVALID_MSG);
  len = EncodePartList(buf, sizeof(buf), 1, 1, 0x7fffffff);
  EXPECT_EQ(tDeserializeTsdbFSetPartList(buf, len, pList), TSDB_CODE_INVALID_MSG);
  tsdbFSetPartListDestroy(&pList);
}

TEST(FSetPartListTest, ValidListAccepted) {
  uint8_t           buf[4096] = {0};
  STsdbFSetPartList *pList = tsdbFSetPartListCreate();
  ASSERT_NE(pList, nullptr);
  int32_t len = EncodePartList(buf, sizeof(buf), 2, TSDB_FSET_RANGE_TYP_MAX, 2);
  EXPECT_EQ(tDeserializeTsdbFSetPartList(buf, len, pList), 0);
  EXPECT_EQ(TARRAY2_SIZE(pList), 2);
  tsdbFSetPartListDestroy(&pList);
}

// ---------------------------------------------------------------------------
// tsdbCacheDeserialize
// ---------------------------------------------------------------------------
typedef struct {
  TSKEY  ts;
  int8_t dirty;
  struct {
    int16_t cid;
    int8_t  type;
    int8_t  flag;
    union {
      int64_t val;
      struct {
        uint32_t nData;
        uint8_t *pData;
      };
    } value;
  } colVal;
} SLastColV0Sec;

TEST(CacheDeserializeTest, ShortV0ValueRejected) {
  uint8_t   buf[sizeof(SLastColV0Sec)] = {0};
  SLastCol *p = NULL;
  for (size_t sz : {(size_t)0, (size_t)1, sizeof(SLastColV0Sec) / 2, sizeof(SLastColV0Sec) - 1}) {
    EXPECT_EQ(tsdbCacheDeserialize((const char *)buf, sz, &p), TSDB_CODE_INVALID_DATA_FMT) << "size=" << sz;
  }
}

TEST(CacheDeserializeTest, ExactV0Accepted) {
  SLastColV0Sec v0 = {};
  v0.ts = 123;
  v0.colVal.cid = 1;
  v0.colVal.type = TSDB_DATA_TYPE_BIGINT;
  v0.colVal.value.val = 42;
  SLastCol *p = NULL;
  ASSERT_EQ(tsdbCacheDeserialize((const char *)&v0, sizeof(v0), &p), 0);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->rowKey.ts, 123);
  taosMemoryFree(p);
}

TEST(CacheDeserializeTest, VarLenBeyondValueRejected) {
  SLastColV0Sec v0 = {};
  v0.colVal.type = TSDB_DATA_TYPE_VARCHAR;
  v0.colVal.value.nData = 0xFFFFFFF0u;  // would wrap a 32-bit offset
  SLastCol *p = NULL;
  EXPECT_EQ(tsdbCacheDeserialize((const char *)&v0, sizeof(v0), &p), TSDB_CODE_INVALID_DATA_FMT);
  v0.colVal.value.nData = 8;  // claims 8 bytes of payload that are not there
  EXPECT_EQ(tsdbCacheDeserialize((const char *)&v0, sizeof(v0), &p), TSDB_CODE_INVALID_DATA_FMT);
}

TEST(CacheDeserializeTest, NumOfPKsAboveMaxRejected) {
  std::vector<uint8_t> buf(sizeof(SLastColV0Sec) + 2 + 300, 0);
  SLastColV0Sec        v0 = {};
  v0.colVal.type = TSDB_DATA_TYPE_BIGINT;
  memcpy(buf.data(), &v0, sizeof(v0));
  buf[sizeof(v0)] = 1;     // version
  buf[sizeof(v0) + 1] = 255;  // numOfPKs
  SLastCol *p = NULL;
  EXPECT_EQ(tsdbCacheDeserialize((const char *)buf.data(), buf.size(), &p), TSDB_CODE_INVALID_DATA_FMT);
}

// ---------------------------------------------------------------------------
// snapshot-info TLV walker
// ---------------------------------------------------------------------------
static std::vector<uint8_t> MakeTlv(int32_t headLen, const std::vector<std::pair<int32_t, int32_t>> &fields,
                                    size_t extra = 0) {
  std::vector<uint8_t> v(sizeof(SSyncTLV), 0);
  SSyncTLV            *h = (SSyncTLV *)v.data();
  h->typ = TDMT_SYNC_PREP_SNAPSHOT_REPLY;
  h->len = headLen;
  for (auto &f : fields) {
    size_t old = v.size();
    v.resize(old + sizeof(SSyncTLV));
    SSyncTLV *s = (SSyncTLV *)(v.data() + old);
    s->typ = f.first;
    s->len = f.second;
    if (f.second > 0 && f.second < 4096) v.resize(v.size() + f.second, 0);
  }
  v.resize(v.size() + extra, 0);
  return v;
}

TEST(SnapTlvTest, WellFormedWalk) {
  // two fields: {RAW, 4 bytes}, {TSDB, 2 bytes}
  auto            v = MakeTlv(2 * sizeof(SSyncTLV) + 6, {{SNAP_DATA_RAW, 4}, {SNAP_DATA_TSDB, 2}});
  const SSyncTLV *head = (const SSyncTLV *)v.data();
  int64_t         off = 0;
  const SSyncTLV *f = NULL;
  ASSERT_EQ(tsdbSnapTlvNext(head, &off, &f), 0);
  ASSERT_NE(f, nullptr);
  EXPECT_EQ(f->typ, SNAP_DATA_RAW);
  EXPECT_EQ(f->len, 4);
  ASSERT_EQ(tsdbSnapTlvNext(head, &off, &f), 0);
  ASSERT_NE(f, nullptr);
  EXPECT_EQ(f->typ, SNAP_DATA_TSDB);
  ASSERT_EQ(tsdbSnapTlvNext(head, &off, &f), 0);
  EXPECT_EQ(f, nullptr);  // finished
}

TEST(SnapTlvTest, NegativeFieldLenRejected) {
  // len == -8 would keep the old loop at the same offset forever
  auto            v = MakeTlv(2 * sizeof(SSyncTLV), {{SNAP_DATA_RAW, -8}});
  const SSyncTLV *head = (const SSyncTLV *)v.data();
  int64_t         off = 0;
  const SSyncTLV *f = NULL;
  EXPECT_EQ(tsdbSnapTlvNext(head, &off, &f), TSDB_CODE_INVALID_DATA_FMT);
  EXPECT_EQ(f, nullptr);
}

TEST(SnapTlvTest, OversizedFieldLenRejected) {
  auto            v = MakeTlv(2 * sizeof(SSyncTLV) + 16, {{SNAP_DATA_TSDB, 0x40000000}}, 16);
  const SSyncTLV *head = (const SSyncTLV *)v.data();
  int64_t         off = 0;
  const SSyncTLV *f = NULL;
  EXPECT_EQ(tsdbSnapTlvNext(head, &off, &f), TSDB_CODE_INVALID_DATA_FMT);
  EXPECT_EQ(f, nullptr);
}

TEST(SnapTlvTest, FieldLenPastHeadLenRejected) {
  // field fits the allocation but not the outer length
  auto            v = MakeTlv(sizeof(SSyncTLV) + 2, {{SNAP_DATA_RAW, 64}});
  const SSyncTLV *head = (const SSyncTLV *)v.data();
  int64_t         off = 0;
  const SSyncTLV *f = NULL;
  EXPECT_EQ(tsdbSnapTlvNext(head, &off, &f), TSDB_CODE_INVALID_DATA_FMT);
}

TEST(SnapTlvTest, NegativeOrHugeHeadLenRejected) {
  auto            v = MakeTlv(-8, {});
  int64_t         off = 0;
  const SSyncTLV *f = NULL;
  EXPECT_EQ(tsdbSnapTlvNext((const SSyncTLV *)v.data(), &off, &f), TSDB_CODE_INVALID_DATA_FMT);
}

TEST(SnapTlvTest, EmptyHeadYieldsNoField) {
  auto            v = MakeTlv(0, {});
  int64_t         off = 0;
  const SSyncTLV *f = (const SSyncTLV *)1;
  EXPECT_EQ(tsdbSnapTlvNext((const SSyncTLV *)v.data(), &off, &f), 0);
  EXPECT_EQ(f, nullptr);
}

// ---------------------------------------------------------------------------
// RAW snapshot block header validator
// ---------------------------------------------------------------------------
class RawBlockTest : public ::testing::Test {
 protected:
  std::vector<uint8_t> v_;
  STsdbDataRAWBlockHeader *H() { return (STsdbDataRAWBlockHeader *)v_.data(); }
  void Make(int64_t dataLen) {
    v_.assign(sizeof(STsdbDataRAWBlockHeader) + dataLen, 0);
    H()->file.type = TSDB_FTYPE_DATA;
    H()->file.fid = 1000;
    H()->file.cid = 5;
    H()->file.size = 4096;
    H()->dataLength = dataLen;
  }
  int32_t Check() { return tsdbSnapRAWCheckBlock(v_.data(), (int64_t)v_.size()); }
};

TEST_F(RawBlockTest, ValidAccepted) {
  Make(128);
  EXPECT_EQ(Check(), 0);
  for (int t : {TSDB_FTYPE_HEAD, TSDB_FTYPE_DATA, TSDB_FTYPE_SMA, TSDB_FTYPE_TOMB, TSDB_FTYPE_STT}) {
    H()->file.type = t;
    EXPECT_EQ(Check(), 0) << t;
  }
}

TEST_F(RawBlockTest, InvalidFileTypeRejected) {
  Make(16);
  // 4 is the hole in tsdb_ftype_t; others index out of g_tfile_info / farr
  for (int t : {4, 6, 100, -1, INT32_MIN, INT32_MAX}) {
    H()->file.type = t;
    EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG) << t;
  }
}

TEST_F(RawBlockTest, DataLengthMismatchRejected) {
  Make(16);
  H()->dataLength = 17;  // reads one byte past the payload
  EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG);
  H()->dataLength = 15;
  EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG);
  H()->dataLength = INT64_MAX;
  EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG);
  H()->dataLength = -1;
  EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG);
}

TEST_F(RawBlockTest, ShortOrNullBufferRejected) {
  Make(0);
  EXPECT_EQ(tsdbSnapRAWCheckBlock(v_.data(), (int64_t)sizeof(STsdbDataRAWBlockHeader) - 1), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(tsdbSnapRAWCheckBlock(v_.data(), 0), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(tsdbSnapRAWCheckBlock(v_.data(), -1), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(tsdbSnapRAWCheckBlock(NULL, 1 << 20), TSDB_CODE_INVALID_MSG);
}

TEST_F(RawBlockTest, InsaneIdentifiersRejected) {
  Make(8);
  H()->file.fid = (int64_t)INT32_MAX + 1;
  EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG);
  H()->file.fid = (int64_t)INT32_MIN - 1;
  EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG);
  Make(8);
  H()->file.cid = -1;
  EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG);
  Make(8);
  H()->file.type = TSDB_FTYPE_STT;
  H()->file.stt->level = -1;
  EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG);
  // the level of a non-stt file is not used, whatever the peer left in the union
  H()->file.type = TSDB_FTYPE_DATA;
  EXPECT_EQ(Check(), 0);
  Make(8);
  H()->file.size = -1;
  EXPECT_EQ(Check(), TSDB_CODE_INVALID_MSG);
}

// ---------------------------------------------------------------------------
// tsdb file type guards
// ---------------------------------------------------------------------------
TEST(FTypeGuardTest, LabelAndNameGuards) {
  EXPECT_TRUE(tsdbFTypeIsValid(TSDB_FTYPE_HEAD));
  EXPECT_TRUE(tsdbFTypeIsValid(TSDB_FTYPE_STT));
  EXPECT_FALSE(tsdbFTypeIsValid(4));
  EXPECT_FALSE(tsdbFTypeIsValid(-1));
  EXPECT_FALSE(tsdbFTypeIsValid(1 << 20));

  EXPECT_STREQ(tsdbFTypeLabel(TSDB_FTYPE_HEAD), "head");
  EXPECT_STREQ(tsdbFTypeLabel(TSDB_FTYPE_STT), "stt");
  // never NULL and never an out-of-table pointer
  EXPECT_STREQ(tsdbFTypeLabel((tsdb_ftype_t)4), "unknown");
  EXPECT_STREQ(tsdbFTypeLabel((tsdb_ftype_t)99999), "unknown");

  // invalid type yields an empty file name before pTsdb is dereferenced
  STFile f = {};
  f.type = (tsdb_ftype_t)4;
  char fname[TSDB_FILENAME_LEN] = "x";
  tsdbTFileName(NULL, &f, fname);
  EXPECT_STREQ(fname, "");
  strcpy(fname, "x");
  tsdbTFileLastChunkName(NULL, &f, fname);
  EXPECT_STREQ(fname, "");
}

// ---------------------------------------------------------------------------
// manifest JSON: did.level / did.id bounds
// ---------------------------------------------------------------------------
static cJSON *MakeFileSetJson(double level, double id) {
  cJSON *root = cJSON_CreateObject();
  cJSON_AddNumberToObject(root, "fid", 1000);
  cJSON *head = cJSON_AddObjectToObject(root, "head");
  cJSON_AddNumberToObject(head, "did.level", level);
  cJSON_AddNumberToObject(head, "did.id", id);
  cJSON_AddNumberToObject(head, "fid", 1000);
  cJSON_AddNumberToObject(head, "cid", 1);
  cJSON_AddNumberToObject(head, "size", 100);
  return root;
}

TEST(TFileJsonTest, DidOutOfRangeRejected) {
  // pTsdb is not touched on the rejection path (fails before tsdbTFileObjInit)
  struct {
    double level, id;
  } bad[] = {{99, 0}, {TFS_MAX_TIERS, 0}, {-1, 0}, {0, TFS_MAX_DISKS_PER_TIER}, {0, -1}, {0, 1e9}, {3e9, 0}};
  for (auto &b : bad) {
    cJSON     *root = MakeFileSetJson(b.level, b.id);
    STFileSet *fset = NULL;
    EXPECT_EQ(tsdbJsonToTFileSet(NULL, root, &fset), TSDB_CODE_FILE_CORRUPTED) << b.level << "," << b.id;
    cJSON_Delete(root);
  }
}

TEST(TFileJsonTest, DidInRangeAccepted) {
  cJSON *root = MakeFileSetJson(TFS_MAX_TIERS - 1, TFS_MAX_DISKS_PER_TIER - 1);
  STFile f = {};
  EXPECT_EQ(tsdbJsonToTFile(root, TSDB_FTYPE_HEAD, &f), 0);
  EXPECT_EQ(f.did.level, TFS_MAX_TIERS - 1);
  EXPECT_EQ(f.did.id, TFS_MAX_DISKS_PER_TIER - 1);
  cJSON_Delete(root);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
int main(int argc, char **argv) { return 0; }
#endif
