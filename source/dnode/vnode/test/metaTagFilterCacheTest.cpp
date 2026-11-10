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

#include <array>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "stub.h"
#include "tdTestSeam.h"

namespace {

// The engine that runs from libtaosd_shared.so reads its own copy of the seam
// pointers, so the test assigns that copy explicitly instead of the one linked
// into this binary.
using tdtestseam::seamSlot;

}  // namespace



extern "C" {
#include "meta.h"
#include "tcommon.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tmd5.h"
#include "tmsgcb.h"
#include "vnd.h"
#include "vnode.h"
}

SDmNotifyHandle dmNotifyHdl = {};

namespace {
const void* failUidAppend = nullptr;
bool        uidAppendFailed = false;
int32_t     cursorNextBeforeFailure = -1;
bool        failCursorSeek = false;
bool        failCursorAdjust = false;

// Clears the seam for the duration of the call so the replacement can reach the
// real implementation.
template <typename T, typename Fn>
auto bypassSeam(T& slot, Fn&& fn) -> decltype(fn()) {
  T saved = slot;
  slot = nullptr;
  auto result = fn();
  slot = saved;
  return result;
}
}  // namespace

extern "C" void* seamTaosArrayAddBatch(SArray* array, const void* data, int32_t count) {
  if (data != nullptr && data == failUidAppend) {
    failUidAppend = nullptr;
    uidAppendFailed = true;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  return bypassSeam(gTdSeam_taosArrayAddBatch, [&] { return taosArrayAddBatch(array, data, count); });
}

extern "C" int32_t seamTdbTbcNext(TBC* cursor, void** key, int32_t* keyLen, void** value, int32_t* valueLen) {
  if (cursorNextBeforeFailure == 0) {
    cursorNextBeforeFailure = -1;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (cursorNextBeforeFailure > 0) --cursorNextBeforeFailure;
  return bypassSeam(gTdSeam_tdbTbcNext, [&] { return tdbTbcNext(cursor, key, keyLen, value, valueLen); });
}

extern "C" int32_t seamTdbTbcMoveTo(TBC* cursor, const void* key, int32_t keyLen, int32_t* comparison) {
  if (failCursorSeek) {
    failCursorSeek = false;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return bypassSeam(gTdSeam_tdbTbcMoveTo, [&] { return tdbTbcMoveTo(cursor, key, keyLen, comparison); });
}

extern "C" int32_t seamTdbTbcMoveToNext(TBC* cursor) {
  if (failCursorAdjust) {
    failCursorAdjust = false;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return bypassSeam(gTdSeam_tdbTbcMoveToNext, [&] { return tdbTbcMoveToNext(cursor); });
}

namespace {

using ArrayOwner = std::unique_ptr<SArray, decltype(&taosArrayDestroy)>;
using TagOwner = std::unique_ptr<STag, decltype(&tTagFree)>;

void destroyVersionTagList(SArray* tags) {
  for (size_t i = 0; i < taosArrayGetSize(tags); ++i) {
    auto* info = static_cast<STUidTagInfo*>(taosArrayGet(tags, i));
    taosMemoryFree(info->pTagVal);
  }
  taosArrayDestroy(tags);
}

constexpr tb_uid_t    kSuid = 700001;
constexpr col_id_t    kTagId = 100;
constexpr int64_t     kTxnId = 42;
const uint8_t         kCondition[] = "100";
std::function<void()> onMetaUnlock;

void expectIntTagValue(const void* tags, int32_t expected) {
  STagVal value = {};
  value.cid = kTagId;
  ASSERT_TRUE(tTagGet(static_cast<const STag*>(tags), &value));
  ASSERT_EQ(TSDB_DATA_TYPE_INT, value.type);
  // tTagGet populates only the bytes belonging to the stored tag type.
  int32_t actual = 0;
  memcpy(&actual, &value.i64, sizeof(actual));
  EXPECT_EQ(expected, actual);
}

void unlockMetaThenMutate(SMeta* meta) {
  EXPECT_EQ(0, taosThreadRwlockUnlock(&meta->lock));
  if (onMetaUnlock) {
    auto action = std::move(onMetaUnlock);
    onMetaUnlock = nullptr;
    action();
  }
}

std::array<uint8_t, 16> tagKey(int32_t value) {
  // The public cache API accepts the digest of (column id, typed TAG value).
  uint8_t bytes[sizeof(kTagId) + sizeof(value)] = {};
  memcpy(bytes, &kTagId, sizeof(kTagId));
  memcpy(bytes + sizeof(kTagId), &value, sizeof(value));
  T_MD5_CTX context = {};
  tMD5Init(&context);
  tMD5Update(&context, bytes, sizeof(bytes));
  tMD5Final(&context);
  std::array<uint8_t, 16> key = {};
  memcpy(key.data(), context.digest, key.size());
  return key;
}

struct CacheResult {
  int32_t               code = 0;
  bool                  acquired = false;
  bool                  needWarmup = false;
  std::vector<uint64_t> uids;
};

class MetaTagFilterCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_NE(nullptr, mkdtemp(directory_));
    vnode_.path = directory_;
    vnode_.config.vgId = 1;
    vnode_.config.szPage = 4096;
    vnode_.config.szCache = 256;
    ASSERT_EQ(0, metaOpen(&vnode_, &vnode_.pMeta, 0));
    ASSERT_EQ(0, metaBegin(vnode_.pMeta, META_BEGIN_HEAP_OS));
    ASSERT_EQ(0, vnodeTxnInit(&vnode_));

    SSchema rows[2] = {};
    rows[0].type = TSDB_DATA_TYPE_TIMESTAMP;
    rows[0].colId = 1;
    rows[0].bytes = sizeof(int64_t);
    tstrncpy(rows[0].name, "ts", sizeof(rows[0].name));
    rows[1].type = TSDB_DATA_TYPE_INT;
    rows[1].colId = 2;
    rows[1].bytes = sizeof(int32_t);
    tstrncpy(rows[1].name, "v", sizeof(rows[1].name));
    tagSchema_.type = tagType_;
    tagSchema_.colId = kTagId;
    tagSchema_.bytes = tagBytes_;
    tstrncpy(tagSchema_.name, "pile_code", sizeof(tagSchema_.name));
    SColCmpr compression[2] = {};
    for (int i = 0; i < 2; ++i) {
      compression[i].id = rows[i].colId;
      compression[i].alg = createDefaultColCmprByType(rows[i].type);
    }
    SVCreateStbReq request = {};
    request.name = const_cast<char*>("pile");
    request.suid = kSuid;
    request.colCmpred = 1;
    request.schemaRow.nCols = 2;
    request.schemaRow.pSchema = rows;
    request.schemaTag.nCols = 1;
    request.schemaTag.pSchema = &tagSchema_;
    request.colCmpr.nCols = 2;
    request.colCmpr.pColCmpr = compression;
    ASSERT_EQ(0, metaCreateSuperTable(vnode_.pMeta, ++version_, &request));

    columns_.reset(taosArrayInit(1, sizeof(col_id_t)));
    ASSERT_NE(nullptr, columns_);
    ASSERT_NE(nullptr, taosArrayPush(columns_.get(), &kTagId));

    *seamSlot(gTdSeam_taosArrayAddBatch, "gTdSeam_taosArrayAddBatch") = reinterpret_cast<void*>(&seamTaosArrayAddBatch);
    *seamSlot(gTdSeam_tdbTbcNext, "gTdSeam_tdbTbcNext") = reinterpret_cast<void*>(&seamTdbTbcNext);
    *seamSlot(gTdSeam_tdbTbcMoveTo, "gTdSeam_tdbTbcMoveTo") = reinterpret_cast<void*>(&seamTdbTbcMoveTo);
    *seamSlot(gTdSeam_tdbTbcMoveToNext, "gTdSeam_tdbTbcMoveToNext") = reinterpret_cast<void*>(&seamTdbTbcMoveToNext);
  }

  void TearDown() override {
    *seamSlot(gTdSeam_taosArrayAddBatch, "gTdSeam_taosArrayAddBatch") = nullptr;
    *seamSlot(gTdSeam_tdbTbcNext, "gTdSeam_tdbTbcNext") = nullptr;
    *seamSlot(gTdSeam_tdbTbcMoveTo, "gTdSeam_tdbTbcMoveTo") = nullptr;
    *seamSlot(gTdSeam_tdbTbcMoveToNext, "gTdSeam_tdbTbcMoveToNext") = nullptr;
    onMetaUnlock = nullptr;
    failUidAppend = nullptr;
    cursorNextBeforeFailure = -1;
    failCursorSeek = false;
    failCursorAdjust = false;
    stub_.reset(metaULock);
    vnodeTxnCleanup(&vnode_);
    metaClose(&vnode_.pMeta);
    taosRemoveDir(directory_);
  }

  int32_t putChild(uint64_t uid, int32_t value, bool isNull = false, bool failAppend = false, int64_t txnId = 0) {
    STagVal tag = {};
    tag.cid = kTagId;
    tag.type = TSDB_DATA_TYPE_INT;
    tag.i64 = value;
    return putChildTag(uid, tag, isNull, failAppend, txnId);
  }

  int32_t putChildTag(uint64_t uid, const STagVal& tag, bool isNull = false, bool failAppend = false,
                      int64_t txnId = 0) {
    ArrayOwner values(taosArrayInit(1, sizeof(STagVal)), taosArrayDestroy);
    if (values == nullptr) return terrno;
    if (!isNull && taosArrayPush(values.get(), &tag) == nullptr) return terrno;
    STag*    raw = nullptr;
    int32_t  code = tTagNew(values.get(), 0, 0, &raw);
    TagOwner tags(raw, tTagFree);
    if (code != 0) return code;
    const std::string name = "p" + std::to_string(uid);
    SMetaEntry        child = {};
    child.version = ++version_;
    child.type = TSDB_CHILD_TABLE;
    child.uid = uid;
    child.name = const_cast<char*>(name.c_str());
    child.ctbEntry.suid = kSuid;
    child.ctbEntry.pTags = reinterpret_cast<uint8_t*>(tags.get());
    child.txnId = txnId;
    child.txnStatus = txnId == 0 ? META_TXN_NORMAL : META_TXN_PRE_CREATE;
    if (failAppend) {
      uidAppendFailed = false;
      failUidAppend = &child.uid;
    }
    code = metaHandleEntry2(vnode_.pMeta, &child);
    failUidAppend = nullptr;
    return code;
  }

  int32_t putPendingChild(uint64_t uid, int32_t value) {
    int32_t code = vnodeTxnEnsureEntry(&vnode_, kTxnId, version_ + 1);
    if (code != 0) return code;
    code = putChild(uid, value, false, false, kTxnId);
    if (code != 0) return code;
    code = metaTxnIdxUpsert(vnode_.pMeta, uid, kTxnId, META_TXN_PRE_CREATE, -1);
    if (code != 0) return code;
    return vnodeTxnTrackTable(&vnode_, kTxnId, uid);
  }

  int32_t commitPendingChildren() {
    SVTxnCommitReq req = {};
    req.txnId = kTxnId;
    std::array<char, 128> buffer = {};
    const int32_t         len = tSerializeSVTxnCommitReq(buffer.data(), buffer.size(), &req);
    if (len < 0) return len;
    return vnodeProcessTxnCommitReq(&vnode_, ++version_, buffer.data(), len, nullptr);
  }

  int32_t dropChild(uint64_t uid) {
    SMetaEntry child = {};
    child.version = ++version_;
    child.type = -TSDB_CHILD_TABLE;
    child.uid = uid;
    return metaHandleEntry2(vnode_.pMeta, &child);
  }

  CacheResult lookup(int32_t value, bool warmup = false) { return lookup(tagKey(value), warmup); }

  CacheResult lookup(const std::array<uint8_t, 16>& key, bool warmup = false) {
    CacheResult result;
    ArrayOwner  uids(taosArrayInit(4, sizeof(uint64_t)), taosArrayDestroy);
    if (uids == nullptr) {
      result.code = terrno;
      return result;
    }
    if (warmup) {
      result.code = metaWarmupStableTagFilterCache(&vnode_, kSuid, kCondition, sizeof(kCondition), key.data(),
                                                   key.size(), columns_.get(), uids.get(), &result.acquired);
    } else {
      result.code = metaStableTagFilterCacheGet(&vnode_, kSuid, kCondition, sizeof(kCondition), key.data(), key.size(),
                                                uids.get(), &result.acquired, &result.needWarmup);
    }
    for (size_t i = 0; i < taosArrayGetSize(uids.get()); ++i) {
      result.uids.push_back(*static_cast<uint64_t*>(taosArrayGet(uids.get(), i)));
    }
    return result;
  }

  void expectHit(int32_t value, const std::vector<uint64_t>& expected, bool warmup = false) {
    const auto result = lookup(value, warmup);
    ASSERT_EQ(0, result.code);
    EXPECT_TRUE(result.acquired);
    EXPECT_EQ(expected, result.uids);
  }

  int32_t alterTagSchema() {
    SMetaEntry* entry = nullptr;
    int32_t     code = metaFetchEntryByUid(vnode_.pMeta, kSuid, &entry);
    if (code != 0) return code;
    SVCreateStbReq request = {};
    request.name = entry->name;
    request.suid = kSuid;
    request.schemaRow = entry->stbEntry.schemaRow;
    request.schemaTag = entry->stbEntry.schemaTag;
    ++request.schemaTag.version;
    request.schemaTag.pSchema = &tagSchema_;
    request.colCmpr = entry->colCmpr;
    code = metaAlterSuperTable(vnode_.pMeta, ++version_, &request);
    metaFetchEntryFree(&entry);
    return code;
  }

  SVnode     vnode_ = {};
  SSchema    tagSchema_ = {};
  int8_t     tagType_ = TSDB_DATA_TYPE_INT;
  int32_t    tagBytes_ = sizeof(int32_t);
  int64_t    version_ = 100;
  ArrayOwner columns_{nullptr, taosArrayDestroy};
  Stub       stub_;
  char       directory_[64] = "/tmp/tsdb-tag-cache-XXXXXX";
};

TEST_F(MetaTagFilterCacheTest, TableNameLookupPreservesUidIndexAllocationFailure) {
  constexpr uint64_t uid = 800001;
  ASSERT_EQ(0, putChild(uid, 1));
  char name[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {};
  ASSERT_EQ(0, metaGetTableNameByUid(&vnode_, uid, name, 0));

  {
    Stub stub;
    stub.set(tdbTbGet, +[](TTB*, const void*, int, void**, int*) -> int {
      return terrno = TSDB_CODE_OUT_OF_MEMORY;
    });
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, metaGetTableNameByUid(&vnode_, uid, name, 0));
  }

  EXPECT_EQ(0, metaGetTableNameByUid(&vnode_, uid, name, 0));
  EXPECT_EQ(TSDB_CODE_PAR_TABLE_NOT_EXIST, metaGetTableNameByUid(&vnode_, uid + 1, name, 0));
}

TEST_F(MetaTagFilterCacheTest, VersionedBatchTagsDoNotScanTheVersionRangePerUid) {
  constexpr uint64_t firstUid = 800001;
  constexpr int32_t count = 257;  // Cross 128-UID lock-slice boundaries.
  for (int32_t i = 0; i < count; ++i) {
    ASSERT_EQ(0, putChild(firstUid + i, 1));
  }
  const int64_t batchVersion = version_ + 1;
  ArrayOwner tags(taosArrayInit(count, sizeof(STUidTagInfo)), destroyVersionTagList);
  ASSERT_NE(nullptr, tags);
  for (int32_t i = 0; i < count; ++i) {
    // A bulk SET TAG gives every updated child the same WAL version.
    version_ = batchVersion - 1;
    ASSERT_EQ(0, putChild(firstUid + i, i + 10));
    STUidTagInfo info = {};
    info.uid = firstUid + i;
    ASSERT_NE(nullptr, taosArrayPush(tags.get(), &info));
  }

  // The existing cursor failure hook must remain unused on exact-version hits.
  failCursorSeek = true;
  ASSERT_EQ(0, metaGetTableTagsByUidsVersion(&vnode_, kSuid, tags.get(), batchVersion));
  EXPECT_TRUE(failCursorSeek);
  failCursorSeek = false;
  for (int32_t i = 0; i < count; ++i) {
    auto* info = static_cast<STUidTagInfo*>(taosArrayGet(tags.get(), i));
    ASSERT_NE(nullptr, info->pTagVal);
    expectIntTagValue(info->pTagVal, i + 10);
  }
}

TEST_F(MetaTagFilterCacheTest, MissingExactVersionFallsBackWithoutReadingFutureTags) {
  constexpr uint64_t uid = 800001;
  ASSERT_EQ(0, putChild(uid, 1));
  const int64_t oldVersion = version_;
  ASSERT_EQ(0, putChild(uid + 1, 2));
  const int64_t betweenVersion = version_;
  ASSERT_EQ(0, putChild(uid, 3));
  ArrayOwner tags(taosArrayInit(1, sizeof(STUidTagInfo)), destroyVersionTagList);
  ASSERT_NE(nullptr, tags);
  STUidTagInfo info = {};
  info.uid = uid;
  ASSERT_NE(nullptr, taosArrayPush(tags.get(), &info));

  for (int64_t version : {oldVersion, betweenVersion, int64_t(-1)}) {
    SCOPED_TRACE(version);
    ASSERT_EQ(0, metaGetTableTagsByUidsVersion(&vnode_, kSuid, tags.get(), version));
    auto* result = static_cast<STUidTagInfo*>(taosArrayGet(tags.get(), 0));
    ASSERT_NE(nullptr, result->pTagVal);
    expectIntTagValue(result->pTagVal, version == -1 ? 3 : 1);
    taosMemoryFreeClear(result->pTagVal);
  }
}

TEST_F(MetaTagFilterCacheTest, VersionedReaderDoesNotScanWhenLaterBatchIsAlreadyApplied) {
  constexpr uint64_t firstUid = 800001;
  constexpr int32_t count = 257;
  for (int32_t i = 0; i < count; ++i) {
    ASSERT_EQ(0, putChild(firstUid + i, 1));
  }
  const int64_t batchVersion = version_ + 1;
  for (int32_t i = 0; i < count; ++i) {
    version_ = batchVersion - 1;
    ASSERT_EQ(0, putChild(firstUid + i, i + 10));
  }
  for (int32_t i = 0; i < count; ++i) {
    version_ = batchVersion;
    ASSERT_EQ(0, putChild(firstUid + i, i + 1000));
  }

  SMetaReader reader = {};
  metaReaderDoInit(&reader, vnode_.pMeta, META_READER_NOLOCK, 0);
  std::unique_ptr<SMetaReader, decltype(&metaReaderClear)> cleanup(&reader, metaReaderClear);
  failCursorSeek = true;
  for (int32_t i = 0; i < count; ++i) {
    ASSERT_EQ(0, metaReaderGetTableEntryByVersionUid(&reader, batchVersion, firstUid + i));
    EXPECT_EQ(batchVersion, reader.me.version);
    expectIntTagValue(reader.me.ctbEntry.pTags, i + 10);
  }
  EXPECT_TRUE(failCursorSeek);
  failCursorSeek = false;
}

TEST_F(MetaTagFilterCacheTest, VersionedReaderFallsBackWithoutReadingFutureTags) {
  constexpr uint64_t uid = 800001;
  ASSERT_EQ(0, putChild(uid, 1));
  const int64_t oldVersion = version_;
  ASSERT_EQ(0, putChild(uid + 1, 2));
  const int64_t betweenVersion = version_;
  ASSERT_EQ(0, putChild(uid, 3));

  SMetaReader reader = {};
  metaReaderDoInit(&reader, vnode_.pMeta, META_READER_NOLOCK, 0);
  std::unique_ptr<SMetaReader, decltype(&metaReaderClear)> cleanup(&reader, metaReaderClear);
  for (int64_t version : {oldVersion, betweenVersion, int64_t(-1)}) {
    SCOPED_TRACE(version);
    ASSERT_EQ(0, metaReaderGetTableEntryByVersionUid(&reader, version, uid));
    expectIntTagValue(reader.me.ctbEntry.pTags, version == -1 ? 3 : 1);
  }
  EXPECT_EQ(TSDB_CODE_PAR_TABLE_NOT_EXIST,
            metaReaderGetTableEntryByVersionUid(&reader, oldVersion - 1, uid));
}

TEST_F(MetaTagFilterCacheTest, VersionedReaderExactHitKeepsPendingCreateVisibility) {
  constexpr uint64_t uid = 800001;
  ASSERT_EQ(0, putPendingChild(uid, 1));
  const int64_t pendingVersion = version_;
  ASSERT_EQ(0, putChild(uid, 2, false, false, kTxnId));
  auto readPending = [&](int64_t txnId) {
    SMetaReader reader = {};
    metaReaderDoInit(&reader, vnode_.pMeta, META_READER_NOLOCK, txnId);
    std::unique_ptr<SMetaReader, decltype(&metaReaderClear)> cleanup(&reader, metaReaderClear);
    return metaReaderGetTableEntryByVersionUid(&reader, pendingVersion, uid);
  };
  EXPECT_EQ(TSDB_CODE_PAR_TABLE_NOT_EXIST, readPending(0));
  EXPECT_EQ(0, readPending(kTxnId));
  // Keep the pending records until vacuum so the exact historical hit must
  // honor the finalized transaction marker instead of inline promotion.
  for (int32_t i = 1; i <= TSDB_TXN_INLINE_THRESHOLD; ++i) {
    ASSERT_EQ(0, putPendingChild(uid + i, 1));
  }
  vnode_.vacuumRunning = 1;
  ASSERT_EQ(0, commitPendingChildren());
  ASSERT_EQ(TXN_META_COMMITTED, metaGetTxnMetaStatus(vnode_.pMeta, kTxnId));
  EXPECT_EQ(0, readPending(0));
  ASSERT_EQ(TSDB_TXN_INLINE_THRESHOLD + 1,
            vnodeTxnVacuumBatch(&vnode_, TSDB_TXN_INLINE_THRESHOLD + 1));
  vnode_.vacuumRunning = 0;
}

class MetaVarTagFilterCacheTest : public MetaTagFilterCacheTest, public ::testing::WithParamInterface<int8_t> {
 protected:
  void SetUp() override {
    tagType_ = GetParam();
    tagBytes_ = 8 * (tagType_ == TSDB_DATA_TYPE_NCHAR ? TSDB_NCHAR_SIZE : 1) + VARSTR_HEADER_SIZE;
    MetaTagFilterCacheTest::SetUp();
  }
};

TEST_P(MetaVarTagFilterCacheTest, WiderTagSchemaKeepsOldAndNewPlansOnTheSameMapping) {
  uint32_t wideText[] = {'p', '1'};
  uint8_t  text[] = {'p', '1'};
  STagVal  tag = {};
  tag.cid = kTagId;
  tag.type = tagType_;
  tag.pData = tagType_ == TSDB_DATA_TYPE_NCHAR ? reinterpret_cast<uint8_t*>(wideText) : text;
  tag.nData = tagType_ == TSDB_DATA_TYPE_NCHAR ? sizeof(wideText) : 2;
  ASSERT_EQ(0, putChildTag(800001, tag));
  ASSERT_EQ(0, putChildTag(800002, tag));
  auto key = [&]() {
    std::vector<uint8_t> bytes(sizeof(kTagId) + sizeof(tag.nData) + tag.nData, 0);
    memcpy(bytes.data(), &kTagId, sizeof(kTagId));
    memcpy(bytes.data() + sizeof(kTagId), &tag.nData, sizeof(tag.nData));
    memcpy(bytes.data() + sizeof(kTagId) + sizeof(tag.nData), tag.pData, tag.nData);
    T_MD5_CTX context = {};
    tMD5Init(&context);
    tMD5Update(&context, bytes.data(), bytes.size());
    tMD5Final(&context);
    std::array<uint8_t, 16> digest = {};
    memcpy(digest.data(), context.digest, digest.size());
    return digest;
  };
  const auto oldPlanKey = key();
  const auto before = lookup(oldPlanKey, true);
  ASSERT_EQ(0, before.code);
  ASSERT_TRUE(before.acquired);
  ASSERT_EQ((std::vector<uint64_t>{800001, 800002}), before.uids);

  tagSchema_.bytes = (tagSchema_.bytes - VARSTR_HEADER_SIZE) * 2 + VARSTR_HEADER_SIZE;
  ASSERT_EQ(0, alterTagSchema());
  const auto after = lookup(key());
  ASSERT_EQ(0, after.code);
  EXPECT_TRUE(after.acquired);
  EXPECT_EQ((std::vector<uint64_t>{800001, 800002}), after.uids);
  const auto oldPlan = lookup(oldPlanKey);
  ASSERT_EQ(0, oldPlan.code);
  EXPECT_TRUE(oldPlan.acquired);
  EXPECT_EQ((std::vector<uint64_t>{800001, 800002}), oldPlan.uids);
  ASSERT_EQ(0, metaStableTagFilterCacheDropSTable(vnode_.pMeta, kSuid));
  const auto rebuilt = lookup(oldPlanKey, true);
  ASSERT_EQ(0, rebuilt.code);
  EXPECT_TRUE(rebuilt.acquired);
  EXPECT_EQ((std::vector<uint64_t>{800001, 800002}), rebuilt.uids);
  const auto newPlan = lookup(key());
  ASSERT_EQ(0, newPlan.code);
  EXPECT_TRUE(newPlan.acquired);
  EXPECT_EQ((std::vector<uint64_t>{800001, 800002}), newPlan.uids);
}

INSTANTIATE_TEST_SUITE_P(VariableTags, MetaVarTagFilterCacheTest,
                         ::testing::Values(TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_VARBINARY, TSDB_DATA_TYPE_NCHAR));

TEST_F(MetaTagFilterCacheTest, RenameTagPreservesCompleteMapping) {
  ASSERT_EQ(0, putChild(800001, 1));
  expectHit(1, {800001}, true);
  tstrncpy(tagSchema_.name, "renamed_pile_code", sizeof(tagSchema_.name));
  ASSERT_EQ(0, alterTagSchema());
  expectHit(1, {800001});
}

TEST_F(MetaTagFilterCacheTest, ColdMissingKeyStillNeedsWarmup) {
  const auto result = lookup(42);
  ASSERT_EQ(0, result.code);
  EXPECT_FALSE(result.acquired);
  EXPECT_TRUE(result.needWarmup);
  EXPECT_TRUE(result.uids.empty());
}

TEST_F(MetaTagFilterCacheTest, CompleteWarmupMakesMissingKeyAnEmptyHit) {
  ASSERT_EQ(0, putChild(800001, 1));
  expectHit(1, {800001}, true);
  expectHit(2, {});
  expectHit(3, {}, true);
}

TEST_F(MetaTagFilterCacheTest, EmptySuperTableHasCompleteEmptyMapping) {
  expectHit(1, {}, true);
  expectHit(2, {});
}

TEST_F(MetaTagFilterCacheTest, CursorReadFailureDoesNotPublishPartialMapping) {
  ASSERT_EQ(0, putChild(800001, 1));
  ASSERT_EQ(0, putChild(800002, 2));
  cursorNextBeforeFailure = 1;
  const auto failed = lookup(2, true);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, failed.code);
  EXPECT_FALSE(failed.acquired);
  const auto missing = lookup(2);
  EXPECT_FALSE(missing.acquired);
  EXPECT_TRUE(missing.needWarmup);
  expectHit(2, {800002}, true);
  expectHit(1, {800001});
}

TEST_F(MetaTagFilterCacheTest, CursorSeekFailureDoesNotPublishEmptyMapping) {
  ASSERT_EQ(0, putChild(800001, 1));
  failCursorSeek = true;
  const auto failed = lookup(1, true);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, failed.code);
  EXPECT_FALSE(failed.acquired);
  const auto missing = lookup(1);
  EXPECT_FALSE(missing.acquired);
  EXPECT_TRUE(missing.needWarmup);
  expectHit(1, {800001}, true);
}

TEST_F(MetaTagFilterCacheTest, CursorInitialAdjustmentFailurePreservesError) {
  ASSERT_EQ(0, putChild(800001, 1));
  failCursorAdjust = true;
  terrno = 0;
  SMCtbCursor* cursor = metaOpenCtbCursor(&vnode_, kSuid + 1, 1, 0);
  EXPECT_EQ(nullptr, cursor);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, terrno);
  metaCloseCtbCursor(cursor);
  expectHit(1, {800001}, true);
}

TEST_F(MetaTagFilterCacheTest, PendingCreateCannotBecomePermanentEmptyHitAfterCommit) {
  ASSERT_EQ(0, putPendingChild(800001, 1));
  ASSERT_TRUE(metaHasPendingTxnEntries(vnode_.pMeta));
  const auto pending = lookup(1, true);
  EXPECT_EQ(0, pending.code);
  EXPECT_FALSE(pending.acquired);
  EXPECT_TRUE(lookup(1).needWarmup);
  ASSERT_EQ(0, commitPendingChildren());
  ASSERT_FALSE(metaHasPendingTxnEntries(vnode_.pMeta));
  expectHit(1, {800001}, true);
  expectHit(2, {});
}

TEST_F(MetaTagFilterCacheTest, PendingCreatesRemainRetryableUntilLazyCommitVacuumCompletes) {
  ASSERT_EQ(0, putChild(800000, 0));
  for (int32_t value = 1; value <= 129; ++value) {
    ASSERT_EQ(0, putPendingChild(800000 + value, value));
  }
  EXPECT_FALSE(lookup(129, true).acquired);
  // Take the vacuum slot so the test can run the real batches synchronously.
  vnode_.vacuumRunning = 1;
  ASSERT_EQ(0, commitPendingChildren());
  ASSERT_EQ(TXN_META_COMMITTED, metaGetTxnMetaStatus(vnode_.pMeta, kTxnId));
  EXPECT_FALSE(lookup(129, true).acquired);
  ASSERT_EQ(129, vnodeTxnVacuumBatch(&vnode_, 256));
  vnode_.vacuumRunning = 0;
  ASSERT_FALSE(metaHasPendingTxnEntries(vnode_.pMeta));
  expectHit(129, {800129}, true);
  expectHit(0, {800000});
  expectHit(1, {800001});
}

TEST_F(MetaTagFilterCacheTest, WarmupAdmitsMappingLargerThanOrdinaryTagCacheLimit) {
  for (int32_t i = 0; i < 512; ++i) {
    ASSERT_EQ(0, putChild(800000 + i, i));
  }
  // 512 distinct digest/UID entries exceed 10 KiB but are far below 2 MiB.
  expectHit(0, {800000}, true);
  expectHit(511, {800511});
  expectHit(600, {});
}

TEST_F(MetaTagFilterCacheTest, WarmupAcceptsTwoMiBAndFallsBackAboveTheLimit) {
  constexpr int64_t limit = 2 * 1024 * 1024;
  constexpr int64_t entryBytes = 16 + sizeof(SArray) + sizeof(int32_t) + sizeof(uint64_t);
  constexpr int32_t distinct = (limit / entryBytes) & ~1;
  constexpr int32_t duplicates = (limit - distinct * entryBytes) / sizeof(uint64_t);
  static_assert(distinct * entryBytes + duplicates * sizeof(uint64_t) == limit, "exact payload boundary");
  const auto ordinaryLimit = tsTagFilterResCacheSize;
  for (int32_t i = 0; i < distinct + duplicates; ++i) {
    ASSERT_EQ(0, putChild(800000 + i, i < distinct ? i : 0));
  }
  expectHit(distinct - 1, {800000 + static_cast<uint64_t>(distinct) - 1}, true);
  expectHit(distinct, {});

  ASSERT_EQ(0, putChild(900000, distinct));
  ASSERT_EQ(0, metaStableTagFilterCacheDropSTable(vnode_.pMeta, kSuid));
  const auto tooLarge = lookup(distinct, true);
  ASSERT_EQ(0, tooLarge.code);
  EXPECT_FALSE(tooLarge.acquired);
  const auto missing = lookup(distinct + 1);
  ASSERT_EQ(0, missing.code);
  EXPECT_FALSE(missing.acquired);
  EXPECT_FALSE(missing.needWarmup);
  EXPECT_EQ(ordinaryLimit, tsTagFilterResCacheSize);
}

TEST_F(MetaTagFilterCacheTest, MetadataInsertAtUnlockCannotBeOverwrittenByWarmupPublication) {
  ASSERT_EQ(0, putChild(800001, 1));
  bool inserted = false;
  onMetaUnlock = [&] {
    ASSERT_EQ(0, putChild(800002, 2));
    inserted = true;
  };
  // Deterministically run the real metadata writer at the first lock release
  // during warmup. Previously this was between map construction and publication.
  stub_.set(metaULock, unlockMetaThenMutate);
  expectHit(1, {800001}, true);
  ASSERT_TRUE(inserted);
  expectHit(2, {800002});
}

TEST_F(MetaTagFilterCacheTest, CompleteMappingTracksInsertUpdateNullAndDrop) {
  ASSERT_EQ(0, putChild(800001, 1));
  expectHit(1, {800001}, true);
  ASSERT_EQ(0, putChild(800002, 2));
  expectHit(2, {800002});
  ASSERT_EQ(0, putChild(800002, 3));
  expectHit(2, {});
  expectHit(3, {800002});
  ASSERT_EQ(0, putChild(800002, 0, true));
  expectHit(3, {});
  ASSERT_EQ(0, putChild(800002, 1));
  expectHit(1, {800001, 800002});
  ASSERT_EQ(0, dropChild(800001));
  expectHit(1, {800002});
  ASSERT_EQ(0, dropChild(800002));
  expectHit(1, {});
}

TEST_F(MetaTagFilterCacheTest, LateIndividualFillCannotPolluteCompleteMapping) {
  ASSERT_EQ(0, putChild(800001, 1));
  expectHit(1, {800001}, true);
  ArrayOwner uids(taosArrayInit(1, sizeof(uint64_t)), taosArrayDestroy);
  ASSERT_NE(nullptr, uids);
  const uint64_t staleUid = 800002;
  ASSERT_NE(nullptr, taosArrayPush(uids.get(), &staleUid));
  SArray* columns = taosArrayDup(columns_.get(), nullptr);
  ASSERT_NE(nullptr, columns);
  const auto    key = tagKey(2);
  const int32_t code = metaStableTagFilterCachePut(&vnode_, kSuid, kCondition, sizeof(kCondition), key.data(),
                                                   key.size(), uids.get(), &columns);
  taosArrayDestroy(columns);
  ASSERT_EQ(0, code);
  expectHit(2, {});
}

TEST_F(MetaTagFilterCacheTest, FailedUidAppendInvalidatesIncompleteCachedResults) {
  for (uint64_t uid = 800001; uid <= 800004; ++uid) {
    ASSERT_EQ(0, putChild(uid, 1));
  }
  expectHit(1, {800001, 800002, 800003, 800004}, true);
  // The metadata writer succeeds even when its optional cache update fails.
  ASSERT_EQ(0, putChild(800005, 1, false, true));
  ASSERT_TRUE(uidAppendFailed);
  const auto existing = lookup(1);
  ASSERT_EQ(0, existing.code);
  EXPECT_FALSE(existing.acquired);
  const auto missing = lookup(2);
  ASSERT_EQ(0, missing.code);
  EXPECT_FALSE(missing.acquired);
  expectHit(1, {800001, 800002, 800003, 800004, 800005}, true);
}

TEST_F(MetaTagFilterCacheTest, FailedNewKeyInsertInvalidatesCompleteMapping) {
  ASSERT_EQ(0, putChild(800001, 1));
  expectHit(1, {800001}, true);
  ASSERT_EQ(0, putChild(800002, 2, false, true));
  ASSERT_TRUE(uidAppendFailed);
  const auto result = lookup(2);
  ASSERT_EQ(0, result.code);
  EXPECT_FALSE(result.acquired);
  expectHit(2, {800002}, true);
  expectHit(1, {800001});
}

// Single-JSON-tag SET TAG persists the client-supplied blob verbatim and re-reads it with
// tTagGet on every later query, so a forged STag header (self-declared len/nTag disagreeing
// with the bytes actually transmitted) must be rejected before it can reach the meta store.
// Regression for the metaUpdateTableJsonTagValue write-path guard (TS-7114608840, engine
// common/block finding 225 write side).
TEST_F(MetaTagFilterCacheTest, JsonTagSetTagRejectsForgedBlobHeader) {
  constexpr tb_uid_t kJsonSuid = 700002;
  constexpr col_id_t kJsonTagId = 101;

  SSchema rows[2] = {};
  rows[0].type = TSDB_DATA_TYPE_TIMESTAMP;
  rows[0].colId = 1;
  rows[0].bytes = sizeof(int64_t);
  tstrncpy(rows[0].name, "ts", sizeof(rows[0].name));
  rows[1].type = TSDB_DATA_TYPE_INT;
  rows[1].colId = 2;
  rows[1].bytes = sizeof(int32_t);
  tstrncpy(rows[1].name, "v", sizeof(rows[1].name));

  SSchema jtag = {};
  jtag.type = TSDB_DATA_TYPE_JSON;
  jtag.colId = kJsonTagId;
  jtag.bytes = TSDB_MAX_TAGS_LEN + VARSTR_HEADER_SIZE;
  tstrncpy(jtag.name, "j", sizeof(jtag.name));

  SVCreateStbReq stReq = {};
  stReq.name = const_cast<char*>("json_st");
  stReq.suid = kJsonSuid;
  stReq.colCmpred = 1;
  stReq.schemaRow.nCols = 2;
  stReq.schemaRow.pSchema = rows;
  stReq.schemaTag.nCols = 1;
  stReq.schemaTag.pSchema = &jtag;
  SColCmpr cmp[2] = {};
  for (int i = 0; i < 2; ++i) {
    cmp[i].id = rows[i].colId;
    cmp[i].alg = createDefaultColCmprByType(rows[i].type);
  }
  stReq.colCmpr.nCols = 2;
  stReq.colCmpr.pColCmpr = cmp;
  ASSERT_EQ(0, metaCreateSuperTable(vnode_.pMeta, ++version_, &stReq));

  // child table of json_st with a valid (empty) JSON tag blob
  constexpr uint64_t uid = 800101;
  const std::string  name = "json_c" + std::to_string(uid);
  STag               emptyTag = {};
  emptyTag.flags = TD_TAG_JSON;
  emptyTag.len = sizeof(STag);
  emptyTag.nTag = 0;
  SMetaEntry child = {};
  child.version = ++version_;
  child.type = TSDB_CHILD_TABLE;
  child.uid = uid;
  child.name = const_cast<char*>(name.c_str());
  child.ctbEntry.suid = kJsonSuid;
  child.ctbEntry.pTags = reinterpret_cast<uint8_t*>(&emptyTag);
  ASSERT_EQ(0, metaHandleEntry2(vnode_.pMeta, &child));

  // a valid single-entry JSON tag blob, and a heap copy we can forge
  SArray* values = taosArrayInit(1, sizeof(STagVal));
  ASSERT_NE(nullptr, values);
  STagVal tv = {};
  tv.pKey = const_cast<char*>("k");
  tv.type = TSDB_DATA_TYPE_JSON;
  const char* json = "{\"k\":1}";
  tv.pData = reinterpret_cast<uint8_t*>(const_cast<char*>(json));
  tv.nData = strlen(json);
  ASSERT_NE(nullptr, taosArrayPush(values, &tv));
  STag* goodTag = nullptr;
  ASSERT_EQ(0, tTagNew(values, 0, 1, &goodTag));
  taosArrayDestroy(values);
  const uint32_t goodLen = goodTag->len;
  ASSERT_GT(goodLen, sizeof(STag));
  ASSERT_EQ(0, tTagValidate(goodTag, goodLen));  // sanity: the generated blob is well-formed

  auto setJsonTag = [&](STag* blob, uint32_t blobLen) -> int32_t {
    SArray* tags = taosArrayInit(1, sizeof(SUpdatedTagVal));
    if (tags == nullptr) {
      return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    }
    SUpdatedTagVal upd = {};
    upd.tagName = const_cast<char*>("j");
    upd.colId = kJsonTagId;
    upd.tagType = TSDB_DATA_TYPE_JSON;
    upd.pTagVal = reinterpret_cast<uint8_t*>(blob);
    upd.nTagVal = blobLen;
    if (taosArrayPush(tags, &upd) == nullptr) {
      taosArrayDestroy(tags);
      return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    }
    SUpdateTableTagVal table = {};
    table.tbName = const_cast<char*>(name.c_str());
    table.tags = tags;
    SArray* tables = taosArrayInit(1, sizeof(SUpdateTableTagVal));
    if (tables == nullptr) {
      taosArrayDestroy(tags);
      return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    }
    if (taosArrayPush(tables, &table) == nullptr) {
      taosArrayDestroy(tags);
      taosArrayDestroy(tables);
      return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    }
    SVAlterTbReq req = {};
    req.action = TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL;
    req.tables = tables;
    int32_t code = metaAlterTable(vnode_.pMeta, ++version_, &req, nullptr);
    taosArrayDestroy(tags);
    taosArrayDestroy(tables);
    return code;
  };

  // a well-formed blob passes
  EXPECT_EQ(0, setJsonTag(goodTag, goodLen));

  // forged: header claims len=INT16_MAX while only goodLen bytes were transmitted
  std::vector<uint8_t> forgedLenBuf(reinterpret_cast<uint8_t*>(goodTag), reinterpret_cast<uint8_t*>(goodTag) + goodLen);
  STag*                forgedLen = reinterpret_cast<STag*>(forgedLenBuf.data());
  forgedLen->len = INT16_MAX;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, setJsonTag(forgedLen, goodLen));

  // forged: nTag=100 makes the idx[] array run past the transmitted bytes
  std::vector<uint8_t> forgedNtBuf(reinterpret_cast<uint8_t*>(goodTag), reinterpret_cast<uint8_t*>(goodTag) + goodLen);
  STag*                forgedNt = reinterpret_cast<STag*>(forgedNtBuf.data());
  forgedNt->nTag = 100;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, setJsonTag(forgedNt, goodLen));

  // forged: an unknown flag bit is rejected
  std::vector<uint8_t> forgedFlBuf(reinterpret_cast<uint8_t*>(goodTag), reinterpret_cast<uint8_t*>(goodTag) + goodLen);
  STag*                forgedFl = reinterpret_cast<STag*>(forgedFlBuf.data());
  forgedFl->flags |= static_cast<int8_t>(0x01);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, setJsonTag(forgedFl, goodLen));

  tTagFree(goodTag);
}

// SET TAG on a fixed-width tag must reject a value whose length differs from the type's width: the length comes from
// the request and is otherwise used to copy into / compare against an 8-byte value.
TEST_F(MetaTagFilterCacheTest, SetTagRejectsWrongFixedWidthValue) {
  constexpr uint64_t uid = 900001;
  ASSERT_EQ(0, putChild(uid, 1));
  const std::string name = "p" + std::to_string(uid);

  auto setTag = [&](uint32_t nVal) -> int32_t {
    std::vector<uint8_t> value(64, 0x7f);
    SUpdatedTagVal       upd = {};
    upd.tagName = const_cast<char*>("pile_code");
    upd.colId = kTagId;
    upd.tagType = TSDB_DATA_TYPE_INT;
    upd.pTagVal = value.data();
    upd.nTagVal = nVal;
    SArray* tags = taosArrayInit(1, sizeof(SUpdatedTagVal));
    SArray* tables = taosArrayInit(1, sizeof(SUpdateTableTagVal));
    if (tags == nullptr || tables == nullptr) return TSDB_CODE_OUT_OF_MEMORY;
    taosArrayPush(tags, &upd);
    SUpdateTableTagVal table = {};
    table.tbName = const_cast<char*>(name.c_str());
    table.tags = tags;
    taosArrayPush(tables, &table);
    SVAlterTbReq req = {};
    req.action = TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL;
    req.tables = tables;
    int32_t code = metaAlterTable(vnode_.pMeta, ++version_, &req, nullptr);
    taosArrayDestroy(tags);
    taosArrayDestroy(tables);
    return code;
  };

  EXPECT_EQ(TSDB_CODE_INVALID_MSG, setTag(16));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, setTag(2));
  EXPECT_EQ(0, setTag(sizeof(int32_t)));
}

}  // namespace

extern "C" int meteDecodeColRefEntry(SDecoder *pDecoder, SMetaEntry *pME);

// GHSA-hr67-c6c8-mwpx / TD-SEC-2026-032: meteDecodeColRefEntry allocates
// nCols * sizeof(SColRef) via tDecoderMalloc, which now rejects sizes that
// would truncate (int32 width) or wrap before allocating. A virtual table's
// column-reference wrapper carrying a huge nCols (zigzag i32) must fail
// decoding instead of getting a truncated allocation that the per-column
// loop then writes past.
TEST(MetaColRefDecodeOverflow, RejectsOversizedNCols) {
  std::array<uint8_t, 32> buf = {};
  SEncoder                encoder = {};
  tEncoderInit(&encoder, buf.data(), (uint32_t)buf.size());
  ASSERT_EQ(tStartEncode(&encoder), 0);
  // 2^28 decodes to a huge SColRef count; sizeof(SColRef) * nCols overflows
  // INT32_MAX and must be rejected by tDecoderMalloc rather than truncated.
  ASSERT_EQ(tEncodeI32v(&encoder, (int32_t)(1 << 28)), 0);
  ASSERT_EQ(tEncodeI32v(&encoder, 0), 0);  // version
  tEndEncode(&encoder);
  int32_t size = encoder.pos;
  tEncoderClear(&encoder);

  SMetaEntry entry = {};
  SDecoder   decoder = {};
  tDecoderInit(&decoder, buf.data(), size);
  // The encoder used tStartEncode/tEndEncode, so the buffer carries a 4-byte length
  // prefix. Consume it with the matching tStartDecode before reading the fields,
  // mirroring the production metaDecodeEntryImpl path.
  ASSERT_EQ(tStartDecode(&decoder), 0);
  int32_t code = meteDecodeColRefEntry(&decoder, &entry);
  EXPECT_NE(code, 0);
  EXPECT_EQ(entry.colRef.pColRef, nullptr);
  tDecoderClear(&decoder);
}
