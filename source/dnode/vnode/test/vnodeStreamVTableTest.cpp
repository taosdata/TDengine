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

// Unit tests for the small leaf helpers in vnodeStreamVTable.c that have
// previously been exercised only as part of the full streamBatchFanoutDrain
// fan-out path. The helpers were de-static'd and forward-declared in
// vnodeStreamVTable.h so the test binary can invoke them without going through
// SVnode/RPC machinery.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "functionMgt.h"
#include "query.h"
#include "streamMsg.h"
#include "streamReader.h"
#include "stub.h"
#include "tarray.h"
#include "thash.h"
#include "vnodeStreamVTable.h"
#include "vnodeInt.h"

// Stub: dmNotifyHdl is defined in mgmt_dnode which is not linked in this unit
// test binary. Provide a minimal definition so the vnode object files resolve.
#include "monitor.h"
SDmNotifyHandle dmNotifyHdl = {0};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

SStreamVTableInfoCache *makeEmptyCache() {
  auto *pCache = (SStreamVTableInfoCache *)taosMemoryCalloc(1, sizeof(SStreamVTableInfoCache));
  if (pCache == nullptr) return nullptr;
  pCache->tblRefCache = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                     false, HASH_NO_LOCK);
  return pCache;
}

void freeCache(SStreamVTableInfoCache *pCache) {
  if (pCache == nullptr) return;
  if (pCache->tblRefCache != nullptr) {
    // Free any tagData payloads we cached during the test.
    void   *p   = nullptr;
    int32_t it  = 0;
    while ((p = taosHashIterate(pCache->tblRefCache, p)) != nullptr) {
      auto *item = (SVTableRefResolveRspItem *)p;
      if (item->tagData != nullptr) taosMemoryFree(item->tagData);
      if (item->nextRef.tagCondJson != nullptr) taosMemoryFree(item->nextRef.tagCondJson);
    }
    (void)it;
    taosHashCleanup(pCache->tblRefCache);
  }
  taosMemoryFree(pCache);
}

SResolveWorkItem mkWorkItem(const char *db, const char *tb, const char *col, int8_t kind = 0) {
  SResolveWorkItem w{};
  w.kind = kind;
  std::snprintf(w.refDbName,    TSDB_DB_NAME_LEN,    "%s", db);
  std::snprintf(w.refTableName, TSDB_TABLE_NAME_LEN, "%s", tb);
  std::snprintf(w.refColName,   TSDB_COL_NAME_LEN,   "%s", col);
  return w;
}

SNode *makeStreamRangePlaceholder(const char *name, int32_t layerIndex = -1) {
  const int32_t funcId = fmGetFuncId(name);
  EXPECT_GE(funcId, 0);
  SFunctionNode *func = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode **>(&func)));
  if (func == nullptr) return nullptr;
  func->funcId = funcId;
  func->funcType = fmGetFuncTypeFromId(funcId);
  func->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  func->node.resType.bytes = sizeof(int64_t);

  SValueNode *result = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&result)));
  if (result == nullptr) {
    nodesDestroyNode(reinterpret_cast<SNode *>(func));
    return nullptr;
  }
  result->node.resType = func->node.resType;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&func->pParameterList, reinterpret_cast<SNode *>(result)));
  if (layerIndex >= 0) {
    SValueNode *layer = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&layer)));
    if (layer == nullptr) {
      nodesDestroyNode(reinterpret_cast<SNode *>(func));
      return nullptr;
    }
    layer->node.resType.type = TSDB_DATA_TYPE_INT;
    layer->node.resType.bytes = sizeof(int32_t);
    layer->datum.i = layerIndex;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&func->pParameterList, reinterpret_cast<SNode *>(layer)));
  }
  return reinterpret_cast<SNode *>(func);
}

SNode *makeStreamRangeBound(EOperatorType opType, const char *placeholder, int32_t layerIndex = -1) {
  SColumnNode *column = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&column)));
  if (column == nullptr) return nullptr;
  column->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  column->node.resType.bytes = sizeof(int64_t);
  column->colId = PRIMARYKEY_TIMESTAMP_COL_ID;

  SOperatorNode *op = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&op)));
  if (op == nullptr) {
    nodesDestroyNode(reinterpret_cast<SNode *>(column));
    return nullptr;
  }
  op->node.resType.type = TSDB_DATA_TYPE_BOOL;
  op->node.resType.bytes = sizeof(bool);
  op->opType = opType;
  op->pLeft = reinterpret_cast<SNode *>(column);
  op->pRight = makeStreamRangePlaceholder(placeholder, layerIndex);
  if (op->pRight == nullptr) {
    nodesDestroyNode(reinterpret_cast<SNode *>(op));
    return nullptr;
  }
  return reinterpret_cast<SNode *>(op);
}

void setNodeGroupReadRange(SStreamRuntimeFuncInfo *runtime, int32_t index, int64_t gid, int64_t firstStart,
                           int64_t lastEnd) {
  auto *read = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(runtime->curGrpRead, index));
  ASSERT_NE(nullptr, read);
  read->gid = gid;
  read->firstParam.wstart = firstStart;
  read->lastParam.wend = lastEnd;
}

void appendNodeGroupRangeContext(SStreamRuntimeFuncInfo *runtime, int64_t gid, int32_t readInfoIndex, int64_t start,
                                 int64_t end) {
  if (runtime->pContextPolicy == nullptr) {
    runtime->pContextPolicy = static_cast<SStreamContextPolicy *>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
    ASSERT_NE(nullptr, runtime->pContextPolicy);
    runtime->pContextPolicy->pEntries = taosArrayInit(2, sizeof(SStreamContextPolicyEntry));
    ASSERT_NE(nullptr, runtime->pContextPolicy->pEntries);
  }
  SStreamContextPolicyEntry policy = {};
  policy.gid = gid;
  policy.paramIndex = readInfoIndex;
  policy.contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR;
  ASSERT_NE(nullptr, taosArrayPush(runtime->pContextPolicy->pEntries, &policy));

  if (runtime->pAncestorContext == nullptr) {
    runtime->pAncestorContext =
        static_cast<SStreamAncestorContext *>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
    ASSERT_NE(nullptr, runtime->pAncestorContext);
    runtime->pAncestorContext->pParamContexts = taosArrayInit(2, sizeof(SStreamAncestorParamContext));
    runtime->pAncestorContext->pReadScopeBindings = taosArrayInit(2, sizeof(SStreamReadScopeBinding));
    ASSERT_NE(nullptr, runtime->pAncestorContext->pParamContexts);
    ASSERT_NE(nullptr, runtime->pAncestorContext->pReadScopeBindings);
  }
  SStreamAncestorParamContext context = {};
  context.paramIndex = readInfoIndex;
  context.leafIdentity.gid = gid;
  context.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
  context.leafIdentity.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  ASSERT_NE(nullptr, context.leafIdentity.lineage.pScopes);
  SScopeInstanceId scope = {};
  scope.layerIndex = 0;
  scope.triggerType = WINDOW_TYPE_INTERVAL;
  scope.openingTs = start;
  scope.nativeDiscriminator = gid;
  ASSERT_NE(nullptr, taosArrayPush(context.leafIdentity.lineage.pScopes, &scope));
  context.pSnapshots = taosArrayInit(1, sizeof(SWindowAncestorSnapshot));
  ASSERT_NE(nullptr, context.pSnapshots);
  SWindowAncestorSnapshot snapshot = {};
  snapshot.layerIndex = 0;
  snapshot.triggerType = WINDOW_TYPE_INTERVAL;
  snapshot.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND;
  snapshot.values.window.start = start;
  snapshot.values.window.end = end;
  ASSERT_NE(nullptr, taosArrayPush(context.pSnapshots, &snapshot));
  ASSERT_NE(nullptr, taosArrayPush(runtime->pAncestorContext->pParamContexts, &context));

  SStreamReadScopeBinding binding = {};
  binding.vgId = 7;
  binding.readInfoIndex = readInfoIndex;
  binding.scope.gid = gid;
  binding.scope.lineage.pScopes = taosArrayDup(context.leafIdentity.lineage.pScopes, nullptr);
  ASSERT_NE(nullptr, binding.scope.lineage.pScopes);
  ASSERT_NE(nullptr, taosArrayPush(runtime->pAncestorContext->pReadScopeBindings, &binding));
}

}  // namespace

class VnodeStreamVTableMetadataTest : public testing::Test {
 protected:
  void SetUp() override {
    readerInfo_.pTask = &task_;
    readerInfo_.storageApi.metaReaderFn.getTableEntryByVersionUid = [](SMetaReader *reader, int64_t, tb_uid_t uid) {
      return uid == 1 ? loadMeta(reader) : TSDB_CODE_PAR_TABLE_NOT_EXIST;
    };
    readerInfo_.storageApi.metaReaderFn.getTableEntryByUid = [](SMetaReader *reader, tb_uid_t uid) {
      return uid == 2 ? loadMeta(reader) : TSDB_CODE_PAR_TABLE_NOT_EXIST;
    };
    cids_ = taosArrayInit(1, sizeof(col_id_t));
    infos_ = taosArrayInit(3, sizeof(VTableInfo));
    result_.colMap = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
    ASSERT_NE(nullptr, cids_);
    ASSERT_NE(nullptr, infos_);
    ASSERT_NE(nullptr, result_.colMap);
    col_id_t cid = 2;
    ASSERT_NE(nullptr, taosArrayPush(cids_, &cid));
    item_.hasRef = true;
    item_.refType = 1;
    item_.tagCondJson = const_cast<char *>("resolved-series");
    item_.tagCondLen = strlen(item_.tagCondJson);
    SColResolveItem *item = &item_;
    ASSERT_EQ(0, tSimpleHashPut(result_.colMap, &cid, sizeof(cid), &item, sizeof(item)));
  }

  void TearDown() override {
    for (size_t index = 0; index < taosArrayGetSize(infos_); ++index) {
      auto *info = static_cast<VTableInfo *>(taosArrayGet(infos_, index));
      for (int32_t column = 0; column < info->cols.nCols; ++column) {
        taosMemoryFree(info->cols.pColRef[column].tagCondJson);
      }
      taosMemoryFree(info->cols.pColRef);
    }
    taosArrayDestroy(infos_);
    taosArrayDestroy(cids_);
    tSimpleHashCleanup(result_.colMap);
    tDecoderClear(&metaReader_.coder);
  }

  static int32_t loadMeta(SMetaReader *reader) {
    reader->me.name = const_cast<char *>("vtable");
    reader->me.colRef.version = 7;
    reader->me.colRef.nCols = 1;
    reader->me.colRef.pColRef = static_cast<SColRef *>(tDecoderMalloc(&reader->coder, sizeof(SColRef)));
    if (reader->me.colRef.pColRef == nullptr) return TSDB_CODE_OUT_OF_MEMORY;
    reader->me.colRef.pColRef[0].id = 2;
    return TSDB_CODE_SUCCESS;
  }

  int32_t fill(int64_t uid, SArray *cids) {
    return streamFillVTableInfoFromResolved(&vnode_, &readerInfo_, uid, 1, 1, cids, &result_, &metaReader_, infos_);
  }

  SVnode vnode_ = {};
  SStreamTask task_ = {};
  SStreamTriggerReaderInfo readerInfo_ = {};
  SMetaReader metaReader_ = {};
  SVTableResolveResult result_ = {};
  SColResolveItem item_ = {};
  SArray *cids_ = nullptr;
  SArray *infos_ = nullptr;
};

TEST_F(VnodeStreamVTableMetadataTest, MissingMetadataAfterDecoderReleaseUsesOwnedResolvedCondition) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fill(1, cids_));
  EXPECT_EQ(nullptr, metaReader_.coder.mList);
  ASSERT_EQ(TSDB_CODE_SUCCESS, fill(3, cids_));
  auto *info = static_cast<VTableInfo *>(taosArrayGet(infos_, 1));
  ASSERT_EQ(1, info->cols.nCols);
  EXPECT_EQ(0, info->cols.version);
  EXPECT_STREQ("resolved-series", info->cols.pColRef[0].tagCondJson);
}

TEST_F(VnodeStreamVTableMetadataTest, UidFallbackLoadsVersionAndAllColumnsAndReleasesDecoder) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, fill(2, nullptr));
  auto *info = static_cast<VTableInfo *>(taosArrayGet(infos_, 0));
  ASSERT_EQ(1, info->cols.nCols);
  EXPECT_EQ(7, info->cols.version);
  EXPECT_STREQ("vtable", info->tbName);
  EXPECT_STREQ("resolved-series", info->cols.pColRef[0].tagCondJson);
  EXPECT_EQ(nullptr, metaReader_.coder.mList);
}

// nodesNodeToString() reports the SERIES condition length including the trailing NUL and the
// resolver keeps that length; a multi-layer virtual table without an own condition hits this
// fallback. The filled col ref must follow the direct-reference contract (NUL-free length),
// otherwise stStreamExtCanonicalizeJson rejects the registry identity and the stream cannot start.
TEST_F(VnodeStreamVTableMetadataTest, ResolvedTagConditionDropsTrailingNulFromLength) {
  item_.tagCondJson = const_cast<char *>("nul-terminated-series");
  item_.tagCondLen = (int32_t)(strlen(item_.tagCondJson) + 1);

  ASSERT_EQ(TSDB_CODE_SUCCESS, fill(3, cids_));
  auto *info = static_cast<VTableInfo *>(taosArrayGet(infos_, 0));
  ASSERT_EQ(1, info->cols.nCols);
  EXPECT_STREQ("nul-terminated-series", info->cols.pColRef[0].tagCondJson);
  EXPECT_EQ((int32_t)strlen("nul-terminated-series"), info->cols.pColRef[0].tagCondLen);
  EXPECT_EQ(nullptr, metaReader_.coder.mList);
}

TEST(VnodeStreamVTableHelpers, RejectsExternalWhereBeforeConditionIsDiscarded) {
  SColRef ref = {};
  ref.hasRef = true;
  ref.refType = 1;
  ref.filterCondJson = const_cast<char *>("external-filter");
  ref.filterCondLen = strlen(ref.filterCondJson);
  EXPECT_EQ(TSDB_CODE_OPS_NOT_SUPPORT, streamValidateVTableColRef(&ref));
  ref.filterCondLen = 0;
  EXPECT_EQ(TSDB_CODE_OPS_NOT_SUPPORT, streamValidateVTableColRef(&ref));
  ref.filterCondJson = nullptr;
  ref.filterCondLen = 1;
  EXPECT_EQ(TSDB_CODE_OPS_NOT_SUPPORT, streamValidateVTableColRef(&ref));
}

TEST(VnodeStreamVTableHelpers, AllowsExternalSeriesAndUnfilteredReferences) {
  SColRef ref = {};
  ref.hasRef = true;
  ref.refType = 1;
  EXPECT_EQ(TSDB_CODE_SUCCESS, streamValidateVTableColRef(&ref));
  ref.tagCondJson = const_cast<char *>("series-condition");
  ref.tagCondLen = strlen(ref.tagCondJson);
  EXPECT_EQ(TSDB_CODE_SUCCESS, streamValidateVTableColRef(&ref));
  ref.refType = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, streamValidateVTableColRef(&ref));
  EXPECT_EQ(TSDB_CODE_SUCCESS, streamValidateVTableColRef(nullptr));
}

// ---------------------------------------------------------------------------
// tagValueEqual
// ---------------------------------------------------------------------------

TEST(VnodeStreamVTableHelpers, tagValueEqual_BothNullEqual) {
  EXPECT_TRUE(tagValueEqual(nullptr, nullptr));
}

TEST(VnodeStreamGroupReadTest, NodeBatchUnionsRawAndCalculatedReadRanges) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  auto *first = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(runtime.curGrpRead, 0));
  auto *second = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(runtime.curGrpRead, 1));
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  setNodeGroupReadRange(&runtime, 0, 101, 1000, 1080);
  setNodeGroupReadRange(&runtime, 1, 202, 3000, 3050);
  appendNodeGroupRangeContext(&runtime, 101, 0, 100, 180);
  appendNodeGroupRangeContext(&runtime, 202, 1, 300, 350);

  STimeWindow range = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, vnodeCollectNodeGroupReadRange(&runtime, &range));
  EXPECT_EQ(1000, range.skey);
  EXPECT_EQ(3050, range.ekey);

  STimeRangeNode node = {};
  node.type = QUERY_NODE_TIME_RANGE;
  node.needCalc = true;
  node.pStart = makeStreamRangeBound(OP_TYPE_GREATER_EQUAL, "_twstart", 0);
  node.pEnd = makeStreamRangeBound(OP_TYPE_LOWER_EQUAL, "_twend", 0);
  ASSERT_NE(nullptr, node.pStart);
  ASSERT_NE(nullptr, node.pEnd);
  SStreamRuntimeFuncInfo temporary = {};
  temporary.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, temporary.pStreamPesudoFuncVals);
  bool calculatedValid = false;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            vnodeCalculateNodeGroupReadRange(&node, &runtime, &temporary, 7, &range, &calculatedValid));
  EXPECT_TRUE(calculatedValid);
  EXPECT_EQ(100, range.skey);
  EXPECT_EQ(350, range.ekey);

  nodesDestroyNode(node.pStart);
  nodesDestroyNode(node.pEnd);
  tDestroyStRtFuncInfo(&temporary);
  tDestroyStreamContextPolicy(&runtime.pContextPolicy);
  tDestroyStreamAncestorContext(&runtime.pAncestorContext);
  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamGroupReadTest, PreservesReadBindings) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 3);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  setNodeGroupReadRange(&runtime, 0, 101, 1000, 1080);
  setNodeGroupReadRange(&runtime, 1, 101, 3000, 3050);
  setNodeGroupReadRange(&runtime, 2, 101, 4000, 4050);
  appendNodeGroupRangeContext(&runtime, 101, 0, 10, 20);
  appendNodeGroupRangeContext(&runtime, 101, 1, 100, 110);
  appendNodeGroupRangeContext(&runtime, 101, 2, 30, 40);
  STimeRangeNode node = {};
  node.type = QUERY_NODE_TIME_RANGE;
  node.needCalc = true;
  node.pStart = makeStreamRangeBound(OP_TYPE_GREATER_EQUAL, "_twstart", 0);
  node.pEnd = makeStreamRangeBound(OP_TYPE_LOWER_THAN, "_twend", 0);
  SStreamRuntimeFuncInfo temporary = {};
  temporary.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  STimeWindow combined = {};
  bool        valid = false;
  SArray     *ranges = nullptr;
  ASSERT_EQ(0, vnodeCalculateNodeGroupReadRanges(&node, &runtime, &temporary, 7, &combined, &valid, &ranges));
  EXPECT_TRUE(valid);
  EXPECT_EQ(10, combined.skey);
  EXPECT_EQ(109, combined.ekey);
  ASSERT_EQ(3, taosArrayGetSize(ranges));
  const int64_t starts[] = {10, 100, 30};
  const int64_t ends[] = {19, 109, 39};
  for (int i = 0; i < 3; ++i) {
    const auto *range = static_cast<const SReaderReadRange *>(taosArrayGet(ranges, i));
    EXPECT_EQ(i, range->readInfoIndex);
    EXPECT_TRUE(range->valid);
    EXPECT_EQ(starts[i], range->window.skey);
    EXPECT_EQ(ends[i], range->window.ekey);
  }
  taosArrayDestroy(ranges);
  ranges = nullptr;

  runtime.withExternalWindow = true;
  runtime.externalWindowNeedsPrevRow = false;
  ASSERT_EQ(0, vnodeCalculateNodeGroupReadRanges(&node, &runtime, &temporary, 7, &combined, &valid, &ranges));
  ASSERT_EQ(3, taosArrayGetSize(ranges));
  for (int i = 0; i < 3; ++i) {
    const auto *range = static_cast<const SReaderReadRange *>(taosArrayGet(ranges, i));
    EXPECT_EQ(starts[i], range->window.skey);
  }
  taosArrayDestroy(ranges);
  ranges = nullptr;

  runtime.externalWindowNeedsPrevRow = true;
  ASSERT_EQ(0, vnodeCalculateNodeGroupReadRanges(&node, &runtime, &temporary, 7, &combined, &valid, nullptr));
  EXPECT_EQ(10, combined.skey);
  EXPECT_EQ(109, combined.ekey);
  ASSERT_EQ(0, vnodeCalculateNodeGroupReadRanges(&node, &runtime, &temporary, 7, &combined, &valid, &ranges));
  EXPECT_TRUE(valid);
  EXPECT_EQ(10, combined.skey);
  EXPECT_EQ(109, combined.ekey);
  ASSERT_EQ(3, taosArrayGetSize(ranges));
  for (int i = 0; i < 3; ++i) {
    const auto *range = static_cast<const SReaderReadRange *>(taosArrayGet(ranges, i));
    EXPECT_TRUE(range->valid);
    EXPECT_EQ(INT64_MIN, range->window.skey);
    EXPECT_EQ(ends[i], range->window.ekey);
  }
  taosArrayDestroy(ranges);
  nodesDestroyNode(node.pStart);
  nodesDestroyNode(node.pEnd);
  tDestroyStRtFuncInfo(&temporary);
  tDestroyStreamContextPolicy(&runtime.pContextPolicy);
  tDestroyStreamAncestorContext(&runtime.pAncestorContext);
  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamGroupReadTest, EmptyAndEndpointRanges) {
  for (int8_t precision = TSDB_TIME_PRECISION_MILLI; precision <= TSDB_TIME_PRECISION_NANO; ++precision) {
    for (bool calculated : {false, true}) {
      SStreamRuntimeFuncInfo runtime = {};
      runtime.isMultiGroupCalc = true;
      runtime.triggerType = STREAM_TRIGGER_SLIDING;
      runtime.precision = precision;
      runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
      setNodeGroupReadRange(&runtime, 0, 101, 10, calculated ? 10 : 20);
      setNodeGroupReadRange(&runtime, 1, 101, 10, 20);
      STimeRangeNode node = {};
      node.type = QUERY_NODE_TIME_RANGE;
      node.needCalc = calculated;
      node.pStart = makeStreamRangeBound(OP_TYPE_GREATER_EQUAL, "_twstart");
      node.pEnd = makeStreamRangeBound(OP_TYPE_LOWER_THAN, "_twend");
      SStreamRuntimeFuncInfo temporary = {};
      temporary.precision = precision;
      temporary.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
      STimeWindow combined = {};
      bool        valid = false;
      SArray     *ranges = nullptr;
      ASSERT_EQ(0, vnodeCalculateNodeGroupReadRanges(&node, &runtime, &temporary, 7, &combined, &valid, &ranges));
      ASSERT_EQ(2, taosArrayGetSize(ranges));
      const auto &emptyRange = *static_cast<const SReaderReadRange *>(taosArrayGet(ranges, 0));
      const auto &range = *static_cast<const SReaderReadRange *>(taosArrayGet(ranges, 1));
      EXPECT_EQ(!calculated, emptyRange.valid);
      EXPECT_EQ(0, emptyRange.readInfoIndex);
      EXPECT_TRUE(range.valid);
      EXPECT_EQ(10, range.window.skey);
      EXPECT_EQ(19, range.window.ekey);
      EXPECT_TRUE(valid);
      EXPECT_EQ(10, combined.skey);
      EXPECT_EQ(19, combined.ekey);
      taosArrayDestroy(ranges);
      ranges = nullptr;
      if (calculated) {
        runtime.withExternalWindow = true;
        runtime.externalWindowNeedsPrevRow = true;
        ASSERT_EQ(0, vnodeCalculateNodeGroupReadRanges(&node, &runtime, &temporary, 7, &combined, &valid, &ranges));
        ASSERT_EQ(2, taosArrayGetSize(ranges));
        const auto &emptyPrevRange = *static_cast<const SReaderReadRange *>(taosArrayGet(ranges, 0));
        const auto &prevRange = *static_cast<const SReaderReadRange *>(taosArrayGet(ranges, 1));
        EXPECT_FALSE(emptyPrevRange.valid);
        EXPECT_EQ(0, emptyPrevRange.readInfoIndex);
        EXPECT_NE(INT64_MIN, emptyPrevRange.window.skey);
        EXPECT_TRUE(prevRange.valid);
        EXPECT_EQ(INT64_MIN, prevRange.window.skey);
        EXPECT_EQ(1, prevRange.readInfoIndex);
        taosArrayDestroy(ranges);
        ranges = nullptr;
      }
      setNodeGroupReadRange(&runtime, 1, 101, INT64_MIN, INT64_MIN);
      EXPECT_EQ(TSDB_CODE_INVALID_PARA,
                vnodeCalculateNodeGroupReadRanges(&node, &runtime, &temporary, 7, &combined, &valid, &ranges));
      EXPECT_EQ(nullptr, ranges);
      EXPECT_FALSE(valid);
      nodesDestroyNode(node.pStart);
      nodesDestroyNode(node.pEnd);
      tDestroyStRtFuncInfo(&temporary);
      taosArrayDestroy(runtime.curGrpRead);
    }
  }
}

TEST(VnodeStreamGroupReadTest, RejectsRawSlidingEqualAndReversedRanges) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.triggerType = STREAM_TRIGGER_SLIDING;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  STimeRangeNode         node = {};
  SStreamRuntimeFuncInfo temporary = {};
  for (int8_t precision = TSDB_TIME_PRECISION_MILLI; precision <= TSDB_TIME_PRECISION_NANO; ++precision) {
    runtime.precision = precision;
    for (int64_t end : {10, 9}) {
      setNodeGroupReadRange(&runtime, 0, 101, 1, 2);
      setNodeGroupReadRange(&runtime, 1, 101, 10, end);
      STimeWindow combined = {};
      bool        valid = true;
      SArray     *ranges = nullptr;
      EXPECT_EQ(TSDB_CODE_INVALID_PARA,
                vnodeCalculateNodeGroupReadRanges(&node, &runtime, &temporary, 7, &combined, &valid, &ranges));
      EXPECT_FALSE(valid);
      taosArrayDestroy(ranges);
      valid = true;
      EXPECT_EQ(TSDB_CODE_INVALID_PARA,
                vnodeCalculateNodeGroupReadRange(&node, &runtime, &temporary, 7, &combined, &valid));
      EXPECT_FALSE(valid);
    }
  }
  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamGroupReadTest, RejectsCalculatedHalfOpenUpperAtInt64Min) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  setNodeGroupReadRange(&runtime, 0, 101, INT64_MIN, INT64_MIN);

  STimeRangeNode node = {};
  node.type = QUERY_NODE_TIME_RANGE;
  node.needCalc = true;
  node.pStart = makeStreamRangeBound(OP_TYPE_GREATER_EQUAL, "_twstart");
  node.pEnd = makeStreamRangeBound(OP_TYPE_LOWER_THAN, "_twend");
  ASSERT_NE(nullptr, node.pStart);
  ASSERT_NE(nullptr, node.pEnd);
  SStreamRuntimeFuncInfo temporary = {};
  temporary.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, temporary.pStreamPesudoFuncVals);
  STimeWindow range = {};
  bool        rangeValid = false;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA,
            vnodeCalculateNodeGroupReadRange(&node, &runtime, &temporary, 7, &range, &rangeValid));
  EXPECT_FALSE(rangeValid);

  nodesDestroyNode(node.pStart);
  nodesDestroyNode(node.pEnd);
  tDestroyStRtFuncInfo(&temporary);
  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamGroupReadTest, RejectsCalculatedExclusiveLowerOverflow) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  setNodeGroupReadRange(&runtime, 0, 101, INT64_MAX, INT64_MAX);

  STimeRangeNode node = {};
  node.type = QUERY_NODE_TIME_RANGE;
  node.needCalc = true;
  node.pStart = makeStreamRangeBound(OP_TYPE_GREATER_THAN, "_twstart");
  node.pEnd = makeStreamRangeBound(OP_TYPE_LOWER_THAN, "_twend");
  ASSERT_NE(nullptr, node.pStart);
  ASSERT_NE(nullptr, node.pEnd);
  SStreamRuntimeFuncInfo temporary = {};
  temporary.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, temporary.pStreamPesudoFuncVals);
  STimeWindow range = {};
  bool        rangeValid = false;

  EXPECT_EQ(TSDB_CODE_OUT_OF_RANGE,
            vnodeCalculateNodeGroupReadRange(&node, &runtime, &temporary, 7, &range, &rangeValid));
  EXPECT_FALSE(rangeValid);

  nodesDestroyNode(node.pStart);
  nodesDestroyNode(node.pEnd);
  tDestroyStRtFuncInfo(&temporary);
  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamGroupReadTest, RejectsCalculatedInclusiveUpperOverflow) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  setNodeGroupReadRange(&runtime, 0, 101, INT64_MIN, INT64_MAX);

  STimeRangeNode node = {};
  node.type = QUERY_NODE_TIME_RANGE;
  node.needCalc = true;
  node.pStart = makeStreamRangeBound(OP_TYPE_GREATER_EQUAL, "_twstart");
  node.pEnd = makeStreamRangeBound(OP_TYPE_LOWER_EQUAL, "_twend");
  ASSERT_NE(nullptr, node.pStart);
  ASSERT_NE(nullptr, node.pEnd);
  SStreamRuntimeFuncInfo temporary = {};
  temporary.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, temporary.pStreamPesudoFuncVals);
  STimeWindow range = {};
  bool        rangeValid = false;

  EXPECT_EQ(TSDB_CODE_OUT_OF_RANGE,
            vnodeCalculateNodeGroupReadRange(&node, &runtime, &temporary, 7, &range, &rangeValid));
  EXPECT_FALSE(rangeValid);

  nodesDestroyNode(node.pStart);
  nodesDestroyNode(node.pEnd);
  tDestroyStRtFuncInfo(&temporary);
  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamGroupReadTest, PreservesCalculatedEmptyReadRange) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  setNodeGroupReadRange(&runtime, 0, 101, 300, 200);

  STimeRangeNode node = {};
  node.type = QUERY_NODE_TIME_RANGE;
  node.needCalc = true;
  node.pStart = makeStreamRangeBound(OP_TYPE_GREATER_EQUAL, "_twstart");
  node.pEnd = makeStreamRangeBound(OP_TYPE_LOWER_THAN, "_twend");
  ASSERT_NE(nullptr, node.pStart);
  ASSERT_NE(nullptr, node.pEnd);
  SStreamRuntimeFuncInfo temporary = {};
  temporary.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, temporary.pStreamPesudoFuncVals);
  STimeWindow range = {};
  bool        rangeValid = false;

  range.skey = 11;
  range.ekey = 22;
  EXPECT_EQ(TSDB_CODE_SUCCESS, vnodeCalculateNodeGroupReadRange(&node, &runtime, &temporary, 7, &range, &rangeValid));
  EXPECT_FALSE(rangeValid);
  EXPECT_EQ(11, range.skey);
  EXPECT_EQ(22, range.ekey);

  nodesDestroyNode(node.pStart);
  nodesDestroyNode(node.pEnd);
  tDestroyStRtFuncInfo(&temporary);
  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamGroupReadTest, SkipsCalculatedEmptySegmentWhenUnioningRanges) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  setNodeGroupReadRange(&runtime, 0, 101, 300, 200);
  setNodeGroupReadRange(&runtime, 1, 202, 100, 180);

  STimeRangeNode node = {};
  node.type = QUERY_NODE_TIME_RANGE;
  node.needCalc = true;
  node.pStart = makeStreamRangeBound(OP_TYPE_GREATER_EQUAL, "_twstart");
  node.pEnd = makeStreamRangeBound(OP_TYPE_LOWER_THAN, "_twend");
  ASSERT_NE(nullptr, node.pStart);
  ASSERT_NE(nullptr, node.pEnd);
  SStreamRuntimeFuncInfo temporary = {};
  temporary.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, temporary.pStreamPesudoFuncVals);
  STimeWindow range = {};
  bool        rangeValid = false;

  EXPECT_EQ(TSDB_CODE_SUCCESS, vnodeCalculateNodeGroupReadRange(&node, &runtime, &temporary, 7, &range, &rangeValid));
  EXPECT_TRUE(rangeValid);
  EXPECT_EQ(100, range.skey);
  EXPECT_EQ(179, range.ekey);

  nodesDestroyNode(node.pStart);
  nodesDestroyNode(node.pEnd);
  tDestroyStRtFuncInfo(&temporary);
  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamGroupReadTest, RejectsRawSlidingHalfOpenUpperAtInt64Min) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.triggerType = STREAM_TRIGGER_SLIDING;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  setNodeGroupReadRange(&runtime, 0, 101, INT64_MIN, INT64_MIN);

  STimeRangeNode node = {};
  node.type = QUERY_NODE_TIME_RANGE;
  node.needCalc = false;
  SStreamRuntimeFuncInfo temporary = {};
  STimeWindow            range = {};
  bool                   rangeValid = false;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA,
            vnodeCalculateNodeGroupReadRange(&node, &runtime, &temporary, 7, &range, &rangeValid));
  EXPECT_FALSE(rangeValid);

  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamGroupReadTest, RejectsInvalidRawRangeAfterValidEntry) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  setNodeGroupReadRange(&runtime, 0, 101, 100, 200);
  setNodeGroupReadRange(&runtime, 1, 202, 300, 250);

  STimeRangeNode node = {};
  node.type = QUERY_NODE_TIME_RANGE;
  node.needCalc = false;
  SStreamRuntimeFuncInfo temporary = {};
  STimeWindow            range = {};
  bool                   rangeValid = false;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA,
            vnodeCalculateNodeGroupReadRange(&node, &runtime, &temporary, 7, &range, &rangeValid));
  EXPECT_FALSE(rangeValid);

  taosArrayDestroy(runtime.curGrpRead);
}

TEST(VnodeStreamVTableHelpers, tagValueEqual_OneNullDiffer) {
  STagValue a{};
  EXPECT_FALSE(tagValueEqual(&a, nullptr));
  EXPECT_FALSE(tagValueEqual(nullptr, &a));
}

TEST(VnodeStreamVTableHelpers, tagValueEqual_DiffType) {
  STagValue a{}; a.type = 1;
  STagValue b{}; b.type = 2;
  EXPECT_FALSE(tagValueEqual(&a, &b));
}

TEST(VnodeStreamVTableHelpers, tagValueEqual_DiffLen) {
  char buf[4] = {0};
  STagValue a{}; a.type = 1; a.nLen = 3; a.pData = buf;
  STagValue b{}; b.type = 1; b.nLen = 4; b.pData = buf;
  EXPECT_FALSE(tagValueEqual(&a, &b));
}

TEST(VnodeStreamVTableHelpers, tagValueEqual_ZeroLenEqual) {
  STagValue a{}; a.type = 5; a.nLen = 0;
  STagValue b{}; b.type = 5; b.nLen = 0;
  EXPECT_TRUE(tagValueEqual(&a, &b));
}

TEST(VnodeStreamVTableHelpers, tagValueEqual_NullDataPointerCompare) {
  STagValue a{}; a.type = 1; a.nLen = 4; a.pData = nullptr;
  STagValue b{}; b.type = 1; b.nLen = 4; b.pData = nullptr;
  EXPECT_TRUE(tagValueEqual(&a, &b));
  char buf[4] = {0};
  b.pData = buf;
  EXPECT_FALSE(tagValueEqual(&a, &b));
}

TEST(VnodeStreamVTableHelpers, tagValueEqual_ContentEqualAndDiffer) {
  char x[4] = {1, 2, 3, 4};
  char y[4] = {1, 2, 3, 4};
  char z[4] = {1, 2, 3, 9};
  STagValue a{}; a.type = 1; a.nLen = 4; a.pData = x;
  STagValue b{}; b.type = 1; b.nLen = 4; b.pData = y;
  STagValue c{}; c.type = 1; c.nLen = 4; c.pData = z;
  EXPECT_TRUE(tagValueEqual(&a, &b));
  EXPECT_FALSE(tagValueEqual(&a, &c));
}

// ---------------------------------------------------------------------------
// streamBuildTblColKey
// ---------------------------------------------------------------------------

TEST(VnodeStreamVTableHelpers, streamBuildTblColKey_FormatAndLen) {
  char    out[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 4];
  int32_t outLen = 0;
  streamBuildTblColKey("mydb", "t1", "voltage", out, &outLen);
  // "mydb\0t1\0voltage"  ->  4 + 1 + 2 + 1 + 7 = 15
  EXPECT_EQ(outLen, 15);
  EXPECT_EQ(0, std::memcmp(out, "mydb", 4));
  EXPECT_EQ(out[4], '\0');
  EXPECT_EQ(0, std::memcmp(out + 5, "t1", 2));
  EXPECT_EQ(out[7], '\0');
  EXPECT_EQ(0, std::memcmp(out + 8, "voltage", 7));
}

TEST(VnodeStreamVTableHelpers, streamBuildTblColKey_DistinctKeysForDistinctInputs) {
  char k1[256]; int32_t l1 = 0;
  char k2[256]; int32_t l2 = 0;
  char k3[256]; int32_t l3 = 0;
  streamBuildTblColKey("d", "t", "c1", k1, &l1);
  streamBuildTblColKey("d", "t", "c2", k2, &l2);
  // db boundary: distinct dbs with concatenation collision must still differ
  streamBuildTblColKey("d", "tx", "1", k3, &l3);
  EXPECT_NE(l1, 0);
  EXPECT_TRUE(l1 != l2 || std::memcmp(k1, k2, l1) != 0);
  EXPECT_TRUE(l1 != l3 || std::memcmp(k1, k3, l1) != 0);
}

TEST(VnodeStreamVTableHelpers, streamBuildRefKey_ExternalSourceIdentityPreventsCollision) {
  SResolveWorkItem sourceA = mkWorkItem("db", "metrics", "value");
  SResolveWorkItem sourceB = mkWorkItem("db", "metrics", "value");
  sourceA.refType = 1;
  sourceB.refType = 1;
  std::snprintf(sourceA.refSourceName, TSDB_EXT_SOURCE_NAME_LEN, "%s", "source_a");
  std::snprintf(sourceB.refSourceName, TSDB_EXT_SOURCE_NAME_LEN, "%s", "source_b");
  std::snprintf(sourceA.refSchemaName, TSDB_EXT_SOURCE_SCHEMA_LEN, "%s", "public");
  std::snprintf(sourceB.refSchemaName, TSDB_EXT_SOURCE_SCHEMA_LEN, "%s", "public");

  char keyA[512] = {0};
  char keyB[512] = {0};
  int32_t lenA = 0;
  int32_t lenB = 0;
  streamBuildRefKey(&sourceA, keyA, &lenA);
  streamBuildRefKey(&sourceB, keyB, &lenB);

  EXPECT_TRUE(lenA != lenB || std::memcmp(keyA, keyB, lenA) != 0);
}

// ---------------------------------------------------------------------------
// streamWriteRspItemDeepCopy
// ---------------------------------------------------------------------------

TEST(VnodeStreamVTableHelpers, streamWriteRspItemDeepCopy_NoTagData) {
  SVTableRefResolveRspItem src{};
  src.code       = 0;
  src.terminated = true;
  src.tagType    = 0;
  src.tagLen     = 0;
  src.tagData    = nullptr;
  SVTableRefResolveRspItem dst{};
  EXPECT_EQ(0, streamWriteRspItemDeepCopy(&src, &dst));
  EXPECT_EQ(dst.code, 0);
  EXPECT_TRUE(dst.terminated);
  EXPECT_EQ(dst.tagLen, 0);
  EXPECT_EQ(dst.tagData, nullptr);
}

TEST(VnodeStreamVTableHelpers, streamWriteRspItemDeepCopy_WithTagData) {
  const char *payload = "abcdef";
  int32_t     n       = (int32_t)std::strlen(payload);
  SVTableRefResolveRspItem src{};
  src.tagType = 7;
  src.tagLen  = n;
  src.tagData = (char *)payload;  // not owned

  SVTableRefResolveRspItem dst{};
  ASSERT_EQ(0, streamWriteRspItemDeepCopy(&src, &dst));
  ASSERT_NE(dst.tagData, nullptr);
  EXPECT_NE(dst.tagData, src.tagData);
  EXPECT_EQ(dst.tagLen, n);
  EXPECT_EQ(0, std::memcmp(dst.tagData, payload, n));
  taosMemoryFree(dst.tagData);
}

TEST(VnodeStreamVTableHelpers, streamWriteRspItemDeepCopy_NullDataNonZeroLenSkipsCopy) {
  SVTableRefResolveRspItem src{};
  src.tagLen  = 8;
  src.tagData = nullptr;
  SVTableRefResolveRspItem dst{};
  EXPECT_EQ(0, streamWriteRspItemDeepCopy(&src, &dst));
  EXPECT_EQ(dst.tagLen, 0);
  EXPECT_EQ(dst.tagData, nullptr);
}

// ---------------------------------------------------------------------------
// streamFanoutSyncCreate / Release / Destroy
// ---------------------------------------------------------------------------

TEST(VnodeStreamVTableHelpers, streamFanoutSyncRelease_NullSafe) {
  EXPECT_FALSE(streamFanoutSyncRelease(nullptr));
  streamFanoutSyncDestroy(nullptr);  // must not crash
}

TEST(VnodeStreamVTableHelpers, streamFanoutSyncCreate_AndDirectDestroy) {
  SStreamFanoutSync *p = streamFanoutSyncCreate();
  ASSERT_NE(p, nullptr);
  streamFanoutSyncDestroy(p);
}

TEST(VnodeStreamVTableHelpers, streamFanoutSyncRelease_RefAboveZeroDoesNotFree) {
  SStreamFanoutSync *p = streamFanoutSyncCreate();
  ASSERT_NE(p, nullptr);
  // Driver bumps refs to 2 (one driver lifetime ref + one fired-cb ref).
  atomic_store_32(&p->refs, 2);
  bool freed = streamFanoutSyncRelease(p);
  EXPECT_FALSE(freed);
  EXPECT_EQ(atomic_load_32(&p->refs), 1);
  // Drain the remaining ref: this time release MUST free.
  freed = streamFanoutSyncRelease(p);
  EXPECT_TRUE(freed);
  // p is freed here; do not touch.
}

// ---------------------------------------------------------------------------
// streamTblRefCacheLookup / Insert
// ---------------------------------------------------------------------------

TEST(VnodeStreamVTableHelpers, streamTblRefCache_NullPathsAreSafe) {
  EXPECT_EQ(nullptr, streamTblRefCacheLookup(nullptr, "d", "t", "c", 0));
  SStreamVTableInfoCache empty{};
  EXPECT_EQ(nullptr, streamTblRefCacheLookup(&empty, "d", "t", "c", 0));
  // Insert into NULL cache and into cache-with-null-hash must be no-op (no crash).
  SVTableRefResolveRspItem item{};
  streamTblRefCacheInsert(nullptr, "d", "t", "c", 0, &item);
  streamTblRefCacheInsert(&empty, "d", "t", "c", 0, &item);
}

TEST(VnodeStreamVTableHelpers, streamTblRefCache_InsertAndLookupNoTag) {
  SStreamVTableInfoCache *pCache = makeEmptyCache();
  ASSERT_NE(pCache, nullptr);

  SVTableRefResolveRspItem item{};
  item.code       = 42;
  item.terminated = true;
  item.tagLen     = 0;
  item.tagData    = nullptr;
  streamTblRefCacheInsert(pCache, "dbX", "tblA", "colY", /*kind*/0, &item);

  auto *got = streamTblRefCacheLookup(pCache, "dbX", "tblA", "colY", /*kind*/0);
  ASSERT_NE(got, nullptr);
  EXPECT_EQ(got->code, 42);
  EXPECT_TRUE(got->terminated);
  EXPECT_EQ(got->tagLen, 0);
  EXPECT_EQ(got->tagData, nullptr);

  // Lookup with different col name must miss.
  EXPECT_EQ(nullptr, streamTblRefCacheLookup(pCache, "dbX", "tblA", "OTHER", 0));
  freeCache(pCache);
}

TEST(VnodeStreamVTableHelpers, streamTblRefCache_InsertDeepCopiesTagData) {
  SStreamVTableInfoCache *pCache = makeEmptyCache();
  ASSERT_NE(pCache, nullptr);

  char       tag[5] = {9, 8, 7, 6, 5};
  SVTableRefResolveRspItem item{};
  item.tagType = 3;
  item.tagLen  = sizeof(tag);
  item.tagData = tag;
  streamTblRefCacheInsert(pCache, "db", "t", "c", /*kind*/1, &item);

  // Mutate the original buffer; the cached copy must NOT change.
  std::memset(tag, 0, sizeof(tag));

  auto *got = streamTblRefCacheLookup(pCache, "db", "t", "c", 1);
  ASSERT_NE(got, nullptr);
  ASSERT_NE(got->tagData, nullptr);
  EXPECT_NE(got->tagData, item.tagData);
  EXPECT_EQ(got->tagLen, (int32_t)sizeof(tag));
  EXPECT_EQ((unsigned char)got->tagData[0], 9);
  EXPECT_EQ((unsigned char)got->tagData[4], 5);
  freeCache(pCache);
}

TEST(VnodeStreamVTableHelpers, streamTblRefCache_TagAndColShareKey_FirstWriterWins) {
  // Design invariant: within one physical table, a tag and a column cannot
  // share a name, so the cache key intentionally omits `kind`. The underlying
  // taosHashPut does NOT overwrite on duplicate key, so the first writer wins
  // and the second insert is dropped with a stWarn. Pin that behavior here so
  // a future refactor cannot silently regress it.
  SStreamVTableInfoCache *pCache = makeEmptyCache();
  ASSERT_NE(pCache, nullptr);

  SVTableRefResolveRspItem a{}; a.code = 1;
  SVTableRefResolveRspItem b{}; b.code = 2;
  streamTblRefCacheInsert(pCache, "db", "t", "c", /*kind=COL*/0, &a);
  // Second insert with same (db,t,c) hits the dup-key branch in
  // streamTblRefCacheInsert (taosHashPut returns DUP_KEY).
  streamTblRefCacheInsert(pCache, "db", "t", "c", /*kind=TAG*/1, &b);

  // Both lookups resolve to the same slot, carrying the first writer's code.
  auto *byCol = streamTblRefCacheLookup(pCache, "db", "t", "c", 0);
  auto *byTag = streamTblRefCacheLookup(pCache, "db", "t", "c", 1);
  ASSERT_NE(byCol, nullptr);
  EXPECT_EQ(byCol, byTag);
  EXPECT_EQ(byCol->code, 1);
  freeCache(pCache);
}

// ---------------------------------------------------------------------------
// streamBatchTryCacheAndDedup
// ---------------------------------------------------------------------------

TEST(VnodeStreamVTableHelpers, streamBatchTryCacheAndDedup_CacheHitsAndDedup) {
  SStreamVTableInfoCache *pCache = makeEmptyCache();
  ASSERT_NE(pCache, nullptr);

  // Pre-populate cache with one entry for (db1, tA, c1).
  SVTableRefResolveRspItem cached{};
  cached.code = 7;
  streamTblRefCacheInsert(pCache, "db1", "tA", "c1", 0, &cached);

  // Build a batch of 4 items:
  //   0: (db1,tA,c1)  -- cache hit
  //   1: (db1,tA,c2)  -- new, dedup slot 0
  //   2: (db1,tA,c2)  -- duplicate of #1, must NOT add a new dedup slot
  //   3: (db2,tA,c1)  -- different db, dedup slot 1 (cache miss b/c db differs)
  SArray *batch = taosArrayInit(4, sizeof(SResolveWorkItem));
  ASSERT_NE(batch, nullptr);
  SResolveWorkItem w0 = mkWorkItem("db1", "tA", "c1");
  SResolveWorkItem w1 = mkWorkItem("db1", "tA", "c2");
  SResolveWorkItem w2 = mkWorkItem("db1", "tA", "c2");
  SResolveWorkItem w3 = mkWorkItem("db2", "tA", "c1");
  taosArrayPush(batch, &w0);
  taosArrayPush(batch, &w1);
  taosArrayPush(batch, &w2);
  taosArrayPush(batch, &w3);

  SArray *outRspItems = taosArrayInit(4, sizeof(SVTableRefResolveRspItem));
  ASSERT_NE(outRspItems, nullptr);
  for (int i = 0; i < 4; ++i) {
    SVTableRefResolveRspItem zero{};
    taosArrayPush(outRspItems, &zero);
  }
  SHashObj *dedupMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                    false, HASH_NO_LOCK);
  SArray   *dedupItems = taosArrayInit(4, sizeof(SResolveWorkItem));
  int32_t   origToDedupIdx[4] = {0, 0, 0, 0};
  int32_t   cacheHits = 0;

  int32_t rc = streamBatchTryCacheAndDedup(pCache, batch, outRspItems, dedupMap,
                                           dedupItems, origToDedupIdx, &cacheHits);
  EXPECT_EQ(rc, 0);
  EXPECT_EQ(cacheHits, 1);
  EXPECT_EQ(origToDedupIdx[0], -1);  // served from cache
  EXPECT_EQ(origToDedupIdx[1], 0);   // first occurrence -> slot 0
  EXPECT_EQ(origToDedupIdx[2], 0);   // dedup'd to same slot 0
  EXPECT_EQ(origToDedupIdx[3], 1);   // distinct -> slot 1
  EXPECT_EQ(2, (int)taosArrayGetSize(dedupItems));

  // The cache-hit slot must carry the cached code; clean any tagData first.
  auto *out0 = (SVTableRefResolveRspItem *)taosArrayGet(outRspItems, 0);
  EXPECT_EQ(out0->code, 7);

  taosArrayDestroy(batch);
  taosArrayDestroy(outRspItems);
  taosArrayDestroy(dedupItems);
  taosHashCleanup(dedupMap);
  freeCache(pCache);
}

TEST(VnodeStreamVTableHelpers, streamBatchTryCacheAndDedup_PrivateCacheDedupsAcrossUidChunks) {
  // Regression for the tag-only resolve (pCache == NULL) that the resolver
  // slices into uid batches: the call-private ref cache is the only structure
  // that spans batches, so it must exist even without a shared reader cache.
  // Without it the 2nd chunk re-resolves the remote ref the 1st chunk already
  // resolved (N chunks -> N RPCs instead of 1).
  SStreamVTableInfoCache view{};
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamResolveCacheInit(&view, nullptr, /*createRefCache=*/true));
  ASSERT_NE(view.tblRefCache, nullptr);

  // Run one uid chunk exactly like streamCallResolveBatched's phase 1 does.
  auto runChunk = [&](int32_t chunkSize, int32_t *cacheHitsOut, int32_t *dedupN, SArray **outRsp) {
    SArray *batch = taosArrayInit(chunkSize, sizeof(SResolveWorkItem));
    ASSERT_NE(batch, nullptr);
    for (int32_t i = 0; i < chunkSize; ++i) {
      // Every child table of the chunk references the same remote tag.
      SResolveWorkItem w = mkWorkItem("db1", "remoteTb", "remoteTag", /*kind=*/1);
      w.originVtbUid = 1000 + i;
      taosArrayPush(batch, &w);
    }
    *outRsp = taosArrayInit(chunkSize, sizeof(SVTableRefResolveRspItem));
    ASSERT_NE(*outRsp, nullptr);
    for (int32_t i = 0; i < chunkSize; ++i) {
      SVTableRefResolveRspItem zero{};
      taosArrayPush(*outRsp, &zero);
    }
    SHashObj *dedupMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false,
                                      HASH_NO_LOCK);
    ASSERT_NE(dedupMap, nullptr);
    SArray *dedupItems = taosArrayInit(chunkSize, sizeof(SResolveWorkItem));
    ASSERT_NE(dedupItems, nullptr);
    std::vector<int32_t> orig(chunkSize, 0);

    int32_t rc = streamBatchTryCacheAndDedup(&view, batch, *outRsp, dedupMap, dedupItems,
                                            orig.data(), cacheHitsOut);
    EXPECT_EQ(rc, 0);
    *dedupN = (int32_t)taosArrayGetSize(dedupItems);

    taosArrayDestroy(batch);
    taosArrayDestroy(dedupItems);
    taosHashCleanup(dedupMap);
  };

  int32_t cacheHits = 0, dedupN = 0;
  SArray *outRsp1 = nullptr;
  runChunk(STREAM_VTB_RESOLVE_UID_CHUNK, &cacheHits, &dedupN, &outRsp1);
  EXPECT_EQ(cacheHits, 0);
  EXPECT_EQ(dedupN, 1);  // one remote ref for the whole chunk

  // Publish the resolved item, as streamBatchScatterAndPublish does.
  SVTableRefResolveRspItem resolved{};
  resolved.code = 9;
  streamTblRefCacheInsert(&view, "db1", "remoteTb", "remoteTag", /*kind=*/1, &resolved);

  // The next chunk is served entirely from the call-private cache: no dedup
  // items, hence no remote resolve RPC for it.
  SArray *outRsp2 = nullptr;
  runChunk(STREAM_VTB_RESOLVE_UID_CHUNK, &cacheHits, &dedupN, &outRsp2);
  EXPECT_EQ(cacheHits, STREAM_VTB_RESOLVE_UID_CHUNK);
  EXPECT_EQ(dedupN, 0);
  auto *first = (SVTableRefResolveRspItem *)taosArrayGet(outRsp2, 0);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->code, 9);

  taosArrayDestroy(outRsp1);
  taosArrayDestroy(outRsp2);
  streamResolveCacheDestroy(&view);
}

TEST(VnodeStreamVTableHelpers, streamBatchTryCacheAndDedup_PrivateCacheDedupsAcrossHopsForSingleUid) {
  // A single virtual table can reach the same remote column twice: `c1 -> T.x`
  // directly and `c2 -> V1.y -> T.x`. Hop 0 resolves both refs (one RPC to the
  // shared vnode); because T.x is published to the call-private cache, hop 1's
  // T.x is served from it instead of being resolved a second time. Gating the
  // ref cache on the uid count broke exactly this case.
  SStreamVTableInfoCache view{};
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamResolveCacheInit(&view, nullptr, /*createRefCache=*/true));
  ASSERT_NE(view.tblRefCache, nullptr);

  // Run one hop exactly like streamCallResolveBatched's phase 1 does. first* reports
  // the response written into slot 0 (only meaningful when the hop has items).
  auto runHop = [&](const std::vector<SResolveWorkItem> &items, int32_t *cacheHitsOut, int32_t *dedupN,
                    int32_t *firstCode, bool *firstTerminated) {
    SArray *batch = taosArrayInit((int32_t)items.size(), sizeof(SResolveWorkItem));
    ASSERT_NE(batch, nullptr);
    for (const auto &w : items) taosArrayPush(batch, &w);
    SArray *outRsp = taosArrayInit((int32_t)items.size(), sizeof(SVTableRefResolveRspItem));
    ASSERT_NE(outRsp, nullptr);
    for (size_t i = 0; i < items.size(); ++i) {
      SVTableRefResolveRspItem zero{};
      taosArrayPush(outRsp, &zero);
    }
    SHashObj *dedupMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false,
                                      HASH_NO_LOCK);
    ASSERT_NE(dedupMap, nullptr);
    SArray *dedupItems = taosArrayInit((int32_t)items.size(), sizeof(SResolveWorkItem));
    ASSERT_NE(dedupItems, nullptr);
    std::vector<int32_t> orig(items.size(), 0);

    int32_t rc = streamBatchTryCacheAndDedup(&view, batch, outRsp, dedupMap, dedupItems,
                                            orig.data(), cacheHitsOut);
    EXPECT_EQ(rc, 0);
    *dedupN = (int32_t)taosArrayGetSize(dedupItems);

    auto *slot0 = (SVTableRefResolveRspItem *)taosArrayGet(outRsp, 0);
    *firstCode       = slot0->code;
    *firstTerminated = slot0->terminated;

    taosArrayDestroy(batch);
    taosArrayDestroy(outRsp);
    taosArrayDestroy(dedupItems);
    taosHashCleanup(dedupMap);
  };

  // Hop 0: the direct tag/col `T.x` and the indirect `V1.y`.
  std::vector<SResolveWorkItem> hop0 = {mkWorkItem("db1", "T", "x"),
                                        mkWorkItem("db1", "V1", "y")};
  int32_t cacheHits = 0, dedupN = 0, slotCode = 0;
  bool    slotTerminated = false;
  runHop(hop0, &cacheHits, &dedupN, &slotCode, &slotTerminated);
  EXPECT_EQ(cacheHits, 0);
  EXPECT_EQ(dedupN, 2);  // two distinct refs, one fan-out RPC to the shared vnode

  // Publish hop 0 results, as streamBatchScatterAndPublish does: `T.x` resolved,
  // and `V1.y` points at `T.x` for the next hop.
  SVTableRefResolveRspItem tx{};
  tx.code       = 3;
  tx.terminated = true;
  SVTableRefResolveRspItem vy{};
  vy.nextRef.hasRef = true;
  vy.nextRef.kind   = 0;
  std::snprintf(vy.nextRef.refDbName, sizeof(vy.nextRef.refDbName), "%s", "db1");
  std::snprintf(vy.nextRef.refTableName, sizeof(vy.nextRef.refTableName), "%s", "T");
  std::snprintf(vy.nextRef.refColName, sizeof(vy.nextRef.refColName), "%s", "x");
  streamTblRefCacheInsert(&view, "db1", "T", "x", /*kind*/0, &tx);
  streamTblRefCacheInsert(&view, "db1", "V1", "y", /*kind*/0, &vy);

  // Hop 1: `V1.y` resolved to `T.x`, which hop 0 already resolved.
  std::vector<SResolveWorkItem> hop1 = {mkWorkItem("db1", "T", "x")};
  runHop(hop1, &cacheHits, &dedupN, &slotCode, &slotTerminated);
  EXPECT_EQ(cacheHits, 1);
  EXPECT_EQ(dedupN, 0);  // no second resolve RPC for the converged ref
  // The cached entry itself must come back, not just any hit.
  EXPECT_EQ(slotCode, 3);
  EXPECT_TRUE(slotTerminated);

  streamResolveCacheDestroy(&view);
}

// A virtual table that maps no source column at all has nothing to be configured by the set table
// request, so a vtable stream reader treats itself as ready (see vnodeProcessStreamVTableInfoReq).
TEST(VnodeStreamVTableHelpers, streamVTableInfoHasColumnRefOnlyForMappedSources) {
  EXPECT_FALSE(streamVTableInfoHasColumnRef(nullptr));
  SStreamMsgVTableInfo info = {};
  EXPECT_FALSE(streamVTableInfoHasColumnRef(&info));

  info.infos = taosArrayInit(1, sizeof(VTableInfo));
  ASSERT_NE(nullptr, info.infos);
  auto *pInfo = (VTableInfo *)taosArrayReserve(info.infos, 1);
  ASSERT_NE(nullptr, pInfo);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tInitDefaultSColRefWrapperByCols(&pInfo->cols, 2));
  EXPECT_FALSE(streamVTableInfoHasColumnRef(&info));

  pInfo->cols.pColRef[1].hasRef = true;
  EXPECT_TRUE(streamVTableInfoHasColumnRef(&info));
  pInfo->cols.pColRef[1].hasRef = false;

  ASSERT_EQ(TSDB_CODE_SUCCESS, tInitDefaultSColRefWrapperByTags(&pInfo->cols, 1, 100));
  EXPECT_FALSE(streamVTableInfoHasColumnRef(&info));
  pInfo->cols.pTagRef[0].hasRef = true;
  EXPECT_TRUE(streamVTableInfoHasColumnRef(&info));

  tDestroySStreamMsgVTableInfo(&info);
}

namespace {

SStreamTriggerReaderInfo *gPullReaderInfo = nullptr;
int32_t gPullResponseCode = TSDB_CODE_SUCCESS;
int32_t gPullResponseCount = 0;

bool readyForStreamRead(int64_t) { return true; }

void *getPullReaderInfo(int64_t, int64_t, void **taskAddr) {
  *taskAddr = nullptr;
  return gPullReaderInfo;
}

void capturePullResponse(SRpcMsg *response) {
  gPullResponseCode = response->code;
  ++gPullResponseCount;
  rpcFreeCont(response->pCont);
}

int32_t failBuildVTableList(SStreamTriggerReaderInfo *) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

class VnodeStreamSetTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    task_.task.streamId = 101;
    task_.task.taskId = 202;
    reader_.pTask = &task_;
    reader_.isVtableStream = true;
    vnode_.config.vgId = 7;
    gPullReaderInfo = &reader_;
    ASSERT_EQ(TSDB_CODE_SUCCESS, initStreamTableListInfo(&reader_.tableList, UIDMAP_SINGLE));
    stub_.set(syncIsReadyForRead, readyForStreamRead);
    stub_.set(qStreamGetReaderInfo, getPullReaderInfo);
    stub_.set(tmsgSendRsp, capturePullResponse);
  }

  void TearDown() override {
    qStreamDestroyTableInfo(&reader_.tableList);
    qStreamDestroyTableInfo(&reader_.vSetTableList);
    tSimpleHashCleanup(reader_.uidHashTrigger);
    tSimpleHashCleanup(reader_.uidHashCalc);
    gPullReaderInfo = nullptr;
  }

  int32_t pull(int32_t type) {
    SSTriggerPullRequestUnion request = {};
    request.base.type = static_cast<decltype(request.base.type)>(type);
    request.base.streamId = task_.task.streamId;
    request.base.readerTaskId = task_.task.taskId;
    int32_t length = tSerializeSTriggerPullRequest(nullptr, 0, &request.base);
    EXPECT_GT(length, 0);
    if (length <= 0) return TSDB_CODE_INVALID_MSG;
    std::vector<char> content(sizeof(SMsgHead) + length);
    EXPECT_EQ(length, tSerializeSTriggerPullRequest(content.data() + sizeof(SMsgHead), length, &request.base));
    SRpcMsg message = {};
    message.msgType = TDMT_STREAM_TRIGGER_PULL;
    message.pCont = content.data();
    message.contLen = static_cast<int32_t>(content.size());
    gPullResponseCode = TSDB_CODE_SUCCESS;
    gPullResponseCount = 0;
    int32_t code = vnodeProcessStreamReaderMsg(&vnode_, &message, nullptr);
    EXPECT_EQ(1, gPullResponseCount);
    EXPECT_EQ(code, gPullResponseCode);
    return code;
  }

  Stub stub_;
  SVnode vnode_ = {};
  SStreamReaderTask task_ = {};
  SStreamTriggerReaderInfo reader_ = {};
};

TEST_F(VnodeStreamSetTableTest, RejectsLastTimestampBeforeSetTable) {
  EXPECT_EQ(TSDB_CODE_STREAM_TASK_NOT_EXIST, pull(STRIGGER_PULL_LAST_TS));
}

TEST_F(VnodeStreamSetTableTest, RejectsMetadataContinuationBeforeSetTable) {
  EXPECT_EQ(TSDB_CODE_STREAM_TASK_NOT_EXIST, pull(STRIGGER_PULL_TSDB_META_NEXT));
}

TEST_F(VnodeStreamSetTableTest, RejectsDataRequestsBeforeSetTable) {
  const int32_t requestTypes[] = {
      STRIGGER_PULL_FIRST_TS, STRIGGER_PULL_TSDB_META, STRIGGER_PULL_TSDB_TS_DATA,
      STRIGGER_PULL_TSDB_TRIGGER_DATA, STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT,
      STRIGGER_PULL_TSDB_CALC_DATA, STRIGGER_PULL_TSDB_CALC_DATA_NEXT,
      STRIGGER_PULL_TSDB_DATA, STRIGGER_PULL_TSDB_DATA_NEXT,
      STRIGGER_PULL_WAL_META_NEW, STRIGGER_PULL_WAL_DATA_NEW,
      STRIGGER_PULL_WAL_META_DATA_NEW, STRIGGER_PULL_WAL_CALC_DATA_NEW};
  for (int32_t type : requestTypes) {
    SCOPED_TRACE(type);
    EXPECT_EQ(TSDB_CODE_STREAM_TASK_NOT_EXIST, pull(type));
  }
}

TEST_F(VnodeStreamSetTableTest, AllowsLastTimestampAfterEmptySetTable) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, pull(STRIGGER_PULL_SET_TABLE));
  EXPECT_EQ(TSDB_CODE_SUCCESS, pull(STRIGGER_PULL_LAST_TS));
}

TEST_F(VnodeStreamSetTableTest, FailedSetTableInvalidatesPreviousState) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, pull(STRIGGER_PULL_SET_TABLE));
  stub_.set(qBuildVTableList, failBuildVTableList);
  ASSERT_EQ(TSDB_CODE_OUT_OF_MEMORY, pull(STRIGGER_PULL_SET_TABLE));
  EXPECT_EQ(TSDB_CODE_STREAM_TASK_NOT_EXIST, pull(STRIGGER_PULL_LAST_TS));
  stub_.reset(qBuildVTableList);
  ASSERT_EQ(TSDB_CODE_SUCCESS, pull(STRIGGER_PULL_SET_TABLE));
  EXPECT_EQ(TSDB_CODE_SUCCESS, pull(STRIGGER_PULL_LAST_TS));
}

TEST_F(VnodeStreamSetTableTest, NormalTableDoesNotRequireSetTable) {
  reader_.isVtableStream = false;
  EXPECT_EQ(TSDB_CODE_SUCCESS, pull(STRIGGER_PULL_LAST_TS));
}

TEST_F(VnodeStreamSetTableTest, GroupColumnsDoNotRequireSetTable) {
  EXPECT_EQ(TSDB_CODE_STREAM_NO_CONTEXT, pull(STRIGGER_PULL_GROUP_COL_VALUE));
}

// Exercise serialized Reader requests against real WAL segments, including
// retention between the index and data passes.
class VnodeStreamWalRecoveryTest : public VnodeStreamSetTableTest {
 protected:
  static void SetUpTestCase() { ASSERT_EQ(0, walInit(nullptr)); }
  static void TearDownTestCase() { walCleanUp(); }

  void SetUp() override {
    VnodeStreamSetTableTest::SetUp();
    active_ = this;
    reader_.vTableReady = true;
    path_ = std::string(TD_TMP_DIR_PATH) + "stream-wal-recovery-" + std::to_string(taosGetTimestampUs());
    SWalCfg cfg = {};
    cfg.vgId = 7;
    cfg.committed = -1;
    cfg.rollPeriod = -1;
    cfg.segSize = 1;
    cfg.retentionPeriod = 0;
    cfg.retentionSize = 0;
    cfg.level = TAOS_WAL_FSYNC;
    vnode_.pWal = walOpen(path_.c_str(), &cfg);
    ASSERT_NE(nullptr, vnode_.pWal);
    // walOpen converts KB to bytes; force one record per segment without sleeps.
    vnode_.pWal->cfg.segSize = 1;

    reader_.uidHashTrigger = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    reader_.uidHashCalc = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    columns_ = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
    int16_t slot = 1;
    int16_t colId = 2;
    ASSERT_EQ(0, tSimpleHashPut(columns_, &slot, sizeof(slot), &colId, sizeof(colId)));
    reader_.triggerTableSchemaMapVTable =
        taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    SSchema schema[2] = {};
    schema[0].colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    schema[0].type = TSDB_DATA_TYPE_TIMESTAMP;
    schema[0].bytes = sizeof(int64_t);
    schema[1].colId = 2;
    schema[1].type = TSDB_DATA_TYPE_BIGINT;
    schema[1].bytes = sizeof(int64_t);
    schema_ = tBuildTSchema(schema, 2, 1);
    ASSERT_NE(nullptr, schema_);
    for (int64_t uid : {101LL, 102LL}) {
      int64_t key[2] = {0, uid};
      ASSERT_EQ(0, tSimpleHashPut(reader_.uidHashTrigger, key, sizeof(key), &columns_, sizeof(columns_)));
      ASSERT_EQ(0, tSimpleHashPut(reader_.uidHashCalc, key, sizeof(key), &columns_, sizeof(columns_)));
      ASSERT_EQ(0, taosHashPut(reader_.triggerTableSchemaMapVTable, &uid, sizeof(uid), &schema_, sizeof(schema_)));
    }
    ASSERT_EQ(0, createDataBlock(&reader_.triggerBlock));
    for (int i = 0; i < 3; ++i) {
      SColumnInfoData column = {};
      column.info.colId = i == 2 ? -1 : schema[i].colId;
      column.info.type = i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_BIGINT;
      column.info.bytes = sizeof(int64_t);
      ASSERT_EQ(0, blockDataAppendColInfo(reader_.triggerBlock, &column));
    }
    reader_.calcBlock = reader_.triggerBlock;
    stub_.set(tmsgSendRsp, captureResponse);
  }

  void TearDown() override {
    stub_.reset(walFetchHead);
    blockDataDestroy(reader_.triggerBlock);
    taosHashCleanup(reader_.triggerTableSchemaMapVTable);
    taosMemoryFree(schema_);
    tSimpleHashCleanup(columns_);
    walClose(vnode_.pWal);
    taosRemoveDir(path_.c_str());
    active_ = nullptr;
    VnodeStreamSetTableTest::TearDown();
  }

  void append(int64_t ver, int64_t uid = 101) {
    SSubmitReq2 submit = {};
    submit.aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData));
    SSubmitTbData table = {};
    table.uid = uid;
    table.sver = 1;
    table.flags = SUBMIT_REQ_COLUMN_DATA_FORMAT;
    table.aCol = taosArrayInit(2, sizeof(SColData));
    for (int16_t cid = 1; cid <= 2; ++cid) {
      SColData column = {};
      tColDataInit(&column, cid, cid == 1 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_BIGINT, 0);
      SColVal value = {};
      value.cid = cid;
      value.flag = CV_FLAG_VALUE;
      value.value.type = column.type;
      value.value.val = (cid == 1 ? 1000 : 2000) + ver;
      ASSERT_EQ(0, tColDataAppendValue(&column, &value));
      ASSERT_NE(nullptr, taosArrayPush(table.aCol, &column));
    }
    ASSERT_NE(nullptr, taosArrayPush(submit.aSubmitTbData, &table));
    int32_t length = 0;
    int32_t code = 0;
    tEncodeSize(tEncodeSubmitReq, &submit, length, code);
    ASSERT_EQ(0, code);
    std::vector<char> body(sizeof(SSubmitReq2Msg) + length);
    SEncoder          encoder = {};
    tEncoderInit(&encoder, reinterpret_cast<uint8_t *>(body.data()) + sizeof(SSubmitReq2Msg), length);
    ASSERT_EQ(0, tEncodeSubmitReq(&encoder, &submit));
    tEncoderClear(&encoder);
    tDestroySubmitReq(&submit, TSDB_MSG_FLG_ENCODE);
    SWalSyncInfo sync = {};
    ASSERT_EQ(0, walAppendLog(vnode_.pWal, ver, TDMT_VND_SUBMIT, sync, body.data(), body.size(), 0, nullptr));
    ASSERT_EQ(0, walCommit(vnode_.pWal, ver));
    walApplyVer(vnode_.pWal, ver);
  }

  void recycleBefore(int64_t ver) {
    ASSERT_EQ(0, walBeginSnapshot(vnode_.pWal, ver - 1, 0));
    ASSERT_EQ(0, walEndSnapshot(vnode_.pWal, false));
    ASSERT_EQ(ver, walGetFirstVer(vnode_.pWal));
  }

  void request(int32_t type, std::initializer_list<int64_t> versions, int32_t expectedCode,
               std::initializer_list<int64_t> expectedVersions) {
    SSTriggerPullRequestUnion req = {};
    req.base.type = static_cast<decltype(req.base.type)>(type);
    req.base.streamId = task_.task.streamId;
    req.base.readerTaskId = task_.task.taskId;
    req.walDataNewReq.versions = taosArrayInit(versions.size(), sizeof(int64_t));
    for (int64_t ver : versions) ASSERT_NE(nullptr, taosArrayPush(req.walDataNewReq.versions, &ver));
    req.walDataNewReq.ranges = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    const int64_t range[] = {INT64_MIN, INT64_MAX};
    for (int64_t uid : {101LL, 102LL}) {
      ASSERT_EQ(0, tSimpleHashPut(req.walDataNewReq.ranges, &uid, sizeof(uid), range, sizeof(range)));
    }
    int32_t length = tSerializeSTriggerPullRequest(nullptr, 0, &req.base);
    ASSERT_GT(length, 0);
    std::vector<char> content(sizeof(SMsgHead) + length);
    ASSERT_EQ(length, tSerializeSTriggerPullRequest(content.data() + sizeof(SMsgHead), length, &req.base));
    taosArrayDestroy(req.walDataNewReq.versions);
    tSimpleHashCleanup(req.walDataNewReq.ranges);
    SRpcMsg msg = {};
    msg.msgType = TDMT_STREAM_TRIGGER_PULL;
    msg.pCont = content.data();
    msg.contLen = content.size();
    response_.clear();
    gPullResponseCount = 0;
    const int32_t code = vnodeProcessStreamReaderMsg(&vnode_, &msg, nullptr);
    ASSERT_EQ(expectedCode == TSDB_CODE_STREAM_NO_DATA ? 0 : expectedCode, code);
    ASSERT_EQ(1, gPullResponseCount);
    ASSERT_EQ(expectedCode, gPullResponseCode);
    if (expectedCode == TSDB_CODE_STREAM_NO_DATA) {
      ASSERT_EQ(sizeof(int64_t), response_.size());
      int64_t lastVer = 0;
      memcpy(&lastVer, response_.data(), sizeof(lastVer));
      EXPECT_EQ(*(versions.end() - 1), lastVer);
    } else if (expectedCode == 0) {
      SSTriggerWalNewRsp rsp = {};
      SSDataBlock       *block = nullptr;
      ASSERT_EQ(0, createOneDataBlock(reader_.triggerBlock, false, &block));
      rsp.dataBlock = block;
      SArray *slices = taosArrayInit(2, 3 * sizeof(int64_t));
      EXPECT_EQ(0, tDeserializeSStreamWalDataResponse(response_.data(), response_.size(), &rsp, slices));
      EXPECT_EQ(expectedVersions.size(), block->info.rows);
      int32_t totalSliceRows = 0;
      for (size_t i = 0; i < taosArrayGetSize(slices); ++i) {
        auto   *slice = static_cast<int64_t *>(taosArrayGet(slices, i));
        int32_t start = slice[2] >> 32;
        int32_t end = static_cast<int32_t>(slice[2]);
        EXPECT_EQ(totalSliceRows, start);
        totalSliceRows += end - start;
      }
      EXPECT_EQ(block->info.rows, totalSliceRows);
      std::vector<int64_t> actual;
      auto                *col = static_cast<SColumnInfoData *>(taosArrayGetLast(block->pDataBlock));
      auto                *ts = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 0));
      auto                *value = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 1));
      for (int32_t i = 0; i < block->info.rows; ++i) {
        const int64_t ver = *reinterpret_cast<int64_t *>(colDataGetData(col, i));
        actual.push_back(ver);
        EXPECT_EQ(1000 + ver, *reinterpret_cast<int64_t *>(colDataGetData(ts, i)));
        EXPECT_EQ(2000 + ver, *reinterpret_cast<int64_t *>(colDataGetData(value, i)));
      }
      std::sort(actual.begin(), actual.end());
      EXPECT_EQ(std::vector<int64_t>(expectedVersions), actual);
      blockDataDestroy(block);
      taosArrayDestroy(slices);
    }
  }

  static void captureResponse(SRpcMsg *response) {
    gPullResponseCode = response->code;
    ++gPullResponseCount;
    if (response->contLen > 0) {
      auto *data = static_cast<char *>(response->pCont);
      active_->response_.assign(data, data + response->contLen);
    }
    rpcFreeCont(response->pCont);
  }

  static int32_t fetchWithRetention(SWalReader *reader, int64_t ver) {
    auto *self = active_;
    self->stub_.reset(walFetchHead);
    const bool recycle = ++self->headCalls_ == self->recycleAt_;
    if (recycle) self->recycleBefore(2);
    int32_t code = recycle && self->headError_ != 0 ? self->headError_ : walFetchHead(reader, ver);
    self->stub_.set(walFetchHead, fetchWithRetention);
    return code;
  }

  static int32_t failedHead(SWalReader *, int64_t) { return active_->headError_; }

  static VnodeStreamWalRecoveryTest *active_;
  std::string                        path_;
  std::vector<char>                  response_;
  SSHashObj                         *columns_ = nullptr;
  STSchema                          *schema_ = nullptr;
  int32_t                            headCalls_ = 0;
  int32_t                            recycleAt_ = 0;
  int32_t                            headError_ = 0;
};

VnodeStreamWalRecoveryTest *VnodeStreamWalRecoveryTest::active_ = nullptr;

TEST_F(VnodeStreamWalRecoveryTest, RetainedVersionsRemainReadable) {
  append(0);
  append(1, 102);
  request(STRIGGER_PULL_WAL_DATA_NEW, {0, 1}, 0, {0, 1});
  request(STRIGGER_PULL_WAL_CALC_DATA_NEW, {1, 0}, 0, {0, 1});
}

TEST_F(VnodeStreamWalRecoveryTest, ExpiredVersionsDoNotFailTriggerOrCalcReader) {
  append(0);
  append(1, 102);
  append(2);
  recycleBefore(2);
  request(STRIGGER_PULL_WAL_DATA_NEW, {0, 1, 2}, 0, {2});
  request(STRIGGER_PULL_WAL_CALC_DATA_NEW, {2, 0, 1}, 0, {2});
}

TEST_F(VnodeStreamWalRecoveryTest, EmptyRecoveryCanReadSubsequentWrites) {
  append(0);
  append(1);
  recycleBefore(2);
  request(STRIGGER_PULL_WAL_DATA_NEW, {0, 1}, TSDB_CODE_STREAM_NO_DATA, {});
  request(STRIGGER_PULL_WAL_CALC_DATA_NEW, {0, 1}, TSDB_CODE_STREAM_NO_DATA, {});
  append(2);
  request(STRIGGER_PULL_WAL_DATA_NEW, {2}, 0, {2});
}

TEST_F(VnodeStreamWalRecoveryTest, RetentionDuringIndexPassRebuildsResult) {
  append(0, 102);
  append(1);
  append(2);
  recycleAt_ = 2;
  stub_.set(walFetchHead, fetchWithRetention);
  request(STRIGGER_PULL_WAL_DATA_NEW, {0, 1, 2}, 0, {2});
}

TEST_F(VnodeStreamWalRecoveryTest, RetentionDuringDataPassDiscardsPartialRowsAndSlices) {
  append(0, 102);
  append(1);
  append(2);
  recycleAt_ = 5;
  stub_.set(walFetchHead, fetchWithRetention);
  request(STRIGGER_PULL_WAL_DATA_NEW, {0, 1, 2}, 0, {2});
}

TEST_F(VnodeStreamWalRecoveryTest, FileRemovalDuringOpenRebuildsResult) {
  append(0, 102);
  append(1);
  append(2);
  recycleAt_ = 5;
  headError_ = TAOS_SYSTEM_ERROR(ENOENT);
  stub_.set(walFetchHead, fetchWithRetention);
  request(STRIGGER_PULL_WAL_CALC_DATA_NEW, {0, 1, 2}, 0, {2});
}

TEST_F(VnodeStreamWalRecoveryTest, ErrorsInsideRetainedRangeAreStillReported) {
  append(0);
  stub_.set(walFetchHead, failedHead);
  const int32_t errors[] = {TSDB_CODE_WAL_FILE_CORRUPTED, TSDB_CODE_WAL_INVALID_VER, TSDB_CODE_WAL_LOG_NOT_EXIST,
                            static_cast<int32_t>(TAOS_SYSTEM_ERROR(ENOENT))};
  for (int32_t error : errors) {
    SCOPED_TRACE(error);
    headError_ = error;
    request(STRIGGER_PULL_WAL_DATA_NEW, {0}, error, {});
  }
}

TEST_F(VnodeStreamWalRecoveryTest, FutureVersionIsNotTreatedAsRetention) {
  append(0);
  request(STRIGGER_PULL_WAL_DATA_NEW, {1}, TSDB_CODE_FAILED, {});
}

TEST_F(VnodeStreamWalRecoveryTest, RetentionDoesNotHideFailureOfRetainedVersion) {
  append(0);
  append(1);
  append(2);
  recycleAt_ = 2;
  headError_ = TAOS_SYSTEM_ERROR(ENOENT);
  stub_.set(walFetchHead, fetchWithRetention);
  request(STRIGGER_PULL_WAL_DATA_NEW, {0, 2}, static_cast<int32_t>(TAOS_SYSTEM_ERROR(ENOENT)), {});
}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(VnodeStreamVTableHelpers, SeriesConditionsOwnStorageAndSeparateCacheEntries) {
  auto* cache = makeEmptyCache();
  ASSERT_NE(cache, nullptr);
  SVTableRefResolveRspItem source{};
  source.nextRef.tagCondJson = taosStrdup("{\"device\":1}");
  source.nextRef.tagCondLen = strlen(source.nextRef.tagCondJson);
  source.nextRef.refType = 1;
  source.nextRef.hasRef = true;
  source.terminated = true;
  streamTblRefCacheInsertEx(cache, "db", "vt", "value", STREAM_VREF_KIND_COL, "one", 3, &source);
  streamTblRefCacheInsertEx(cache, "db", "vt", "value", STREAM_VREF_KIND_COL, "two", 3, &source);
  taosMemoryFreeClear(source.nextRef.tagCondJson);
  auto* first = streamTblRefCacheLookupEx(cache, "db", "vt", "value", STREAM_VREF_KIND_COL, "one", 3);
  auto* second = streamTblRefCacheLookupEx(cache, "db", "vt", "value", STREAM_VREF_KIND_COL, "two", 3);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_NE(first->nextRef.tagCondJson, second->nextRef.tagCondJson);
  SVTableRefResolveRspItem copied{};
  ASSERT_EQ(streamWriteRspItemDeepCopy(first, &copied), 0);
  EXPECT_NE(first->nextRef.tagCondJson, copied.nextRef.tagCondJson);
  freeCache(cache);
  EXPECT_STREQ(copied.nextRef.tagCondJson, "{\"device\":1}");
  taosMemoryFreeClear(copied.nextRef.tagCondJson);
}

TEST(VnodeStreamVTableHelpers, ColResolveItemEqualityIncludesSeriesCondition) {
  SColResolveItem first{};
  first.hasRef = true;
  first.refType = 1;
  std::snprintf(first.refSourceName, sizeof(first.refSourceName), "%s", "source");
  std::snprintf(first.refSchemaName, sizeof(first.refSchemaName), "%s", "schema");
  std::snprintf(first.refDbName, sizeof(first.refDbName), "%s", "db");
  std::snprintf(first.refTableName, sizeof(first.refTableName), "%s", "table");
  std::snprintf(first.refColName, sizeof(first.refColName), "%s", "value");
  first.tagCondJson = const_cast<char*>("{\"device\":\"d1\"}");
  first.tagCondLen = static_cast<int32_t>(std::strlen(first.tagCondJson));
  SColResolveItem second = first;
  second.tagCondJson = const_cast<char*>("{\"device\":\"d2\"}");
  EXPECT_FALSE(streamColResolveItemEqual(&first, &second));

  char sameCondition[] = "{\"device\":\"d1\"}";
  second.tagCondJson = sameCondition;
  EXPECT_TRUE(streamColResolveItemEqual(&first, &second));

  second.tagCondJson = nullptr;
  EXPECT_FALSE(streamColResolveItemEqual(&first, &second));
  second.tagCondLen = 0;
  EXPECT_FALSE(streamColResolveItemEqual(&first, &second));
  first.tagCondJson = nullptr;
  first.tagCondLen = 0;
  EXPECT_TRUE(streamColResolveItemEqual(&first, &second));
}

// ---------------------------------------------------------------------------
// streamResolveBatchFull: dual budget (uids / work items)
// ---------------------------------------------------------------------------

TEST(VnodeStreamVTableHelpers, streamResolveBatchFull_UidBudgetClosesBatch) {
  EXPECT_FALSE(streamResolveBatchFull(0, 0));
  EXPECT_FALSE(streamResolveBatchFull(STREAM_VTB_RESOLVE_UID_CHUNK - 1, 0));
  EXPECT_TRUE(streamResolveBatchFull(STREAM_VTB_RESOLVE_UID_CHUNK, 0));
  EXPECT_TRUE(streamResolveBatchFull(STREAM_VTB_RESOLVE_UID_CHUNK + 1, 0));
}

TEST(VnodeStreamVTableHelpers, streamResolveBatchFull_WorkItemBudgetClosesBatch) {
  // One virtual child table can contribute thousands of ref items, so the item
  // budget must be able to close a batch long before the uid budget is reached.
  EXPECT_FALSE(streamResolveBatchFull(1, STREAM_VTB_RESOLVE_MAX_ITEMS - 1));
  EXPECT_TRUE(streamResolveBatchFull(1, STREAM_VTB_RESOLVE_MAX_ITEMS));
  EXPECT_TRUE(streamResolveBatchFull(1, STREAM_VTB_RESOLVE_MAX_ITEMS + 1));
}

// ---------------------------------------------------------------------------
// streamResolveCacheInit / streamResolveCacheDestroy (call-private cache view)
// ---------------------------------------------------------------------------

TEST(VnodeStreamVTableHelpers, streamResolveNeedsRefCache_SparseBatchMatrix) {
  // Empty batches need no ref cache; a nonempty batch must retain its ref
  // whenever more uids remain, even when every batch has only one reference.
  EXPECT_FALSE(streamResolveNeedsRefCache(0, true));
  EXPECT_TRUE(streamResolveNeedsRefCache(1, true));
  EXPECT_FALSE(streamResolveNeedsRefCache(0, false));
  EXPECT_FALSE(streamResolveNeedsRefCache(1, false));
  EXPECT_TRUE(streamResolveNeedsRefCache(2, false));
  EXPECT_TRUE(streamResolveNeedsRefCache(3, false));
}

TEST(VnodeStreamVTableHelpers, streamResolveNeedsPrivateCacheView_SharedCacheOrCachedRefs) {
  // The view is only an adapter around routing + ref caching; either reason
  // alone is enough.
  EXPECT_FALSE(streamResolveNeedsPrivateCacheView(nullptr, false));
  EXPECT_TRUE(streamResolveNeedsPrivateCacheView(nullptr, true));

  // A shared reader cache always needs the view so routing can be borrowed.
  SStreamVTableInfoCache readerCache{};
  EXPECT_TRUE(streamResolveNeedsPrivateCacheView(&readerCache, false));
  EXPECT_TRUE(streamResolveNeedsPrivateCacheView(&readerCache, true));
}

TEST(VnodeStreamVTableHelpers, streamResolveCacheInit_SingleUidSkipsRefCacheAndBorrowsRouting) {
  SHashObj *routing = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false,
                                   HASH_NO_LOCK);
  ASSERT_NE(routing, nullptr);

  SStreamVTableInfoCache readerCache{};
  readerCache.dbVgInfo = routing;

  // Pre-fill the destination with garbage: init must reset every field, not just
  // set the two it fills in, so a future callee that reads lock/uid2Result sees
  // defined data instead of stack garbage.
  SStreamVTableInfoCache view{};
  view.uid2Result  = (SSHashObj *)0x1;
  view.tblRefCache = (SHashObj *)0x1;

  ASSERT_EQ(TSDB_CODE_SUCCESS, streamResolveCacheInit(&view, &readerCache, /*createRefCache=*/false));
  EXPECT_EQ(view.tblRefCache, nullptr);  // single uid: no hash allocation
  EXPECT_EQ(view.uid2Result, nullptr);
  EXPECT_EQ(view.dbVgInfo, routing);     // borrowed, not copied

  streamResolveCacheDestroy(&view);
  EXPECT_EQ(view.tblRefCache, nullptr);
  EXPECT_EQ(view.dbVgInfo, nullptr);           // cleared ...
  EXPECT_EQ(readerCache.dbVgInfo, routing);    // ... but the owner keeps it
  EXPECT_EQ(0, (int)taosHashGetSize(routing)); // and it is still usable, i.e. not freed

  taosHashCleanup(routing);
}

TEST(VnodeStreamVTableHelpers, streamResolveCacheInit_MultiUidCreatesRefCache) {
  SStreamVTableInfoCache readerCache{};
  SStreamVTableInfoCache view{};

  ASSERT_EQ(TSDB_CODE_SUCCESS, streamResolveCacheInit(&view, &readerCache, /*createRefCache=*/true));
  ASSERT_NE(view.tblRefCache, nullptr);
  EXPECT_EQ(view.dbVgInfo, nullptr);  // no shared routing cache supplied
  EXPECT_EQ(0, (int)taosHashGetSize(view.tblRefCache));

  char                    tag[3] = {1, 2, 3};
  SVTableRefResolveRspItem item{};
  item.tagLen  = (int32_t)sizeof(tag);
  item.tagData = tag;
  streamTblRefCacheInsert(&view, "db", "t", "c", /*kind*/ 0, &item);
  EXPECT_EQ(1, (int)taosHashGetSize(view.tblRefCache));

  // Destroy releases the entry through streamTblRefCacheItemFree; a dropped free
  // callback turns into a leak that ASAN/valgrind reports.
  streamResolveCacheDestroy(&view);
}

TEST(VnodeStreamVTableHelpers, streamResolveCacheInit_NullSharedCreatesRefCacheWithoutRouting) {
  // The tag-only resolve passes no shared cache at all: the view must still be
  // created with a ref cache, otherwise multi-uid resolves lose cross-batch
  // dedup, but it must not try to borrow routing from NULL.
  SStreamVTableInfoCache view{};

  ASSERT_EQ(TSDB_CODE_SUCCESS, streamResolveCacheInit(&view, nullptr, /*createRefCache=*/true));
  EXPECT_EQ(view.dbVgInfo, nullptr);
  ASSERT_NE(view.tblRefCache, nullptr);

  streamResolveCacheDestroy(&view);
  EXPECT_EQ(view.tblRefCache, nullptr);
  EXPECT_EQ(view.dbVgInfo, nullptr);
}

TEST(VnodeStreamVTableHelpers, streamResolveCacheEnableRefCache_IsIdempotentAndNullSafe) {
  EXPECT_EQ(TSDB_CODE_SUCCESS, streamResolveCacheEnableRefCache(nullptr));

  // A view created without the ref cache (single-ref resolve) can be upgraded
  // later when a following batch brings a second ref.
  SStreamVTableInfoCache view{};
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamResolveCacheInit(&view, nullptr, /*createRefCache=*/false));
  EXPECT_EQ(view.tblRefCache, nullptr);

  ASSERT_EQ(TSDB_CODE_SUCCESS, streamResolveCacheEnableRefCache(&view));
  ASSERT_NE(view.tblRefCache, nullptr);
  SHashObj *allocated = view.tblRefCache;

  // Second call must not replace the hash (entries would be lost).
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamResolveCacheEnableRefCache(&view));
  EXPECT_EQ(view.tblRefCache, allocated);

  streamResolveCacheDestroy(&view);
}

TEST(VnodeStreamVTableHelpers, streamResolveCacheDestroy_NullSafe) {
  streamResolveCacheDestroy(nullptr);
  SStreamVTableInfoCache empty{};
  streamResolveCacheDestroy(&empty);  // both tblRefCache and dbVgInfo are NULL
  EXPECT_EQ(empty.tblRefCache, nullptr);
  EXPECT_EQ(empty.dbVgInfo, nullptr);
}

TEST(VnodeStreamVTableHelpers, streamVTableInfoCacheInit_LeavesRefCacheToResolveViews) {
  // The reader-lifetime cache keeps routing but not reference results: the
  // resolver caches refs in a call-private view, so allocating a hash here
  // would only produce a cache that nothing ever writes.
  SStreamVTableInfoCache cache{};
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamVTableInfoCacheInit(&cache));
  EXPECT_NE(nullptr, cache.dbVgInfo);
  EXPECT_NE(nullptr, cache.uid2Result);
  EXPECT_EQ(nullptr, cache.tblRefCache);

  // A NULL ref cache must stay usable: lookup skips it and insert is a no-op.
  EXPECT_EQ(nullptr, streamTblRefCacheLookup(&cache, "db", "t", "c", /*kind*/0));
  SVTableRefResolveRspItem item{};
  streamTblRefCacheInsert(&cache, "db", "t", "c", /*kind*/0, &item);
  EXPECT_EQ(nullptr, cache.tblRefCache);

  streamVTableInfoCacheDestroy(&cache);
}

// Regression: the partial (non-fullScan) commit must accept a uid the reader
// cache does not hold yet. tSimpleHashRemove() reports a missing key as
// TSDB_CODE_INVALID_PARA, which used to be propagated as a hard failure on the
// very first VTABLE_INFO request (empty cache), so the reader never received its
// vtable info and the stream never started.
TEST(VnodeStreamVTableHelpers, streamCacheCommitResolved_FirstPartialCommitAcceptsMissingUid) {
  SStreamVTableInfoCache cache{};
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamVTableInfoCacheInit(&cache));
  ASSERT_EQ(0, (int32_t)tSimpleHashGetSize(cache.uid2Result));

  SSHashObj *resolved = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(nullptr, resolved);
  tSimpleHashSetFreeFp(resolved, streamVTableResolveResultDestroy);

  int64_t               uid  = 4580749941641793279LL;
  SVTableResolveResult *pRes = static_cast<SVTableResolveResult *>(taosMemoryCalloc(1, sizeof(SVTableResolveResult)));
  ASSERT_NE(nullptr, pRes);
  ASSERT_EQ(0, tSimpleHashPut(resolved, &uid, sizeof(uid), &pRes, POINTER_BYTES));

  EXPECT_EQ(TSDB_CODE_SUCCESS, streamCacheCommitResolved(&cache, /*fullScan=*/false, nullptr, nullptr, &resolved));
  EXPECT_TRUE(cache.valid);

  EXPECT_EQ(1, (int32_t)tSimpleHashGetSize(cache.uid2Result));
  auto **ppCached = static_cast<SVTableResolveResult **>(tSimpleHashGet(cache.uid2Result, &uid, sizeof(uid)));
  ASSERT_NE(nullptr, ppCached);
  EXPECT_EQ(pRes, *ppCached);  // ownership moved into the reader cache
  EXPECT_EQ(0, (int32_t)tSimpleHashGetSize(resolved));

  tSimpleHashCleanup(resolved);
  streamVTableInfoCacheDestroy(&cache);
}

// A following partial commit for the same uid replaces the cached result and
// releases the previous one; tSimpleHashPut() alone would leak it.
TEST(VnodeStreamVTableHelpers, streamCacheCommitResolved_PartialRecommitReplacesCachedUid) {
  SStreamVTableInfoCache cache{};
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamVTableInfoCacheInit(&cache));

  const int64_t uid = 7;
  for (int32_t round = 0; round < 2; ++round) {
    SSHashObj *resolved = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    ASSERT_NE(nullptr, resolved);
    tSimpleHashSetFreeFp(resolved, streamVTableResolveResultDestroy);

    SVTableResolveResult *pRes = static_cast<SVTableResolveResult *>(taosMemoryCalloc(1, sizeof(SVTableResolveResult)));
    ASSERT_NE(nullptr, pRes);
    ASSERT_EQ(0, tSimpleHashPut(resolved, &uid, sizeof(uid), &pRes, POINTER_BYTES));

    EXPECT_EQ(TSDB_CODE_SUCCESS, streamCacheCommitResolved(&cache, /*fullScan=*/false, nullptr, nullptr, &resolved));
    auto **ppCached = static_cast<SVTableResolveResult **>(tSimpleHashGet(cache.uid2Result, &uid, sizeof(uid)));
    ASSERT_NE(nullptr, ppCached);
    EXPECT_EQ(pRes, *ppCached);

    tSimpleHashCleanup(resolved);
  }

  EXPECT_EQ(1, (int32_t)tSimpleHashGetSize(cache.uid2Result));
  streamVTableInfoCacheDestroy(&cache);
}

TEST(VnodeStreamVTableHelpers, streamTblRefCacheItemFree_ReleasesBothPayloads) {
  // The single free callback shared by the reader cache and the call-private
  // resolver cache must clear both heap payloads; a miss shows up as a leak.
  SVTableRefResolveRspItem item{};
  item.tagData = static_cast<char *>(taosMemoryMalloc(8));
  ASSERT_NE(item.tagData, nullptr);
  item.nextRef.tagCondJson = taosStrdup("{\"k\":1}");
  ASSERT_NE(item.nextRef.tagCondJson, nullptr);

  streamTblRefCacheItemFree(&item);
  EXPECT_EQ(item.tagData, nullptr);
  EXPECT_EQ(item.nextRef.tagCondJson, nullptr);

  streamTblRefCacheItemFree(nullptr);  // must be a no-op
}

// ---------------------------------------------------------------------------
// streamDbRouteMemo: inline slots for the common case, overflow hash beyond
// ---------------------------------------------------------------------------

namespace {
// Minimal SUseDbRsp holder; pVgroupInfos stays NULL and tFreeSUsedbRsp() is
// NULL-safe, so the memo can take ownership without a real routing payload.
SUseDbRsp *mkRouteRsp() { return static_cast<SUseDbRsp *>(taosMemoryCalloc(1, sizeof(SUseDbRsp))); }
}  // namespace

TEST(VnodeStreamVTableHelpers, streamDbRouteMemo_MemoizesEveryDbBeyondInlineCap) {
  SStreamDbRouteMemo memo;
  streamDbRouteMemoInit(&memo);

  EXPECT_EQ(nullptr, streamDbRouteMemoGet(&memo, "1.db0"));

  // Two dbs past the inline cap: without the overflow hash these would be
  // re-fetched from mnode once per work item, which is the storm the memo
  // exists to remove.
  const int32_t total = STREAM_VTB_LOCAL_DB_ROUTE_MAX + 2;
  std::vector<SUseDbRsp *> rsps;
  char                     name[TSDB_DB_FNAME_LEN];
  for (int32_t i = 0; i < total; ++i) {
    std::snprintf(name, sizeof(name), "1.db%d", i);
    SUseDbRsp *pRsp = mkRouteRsp();
    ASSERT_NE(nullptr, pRsp);
    ASSERT_EQ(TSDB_CODE_SUCCESS, streamDbRouteMemoPut(&memo, name, pRsp));
    rsps.push_back(pRsp);
  }
  ASSERT_EQ(static_cast<int>(STREAM_VTB_LOCAL_DB_ROUTE_MAX), memo.num);
  ASSERT_NE(nullptr, memo.pOverflow);

  for (int32_t i = 0; i < total; ++i) {
    std::snprintf(name, sizeof(name), "1.db%d", i);
    EXPECT_EQ(rsps[i], streamDbRouteMemoGet(&memo, name));
  }
  EXPECT_EQ(nullptr, streamDbRouteMemoGet(&memo, "1.missing"));

  streamDbRouteMemoDestroy(&memo);  // releases every response it owns
  EXPECT_EQ(nullptr, memo.pOverflow);
  EXPECT_EQ(0, memo.num);
}

TEST(VnodeStreamVTableHelpers, streamDbRouteMemo_NullSafe) {
  EXPECT_EQ(nullptr, streamDbRouteMemoGet(nullptr, "1.db"));
  EXPECT_EQ(nullptr, streamDbRouteMemoGet(nullptr, nullptr));
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, streamDbRouteMemoPut(nullptr, "1.db", nullptr));

  SStreamDbRouteMemo memo;
  streamDbRouteMemoInit(&memo);
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, streamDbRouteMemoPut(&memo, "1.db", nullptr));
  EXPECT_EQ(nullptr, streamDbRouteMemoGet(&memo, nullptr));

  streamDbRouteMemoDestroy(&memo);
  streamDbRouteMemoDestroy(nullptr);
}

TEST(VnodeStreamVTableHelpers, streamDbRouteMemoPut_DuplicateKeepsCallerOwnership) {
  // A duplicate put must not free the caller's response: the caller still routes
  // with it and releases it afterwards. Freeing it here and reporting success
  // made streamBatchRouteToVgs route on freed memory.
  SStreamDbRouteMemo memo;
  streamDbRouteMemoInit(&memo);

  // Fill the inline slots so the next insert lands in the overflow hash, which
  // is the only place that can answer DUP_KEY.
  char name[TSDB_DB_FNAME_LEN];
  for (int32_t i = 0; i < STREAM_VTB_LOCAL_DB_ROUTE_MAX; ++i) {
    std::snprintf(name, sizeof(name), "1.db%d", i);
    SUseDbRsp *pRsp = mkRouteRsp();
    ASSERT_NE(nullptr, pRsp);
    ASSERT_EQ(TSDB_CODE_SUCCESS, streamDbRouteMemoPut(&memo, name, pRsp));
  }

  const char *dupDb  = "1.dup";
  SUseDbRsp  *first  = mkRouteRsp();
  SUseDbRsp  *second = mkRouteRsp();
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  ASSERT_EQ(TSDB_CODE_SUCCESS, streamDbRouteMemoPut(&memo, dupDb, first));

  EXPECT_EQ(TSDB_CODE_DUP_KEY, streamDbRouteMemoPut(&memo, dupDb, second));
  EXPECT_EQ(first, streamDbRouteMemoGet(&memo, dupDb));

  // The rejected response is still owned by the caller and must be freed here.
  tFreeSUsedbRsp(second);
  taosMemoryFree(second);

  streamDbRouteMemoDestroy(&memo);
}

// Exercise the production seed/batch/cache/RPC/scatter pipeline. Only metadata
// storage and the transport boundary are stubbed; no cache is pre-populated.
// The metadata stub fills only the SMetaEntry fields that today's seeding path
// reads (type, ctbEntry.suid/pTags, colRef.*). If seeding starts reading another
// field, this fixture will silently see zeros: extend readEntry() with it and
// assert on it here rather than relying on the resolve to stay green.
namespace {
class VnodeSparseRefResolveTest : public ::testing::Test {
 protected:
  static VnodeSparseRefResolveTest *active_;
  static constexpr col_id_t kTagCid = 2;
  static constexpr tb_uid_t kStableUid = 100000;

  static void initReader(SMetaReader *reader, SMeta *, int32_t, int64_t) {
    memset(reader, 0, sizeof(*reader));
  }
  static void clearReader(SMetaReader *) {}
  static void getMnodeEps(SEpSet *eps) { memset(eps, 0, sizeof(*eps)); }

  static int32_t readEntry(SMetaReader *reader, tb_uid_t uid) {
    auto *self = active_;
    memset(&reader->me, 0, sizeof(reader->me));
    // Simulates a child that disappears (or whose ref source is dropped) before the
    // resolve batch runs: the seed skips it, so uid2Result ends up without an entry.
    if (uid == self->metaMissingUid_) {
      return TSDB_CODE_TDB_TABLE_NOT_EXIST;
    }
    if (uid == kStableUid) {
      reader->me.type = TSDB_SUPER_TABLE;
      TABLE_SET_VIRTUAL(reader->me.flags);
      reader->me.stbEntry.schemaTag.nCols = 1;
      reader->me.stbEntry.schemaTag.pSchema = &self->schema_;
    } else {
      reader->me.type = TSDB_VIRTUAL_CHILD_TABLE;
      reader->me.ctbEntry.suid = kStableUid;
      reader->me.ctbEntry.pTags = reinterpret_cast<uint8_t *>(self->literal_);
      if (self->isRemote(uid)) {
        reader->me.colRef.nTagRefs = 1;
        reader->me.colRef.pTagRef = &self->ref_;
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  static int32_t sendMessage(void *, SEpSet *, int64_t *, SMsgSendInfo *info) {
    auto *self = active_;
    SDataBuf response = {};
    if (info->msgType == TDMT_MND_GET_DB_INFO) {
      ++self->routeRequests_;
      SUseDbRsp route = {};
      route.vgNum = 1;
      route.pVgroupInfos = taosArrayInit(1, sizeof(SVgroupInfo));
      SVgroupInfo vg = {};
      vg.vgId = 8;  // Remote to the triggering vnode (7).
      vg.hashEnd = UINT32_MAX;
      taosArrayPush(route.pVgroupInfos, &vg);
      response.len = tSerializeSUseDbRsp(nullptr, 0, &route);
      response.pData = taosMemoryMalloc(response.len);
      EXPECT_GT(tSerializeSUseDbRsp(response.pData, response.len, &route), 0);
      tFreeSUsedbRsp(&route);
    } else {
      EXPECT_EQ(TDMT_VND_VTABLE_REF_RESOLVE, info->msgType);
      ++self->resolveRequests_;
      if (self->rpcError_ != TSDB_CODE_SUCCESS) {
        EXPECT_EQ(self->rpcError_, info->fp(info->param, &response, self->rpcError_));
        destroySendMsgInfo(info);
        return TSDB_CODE_SUCCESS;
      }
      SVTableRefResolveReq request = {};
      EXPECT_EQ(0, tDeserializeSVTableRefResolveReq(
                       static_cast<char *>(info->msgInfo.pData) + sizeof(SMsgHead),
                       info->msgInfo.len - sizeof(SMsgHead), &request));
      EXPECT_EQ(1, taosArrayGetSize(request.groups));
      auto *group = static_cast<SVTableRefResolveGroupItem *>(taosArrayGet(request.groups, 0));
      EXPECT_EQ(1, taosArrayGetSize(group->cols));
      EXPECT_STREQ("remote", group->tableName);
      SVTableRefResolveRsp reply = {};
      reply.items = taosArrayInit(1, sizeof(SVTableRefResolveRspItem));
      SVTableRefResolveRspItem item = {};
      item.code = self->itemError_;
      int32_t value = 42;
      item.terminated = true;
      item.nextRef.kind = STREAM_VREF_KIND_TAG;
      item.tagType = TSDB_DATA_TYPE_INT;
      item.tagLen = self->nullTag_ ? 0 : sizeof(value);
      item.tagData = self->nullTag_ ? nullptr : reinterpret_cast<char *>(&value);
      taosArrayPush(reply.items, &item);
      response.len = tSerializeSVTableRefResolveRsp(nullptr, 0, &reply);
      response.pData = taosMemoryMalloc(response.len);
      EXPECT_GT(tSerializeSVTableRefResolveRsp(response.pData, response.len, &reply), 0);
      taosArrayDestroy(reply.items);  // The payload above is stack-owned.
      tFreeSVTableRefResolveReq(&request);
    }
    EXPECT_EQ(TSDB_CODE_SUCCESS, info->fp(info->param, &response, TSDB_CODE_SUCCESS));
    destroySendMsgInfo(info);
    return TSDB_CODE_SUCCESS;
  }

  void SetUp() override {
    active_ = this;
    vnode_.config.vgId = 7;
    tstrncpy(vnode_.config.dbname, "1.db", sizeof(vnode_.config.dbname));
    vnode_.msgCb.clientRpc = this;
    schema_.colId = kTagCid;
    schema_.type = TSDB_DATA_TYPE_INT;
    schema_.bytes = sizeof(int32_t);
    tstrncpy(schema_.name, "tag", sizeof(schema_.name));
    ref_.id = kTagCid;
    ref_.hasRef = true;
    tstrncpy(ref_.refDbName, "db", sizeof(ref_.refDbName));
    tstrncpy(ref_.refTableName, "remote", sizeof(ref_.refTableName));
    tstrncpy(ref_.refColName, "tag", sizeof(ref_.refColName));
    SArray *values = taosArrayInit(1, sizeof(STagVal));
    ASSERT_NE(nullptr, values);
    STagVal value = {};
    value.cid = kTagCid;
    value.type = TSDB_DATA_TYPE_INT;
    value.i64 = 7;
    taosArrayPush(values, &value);
    int32_t code = tTagNew(values, 1, false, &literal_);
    taosArrayDestroy(values);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    uids_ = taosArrayInit(1, sizeof(int64_t));
    cols_ = taosArrayInit(0, sizeof(col_id_t));
    tags_ = taosArrayInit(1, sizeof(col_id_t));
    ASSERT_NE(nullptr, uids_);
    ASSERT_NE(nullptr, cols_);
    ASSERT_NE(nullptr, tags_);
    col_id_t cid = kTagCid;
    ASSERT_NE(nullptr, taosArrayPush(tags_, &cid));
    ASSERT_EQ(TSDB_CODE_SUCCESS, streamVTableInfoCacheInit(&shared_));
    stub_.set(metaReaderDoInit, initReader);
    stub_.set(metaReaderGetTableEntryByUid, readEntry);
    stub_.set(metaReaderReleaseLock, clearReader);
    stub_.set(metaReaderClear, clearReader);
    stub_.set(streamGetMnodeEpset, getMnodeEps);
    stub_.set(asyncSendMsgToServer, sendMessage);
  }

  void TearDown() override {
    if (uidTags_ != nullptr) {
      for (size_t i = 0; i < taosArrayGetSize(uidTags_); ++i) {
        auto *entry = static_cast<STUidTagInfo *>(taosArrayGet(uidTags_, i));
        taosMemoryFree(entry->pTagVal);
      }
      taosArrayDestroy(uidTags_);
    }
    tSimpleHashCleanup(results_);
    streamVTableInfoCacheDestroy(&shared_);
    taosArrayDestroy(uids_);
    taosArrayDestroy(cols_);
    taosArrayDestroy(tags_);
    taosMemoryFree(literal_);
    active_ = nullptr;
  }

  // uid layout of the sparse fixture: uid == kFirstUid + k * CHUNK is the only
  // child in chunk k that references the remote tag; every other child owns a
  // constant tag and contributes no work item. Each 1000-uid batch therefore
  // holds exactly one identical remote ref, the shape a ref cache must span.
  // Column refs cannot produce this shape (a vtable column either references a
  // remote column or fails the resolve), so cols always yield one work item per
  // uid and are covered by the multi-item path instead.
  bool isRemote(tb_uid_t uid) const {
    if (uid == extraRemoteUid_) return true;
    constexpr tb_uid_t kFirstUid = 1;  // uid = kFirstUid + k * CHUNK
    return (uid - kFirstUid) / STREAM_VTB_RESOLVE_UID_CHUNK >= emptyChunks_ &&
           (uid - kFirstUid) % STREAM_VTB_RESOLVE_UID_CHUNK == 0;
  }

  void resolve(int32_t count, bool shared) {
    for (int64_t uid = 1; uid <= count; ++uid) {
      ASSERT_NE(nullptr, taosArrayPush(uids_, &uid));
    }
    ASSERT_EQ(TSDB_CODE_SUCCESS, streamResolveVTableRefChain(
        &vnode_, shared ? &shared_ : nullptr, nullptr, -1, uids_, cols_, tags_, &results_, 0));
    ASSERT_NE(nullptr, results_);
    EXPECT_EQ(count, tSimpleHashGetSize(results_));
    EXPECT_EQ(1, routeRequests_);
    EXPECT_EQ(1, resolveRequests_);
    for (int64_t uid = 1; uid <= count; ++uid) {
      auto **result = static_cast<SVTableResolveResult **>(tSimpleHashGet(results_, &uid, sizeof(uid)));
      ASSERT_NE(nullptr, result);
      col_id_t cid = kTagCid;
      auto **tag = static_cast<STagValue **>(tSimpleHashGet((*result)->tagMap, &cid, sizeof(cid)));
      ASSERT_NE(nullptr, tag);
      ASSERT_NE(nullptr, (*tag)->pData);
      ASSERT_EQ(sizeof(int32_t), (*tag)->nLen);
      int32_t value = 0;
      memcpy(&value, (*tag)->pData, sizeof(value));
      EXPECT_EQ(isRemote(uid) ? 42 : 7, value);
      EXPECT_EQ(0, tSimpleHashGetSize((*result)->colMap));
    }
  }

  void prepareTagChain() {
    uidTags_ = taosArrayInit(1, sizeof(STUidTagInfo));
    ASSERT_NE(nullptr, uidTags_);
    // Referenced TAGs have no local literal value. Keep that state distinct
    // from a successfully resolved NULL, which still has a result-map entry.
    STUidTagInfo entry = {};
    entry.uid = 1;
    ASSERT_NE(nullptr, taosArrayPush(uidTags_, &entry));
  }

  Stub stub_;
  SVnode vnode_ = {};
  SSchema schema_ = {};
  SColRef ref_ = {};
  STag *literal_ = nullptr;
  SArray *uids_ = nullptr;
  SArray *cols_ = nullptr;
  SArray *tags_ = nullptr;
  SSHashObj *results_ = nullptr;
  SStreamVTableInfoCache shared_ = {};
  int32_t emptyChunks_ = 0;
  int32_t routeRequests_ = 0;
  int32_t resolveRequests_ = 0;
  SArray *uidTags_ = nullptr;
  int32_t rpcError_ = TSDB_CODE_SUCCESS;
  int32_t itemError_ = TSDB_CODE_SUCCESS;
  bool nullTag_ = false;
  tb_uid_t extraRemoteUid_ = 0;
  tb_uid_t metaMissingUid_ = 0;
};

VnodeSparseRefResolveTest *VnodeSparseRefResolveTest::active_ = nullptr;

TEST_F(VnodeSparseRefResolveTest, SparseTagOnlyChunksReuseRemoteResult) {
  resolve(2 * STREAM_VTB_RESOLVE_UID_CHUNK, false);
}

TEST_F(VnodeSparseRefResolveTest, EmptyChunkThenSparseChunksEnableBorrowedViewCache) {
  emptyChunks_ = 1;
  resolve(3 * STREAM_VTB_RESOLVE_UID_CHUNK, true);
}

TEST_F(VnodeSparseRefResolveTest, EmptyChunkThenSparseChunksCreatePrivateCache) {
  emptyChunks_ = 1;
  resolve(3 * STREAM_VTB_RESOLVE_UID_CHUNK, false);
}

TEST_F(VnodeSparseRefResolveTest, SingleReferenceStillResolves) {
  resolve(1, false);
}

TEST_F(VnodeSparseRefResolveTest, TagChainSkipsNormalTable) {
  prepareTagChain();
  EXPECT_EQ(TSDB_CODE_SUCCESS, vnodeResolveVTableTagChain(&vnode_, 0, uidTags_, tags_, 0));
  EXPECT_EQ(0, resolveRequests_);
  EXPECT_EQ(0, routeRequests_);
}

TEST_F(VnodeSparseRefResolveTest, TagChainPropagatesRpcFailure) {
  prepareTagChain();
  rpcError_ = TSDB_CODE_TIMEOUT_ERROR;
  EXPECT_EQ(TSDB_CODE_TIMEOUT_ERROR,
            vnodeResolveVTableTagChain(&vnode_, kStableUid, uidTags_, tags_, 0));
  EXPECT_EQ(1, resolveRequests_);
  auto *entry = static_cast<STUidTagInfo *>(taosArrayGet(uidTags_, 0));
  EXPECT_EQ(nullptr, entry->pTagVal);
}

TEST_F(VnodeSparseRefResolveTest, TagChainRejectsMissingResultAfterItemFailure) {
  prepareTagChain();
  itemError_ = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG,
            vnodeResolveVTableTagChain(&vnode_, kStableUid, uidTags_, tags_, 0));
  EXPECT_EQ(1, resolveRequests_);
  auto *entry = static_cast<STUidTagInfo *>(taosArrayGet(uidTags_, 0));
  EXPECT_EQ(nullptr, entry->pTagVal);
}

TEST_F(VnodeSparseRefResolveTest, TagChainAcceptsResolvedNull) {
  prepareTagChain();
  nullTag_ = true;
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            vnodeResolveVTableTagChain(&vnode_, kStableUid, uidTags_, tags_, 0));
  EXPECT_EQ(1, resolveRequests_);
  auto *entry = static_cast<STUidTagInfo *>(taosArrayGet(uidTags_, 0));
  STagVal value = {};
  value.cid = kTagCid;
  // A resolved NULL must not fabricate a value: the child keeps either no STag
  // at all or one that still has no entry for this cid. tTagGet(nullptr) is
  // false, so one assertion covers both shapes.
  EXPECT_FALSE(tTagGet(static_cast<const STag *>(entry->pTagVal), &value));
}

TEST_F(VnodeSparseRefResolveTest, TagChainRebuildsResolvedValue) {
  prepareTagChain();
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            vnodeResolveVTableTagChain(&vnode_, kStableUid, uidTags_, tags_, 0));
  EXPECT_EQ(1, resolveRequests_);
  auto *entry = static_cast<STUidTagInfo *>(taosArrayGet(uidTags_, 0));
  ASSERT_NE(nullptr, entry->pTagVal);
  STagVal value = {};
  value.cid = kTagCid;
  ASSERT_TRUE(tTagGet(static_cast<const STag *>(entry->pTagVal), &value));
  EXPECT_EQ(TSDB_DATA_TYPE_INT, value.type);
  // STagVal has no i32 member and tTagGet only fills the low tagLen bytes of
  // the i64 slot, so read the value back with the schema width like the
  // production merge path does instead of touching value.i64 directly.
  int32_t rebuilt = 0;
  memcpy(&rebuilt, &value.i64, sizeof(rebuilt));
  EXPECT_EQ(42, rebuilt);
}

TEST_F(VnodeSparseRefResolveTest, TagChainKeepsRebuildingAfterEarlierUidFails) {
  // uid 1 has no resolve result (its ref source is gone), uid 2 resolves fine. The
  // error must reach the caller, but uid 2 must still get its rebuilt STag: leaving
  // it with the unresolved placeholder makes a grouping caller fold unrelated
  // children into one NULL group.
  prepareTagChain();  // uid = 1
  STUidTagInfo second = {};
  second.uid = 2;
  ASSERT_NE(nullptr, taosArrayPush(uidTags_, &second));
  extraRemoteUid_ = 2;
  metaMissingUid_ = 1;

  EXPECT_EQ(TSDB_CODE_INVALID_MSG,
            vnodeResolveVTableTagChain(&vnode_, kStableUid, uidTags_, tags_, 0));
  EXPECT_EQ(1, resolveRequests_);

  auto *failed = static_cast<STUidTagInfo *>(taosArrayGet(uidTags_, 0));
  EXPECT_EQ(nullptr, failed->pTagVal);

  auto *resolved = static_cast<STUidTagInfo *>(taosArrayGet(uidTags_, 1));
  ASSERT_NE(nullptr, resolved->pTagVal);
  STagVal value = {};
  value.cid = kTagCid;
  ASSERT_TRUE(tTagGet(static_cast<const STag *>(resolved->pTagVal), &value));
  int32_t rebuilt = 0;
  memcpy(&rebuilt, &value.i64, sizeof(rebuilt));
  EXPECT_EQ(42, rebuilt);
}
}  // namespace
