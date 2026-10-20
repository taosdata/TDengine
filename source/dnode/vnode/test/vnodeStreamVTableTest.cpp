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

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "functionMgt.h"
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
