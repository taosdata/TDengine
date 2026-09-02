#include "gtest/gtest.h"

#include "executil.h"
#include "executorInt.h"
#include "functionMgt.h"
#include "libs/new-stream/dataSink.h"
#include "osMemory.h"
#include "querytask.h"
#include "querynodes.h"
#include "tdatablock.h"

extern "C" SHashObj *gStreamGrpTableHash;
extern "C" char      tsStableTagFilterCache;
extern "C" int32_t getTableList(void *pVnode, SScanPhysiNode *pScanNode, SNode *pTagCond, SNode *pTagIndexCond,
                                STableListInfo *pListInfo, uint8_t *digest, const char *idstr, SStorageAPI *pStorageAPI,
                                void *pStreamInfo, int64_t txnId);
extern "C" void handleRemoteRowRes(SScalarFetchParam* pParam, STaskSubJobCtx* ctx, SRetrieveTableRsp* pRsp,
                                    bool* fetchDone);

namespace {

SStreamAncestorContext *makeFetchProjectionContext(int64_t gid, int32_t paramIndex) {
  auto *context = static_cast<SStreamAncestorContext *>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
  EXPECT_NE(context, nullptr);
  if (context == nullptr) return nullptr;
  context->pParamContexts = taosArrayInit(1, sizeof(SStreamAncestorParamContext));
  EXPECT_NE(context->pParamContexts, nullptr);
  if (context->pParamContexts == nullptr) return context;

  SStreamAncestorParamContext param = {};
  param.paramIndex = paramIndex;
  param.leafIdentity.gid = gid;
  param.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
  param.leafIdentity.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  EXPECT_NE(param.leafIdentity.lineage.pScopes, nullptr);
  SScopeInstanceId scope = {};
  scope.layerIndex = 0;
  scope.triggerType = WINDOW_TYPE_SESSION;
  scope.openingTs = 100;
  scope.nativeDiscriminator = 1;
  EXPECT_NE(taosArrayPush(param.leafIdentity.lineage.pScopes, &scope), nullptr);
  param.pSnapshots = taosArrayInit(1, sizeof(SWindowAncestorSnapshot));
  EXPECT_NE(param.pSnapshots, nullptr);
  SWindowAncestorSnapshot snapshot = {};
  snapshot.layerIndex = 0;
  snapshot.triggerType = WINDOW_TYPE_SESSION;
  snapshot.placeholderMask = PLACE_HOLDER_WSTART;
  snapshot.values.window.start = 100;
  EXPECT_NE(taosArrayPush(param.pSnapshots, &snapshot), nullptr);
  EXPECT_NE(taosArrayPush(context->pParamContexts, &param), nullptr);
  return context;
}

SStreamContextPolicy *makeFetchProjectionPolicy(int64_t gid, int32_t paramIndex) {
  auto *policy = static_cast<SStreamContextPolicy *>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
  EXPECT_NE(policy, nullptr);
  if (policy == nullptr) return nullptr;
  policy->pEntries = taosArrayInit(paramIndex + 1, sizeof(SStreamContextPolicyEntry));
  EXPECT_NE(policy->pEntries, nullptr);
  if (policy->pEntries == nullptr) return policy;
  for (int32_t i = 0; i <= paramIndex; ++i) {
    SStreamContextPolicyEntry entry = {};
    entry.gid = gid;
    entry.paramIndex = i;
    entry.contextPolicy = i == paramIndex ? STREAM_CONTEXT_POLICY_ANCESTOR : STREAM_CONTEXT_POLICY_NONE;
    EXPECT_NE(taosArrayPush(policy->pEntries, &entry), nullptr);
  }
  return policy;
}

struct StableTagFilterWarmupMock {
  int32_t getCacheCalls = 0;
  int32_t warmupCalls = 0;
  int32_t getTableTagsCalls = 0;
  bool    warmed = false;
  bool    cacheHitAfterWarmup = true;
};

int32_t mockGetStableCachedTableList(void *pVnode, tb_uid_t suid, const uint8_t *pTagCondKey, int32_t tagCondKeyLen,
                                     const uint8_t *pKey, int32_t keyLen, SArray *pList, bool *acquired,
                                     bool *needWarmup) {
  (void)suid;
  (void)pTagCondKey;
  (void)tagCondKeyLen;
  (void)pKey;
  (void)keyLen;
  StableTagFilterWarmupMock *pMock = static_cast<StableTagFilterWarmupMock *>(pVnode);
  pMock->getCacheCalls += 1;
  *acquired = false;
  if (needWarmup != nullptr) {
    *needWarmup = !pMock->warmed;
  }

  if (pMock->warmed && pMock->cacheHitAfterWarmup) {
    uint64_t uid = 1001;
    if (taosArrayPush(pList, &uid) == nullptr) {
      return terrno;
    }
    *acquired = true;
    if (needWarmup != nullptr) {
      *needWarmup = false;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mockWarmupStableCachedTableList(void *pVnode, uint64_t suid, const void *pTagCondKey, int32_t tagCondKeyLen,
                                        const uint8_t *pKey, int32_t keyLen, const SArray *pTagColIds, SArray *pList,
                                        bool *acquired) {
  (void)suid;
  (void)pTagCondKey;
  (void)tagCondKeyLen;
  (void)pKey;
  (void)keyLen;
  StableTagFilterWarmupMock *pMock = static_cast<StableTagFilterWarmupMock *>(pVnode);
  pMock->warmupCalls += 1;
  pMock->warmed = true;
  EXPECT_NE(pTagColIds, nullptr);
  EXPECT_EQ(taosArrayGetSize(pTagColIds), 1);
  if (acquired != nullptr) {
    *acquired = false;
  }
  if (pMock->cacheHitAfterWarmup) {
    uint64_t uid = 1001;
    if (taosArrayPush(pList, &uid) == nullptr) {
      return terrno;
    }
    if (acquired != nullptr) {
      *acquired = true;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t mockGetTableTags(void *pVnode, uint64_t suid, SArray *uidList, int64_t txnId) {
  (void)suid;
  (void)uidList;
  (void)txnId;
  StableTagFilterWarmupMock *pMock = static_cast<StableTagFilterWarmupMock *>(pVnode);
  pMock->getTableTagsCalls += 1;
  return TSDB_CODE_SUCCESS;
}

int32_t mockPutStableCachedTableList(void *pVnode, uint64_t suid, const void *pTagCondKey, int32_t tagCondKeyLen,
                                     const void *pKey, int32_t keyLen, SArray *pUidList, SArray **pTagColIds) {
  (void)pVnode;
  (void)suid;
  (void)pTagCondKey;
  (void)tagCondKeyLen;
  (void)pKey;
  (void)keyLen;
  (void)pUidList;
  (void)pTagColIds;
  return TSDB_CODE_SUCCESS;
}

SNode *makeTagColumn(col_id_t colId, int8_t type, int32_t bytes) {
  SColumnNode *pColumn = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pColumn)), TSDB_CODE_SUCCESS);
  pColumn->node.resType.type = type;
  pColumn->node.resType.bytes = bytes;
  pColumn->colId = colId;
  pColumn->colType = COLUMN_TYPE_TAG;
  return reinterpret_cast<SNode *>(pColumn);
}

SNode *makeIntValue(int32_t value) {
  SValueNode *pValue = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pValue)), TSDB_CODE_SUCCESS);
  pValue->node.resType.type = TSDB_DATA_TYPE_INT;
  pValue->node.resType.bytes = sizeof(value);
  EXPECT_EQ(nodesSetValueNodeValue(pValue, &value), TSDB_CODE_SUCCESS);
  pValue->translate = true;
  return reinterpret_cast<SNode *>(pValue);
}

SNode *makeEqualCond(SNode *pLeft, SNode *pRight) {
  SOperatorNode *pOp = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&pOp)), TSDB_CODE_SUCCESS);
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = sizeof(bool);
  pOp->opType = OP_TYPE_EQUAL;
  pOp->pLeft = pLeft;
  pOp->pRight = pRight;
  return reinterpret_cast<SNode *>(pOp);
}

}  // namespace

TEST(execUtilTest, resRowTest) {
  SDiskbasedBuf *pBuf = nullptr;
  int32_t        pageSize = 32;
  int32_t        numPages = 3;
  int32_t        code = createDiskbasedBuf(&pBuf, pageSize, pageSize * numPages, "test_buf", "/");
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);

  std::vector<void *>  pages(numPages);
  std::vector<int32_t> pageIds(numPages);
  for (int32_t i = 0; i < numPages; ++i) {
    pages[i] = getNewBufPage(pBuf, &pageIds[i]);
    EXPECT_NE(pages[i], nullptr);
    EXPECT_EQ(pageIds[i], i);
  }

  EXPECT_EQ(getNewBufPage(pBuf, nullptr), nullptr);

  SResultRowPosition pos;
  pos.offset = 0;
  for (int32_t i = 0; i < numPages; ++i) {
    pos.pageId = pageIds[i];
    bool forUpdate = i & 0x1;
    SResultRow *row =  getResultRowByPos(pBuf, &pos, forUpdate);
    EXPECT_EQ((void *)row, pages[i]);
  }

  pos.pageId = numPages + 1;
  EXPECT_EQ(getResultRowByPos(pBuf, &pos, true), nullptr);

  destroyDiskbasedBuf(pBuf);
}

TEST(streamDataInserterTest, groupTableHashUsesFullStreamGroupKey) {
  ASSERT_EQ(initInserterGrpInfo(), TSDB_CODE_SUCCESS);
  ASSERT_NE(gStreamGrpTableHash, nullptr);

  constexpr int32_t numOfGroups = 512;
  constexpr int64_t streamId = 0x12345678;

  for (int64_t groupId = 0; groupId < numOfGroups; ++groupId) {
    int64_t key[2] = {streamId, groupId};
    auto    pInfo = (SInsertTableInfo *)taosMemoryCalloc(1, sizeof(SInsertTableInfo));
    ASSERT_NE(pInfo, nullptr);
    ASSERT_EQ(taosHashPut(gStreamGrpTableHash, key, sizeof(key), &pInfo, sizeof(SInsertTableInfo *)),
              TSDB_CODE_SUCCESS);
  }

  EXPECT_LT(taosHashGetMaxOverflowLinkLength(gStreamGrpTableHash), 64);

  taosHashCleanup(gStreamGrpTableHash);
  gStreamGrpTableHash = nullptr;
}

TEST(execUtilTest, stableTagFilterCacheWarmsAllEqualTagGroupsOnMiss) {
  char oldStableTagFilterCache = tsStableTagFilterCache;
  tsStableTagFilterCache = 1;

  StableTagFilterWarmupMock mock;
  SStorageAPI               api = {0};
  api.metaFn.getStableCachedTableList = mockGetStableCachedTableList;
  api.metaFn.warmupStableCachedTableList = mockWarmupStableCachedTableList;
  api.metaFn.getTableTags = mockGetTableTags;
  api.metaFn.putStableCachedTableList = mockPutStableCachedTableList;

  SScanPhysiNode scan;
  memset(&scan, 0, sizeof(scan));
  scan.suid = 42;
  scan.uid = 42;
  scan.tableType = TSDB_SUPER_TABLE;

  SStreamRuntimeFuncInfo streamInfo = {0};
  STableListInfo         tableListInfo = {0};
  tableListInfo.pTableList = taosArrayInit(8, sizeof(STableKeyInfo));
  ASSERT_NE(tableListInfo.pTableList, nullptr);

  SNode *pTagCond = makeEqualCond(makeTagColumn(3, TSDB_DATA_TYPE_INT, sizeof(int32_t)), makeIntValue(7));

  ASSERT_EQ(
      getTableList(&mock, &scan, pTagCond, nullptr, &tableListInfo, nullptr, "stable-cache-warmup", &api, &streamInfo, 0),
      TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(tableListInfo.pTableList), 1);

  STableKeyInfo *pTable = static_cast<STableKeyInfo *>(taosArrayGet(tableListInfo.pTableList, 0));
  ASSERT_NE(pTable, nullptr);
  EXPECT_EQ(pTable->uid, 1001);
  EXPECT_EQ(mock.getCacheCalls, 1);
  EXPECT_EQ(mock.warmupCalls, 1);
  EXPECT_EQ(mock.getTableTagsCalls, 0);

  nodesDestroyNode(pTagCond);
  taosArrayDestroy(tableListInfo.pTableList);
  tsStableTagFilterCache = oldStableTagFilterCache;
}

TEST(execUtilTest, stableTagFilterCacheFallsBackWhenWarmupMissesDigest) {
  char oldStableTagFilterCache = tsStableTagFilterCache;
  tsStableTagFilterCache = 1;

  StableTagFilterWarmupMock mock;
  mock.cacheHitAfterWarmup = false;

  SStorageAPI api = {0};
  api.metaFn.getStableCachedTableList = mockGetStableCachedTableList;
  api.metaFn.warmupStableCachedTableList = mockWarmupStableCachedTableList;
  api.metaFn.getTableTags = mockGetTableTags;
  api.metaFn.putStableCachedTableList = mockPutStableCachedTableList;

  SScanPhysiNode scan;
  memset(&scan, 0, sizeof(scan));
  scan.suid = 42;
  scan.uid = 42;
  scan.tableType = TSDB_SUPER_TABLE;

  SStreamRuntimeFuncInfo streamInfo = {0};
  STableListInfo         tableListInfo = {0};
  tableListInfo.pTableList = taosArrayInit(8, sizeof(STableKeyInfo));
  ASSERT_NE(tableListInfo.pTableList, nullptr);

  SNode *pTagCond = makeEqualCond(makeTagColumn(3, TSDB_DATA_TYPE_INT, sizeof(int32_t)), makeIntValue(7));

  ASSERT_EQ(
      getTableList(&mock, &scan, pTagCond, nullptr, &tableListInfo, nullptr, "stable-cache-warmup-miss", &api,
                   &streamInfo, 0),
      TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosArrayGetSize(tableListInfo.pTableList), 0);
  EXPECT_EQ(mock.getCacheCalls, 1);
  EXPECT_EQ(mock.warmupCalls, 1);
  EXPECT_EQ(mock.getTableTagsCalls, 1);

  nodesDestroyNode(pTagCond);
  taosArrayDestroy(tableListInfo.pTableList);
  tsStableTagFilterCache = oldStableTagFilterCache;
}

TEST(execUtilTest, stableTagFilterCacheSkipsWarmupWhenEntryAlreadyPrewarmed) {
  char oldStableTagFilterCache = tsStableTagFilterCache;
  tsStableTagFilterCache = 1;

  StableTagFilterWarmupMock mock;
  mock.warmed = true;
  mock.cacheHitAfterWarmup = false;

  SStorageAPI api = {0};
  api.metaFn.getStableCachedTableList = mockGetStableCachedTableList;
  api.metaFn.warmupStableCachedTableList = mockWarmupStableCachedTableList;
  api.metaFn.getTableTags = mockGetTableTags;
  api.metaFn.putStableCachedTableList = mockPutStableCachedTableList;

  SScanPhysiNode scan;
  memset(&scan, 0, sizeof(scan));
  scan.suid = 42;
  scan.uid = 42;
  scan.tableType = TSDB_SUPER_TABLE;

  SStreamRuntimeFuncInfo streamInfo = {0};
  STableListInfo         tableListInfo = {0};
  tableListInfo.pTableList = taosArrayInit(8, sizeof(STableKeyInfo));
  ASSERT_NE(tableListInfo.pTableList, nullptr);

  SNode *pTagCond = makeEqualCond(makeTagColumn(3, TSDB_DATA_TYPE_INT, sizeof(int32_t)), makeIntValue(7));

  ASSERT_EQ(getTableList(&mock, &scan, pTagCond, nullptr, &tableListInfo, nullptr, "stable-cache-prewarmed-miss", &api,
                         &streamInfo, 0),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosArrayGetSize(tableListInfo.pTableList), 0);
  EXPECT_EQ(mock.getCacheCalls, 1);
  EXPECT_EQ(mock.warmupCalls, 0);
  EXPECT_EQ(mock.getTableTagsCalls, 1);

  nodesDestroyNode(pTagCond);
  taosArrayDestroy(tableListInfo.pTableList);
  tsStableTagFilterCache = oldStableTagFilterCache;
}

TEST(execUtilTest, buildRunnerFetchInfoProjectsCurrentAncestorContext) {
  SStreamRuntimeFuncInfo source = {};
  source.groupId = 101;
  source.curIdx = 1;
  source.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  source.pStreamPesudoFuncVals = taosArrayInit(2, sizeof(SSTriggerCalcParam));
  ASSERT_NE(source.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam first = {};
  first.wstart = 100;
  first.wend = 199;
  first.notifyType = BIT_FLAG_MASK(0);
  SSTriggerCalcParam second = {};
  second.wstart = 200;
  second.wend = 299;
  second.notifyType = BIT_FLAG_MASK(0);
  ASSERT_NE(taosArrayPush(source.pStreamPesudoFuncVals, &first), nullptr);
  ASSERT_NE(taosArrayPush(source.pStreamPesudoFuncVals, &second), nullptr);
  source.pContextPolicy = makeFetchProjectionPolicy(source.groupId, 1);
  source.pAncestorContext = makeFetchProjectionContext(source.groupId, 1);

  SStreamRuntimeFuncInfo projected = {};
  ASSERT_EQ(buildStreamRunnerFetchRtInfo(&source, &projected), TSDB_CODE_SUCCESS);
  ASSERT_NE(projected.pContextPolicy, nullptr);
  EXPECT_NE(source.pContextPolicy, projected.pContextPolicy);
  ASSERT_EQ(taosArrayGetSize(projected.pContextPolicy->pEntries), 1);
  const auto *policyEntry =
      static_cast<const SStreamContextPolicyEntry *>(taosArrayGet(projected.pContextPolicy->pEntries, 0));
  ASSERT_NE(policyEntry, nullptr);
  EXPECT_EQ(policyEntry->paramIndex, 0);
  ASSERT_NE(projected.pAncestorContext, nullptr);
  EXPECT_NE(source.pAncestorContext, projected.pAncestorContext);
  ASSERT_EQ(taosArrayGetSize(projected.pAncestorContext->pParamContexts), 1);
  const auto *param =
      static_cast<const SStreamAncestorParamContext *>(taosArrayGet(projected.pAncestorContext->pParamContexts, 0));
  ASSERT_NE(param, nullptr);
  EXPECT_EQ(param->paramIndex, 0);
  const auto *snapshot = static_cast<const SWindowAncestorSnapshot *>(taosArrayGet(param->pSnapshots, 0));
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->values.window.start, 100);

  tDestroyStRtFuncInfo(&source);
  ASSERT_NE(projected.pContextPolicy, nullptr);
  ASSERT_NE(projected.pAncestorContext, nullptr);
  cleanupStreamRunnerFetchRtInfo(&projected);
}

// Test for fix of double-free bug in projectApplyFunction (GHSA-f8pf-77fh-53wv).
//
// When scalarCalculate succeeds but the subsequent colDataMergeCol/colDataAssign
// fails, pBlockList was freed manually and then freed again at _exit because the
// pointer was not nullified after the first taosArrayDestroy call.
//
// The fix: set pBlockList = NULL immediately after taosArrayDestroy so the
// second call at _exit is a safe no-op.
//
// This test exercises the scalar function path of projectApplyFunctions using
// the abs() built-in and verifies:
//   1. Success path (createNewColModel=true): result column is populated correctly.
//   2. Merge path (createNewColModel=false, pResult->info.rows > 0): subsequent
//      rows are appended without memory errors.
//
// Running under AddressSanitizer (-fsanitize=address) will catch any double-free
// regression on either path.
TEST(projectApplyFunctionsTest, scalarFuncNoDoubleFreeOnErrorAndSuccessPath) {
  ASSERT_EQ(fmFuncMgtInit(), TSDB_CODE_SUCCESS);

  // --- resolve abs() funcId ---
  int32_t absFuncId = fmGetFuncId("abs");
  ASSERT_GE(absFuncId, 0);
  ASSERT_TRUE(fmIsScalarFunc(absFuncId));

  // --- build SFunctionNode for abs(col) ---
  SFunctionNode* pFuncNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode**>(&pFuncNode)), TSDB_CODE_SUCCESS);
  pFuncNode->funcId = absFuncId;
  pFuncNode->node.resType.type  = TSDB_DATA_TYPE_BIGINT;
  pFuncNode->node.resType.bytes = sizeof(int64_t);

  // Build the column parameter node for abs(col0)
  SColumnNode* pColNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode**>(&pColNode)), TSDB_CODE_SUCCESS);
  pColNode->node.resType.type  = TSDB_DATA_TYPE_BIGINT;
  pColNode->node.resType.bytes = sizeof(int64_t);
  pColNode->slotId             = 0;
  ASSERT_EQ(nodesListMakeAppend(&pFuncNode->pParameterList, reinterpret_cast<SNode*>(pColNode)),
            TSDB_CODE_SUCCESS);

  // --- build tExprNode wrapper ---
  tExprNode exprNode;
  memset(&exprNode, 0, sizeof(exprNode));
  exprNode.nodeType                    = QUERY_NODE_FUNCTION;
  exprNode._function.functionId        = absFuncId;
  exprNode._function.pFunctNode        = pFuncNode;

  // --- build SExprInfo ---
  SExprInfo exprInfo;
  memset(&exprInfo, 0, sizeof(exprInfo));
  exprInfo.pExpr                       = &exprNode;
  exprInfo.base.resSchema.type         = TSDB_DATA_TYPE_BIGINT;
  exprInfo.base.resSchema.bytes        = sizeof(int64_t);
  exprInfo.base.resSchema.slotId       = 0;

  // --- build SqlFunctionCtx (only the fields accessed on the scalar path) ---
  SExprInfo ctxExprInfo;
  memset(&ctxExprInfo, 0, sizeof(ctxExprInfo));
  ctxExprInfo.base.pParamList = nullptr;  // ensures fmIsPlaceHolderFunc branch is skipped

  SqlFunctionCtx funcCtx;
  memset(&funcCtx, 0, sizeof(funcCtx));
  funcCtx.functionId = static_cast<int16_t>(absFuncId);
  funcCtx.pExpr      = &ctxExprInfo;

  // --- build source block with one BIGINT column, 3 rows ---
  SSDataBlock* pSrc = nullptr;
  ASSERT_EQ(createDataBlock(&pSrc), TSDB_CODE_SUCCESS);
  SColumnInfoData srcCol = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 1);
  ASSERT_EQ(blockDataAppendColInfo(pSrc, &srcCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pSrc, 3), TSDB_CODE_SUCCESS);
  pSrc->info.rows = 3;
  int64_t vals[3] = {-1LL, 2LL, -3LL};
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(pSrc->pDataBlock, 0)), i,
                            reinterpret_cast<const char*>(&vals[i]), false),
              TSDB_CODE_SUCCESS);
  }

  // --- build result block with matching BIGINT output column ---
  SSDataBlock* pResult = nullptr;
  ASSERT_EQ(createDataBlock(&pResult), TSDB_CODE_SUCCESS);
  SColumnInfoData resCol = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 1);
  ASSERT_EQ(blockDataAppendColInfo(pResult, &resCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pResult, 8), TSDB_CODE_SUCCESS);

  SArray* pPseudoList = taosArrayInit(1, sizeof(int32_t));
  ASSERT_NE(pPseudoList, nullptr);

  // --- Test 1: createNewColModel path (pResult->info.rows == 0) ---
  // Exercises colDataAssign; pBlockList must be freed exactly once.
  pResult->info.rows = 0;
  int32_t code = projectApplyFunctions(&exprInfo, pResult, pSrc, &funcCtx, 1, pPseudoList, nullptr, nullptr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(pResult->info.rows, 3);

  // Verify abs values: |-1|=1, |2|=2, |-3|=3
  SColumnInfoData* pOut = static_cast<SColumnInfoData*>(taosArrayGet(pResult->pDataBlock, 0));
  ASSERT_NE(pOut, nullptr);
  EXPECT_EQ(*reinterpret_cast<int64_t*>(colDataGetData(pOut, 0)), 1LL);
  EXPECT_EQ(*reinterpret_cast<int64_t*>(colDataGetData(pOut, 1)), 2LL);
  EXPECT_EQ(*reinterpret_cast<int64_t*>(colDataGetData(pOut, 2)), 3LL);

  // --- Test 2: merge path (pResult->info.rows > 0, createNewColModel=false) ---
  // Exercises colDataMergeCol; pBlockList must also be freed exactly once.
  // pResult already has 3 rows; append 3 more.
  ASSERT_EQ(blockDataEnsureCapacity(pResult, 6), TSDB_CODE_SUCCESS);
  code = projectApplyFunctions(&exprInfo, pResult, pSrc, &funcCtx, 1, pPseudoList, nullptr, nullptr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(pResult->info.rows, 6);

  // Verify the newly appended rows
  EXPECT_EQ(*reinterpret_cast<int64_t*>(colDataGetData(pOut, 3)), 1LL);
  EXPECT_EQ(*reinterpret_cast<int64_t*>(colDataGetData(pOut, 4)), 2LL);
  EXPECT_EQ(*reinterpret_cast<int64_t*>(colDataGetData(pOut, 5)), 3LL);

  // --- cleanup ---
  taosArrayDestroy(pPseudoList);
  blockDataDestroy(pSrc);
  blockDataDestroy(pResult);
  nodesDestroyNode(reinterpret_cast<SNode*>(pFuncNode));
  fmFuncMgtDestroy();
}

// Test for fix of double-free bug in handleRemoteRowRes / extractSingleRspBlock
// (GHSA-2gr8-qqmv-v247).
//
// extractSingleRspBlock previously destroyed the caller-owned SSDataBlock on
// error, and handleRemoteRowRes then destroyed the same block again in its
// unified cleanup path. The fix keeps cleanup ownership in the caller.
TEST(execUtilTest, handleRemoteRowResNoDoubleFreeOnExtractFailure) {
  SExecTaskInfo    task;
  STaskSubJobCtx   ctx = {0};
  SScalarFetchParam param = {0};
  SRemoteRowNode   remote;
  SRetrieveTableRsp rsp = {0};
  bool             fetchDone = false;
  SNode*           pStored = nullptr;

  memset(&task, 0, sizeof(task));
  memset(&remote, 0, sizeof(remote));
  task.execModel = OPTR_EXEC_MODEL_BATCH;

  ctx.idStr = const_cast<char*>("executil-test");
  ctx.pTaskInfo = &task;
  ctx.subResNodes = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(ctx.subResNodes, nullptr);
  ASSERT_NE(taosArrayPush(ctx.subResNodes, &pStored), nullptr);

  remote.val.node.resType.type = TSDB_DATA_TYPE_INT;
  remote.val.node.resType.bytes = sizeof(int32_t);
  remote.val.flag = VALUE_FLAG_VAL_UNSET;

  param.subQIdx = 0;
  param.pRes = reinterpret_cast<SNode*>(&remote);

  rsp.numOfRows = 1;
  rsp.numOfBlocks = 1;
  rsp.numOfCols = 2;
  rsp.completed = true;

  execUtilTestResetHandleRemoteRowResState();
  execUtilTestSetExtractSingleRspBlockFailOnce(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

  handleRemoteRowRes(&param, &ctx, &rsp, &fetchDone);

  EXPECT_EQ(ctx.code, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  EXPECT_TRUE(fetchDone);
  EXPECT_EQ(execUtilTestGetHandleRemoteRowResDestroyCount(), 1);
  EXPECT_FALSE(remote.valSet);
  EXPECT_FALSE(remote.hasValue);

  execUtilTestResetHandleRemoteRowResState();
  taosArrayDestroy(ctx.subResNodes);
}

TEST(execUtilTest, externalWindowReusableBlockGrowsToRequestedRows) {
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);

  SColumnInfoData col = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &col), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pBlock, 4096), TSDB_CODE_SUCCESS);
  ASSERT_EQ(pBlock->info.capacity, 4096);

  SArray *pIdx = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(pIdx, nullptr);

  SList *pFreeBlocks = tdListNew(POINTER_BYTES * 2);
  SList *pTargetBlocks = tdListNew(POINTER_BYTES * 2);
  ASSERT_NE(pFreeBlocks, nullptr);
  ASSERT_NE(pTargetBlocks, nullptr);

  void *reusable[2] = {pBlock, pIdx};
  ASSERT_EQ(tdListAppend(pFreeBlocks, reusable), TSDB_CODE_SUCCESS);

  SSDataBlock *pReusedBlock = nullptr;
  SArray      *pReusedIdx = nullptr;
  ASSERT_EQ(extWinTestTakeReusableBlock(pFreeBlocks, pTargetBlocks, 4097, &pReusedBlock, &pReusedIdx),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pReusedBlock, pBlock);
  EXPECT_EQ(pReusedIdx, pIdx);
  EXPECT_GE(pReusedBlock->info.capacity, 4097);
  EXPECT_EQ(listNEles(pFreeBlocks), 0);
  EXPECT_EQ(listNEles(pTargetBlocks), 1);

  SListNode *pNode = listTail(pTargetBlocks);
  ASSERT_NE(pNode, nullptr);
  int32_t overlapCol = -1;
  EXPECT_TRUE(extWinTestBlockNodeInvariantHolds(pTargetBlocks, pNode, pNode->dl_prev_, pReusedBlock, pReusedIdx, 0,
                                                &overlapCol));
  EXPECT_EQ(overlapCol, -1);

  SColumnInfoData *pCol = static_cast<SColumnInfoData *>(taosArrayGet(pReusedBlock->pDataBlock, 0));
  ASSERT_NE(pCol, nullptr);
  char *pOriginalData = pCol->pData;
  pCol->pData = reinterpret_cast<char *>(pNode);
  EXPECT_FALSE(extWinTestBlockNodeInvariantHolds(pTargetBlocks, pNode, pNode->dl_prev_, pReusedBlock, pReusedIdx, 0,
                                                 &overlapCol));
  EXPECT_EQ(overlapCol, 0);
  pCol->pData = pOriginalData;

  pNode = tdListPopHead(pTargetBlocks);
  ASSERT_NE(pNode, nullptr);
  blockDataDestroy(*(SSDataBlock **)pNode->data);
  taosArrayDestroy(*(SArray **)((SSDataBlock **)pNode->data + 1));
  taosMemoryFree(pNode);
  tdListFree(pFreeBlocks);
  tdListFree(pTargetBlocks);
}

TEST(execUtilTest, externalWindowReusableBlockRejectsEmptyFreeListAsInternalError) {
  SList *pFreeBlocks = tdListNew(POINTER_BYTES * 2);
  SList *pTargetBlocks = tdListNew(POINTER_BYTES * 2);
  ASSERT_NE(pFreeBlocks, nullptr);
  ASSERT_NE(pTargetBlocks, nullptr);

  SSDataBlock *pBlock = nullptr;
  SArray      *pIdx = nullptr;
  EXPECT_EQ(extWinTestTakeReusableBlock(pFreeBlocks, pTargetBlocks, 1, &pBlock, &pIdx),
            TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  EXPECT_EQ(pBlock, nullptr);
  EXPECT_EQ(pIdx, nullptr);
  EXPECT_EQ(listNEles(pFreeBlocks), 0);
  EXPECT_EQ(listNEles(pTargetBlocks), 0);

  tdListFree(pFreeBlocks);
  tdListFree(pTargetBlocks);
}

static void checkExternalWindowOutputAliasLifecycle(bool reset) {
  SList *pFreeBlocks = tdListNew(POINTER_BYTES * 2);
  SList *pOutputBlocks = tdListNew(POINTER_BYTES * 2);
  ASSERT_NE(pFreeBlocks, nullptr);
  ASSERT_NE(pOutputBlocks, nullptr);

  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  SArray *pIdx = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(pIdx, nullptr);

  void *outputBlock[2] = {pBlock, pIdx};
  ASSERT_EQ(tdListAppend(pOutputBlocks, outputBlock), TSDB_CODE_SUCCESS);

  bool aliasEstablished = false;
  bool aliasCleared = false;
  EXPECT_EQ(extWinTestOutputAliasLifecycle(pOutputBlocks, pFreeBlocks, reset, &aliasEstablished, &aliasCleared),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(aliasEstablished);
  EXPECT_TRUE(aliasCleared);
  EXPECT_EQ(listNEles(pOutputBlocks), 0);
  EXPECT_EQ(listNEles(pFreeBlocks), 1);

  SListNode *pNode = tdListPopHead(pFreeBlocks);
  ASSERT_NE(pNode, nullptr);
  blockDataDestroy(*(SSDataBlock **)pNode->data);
  taosArrayDestroy(*(SArray **)((SSDataBlock **)pNode->data + 1));
  taosMemoryFree(pNode);

  tdListFree(pOutputBlocks);
  tdListFree(pFreeBlocks);
}

TEST(execUtilTest, externalWindowNextClearsBorrowedIndexBeforeRecycle) {
  checkExternalWindowOutputAliasLifecycle(false);
}

TEST(execUtilTest, externalWindowResetClearsBorrowedIndexBeforeRecycle) {
  checkExternalWindowOutputAliasLifecycle(true);
}

TEST(execUtilTest, externalWindowLastClosedRejectsMissingIndexArray) {
  SList *pOutputBlocks = tdListNew(POINTER_BYTES * 2);
  ASSERT_NE(pOutputBlocks, nullptr);

  void *outputBlock[2] = {nullptr, nullptr};
  ASSERT_EQ(tdListAppend(pOutputBlocks, outputBlock), TSDB_CODE_SUCCESS);

  bool closed = true;
  EXPECT_EQ(extWinTestLastWinClosed(pOutputBlocks, &closed), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  EXPECT_FALSE(closed);

  tdListFree(pOutputBlocks);
}
