#include "gtest/gtest.h"

#include "executil.h"
#include "executorInt.h"
#include "functionMgt.h"
#include "libs/new-stream/dataSink.h"
#include "osMemory.h"
#include "querynodes.h"
#include "tdatablock.h"

extern "C" SHashObj *gStreamGrpTableHash;
extern "C" char      tsStableTagFilterCache;
extern "C" int32_t getTableList(void *pVnode, SScanPhysiNode *pScanNode, SNode *pTagCond, SNode *pTagIndexCond,
                                STableListInfo *pListInfo, uint8_t *digest, const char *idstr, SStorageAPI *pStorageAPI,
                                void *pStreamInfo);

namespace {

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

int32_t mockGetTableTags(void *pVnode, uint64_t suid, SArray *uidList) {
  (void)suid;
  (void)uidList;
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
      getTableList(&mock, &scan, pTagCond, nullptr, &tableListInfo, nullptr, "stable-cache-warmup", &api, &streamInfo),
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
                   &streamInfo),
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
                         &streamInfo),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosArrayGetSize(tableListInfo.pTableList), 0);
  EXPECT_EQ(mock.getCacheCalls, 1);
  EXPECT_EQ(mock.warmupCalls, 0);
  EXPECT_EQ(mock.getTableTagsCalls, 1);

  nodesDestroyNode(pTagCond);
  taosArrayDestroy(tableListInfo.pTableList);
  tsStableTagFilterCache = oldStableTagFilterCache;
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
