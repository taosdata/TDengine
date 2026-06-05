#include "gtest/gtest.h"

#include "executil.h"
#include "libs/new-stream/dataSink.h"
#include "osMemory.h"

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
