#include "gtest/gtest.h"

#include "executil.h"
#include "libs/new-stream/dataSink.h"
#include "osMemory.h"

extern "C" SHashObj *gStreamGrpTableHash;

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
