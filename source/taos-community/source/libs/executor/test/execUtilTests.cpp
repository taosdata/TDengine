#include "gtest/gtest.h"

#include "executil.h"

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
