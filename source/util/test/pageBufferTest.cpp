#include <gtest/gtest.h>
#include <cassert>
#include <iostream>

#include "taos.h"
#include "tpagedbuf.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"

namespace {
// simple test
void simpleTest() {
  SDiskbasedBuf* pBuf = NULL;
  int32_t        ret = createDiskbasedBuf(&pBuf, 1024, 4096, "", TD_TMP_DIR_PATH);

  int32_t pageId = 0;
  int32_t groupId = 0;

  SFilePage* pBufPage = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  ASSERT_TRUE(pBufPage != NULL);

  ASSERT_EQ(getTotalBufSize(pBuf), 1024);

  SArray* list = getDataBufPagesIdList(pBuf);
  ASSERT_EQ(taosArrayGetSize(list), 1);
  // ASSERT_EQ(getNumOfBufGroupId(pBuf), 1);

  releaseBufPage(pBuf, pBufPage);

  SFilePage* pBufPage1 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));

  SFilePage* t = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t == pBufPage1);

  SFilePage* pBufPage2 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t1 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t1 == pBufPage2);

  SFilePage* pBufPage3 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t2 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t2 == pBufPage3);

  SFilePage* pBufPage4 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t3 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t3 == pBufPage4);

  releaseBufPage(pBuf, pBufPage2);

  SFilePage* pBufPage5 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t4 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t4 == pBufPage5);

  destroyDiskbasedBuf(pBuf);
}

void writeDownTest() {
  SDiskbasedBuf* pBuf = NULL;
  int32_t        ret = createDiskbasedBuf(&pBuf, 1024, 4 * 1024, "1", TD_TMP_DIR_PATH);

  int32_t pageId = 0;
  int32_t writePageId = 0;
  int32_t groupId = 0;
  int32_t nx = 12345;

  SFilePage* pBufPage = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  ASSERT_TRUE(pBufPage != NULL);

  *(int32_t*)(pBufPage->data) = nx;
  writePageId = pageId;

  setBufPageDirty(pBufPage, true);
  releaseBufPage(pBuf, pBufPage);

  SFilePage* pBufPage1 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t1 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t1 == pBufPage1);
  ASSERT_TRUE(pageId == 1);

  SFilePage* pBufPage2 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t2 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t2 == pBufPage2);
  ASSERT_TRUE(pageId == 2);

  SFilePage* pBufPage3 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t3 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t3 == pBufPage3);
  ASSERT_TRUE(pageId == 3);

  SFilePage* pBufPage4 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t4 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t4 == pBufPage4);
  ASSERT_TRUE(pageId == 4);
  releaseBufPage(pBuf, t4);

  // flush the written page to disk, and read it out again
  SFilePage* pBufPagex = static_cast<SFilePage*>(getBufPage(pBuf, writePageId));
  ASSERT_EQ(*(int32_t*)pBufPagex->data, nx);

  SArray* pa = getDataBufPagesIdList(pBuf);
  ASSERT_EQ(taosArrayGetSize(pa), 5);

  destroyDiskbasedBuf(pBuf);
}

void recyclePageTest() {
  SDiskbasedBuf* pBuf = NULL;
  int32_t        ret = createDiskbasedBuf(&pBuf, 1024, 4 * 1024, "1", TD_TMP_DIR_PATH);

  int32_t pageId = 0;
  int32_t writePageId = 0;
  int32_t groupId = 0;
  int32_t nx = 12345;

  SFilePage* pBufPage = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  ASSERT_TRUE(pBufPage != NULL);
  releaseBufPage(pBuf, pBufPage);

  SFilePage* pBufPage1 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t1 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t1 == pBufPage1);
  ASSERT_TRUE(pageId == 1);

  SFilePage* pBufPage2 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t2 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t2 == pBufPage2);
  ASSERT_TRUE(pageId == 2);

  SFilePage* pBufPage3 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t3 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t3 == pBufPage3);
  ASSERT_TRUE(pageId == 3);

  SFilePage* pBufPage4 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t4 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t4 == pBufPage4);
  ASSERT_TRUE(pageId == 4);
  releaseBufPage(pBuf, t4);

  SFilePage* pBufPage5 = static_cast<SFilePage*>(getNewBufPage(pBuf, &pageId));
  SFilePage* t5 = static_cast<SFilePage*>(getBufPage(pBuf, pageId));
  ASSERT_TRUE(t5 == pBufPage5);
  ASSERT_TRUE(pageId == 5);
  releaseBufPage(pBuf, t5);

  // flush the written page to disk, and read it out again
  SFilePage* pBufPagex = static_cast<SFilePage*>(getBufPage(pBuf, writePageId));
  *(int32_t*)(pBufPagex->data) = nx;
  writePageId = pageId;  // update the data
  releaseBufPage(pBuf, pBufPagex);

  SFilePage* pBufPagex1 = static_cast<SFilePage*>(getBufPage(pBuf, 1));

  SArray* pa = getDataBufPagesIdList(pBuf);
  ASSERT_EQ(taosArrayGetSize(pa), 6);

  destroyDiskbasedBuf(pBuf);
}
}  // namespace

TEST(testCase, resultBufferTest) {
  taosSeedRand(taosGetTimestampSec());
  simpleTest();
  writeDownTest();
  recyclePageTest();
}

#pragma GCC diagnostic pop