#include <gtest/gtest.h>
#include <cassert>
#include <iostream>

#include "qResultbuf.h"
#include "taos.h"
#include "tsdb.h"

namespace {
// simple test
void simpleTest() {
  SDiskbasedResultBuf* pResultBuf = NULL;
  int32_t ret = createDiskbasedResultBuffer(&pResultBuf, 1024, 4096, NULL);
  
  int32_t pageId = 0;
  int32_t groupId = 0;
  
  tFilePage* pBufPage = getNewDataBuf(pResultBuf, groupId, &pageId);
  ASSERT_TRUE(pBufPage != NULL);
  
  ASSERT_EQ(getResBufSize(pResultBuf), 1024);
  
  SIDList list = getDataBufPagesIdList(pResultBuf, groupId);
  ASSERT_EQ(taosArrayGetSize(list), 1);
  ASSERT_EQ(getNumOfResultBufGroupId(pResultBuf), 1);

  releaseResBufPage(pResultBuf, pBufPage);

  tFilePage* pBufPage1 = getNewDataBuf(pResultBuf, groupId, &pageId);

  tFilePage* t = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t == pBufPage1);

  tFilePage* pBufPage2 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t1 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t1 == pBufPage2);

  tFilePage* pBufPage3 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t2 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t2 == pBufPage3);

  tFilePage* pBufPage4 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t3 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t3 == pBufPage4);

  tFilePage* pBufPage5 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t4 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t4 == pBufPage5);

  destroyResultBuf(pResultBuf);
}

void writeDownTest() {
  SDiskbasedResultBuf* pResultBuf = NULL;
  int32_t ret = createDiskbasedResultBuffer(&pResultBuf, 1024, 4*1024, NULL);

  int32_t pageId = 0;
  int32_t writePageId = 0;
  int32_t groupId = 0;
  int32_t nx = 12345;

  tFilePage* pBufPage = getNewDataBuf(pResultBuf, groupId, &pageId);
  ASSERT_TRUE(pBufPage != NULL);

  *(int32_t*)(pBufPage->data) = nx;
  writePageId = pageId;
  releaseResBufPage(pResultBuf, pBufPage);

  tFilePage* pBufPage1 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t1 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t1 == pBufPage1);
  ASSERT_TRUE(pageId == 1);

  tFilePage* pBufPage2 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t2 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t2 == pBufPage2);
  ASSERT_TRUE(pageId == 2);

  tFilePage* pBufPage3 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t3 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t3 == pBufPage3);
  ASSERT_TRUE(pageId == 3);

  tFilePage* pBufPage4 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t4 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t4 == pBufPage4);
  ASSERT_TRUE(pageId == 4);
  releaseResBufPage(pResultBuf, t4);

  // flush the written page to disk, and read it out again
  tFilePage* pBufPagex = getResBufPage(pResultBuf, writePageId);
  ASSERT_EQ(*(int32_t*)pBufPagex->data, nx);

  SArray* pa = getDataBufPagesIdList(pResultBuf, groupId);
  ASSERT_EQ(taosArrayGetSize(pa), 5);

  destroyResultBuf(pResultBuf);
}

void recyclePageTest() {
  SDiskbasedResultBuf* pResultBuf = NULL;
  int32_t ret = createDiskbasedResultBuffer(&pResultBuf, 1024, 4*1024, NULL);

  int32_t pageId = 0;
  int32_t writePageId = 0;
  int32_t groupId = 0;
  int32_t nx = 12345;

  tFilePage* pBufPage = getNewDataBuf(pResultBuf, groupId, &pageId);
  ASSERT_TRUE(pBufPage != NULL);
  releaseResBufPage(pResultBuf, pBufPage);

  tFilePage* pBufPage1 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t1 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t1 == pBufPage1);
  ASSERT_TRUE(pageId == 1);

  tFilePage* pBufPage2 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t2 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t2 == pBufPage2);
  ASSERT_TRUE(pageId == 2);

  tFilePage* pBufPage3 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t3 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t3 == pBufPage3);
  ASSERT_TRUE(pageId == 3);

  tFilePage* pBufPage4 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t4 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t4 == pBufPage4);
  ASSERT_TRUE(pageId == 4);
  releaseResBufPage(pResultBuf, t4);

  tFilePage* pBufPage5 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t5 = getResBufPage(pResultBuf, pageId);
  ASSERT_TRUE(t5 == pBufPage5);
  ASSERT_TRUE(pageId == 5);

  // flush the written page to disk, and read it out again
  tFilePage* pBufPagex = getResBufPage(pResultBuf, writePageId);
  *(int32_t*)(pBufPagex->data) = nx;
  writePageId = pageId;   // update the data
  releaseResBufPage(pResultBuf, pBufPagex);

  tFilePage* pBufPagex1 = getResBufPage(pResultBuf, 1);

  SArray* pa = getDataBufPagesIdList(pResultBuf, groupId);
  ASSERT_EQ(taosArrayGetSize(pa), 6);

  destroyResultBuf(pResultBuf);
}
} // namespace


TEST(testCase, resultBufferTest) {
  srand(time(NULL));
  simpleTest();
  writeDownTest();
  recyclePageTest();
}
