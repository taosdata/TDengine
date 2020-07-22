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
  int32_t ret = createDiskbasedResultBuffer(&pResultBuf, 1000, 64, 1024, 4, NULL);
  
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
  assert(t == pBufPage1);

  tFilePage* pBufPage2 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t1 = getResBufPage(pResultBuf, pageId);
  assert(t1 == pBufPage2);

  tFilePage* pBufPage3 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t2 = getResBufPage(pResultBuf, pageId);
  assert(t2 == pBufPage3);

  tFilePage* pBufPage4 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t3 = getResBufPage(pResultBuf, pageId);
  assert(t3 == pBufPage4);

  tFilePage* pBufPage5 = getNewDataBuf(pResultBuf, groupId, &pageId);
  tFilePage* t4 = getResBufPage(pResultBuf, pageId);
  assert(t4 == pBufPage5);

  destroyResultBuf(pResultBuf, NULL);
}
} // namespace

TEST(testCase, resultBufferTest) {
  simpleTest();
}
