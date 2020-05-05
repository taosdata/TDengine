#include <gtest/gtest.h>
#include <cassert>
#include <iostream>

#include "taos.h"
#include "qresultBuf.h"
#include "tsdb.h"

namespace {
// simple test
void simpleTest() {
  SDiskbasedResultBuf* pResultBuf = NULL;
  int32_t ret = createDiskbasedResultBuffer(&pResultBuf, 1000, 64, NULL);
  
  int32_t pageId = 0;
  int32_t groupId = 0;
  
  tFilePage* pBufPage = getNewDataBuf(pResultBuf, groupId, &pageId);
  ASSERT_TRUE(pBufPage != NULL);
  
  ASSERT_EQ(getNumOfRowsPerPage(pResultBuf), (16384L - sizeof(int64_t))/64);
  ASSERT_EQ(getResBufSize(pResultBuf), 1000*16384L);
  
  SIDList list = getDataBufPagesIdList(pResultBuf, groupId);
  ASSERT_EQ(list.size, 1);
  
  ASSERT_EQ(getNumOfResultBufGroupId(pResultBuf), 1);
  
  destroyResultBuf(pResultBuf, NULL);
}
} // namespace

TEST(testCase, resultBufferTest) {
  simpleTest();
}
