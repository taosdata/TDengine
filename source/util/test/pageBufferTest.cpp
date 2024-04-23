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

int saveDataToPage(SFilePage* pPg, const char* data, uint32_t len) {
  memcpy(pPg->data + pPg->num, data, len);
  pPg->num += len;
  setBufPageDirty(pPg, true);
  return 0;
}

bool checkBufVarData(SFilePage* pPg, const char* varData, uint32_t varLen) {
  const char* start = pPg->data + sizeof(SFilePage);
  for (uint32_t i = 0; i < (pPg->num - sizeof(SFilePage)) / varLen; ++i) {
    if (0 != strncmp(start + 6 * i + 3, varData, varLen - 3)) {
      using namespace std;
      cout << "pos: " << sizeof(SFilePage) + 6 * i + 3 << " should be " << varData << " but is: " << start + 6 * i + 3
           << endl;
      return false;
    }
  }
  return true;
}

// SPageInfo.pData: |  sizeof(void*) 8 bytes | sizeof(SFilePage) 4 bytes| 4096 bytes |
//                                           ^
//                                           |
//                                       SFilePage: flush to disk from here
void testFlushAndReadBackBuffer() {
  SDiskbasedBuf* pBuf = NULL;
  uint32_t       totalLen = 4096;
  auto           code = createDiskbasedBuf(&pBuf, totalLen, totalLen * 2, "1", TD_TMP_DIR_PATH);
  int32_t        pageId = -1;
  auto*          pPg = (SFilePage*)getNewBufPage(pBuf, &pageId);
  ASSERT_TRUE(pPg != nullptr);
  pPg->num = sizeof(SFilePage);

  // save data into page
  uint32_t len = 6;  // sizeof(SFilePage) + 6 * 682 = 4096
  // nullbitmap(1) + len(2) + AA\0(3)
  char* rowData = (char*)taosMemoryCalloc(1, len);
  *(uint16_t*)(rowData + 2) = (uint16_t)2;
  rowData[3] = 'A';
  rowData[4] = 'A';

  while (pPg->num + len <= getBufPageSize(pBuf)) {
    saveDataToPage(pPg, rowData, len);
  }
  ASSERT_EQ(pPg->num, totalLen);
  ASSERT_TRUE(checkBufVarData(pPg, rowData + 3, len));
  releaseBufPage(pBuf, pPg);

  // flush to disk
  int32_t newPgId = -1;
  pPg = (SFilePage*)getNewBufPage(pBuf, &newPgId);
  releaseBufPage(pBuf, pPg);
  pPg = (SFilePage*)getNewBufPage(pBuf, &newPgId);
  releaseBufPage(pBuf, pPg);

  // reload it from disk
  pPg = (SFilePage*)getBufPage(pBuf, pageId);
  ASSERT_TRUE(checkBufVarData(pPg, rowData + 3, len));
  destroyDiskbasedBuf(pBuf);
  taosMemoryFree(rowData);
}

}  // namespace

TEST(testCase, resultBufferTest) {
  taosSeedRand(taosGetTimestampSec());
  simpleTest();
  writeDownTest();
  recyclePageTest();
  testFlushAndReadBackBuffer();
}

#pragma GCC diagnostic pop
