#include <gtest/gtest.h>
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#include "crypt.h"
#include "taos.h"
#include "taoserror.h"
#include "tglobal.h"
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

void testSpillCryptContext() {
  SSpillCryptCtx* pCtx = nullptr;
  ASSERT_EQ(taosCreateSpillCryptCtx(&pCtx, 0), TSDB_CODE_SUCCESS);

  char plain[32] = {0};
  for (int32_t i = 0; i < (int32_t)sizeof(plain); ++i) {
    plain[i] = (char)(i + 1);
  }

  const int32_t lengths[] = {0, 1, 15, 16, 17};
  for (int32_t plainLen : lengths) {
    char*   pEncoded = nullptr;
    int32_t encodedLen = 0;
    if (plainLen == 0) {
      // An empty block is not a legal spill image on either side: the
      // encrypt path rejects it just like the decrypt path always has.
      ASSERT_EQ(taosEncryptSpillBlock(pCtx, plain, plainLen, &pEncoded,
                                      &encodedLen),
                TSDB_CODE_INVALID_PARA);
      continue;
    }
    ASSERT_EQ(
        taosEncryptSpillBlock(pCtx, plain, plainLen, &pEncoded, &encodedLen),
        TSDB_CODE_SUCCESS);
    ASSERT_EQ(encodedLen, (int32_t)sizeof(int32_t) + ENCRYPTED_LEN(plainLen));

    int32_t storedPlainLen = -1;
    memcpy(&storedPlainLen, pEncoded, sizeof(storedPlainLen));
    ASSERT_EQ(storedPlainLen, plainLen);

    std::string encoded(pEncoded, encodedLen);
    char*       pReadBuffer = nullptr;
    ASSERT_EQ(taosPrepareSpillBlockRead(pCtx, encodedLen, &pReadBuffer),
              TSDB_CODE_SUCCESS);
    memcpy(pReadBuffer, encoded.data(), encoded.size());

    char*   pDecoded = nullptr;
    int32_t decodedLen = 0;
    ASSERT_EQ(taosDecryptSpillBlock(pCtx, pReadBuffer, encodedLen, &pDecoded,
                                    &decodedLen),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(decodedLen, plainLen);
    ASSERT_EQ(memcmp(pDecoded, plain, plainLen), 0);
  }

  char* pReadBuffer = nullptr;
  ASSERT_EQ(taosPrepareSpillBlockRead(pCtx, sizeof(int32_t), &pReadBuffer),
            TSDB_CODE_FILE_CORRUPTED);
  ASSERT_EQ(taosPrepareSpillBlockRead(pCtx, sizeof(int32_t) + 1, &pReadBuffer),
            TSDB_CODE_FILE_CORRUPTED);

  int32_t encodedLen = (int32_t)sizeof(int32_t) + 16;
  ASSERT_EQ(taosPrepareSpillBlockRead(pCtx, encodedLen, &pReadBuffer),
            TSDB_CODE_SUCCESS);
  int32_t invalidPlainLen = 17;
  memcpy(pReadBuffer, &invalidPlainLen, sizeof(invalidPlainLen));
  memset(pReadBuffer + sizeof(invalidPlainLen), 0, 16);
  char*   pDecoded = nullptr;
  int32_t decodedLen = 0;
  ASSERT_EQ(taosDecryptSpillBlock(pCtx, pReadBuffer, encodedLen, &pDecoded,
                                  &decodedLen),
            TSDB_CODE_FILE_CORRUPTED);

  // A buffer the context does not own must be refused rather than decrypted
  // in place: growing the context buffer for it would move the ciphertext.
  std::vector<char> outside(encodedLen, 0);
  ASSERT_EQ(taosDecryptSpillBlock(pCtx, outside.data(), encodedLen, &pDecoded,
                                  &decodedLen),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(taosDecryptSpillBlock(pCtx, pReadBuffer + 1, encodedLen, &pDecoded,
                                  &decodedLen),
            TSDB_CODE_INVALID_PARA);

  // taosSpillCryptEncodedLen must reject the same out-of-range inputs as
  // taosEncryptSpillBlock instead of silently wrapping.
  int32_t boundedLen = 0;
  ASSERT_EQ(taosSpillCryptEncodedLen(0, nullptr), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(taosSpillCryptEncodedLen(-1, &boundedLen),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(taosSpillCryptEncodedLen(0, &boundedLen),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(taosSpillCryptEncodedLen(INT32_MAX, &boundedLen),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(taosSpillCryptEncodedLen(INT32_MAX - 14, &boundedLen),
            TSDB_CODE_INVALID_PARA);
  // Largest legal plaintext length: already 16-aligned, so the encoded
  // image is header + plainLen exactly. Pins the boundary against an
  // accidental future off-by-one (e.g. > rewritten as >=).
  ASSERT_EQ(taosSpillCryptEncodedLen(INT32_MAX - 15, &boundedLen),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(boundedLen, INT32_MAX - 11);
  ASSERT_EQ(taosSpillCryptEncodedLen(16, &boundedLen), TSDB_CODE_SUCCESS);
  ASSERT_EQ(boundedLen, (int32_t)sizeof(int32_t) + 16);
  ASSERT_EQ(taosSpillCryptEncodedLen(17, &boundedLen), TSDB_CODE_SUCCESS);
  ASSERT_EQ(boundedLen, (int32_t)sizeof(int32_t) + 32);

  taosDestroySpillCryptCtx(&pCtx);
  ASSERT_EQ(pCtx, nullptr);
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

void testGetBufPageNoLRU() {
  SDiskbasedBuf* pBuf = nullptr;
  ASSERT_EQ(createDiskbasedBuf(&pBuf, 1024, 2 * 1024, "no_lru",
                               TD_TMP_DIR_PATH),
            TSDB_CODE_SUCCESS);

  int32_t pageId1 = -1;
  auto*   pPage1 = (SFilePage*)getNewBufPage(pBuf, &pageId1);
  ASSERT_NE(pPage1, nullptr);
  pPage1->num = 42;
  setBufPageDirty(pPage1, true);
  releaseBufPage(pBuf, pPage1);

  int32_t pageId2 = -1;
  auto*   pPage2 = (SFilePage*)getNewBufPage(pBuf, &pageId2);
  ASSERT_NE(pPage2, nullptr);
  releaseBufPage(pBuf, pPage2);

  pPage1 = (SFilePage*)getBufPageNoLRU(pBuf, pageId1);
  ASSERT_NE(pPage1, nullptr);
  releaseBufPage(pBuf, pPage1);

  int32_t pageId3 = -1;
  auto*   pPage3 = (SFilePage*)getNewBufPage(pBuf, &pageId3);
  ASSERT_NE(pPage3, nullptr);
  releaseBufPage(pBuf, pPage3);

  ASSERT_EQ(getDBufStatis(pBuf).flushPages, 1);
  pPage1 = (SFilePage*)getBufPageNoLRU(pBuf, pageId1);
  ASSERT_NE(pPage1, nullptr);
  ASSERT_EQ(pPage1->num, 42);
  ASSERT_EQ(getDBufStatis(pBuf).loadPages, 1);
  releaseBufPage(pBuf, pPage1);

  destroyDiskbasedBuf(pBuf);
}

void testPinnedPagesSetOutOfMemory() {
  int32_t        savedErrno = terrno;
  SDiskbasedBuf* pBuf = nullptr;
  ASSERT_EQ(createDiskbasedBuf(&pBuf, 1024, 2 * 1024, "pinned",
                               TD_TMP_DIR_PATH),
            TSDB_CODE_SUCCESS);

  int32_t pageId1 = -1;
  auto*   pPage1 = (SFilePage*)getNewBufPage(pBuf, &pageId1);
  ASSERT_NE(pPage1, nullptr);

  int32_t pageId2 = -1;
  auto*   pPage2 = (SFilePage*)getNewBufPage(pBuf, &pageId2);
  ASSERT_NE(pPage2, nullptr);

  terrno = TSDB_CODE_SUCCESS;
  int32_t pageId3 = -1;
  auto*   pPage3 = (SFilePage*)getNewBufPage(pBuf, &pageId3);
  EXPECT_EQ(pPage3, nullptr);
  EXPECT_EQ(terrno, TSDB_CODE_OUT_OF_MEMORY);

  releaseBufPage(pBuf, pPage1);
  releaseBufPage(pBuf, pPage2);
  destroyDiskbasedBuf(pBuf);
  terrno = savedErrno;
}

// Same flush-and-readback as testFlushAndReadBackBuffer but with spill
// encryption enabled. Verifies the encrypt-on-write / decrypt-on-read round
// trip preserves data (padding + plaintext-length bookkeeping), and that the
// on-disk spill file holds ciphertext (no plaintext leak) under real crypto.
void testEncryptedFlushAndReadBackBuffer() {
  EEncryptScope savedScope = tsiEncryptScope;
  tsiEncryptScope = (EEncryptScope)(tsiEncryptScope | DND_CS_QUERY_SPILL);

  // Dedicated temp dir so the spill file on disk is unambiguous to inspect.
  char tmpDir[512] = {0};
  (void)snprintf(tmpDir, sizeof(tmpDir), "%s/qspill_gtest_%d", TD_TMP_DIR_PATH,
                 taosGetPId());
  (void)taosMulMkDir(tmpDir);  // recursive: TD_TMP_DIR_PATH may not exist yet

  SDiskbasedBuf* pBuf = NULL;
  uint32_t       totalLen = 4096;
  auto           code =
      createDiskbasedBuf(&pBuf, totalLen, totalLen * 2, "enc", tmpDir);
  ASSERT_EQ(code, 0);
  int32_t pageId = -1;
  auto*   pPg = (SFilePage*)getNewBufPage(pBuf, &pageId);
  ASSERT_TRUE(pPg != nullptr);
  pPg->num = sizeof(SFilePage);

  // distinctive 3-byte payload so the plaintext page is greppable on disk
  uint32_t len = 6;
  char*    rowData = (char*)taosMemoryCalloc(1, len);
  *(uint16_t*)(rowData + 2) = (uint16_t)2;
  rowData[3] = 'Q';
  rowData[4] = 'Z';
  rowData[5] = '!';

  while (pPg->num + len <= getBufPageSize(pBuf)) {
    saveDataToPage(pPg, rowData, len);
  }
  ASSERT_EQ(pPg->num, totalLen);
  releaseBufPage(pBuf, pPg);

  // force the dirty page to disk (encrypted), then evict so it must be reloaded
  int32_t newPgId = -1;
  pPg = (SFilePage*)getNewBufPage(pBuf, &newPgId);
  releaseBufPage(pBuf, pPg);
  pPg = (SFilePage*)getNewBufPage(pBuf, &newPgId);
  releaseBufPage(pBuf, pPg);

  // Inspect the raw on-disk spill bytes. The marker (a few repeated rows) is
  // present in plaintext; real SM4 must turn it into ciphertext.
  char    diskBuf[16384] = {0};
  int64_t nbytes = dbgReadDiskbasedBufFile(pBuf, diskBuf, sizeof(diskBuf));
  ASSERT_GT(nbytes, 0);  // the page really spilled to disk
  std::string onDisk(diskBuf, (size_t)nbytes);
  std::string marker;
  for (int i = 0; i < 4; ++i) marker.append(rowData, len);
  bool leaked = onDisk.find(marker) != std::string::npos;
#if defined(TD_ENTERPRISE) || defined(TD_ASTRA)
  ASSERT_FALSE(leaked);  // real SM4-CBC on any platform: ciphertext only
#else
  ASSERT_TRUE(leaked);   // community memcpy stub: plaintext present (control)
#endif

  // reload from disk and verify the decrypted data matches
  pPg = (SFilePage*)getBufPage(pBuf, pageId);
  ASSERT_TRUE(pPg != nullptr);
  ASSERT_TRUE(checkBufVarData(pPg, rowData + 3, len));

  destroyDiskbasedBuf(pBuf);
  taosRemoveDir(tmpDir);
  taosMemoryFree(rowData);
  tsiEncryptScope = savedScope;
}

}  // namespace

TEST(testCase, resultBufferTest) {
  taosSeedRand(taosGetTimestampSec());
  simpleTest();
  writeDownTest();
  recyclePageTest();
  testSpillCryptContext();
  testFlushAndReadBackBuffer();
  testGetBufPageNoLRU();
  testPinnedPagesSetOutOfMemory();
  testEncryptedFlushAndReadBackBuffer();
}

#pragma GCC diagnostic pop
