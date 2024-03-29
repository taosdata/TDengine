#include <gtest/gtest.h>

#include "streamBackendRocksdb.h"
#include "tstream.h"
#include "tstreamUpdate.h"
#include "ttime.h"

using namespace std;
#define MAX_NUM_SCALABLE_BF 100000

class StreamStateEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    streamMetaInit();
    backend = streamBackendInit(path, 0, 0);
  }
  virtual void TearDown() {
    streamMetaCleanup();
    // indexClose(index);
  }

  const char *path = TD_TMP_DIR_PATH "stream";
  void       *backend;
};

bool equalSBF(SScalableBf *left, SScalableBf *right) {
  if (left->growth != right->growth) return false;
  if (left->numBits != right->numBits) return false;
  int lsize = taosArrayGetSize(left->bfArray);
  int rsize = taosArrayGetSize(right->bfArray);
  if (lsize != rsize) return false;
  for (int32_t i = 0; i < lsize; i++) {
    SBloomFilter *pLeftBF = (SBloomFilter *)taosArrayGetP(left->bfArray, i);
    SBloomFilter *pRightBF = (SBloomFilter *)taosArrayGetP(right->bfArray, i);
    if (pLeftBF->errorRate != pRightBF->errorRate) return false;
    if (pLeftBF->expectedEntries != pRightBF->expectedEntries) return false;
    if (pLeftBF->hashFn1 != pRightBF->hashFn1) return false;
    if (pLeftBF->hashFn2 != pRightBF->hashFn2) return false;
    if (pLeftBF->hashFunctions != pRightBF->hashFunctions) return false;
    if (pLeftBF->numBits != pRightBF->numBits) return false;
    if (pLeftBF->numUnits != pRightBF->numUnits) return false;
    if (pLeftBF->size != pRightBF->size) return false;
    uint64_t *leftUint = (uint64_t *)pLeftBF->buffer;
    uint64_t *rightUint = (uint64_t *)pRightBF->buffer;
    for (int32_t j = 0; j < pLeftBF->numUnits; j++) {
      if (leftUint[j] != rightUint[j]) return false;
    }
  }
  return true;
}

TEST(TD_STREAM_UPDATE_TEST, update) {
  // const int64_t interval = 20 * 1000;
  // const int64_t watermark = 10 * 60 * 1000;
  // SUpdateInfo  *pSU = updateInfoInit(interval, TSDB_TIME_PRECISION_MILLI, watermark);
  // GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU, 1, 0), false);
  // GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU, 1, -1), true);

  // for (int i = 0; i < 1024; i++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU, i, 1), false);
  // }
  // for (int i = 0; i < 1024; i++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU, i, 1), true);
  // }

  // for (int i = 0; i < 1024; i++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU, i, 2), false);
  // }
  // for (int i = 0; i < 1024; i++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU, i, 2), true);
  // }

  // for (int i = 0; i < 1024; i++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU, i, 1), true);
  // }

  // TSKEY uid = 0;
  // for (int i = 3; i < 1024; i++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU, uid, i), false);
  // }
  // GTEST_ASSERT_EQ(*(TSKEY *)taosHashGet(pSU->pMap, &uid, sizeof(uint64_t)), 1023);

  // for (int i = 3; i < 1024; i++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU, uid, i), true);
  // }
  // GTEST_ASSERT_EQ(*(TSKEY *)taosHashGet(pSU->pMap, &uid, sizeof(uint64_t)), 1023);

  // SUpdateInfo *pSU1 = updateInfoInit(interval, TSDB_TIME_PRECISION_MILLI, watermark);
  // for (int i = 1; i <= watermark / interval; i++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU1, 1, i * interval + 5), false);
  //   GTEST_ASSERT_EQ(pSU1->minTS, interval);
  //   GTEST_ASSERT_EQ(pSU1->numSBFs, watermark / interval);
  // }
  // for (int i = 0; i < pSU1->numSBFs; i++) {
  //   SScalableBf  *pSBF = (SScalableBf *)taosArrayGetP(pSU1->pTsSBFs, i);
  //   SBloomFilter *pBF = (SBloomFilter *)taosArrayGetP(pSBF->bfArray, 0);
  //   GTEST_ASSERT_EQ(pBF->size, 1);
  // }

  // for (int i = watermark / interval + 1, j = 2; i <= watermark / interval + 10; i++, j++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU1, 1, i * interval + 5), false);
  //   GTEST_ASSERT_EQ(pSU1->minTS, interval * j);
  //   GTEST_ASSERT_EQ(pSU1->numSBFs, watermark / interval);
  //   SScalableBf  *pSBF = (SScalableBf *)taosArrayGetP(pSU1->pTsSBFs, pSU1->numSBFs - 1);
  //   SBloomFilter *pBF = (SBloomFilter *)taosArrayGetP(pSBF->bfArray, 0);
  //   GTEST_ASSERT_EQ(pBF->size, 1);
  // }

  // for (int i = watermark / interval * 100, j = 0; j < 10; i += (watermark / interval * 2), j++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU1, 1, i * interval + 5), false);
  //   GTEST_ASSERT_EQ(pSU1->minTS, (i - (pSU1->numSBFs - 1)) * interval);
  //   GTEST_ASSERT_EQ(pSU1->numSBFs, watermark / interval);
  // }

  // SUpdateInfo *pSU2 = updateInfoInit(interval, TSDB_TIME_PRECISION_MILLI, watermark);
  // GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU2, 1, 1 * interval + 5), false);
  // GTEST_ASSERT_EQ(pSU2->minTS, interval);
  // for (int i = watermark / interval * 100, j = 0; j < 10; i += (watermark / interval * 10), j++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU2, 1, i * interval + 5), false);
  //   GTEST_ASSERT_EQ(pSU2->minTS, (i - (pSU2->numSBFs - 1)) * interval);
  //   GTEST_ASSERT_EQ(pSU2->numSBFs, watermark / interval);
  //   TSKEY uid2 = 1;
  //   GTEST_ASSERT_EQ(*(TSKEY *)taosHashGet(pSU2->pMap, &uid2, sizeof(uint64_t)), i * interval + 5);
  // }

  // SUpdateInfo *pSU3 = updateInfoInit(interval, TSDB_TIME_PRECISION_MILLI, watermark);
  // for (int j = 1; j < 100; j++) {
  //   for (int i = 0; i < pSU3->numSBFs; i++) {
  //     GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU3, i, i * interval + 5 * j), false);
  //     GTEST_ASSERT_EQ(pSU3->minTS, 0);
  //     GTEST_ASSERT_EQ(pSU3->numSBFs, watermark / interval);
  //     uint64_t uid3 = i;
  //     GTEST_ASSERT_EQ(*(TSKEY *)taosHashGet(pSU3->pMap, &uid3, sizeof(uint64_t)), i * interval + 5 * j);
  //     SScalableBf  *pSBF = (SScalableBf *)taosArrayGetP(pSU3->pTsSBFs, i);
  //     SBloomFilter *pBF = (SBloomFilter *)taosArrayGetP(pSBF->bfArray, 0);
  //     GTEST_ASSERT_EQ(pBF->size, j);
  //   }
  // }

  // SUpdateInfo *pSU4 = updateInfoInit(-1, TSDB_TIME_PRECISION_MILLI, -1);
  // GTEST_ASSERT_EQ(pSU4->watermark, pSU4->interval);
  // GTEST_ASSERT_EQ(pSU4->interval, MILLISECOND_PER_MINUTE);

  // SUpdateInfo *pSU5 = updateInfoInit(0, TSDB_TIME_PRECISION_MILLI, 0);
  // GTEST_ASSERT_EQ(pSU5->watermark, pSU4->interval);
  // GTEST_ASSERT_EQ(pSU5->interval, MILLISECOND_PER_MINUTE);

  // SUpdateInfo *pSU7 = updateInfoInit(interval, TSDB_TIME_PRECISION_MILLI, watermark);
  // updateInfoAddCloseWindowSBF(pSU7);
  // for (int64_t i = 1; i < 2048000; i++) {
  //   GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU7, i, i), false);
  // }
  // GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU7, 100, 1), true);
  // GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU7, 110, 10), true);
  // GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU7, 200, 20), true);

  // int32_t bufLen = updateInfoSerialize(NULL, 0, pSU7);
  // void   *buf = taosMemoryCalloc(1, bufLen);
  // int32_t resSize = updateInfoSerialize(buf, bufLen, pSU7);

  // SUpdateInfo *pSU6 = taosMemoryCalloc(1, sizeof(SUpdateInfo));
  // int32_t      desSize = updateInfoDeserialize(buf, bufLen, pSU6);
  // GTEST_ASSERT_EQ(desSize, 0);

  // GTEST_ASSERT_EQ(pSU7->interval, pSU6->interval);
  // GTEST_ASSERT_EQ(pSU7->maxDataVersion, pSU6->maxVersion);
  // GTEST_ASSERT_EQ(pSU7->minTS, pSU6->minTS);
  // GTEST_ASSERT_EQ(pSU7->numBuckets, pSU6->numBuckets);
  // GTEST_ASSERT_EQ(pSU7->numSBFs, pSU6->numSBFs);
  // GTEST_ASSERT_EQ(pSU7->scanGroupId, pSU6->scanGroupId);
  // GTEST_ASSERT_EQ(pSU7->scanWindow.ekey, pSU6->scanWindow.ekey);
  // GTEST_ASSERT_EQ(pSU7->scanWindow.skey, pSU6->scanWindow.skey);
  // GTEST_ASSERT_EQ(pSU7->watermark, pSU6->watermark);
  // GTEST_ASSERT_EQ(equalSBF(pSU7->pCloseWinSBF, pSU6->pCloseWinSBF), true);

  // int32_t mapSize = taosHashGetSize(pSU7->pMap);
  // GTEST_ASSERT_EQ(mapSize, taosHashGetSize(pSU6->pMap));
  // void  *pIte = NULL;
  // size_t keyLen = 0;
  // while ((pIte = taosHashIterate(pSU7->pMap, pIte)) != NULL) {
  //   void *key = taosHashGetKey(pIte, &keyLen);
  //   void *value6 = taosHashGet(pSU6->pMap, key, keyLen);
  //   GTEST_ASSERT_EQ(*(TSKEY *)pIte, *(TSKEY *)value6);
  // }

  // int32_t buSize = taosArrayGetSize(pSU7->pTsBuckets);
  // GTEST_ASSERT_EQ(buSize, taosArrayGetSize(pSU6->pTsBuckets));
  // for (int32_t i = 0; i < buSize; i++) {
  //   TSKEY ts1 = *(TSKEY *)taosArrayGet(pSU7->pTsBuckets, i);
  //   TSKEY ts2 = *(TSKEY *)taosArrayGet(pSU6->pTsBuckets, i);
  //   GTEST_ASSERT_EQ(ts1, ts2);
  // }
  // int32_t lSize = taosArrayGetSize(pSU7->pTsSBFs);
  // int32_t rSize = taosArrayGetSize(pSU6->pTsSBFs);
  // GTEST_ASSERT_EQ(lSize, rSize);
  // for (int32_t i = 0; i < lSize; i++) {
  //   SScalableBf *pLeftSBF = (SScalableBf *)taosArrayGetP(pSU7->pTsSBFs, i);
  //   SScalableBf *pRightSBF = (SScalableBf *)taosArrayGetP(pSU6->pTsSBFs, i);
  //   GTEST_ASSERT_EQ(equalSBF(pLeftSBF, pRightSBF), true);
  // }

  // updateInfoDestroy(pSU);
  // updateInfoDestroy(pSU1);
  // updateInfoDestroy(pSU2);
  // updateInfoDestroy(pSU3);
  // updateInfoDestroy(pSU4);
  // updateInfoDestroy(pSU5);
  // updateInfoDestroy(pSU6);
  // updateInfoDestroy(pSU7);
}
// TEST()
TEST(StreamStateEnv, test1) {}
// int main(int argc, char *argv[]) {
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }