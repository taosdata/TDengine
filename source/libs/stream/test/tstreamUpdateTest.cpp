#include <gtest/gtest.h>

#include "tstreamUpdate.h"
#include "ttime.h"

using namespace std;

TEST(TD_STREAM_UPDATE_TEST, update) {
  int64_t interval = 20 * 1000;
  int64_t watermark = 10 * 60 * 1000;
  SUpdateInfo *pSU = updateInfoInit(interval, TSDB_TIME_PRECISION_MILLI, watermark);
  GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU,1, 0), true);
  GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU,1, -1), true);

  for(int i=0; i < 1024; i++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU,i, 1), false);
  }
  for(int i=0; i < 1024; i++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU,i, 1), true);
  }

  for(int i=0; i < 1024; i++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU,i, 2), false);
  }
  for(int i=0; i < 1024; i++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU,i, 2), true);
  }

  for(int i=0; i < 1024; i++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU,i, 1), true);
  }

  for(int i=3; i < 1024; i++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU,0, i), false);
  }
  GTEST_ASSERT_EQ(*(int64_t*)taosArrayGet(pSU->pTsBuckets,0), 1023);

  for(int i=3; i < 1024; i++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU,0, i), true);
  }
  GTEST_ASSERT_EQ(*(int64_t*)taosArrayGet(pSU->pTsBuckets,0), 1023);

  SUpdateInfo *pSU1 = updateInfoInit(interval, TSDB_TIME_PRECISION_MILLI, watermark);
  for(int i=1; i <= watermark / interval; i++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU1, 1, i * interval + 5), false);
    GTEST_ASSERT_EQ(pSU1->minTS, interval);
    GTEST_ASSERT_EQ(pSU1->numSBFs, watermark / interval);
  }
  for(int i=0; i < pSU1->numSBFs; i++) {
    SScalableBf *pSBF = (SScalableBf *)taosArrayGetP(pSU1->pTsSBFs, i);
    SBloomFilter *pBF = (SBloomFilter *)taosArrayGetP(pSBF->bfArray, 0);
    GTEST_ASSERT_EQ(pBF->size, 1);
  }

  for(int i= watermark / interval + 1, j = 2 ; i <= watermark / interval + 10; i++,j++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU1, 1, i * interval + 5), false);
    GTEST_ASSERT_EQ(pSU1->minTS, interval*j);
    GTEST_ASSERT_EQ(pSU1->numSBFs, watermark / interval);
    SScalableBf *pSBF = (SScalableBf *)taosArrayGetP(pSU1->pTsSBFs, pSU1->numSBFs - 1);
    SBloomFilter *pBF = (SBloomFilter *)taosArrayGetP(pSBF->bfArray, 0);
    GTEST_ASSERT_EQ(pBF->size, 1);
  }
  
  for(int i= watermark / interval * 100, j = 0; j < 10; i+= (watermark / interval * 2), j++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU1, 1, i * interval + 5), false);
    GTEST_ASSERT_EQ(pSU1->minTS, (i-(pSU1->numSBFs-1))*interval);
    GTEST_ASSERT_EQ(pSU1->numSBFs, watermark / interval);
  }

  SUpdateInfo *pSU2 = updateInfoInit(interval, TSDB_TIME_PRECISION_MILLI, watermark);
  GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU2, 1, 1 * interval + 5), false);
  GTEST_ASSERT_EQ(pSU2->minTS, interval);
  for(int i= watermark / interval * 100, j = 0; j < 10; i+= (watermark / interval * 10), j++) {
    GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU2, 1, i * interval + 5), false);
    GTEST_ASSERT_EQ(pSU2->minTS, (i-(pSU2->numSBFs-1))*interval);
    GTEST_ASSERT_EQ(pSU2->numSBFs, watermark / interval);
    GTEST_ASSERT_EQ(*(int64_t*)taosArrayGet(pSU2->pTsBuckets,1), i * interval + 5);
  }
  
  SUpdateInfo *pSU3 = updateInfoInit(interval, TSDB_TIME_PRECISION_MILLI, watermark);
  for(int j = 1; j < 100; j++) {
    for(int i = 0; i < pSU3->numSBFs; i++) {
      GTEST_ASSERT_EQ(updateInfoIsUpdated(pSU3, i, i * interval + 5 * j), false);
      GTEST_ASSERT_EQ(pSU3->minTS, 0);
      GTEST_ASSERT_EQ(pSU3->numSBFs, watermark / interval);
      GTEST_ASSERT_EQ(*(int64_t*)taosArrayGet(pSU3->pTsBuckets, i), i * interval + 5 * j);
      SScalableBf *pSBF = (SScalableBf *)taosArrayGetP(pSU3->pTsSBFs, i);
      SBloomFilter *pBF = (SBloomFilter *)taosArrayGetP(pSBF->bfArray, 0);
      GTEST_ASSERT_EQ(pBF->size, j);
    }
  }

  SUpdateInfo *pSU4 = updateInfoInit(-1, TSDB_TIME_PRECISION_MILLI, -1);
  GTEST_ASSERT_EQ(pSU4->watermark, 120 * pSU4->interval);
  GTEST_ASSERT_EQ(pSU4->interval, MILLISECOND_PER_MINUTE);

  SUpdateInfo *pSU5 = updateInfoInit(0, TSDB_TIME_PRECISION_MILLI, 0);
  GTEST_ASSERT_EQ(pSU5->watermark, 120 * pSU4->interval);
  GTEST_ASSERT_EQ(pSU5->interval, MILLISECOND_PER_MINUTE);


  updateInfoDestroy(pSU);
  updateInfoDestroy(pSU1);
  updateInfoDestroy(pSU2);
  updateInfoDestroy(pSU3);
  updateInfoDestroy(pSU4);
  updateInfoDestroy(pSU5);
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}