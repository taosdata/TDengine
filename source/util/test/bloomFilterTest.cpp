#include <gtest/gtest.h>

#include "taoserror.h"
#include "tscalablebf.h"

using namespace std;

TEST(TD_UTIL_BLOOMFILTER_TEST, normal_bloomFilter) {
  int64_t ts1 = 1650803518000;

  GTEST_ASSERT_EQ(NULL, tBloomFilterInit(100, 0));
  GTEST_ASSERT_EQ(NULL, tBloomFilterInit(100, 1));
  GTEST_ASSERT_EQ(NULL, tBloomFilterInit(100, -0.1));
  GTEST_ASSERT_EQ(NULL, tBloomFilterInit(0, 0.01));

  SBloomFilter *pBF1 = tBloomFilterInit(100, 0.005);
  GTEST_ASSERT_EQ(pBF1->numBits, 1152);
  GTEST_ASSERT_EQ(pBF1->numUnits, 1152 / 64);
  int64_t count = 0;
  for (int64_t i = 0; count < 100; i++) {
    int64_t ts = i + ts1;
    if (tBloomFilterPut(pBF1, &ts, sizeof(int64_t)) == TSDB_CODE_SUCCESS) {
      count++;
    }
  }
  ASSERT_TRUE(tBloomFilterIsFull(pBF1));

  SBloomFilter *pBF2 = tBloomFilterInit(1000 * 10000, 0.1);
  GTEST_ASSERT_EQ(pBF2->numBits, 47925312);
  GTEST_ASSERT_EQ(pBF2->numUnits, 47925312 / 64);

  SBloomFilter *pBF3 = tBloomFilterInit(10000 * 10000, 0.001);
  GTEST_ASSERT_EQ(pBF3->numBits, 1437758784);
  GTEST_ASSERT_EQ(pBF3->numUnits, 1437758784 / 64);

  int64_t       size = 10000;
  SBloomFilter *pBF4 = tBloomFilterInit(size, 0.001);
  for (int64_t i = 0; i < 1000; i++) {
    int64_t ts = i + ts1;
    GTEST_ASSERT_EQ(tBloomFilterPut(pBF4, &ts, sizeof(int64_t)), TSDB_CODE_SUCCESS);
  }
  ASSERT_TRUE(!tBloomFilterIsFull(pBF4));

  for (int64_t i = 0; i < 1000; i++) {
    int64_t ts = i + ts1;
    uint64_t h1 = (uint64_t) pBF4->hashFn1((const char*)&ts, sizeof(int64_t));
    uint64_t h2 = (uint64_t) pBF4->hashFn2((const char*)&ts, sizeof(int64_t));
    GTEST_ASSERT_EQ(tBloomFilterNoContain(pBF4, h1, h2), TSDB_CODE_FAILED);
  }

  for (int64_t i = 2000; i < 3000; i++) {
    int64_t ts = i + ts1;
    uint64_t h1 = (uint64_t) pBF4->hashFn1((const char*)&ts, sizeof(int64_t));
    uint64_t h2 = (uint64_t) pBF4->hashFn2((const char*)&ts, sizeof(int64_t));
    GTEST_ASSERT_EQ(tBloomFilterNoContain(pBF4,  h1, h2), TSDB_CODE_SUCCESS);
  }

  tBloomFilterDestroy(pBF1);
  tBloomFilterDestroy(pBF2);
  tBloomFilterDestroy(pBF3);
  tBloomFilterDestroy(pBF4);
}

TEST(TD_UTIL_BLOOMFILTER_TEST, scalable_bloomFilter) {
  int64_t ts1 = 1650803518000;

  GTEST_ASSERT_EQ(NULL, tScalableBfInit(100, 0));
  GTEST_ASSERT_EQ(NULL, tScalableBfInit(100, 1));
  GTEST_ASSERT_EQ(NULL, tScalableBfInit(100, -0.1));
  GTEST_ASSERT_EQ(NULL, tScalableBfInit(0, 0.01));

  SScalableBf *pSBF1 = tScalableBfInit(100, 0.01);
  GTEST_ASSERT_EQ(pSBF1->numBits, 1152);
  int64_t count = 0;
  int64_t index = 0;
  for (; count < 100; index++) {
    int64_t ts = index + ts1;
    if (tScalableBfPut(pSBF1, &ts, sizeof(int64_t)) == TSDB_CODE_SUCCESS) {
      count++;
    }
  }
  GTEST_ASSERT_EQ(pSBF1->numBits, 1152);

  for (; count < 300; index++) {
    int64_t ts = index + ts1;
    if (tScalableBfPut(pSBF1, &ts, sizeof(int64_t)) == TSDB_CODE_SUCCESS) {
      count++;
    }
  }
  GTEST_ASSERT_EQ(pSBF1->numBits, 1152 + 2496);

  for (; count < 700; index++) {
    int64_t ts = index + ts1;
    if (tScalableBfPut(pSBF1, &ts, sizeof(int64_t)) == TSDB_CODE_SUCCESS) {
      count++;
    }
  }
  GTEST_ASSERT_EQ(pSBF1->numBits, 1152 + 2496 + 5568);

  for (; count < 1500; index++) {
    int64_t ts = index + ts1;
    if (tScalableBfPut(pSBF1, &ts, sizeof(int64_t)) == TSDB_CODE_SUCCESS) {
      count++;
    }
  }
  GTEST_ASSERT_EQ(pSBF1->numBits, 1152 + 2496 + 5568 + 12288);

  int32_t aSize = taosArrayGetSize(pSBF1->bfArray);
  int64_t totalBits = 0;
  for (int64_t i = 0; i < aSize; i++) {
    SBloomFilter *pBF = (SBloomFilter *)taosArrayGetP(pSBF1->bfArray, i);
    ASSERT_TRUE(tBloomFilterIsFull(pBF));
    totalBits += pBF->numBits;
  }
  GTEST_ASSERT_EQ(pSBF1->numBits, totalBits);

  for (int64_t i = 0; i < index; i++) {
    int64_t ts = i + ts1;
    GTEST_ASSERT_EQ(tScalableBfNoContain(pSBF1, &ts, sizeof(int64_t)), TSDB_CODE_FAILED);
  }

  int64_t      size = 10000;
  SScalableBf *pSBF4 = tScalableBfInit(size, 0.001);
  for (int64_t i = 0; i < 1000; i++) {
    int64_t ts = i + ts1;
    GTEST_ASSERT_EQ(tScalableBfPut(pSBF4, &ts, sizeof(int64_t)), TSDB_CODE_SUCCESS);
  }

  for (int64_t i = 0; i < 1000; i++) {
    int64_t ts = i + ts1;
    GTEST_ASSERT_EQ(tScalableBfNoContain(pSBF4, &ts, sizeof(int64_t)), TSDB_CODE_FAILED);
  }

  for (int64_t i = 2000; i < 3000; i++) {
    int64_t ts = i + ts1;
    GTEST_ASSERT_EQ(tScalableBfNoContain(pSBF4, &ts, sizeof(int64_t)), TSDB_CODE_SUCCESS);
  }

  tScalableBfDestroy(pSBF1);
  tScalableBfDestroy(pSBF4);
}