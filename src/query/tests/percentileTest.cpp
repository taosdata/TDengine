#include <gtest/gtest.h>
#include <iostream>

#include "qResultbuf.h"
#include "taos.h"
#include "taosdef.h"

#include "qPercentile.h"

namespace {
tMemBucket *createBigIntDataBucket(int32_t start, int32_t end) {
  tMemBucket *pBucket = tMemBucketCreate(sizeof(int64_t), TSDB_DATA_TYPE_BIGINT, start, end);
  for (int32_t i = start; i <= end; ++i) {
    int64_t val = i;
    tMemBucketPut(pBucket, &val, 1);
  }

  return pBucket;
}

tMemBucket *createIntDataBucket(int32_t start, int32_t end) {
  tMemBucket *pBucket = tMemBucketCreate(sizeof(int32_t), TSDB_DATA_TYPE_INT, start, end);

  for (int32_t i = start; i <= end; ++i) {
    int32_t val = i;
    tMemBucketPut(pBucket, &val, 1);
  }

  return pBucket;
}

tMemBucket *createDoubleDataBucket(int32_t start, int32_t end) {
  tMemBucket *pBucket = tMemBucketCreate(sizeof(double), TSDB_DATA_TYPE_DOUBLE, start, end);
  for (int32_t i = start; i <= end; ++i) {
    double  val = i;
    int32_t ret = tMemBucketPut(pBucket, &val, 1);
    if (ret != 0) {
      printf("value out of range:%f", val);
    }
  }

  return pBucket;
}

tMemBucket *createUnsignedDataBucket(int32_t start, int32_t end, int32_t type) {
  tMemBucket *pBucket = tMemBucketCreate(tDataTypes[type].nSize, type, start, end);
  for (int32_t i = start; i <= end; ++i) {
    uint64_t k = i;
    int32_t ret = tMemBucketPut(pBucket, &k, 1);
    if (ret != 0) {
      printf("value out of range:%f", k);
    }
  }

  return pBucket;
}

void intDataTest() {
  printf("running %s\n", __FUNCTION__);

  tMemBucket *pBucket = NULL;
  double      result = 0.;

  pBucket = createIntDataBucket(0, 0);
  result = getPercentile(pBucket, 0);
  ASSERT_DOUBLE_EQ(result, 0);
  tMemBucketDestroy(pBucket);

  pBucket = createIntDataBucket(0, 1);
  result = getPercentile(pBucket, 100);
  ASSERT_DOUBLE_EQ(result, 1);

  result = getPercentile(pBucket, 0);
  ASSERT_DOUBLE_EQ(result, 0);
  tMemBucketDestroy(pBucket);

  pBucket = createIntDataBucket(-1, 1);

  result = getPercentile(pBucket, 50);
  ASSERT_DOUBLE_EQ(result, 0);

  result = getPercentile(pBucket, 0);
  ASSERT_DOUBLE_EQ(result, -1);

  result = getPercentile(pBucket, 75);
  ASSERT_DOUBLE_EQ(result, 0.5);

  result = getPercentile(pBucket, 100);
  ASSERT_DOUBLE_EQ(result, 1);
  tMemBucketDestroy(pBucket);

  pBucket = createIntDataBucket(0, 99999);
  result = getPercentile(pBucket, 50);
  ASSERT_DOUBLE_EQ(result, 49999.5);

  tMemBucketDestroy(pBucket);
}

void bigintDataTest() {
  printf("running %s\n", __FUNCTION__);

  tMemBucket *pBucket = NULL;
  double      result = 0.0;

  pBucket = createBigIntDataBucket(-1000, 1000);
  result = getPercentile(pBucket, 50);
  ASSERT_DOUBLE_EQ(result, 0.);
  tMemBucketDestroy(pBucket);

  pBucket = createBigIntDataBucket(-10000, 10000);
  result = getPercentile(pBucket, 100);
  ASSERT_DOUBLE_EQ(result, 10000.0);
  tMemBucketDestroy(pBucket);

  pBucket = createBigIntDataBucket(-10000, 10000);
  result = getPercentile(pBucket, 75);
  ASSERT_DOUBLE_EQ(result, 5000.0);

  tMemBucketDestroy(pBucket);
}

void doubleDataTest() {
  printf("running %s\n", __FUNCTION__);

  tMemBucket *pBucket = NULL;
  double      result = 0;

  pBucket = createDoubleDataBucket(-10, 10);
  result = getPercentile(pBucket, 0);
  ASSERT_DOUBLE_EQ(result, -10.0);

  printf("result is: %lf\n", result);
  tMemBucketDestroy(pBucket);

  pBucket = createDoubleDataBucket(-100000, 100000);
  result = getPercentile(pBucket, 25);
  ASSERT_DOUBLE_EQ(result, -50000);

  printf("result is: %lf\n", result);

  tMemBucketDestroy(pBucket);

  pBucket = createDoubleDataBucket(-100000, 100000);
  result = getPercentile(pBucket, 50);
  ASSERT_DOUBLE_EQ(result, 0);

  tMemBucketDestroy(pBucket);

  pBucket = createDoubleDataBucket(-100000, 100000);
  result = getPercentile(pBucket, 75);
  ASSERT_DOUBLE_EQ(result, 50000);
  tMemBucketDestroy(pBucket);

  pBucket = createDoubleDataBucket(-100000, 100000);

  result = getPercentile(pBucket, 100);
  ASSERT_DOUBLE_EQ(result, 100000.0);

  printf("result is: %lf\n", result);
  tMemBucketDestroy(pBucket);
}

/*
 * large data test, we employ 0.1billion double data to calculated the percentile
 * which is 800MB data
 */
void largeDataTest() {
  printf("running : %s\n", __FUNCTION__);

  tMemBucket *pBucket = NULL;
  double      result = 0;

  struct timeval tv;
  gettimeofday(&tv, NULL);

  int64_t start = tv.tv_sec;
  printf("start time: %" PRId64 "\n", tv.tv_sec);
  pBucket = createDoubleDataBucket(0, 100000000);
  result = getPercentile(pBucket, 50);
  ASSERT_DOUBLE_EQ(result, 50000000);

  gettimeofday(&tv, NULL);

  printf("total elapsed time: %" PRId64 " sec.", -start + tv.tv_sec);
  printf("the result of %d is: %lf\n", 50, result);
  tMemBucketDestroy(pBucket);
}

void qsortTest() {
  printf("running : %s\n", __FUNCTION__);

  SSchema field[1] = {
      {TSDB_DATA_TYPE_INT, "k", sizeof(int32_t)},
  };

  const int32_t num = 2000;

  int32_t *d = (int32_t *)malloc(sizeof(int32_t) * num);
  for (int32_t i = 0; i < num; ++i) {
    d[i] = i % 4;
  }

  const int32_t     numOfOrderCols = 1;
  int32_t           orderColIdx = 0;
  SColumnModel *    pModel = createColumnModel(field, 1, 1000);
  tOrderDescriptor *pDesc = tOrderDesCreate(&orderColIdx, numOfOrderCols, pModel, 1);

  tColDataQSort(pDesc, num, 0, num - 1, (char *)d, 1);

  for (int32_t i = 0; i < num; ++i) {
    printf("%d\t", d[i]);
  }
  printf("\n");

  destroyColumnModel(pModel);
}

void unsignedDataTest() {
  printf("running %s\n", __FUNCTION__);

  tMemBucket *pBucket = NULL;
  double      result = 0.0;

  pBucket = createUnsignedDataBucket(0, 1000, TSDB_DATA_TYPE_UINT);
  result = getPercentile(pBucket, 50);
  ASSERT_DOUBLE_EQ(result, 500.0);
  tMemBucketDestroy(pBucket);

  pBucket = createUnsignedDataBucket(0, 10000, TSDB_DATA_TYPE_UBIGINT);
  result = getPercentile(pBucket, 100);
  ASSERT_DOUBLE_EQ(result, 10000.0);

  result = getPercentile(pBucket, 0);
  ASSERT_DOUBLE_EQ(result, 0.0);

  result = getPercentile(pBucket, 50);
  ASSERT_DOUBLE_EQ(result, 5000);

  result = getPercentile(pBucket, 75);
  ASSERT_DOUBLE_EQ(result, 7500);
  tMemBucketDestroy(pBucket);

}

}  // namespace

TEST(testCase, percentileTest) {
  //  qsortTest();
  intDataTest();
  bigintDataTest();
  doubleDataTest();
  unsignedDataTest();
  largeDataTest();
}
