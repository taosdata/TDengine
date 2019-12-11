#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <assert.h>
#include <math.h>
#include <float.h>

#include "taos.h"
#include "taosmsg.h"
#include "textbuffer.h"

void intDataTest();
void bigintDataTest();
void doubleDataTest();
void outofMemTest();
void largeDataTest();
void hashTest();

void qsortTest();

void differentMemoryBufferTest();

tMemBucket *createBigIntDataBucket(int32_t start, int32_t end, int32_t bufferSize, tOrderDescriptor*);
tMemBucket *createIntDataBucket(int32_t start, int32_t end, int32_t bufferSize, tOrderDescriptor*);
tMemBucket *createDoubleDataBucket(int32_t start, int32_t end, int32_t bufferSize, tOrderDescriptor*);

int32_t main(int32_t argc, char **argv) {
    debugFlag = 199;
    qsortTest();
    intDataTest();
    bigintDataTest();
    doubleDataTest();
    largeDataTest();
}

/*
 * test int data percentile process
 */
void intDataTest() {
    printf("running %s\n", __FUNCTION__);

    tMemBucket *pBucket = NULL;
    double result = 0.;

    int32_t colOffset[1] = {0};
    SSchema field[1] = {
            {TSDB_DATA_TYPE_INT, "k", sizeof(int32_t)},
    };

    tColModel* pModel = tColModelCreate(field, 1, 1000);
    tOrderDescriptor* pDesc = tOrderDesCreate(colOffset, 1, pModel, TSQL_SO_ASC);

    pBucket = createIntDataBucket(-1, 1, 1 << 20, pDesc);

    result = getPercentile(pBucket, 50);
    assert(fabs(result) < DBL_EPSILON);
    printf("%lf\n", result);

    result = getPercentile(pBucket, 0);
    assert(fabs(result + 1) < DBL_EPSILON);
    printf("%lf\n", result);

    result = getPercentile(pBucket, 75);
    assert(fabs(result - 0.5) < DBL_EPSILON);
    printf("%lf\n", result);

    result = getPercentile(pBucket, 100);
    assert(fabs(result - 1) < DBL_EPSILON);
    printf("%lf\n", result);
    tMemBucketDestroy(pBucket);

    pBucket = createIntDataBucket(0, 99999, 1 << 20, pDesc);
    result = getPercentile(pBucket, 50);
    assert(result - 49999.5 < DBL_EPSILON);
    printf("%lf\n", result);

    tColModelDestroy(pModel);
    tOrderDescDestroy(&pDesc);
    tMemBucketDestroy(pBucket);
}

void bigintDataTest() {
    printf("running %s\n", __FUNCTION__);

    tMemBucket *pBucket = NULL;
    double result = 0.0;

    int32_t orderIdx[1] = {0};
    SSchema field[1] = {
            {TSDB_DATA_TYPE_BIGINT, "k", sizeof(int64_t)},
    };

    tColModel* pModel = tColModelCreate(field, 1, 1000);
    tOrderDescriptor* pDesc = tOrderDesCreate(orderIdx, 1, pModel, TSQL_SO_ASC);

    pBucket = createBigIntDataBucket(-1000, 1000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 50);
    assert(result == 0.);
    tMemBucketDestroy(pBucket);

    pBucket = createBigIntDataBucket(-10000, 10000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 100);
    assert(result == 10000.0);
    tMemBucketDestroy(pBucket);

    pBucket = createBigIntDataBucket(-10000, 10000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 75);
    assert(result == 5000.0);

    tColModelDestroy(pModel);
    tOrderDescDestroy(&pDesc);
    tMemBucketDestroy(pBucket);
}

tMemBucket *createDoubleDataBucket(int32_t start, int32_t end, int32_t bufferSize, tOrderDescriptor* pDesc) {
    tMemBucket *pBucket = tMemBucketCreate(1024, bufferSize, sizeof(double), TSDB_DATA_TYPE_DOUBLE, pDesc);
    for (int32_t i = start; i <= end; ++i) {
        double val = i;
        tMemBucketPut(pBucket, &val, 1);
    }

    return pBucket;
}

tMemBucket *createIntDataBucket(int32_t start, int32_t end, int32_t bufferSize, tOrderDescriptor* pDesc) {
    tMemBucket *pBucket = tMemBucketCreate(1024, bufferSize, sizeof(int32_t), TSDB_DATA_TYPE_INT, pDesc);

    for (int32_t i = start; i <= end; ++i) {
        int32_t val = i;
        tMemBucketPut(pBucket, &val, 1);
    }

    return pBucket;
}

tMemBucket *createBigIntDataBucket(int32_t start, int32_t end, int32_t bufferSize, tOrderDescriptor* pDesc) {
    tMemBucket *pBucket = tMemBucketCreate(1024, bufferSize, sizeof(int64_t), TSDB_DATA_TYPE_BIGINT, pDesc);
    for (int32_t i = start; i <= end; ++i) {
        int64_t val = i;
        tMemBucketPut(pBucket, &val, 1);
    }

    return pBucket;
}

void createShortDataArrays(int32_t start, int32_t end) {
    int32_t orderIdx[1] = {0};
    SSchema field[1] = {
            {TSDB_DATA_TYPE_DOUBLE, "k", sizeof(double)},
    };
    tColModel* pModel = tColModelCreate(field, 1, 1000);
    tOrderDescriptor* pDesc = tOrderDesCreate(orderIdx, 1, pModel, TSQL_SO_ASC);

    tMemBucket *pBucket = NULL;
    double result = 0;

    pBucket = createDoubleDataBucket(-10, 10, 1 << 20, pDesc);
    result = getPercentile(pBucket, 0);
    assert(fabs(result - 10.0) < DBL_EPSILON);

    printf("result is: %lf\n", result);
    tMemBucketDestroy(pBucket);

    pBucket = createDoubleDataBucket(-100000, 100000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 25);
    assert(fabs(result + 75000) < DBL_EPSILON);

    printf("result is: %lf\n", result);

    tMemBucketDestroy(pBucket);

    pBucket = createDoubleDataBucket(-100000, 100000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 50);
    assert(result < DBL_EPSILON);

    printf("result is: %lf\n", result);

    tMemBucketDestroy(pBucket);

    pBucket = createDoubleDataBucket(-100000, 100000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 75);
    assert(fabs(result - 75000) < DBL_EPSILON);

    printf("result is: %lf\n", result);

    tMemBucketDestroy(pBucket);

    pBucket = createDoubleDataBucket(-100000, 100000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 100);
    assert(fabs(result - 100000.0) < DBL_EPSILON);

    printf("result is: %lf\n", result);

    tColModelDestroy(pModel);
    tOrderDescDestroy(&pDesc);
    tMemBucketDestroy(pBucket);
}

void doubleDataTest() {
    printf("running %s\n", __FUNCTION__);

    int32_t orderIdx[1] = {0};
    SSchema field[1] = {
            {TSDB_DATA_TYPE_DOUBLE, "k", sizeof(double)},
    };
    tMemBucket *pBucket = NULL;
    tColModel* pModel = tColModelCreate(field, 1, 1000);
    tOrderDescriptor* pDesc = tOrderDesCreate(orderIdx, 1, pModel, TSQL_SO_ASC);

    double result = 0;

    pBucket = createDoubleDataBucket(-10, 10, 1 << 20, pDesc);
    result = getPercentile(pBucket, 0);
    assert(fabs(result + 10.0) < DBL_EPSILON);

    printf("result is: %lf\n", result);
    tMemBucketDestroy(pBucket);

    pBucket = createDoubleDataBucket(-100000, 100000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 25);
    assert(fabs(result + 50000) < DBL_EPSILON);

    printf("result is: %lf\n", result);

    tMemBucketDestroy(pBucket);

    pBucket = createDoubleDataBucket(-100000, 100000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 50);
    assert(result < DBL_EPSILON);

    printf("result is: %lf\n", result);

    tMemBucketDestroy(pBucket);

    pBucket = createDoubleDataBucket(-100000, 100000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 75);
    printf("result is: %lf\n", result);
    assert(fabs(result - 50000) < DBL_EPSILON);
    tMemBucketDestroy(pBucket);

    pBucket = createDoubleDataBucket(-100000, 100000, 1 << 20, pDesc);

    result = getPercentile(pBucket, 100);
    assert(fabs(result - 100000.0) < DBL_EPSILON);

    printf("result is: %lf\n", result);

    tColModelDestroy(pModel);
    tOrderDescDestroy(&pDesc);

    tMemBucketDestroy(pBucket);
}

/*
 * large data test, we employ 0.1billion double data to calculated the percentile
 * which is 800MB data
 */
void largeDataTest() {
    printf("running : %s\n", __FUNCTION__);

    int32_t orderIdx[1] = {0};
    SSchema field[1] = {
            {TSDB_DATA_TYPE_DOUBLE, "k", sizeof(double)},
    };

    tColModel* pModel = tColModelCreate(field, 1, 1000);
    tOrderDescriptor* pDesc = tOrderDesCreate(orderIdx, 1, pModel, TSQL_SO_ASC);

    tMemBucket *pBucket = NULL;
    double result = 0;

    struct timeval tv;
    gettimeofday(&tv, NULL);

    int64_t start = tv.tv_sec;
    printf("start time: %lld\n", tv.tv_sec);
    pBucket = createDoubleDataBucket(0, 100000000, 1 << 20, pDesc);
    result = getPercentile(pBucket, 50);
    assert(result - 50000000 < DBL_EPSILON);

    gettimeofday(&tv, NULL);
    printf("total elapsed time: %lld\n sec.", -start + tv.tv_sec);
    printf("the result of %d is: %lf\n", 50, result);

    tColModelDestroy(pModel);
    tOrderDescDestroy(&pDesc);
    tMemBucketDestroy(pBucket);
}

void hashTest() {
    printf("running : %s\n", __FUNCTION__);

}

void qsortTest() {
    printf("running : %s\n", __FUNCTION__);

    SSchema field[1] = {
            {TSDB_DATA_TYPE_INT, "k", sizeof(int32_t)},
    };

    const int32_t num = 2000;

    int32_t *d = (int32_t *) malloc(sizeof(int32_t) * num);
    for (int32_t i = 0; i < num; ++i) {
        d[i] = i % 4;
    }

    const int32_t numOfOrderCols = 1;
    int32_t orderColIdx = 0;
    tColModel* pModel = tColModelCreate(field, 1, 1000);
    tOrderDescriptor* pDesc = tOrderDesCreate(&orderColIdx, numOfOrderCols, pModel, TSQL_SO_ASC);

    tColDataQSort(pDesc, num, 0, num - 1, d, TSQL_SO_ASC);

    for(int32_t i=0; i<num; ++i) {
        printf("%d\t", d[i]);
    }
    printf("\n");

    tColModelDestroy(pModel);
    tOrderDescDestroy(&pDesc);
}

