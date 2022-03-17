#include <gtest/gtest.h>
#include <iostream>

#include "qResultbuf.h"
#include "taos.h"
#include "taosdef.h"

#include "assert.h"
#include "qHistogram.h"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"

extern "C" {
#include "tdigest.h"
#include "qHistogram.h"

}


namespace {

enum {
  TEST_DATA_TYPE_INT  = 0,
  TEST_DATA_TYPE_BIGINT,
  TEST_DATA_TYPE_FLOAT,
  TEST_DATA_TYPE_DOUBLE
};

enum {
  TEST_DATA_MODE_SEQ  = 0,
  TEST_DATA_MODE_DSEQ,
  TEST_DATA_MODE_RAND_PER,
  TEST_DATA_MODE_RAND_LIMIT,
};


void tdigest_init(TDigest **pTDigest) {
  void *tmp = calloc(1, (size_t)(TDIGEST_SIZE(COMPRESSION)));
  *pTDigest = tdigestNewFrom(tmp, COMPRESSION);
}

void thistogram_init(SHistogramInfo **pHisto) {
  void *tmp = calloc(1, (int16_t)(sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1) + sizeof(SHistogramInfo)));  
  *pHisto = tHistogramCreateFrom(tmp, MAX_HISTOGRAM_BIN);
}


static FORCE_INLINE int64_t testGetTimestampUs() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
}


double *  thistogram_end(SHistogramInfo* pHisto, double* ratio, int32_t num){
  assert(pHisto->numOfElems > 0);

  double ratio2 = *ratio * 100;

  return tHistogramUniform(pHisto, &ratio2, 1);
}


void setTestData(void *data, int64_t idx, int32_t type, int64_t value) {
  switch (type) {
    case TEST_DATA_TYPE_INT:
      *((int32_t*)data + idx) = (int32_t)value;
      break;
    case TEST_DATA_TYPE_BIGINT:
      *((int64_t*)data + idx) = (int64_t)value;
      break;
    case TEST_DATA_TYPE_FLOAT:
      *((float*)data + idx) = (float)value;
      break;
    case TEST_DATA_TYPE_DOUBLE:
      *((double*)data + idx) = (double)value;
      break;
    default:
      assert(0);
  }
}


void addDTestData(void *data, int64_t idx, int32_t type, TDigest* pTDigest) {
  switch (type) {
    case TEST_DATA_TYPE_INT:
      tdigestAdd(pTDigest, (double)*((int32_t*)data + idx), 1);
      break;
    case TEST_DATA_TYPE_BIGINT:
      tdigestAdd(pTDigest, (double)*((int64_t*)data + idx), 1);
      break;
    case TEST_DATA_TYPE_FLOAT:
      tdigestAdd(pTDigest, (double)*((float*)data + idx), 1);
      break;
    case TEST_DATA_TYPE_DOUBLE:
      tdigestAdd(pTDigest, (double)*((double*)data + idx), 1);
      break;
    default:
      assert(0);
  }
}

void addHTestData(void *data, int64_t idx, int32_t type, SHistogramInfo *pHisto) {
  switch (type) {
    case TEST_DATA_TYPE_INT:
      tHistogramAdd(&pHisto, (double)*((int32_t*)data + idx));
      break;
    case TEST_DATA_TYPE_BIGINT:
      tHistogramAdd(&pHisto, (double)*((int64_t*)data + idx));
      break;
    case TEST_DATA_TYPE_FLOAT:
      tHistogramAdd(&pHisto, (double)*((float*)data + idx));
      break;
    case TEST_DATA_TYPE_DOUBLE:
      tHistogramAdd(&pHisto, (double)*((double*)data + idx));
      break;
    default:
      assert(0);
  }
}




void initTestData(void **data, int32_t type, int64_t num, int32_t mode, int32_t randPar) {
  int32_t tsize[] = {4, 8, 4, 8};
  
  *data = malloc(num * tsize[type]);

  switch (mode) {
    case TEST_DATA_MODE_SEQ:
      for (int64_t i = 0; i < num; ++i) {
        setTestData(*data, i, type, i);
      }
      break;
    case TEST_DATA_MODE_DSEQ:
      for (int64_t i = 0; i < num; ++i) {
        setTestData(*data, i, type, num - i);
      }
      break;
    case TEST_DATA_MODE_RAND_PER: {
        srand(time(NULL));
        int64_t randMax = num * randPar / 100;

        if (randMax == 0) {
          for (int64_t i = 0; i < num; ++i) {
            setTestData(*data, i, type, rand());
          }
        } else {
          for (int64_t i = 0; i < num; ++i) {
            setTestData(*data, i, type, rand() % randMax);
          }
        }
      }
      break;
    case TEST_DATA_MODE_RAND_LIMIT:
      srand(time(NULL));
      for (int64_t i = 0; i < num; ++i) {
        setTestData(*data, i, type, rand() % randPar);
      }
      break;

    default:
      assert(0);
  }
}


void tdigestTest() {
  printf("running %s\n", __FUNCTION__);

  TDigest *pTDigest = NULL;
  void *data = NULL;
  SHistogramInfo *pHisto = NULL;
  double ratio = 0.5;

  int64_t totalNum[] = {100,10000,10000000};
  int32_t numTimes = sizeof(totalNum)/sizeof(totalNum[0]);
  int64_t biggestNum = totalNum[numTimes - 1];
  int32_t unitNum[] = {1,10,100,1000,5000,10000,100000};
  int32_t unitTimes = sizeof(unitNum)/sizeof(unitNum[0]);
  int32_t dataMode[] = {TEST_DATA_MODE_SEQ, TEST_DATA_MODE_DSEQ, TEST_DATA_MODE_RAND_PER, TEST_DATA_MODE_RAND_LIMIT};
  int32_t modeTimes = sizeof(dataMode)/sizeof(dataMode[0]);
  int32_t dataTypes[] = {TEST_DATA_TYPE_INT, TEST_DATA_TYPE_BIGINT, TEST_DATA_TYPE_FLOAT, TEST_DATA_TYPE_DOUBLE};
  int32_t typeTimes = sizeof(dataTypes)/sizeof(dataTypes[0]);
  int32_t randPers[] = {0, 1, 10, 50, 90};
  int32_t randPTimes = sizeof(randPers)/sizeof(randPers[0]);
  int32_t randLimits[] = {10, 50, 100, 1000, 10000};
  int32_t randLTimes = sizeof(randLimits)/sizeof(randLimits[0]);

  double useTime[2][10][10][10][10] = {0.0};
  
  for (int32_t i = 0; i < modeTimes; ++i) {
    if (dataMode[i] == TEST_DATA_MODE_RAND_PER) {
      for (int32_t p = 0; p < randPTimes; ++p) {
        for (int32_t j = 0; j < typeTimes; ++j) {
          initTestData(&data, dataTypes[j], biggestNum, dataMode[i], randPers[p]);
          for (int32_t m = 0; m < numTimes; ++m) {          
            int64_t startu = testGetTimestampUs();
            tdigest_init(&pTDigest);
            for (int64_t n = 0; n < totalNum[m]; ++n) {
              addDTestData(data, n, dataTypes[j], pTDigest);
            }
            double res = tdigestQuantile(pTDigest, ratio);
            free(pTDigest);
            useTime[0][i][j][m][p] = ((double)(testGetTimestampUs() - startu))/1000;
            printf("DMode:%d,Type:%d,Num:%" PRId64 ",randP:%d,Used:%fms\tRES:%f\n", dataMode[i], dataTypes[j], totalNum[m], randPers[p], useTime[0][i][j][m][p], res);

            startu = testGetTimestampUs();
            thistogram_init(&pHisto);
            for (int64_t n = 0; n < totalNum[m]; ++n) {
              addHTestData(data, n, dataTypes[j], pHisto);
            }
            double *res2 = thistogram_end(pHisto, &ratio, 1);
            free(pHisto);
            useTime[1][i][j][m][p] = ((double)(testGetTimestampUs() - startu))/1000;
            printf("HMode:%d,Type:%d,Num:%" PRId64 ",randP:%d,Used:%fms\tRES:%f\n", dataMode[i], dataTypes[j], totalNum[m], randPers[p], useTime[1][i][j][m][p], *res2);
            
          }
          free(data);
        }
      }
    } else if (dataMode[i] == TEST_DATA_MODE_RAND_LIMIT) {
      for (int32_t p = 0; p < randLTimes; ++p) {
        for (int32_t j = 0; j < typeTimes; ++j) {
          initTestData(&data, dataTypes[j], biggestNum, dataMode[i], randLimits[p]);
          for (int64_t m = 0; m < numTimes; ++m) {
            int64_t startu = testGetTimestampUs();          
            tdigest_init(&pTDigest);
            for (int64_t n = 0; n < totalNum[m]; ++n) {
              addDTestData(data, m, dataTypes[j], pTDigest);
            }
            double res = tdigestQuantile(pTDigest, ratio);
            free(pTDigest);                  
            useTime[0][i][j][m][p] = ((double)(testGetTimestampUs() - startu))/1000;
            printf("DMode:%d,Type:%d,Num:%" PRId64 ",randL:%d,Used:%fms\tRES:%f\n", dataMode[i], dataTypes[j], totalNum[m], randLimits[p], useTime[0][i][j][m][p], res);


            startu = testGetTimestampUs();          
            thistogram_init(&pHisto);
            for (int64_t n = 0; n < totalNum[m]; ++n) {
              addHTestData(data, n, dataTypes[j], pHisto);
            }
            double* res2 = thistogram_end(pHisto, &ratio, 1);
            free(pHisto);                  
            useTime[1][i][j][m][p] = ((double)(testGetTimestampUs() - startu))/1000;
            printf("HMode:%d,Type:%d,Num:%" PRId64 ",randL:%d,Used:%fms\tRES:%f\n", dataMode[i], dataTypes[j], totalNum[m], randLimits[p], useTime[1][i][j][m][p], *res2);
          }          
          free(data);
        }
      }
    } else {
      for (int32_t j = 0; j < typeTimes; ++j) {
        initTestData(&data, dataTypes[j], biggestNum, dataMode[i], 0);
        for (int64_t m = 0; m < numTimes; ++m) {
          int64_t startu = testGetTimestampUs();        
          tdigest_init(&pTDigest);        
          for (int64_t n = 0; n < totalNum[m]; ++n) {
            addDTestData(data, n, dataTypes[j], pTDigest);
          }
          double res = tdigestQuantile(pTDigest, ratio);
          free(pTDigest);
          useTime[0][i][j][m][0] = ((double)(testGetTimestampUs() - startu))/1000;
          printf("DMode:%d,Type:%d,Num:%" PRId64 ",Used:%fms\tRES:%f\n", dataMode[i], dataTypes[j], totalNum[m], useTime[0][i][j][m][0], res);


          startu = testGetTimestampUs();        
          thistogram_init(&pHisto);        
          for (int64_t n = 0; n < totalNum[m]; ++n) {
            addHTestData(data, n, dataTypes[j], pHisto);
          }
          double* res2 = thistogram_end(pHisto, &ratio, 1);
          free(pHisto);
          useTime[1][i][j][m][0] = ((double)(testGetTimestampUs() - startu))/1000;
          printf("HMode:%d,Type:%d,Num:%" PRId64 ",Used:%fms\tRES:%f\n", dataMode[i], dataTypes[j], totalNum[m], useTime[1][i][j][m][0], *res2);

        }        
        free(data);
      }
    }
  }


  printf("\n\n");

  
  for (int32_t i = 0; i < modeTimes; ++i) {
    if (dataMode[i] == TEST_DATA_MODE_RAND_PER) {
      for (int32_t p = 0; p < randPTimes; ++p) {
        for (int32_t j = 0; j < typeTimes; ++j) {
          printf("DMode:%d,Type:%d,randP:%d -", dataMode[i], dataTypes[j], randPers[p]);
          for (int32_t m = 0; m < numTimes; ++m) {          
            printf(" %d:%f", totalNum[m], useTime[0][i][j][m][p]);
          }
          printf("\n");

          printf("HMode:%d,Type:%d,randP:%d -", dataMode[i], dataTypes[j], randPers[p]);
          for (int32_t m = 0; m < numTimes; ++m) {          
            printf(" %d:%f", totalNum[m], useTime[1][i][j][m][p]);
          }
          printf("\n");          
        }
      }
    } else if (dataMode[i] == TEST_DATA_MODE_RAND_LIMIT) {
      for (int32_t p = 0; p < randLTimes; ++p) {
        for (int32_t j = 0; j < typeTimes; ++j) {
          printf("DMode:%d,Type:%d,randL:%d -", dataMode[i], dataTypes[j], randLimits[p]);
          for (int64_t m = 0; m < numTimes; ++m) {
            printf(" %d:%f", totalNum[m], useTime[0][i][j][m][p]);
          }
          printf("\n");

          printf("HMode:%d,Type:%d,randL:%d -", dataMode[i], dataTypes[j], randLimits[p]);
          for (int64_t m = 0; m < numTimes; ++m) {
            printf(" %d:%f", totalNum[m], useTime[1][i][j][m][p]);
          }
          printf("\n");          
        }
      }
    } else {
      for (int32_t j = 0; j < typeTimes; ++j) {
        printf("DMode:%d,Type:%d -", dataMode[i], dataTypes[j]);
        for (int64_t m = 0; m < numTimes; ++m) {
          printf(" %d:%f", totalNum[m], useTime[0][i][j][m][0]);
        }
        printf("\n");

        printf("HMode:%d,Type:%d -", dataMode[i], dataTypes[j]);
        for (int64_t m = 0; m < numTimes; ++m) {
          printf(" %d:%f", totalNum[m], useTime[1][i][j][m][0]);
        }
        printf("\n");        
      }
    }
  }
}


}  // namespace

TEST(testCase, apercentileTest) {
  tdigestTest();
}
