#include <float.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "taosmsg.h"
#include "taosdef.h"
#include "tskiplist.h"
#include "ttime.h"
#include "tutil.h"

void doubleSkipListTest();
//void intSkipListTest();
void stringKeySkiplistTest();
void skiplistPerformanceTest();
void duplicatedKeyTest();

int32_t main(int argc, char **argv) {
  assert(sizeof(tSkipListNode) == 48);
  assert(sizeof(tSkipListKey) == 16);

  srand(time(NULL));

  stringKeySkiplistTest();
  doubleSkipListTest();
  skiplistPerformanceTest();
  duplicatedKeyTest();

  tSKipListQueryCond q;
  q.upperBndRelOptr = true;
  q.lowerBndRelOptr = true;
  q.upperBnd.nType = TSDB_DATA_TYPE_DOUBLE;
  q.lowerBnd.nType = TSDB_DATA_TYPE_DOUBLE;
  q.lowerBnd.dKey = 120;
  q.upperBnd.dKey = 171.989;
  /*
      int32_t size = tSkipListQuery(pSkipList, &q, &pNodes);
      for (int32_t i = 0; i < size; ++i) {
          printf("-----%lf\n", pNodes[i]->key.dKey);
      }
      printf("the range query result size is: %d\n", size);
      tfree(pNodes);

      tSkipListKey *pKeys = malloc(sizeof(tSkipListKey) * 20);
      for (int32_t i = 0; i < 8; i += 2) {
          pKeys[i].dKey = i * 0.997;
          pKeys[i].nType = TSDB_DATA_TYPE_DOUBLE;
          printf("%lf ", pKeys[i].dKey);
      }

      int32_t r = tSkipListPointQuery(pSkipList, pKeys, 8, EXCLUDE_POINT_QUERY, &pNodes);
      printf("\nthe exclude query result is: %d\n", r);
      for (int32_t i = 0; i < r; ++i) {
  //        printf("%lf ", pNodes[i]->key.dKey);
      }
      tfree(pNodes);

      free(pKeys);*/
  getchar();
  return 0;
}

void doubleSkipListTest() {
  tSkipList *pSkipList = tSkipListCreate(10, TSDB_DATA_TYPE_DOUBLE, sizeof(double));

  tSkipListKey key;
  double       doubleVal[1000] = {0};
  printf("generated 200000 keys is: \n");

  for (int32_t i = 0; i < 200000; ++i) {
    key.dKey = i * 0.997;
    key.nType = TSDB_DATA_TYPE_DOUBLE;

    if (i < 1000) {
      doubleVal[i] = i * 0.997;
    }

    tSkipListPut(pSkipList, "", &key, 1);
  }

  printf("the first level of skip list is:\n");
  tSkipListPrint(pSkipList, 1);

  tSkipListNode **pNodes = NULL;
  tSkipListKey    sk;
  for (int32_t i = 0; i < 100; ++i) {
    sk.nType = TSDB_DATA_TYPE_DOUBLE;
    int32_t idx = abs((i * rand()) % 1000);

    sk.dKey = doubleVal[idx];

    int32_t size = tSkipListGets(pSkipList, &sk, &pNodes);

    printf("the query result size is: %d\n", size);
    for (int32_t j = 0; j < size; ++j) {
      printf("the result is: %lf\n", pNodes[j]->key.dKey);
    }

    if (size > 0) {
      tfree(pNodes);
    }
  }

  printf("double test end...\n");
  tSkipListDestroy(pSkipList);
}

void stringKeySkiplistTest() {
  const int32_t max_key_size = 12;

  tSkipList *pSkipList = tSkipListCreate(10, TSDB_DATA_TYPE_BINARY, max_key_size);

  tSkipListKey key = tSkipListCreateKey(TSDB_DATA_TYPE_BINARY, "nyse", strlen("nyse"));
  key.nLen = max_key_size;
  char dd[1] = {0};
  tSkipListPut(pSkipList, dd, &key, 1);

  tSkipListKey key1 = tSkipListCreateKey(TSDB_DATA_TYPE_BINARY, "beijing", strlen("beijing"));
  tSkipListPut(pSkipList, dd, &key1, 1);

  tSkipListPrint(pSkipList, 1);

  tSkipListNode **pRes = NULL;
  int32_t         ret = tSkipListGets(pSkipList, &key1, &pRes);

  assert(ret == 1);
  assert(strcmp(pRes[0]->key.pz, "beijing") == 0);
  assert(pRes[0]->key.nType == TSDB_DATA_TYPE_BINARY);

  tSkipListDestroyKey(&key1);
  tSkipListDestroyKey(&key);

  tSkipListDestroy(pSkipList);

  free(pRes);

  int64_t s = taosGetTimestampUs();
  pSkipList = tSkipListCreate(10, TSDB_DATA_TYPE_BINARY, 20);
  char k[256] = {0};
  
  int32_t total = 10000000;
  for(int32_t i = 0; i < total; ++i) {
    int32_t n = sprintf(k, "abc_%d_%d", i, i);
    key = tSkipListCreateKey(TSDB_DATA_TYPE_BINARY, k, n);

    tSkipListPut(pSkipList, " ", &key, 1);
  }

  int64_t e = taosGetTimestampUs();
  printf("elapsed time:%lld us to insert %d data, avg:%f us\n", (e-s), total, (double)(e-s)/total);

  tSkipListNode** pres = NULL;

  s = taosGetTimestampMs();
  for(int32_t j = 0; j < total; ++j) {
    int32_t n = sprintf(k, "abc_%d_%d", j, j);
    key = tSkipListCreateKey(TSDB_DATA_TYPE_BINARY, k, n);

    int32_t num = tSkipListGets(pSkipList, &key, &pres);
    assert(num > 0);

//    tSkipListRemove(pSkipList, &key);
    tSkipListRemoveNode(pSkipList, pres[0]);

    if (num > 0) {
      tfree(pres);
    }
  }

  e = taosGetTimestampMs();
  printf("elapsed time:%lldms\n", e - s);
}

void skiplistPerformanceTest() {
  tSkipList *pSkipList = tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_DOUBLE, sizeof(double));

  int32_t      size = 10000000;
  tSkipListKey key;
  int64_t      prev = taosGetTimestampMs();
  int64_t s = prev;

  for (int32_t i = 0; i < size; ++i) {
    key.dKey = i * 0.997;
    key.nType = TSDB_DATA_TYPE_DOUBLE;
    tSkipListPut(pSkipList, "", &key, 1);

    if (i % 100000 == 0) {
      int64_t cur = taosGetTimestampMs();

      int64_t elapsed = cur - prev;
      printf("add %d, elapsed time: %lld ms, avg elapsed:%f ms, total:%d\n", 100000, elapsed, elapsed / 100000.0, i);
      prev = cur;
    }
  }

  int64_t e = taosGetTimestampMs();
  printf("total:%lld ms, avg:%f\n", e-s, (e-s)/(double)size);
  printf("max level of skiplist:%d, actually level:%d\n ", pSkipList->nMaxLevel, pSkipList->nLevel);
  assert(pSkipList->nSize == size);

  printf("the level of skiplist is:\n");
//  tSkipListPrint(pSkipList, 1);

  printf("level two------------------\n");
  tSkipListPrint(pSkipList, 2);

  printf("level three------------------\n");
  tSkipListPrint(pSkipList, 3);

  printf("level four------------------\n");
  tSkipListPrint(pSkipList, 4);

  printf("level nine------------------\n");
  tSkipListPrint(pSkipList, 10);

  int64_t st = taosGetTimestampMs();
  for (int32_t i = 0; i < 100000; i += 1) {
    key.dKey = i * 0.997;
    tSkipListRemove(pSkipList, &key);
  }

  int64_t et = taosGetTimestampMs();
  printf("delete %d data from skiplist, elapased time:%lldms\n", 10000, et - st);
  assert(pSkipList->nSize == 900000);

  tSkipListDestroy(pSkipList);
}

void duplicatedKeyTest() {
  tSkipListKey key;
  key.nType = TSDB_DATA_TYPE_INT;

  tSkipListNode **pNodes = NULL;

  tSkipList *pSkipList = tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_INT, sizeof(int));

  for(int32_t i = 0; i < 10000; ++i) {
    for(int32_t j = 0; j < 5; ++j) {
      key.i64Key = i;
      tSkipListPut(pSkipList, "", &key, 1);
    }
  }

  tSkipListPrint(pSkipList, 1);

  for(int32_t i = 0; i < 100; ++i) {
    key.i64Key = rand()%1000;
    int32_t size = tSkipListGets(pSkipList, &key, &pNodes);

    assert(size == 5);

    tfree(pNodes);
  }

  tSkipListDestroy(pSkipList);
}
