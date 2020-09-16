#include <gtest/gtest.h>
#include <limits.h>
#include <taosdef.h>
#include <iostream>

#include "os.h"
#include "taosmsg.h"
#include "tskiplist.h"
#include "tutil.h"

namespace {

char* getkey(const void* data) { return (char*)(data); }

void doubleSkipListTest() {
  SSkipList* pSkipList = tSkipListCreate(10, TSDB_DATA_TYPE_DOUBLE, sizeof(double), 0, false, true, getkey);

  double  doubleVal[1000] = {0};
  int32_t size = 20000;

  printf("generated %d keys is: \n", size);

  for (int32_t i = 0; i < size; ++i) {
    if (i < 1000) {
      doubleVal[i] = i * 0.997;
    }

    int32_t level = 0;
    int32_t size = 0;

    tSkipListNewNodeInfo(pSkipList, &level, &size);
    auto d = (SSkipListNode*)calloc(1, size + sizeof(double) * 2);
    d->level = level;

    double* key = (double*)SL_GET_NODE_KEY(pSkipList, d);
    key[0] = i * 0.997;
    key[1] = i * 0.997;

    tSkipListPut(pSkipList, d);
  }

  printf("the first level of skip list is:\n");
  tSkipListPrint(pSkipList, 1);

#if 0
  SSkipListNode **pNodes = NULL;
  SSkipListKey    sk;
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
      taosTFree(pNodes);
    }
  }

#endif

  printf("double test end...\n");
  tSkipListDestroy(pSkipList);
}

void randKeyTest() {
  SSkipList* pSkipList = tSkipListCreate(10, TSDB_DATA_TYPE_INT, sizeof(int32_t), 0, false, true, getkey);

  int32_t size = 200000;
  srand(time(NULL));

  printf("generated %d keys is: \n", size);

  for (int32_t i = 0; i < size; ++i) {
    int32_t level = 0;
    int32_t s = 0;

    tSkipListNewNodeInfo(pSkipList, &level, &s);
    auto d = (SSkipListNode*)calloc(1, s + sizeof(int32_t) * 2);
    d->level = level;

    int32_t* key = (int32_t*)SL_GET_NODE_KEY(pSkipList, d);
    key[0] = rand() % 1000000000;

    key[1] = key[0];

    tSkipListPut(pSkipList, d);
  }

  printf("the first level of skip list is:\n");
  tSkipListPrint(pSkipList, 1);

  printf("the sec level of skip list is:\n");
  tSkipListPrint(pSkipList, 2);

  printf("the 5 level of skip list is:\n");
  tSkipListPrint(pSkipList, 5);

  tSkipListDestroy(pSkipList);
}

void stringKeySkiplistTest() {
  const int32_t max_key_size = 12;

  SSkipList* pSkipList = tSkipListCreate(10, TSDB_DATA_TYPE_BINARY, max_key_size, 0, false, true, getkey);

  int32_t level = 0;
  int32_t headsize = 0;
  tSkipListNewNodeInfo(pSkipList, &level, &headsize);

  auto pNode = (SSkipListNode*)calloc(1, headsize + max_key_size + sizeof(double));
  pNode->level = level;

  char* d = SL_GET_NODE_DATA(pNode);
  strncpy(d, "nyse", 5);

  *(double*)(d + max_key_size) = 12;

  tSkipListPut(pSkipList, pNode);

  tSkipListNewNodeInfo(pSkipList, &level, &headsize);

  pNode = (SSkipListNode*)calloc(1, headsize + max_key_size + sizeof(double));
  pNode->level = level;

  d = SL_GET_NODE_DATA(pNode);
  strncpy(d, "beijing", 8);

  *(double*)(d + max_key_size) = 911;

  tSkipListPut(pSkipList, pNode);

  printf("level one------------------\n");
  tSkipListPrint(pSkipList, 1);

#if 0
  SSkipListNode **pRes = NULL;
  int32_t ret = tSkipListGets(pSkipList, &key1, &pRes);

  assert(ret == 1);
  assert(strcmp(pRes[0]->key.pz, "beijing") == 0);
  assert(pRes[0]->key.nType == TSDB_DATA_TYPE_BINARY);

  tSkipListDestroyKey(&key1);
  tSkipListDestroyKey(&key);

  tSkipListDestroy(pSkipList);

  free(pRes);
#endif

  tSkipListDestroy(pSkipList);

  int64_t s = taosGetTimestampUs();
  pSkipList = tSkipListCreate(10, TSDB_DATA_TYPE_BINARY, 20, 0, false, true, getkey);
  char k[256] = {0};

  int32_t total = 10000;
  for (int32_t i = 0; i < total; ++i) {
    int32_t n = sprintf(k, "abc_%d_%d", i, i);
    tSkipListNewNodeInfo(pSkipList, &level, &headsize);

    auto pNode = (SSkipListNode*)calloc(1, headsize + 20 + sizeof(double));
    pNode->level = level;

    char* d = SL_GET_NODE_DATA(pNode);
    strncpy(d, k, strlen(k));

    tSkipListPut(pSkipList, pNode);
  }

  int64_t e = taosGetTimestampUs();
  printf("elapsed time:%" PRIu64 " us to insert %d data, avg:%f us\n", (e - s), total, (double)(e - s) / total);

  printf("level two------------------\n");
  tSkipListPrint(pSkipList, 1);

#if 0
  SSkipListNode **pres = NULL;

  s = taosGetTimestampMs();
  for (int32_t j = 0; j < total; ++j) {
    int32_t n = sprintf(k, "abc_%d_%d", j, j);
    key = tSkipListCreateKey(TSDB_DATA_TYPE_BINARY, k, n);

    int32_t num = tSkipListGets(pSkipList, &key, &pres);
    assert(num > 0);

    //    tSkipListRemove(pSkipList, &key);
    tSkipListRemoveNode(pSkipList, pres[0]);

    if (num > 0) {
      taosTFree(pres);
    }
  }

  e = taosGetTimestampMs();
  printf("elapsed time:%lldms\n", e - s);
#endif
  tSkipListDestroy(pSkipList);
}

void skiplistPerformanceTest() {
  SSkipList* pSkipList = tSkipListCreate(10, TSDB_DATA_TYPE_DOUBLE, sizeof(double), 0, false, false, getkey);

  int32_t size = 1000000;
  int64_t prev = taosGetTimestampMs();
  int64_t s = prev;

  int32_t level = 0;
  int32_t headsize = 0;

  int32_t unit = MAX_SKIP_LIST_LEVEL * POINTER_BYTES * 2 + sizeof(double) * 2 + sizeof(int16_t);

  char* total = (char*)calloc(1, unit * size);
  char* p = total;

  for (int32_t i = 0; i < size; ++i) {
    tSkipListNewNodeInfo(pSkipList, &level, &headsize);

    SSkipListNode* d = (SSkipListNode*)p;
    p += headsize + sizeof(double) * 2;

    d->level = level;
    double* v = (double*)SL_GET_NODE_DATA(d);
    v[0] = i * 0.997;
    v[1] = i * 0.997;

    tSkipListPut(pSkipList, d);

    if (i % 100000 == 0) {
      int64_t cur = taosGetTimestampMs();

      int64_t elapsed = cur - prev;
      printf("add %d, elapsed time: %" PRIu64 " ms, avg elapsed:%f ms, total:%d\n", 100000, elapsed, elapsed / 100000.0, i);
      prev = cur;
    }
  }

  int64_t e = taosGetTimestampMs();
  printf("total:%" PRIu64 " ms, avg:%f\n", e - s, (e - s) / (double)size);
  printf("max level of skiplist:%d, actually level:%d\n ", pSkipList->maxLevel, pSkipList->level);

  assert(SL_GET_SIZE(pSkipList) == size);

  //  printf("the level of skiplist is:\n");
  //
  //  printf("level two------------------\n");
  //  tSkipListPrint(pSkipList, 2);
  //
  //  printf("level three------------------\n");
  //  tSkipListPrint(pSkipList, 3);
  //
  //  printf("level four------------------\n");
  //  tSkipListPrint(pSkipList, 4);
  //
  //  printf("level nine------------------\n");
  //  tSkipListPrint(pSkipList, 10);

  int64_t st = taosGetTimestampMs();
#if 0
  for (int32_t i = 0; i < 100000; i += 1) {
    key.dKey = i * 0.997;
    tSkipListRemove(pSkipList, &key);
  }
#endif

  int64_t et = taosGetTimestampMs();
  printf("delete %d data from skiplist, elapased time:%" PRIu64 "ms\n", 10000, et - st);
  assert(SL_GET_SIZE(pSkipList) == size);

  tSkipListDestroy(pSkipList);
  taosTFree(total);
}

// todo not support duplicated key yet
void duplicatedKeyTest() {
  SSkipList *pSkipList = tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_INT, sizeof(int), true, false, true, getkey);

  for (int32_t i = 0; i < 200; ++i) {
    for (int32_t j = 0; j < 5; ++j) {
      int32_t level, size;
      tSkipListNewNodeInfo(pSkipList, &level, &size);
      SSkipListNode* d = (SSkipListNode*)calloc(1, size + sizeof(int32_t));
      d->level = level;
      int32_t* key = (int32_t*)SL_GET_NODE_KEY(pSkipList, d);
      key[0] = i;
      tSkipListPut(pSkipList, d);
    }
  }

  for (int32_t i = 0; i < 100; ++i) {
    SSkipListKey key;
    SArray*  nodes = tSkipListGet(pSkipList, (char*)(&i));
    assert( taosArrayGetSize(nodes) == 5 );
    taosArrayDestroy(nodes);
  }

  int32_t key = 101;
  uint32_t num = tSkipListRemove(pSkipList, (char*)(&key));
  assert(num == 5);

  SArray*  nodes = tSkipListGet(pSkipList, (char*)(&key));
  assert( taosArrayGetSize(nodes) == 0 );
  taosArrayDestroy(nodes);

  key = 102;
  SSkipListIterator* iter = tSkipListCreateIterFromVal(pSkipList, (char*)(&key), TSDB_DATA_TYPE_INT, TSDB_ORDER_ASC);
  for(int i = 0; i < 6; i++) {
    assert(tSkipListIterNext(iter) == true);
    SSkipListNode* node = tSkipListIterGet(iter);
    int32_t* val = (int32_t*)SL_GET_NODE_KEY(pSkipList, node);
    assert((i < 5) == ((*val) == key));
  }
  tSkipListDestroyIter(iter);

  iter = tSkipListCreateIterFromVal(pSkipList, (char*)(&key), TSDB_DATA_TYPE_INT, TSDB_ORDER_DESC);
  for(int i = 0; i < 6; i++) {
    assert(tSkipListIterNext(iter) == true);
    SSkipListNode* node = tSkipListIterGet(iter);
    int32_t* val = (int32_t*)SL_GET_NODE_KEY(pSkipList, node);
    assert((i < 5) == ((*val) == key));
  }
  tSkipListDestroyIter(iter);

  tSkipListDestroy(pSkipList);
}

}  // namespace

TEST(testCase, skiplist_test) {
  assert(sizeof(SSkipListKey) == 8);
  srand(time(NULL));

  stringKeySkiplistTest();
  doubleSkipListTest();
  skiplistPerformanceTest();
  duplicatedKeyTest();
  randKeyTest();

  //  tSKipListQueryCond q;
  //  q.upperBndRelOptr = true;
  //  q.lowerBndRelOptr = true;
  //  q.upperBnd.nType = TSDB_DATA_TYPE_DOUBLE;
  //  q.lowerBnd.nType = TSDB_DATA_TYPE_DOUBLE;
  //  q.lowerBnd.dKey = 120;
  //  q.upperBnd.dKey = 171.989;
  /*
      int32_t size = tSkipListQuery(pSkipList, &q, &pNodes);
      for (int32_t i = 0; i < size; ++i) {
          printf("-----%lf\n", pNodes[i]->key.dKey);
      }
      printf("the range query result size is: %d\n", size);
      taosTFree(pNodes);

      SSkipListKey *pKeys = malloc(sizeof(SSkipListKey) * 20);
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
      taosTFree(pNodes);

      free(pKeys);*/
}
