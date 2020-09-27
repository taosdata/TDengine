/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */


#include "hash.h"

#include "ehash.h"
#include "tlog.h"

#include <stdio.h>

static uint32_t hashing(const char *k, uint32_t v) {
  return MurmurHash3_32(k, v);
}

static void test1_cb2(void *p) {
}

static int test1_cb3_succ = 1;
static bool test1_cb3(void *param, void *data) {
  SHashObj *obj = (SHashObj*)param;
  char key[] = "key";
  void *v = taosHashGet(obj, key, strlen(key));
  if (!v || *(int32_t*)v!=32) {
    test1_cb3_succ = 0;
    return false;
  }
  return true;
}

static int test1_cb4_succ = 1;
static bool test1_cb4(void *param, void *data) {
  SHashObj *obj = (SHashObj*)param;
  for (int i=0; i<10001; ++i) {
    int j = i;
    char key[64];
    snprintf(key, sizeof(key), "key%d", j);
    int32_t r = taosHashPut(obj, key, strlen(key), &j, sizeof(j));
    if (r) {
      test1_cb4_succ = 0;
      return false;
    }
  }
  return false;
}

static int test1(void) {
  D();
  SHashObj *obj = taosHashInit(10, hashing, false, HASH_ENTRY_LOCK);
  if (!obj) return 1;

  // for (int i=0; i<10001; ++i) {
  //   int j = i;
  //   char key[64];
  //   snprintf(key, sizeof(key), "key%d", j);
  //   int32_t r = taosHashPut(obj, key, strlen(key), &j, sizeof(j));
  //   if (r) {
  //     test1_cb4_succ = 0;
  //     return 1;
  //   }
  // }
  // if (1) {
  //   taosHashCleanup(obj);
  //   return 0;
  // }

  char key[] = "key";
  int val = 32;
  int32_t r = taosHashPut(obj, key, strlen(key), &val, sizeof(val));
  if (r) return 2;
  void *v = taosHashGet(obj, key, strlen(key));
  if (!v || *(int32_t*)v!=val) return 3;

  int32_t vv = 0;
  v = taosHashGetCB(obj, key, strlen(key), test1_cb2, &vv, sizeof(vv));
  if (vv != val) return 4;

  if (0) {
    r = taosHashCondTraverse(obj, test1_cb3, obj);
    if (!test1_cb3_succ) return 5;
  }

  if (0) {
    r = taosHashCondTraverse(obj, test1_cb4, obj);
    if (!test1_cb4_succ) return 5;
  }

  taosHashCleanup(obj);
  return 0;
}

static int test2(void) {
  D();
  int r = 1;
  SHashObj *obj = taosHashInit(10, hashing, false, HASH_ENTRY_LOCK);
  if (!obj) return 1;

  SHashMutableIterator* iter = NULL;
  do {
    iter = taosHashCreateIter(obj);
    if (!iter) break;
    const char *key = "hello";
    const char *val = "world";
    r = taosHashPut(obj, key, strlen(key), &val, sizeof(val));
  } while (0);

  if (iter) taosHashDestroyIter(iter);

  taosHashCleanup(obj);
  return r;
}

static int test3(void) {
  D();
  int r = 0;
  SHashObj *obj = taosHashInit(10, hashing, false, HASH_ENTRY_LOCK);
  if (!obj) return 1;

  do {
    char key[] = "key";
    char *val = strdup("world");
    r = taosHashPut(obj, key, strlen(key), &val, sizeof(val));
    if (r) { r = 1; D(); break; }
    r = taosHashRemove(obj, key, strlen(key));
    if (r) { r = 2; D(); break; }
    free(val);
  } while (0);

  taosHashCleanup(obj);
  return r;
}

static int test4(void) {
  D();
  int r = 0;
  SHashObj *obj = taosHashInit(10, hashing, false, HASH_ENTRY_LOCK);
  if (!obj) return 1;

  do {
    char key[] = "key";
    char *val = strdup("world");
    r = taosHashPut(obj, key, strlen(key), &val, sizeof(val));
    if (r) { r = 1; D(); break; }
    r = taosHashRemove(obj, key, strlen(key));
    if (r) { r = 2; D(); break; }
    void *v = taosHashGet(obj, key, strlen(key));
    if (v) { r = 3; D(); break; }
    free(val);
  } while (0);

  taosHashCleanup(obj);
  return r;
}

static int test5(void) {
  D();
  int r = 0;
  SHashObj *obj = taosHashInit(10, hashing, false, HASH_ENTRY_LOCK);
  if (!obj) return 1;

  do {
    char key[] = "key";
    char *val = strdup("world");
    r = taosHashPut(obj, key, strlen(key), &val, sizeof(val));
    if (r) { r = 1; D(); break; }

    // mock data-race
    void *v = taosHashGet(obj, key, strlen(key));
    if (!v || *(char**)v != val) { r = 2; D(); break; }
    fprintf(stderr, "v: [%s]\n", *(char**)v);

    r = taosHashRemove(obj, key, strlen(key));
    if (r) { r = 3; D(); break; }
    val[0] = 'a';
    free(val);

    // back to v
    // v has already been freed
    fprintf(stderr, "v: [%s]\n", *(char**)v);
  } while (0);

  taosHashCleanup(obj);
  return r;
}

static void test6_put(SHashObj *obj, size_t n) {
}

static int test6(void) {
  D();
  int r = 0;
  SHashObj *obj = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  if (!obj) return 1;

  do {
    test6_put(obj, 1000);
  } while (0);

  taosHashCleanup(obj);
  return r;
}

int test_hash(void) {
  if (test1()) return 1;
  if (test2()) return 2;
  if (test3()) return 3;
  if (test4()) return 4;
  if (0 && test5()) return 5;
  if (test6()) return 6;
  return 0;
}

