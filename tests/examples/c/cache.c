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

#include "tcache.h"

#include "ehash.h"
#include "tlog.h"

#include <stdio.h>
#include <unistd.h>

static int test1(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, NULL, "test");
  if (!cache) return 1;

  taosCacheCleanup(cache);
  return 0;
}

static const char *test2_key = "hello";
static void test2_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
}

static int test2(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test2_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test2_key, strlen(test2_key), &val, sizeof(val), 10*1000);
    if (!data) { r = 1; D(); break; }
  } while (0);

  taosCacheCleanup(cache);
  return r;
}

static const char *test3_key = "hello";
static int test3_freed = 0;
static void test3_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test3_freed = 1;
}

static int test3(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test3_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test3_key, strlen(test3_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }
    taosCacheRelease(cache, &data, false);
    if (data) { r = 2; D(); break; }
    if (test3_freed) { r = 3; D(); break; }
  } while (0);

  taosCacheCleanup(cache);
  return r;
}

static const char *test4_key = "hello";
static int test4_freed = 0;
static void test4_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test4_freed = 1;
}

static int test4(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test4_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test4_key, strlen(test4_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }
    void *d = taosCacheAcquireByKey(cache, test4_key, strlen(test4_key));
    if (d != data) { r = 2; D(); break; }
    taosCacheRelease(cache, &data, false);
    if (data) { r = 3; D(); break; }
    if (test4_freed) { r = 4; D(); break; }
  } while (0);

  taosCacheCleanup(cache);
  return r;
}

static const char *test5_key = "hello";
static int test5_freed = 0;
static void test5_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test5_freed = 1;
}

static int test5(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test5_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test5_key, strlen(test5_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }
    taosCacheRelease(cache, &data, true);
    if (data) { r = 3; D(); break; }
    if (!test5_freed) { r = 4; D(); break; }
  } while (0);

  taosCacheCleanup(cache);
  return r;
}

static const char *test6_key = "hello";
static int test6_freed = 0;
static void test6_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test6_freed = 1;
}

static int test6(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test6_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test6_key, strlen(test6_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }
    taosCacheRelease(cache, &data, false);
    if (data) { r = 3; D(); break; }
    if (test6_freed) { r = 4; D(); break; }
    data = taosCacheAcquireByKey(cache, test6_key, strlen(test6_key));
    if (!data) { r = 5; D(); break; }
  } while (0);

  taosCacheCleanup(cache);
  if (test6_freed!=1) r = 6;
  return r;
}

static const char *test7_key = "hello";
static int test7_freed = 0;
static void test7_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test7_freed += 1;
}

static int test7(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test7_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test7_key, strlen(test7_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }
    taosCacheRelease(cache, &data, false);
    if (data) { r = 3; D(); break; }
    if (test7_freed) { r = 4; D(); break; }
    data = taosCacheAcquireByKey(cache, test7_key, strlen(test7_key));
    if (!data) { r = 5; D(); break; }

    val = strdup("foo");
    data = taosCachePut(cache, "bar", strlen("bar"), &val, sizeof(val), 1000);
    if (!data) { r = 6; D(); break; }
    if (test7_freed) { r = 7; D(); break; }

  } while (0);

  taosCacheCleanup(cache);
  if (test7_freed!=2) r = 8;
  return r;
}

static const char *test8_key = "hello";
static int test8_freed = 0;
static void test8_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test8_freed += 1;
}

static int test8(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test8_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test8_key, strlen(test8_key), &val, sizeof(val), 10*1000);
    if (!data) { r = 1; D(); break; }
    void *d = taosCacheAcquireByKey(cache, test8_key, strlen(test8_key));
    if (d != data) { r = 2; D(); break; }
  } while (0);

  taosCacheCleanup(cache);
  if (test8_freed!=1) r = 3;
  return r;
}

static const char *test9_key = "hello";
static int test9_freed = 0;
static void test9_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test9_freed += 1;
}

static int test9(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test9_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test9_key, strlen(test9_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }

    char *val9 = strdup("foo");
    void *data9 = taosCachePut(cache, "test9_key", strlen("test9_key"), &val9, sizeof(val9), 1000);
    if (!data9) { r = 2; D(); break; }

    if (test9_freed) { r = 3; D(); break; }

    if (strcmp("world", *(char**)data)) { r = 4; D(); break; }
    if (strcmp("foo", *(char**)data9)) { r = 5; D(); break; }

    void *d = taosCacheAcquireByKey(cache, test9_key, strlen(test9_key));
    if (!d) { r = 6; D(); break; }
    if (strcmp("world", *(char**)d)) { r = 6; D(); break; }
  } while (0);

  taosCacheCleanup(cache);
  if (test9_freed!=2) r = 9;
  return r;
}

static const char *test10_key = "hello";
static int test10_freed = 0;
static void test10_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test10_freed += 1;
}

static int test10(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test10_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test10_key, strlen(test10_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }

    char *val10 = strdup("foo");
    void *data10 = taosCachePut(cache, test10_key, strlen(test10_key), &val10, sizeof(val10), 1000);
    if (!data10) { r = 2; D(); break; }

    if (test10_freed) { r = 3; D(); break; }

    if (strcmp("world", *(char**)data)) { r = 4; D(); break; }
    if (strcmp("foo", *(char**)data10)) { r = 5; D(); break; }

    void *d = taosCacheAcquireByKey(cache, test10_key, strlen(test10_key));
    if (!d) { r = 6; D(); break; }
    if (strcmp("foo", *(char**)d)) { r = 6; D(); break; }
  } while (0);

  taosCacheCleanup(cache);
  if (test10_freed!=2) r = 10;
  return r;
}

static const char *test11_key = "hello";
static int test11_freed = 0;
static void test11_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test11_freed += 1;
}

static int test11(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test11_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test11_key, strlen(test11_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }
    taosCacheRelease(cache, &data, false);
    if (data) { r = 2; D(); break; }
    if (test11_freed) { r = 3; D(); break; }
    while (test11_freed==0) {
      ;
    }
  } while (0);

  taosCacheCleanup(cache);
  if (test11_freed!=1) r = 4;
  return r;
}

static const char *test12_key = "hello";
static int test12_freed = 0;
static void test12_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test12_freed += 1;
}

static int test12(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test12_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test12_key, strlen(test12_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }
    if (test12_freed) { r = 4; D(); break; }
    while (test12_freed==0) {
      ;
    }
  } while (0);

  taosCacheCleanup(cache);
  if (test12_freed!=1) r = 4;
  return r;
}

static const char *test13_key = "hello";
static int test13_freed = 0;
static void test13_free(void* arg) {
  char *p = *(char**)arg;
  free(p);
  test13_freed += 1;
}

static int test13(void) {
  D();
  SCacheObj *cache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 3, false, test13_free, "test");
  if (!cache) return 1;

  int r = 0;
  do {
    char *val = strdup("world");
    void *data = taosCachePut(cache, test13_key, strlen(test13_key), &val, sizeof(val), 1000);
    if (!data) { r = 1; D(); break; }
    taosCacheRelease(cache, &data, false);
    if (data) { r = 3; D(); break; }
    if (test13_freed) { r = 4; D(); break; }
  } while (0);

  taosCacheCleanup(cache);
  if (test13_freed!=1) r = 4;
  return r;
}

int test_cache(void) {
  if (test1()) return 1;
  if (test2()) return 2;
  if (test3()) return 3;
  if (test4()) return 4;
  if (test5()) return 5;
  if (test6()) return 6;
  if (test7()) return 7;
  if (test8()) return 8;
  if (test9()) return 9;
  if (test10()) return 10;
  if (test11()) return 11;
  if (0 && test12()) return 12;
  if (test13()) return 13;
  return 0;
}

