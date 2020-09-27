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


#include "ehash.h"

#include "tlog.h"

#include <stdio.h>
#include <string.h>

static uint32_t hashing(const char *k, uint32_t v) {
  return MurmurHash3_32(k, v);
}

static int test1(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  ehash_destroy(obj);
  return 0;
}

static const char *test2_key = "hello";
static const char *test2_val = "world";
static int test2_freed = 0;

static void test2_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  if (strcmp(s, test2_val)) return;
  free(s);
  test2_freed += 1;
}

static int test2(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;
  do {
    char *v = strdup("world");
    ehash_node_val_t val;
    val.buf      = (char*)&v;
    val.blen     = sizeof(v);
    val.free_cb  = test2_free_val;

    ehash_node_t *node = ehash_put(obj, test2_key, strlen(test2_key), val);
    if (!node) { D(); break; }
    if (memcmp(ehash_node_get_key(node), test2_key, strlen(test2_key))) {
      D(); break;
    }
    const char *buf  = ehash_node_get_buf(node);
    size_t      blen = ehash_node_get_blen(node);
    if (blen != sizeof(v)) {
      D(); break;
    }
    if (v != *(const char **)buf) {
      D(); break;
    }

    ehash_node_release(node);
    if (test2_freed) { D(); break; }
    r = 0;
  } while (0);
  ehash_destroy(obj);

  if (test2_freed!=1) { D(); return 1; }

  return r;
}

static const char *test3_key = "hello";
static const char *test3_val = "world";
static int test3_freed = 0;

static void test3_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  if (strcmp(s, test3_val)) return;
  free(s);
  test3_freed += 1;
}

static int test3(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;
  do {
    char *v = strdup("world");
    ehash_node_val_t val;
    val.buf      = (char*)&v;
    val.blen     = sizeof(v);
    val.free_cb  = test3_free_val;

    ehash_node_t *node = ehash_put(obj, test3_key, strlen(test3_key), val);
    if (!node) { D(); break; }

    ehash_node_release(node);
    if (test3_freed) { D(); break; }

    ehash_remove(obj, test3_key, strlen(test3_key));
    if (test3_freed!=1) { D(); break; }

    r = 0;
  } while (0);
  ehash_destroy(obj);

  if (test3_freed!=1) { D(); return 1; }

  return r;
}

static const char *test4_key = "hello";
static const char *test4_val = "world";
static int test4_freed = 0;

static void test4_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  if (strcmp(s, test4_val)) return;
  free(s);
  test4_freed += 1;
}

static int test4(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;
  do {
    char *v = strdup("world");
    ehash_node_val_t val;
    val.buf      = (char*)&v;
    val.blen     = sizeof(v);
    val.free_cb  = test4_free_val;

    ehash_node_t *node = ehash_put(obj, test4_key, strlen(test4_key), val);
    if (!node) { D(); break; }

    ehash_node_release(node);
    if (test4_freed) { D(); break; }

    ehash_remove(obj, test4_key, strlen(test4_key));
    if (test4_freed!=1) { D(); break; }

    node = ehash_get(obj, test4_key, strlen(test4_key));
    if (node) { D(); break; }

    r = 0;
  } while (0);
  ehash_destroy(obj);

  if (test4_freed!=1) { D(); return 1; }

  return r;
}

static const char *test5_key = "hello";
static const char *test5_val = "world";
static int test5_freed = 0;

static void test5_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  if (strcmp(s, test5_val)) return;
  free(s);
  test5_freed += 1;
}

static int test5(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;
  do {
    char *v = strdup("world");
    ehash_node_val_t val;
    val.buf      = (char*)&v;
    val.blen     = sizeof(v);
    val.free_cb  = test5_free_val;

    ehash_node_t *node = ehash_put(obj, test5_key, strlen(test5_key), val);
    if (!node) { D(); break; }

    ehash_remove(obj, test5_key, strlen(test5_key));
    if (test5_freed) { D(); break; }
    if (memcmp(ehash_node_get_key(node), test5_key, strlen(test5_key))) {
      r = 3; D(); break;
    }
    const char *buf  = ehash_node_get_buf(node);
    size_t      blen = ehash_node_get_blen(node);
    if (blen != sizeof(v)) {
      D(); break;
    }
    if (v != *(const char **)buf) {
      D(); break;
    }

    ehash_node_t *node1 = ehash_get(obj, test5_key, strlen(test5_key));
    if (node1) { D(); break; }
    if (test5_freed) { D(); break; }

    ehash_node_release(node);
    if (test5_freed!=1) { D(); break; }

    r = 0;
  } while (0);
  ehash_destroy(obj);

  if (test5_freed!=1) { D(); return 1; }

  return r;
}

static const char *test6_key = "hello";
static int test6_freed = 0;

static void test6_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test6_freed += 1;
}

static int test6(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;
  do {
    char *v = strdup("world");
    ehash_node_val_t val;
    val.buf      = (char*)&v;
    val.blen     = sizeof(v);
    val.free_cb  = test6_free_val;

    ehash_node_t *node = ehash_put(obj, test6_key, strlen(test6_key), val);
    if (!node) { D(); break; }

    {
      char *v = strdup("foobar");
      ehash_node_val_t val;
      val.buf      = (char*)&v;
      val.blen     = sizeof(v);
      val.free_cb  = test6_free_val;

      ehash_node_t *node = ehash_put(obj, test6_key, strlen(test6_key), val);
      if (!node) { D(); break; }
      if (memcmp(ehash_node_get_key(node), test6_key, strlen(test6_key))) {
        D(); break;
      }
      const char *buf  = ehash_node_get_buf(node);
      size_t      blen = ehash_node_get_blen(node);
      if (blen != sizeof(v)) {
        D(); break;
      }
      if (v != *(const char **)buf) {
        D(); break;
      }

      ehash_node_release(node);
      if (test6_freed) { D(); break; }
    }

    ehash_remove(obj, test6_key, strlen(test6_key));
    if (test6_freed!=1) { D(); break; }
    if (memcmp(ehash_node_get_key(node), test6_key, strlen(test6_key))) {
      D(); break;
    }
    const char *buf  = ehash_node_get_buf(node);
    size_t      blen = ehash_node_get_blen(node);
    if (blen != sizeof(v)) {
      D(); break;
    }
    if (v != *(const char **)buf) {
      D(); break;
    }

    ehash_node_t *node1 = ehash_get(obj, test6_key, strlen(test6_key));
    if (node1) { D(); break; }
    if (test6_freed!=1) { D(); break; }

    ehash_node_release(node);
    if (test6_freed!=2) { D(); break; }

    r = 0;
  } while (0);
  ehash_destroy(obj);

  if (test6_freed!=2) { D(); return 1; }

  return r;
}

static int test7_freed = 0;
static void test7_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test7_freed += 1;
}

static void test7_visit(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop) {
  *keep = 1;
}

static void test7_visit1(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop) {
  *keep = 0;
}

static int test7_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test7_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test7(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test7_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_traverse(obj, test7_visit, NULL);
    if (ehash_size(obj)!=1000) { D(); break; }
    if (test7_freed) { D(); break; }

    ehash_traverse(obj, test7_visit1, NULL);
    if (ehash_size(obj)!=0) { D(); break; }
    if (test7_freed!=1000) { D(); break; }

    r = 0;
  } while (0);

  ehash_destroy(obj);

  if (test7_freed!=1000) { D(); return 1; }

  return r;
}

static int test8_freed = 0;
static void test8_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test8_freed += 1;
}

static void test8_visit(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop) {
  *keep = 1;
}

static void test8_visit1(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop) {
  static int tick = 0;
  ehash_remove(obj, kv.key, kv.klen);
  ++tick;
  if (tick%2) {
    *keep = 1;
  } else {
    *keep = 0;
  }
}

static int test8_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test8_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test8(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test8_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_traverse(obj, test8_visit, NULL);
    if (ehash_size(obj)!=1000) { D(); break; }
    if (test8_freed) { D(); break; }

    ehash_traverse(obj, test8_visit1, NULL);
    if (ehash_size(obj)) { D(); break; }
    if (test8_freed!=1000) { D(); break; }

    r = 0;
  } while (0);

  ehash_destroy(obj);
  if (test8_freed!=1000) { D(); return 1; }

  return r;
}

static int test9_freed = 0;
static void test9_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test9_freed += 1;
}

static void test9_visit(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop) {
  *keep = 1;
}

static void test9_visit1(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop) {
  ehash_remove(obj, "key0001", strlen("key0001"));
  *keep = 1;
}

static int test9_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test9_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test9(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test9_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_traverse(obj, test9_visit, NULL);
    if (ehash_size(obj)!=1000) { D(); break; }
    if (test9_freed) { D(); break; }

    ehash_traverse(obj, test9_visit1, NULL);
    if (ehash_size(obj)!=999) { D(); break; }
    if (test9_freed!=1) { D(); break; }

    r = 0;
  } while (0);

  ehash_destroy(obj);
  if (test9_freed!=1000) { D(); return 1; }

  return r;
}

static int test10_freed = 0;
static void test10_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test10_freed += 1;
}

static void test10_visit(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop) {
  *keep = 1;
}

static int test10_failed = 0;

static void test10_visit1(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop) {
  static int tick = 0;
  if (tick==0) {
    ehash_remove(obj, "key0011", strlen("key0011"));
    if (ehash_size(obj)!=999) {
      test10_failed = 1;
    }
  }
  ++tick;
  *keep = 1;
}

static int test10_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test10_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test10(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test10_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_traverse(obj, test10_visit, NULL);
    if (ehash_size(obj)!=1000) { D(); break; }
    if (test10_freed) { D(); break; }
    if (test10_failed) { D(); break; }

    ehash_traverse(obj, test10_visit1, NULL);
    if (ehash_size(obj)!=999) { D(); break; }
    if (test10_freed!=1) { D(); break; }
    if (test10_failed) { D(); break; }

    r = 0;
  } while (0);

  ehash_destroy(obj);
  if (test10_freed!=1000) { D(); return 1; }

  return r;
}

static int test11_freed = 0;
static void test11_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test11_freed += 1;
}

static int test11_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test11_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test11(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test11_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_iter_t *iter = ehash_iter_create(obj);
    if (!iter) { D(); break; }

    do {
      ehash_node_kv_t kv = {0};
      while (ehash_iter_next(iter, &kv)) {
        ;
      }
      r = 0;
    } while (0);
    ehash_iter_destroy(iter);

    if (r) break;

    r = 1;

    r = 0;
  } while (0);

  ehash_destroy(obj);
  if (test11_freed!=1000) { D(); return 1; }

  return r;
}

static int test12_freed = 0;
static void test12_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test12_freed += 1;
}

static int test12_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test12_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test12(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test12_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_iter_t *iter = ehash_iter_create(obj);
    if (!iter) { D(); break; }

    do {
      ehash_node_kv_t kv = {0};
      int tick = 0;
      int pos = 10;
      while (ehash_iter_next(iter, &kv)) {
        if (tick==pos) {
          ehash_remove(obj, kv.key, kv.klen);
          if (ehash_size(obj)!=1000) { D(); break; }
        } else if (tick<pos) {
            if (ehash_size(obj)!=1000) { D(); break; }
        } else {
            if (ehash_size(obj)!=999) { D(); break; }
        }
        ++tick;
      }
      if (tick!=1000) break;
      r = 0;
    } while (0);
    ehash_iter_destroy(iter);

    if (r) break;

    r = 1;
    if (ehash_size(obj)!=999) { D(); break; }

    r = 0;
  } while (0);

  ehash_destroy(obj);
  if (test12_freed!=1000) { D(); return 1; }

  return r;
}

static int test13_freed = 0;
static void test13_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test13_freed += 1;
}

static int test13_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test13_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test13(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test13_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_iter_t *iter = ehash_iter_create(obj);
    if (!iter) { D(); break; }

    do {
      ehash_node_kv_t kv = {0};
      int tick = 0;
      const char *key = "key0000";
      while (ehash_iter_next(iter, &kv)) {
        ehash_remove(obj, key, strlen(key));
        if (strcmp(kv.key, key)==0) {
          if (ehash_size(obj)!=1000) { D(); break; }
        } else {
          if (ehash_size(obj)!=999) { D(); break; }
        }
        ++tick;
      }
      if (tick!=1000) break;
      r = 0;
    } while (0);
    ehash_iter_destroy(iter);

    if (r) break;

    r = 1;
    if (ehash_size(obj)!=999) { D(); break; }

    r = 0;
  } while (0);

  ehash_destroy(obj);
  if (test13_freed!=1000) { D(); return 1; }

  return r;
}

static int test14_freed = 0;
static void test14_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test14_freed += 1;
}

static int test14_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test14_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test14(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test14_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_iter_t *iter = ehash_iter_create(obj);
    if (!iter) { D(); break; }

    do {
      ehash_node_kv_t kv = {0};
      int tick = 0;
      const char *key = "key0000";
      while (ehash_iter_next(iter, &kv)) {
        ehash_remove(obj, key, strlen(key));
        if (strcmp(kv.key, key)==0) {
          if (ehash_size(obj)!=1000) { D(); break; }
        } else {
          if (ehash_size(obj)!=999) { D(); break; }
        }
        ++tick;
        if (tick==500) break;
      }

      if (tick!=500) break;
      r = 0;
    } while (0);
    ehash_iter_destroy(iter);

    if (r) break;

    r = 1;
    if (ehash_size(obj)!=999) { D(); break; }

    r = 0;
  } while (0);

  ehash_destroy(obj);
  if (test14_freed!=1000) { D(); return 1; }

  return r;
}

static int test15_freed = 0;
static void test15_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test15_freed += 1;
}

static int test15_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test15_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test15(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test15_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_iter_t *iter = ehash_iter_create(obj);
    if (!iter) { D(); break; }

    do {
      ehash_node_kv_t kv = {0};
      int tick = 0;
      const char *key = "key0000";
      while (ehash_iter_next(iter, &kv)) {
        ehash_remove(obj, key, strlen(key));
        if (strcmp(kv.key, key)==0) {
          if (ehash_size(obj)!=1000) { D(); break; }
        } else {
          if (ehash_size(obj)!=999) { D(); break; }
        }
        ++tick;
        if (tick==500) break;
      }

      if (tick!=500) break;
      r = 0;
    } while (0);
    ehash_iter_destroy(iter);

    if (r) break;

    r = 1;
    if (ehash_size(obj)!=999) { D(); break; }

    r = 0;
  } while (0);

  ehash_destroy(obj);
  if (test15_freed!=1000) { D(); return 1; }

  return r;
}

static int test16_freed = 0;
static void test16_free_val(ehash_obj_t *obj, const char *buf, size_t blen) {
  char *s = NULL;
  if (blen != sizeof(s)) return;
  s= *(char**)buf;
  free(s);
  test16_freed += 1;
}

static int test16_put(ehash_obj_t *obj, size_t n) {
  int r = 0;
  for (int i=0; i<n; ++i) {
    char key[64];
    snprintf(key, sizeof(key), "key%04d", i);
    char *val = strdup(key);
    const char *s = "val";
    strncpy(val, s, 3);
    ehash_node_val_t v = {0};
    v.buf     = (const char*)&val;
    v.blen    = sizeof(val);
    v.free_cb = test16_free_val;
    ehash_node_t *node = ehash_put(obj, key, strlen(key), v);
    if (!node) { r = 1; D(); break; }
    ehash_node_release(node);
  }

  return r;
}

static int test16_predict(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv) {
  const char *key = (const char*)arg;
  if (strcmp(key, kv.key)==0) return 1;
  return 0;
}

static int test16(void) {
  D();
  ehash_conf_t conf = {0};
  conf.capacity       = 10;
  conf.fn             = hashing;
  conf.enableUpdate   = 1;
  ehash_obj_t *obj = ehash_create(conf);
  if (!obj) return 1;

  int r = 1;

  do {
    if (test16_put(obj, 1000)) { D(); break; }
    if (ehash_size(obj)!=1000) { D(); break; }

    ehash_node_t *node = ehash_query(obj, test16_predict, "key0010");
    if (!node) { D(); break; }
    ehash_node_release(node);
    if (test16_freed) { D(); break; }

    r = 0;
  } while (0);

  ehash_destroy(obj);
  if (test16_freed!=1000) { D(); return 1; }

  return r;
}

int test_ehash(void) {
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
  if (test12()) return 12;
  if (test13()) return 13;
  if (test14()) return 14;
  if (test15()) return 15;
  if (test16()) return 16;
  return 0;
}

