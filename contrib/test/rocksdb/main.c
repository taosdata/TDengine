#include <assert.h>
#include <bits/stdint-uintn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>  // sysconf() - get CPU count
#include "rocksdb/c.h"

// const char DBPath[] = "/tmp/rocksdb_c_simple_example";
const char DBPath[] = "rocksdb_c_simple_example";
const char DBBackupPath[] = "/tmp/rocksdb_c_simple_example_backup";

#define POINTER_SHIFT(p, b) ((void *)((char *)(p) + (b)))
static int32_t taosEncodeFixedU64(void **buf, uint64_t value) {
  if (buf != NULL) {
    ((uint8_t *)(*buf))[0] = value & 0xff;
    ((uint8_t *)(*buf))[1] = (value >> 8) & 0xff;
    ((uint8_t *)(*buf))[2] = (value >> 16) & 0xff;
    ((uint8_t *)(*buf))[3] = (value >> 24) & 0xff;
    ((uint8_t *)(*buf))[4] = (value >> 32) & 0xff;
    ((uint8_t *)(*buf))[5] = (value >> 40) & 0xff;
    ((uint8_t *)(*buf))[6] = (value >> 48) & 0xff;
    ((uint8_t *)(*buf))[7] = (value >> 56) & 0xff;
    *buf = POINTER_SHIFT(*buf, sizeof(value));
  }
  return (int32_t)sizeof(value);
}

static void *taosDecodeFixedU64(const void *buf, uint64_t *value) {
  ((uint8_t *)value)[7] = ((uint8_t *)buf)[0];
  ((uint8_t *)value)[6] = ((uint8_t *)buf)[1];
  ((uint8_t *)value)[5] = ((uint8_t *)buf)[2];
  ((uint8_t *)value)[4] = ((uint8_t *)buf)[3];
  ((uint8_t *)value)[3] = ((uint8_t *)buf)[4];
  ((uint8_t *)value)[2] = ((uint8_t *)buf)[5];
  ((uint8_t *)value)[1] = ((uint8_t *)buf)[6];
  ((uint8_t *)value)[0] = ((uint8_t *)buf)[7];
  return POINTER_SHIFT(buf, sizeof(*value));
}

typedef struct KV {
  uint64_t k1;
  uint64_t k2;
} KV;

int kvSerial(KV *kv, char *buf) {
  int len = 0;
  buf[0] = 'a';
  buf += 1;
  len += taosEncodeFixedU64((void **)&buf, kv->k1);
  len += taosEncodeFixedU64((void **)&buf, kv->k2);
  return len;
}
const char *kvDBName(void *name) { return "kvDBname"; }
int         kvDBComp(void *state, const char *aBuf, size_t aLen, const char *bBuf, size_t bLen) {
  KV w1, w2;

  memset(&w1, 0, sizeof(w1));
  memset(&w2, 0, sizeof(w2));

  char *p1 = (char *)aBuf;
  char *p2 = (char *)bBuf;
  p1 += 1;
  p2 += 1;

  p1 = taosDecodeFixedU64(p1, &w1.k1);
  p2 = taosDecodeFixedU64(p2, &w2.k1);

  p1 = taosDecodeFixedU64(p1, &w1.k2);
  p2 = taosDecodeFixedU64(p2, &w2.k2);

  if (w1.k1 < w2.k1) {
    return -1;
  } else if (w1.k1 > w2.k1) {
    return 1;
  }

  if (w1.k2 < w2.k2) {
    return -1;
  } else if (w1.k2 > w2.k2) {
    return 1;
  }
  return 0;
}
int kvDeserial(KV *kv, char *buf) {
  char *p1 = (char *)buf;
  p1 += 1;
  p1 = taosDecodeFixedU64(p1, &kv->k1);
  p1 = taosDecodeFixedU64(p1, &kv->k2);

  return 0;
}

int main(int argc, char const *argv[]) {
  rocksdb_t               *db;
  rocksdb_backup_engine_t *be;

  char       *err = NULL;
  const char *path = "/tmp/db";

  rocksdb_options_t *opt = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(opt, 1);
  rocksdb_options_set_create_missing_column_families(opt, 1);

  const char *cfName[] = {"default", "cf1"};
  int         len = sizeof(cfName) / sizeof(cfName[0]);

  const rocksdb_options_t **cfOpt = malloc(len * sizeof(rocksdb_options_t *));
  for (int i = 0; i < len; i++) {
    cfOpt[i] = opt;
  }

  rocksdb_column_family_handle_t **cfHandle = malloc(len * sizeof(rocksdb_column_family_handle_t *));
  db = rocksdb_open_column_families(opt, "test", len, cfName, cfOpt, cfHandle, &err);

  {
    rocksdb_readoptions_t *rOpt = rocksdb_readoptions_create();
    size_t                 vlen = 0;

    char *v = rocksdb_get_cf(db, rOpt, cfHandle[0], "key", strlen("key"), &vlen, &err);
    printf("Get value %s, and len = %d\n", v, (int)vlen);

    char *v1 = rocksdb_get_cf(db, rOpt, cfHandle[1], "key", strlen("key"), &vlen, &err);
    printf("Get value %s, and len = %d\n", v1, (int)vlen);
    rocksdb_readoptions_destroy(rOpt);
  }

  rocksdb_writeoptions_t *wOpt = rocksdb_writeoptions_create();
  rocksdb_writebatch_t   *wBatch = rocksdb_writebatch_create();
  rocksdb_writebatch_put_cf(wBatch, cfHandle[0], "key", strlen("key"), "value", strlen("value"));
  rocksdb_write(db, wOpt, wBatch, &err);

  rocksdb_readoptions_t *rOpt = rocksdb_readoptions_create();
  size_t                 vlen = 0;

  char *v = rocksdb_get_cf(db, rOpt, cfHandle[0], "key", strlen("key"), &vlen, &err);
  printf("Get value %s, and len = %d\n", v, (int)vlen);

  rocksdb_column_family_handle_destroy(cfHandle[0]);
  rocksdb_column_family_handle_destroy(cfHandle[1]);
  rocksdb_close(db);

  // {
  //   // rocksdb_options_t *Options = rocksdb_options_create();
  //   db = rocksdb_open(comm, path, &err);
  //   if (db != NULL) {
  //     rocksdb_options_t    *cfo = rocksdb_options_create_copy(comm);
  //     rocksdb_comparator_t *cmp1 = rocksdb_comparator_create(NULL, NULL, kvDBComp, kvDBName);
  //     rocksdb_options_set_comparator(cfo, cmp1);

  //     rocksdb_column_family_handle_t *handle = rocksdb_create_column_family(db, cfo, "cf1", &err);

  //     rocksdb_column_family_handle_destroy(handle);
  //     rocksdb_close(db);
  //     db = NULL;
  //   }
  // }

  // int ncf = 2;

  // rocksdb_column_family_handle_t **pHandle = malloc(ncf * sizeof(rocksdb_column_family_handle_t *));

  // {
  //   rocksdb_options_t *options = rocksdb_options_create_copy(comm);

  //   rocksdb_comparator_t *cmp1 = rocksdb_comparator_create(NULL, NULL, kvDBComp, kvDBName);
  //   rocksdb_options_t    *dbOpts1 = rocksdb_options_create_copy(comm);
  //   rocksdb_options_t    *dbOpts2 = rocksdb_options_create_copy(comm);
  //   rocksdb_options_set_comparator(dbOpts2, cmp1);
  //   // rocksdb_column_family_handle_t *cf = rocksdb_create_column_family(db, dbOpts1, "cmp1", &err);

  //   const char *pName[] = {"default", "cf1"};

  //   const rocksdb_options_t **pOpts = malloc(ncf * sizeof(rocksdb_options_t *));
  //   pOpts[0] = dbOpts1;
  //   pOpts[1] = dbOpts2;

  //   rocksdb_options_t *allOptions = rocksdb_options_create_copy(comm);
  //   db = rocksdb_open_column_families(allOptions, "test", ncf, pName, pOpts, pHandle, &err);
  // }

  // // rocksdb_options_t *options = rocksdb_options_create();
  // // rocksdb_options_set_create_if_missing(options, 1);

  // // //rocksdb_open_column_families(const rocksdb_options_t *options, const char *name, int num_column_families,
  // //                              const char *const               *column_family_names,
  // //                              const rocksdb_options_t *const  *column_family_options,
  // //                              rocksdb_column_family_handle_t **column_family_handles, char **errptr);

  // for (int i = 0; i < 100; i++) {
  //   char buf[128] = {0};

  //   rocksdb_writeoptions_t *wopt = rocksdb_writeoptions_create();
  //   KV                      kv = {.k1 = i, .k2 = i};
  //   kvSerial(&kv, buf);
  //   rocksdb_put_cf(db, wopt, pHandle[0], buf, strlen(buf), (const char *)&i, sizeof(i), &err);
  // }

  // rocksdb_close(db);
  // Write
  // rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
  // rocksdb_put(db, writeoptions, "key", 3, "value", 5, &err);

  //// Read
  // rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  // rocksdb_readoptions_set_snapshot(readoptions, rocksdb_create_snapshot(db));
  // size_t vallen = 0;
  // char  *val = rocksdb_get(db, readoptions, "key", 3, &vallen, &err);
  // printf("val:%s\n", val);

  //// Update
  //// rocksdb_put(db, writeoptions, "key", 3, "eulav", 5, &err);

  //// Delete
  // rocksdb_delete(db, writeoptions, "key", 3, &err);

  //// Read again
  // val = rocksdb_get(db, readoptions, "key", 3, &vallen, &err);
  // printf("val:%s\n", val);

  // rocksdb_close(db);

  return 0;
}