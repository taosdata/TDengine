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

#ifdef USE_ROCKSDB
#include <rocksdb/c.h>
#endif

#include "tkv.h"

struct STkvDb {
#ifdef USE_ROCKSDB
  rocksdb_t *db;
#endif
};

struct STkvOpts {
#ifdef USE_ROCKSDB
  rocksdb_options_t *opts;
#endif
};

struct STkvCache {
  // TODO
};

struct STkvReadOpts {
#ifdef USE_ROCKSDB
  rocksdb_readoptions_t *ropts;
#endif
};

struct STkvWriteOpts {
#ifdef USE_ROCKSDB
  rocksdb_writeoptions_t *wopts;
#endif
};

STkvDb *tkvOpen(const STkvOpts *options, const char *path) {
  STkvDb *pDb = NULL;

  pDb = (STkvDb *)malloc(sizeof(*pDb));
  if (pDb == NULL) {
    return NULL;
  }

#ifdef USE_ROCKSDB
  char *err = NULL;

  pDb->db = rocksdb_open(options->opts, path, &err);
  // TODO: check err
#endif

  return pDb;
}

void tkvClose(STkvDb *pDb) {
  if (pDb) {
#ifdef USE_ROCKSDB
    rocksdb_close(pDb->db);
#endif
    free(pDb);
  }
}

void tkvPut(STkvDb *pDb, const STkvWriteOpts *pwopts, const char *key, size_t keylen, const char *val, size_t vallen) {
#ifdef USE_ROCKSDB
  char *err = NULL;
  rocksdb_put(pDb->db, pwopts->wopts, key, keylen, val, vallen, &err);
  // TODO: check error
#endif
}

char *tkvGet(STkvDb *pDb, const STkvReadOpts *propts, const char *key, size_t keylen, size_t *vallen) {
  char *ret = NULL;

#ifdef USE_ROCKSDB
  char *err = NULL;
  ret = rocksdb_get(pDb->db, propts->ropts, key, keylen, vallen, &err);
  // TODD: check error
#endif

  return ret;
}

STkvOpts *tkvOptsCreate() {
  STkvOpts *pOpts = NULL;

  pOpts = (STkvOpts *)malloc(sizeof(*pOpts));
  if (pOpts == NULL) {
    return NULL;
  }

#ifdef USE_ROCKSDB
  pOpts->opts = rocksdb_options_create();
  // TODO: check error
#endif

  return pOpts;
}

void tkvOptsDestroy(STkvOpts *pOpts) {
  if (pOpts) {
#ifdef USE_ROCKSDB
    rocksdb_options_destroy(pOpts->opts);
#endif
    free(pOpts);
  }
}

void tkvOptionsSetCache(STkvOpts *popts, STkvCache *pCache) {
  // TODO
}

void tkvOptsSetCreateIfMissing(STkvOpts *pOpts, unsigned char c) {
#ifdef USE_ROCKSDB
  rocksdb_options_set_create_if_missing(pOpts->opts, c);
#endif
}

STkvReadOpts *tkvReadOptsCreate() {
  STkvReadOpts *pReadOpts = NULL;

  pReadOpts = (STkvReadOpts *)malloc(sizeof(*pReadOpts));
  if (pReadOpts == NULL) {
    return NULL;
  }

#ifdef USE_ROCKSDB
  pReadOpts->ropts = rocksdb_readoptions_create();
#endif

  return pReadOpts;
}

void tkvReadOptsDestroy(STkvReadOpts *pReadOpts) {
  if (pReadOpts) {
#ifdef USE_ROCKSDB
    rocksdb_readoptions_destroy(pReadOpts->ropts);
#endif
    free(pReadOpts);
  }
}

STkvWriteOpts *tkvWriteOptsCreate() {
  STkvWriteOpts *pWriteOpts = NULL;

  pWriteOpts = (STkvWriteOpts *)malloc(sizeof(*pWriteOpts));
  if (pWriteOpts == NULL) {
    return NULL;
  }

#ifdef USE_ROCKSDB
  pWriteOpts->wopts = rocksdb_writeoptions_create();
#endif

  return pWriteOpts;
}

void tkvWriteOptsDestroy(STkvWriteOpts *pWriteOpts) {
  if (pWriteOpts) {
#ifdef USE_ROCKSDB
    rocksdb_writeoptions_destroy(pWriteOpts->wopts);
#endif
    free(pWriteOpts);
  }
  // TODO
}