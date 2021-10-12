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

#include <rocksdb/c.h>

#include "thash.h"
#include "tlist.h"
#include "tlockfree.h"
#include "ttypes.h"

#include "meta.h"

/* -------------------- Structures -------------------- */
typedef struct STable {
  uint64_t uid;
  tstr *   name;
  uint64_t suid;
  SArray * schema;
} STable;

typedef struct STableObj {
  bool     pin;
  uint64_t ref;
  SRWLatch latch;
  uint64_t offset;
  SList *  ctbList;  //  child table list
  STable * pTable;
} STableObj;

struct SMeta {
  pthread_rwlock_t rwLock;

  SHashObj * pTableObjHash;  // uid --> STableObj
  SList *    stbList;        // super table list
  rocksdb_t *tbnameDb;       // tbname --> uid
  rocksdb_t *tagDb;          // uid --> tag
  rocksdb_t *schemaDb;
  rocksdb_t *tagIdx;
  size_t     totalUsed;
};

/* -------------------- Methods -------------------- */

SMeta *metaOpen(SMetaOptions *options) {
  SMeta *pMeta = NULL;
  char * err = NULL;

  pMeta = (SMeta *)calloc(1, sizeof(*pMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  pthread_rwlock_init(&(pMeta->rwLock), NULL);

  pMeta->pTableObjHash = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);

  pMeta->stbList = tdListNew(sizeof(STableObj *));

  // Options
  rocksdb_options_t *dbOptions = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(dbOptions, 1);

  taosMkDir("meta");

  // Open tbname DB
  pMeta->tbnameDb = rocksdb_open(dbOptions, "meta/tbname_uid_db", &err);

  // Open tag DB
  pMeta->tagDb = rocksdb_open(dbOptions, "meta/uid_tag_db", &err);

  // Open schema DB
  pMeta->schemaDb = rocksdb_open(dbOptions, "meta/schema_db", &err);

  // Open tag index
  pMeta->tagIdx = rocksdb_open(dbOptions, "meta/tag_idx_db", &err);

  rocksdb_options_destroy(dbOptions);

  return pMeta;
}

void metaClose(SMeta *pMeta) {
  if (pMeta) {
    rocksdb_close(pMeta->tagIdx);
    rocksdb_close(pMeta->schemaDb);
    rocksdb_close(pMeta->tagDb);
    rocksdb_close(pMeta->tbnameDb);

    tdListFree(pMeta->stbList);
    taosHashCleanup(pMeta->pTableObjHash);
    pthread_rwlock_destroy(&(pMeta->rwLock));
  }
}

void metaDestroy(const char *path) { taosRemoveDir(path); }

int metaCommit(SMeta *meta) { return 0; }