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
#include "metaUid.h"

/* -------------------- Structures -------------------- */

typedef struct STable {
  tb_uid_t uid;
  char *   name;
  tb_uid_t suid;
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

static STable *   metaTableNew(tb_uid_t uid, const char *name, int32_t sver);
static STableObj *metaTableObjNew();

/* -------------------- Methods -------------------- */

SMeta *metaOpen(SMetaOpts *options) {
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

int metaCreateTable(SMeta *pMeta, STableOpts *pTableOpts) {
  size_t                  vallen;
  char *                  err = NULL;
  rocksdb_readoptions_t * ropt;
  STableObj *             pTableObj = NULL;
  rocksdb_writeoptions_t *wopt;

  // Check if table already exists
  ropt = rocksdb_readoptions_create();

  char *uidStr = rocksdb_get(pMeta->tbnameDb, ropt, pTableOpts->name, strlen(pTableOpts->name), &vallen, &err);
  if (uidStr != NULL) {
    // Has duplicate named table
    return -1;
  }

  rocksdb_readoptions_destroy(ropt);

  // Create table obj
  pTableObj = metaTableObjNew();
  if (pTableObj == NULL) {
    // TODO
    return -1;
  }

  // Create table object
  pTableObj->pTable = metaTableNew(metaGenerateUid(), pTableOpts->name, schemaVersion(pTableOpts->pSchema));
  if (pTableObj->pTable == NULL) {
    // TODO
  }

  pthread_rwlock_rdlock(&pMeta->rwLock);

  taosHashPut(pMeta->pTableObjHash, &(pTableObj->pTable->uid), sizeof(tb_uid_t), &pTableObj, sizeof(pTableObj));

  wopt = rocksdb_writeoptions_create();
  rocksdb_writeoptions_disable_WAL(wopt, 1);

  // Add to tbname db
  rocksdb_put(pMeta->tbnameDb, wopt, pTableOpts->name, strlen(pTableOpts->name), &pTableObj->pTable->uid,
              sizeof(tb_uid_t), &err);

  // Add to schema db
  char  id[12];
  char  buf[256];
  void *pBuf = buf;
  *(tb_uid_t *)id = pTableObj->pTable->uid;
  *(int32_t *)(id + sizeof(tb_uid_t)) = schemaVersion(pTableOpts->pSchema);
  int size = tdEncodeSchema(&pBuf, pTableOpts->pSchema);

  rocksdb_put(pMeta->schemaDb, wopt, id, 12, buf, size, &err);

  rocksdb_writeoptions_destroy(wopt);

  pthread_rwlock_unlock(&pMeta->rwLock);

  return 0;
}

void metaDestroy(const char *path) { taosRemoveDir(path); }

int metaCommit(SMeta *meta) { return 0; }

void metaTableOptsInit(STableOpts *pTableOpts, int8_t type, const char *name, const STSchema *pSchema) {
  pTableOpts->type = type;
  pTableOpts->name = strdup(name);
  pTableOpts->pSchema = tdDupSchema(pSchema);
}

/* -------------------- Static Methods -------------------- */

static STable *metaTableNew(tb_uid_t uid, const char *name, int32_t sver) {
  STable *pTable = NULL;

  pTable = (STable *)malloc(sizeof(*pTable));
  if (pTable == NULL) {
    // TODO
    return NULL;
  }

  pTable->schema = taosArrayInit(0, sizeof(int32_t));
  if (pTable->schema == NULL) {
    // TODO
    return NULL;
  }

  pTable->uid = uid;
  pTable->name = strdup(name);
  pTable->suid = IVLD_TB_UID;
  taosArrayPush(pTable->schema, &sver);

  return pTable;
}

static STableObj *metaTableObjNew() {
  STableObj *pTableObj = NULL;

  pTableObj = (STableObj *)malloc(sizeof(*pTableObj));
  if (pTableObj == NULL) {
    return NULL;
  }

  pTableObj->pin = true;
  pTableObj->ref = 1;
  taosInitRWLatch(&(pTableObj->latch));
  pTableObj->offset = UINT64_MAX;
  pTableObj->ctbList = NULL;
  pTableObj->pTable = NULL;

  return pTableObj;
}