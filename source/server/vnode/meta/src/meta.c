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

#include "tkv.h"
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

  SHashObj *pTableObjHash;  // uid --> STableObj
  SList *   stbList;        // super table list
  STkvDb *  tbnameDb;       // tbname --> uid
  STkvDb *  tagDb;          // uid --> tag
  STkvDb *  schemaDb;
  STkvDb *  tagIdx;
  size_t    totalUsed;
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
  STkvOpts *dbOptions = tkvOptsCreate();
  tkvOptsSetCreateIfMissing(dbOptions, 1);

  taosMkDir("meta");

  // Open tbname DB
  pMeta->tbnameDb = tkvOpen(dbOptions, "meta/tbname_uid_db");

  // Open tag DB
  pMeta->tagDb = tkvOpen(dbOptions, "meta/uid_tag_db");

  // Open schema DB
  pMeta->schemaDb = tkvOpen(dbOptions, "meta/schema_db");

  // Open tag index
  pMeta->tagIdx = tkvOpen(dbOptions, "meta/tag_idx_db");

  tkvOptsDestroy(dbOptions);

  return pMeta;
}

void metaClose(SMeta *pMeta) {
  if (pMeta) {
    tkvClose(pMeta->tagIdx);
    tkvClose(pMeta->schemaDb);
    tkvClose(pMeta->tagDb);
    tkvClose(pMeta->tbnameDb);

    tdListFree(pMeta->stbList);
    taosHashCleanup(pMeta->pTableObjHash);
    pthread_rwlock_destroy(&(pMeta->rwLock));
  }
}

int metaCreateTable(SMeta *pMeta, STableOpts *pTableOpts) {
  size_t        vallen;
  STkvReadOpts *ropt;
  STableObj *   pTableObj = NULL;
  STkvWriteOpts *wopt;

  // Check if table already exists
  ropt = tkvReadOptsCreate();

  char *uidStr = tkvGet(pMeta->tbnameDb, ropt, pTableOpts->name, strlen(pTableOpts->name), &vallen);
  if (uidStr != NULL) {
    // Has duplicate named table
    return -1;
  }

  tkvReadOptsDestroy(ropt);

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

  wopt = tkvWriteOptsCreate();
  // rocksdb_writeoptions_disable_WAL(wopt, 1);

  // Add to tbname db
  tkvPut(pMeta->tbnameDb, wopt, pTableOpts->name, strlen(pTableOpts->name), (char *)&pTableObj->pTable->uid,
              sizeof(tb_uid_t));

  // Add to schema db
  char  id[12];
  char  buf[256];
  void *pBuf = buf;
  *(tb_uid_t *)id = pTableObj->pTable->uid;
  *(int32_t *)(id + sizeof(tb_uid_t)) = schemaVersion(pTableOpts->pSchema);
  int size = tdEncodeSchema(&pBuf, pTableOpts->pSchema);

  tkvPut(pMeta->schemaDb, wopt, id, 12, buf, size);

  tkvWriteOptsDestroy(wopt);

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