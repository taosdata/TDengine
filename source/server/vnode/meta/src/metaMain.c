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

#include "meta.h"
#include "metaDef.h"
#include "tcoding.h"

static int metaCreateSuperTable(SMeta *pMeta, const char *tbname, const SSuperTableOpts *pSuperTableOpts);
static int metaCreateChildTable(SMeta *pMeta, const char *tbname, const SChildTableOpts *pChildTableOpts);
static int metaCreateNormalTable(SMeta *pMeta, const char *tbname, const SNormalTableOpts *pNormalTableOpts);

SMeta *metaOpen(SMetaOpts *pMetaOpts) {
  SMeta *pMeta = NULL;

  pMeta = (SMeta *)calloc(1, sizeof(*pMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  // TODO: check if file exists and handle the error
  taosMkDir("meta");

  // Open tableDb
  STkvOpts *tableDbOpts = tkvOptsCreate();
  tkvOptsSetCreateIfMissing(tableDbOpts, 1);
  pMeta->tableDb = tkvOpen(tableDbOpts, "meta/table_db");
  tkvOptsDestroy(tableDbOpts);

  // Open tbnameDb
  STkvOpts *tbnameDbOpts = tkvOptsCreate();
  tkvOptsSetCreateIfMissing(tbnameDbOpts, 1);
  pMeta->tbnameDb = tkvOpen(tbnameDbOpts, "meta/tbname_db");
  tkvOptsDestroy(tbnameDbOpts);

  // Open schemaDb
  STkvOpts *schemaDbOpts = tkvOptsCreate();
  tkvOptsSetCreateIfMissing(schemaDbOpts, 1);
  pMeta->schemaDb = tkvOpen(schemaDbOpts, "meta/schema_db");
  tkvOptsDestroy(schemaDbOpts);

  // Open tagDb
  STkvOpts *tagDbOpts = tkvOptsCreate();
  tkvOptsSetCreateIfMissing(tagDbOpts, 1);
  pMeta->tagDb = tkvOpen(tagDbOpts, "meta/tag_db");
  tkvOptsDestroy(tagDbOpts);

  // Open tagIdx
  STkvOpts *tagIdxDbOpts = tkvOptsCreate();
  tkvOptsSetCreateIfMissing(tagIdxDbOpts, 1);
  pMeta->tagIdx = tkvOpen(tagIdxDbOpts, "meta/tag_idx_db");
  tkvOptsDestroy(tagIdxDbOpts);

  // TODO: need to figure out how to persist the START UID
  tableUidGeneratorInit(&(pMeta->uidGenerator), IVLD_TB_UID);
}

void metaClose(SMeta *pMeta) {
  if (pMeta) {
    tableUidGeneratorClear(&pMeta->uidGenerator);

    tkvClose(pMeta->tagIdx);
    tkvClose(pMeta->tagDb);
    tkvClose(pMeta->schemaDb);
    tkvClose(pMeta->tbnameDb);
    tkvClose(pMeta->tableDb);

    free(pMeta);
  }
}

int metaCreateTable(SMeta *pMeta, const STableOpts *pTableOpts) {
  size_t vallen;
  char * pUid;

  // Check if table already exists
  pUid = tkvGet(pMeta->tbnameDb, NULL, pTableOpts->name, strlen(pTableOpts->name), &vallen);
  if (pUid) {
    free(pUid);
    // Table already exists, return error code
    return -1;
  }

  switch (pTableOpts->type) {
    case META_SUPER_TABLE:
      return metaCreateSuperTable(pMeta, pTableOpts->name, &(pTableOpts->superOpts));
    case META_CHILD_TABLE:
      return metaCreateChildTable(pMeta, pTableOpts->name, &(pTableOpts->childOpts));
    case META_NORMAL_TABLE:
      return metaCreateNormalTable(pMeta, pTableOpts->name, &(pTableOpts->normalOpts));
    default:
      ASSERT(0);
  }

  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */
static int metaCreateSuperTable(SMeta *pMeta, const char *tbname, const SSuperTableOpts *pSuperTableOpts) {
  size_t vallen;
  size_t keylen;
  char * pVal = NULL;
  char   schemaKey[sizeof(tb_uid_t) * 2];
  char   buffer[1024]; /* TODO */
  void * pBuf = NULL;

  pVal = tkvGet(pMeta->tableDb, NULL, (char *)(&(pSuperTableOpts->uid)), sizeof(pSuperTableOpts->uid), &vallen);
  if (pVal) {
    free(pVal);
    // TODO: table with same uid exists, just return error
    return -1;
  }

  // Put uid -> tbObj
  vallen = 0;
  pBuf = (void *)buffer;
  vallen += taosEncodeString(&pBuf, tbname);  // ENCODE TABLE NAME
  vallen += taosEncodeFixedI32(&pBuf, 1);     // ENCODE SCHEMA, SCHEMA HERE IS AN ARRAY OF VERSIONS
  vallen += taosEncodeFixedI32(&pBuf, schemaVersion(pSuperTableOpts->pSchema));
  vallen += tdEncodeSchema(&pBuf, pSuperTableOpts->pTagSchema);  // ENCODE TAG SCHEMA
  tkvPut(pMeta->tableDb, NULL, (char *)(&(pSuperTableOpts->uid)), sizeof(pSuperTableOpts->uid), buffer, vallen);

  // Put tbname -> uid
  tkvPut(pMeta->tbnameDb, NULL, tbname, strlen(tbname), (char *)(&(pSuperTableOpts->uid)),
         sizeof(pSuperTableOpts->uid));

  // Put uid+sversion -> schema
  *(tb_uid_t *)schemaKey = pSuperTableOpts->uid;
  *(int32_t *)(POINTER_SHIFT(schemaKey, sizeof(tb_uid_t))) = schemaVersion(pSuperTableOpts->pSchema);
  keylen = sizeof(tb_uid_t) + sizeof(int32_t);
  pBuf = (void *)buffer;
  vallen = tdEncodeSchema(&pBuf, pSuperTableOpts->pSchema);
  tkvPut(pMeta->schemaDb, NULL, schemaKey, keylen, buffer, vallen);

  return 0;
}

static int metaCreateChildTable(SMeta *pMeta, const char *tbname, const SChildTableOpts *pChildTableOpts) {
  size_t   vallen;
  char     buffer[1024]; /* TODO */
  void *   pBuf = NULL;
  char *   pTable;
  tb_uid_t uid;

  // Check if super table exists
  pTable = tkvGet(pMeta->tableDb, NULL, (char *)(&(pChildTableOpts->suid)), sizeof(pChildTableOpts->suid), &vallen);
  if (pTable == NULL) {
    // Super table not exists, just return error
    return -1;
  }

  // Generate a uid to the new table
  uid = generateUid(&pMeta->uidGenerator);

  // Put uid -> tbObj
  vallen = 0;
  pBuf = (void *)buffer;
  vallen += taosEncodeString(&pBuf, tbname);
  vallen += taosEncodeFixedU64(&pBuf, pChildTableOpts->suid);
  tkvPut(pMeta->tableDb, NULL, (char *)(&uid), sizeof(uid), buffer, vallen);

  // Put tbname -> uid
  tkvPut(pMeta->tbnameDb, NULL, tbname, strlen(tbname), (char *)(&uid), sizeof(uid));

  // Put uid-> tags
  tkvPut(pMeta->tagDb, NULL, (char *)(&uid), sizeof(uid), (char *)pChildTableOpts->tags,
         (size_t)kvRowLen(pChildTableOpts->tags));

  // TODO: Put tagIdx

  return 0;
}

static int metaCreateNormalTable(SMeta *pMeta, const char *tbname, const SNormalTableOpts *pNormalTableOpts) {
  size_t   vallen;
  char     keyBuf[sizeof(tb_uid_t) + sizeof(int32_t)];
  char     buffer[1024]; /* TODO */
  void *   pBuf = NULL;
  tb_uid_t uid;

  // Generate a uid to the new table
  uid = generateUid(&pMeta->uidGenerator);

  // Put uid -> tbObj
  vallen = 0;
  pBuf = (void *)buffer;
  vallen += taosEncodeString(&pBuf, tbname);
  vallen += taosEncodeFixedI32(&pBuf, 1);
  vallen += taosEncodeFixedI32(&pBuf, schemaVersion(pNormalTableOpts->pSchema));
  tkvPut(pMeta->tableDb, NULL, (char *)(&uid), sizeof(uid), buffer, vallen);

  // Put tbname -> uid
  tkvPut(pMeta->tbnameDb, NULL, tbname, strlen(tbname), (char *)(&(uid)), sizeof(uid));

  // Put uid+sversion -> schema
  vallen = 0;
  pBuf = (void *)buffer;
  vallen += tdEncodeSchema(&pBuf, pNormalTableOpts->pSchema);
  tkvPut(pMeta->schemaDb, NULL, keyBuf, sizeof(tb_uid_t) + sizeof(int32_t), buffer, vallen);

  return 0;
}

void metaNormalTableOptsInit(STableOpts *pTableOpts, const char *name, const STSchema *pSchema) {
  pTableOpts->type = META_NORMAL_TABLE;
  pTableOpts->name = strdup(name);
  pTableOpts->normalOpts.pSchema = tdDupSchema(pSchema);
}

void metaSuperTableOptsInit(STableOpts *pTableOpts, const char *name, tb_uid_t uid, const STSchema *pSchema,
                            const STSchema *pTagSchema) {
  pTableOpts->type = META_SUPER_TABLE;
  pTableOpts->name = strdup(name);
  pTableOpts->superOpts.uid = uid;
  pTableOpts->superOpts.pSchema = tdDupSchema(pSchema);
  pTableOpts->superOpts.pTagSchema = tdDupSchema(pTagSchema);
}

void metaChildTableOptsInit(STableOpts *pTableOpts, const char *name, tb_uid_t suid, const SKVRow tags) {
  pTableOpts->type = META_CHILD_TABLE;
  pTableOpts->name = strdup(name);
  pTableOpts->childOpts.suid = suid;
  pTableOpts->childOpts.tags = tdKVRowDup(tags);
}

void metaTableOptsClear(STableOpts *pTableOpts) {
  switch (pTableOpts->type) {
    case META_NORMAL_TABLE:
      tfree(pTableOpts->name);
      tdFreeSchema(pTableOpts->normalOpts.pSchema);
      break;
    case META_SUPER_TABLE:
      tdFreeSchema(pTableOpts->superOpts.pTagSchema);
      tdFreeSchema(pTableOpts->superOpts.pSchema);
      tfree(pTableOpts->name);
      break;
    case META_CHILD_TABLE:
      kvRowFree(pTableOpts->childOpts.tags);
      tfree(pTableOpts->name);
      break;
    default:
      break;
  }

  memset(pTableOpts, 0, sizeof(*pTableOpts));
}

void metaDestroy(const char *path) { taosRemoveDir(path); }
