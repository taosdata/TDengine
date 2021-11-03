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

#include "tcoding.h"

#include "meta.h"
#include "metaDB.h"
#include "metaDef.h"
#include "metaOptions.h"

static SMeta *metaNew(const char *path, const SMetaOptions *pMetaOptions);
static void   metaFree(SMeta *pMeta);
static int    metaOpenImpl(SMeta *pMeta);
static void   metaCloseImpl(SMeta *pMeta);

SMeta *metaOpen(const char *path, const SMetaOptions *pMetaOptions) {
  SMeta *pMeta = NULL;

  // Set default options
  if (pMetaOptions == NULL) {
    pMetaOptions = &defaultMetaOptions;
  }

  // Validate the options
  if (metaValidateOptions(pMetaOptions) < 0) {
    // TODO: deal with error
    return NULL;
  }

  // Allocate handle
  pMeta = metaNew(path, pMetaOptions);
  if (pMeta == NULL) {
    // TODO: handle error
    return NULL;
  }

  // Create META path (TODO)
  taosMkDir(path);

  // Open meta
  if (metaOpenImpl(pMeta) < 0) {
    metaFree(pMeta);
    return NULL;
  }

  return pMeta;
}

void metaClose(SMeta *pMeta) {
  if (pMeta) {
    metaCloseImpl(pMeta);
    metaFree(pMeta);
  }
}

void metaRemove(const char *path) { taosRemoveDir(path); }

/* ------------------------ STATIC METHODS ------------------------ */
static SMeta *metaNew(const char *path, const SMetaOptions *pMetaOptions) {
  SMeta *pMeta;
  size_t psize = strlen(path);

  pMeta = (SMeta *)calloc(1, sizeof(*pMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  pMeta->path = strdup(path);
  if (pMeta->path == NULL) {
    metaFree(pMeta);
    return NULL;
  }

  metaOptionsCopy(&(pMeta->options), pMetaOptions);

  return pMeta;
};

static void metaFree(SMeta *pMeta) {
  if (pMeta) {
    tfree(pMeta->path);
    free(pMeta);
  }
}

static int metaOpenImpl(SMeta *pMeta) {
  // Open meta cache
  if (metaOpenCache(pMeta) < 0) {
    // TODO: handle error
    metaCloseImpl(pMeta);
    return -1;
  }

  // Open meta db
  if (metaOpenDB(pMeta) < 0) {
    // TODO: handle error
    metaCloseImpl(pMeta);
    return -1;
  }

  // Open meta index
  if (metaOpenIdx(pMeta) < 0) {
    // TODO: handle error
    metaCloseImpl(pMeta);
    return -1;
  }

  // Open meta table uid generator
  if (metaOpenUidGnrt(pMeta) < 0) {
    // TODO: handle error
    metaCloseImpl(pMeta);
    return -1;
  }

  return 0;
}

static void metaCloseImpl(SMeta *pMeta) {
  metaCloseUidGnrt(pMeta);
  metaCloseIdx(pMeta);
  metaCloseDB(pMeta);
  metaCloseCache(pMeta);
}

// OLD -------------------------------------------------------------------
#if 0
static int    metaCreateSuperTable(SMeta *pMeta, const char *tbname, const SSuperTableOpts *pSuperTableOpts);
static int    metaCreateChildTable(SMeta *pMeta, const char *tbname, const SChildTableOpts *pChildTableOpts);
static int    metaCreateNormalTable(SMeta *pMeta, const char *tbname, const SNormalTableOpts *pNormalTableOpts);

int metaCreateTable(SMeta *pMeta, const STableOptions *pTableOpts) {
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

void metaNormalTableOptsInit(STableOptions *pTableOpts, const char *name, const STSchema *pSchema) {
  pTableOpts->type = META_NORMAL_TABLE;
  pTableOpts->name = strdup(name);
  pTableOpts->normalOpts.pSchema = tdDupSchema(pSchema);
}

void metaSuperTableOptsInit(STableOptions *pTableOpts, const char *name, tb_uid_t uid, const STSchema *pSchema,
                            const STSchema *pTagSchema) {
  pTableOpts->type = META_SUPER_TABLE;
  pTableOpts->name = strdup(name);
  pTableOpts->superOpts.uid = uid;
  pTableOpts->superOpts.pSchema = tdDupSchema(pSchema);
  pTableOpts->superOpts.pTagSchema = tdDupSchema(pTagSchema);
}

void metaChildTableOptsInit(STableOptions *pTableOpts, const char *name, tb_uid_t suid, const SKVRow tags) {
  pTableOpts->type = META_CHILD_TABLE;
  pTableOpts->name = strdup(name);
  pTableOpts->childOpts.suid = suid;
  pTableOpts->childOpts.tags = tdKVRowDup(tags);
}

void metaTableOptsClear(STableOptions *pTableOpts) {
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

#endif