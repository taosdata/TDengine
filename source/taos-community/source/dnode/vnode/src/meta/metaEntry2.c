/*
 * Copyright (c) 2023 Hongze Cheng <hzcheng@umich.edu>.
 * All rights reserved.
 *
 * This code is the intellectual property of Hongze Cheng.
 * Any reproduction or distribution, in whole or in part,
 * without the express written permission of Hongze Cheng is
 * strictly prohibited.
 */

#include "meta.h"
#include "vnodeInt.h"

extern SDmNotifyHandle dmNotifyHdl;

int32_t metaCloneEntry(const SMetaEntry *pEntry, SMetaEntry **ppEntry);
void    metaCloneEntryFree(SMetaEntry **ppEntry);
void    metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey);
int     metaSaveJsonVarToIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);
int     metaDelJsonVarFromIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);
int     tagIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);

static void    metaTimeSeriesNotifyCheck(SMeta *pMeta);
static int32_t metaFetchTagIdxKey(SMeta *pMeta, const SMetaEntry *pEntry, const SSchema *pTagColumn,
                                  STagIdxKey **ppTagIdxKey, int32_t *pTagIdxKeySize);
static void    metaFetchTagIdxKeyFree(STagIdxKey **ppTagIdxKey);

#define metaErr(VGID, ERRNO)                                                                                     \
  do {                                                                                                           \
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " type:%d uid:%" PRId64 " name:%s", VGID, \
              __func__, __FILE__, __LINE__, tstrerror(ERRNO), pEntry->version, pEntry->type, pEntry->uid,        \
              pEntry->type > 0 ? pEntry->name : NULL);                                                           \
  } while (0)

typedef enum {
  META_ENTRY_TABLE = 0,
  META_SCHEMA_TABLE,
  META_UID_IDX,
  META_NAME_IDX,
  META_SUID_IDX,
  META_CHILD_IDX,
  META_TAG_IDX,
  META_BTIME_IDX,
  META_TTL_IDX,
  META_TABLE_MAX,
} EMetaTable;

typedef enum {
  META_TABLE_OP_INSERT = 0,
  META_TABLE_OP_UPDATA,
  META_TABLE_OP_DELETE,
  META_TABLE_OP_MAX,
} EMetaTableOp;

typedef struct {
  const SMetaEntry *pEntry;
  const SMetaEntry *pSuperEntry;
  const SMetaEntry *pOldEntry;
} SMetaHandleParam;

typedef struct {
  EMetaTable   table;
  EMetaTableOp op;
} SMetaTableOp;

int32_t metaFetchEntryByUid(SMeta *pMeta, int64_t uid, SMetaEntry **ppEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  void   *value = NULL;
  int32_t valueSize = 0;

  // search uid index
  code = tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &value, &valueSize);
  if (TSDB_CODE_SUCCESS != code) {
    metaError("vgId:%d, failed to get entry by uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), uid, tstrerror(code));
    return code;
  }

  // search entry table
  STbDbKey key = {
      .version = ((SUidIdxVal *)value)->version,
      .uid = uid,
  };
  tdbFreeClear(value);

  code = tdbTbGet(pMeta->pTbDb, &key, sizeof(key), &value, &valueSize);
  if (TSDB_CODE_SUCCESS != code) {
    metaError("vgId:%d, failed to get entry by uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), uid, tstrerror(code));
    code = TSDB_CODE_INTERNAL_ERROR;
    return code;
  }

  // decode entry
  SDecoder   decoder = {0};
  SMetaEntry entry = {0};

  tDecoderInit(&decoder, value, valueSize);
  code = metaDecodeEntry(&decoder, &entry);
  if (code) {
    metaError("vgId:%d, failed to decode entry by uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), uid,
              tstrerror(code));
    tDecoderClear(&decoder);
    tdbFreeClear(value);
    return code;
  }

  code = metaCloneEntry(&entry, ppEntry);
  if (code) {
    metaError("vgId:%d, failed to clone entry by uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), uid,
              tstrerror(code));
    tDecoderClear(&decoder);
    tdbFreeClear(value);
    return code;
  }

  tdbFreeClear(value);
  tDecoderClear(&decoder);
  return code;
}

int32_t metaFetchEntryByName(SMeta *pMeta, const char *name, SMetaEntry **ppEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  void   *value = NULL;
  int32_t valueSize = 0;

  code = tdbTbGet(pMeta->pNameIdx, name, strlen(name) + 1, &value, &valueSize);
  if (TSDB_CODE_SUCCESS != code) {
    metaError("vgId:%d, failed to get entry by name:%s since %s", TD_VID(pMeta->pVnode), name, tstrerror(code));
    return code;
  }
  int64_t uid = *(int64_t *)value;
  tdbFreeClear(value);

  code = metaFetchEntryByUid(pMeta, uid, ppEntry);
  if (TSDB_CODE_SUCCESS != code) {
    metaError("vgId:%d, failed to get entry by uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), uid, tstrerror(code));
    code = TSDB_CODE_INTERNAL_ERROR;
  }
  return code;
}

void metaFetchEntryFree(SMetaEntry **ppEntry) { metaCloneEntryFree(ppEntry); }

// Entry Table
static int32_t metaEntryTableUpsert(SMeta *pMeta, const SMetaHandleParam *pParam, EMetaTableOp op) {
  const SMetaEntry *pEntry = pParam->pEntry;

  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  vgId = TD_VID(pMeta->pVnode);
  void    *value = NULL;
  int32_t  valueSize = 0;
  SEncoder encoder = {0};
  STbDbKey key = {
      .version = pEntry->version,
      .uid = pEntry->uid,
  };

  // encode entry
  tEncodeSize(metaEncodeEntry, pEntry, valueSize, code);
  if (code != 0) {
    metaErr(vgId, code);
    return code;
  }

  value = taosMemoryMalloc(valueSize);
  if (NULL == value) {
    metaErr(vgId, terrno);
    return terrno;
  }

  tEncoderInit(&encoder, value, valueSize);
  code = metaEncodeEntry(&encoder, pEntry);
  if (code) {
    metaErr(vgId, code);
    tEncoderClear(&encoder);
    taosMemoryFree(value);
    return code;
  }
  tEncoderClear(&encoder);

  // put to tdb
  if (META_TABLE_OP_INSERT == op) {
    code = tdbTbInsert(pMeta->pTbDb, &key, sizeof(key), value, valueSize, pMeta->txn);
  } else if (META_TABLE_OP_UPDATA == op) {
    code = tdbTbUpsert(pMeta->pTbDb, &key, sizeof(key), value, valueSize, pMeta->txn);
  } else if (META_TABLE_OP_DELETE == op) {
    code = tdbTbInsert(pMeta->pTbDb, &key, sizeof(key), value, valueSize, pMeta->txn);
  } else {
    code = TSDB_CODE_INVALID_PARA;
  }
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
  }
  taosMemoryFree(value);
  return code;
}

static int32_t metaEntryTableInsert(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaEntryTableUpsert(pMeta, pParam, META_TABLE_OP_INSERT);
}

static int32_t metaEntryTableUpdate(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaEntryTableUpsert(pMeta, pParam, META_TABLE_OP_UPDATA);
}

static int32_t metaEntryTableDelete(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaEntryTableUpsert(pMeta, pParam, META_TABLE_OP_DELETE);
}

// Schema Table
static int32_t metaSchemaTableUpsert(SMeta *pMeta, const SMetaHandleParam *pParam, EMetaTableOp op) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  vgId = TD_VID(pMeta->pVnode);
  SEncoder encoder = {0};
  void    *value = NULL;
  int32_t  valueSize = 0;

  const SMetaEntry     *pEntry = pParam->pEntry;
  const SSchemaWrapper *pSchema = NULL;
  if (pEntry->type == TSDB_SUPER_TABLE) {
    pSchema = &pEntry->stbEntry.schemaRow;
  } else if (pEntry->type == TSDB_NORMAL_TABLE || pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    pSchema = &pEntry->ntbEntry.schemaRow;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
  SSkmDbKey key = {
      .uid = pEntry->uid,
      .sver = pSchema->version,
  };

  // encode schema
  tEncodeSize(tEncodeSSchemaWrapper, pSchema, valueSize, code);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
    return code;
  }

  value = taosMemoryMalloc(valueSize);
  if (NULL == value) {
    metaErr(vgId, terrno);
    return terrno;
  }

  tEncoderInit(&encoder, value, valueSize);
  code = tEncodeSSchemaWrapper(&encoder, pSchema);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
    tEncoderClear(&encoder);
    taosMemoryFree(value);
    return code;
  }
  tEncoderClear(&encoder);

  // put to tdb
  if (META_TABLE_OP_INSERT == op) {
    code = tdbTbInsert(pMeta->pSkmDb, &key, sizeof(key), value, valueSize, pMeta->txn);
  } else if (META_TABLE_OP_UPDATA == op) {
    code = tdbTbUpsert(pMeta->pSkmDb, &key, sizeof(key), value, valueSize, pMeta->txn);
  } else {
    code = TSDB_CODE_INVALID_PARA;
  }
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
  }
  taosMemoryFree(value);
  return code;
}

static int32_t metaSchemaTableInsert(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaSchemaTableUpsert(pMeta, pParam, META_TABLE_OP_INSERT);
}

static int32_t metaAddOrDropTagIndexOfSuperTable(SMeta *pMeta, const SMetaHandleParam *pParam,
                                                 const SSchema *pOldColumn, const SSchema *pNewColumn) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pOldEntry = pParam->pOldEntry;
  enum { ADD_INDEX, DROP_INDEX } action;

  if (pOldColumn && pNewColumn) {
    if (IS_IDX_ON(pOldColumn) && IS_IDX_ON(pNewColumn)) {
      return TSDB_CODE_SUCCESS;
    } else if (IS_IDX_ON(pOldColumn) && !IS_IDX_ON(pNewColumn)) {
      action = DROP_INDEX;
    } else if (!IS_IDX_ON(pOldColumn) && IS_IDX_ON(pNewColumn)) {
      action = ADD_INDEX;
    } else {
      return TSDB_CODE_SUCCESS;
    }
  } else if (pOldColumn) {
    if (IS_IDX_ON(pOldColumn)) {
      action = DROP_INDEX;
    } else {
      return TSDB_CODE_SUCCESS;
    }
  } else {
    if (IS_IDX_ON(pNewColumn)) {
      action = ADD_INDEX;
    } else {
      return TSDB_CODE_SUCCESS;
    }
  }

  // fetch all child tables
  SArray *childTables = 0;
  code = metaGetChildUidsOfSuperTable(pMeta, pEntry->uid, &childTables);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  // do drop or add index
  for (int32_t i = 0; i < taosArrayGetSize(childTables); i++) {
    int64_t uid = *(int64_t *)taosArrayGet(childTables, i);

    // fetch child entry
    SMetaEntry *pChildEntry = NULL;
    code = metaFetchEntryByUid(pMeta, uid, &pChildEntry);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      taosArrayDestroy(childTables);
      return code;
    }

    STagIdxKey *pTagIdxKey = NULL;
    int32_t     tagIdxKeySize = 0;

    if (action == ADD_INDEX) {
      code = metaFetchTagIdxKey(pMeta, pChildEntry, pNewColumn, &pTagIdxKey, &tagIdxKeySize);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        taosArrayDestroy(childTables);
        metaFetchEntryFree(&pChildEntry);
        return code;
      }

      code = tdbTbInsert(pMeta->pTagIdx, pTagIdxKey, tagIdxKeySize, NULL, 0, pMeta->txn);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        taosArrayDestroy(childTables);
        metaFetchEntryFree(&pChildEntry);
        metaFetchTagIdxKeyFree(&pTagIdxKey);
        return code;
      }
    } else {
      code = metaFetchTagIdxKey(pMeta, pChildEntry, pOldColumn, &pTagIdxKey, &tagIdxKeySize);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        taosArrayDestroy(childTables);
        metaFetchEntryFree(&pChildEntry);
        return code;
      }

      code = tdbTbDelete(pMeta->pTagIdx, pTagIdxKey, tagIdxKeySize, pMeta->txn);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        taosArrayDestroy(childTables);
        metaFetchEntryFree(&pChildEntry);
        metaFetchTagIdxKeyFree(&pTagIdxKey);
        return code;
      }
    }

    metaFetchTagIdxKeyFree(&pTagIdxKey);
    metaFetchEntryFree(&pChildEntry);
  }

  taosArrayDestroy(childTables);
  return code;
}

static int32_t metaAddOrDropColumnIndexOfVirtualSuperTable(SMeta *pMeta, const SMetaHandleParam *pParam,
                                                           const SSchema *pOldColumn, const SSchema *pNewColumn) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pOldEntry = pParam->pOldEntry;
  enum { ADD_COLUMN, DROP_COLUMN } action;

  if (pOldColumn && pNewColumn) {
    return TSDB_CODE_SUCCESS;
  } else if (pOldColumn) {
    action = DROP_COLUMN;
  } else {
    action = ADD_COLUMN;
  }

  // fetch all child tables
  SArray *childTables = 0;
  code = metaGetChildUidsOfSuperTable(pMeta, pEntry->uid, &childTables);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  // do drop or add index
  for (int32_t i = 0; i < taosArrayGetSize(childTables); i++) {
    int64_t uid = *(int64_t *)taosArrayGet(childTables, i);

    // fetch child entry
    SMetaEntry *pChildEntry = NULL;
    code = metaFetchEntryByUid(pMeta, uid, &pChildEntry);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      taosArrayDestroy(childTables);
      return code;
    }

    SMetaHandleParam param = {.pEntry = pChildEntry};

    if (action == ADD_COLUMN) {
      code = updataTableColRef(&pChildEntry->colRef, pNewColumn, 1, NULL);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        taosArrayDestroy(childTables);
        metaFetchEntryFree(&pChildEntry);
        return code;
      }

      code = metaEntryTableUpdate(pMeta, &param);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        taosArrayDestroy(childTables);
        metaFetchEntryFree(&pChildEntry);
        return code;
      }
    } else {
      code = updataTableColRef(&pChildEntry->colRef, pOldColumn, 0, NULL);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        taosArrayDestroy(childTables);
        metaFetchEntryFree(&pChildEntry);
        return code;
      }

      code = metaEntryTableUpdate(pMeta, &param);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        taosArrayDestroy(childTables);
        metaFetchEntryFree(&pChildEntry);
        return code;
      }
    }
    metaFetchEntryFree(&pChildEntry);
  }

  taosArrayDestroy(childTables);
  return code;
}

static int32_t metaUpdateSuperTableTagSchema(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t               code = TSDB_CODE_SUCCESS;
  const SMetaEntry     *pEntry = pParam->pEntry;
  const SMetaEntry     *pOldEntry = pParam->pOldEntry;
  const SSchemaWrapper *pNewTagSchema = &pEntry->stbEntry.schemaTag;
  const SSchemaWrapper *pOldTagSchema = &pOldEntry->stbEntry.schemaTag;

  int32_t iOld = 0, iNew = 0;
  for (; iOld < pOldTagSchema->nCols && iNew < pNewTagSchema->nCols;) {
    SSchema *pOldColumn = pOldTagSchema->pSchema + iOld;
    SSchema *pNewColumn = pNewTagSchema->pSchema + iNew;

    if (pOldColumn->colId == pNewColumn->colId) {
      code = metaAddOrDropTagIndexOfSuperTable(pMeta, pParam, pOldColumn, pNewColumn);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      iOld++;
      iNew++;
    } else if (pOldColumn->colId < pNewColumn->colId) {
      code = metaAddOrDropTagIndexOfSuperTable(pMeta, pParam, pOldColumn, NULL);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      // drop old tag from meta stable tag filter cache
      code = metaStableTagFilterCacheDropTag(pMeta, pEntry->uid, pOldColumn->colId);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      iOld++;
    } else {
      code = metaAddOrDropTagIndexOfSuperTable(pMeta, pParam, NULL, pNewColumn);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      iNew++;
    }
  }

  for (; iOld < pOldTagSchema->nCols; iOld++) {
    SSchema *pOldColumn = pOldTagSchema->pSchema + iOld;
    code = metaAddOrDropTagIndexOfSuperTable(pMeta, pParam, pOldColumn, NULL);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
    // drop old tag from meta stable tag filter cache
    code = metaStableTagFilterCacheDropTag(pMeta, pEntry->uid, pOldColumn->colId);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  for (; iNew < pNewTagSchema->nCols; iNew++) {
    SSchema *pNewColumn = pNewTagSchema->pSchema + iNew;
    code = metaAddOrDropTagIndexOfSuperTable(pMeta, pParam, NULL, pNewColumn);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  return code;
}

static int32_t metaUpdateSuperTableRowSchema(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t               code = TSDB_CODE_SUCCESS;
  const SMetaEntry     *pEntry = pParam->pEntry;
  const SMetaEntry     *pOldEntry = pParam->pOldEntry;
  const SSchemaWrapper *pNewRowSchema = &pEntry->stbEntry.schemaRow;
  const SSchemaWrapper *pOldRowSchema = &pOldEntry->stbEntry.schemaRow;

  int32_t iOld = 0, iNew = 0;
  for (; iOld < pOldRowSchema->nCols && iNew < pNewRowSchema->nCols;) {
    SSchema *pOldColumn = pOldRowSchema->pSchema + iOld;
    SSchema *pNewColumn = pNewRowSchema->pSchema + iNew;

    if (pOldColumn->colId == pNewColumn->colId) {
      code = metaAddOrDropColumnIndexOfVirtualSuperTable(pMeta, pParam, pOldColumn, pNewColumn);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      iOld++;
      iNew++;
    } else if (pOldColumn->colId < pNewColumn->colId) {
      code = metaAddOrDropColumnIndexOfVirtualSuperTable(pMeta, pParam, pOldColumn, NULL);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      iOld++;
    } else {
      code = metaAddOrDropColumnIndexOfVirtualSuperTable(pMeta, pParam, NULL, pNewColumn);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      iNew++;
    }
  }

  for (; iOld < pOldRowSchema->nCols; iOld++) {
    SSchema *pOldColumn = pOldRowSchema->pSchema + iOld;
    code = metaAddOrDropColumnIndexOfVirtualSuperTable(pMeta, pParam, pOldColumn, NULL);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  for (; iNew < pNewRowSchema->nCols; iNew++) {
    SSchema *pNewColumn = pNewRowSchema->pSchema + iNew;
    code = metaAddOrDropColumnIndexOfVirtualSuperTable(pMeta, pParam, NULL, pNewColumn);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  return code;
}

static int32_t metaSchemaTableUpdate(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pOldEntry = pParam->pOldEntry;

  if (NULL == pOldEntry) {
    return metaSchemaTableUpsert(pMeta, pParam, META_TABLE_OP_UPDATA);
  }

  if (pEntry->type == TSDB_NORMAL_TABLE || pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    // check row schema
    if (pOldEntry->ntbEntry.schemaRow.version != pEntry->ntbEntry.schemaRow.version) {
      return metaSchemaTableUpsert(pMeta, pParam, META_TABLE_OP_UPDATA);
    }
  } else if (pEntry->type == TSDB_SUPER_TABLE) {
    // check row schema
    if (pOldEntry->stbEntry.schemaRow.version != pEntry->stbEntry.schemaRow.version) {
      if (TABLE_IS_VIRTUAL(pEntry->flags)) {
        return metaUpdateSuperTableRowSchema(pMeta, pParam);
      } else {
        return metaSchemaTableUpsert(pMeta, pParam, META_TABLE_OP_UPDATA);
      }
    }

    // check tag schema
    code = metaUpdateSuperTableTagSchema(pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }

  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t metaSchemaTableDelete(SMeta *pMeta, const SMetaHandleParam *pEntry) {
  // TODO
  return TSDB_CODE_SUCCESS;
}

// Uid Index
static void metaBuildEntryInfo(const SMetaEntry *pEntry, SMetaInfo *pInfo) {
  pInfo->uid = pEntry->uid;
  pInfo->version = pEntry->version;
  if (pEntry->type == TSDB_SUPER_TABLE) {
    pInfo->suid = pEntry->uid;
    pInfo->skmVer = pEntry->stbEntry.schemaRow.version;
  } else if (pEntry->type == TSDB_CHILD_TABLE || pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) {
    pInfo->suid = pEntry->ctbEntry.suid;
    pInfo->skmVer = 0;
  } else if (pEntry->type == TSDB_NORMAL_TABLE || pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    pInfo->suid = 0;
    pInfo->skmVer = pEntry->ntbEntry.schemaRow.version;
  }
}

static int32_t metaUidIdxUpsert(SMeta *pMeta, const SMetaHandleParam *pParam, EMetaTableOp op) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgId = TD_VID(pMeta->pVnode);

  const SMetaEntry *pEntry = pParam->pEntry;

  // update cache
  SMetaInfo info = {0};
  metaBuildEntryInfo(pEntry, &info);
  code = metaCacheUpsert(pMeta, &info);
  if (code) {
    metaErr(vgId, code);
  }

  // put to tdb
  SUidIdxVal value = {
      .suid = info.suid,
      .skmVer = info.skmVer,
      .version = pEntry->version,
  };
  if (META_TABLE_OP_INSERT == op) {
    code = tdbTbInsert(pMeta->pUidIdx, &pEntry->uid, sizeof(pEntry->uid), &value, sizeof(value), pMeta->txn);
  } else if (META_TABLE_OP_UPDATA == op) {
    code = tdbTbUpsert(pMeta->pUidIdx, &pEntry->uid, sizeof(pEntry->uid), &value, sizeof(value), pMeta->txn);
  }
  return code;
}

static int32_t metaUidIdxInsert(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaUidIdxUpsert(pMeta, pParam, META_TABLE_OP_INSERT);
}

static int32_t metaUidIdxUpdate(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaUidIdxUpsert(pMeta, pParam, META_TABLE_OP_UPDATA);
}

static int32_t metaUidIdxDelete(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = 0;

  const SMetaEntry *pEntry = pParam->pOldEntry;

  // delete tdb
  code = tdbTbDelete(pMeta->pUidIdx, &pEntry->uid, sizeof(pEntry->uid), pMeta->txn);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }

  // delete cache
  (void)metaCacheDrop(pMeta, pEntry->uid);
  return code;
}

// Name Index
static int32_t metaNameIdxUpsert(SMeta *pMeta, const SMetaHandleParam *pParam, EMetaTableOp op) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;

  if (META_TABLE_OP_INSERT == op) {
    code = tdbTbInsert(pMeta->pNameIdx, pEntry->name, strlen(pEntry->name) + 1, &pEntry->uid, sizeof(pEntry->uid),
                       pMeta->txn);
  } else if (META_TABLE_OP_UPDATA == op) {
    code = tdbTbUpsert(pMeta->pNameIdx, pEntry->name, strlen(pEntry->name) + 1, &pEntry->uid, sizeof(pEntry->uid),
                       pMeta->txn);
  } else {
    code = TSDB_CODE_INVALID_PARA;
  }
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

static int32_t metaNameIdxInsert(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;
  return metaNameIdxUpsert(pMeta, pParam, META_TABLE_OP_INSERT);
}

static int32_t metaNameIdxUpdate(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaNameIdxUpsert(pMeta, pParam, META_TABLE_OP_UPDATA);
}

static int32_t metaNameIdxDelete(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pOldEntry;
  code = tdbTbDelete(pMeta->pNameIdx, pEntry->name, strlen(pEntry->name) + 1, pMeta->txn);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

// Suid Index
static int32_t metaSUidIdxInsert(SMeta *pMeta, const SMetaHandleParam *pParam) {
  const SMetaEntry *pEntry = pParam->pEntry;

  int32_t code = tdbTbInsert(pMeta->pSuidIdx, &pEntry->uid, sizeof(pEntry->uid), NULL, 0, pMeta->txn);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

static int32_t metaSUidIdxDelete(SMeta *pMeta, const SMetaHandleParam *pParam) {
  const SMetaEntry *pEntry = pParam->pOldEntry;

  int32_t code = tdbTbDelete(pMeta->pSuidIdx, &pEntry->uid, sizeof(pEntry->uid), pMeta->txn);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

// Child Index
static int32_t metaChildIdxUpsert(SMeta *pMeta, const SMetaHandleParam *pParam, EMetaTableOp op) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;

  SCtbIdxKey key = {
      .suid = pEntry->ctbEntry.suid,
      .uid = pEntry->uid,
  };

  if (META_TABLE_OP_INSERT == op) {
    code = tdbTbInsert(pMeta->pCtbIdx, &key, sizeof(key), pEntry->ctbEntry.pTags,
                       ((STag *)(pEntry->ctbEntry.pTags))->len, pMeta->txn);
  } else if (META_TABLE_OP_UPDATA == op) {
    code = tdbTbUpsert(pMeta->pCtbIdx, &key, sizeof(key), pEntry->ctbEntry.pTags,
                       ((STag *)(pEntry->ctbEntry.pTags))->len, pMeta->txn);
  } else {
    code = TSDB_CODE_INVALID_PARA;
  }
  return code;
}

static int32_t metaChildIdxInsert(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaChildIdxUpsert(pMeta, pParam, META_TABLE_OP_INSERT);
}

static int32_t metaChildIdxUpdate(SMeta *pMeta, const SMetaHandleParam *pParam) {
  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pOldEntry = pParam->pOldEntry;
  const SMetaEntry *pSuperEntry = pParam->pSuperEntry;

  const STag *pNewTags = (const STag *)pEntry->ctbEntry.pTags;
  const STag *pOldTags = (const STag *)pOldEntry->ctbEntry.pTags;
  if (pNewTags->len != pOldTags->len || memcmp(pNewTags, pOldTags, pNewTags->len)) {
    return metaChildIdxUpsert(pMeta, pParam, META_TABLE_OP_UPDATA);
  }
  return 0;
}

static int32_t metaChildIdxDelete(SMeta *pMeta, const SMetaHandleParam *pParam) {
  const SMetaEntry *pEntry = pParam->pOldEntry;

  SCtbIdxKey key = {
      .suid = pEntry->ctbEntry.suid,
      .uid = pEntry->uid,
  };
  return tdbTbDelete(pMeta->pCtbIdx, &key, sizeof(key), pMeta->txn);
}

// Tag Index
static int32_t metaFetchTagIdxKey(SMeta *pMeta, const SMetaEntry *pEntry, const SSchema *pTagColumn,
                                  STagIdxKey **ppTagIdxKey, int32_t *pTagIdxKeySize) {
  int32_t code = TSDB_CODE_SUCCESS;

  STagIdxKey *pTagIdxKey = NULL;
  int32_t     nTagIdxKey;
  const void *pTagData = NULL;
  int32_t     nTagData = 0;

  STagVal tagVal = {
      .cid = pTagColumn->colId,
  };

  if (tTagGet((const STag *)pEntry->ctbEntry.pTags, &tagVal)) {
    if (IS_VAR_DATA_TYPE(pTagColumn->type)) {
      pTagData = tagVal.pData;
      nTagData = (int32_t)tagVal.nData;
    } else {
      pTagData = &(tagVal.i64);
      nTagData = tDataTypes[pTagColumn->type].bytes;
    }
  } else {
    if (!IS_VAR_DATA_TYPE(pTagColumn->type)) {
      nTagData = tDataTypes[pTagColumn->type].bytes;
    }
  }

  code = metaCreateTagIdxKey(pEntry->ctbEntry.suid, pTagColumn->colId, pTagData, nTagData, pTagColumn->type,
                             pEntry->uid, &pTagIdxKey, &nTagIdxKey);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  *ppTagIdxKey = pTagIdxKey;
  *pTagIdxKeySize = nTagIdxKey;
  return code;
}

static void metaFetchTagIdxKeyFree(STagIdxKey **ppTagIdxKey) {
  metaDestroyTagIdxKey(*ppTagIdxKey);
  *ppTagIdxKey = NULL;
}

static int32_t metaTagIdxInsert(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pSuperEntry = pParam->pSuperEntry;

  const SSchemaWrapper *pTagSchema = &pSuperEntry->stbEntry.schemaTag;
  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    const SSchema *pTagColumn = &pTagSchema->pSchema[0];

    STagVal tagVal = {
        .cid = pTagColumn->colId,
    };

    const void *pTagData = pEntry->ctbEntry.pTags;
    int32_t     nTagData = ((const STag *)pEntry->ctbEntry.pTags)->len;
    code = metaSaveJsonVarToIdx(pMeta, pEntry, pTagColumn);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
    }
  } else {
    for (int32_t i = 0; i < pTagSchema->nCols; i++) {
      STagIdxKey    *pTagIdxKey = NULL;
      int32_t        nTagIdxKey;
      const SSchema *pTagColumn = &pTagSchema->pSchema[i];

      if (!IS_IDX_ON(pTagColumn)) {
        continue;
      }

      code = metaFetchTagIdxKey(pMeta, pEntry, pTagColumn, &pTagIdxKey, &nTagIdxKey);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      code = tdbTbInsert(pMeta->pTagIdx, pTagIdxKey, nTagIdxKey, NULL, 0, pMeta->txn);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        metaFetchTagIdxKeyFree(&pTagIdxKey);
        return code;
      }
      metaFetchTagIdxKeyFree(&pTagIdxKey);
    }
  }
  return code;
}

static int32_t metaTagIdxUpdate(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry     *pEntry = pParam->pEntry;
  const SMetaEntry     *pOldEntry = pParam->pOldEntry;
  const SMetaEntry     *pSuperEntry = pParam->pSuperEntry;
  const SSchemaWrapper *pTagSchema = &pSuperEntry->stbEntry.schemaTag;
  const STag           *pNewTags = (const STag *)pEntry->ctbEntry.pTags;
  const STag           *pOldTags = (const STag *)pOldEntry->ctbEntry.pTags;

  if (pNewTags->len == pOldTags->len && !memcmp(pNewTags, pOldTags, pNewTags->len)) {
    return code;
  }

  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    code = metaDelJsonVarFromIdx(pMeta, pOldEntry, &pTagSchema->pSchema[0]);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }

    code = metaSaveJsonVarToIdx(pMeta, pEntry, &pTagSchema->pSchema[0]);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  } else {
    for (int32_t i = 0; i < pTagSchema->nCols; i++) {
      const SSchema *pTagColumn = &pTagSchema->pSchema[i];

      if (!IS_IDX_ON(pTagColumn)) {
        continue;
      }

      STagIdxKey *pOldTagIdxKey = NULL;
      int32_t     oldTagIdxKeySize = 0;
      STagIdxKey *pNewTagIdxKey = NULL;
      int32_t     newTagIdxKeySize = 0;

      code = metaFetchTagIdxKey(pMeta, pOldEntry, pTagColumn, &pOldTagIdxKey, &oldTagIdxKeySize);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      code = metaFetchTagIdxKey(pMeta, pEntry, pTagColumn, &pNewTagIdxKey, &newTagIdxKeySize);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        metaFetchTagIdxKeyFree(&pOldTagIdxKey);
        return code;
      }

      if (tagIdxKeyCmpr(pOldTagIdxKey, oldTagIdxKeySize, pNewTagIdxKey, newTagIdxKeySize)) {
        code = tdbTbDelete(pMeta->pTagIdx, pOldTagIdxKey, oldTagIdxKeySize, pMeta->txn);
        if (code) {
          metaErr(TD_VID(pMeta->pVnode), code);
          metaFetchTagIdxKeyFree(&pOldTagIdxKey);
          metaFetchTagIdxKeyFree(&pNewTagIdxKey);
          return code;
        }

        code = tdbTbInsert(pMeta->pTagIdx, pNewTagIdxKey, newTagIdxKeySize, NULL, 0, pMeta->txn);
        if (code) {
          metaErr(TD_VID(pMeta->pVnode), code);
          metaFetchTagIdxKeyFree(&pOldTagIdxKey);
          metaFetchTagIdxKeyFree(&pNewTagIdxKey);
          return code;
        }
      }

      metaFetchTagIdxKeyFree(&pOldTagIdxKey);
      metaFetchTagIdxKeyFree(&pNewTagIdxKey);
    }
  }
  return code;
}

static int32_t metaTagIdxDelete(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry     *pEntry = pParam->pEntry;
  const SMetaEntry     *pChild = pParam->pOldEntry;
  const SMetaEntry     *pSuper = pParam->pSuperEntry;
  const SSchemaWrapper *pTagSchema = &pSuper->stbEntry.schemaTag;
  const SSchema        *pTagColumn = NULL;
  const STag           *pTags = (const STag *)pChild->ctbEntry.pTags;

  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    pTagColumn = &pTagSchema->pSchema[0];
    code = metaDelJsonVarFromIdx(pMeta, pChild, pTagColumn);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
    }
  } else {
    for (int32_t i = 0; i < pTagSchema->nCols; i++) {
      pTagColumn = &pTagSchema->pSchema[i];
      if (!IS_IDX_ON(pTagColumn)) {
        continue;
      }

      STagIdxKey *pTagIdxKey = NULL;
      int32_t     nTagIdxKey;

      code = metaFetchTagIdxKey(pMeta, pChild, pTagColumn, &pTagIdxKey, &nTagIdxKey);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        return code;
      }

      code = tdbTbDelete(pMeta->pTagIdx, pTagIdxKey, nTagIdxKey, pMeta->txn);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        metaFetchTagIdxKeyFree(&pTagIdxKey);
        return code;
      }
      metaFetchTagIdxKeyFree(&pTagIdxKey);
    }
  }
  return code;
}

// Btime Index
static int32_t metaBtimeIdxUpsert(SMeta *pMeta, const SMetaHandleParam *pParam, EMetaTableOp op) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry;
  if (META_TABLE_OP_DELETE == op) {
    pEntry = pParam->pOldEntry;
  } else {
    pEntry = pParam->pEntry;
  }

  SBtimeIdxKey key = {
      .uid = pEntry->uid,
  };

  if (TSDB_CHILD_TABLE == pEntry->type || TSDB_VIRTUAL_CHILD_TABLE == pEntry->type) {
    key.btime = pEntry->ctbEntry.btime;
  } else if (TSDB_NORMAL_TABLE == pEntry->type || TSDB_VIRTUAL_NORMAL_TABLE == pEntry->type) {
    key.btime = pEntry->ntbEntry.btime;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  if (META_TABLE_OP_INSERT == op) {
    code = tdbTbInsert(pMeta->pBtimeIdx, &key, sizeof(key), NULL, 0, pMeta->txn);
  } else if (META_TABLE_OP_UPDATA == op) {
    code = tdbTbUpsert(pMeta->pBtimeIdx, &key, sizeof(key), NULL, 0, pMeta->txn);
  } else if (META_TABLE_OP_DELETE == op) {
    code = tdbTbDelete(pMeta->pBtimeIdx, &key, sizeof(key), pMeta->txn);
  } else {
    code = TSDB_CODE_INVALID_PARA;
  }
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

static int32_t metaBtimeIdxInsert(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaBtimeIdxUpsert(pMeta, pParam, META_TABLE_OP_INSERT);
}

static int32_t metaBtimeIdxUpdate(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaBtimeIdxUpsert(pMeta, pParam, META_TABLE_OP_UPDATA);
}

static int32_t metaBtimeIdxDelete(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaBtimeIdxUpsert(pMeta, pParam, META_TABLE_OP_DELETE);
}

// TTL Index
static int32_t metaTtlIdxUpsert(SMeta *pMeta, const SMetaHandleParam *pParam, EMetaTableOp op) {
  const SMetaEntry *pEntry = pParam->pEntry;

  STtlUpdTtlCtx ctx = {
      .uid = pEntry->uid,
      .pTxn = pMeta->txn,
  };
  if (TSDB_CHILD_TABLE == pEntry->type) {
    ctx.ttlDays = pEntry->ctbEntry.ttlDays;
    ctx.changeTimeMs = pEntry->ctbEntry.btime;
  } else if (TSDB_NORMAL_TABLE == pEntry->type) {
    ctx.ttlDays = pEntry->ntbEntry.ttlDays;
    ctx.changeTimeMs = pEntry->ntbEntry.btime;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t ret = ttlMgrInsertTtl(pMeta->pTtlMgr, &ctx);
  if (ret < 0) {
    metaError("vgId:%d, failed to insert ttl, uid: %" PRId64 " %s", TD_VID(pMeta->pVnode), pEntry->uid, tstrerror(ret));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t metaTtlIdxInsert(SMeta *pMeta, const SMetaHandleParam *pParam) {
  return metaTtlIdxUpsert(pMeta, pParam, META_TABLE_OP_INSERT);
}

static int32_t metaTtlIdxDelete(SMeta *pMeta, const SMetaHandleParam *pParam);

static int32_t metaTtlIdxUpdate(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pOldEntry = pParam->pOldEntry;

  if ((pEntry->type == TSDB_CHILD_TABLE && pOldEntry->ctbEntry.ttlDays != pEntry->ctbEntry.ttlDays) ||
      (pEntry->type == TSDB_NORMAL_TABLE && pOldEntry->ntbEntry.ttlDays != pEntry->ntbEntry.ttlDays)) {
    code = metaTtlIdxDelete(pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
    }

    code = metaTtlIdxInsert(pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t metaTtlIdxDelete(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pOldEntry;
  STtlDelTtlCtx     ctx = {
          .uid = pEntry->uid,
          .pTxn = pMeta->txn,
  };

  if (TSDB_CHILD_TABLE == pEntry->type) {
    ctx.ttlDays = pEntry->ctbEntry.ttlDays;
  } else if (TSDB_NORMAL_TABLE == pEntry->type) {
    ctx.ttlDays = pEntry->ntbEntry.ttlDays;
  } else {
    code = TSDB_CODE_INVALID_PARA;
  }

  if (TSDB_CODE_SUCCESS == code) {
    int32_t ret = ttlMgrDeleteTtl(pMeta->pTtlMgr, &ctx);
    if (ret < 0) {
      metaError("vgId:%d, failed to delete ttl, uid: %" PRId64 " %s", TD_VID(pMeta->pVnode), pEntry->uid,
                tstrerror(ret));
    }
  }
  return code;
}

static void metaTimeSeriesNotifyCheck(SMeta *pMeta) {
#if defined(TD_ENTERPRISE)
  int64_t nTimeSeries = metaGetTimeSeriesNum(pMeta, 0);
  int64_t deltaTS = nTimeSeries - pMeta->pVnode->config.vndStats.numOfReportedTimeSeries;
  if (deltaTS > tsTimeSeriesThreshold) {
    if (0 == atomic_val_compare_exchange_8(&dmNotifyHdl.state, 1, 2)) {
      if (tsem_post(&dmNotifyHdl.sem) != 0) {
        metaError("vgId:%d, failed to post semaphore, errno:%d", TD_VID(pMeta->pVnode), ERRNO);
      }
    }
  }
#endif
}

static int32_t (*metaTableOpFn[META_TABLE_MAX][META_TABLE_OP_MAX])(SMeta *pMeta, const SMetaHandleParam *pParam) =
    {
        [META_ENTRY_TABLE] =
            {
                [META_TABLE_OP_INSERT] = metaEntryTableInsert,
                [META_TABLE_OP_UPDATA] = metaEntryTableUpdate,
                [META_TABLE_OP_DELETE] = metaEntryTableDelete,
            },
        [META_SCHEMA_TABLE] =
            {
                [META_TABLE_OP_INSERT] = metaSchemaTableInsert,
                [META_TABLE_OP_UPDATA] = metaSchemaTableUpdate,
                [META_TABLE_OP_DELETE] = metaSchemaTableDelete,
            },
        [META_UID_IDX] =
            {
                [META_TABLE_OP_INSERT] = metaUidIdxInsert,
                [META_TABLE_OP_UPDATA] = metaUidIdxUpdate,
                [META_TABLE_OP_DELETE] = metaUidIdxDelete,
            },
        [META_NAME_IDX] =
            {
                [META_TABLE_OP_INSERT] = metaNameIdxInsert,
                [META_TABLE_OP_UPDATA] = metaNameIdxUpdate,
                [META_TABLE_OP_DELETE] = metaNameIdxDelete,
            },
        [META_SUID_IDX] =
            {
                [META_TABLE_OP_INSERT] = metaSUidIdxInsert,
                [META_TABLE_OP_UPDATA] = NULL,
                [META_TABLE_OP_DELETE] = metaSUidIdxDelete,
            },
        [META_CHILD_IDX] =
            {
                [META_TABLE_OP_INSERT] = metaChildIdxInsert,
                [META_TABLE_OP_UPDATA] = metaChildIdxUpdate,
                [META_TABLE_OP_DELETE] = metaChildIdxDelete,
            },
        [META_TAG_IDX] =
            {
                [META_TABLE_OP_INSERT] = metaTagIdxInsert,
                [META_TABLE_OP_UPDATA] = metaTagIdxUpdate,
                [META_TABLE_OP_DELETE] = metaTagIdxDelete,
            },
        [META_BTIME_IDX] =
            {
                [META_TABLE_OP_INSERT] = metaBtimeIdxInsert,
                [META_TABLE_OP_UPDATA] = metaBtimeIdxUpdate,
                [META_TABLE_OP_DELETE] = metaBtimeIdxDelete,
            },
        [META_TTL_IDX] =
            {
                [META_TABLE_OP_INSERT] = metaTtlIdxInsert,
                [META_TABLE_OP_UPDATA] = metaTtlIdxUpdate,
                [META_TABLE_OP_DELETE] = metaTtlIdxDelete,
            },
};

static int32_t metaHandleSuperTableCreateImpl(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_INSERT},   //
      {META_SCHEMA_TABLE, META_TABLE_OP_UPDATA},  // TODO: here should be insert
      {META_UID_IDX, META_TABLE_OP_INSERT},       //
      {META_NAME_IDX, META_TABLE_OP_INSERT},      //
      {META_SUID_IDX, META_TABLE_OP_INSERT},      //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp          *op = &ops[i];
    const SMetaHandleParam param = {
        .pEntry = pEntry,
    };
    code = metaTableOpFn[op->table][op->op](pMeta, &param);
    if (TSDB_CODE_SUCCESS != code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  return code;
}
static int32_t metaHandleSuperTableCreate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  metaWLock(pMeta);
  code = metaHandleSuperTableCreateImpl(pMeta, pEntry);
  metaULock(pMeta);

  if (TSDB_CODE_SUCCESS == code) {
    pMeta->pVnode->config.vndStats.numOfSTables++;

    metaInfo("vgId:%d, %s success, version:%" PRId64 " type:%d uid:%" PRId64 " name:%s", TD_VID(pMeta->pVnode),
             __func__, pEntry->version, pEntry->type, pEntry->uid, pEntry->name);
  } else {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

static int32_t metaHandleNormalTableCreateImpl(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_INSERT},   //
      {META_SCHEMA_TABLE, META_TABLE_OP_UPDATA},  // TODO: need to be insert
      {META_UID_IDX, META_TABLE_OP_INSERT},       //
      {META_NAME_IDX, META_TABLE_OP_INSERT},      //
      {META_BTIME_IDX, META_TABLE_OP_INSERT},     //
      {META_TTL_IDX, META_TABLE_OP_INSERT},       //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];

    SMetaHandleParam param = {
        .pEntry = pEntry,
    };

    code = metaTableOpFn[op->table][op->op](pMeta, &param);
    if (TSDB_CODE_SUCCESS != code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  return code;
}
static int32_t metaHandleNormalTableCreate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  // update TDB
  metaWLock(pMeta);
  code = metaHandleNormalTableCreateImpl(pMeta, pEntry);
  metaULock(pMeta);

  // update other stuff
  if (TSDB_CODE_SUCCESS == code) {
    pMeta->pVnode->config.vndStats.numOfNTables++;
    pMeta->pVnode->config.vndStats.numOfNTimeSeries += pEntry->ntbEntry.schemaRow.nCols - 1;

    if (!TSDB_CACHE_NO(pMeta->pVnode->config) && pMeta->pVnode->pTsdb) {
      int32_t rc = tsdbCacheNewTable(pMeta->pVnode->pTsdb, pEntry->uid, -1, &pEntry->ntbEntry.schemaRow);
      if (rc < 0) {
        metaError("vgId:%d, failed to create table:%s since %s", TD_VID(pMeta->pVnode), pEntry->name, tstrerror(rc));
      }
    }
    metaTimeSeriesNotifyCheck(pMeta);
  } else {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

static int32_t metaHandleChildTableCreateImpl(SMeta *pMeta, const SMetaEntry *pEntry, const SMetaEntry *pSuperEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_INSERT},  //
      {META_UID_IDX, META_TABLE_OP_INSERT},      //
      {META_NAME_IDX, META_TABLE_OP_INSERT},     //
      {META_CHILD_IDX, META_TABLE_OP_INSERT},    //
      {META_TAG_IDX, META_TABLE_OP_INSERT},      //
      {META_BTIME_IDX, META_TABLE_OP_INSERT},    //
      {META_TTL_IDX, META_TABLE_OP_INSERT},      //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];

    SMetaHandleParam param = {
        .pEntry = pEntry,
        .pSuperEntry = pSuperEntry,
    };

    code = metaTableOpFn[op->table][op->op](pMeta, &param);
    if (TSDB_CODE_SUCCESS != code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    metaUpdateStbStats(pMeta, pSuperEntry->uid, 1, 0, -1);
    int32_t ret = metaUidCacheClear(pMeta, pSuperEntry->uid);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }

    ret = metaStableTagFilterCacheUpdateUid(
      pMeta, pEntry, pSuperEntry, STABLE_TAG_FILTER_CACHE_ADD_TABLE);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }

    ret = metaTbGroupCacheClear(pMeta, pSuperEntry->uid);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }
  }
  return code;
}

static int32_t metaHandleChildTableCreate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pSuperEntry = NULL;

  // get the super table entry
  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuperEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  // update TDB
  metaWLock(pMeta);
  code = metaHandleChildTableCreateImpl(pMeta, pEntry, pSuperEntry);
  metaULock(pMeta);

  // update other stuff
  if (TSDB_CODE_SUCCESS == code) {
    pMeta->pVnode->config.vndStats.numOfCTables++;

    if (!metaTbInFilterCache(pMeta, pSuperEntry->name, 1)) {
      int32_t nCols = 0;
      int32_t ret = metaGetStbStats(pMeta->pVnode, pSuperEntry->uid, 0, &nCols, 0);
      if (ret < 0) {
        metaErr(TD_VID(pMeta->pVnode), ret);
      }
      pMeta->pVnode->config.vndStats.numOfTimeSeries += (nCols > 0 ? nCols - 1 : 0);
    }

    if (!TSDB_CACHE_NO(pMeta->pVnode->config) && pMeta->pVnode->pTsdb) {
      int32_t rc = tsdbCacheNewTable(pMeta->pVnode->pTsdb, pEntry->uid, pEntry->ctbEntry.suid, NULL);
      if (rc < 0) {
        metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
                  tstrerror(rc));
      }
    }

  } else {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  metaTimeSeriesNotifyCheck(pMeta);
  metaFetchEntryFree(&pSuperEntry);
  return code;
}

static int32_t metaHandleVirtualNormalTableCreateImpl(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_INSERT},   //
      {META_SCHEMA_TABLE, META_TABLE_OP_UPDATA},  // TODO: need to be insert
      {META_UID_IDX, META_TABLE_OP_INSERT},       //
      {META_NAME_IDX, META_TABLE_OP_INSERT},      //
      {META_BTIME_IDX, META_TABLE_OP_INSERT},     //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];

    SMetaHandleParam param = {
        .pEntry = pEntry,
    };

    code = metaTableOpFn[op->table][op->op](pMeta, &param);
    if (TSDB_CODE_SUCCESS != code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  return code;
}

static int32_t metaHandleVirtualNormalTableCreate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  // update TDB
  metaWLock(pMeta);
  code = metaHandleVirtualNormalTableCreateImpl(pMeta, pEntry);
  metaULock(pMeta);

  // update other stuff
  if (TSDB_CODE_SUCCESS == code) {
    pMeta->pVnode->config.vndStats.numOfVTables++;
  } else {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

static int32_t metaHandleVirtualChildTableCreateImpl(SMeta *pMeta, const SMetaEntry *pEntry,
                                                     const SMetaEntry *pSuperEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_INSERT},  //
      {META_UID_IDX, META_TABLE_OP_INSERT},      //
      {META_NAME_IDX, META_TABLE_OP_INSERT},     //
      {META_CHILD_IDX, META_TABLE_OP_INSERT},    //
      {META_TAG_IDX, META_TABLE_OP_INSERT},      //
      {META_BTIME_IDX, META_TABLE_OP_INSERT},    //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];

    SMetaHandleParam param = {
        .pEntry = pEntry,
        .pSuperEntry = pSuperEntry,
    };

    code = metaTableOpFn[op->table][op->op](pMeta, &param);
    if (TSDB_CODE_SUCCESS != code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    metaUpdateStbStats(pMeta, pSuperEntry->uid, 1, 0, -1);
    int32_t ret = metaUidCacheClear(pMeta, pSuperEntry->uid);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }

    ret = metaStableTagFilterCacheUpdateUid(
      pMeta, pEntry, pSuperEntry, STABLE_TAG_FILTER_CACHE_ADD_TABLE);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }

    ret = metaTbGroupCacheClear(pMeta, pSuperEntry->uid);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }

    ret = metaRefDbsCacheClear(pMeta, pSuperEntry->uid);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }
  }

  return code;
}

static int32_t metaHandleVirtualChildTableCreate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pSuperEntry = NULL;

  // get the super table entry
  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuperEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  // update TDB
  metaWLock(pMeta);
  code = metaHandleVirtualChildTableCreateImpl(pMeta, pEntry, pSuperEntry);
  metaULock(pMeta);

  // update other stuff
  if (TSDB_CODE_SUCCESS == code) {
    pMeta->pVnode->config.vndStats.numOfVCTables++;
  } else {
    metaErr(TD_VID(pMeta->pVnode), code);
  }

  metaFetchEntryFree(&pSuperEntry);
  return code;
}

static int32_t metaHandleNormalTableDropImpl(SMeta *pMeta, SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_DELETE},  //
      {META_UID_IDX, META_TABLE_OP_DELETE},      //
      {META_NAME_IDX, META_TABLE_OP_DELETE},     //
      {META_BTIME_IDX, META_TABLE_OP_DELETE},    //
      {META_TTL_IDX, META_TABLE_OP_DELETE},      //

      // {META_SCHEMA_TABLE, META_TABLE_OP_DELETE},  //
  };

  for (int32_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];
    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (code) {
      const SMetaEntry *pEntry = pParam->pEntry;
      metaErr(TD_VID(pMeta->pVnode), code);
    }
  }

  return code;
}

static int32_t metaHandleNormalTableDrop(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pOldEntry = NULL;

  // fetch the entry
  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pOldEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pOldEntry,
  };

  // do the drop
  metaWLock(pMeta);
  code = metaHandleNormalTableDropImpl(pMeta, &param);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    return code;
  }

  // update other stuff
  pMeta->pVnode->config.vndStats.numOfNTables--;
  pMeta->pVnode->config.vndStats.numOfNTimeSeries -= (pOldEntry->ntbEntry.schemaRow.nCols - 1);

#if 0
  if (tbUids) {
    if (taosArrayPush(tbUids, &uid) == NULL) {
      rc = terrno;
      goto _exit;
    }
  }
#endif

  if (!TSDB_CACHE_NO(pMeta->pVnode->config) && pMeta->pVnode->pTsdb) {
    int32_t ret = tsdbCacheDropTable(pMeta->pVnode->pTsdb, pOldEntry->uid, 0, NULL);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }
  }

  metaFetchEntryFree(&pOldEntry);
  return code;
}

static int32_t metaHandleChildTableDropImpl(SMeta *pMeta, const SMetaHandleParam *pParam, bool superDropped) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pChild = pParam->pOldEntry;
  const SMetaEntry *pSuper = pParam->pSuperEntry;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_DELETE},  //
      {META_UID_IDX, META_TABLE_OP_DELETE},      //
      {META_NAME_IDX, META_TABLE_OP_DELETE},     //
      {META_CHILD_IDX, META_TABLE_OP_DELETE},    //
      {META_TAG_IDX, META_TABLE_OP_DELETE},      //
      {META_BTIME_IDX, META_TABLE_OP_DELETE},    //
      {META_TTL_IDX, META_TABLE_OP_DELETE},      //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];

    if (op->table == META_ENTRY_TABLE && superDropped) {
      continue;
    }

    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  --pMeta->pVnode->config.vndStats.numOfCTables;
  metaUpdateStbStats(pMeta, pParam->pSuperEntry->uid, -1, 0, -1);
  int32_t ret = metaUidCacheClear(pMeta, pSuper->uid);
  if (ret < 0) {
    metaErr(TD_VID(pMeta->pVnode), ret);
  }

  ret = metaStableTagFilterCacheUpdateUid(
    pMeta, pChild, pSuper, STABLE_TAG_FILTER_CACHE_DROP_TABLE);
  if (ret < 0) {
    metaErr(TD_VID(pMeta->pVnode), ret);
  }

  ret = metaTbGroupCacheClear(pMeta, pSuper->uid);
  if (ret < 0) {
    metaErr(TD_VID(pMeta->pVnode), ret);
  }
  return code;
}

static int32_t metaHandleChildTableDrop(SMeta *pMeta, const SMetaEntry *pEntry, bool superDropped) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pChild = NULL;
  SMetaEntry *pSuper = NULL;

  // fetch old entry
  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pChild);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  // fetch super entry
  code = metaFetchEntryByUid(pMeta, pChild->ctbEntry.suid, &pSuper);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pChild);
    return code;
  }

  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pChild,
      .pSuperEntry = pSuper,
  };

  // do the drop
  metaWLock(pMeta);
  code = metaHandleChildTableDropImpl(pMeta, &param, superDropped);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    return code;
  }

  // do other stuff
  if (!metaTbInFilterCache(pMeta, pSuper->name, 1)) {
    int32_t      nCols = 0;
    SVnodeStats *pStats = &pMeta->pVnode->config.vndStats;
    if (metaGetStbStats(pMeta->pVnode, pSuper->uid, NULL, &nCols, 0) == 0) {
      pStats->numOfTimeSeries -= nCols - 1;
    }
  }

  if (!TSDB_CACHE_NO(pMeta->pVnode->config) && pMeta->pVnode->pTsdb) {
    int32_t ret = tsdbCacheDropTable(pMeta->pVnode->pTsdb, pChild->uid, pSuper->uid, NULL);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }
  }

#if 0
  if (tbUids) {
    if (taosArrayPush(tbUids, &uid) == NULL) {
      rc = terrno;
      goto _exit;
    }
  }

  if ((type == TSDB_CHILD_TABLE) && tbUid) {
    *tbUid = uid;
  }
#endif
  metaFetchEntryFree(&pChild);
  metaFetchEntryFree(&pSuper);
  return code;
}

static int32_t metaHandleVirtualNormalTableDropImpl(SMeta *pMeta, SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_DELETE},  //
      {META_UID_IDX, META_TABLE_OP_DELETE},      //
      {META_NAME_IDX, META_TABLE_OP_DELETE},     //
      {META_BTIME_IDX, META_TABLE_OP_DELETE},    //

      // {META_SCHEMA_TABLE, META_TABLE_OP_DELETE},  //
  };

  for (int32_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];
    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (code) {
      const SMetaEntry *pEntry = pParam->pEntry;
      metaErr(TD_VID(pMeta->pVnode), code);
    }
  }

  return code;
}

static int32_t metaHandleVirtualNormalTableDrop(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pOldEntry = NULL;

  // fetch the entry
  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pOldEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pOldEntry,
  };

  // do the drop
  metaWLock(pMeta);
  code = metaHandleVirtualNormalTableDropImpl(pMeta, &param);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    return code;
  }

  // update other stuff
  pMeta->pVnode->config.vndStats.numOfVTables--;

#if 0
  if (tbUids) {
    if (taosArrayPush(tbUids, &uid) == NULL) {
      rc = terrno;
      goto _exit;
    }
  }
#endif

  if (!TSDB_CACHE_NO(pMeta->pVnode->config) && pMeta->pVnode->pTsdb) {
    int32_t ret = tsdbCacheDropTable(pMeta->pVnode->pTsdb, pOldEntry->uid, 0, NULL);
    if (ret < 0) {
      metaErr(TD_VID(pMeta->pVnode), ret);
    }
  }

  metaFetchEntryFree(&pOldEntry);
  return code;
}

static int32_t metaHandleVirtualChildTableDropImpl(SMeta *pMeta, const SMetaHandleParam *pParam, bool superDropped) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pChild = pParam->pOldEntry;
  const SMetaEntry *pSuper = pParam->pSuperEntry;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_DELETE},  //
      {META_UID_IDX, META_TABLE_OP_DELETE},      //
      {META_NAME_IDX, META_TABLE_OP_DELETE},     //
      {META_CHILD_IDX, META_TABLE_OP_DELETE},    //
      {META_TAG_IDX, META_TABLE_OP_DELETE},      //
      {META_BTIME_IDX, META_TABLE_OP_DELETE},    //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];

    if (op->table == META_ENTRY_TABLE && superDropped) {
      continue;
    }

    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  --pMeta->pVnode->config.vndStats.numOfVCTables;
  metaUpdateStbStats(pMeta, pParam->pSuperEntry->uid, -1, 0, -1);
  int32_t ret = metaUidCacheClear(pMeta, pSuper->uid);
  if (ret < 0) {
    metaErr(TD_VID(pMeta->pVnode), ret);
  }

  ret = metaStableTagFilterCacheUpdateUid(
    pMeta, pChild, pSuper, STABLE_TAG_FILTER_CACHE_DROP_TABLE);
  if (ret < 0) {
    metaErr(TD_VID(pMeta->pVnode), ret);
  }

  ret = metaTbGroupCacheClear(pMeta, pSuper->uid);
  if (ret < 0) {
    metaErr(TD_VID(pMeta->pVnode), ret);
  }

  ret = metaRefDbsCacheClear(pMeta, pSuper->uid);
  if (ret < 0) {
    metaErr(TD_VID(pMeta->pVnode), ret);
  }

  return code;
}

static int32_t metaHandleVirtualChildTableDrop(SMeta *pMeta, const SMetaEntry *pEntry, bool superDropped) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pChild = NULL;
  SMetaEntry *pSuper = NULL;

  // fetch old entry
  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pChild);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  // fetch super entry
  code = metaFetchEntryByUid(pMeta, pChild->ctbEntry.suid, &pSuper);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pChild);
    return code;
  }

  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pChild,
      .pSuperEntry = pSuper,
  };

  // do the drop
  metaWLock(pMeta);
  code = metaHandleVirtualChildTableDropImpl(pMeta, &param, superDropped);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    return code;
  }

  metaFetchEntryFree(&pChild);
  metaFetchEntryFree(&pSuper);
  return code;
}

int32_t metaGetChildUidsOfSuperTable(SMeta *pMeta, tb_uid_t suid, SArray **childList) {
  int32_t code = TSDB_CODE_SUCCESS;
  void   *key = NULL;
  int32_t keySize = 0;
  int32_t c;

  *childList = taosArrayInit(64, sizeof(tb_uid_t));
  if (*childList == NULL) {
    return terrno;
  }

  TBC *cursor = NULL;
  code = tdbTbcOpen(pMeta->pCtbIdx, &cursor, NULL);
  if (code) {
    taosArrayDestroy(*childList);
    *childList = NULL;
    return code;
  }

  int32_t rc = tdbTbcMoveTo(cursor,
                            &(SCtbIdxKey){
                                .suid = suid,
                                .uid = INT64_MIN,
                            },
                            sizeof(SCtbIdxKey), &c);
  if (rc < 0) {
    tdbTbcClose(cursor);
    return 0;
  }

  for (;;) {
    if (tdbTbcNext(cursor, &key, &keySize, NULL, NULL) < 0) {
      break;
    }

    if (((SCtbIdxKey *)key)->suid < suid) {
      continue;
    } else if (((SCtbIdxKey *)key)->suid > suid) {
      break;
    }

    if (taosArrayPush(*childList, &(((SCtbIdxKey *)key)->uid)) == NULL) {
      tdbFreeClear(key);
      tdbTbcClose(cursor);
      taosArrayDestroy(*childList);
      *childList = NULL;
      return terrno;
    }
  }

  tdbTbcClose(cursor);
  tdbFreeClear(key);
  return code;
}

static int32_t metaHandleSuperTableDropImpl(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t           code = TSDB_CODE_SUCCESS;
  const SMetaEntry *pEntry = pParam->pEntry;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_DELETE},  //
      {META_UID_IDX, META_TABLE_OP_DELETE},      //
      {META_NAME_IDX, META_TABLE_OP_DELETE},     //
      {META_SUID_IDX, META_TABLE_OP_DELETE},     //

      // {META_SCHEMA_TABLE, META_TABLE_OP_UPDATA},  // TODO: here should be insert
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];

    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (TSDB_CODE_SUCCESS != code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  int32_t ret = metaStatsCacheDrop(pMeta, pEntry->uid);
  if (ret < 0) {
    metaErr(TD_VID(pMeta->pVnode), ret);
  }
  return code;
}

static int32_t metaHandleNormalTableUpdateImpl(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_UPDATA},   //
      {META_SCHEMA_TABLE, META_TABLE_OP_UPDATA},  //
      {META_UID_IDX, META_TABLE_OP_UPDATA},       //
      {META_TTL_IDX, META_TABLE_OP_UPDATA},       //
  };
  for (int32_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];
    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }
#if 0
  if (metaUpdateChangeTime(pMeta, entry.uid, pAlterTbReq->ctimeMs) < 0) {
    metaError("vgId:%d, failed to update change time:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name, entry.uid);
  }
#endif
  return code;
}

static int32_t metaHandleVirtualNormalTableUpdateImpl(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_UPDATA},   //
      {META_SCHEMA_TABLE, META_TABLE_OP_UPDATA},  //
      {META_UID_IDX, META_TABLE_OP_UPDATA},       //
  };
  for (int32_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];
    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }
#if 0
  if (metaUpdateChangeTime(pMeta, entry.uid, pAlterTbReq->ctimeMs) < 0) {
    metaError("vgId:%d, failed to update change time:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name, entry.uid);
  }
#endif
  return code;
}

static int32_t metaHandleVirtualChildTableUpdateImpl(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pOldEntry = pParam->pOldEntry;
  const SMetaEntry *pSuperEntry = pParam->pSuperEntry;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_UPDATA},  //
      {META_UID_IDX, META_TABLE_OP_UPDATA},      //
      {META_TAG_IDX, META_TABLE_OP_UPDATA},      //
      {META_CHILD_IDX, META_TABLE_OP_UPDATA},    //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];
    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  if (metaUidCacheClear(pMeta, pSuperEntry->uid) < 0) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }

  // update stable tag filter cache: drop old then add new
  code = metaStableTagFilterCacheUpdateUid(
    pMeta, pOldEntry, pSuperEntry, STABLE_TAG_FILTER_CACHE_DROP_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  code = metaStableTagFilterCacheUpdateUid(
    pMeta, pEntry, pSuperEntry, STABLE_TAG_FILTER_CACHE_ADD_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }

  if (metaTbGroupCacheClear(pMeta, pSuperEntry->uid) < 0) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }

  if (metaRefDbsCacheClear(pMeta, pSuperEntry->uid) < 0) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

static int32_t metaHandleChildTableUpdateImpl(SMeta *pMeta, const SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pOldEntry = pParam->pOldEntry;
  const SMetaEntry *pSuperEntry = pParam->pSuperEntry;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_UPDATA},  //
      {META_UID_IDX, META_TABLE_OP_UPDATA},      //
      {META_TAG_IDX, META_TABLE_OP_UPDATA},      //
      {META_CHILD_IDX, META_TABLE_OP_UPDATA},    //
      {META_TTL_IDX, META_TABLE_OP_UPDATA},      //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];
    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  if (metaUidCacheClear(pMeta, pSuperEntry->uid) < 0) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }

  // update stable tag filter cache: drop old then add new
  code = metaStableTagFilterCacheUpdateUid(
    pMeta, pOldEntry, pSuperEntry, STABLE_TAG_FILTER_CACHE_DROP_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  code = metaStableTagFilterCacheUpdateUid(
    pMeta, pEntry, pSuperEntry, STABLE_TAG_FILTER_CACHE_ADD_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }

  if (metaTbGroupCacheClear(pMeta, pSuperEntry->uid) < 0) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
#if 0
  if (metaUpdateChangeTime(pMeta, ctbEntry.uid, pReq->ctimeMs) < 0) {
    metaError("meta/table: failed to update change time:%s uid:%" PRId64, ctbEntry.name, ctbEntry.uid);
  }
#endif
}

static int32_t metaHandleSuperTableUpdateImpl(SMeta *pMeta, SMetaHandleParam *pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  const SMetaEntry *pEntry = pParam->pEntry;
  const SMetaEntry *pOldEntry = pParam->pOldEntry;

  SMetaTableOp ops[] = {
      {META_ENTRY_TABLE, META_TABLE_OP_UPDATA},   //
      {META_UID_IDX, META_TABLE_OP_UPDATA},       //
      {META_SCHEMA_TABLE, META_TABLE_OP_UPDATA},  //
  };

  for (int i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
    SMetaTableOp *op = &ops[i];
    code = metaTableOpFn[op->table][op->op](pMeta, pParam);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
      return code;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    metaUpdateStbStats(pMeta, pEntry->uid, 0, pEntry->stbEntry.schemaRow.nCols - pOldEntry->stbEntry.schemaRow.nCols,
                       pEntry->stbEntry.keep);
  }

  return code;
}

static int32_t metaHandleSuperTableUpdate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaEntry *pOldEntry = NULL;

  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pOldEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pOldEntry,
  };
  metaWLock(pMeta);
  code = metaHandleSuperTableUpdateImpl(pMeta, &param);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    return code;
  }

  int     nCols = pEntry->stbEntry.schemaRow.nCols;
  int     onCols = pOldEntry->stbEntry.schemaRow.nCols;
  int32_t deltaCol = nCols - onCols;
  bool    updStat = deltaCol != 0 && !TABLE_IS_VIRTUAL(pEntry->flags) && !metaTbInFilterCache(pMeta, pEntry->name, 1);

  if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
    STsdb  *pTsdb = pMeta->pVnode->pTsdb;
    SArray *uids = NULL; /*taosArrayInit(8, sizeof(int64_t));
     if (uids == NULL) {
       metaErr(TD_VID(pMeta->pVnode), code);
       metaFetchEntryFree(&pOldEntry);
       return terrno;
       }*/
    if (deltaCol == 1) {
      int16_t cid = pEntry->stbEntry.schemaRow.pSchema[nCols - 1].colId;
      int8_t  col_type = pEntry->stbEntry.schemaRow.pSchema[nCols - 1].type;

      code = metaGetChildUidsOfSuperTable(pMeta, pEntry->uid, &uids);
      if (code) {
        metaErr(TD_VID(pMeta->pVnode), code);
        metaFetchEntryFree(&pOldEntry);
        return code;
      }
      if (pTsdb) {
        TAOS_CHECK_RETURN(tsdbCacheNewSTableColumn(pTsdb, uids, cid, col_type));
      }
    } else if (deltaCol == -1) {
      int16_t cid = -1;
      bool    hasPrimaryKey = false;
      if (onCols >= 2) {
        hasPrimaryKey = (pOldEntry->stbEntry.schemaRow.pSchema[1].flags & COL_IS_KEY) ? true : false;
      }
      for (int i = 0, j = 0; i < nCols && j < onCols; ++i, ++j) {
        if (pEntry->stbEntry.schemaRow.pSchema[i].colId != pOldEntry->stbEntry.schemaRow.pSchema[j].colId) {
          cid = pOldEntry->stbEntry.schemaRow.pSchema[j].colId;
          break;
        }
      }

      if (cid != -1) {
        code = metaGetChildUidsOfSuperTable(pMeta, pEntry->uid, &uids);
        if (code) {
          metaErr(TD_VID(pMeta->pVnode), code);
          metaFetchEntryFree(&pOldEntry);
          return code;
        }
        if (pTsdb) {
          TAOS_CHECK_RETURN(tsdbCacheDropSTableColumn(pTsdb, uids, cid, hasPrimaryKey));
        }
      }
    }
    if (uids) taosArrayDestroy(uids);

    if (pTsdb) {
      tsdbCacheInvalidateSchema(pTsdb, pEntry->uid, -1, pEntry->stbEntry.schemaRow.version);
    }
  }
  if (updStat) {
    int64_t ctbNum = 0;
    int32_t ret = metaGetStbStats(pMeta->pVnode, pEntry->uid, &ctbNum, 0, 0);
    if (ret < 0) {
      metaError("vgId:%d, failed to get stb stats:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pEntry->name,
                pEntry->uid, tstrerror(ret));
    }
    pMeta->pVnode->config.vndStats.numOfTimeSeries += (ctbNum * deltaCol);
    if (deltaCol > 0) metaTimeSeriesNotifyCheck(pMeta);
  }
  metaFetchEntryFree(&pOldEntry);
  return code;
}

static int32_t metaHandleChildTableUpdate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaEntry *pOldEntry = NULL;
  SMetaEntry *pSuperEntry = NULL;

  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pOldEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuperEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    return code;
  }

  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pOldEntry,
      .pSuperEntry = pSuperEntry,
  };

  metaWLock(pMeta);
  code = metaHandleChildTableUpdateImpl(pMeta, &param);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    metaFetchEntryFree(&pSuperEntry);
    return code;
  }

  metaFetchEntryFree(&pOldEntry);
  metaFetchEntryFree(&pSuperEntry);
  return code;
}

static int32_t metaHandleNormalTableUpdate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pOldEntry = NULL;

  // fetch old entry
  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pOldEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  // handle update
  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pOldEntry,
  };
  metaWLock(pMeta);
  code = metaHandleNormalTableUpdateImpl(pMeta, &param);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    return code;
  }

  // do other stuff
  if (!TSDB_CACHE_NO(pMeta->pVnode->config) &&
      pEntry->ntbEntry.schemaRow.version != pOldEntry->ntbEntry.schemaRow.version) {
#if 0
    {  // for add column
      int16_t cid = pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].colId;
      int8_t  col_type = pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].type;
      int32_t ret = tsdbCacheNewNTableColumn(pMeta->pVnode->pTsdb, entry.uid, cid, col_type);
      if (ret < 0) {
        terrno = ret;
        goto _err;
      }
    }
    {  // for drop column

      if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
        int16_t cid = pColumn->colId;

        if (tsdbCacheDropNTableColumn(pMeta->pVnode->pTsdb, entry.uid, cid, hasPrimayKey) != 0) {
          metaError("vgId:%d, failed to drop ntable column:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name,
                    entry.uid);
        }
        tsdbCacheInvalidateSchema(pMeta->pVnode->pTsdb, 0, entry.uid, pSchema->version);
      }
    }
    }
#endif
    if (pMeta->pVnode->pTsdb) {
      tsdbCacheInvalidateSchema(pMeta->pVnode->pTsdb, 0, pEntry->uid, pEntry->ntbEntry.schemaRow.version);
    }
  }
  int32_t deltaCol = pEntry->ntbEntry.schemaRow.nCols - pOldEntry->ntbEntry.schemaRow.nCols;
  pMeta->pVnode->config.vndStats.numOfNTimeSeries += deltaCol;
  if (deltaCol > 0) metaTimeSeriesNotifyCheck(pMeta);
  metaFetchEntryFree(&pOldEntry);
  return code;
}

static int32_t metaHandleVirtualNormalTableUpdate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pOldEntry = NULL;

  // fetch old entry
  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pOldEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  // handle update
  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pOldEntry,
  };
  metaWLock(pMeta);
  code = metaHandleVirtualNormalTableUpdateImpl(pMeta, &param);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    return code;
  }

  metaTimeSeriesNotifyCheck(pMeta);
  metaFetchEntryFree(&pOldEntry);
  return code;
}

static int32_t metaHandleVirtualChildTableUpdate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaEntry *pOldEntry = NULL;
  SMetaEntry *pSuperEntry = NULL;

  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pOldEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuperEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    return code;
  }

  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pOldEntry,
      .pSuperEntry = pSuperEntry,
  };

  metaWLock(pMeta);
  code = metaHandleVirtualChildTableUpdateImpl(pMeta, &param);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    metaFetchEntryFree(&pSuperEntry);
    return code;
  }

  metaFetchEntryFree(&pOldEntry);
  metaFetchEntryFree(&pSuperEntry);
  return code;
}

static int32_t metaHandleSuperTableDrop(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SArray     *childList = NULL;
  SMetaEntry *pOldEntry = NULL;

  code = metaFetchEntryByUid(pMeta, pEntry->uid, &pOldEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    return code;
  }

  code = metaGetChildUidsOfSuperTable(pMeta, pEntry->uid, &childList);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    metaFetchEntryFree(&pOldEntry);
    return code;
  }

  if (pMeta->pVnode->pTsdb && tsdbCacheDropSubTables(pMeta->pVnode->pTsdb, childList, pEntry->uid) < 0) {
    metaError("vgId:%d, failed to drop stb:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pEntry->name,
              pEntry->uid, tstrerror(terrno));
  }

  // loop to drop all child tables
  for (int32_t i = 0; i < taosArrayGetSize(childList); i++) {
    SMetaEntry childEntry = {
        .version = pEntry->version,
        .uid = *(tb_uid_t *)taosArrayGet(childList, i),
        .type = -TSDB_CHILD_TABLE,
    };

    code = metaHandleChildTableDrop(pMeta, &childEntry, true);
    if (code) {
      metaErr(TD_VID(pMeta->pVnode), code);
    }
  }

  // do drop super table
  SMetaHandleParam param = {
      .pEntry = pEntry,
      .pOldEntry = pOldEntry,
  };
  metaWLock(pMeta);
  code = metaHandleSuperTableDropImpl(pMeta, &param);
  metaULock(pMeta);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
    taosArrayDestroy(childList);
    metaFetchEntryFree(&pOldEntry);
    return code;
  }

  // do other stuff
  // metaUpdTimeSeriesNum(pMeta);

  // free resource and return
  taosArrayDestroy(childList);
  metaFetchEntryFree(&pOldEntry);
  return code;
}

int32_t metaHandleEntry2(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   vgId = TD_VID(pMeta->pVnode);
  SMetaInfo info = {0};
  int8_t    type = pEntry->type > 0 ? pEntry->type : -pEntry->type;

  if (NULL == pMeta || NULL == pEntry) {
    metaError("%s failed at %s:%d since invalid parameter", __func__, __FILE__, __LINE__);
    return TSDB_CODE_INVALID_PARA;
  }

  if (pEntry->type > 0) {
    bool isExist = false;
    if (TSDB_CODE_SUCCESS == metaGetInfo(pMeta, pEntry->uid, &info, NULL)) {
      isExist = true;
    }

    switch (type) {
      case TSDB_SUPER_TABLE: {
        if (isExist) {
          code = metaHandleSuperTableUpdate(pMeta, pEntry);
        } else {
          code = metaHandleSuperTableCreate(pMeta, pEntry);
        }
        break;
      }
      case TSDB_CHILD_TABLE: {
        if (isExist) {
          code = metaHandleChildTableUpdate(pMeta, pEntry);
        } else {
          code = metaHandleChildTableCreate(pMeta, pEntry);
        }
        break;
      }
      case TSDB_NORMAL_TABLE: {
        if (isExist) {
          code = metaHandleNormalTableUpdate(pMeta, pEntry);
        } else {
          code = metaHandleNormalTableCreate(pMeta, pEntry);
        }
        break;
      }
      case TSDB_VIRTUAL_NORMAL_TABLE: {
        if (isExist) {
          code = metaHandleVirtualNormalTableUpdate(pMeta, pEntry);
        } else {
          code = metaHandleVirtualNormalTableCreate(pMeta, pEntry);
        }
        break;
      }
      case TSDB_VIRTUAL_CHILD_TABLE: {
        if (isExist) {
          code = metaHandleVirtualChildTableUpdate(pMeta, pEntry);
        } else {
          code = metaHandleVirtualChildTableCreate(pMeta, pEntry);
        }
        break;
      }
      default: {
        code = TSDB_CODE_INVALID_PARA;
        break;
      }
    }
  } else {
    switch (type) {
      case TSDB_SUPER_TABLE: {
        code = metaHandleSuperTableDrop(pMeta, pEntry);
        break;
      }
      case TSDB_CHILD_TABLE: {
        code = metaHandleChildTableDrop(pMeta, pEntry, false);
        break;
      }
      case TSDB_NORMAL_TABLE: {
        code = metaHandleNormalTableDrop(pMeta, pEntry);
        break;
      }
      case TSDB_VIRTUAL_NORMAL_TABLE: {
        code = metaHandleVirtualNormalTableDrop(pMeta, pEntry);
        break;
      }
      case TSDB_VIRTUAL_CHILD_TABLE: {
        code = metaHandleVirtualChildTableDrop(pMeta, pEntry, false);
        break;
      }
      default: {
        code = TSDB_CODE_INVALID_PARA;
        break;
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pMeta->changed = true;
    metaDebug("vgId:%d, index:%" PRId64 ", handle meta entry success, type:%d tb:%s uid:%" PRId64, vgId,
              pEntry->version, pEntry->type, pEntry->type > 0 ? pEntry->name : "", pEntry->uid);
  } else {
    metaErr(vgId, code);
  }
  TAOS_RETURN(code);
}

void metaHandleSyncEntry(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return;
}
