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

static int  metaSaveJsonVarToIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);
static int  metaDelJsonVarFromIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);
static int  metaSaveToTbDb(SMeta *pMeta, const SMetaEntry *pME);
static int  metaUpdateUidIdx(SMeta *pMeta, const SMetaEntry *pME);
static int  metaUpdateNameIdx(SMeta *pMeta, const SMetaEntry *pME);
static int  metaUpdateTtlIdx(SMeta *pMeta, const SMetaEntry *pME);
static int  metaSaveToSkmDb(SMeta *pMeta, const SMetaEntry *pME);
static int  metaUpdateCtbIdx(SMeta *pMeta, const SMetaEntry *pME);
static int  metaUpdateSuidIdx(SMeta *pMeta, const SMetaEntry *pME);
static int  metaUpdateTagIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry);
static int  metaDropTableByUid(SMeta *pMeta, tb_uid_t uid, int *type);
static void metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey);
// opt ins_tables query
static int metaUpdateCtimeIdx(SMeta *pMeta, const SMetaEntry *pME);
static int metaDeleteCtimeIdx(SMeta *pMeta, const SMetaEntry *pME);
static int metaUpdateNcolIdx(SMeta *pMeta, const SMetaEntry *pME);
static int metaDeleteNcolIdx(SMeta *pMeta, const SMetaEntry *pME);

static void metaGetEntryInfo(const SMetaEntry *pEntry, SMetaInfo *pInfo) {
  pInfo->uid = pEntry->uid;
  pInfo->version = pEntry->version;
  if (pEntry->type == TSDB_SUPER_TABLE) {
    pInfo->suid = pEntry->uid;
    pInfo->skmVer = pEntry->stbEntry.schemaRow.version;
  } else if (pEntry->type == TSDB_CHILD_TABLE) {
    pInfo->suid = pEntry->ctbEntry.suid;
    pInfo->skmVer = 0;
  } else if (pEntry->type == TSDB_NORMAL_TABLE) {
    pInfo->suid = 0;
    pInfo->skmVer = pEntry->ntbEntry.schemaRow.version;
  } else {
    metaError("meta/table: invalide table type: %" PRId8 " get entry info failed.", pEntry->type);
  }
}

static int metaUpdateMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema, STableMetaRsp *pMetaRsp) {
  pMetaRsp->pSchemas = taosMemoryMalloc(pSchema->nCols * sizeof(SSchema));
  if (NULL == pMetaRsp->pSchemas) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tstrncpy(pMetaRsp->tbName, tbName, TSDB_TABLE_NAME_LEN);
  pMetaRsp->numOfColumns = pSchema->nCols;
  pMetaRsp->tableType = TSDB_NORMAL_TABLE;
  pMetaRsp->sversion = pSchema->version;
  pMetaRsp->tuid = uid;

  memcpy(pMetaRsp->pSchemas, pSchema->pSchema, pSchema->nCols * sizeof(SSchema));

  return 0;
}

static int metaSaveJsonVarToIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema) {
#ifdef USE_INVERTED_INDEX
  if (pMeta->pTagIvtIdx == NULL || pCtbEntry == NULL) {
    return -1;
  }
  void       *data = pCtbEntry->ctbEntry.pTags;
  const char *tagName = pSchema->name;

  tb_uid_t    suid = pCtbEntry->ctbEntry.suid;
  tb_uid_t    tuid = pCtbEntry->uid;
  const void *pTagData = pCtbEntry->ctbEntry.pTags;
  int32_t     nTagData = 0;

  SArray *pTagVals = NULL;
  if (tTagToValArray((const STag *)data, &pTagVals) != 0) {
    return -1;
  }

  SIndexMultiTerm *terms = indexMultiTermCreate();
  int16_t          nCols = taosArrayGetSize(pTagVals);
  for (int i = 0; i < nCols; i++) {
    STagVal *pTagVal = (STagVal *)taosArrayGet(pTagVals, i);
    char     type = pTagVal->type;

    char   *key = pTagVal->pKey;
    int32_t nKey = strlen(key);

    SIndexTerm *term = NULL;
    if (type == TSDB_DATA_TYPE_NULL) {
      term = indexTermCreate(suid, ADD_VALUE, TSDB_DATA_TYPE_VARCHAR, key, nKey, NULL, 0);
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      if (pTagVal->nData > 0) {
        char   *val = taosMemoryCalloc(1, pTagVal->nData + VARSTR_HEADER_SIZE);
        int32_t len = taosUcs4ToMbs((TdUcs4 *)pTagVal->pData, pTagVal->nData, val + VARSTR_HEADER_SIZE);
        memcpy(val, (uint16_t *)&len, VARSTR_HEADER_SIZE);
        type = TSDB_DATA_TYPE_VARCHAR;
        term = indexTermCreate(suid, ADD_VALUE, type, key, nKey, val, len);
        taosMemoryFree(val);
      } else if (pTagVal->nData == 0) {
        term = indexTermCreate(suid, ADD_VALUE, TSDB_DATA_TYPE_VARCHAR, key, nKey, pTagVal->pData, 0);
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double val = *(double *)(&pTagVal->i64);
      int    len = sizeof(val);
      term = indexTermCreate(suid, ADD_VALUE, type, key, nKey, (const char *)&val, len);
    } else if (type == TSDB_DATA_TYPE_BOOL) {
      int val = *(int *)(&pTagVal->i64);
      int len = sizeof(val);
      term = indexTermCreate(suid, ADD_VALUE, TSDB_DATA_TYPE_BOOL, key, nKey, (const char *)&val, len);
    }
    if (term != NULL) {
      indexMultiTermAdd(terms, term);
    }
  }
  indexJsonPut(pMeta->pTagIvtIdx, terms, tuid);
  indexMultiTermDestroy(terms);

  taosArrayDestroy(pTagVals);
#endif
  return 0;
}
int metaDelJsonVarFromIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema) {
#ifdef USE_INVERTED_INDEX
  if (pMeta->pTagIvtIdx == NULL || pCtbEntry == NULL) {
    return -1;
  }
  void       *data = pCtbEntry->ctbEntry.pTags;
  const char *tagName = pSchema->name;

  tb_uid_t    suid = pCtbEntry->ctbEntry.suid;
  tb_uid_t    tuid = pCtbEntry->uid;
  const void *pTagData = pCtbEntry->ctbEntry.pTags;
  int32_t     nTagData = 0;

  SArray *pTagVals = NULL;
  if (tTagToValArray((const STag *)data, &pTagVals) != 0) {
    return -1;
  }

  SIndexMultiTerm *terms = indexMultiTermCreate();
  int16_t          nCols = taosArrayGetSize(pTagVals);
  for (int i = 0; i < nCols; i++) {
    STagVal *pTagVal = (STagVal *)taosArrayGet(pTagVals, i);
    char     type = pTagVal->type;

    char   *key = pTagVal->pKey;
    int32_t nKey = strlen(key);

    SIndexTerm *term = NULL;
    if (type == TSDB_DATA_TYPE_NULL) {
      term = indexTermCreate(suid, DEL_VALUE, TSDB_DATA_TYPE_VARCHAR, key, nKey, NULL, 0);
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      if (pTagVal->nData > 0) {
        char   *val = taosMemoryCalloc(1, pTagVal->nData + VARSTR_HEADER_SIZE);
        int32_t len = taosUcs4ToMbs((TdUcs4 *)pTagVal->pData, pTagVal->nData, val + VARSTR_HEADER_SIZE);
        memcpy(val, (uint16_t *)&len, VARSTR_HEADER_SIZE);
        type = TSDB_DATA_TYPE_VARCHAR;
        term = indexTermCreate(suid, DEL_VALUE, type, key, nKey, val, len);
        taosMemoryFree(val);
      } else if (pTagVal->nData == 0) {
        term = indexTermCreate(suid, DEL_VALUE, TSDB_DATA_TYPE_VARCHAR, key, nKey, pTagVal->pData, 0);
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double val = *(double *)(&pTagVal->i64);
      int    len = sizeof(val);
      term = indexTermCreate(suid, DEL_VALUE, type, key, nKey, (const char *)&val, len);
    } else if (type == TSDB_DATA_TYPE_BOOL) {
      int val = *(int *)(&pTagVal->i64);
      int len = sizeof(val);
      term = indexTermCreate(suid, DEL_VALUE, TSDB_DATA_TYPE_BOOL, key, nKey, (const char *)&val, len);
    }
    if (term != NULL) {
      indexMultiTermAdd(terms, term);
    }
  }
  indexJsonPut(pMeta->pTagIvtIdx, terms, tuid);
  indexMultiTermDestroy(terms);
  taosArrayDestroy(pTagVals);
#endif
  return 0;
}

int metaCreateSTable(SMeta *pMeta, int64_t version, SVCreateStbReq *pReq) {
  SMetaEntry  me = {0};
  int         kLen = 0;
  int         vLen = 0;
  const void *pKey = NULL;
  const void *pVal = NULL;
  void       *pBuf = NULL;
  int32_t     szBuf = 0;
  void       *p = NULL;

  // validate req
  void *pData = NULL;
  int   nData = 0;
  if (tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &pData, &nData) == 0) {
    tb_uid_t uid = *(tb_uid_t *)pData;
    tdbFree(pData);
    SMetaInfo info;
    metaGetInfo(pMeta, uid, &info, NULL);
    if (info.uid == info.suid) {
      return 0;
    } else {
      terrno = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
      return -1;
    }
  }

  // set structs
  me.version = version;
  me.type = TSDB_SUPER_TABLE;
  me.uid = pReq->suid;
  me.name = pReq->name;
  me.stbEntry.schemaRow = pReq->schemaRow;
  me.stbEntry.schemaTag = pReq->schemaTag;
  if (pReq->rollup) {
    TABLE_SET_ROLLUP(me.flags);
    me.stbEntry.rsmaParam = pReq->rsmaParam;
  }

  if (metaHandleEntry(pMeta, &me) < 0) goto _err;

  ++pMeta->pVnode->config.vndStats.numOfSTables;

  metaDebug("vgId:%d, stb:%s is created, suid:%" PRId64, TD_VID(pMeta->pVnode), pReq->name, pReq->suid);

  return 0;

_err:
  metaError("vgId:%d, failed to create stb:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pReq->name, pReq->suid,
            tstrerror(terrno));
  return -1;
}

int metaDropSTable(SMeta *pMeta, int64_t verison, SVDropStbReq *pReq, SArray *tbUidList) {
  void *pKey = NULL;
  int   nKey = 0;
  void *pData = NULL;
  int   nData = 0;
  int   c = 0;
  int   rc = 0;

  // check if super table exists
  rc = tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &pData, &nData);
  if (rc < 0 || *(tb_uid_t *)pData != pReq->suid) {
    tdbFree(pData);
    terrno = TSDB_CODE_TDB_STB_NOT_EXIST;
    return -1;
  }

  // drop all child tables
  TBC *pCtbIdxc = NULL;

  tdbTbcOpen(pMeta->pCtbIdx, &pCtbIdxc, NULL);
  rc = tdbTbcMoveTo(pCtbIdxc, &(SCtbIdxKey){.suid = pReq->suid, .uid = INT64_MIN}, sizeof(SCtbIdxKey), &c);
  if (rc < 0) {
    tdbTbcClose(pCtbIdxc);
    metaWLock(pMeta);
    goto _drop_super_table;
  }

  for (;;) {
    rc = tdbTbcNext(pCtbIdxc, &pKey, &nKey, NULL, NULL);
    if (rc < 0) break;

    if (((SCtbIdxKey *)pKey)->suid < pReq->suid) {
      continue;
    } else if (((SCtbIdxKey *)pKey)->suid > pReq->suid) {
      break;
    }

    taosArrayPush(tbUidList, &(((SCtbIdxKey *)pKey)->uid));
  }

  tdbTbcClose(pCtbIdxc);

  metaWLock(pMeta);

  for (int32_t iChild = 0; iChild < taosArrayGetSize(tbUidList); iChild++) {
    tb_uid_t uid = *(tb_uid_t *)taosArrayGet(tbUidList, iChild);
    metaDropTableByUid(pMeta, uid, NULL);
  }

  // drop super table
_drop_super_table:
  tdbTbGet(pMeta->pUidIdx, &pReq->suid, sizeof(tb_uid_t), &pData, &nData);
  tdbTbDelete(pMeta->pTbDb, &(STbDbKey){.version = ((SUidIdxVal *)pData)[0].version, .uid = pReq->suid},
              sizeof(STbDbKey), pMeta->txn);
  tdbTbDelete(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, pMeta->txn);
  tdbTbDelete(pMeta->pUidIdx, &pReq->suid, sizeof(tb_uid_t), pMeta->txn);
  tdbTbDelete(pMeta->pSuidIdx, &pReq->suid, sizeof(tb_uid_t), pMeta->txn);

  metaULock(pMeta);

_exit:
  tdbFree(pKey);
  tdbFree(pData);
  metaDebug("vgId:%d, super table %s uid:%" PRId64 " is dropped", TD_VID(pMeta->pVnode), pReq->name, pReq->suid);
  return 0;
}

int metaAlterSTable(SMeta *pMeta, int64_t version, SVCreateStbReq *pReq) {
  SMetaEntry  oStbEntry = {0};
  SMetaEntry  nStbEntry = {0};
  TBC        *pUidIdxc = NULL;
  TBC        *pTbDbc = NULL;
  const void *pData;
  int         nData;
  int64_t     oversion;
  SDecoder    dc = {0};
  int32_t     ret;
  int32_t     c = -2;

  tdbTbcOpen(pMeta->pUidIdx, &pUidIdxc, NULL);
  ret = tdbTbcMoveTo(pUidIdxc, &pReq->suid, sizeof(tb_uid_t), &c);
  if (ret < 0 || c) {
    tdbTbcClose(pUidIdxc);

    terrno = TSDB_CODE_TDB_STB_NOT_EXIST;
    return -1;
  }

  ret = tdbTbcGet(pUidIdxc, NULL, NULL, &pData, &nData);
  if (ret < 0) {
    tdbTbcClose(pUidIdxc);

    terrno = TSDB_CODE_TDB_STB_NOT_EXIST;
    return -1;
  }

  oversion = ((SUidIdxVal *)pData)[0].version;

  tdbTbcOpen(pMeta->pTbDb, &pTbDbc, NULL);
  ret = tdbTbcMoveTo(pTbDbc, &((STbDbKey){.uid = pReq->suid, .version = oversion}), sizeof(STbDbKey), &c);
  if (!(ret == 0 && c == 0)) {
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);

    terrno = TSDB_CODE_TDB_STB_NOT_EXIST;
    metaError("meta/table: invalide ret: %" PRId32 " or c: %" PRId32 "alter stb failed.", ret, c);
    return -1;
  }

  ret = tdbTbcGet(pTbDbc, NULL, NULL, &pData, &nData);
  if (ret < 0) {
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);

    terrno = TSDB_CODE_TDB_STB_NOT_EXIST;
    return -1;
  }

  oStbEntry.pBuf = taosMemoryMalloc(nData);
  memcpy(oStbEntry.pBuf, pData, nData);
  tDecoderInit(&dc, oStbEntry.pBuf, nData);
  metaDecodeEntry(&dc, &oStbEntry);

  nStbEntry.version = version;
  nStbEntry.type = TSDB_SUPER_TABLE;
  nStbEntry.uid = pReq->suid;
  nStbEntry.name = pReq->name;
  nStbEntry.stbEntry.schemaRow = pReq->schemaRow;
  nStbEntry.stbEntry.schemaTag = pReq->schemaTag;

  metaWLock(pMeta);
  // compare two entry
  if (oStbEntry.stbEntry.schemaRow.version != pReq->schemaRow.version) {
    metaSaveToSkmDb(pMeta, &nStbEntry);
  }

  // update table.db
  metaSaveToTbDb(pMeta, &nStbEntry);

  // update uid index
  metaUpdateUidIdx(pMeta, &nStbEntry);

  // metaStatsCacheDrop(pMeta, nStbEntry.uid);

  metaULock(pMeta);

  if (oStbEntry.pBuf) taosMemoryFree(oStbEntry.pBuf);
  tDecoderClear(&dc);
  tdbTbcClose(pTbDbc);
  tdbTbcClose(pUidIdxc);
  return 0;
}
int metaAddIndexToSTable(SMeta *pMeta, int64_t version, SVCreateStbReq *pReq) {
  SMetaEntry oStbEntry = {0};
  SMetaEntry nStbEntry = {0};

  STbDbKey tbDbKey = {0};

  TBC     *pUidIdxc = NULL;
  TBC     *pTbDbc = NULL;
  void    *pData = NULL;
  int      nData = 0;
  int64_t  oversion;
  SDecoder dc = {0};
  int32_t  ret;
  int32_t  c = -2;
  tb_uid_t suid = pReq->suid;

  // get super table
  if (tdbTbGet(pMeta->pUidIdx, &suid, sizeof(tb_uid_t), &pData, &nData) != 0) {
    ret = -1;
    goto _err;
  }

  tbDbKey.uid = suid;
  tbDbKey.version = ((SUidIdxVal *)pData)[0].version;
  tdbTbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &pData, &nData);

  tDecoderInit(&dc, pData, nData);
  ret = metaDecodeEntry(&dc, &oStbEntry);
  if (ret < 0) {
    goto _err;
  }

  if (oStbEntry.stbEntry.schemaTag.pSchema == NULL || oStbEntry.stbEntry.schemaTag.pSchema == NULL) {
    goto _err;
  }

  if (oStbEntry.stbEntry.schemaTag.version == pReq->schemaTag.version) {
    goto _err;
  }

  if (oStbEntry.stbEntry.schemaTag.nCols != pReq->schemaTag.nCols) {
    goto _err;
  }

  int diffIdx = -1;
  for (int i = 0; i < pReq->schemaTag.nCols; i++) {
    SSchema *pNew = pReq->schemaTag.pSchema + i;
    SSchema *pOld = oStbEntry.stbEntry.schemaTag.pSchema + i;
    if (pNew->type != pOld->type || pNew->colId != pOld->colId || pNew->bytes != pOld->bytes ||
        strncmp(pOld->name, pNew->name, sizeof(pNew->name))) {
      goto _err;
    }
    if (IS_IDX_ON(pNew) && !IS_IDX_ON(pOld)) {
      if (diffIdx != -1) goto _err;
      diffIdx = i;
    }
  }

  if (diffIdx == -1 && diffIdx == 0) {
    goto _err;
  }

  // Get target schema info
  SSchemaWrapper *pTagSchema = &pReq->schemaTag;
  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    terrno = TSDB_CODE_VND_COL_ALREADY_EXISTS;
    goto _err;
  }
  SSchema *pCol = pTagSchema->pSchema + diffIdx;

  /*
   * iterator all pTdDbc by uid and version
   */
  TBC *pCtbIdxc = NULL;
  tdbTbcOpen(pMeta->pCtbIdx, &pCtbIdxc, NULL);
  int rc = tdbTbcMoveTo(pCtbIdxc, &(SCtbIdxKey){.suid = suid, .uid = INT64_MIN}, sizeof(SCtbIdxKey), &c);
  if (rc < 0) {
    tdbTbcClose(pCtbIdxc);
    goto _err;
  }
  for (;;) {
    void *pKey = NULL, *pVal = NULL;
    int   nKey = 0, nVal = 0;
    rc = tdbTbcNext(pCtbIdxc, &pKey, &nKey, &pVal, &nVal);
    if (rc < 0) {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCtbIdxc);
      pCtbIdxc = NULL;
      break;
    }
    if (((SCtbIdxKey *)pKey)->suid != suid) {
      tdbFree(pKey);
      tdbFree(pVal);
      continue;
    }
    STagIdxKey *pTagIdxKey = NULL;
    int32_t     nTagIdxKey;

    const void *pTagData = NULL;
    int32_t     nTagData = 0;

    SCtbIdxKey *table = (SCtbIdxKey *)pKey;
    STagVal     tagVal = {.cid = pCol->colId};
    tTagGet((const STag *)pVal, &tagVal);
    if (IS_VAR_DATA_TYPE(pCol->type)) {
      pTagData = tagVal.pData;
      nTagData = (int32_t)tagVal.nData;
    } else {
      pTagData = &(tagVal.i64);
      nTagData = tDataTypes[pCol->type].bytes;
    }
    rc = metaCreateTagIdxKey(suid, pCol->colId, pTagData, nTagData, pCol->type, table->uid, &pTagIdxKey, &nTagIdxKey);
    tdbFree(pKey);
    tdbFree(pVal);
    if (rc < 0) {
      metaDestroyTagIdxKey(pTagIdxKey);
      tdbTbcClose(pCtbIdxc);
      goto _err;
    }

    metaWLock(pMeta);
    tdbTbUpsert(pMeta->pTagIdx, pTagIdxKey, nTagIdxKey, NULL, 0, pMeta->txn);
    metaULock(pMeta);
    metaDestroyTagIdxKey(pTagIdxKey);
  }

  nStbEntry.version = version;
  nStbEntry.type = TSDB_SUPER_TABLE;
  nStbEntry.uid = pReq->suid;
  nStbEntry.name = pReq->name;
  nStbEntry.stbEntry.schemaRow = pReq->schemaRow;
  nStbEntry.stbEntry.schemaTag = pReq->schemaTag;

  metaWLock(pMeta);
  // update table.db
  metaSaveToTbDb(pMeta, &nStbEntry);
  // update uid index
  metaUpdateUidIdx(pMeta, &nStbEntry);
  metaULock(pMeta);

  if (oStbEntry.pBuf) taosMemoryFree(oStbEntry.pBuf);
  tDecoderClear(&dc);
  tdbFree(pData);

  tdbTbcClose(pCtbIdxc);
  return TSDB_CODE_SUCCESS;
_err:
  if (oStbEntry.pBuf) taosMemoryFree(oStbEntry.pBuf);
  tDecoderClear(&dc);
  tdbFree(pData);

  return TSDB_CODE_VND_COL_ALREADY_EXISTS;
}
int metaDropIndexFromSTable(SMeta *pMeta, int64_t version, SDropIndexReq *pReq) {
  SMetaEntry oStbEntry = {0};
  SMetaEntry nStbEntry = {0};

  STbDbKey tbDbKey = {0};
  TBC     *pUidIdxc = NULL;
  TBC     *pTbDbc = NULL;
  int      ret = 0;
  int      c = -2;
  void    *pData = NULL;
  int      nData = 0;
  int64_t  oversion;
  SDecoder dc = {0};

  tb_uid_t suid = pReq->stbUid;

  if (tdbTbGet(pMeta->pUidIdx, &suid, sizeof(tb_uid_t), &pData, &nData) != 0) {
    ret = -1;
    goto _err;
  }

  tbDbKey.uid = suid;
  tbDbKey.version = ((SUidIdxVal *)pData)[0].version;
  tdbTbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &pData, &nData);
  tDecoderInit(&dc, pData, nData);
  ret = metaDecodeEntry(&dc, &oStbEntry);
  if (ret < 0) {
    goto _err;
  }

  SSchema *pCol = NULL;
  int32_t  colId = -1;
  for (int i = 0; i < oStbEntry.stbEntry.schemaTag.nCols; i++) {
    SSchema *schema = oStbEntry.stbEntry.schemaTag.pSchema + i;
    if (0 == strncmp(schema->name, pReq->colName, sizeof(pReq->colName))) {
      if (i != 0 || IS_IDX_ON(schema)) {
        pCol = schema;
      }
      break;
    }
  }

  if (pCol == NULL) {
    goto _err;
  }

  /*
   * iterator all pTdDbc by uid and version
   */
  TBC *pCtbIdxc = NULL;
  tdbTbcOpen(pMeta->pCtbIdx, &pCtbIdxc, NULL);
  int rc = tdbTbcMoveTo(pCtbIdxc, &(SCtbIdxKey){.suid = suid, .uid = INT64_MIN}, sizeof(SCtbIdxKey), &c);
  if (rc < 0) {
    tdbTbcClose(pCtbIdxc);
    goto _err;
  }
  for (;;) {
    void *pKey = NULL, *pVal = NULL;
    int   nKey = 0, nVal = 0;
    rc = tdbTbcNext(pCtbIdxc, &pKey, &nKey, &pVal, &nVal);
    if (rc < 0) {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCtbIdxc);
      pCtbIdxc = NULL;
      break;
    }
    if (((SCtbIdxKey *)pKey)->suid != suid) {
      tdbFree(pKey);
      tdbFree(pVal);
      continue;
    }
    STagIdxKey *pTagIdxKey = NULL;
    int32_t     nTagIdxKey;

    const void *pTagData = NULL;
    int32_t     nTagData = 0;

    SCtbIdxKey *table = (SCtbIdxKey *)pKey;
    STagVal     tagVal = {.cid = pCol->colId};
    tTagGet((const STag *)pVal, &tagVal);
    if (IS_VAR_DATA_TYPE(pCol->type)) {
      pTagData = tagVal.pData;
      nTagData = (int32_t)tagVal.nData;
    } else {
      pTagData = &(tagVal.i64);
      nTagData = tDataTypes[pCol->type].bytes;
    }
    rc = metaCreateTagIdxKey(suid, pCol->colId, pTagData, nTagData, pCol->type, table->uid, &pTagIdxKey, &nTagIdxKey);
    tdbFree(pKey);
    tdbFree(pVal);
    if (rc < 0) {
      metaDestroyTagIdxKey(pTagIdxKey);
      tdbTbcClose(pCtbIdxc);
      goto _err;
    }

    metaWLock(pMeta);
    tdbTbDelete(pMeta->pTagIdx, pTagIdxKey, nTagIdxKey, pMeta->txn);
    metaULock(pMeta);
    metaDestroyTagIdxKey(pTagIdxKey);
  }

  // clear idx flag
  SSCHMEA_SET_IDX_OFF(pCol);

  nStbEntry.version = version;
  nStbEntry.type = TSDB_SUPER_TABLE;
  nStbEntry.uid = oStbEntry.uid;
  nStbEntry.name = oStbEntry.name;

  SSchemaWrapper *row = tCloneSSchemaWrapper(&oStbEntry.stbEntry.schemaRow);
  SSchemaWrapper *tag = tCloneSSchemaWrapper(&oStbEntry.stbEntry.schemaTag);

  nStbEntry.stbEntry.schemaRow = *row;
  nStbEntry.stbEntry.schemaTag = *tag;
  nStbEntry.stbEntry.rsmaParam = oStbEntry.stbEntry.rsmaParam;

  metaWLock(pMeta);
  // update table.db
  metaSaveToTbDb(pMeta, &nStbEntry);
  // update uid index
  metaUpdateUidIdx(pMeta, &nStbEntry);
  metaULock(pMeta);

  tDeleteSchemaWrapper(tag);
  tDeleteSchemaWrapper(row);

  if (oStbEntry.pBuf) taosMemoryFree(oStbEntry.pBuf);
  tDecoderClear(&dc);
  tdbFree(pData);

  tdbTbcClose(pCtbIdxc);
  return TSDB_CODE_SUCCESS;
_err:
  if (oStbEntry.pBuf) taosMemoryFree(oStbEntry.pBuf);
  tDecoderClear(&dc);
  tdbFree(pData);

  return -1;
}

int metaCreateTable(SMeta *pMeta, int64_t ver, SVCreateTbReq *pReq, STableMetaRsp **pMetaRsp) {
  SMetaEntry  me = {0};
  SMetaReader mr = {0};

  // validate message
  if (pReq->type != TSDB_CHILD_TABLE && pReq->type != TSDB_NORMAL_TABLE) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _err;
  }

  if (pReq->type == TSDB_CHILD_TABLE) {
    tb_uid_t suid = metaGetTableEntryUidByName(pMeta, pReq->ctb.stbName);
    if (suid != pReq->ctb.suid) {
      terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
      return -1;
    }
  }

  // validate req
  metaReaderInit(&mr, pMeta, 0);
  if (metaGetTableEntryByName(&mr, pReq->name) == 0) {
    if (pReq->type == TSDB_CHILD_TABLE && pReq->ctb.suid != mr.me.ctbEntry.suid) {
      terrno = TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE;
      metaReaderClear(&mr);
      return -1;
    }
    pReq->uid = mr.me.uid;
    if (pReq->type == TSDB_CHILD_TABLE) {
      pReq->ctb.suid = mr.me.ctbEntry.suid;
    }
    terrno = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
    metaReaderClear(&mr);
    return -1;
  } else if (terrno == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
    terrno = TSDB_CODE_SUCCESS;
  }
  metaReaderClear(&mr);

  // build SMetaEntry
  me.version = ver;
  me.type = pReq->type;
  me.uid = pReq->uid;
  me.name = pReq->name;
  if (me.type == TSDB_CHILD_TABLE) {
    me.ctbEntry.ctime = pReq->ctime;
    me.ctbEntry.ttlDays = pReq->ttl;
    me.ctbEntry.commentLen = pReq->commentLen;
    me.ctbEntry.comment = pReq->comment;
    me.ctbEntry.suid = pReq->ctb.suid;
    me.ctbEntry.pTags = pReq->ctb.pTag;

#ifdef TAG_FILTER_DEBUG
    SArray *pTagVals = NULL;
    int32_t code = tTagToValArray((STag *)pReq->ctb.pTag, &pTagVals);
    for (int i = 0; i < taosArrayGetSize(pTagVals); i++) {
      STagVal *pTagVal = (STagVal *)taosArrayGet(pTagVals, i);

      if (IS_VAR_DATA_TYPE(pTagVal->type)) {
        char *buf = taosMemoryCalloc(pTagVal->nData + 1, 1);
        memcpy(buf, pTagVal->pData, pTagVal->nData);
        metaDebug("metaTag table:%s varchar index:%d cid:%d type:%d value:%s", pReq->name, i, pTagVal->cid,
                  pTagVal->type, buf);
        taosMemoryFree(buf);
      } else {
        double val = 0;
        GET_TYPED_DATA(val, double, pTagVal->type, &pTagVal->i64);
        metaDebug("metaTag table:%s number index:%d cid:%d type:%d value:%f", pReq->name, i, pTagVal->cid,
                  pTagVal->type, val);
      }
    }
#endif

    ++pMeta->pVnode->config.vndStats.numOfCTables;

    metaWLock(pMeta);
    metaUpdateStbStats(pMeta, me.ctbEntry.suid, 1);
    metaUidCacheClear(pMeta, me.ctbEntry.suid);
    metaTbGroupCacheClear(pMeta, me.ctbEntry.suid);
    metaULock(pMeta);
  } else {
    me.ntbEntry.ctime = pReq->ctime;
    me.ntbEntry.ttlDays = pReq->ttl;
    me.ntbEntry.commentLen = pReq->commentLen;
    me.ntbEntry.comment = pReq->comment;
    me.ntbEntry.schemaRow = pReq->ntb.schemaRow;
    me.ntbEntry.ncid = me.ntbEntry.schemaRow.pSchema[me.ntbEntry.schemaRow.nCols - 1].colId + 1;

    ++pMeta->pVnode->config.vndStats.numOfNTables;
    pMeta->pVnode->config.vndStats.numOfNTimeSeries += me.ntbEntry.schemaRow.nCols - 1;
  }

  if (metaHandleEntry(pMeta, &me) < 0) goto _err;

  if (pMetaRsp) {
    *pMetaRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));

    if (*pMetaRsp) {
      if (me.type == TSDB_CHILD_TABLE) {
        (*pMetaRsp)->tableType = TSDB_CHILD_TABLE;
        (*pMetaRsp)->tuid = pReq->uid;
        (*pMetaRsp)->suid = pReq->ctb.suid;
        strcpy((*pMetaRsp)->tbName, pReq->name);
      } else {
        metaUpdateMetaRsp(pReq->uid, pReq->name, &pReq->ntb.schemaRow, *pMetaRsp);
      }
    }
  }

  metaDebug("vgId:%d, table:%s uid %" PRId64 " is created, type:%" PRId8, TD_VID(pMeta->pVnode), pReq->name, pReq->uid,
            pReq->type);
  return 0;

_err:
  metaError("vgId:%d, failed to create table:%s type:%s since %s", TD_VID(pMeta->pVnode), pReq->name,
            pReq->type == TSDB_CHILD_TABLE ? "child table" : "normal table", tstrerror(terrno));
  return -1;
}

int metaDropTable(SMeta *pMeta, int64_t version, SVDropTbReq *pReq, SArray *tbUids, tb_uid_t *tbUid) {
  void    *pData = NULL;
  int      nData = 0;
  int      rc = 0;
  tb_uid_t uid;
  int      type;

  rc = tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &pData, &nData);
  if (rc < 0) {
    terrno = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    return -1;
  }
  uid = *(tb_uid_t *)pData;

  metaWLock(pMeta);
  metaDropTableByUid(pMeta, uid, &type);
  metaULock(pMeta);

  if ((type == TSDB_CHILD_TABLE || type == TSDB_NORMAL_TABLE) && tbUids) {
    taosArrayPush(tbUids, &uid);
  }

  if ((type == TSDB_CHILD_TABLE) && tbUid) {
    *tbUid = uid;
  }

  tdbFree(pData);
  return 0;
}

int metaTtlDropTable(SMeta *pMeta, int64_t ttl, SArray *tbUids) {
  int ret = metaTtlSmaller(pMeta, ttl, tbUids);
  if (ret != 0) {
    return ret;
  }
  if (taosArrayGetSize(tbUids) == 0) {
    return 0;
  }

  metaWLock(pMeta);
  for (int i = 0; i < taosArrayGetSize(tbUids); ++i) {
    tb_uid_t *uid = (tb_uid_t *)taosArrayGet(tbUids, i);
    metaDropTableByUid(pMeta, *uid, NULL);
    metaDebug("ttl drop table:%" PRId64, *uid);
  }
  metaULock(pMeta);
  return 0;
}

static void metaBuildTtlIdxKey(STtlIdxKey *ttlKey, const SMetaEntry *pME) {
  int64_t ttlDays = 0;
  int64_t ctime = 0;
  if (pME->type == TSDB_CHILD_TABLE) {
    ctime = pME->ctbEntry.ctime;
    ttlDays = pME->ctbEntry.ttlDays;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    ctime = pME->ntbEntry.ctime;
    ttlDays = pME->ntbEntry.ttlDays;
  } else {
    metaError("meta/table: invalide table type: %" PRId8 " build ttl idx key failed.", pME->type);
    return;
  }

  if (ttlDays <= 0) return;

  ttlKey->dtime = ctime / 1000 + ttlDays * tsTtlUnit;
  ttlKey->uid = pME->uid;
}
static int metaBuildCtimeIdxKey(SCtimeIdxKey *ctimeKey, const SMetaEntry *pME) {
  int64_t ctime;
  if (pME->type == TSDB_CHILD_TABLE) {
    ctime = pME->ctbEntry.ctime;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    ctime = pME->ntbEntry.ctime;
  } else {
    return -1;
  }

  ctimeKey->ctime = ctime;
  ctimeKey->uid = pME->uid;
  return 0;
}

static int metaBuildNColIdxKey(SNcolIdxKey *ncolKey, const SMetaEntry *pME) {
  if (pME->type == TSDB_NORMAL_TABLE) {
    ncolKey->ncol = pME->ntbEntry.schemaRow.nCols;
    ncolKey->uid = pME->uid;
  } else {
    return -1;
  }
  return 0;
}

static int metaDeleteTtlIdx(SMeta *pMeta, const SMetaEntry *pME) {
  STtlIdxKey ttlKey = {0};
  metaBuildTtlIdxKey(&ttlKey, pME);
  if (ttlKey.dtime == 0) return 0;
  return tdbTbDelete(pMeta->pTtlIdx, &ttlKey, sizeof(ttlKey), pMeta->txn);
}

static int metaDropTableByUid(SMeta *pMeta, tb_uid_t uid, int *type) {
  void      *pData = NULL;
  int        nData = 0;
  int        rc = 0;
  SMetaEntry e = {0};
  SDecoder   dc = {0};

  rc = tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pData, &nData);
  if (rc < 0) {
    return -1;
  }
  int64_t version = ((SUidIdxVal *)pData)[0].version;

  tdbTbGet(pMeta->pTbDb, &(STbDbKey){.version = version, .uid = uid}, sizeof(STbDbKey), &pData, &nData);

  tDecoderInit(&dc, pData, nData);
  rc = metaDecodeEntry(&dc, &e);
  if (rc < 0) {
    tDecoderClear(&dc);
    return -1;
  }

  if (type) *type = e.type;

  if (e.type == TSDB_CHILD_TABLE) {
    void *tData = NULL;
    int   tLen = 0;

    if (tdbTbGet(pMeta->pUidIdx, &e.ctbEntry.suid, sizeof(tb_uid_t), &tData, &tLen) == 0) {
      STbDbKey tbDbKey = {.uid = e.ctbEntry.suid, .version = ((SUidIdxVal *)tData)[0].version};
      if (tdbTbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &tData, &tLen) == 0) {
        SDecoder   tdc = {0};
        SMetaEntry stbEntry = {0};

        tDecoderInit(&tdc, tData, tLen);
        metaDecodeEntry(&tdc, &stbEntry);

        SSchema        *pTagColumn = NULL;
        SSchemaWrapper *pTagSchema = &stbEntry.stbEntry.schemaTag;
        if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
          pTagColumn = &stbEntry.stbEntry.schemaTag.pSchema[0];
          metaDelJsonVarFromIdx(pMeta, &e, pTagColumn);
        } else {
          for (int i = 0; i < pTagSchema->nCols; i++) {
            pTagColumn = &stbEntry.stbEntry.schemaTag.pSchema[i];
            if (!IS_IDX_ON(pTagColumn)) continue;
            STagIdxKey *pTagIdxKey = NULL;
            int32_t     nTagIdxKey;

            const void *pTagData = NULL;
            int32_t     nTagData = 0;

            STagVal tagVal = {.cid = pTagColumn->colId};
            tTagGet((const STag *)e.ctbEntry.pTags, &tagVal);
            if (IS_VAR_DATA_TYPE(pTagColumn->type)) {
              pTagData = tagVal.pData;
              nTagData = (int32_t)tagVal.nData;
            } else {
              pTagData = &(tagVal.i64);
              nTagData = tDataTypes[pTagColumn->type].bytes;
            }

            if (metaCreateTagIdxKey(e.ctbEntry.suid, pTagColumn->colId, pTagData, nTagData, pTagColumn->type, uid,
                                    &pTagIdxKey, &nTagIdxKey) == 0) {
              tdbTbDelete(pMeta->pTagIdx, pTagIdxKey, nTagIdxKey, pMeta->txn);
            }
            metaDestroyTagIdxKey(pTagIdxKey);
          }
        }
        tDecoderClear(&tdc);
      }
      tdbFree(tData);
    }
  }

  tdbTbDelete(pMeta->pTbDb, &(STbDbKey){.version = version, .uid = uid}, sizeof(STbDbKey), pMeta->txn);
  tdbTbDelete(pMeta->pNameIdx, e.name, strlen(e.name) + 1, pMeta->txn);
  tdbTbDelete(pMeta->pUidIdx, &uid, sizeof(uid), pMeta->txn);

  if (e.type == TSDB_CHILD_TABLE || e.type == TSDB_NORMAL_TABLE) metaDeleteCtimeIdx(pMeta, &e);
  if (e.type == TSDB_NORMAL_TABLE) metaDeleteNcolIdx(pMeta, &e);

  if (e.type != TSDB_SUPER_TABLE) metaDeleteTtlIdx(pMeta, &e);

  if (e.type == TSDB_CHILD_TABLE) {
    tdbTbDelete(pMeta->pCtbIdx, &(SCtbIdxKey){.suid = e.ctbEntry.suid, .uid = uid}, sizeof(SCtbIdxKey), pMeta->txn);

    --pMeta->pVnode->config.vndStats.numOfCTables;

    metaUpdateStbStats(pMeta, e.ctbEntry.suid, -1);
    metaUidCacheClear(pMeta, e.ctbEntry.suid);
    metaTbGroupCacheClear(pMeta, e.ctbEntry.suid);    
  } else if (e.type == TSDB_NORMAL_TABLE) {
    // drop schema.db (todo)

    --pMeta->pVnode->config.vndStats.numOfNTables;
    pMeta->pVnode->config.vndStats.numOfNTimeSeries -= e.ntbEntry.schemaRow.nCols - 1;
  } else if (e.type == TSDB_SUPER_TABLE) {
    tdbTbDelete(pMeta->pSuidIdx, &e.uid, sizeof(tb_uid_t), pMeta->txn);
    // drop schema.db (todo)

    metaStatsCacheDrop(pMeta, uid);
    metaUidCacheClear(pMeta, uid);
    metaTbGroupCacheClear(pMeta, uid);        
    --pMeta->pVnode->config.vndStats.numOfSTables;
  }

  metaCacheDrop(pMeta, uid);

  tDecoderClear(&dc);
  tdbFree(pData);

  return 0;
}
// opt ins_tables
int metaUpdateCtimeIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SCtimeIdxKey ctimeKey = {0};
  if (metaBuildCtimeIdxKey(&ctimeKey, pME) < 0) {
    return 0;
  }
  metaTrace("vgId:%d, start to save version:%" PRId64 " uid:%" PRId64 " ctime:%" PRId64, TD_VID(pMeta->pVnode),
            pME->version, pME->uid, ctimeKey.ctime);

  return tdbTbUpsert(pMeta->pCtimeIdx, &ctimeKey, sizeof(ctimeKey), NULL, 0, pMeta->txn);
}

int metaDeleteCtimeIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SCtimeIdxKey ctimeKey = {0};
  if (metaBuildCtimeIdxKey(&ctimeKey, pME) < 0) {
    return 0;
  }
  return tdbTbDelete(pMeta->pCtimeIdx, &ctimeKey, sizeof(ctimeKey), pMeta->txn);
}
int metaUpdateNcolIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SNcolIdxKey ncolKey = {0};
  if (metaBuildNColIdxKey(&ncolKey, pME) < 0) {
    return 0;
  }
  return tdbTbUpsert(pMeta->pNcolIdx, &ncolKey, sizeof(ncolKey), NULL, 0, pMeta->txn);
}

int metaDeleteNcolIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SNcolIdxKey ncolKey = {0};
  if (metaBuildNColIdxKey(&ncolKey, pME) < 0) {
    return 0;
  }
  return tdbTbDelete(pMeta->pNcolIdx, &ncolKey, sizeof(ncolKey), pMeta->txn);
}

static int metaAlterTableColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pAlterTbReq, STableMetaRsp *pMetaRsp) {
  void           *pVal = NULL;
  int             nVal = 0;
  const void     *pData = NULL;
  int             nData = 0;
  int             ret = 0;
  tb_uid_t        uid;
  int64_t         oversion;
  SSchema        *pColumn = NULL;
  SMetaEntry      entry = {0};
  SSchemaWrapper *pSchema;
  int             c;

  if (pAlterTbReq->colName == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  // search name index
  ret = tdbTbGet(pMeta->pNameIdx, pAlterTbReq->tbName, strlen(pAlterTbReq->tbName) + 1, &pVal, &nVal);
  if (ret < 0) {
    terrno = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    return -1;
  }

  uid = *(tb_uid_t *)pVal;
  tdbFree(pVal);
  pVal = NULL;

  // search uid index
  TBC *pUidIdxc = NULL;

  tdbTbcOpen(pMeta->pUidIdx, &pUidIdxc, NULL);
  tdbTbcMoveTo(pUidIdxc, &uid, sizeof(uid), &c);
  if (c != 0) {
    tdbTbcClose(pUidIdxc);
    metaError("meta/table: invalide c: %" PRId32 " alt tb column failed.", c);
    return -1;
  }

  tdbTbcGet(pUidIdxc, NULL, NULL, &pData, &nData);
  oversion = ((SUidIdxVal *)pData)[0].version;

  // search table.db
  TBC *pTbDbc = NULL;

  tdbTbcOpen(pMeta->pTbDb, &pTbDbc, NULL);
  tdbTbcMoveTo(pTbDbc, &((STbDbKey){.uid = uid, .version = oversion}), sizeof(STbDbKey), &c);
  if (c != 0) {
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);
    metaError("meta/table: invalide c: %" PRId32 " alt tb column failed.", c);
    return -1;
  }

  tdbTbcGet(pTbDbc, NULL, NULL, &pData, &nData);

  // get table entry
  SDecoder dc = {0};
  entry.pBuf = taosMemoryMalloc(nData);
  memcpy(entry.pBuf, pData, nData);
  tDecoderInit(&dc, entry.pBuf, nData);
  ret = metaDecodeEntry(&dc, &entry);
  if (ret != 0) {
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);
    tDecoderClear(&dc);
    metaError("meta/table: invalide ret: %" PRId32 " alt tb column failed.", ret);
    return -1;
  }

  if (entry.type != TSDB_NORMAL_TABLE) {
    terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
    goto _err;
  }
  // search the column to add/drop/update
  pSchema = &entry.ntbEntry.schemaRow;

  // save old entry
  SMetaEntry oldEntry = {.type = TSDB_NORMAL_TABLE, .uid = entry.uid};
  oldEntry.ntbEntry.schemaRow.nCols = pSchema->nCols;

  int32_t iCol = 0;
  for (;;) {
    pColumn = NULL;

    if (iCol >= pSchema->nCols) break;
    pColumn = &pSchema->pSchema[iCol];

    if (NULL == pAlterTbReq->colName) {
      metaError("meta/table: null pAlterTbReq->colName");
      return -1;
    }

    if (strcmp(pColumn->name, pAlterTbReq->colName) == 0) break;
    iCol++;
  }

  entry.version = version;
  int      tlen;
  SSchema *pNewSchema = NULL;
  switch (pAlterTbReq->action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      if (pColumn) {
        terrno = TSDB_CODE_VND_COL_ALREADY_EXISTS;
        goto _err;
      }
      pSchema->version++;
      pSchema->nCols++;
      pNewSchema = taosMemoryMalloc(sizeof(SSchema) * pSchema->nCols);
      memcpy(pNewSchema, pSchema->pSchema, sizeof(SSchema) * (pSchema->nCols - 1));
      pSchema->pSchema = pNewSchema;
      pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].bytes = pAlterTbReq->bytes;
      pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].type = pAlterTbReq->type;
      pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].flags = pAlterTbReq->flags;
      pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].colId = entry.ntbEntry.ncid++;
      strcpy(pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].name, pAlterTbReq->colName);

      ++pMeta->pVnode->config.vndStats.numOfNTimeSeries;
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      if (pColumn == NULL) {
        terrno = TSDB_CODE_VND_COL_NOT_EXISTS;
        goto _err;
      }
      if (pColumn->colId == 0) {
        terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
        goto _err;
      }
      if (tqCheckColModifiable(pMeta->pVnode->pTq, uid, pColumn->colId) != 0) {
        terrno = TSDB_CODE_VND_COL_SUBSCRIBED;
        goto _err;
      }
      pSchema->version++;
      tlen = (pSchema->nCols - iCol - 1) * sizeof(SSchema);
      if (tlen) {
        memmove(pColumn, pColumn + 1, tlen);
      }
      pSchema->nCols--;

      --pMeta->pVnode->config.vndStats.numOfNTimeSeries;
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      if (pColumn == NULL) {
        terrno = TSDB_CODE_VND_COL_NOT_EXISTS;
        goto _err;
      }
      if (!IS_VAR_DATA_TYPE(pColumn->type) || pColumn->bytes > pAlterTbReq->colModBytes) {
        terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
        goto _err;
      }
      if (tqCheckColModifiable(pMeta->pVnode->pTq, uid, pColumn->colId) != 0) {
        terrno = TSDB_CODE_VND_COL_SUBSCRIBED;
        goto _err;
      }
      pSchema->version++;
      pColumn->bytes = pAlterTbReq->colModBytes;
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      if (pAlterTbReq->colNewName == NULL) {
        terrno = TSDB_CODE_INVALID_MSG;
        goto _err;
      }
      if (pColumn == NULL) {
        terrno = TSDB_CODE_VND_COL_NOT_EXISTS;
        goto _err;
      }
      if (tqCheckColModifiable(pMeta->pVnode->pTq, uid, pColumn->colId) != 0) {
        terrno = TSDB_CODE_VND_COL_SUBSCRIBED;
        goto _err;
      }
      pSchema->version++;
      strcpy(pColumn->name, pAlterTbReq->colNewName);
      break;
  }

  entry.version = version;

  // do actual write
  metaWLock(pMeta);

  metaDeleteNcolIdx(pMeta, &oldEntry);
  metaUpdateNcolIdx(pMeta, &entry);
  // save to table db
  metaSaveToTbDb(pMeta, &entry);

  metaUpdateUidIdx(pMeta, &entry);

  metaSaveToSkmDb(pMeta, &entry);

  metaULock(pMeta);

  metaUpdateMetaRsp(uid, pAlterTbReq->tbName, pSchema, pMetaRsp);

  if (entry.pBuf) taosMemoryFree(entry.pBuf);
  if (pNewSchema) taosMemoryFree(pNewSchema);
  tdbTbcClose(pTbDbc);
  tdbTbcClose(pUidIdxc);
  tDecoderClear(&dc);

  return 0;

_err:
  if (entry.pBuf) taosMemoryFree(entry.pBuf);
  tdbTbcClose(pTbDbc);
  tdbTbcClose(pUidIdxc);
  tDecoderClear(&dc);

  return -1;
}

static int metaUpdateTableTagVal(SMeta *pMeta, int64_t version, SVAlterTbReq *pAlterTbReq) {
  SMetaEntry  ctbEntry = {0};
  SMetaEntry  stbEntry = {0};
  void       *pVal = NULL;
  int         nVal = 0;
  int         ret;
  int         c;
  tb_uid_t    uid;
  int64_t     oversion;
  const void *pData = NULL;
  int         nData = 0;

  if (pAlterTbReq->tagName == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  // search name index
  ret = tdbTbGet(pMeta->pNameIdx, pAlterTbReq->tbName, strlen(pAlterTbReq->tbName) + 1, &pVal, &nVal);
  if (ret < 0) {
    terrno = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    return -1;
  }

  uid = *(tb_uid_t *)pVal;
  tdbFree(pVal);
  pVal = NULL;

  // search uid index
  TBC *pUidIdxc = NULL;

  tdbTbcOpen(pMeta->pUidIdx, &pUidIdxc, NULL);
  tdbTbcMoveTo(pUidIdxc, &uid, sizeof(uid), &c);
  if (c != 0) {
    tdbTbcClose(pUidIdxc);
    terrno = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    metaError("meta/table: invalide c: %" PRId32 " update tb tag val failed.", c);
    return -1;
  }

  tdbTbcGet(pUidIdxc, NULL, NULL, &pData, &nData);
  oversion = ((SUidIdxVal *)pData)[0].version;

  // search table.db
  TBC     *pTbDbc = NULL;
  SDecoder dc1 = {0};
  SDecoder dc2 = {0};

  /* get ctbEntry */
  tdbTbcOpen(pMeta->pTbDb, &pTbDbc, NULL);
  tdbTbcMoveTo(pTbDbc, &((STbDbKey){.uid = uid, .version = oversion}), sizeof(STbDbKey), &c);
  if (c != 0) {
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);
    terrno = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    metaError("meta/table: invalide c: %" PRId32 " update tb tag val failed.", c);
    return -1;
  }

  tdbTbcGet(pTbDbc, NULL, NULL, &pData, &nData);

  ctbEntry.pBuf = taosMemoryMalloc(nData);
  memcpy(ctbEntry.pBuf, pData, nData);
  tDecoderInit(&dc1, ctbEntry.pBuf, nData);
  metaDecodeEntry(&dc1, &ctbEntry);

  /* get stbEntry*/
  tdbTbGet(pMeta->pUidIdx, &ctbEntry.ctbEntry.suid, sizeof(tb_uid_t), &pVal, &nVal);
  if (!pVal) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _err;
  }

  tdbTbGet(pMeta->pTbDb, &((STbDbKey){.uid = ctbEntry.ctbEntry.suid, .version = ((SUidIdxVal *)pVal)[0].version}),
           sizeof(STbDbKey), (void **)&stbEntry.pBuf, &nVal);
  tdbFree(pVal);
  tDecoderInit(&dc2, stbEntry.pBuf, nVal);
  metaDecodeEntry(&dc2, &stbEntry);

  SSchemaWrapper *pTagSchema = &stbEntry.stbEntry.schemaTag;
  SSchema        *pColumn = NULL;
  int32_t         iCol = 0;
  for (;;) {
    pColumn = NULL;

    if (iCol >= pTagSchema->nCols) break;
    pColumn = &pTagSchema->pSchema[iCol];

    if (strcmp(pColumn->name, pAlterTbReq->tagName) == 0) break;
    iCol++;
  }

  if (pColumn == NULL) {
    terrno = TSDB_CODE_VND_COL_NOT_EXISTS;
    goto _err;
  }

  ctbEntry.version = version;
  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    ctbEntry.ctbEntry.pTags = taosMemoryMalloc(pAlterTbReq->nTagVal);
    if (ctbEntry.ctbEntry.pTags == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    memcpy((void *)ctbEntry.ctbEntry.pTags, pAlterTbReq->pTagVal, pAlterTbReq->nTagVal);
  } else {
    const STag *pOldTag = (const STag *)ctbEntry.ctbEntry.pTags;
    STag       *pNewTag = NULL;
    SArray     *pTagArray = taosArrayInit(pTagSchema->nCols, sizeof(STagVal));
    if (!pTagArray) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    for (int32_t i = 0; i < pTagSchema->nCols; i++) {
      SSchema *pCol = &pTagSchema->pSchema[i];
      if (iCol == i) {
        if (pAlterTbReq->isNull) {
          continue;
        }
        STagVal val = {0};
        val.type = pCol->type;
        val.cid = pCol->colId;
        if (IS_VAR_DATA_TYPE(pCol->type)) {
          val.pData = pAlterTbReq->pTagVal;
          val.nData = pAlterTbReq->nTagVal;
        } else {
          memcpy(&val.i64, pAlterTbReq->pTagVal, pAlterTbReq->nTagVal);
        }
        taosArrayPush(pTagArray, &val);
      } else {
        STagVal val = {.cid = pCol->colId};
        if (tTagGet(pOldTag, &val)) {
          taosArrayPush(pTagArray, &val);
        }
      }
    }
    if ((terrno = tTagNew(pTagArray, pTagSchema->version, false, &pNewTag)) < 0) {
      taosArrayDestroy(pTagArray);
      goto _err;
    }
    ctbEntry.ctbEntry.pTags = (uint8_t *)pNewTag;
    taosArrayDestroy(pTagArray);
  }

  metaWLock(pMeta);

  // save to table.db
  metaSaveToTbDb(pMeta, &ctbEntry);

  // save to uid.idx
  metaUpdateUidIdx(pMeta, &ctbEntry);

  metaUpdateTagIdx(pMeta, &ctbEntry);

  if (NULL == ctbEntry.ctbEntry.pTags) {
    metaError("meta/table: null tags, update tag val failed.");
    goto _err;
  }

  SCtbIdxKey ctbIdxKey = {.suid = ctbEntry.ctbEntry.suid, .uid = uid};
  tdbTbUpsert(pMeta->pCtbIdx, &ctbIdxKey, sizeof(ctbIdxKey), ctbEntry.ctbEntry.pTags,
              ((STag *)(ctbEntry.ctbEntry.pTags))->len, pMeta->txn);

  metaUidCacheClear(pMeta, ctbEntry.ctbEntry.suid);
  metaTbGroupCacheClear(pMeta, ctbEntry.ctbEntry.suid);        

  metaULock(pMeta);

  tDecoderClear(&dc1);
  tDecoderClear(&dc2);
  taosMemoryFree((void *)ctbEntry.ctbEntry.pTags);
  if (ctbEntry.pBuf) taosMemoryFree(ctbEntry.pBuf);
  if (stbEntry.pBuf) tdbFree(stbEntry.pBuf);
  tdbTbcClose(pTbDbc);
  tdbTbcClose(pUidIdxc);
  return 0;

_err:
  tDecoderClear(&dc1);
  tDecoderClear(&dc2);
  if (ctbEntry.pBuf) taosMemoryFree(ctbEntry.pBuf);
  if (stbEntry.pBuf) tdbFree(stbEntry.pBuf);
  tdbTbcClose(pTbDbc);
  tdbTbcClose(pUidIdxc);
  return -1;
}

static int metaUpdateTableOptions(SMeta *pMeta, int64_t version, SVAlterTbReq *pAlterTbReq) {
  void       *pVal = NULL;
  int         nVal = 0;
  const void *pData = NULL;
  int         nData = 0;
  int         ret = 0;
  tb_uid_t    uid;
  int64_t     oversion;
  SMetaEntry  entry = {0};
  int         c = 0;

  // search name index
  ret = tdbTbGet(pMeta->pNameIdx, pAlterTbReq->tbName, strlen(pAlterTbReq->tbName) + 1, &pVal, &nVal);
  if (ret < 0) {
    terrno = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    return -1;
  }

  uid = *(tb_uid_t *)pVal;
  tdbFree(pVal);
  pVal = NULL;

  // search uid index
  TBC *pUidIdxc = NULL;

  tdbTbcOpen(pMeta->pUidIdx, &pUidIdxc, NULL);
  tdbTbcMoveTo(pUidIdxc, &uid, sizeof(uid), &c);
  if (c != 0) {
    tdbTbcClose(pUidIdxc);
    metaError("meta/table: invalide c: %" PRId32 " update tb options failed.", c);
    return -1;
  }

  tdbTbcGet(pUidIdxc, NULL, NULL, &pData, &nData);
  oversion = ((SUidIdxVal *)pData)[0].version;

  // search table.db
  TBC *pTbDbc = NULL;

  tdbTbcOpen(pMeta->pTbDb, &pTbDbc, NULL);
  tdbTbcMoveTo(pTbDbc, &((STbDbKey){.uid = uid, .version = oversion}), sizeof(STbDbKey), &c);
  if (c != 0) {
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);
    metaError("meta/table: invalide c: %" PRId32 " update tb options failed.", c);
    return -1;
  }

  tdbTbcGet(pTbDbc, NULL, NULL, &pData, &nData);

  // get table entry
  SDecoder dc = {0};
  entry.pBuf = taosMemoryMalloc(nData);
  memcpy(entry.pBuf, pData, nData);
  tDecoderInit(&dc, entry.pBuf, nData);
  ret = metaDecodeEntry(&dc, &entry);
  if (ret != 0) {
    tDecoderClear(&dc);
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);
    metaError("meta/table: invalide ret: %" PRId32 " alt tb options failed.", ret);
    return -1;
  }

  entry.version = version;
  metaWLock(pMeta);
  // build SMetaEntry
  if (entry.type == TSDB_CHILD_TABLE) {
    if (pAlterTbReq->updateTTL) {
      metaDeleteTtlIdx(pMeta, &entry);
      entry.ctbEntry.ttlDays = pAlterTbReq->newTTL;
      metaUpdateTtlIdx(pMeta, &entry);
    }
    if (pAlterTbReq->newCommentLen >= 0) {
      entry.ctbEntry.commentLen = pAlterTbReq->newCommentLen;
      entry.ctbEntry.comment = pAlterTbReq->newComment;
    }
  } else {
    if (pAlterTbReq->updateTTL) {
      metaDeleteTtlIdx(pMeta, &entry);
      entry.ntbEntry.ttlDays = pAlterTbReq->newTTL;
      metaUpdateTtlIdx(pMeta, &entry);
    }
    if (pAlterTbReq->newCommentLen >= 0) {
      entry.ntbEntry.commentLen = pAlterTbReq->newCommentLen;
      entry.ntbEntry.comment = pAlterTbReq->newComment;
    }
  }

  // save to table db
  metaSaveToTbDb(pMeta, &entry);
  metaUpdateUidIdx(pMeta, &entry);
  metaULock(pMeta);

  tdbTbcClose(pTbDbc);
  tdbTbcClose(pUidIdxc);
  tDecoderClear(&dc);
  if (entry.pBuf) taosMemoryFree(entry.pBuf);
  return 0;
}

static int metaAddTagIndex(SMeta *pMeta, int64_t version, SVAlterTbReq *pAlterTbReq) {
  SMetaEntry  stbEntry = {0};
  void       *pVal = NULL;
  int         nVal = 0;
  int         ret;
  int         c;
  tb_uid_t    uid, suid;
  int64_t     oversion;
  const void *pData = NULL;
  int         nData = 0;
  SDecoder    dc = {0};

  if (pAlterTbReq->tagName == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  // search name index
  ret = tdbTbGet(pMeta->pNameIdx, pAlterTbReq->tbName, strlen(pAlterTbReq->tbName) + 1, &pVal, &nVal);
  if (ret < 0) {
    terrno = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    return -1;
  }
  uid = *(tb_uid_t *)pVal;
  tdbFree(pVal);
  pVal = NULL;

  if (tdbTbGet(pMeta->pUidIdx, &uid, sizeof(tb_uid_t), &pVal, &nVal) == -1) {
    ret = -1;
    goto _err;
  }
  suid = ((SUidIdxVal *)pVal)[0].suid;

  STbDbKey tbDbKey = {0};
  tbDbKey.uid = suid;
  tbDbKey.version = ((SUidIdxVal *)pVal)[0].version;
  tdbTbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &pVal, &nVal);
  tDecoderInit(&dc, pVal, nVal);
  ret = metaDecodeEntry(&dc, &stbEntry);
  if (ret < 0) {
    goto _err;
  }

  // Get target schema info
  SSchemaWrapper *pTagSchema = &stbEntry.stbEntry.schemaTag;
  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    terrno = TSDB_CODE_VND_COL_ALREADY_EXISTS;
    goto _err;
  }
  SSchema *pCol = NULL;
  int32_t  iCol = 0;
  for (;;) {
    pCol = NULL;
    if (iCol >= pTagSchema->nCols) break;
    pCol = &pTagSchema->pSchema[iCol];
    if (strcmp(pCol->name, pAlterTbReq->tagName) == 0) break;
    iCol++;
  }

  if (iCol == 0) {
    terrno = TSDB_CODE_VND_COL_ALREADY_EXISTS;
    goto _err;
  }
  if (pCol == NULL) {
    terrno = TSDB_CODE_VND_COL_NOT_EXISTS;
    goto _err;
  }

  /*
   * iterator all pTdDbc by uid and version
   */
  TBC *pCtbIdxc = NULL;
  tdbTbcOpen(pMeta->pCtbIdx, &pCtbIdxc, NULL);
  int rc = tdbTbcMoveTo(pCtbIdxc, &(SCtbIdxKey){.suid = suid, .uid = INT64_MIN}, sizeof(SCtbIdxKey), &c);
  if (rc < 0) {
    tdbTbcClose(pCtbIdxc);
    goto _err;
  }
  for (;;) {
    void *pKey, *pVal;
    int   nKey, nVal;
    rc = tdbTbcNext(pCtbIdxc, &pKey, &nKey, &pVal, &nVal);
    if (rc < 0) break;
    if (((SCtbIdxKey *)pKey)->suid != uid) {
      tdbFree(pKey);
      tdbFree(pVal);
      continue;
    }
    STagIdxKey *pTagIdxKey = NULL;
    int32_t     nTagIdxKey;

    const void *pTagData = NULL;
    int32_t     nTagData = 0;

    STagVal tagVal = {.cid = pCol->colId};
    tTagGet((const STag *)pVal, &tagVal);
    if (IS_VAR_DATA_TYPE(pCol->type)) {
      pTagData = tagVal.pData;
      nTagData = (int32_t)tagVal.nData;
    } else {
      pTagData = &(tagVal.i64);
      nTagData = tDataTypes[pCol->type].bytes;
    }
    if (metaCreateTagIdxKey(suid, pCol->colId, pTagData, nTagData, pCol->type, uid, &pTagIdxKey, &nTagIdxKey) < 0) {
      metaDestroyTagIdxKey(pTagIdxKey);
      goto _err;
    }
    tdbTbUpsert(pMeta->pTagIdx, pTagIdxKey, nTagIdxKey, NULL, 0, pMeta->txn);
    metaDestroyTagIdxKey(pTagIdxKey);
  }
  return 0;

_err:
  // tDecoderClear(&dc1);
  // tDecoderClear(&dc2);
  // if (ctbEntry.pBuf) taosMemoryFree(ctbEntry.pBuf);
  // if (stbEntry.pBuf) tdbFree(stbEntry.pBuf);
  // tdbTbcClose(pTbDbc);
  // tdbTbcClose(pUidIdxc);
  return -1;
}

typedef struct SMetaPair {
  void *key;
  int   nkey;
} SMetaPair;

static int metaDropTagIndex(SMeta *pMeta, int64_t version, SVAlterTbReq *pAlterTbReq) {
  SMetaEntry  stbEntry = {0};
  void       *pVal = NULL;
  int         nVal = 0;
  int         ret;
  int         c;
  tb_uid_t    suid;
  int64_t     oversion;
  const void *pData = NULL;
  int         nData = 0;
  SDecoder    dc = {0};

  if (pAlterTbReq->tagName == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  // search name index
  ret = tdbTbGet(pMeta->pNameIdx, pAlterTbReq->tbName, strlen(pAlterTbReq->tbName) + 1, &pVal, &nVal);
  if (ret < 0) {
    terrno = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    return -1;
  }
  suid = *(tb_uid_t *)pVal;
  tdbFree(pVal);
  pVal = NULL;

  if (tdbTbGet(pMeta->pUidIdx, &suid, sizeof(tb_uid_t), &pVal, &nVal) == -1) {
    ret = -1;
    goto _err;
  }

  STbDbKey tbDbKey = {0};
  tbDbKey.uid = suid;
  tbDbKey.version = ((SUidIdxVal *)pVal)[0].version;
  tdbTbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &pVal, &nVal);

  tDecoderInit(&dc, pVal, nVal);
  ret = metaDecodeEntry(&dc, &stbEntry);
  if (ret < 0) {
    goto _err;
  }

  // Get targe schema info
  SSchemaWrapper *pTagSchema = &stbEntry.stbEntry.schemaTag;
  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    terrno = TSDB_CODE_VND_COL_ALREADY_EXISTS;
    goto _err;
  }
  SSchema *pCol = NULL;
  int32_t  iCol = 0;
  for (;;) {
    pCol = NULL;
    if (iCol >= pTagSchema->nCols) break;
    pCol = &pTagSchema->pSchema[iCol];
    if (strcmp(pCol->name, pAlterTbReq->tagName) == 0) break;
    iCol++;
  }
  if (iCol == 0) {
    // cannot drop 1th tag index
    terrno = -1;
    goto _err;
  }
  if (pCol == NULL) {
    terrno = TSDB_CODE_VND_COL_NOT_EXISTS;
    goto _err;
  }

  if (IS_IDX_ON(pCol)) {
    terrno = TSDB_CODE_VND_COL_ALREADY_EXISTS;
    goto _err;
  }

  SArray *tagIdxList = taosArrayInit(512, sizeof(SMetaPair));

  TBC *pTagIdxc = NULL;
  tdbTbcOpen(pMeta->pTagIdx, &pTagIdxc, NULL);
  int rc =
      tdbTbcMoveTo(pTagIdxc, &(STagIdxKey){.suid = suid, .cid = INT32_MIN, .type = pCol->type}, sizeof(STagIdxKey), &c);
  for (;;) {
    void *pKey, *pVal;
    int   nKey, nVal;
    rc = tdbTbcNext(pTagIdxc, &pKey, &nKey, &pVal, &nVal);
    STagIdxKey *pIdxKey = (STagIdxKey *)pKey;
    if (pIdxKey->suid != suid || pIdxKey->cid != pCol->colId) {
      tdbFree(pKey);
      tdbFree(pVal);
      continue;
    }

    SMetaPair pair = {.key = pKey, nKey = nKey};
    taosArrayPush(tagIdxList, &pair);
  }
  tdbTbcClose(pTagIdxc);

  metaWLock(pMeta);
  for (int i = 0; i < taosArrayGetSize(tagIdxList); i++) {
    SMetaPair *pair = taosArrayGet(tagIdxList, i);
    tdbTbDelete(pMeta->pTagIdx, pair->key, pair->nkey, pMeta->txn);
  }
  metaULock(pMeta);

  taosArrayDestroy(tagIdxList);

  // set pCol->flags; INDEX_ON
  return 0;
_err:
  return -1;
}

int metaAlterTable(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pMetaRsp) {
  switch (pReq->action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
    case TSDB_ALTER_TABLE_DROP_COLUMN:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      return metaAlterTableColumn(pMeta, version, pReq, pMetaRsp);
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      return metaUpdateTableTagVal(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      return metaUpdateTableOptions(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_ADD_TAG_INDEX:
      return metaAddTagIndex(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_DROP_TAG_INDEX:
      return metaDropTagIndex(pMeta, version, pReq);
    default:
      terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
      return -1;
      break;
  }
}

static int metaSaveToTbDb(SMeta *pMeta, const SMetaEntry *pME) {
  STbDbKey tbDbKey;
  void    *pKey = NULL;
  void    *pVal = NULL;
  int      kLen = 0;
  int      vLen = 0;
  SEncoder coder = {0};

  // set key and value
  tbDbKey.version = pME->version;
  tbDbKey.uid = pME->uid;

  metaDebug("vgId:%d, start to save table version:%" PRId64 " uid:%" PRId64, TD_VID(pMeta->pVnode), pME->version,
            pME->uid);

  pKey = &tbDbKey;
  kLen = sizeof(tbDbKey);

  int32_t ret = 0;
  tEncodeSize(metaEncodeEntry, pME, vLen, ret);
  if (ret < 0) {
    goto _err;
  }

  pVal = taosMemoryMalloc(vLen);
  if (pVal == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  tEncoderInit(&coder, pVal, vLen);

  if (metaEncodeEntry(&coder, pME) < 0) {
    goto _err;
  }

  tEncoderClear(&coder);

  // write to table.db
  if (tdbTbInsert(pMeta->pTbDb, pKey, kLen, pVal, vLen, pMeta->txn) < 0) {
    goto _err;
  }

  taosMemoryFree(pVal);
  return 0;

_err:
  metaError("vgId:%d, failed to save table version:%" PRId64 "uid:%" PRId64 " %s", TD_VID(pMeta->pVnode), pME->version,
            pME->uid, tstrerror(terrno));

  taosMemoryFree(pVal);
  return -1;
}

static int metaUpdateUidIdx(SMeta *pMeta, const SMetaEntry *pME) {
  // upsert cache
  SMetaInfo info;
  metaGetEntryInfo(pME, &info);
  metaCacheUpsert(pMeta, &info);

  SUidIdxVal uidIdxVal = {.suid = info.suid, .version = info.version, .skmVer = info.skmVer};

  return tdbTbUpsert(pMeta->pUidIdx, &pME->uid, sizeof(tb_uid_t), &uidIdxVal, sizeof(uidIdxVal), pMeta->txn);
}

static int metaUpdateSuidIdx(SMeta *pMeta, const SMetaEntry *pME) {
  return tdbTbUpsert(pMeta->pSuidIdx, &pME->uid, sizeof(tb_uid_t), NULL, 0, pMeta->txn);
}

static int metaUpdateNameIdx(SMeta *pMeta, const SMetaEntry *pME) {
  return tdbTbUpsert(pMeta->pNameIdx, pME->name, strlen(pME->name) + 1, &pME->uid, sizeof(tb_uid_t), pMeta->txn);
}

static int metaUpdateTtlIdx(SMeta *pMeta, const SMetaEntry *pME) {
  STtlIdxKey ttlKey = {0};
  metaBuildTtlIdxKey(&ttlKey, pME);
  if (ttlKey.dtime == 0) return 0;
  return tdbTbUpsert(pMeta->pTtlIdx, &ttlKey, sizeof(ttlKey), NULL, 0, pMeta->txn);
}

static int metaUpdateCtbIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SCtbIdxKey ctbIdxKey = {.suid = pME->ctbEntry.suid, .uid = pME->uid};

  return tdbTbUpsert(pMeta->pCtbIdx, &ctbIdxKey, sizeof(ctbIdxKey), pME->ctbEntry.pTags,
                     ((STag *)(pME->ctbEntry.pTags))->len, pMeta->txn);
}

int metaCreateTagIdxKey(tb_uid_t suid, int32_t cid, const void *pTagData, int32_t nTagData, int8_t type, tb_uid_t uid,
                        STagIdxKey **ppTagIdxKey, int32_t *nTagIdxKey) {
  if (IS_VAR_DATA_TYPE(type)) {
    *nTagIdxKey = sizeof(STagIdxKey) + nTagData + VARSTR_HEADER_SIZE + sizeof(tb_uid_t);
  } else {
    *nTagIdxKey = sizeof(STagIdxKey) + nTagData + sizeof(tb_uid_t);
  }

  *ppTagIdxKey = (STagIdxKey *)taosMemoryMalloc(*nTagIdxKey);
  if (*ppTagIdxKey == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  (*ppTagIdxKey)->suid = suid;
  (*ppTagIdxKey)->cid = cid;
  (*ppTagIdxKey)->isNull = (pTagData == NULL) ? 1 : 0;
  (*ppTagIdxKey)->type = type;

  // refactor
  if (IS_VAR_DATA_TYPE(type)) {
    memcpy((*ppTagIdxKey)->data, (uint16_t *)&nTagData, VARSTR_HEADER_SIZE);
    if (pTagData != NULL) memcpy((*ppTagIdxKey)->data + VARSTR_HEADER_SIZE, pTagData, nTagData);
    *(tb_uid_t *)((*ppTagIdxKey)->data + VARSTR_HEADER_SIZE + nTagData) = uid;
  } else {
    if (pTagData != NULL) memcpy((*ppTagIdxKey)->data, pTagData, nTagData);
    *(tb_uid_t *)((*ppTagIdxKey)->data + nTagData) = uid;
  }

  return 0;
}

static void metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey) {
  if (pTagIdxKey) taosMemoryFree(pTagIdxKey);
}

static int metaUpdateTagIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry) {
  void          *pData = NULL;
  int            nData = 0;
  STbDbKey       tbDbKey = {0};
  SMetaEntry     stbEntry = {0};
  STagIdxKey    *pTagIdxKey = NULL;
  int32_t        nTagIdxKey;
  const SSchema *pTagColumn;
  const void    *pTagData = NULL;
  int32_t        nTagData = 0;
  SDecoder       dc = {0};
  int32_t        ret = 0;
  // get super table
  if (tdbTbGet(pMeta->pUidIdx, &pCtbEntry->ctbEntry.suid, sizeof(tb_uid_t), &pData, &nData) != 0) {
    metaError("vgId:%d, failed to get stable suid for update. version:%" PRId64, TD_VID(pMeta->pVnode),
              pCtbEntry->version);
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    ret = -1;
    goto end;
  }
  tbDbKey.uid = pCtbEntry->ctbEntry.suid;
  tbDbKey.version = ((SUidIdxVal *)pData)[0].version;
  tdbTbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &pData, &nData);

  tDecoderInit(&dc, pData, nData);
  ret = metaDecodeEntry(&dc, &stbEntry);
  if (ret < 0) {
    goto end;
  }

  if (stbEntry.stbEntry.schemaTag.pSchema == NULL) {
    goto end;
  }

  SSchemaWrapper *pTagSchema = &stbEntry.stbEntry.schemaTag;
  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    pTagColumn = &stbEntry.stbEntry.schemaTag.pSchema[0];
    STagVal tagVal = {.cid = pTagColumn->colId};

    pTagData = pCtbEntry->ctbEntry.pTags;
    nTagData = ((const STag *)pCtbEntry->ctbEntry.pTags)->len;
    ret = metaSaveJsonVarToIdx(pMeta, pCtbEntry, pTagColumn);
    goto end;
  } else {
    for (int i = 0; i < pTagSchema->nCols; i++) {
      pTagColumn = &pTagSchema->pSchema[i];
      if (i != 0 && !IS_IDX_ON(pTagColumn)) continue;

      STagVal tagVal = {.cid = pTagColumn->colId};
      tTagGet((const STag *)pCtbEntry->ctbEntry.pTags, &tagVal);
      if (IS_VAR_DATA_TYPE(pTagColumn->type)) {
        pTagData = tagVal.pData;
        nTagData = (int32_t)tagVal.nData;
      } else {
        pTagData = &(tagVal.i64);
        nTagData = tDataTypes[pTagColumn->type].bytes;
      }

      if (pTagData != NULL) {
        if (metaCreateTagIdxKey(pCtbEntry->ctbEntry.suid, pTagColumn->colId, pTagData, nTagData, pTagColumn->type,
                                pCtbEntry->uid, &pTagIdxKey, &nTagIdxKey) < 0) {
          ret = -1;
          goto end;
        }
        tdbTbUpsert(pMeta->pTagIdx, pTagIdxKey, nTagIdxKey, NULL, 0, pMeta->txn);
      }
      metaDestroyTagIdxKey(pTagIdxKey);
    }
  }
end:
  // metaDestroyTagIdxKey(pTagIdxKey);
  tDecoderClear(&dc);
  tdbFree(pData);
  return ret;
}

static int metaSaveToSkmDb(SMeta *pMeta, const SMetaEntry *pME) {
  SEncoder              coder = {0};
  void                 *pVal = NULL;
  int                   vLen = 0;
  int                   rcode = 0;
  SSkmDbKey             skmDbKey = {0};
  const SSchemaWrapper *pSW;

  if (pME->type == TSDB_SUPER_TABLE) {
    pSW = &pME->stbEntry.schemaRow;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    pSW = &pME->ntbEntry.schemaRow;
  } else {
    metaError("meta/table: invalide table type: %" PRId8 " save skm db failed.", pME->type);
    return TSDB_CODE_FAILED;
  }

  skmDbKey.uid = pME->uid;
  skmDbKey.sver = pSW->version;

  // if receive tmq meta message is: create stable1 then delete stable1 then create stable1 with multi vgroups
  if (tdbTbGet(pMeta->pSkmDb, &skmDbKey, sizeof(skmDbKey), NULL, NULL) == 0) {
    return rcode;
  }

  // encode schema
  int32_t ret = 0;
  tEncodeSize(tEncodeSSchemaWrapper, pSW, vLen, ret);
  if (ret < 0) return -1;
  pVal = taosMemoryMalloc(vLen);
  if (pVal == NULL) {
    rcode = -1;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  tEncoderInit(&coder, pVal, vLen);
  tEncodeSSchemaWrapper(&coder, pSW);

  if (tdbTbInsert(pMeta->pSkmDb, &skmDbKey, sizeof(skmDbKey), pVal, vLen, pMeta->txn) < 0) {
    rcode = -1;
    goto _exit;
  }

  metaDebug("vgId:%d, set schema:(%" PRId64 ") sver:%d since %s", TD_VID(pMeta->pVnode), pME->uid, pSW->version,
            tstrerror(terrno));

_exit:
  taosMemoryFree(pVal);
  tEncoderClear(&coder);
  return rcode;
}

int metaHandleEntry(SMeta *pMeta, const SMetaEntry *pME) {
  int32_t code = 0;
  int32_t line = 0;
  metaWLock(pMeta);

  // save to table.db
  code = metaSaveToTbDb(pMeta, pME);
  VND_CHECK_CODE(code, line, _err);

  // update uid.idx
  code = metaUpdateUidIdx(pMeta, pME);
  VND_CHECK_CODE(code, line, _err);

  // update name.idx
  code = metaUpdateNameIdx(pMeta, pME);
  VND_CHECK_CODE(code, line, _err);

  if (pME->type == TSDB_CHILD_TABLE) {
    // update ctb.idx
    code = metaUpdateCtbIdx(pMeta, pME);
    VND_CHECK_CODE(code, line, _err);

    // update tag.idx
    code = metaUpdateTagIdx(pMeta, pME);
    VND_CHECK_CODE(code, line, _err);
  } else {
    // update schema.db
    code = metaSaveToSkmDb(pMeta, pME);
    VND_CHECK_CODE(code, line, _err);

    if (pME->type == TSDB_SUPER_TABLE) {
      code = metaUpdateSuidIdx(pMeta, pME);
      VND_CHECK_CODE(code, line, _err);
    }
  }

  code = metaUpdateCtimeIdx(pMeta, pME);
  VND_CHECK_CODE(code, line, _err);

  if (pME->type == TSDB_NORMAL_TABLE) {
    code = metaUpdateNcolIdx(pMeta, pME);
    VND_CHECK_CODE(code, line, _err);
  }

  if (pME->type != TSDB_SUPER_TABLE) {
    code = metaUpdateTtlIdx(pMeta, pME);
    VND_CHECK_CODE(code, line, _err);
  }

  metaULock(pMeta);
  metaDebug("vgId:%d, handle meta entry, ver:%" PRId64 ", uid:%" PRId64 ", name:%s", TD_VID(pMeta->pVnode),
            pME->version, pME->uid, pME->name);
  return 0;

_err:
  metaULock(pMeta);
  metaError("vgId:%d, failed to handle meta entry since %s at line:%d, ver:%" PRId64 ", uid:%" PRId64 ", name:%s",
            TD_VID(pMeta->pVnode), terrstr(), line, pME->version, pME->uid, pME->name);
  return -1;
}

// refactor later
void *metaGetIdx(SMeta *pMeta) { return pMeta->pTagIdx; }
void *metaGetIvtIdx(SMeta *pMeta) { return pMeta->pTagIvtIdx; }
