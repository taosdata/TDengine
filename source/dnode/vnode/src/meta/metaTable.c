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

extern SDmNotifyHandle dmNotifyHdl;

int32_t    metaSaveJsonVarToIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);
int32_t    metaDelJsonVarFromIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);
static int metaUpdateChangeTime(SMeta *pMeta, tb_uid_t uid, int64_t changeTimeMs);
void       metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey);

int32_t updataTableColCmpr(SColCmprWrapper *pWp, SSchema *pSchema, int8_t add, uint32_t compress) {
  int32_t nCols = pWp->nCols;
  int32_t ver = pWp->version;
  if (add) {
    SColCmpr *p = taosMemoryRealloc(pWp->pColCmpr, sizeof(SColCmpr) * (nCols + 1));
    if (p == NULL) {
      return terrno;
    }
    pWp->pColCmpr = p;

    SColCmpr *pCol = p + nCols;
    pCol->id = pSchema->colId;
    pCol->alg = compress;
    pWp->nCols = nCols + 1;
    pWp->version = ver;
  } else {
    for (int32_t i = 0; i < nCols; i++) {
      SColCmpr *pOCmpr = &pWp->pColCmpr[i];
      if (pOCmpr->id == pSchema->colId) {
        int32_t left = (nCols - i - 1) * sizeof(SColCmpr);
        if (left) {
          memmove(pWp->pColCmpr + i, pWp->pColCmpr + i + 1, left);
        }
        nCols--;
        break;
      }
    }
    pWp->nCols = nCols;
    pWp->version = ver;
  }
  return 0;
}

int metaUpdateMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema, STableMetaRsp *pMetaRsp) {
  pMetaRsp->pSchemas = taosMemoryMalloc(pSchema->nCols * sizeof(SSchema));
  if (NULL == pMetaRsp->pSchemas) {
    return terrno;
  }

  pMetaRsp->pSchemaExt = taosMemoryMalloc(pSchema->nCols * sizeof(SSchemaExt));
  if (pMetaRsp->pSchemaExt == NULL) {
    taosMemoryFree(pMetaRsp->pSchemas);
    return terrno;
  }

  tstrncpy(pMetaRsp->tbName, tbName, TSDB_TABLE_NAME_LEN);
  pMetaRsp->numOfColumns = pSchema->nCols;
  pMetaRsp->tableType = TSDB_NORMAL_TABLE;
  pMetaRsp->sversion = pSchema->version;
  pMetaRsp->tuid = uid;

  memcpy(pMetaRsp->pSchemas, pSchema->pSchema, pSchema->nCols * sizeof(SSchema));

  return 0;
}

int metaSaveJsonVarToIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema) {
  int32_t code = 0;

#ifdef USE_INVERTED_INDEX
  if (pMeta->pTagIvtIdx == NULL || pCtbEntry == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  void       *data = pCtbEntry->ctbEntry.pTags;
  const char *tagName = pSchema->name;

  tb_uid_t    suid = pCtbEntry->ctbEntry.suid;
  tb_uid_t    tuid = pCtbEntry->uid;
  const void *pTagData = pCtbEntry->ctbEntry.pTags;
  int32_t     nTagData = 0;

  SArray *pTagVals = NULL;
  code = tTagToValArray((const STag *)data, &pTagVals);
  if (code) {
    return code;
  }

  SIndexMultiTerm *terms = indexMultiTermCreate();
  if (terms == NULL) {
    return terrno;
  }

  int16_t nCols = taosArrayGetSize(pTagVals);
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
        char *val = taosMemoryCalloc(1, pTagVal->nData + VARSTR_HEADER_SIZE);
        if (val == NULL) {
          TAOS_CHECK_GOTO(terrno, NULL, _exception);
        }
        int32_t len = taosUcs4ToMbs((TdUcs4 *)pTagVal->pData, pTagVal->nData, val + VARSTR_HEADER_SIZE, NULL);
        if (len < 0) {
          TAOS_CHECK_GOTO(len, NULL, _exception);
        }
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
      int32_t ret = indexMultiTermAdd(terms, term);
      if (ret < 0) {
        metaError("vgId:%d, failed to add term to multi term, uid: %" PRId64 ", key: %s, type: %d, ret: %d",
                  TD_VID(pMeta->pVnode), tuid, key, type, ret);
      }
    } else {
      code = terrno;
      goto _exception;
    }
  }
  code = indexJsonPut(pMeta->pTagIvtIdx, terms, tuid);
  indexMultiTermDestroy(terms);

  taosArrayDestroy(pTagVals);
#endif
  return code;
_exception:
  indexMultiTermDestroy(terms);
  taosArrayDestroy(pTagVals);
  return code;
}

int metaDelJsonVarFromIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema) {
#ifdef USE_INVERTED_INDEX
  if (pMeta->pTagIvtIdx == NULL || pCtbEntry == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  void       *data = pCtbEntry->ctbEntry.pTags;
  const char *tagName = pSchema->name;

  tb_uid_t    suid = pCtbEntry->ctbEntry.suid;
  tb_uid_t    tuid = pCtbEntry->uid;
  const void *pTagData = pCtbEntry->ctbEntry.pTags;
  int32_t     nTagData = 0;

  SArray *pTagVals = NULL;
  int32_t code = tTagToValArray((const STag *)data, &pTagVals);
  if (code) {
    return code;
  }

  SIndexMultiTerm *terms = indexMultiTermCreate();
  if (terms == NULL) {
    return terrno;
  }

  int16_t nCols = taosArrayGetSize(pTagVals);
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
        char *val = taosMemoryCalloc(1, pTagVal->nData + VARSTR_HEADER_SIZE);
        if (val == NULL) {
          TAOS_CHECK_GOTO(terrno, NULL, _exception);
        }
        int32_t len = taosUcs4ToMbs((TdUcs4 *)pTagVal->pData, pTagVal->nData, val + VARSTR_HEADER_SIZE, NULL);
        if (len < 0) {
          TAOS_CHECK_GOTO(len, NULL, _exception);
        }
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
      int32_t ret = indexMultiTermAdd(terms, term);
      if (ret < 0) {
        metaError("vgId:%d, failed to add term to multi term, uid: %" PRId64 ", key: %s, type: %d, ret: %d",
                  TD_VID(pMeta->pVnode), tuid, key, type, ret);
      }
    } else {
      code = terrno;
      goto _exception;
    }
  }
  code = indexJsonPut(pMeta->pTagIvtIdx, terms, tuid);
  indexMultiTermDestroy(terms);
  taosArrayDestroy(pTagVals);
#endif
  return code;
_exception:
  indexMultiTermDestroy(terms);
  taosArrayDestroy(pTagVals);
  return code;
}

void metaTimeSeriesNotifyCheck(SMeta *pMeta) {
#if defined(TD_ENTERPRISE)
  int64_t nTimeSeries = metaGetTimeSeriesNum(pMeta, 0);
  int64_t deltaTS = nTimeSeries - pMeta->pVnode->config.vndStats.numOfReportedTimeSeries;
  if (deltaTS > tsTimeSeriesThreshold) {
    if (0 == atomic_val_compare_exchange_8(&dmNotifyHdl.state, 1, 2)) {
      if (tsem_post(&dmNotifyHdl.sem) != 0) {
        metaError("vgId:%d, failed to post semaphore, errno:%d", TD_VID(pMeta->pVnode), errno);
      }
    }
  }
#endif
}

static int32_t metaFilterTableByHash(SMeta *pMeta, SArray *uidList) {
  int32_t code = 0;
  // 1, tranverse table's
  // 2, validate table name using vnodeValidateTableHash
  // 3, push invalidated table's uid into uidList

  TBC *pCur;
  code = tdbTbcOpen(pMeta->pTbDb, &pCur, NULL);
  if (code < 0) {
    return code;
  }

  code = tdbTbcMoveToFirst(pCur);
  if (code) {
    tdbTbcClose(pCur);
    return code;
  }

  void *pData = NULL, *pKey = NULL;
  int   nData = 0, nKey = 0;

  while (1) {
    int32_t ret = tdbTbcNext(pCur, &pKey, &nKey, &pData, &nData);
    if (ret < 0) {
      break;
    }

    SMetaEntry me = {0};
    SDecoder   dc = {0};
    tDecoderInit(&dc, pData, nData);
    code = metaDecodeEntry(&dc, &me);
    if (code < 0) {
      tDecoderClear(&dc);
      return code;
    }

    if (me.type != TSDB_SUPER_TABLE) {
      char tbFName[TSDB_TABLE_FNAME_LEN + 1];
      snprintf(tbFName, sizeof(tbFName), "%s.%s", pMeta->pVnode->config.dbname, me.name);
      tbFName[TSDB_TABLE_FNAME_LEN] = '\0';
      int32_t ret = vnodeValidateTableHash(pMeta->pVnode, tbFName);
      if (ret < 0 && terrno == TSDB_CODE_VND_HASH_MISMATCH) {
        if (taosArrayPush(uidList, &me.uid) == NULL) {
          code = terrno;
          break;
        }
      }
    }
    tDecoderClear(&dc);
  }
  tdbFree(pData);
  tdbFree(pKey);
  tdbTbcClose(pCur);

  return 0;
}

int32_t metaTrimTables(SMeta *pMeta, int64_t version) {
  int32_t code = 0;

  SArray *tbUids = taosArrayInit(8, sizeof(int64_t));
  if (tbUids == NULL) {
    return terrno;
  }

  code = metaFilterTableByHash(pMeta, tbUids);
  if (code != 0) {
    goto end;
  }
  if (TARRAY_SIZE(tbUids) == 0) {
    goto end;
  }

  metaInfo("vgId:%d, trim %ld tables", TD_VID(pMeta->pVnode), taosArrayGetSize(tbUids));
  code = metaDropMultipleTables(pMeta, version, tbUids);
  if (code) goto end;

end:
  taosArrayDestroy(tbUids);

  return code;
}

int metaTtlFindExpired(SMeta *pMeta, int64_t timePointMs, SArray *tbUids, int32_t ttlDropMaxCount) {
  metaRLock(pMeta);

  int ret = ttlMgrFindExpired(pMeta->pTtlMgr, timePointMs, tbUids, ttlDropMaxCount);

  metaULock(pMeta);

  if (ret != 0) {
    metaError("ttl failed to find expired table, ret:%d", ret);
  }

  return ret;
}

typedef struct SMetaPair {
  void *key;
  int   nkey;
} SMetaPair;

static int metaUpdateChangeTime(SMeta *pMeta, tb_uid_t uid, int64_t changeTimeMs) {
  if (!tsTtlChangeOnWrite) return 0;

  if (changeTimeMs <= 0) {
    metaWarn("Skip to change ttl deletetion time on write, uid: %" PRId64, uid);
    return TSDB_CODE_VERSION_NOT_COMPATIBLE;
  }

  STtlUpdCtimeCtx ctx = {.uid = uid, .changeTimeMs = changeTimeMs, .pTxn = pMeta->txn};

  return ttlMgrUpdateChangeTime(pMeta->pTtlMgr, &ctx);
}

int metaUpdateChangeTimeWithLock(SMeta *pMeta, tb_uid_t uid, int64_t changeTimeMs) {
  if (!tsTtlChangeOnWrite) return 0;

  metaWLock(pMeta);
  int ret = metaUpdateChangeTime(pMeta, uid, changeTimeMs);
  metaULock(pMeta);
  return ret;
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
    return terrno;
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

void metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey) {
  if (pTagIdxKey) taosMemoryFree(pTagIdxKey);
}

static void colCompressDebug(SHashObj *pColCmprObj) {
  void *p = taosHashIterate(pColCmprObj, NULL);
  while (p) {
    uint32_t cmprAlg = *(uint32_t *)p;
    col_id_t colId = *(col_id_t *)taosHashGetKey(p, NULL);
    p = taosHashIterate(pColCmprObj, p);

    uint8_t l1, l2, lvl;
    tcompressDebug(cmprAlg, &l1, &l2, &lvl);

    const char *l1str = columnEncodeStr(l1);
    const char *l2str = columnCompressStr(l2);
    const char *lvlstr = columnLevelStr(lvl);
    metaDebug("colId: %d, encode:%s, compress:%s,level:%s", colId, l1str, l2str, lvlstr);
  }
  return;
}
int32_t metaGetColCmpr(SMeta *pMeta, tb_uid_t uid, SHashObj **ppColCmprObj) {
  int rc = 0;

  SHashObj *pColCmprObj = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), false, HASH_NO_LOCK);
  if (pColCmprObj == NULL) {
    pColCmprObj = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  void      *pData = NULL;
  int        nData = 0;
  SMetaEntry e = {0};
  SDecoder   dc = {0};

  *ppColCmprObj = NULL;

  metaRLock(pMeta);
  rc = tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pData, &nData);
  if (rc < 0) {
    taosHashClear(pColCmprObj);
    metaULock(pMeta);
    return TSDB_CODE_FAILED;
  }
  int64_t version = ((SUidIdxVal *)pData)[0].version;
  rc = tdbTbGet(pMeta->pTbDb, &(STbDbKey){.version = version, .uid = uid}, sizeof(STbDbKey), &pData, &nData);
  if (rc < 0) {
    metaULock(pMeta);
    taosHashClear(pColCmprObj);
    metaError("failed to get table entry");
    return rc;
  }

  tDecoderInit(&dc, pData, nData);
  rc = metaDecodeEntry(&dc, &e);
  if (rc < 0) {
    tDecoderClear(&dc);
    tdbFree(pData);
    metaULock(pMeta);
    taosHashClear(pColCmprObj);
    return rc;
  }
  if (useCompress(e.type)) {
    SColCmprWrapper *p = &e.colCmpr;
    for (int32_t i = 0; i < p->nCols; i++) {
      SColCmpr *pCmpr = &p->pColCmpr[i];
      rc = taosHashPut(pColCmprObj, &pCmpr->id, sizeof(pCmpr->id), &pCmpr->alg, sizeof(pCmpr->alg));
      if (rc < 0) {
        tDecoderClear(&dc);
        tdbFree(pData);
        metaULock(pMeta);
        taosHashClear(pColCmprObj);
        return rc;
      }
    }
  } else {
    tDecoderClear(&dc);
    tdbFree(pData);
    metaULock(pMeta);
    taosHashClear(pColCmprObj);
    return 0;
  }
  tDecoderClear(&dc);
  tdbFree(pData);
  metaULock(pMeta);

  *ppColCmprObj = pColCmprObj;
  colCompressDebug(pColCmprObj);

  return 0;
}
// refactor later
void *metaGetIdx(SMeta *pMeta) { return pMeta->pTagIdx; }
void *metaGetIvtIdx(SMeta *pMeta) { return pMeta->pTagIvtIdx; }
