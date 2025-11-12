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

int32_t metaAddTableColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp);
int32_t metaDropTableColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp);
int32_t metaAlterTableColumnName(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp);
int32_t metaAlterTableColumnBytes(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp);
int32_t metaUpdateTableTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq);
int32_t metaUpdateTableMultiTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq);
int32_t metaUpdateTableOptions2(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq);
int32_t metaUpdateTableColCompress2(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq);
int32_t metaAlterTableColumnRef(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp);
int32_t metaRemoveTableColumnRef(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp);
int32_t metaSaveJsonVarToIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);

int32_t    metaDelJsonVarFromIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);
static int metaUpdateChangeTime(SMeta *pMeta, tb_uid_t uid, int64_t changeTimeMs);
static int metaDropTableByUid(SMeta *pMeta, tb_uid_t uid, int *type, tb_uid_t *pSuid, int8_t *pSysTbl);
void       metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey);
// opt ins_tables query
static int metaDeleteBtimeIdx(SMeta *pMeta, const SMetaEntry *pME);
static int metaDeleteNcolIdx(SMeta *pMeta, const SMetaEntry *pME);

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

int32_t addTableExtSchema(SMetaEntry *pEntry, const SSchema *pColumn, int32_t newColNum, SExtSchema *pExtSchema) {
  // no need to add ext schema when no column needs ext schemas
  if (!HAS_TYPE_MOD(pColumn) && !pEntry->pExtSchemas) return 0;
  if (!pEntry->pExtSchemas) {
    // add a column which needs ext schema
    // set all extschemas to zero for all columns alrady existed
    pEntry->pExtSchemas = (SExtSchema *)taosMemoryCalloc(newColNum, sizeof(SExtSchema));
  } else {
    // already has columns with ext schema
    pEntry->pExtSchemas = (SExtSchema *)taosMemoryRealloc(pEntry->pExtSchemas, sizeof(SExtSchema) * newColNum);
  }
  if (!pEntry->pExtSchemas) return terrno;
  pEntry->pExtSchemas[newColNum - 1] = *pExtSchema;
  return 0;
}

int32_t dropTableExtSchema(SMetaEntry *pEntry, int32_t dropColId, int32_t newColNum) {
  // no ext schema, no need to drop
  if (!pEntry->pExtSchemas) return 0;
  if (dropColId == newColNum) {
    // drop the last column
    pEntry->pExtSchemas[dropColId - 1] = (SExtSchema){0};
  } else {
    // drop a column in the middle
    memmove(pEntry->pExtSchemas + dropColId, pEntry->pExtSchemas + dropColId + 1,
            (newColNum - dropColId) * sizeof(SExtSchema));
  }
  for (int32_t i = 0; i < newColNum; i++) {
    if (hasExtSchema(pEntry->pExtSchemas + i)) return 0;
  }
  taosMemoryFreeClear(pEntry->pExtSchemas);
  return 0;
}

int32_t updataTableColRef(SColRefWrapper *pWp, const SSchema *pSchema, int8_t add, SColRef *pColRef) {
  int32_t nCols = pWp->nCols;
  if (add) {
    SColRef *p = taosMemoryRealloc(pWp->pColRef, sizeof(SColRef) * (nCols + 1));
    if (p == NULL) {
      return terrno;
    }
    pWp->pColRef = p;

    SColRef *pCol = p + nCols;
    if (NULL == pColRef) {
      pCol->hasRef = false;
      pCol->id = pSchema->colId;
    } else {
      pCol->hasRef = pColRef->hasRef;
      pCol->id = pSchema->colId;
      if (pCol->hasRef) {
        tstrncpy(pCol->refDbName, pColRef->refDbName, TSDB_DB_NAME_LEN);
        tstrncpy(pCol->refTableName, pColRef->refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(pCol->refColName, pColRef->refColName, TSDB_COL_NAME_LEN);
      }
    }
    pWp->nCols = nCols + 1;
    pWp->version++;
  } else {
    for (int32_t i = 0; i < nCols; i++) {
      SColRef *pOColRef = &pWp->pColRef[i];
      if (pOColRef->id == pSchema->colId) {
        int32_t left = (nCols - i - 1) * sizeof(SColRef);
        if (left) {
          memmove(pWp->pColRef + i, pWp->pColRef + i + 1, left);
        }
        nCols--;
        break;
      }
    }
    pWp->nCols = nCols;
    pWp->version++;
  }
  return 0;
}

int metaUpdateMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema, STableMetaRsp *pMetaRsp) {
  pMetaRsp->pSchemas = taosMemoryMalloc(pSchema->nCols * sizeof(SSchema));
  if (NULL == pMetaRsp->pSchemas) {
    return terrno;
  }

  pMetaRsp->pSchemaExt = taosMemoryCalloc(1, pSchema->nCols * sizeof(SSchemaExt));
  if (pMetaRsp->pSchemaExt == NULL) {
    taosMemoryFree(pMetaRsp->pSchemas);
    return terrno;
  }

  tstrncpy(pMetaRsp->tbName, tbName, TSDB_TABLE_NAME_LEN);
  pMetaRsp->numOfColumns = pSchema->nCols;
  pMetaRsp->tableType = TSDB_NORMAL_TABLE;
  pMetaRsp->sversion = pSchema->version;
  pMetaRsp->rversion = 1;
  pMetaRsp->tuid = uid;
  pMetaRsp->virtualStb = false; // super table will never be processed here

  memcpy(pMetaRsp->pSchemas, pSchema->pSchema, pSchema->nCols * sizeof(SSchema));

  return 0;
}

int32_t metaUpdateVtbMetaRsp(SMetaEntry *pEntry, char *tbName, SSchemaWrapper *pSchema, SColRefWrapper *pRef,
                             STableMetaRsp *pMetaRsp, int8_t tableType) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (!pRef) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pSchema) {
    pMetaRsp->pSchemas = taosMemoryMalloc(pSchema->nCols * sizeof(SSchema));
    if (NULL == pMetaRsp->pSchemas) {
      code = terrno;
      goto _return;
    }

    pMetaRsp->pSchemaExt = taosMemoryMalloc(pSchema->nCols * sizeof(SSchemaExt));
    if (pMetaRsp->pSchemaExt == NULL) {
      code = terrno;
      goto _return;
    }

    pMetaRsp->numOfColumns = pSchema->nCols;
    pMetaRsp->sversion = pSchema->version;
    memcpy(pMetaRsp->pSchemas, pSchema->pSchema, pSchema->nCols * sizeof(SSchema));
  }
  pMetaRsp->pColRefs = taosMemoryMalloc(pRef->nCols * sizeof(SColRef));
  if (NULL == pMetaRsp->pColRefs) {
    code = terrno;
    goto _return;
  }
  memcpy(pMetaRsp->pColRefs, pRef->pColRef, pRef->nCols * sizeof(SColRef));
  tstrncpy(pMetaRsp->tbName, tbName, TSDB_TABLE_NAME_LEN);
  if (tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
    pMetaRsp->tuid = pEntry->uid;
  } else if (tableType == TSDB_VIRTUAL_CHILD_TABLE) {
    pMetaRsp->tuid = pEntry->uid;
    pMetaRsp->suid = pEntry->ctbEntry.suid;
  }

  pMetaRsp->tableType = tableType;
  pMetaRsp->virtualStb = false; // super table will never be processed here
  pMetaRsp->numOfColRefs = pRef->nCols;
  pMetaRsp->rversion = pRef->version;

  return code;
_return:
  taosMemoryFreeClear(pMetaRsp->pSchemaExt);
  taosMemoryFreeClear(pMetaRsp->pSchemas);
  taosMemoryFreeClear(pMetaRsp->pColRefs);
  return code;
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
  return code;
_exception:
  indexMultiTermDestroy(terms);
  taosArrayDestroy(pTagVals);
#endif
  return code;
}
int metaDelJsonVarFromIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema) {
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
  return code;
_exception:
  indexMultiTermDestroy(terms);
  taosArrayDestroy(pTagVals);
#endif
  return code;
}

static int32_t metaDropTables(SMeta *pMeta, SArray *tbUids) {
  int32_t code = 0;
  if (taosArrayGetSize(tbUids) == 0) return TSDB_CODE_SUCCESS;

  int64_t    nCtbDropped = 0;
  SSHashObj *suidHash = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (suidHash == NULL) {
    return terrno;
  }

  metaWLock(pMeta);
  for (int i = 0; i < taosArrayGetSize(tbUids); ++i) {
    tb_uid_t uid = *(tb_uid_t *)taosArrayGet(tbUids, i);
    tb_uid_t suid = 0;
    int8_t   sysTbl = 0;
    int      type;
    code = metaDropTableByUid(pMeta, uid, &type, &suid, &sysTbl);
    if (code) return code;
    if (!sysTbl && type == TSDB_CHILD_TABLE && suid != 0 && suidHash) {
      int64_t *pVal = tSimpleHashGet(suidHash, &suid, sizeof(tb_uid_t));
      if (pVal) {
        nCtbDropped = *pVal + 1;
      } else {
        nCtbDropped = 1;
      }
      code = tSimpleHashPut(suidHash, &suid, sizeof(tb_uid_t), &nCtbDropped, sizeof(int64_t));
      if (code) return code;
    }
    /*
    if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
      tsdbCacheDropTable(pMeta->pVnode->pTsdb, uid, suid, NULL);
    }
    */
    metaDebug("batch drop table:%" PRId64, uid);
  }
  metaULock(pMeta);

  // update timeseries
  void   *pCtbDropped = NULL;
  int32_t iter = 0;
  while ((pCtbDropped = tSimpleHashIterate(suidHash, pCtbDropped, &iter))) {
    tb_uid_t    *pSuid = tSimpleHashGetKey(pCtbDropped, NULL);
    int32_t      nCols = 0;
    int8_t       flags = 0;
    SVnodeStats *pStats = &pMeta->pVnode->config.vndStats;
    if (metaGetStbStats(pMeta->pVnode, *pSuid, NULL, &nCols, &flags) == 0) {
      if (!TABLE_IS_VIRTUAL(flags)) {
        pStats->numOfTimeSeries -= *(int64_t *)pCtbDropped * (nCols - 1);
      }
    }
  }
  tSimpleHashCleanup(suidHash);

  pMeta->changed = true;
  return 0;
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
      if (pMeta->pVnode->mounted) tTrimMountPrefix(tbFName);
      ret = vnodeValidateTableHash(pMeta->pVnode, tbFName);
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
  code = metaDropTables(pMeta, tbUids);
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

static int metaBuildBtimeIdxKey(SBtimeIdxKey *btimeKey, const SMetaEntry *pME) {
  int64_t btime;
  if (pME->type == TSDB_CHILD_TABLE) {
    btime = pME->ctbEntry.btime;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    btime = pME->ntbEntry.btime;
  } else {
    return TSDB_CODE_FAILED;
  }

  btimeKey->btime = btime;
  btimeKey->uid = pME->uid;
  return 0;
}

static int metaBuildNColIdxKey(SNcolIdxKey *ncolKey, const SMetaEntry *pME) {
  if (pME->type == TSDB_NORMAL_TABLE) {
    ncolKey->ncol = pME->ntbEntry.schemaRow.nCols;
    ncolKey->uid = pME->uid;
  } else {
    return TSDB_CODE_FAILED;
  }
  return 0;
}

static void metaDeleteTtl(SMeta *pMeta, const SMetaEntry *pME) {
  if (pME->type != TSDB_CHILD_TABLE && pME->type != TSDB_NORMAL_TABLE) return;

  STtlDelTtlCtx ctx = {.uid = pME->uid, .pTxn = pMeta->txn};
  if (pME->type == TSDB_CHILD_TABLE) {
    ctx.ttlDays = pME->ctbEntry.ttlDays;
  } else {
    ctx.ttlDays = pME->ntbEntry.ttlDays;
  }

  int32_t ret = ttlMgrDeleteTtl(pMeta->pTtlMgr, &ctx);
  if (ret < 0) {
    metaError("vgId:%d, failed to delete ttl for table:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pME->name,
              pME->uid, tstrerror(ret));
  }
  return;
}

static int metaDropTableByUid(SMeta *pMeta, tb_uid_t uid, int *type, tb_uid_t *pSuid, int8_t *pSysTbl) {
  void      *pData = NULL;
  int        nData = 0;
  int        rc = 0;
  SMetaEntry e = {0};
  SDecoder   dc = {0};
  int32_t    ret = 0;

  rc = tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pData, &nData);
  if (rc < 0) {
    return rc;
  }
  int64_t version = ((SUidIdxVal *)pData)[0].version;

  rc = tdbTbGet(pMeta->pTbDb, &(STbDbKey){.version = version, .uid = uid}, sizeof(STbDbKey), &pData, &nData);
  if (rc < 0) {
    tdbFree(pData);
    return rc;
  }

  tDecoderInit(&dc, pData, nData);
  rc = metaDecodeEntry(&dc, &e);
  if (rc < 0) {
    tDecoderClear(&dc);
    return rc;
  }

  if (type) *type = e.type;

  if (e.type == TSDB_CHILD_TABLE) {
    if (pSuid) *pSuid = e.ctbEntry.suid;
    void *tData = NULL;
    int   tLen = 0;

    if (tdbTbGet(pMeta->pUidIdx, &e.ctbEntry.suid, sizeof(tb_uid_t), &tData, &tLen) == 0) {
      STbDbKey tbDbKey = {.uid = e.ctbEntry.suid, .version = ((SUidIdxVal *)tData)[0].version};
      if (tdbTbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &tData, &tLen) == 0) {
        SDecoder   tdc = {0};
        SMetaEntry stbEntry = {0};

        tDecoderInit(&tdc, tData, tLen);
        ret = metaDecodeEntry(&tdc, &stbEntry);
        if (ret < 0) {
          tDecoderClear(&tdc);
          metaError("vgId:%d, failed to decode child table:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name,
                    e.ctbEntry.suid, tstrerror(ret));
          return ret;
        }

        if (pSysTbl) *pSysTbl = metaTbInFilterCache(pMeta, stbEntry.name, 1) ? 1 : 0;
        
        ret = metaStableTagFilterCacheUpdateUid(
          pMeta, &e, &stbEntry, STABLE_TAG_FILTER_CACHE_DROP_TABLE);
        if (ret < 0) {
          metaError("vgId:%d, failed to update stable tag filter cache:%s "
            "uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name,
            e.ctbEntry.suid, tstrerror(ret));
        }

        SSchema        *pTagColumn = NULL;
        SSchemaWrapper *pTagSchema = &stbEntry.stbEntry.schemaTag;
        if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
          pTagColumn = &stbEntry.stbEntry.schemaTag.pSchema[0];
          ret = metaDelJsonVarFromIdx(pMeta, &e, pTagColumn);
          if (ret < 0) {
            metaError("vgId:%d, failed to delete json var from idx:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode),
                      e.name, e.uid, tstrerror(ret));
          }
        } else {
          for (int i = 0; i < pTagSchema->nCols; i++) {
            pTagColumn = &stbEntry.stbEntry.schemaTag.pSchema[i];
            if (!IS_IDX_ON(pTagColumn)) continue;
            STagIdxKey *pTagIdxKey = NULL;
            int32_t     nTagIdxKey;

            const void *pTagData = NULL;
            int32_t     nTagData = 0;

            STagVal tagVal = {.cid = pTagColumn->colId};
            if (tTagGet((const STag *)e.ctbEntry.pTags, &tagVal)) {
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

            if (metaCreateTagIdxKey(e.ctbEntry.suid, pTagColumn->colId, pTagData, nTagData, pTagColumn->type, uid,
                                    &pTagIdxKey, &nTagIdxKey) == 0) {
              ret = tdbTbDelete(pMeta->pTagIdx, pTagIdxKey, nTagIdxKey, pMeta->txn);
              if (ret < 0) {
                metaError("vgId:%d, failed to delete tag idx key:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode),
                          e.name, e.uid, tstrerror(ret));
              }
            }
            metaDestroyTagIdxKey(pTagIdxKey);
            pTagIdxKey = NULL;
          }
        }
        tDecoderClear(&tdc);
      }
      tdbFree(tData);
    }
  }

  ret = tdbTbDelete(pMeta->pTbDb, &(STbDbKey){.version = version, .uid = uid}, sizeof(STbDbKey), pMeta->txn);
  if (ret < 0) {
    metaError("vgId:%d, failed to delete table:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name, e.uid,
              tstrerror(ret));
  }
  ret = tdbTbDelete(pMeta->pNameIdx, e.name, strlen(e.name) + 1, pMeta->txn);
  if (ret < 0) {
    metaError("vgId:%d, failed to delete name idx:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name, e.uid,
              tstrerror(ret));
  }
  ret = tdbTbDelete(pMeta->pUidIdx, &uid, sizeof(uid), pMeta->txn);
  if (ret < 0) {
    metaError("vgId:%d, failed to delete uid idx:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name, e.uid,
              tstrerror(ret));
  }

  if (e.type == TSDB_CHILD_TABLE || e.type == TSDB_NORMAL_TABLE) metaDeleteBtimeIdx(pMeta, &e);
  if (e.type == TSDB_NORMAL_TABLE) metaDeleteNcolIdx(pMeta, &e);

  if (e.type != TSDB_SUPER_TABLE) metaDeleteTtl(pMeta, &e);

  if (e.type == TSDB_CHILD_TABLE) {
    ret =
        tdbTbDelete(pMeta->pCtbIdx, &(SCtbIdxKey){.suid = e.ctbEntry.suid, .uid = uid}, sizeof(SCtbIdxKey), pMeta->txn);
    if (ret < 0) {
      metaError("vgId:%d, failed to delete ctb idx:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name, e.uid,
                tstrerror(ret));
    }

    --pMeta->pVnode->config.vndStats.numOfCTables;
    metaUpdateStbStats(pMeta, e.ctbEntry.suid, -1, 0, -1);
    ret = metaUidCacheClear(pMeta, e.ctbEntry.suid);
    if (ret < 0) {
      metaError("vgId:%d, failed to clear uid cache:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name,
                e.ctbEntry.suid, tstrerror(ret));
    }
    ret = metaTbGroupCacheClear(pMeta, e.ctbEntry.suid);
    if (ret < 0) {
      metaError("vgId:%d, failed to clear group cache:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name,
                e.ctbEntry.suid, tstrerror(ret));
    }
    /*
    if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
      tsdbCacheDropTable(pMeta->pVnode->pTsdb, e.uid, e.ctbEntry.suid, NULL);
    }
    */
  } else if (e.type == TSDB_NORMAL_TABLE) {
    // drop schema.db (todo)

    --pMeta->pVnode->config.vndStats.numOfNTables;
    pMeta->pVnode->config.vndStats.numOfNTimeSeries -= e.ntbEntry.schemaRow.nCols - 1;

    /*
    if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
      tsdbCacheDropTable(pMeta->pVnode->pTsdb, e.uid, -1, &e.ntbEntry.schemaRow);
    }
    */
  } else if (e.type == TSDB_SUPER_TABLE) {
    ret = tdbTbDelete(pMeta->pSuidIdx, &e.uid, sizeof(tb_uid_t), pMeta->txn);
    if (ret < 0) {
      metaError("vgId:%d, failed to delete suid idx:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name, e.uid,
                tstrerror(ret));
    }
    // drop schema.db (todo)

    ret = metaStatsCacheDrop(pMeta, uid);
    if (ret < 0) {
      metaError("vgId:%d, failed to drop stats cache:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name, e.uid,
                tstrerror(ret));
    }
    ret = metaUidCacheClear(pMeta, uid);
    if (ret < 0) {
      metaError("vgId:%d, failed to clear uid cache:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name, e.uid,
                tstrerror(ret));
    }
    ret = metaStableTagFilterCacheDropSTable(pMeta, uid);
    if (ret < 0) {
      metaError("vgId:%d, failed to clear stable tag filter cache:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name,
                e.uid, tstrerror(ret));
    }
    ret = metaTbGroupCacheClear(pMeta, uid);
    if (ret < 0) {
      metaError("vgId:%d, failed to clear group cache:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name,
                e.uid, tstrerror(ret));
    }
    --pMeta->pVnode->config.vndStats.numOfSTables;
  }

  ret = metaCacheDrop(pMeta, uid);
  if (ret < 0) {
    metaError("vgId:%d, failed to drop cache:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), e.name, e.uid,
              tstrerror(ret));
  }

  tDecoderClear(&dc);
  tdbFree(pData);

  return 0;
}

static int metaDeleteBtimeIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SBtimeIdxKey btimeKey = {0};
  if (metaBuildBtimeIdxKey(&btimeKey, pME) < 0) {
    return 0;
  }
  return tdbTbDelete(pMeta->pBtimeIdx, &btimeKey, sizeof(btimeKey), pMeta->txn);
}

int metaDeleteNcolIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SNcolIdxKey ncolKey = {0};
  if (metaBuildNColIdxKey(&ncolKey, pME) < 0) {
    return 0;
  }
  return tdbTbDelete(pMeta->pNcolIdx, &ncolKey, sizeof(ncolKey), pMeta->txn);
}

int metaAlterTable(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pMetaRsp) {
  pMeta->changed = true;
  switch (pReq->action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF:
      return metaAddTableColumn(pMeta, version, pReq, pMetaRsp);
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      return metaDropTableColumn(pMeta, version, pReq, pMetaRsp);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      return metaAlterTableColumnBytes(pMeta, version, pReq, pMetaRsp);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      return metaAlterTableColumnName(pMeta, version, pReq, pMetaRsp);
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      return metaUpdateTableTagValue(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL:
      return metaUpdateTableMultiTagValue(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      return metaUpdateTableOptions2(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
      return metaUpdateTableColCompress2(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_ALTER_COLUMN_REF:
      return metaAlterTableColumnRef(pMeta, version, pReq, pMetaRsp);
    case TSDB_ALTER_TABLE_REMOVE_COLUMN_REF:
      return metaRemoveTableColumnRef(pMeta, version, pReq, pMetaRsp);
    default:
      return terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
      break;
  }
}

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

  taosSetInt64Aligned(&((*ppTagIdxKey)->suid), suid);
  (*ppTagIdxKey)->cid = cid;
  (*ppTagIdxKey)->isNull = (pTagData == NULL) ? 1 : 0;
  (*ppTagIdxKey)->type = type;

  // refactor
  if (IS_VAR_DATA_TYPE(type)) {
    memcpy((*ppTagIdxKey)->data, (uint16_t *)&nTagData, VARSTR_HEADER_SIZE);
    if (pTagData != NULL) memcpy((*ppTagIdxKey)->data + VARSTR_HEADER_SIZE, pTagData, nTagData);
    taosSetInt64Aligned((tb_uid_t *)((*ppTagIdxKey)->data + VARSTR_HEADER_SIZE + nTagData), uid);
  } else {
    if (pTagData != NULL) memcpy((*ppTagIdxKey)->data, pTagData, nTagData);
    taosSetInt64Aligned((tb_uid_t *)((*ppTagIdxKey)->data + nTagData), uid);
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
    taosHashCleanup(pColCmprObj);
    metaULock(pMeta);
    return TSDB_CODE_FAILED;
  }
  int64_t version = ((SUidIdxVal *)pData)[0].version;
  rc = tdbTbGet(pMeta->pTbDb, &(STbDbKey){.version = version, .uid = uid}, sizeof(STbDbKey), &pData, &nData);
  if (rc < 0) {
    metaULock(pMeta);
    taosHashCleanup(pColCmprObj);
    metaError("failed to get table entry");
    return rc;
  }

  tDecoderInit(&dc, pData, nData);
  rc = metaDecodeEntry(&dc, &e);
  if (rc < 0) {
    tDecoderClear(&dc);
    tdbFree(pData);
    metaULock(pMeta);
    taosHashCleanup(pColCmprObj);
    return rc;
  }
  if (withExtSchema(e.type)) {
    SColCmprWrapper *p = &e.colCmpr;
    for (int32_t i = 0; i < p->nCols; i++) {
      SColCmpr *pCmpr = &p->pColCmpr[i];
      rc = taosHashPut(pColCmprObj, &pCmpr->id, sizeof(pCmpr->id), &pCmpr->alg, sizeof(pCmpr->alg));
      if (rc < 0) {
        tDecoderClear(&dc);
        tdbFree(pData);
        metaULock(pMeta);
        taosHashCleanup(pColCmprObj);
        return rc;
      }
    }
  } else {
    tDecoderClear(&dc);
    tdbFree(pData);
    metaULock(pMeta);
    taosHashCleanup(pColCmprObj);
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
