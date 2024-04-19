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

extern int32_t metaGetTableEntryByUidImpl(SMeta *meta, int64_t uid, SMetaEntry **entry);
extern int32_t metaGetTableEntryByNameImpl(SMeta *meta, const char *name, SMetaEntry **entry);
extern int32_t metaEntryCloneDestroy(SMetaEntry *entry);

static int  metaSaveJsonVarToIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);
static int  metaDelJsonVarFromIdx(SMeta *pMeta, const SMetaEntry *pCtbEntry, const SSchema *pSchema);
static int  metaSaveToTbDb(SMeta *pMeta, const SMetaEntry *pME);
static int  metaUpdateUidIdx(SMeta *pMeta, const SMetaEntry *pME);
static int  metaUpdateTtl(SMeta *pMeta, const SMetaEntry *pME);
static int  metaUpdateChangeTime(SMeta *pMeta, tb_uid_t uid, int64_t changeTimeMs);
static int  metaDropTableByUid(SMeta *pMeta, tb_uid_t uid, int *type, tb_uid_t *pSuid, int8_t *pSysTbl);
static void metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey);
// opt ins_tables query
static int metaUpdateBtimeIdx(SMeta *pMeta, const SMetaEntry *pME);
static int metaDeleteBtimeIdx(SMeta *pMeta, const SMetaEntry *pME);
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

static inline void metaTimeSeriesNotifyCheck(SMeta *pMeta) {
#ifdef TD_ENTERPRISE
  int64_t nTimeSeries = metaGetTimeSeriesNum(pMeta, 0);
  int64_t deltaTS = nTimeSeries - pMeta->pVnode->config.vndStats.numOfReportedTimeSeries;
  if (deltaTS > tsTimeSeriesThreshold) {
    if (0 == atomic_val_compare_exchange_8(&dmNotifyHdl.state, 1, 2)) {
      tsem_post(&dmNotifyHdl.sem);
    }
  }
#endif
}

void metaDropTables(SMeta *pMeta, SArray *tbUids) {
  if (taosArrayGetSize(tbUids) == 0) return;

  int64_t    nCtbDropped = 0;
  SSHashObj *suidHash = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));

  metaWLock(pMeta);
  for (int i = 0; i < taosArrayGetSize(tbUids); ++i) {
    tb_uid_t uid = *(tb_uid_t *)taosArrayGet(tbUids, i);
    tb_uid_t suid = 0;
    int8_t   sysTbl = 0;
    int      type;
    metaDropTableByUid(pMeta, uid, &type, &suid, &sysTbl);
    if (!sysTbl && type == TSDB_CHILD_TABLE && suid != 0 && suidHash) {
      int64_t *pVal = tSimpleHashGet(suidHash, &suid, sizeof(tb_uid_t));
      if (pVal) {
        nCtbDropped = *pVal + 1;
      } else {
        nCtbDropped = 1;
      }
      tSimpleHashPut(suidHash, &suid, sizeof(tb_uid_t), &nCtbDropped, sizeof(int64_t));
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
    SVnodeStats *pStats = &pMeta->pVnode->config.vndStats;
    if (metaGetStbStats(pMeta->pVnode, *pSuid, NULL, &nCols) == 0) {
      pStats->numOfTimeSeries -= *(int64_t *)pCtbDropped * (nCols - 1);
    }
  }
  tSimpleHashCleanup(suidHash);

  pMeta->changed = true;
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
    metaDecodeEntry(&dc, &me);

    if (me.type != TSDB_SUPER_TABLE) {
      char tbFName[TSDB_TABLE_FNAME_LEN + 1];
      snprintf(tbFName, sizeof(tbFName), "%s.%s", pMeta->pVnode->config.dbname, me.name);
      tbFName[TSDB_TABLE_FNAME_LEN] = '\0';
      int32_t ret = vnodeValidateTableHash(pMeta->pVnode, tbFName);
      if (ret < 0 && terrno == TSDB_CODE_VND_HASH_MISMATCH) {
        taosArrayPush(uidList, &me.uid);
      }
    }
    tDecoderClear(&dc);
  }
  tdbFree(pData);
  tdbFree(pKey);
  tdbTbcClose(pCur);

  return 0;
}

int32_t metaTrimTables(SMeta *pMeta) {
  int32_t code = 0;

  SArray *tbUids = taosArrayInit(8, sizeof(int64_t));
  if (tbUids == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  code = metaFilterTableByHash(pMeta, tbUids);
  if (code != 0) {
    goto end;
  }
  if (TARRAY_SIZE(tbUids) == 0) {
    goto end;
  }

  metaInfo("vgId:%d, trim %ld tables", TD_VID(pMeta->pVnode), taosArrayGetSize(tbUids));
  metaDropTables(pMeta, tbUids);

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
    return -1;
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
    return -1;
  }
  return 0;
}

static int metaDeleteTtl(SMeta *pMeta, const SMetaEntry *pME) {
  if (pME->type != TSDB_CHILD_TABLE && pME->type != TSDB_NORMAL_TABLE) return 0;

  STtlDelTtlCtx ctx = {.uid = pME->uid, .pTxn = pMeta->txn};
  if (pME->type == TSDB_CHILD_TABLE) {
    ctx.ttlDays = pME->ctbEntry.ttlDays;
  } else {
    ctx.ttlDays = pME->ntbEntry.ttlDays;
  }

  return ttlMgrDeleteTtl(pMeta->pTtlMgr, &ctx);
}

static int metaDropTableByUid(SMeta *pMeta, tb_uid_t uid, int *type, tb_uid_t *pSuid, int8_t *pSysTbl) {
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
    if (pSuid) *pSuid = e.ctbEntry.suid;
    void *tData = NULL;
    int   tLen = 0;

    if (tdbTbGet(pMeta->pUidIdx, &e.ctbEntry.suid, sizeof(tb_uid_t), &tData, &tLen) == 0) {
      STbDbKey tbDbKey = {.uid = e.ctbEntry.suid, .version = ((SUidIdxVal *)tData)[0].version};
      if (tdbTbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &tData, &tLen) == 0) {
        SDecoder   tdc = {0};
        SMetaEntry stbEntry = {0};

        tDecoderInit(&tdc, tData, tLen);
        metaDecodeEntry(&tdc, &stbEntry);

        if (pSysTbl) *pSysTbl = metaTbInFilterCache(pMeta, stbEntry.name, 1) ? 1 : 0;

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
              tdbTbDelete(pMeta->pTagIdx, pTagIdxKey, nTagIdxKey, pMeta->txn);
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

  tdbTbDelete(pMeta->pTbDb, &(STbDbKey){.version = version, .uid = uid}, sizeof(STbDbKey), pMeta->txn);
  tdbTbDelete(pMeta->pNameIdx, e.name, strlen(e.name) + 1, pMeta->txn);
  tdbTbDelete(pMeta->pUidIdx, &uid, sizeof(uid), pMeta->txn);

  if (e.type == TSDB_CHILD_TABLE || e.type == TSDB_NORMAL_TABLE) metaDeleteBtimeIdx(pMeta, &e);
  if (e.type == TSDB_NORMAL_TABLE) metaDeleteNcolIdx(pMeta, &e);

  if (e.type != TSDB_SUPER_TABLE) metaDeleteTtl(pMeta, &e);

  if (e.type == TSDB_CHILD_TABLE) {
    tdbTbDelete(pMeta->pCtbIdx, &(SCtbIdxKey){.suid = e.ctbEntry.suid, .uid = uid}, sizeof(SCtbIdxKey), pMeta->txn);

    --pMeta->pVnode->config.vndStats.numOfCTables;

    metaUpdateStbStats(pMeta, e.ctbEntry.suid, -1, 0);
    metaUidCacheClear(pMeta, e.ctbEntry.suid);
    metaTbGroupCacheClear(pMeta, e.ctbEntry.suid);
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
static int metaUpdateBtimeIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SBtimeIdxKey btimeKey = {0};
  if (metaBuildBtimeIdxKey(&btimeKey, pME) < 0) {
    return 0;
  }
  metaTrace("vgId:%d, start to save version:%" PRId64 " uid:%" PRId64 " btime:%" PRId64, TD_VID(pMeta->pVnode),
            pME->version, pME->uid, btimeKey.btime);

  return tdbTbUpsert(pMeta->pBtimeIdx, &btimeKey, sizeof(btimeKey), NULL, 0, pMeta->txn);
}

static int metaDeleteBtimeIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SBtimeIdxKey btimeKey = {0};
  if (metaBuildBtimeIdxKey(&btimeKey, pME) < 0) {
    return 0;
  }
  return tdbTbDelete(pMeta->pBtimeIdx, &btimeKey, sizeof(btimeKey), pMeta->txn);
}
static int metaUpdateNcolIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SNcolIdxKey ncolKey = {0};
  if (metaBuildNColIdxKey(&ncolKey, pME) < 0) {
    return 0;
  }
  return tdbTbUpsert(pMeta->pNcolIdx, &ncolKey, sizeof(ncolKey), NULL, 0, pMeta->txn);
}

static int metaDeleteNcolIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SNcolIdxKey ncolKey = {0};
  if (metaBuildNColIdxKey(&ncolKey, pME) < 0) {
    return 0;
  }
  return tdbTbDelete(pMeta->pNcolIdx, &ncolKey, sizeof(ncolKey), pMeta->txn);
}

static int metaUpdateTableOptions(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (entry->type == TSDB_CHILD_TABLE) {
    // if (request->updateTTL) {
    //   metaDeleteTtl(meta, entry);
    //   entry->ctbEntry.ttlDays = request->newTTL;
    //   metaUpdateTtl(meta, entry);
    // }
    // if (request->newCommentLen >= 0) {
    //   entry->ctbEntry.commentLen = request->newCommentLen;
    //   entry->ctbEntry.comment = request->newComment;
    // }
  } else {
    // if (request->updateTTL) {
    //   metaDeleteTtl(meta, entry);
    //   entry->ntbEntry.ttlDays = request->newTTL;
    //   metaUpdateTtl(meta, entry);
    // }
    // if (request->newCommentLen >= 0) {
    //   entry->ntbEntry.commentLen = request->newCommentLen;
    //   entry->ntbEntry.comment = request->newComment;
    // }
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
#if 0
  metaUpdateChangeTime(pMeta, entry.uid, pAlterTbReq->ctimeMs);
#endif
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

static int metaUpdateTtl(SMeta *pMeta, const SMetaEntry *pME) {
  if (pME->type != TSDB_CHILD_TABLE && pME->type != TSDB_NORMAL_TABLE) return 0;

  STtlUpdTtlCtx ctx = {.uid = pME->uid, .pTxn = pMeta->txn};
  if (pME->type == TSDB_CHILD_TABLE) {
    ctx.ttlDays = pME->ctbEntry.ttlDays;
    ctx.changeTimeMs = pME->ctbEntry.btime;
  } else {
    ctx.ttlDays = pME->ntbEntry.ttlDays;
    ctx.changeTimeMs = pME->ntbEntry.btime;
  }

  return ttlMgrInsertTtl(pMeta->pTtlMgr, &ctx);
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

void *metaGetIdx(SMeta *pMeta) { return pMeta->pTagIdx; }
void *metaGetIvtIdx(SMeta *pMeta) { return pMeta->pTagIvtIdx; }

static int32_t metaValidateCreateTableRequest(SMeta *meta, SVCreateTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (request->type != TSDB_CHILD_TABLE && request->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (request->type == TSDB_CHILD_TABLE) {
    if (tdbTbGet(meta->pNameIdx, request->ctb.stbName, strlen(request->ctb.stbName) + 1, &value, &valueSize) != 0 ||
        *(int64_t *)value != request->ctb.suid) {
    }
    TSDB_CHECK_CODE(code = TSDB_CODE_PAR_TABLE_NOT_EXIST, lino, _exit);
  }

  if (tdbTbGet(meta->pNameIdx, request->name, strlen(request->name) + 1, &value, &valueSize) == 0) {
    // TODO
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_TABLE_ALREADY_EXIST, lino, _exit);
  }

  //   // validate req
  //   metaReaderDoInit(&mr, meta, META_READER_LOCK);
  //   if (metaGetTableEntryByName(&mr, request->name) == 0) {
  //     if (request->type == TSDB_CHILD_TABLE && request->ctb.suid != mr.me.ctbEntry.suid) {
  //       terrno = TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE;
  //       metaReaderClear(&mr);
  //       return -1;
  //     }
  //     request->uid = mr.me.uid;
  //     if (request->type == TSDB_CHILD_TABLE) {
  //       request->ctb.suid = mr.me.ctbEntry.suid;
  //     }
  //     terrno = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  //     metaReaderClear(&mr);
  //     return -1;
  //   } else if (terrno == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
  //     terrno = TSDB_CODE_SUCCESS;
  //   }
  //   metaReaderClear(&mr);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  tdbFree(value);
  return code;
}

static int32_t metaCreateNormalTable(SMeta *meta, int64_t version, SVCreateTbReq *request, STableMetaRsp **response) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry entry = {
      .version = version,
      .type = TSDB_NORMAL_TABLE,
      .uid = request->uid,
      .name = request->name,
      .ntbEntry.btime = request->btime,
      .ntbEntry.ttlDays = request->ttl,
      .ntbEntry.commentLen = request->commentLen,
      .ntbEntry.comment = request->comment,
      .ntbEntry.schemaRow = request->ntb.schemaRow,
      .ntbEntry.ncid = request->ntb.schemaRow.pSchema[request->ntb.schemaRow.nCols - 1].colId + 1,
  };
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (response) {
    if ((*response = taosMemoryCalloc(1, sizeof(STableMetaRsp))) == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    }
    metaUpdateMetaRsp(request->uid, request->name, &request->ntb.schemaRow, *response);
  }
  // if (!TSDB_CACHE_NO(meta->pVnode->config)) {
  //   tsdbCacheNewTable(meta->pVnode->pTsdb, me.uid, -1, &me.ntbEntry.schemaRow);
  // // }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaCreateChildTable(SMeta *meta, int64_t version, SVCreateTbReq *request, STableMetaRsp **response) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry entry = {
      .version = version,
      .type = TSDB_CHILD_TABLE,
      .uid = request->uid,
      .name = request->name,
      .ctbEntry.btime = request->btime,
      .ctbEntry.ttlDays = request->ttl,
      .ctbEntry.commentLen = request->commentLen,
      .ctbEntry.comment = request->comment,
      .ctbEntry.suid = request->ctb.suid,
      .ctbEntry.pTags = request->ctb.pTag,
  };
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (response) {
    if ((*response = taosMemoryCalloc(1, sizeof(STableMetaRsp))) == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    }
    (*response)->tableType = TSDB_CHILD_TABLE;
    (*response)->tuid = request->uid;
    (*response)->suid = request->ctb.suid;
    strcpy((*response)->tbName, request->name);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;

#if 0
    if (!sysTbl) {
      int32_t nCols = 0;
      metaGetStbStats(meta->pVnode, me.ctbEntry.suid, 0, &nCols);
      pStats->numOfTimeSeries += nCols - 1;
    }

    metaWLock(meta);
    metaUpdateStbStats(meta, me.ctbEntry.suid, 1, 0);
    metaUidCacheClear(meta, me.ctbEntry.suid);
    metaTbGroupCacheClear(meta, me.ctbEntry.suid);
    metaULock(meta);

    if (!TSDB_CACHE_NO(meta->pVnode->config)) {
      tsdbCacheNewTable(meta->pVnode->pTsdb, me.uid, me.ctbEntry.suid, NULL);
    }
#endif
}

static int32_t metaValidateCreateSuperTableRequest(SMeta *meta, SVCreateStbReq *request, bool *isExist) {
  int32_t   code = 0;
  int32_t   lino = 0;
  void     *value = NULL;
  int32_t   valueSize = 0;
  SMetaInfo info;

  if (request->name == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (tdbTbGet(meta->pNameIdx, request->name, strlen(request->name) + 1, &value, &valueSize) == 0) {
    code = metaGetInfo(meta, *(int64_t *)value, &info, NULL);
    ASSERT(code == 0);
    if (info.uid == info.suid) {
      *isExist = true;
    } else {
      TSDB_CHECK_CODE(code = TSDB_CODE_TDB_TABLE_ALREADY_EXIST, lino, _exit);
    }
  }

_exit:
  tdbFree(value);
  return code;
}

int32_t metaCreateSuperTable(SMeta *meta, int64_t version, SVCreateStbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    isExist = false;

  // validate request
  code = metaValidateCreateSuperTableRequest(meta, request, &isExist);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (isExist) {
    return (terrno = code);
  }

  // create super table
  SMetaEntry entry = {
      .version = version,
      .type = TSDB_SUPER_TABLE,
      .uid = request->suid,
      .name = request->name,
      .stbEntry.schemaRow = request->schemaRow,
      .stbEntry.schemaTag = request->schemaTag,
  };
  if (request->rollup) {
    TABLE_SET_ROLLUP(entry.flags);
    entry.stbEntry.rsmaParam = request->rsmaParam;
  }
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code == 0) {
    metaInfo("vgId:%d, super table is created, name:%s uid:%" PRId64 " version:%" PRId64, TD_VID(meta->pVnode),
             request->name, request->suid, version);
  } else if (code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
    metaTrace("vgId:%d, failed to create super table at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->suid, version);
  } else {
    metaError("vgId:%d, failed to create super table at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->suid, version);
  }
  return (terrno = code);
}

int32_t metaCreateTable(SMeta *meta, int64_t version, SVCreateTbReq *request, STableMetaRsp **response) {
  int32_t code = 0;
  int32_t lino = 0;

  code = metaValidateCreateTableRequest(meta, request);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (request->type == TSDB_CHILD_TABLE) {
    code = metaCreateChildTable(meta, version, request, response);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = metaCreateNormalTable(meta, version, request, response);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaInfo("vgId:%d create table failed at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64 " type:%d",
             TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->uid, version, request->type);
  } else {
    metaInfo("vgId:%d create table success, name:%s uid:%" PRId64 " version:%" PRId64 " type:%d", TD_VID(meta->pVnode),
             request->name, request->uid, version, request->type);
  }
  return (terrno = code);

#if 0
  bool sysTbl = (request->type == TSDB_CHILD_TABLE) && metaTbInFilterCache(meta, request->ctb.stbName, 1);
  metaTimeSeriesNotifyCheck(meta);
#endif
}

static int32_t metaValidateDropTableReq(SMeta *meta, SVDropTbReq *request, SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (tdbTbGet(meta->pNameIdx, request->name, strlen(request->name) + 1, &value, &valueSize) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_TABLE_NOT_EXIST, lino, _exit);
  }

  entry->uid = *(tb_uid_t *)value;

  if (tdbTbGet(meta->pUidIdx, &entry->uid, sizeof(tb_uid_t), &value, &valueSize) != 0) {
    ASSERT(0);
  }

  SUidIdxVal *uidIdxVal = (SUidIdxVal *)value;
  if (uidIdxVal->suid == 0) {
    entry->type = TSDB_NORMAL_TABLE;
  } else if (uidIdxVal->suid == entry->uid) {
    entry->type = TSDB_SUPER_TABLE;
  } else {
    entry->type = TSDB_CHILD_TABLE;
  }

_exit:
  tdbFree(value);
  return code;
}

int32_t metaDropTable(SMeta *meta, int64_t version, SVDropTbReq *request, SArray *tbUids, tb_uid_t *tbUid) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry entry = {
      .version = version,
  };

  // validate
  code = metaValidateDropTableReq(meta, request, &entry);
  if (code == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
    return (terrno = code);
  } else {
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // do drop
  entry.type = -entry.type;
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d drop table failed at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, entry.uid, version);
  } else {
    metaInfo("vgId:%d drop table success, name:%s uid:%" PRId64 " version:%" PRId64, TD_VID(meta->pVnode),
             request->name, entry.uid, version);
  }
  return (terrno = code);

#if 0
  metaWLock(meta);
  rc = metaDropTableByUid(meta, uid, &type, &suid, &sysTbl);
  metaULock(meta);

  if (rc < 0) goto _exit;

  if (!sysTbl && type == TSDB_CHILD_TABLE) {
    int32_t      nCols = 0;
    SVnodeStats *pStats = &meta->pVnode->config.vndStats;
    if (metaGetStbStats(meta->pVnode, suid, NULL, &nCols) == 0) {
      pStats->numOfTimeSeries -= nCols - 1;
    }
  }

  if ((type == TSDB_CHILD_TABLE || type == TSDB_NORMAL_TABLE) && tbUids) {
    taosArrayPush(tbUids, &uid);

    if (!TSDB_CACHE_NO(meta->pVnode->config)) {
      tsdbCacheDropTable(meta->pVnode->pTsdb, uid, suid, NULL);
    }
  }

  if ((type == TSDB_CHILD_TABLE) && tbUid) {
    *tbUid = uid;
  }
#endif
}

static int32_t metaValidateDropSuperTableReq(SMeta *meta, SVDropStbReq *request, SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (tdbTbGet(meta->pNameIdx, request->name, strlen(request->name) + 1, &value, &valueSize) != 0 ||
      *(tb_uid_t *)value != request->suid) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_STB_NOT_EXIST, lino, _exit);
  }

_exit:
  return code;
}

int32_t metaDropSuperTable(SMeta *meta, int64_t verison, SVDropStbReq *request, SArray *tbUidList) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SMetaEntry entry = {
      .version = verison,
  };

  // validate
  code = metaValidateDropSuperTableReq(meta, request, &entry);
  if (code == TSDB_CODE_TDB_STB_NOT_EXIST) {
    return (terrno = code);
  }
  TSDB_CHECK_CODE(code, lino, _exit);

  // handle
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d drop super table failed at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->suid, verison);
  } else {
  }
  return (terrno = code);
#if 0
  (void)tsdbCacheDropSubTables(meta->pVnode->pTsdb, tbUidList, request->suid);

_drop_super_table:
  metaStatsCacheDrop(meta, request->suid);
  metaUpdTimeSeriesNum(meta);
#endif
}

static int32_t metaValidateAlterSuperTableReq(SMeta *meta, SVCreateStbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry *superTableEntry = NULL;

  if (request->name == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  code = metaGetTableEntryByNameImpl(meta, request->name, &superTableEntry);
  if (code == TSDB_CODE_NOT_FOUND) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_STB_NOT_EXIST, lino, _exit);
  }

  if (superTableEntry->type != TSDB_SUPER_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_STB_NOT_EXIST, lino, _exit);
  }

  if (request->suid != superTableEntry->uid) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  if (request->schemaRow.version < superTableEntry->stbEntry.schemaRow.version) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (request->schemaTag.version < superTableEntry->stbEntry.schemaTag.version) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (request->schemaRow.version == superTableEntry->stbEntry.schemaRow.version &&
      request->schemaTag.version == superTableEntry->stbEntry.schemaTag.version) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryCloneDestroy(superTableEntry);
  return code;
}

int32_t metaAlterSuperTable(SMeta *meta, int64_t version, SVCreateStbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  // validate
  code = metaValidateAlterSuperTableReq(meta, request);
  TSDB_CHECK_CODE(code, lino, _exit);

  // handle
  SMetaEntry entry = {
      .version = version,
      .type = TSDB_SUPER_TABLE,
      .uid = request->suid,
      .name = request->name,
      .stbEntry.schemaRow = request->schemaRow,
      .stbEntry.schemaTag = request->schemaTag,

  };
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d alter super table failed at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->suid, version);
  } else {
    metaInfo("vgId:%d alter super table success, name:%s uid:%" PRId64 " version:%" PRId64, TD_VID(meta->pVnode),
             request->name, request->suid, version);
  }
  return (terrno = code);
}

static int32_t metaAddTableColumn(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (request->colName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (entry->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  SSchemaWrapper *schema = &entry->ntbEntry.schemaRow;
  int32_t         rowBytes = 0;
  for (int32_t i = 0; i < schema->nCols; i++) {
    if (strcmp(schema->pSchema[i].name, request->colName) == 0) {
      TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_ALREADY_EXISTS, lino, _exit);
    }
    rowBytes += schema->pSchema[i].bytes;
  }

  code = grantCheck(TSDB_GRANT_TIMESERIES);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (rowBytes + request->bytes > TSDB_MAX_BYTES_PER_ROW) {
    TSDB_CHECK_CODE(code = TSDB_CODE_PAR_INVALID_ROW_LENGTH, lino, _exit);
  }

  schema->version++;
  schema->nCols++;
  SSchema *newSchema = taosMemoryRealloc(schema->pSchema, sizeof(SSchema) * schema->nCols);
  if (newSchema == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }
  schema->pSchema = newSchema;
  schema->pSchema[schema->nCols - 1] = (SSchema){
      .type = request->type,
      .flags = request->flags,
      .colId = entry->ntbEntry.ncid++,
      .bytes = request->bytes,
  };
  strcpy(schema->pSchema[schema->nCols - 1].name, request->colName);

#if 0
  ++pMeta->pVnode->config.vndStats.numOfNTimeSeries;
  metaTimeSeriesNotifyCheck(pMeta);
  if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
    int16_t cid = pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].colId;
    int8_t  col_type = pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].type;
    (void)tsdbCacheNewNTableColumn(pMeta->pVnode->pTsdb, entry.uid, cid, col_type);
  }
#endif

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropTableColumn(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (request->colName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (entry->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  SSchemaWrapper *schema = &entry->ntbEntry.schemaRow;
  int32_t         i = 0;
  for (; i < schema->nCols; i++) {
    if (strcmp(schema->pSchema[i].name, request->colName) == 0) {
      break;
    }
  }

  if (i == schema->nCols) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_NOT_EXISTS, lino, _exit);
  }

  if (schema->pSchema[i].colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  code = tqCheckColModifiable(meta->pVnode->pTq, entry->uid, schema->pSchema[i].colId);
  TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_SUBSCRIBED, lino, _exit);

  schema->version++;
  memmove(schema->pSchema + i, schema->pSchema + i + 1, (schema->nCols - i - 1) * sizeof(SSchema));
  schema->nCols--;

#if 0
  --pMeta->pVnode->config.vndStats.numOfNTimeSeries;

  if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
    int16_t cid = pColumn->colId;
    int8_t  col_type = pColumn->type;

    (void)tsdbCacheDropNTableColumn(pMeta->pVnode->pTsdb, entry.uid, cid, col_type);
  }
#endif

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaUpdateTableColumnBytes(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (request->colName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (entry->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  SSchemaWrapper *schema = &entry->ntbEntry.schemaRow;
  int32_t         i = -1;
  int32_t         rowBytes = 0;
  for (int32_t j = 0; j < schema->nCols; j++) {
    if (strcmp(schema->pSchema[j].name, request->colName) == 0) {
      i = j;
    }
    rowBytes += schema->pSchema[j].bytes;
  }

  if (i == -1) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_NOT_EXISTS, lino, _exit);
  }

  if (!IS_VAR_DATA_TYPE(schema->pSchema[i].type) || schema->pSchema[i].bytes >= request->colModBytes) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  if (rowBytes + request->colModBytes - schema->pSchema[i].bytes > TSDB_MAX_BYTES_PER_ROW) {
    TSDB_CHECK_CODE(code = TSDB_CODE_PAR_INVALID_ROW_LENGTH, lino, _exit);
  }

  code = tqCheckColModifiable(meta->pVnode->pTq, entry->uid, schema->pSchema[i].colId);
  TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_SUBSCRIBED, lino, _exit);

  schema->version++;
  schema->pSchema[i].bytes = request->colModBytes;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaUpdateTableColumnName(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (request->colName == NULL || request->colNewName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (entry->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  SSchemaWrapper *schema = &entry->ntbEntry.schemaRow;
  int32_t         i = 0;
  for (; i < schema->nCols; i++) {
    if (strcmp(schema->pSchema[i].name, request->colName) == 0) {
      break;
    }
  }

  if (i >= schema->nCols) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_NOT_EXISTS, lino, _exit);
  }
  code = tqCheckColModifiable(meta->pVnode->pTq, entry->uid, schema->pSchema[i].colId);
  TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_SUBSCRIBED, lino, _exit);

  schema->version++;
  strncpy(schema->pSchema[i].name, request->colNewName, TSDB_COL_NAME_LEN - 1);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaUpdateTableTagValue(SMeta *meta, SMetaEntry *childTableEntry, SVAlterTbReq *request) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaEntry *superTableEntry = NULL;
  SArray     *tagValArray = NULL;

  if (request->tagName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (childTableEntry->type != TSDB_CHILD_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  code = metaGetTableEntryByUidImpl(meta, childTableEntry->ctbEntry.suid, &superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  SSchemaWrapper *tagSchema = &superTableEntry->stbEntry.schemaTag;
  int32_t         i = 0;
  for (; i < tagSchema->nCols && strcmp(tagSchema->pSchema[i].name, request->tagName) != 0; i++) {
  }

  if (i >= tagSchema->nCols) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_NOT_EXISTS, lino, _exit);
  }

  if (tagSchema->nCols == 1 && tagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    STag *newTag = taosMemoryRealloc(childTableEntry->ctbEntry.pTags, request->nTagVal);
    if (newTag == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    }
    memcpy(newTag, request->pTagVal, request->nTagVal);
    childTableEntry->ctbEntry.pTags = newTag;
  } else {
    if ((tagValArray = taosArrayInit(tagSchema->nCols, sizeof(STagVal))) == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    }

    // generate tag value array
    for (int32_t j = 0; j < tagSchema->nCols; j++) {
      STagVal tagValue = {
          .cid = tagSchema->pSchema[j].colId,
      };

      if (i == j) {
        if (!request->isNull) {
          tagValue.type = tagSchema->pSchema[j].type;
          if (IS_VAR_DATA_TYPE(tagSchema->pSchema[j].type)) {
            tagValue.pData = request->pTagVal;
            tagValue.nData = request->nTagVal;
          } else {
            memcpy(&tagValue.i64, request->pTagVal, request->nTagVal);
          }
          taosArrayPush(tagValArray, &tagValue);
        }
      } else {
        if (tTagGet(childTableEntry->ctbEntry.pTags, &tagValue)) {
          taosArrayPush(tagValArray, &tagValue);
        }
      }

      // generate new tag
      void *newTag = NULL;
      code = tTagNew(tagValArray, tagSchema->version, false, (STag **)&newTag);
      TSDB_CHECK_CODE(code, lino, _exit);

      taosMemoryFree(childTableEntry->ctbEntry.pTags);
      childTableEntry->ctbEntry.pTags = newTag;
    }
  }

#if 0
  metaWLock(pMeta);
  metaUidCacheClear(pMeta, ctbEntry.ctbEntry.suid);
  metaTbGroupCacheClear(pMeta, ctbEntry.ctbEntry.suid);
  metaUpdateChangeTime(pMeta, ctbEntry.uid, pAlterTbReq->ctimeMs);
  metaULock(pMeta);
#endif

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryCloneDestroy(superTableEntry);
  taosArrayDestroy(tagValArray);
  return code;
}

static int32_t metaValidateAlterTableReq(SMeta *meta, SVAlterTbReq *request, SMetaEntry **entry) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (request->tbName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  code = metaGetTableEntryByNameImpl(meta, request->tbName, entry);
  if (code == TSDB_CODE_NOT_FOUND) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_TABLE_NOT_EXIST, lino, _exit);
  }
  TSDB_CHECK_CODE(code, lino, _exit);

  if (request->action == TSDB_ALTER_TABLE_ADD_COLUMN) {
    // TSDB_NORMAL_TABLE
    code = metaAddTableColumn(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_DROP_COLUMN) {
    // TSDB_NORMAL_TABLE
    code = metaDropTableColumn(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES) {
    // TSDB_NORMAL_TABLE
    code = metaUpdateTableColumnBytes(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME) {
    // TSDB_NORMAL_TABLE
    code = metaUpdateTableColumnName(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    // TSDB_CHILD_TABLE
    code = metaUpdateTableTagValue(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_UPDATE_OPTIONS) {
    code = metaUpdateTableOptions(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  tdbFree(value);
  return code;
}

int32_t metaAlterTable(SMeta *meta, int64_t version, SVAlterTbReq *request, STableMetaRsp *response) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaEntry *entry = NULL;

  // validate
  code = metaValidateAlterTableReq(meta, request, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  ASSERT(version > entry->version);

  // do alter table
  entry->version = version;
  code = metaHandleEntry(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d alter table failed at line %d since %s", TD_VID(meta->pVnode), lino, tstrerror(code));
  } else {
    metaInfo("vgId:%d alter table success", TD_VID(meta->pVnode));
  }
  metaEntryCloneDestroy(entry);
  return (terrno = code);
}

int32_t metaDropIndexFromSTable(SMeta *pMeta, int64_t version, SDropIndexReq *request) {
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

  tb_uid_t suid = request->stbUid;

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
    if (0 == strncmp(schema->name, request->colName, sizeof(request->colName))) {
      if (IS_IDX_ON(schema)) {
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
    if (tTagGet((const STag *)pVal, &tagVal)) {
      if (IS_VAR_DATA_TYPE(pCol->type)) {
        pTagData = tagVal.pData;
        nTagData = (int32_t)tagVal.nData;
      } else {
        pTagData = &(tagVal.i64);
        nTagData = tDataTypes[pCol->type].bytes;
      }
    } else {
      if (!IS_VAR_DATA_TYPE(pCol->type)) {
        nTagData = tDataTypes[pCol->type].bytes;
      }
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
    pTagIdxKey = NULL;
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
