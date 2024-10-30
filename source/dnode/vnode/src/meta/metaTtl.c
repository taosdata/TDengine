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

#include "metaTtl.h"
#include "meta.h"

typedef struct {
  TTB   *pNewTtlIdx;
  SMeta *pMeta;
} SConvertData;

typedef struct {
  int32_t      ttlDropMaxCount;
  int32_t      count;
  STtlIdxKeyV1 expiredKey;
  SArray      *pTbUids;
} STtlExpiredCtx;

static void ttlMgrCleanup(STtlManger *pTtlMgr);

static int ttlMgrConvert(TTB *pOldTtlIdx, TTB *pNewTtlIdx, void *pMeta);

static void    ttlMgrBuildKey(STtlIdxKeyV1 *pTtlKey, int64_t ttlDays, int64_t changeTimeMs, tb_uid_t uid);
static int     ttlIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int     ttlIdxKeyV1Cmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int     ttlMgrFillCache(STtlManger *pTtlMgr);
static int32_t ttlMgrFillCacheOneEntry(const void *pKey, int keyLen, const void *pVal, int valLen, void *pTtlCache);
static int32_t ttlMgrConvertOneEntry(const void *pKey, int keyLen, const void *pVal, int valLen, void *pConvertData);
static int32_t ttlMgrFindExpiredOneEntry(const void *pKey, int keyLen, const void *pVal, int valLen,
                                         void *pExpiredInfo);

static bool ttlMgrNeedFlush(STtlManger *pTtlMgr);

const char *ttlTbname = "ttl.idx";
const char *ttlV1Tbname = "ttlv1.idx";

int32_t ttlMgrOpen(STtlManger **ppTtlMgr, TDB *pEnv, int8_t rollback, const char *logPrefix, int32_t flushThreshold) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t startNs = taosGetTimestampNs();

  *ppTtlMgr = NULL;

  STtlManger *pTtlMgr = (STtlManger *)tdbOsCalloc(1, sizeof(*pTtlMgr));
  if (pTtlMgr == NULL) TAOS_RETURN(terrno);

  char *logBuffer = (char *)tdbOsCalloc(1, strlen(logPrefix) + 1);
  if (logBuffer == NULL) {
    tdbOsFree(pTtlMgr);
    TAOS_RETURN(terrno);
  }
  (void)strcpy(logBuffer, logPrefix);
  pTtlMgr->logPrefix = logBuffer;
  pTtlMgr->flushThreshold = flushThreshold;

  code = tdbTbOpen(ttlV1Tbname, TDB_VARIANT_LEN, TDB_VARIANT_LEN, ttlIdxKeyV1Cmpr, pEnv, &pTtlMgr->pTtlIdx, rollback);
  if (TSDB_CODE_SUCCESS != code) {
    metaError("%s, failed to open %s since %s", pTtlMgr->logPrefix, ttlV1Tbname, tstrerror(code));
    tdbOsFree(pTtlMgr);
    TAOS_RETURN(code);
  }

  pTtlMgr->pTtlCache = taosHashInit(8192, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  pTtlMgr->pDirtyUids = taosHashInit(8192, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);

  if ((code = ttlMgrFillCache(pTtlMgr)) != TSDB_CODE_SUCCESS) {
    metaError("%s, failed to fill hash since %s", pTtlMgr->logPrefix, tstrerror(terrno));
    ttlMgrCleanup(pTtlMgr);
    TAOS_RETURN(code);
  }

  int64_t endNs = taosGetTimestampNs();
  metaInfo("%s, ttl mgr open end, hash size: %d, time consumed: %" PRId64 " ns", pTtlMgr->logPrefix,
           taosHashGetSize(pTtlMgr->pTtlCache), endNs - startNs);

  *ppTtlMgr = pTtlMgr;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

void ttlMgrClose(STtlManger *pTtlMgr) { ttlMgrCleanup(pTtlMgr); }

bool ttlMgrNeedUpgrade(TDB *pEnv) {
  bool needUpgrade = tdbTbExist(ttlTbname, pEnv);
  if (needUpgrade) {
    metaInfo("find ttl idx in old version , will convert");
  }
  return needUpgrade;
}

int32_t ttlMgrUpgrade(STtlManger *pTtlMgr, void *pMeta) {
  SMeta  *meta = (SMeta *)pMeta;
  int32_t code = TSDB_CODE_SUCCESS;

  if (!tdbTbExist(ttlTbname, meta->pEnv)) TAOS_RETURN(TSDB_CODE_SUCCESS);

  metaInfo("%s, ttl mgr start upgrade", pTtlMgr->logPrefix);

  int64_t startNs = taosGetTimestampNs();

  code = tdbTbOpen(ttlTbname, sizeof(STtlIdxKey), 0, ttlIdxKeyCmpr, meta->pEnv, &pTtlMgr->pOldTtlIdx, 0);
  if (TSDB_CODE_SUCCESS != code) {
    metaError("%s, failed to open %s index since %s", pTtlMgr->logPrefix, ttlTbname, tstrerror(code));
    goto _out;
  }

  if ((code = ttlMgrConvert(pTtlMgr->pOldTtlIdx, pTtlMgr->pTtlIdx, pMeta)) != TSDB_CODE_SUCCESS) {
    metaError("%s, failed to convert ttl index since %s", pTtlMgr->logPrefix, tstrerror(code));
    goto _out;
  }

  if ((code = tdbTbDropByName(ttlTbname, meta->pEnv, meta->txn)) != TSDB_CODE_SUCCESS) {
    metaError("%s, failed to drop old ttl index since %s", pTtlMgr->logPrefix, tstrerror(code));
    goto _out;
  }

  if ((code = ttlMgrFillCache(pTtlMgr)) != TSDB_CODE_SUCCESS) {
    metaError("%s, failed to fill hash since %s", pTtlMgr->logPrefix, tstrerror(code));
    goto _out;
  }

  int64_t endNs = taosGetTimestampNs();
  metaInfo("%s, ttl mgr upgrade end, hash size: %d, time consumed: %" PRId64 " ns", pTtlMgr->logPrefix,
           taosHashGetSize(pTtlMgr->pTtlCache), endNs - startNs);

_out:
  tdbTbClose(pTtlMgr->pOldTtlIdx);
  pTtlMgr->pOldTtlIdx = NULL;

  TAOS_RETURN(code);
}

static void ttlMgrCleanup(STtlManger *pTtlMgr) {
  taosMemoryFree(pTtlMgr->logPrefix);
  taosHashCleanup(pTtlMgr->pTtlCache);
  taosHashCleanup(pTtlMgr->pDirtyUids);
  tdbTbClose(pTtlMgr->pTtlIdx);
  taosMemoryFree(pTtlMgr);
}

static void ttlMgrBuildKey(STtlIdxKeyV1 *pTtlKey, int64_t ttlDays, int64_t changeTimeMs, tb_uid_t uid) {
  if (ttlDays <= 0) return;

  pTtlKey->deleteTimeMs = changeTimeMs + ttlDays * tsTtlUnit * 1000;
  pTtlKey->uid = uid;
}

static int ttlIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  STtlIdxKey *pTtlIdxKey1 = (STtlIdxKey *)pKey1;
  STtlIdxKey *pTtlIdxKey2 = (STtlIdxKey *)pKey2;

  if (pTtlIdxKey1->deleteTimeSec > pTtlIdxKey2->deleteTimeSec) {
    return 1;
  } else if (pTtlIdxKey1->deleteTimeSec < pTtlIdxKey2->deleteTimeSec) {
    return -1;
  }

  if (pTtlIdxKey1->uid > pTtlIdxKey2->uid) {
    return 1;
  } else if (pTtlIdxKey1->uid < pTtlIdxKey2->uid) {
    return -1;
  }

  return 0;
}

static int ttlIdxKeyV1Cmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  STtlIdxKeyV1 *pTtlIdxKey1 = (STtlIdxKeyV1 *)pKey1;
  STtlIdxKeyV1 *pTtlIdxKey2 = (STtlIdxKeyV1 *)pKey2;

  if (pTtlIdxKey1->deleteTimeMs > pTtlIdxKey2->deleteTimeMs) {
    return 1;
  } else if (pTtlIdxKey1->deleteTimeMs < pTtlIdxKey2->deleteTimeMs) {
    return -1;
  }

  if (pTtlIdxKey1->uid > pTtlIdxKey2->uid) {
    return 1;
  } else if (pTtlIdxKey1->uid < pTtlIdxKey2->uid) {
    return -1;
  }

  return 0;
}

static int ttlMgrFillCache(STtlManger *pTtlMgr) {
  return tdbTbTraversal(pTtlMgr->pTtlIdx, pTtlMgr->pTtlCache, ttlMgrFillCacheOneEntry);
}

static int32_t ttlMgrFillCacheOneEntry(const void *pKey, int keyLen, const void *pVal, int valLen, void *pTtlCache) {
  SHashObj *pCache = (SHashObj *)pTtlCache;

  STtlIdxKeyV1 *ttlKey = (STtlIdxKeyV1 *)pKey;
  tb_uid_t      uid = ttlKey->uid;
  int64_t       ttlDays = *(int64_t *)pVal;
  int64_t       changeTimeMs = ttlKey->deleteTimeMs - ttlDays * tsTtlUnit * 1000;

  STtlCacheEntry data = {
      .ttlDays = ttlDays, .changeTimeMs = changeTimeMs, .ttlDaysDirty = ttlDays, .changeTimeMsDirty = changeTimeMs};

  return taosHashPut(pCache, &uid, sizeof(uid), &data, sizeof(data));
}

static int32_t ttlMgrConvertOneEntry(const void *pKey, int keyLen, const void *pVal, int valLen, void *pConvertData) {
  SConvertData *pData = (SConvertData *)pConvertData;

  STtlIdxKey *ttlKey = (STtlIdxKey *)pKey;
  tb_uid_t    uid = ttlKey->uid;
  int64_t     ttlDays = 0;

  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = metaGetTableTtlByUid(pData->pMeta, uid, &ttlDays)) != TSDB_CODE_SUCCESS) {
    metaError("ttlMgr convert failed to get ttl since %s", tstrerror(code));
    goto _out;
  }

  STtlIdxKeyV1 ttlKeyV1 = {.deleteTimeMs = ttlKey->deleteTimeSec * 1000, .uid = uid};
  code = tdbTbUpsert(pData->pNewTtlIdx, &ttlKeyV1, sizeof(ttlKeyV1), &ttlDays, sizeof(ttlDays), pData->pMeta->txn);
  if (code != TSDB_CODE_SUCCESS) {
    metaError("ttlMgr convert failed to upsert since %s", tstrerror(code));
    goto _out;
  }

  code = TSDB_CODE_SUCCESS;

_out:
  TAOS_RETURN(code);
}

static int32_t ttlMgrFindExpiredOneEntry(const void *pKey, int keyLen, const void *pVal, int valLen,
                                         void *pExpiredCtx) {
  STtlExpiredCtx *pCtx = (STtlExpiredCtx *)pExpiredCtx;
  if (pCtx->count >= pCtx->ttlDropMaxCount) return -1;

  int c = ttlIdxKeyV1Cmpr(&pCtx->expiredKey, sizeof(pCtx->expiredKey), pKey, keyLen);
  if (c > 0) {
    if (NULL == taosArrayPush(pCtx->pTbUids, &((STtlIdxKeyV1 *)pKey)->uid)) {
      metaError("ttlMgr find expired failed since %s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      return -1;
    }
    pCtx->count++;
  }

  return c;
}

// static int32_t ttlMgrDumpOneEntry(const void *pKey, int keyLen, const void *pVal, int valLen, void *pDumpCtx) {
//   STtlIdxKeyV1 *ttlKey = (STtlIdxKeyV1 *)pKey;
//   int64_t      *ttlDays = (int64_t *)pVal;

//   metaInfo("ttlMgr dump, ttl: %" PRId64 ", ctime: %" PRId64 ", uid: %" PRId64, *ttlDays, ttlKey->deleteTimeMs,
//            ttlKey->uid);

//   TAOS_RETURN(TSDB_CODE_SUCCESS);
// }

static int ttlMgrConvert(TTB *pOldTtlIdx, TTB *pNewTtlIdx, void *pMeta) {
  SMeta *meta = pMeta;

  metaInfo("ttlMgr convert start.");

  SConvertData cvData = {.pNewTtlIdx = pNewTtlIdx, .pMeta = meta};

  int code = TSDB_CODE_SUCCESS;
  if ((code = tdbTbTraversal(pOldTtlIdx, &cvData, ttlMgrConvertOneEntry)) != TSDB_CODE_SUCCESS) {
    metaError("failed to convert since %s", tstrerror(code));
  }

  metaInfo("ttlMgr convert end.");
  TAOS_RETURN(code);
}

int32_t ttlMgrInsertTtl(STtlManger *pTtlMgr, const STtlUpdTtlCtx *updCtx) {
  if (updCtx->ttlDays == 0) return 0;

  STtlCacheEntry cacheEntry = {.ttlDays = updCtx->ttlDays,
                               .changeTimeMs = updCtx->changeTimeMs,
                               .ttlDaysDirty = updCtx->ttlDays,
                               .changeTimeMsDirty = updCtx->changeTimeMs};
  STtlDirtyEntry dirtryEntry = {.type = ENTRY_TYPE_UPSERT};

  int32_t code = taosHashPut(pTtlMgr->pTtlCache, &updCtx->uid, sizeof(updCtx->uid), &cacheEntry, sizeof(cacheEntry));
  if (TSDB_CODE_SUCCESS != code) {
    metaError("%s, ttlMgr insert failed to update cache since %s", pTtlMgr->logPrefix, tstrerror(code));
    goto _out;
  }

  code = taosHashPut(pTtlMgr->pDirtyUids, &updCtx->uid, sizeof(updCtx->uid), &dirtryEntry, sizeof(dirtryEntry));
  if (TSDB_CODE_SUCCESS != code) {
    metaError("%s, ttlMgr insert failed to update dirty uids since %s", pTtlMgr->logPrefix, tstrerror(code));
    goto _out;
  }

  if (ttlMgrNeedFlush(pTtlMgr)) {
    int32_t ret = ttlMgrFlush(pTtlMgr, updCtx->pTxn);
    if (ret < 0) {
      metaError("%s, ttlMgr insert failed to flush since %s", pTtlMgr->logPrefix, tstrerror(ret));
    }
  }

  code = TSDB_CODE_SUCCESS;

_out:
  metaTrace("%s, ttl mgr insert ttl, uid: %" PRId64 ", ctime: %" PRId64 ", ttlDays: %" PRId64, pTtlMgr->logPrefix,
            updCtx->uid, updCtx->changeTimeMs, updCtx->ttlDays);

  TAOS_RETURN(code);
}

int32_t ttlMgrDeleteTtl(STtlManger *pTtlMgr, const STtlDelTtlCtx *delCtx) {
  if (delCtx->ttlDays == 0) return 0;

  STtlDirtyEntry dirtryEntry = {.type = ENTRY_TYPE_DELETE};

  int32_t code = taosHashPut(pTtlMgr->pDirtyUids, &delCtx->uid, sizeof(delCtx->uid), &dirtryEntry, sizeof(dirtryEntry));
  if (TSDB_CODE_SUCCESS != code) {
    metaError("%s, ttlMgr del failed to update dirty uids since %s", pTtlMgr->logPrefix, tstrerror(code));
    goto _out;
  }

  if (ttlMgrNeedFlush(pTtlMgr)) {
    int32_t ret = ttlMgrFlush(pTtlMgr, delCtx->pTxn);
    if (ret < 0) {
      metaError("%s, ttlMgr del failed to flush since %s", pTtlMgr->logPrefix, tstrerror(ret));
    }
  }

  code = TSDB_CODE_SUCCESS;

_out:
  metaTrace("%s, ttl mgr delete ttl, uid: %" PRId64, pTtlMgr->logPrefix, delCtx->uid);
  TAOS_RETURN(code);
}

int32_t ttlMgrUpdateChangeTime(STtlManger *pTtlMgr, const STtlUpdCtimeCtx *pUpdCtimeCtx) {
  int32_t code = TSDB_CODE_SUCCESS;

  STtlCacheEntry *oldData = taosHashGet(pTtlMgr->pTtlCache, &pUpdCtimeCtx->uid, sizeof(pUpdCtimeCtx->uid));
  if (oldData == NULL) {
    goto _out;
  }

  STtlCacheEntry cacheEntry = {.ttlDays = oldData->ttlDays,
                               .changeTimeMs = oldData->changeTimeMs,
                               .ttlDaysDirty = oldData->ttlDays,
                               .changeTimeMsDirty = pUpdCtimeCtx->changeTimeMs};
  STtlDirtyEntry dirtryEntry = {.type = ENTRY_TYPE_UPSERT};

  code =
      taosHashPut(pTtlMgr->pTtlCache, &pUpdCtimeCtx->uid, sizeof(pUpdCtimeCtx->uid), &cacheEntry, sizeof(cacheEntry));
  if (TSDB_CODE_SUCCESS != code) {
    metaError("%s, ttlMgr update ctime failed to update cache since %s", pTtlMgr->logPrefix, tstrerror(code));
    goto _out;
  }

  code = taosHashPut(pTtlMgr->pDirtyUids, &pUpdCtimeCtx->uid, sizeof(pUpdCtimeCtx->uid), &dirtryEntry,
                     sizeof(dirtryEntry));
  if (TSDB_CODE_SUCCESS != code) {
    metaError("%s, ttlMgr update ctime failed to update dirty uids since %s", pTtlMgr->logPrefix, tstrerror(code));
    goto _out;
  }

  if (ttlMgrNeedFlush(pTtlMgr)) {
    int32_t ret = ttlMgrFlush(pTtlMgr, pUpdCtimeCtx->pTxn);
    if (ret < 0) {
      metaError("%s, ttlMgr update ctime failed to flush since %s", pTtlMgr->logPrefix, tstrerror(ret));
    }
  }

  code = TSDB_CODE_SUCCESS;

_out:
  metaTrace("%s, ttl mgr update ctime, uid: %" PRId64 ", ctime: %" PRId64, pTtlMgr->logPrefix, pUpdCtimeCtx->uid,
            pUpdCtimeCtx->changeTimeMs);
  TAOS_RETURN(code);
}

int32_t ttlMgrFindExpired(STtlManger *pTtlMgr, int64_t timePointMs, SArray *pTbUids, int32_t ttlDropMaxCount) {
  int32_t code = TSDB_CODE_SUCCESS;

  STtlIdxKeyV1   ttlKey = {.deleteTimeMs = timePointMs, .uid = INT64_MAX};
  STtlExpiredCtx expiredCtx = {
      .ttlDropMaxCount = ttlDropMaxCount, .count = 0, .expiredKey = ttlKey, .pTbUids = pTbUids};
  TAOS_CHECK_GOTO(tdbTbTraversal(pTtlMgr->pTtlIdx, &expiredCtx, ttlMgrFindExpiredOneEntry), NULL, _out);

  size_t vIdx = 0;
  for (size_t i = 0; i < pTbUids->size; i++) {
    tb_uid_t *pUid = taosArrayGet(pTbUids, i);
    if (taosHashGet(pTtlMgr->pDirtyUids, pUid, sizeof(tb_uid_t)) == NULL) {
      // not in dirty && expired in tdb => must be expired
      taosArraySet(pTbUids, vIdx, pUid);
      vIdx++;
    }
  }

  taosArrayPopTailBatch(pTbUids, pTbUids->size - vIdx);

_out:
  TAOS_RETURN(code);
}

static bool ttlMgrNeedFlush(STtlManger *pTtlMgr) {
  return pTtlMgr->flushThreshold > 0 && taosHashGetSize(pTtlMgr->pDirtyUids) > pTtlMgr->flushThreshold;
}

int32_t ttlMgrFlush(STtlManger *pTtlMgr, TXN *pTxn) {
  int64_t startNs = taosGetTimestampNs();
  int64_t endNs = startNs;

  metaTrace("%s, ttl mgr flush start. num of dirty uids:%d", pTtlMgr->logPrefix, taosHashGetSize(pTtlMgr->pDirtyUids));

  int32_t code = TSDB_CODE_SUCCESS;

  void *pIter = NULL;
  while ((pIter = taosHashIterate(pTtlMgr->pDirtyUids, pIter)) != NULL) {
    STtlDirtyEntry *pEntry = (STtlDirtyEntry *)pIter;
    tb_uid_t       *pUid = taosHashGetKey(pIter, NULL);

    STtlCacheEntry *cacheEntry = taosHashGet(pTtlMgr->pTtlCache, pUid, sizeof(*pUid));
    if (cacheEntry == NULL) {
      metaError("%s, ttlMgr flush failed to get ttl cache, uid: %" PRId64 ", type: %d", pTtlMgr->logPrefix, *pUid,
                pEntry->type);
      continue;
    }

    STtlIdxKeyV1 ttlKey;
    ttlMgrBuildKey(&ttlKey, cacheEntry->ttlDays, cacheEntry->changeTimeMs, *pUid);

    STtlIdxKeyV1 ttlKeyDirty;
    ttlMgrBuildKey(&ttlKeyDirty, cacheEntry->ttlDaysDirty, cacheEntry->changeTimeMsDirty, *pUid);

    if (pEntry->type == ENTRY_TYPE_UPSERT) {
      // delete old key & upsert new key
      code = tdbTbDelete(pTtlMgr->pTtlIdx, &ttlKey, sizeof(ttlKey), pTxn);  // maybe first insert, ignore error
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_NOT_FOUND != code) {
        metaError("%s, ttlMgr flush failed to delete since %s", pTtlMgr->logPrefix, tstrerror(code));
        continue;
      }
      code = tdbTbUpsert(pTtlMgr->pTtlIdx, &ttlKeyDirty, sizeof(ttlKeyDirty), &cacheEntry->ttlDaysDirty,
                         sizeof(cacheEntry->ttlDaysDirty), pTxn);
      if (TSDB_CODE_SUCCESS != code) {
        metaError("%s, ttlMgr flush failed to upsert since %s", pTtlMgr->logPrefix, tstrerror(code));
        continue;
      }

      cacheEntry->ttlDays = cacheEntry->ttlDaysDirty;
      cacheEntry->changeTimeMs = cacheEntry->changeTimeMsDirty;
    } else if (pEntry->type == ENTRY_TYPE_DELETE) {
      code = tdbTbDelete(pTtlMgr->pTtlIdx, &ttlKey, sizeof(ttlKey), pTxn);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_NOT_FOUND != code) {
        metaError("%s, ttlMgr flush failed to delete since %s", pTtlMgr->logPrefix, tstrerror(code));
        continue;
      }

      code = taosHashRemove(pTtlMgr->pTtlCache, pUid, sizeof(*pUid));
      if (TSDB_CODE_SUCCESS != code) {
        metaError("%s, ttlMgr flush failed to remove cache since %s", pTtlMgr->logPrefix, tstrerror(code));
        continue;
      }
    } else {
      metaError("%s, ttlMgr flush failed, unknown type: %d", pTtlMgr->logPrefix, pEntry->type);
      continue;
    }

    metaDebug("isdel:%d", pEntry->type == ENTRY_TYPE_DELETE);
    metaDebug("ttlkey:%" PRId64 ", uid:%" PRId64, ttlKey.deleteTimeMs, ttlKey.uid);
    metaDebug("ttlkeyDirty:%" PRId64 ", uid:%" PRId64, ttlKeyDirty.deleteTimeMs, ttlKeyDirty.uid);

    code = taosHashRemove(pTtlMgr->pDirtyUids, pUid, sizeof(*pUid));
    if (TSDB_CODE_SUCCESS != code) {
      metaError("%s, ttlMgr flush failed to remove dirty uid since %s", pTtlMgr->logPrefix, tstrerror(code));
      continue;
    }
  }

  int32_t count = taosHashGetSize(pTtlMgr->pDirtyUids);
  if (count != 0) {
    taosHashClear(pTtlMgr->pDirtyUids);
    metaError("%s, ttlMgr flush failed, dirty uids not empty, count: %d", pTtlMgr->logPrefix, count);
    code = TSDB_CODE_VND_TTL_FLUSH_INCOMPLETION;

    goto _out;
  }

  code = TSDB_CODE_SUCCESS;

_out:
  taosHashCancelIterate(pTtlMgr->pDirtyUids, pIter);

  endNs = taosGetTimestampNs();
  metaTrace("%s, ttl mgr flush end, time consumed: %" PRId64 " ns", pTtlMgr->logPrefix, endNs - startNs);

  TAOS_RETURN(code);
}
