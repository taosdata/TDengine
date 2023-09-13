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

int ttlMgrOpen(STtlManger **ppTtlMgr, TDB *pEnv, int8_t rollback, const char *logPrefix, int32_t flushThreshold) {
  int     ret = TSDB_CODE_SUCCESS;
  int64_t startNs = taosGetTimestampNs();

  *ppTtlMgr = NULL;

  STtlManger *pTtlMgr = (STtlManger *)tdbOsCalloc(1, sizeof(*pTtlMgr));
  if (pTtlMgr == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  char *logBuffer = (char *)tdbOsCalloc(1, strlen(logPrefix) + 1);
  if (logBuffer == NULL) {
    tdbOsFree(pTtlMgr);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(logBuffer, logPrefix);
  pTtlMgr->logPrefix = logBuffer;
  pTtlMgr->flushThreshold = flushThreshold;

  ret = tdbTbOpen(ttlV1Tbname, TDB_VARIANT_LEN, TDB_VARIANT_LEN, ttlIdxKeyV1Cmpr, pEnv, &pTtlMgr->pTtlIdx, rollback);
  if (ret < 0) {
    metaError("%s, failed to open %s since %s", pTtlMgr->logPrefix, ttlV1Tbname, tstrerror(terrno));
    tdbOsFree(pTtlMgr);
    return ret;
  }

  pTtlMgr->pTtlCache = taosHashInit(8192, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  pTtlMgr->pDirtyUids = taosHashInit(8192, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);

  ret = ttlMgrFillCache(pTtlMgr);
  if (ret < 0) {
    metaError("%s, failed to fill hash since %s", pTtlMgr->logPrefix, tstrerror(terrno));
    ttlMgrCleanup(pTtlMgr);
    return ret;
  }

  int64_t endNs = taosGetTimestampNs();
  metaInfo("%s, ttl mgr open end, hash size: %d, time consumed: %" PRId64 " ns", pTtlMgr->logPrefix,
           taosHashGetSize(pTtlMgr->pTtlCache), endNs - startNs);

  *ppTtlMgr = pTtlMgr;
  return TSDB_CODE_SUCCESS;
}

void ttlMgrClose(STtlManger *pTtlMgr) { ttlMgrCleanup(pTtlMgr); }

bool ttlMgrNeedUpgrade(TDB *pEnv) {
  bool needUpgrade = tdbTbExist(ttlTbname, pEnv);
  if (needUpgrade) {
    metaInfo("find ttl idx in old version , will convert");
  }
  return needUpgrade;
}

int ttlMgrUpgrade(STtlManger *pTtlMgr, void *pMeta) {
  SMeta *meta = (SMeta *)pMeta;
  int    ret = TSDB_CODE_SUCCESS;

  if (!tdbTbExist(ttlTbname, meta->pEnv)) return TSDB_CODE_SUCCESS;

  metaInfo("%s, ttl mgr start upgrade", pTtlMgr->logPrefix);

  int64_t startNs = taosGetTimestampNs();

  ret = tdbTbOpen(ttlTbname, sizeof(STtlIdxKey), 0, ttlIdxKeyCmpr, meta->pEnv, &pTtlMgr->pOldTtlIdx, 0);
  if (ret < 0) {
    metaError("%s, failed to open %s index since %s", pTtlMgr->logPrefix, ttlTbname, tstrerror(terrno));
    goto _out;
  }

  ret = ttlMgrConvert(pTtlMgr->pOldTtlIdx, pTtlMgr->pTtlIdx, pMeta);
  if (ret < 0) {
    metaError("%s, failed to convert ttl index since %s", pTtlMgr->logPrefix, tstrerror(terrno));
    goto _out;
  }

  ret = tdbTbDropByName(ttlTbname, meta->pEnv, meta->txn);
  if (ret < 0) {
    metaError("%s, failed to drop old ttl index since %s", pTtlMgr->logPrefix, tstrerror(terrno));
    goto _out;
  }

  ret = ttlMgrFillCache(pTtlMgr);
  if (ret < 0) {
    metaError("%s, failed to fill hash since %s", pTtlMgr->logPrefix, tstrerror(terrno));
    goto _out;
  }

  int64_t endNs = taosGetTimestampNs();
  metaInfo("%s, ttl mgr upgrade end, hash size: %d, time consumed: %" PRId64 " ns", pTtlMgr->logPrefix,
           taosHashGetSize(pTtlMgr->pTtlCache), endNs - startNs);

_out:
  tdbTbClose(pTtlMgr->pOldTtlIdx);
  pTtlMgr->pOldTtlIdx = NULL;

  return ret;
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

static int ttlMgrConvertOneEntry(const void *pKey, int keyLen, const void *pVal, int valLen, void *pConvertData) {
  SConvertData *pData = (SConvertData *)pConvertData;

  STtlIdxKey *ttlKey = (STtlIdxKey *)pKey;
  tb_uid_t    uid = ttlKey->uid;
  int64_t     ttlDays = 0;

  int ret = metaGetTableTtlByUid(pData->pMeta, uid, &ttlDays);
  if (ret < 0) {
    metaError("ttlMgr convert failed to get ttl since %s", tstrerror(terrno));
    goto _out;
  }

  STtlIdxKeyV1 ttlKeyV1 = {.deleteTimeMs = ttlKey->deleteTimeSec * 1000, .uid = uid};
  ret = tdbTbUpsert(pData->pNewTtlIdx, &ttlKeyV1, sizeof(ttlKeyV1), &ttlDays, sizeof(ttlDays), pData->pMeta->txn);
  if (ret < 0) {
    metaError("ttlMgr convert failed to upsert since %s", tstrerror(terrno));
    goto _out;
  }

  ret = 0;

_out:
  return ret;
}

static int32_t ttlMgrFindExpiredOneEntry(const void *pKey, int keyLen, const void *pVal, int valLen,
                                         void *pExpiredCtx) {
  STtlExpiredCtx *pCtx = (STtlExpiredCtx *)pExpiredCtx;
  if (pCtx->count >= pCtx->ttlDropMaxCount) return -1;

  int c = ttlIdxKeyV1Cmpr(&pCtx->expiredKey, sizeof(pCtx->expiredKey), pKey, keyLen);
  if (c > 0) {
    taosArrayPush(pCtx->pTbUids, &((STtlIdxKeyV1 *)pKey)->uid);
    pCtx->count++;
  }

  return c;
}

static int ttlMgrConvert(TTB *pOldTtlIdx, TTB *pNewTtlIdx, void *pMeta) {
  SMeta *meta = pMeta;

  metaInfo("ttlMgr convert start.");

  SConvertData cvData = {.pNewTtlIdx = pNewTtlIdx, .pMeta = meta};

  int ret = tdbTbTraversal(pOldTtlIdx, &cvData, ttlMgrConvertOneEntry);
  if (ret < 0) {
    metaError("failed to convert since %s", tstrerror(terrno));
  }

  metaInfo("ttlMgr convert end.");
  return ret;
}

int ttlMgrInsertTtl(STtlManger *pTtlMgr, const STtlUpdTtlCtx *updCtx) {
  if (updCtx->ttlDays == 0) return 0;

  STtlCacheEntry cacheEntry = {.ttlDays = updCtx->ttlDays,
                               .changeTimeMs = updCtx->changeTimeMs,
                               .ttlDaysDirty = updCtx->ttlDays,
                               .changeTimeMsDirty = updCtx->changeTimeMs};
  STtlDirtyEntry dirtryEntry = {.type = ENTRY_TYPE_UPSERT};

  int ret = taosHashPut(pTtlMgr->pTtlCache, &updCtx->uid, sizeof(updCtx->uid), &cacheEntry, sizeof(cacheEntry));
  if (ret < 0) {
    metaError("%s, ttlMgr insert failed to update cache since %s", pTtlMgr->logPrefix, tstrerror(terrno));
    goto _out;
  }

  ret = taosHashPut(pTtlMgr->pDirtyUids, &updCtx->uid, sizeof(updCtx->uid), &dirtryEntry, sizeof(dirtryEntry));
  if (ret < 0) {
    metaError("%s, ttlMgr insert failed to update dirty uids since %s", pTtlMgr->logPrefix, tstrerror(terrno));
    goto _out;
  }

  if (ttlMgrNeedFlush(pTtlMgr)) {
    ttlMgrFlush(pTtlMgr, updCtx->pTxn);
  }

  ret = 0;

_out:
  metaTrace("%s, ttl mgr insert ttl, uid: %" PRId64 ", ctime: %" PRId64 ", ttlDays: %" PRId64, pTtlMgr->logPrefix,
            updCtx->uid, updCtx->changeTimeMs, updCtx->ttlDays);

  return ret;
}

int ttlMgrDeleteTtl(STtlManger *pTtlMgr, const STtlDelTtlCtx *delCtx) {
  if (delCtx->ttlDays == 0) return 0;

  STtlDirtyEntry dirtryEntry = {.type = ENTRY_TYPE_DELETE};

  int ret = taosHashPut(pTtlMgr->pDirtyUids, &delCtx->uid, sizeof(delCtx->uid), &dirtryEntry, sizeof(dirtryEntry));
  if (ret < 0) {
    metaError("%s, ttlMgr del failed to update dirty uids since %s", pTtlMgr->logPrefix, tstrerror(terrno));
    goto _out;
  }

  if (ttlMgrNeedFlush(pTtlMgr)) {
    ttlMgrFlush(pTtlMgr, delCtx->pTxn);
  }

  ret = 0;

_out:
  metaTrace("%s, ttl mgr delete ttl, uid: %" PRId64, pTtlMgr->logPrefix, delCtx->uid);

  return ret;
}

int ttlMgrUpdateChangeTime(STtlManger *pTtlMgr, const STtlUpdCtimeCtx *pUpdCtimeCtx) {
  int ret = 0;

  STtlCacheEntry *oldData = taosHashGet(pTtlMgr->pTtlCache, &pUpdCtimeCtx->uid, sizeof(pUpdCtimeCtx->uid));
  if (oldData == NULL) {
    goto _out;
  }

  STtlCacheEntry cacheEntry = {.ttlDays = oldData->ttlDays,
                               .changeTimeMs = oldData->changeTimeMs,
                               .ttlDaysDirty = oldData->ttlDays,
                               .changeTimeMsDirty = pUpdCtimeCtx->changeTimeMs};
  STtlDirtyEntry dirtryEntry = {.type = ENTRY_TYPE_UPSERT};

  ret = taosHashPut(pTtlMgr->pTtlCache, &pUpdCtimeCtx->uid, sizeof(pUpdCtimeCtx->uid), &cacheEntry, sizeof(cacheEntry));
  if (ret < 0) {
    metaError("%s, ttlMgr update ctime failed to update cache since %s", pTtlMgr->logPrefix, tstrerror(terrno));
    goto _out;
  }

  ret = taosHashPut(pTtlMgr->pDirtyUids, &pUpdCtimeCtx->uid, sizeof(pUpdCtimeCtx->uid), &dirtryEntry,
                    sizeof(dirtryEntry));
  if (ret < 0) {
    metaError("%s, ttlMgr update ctime failed to update dirty uids since %s", pTtlMgr->logPrefix,
              tstrerror(terrno));
    goto _out;
  }

  if (ttlMgrNeedFlush(pTtlMgr)) {
    ttlMgrFlush(pTtlMgr, pUpdCtimeCtx->pTxn);
  }

  ret = 0;

_out:
  metaTrace("%s, ttl mgr update ctime, uid: %" PRId64 ", ctime: %" PRId64, pTtlMgr->logPrefix, pUpdCtimeCtx->uid,
            pUpdCtimeCtx->changeTimeMs);

  return ret;
}

int ttlMgrFindExpired(STtlManger *pTtlMgr, int64_t timePointMs, SArray *pTbUids, int32_t ttlDropMaxCount) {
  int ret = -1;

  STtlIdxKeyV1   ttlKey = {.deleteTimeMs = timePointMs, .uid = INT64_MAX};
  STtlExpiredCtx expiredCtx = {
      .ttlDropMaxCount = ttlDropMaxCount, .count = 0, .expiredKey = ttlKey, .pTbUids = pTbUids};
  ret = tdbTbTraversal(pTtlMgr->pTtlIdx, &expiredCtx, ttlMgrFindExpiredOneEntry);
  if (ret) {
    goto _out;
  }

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
  return ret;
}

static bool ttlMgrNeedFlush(STtlManger *pTtlMgr) {
  return pTtlMgr->flushThreshold > 0 && taosHashGetSize(pTtlMgr->pDirtyUids) > pTtlMgr->flushThreshold;
}

int ttlMgrFlush(STtlManger *pTtlMgr, TXN *pTxn) {
  int64_t startNs = taosGetTimestampNs();
  int64_t endNs = startNs;

  metaTrace("%s, ttl mgr flush start. dirty uids:%d", pTtlMgr->logPrefix, taosHashGetSize(pTtlMgr->pDirtyUids));

  int ret = -1;

  void *pIter = taosHashIterate(pTtlMgr->pDirtyUids, NULL);
  while (pIter != NULL) {
    STtlDirtyEntry *pEntry = (STtlDirtyEntry *)pIter;
    tb_uid_t       *pUid = taosHashGetKey(pIter, NULL);

    STtlCacheEntry *cacheEntry = taosHashGet(pTtlMgr->pTtlCache, pUid, sizeof(*pUid));
    if (cacheEntry == NULL) {
      metaError("%s, ttlMgr flush failed to get ttl cache since %s, uid: %" PRId64 ", type: %d", pTtlMgr->logPrefix,
                tstrerror(terrno), *pUid, pEntry->type);
      continue;
    }

    STtlIdxKeyV1 ttlKey;
    ttlMgrBuildKey(&ttlKey, cacheEntry->ttlDays, cacheEntry->changeTimeMs, *pUid);

    STtlIdxKeyV1 ttlKeyDirty;
    ttlMgrBuildKey(&ttlKeyDirty, cacheEntry->ttlDaysDirty, cacheEntry->changeTimeMsDirty, *pUid);

    if (pEntry->type == ENTRY_TYPE_UPSERT) {
      // delete old key & upsert new key
      tdbTbDelete(pTtlMgr->pTtlIdx, &ttlKey, sizeof(ttlKey), pTxn); // maybe first insert, ignore error
      ret = tdbTbUpsert(pTtlMgr->pTtlIdx, &ttlKeyDirty, sizeof(ttlKeyDirty), &cacheEntry->ttlDaysDirty,
                        sizeof(cacheEntry->ttlDaysDirty), pTxn);
      if (ret < 0) {
        metaError("%s, ttlMgr flush failed to upsert since %s", pTtlMgr->logPrefix, tstrerror(terrno));
        goto _out;
      }

      cacheEntry->ttlDays = cacheEntry->ttlDaysDirty;
      cacheEntry->changeTimeMs = cacheEntry->changeTimeMsDirty;
    } else if (pEntry->type == ENTRY_TYPE_DELETE) {
      ret = tdbTbDelete(pTtlMgr->pTtlIdx, &ttlKey, sizeof(ttlKey), pTxn);
      if (ret < 0) {
        metaError("%s, ttlMgr flush failed to delete since %s", pTtlMgr->logPrefix, tstrerror(terrno));
        goto _out;
      }

      ret = taosHashRemove(pTtlMgr->pTtlCache, pUid, sizeof(*pUid));
      if (ret < 0) {
        metaError("%s, ttlMgr flush failed to remove cache since %s", pTtlMgr->logPrefix, tstrerror(terrno));
        goto _out;
      }
    } else {
      metaError("%s, ttlMgr flush failed, unknown type: %d", pTtlMgr->logPrefix, pEntry->type);
      goto _out;
    }

    void *pIterTmp = pIter;
    pIter = taosHashIterate(pTtlMgr->pDirtyUids, pIterTmp);
    taosHashRemove(pTtlMgr->pDirtyUids, pUid, sizeof(tb_uid_t));
  }

  taosHashClear(pTtlMgr->pDirtyUids);

  ret = 0;

_out:
  endNs = taosGetTimestampNs();
  metaTrace("%s, ttl mgr flush end, time consumed: %" PRId64 " ns", pTtlMgr->logPrefix, endNs - startNs);

  return ret;
}
