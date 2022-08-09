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

#include "sma.h"

#define RSMA_QTASKINFO_BUFSIZE  32768
#define RSMA_QTASKINFO_HEAD_LEN (sizeof(int32_t) + sizeof(int8_t) + sizeof(int64_t))  // len + type + suid

SSmaMgmt smaMgmt = {
    .inited = 0,
    .rsetId = -1,
};

#define TD_QTASKINFO_FNAME_PREFIX "qtaskinfo.ver"
#define TD_RSMAINFO_DEL_FILE      "rsmainfo.del"
typedef struct SRSmaQTaskInfoItem SRSmaQTaskInfoItem;
typedef struct SRSmaQTaskInfoIter SRSmaQTaskInfoIter;

static int32_t    tdUidStorePut(STbUidStore *pStore, tb_uid_t suid, tb_uid_t *uid);
static int32_t    tdUpdateTbUidListImpl(SSma *pSma, tb_uid_t *suid, SArray *tbUids);
static int32_t    tdSetRSmaInfoItemParams(SSma *pSma, SRSmaParam *param, SRSmaStat *pStat, SRSmaInfo *pRSmaInfo,
                                          int8_t idx);
static int32_t    tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t inputType, SRSmaInfo *pInfo, tb_uid_t suid,
                                    int8_t level);
static SRSmaInfo *tdAcquireRSmaInfoBySuid(SSma *pSma, int64_t suid);
static void       tdReleaseRSmaInfo(SSma *pSma, SRSmaInfo *pInfo);
static int32_t    tdRSmaFetchAndSubmitResult(SSma *pSma, qTaskInfo_t taskInfo, SRSmaInfoItem *pItem, STSchema *pTSchema,
                                             int64_t suid, int8_t blkType);
static void       tdRSmaFetchTrigger(void *param, void *tmrId);
static int32_t    tdRSmaFetchSend(SSma *pSma, SRSmaInfo *pInfo, int8_t level);
static int32_t    tdRSmaQTaskInfoIterInit(SRSmaQTaskInfoIter *pIter, STFile *pTFile);
static int32_t    tdRSmaQTaskInfoIterNextBlock(SRSmaQTaskInfoIter *pIter, bool *isFinish);
static int32_t    tdRSmaQTaskInfoRestore(SSma *pSma, int8_t type, SRSmaQTaskInfoIter *pIter);
static int32_t    tdRSmaQTaskInfoItemRestore(SSma *pSma, const SRSmaQTaskInfoItem *infoItem);
static int32_t    tdRSmaRestoreQTaskInfoInit(SSma *pSma, int64_t *nTables);
static int32_t    tdRSmaRestoreQTaskInfoReload(SSma *pSma, int8_t type, int64_t qTaskFileVer);
static int32_t    tdRSmaRestoreTSDataReload(SSma *pSma);

static SRSmaInfo *tdGetRSmaInfoByItem(SRSmaInfoItem *pItem) {
  // adapt accordingly if definition of SRSmaInfo update
  SRSmaInfo *pResult = NULL;
  ASSERT(pItem->level == TSDB_RETENTION_L1 || pItem->level == TSDB_RETENTION_L2);
  pResult = (SRSmaInfo *)POINTER_SHIFT(pItem, -(sizeof(SRSmaInfoItem) * (pItem->level - 1) + RSMA_INFO_HEAD_LEN));
  ASSERT(pResult->pTSchema->numOfCols > 1);
  return pResult;
}

struct SRSmaQTaskInfoItem {
  int32_t len;
  int8_t  type;
  int64_t suid;
  void   *qTaskInfo;
};

struct SRSmaQTaskInfoIter {
  STFile *pTFile;
  int64_t offset;
  int64_t fsize;
  int32_t nBytes;
  int32_t nAlloc;
  char   *pBuf;
  // ------------
  char   *qBuf;  // for iterator
  int32_t nBufPos;
};

void tdRSmaQTaskInfoGetFileName(int32_t vgId, int64_t version, char *outputName) {
  tdGetVndFileName(vgId, NULL, VNODE_RSMA_DIR, TD_QTASKINFO_FNAME_PREFIX, version, outputName);
}

void tdRSmaQTaskInfoGetFullName(int32_t vgId, int64_t version, const char *path, char *outputName) {
  tdGetVndFileName(vgId, path, VNODE_RSMA_DIR, TD_QTASKINFO_FNAME_PREFIX, version, outputName);
}

static FORCE_INLINE int32_t tdRSmaQTaskInfoContLen(int32_t lenWithHead) {
  return lenWithHead - RSMA_QTASKINFO_HEAD_LEN;
}

static FORCE_INLINE void tdRSmaQTaskInfoIterDestroy(SRSmaQTaskInfoIter *pIter) { taosMemoryFreeClear(pIter->pBuf); }

void tdFreeQTaskInfo(qTaskInfo_t *taskHandle, int32_t vgId, int32_t level) {
  // Note: free/kill may in RC
  if (!taskHandle || !(*taskHandle)) return;
  qTaskInfo_t otaskHandle = atomic_load_ptr(taskHandle);
  if (otaskHandle && atomic_val_compare_exchange_ptr(taskHandle, otaskHandle, NULL)) {
    smaDebug("vgId:%d, free qTaskInfo_t %p of level %d", vgId, otaskHandle, level);
    qDestroyTask(otaskHandle);
  } else {
    smaDebug("vgId:%d, not free qTaskInfo_t %p of level %d", vgId, otaskHandle, level);
  }
  // TODO: clear files related to qTaskInfo?
}

/**
 * @brief general function to free rsmaInfo
 *
 * @param pSma
 * @param pInfo
 * @param isDeepFree Only stop tmrId and free pTSchema for deep free
 * @return void*
 */
void *tdFreeRSmaInfo(SSma *pSma, SRSmaInfo *pInfo, bool isDeepFree) {
  if (pInfo) {
    for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
      SRSmaInfoItem *pItem = &pInfo->items[i];

      if (isDeepFree && pItem->tmrId) {
        smaDebug("vgId:%d, stop fetch timer %p for table %" PRIi64 " level %d", SMA_VID(pSma), pInfo->suid,
                 pItem->tmrId, i + 1);
        taosTmrStopA(&pItem->tmrId);
      }

      if (isDeepFree && pInfo->taskInfo[i]) {
        tdFreeQTaskInfo(&pInfo->taskInfo[i], SMA_VID(pSma), i + 1);
      } else {
        smaDebug("vgId:%d, table %" PRIi64 " no need to destroy rsma info level %d since empty taskInfo", SMA_VID(pSma),
                 pInfo->suid, i + 1);
      }

      if (pInfo->iTaskInfo[i]) {
        tdFreeQTaskInfo(&pInfo->iTaskInfo[i], SMA_VID(pSma), i + 1);
      } else {
        smaDebug("vgId:%d, table %" PRIi64 " no need to destroy rsma info level %d since empty iTaskInfo",
                 SMA_VID(pSma), pInfo->suid, i + 1);
      }
    }
    if (isDeepFree) {
      taosMemoryFreeClear(pInfo->pTSchema);
    }
    taosMemoryFree(pInfo);
  }

  return NULL;
}

static FORCE_INLINE int32_t tdUidStoreInit(STbUidStore **pStore) {
  ASSERT(*pStore == NULL);
  *pStore = taosMemoryCalloc(1, sizeof(STbUidStore));
  if (*pStore == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tdUpdateTbUidListImpl(SSma *pSma, tb_uid_t *suid, SArray *tbUids) {
  SRSmaInfo *pRSmaInfo = NULL;

  if (!suid || !tbUids) {
    terrno = TSDB_CODE_INVALID_PTR;
    smaError("vgId:%d, failed to get rsma info for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr());
    return TSDB_CODE_FAILED;
  }

  if (!taosArrayGetSize(tbUids)) {
    smaDebug("vgId:%d, no need to update tbUidList for suid:%" PRIi64 " since Empty tbUids", SMA_VID(pSma), *suid);
    return TSDB_CODE_SUCCESS;
  }

  pRSmaInfo = tdAcquireRSmaInfoBySuid(pSma, *suid);

  if (!pRSmaInfo) {
    smaError("vgId:%d, failed to get rsma info for uid:%" PRIi64, SMA_VID(pSma), *suid);
    terrno = TSDB_CODE_RSMA_INVALID_STAT;
    return TSDB_CODE_FAILED;
  }

  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pRSmaInfo->taskInfo[i]) {
      if ((qUpdateQualifiedTableId(pRSmaInfo->taskInfo[i], tbUids, true) < 0)) {
        tdReleaseRSmaInfo(pSma, pRSmaInfo);
        smaError("vgId:%d, update tbUidList failed for uid:%" PRIi64 " level %d since %s", SMA_VID(pSma), *suid, i,
                 terrstr());
        return TSDB_CODE_FAILED;
      } else {
        smaDebug("vgId:%d, update tbUidList succeed for qTaskInfo:%p with suid:%" PRIi64 " uid:%" PRIi64 " level %d",
                 SMA_VID(pSma), pRSmaInfo->taskInfo[0], *suid, *(int64_t *)taosArrayGet(tbUids, 0), i);
      }
    }
  }

  tdReleaseRSmaInfo(pSma, pRSmaInfo);
  return TSDB_CODE_SUCCESS;
}

int32_t tdUpdateTbUidList(SSma *pSma, STbUidStore *pStore) {
  if (!pStore || (taosArrayGetSize(pStore->tbUids) == 0)) {
    return TSDB_CODE_SUCCESS;
  }

  if (tdUpdateTbUidListImpl(pSma, &pStore->suid, pStore->tbUids) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  void *pIter = taosHashIterate(pStore->uidHash, NULL);
  while (pIter) {
    tb_uid_t *pTbSuid = (tb_uid_t *)taosHashGetKey(pIter, NULL);
    SArray   *pTbUids = *(SArray **)pIter;

    if (tdUpdateTbUidListImpl(pSma, pTbSuid, pTbUids) != TSDB_CODE_SUCCESS) {
      taosHashCancelIterate(pStore->uidHash, pIter);
      return TSDB_CODE_FAILED;
    }

    pIter = taosHashIterate(pStore->uidHash, pIter);
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief fetch suid/uids when create child tables of rollup SMA
 *
 * @param pTsdb
 * @param ppStore
 * @param suid
 * @param uid
 * @return int32_t
 */
int32_t tdFetchTbUidList(SSma *pSma, STbUidStore **ppStore, tb_uid_t suid, tb_uid_t uid) {
  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);

  // only applicable to rollup SMA ctables
  if (!pEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SHashObj  *infoHash = NULL;
  if (!pStat || !(infoHash = RSMA_INFO_HASH(pStat))) {
    terrno = TSDB_CODE_RSMA_INVALID_STAT;
    return TSDB_CODE_FAILED;
  }

  // info cached when create rsma stable and return directly for non-rsma ctables
  if (!taosHashGet(infoHash, &suid, sizeof(tb_uid_t))) {
    return TSDB_CODE_SUCCESS;
  }

  ASSERT(ppStore != NULL);

  if (!(*ppStore)) {
    if (tdUidStoreInit(ppStore) < 0) {
      return TSDB_CODE_FAILED;
    }
  }

  if (tdUidStorePut(*ppStore, suid, &uid) < 0) {
    *ppStore = tdUidStoreFree(*ppStore);
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tdSetRSmaInfoItemParams(SSma *pSma, SRSmaParam *param, SRSmaStat *pStat, SRSmaInfo *pRSmaInfo,
                                       int8_t idx) {
  if ((param->qmsgLen > 0) && param->qmsg[idx]) {
    SRetention *pRetention = SMA_RETENTION(pSma);
    STsdbCfg   *pTsdbCfg = SMA_TSDB_CFG(pSma);
    SVnode     *pVnode = pSma->pVnode;
    SReadHandle handle = {
        .meta = pVnode->pMeta,
        .vnode = pVnode,
        .initTqReader = 1,
    };

    pRSmaInfo->taskInfo[idx] = qCreateStreamExecTaskInfo(param->qmsg[idx], &handle);
    if (!pRSmaInfo->taskInfo[idx]) {
      terrno = TSDB_CODE_RSMA_QTASKINFO_CREATE;
      return TSDB_CODE_FAILED;
    }
    SRSmaInfoItem *pItem = &(pRSmaInfo->items[idx]);
    pItem->triggerStat = TASK_TRIGGER_STAT_INACTIVE;
    if (param->maxdelay[idx] < TSDB_MIN_ROLLUP_MAX_DELAY) {
      int64_t msInterval =
          convertTimeFromPrecisionToUnit(pRetention[idx + 1].freq, pTsdbCfg->precision, TIME_UNIT_MILLISECOND);
      pItem->maxDelay = (int32_t)msInterval;
    } else {
      pItem->maxDelay = (int32_t)param->maxdelay[idx];
    }
    if (pItem->maxDelay > TSDB_MAX_ROLLUP_MAX_DELAY) {
      pItem->maxDelay = TSDB_MAX_ROLLUP_MAX_DELAY;
    }
    pItem->level = idx == 0 ? TSDB_RETENTION_L1 : TSDB_RETENTION_L2;
    smaInfo("vgId:%d, table:%" PRIi64 " level:%" PRIi8 " maxdelay:%" PRIi64 " watermark:%" PRIi64
            ", finally maxdelay:%" PRIi32,
            TD_VID(pVnode), pRSmaInfo->suid, idx + 1, param->maxdelay[idx], param->watermark[idx], pItem->maxDelay);
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief for rsam create or restore
 *
 * @param pSma
 * @param param
 * @param suid
 * @param tbName
 * @return int32_t
 */
int32_t tdProcessRSmaCreateImpl(SSma *pSma, SRSmaParam *param, int64_t suid, const char *tbName) {
  if ((param->qmsgLen[0] == 0) && (param->qmsgLen[1] == 0)) {
    smaDebug("vgId:%d, no qmsg1/qmsg2 for rollup table %s %" PRIi64, SMA_VID(pSma), tbName, suid);
    return TSDB_CODE_SUCCESS;
  }

  if (tdCheckAndInitSmaEnv(pSma, TSDB_SMA_TYPE_ROLLUP) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  pRSmaInfo = taosHashGet(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t));
  if (pRSmaInfo) {
    // TODO: free original pRSmaInfo if exists abnormally
    tdFreeRSmaInfo(pSma, *(SRSmaInfo **)pRSmaInfo, true);
    if (taosHashRemove(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t)) < 0) {
      terrno = TSDB_CODE_RSMA_REMOVE_EXISTS;
      goto _err;
    }
    smaWarn("vgId:%d, remove the rsma info already exists for table %s, %" PRIi64, SMA_VID(pSma), tbName, suid);
  }

  // from write queue: single thead
  pRSmaInfo = (SRSmaInfo *)taosMemoryCalloc(1, sizeof(SRSmaInfo));
  if (!pRSmaInfo) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  STSchema *pTSchema = metaGetTbTSchema(SMA_META(pSma), suid, -1);
  if (!pTSchema) {
    terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
    goto _err;
  }
  pRSmaInfo->pTSchema = pTSchema;
  pRSmaInfo->suid = suid;
  pRSmaInfo->refId = RSMA_REF_ID(pStat);
  T_REF_INIT_VAL(pRSmaInfo, 1);

  if (tdSetRSmaInfoItemParams(pSma, param, pStat, pRSmaInfo, 0) < 0) {
    goto _err;
  }

  if (tdSetRSmaInfoItemParams(pSma, param, pStat, pRSmaInfo, 1) < 0) {
    goto _err;
  }

  if (taosHashPut(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t), &pRSmaInfo, sizeof(pRSmaInfo)) < 0) {
    goto _err;
  }

  smaDebug("vgId:%d, register rsma info succeed for table %" PRIi64, SMA_VID(pSma), suid);

  return TSDB_CODE_SUCCESS;
_err:
  tdFreeRSmaInfo(pSma, pRSmaInfo, true);
  return TSDB_CODE_FAILED;
}

/**
 * @brief Check and init qTaskInfo_t, only applicable to stable with SRSmaParam currently
 *
 * @param pSma
 * @param pReq
 * @return int32_t
 */
int32_t tdProcessRSmaCreate(SSma *pSma, SVCreateStbReq *pReq) {
  SVnode *pVnode = pSma->pVnode;
  if (!pReq->rollup) {
    smaTrace("vgId:%d, not create rsma for stable %s %" PRIi64 " since no rollup in req", TD_VID(pVnode), pReq->name,
             pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  if (!VND_IS_RSMA(pVnode)) {
    smaTrace("vgId:%d, not create rsma for stable %s %" PRIi64 " since vnd is not rsma", TD_VID(pVnode), pReq->name,
             pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  return tdProcessRSmaCreateImpl(pSma, &pReq->rsmaParam, pReq->suid, pReq->name);
}

/**
 * @brief drop cache for stb
 *
 * @param pSma
 * @param pReq
 * @return int32_t
 */
int32_t tdProcessRSmaDrop(SSma *pSma, SVDropStbReq *pReq) {
  SVnode *pVnode = pSma->pVnode;
  if (!VND_IS_RSMA(pVnode)) {
    smaTrace("vgId:%d, not drop rsma for stable %s %" PRIi64 " since vnd is not rsma", TD_VID(pVnode), pReq->name,
             pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SSmaStat  *pStat = SMA_ENV_STAT(pSmaEnv);
  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(pStat);

  SRSmaInfo *pRSmaInfo = tdAcquireRSmaInfoBySuid(pSma, pReq->suid);

  if (!pRSmaInfo) {
    smaWarn("vgId:%d, drop rsma for stable %s %" PRIi64 " failed no rsma in hash", TD_VID(pVnode), pReq->name,
            pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  // set del flag for data in mem
  RSMA_INFO_SET_DEL(pRSmaInfo);
  tdUnRefRSmaInfo(pSma, pRSmaInfo);

  tdReleaseRSmaInfo(pSma, pRSmaInfo);

  // save to file
  // TODO
  smaDebug("vgId:%d, drop rsma for table %" PRIi64 " succeed", TD_VID(pVnode), pReq->suid);
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief store suid/[uids], prefer to use array and then hash
 *
 * @param pStore
 * @param suid
 * @param uid
 * @return int32_t
 */
static int32_t tdUidStorePut(STbUidStore *pStore, tb_uid_t suid, tb_uid_t *uid) {
  // prefer to store suid/uids in array
  if ((suid == pStore->suid) || (pStore->suid == 0)) {
    if (pStore->suid == 0) {
      pStore->suid = suid;
    }
    if (uid) {
      if (!pStore->tbUids) {
        if (!(pStore->tbUids = taosArrayInit(1, sizeof(tb_uid_t)))) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return TSDB_CODE_FAILED;
        }
      }
      if (!taosArrayPush(pStore->tbUids, uid)) {
        return TSDB_CODE_FAILED;
      }
    }
  } else {
    // store other suid/uids in hash when multiple stable/table included in 1 batch of request
    if (!pStore->uidHash) {
      pStore->uidHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
      if (!pStore->uidHash) {
        return TSDB_CODE_FAILED;
      }
    }
    if (uid) {
      SArray *uidArray = taosHashGet(pStore->uidHash, &suid, sizeof(tb_uid_t));
      if (uidArray && ((uidArray = *(SArray **)uidArray))) {
        taosArrayPush(uidArray, uid);
      } else {
        SArray *pUidArray = taosArrayInit(1, sizeof(tb_uid_t));
        if (!pUidArray) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return TSDB_CODE_FAILED;
        }
        if (!taosArrayPush(pUidArray, uid)) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return TSDB_CODE_FAILED;
        }
        if (taosHashPut(pStore->uidHash, &suid, sizeof(suid), &pUidArray, sizeof(pUidArray)) < 0) {
          return TSDB_CODE_FAILED;
        }
      }
    } else {
      if (taosHashPut(pStore->uidHash, &suid, sizeof(suid), NULL, 0) < 0) {
        return TSDB_CODE_FAILED;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

void tdUidStoreDestory(STbUidStore *pStore) {
  if (pStore) {
    if (pStore->uidHash) {
      if (pStore->tbUids) {
        // When pStore->tbUids not NULL, the pStore->uidHash has k/v; otherwise pStore->uidHash only has keys.
        void *pIter = taosHashIterate(pStore->uidHash, NULL);
        while (pIter) {
          SArray *arr = *(SArray **)pIter;
          taosArrayDestroy(arr);
          pIter = taosHashIterate(pStore->uidHash, pIter);
        }
      }
      taosHashCleanup(pStore->uidHash);
    }
    taosArrayDestroy(pStore->tbUids);
  }
}

void *tdUidStoreFree(STbUidStore *pStore) {
  if (pStore) {
    tdUidStoreDestory(pStore);
    taosMemoryFree(pStore);
  }
  return NULL;
}

static int32_t tdProcessSubmitReq(STsdb *pTsdb, int64_t version, void *pReq) {
  if (!pReq) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  SSubmitReq *pSubmitReq = (SSubmitReq *)pReq;

  if (tsdbInsertData(pTsdb, version, pSubmitReq, NULL) < 0) {
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tdFetchSubmitReqSuids(SSubmitReq *pMsg, STbUidStore *pStore) {
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock = NULL;
  SSubmitBlkIter blkIter = {0};
  STSRow        *row = NULL;

  terrno = TSDB_CODE_SUCCESS;

  if (tInitSubmitMsgIter(pMsg, &msgIter) < 0) {
    return -1;
  }
  while (true) {
    if (tGetSubmitMsgNext(&msgIter, &pBlock) < 0) {
      return -1;
    }

    if (!pBlock) break;
    tdUidStorePut(pStore, msgIter.suid, NULL);
  }

  if (terrno != TSDB_CODE_SUCCESS) {
    return -1;
  }
  return 0;
}

static void tdDestroySDataBlockArray(SArray *pArray) {
  // TODO
#if 0
  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    SSDataBlock *pDataBlock = taosArrayGet(pArray, i);
    blockDestroyInner(pDataBlock);
  }
#endif
  taosArrayDestroy(pArray);
}

/**
 * @brief retention of rsma1/rsma2
 *
 * @param pSma
 * @param now
 * @return int32_t
 */
int32_t smaDoRetention(SSma *pSma, int64_t now) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (VND_IS_RSMA(pSma->pVnode)) {
    return code;
  }

  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pSma->pRSmaTsdb[i]) {
      code = tsdbDoRetention(pSma->pRSmaTsdb[i], now);
      if (code) goto _end;
    }
  }

_end:
  return code;
}

static int32_t tdRSmaFetchAndSubmitResult(SSma *pSma, qTaskInfo_t taskInfo, SRSmaInfoItem *pItem, STSchema *pTSchema,
                                          int64_t suid, int8_t blkType) {
  while (1) {
    SSDataBlock *output = NULL;
    uint64_t     ts;

    int32_t code = qExecTask(taskInfo, &output, &ts);
    if (code < 0) {
      smaError("vgId:%d, qExecTask for rsma table %" PRIi64 " level %" PRIi8 " failed since %s", SMA_VID(pSma), suid,
               pItem->level, terrstr(code));
      goto _err;
    }

    if (output) {
#if 0
      char flag[10] = {0};
      snprintf(flag, 10, "level %" PRIi8, pItem->level);
      SArray *pResult = taosArrayInit(1, sizeof(SSDataBlock));
      taosArrayPush(pResult, output);
      blockDebugShowDataBlocks(pResult, flag);
      taosArrayDestroy(pResult);
#endif
      STsdb      *sinkTsdb = (pItem->level == TSDB_RETENTION_L1 ? pSma->pRSmaTsdb[0] : pSma->pRSmaTsdb[1]);
      SSubmitReq *pReq = NULL;
      // TODO: the schema update should be handled later(TD-17965)
      if (buildSubmitReqFromDataBlock(&pReq, output, pTSchema, SMA_VID(pSma), suid) < 0) {
        smaError("vgId:%d, build submit req for rsma stable %" PRIi64 " level %" PRIi8 " failed since %s",
                 SMA_VID(pSma), suid, pItem->level, terrstr());
        goto _err;
      }

      if (pReq && tdProcessSubmitReq(sinkTsdb, output->info.version, pReq) < 0) {
        taosMemoryFreeClear(pReq);
        smaError("vgId:%d, process submit req for rsma stable %" PRIi64 " level %" PRIi8 " failed since %s",
                 SMA_VID(pSma), suid, pItem->level, terrstr());
        goto _err;
      }

      smaDebug("vgId:%d, process submit req for rsma table %" PRIi64 " level %" PRIi8 " version:%" PRIi64,
               SMA_VID(pSma), suid, pItem->level, output->info.version);

      taosMemoryFreeClear(pReq);
    } else if (terrno == 0) {
      smaDebug("vgId:%d, no rsma %" PRIi8 " data fetched yet", SMA_VID(pSma), pItem->level);
      break;
    } else {
      smaDebug("vgId:%d, no rsma %" PRIi8 " data fetched since %s", SMA_VID(pSma), pItem->level, terrstr());
      goto _err;
    }
  }

  return TSDB_CODE_SUCCESS;
_err:
  return TSDB_CODE_FAILED;
}

static int32_t tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t inputType, SRSmaInfo *pInfo, tb_uid_t suid,
                                 int8_t level) {
  int32_t idx = level - 1;
  if (!pInfo || !RSMA_INFO_QTASK(pInfo, idx)) {
    smaDebug("vgId:%d, no qTaskInfo to execute rsma %" PRIi8 " task for suid:%" PRIu64, SMA_VID(pSma), level, suid);
    return TSDB_CODE_SUCCESS;
  }
  if (!pInfo->pTSchema) {
    smaWarn("vgId:%d, no schema to execute rsma %" PRIi8 " task for suid:%" PRIu64, SMA_VID(pSma), level, suid);
    return TSDB_CODE_FAILED;
  }

  smaDebug("vgId:%d, execute rsma %" PRIi8 " task for qTaskInfo:%p suid:%" PRIu64, SMA_VID(pSma), level,
           RSMA_INFO_QTASK(pInfo, idx), suid);

  if (qSetMultiStreamInput(RSMA_INFO_QTASK(pInfo, idx), pMsg, 1, inputType) < 0) {  // INPUT__DATA_SUBMIT
    smaError("vgId:%d, rsma %" PRIi8 " qSetStreamInput failed since %s", SMA_VID(pSma), level, tstrerror(terrno));
    return TSDB_CODE_FAILED;
  }

  SRSmaInfoItem *pItem = RSMA_INFO_ITEM(pInfo, idx);

  tdRSmaFetchAndSubmitResult(pSma, RSMA_INFO_QTASK(pInfo, idx), pItem, pInfo->pTSchema, suid,
                             STREAM_INPUT__DATA_SUBMIT);
  atomic_store_8(&pItem->triggerStat, TASK_TRIGGER_STAT_ACTIVE);

  if (smaMgmt.tmrHandle) {
    taosTmrReset(tdRSmaFetchTrigger, pItem->maxDelay, pItem, smaMgmt.tmrHandle, &pItem->tmrId);
  } else {
    ASSERT(0);
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief During async commit, the SRSmaInfo object would be COW from iRSmaInfoHash and write lock should be applied.
 *
 * @param pSma
 * @param suid
 * @return SRSmaInfo*
 */
static SRSmaInfo *tdAcquireRSmaInfoBySuid(SSma *pSma, int64_t suid) {
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = NULL;
  SRSmaInfo *pRSmaInfo = NULL;

  if (!pEnv) {
    terrno = TSDB_CODE_RSMA_INVALID_ENV;
    return NULL;
  }

  pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  if (!pStat || !RSMA_INFO_HASH(pStat)) {
    terrno = TSDB_CODE_RSMA_INVALID_STAT;
    return NULL;
  }

  taosRLockLatch(SMA_ENV_LOCK(pEnv));
  pRSmaInfo = taosHashGet(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t));
  if (pRSmaInfo && (pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    if (RSMA_INFO_IS_DEL(pRSmaInfo)) {
      taosRUnLockLatch(SMA_ENV_LOCK(pEnv));
      return NULL;
    }
    tdRefRSmaInfo(pSma, pRSmaInfo);
    taosRUnLockLatch(SMA_ENV_LOCK(pEnv));
    return pRSmaInfo;
  }
  taosRUnLockLatch(SMA_ENV_LOCK(pEnv));

  if (RSMA_COMMIT_STAT(pStat) == 0) {  // return NULL if not in committing stat
    return NULL;
  }


  // clone the SRSmaInfo from iRsmaInfoHash to rsmaInfoHash if in committing stat
  SRSmaInfo *pCowRSmaInfo = NULL;
  // lock
  taosWLockLatch(SMA_ENV_LOCK(pEnv));
  if (!(pCowRSmaInfo = taosHashGet(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t)))) {  // 2-phase lock
    void *iRSmaInfo = taosHashGet(RSMA_IMU_INFO_HASH(pStat), &suid, sizeof(tb_uid_t));
    if (iRSmaInfo) {
      SRSmaInfo *pIRSmaInfo = *(SRSmaInfo **)iRSmaInfo;
      if (pIRSmaInfo && !RSMA_INFO_IS_DEL(pIRSmaInfo)) {
        if (tdCloneRSmaInfo(pSma, &pCowRSmaInfo, pIRSmaInfo) < 0) {
          // unlock
          taosWUnLockLatch(SMA_ENV_LOCK(pEnv));
          smaError("vgId:%d, clone rsma info failed for suid:%" PRIu64 " since %s", SMA_VID(pSma), suid, terrstr());
          return NULL;
        }
        smaDebug("vgId:%d, clone rsma info succeed for suid:%" PRIu64, SMA_VID(pSma), suid);
        if (taosHashPut(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t), &pCowRSmaInfo, sizeof(pCowRSmaInfo)) < 0) {
          // unlock
          taosWUnLockLatch(SMA_ENV_LOCK(pEnv));
          smaError("vgId:%d, clone rsma info failed for suid:%" PRIu64 " since %s", SMA_VID(pSma), suid, terrstr());
          return NULL;
        }
      }
    }
  } else {
    pCowRSmaInfo = *(SRSmaInfo **)pCowRSmaInfo;
    ASSERT(!pCowRSmaInfo);
  }

  if (pCowRSmaInfo) {
    tdRefRSmaInfo(pSma, pCowRSmaInfo);
  }
  // unlock
  taosWUnLockLatch(SMA_ENV_LOCK(pEnv));
  return pCowRSmaInfo;
}

static FORCE_INLINE void tdReleaseRSmaInfo(SSma *pSma, SRSmaInfo *pInfo) {
  if (pInfo) {
    tdUnRefRSmaInfo(pSma, pInfo);
  }
}

static int32_t tdExecuteRSma(SSma *pSma, const void *pMsg, int32_t inputType, tb_uid_t suid) {
  SRSmaInfo *pRSmaInfo = tdAcquireRSmaInfoBySuid(pSma, suid);
  if (!pRSmaInfo) {
    smaDebug("vgId:%d, execute rsma, no rsma info for suid:%" PRIu64, SMA_VID(pSma), suid);
    return TSDB_CODE_SUCCESS;
  }

  if (!RSMA_INFO_QTASK(pRSmaInfo, 0)) {
    tdReleaseRSmaInfo(pSma, pRSmaInfo);
    smaDebug("vgId:%d, execute rsma, no rsma qTaskInfo for suid:%" PRIu64, SMA_VID(pSma), suid);
    return TSDB_CODE_SUCCESS;
  }

  if (inputType == STREAM_INPUT__DATA_SUBMIT) {
    tdExecuteRSmaImpl(pSma, pMsg, inputType, pRSmaInfo, suid, TSDB_RETENTION_L1);
    tdExecuteRSmaImpl(pSma, pMsg, inputType, pRSmaInfo, suid, TSDB_RETENTION_L2);
  }

  tdReleaseRSmaInfo(pSma, pRSmaInfo);
  return TSDB_CODE_SUCCESS;
}

int32_t tdProcessRSmaSubmit(SSma *pSma, void *pMsg, int32_t inputType) {
  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);
  if (!pEnv) {
    // only applicable when rsma env exists
    return TSDB_CODE_SUCCESS;
  }

  SRetention *pRetention = SMA_RETENTION(pSma);
  if (!RETENTION_VALID(pRetention + 1)) {
    // return directly if retention level 1 is invalid
    return TSDB_CODE_SUCCESS;
  }

  if (inputType == STREAM_INPUT__DATA_SUBMIT) {
    STbUidStore uidStore = {0};
    tdFetchSubmitReqSuids(pMsg, &uidStore);

    if (uidStore.suid != 0) {
      tdExecuteRSma(pSma, pMsg, inputType, uidStore.suid);

      void *pIter = taosHashIterate(uidStore.uidHash, NULL);
      while (pIter) {
        tb_uid_t *pTbSuid = (tb_uid_t *)taosHashGetKey(pIter, NULL);
        tdExecuteRSma(pSma, pMsg, inputType, *pTbSuid);
        pIter = taosHashIterate(uidStore.uidHash, pIter);
      }

      tdUidStoreDestory(&uidStore);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tdRSmaRestoreQTaskInfoInit(SSma *pSma, int64_t *nTables) {
  SVnode *pVnode = pSma->pVnode;

  SArray *suidList = taosArrayInit(1, sizeof(tb_uid_t));
  if (tsdbGetStbIdList(SMA_META(pSma), 0, suidList) < 0) {
    taosArrayDestroy(suidList);
    smaError("vgId:%d, failed to restore rsma env since get stb id list error: %s", TD_VID(pVnode), terrstr());
    return TSDB_CODE_FAILED;
  }

  int64_t arrSize = taosArrayGetSize(suidList);

  if (nTables) {
    *nTables = arrSize;
  }

  if (arrSize == 0) {
    taosArrayDestroy(suidList);
    smaDebug("vgId:%d, no need to restore rsma env since empty stb id list", TD_VID(pVnode));
    return TSDB_CODE_SUCCESS;
  }

  SMetaReader mr = {0};
  metaReaderInit(&mr, SMA_META(pSma), 0);
  for (int64_t i = 0; i < arrSize; ++i) {
    tb_uid_t suid = *(tb_uid_t *)taosArrayGet(suidList, i);
    smaDebug("vgId:%d, rsma restore, suid is %" PRIi64, TD_VID(pVnode), suid);
    if (metaGetTableEntryByUid(&mr, suid) < 0) {
      smaError("vgId:%d, rsma restore, failed to get table meta for %" PRIi64 " since %s", TD_VID(pVnode), suid,
               terrstr());
      goto _err;
    }
    tDecoderClear(&mr.coder);
    ASSERT(mr.me.type == TSDB_SUPER_TABLE);
    ASSERT(mr.me.uid == suid);
    if (TABLE_IS_ROLLUP(mr.me.flags)) {
      SRSmaParam *param = &mr.me.stbEntry.rsmaParam;
      for (int i = 0; i < TSDB_RETENTION_L2; ++i) {
        smaDebug("vgId:%d, rsma restore, table:%" PRIi64 " level:%d, maxdelay:%" PRIi64 " watermark:%" PRIi64
                 " qmsgLen:%" PRIi32,
                 TD_VID(pVnode), suid, i, param->maxdelay[i], param->watermark[i], param->qmsgLen[i]);
      }
      if (tdProcessRSmaCreateImpl(pSma, &mr.me.stbEntry.rsmaParam, suid, mr.me.name) < 0) {
        smaError("vgId:%d, rsma restore env failed for %" PRIi64 " since %s", TD_VID(pVnode), suid, terrstr());
        goto _err;
      }
      smaDebug("vgId:%d, rsma restore env success for %" PRIi64, TD_VID(pVnode), suid);
    }
  }

  metaReaderClear(&mr);
  taosArrayDestroy(suidList);

  return TSDB_CODE_SUCCESS;
_err:
  metaReaderClear(&mr);
  taosArrayDestroy(suidList);

  return TSDB_CODE_FAILED;
}

static int32_t tdRSmaRestoreQTaskInfoReload(SSma *pSma, int8_t type, int64_t qTaskFileVer) {
  SVnode *pVnode = pSma->pVnode;
  STFile  tFile = {0};
  char    qTaskInfoFName[TSDB_FILENAME_LEN] = {0};

  tdRSmaQTaskInfoGetFileName(TD_VID(pVnode), qTaskFileVer, qTaskInfoFName);
  if (tdInitTFile(&tFile, tfsGetPrimaryPath(pVnode->pTfs), qTaskInfoFName) < 0) {
    goto _err;
  }

  if (!taosCheckExistFile(TD_TFILE_FULL_NAME(&tFile))) {
    if (qTaskFileVer > 0) {
      smaWarn("vgId:%d, restore rsma task %" PRIi8 " for version %" PRIi64 ", not start as %s not exist",
              TD_VID(pVnode), type, qTaskFileVer, TD_TFILE_FULL_NAME(&tFile));
    } else {
      smaDebug("vgId:%d, restore rsma task %" PRIi8 " for version %" PRIi64 ", no need as %s not exist", TD_VID(pVnode),
               type, qTaskFileVer, TD_TFILE_FULL_NAME(&tFile));
    }
    return TSDB_CODE_SUCCESS;
  }

  if (tdOpenTFile(&tFile, TD_FILE_READ) < 0) {
    goto _err;
  }

  STFInfo tFileInfo = {0};
  if (tdLoadTFileHeader(&tFile, &tFileInfo) < 0) {
    goto _err;
  }

  SSmaEnv   *pRSmaEnv = pSma->pRSmaEnv;
  SRSmaStat *pRSmaStat = (SRSmaStat *)SMA_ENV_STAT(pRSmaEnv);

  SRSmaQTaskInfoIter fIter = {0};
  if (tdRSmaQTaskInfoIterInit(&fIter, &tFile) < 0) {
    tdRSmaQTaskInfoIterDestroy(&fIter);
    tdCloseTFile(&tFile);
    tdDestroyTFile(&tFile);
    goto _err;
  }

  if (tdRSmaQTaskInfoRestore(pSma, type, &fIter) < 0) {
    tdRSmaQTaskInfoIterDestroy(&fIter);
    tdCloseTFile(&tFile);
    tdDestroyTFile(&tFile);
    goto _err;
  }

  tdRSmaQTaskInfoIterDestroy(&fIter);
  tdCloseTFile(&tFile);
  tdDestroyTFile(&tFile);

  // restored successfully from committed or sync
  smaInfo("vgId:%d, restore rsma task %" PRIi8 " for version %" PRIi64 ", qtaskinfo reload succeed", TD_VID(pVnode),
          type, qTaskFileVer);
  return TSDB_CODE_SUCCESS;
_err:
  smaError("vgId:%d, restore rsma task %" PRIi8 " for version %" PRIi64 ", qtaskinfo reload failed since %s",
           TD_VID(pVnode), type, qTaskFileVer, terrstr());
  return TSDB_CODE_FAILED;
}

/**
 * @brief reload ts data from checkpoint
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdRSmaRestoreTSDataReload(SSma *pSma) {
  // NOTHING TODO: the data would be restored from the unified WAL replay procedure
  return TSDB_CODE_SUCCESS;
}

int32_t tdProcessRSmaRestoreImpl(SSma *pSma, int8_t type, int64_t qtaskFileVer) {
  // step 1: iterate all stables to restore the rsma env
  int64_t nTables = 0;
  if (tdRSmaRestoreQTaskInfoInit(pSma, &nTables) < 0) {
    goto _err;
  }

  if (nTables <= 0) {
    smaDebug("vgId:%d, no need to restore rsma task %" PRIi8 " since no tables", SMA_VID(pSma), type);
    return TSDB_CODE_SUCCESS;
  }

  // step 2: retrieve qtaskinfo items from the persistence file(rsma/qtaskinfo) and restore
  if (tdRSmaRestoreQTaskInfoReload(pSma, type, qtaskFileVer) < 0) {
    goto _err;
  }

  // step 3: reload ts data from checkpoint
  if (tdRSmaRestoreTSDataReload(pSma) < 0) {
    goto _err;
  }
  smaInfo("vgId:%d, restore rsma task %" PRIi8 " from qtaskf %" PRIi64 " succeed", SMA_VID(pSma), type, qtaskFileVer);
  return TSDB_CODE_SUCCESS;
_err:
  smaError("vgId:%d, restore rsma task %" PRIi8 "from qtaskf %" PRIi64 " failed since %s", SMA_VID(pSma), type,
           qtaskFileVer, terrstr());
  return TSDB_CODE_FAILED;
}

/**
 * @brief Restore from SRSmaQTaskInfoItem
 *
 * @param pSma
 * @param pItem
 * @return int32_t
 */
static int32_t tdRSmaQTaskInfoItemRestore(SSma *pSma, const SRSmaQTaskInfoItem *pItem) {
  SRSmaInfo *pRSmaInfo = NULL;
  void      *qTaskInfo = NULL;

  pRSmaInfo = tdAcquireRSmaInfoBySuid(pSma, pItem->suid);
  if (!pRSmaInfo) {
    smaDebug("vgId:%d, no restore as no rsma info for table:%" PRIu64, SMA_VID(pSma), pItem->suid);
    return TSDB_CODE_SUCCESS;
  }

  if (pItem->type == TSDB_RETENTION_L1) {
    qTaskInfo = RSMA_INFO_QTASK(pRSmaInfo, 0);
  } else if (pItem->type == TSDB_RETENTION_L2) {
    qTaskInfo = RSMA_INFO_QTASK(pRSmaInfo, 1);
  } else {
    ASSERT(0);
  }

  if (!qTaskInfo) {
    tdReleaseRSmaInfo(pSma, pRSmaInfo);
    smaDebug("vgId:%d, no restore as NULL rsma qTaskInfo for table:%" PRIu64, SMA_VID(pSma), pItem->suid);
    return TSDB_CODE_SUCCESS;
  }

  if (qDeserializeTaskStatus(qTaskInfo, pItem->qTaskInfo, pItem->len) < 0) {
    tdReleaseRSmaInfo(pSma, pRSmaInfo);
    smaError("vgId:%d, restore rsma task failed for table:%" PRIi64 " level %d since %s", SMA_VID(pSma), pItem->suid,
             pItem->type, terrstr());
    return TSDB_CODE_FAILED;
  }
  smaDebug("vgId:%d, restore rsma task success for table:%" PRIi64 " level %d", SMA_VID(pSma), pItem->suid,
           pItem->type);

  tdReleaseRSmaInfo(pSma, pRSmaInfo);
  return TSDB_CODE_SUCCESS;
}

static int32_t tdRSmaQTaskInfoIterInit(SRSmaQTaskInfoIter *pIter, STFile *pTFile) {
  memset(pIter, 0, sizeof(*pIter));
  pIter->pTFile = pTFile;
  pIter->offset = TD_FILE_HEAD_SIZE;

  if (tdGetTFileSize(pTFile, &pIter->fsize) < 0) {
    return TSDB_CODE_FAILED;
  }

  if ((pIter->fsize - TD_FILE_HEAD_SIZE) < RSMA_QTASKINFO_BUFSIZE) {
    pIter->nAlloc = pIter->fsize - TD_FILE_HEAD_SIZE;
  } else {
    pIter->nAlloc = RSMA_QTASKINFO_BUFSIZE;
  }

  if (pIter->nAlloc < TD_FILE_HEAD_SIZE) {
    pIter->nAlloc = TD_FILE_HEAD_SIZE;
  }

  pIter->pBuf = taosMemoryMalloc(pIter->nAlloc);
  if (!pIter->pBuf) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }
  pIter->qBuf = pIter->pBuf;

  return TSDB_CODE_SUCCESS;
}

static int32_t tdRSmaQTaskInfoIterNextBlock(SRSmaQTaskInfoIter *pIter, bool *isFinish) {
  STFile *pTFile = pIter->pTFile;
  int64_t nBytes = RSMA_QTASKINFO_BUFSIZE;

  if (pIter->offset >= pIter->fsize) {
    *isFinish = true;
    return TSDB_CODE_SUCCESS;
  }

  if ((pIter->fsize - pIter->offset) < RSMA_QTASKINFO_BUFSIZE) {
    nBytes = pIter->fsize - pIter->offset;
  }

  if (tdSeekTFile(pTFile, pIter->offset, SEEK_SET) < 0) {
    return TSDB_CODE_FAILED;
  }

  if (tdReadTFile(pTFile, pIter->qBuf, nBytes) != nBytes) {
    return TSDB_CODE_FAILED;
  }

  int32_t infoLen = 0;
  taosDecodeFixedI32(pIter->qBuf, &infoLen);
  if (infoLen > nBytes) {
    if (infoLen <= RSMA_QTASKINFO_BUFSIZE) {
      terrno = TSDB_CODE_RSMA_FILE_CORRUPTED;
      smaError("iterate rsma qtaskinfo file %s failed since %s", TD_TFILE_FULL_NAME(pIter->pTFile), terrstr());
      return TSDB_CODE_FAILED;
    }
    pIter->nAlloc = infoLen;
    void *pBuf = taosMemoryRealloc(pIter->pBuf, infoLen);
    if (!pBuf) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_FAILED;
    }
    pIter->pBuf = pBuf;
    pIter->qBuf = pIter->pBuf;
    nBytes = infoLen;

    if (tdSeekTFile(pTFile, pIter->offset, SEEK_SET)) {
      return TSDB_CODE_FAILED;
    }

    if (tdReadTFile(pTFile, pIter->pBuf, nBytes) != nBytes) {
      return TSDB_CODE_FAILED;
    }
  }

  pIter->offset += nBytes;
  pIter->nBytes = nBytes;
  pIter->nBufPos = 0;

  return TSDB_CODE_SUCCESS;
}

static int32_t tdRSmaQTaskInfoRestore(SSma *pSma, int8_t type, SRSmaQTaskInfoIter *pIter) {
  while (1) {
    // block iter
    bool isFinish = false;
    if (tdRSmaQTaskInfoIterNextBlock(pIter, &isFinish) < 0) {
      return TSDB_CODE_FAILED;
    }
    if (isFinish) {
      return TSDB_CODE_SUCCESS;
    }

    // consume the block
    int32_t qTaskInfoLenWithHead = 0;
    pIter->qBuf = taosDecodeFixedI32(pIter->qBuf, &qTaskInfoLenWithHead);
    if (qTaskInfoLenWithHead < RSMA_QTASKINFO_HEAD_LEN) {
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      smaError("vgId:%d, restore rsma task %" PRIi8 " from qtaskinfo file %s failed since %s", SMA_VID(pSma), type,
               TD_TFILE_FULL_NAME(pIter->pTFile), terrstr());
      return TSDB_CODE_FAILED;
    }

    while (1) {
      if ((pIter->nBufPos + qTaskInfoLenWithHead) <= pIter->nBytes) {
        SRSmaQTaskInfoItem infoItem = {0};
        pIter->qBuf = taosDecodeFixedI8(pIter->qBuf, &infoItem.type);
        pIter->qBuf = taosDecodeFixedI64(pIter->qBuf, &infoItem.suid);
        infoItem.qTaskInfo = pIter->qBuf;
        infoItem.len = tdRSmaQTaskInfoContLen(qTaskInfoLenWithHead);
        // do the restore job
        smaDebug("vgId:%d, restore rsma task %" PRIi8 " from qtaskinfo file %s offset:%" PRIi64 "\n", SMA_VID(pSma),
                 type, TD_TFILE_FULL_NAME(pIter->pTFile), pIter->offset - pIter->nBytes + pIter->nBufPos);
        tdRSmaQTaskInfoItemRestore(pSma, &infoItem);

        pIter->qBuf = POINTER_SHIFT(pIter->qBuf, infoItem.len);
        pIter->nBufPos += qTaskInfoLenWithHead;

        if ((pIter->nBufPos + RSMA_QTASKINFO_HEAD_LEN) >= pIter->nBytes) {
          // prepare and load next block in the file
          pIter->offset -= (pIter->nBytes - pIter->nBufPos);
          break;
        }

        pIter->qBuf = taosDecodeFixedI32(pIter->qBuf, &qTaskInfoLenWithHead);
        continue;
      }
      // prepare and load next block in the file
      pIter->offset -= (pIter->nBytes - pIter->nBufPos);
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tdRSmaPersistExecImpl(SRSmaStat *pRSmaStat, SHashObj *pInfoHash) {
  SSma   *pSma = pRSmaStat->pSma;
  SVnode *pVnode = pSma->pVnode;
  int32_t vid = SMA_VID(pSma);
  int64_t toffset = 0;
  bool    isFileCreated = false;

  if (taosHashGetSize(pInfoHash) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  void *infoHash = taosHashIterate(pInfoHash, NULL);
  if (!infoHash) {
    return TSDB_CODE_SUCCESS;
  }

  STFile tFile = {0};
#if 0
  if (pRSmaStat->commitAppliedVer > 0) {
    char qTaskInfoFName[TSDB_FILENAME_LEN];
    tdRSmaQTaskInfoGetFileName(vid, pRSmaStat->commitAppliedVer, qTaskInfoFName);
    if (tdInitTFile(&tFile, tfsGetPrimaryPath(pVnode->pTfs), qTaskInfoFName) < 0) {
      smaError("vgId:%d, rsma persit, init %s failed since %s", vid, qTaskInfoFName, terrstr());
      goto _err;
    }
    if (tdCreateTFile(&tFile, true, TD_FTYPE_RSMA_QTASKINFO) < 0) {
      smaError("vgId:%d, rsma persit, create %s failed since %s", vid, TD_TFILE_FULL_NAME(&tFile), terrstr());
      goto _err;
    }
    smaDebug("vgId:%d, rsma, serialize qTaskInfo, file %s created", vid, TD_TFILE_FULL_NAME(&tFile));

    isFileCreated = true;
  }
#endif

  while (infoHash) {
    SRSmaInfo *pRSmaInfo = *(SRSmaInfo **)infoHash;

    if (RSMA_INFO_IS_DEL(pRSmaInfo)) {
      infoHash = taosHashIterate(pInfoHash, infoHash);
      continue;
    }

    for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
      qTaskInfo_t taskInfo = RSMA_INFO_QTASK(pRSmaInfo, i);
      if (!taskInfo) {
        smaDebug("vgId:%d, rsma, table %" PRIi64 " level %d qTaskInfo is NULL", vid, pRSmaInfo->suid, i + 1);
        continue;
      }

      char   *pOutput = NULL;
      int32_t len = 0;
      int8_t  type = (int8_t)(i + 1);
      if (qSerializeTaskStatus(taskInfo, &pOutput, &len) < 0) {
        smaError("vgId:%d, rsma, table %" PRIi64 " level %d serialize qTaskInfo failed since %s", vid, pRSmaInfo->suid,
                 i + 1, terrstr());
        goto _err;
      }
      if (!pOutput || len <= 0) {
        smaDebug("vgId:%d, rsma, table %" PRIi64
                 " level %d serialize qTaskInfo success but no output(len %d), not persist",
                 vid, pRSmaInfo->suid, i + 1, len);
        taosMemoryFreeClear(pOutput);
        continue;
      }

      smaDebug("vgId:%d, rsma, table %" PRIi64 " level %d serialize qTaskInfo success with len %d, need persist", vid,
               pRSmaInfo->suid, i + 1, len);

      if (!isFileCreated) {
        char qTaskInfoFName[TSDB_FILENAME_LEN];
        tdRSmaQTaskInfoGetFileName(vid, pRSmaStat->commitAppliedVer, qTaskInfoFName);
        if (tdInitTFile(&tFile, tfsGetPrimaryPath(pVnode->pTfs), qTaskInfoFName) < 0) {
          smaError("vgId:%d, rsma persit, init %s failed since %s", vid, qTaskInfoFName, terrstr());
          goto _err;
        }
        if (tdCreateTFile(&tFile, true, TD_FTYPE_RSMA_QTASKINFO) < 0) {
          smaError("vgId:%d, rsma persit, create %s failed since %s", vid, TD_TFILE_FULL_NAME(&tFile), terrstr());
          goto _err;
        }
        smaDebug("vgId:%d, rsma, table %" PRIi64 " serialize qTaskInfo, file %s created", vid, pRSmaInfo->suid,
                 TD_TFILE_FULL_NAME(&tFile));

        isFileCreated = true;
      }

      char    tmpBuf[RSMA_QTASKINFO_HEAD_LEN] = {0};
      void   *pTmpBuf = &tmpBuf;
      int32_t headLen = 0;
      headLen += taosEncodeFixedI32(&pTmpBuf, len + RSMA_QTASKINFO_HEAD_LEN);
      headLen += taosEncodeFixedI8(&pTmpBuf, type);
      headLen += taosEncodeFixedI64(&pTmpBuf, pRSmaInfo->suid);

      ASSERT(headLen <= RSMA_QTASKINFO_HEAD_LEN);
      tdAppendTFile(&tFile, (void *)&tmpBuf, headLen, &toffset);
      smaDebug("vgId:%d, rsma, table %" PRIi64 " level %d head part(len:%d) appended to offset:%" PRIi64, vid,
               pRSmaInfo->suid, i + 1, headLen, toffset);
      tdAppendTFile(&tFile, pOutput, len, &toffset);
      smaDebug("vgId:%d, rsma, table %" PRIi64 " level %d body part len:%d appended to offset:%" PRIi64, vid,
               pRSmaInfo->suid, i + 1, len, toffset);

      taosMemoryFree(pOutput);
    }

    infoHash = taosHashIterate(pInfoHash, infoHash);
  }

  if (isFileCreated) {
    if (tdUpdateTFileHeader(&tFile) < 0) {
      smaError("vgId:%d, rsma, failed to update tfile %s header since %s", vid, TD_TFILE_FULL_NAME(&tFile),
               tstrerror(terrno));
      goto _err;
    } else {
      smaDebug("vgId:%d, rsma, succeed to update tfile %s header", vid, TD_TFILE_FULL_NAME(&tFile));
    }

    tdCloseTFile(&tFile);
    tdDestroyTFile(&tFile);
  }
  return TSDB_CODE_SUCCESS;
_err:
  smaError("vgId:%d, rsma persit failed since %s", vid, terrstr());
  if (isFileCreated) {
    tdRemoveTFile(&tFile);
    tdDestroyTFile(&tFile);
  }
  return TSDB_CODE_FAILED;
}

/**
 * @brief trigger to get rsma result in async mode
 *
 * @param param
 * @param tmrId
 */
static void tdRSmaFetchTrigger(void *param, void *tmrId) {
  SRSmaInfoItem *pItem = param;
  SSma          *pSma = NULL;
  SRSmaInfo     *pRSmaInfo = tdGetRSmaInfoByItem(pItem);

  if (RSMA_INFO_IS_DEL(pRSmaInfo)) {
    return;
  }

  SRSmaStat *pStat = (SRSmaStat *)tdAcquireSmaRef(smaMgmt.rsetId, pRSmaInfo->refId);

  if (!pStat) {
    smaDebug("rsma fetch task not start since already destroyed, rsetId rsetId:%" PRIi64 " refId:%d)", smaMgmt.rsetId,
             pRSmaInfo->refId);
    return;
  }

  pSma = pStat->pSma;

  // if rsma trigger stat in paused, cancelled or finished, not start fetch task
  int8_t rsmaTriggerStat = atomic_load_8(RSMA_TRIGGER_STAT(pStat));
  switch (rsmaTriggerStat) {
    case TASK_TRIGGER_STAT_PAUSED:
    case TASK_TRIGGER_STAT_CANCELLED: {
      tdReleaseSmaRef(smaMgmt.rsetId, pRSmaInfo->refId);
      smaDebug("vgId:%d, not fetch rsma level %" PRIi8 " data since stat is %" PRIi8 ", rsetId rsetId:%" PRIi64
               " refId:%d",
               SMA_VID(pSma), pItem->level, rsmaTriggerStat, smaMgmt.rsetId, pRSmaInfo->refId);
      if (rsmaTriggerStat == TASK_TRIGGER_STAT_PAUSED) {
        taosTmrReset(tdRSmaFetchTrigger, 5000, pItem, smaMgmt.tmrHandle, &pItem->tmrId);
      }
      return;
    }
    default:
      break;
  }

  int8_t fetchTriggerStat =
      atomic_val_compare_exchange_8(&pItem->triggerStat, TASK_TRIGGER_STAT_ACTIVE, TASK_TRIGGER_STAT_INACTIVE);
  switch (fetchTriggerStat) {
    case TASK_TRIGGER_STAT_ACTIVE: {
      smaDebug("vgId:%d, fetch rsma level %" PRIi8 " data for table:%" PRIi64 " since stat is active", SMA_VID(pSma),
               pItem->level, pRSmaInfo->suid);
      // async process
      tdRSmaFetchSend(pSma, pRSmaInfo, pItem->level);
    } break;
    case TASK_TRIGGER_STAT_PAUSED: {
      smaDebug("vgId:%d, not fetch rsma level %" PRIi8 " data for table:%" PRIi64 " since stat is paused",
               SMA_VID(pSma), pItem->level, pRSmaInfo->suid);
    } break;
    case TASK_TRIGGER_STAT_INACTIVE: {
      smaDebug("vgId:%d, not fetch rsma level %" PRIi8 " data for table:%" PRIi64 " since stat is inactive",
               SMA_VID(pSma), pItem->level, pRSmaInfo->suid);
    } break;
    case TASK_TRIGGER_STAT_INIT: {
      smaDebug("vgId:%d, not fetch rsma level %" PRIi8 " data for table:%" PRIi64 " since stat is init", SMA_VID(pSma),
               pItem->level, pRSmaInfo->suid);
    } break;
    default: {
      smaWarn("vgId:%d, not fetch rsma level %" PRIi8 " data for table:%" PRIi64 " since stat is unknown",
              SMA_VID(pSma), pItem->level, pRSmaInfo->suid);
    } break;
  }

_end:
  tdReleaseSmaRef(smaMgmt.rsetId, pRSmaInfo->refId);
}

/**
 * @brief put rsma fetch msg to fetch queue
 *
 * @param pSma
 * @param pInfo
 * @param level
 * @return int32_t
 */
int32_t tdRSmaFetchSend(SSma *pSma, SRSmaInfo *pInfo, int8_t level) {
  SRSmaFetchMsg fetchMsg = { .suid = pInfo->suid, .level = level};
  int32_t       ret = 0;
  int32_t       contLen = 0;
  SEncoder      encoder = {0};
  tEncodeSize(tEncodeSRSmaFetchMsg, &fetchMsg, contLen, ret);
  if (ret < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tEncoderClear(&encoder);
    goto _err;
  }

  void *pBuf = rpcMallocCont(contLen + sizeof(SMsgHead));
  tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SMsgHead)), contLen);
  if (tEncodeSRSmaFetchMsg(&encoder, &fetchMsg) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tEncoderClear(&encoder);
  }
  tEncoderClear(&encoder);
  
  ((SMsgHead *)pBuf)->vgId = SMA_VID(pSma);
  ((SMsgHead *)pBuf)->contLen = contLen + sizeof(SMsgHead);

  SRpcMsg rpcMsg = {
      .code = 0,
      .msgType = TDMT_VND_FETCH_RSMA,
      .pCont = pBuf,
      .contLen = contLen,
  };

  if ((terrno = tmsgPutToQueue(&pSma->pVnode->msgCb, FETCH_QUEUE, &rpcMsg)) != 0) {
    smaError("vgId:%d, failed to put rsma fetch msg into fetch-queue for suid:%" PRIi64 " level:%" PRIi8 " since %s",
             SMA_VID(pSma), pInfo->suid, level, terrstr());
    goto _err;
  }

  smaDebug("vgId:%d, success to put rsma fetch msg into fetch-queue for suid:%" PRIi64 " level:%" PRIi8, SMA_VID(pSma),
           pInfo->suid, level);

  return TSDB_CODE_SUCCESS;
_err:
  return TSDB_CODE_FAILED;
}

/**
 * @brief fetch rsma data of level 2/3 and submit
 * 
 * @param pSma 
 * @param pMsg 
 * @return int32_t 
 */
int32_t smaProcessFetch(SSma *pSma, void *pMsg) {
  SRpcMsg       *pRpcMsg = (SRpcMsg *)pMsg;
  SRSmaFetchMsg  req = {0};
  SDecoder       decoder = {0};
  void          *pBuf = NULL;
  SRSmaInfo     *pInfo = NULL;
  SRSmaInfoItem *pItem = NULL;

  if (!pRpcMsg || pRpcMsg->contLen < sizeof(SMsgHead)) {
    terrno = TSDB_CODE_RSMA_FETCH_MSG_MSSED_UP;
    return -1;
  }

  pBuf = POINTER_SHIFT(pRpcMsg->pCont, sizeof(SMsgHead));

  tDecoderInit(&decoder, pBuf, pRpcMsg->contLen);
  if (tDecodeSRSmaFetchMsg(&decoder, &req) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _err;
  }

  pInfo = tdAcquireRSmaInfoBySuid(pSma, req.suid);
  if (!pInfo) {
    if (terrno == TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_RSMA_EMPTY_INFO;
    }
    smaWarn("vgId:%d, failed to process rsma fetch msg for suid:%" PRIi64 " level:%" PRIi8 " since %s", SMA_VID(pSma),
             req.suid, req.level, terrstr());
    goto _err;
  }

  pItem = RSMA_INFO_ITEM(pInfo, req.level - 1);

  SSDataBlock dataBlock = {.info.type = STREAM_GET_ALL};
  qTaskInfo_t taskInfo = RSMA_INFO_QTASK(pInfo, req.level - 1);
  if ((terrno = qSetMultiStreamInput(taskInfo, &dataBlock, 1, STREAM_INPUT__DATA_BLOCK)) < 0) {
    goto _err;
  }
  if (tdRSmaFetchAndSubmitResult(pSma, taskInfo, pItem, pInfo->pTSchema, pInfo->suid, STREAM_INPUT__DATA_BLOCK) < 0) {
    goto _err;
  }

  tdCleanupStreamInputDataBlock(taskInfo);

  tdReleaseRSmaInfo(pSma, pInfo);
  tDecoderClear(&decoder);
  smaDebug("vgId:%d, success to process rsma fetch msg for suid:%" PRIi64 " level:%" PRIi8, SMA_VID(pSma), req.suid,
           req.level);
  return TSDB_CODE_SUCCESS;
_err:
  tdReleaseRSmaInfo(pSma, pInfo);
  tDecoderClear(&decoder);
  smaError("vgId:%d, failed to process rsma fetch msg since %s", SMA_VID(pSma), terrstr());
  return TSDB_CODE_FAILED;
}
