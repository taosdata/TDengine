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

#define RSMA_QTASKINFO_PERSIST_MS 7200000
#define RSMA_QTASKINFO_BUFSIZE    32768
#define RSMA_QTASKINFO_HEAD_LEN   (sizeof(int32_t) + sizeof(int8_t) + sizeof(int64_t))  // len + type + suid

SSmaMgmt smaMgmt = {
    .smaRef = -1,
};

typedef enum { TD_QTASK_TMP_F = 0, TD_QTASK_CUR_F } TD_QTASK_FILE_T;
static const char                *tdQTaskInfoFname[] = {"qtaskinfo.t", "qtaskinfo"};
typedef struct SRSmaQTaskInfoItem SRSmaQTaskInfoItem;
typedef struct SRSmaQTaskInfoIter SRSmaQTaskInfoIter;

static int32_t tdUidStorePut(STbUidStore *pStore, tb_uid_t suid, tb_uid_t *uid);
static int32_t tdUpdateTbUidListImpl(SSma *pSma, tb_uid_t *suid, SArray *tbUids);
static int32_t tdSetRSmaInfoItemParams(SSma *pSma, SRSmaParam *param, SRSmaInfo *pRSmaInfo, SReadHandle *handle,
                                       int8_t idx);
static int32_t tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t inputType, SRSmaInfoItem *rsmaItem,
                                 tb_uid_t suid, int8_t level);
static void    tdRSmaFetchTrigger(void *param, void *tmrId);
static void    tdRSmaPersistTrigger(void *param, void *tmrId);
static void   *tdRSmaPersistExec(void *param);
static void    tdRSmaQTaskInfoGetFName(int32_t vid, int8_t ftype, char *outputName);

static int32_t tdRSmaQTaskInfoIterInit(SRSmaQTaskInfoIter *pIter, STFile *pTFile);
static int32_t tdRSmaQTaskInfoIterNextBlock(SRSmaQTaskInfoIter *pIter, bool *isFinish);
static int32_t tdRSmaQTaskInfoRestore(SSma *pSma, SRSmaQTaskInfoIter *pIter);
static int32_t tdRSmaQTaskInfoItemRestore(SSma *pSma, const SRSmaQTaskInfoItem *infoItem);

static int32_t tdRSmaRestoreQTaskInfoInit(SSma *pSma);
static int32_t tdRSmaRestoreQTaskInfoReload(SSma *pSma);
static int32_t tdRSmaRestoreTSDataReload(SSma *pSma);

struct SRSmaInfoItem {
  SRSmaInfo *pRsmaInfo;
  int64_t    refId;
  void      *taskInfo;  // qTaskInfo_t
  tmr_h      tmrId;
  int8_t     level;
  int8_t     tmrInitFlag;
  int8_t     triggerStat;
  int32_t    maxDelay;
};

struct SRSmaInfo {
  STSchema     *pTSchema;
  SRSmaStat    *pStat;
  int64_t       suid;
  SRSmaInfoItem items[TSDB_RETENTION_L2];
};

#define RSMA_INFO_SMA(r)  ((r)->pStat->pSma)
#define RSMA_INFO_STAT(r) ((r)->pStat)

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

static void tdRSmaQTaskInfoGetFName(int32_t vgId, int8_t ftype, char *outputName) {
  tdGetVndFileName(vgId, VNODE_RSMA_DIR, tdQTaskInfoFname[ftype], outputName);
}

static FORCE_INLINE int32_t tdRSmaQTaskInfoContLen(int32_t lenWithHead) {
  return lenWithHead - RSMA_QTASKINFO_HEAD_LEN;
}

static FORCE_INLINE void tdRSmaQTaskInfoIterDestroy(SRSmaQTaskInfoIter *pIter) { taosMemoryFreeClear(pIter->pBuf); }

static FORCE_INLINE void tdFreeTaskHandle(qTaskInfo_t *taskHandle, int32_t vgId, int32_t level) {
  // Note: free/kill may in RC
  qTaskInfo_t otaskHandle = atomic_load_ptr(taskHandle);
  if (otaskHandle && atomic_val_compare_exchange_ptr(taskHandle, otaskHandle, NULL)) {
    smaDebug("vgId:%d, free qTaskInfo_t %p of level %d", vgId, otaskHandle, level);
    qDestroyTask(otaskHandle);
  } else {
    smaDebug("vgId:%d, not free qTaskInfo_t %p of level %d", vgId, otaskHandle, level);
  }
}

void *tdFreeRSmaInfo(SRSmaInfo *pInfo) {
  if (pInfo) {
    SSma *pSma = RSMA_INFO_SMA(pInfo);
    for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
      SRSmaInfoItem *pItem = &pInfo->items[i];
      if (pItem->taskInfo) {
        smaDebug("vgId:%d, stb %" PRIi64 " stop fetch-timer %p level %d", SMA_VID(pSma), pInfo->suid, pItem->tmrId,
                 i + 1);
        taosTmrStopA(&pItem->tmrId);
        tdFreeTaskHandle(&pItem->taskInfo, SMA_VID(pSma), i + 1);
      } else {
        smaDebug("vgId:%d, stb %" PRIi64 " no need to destroy rsma info level %d since empty taskInfo", SMA_VID(pSma),
                 pInfo->suid, i + 1);
      }
    }
    taosMemoryFree(pInfo->pTSchema);
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
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  if (!suid || !tbUids) {
    terrno = TSDB_CODE_INVALID_PTR;
    smaError("vgId:%d, failed to get rsma info for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  }

  pRSmaInfo = taosHashGet(RSMA_INFO_HASH(pStat), suid, sizeof(tb_uid_t));
  if (!pRSmaInfo || !(pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    smaError("vgId:%d, failed to get rsma info for uid:%" PRIi64, SMA_VID(pSma), *suid);
    terrno = TSDB_CODE_RSMA_INVALID_STAT;
    return TSDB_CODE_FAILED;
  }

  if (pRSmaInfo->items[0].taskInfo) {
    if ((qUpdateQualifiedTableId(pRSmaInfo->items[0].taskInfo, tbUids, true) < 0)) {
      smaError("vgId:%d, update tbUidList failed for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr(terrno));
      return TSDB_CODE_FAILED;
    } else {
      smaDebug("vgId:%d, update tbUidList succeed for qTaskInfo:%p with suid:%" PRIi64 ", uid:%" PRIi64, SMA_VID(pSma),
               pRSmaInfo->items[0].taskInfo, *suid, *(int64_t *)taosArrayGet(tbUids, 0));
    }
  }

  if (pRSmaInfo->items[1].taskInfo) {
    if ((qUpdateQualifiedTableId(pRSmaInfo->items[1].taskInfo, tbUids, true) < 0)) {
      smaError("vgId:%d, update tbUidList failed for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr(terrno));
      return TSDB_CODE_FAILED;
    } else {
      smaDebug("vgId:%d, update tbUidList succeed for qTaskInfo:%p with suid:%" PRIi64 ", uid:%" PRIi64, SMA_VID(pSma),
               pRSmaInfo->items[1].taskInfo, *suid, *(int64_t *)taosArrayGet(tbUids, 0));
    }
  }

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

static int32_t tdSetRSmaInfoItemParams(SSma *pSma, SRSmaParam *param, SRSmaInfo *pRSmaInfo, SReadHandle *pReadHandle,
                                       int8_t idx) {
  SRetention *pRetention = SMA_RETENTION(pSma);
  STsdbCfg   *pTsdbCfg = SMA_TSDB_CFG(pSma);

  if (param->qmsg[idx]) {
    SRSmaInfoItem *pItem = &(pRSmaInfo->items[idx]);
    pItem->refId = RSMA_REF_ID(pRSmaInfo->pStat);
    pItem->pRsmaInfo = pRSmaInfo;
    pItem->taskInfo = qCreateStreamExecTaskInfo(param->qmsg[idx], pReadHandle);
    if (!pItem->taskInfo) {
      terrno = TSDB_CODE_RSMA_QTASKINFO_CREATE;
      goto _err;
    }
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
    pItem->level = (idx == 0 ? TSDB_RETENTION_L1 : TSDB_RETENTION_L2);
  }
  return TSDB_CODE_SUCCESS;
_err:
  return TSDB_CODE_FAILED;
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
  SVnode *pVnode = pSma->pVnode;
  SMeta  *pMeta = pVnode->pMeta;
  SMsgCb *pMsgCb = &pVnode->msgCb;

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
    ASSERT(0);  // TODO: free original pRSmaInfo if exists abnormally
    smaDebug("vgId:%d, rsma info already exists for table %s, %" PRIi64, SMA_VID(pSma), tbName, suid);
    return TSDB_CODE_SUCCESS;
  }

  // from write queue: single thead
  pRSmaInfo = (SRSmaInfo *)taosMemoryCalloc(1, sizeof(SRSmaInfo));
  if (!pRSmaInfo) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  SStreamReader *pReadHandle = tqInitSubmitMsgScanner(pMeta);
  if (!pReadHandle) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  SReadHandle handle = {
      .reader = pReadHandle,
      .meta = pMeta,
      .pMsgCb = pMsgCb,
      .vnode = pVnode,
  };

  STSchema *pTSchema = metaGetTbTSchema(SMA_META(pSma), suid, -1);
  if (!pTSchema) {
    terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
    goto _err;
  }
  pRSmaInfo->pTSchema = pTSchema;
  pRSmaInfo->pStat = pStat;
  pRSmaInfo->suid = suid;

  if (tdSetRSmaInfoItemParams(pSma, param, pRSmaInfo, &handle, 0) < 0) {
    goto _err;
  }

  if (tdSetRSmaInfoItemParams(pSma, param, pRSmaInfo, &handle, 1) < 0) {
    goto _err;
  }

  if (taosHashPut(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t), &pRSmaInfo, sizeof(pRSmaInfo)) < 0) {
    goto _err;
  }

  smaDebug("vgId:%d, register rsma info succeed for suid:%" PRIi64, SMA_VID(pSma), suid);

  // start the persist timer
  if (TASK_TRIGGER_STAT_INIT ==
      atomic_val_compare_exchange_8(RSMA_TRIGGER_STAT(pStat), TASK_TRIGGER_STAT_INIT, TASK_TRIGGER_STAT_ACTIVE)) {
    taosTmrStart(tdRSmaPersistTrigger, RSMA_QTASKINFO_PERSIST_MS, pStat, RSMA_TMR_HANDLE(pStat));
  }

  return TSDB_CODE_SUCCESS;
_err:
  tdFreeRSmaInfo(pRSmaInfo);
  taosMemoryFree(pReadHandle);
  return TSDB_CODE_FAILED;
}

/**
 * @brief Check and init qTaskInfo_t, only applicable to stable with SRSmaParam currently
 *
 * @param pVnode
 * @param pReq
 * @return int32_t
 */
int32_t tdProcessRSmaCreate(SVnode *pVnode, SVCreateStbReq *pReq) {
  SSma *pSma = pVnode->pSma;
  if (!pReq->rollup) {
    smaTrace("vgId:%d, return directly since no rollup for stable %s %" PRIi64, SMA_VID(pSma), pReq->name, pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  return tdProcessRSmaCreateImpl(pSma, &pReq->rsmaParam, pReq->suid, pReq->name);
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
  ASSERT(pMsg != NULL);
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock = NULL;
  SSubmitBlkIter blkIter = {0};
  STSRow        *row = NULL;

  terrno = TSDB_CODE_SUCCESS;

  if (tInitSubmitMsgIter(pMsg, &msgIter) < 0) return -1;
  while (true) {
    if (tGetSubmitMsgNext(&msgIter, &pBlock) < 0) return -1;

    if (!pBlock) break;
    tdUidStorePut(pStore, msgIter.suid, NULL);
  }

  if (terrno != TSDB_CODE_SUCCESS) return -1;
  return 0;
}

static void tdDestroySDataBlockArray(SArray *pArray) {
#if 0
  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    SSDataBlock *pDataBlock = taosArrayGet(pArray, i);
    blockDestroyInner(pDataBlock);
  }
#endif
  taosArrayDestroy(pArray);
}

static int32_t tdFetchAndSubmitRSmaResult(SRSmaInfoItem *pItem, int8_t blkType) {
  SArray    *pResult = NULL;
  SRSmaInfo *pRSmaInfo = pItem->pRsmaInfo;
  SSma      *pSma = RSMA_INFO_SMA(pRSmaInfo);

  while (1) {
    SSDataBlock *output = NULL;
    uint64_t     ts;
    if (qExecTask(pItem->taskInfo, &output, &ts) < 0) {
      ASSERT(false);
    }
    if (!output) {
      break;
    }

    if (!pResult) {
      pResult = taosArrayInit(1, sizeof(SSDataBlock));
      if (!pResult) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return TSDB_CODE_FAILED;
      }
    }

    taosArrayPush(pResult, output);
  }

  if (taosArrayGetSize(pResult) > 0) {
#if 1
    char flag[10] = {0};
    snprintf(flag, 10, "level %" PRIi8, pItem->level);
    blockDebugShowDataBlocks(pResult, flag);
#endif
    STsdb      *sinkTsdb = (pItem->level == TSDB_RETENTION_L1 ? pSma->pRSmaTsdb1 : pSma->pRSmaTsdb2);
    SSubmitReq *pReq = NULL;
    // TODO: the schema update should be handled
    if (buildSubmitReqFromDataBlock(&pReq, pResult, pRSmaInfo->pTSchema, SMA_VID(pSma), pRSmaInfo->suid) < 0) {
      goto _err;
    }

    if (pReq && tdProcessSubmitReq(sinkTsdb, INT64_MAX, pReq) < 0) {
      taosMemoryFreeClear(pReq);
      goto _err;
    }

    taosMemoryFreeClear(pReq);
  } else if (terrno == 0) {
    smaDebug("vgId:%d, no rsma %" PRIi8 " data fetched yet", SMA_VID(pSma), pItem->level);
  } else {
    smaDebug("vgId:%d, no rsma %" PRIi8 " data fetched since %s", SMA_VID(pSma), pItem->level, tstrerror(terrno));
  }

  tdDestroySDataBlockArray(pResult);
  return TSDB_CODE_SUCCESS;
_err:
  tdDestroySDataBlockArray(pResult);
  return TSDB_CODE_FAILED;
}

/**
 * @brief trigger to get rsma result
 *
 * @param param
 * @param tmrId
 */
static void tdRSmaFetchTrigger(void *param, void *tmrId) {
  SRSmaInfoItem *pItem = param;
  SSma          *pSma = NULL;
  SRSmaStat     *pStat = (SRSmaStat *)taosAcquireRef(smaMgmt.smaRef, pItem->refId);
  if (!pStat) {
    smaDebug("rsma fetch task not start since already destroyed");
    return;
  }

  pSma = RSMA_INFO_SMA(pItem->pRsmaInfo);

  // if rsma trigger stat in cancelled or finished, not start fetch task anymore
  int8_t rsmaTriggerStat = atomic_load_8(RSMA_TRIGGER_STAT(pStat));
  if (rsmaTriggerStat == TASK_TRIGGER_STAT_CANCELLED || rsmaTriggerStat == TASK_TRIGGER_STAT_FINISHED) {
    taosReleaseRef(smaMgmt.smaRef, pItem->refId);
    smaDebug("vgId:%d, not fetch rsma level %" PRIi8 " data for table:%" PRIi64 " since stat is cancelled",
             SMA_VID(pSma), pItem->level, pItem->pRsmaInfo->suid);
    return;
  }

  int8_t fetchTriggerStat =
      atomic_val_compare_exchange_8(&pItem->triggerStat, TASK_TRIGGER_STAT_ACTIVE, TASK_TRIGGER_STAT_INACTIVE);
  if (fetchTriggerStat == TASK_TRIGGER_STAT_ACTIVE) {
    smaDebug("vgId:%d, fetch rsma level %" PRIi8 " data for table:%" PRIi64 " since stat is active", SMA_VID(pSma),
             pItem->level, pItem->pRsmaInfo->suid);

    tdRefSmaStat(pSma, (SSmaStat *)pStat);

    SSDataBlock dataBlock = {.info.type = STREAM_GET_ALL};
    qSetStreamInput(pItem->taskInfo, &dataBlock, STREAM_INPUT__DATA_BLOCK, false);
    tdFetchAndSubmitRSmaResult(pItem, STREAM_INPUT__DATA_BLOCK);

    tdUnRefSmaStat(pSma, (SSmaStat *)pStat);

  } else {
    smaDebug("vgId:%d, not fetch rsma level %" PRIi8 " data for table:%" PRIi64 " since stat is inactive",
             SMA_VID(pSma), pItem->level, pItem->pRsmaInfo->suid);
  }
_end:
  taosReleaseRef(smaMgmt.smaRef, pItem->refId);
}

static int32_t tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t inputType, SRSmaInfoItem *pItem, tb_uid_t suid,
                                 int8_t level) {
  if (!pItem || !pItem->taskInfo) {
    smaDebug("vgId:%d, no qTaskInfo to execute rsma %" PRIi8 " task for suid:%" PRIu64, SMA_VID(pSma), level, suid);
    return TSDB_CODE_SUCCESS;
  }

  smaDebug("vgId:%d, execute rsma %" PRIi8 " task for qTaskInfo:%p suid:%" PRIu64, SMA_VID(pSma), level,
           pItem->taskInfo, suid);

  if (qSetStreamInput(pItem->taskInfo, pMsg, inputType, true) < 0) {  // INPUT__DATA_SUBMIT
    smaError("vgId:%d, rsma % " PRIi8 " qSetStreamInput failed since %s", SMA_VID(pSma), level, tstrerror(terrno));
    return TSDB_CODE_FAILED;
  }

  tdFetchAndSubmitRSmaResult(pItem, STREAM_INPUT__DATA_SUBMIT);
  atomic_store_8(&pItem->triggerStat, TASK_TRIGGER_STAT_ACTIVE);

  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = SMA_RSMA_STAT(pEnv->pStat);

  if (pStat->tmrHandle) {
    taosTmrReset(tdRSmaFetchTrigger, pItem->maxDelay, pItem, pStat->tmrHandle, &pItem->tmrId);
  } else {
    ASSERT(0);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tdExecuteRSma(SSma *pSma, const void *pMsg, int32_t inputType, tb_uid_t suid) {
  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);
  if (!pEnv) {
    // only applicable when rsma env exists
    return TSDB_CODE_SUCCESS;
  }

  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  pRSmaInfo = taosHashGet(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t));

  if (!pRSmaInfo || !(pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    smaDebug("vgId:%d, return as no rsma info for suid:%" PRIu64, SMA_VID(pSma), suid);
    return TSDB_CODE_SUCCESS;
  }

  if (!pRSmaInfo->items[0].taskInfo) {
    smaDebug("vgId:%d, return as no rsma qTaskInfo for suid:%" PRIu64, SMA_VID(pSma), suid);
    return TSDB_CODE_SUCCESS;
  }

  if (inputType == STREAM_INPUT__DATA_SUBMIT) {
    tdExecuteRSmaImpl(pSma, pMsg, inputType, &pRSmaInfo->items[0], suid, TSDB_RETENTION_L1);
    tdExecuteRSmaImpl(pSma, pMsg, inputType, &pRSmaInfo->items[1], suid, TSDB_RETENTION_L2);
  }

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

static int32_t tdRSmaRestoreQTaskInfoInit(SSma *pSma) {
  SVnode *pVnode = pSma->pVnode;

  SArray *suidList = taosArrayInit(1, sizeof(tb_uid_t));
  if (tsdbGetStbIdList(SMA_META(pSma), 0, suidList) < 0) {
    taosArrayDestroy(suidList);
    smaError("vgId:%d, failed to restore rsma env since get stb id list error: %s", TD_VID(pVnode), terrstr());
    return TSDB_CODE_FAILED;
  }

  int32_t arrSize = taosArrayGetSize(suidList);
  if (arrSize == 0) {
    taosArrayDestroy(suidList);
    smaDebug("vgId:%d, no need to restore rsma env since empty stb id list", TD_VID(pVnode));
    return TSDB_CODE_SUCCESS;
  }

  SMetaReader mr = {0};
  metaReaderInit(&mr, SMA_META(pSma), 0);
  for (int32_t i = 0; i < arrSize; ++i) {
    tb_uid_t suid = *(tb_uid_t *)taosArrayGet(suidList, i);
    smaDebug("vgId:%d, rsma restore, suid[%d] is %" PRIi64, TD_VID(pVnode), i, suid);
    if (metaGetTableEntryByUid(&mr, suid) < 0) {
      smaError("vgId:%d, rsma restore, failed to get table meta for %" PRIi64 " since %s", TD_VID(pVnode), suid,
               terrstr());
      goto _err;
    }
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

static int32_t tdRSmaRestoreQTaskInfoReload(SSma *pSma) {
  SVnode *pVnode = pSma->pVnode;
  STFile  tFile = {0};
  char    qTaskInfoFName[TSDB_FILENAME_LEN];

  tdRSmaQTaskInfoGetFName(TD_VID(pVnode), TD_QTASK_TMP_F, qTaskInfoFName);
  if (tdInitTFile(&tFile, pVnode->pTfs, qTaskInfoFName) < 0) {
    goto _err;
  }

  if (!taosCheckExistFile(TD_TFILE_FULL_NAME(&tFile))) {
    return TSDB_CODE_SUCCESS;
  }

  if (tdOpenTFile(&tFile, TD_FILE_READ) < 0) {
    goto _err;
  }

  SRSmaQTaskInfoIter fIter = {0};
  if (tdRSmaQTaskInfoIterInit(&fIter, &tFile) < 0) {
    tdRSmaQTaskInfoIterDestroy(&fIter);
    tdCloseTFile(&tFile);
    goto _err;
  }

  if (tdRSmaQTaskInfoRestore(pSma, &fIter) < 0) {
    tdRSmaQTaskInfoIterDestroy(&fIter);
    tdCloseTFile(&tFile);
    goto _err;
  }

  tdRSmaQTaskInfoIterDestroy(&fIter);
  tdCloseTFile(&tFile);
  return TSDB_CODE_SUCCESS;
_err:
  smaError("rsma restore, qtaskinfo reload failed since %s", terrstr());
  return TSDB_CODE_FAILED;
}

/**
 * @brief reload ts data from checkpoint
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdRSmaRestoreTSDataReload(SSma *pSma) {
  // TODO
  return TSDB_CODE_SUCCESS;
_err:
  smaError("rsma restore, ts data reload failed since %s", terrstr());
  return TSDB_CODE_FAILED;
}

int32_t tdProcessRSmaRestoreImpl(SSma *pSma) {
  // step 1: iterate all stables to restore the rsma env
  if (tdRSmaRestoreQTaskInfoInit(pSma) < 0) {
    goto _err;
  }

  // step 2: retrieve qtaskinfo items from the persistence file(rsma/qtaskinfo) and restore
  if (tdRSmaRestoreQTaskInfoReload(pSma) < 0) {
    goto _err;
  }

  // step 3: reload ts data from checkpoint
  if (tdRSmaRestoreTSDataReload(pSma) < 0) {
    goto _err;
  }

  return TSDB_CODE_SUCCESS;
_err:
  smaError("failed to restore rsma task since %s", terrstr());
  return TSDB_CODE_FAILED;
}

static int32_t tdRSmaQTaskInfoItemRestore(SSma *pSma, const SRSmaQTaskInfoItem *pItem) {
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT((SSmaEnv *)pSma->pRSmaEnv);
  SRSmaInfo *pRSmaInfo = NULL;
  void      *qTaskInfo = NULL;

  pRSmaInfo = taosHashGet(RSMA_INFO_HASH(pStat), &pItem->suid, sizeof(pItem->suid));

  if (!pRSmaInfo || !(pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    smaDebug("vgId:%d, no restore as no rsma info for table:%" PRIu64, SMA_VID(pSma), pItem->suid);
    return TSDB_CODE_SUCCESS;
  }

  if (pItem->type == 1) {
    qTaskInfo = pRSmaInfo->items[0].taskInfo;
  } else if (pItem->type == 2) {
    qTaskInfo = pRSmaInfo->items[1].taskInfo;
  } else {
    ASSERT(0);
  }

  if (!qTaskInfo) {
    smaDebug("vgId:%d, no restore as NULL rsma qTaskInfo for table:%" PRIu64, SMA_VID(pSma), pItem->suid);
    return TSDB_CODE_SUCCESS;
  }

  if (qDeserializeTaskStatus(qTaskInfo, pItem->qTaskInfo, pItem->len) < 0) {
    smaError("vgId:%d, restore rsma task failed for table:%" PRIi64 " level %d since %s", SMA_VID(pSma), pItem->suid,
             pItem->type, terrstr(terrno));
    return TSDB_CODE_FAILED;
  }
  smaDebug("vgId:%d, restore rsma task success for table:%" PRIi64 " level %d", SMA_VID(pSma), pItem->suid,
           pItem->type);

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
    ASSERT(0);
    return TSDB_CODE_FAILED;
  }

  if (tdReadTFile(pTFile, pIter->qBuf, nBytes) != nBytes) {
    ASSERT(0);
    return TSDB_CODE_FAILED;
  }

  int32_t infoLen = 0;
  taosDecodeFixedI32(pIter->qBuf, &infoLen);
  if (infoLen > nBytes) {
    ASSERT(infoLen > RSMA_QTASKINFO_BUFSIZE);
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
      ASSERT(0);
      return TSDB_CODE_FAILED;
    }

    if (tdReadTFile(pTFile, pIter->pBuf, nBytes) != nBytes) {
      ASSERT(0);
      return TSDB_CODE_FAILED;
    }
  }

  pIter->offset += nBytes;
  pIter->nBytes = nBytes;
  pIter->nBufPos = 0;

  return TSDB_CODE_SUCCESS;
}

static int32_t tdRSmaQTaskInfoRestore(SSma *pSma, SRSmaQTaskInfoIter *pIter) {
  while (1) {
    // block iter
    bool isFinish = false;
    if (tdRSmaQTaskInfoIterNextBlock(pIter, &isFinish) < 0) {
      ASSERT(0);
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
        smaDebug("vgId:%d, restore the qtask info %s offset:%" PRIi64 "\n", SMA_VID(pSma),
                 TD_TFILE_FULL_NAME(pIter->pTFile), pIter->offset - pIter->nBytes + pIter->nBufPos);
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

static void *tdRSmaPersistExec(void *param) {
  setThreadName("rsma-task-persist");
  SRSmaStat *pRSmaStat = param;
  SSma      *pSma = pRSmaStat->pSma;
  STfs      *pTfs = pSma->pVnode->pTfs;
  int32_t    vid = SMA_VID(pSma);
  int64_t    toffset = 0;
  bool       isFileCreated = false;

  if (TASK_TRIGGER_STAT_CANCELLED == atomic_load_8(RSMA_TRIGGER_STAT(pRSmaStat))) {
    goto _end;
  }

  void *infoHash = taosHashIterate(RSMA_INFO_HASH(pRSmaStat), NULL);
  if (!infoHash) {
    goto _end;
  }

  STFile tFile = {0};
  while (infoHash) {
    SRSmaInfo *pRSmaInfo = *(SRSmaInfo **)infoHash;
    for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
      qTaskInfo_t taskInfo = pRSmaInfo->items[i].taskInfo;
      if (!taskInfo) {
        smaDebug("vgId:%d, rsma, table %" PRIi64 " level %d qTaskInfo is NULL", vid, pRSmaInfo->suid, i + 1);
        continue;
      }

      char   *pOutput = NULL;
      int32_t len = 0;
      int8_t  type = (int8_t)(i + 1);
      if (qSerializeTaskStatus(taskInfo, &pOutput, &len) < 0) {
        smaError("vgId:%d, rsma, table %" PRIi64 " level %d serialize qTaskInfo failed since %s", vid, pRSmaInfo->suid,
                 i + 1, terrstr(terrno));
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
        tdRSmaQTaskInfoGetFName(vid, TD_QTASK_TMP_F, qTaskInfoFName);
        tdInitTFile(&tFile, pTfs, qTaskInfoFName);
        tdCreateTFile(&tFile, pTfs, true, -1);

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

    infoHash = taosHashIterate(RSMA_INFO_HASH(pRSmaStat), infoHash);
  }
_normal:
  if (isFileCreated) {
    if (tdUpdateTFileHeader(&tFile) < 0) {
      smaError("vgId:%d, rsma, failed to update tfile %s header since %s", vid, TD_TFILE_FULL_NAME(&tFile),
               tstrerror(terrno));
      tdCloseTFile(&tFile);
      tdRemoveTFile(&tFile);
      goto _err;
    } else {
      smaDebug("vgId:%d, rsma, succeed to update tfile %s header", vid, TD_TFILE_FULL_NAME(&tFile));
    }

    tdCloseTFile(&tFile);

    char newFName[TSDB_FILENAME_LEN];
    strncpy(newFName, TD_TFILE_FULL_NAME(&tFile), TSDB_FILENAME_LEN);
    char *pos = strstr(newFName, tdQTaskInfoFname[TD_QTASK_TMP_F]);
    strncpy(pos, tdQTaskInfoFname[TD_QTASK_TMP_F], TSDB_FILENAME_LEN - POINTER_DISTANCE(pos, newFName));
    if (taosRenameFile(TD_TFILE_FULL_NAME(&tFile), newFName) != 0) {
      smaError("vgId:%d, rsma, failed to rename %s to %s", vid, TD_TFILE_FULL_NAME(&tFile), newFName);
      goto _err;
    } else {
      smaDebug("vgId:%d, rsma, succeed to rename %s to %s", vid, TD_TFILE_FULL_NAME(&tFile), newFName);
    }
  }
  goto _end;
_err:
  if (isFileCreated) {
    tdRemoveTFile(&tFile);
  }
_end:
  if (TASK_TRIGGER_STAT_INACTIVE == atomic_val_compare_exchange_8(RSMA_TRIGGER_STAT(pRSmaStat),
                                                                  TASK_TRIGGER_STAT_INACTIVE,
                                                                  TASK_TRIGGER_STAT_ACTIVE)) {
    smaDebug("vgId:%d, rsma persist task is active again", vid);
  } else if (TASK_TRIGGER_STAT_CANCELLED == atomic_val_compare_exchange_8(RSMA_TRIGGER_STAT(pRSmaStat),
                                                                          TASK_TRIGGER_STAT_CANCELLED,
                                                                          TASK_TRIGGER_STAT_FINISHED)) {
    smaDebug("vgId:%d, rsma persist task is cancelled", vid);
  } else {
    smaWarn("vgId:%d, rsma persist task in abnormal stat %" PRIi8, vid, atomic_load_8(RSMA_TRIGGER_STAT(pRSmaStat)));
    ASSERT(0);
  }
  atomic_store_8(RSMA_RUNNING_STAT(pRSmaStat), 0);
  taosReleaseRef(smaMgmt.smaRef, pRSmaStat->refId);
  taosThreadExit(NULL);
  return NULL;
}

static void tdRSmaPersistTask(SRSmaStat *pRSmaStat) {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_DETACHED);
  TdThread tid;

  if (taosThreadCreate(&tid, &thAttr, tdRSmaPersistExec, pRSmaStat) != 0) {
    if (TASK_TRIGGER_STAT_INACTIVE == atomic_val_compare_exchange_8(RSMA_TRIGGER_STAT(pRSmaStat),
                                                                    TASK_TRIGGER_STAT_INACTIVE,
                                                                    TASK_TRIGGER_STAT_ACTIVE)) {
      smaDebug("vgId:%d, persist task is active again", SMA_VID(pRSmaStat->pSma));
    } else if (TASK_TRIGGER_STAT_CANCELLED == atomic_val_compare_exchange_8(RSMA_TRIGGER_STAT(pRSmaStat),
                                                                            TASK_TRIGGER_STAT_CANCELLED,
                                                                            TASK_TRIGGER_STAT_FINISHED)) {
      smaDebug("vgId:%d, persist task is cancelled and set finished", SMA_VID(pRSmaStat->pSma));
    } else {
      smaWarn("vgId:%d, persist task in abnormal stat %" PRIi8, atomic_load_8(RSMA_TRIGGER_STAT(pRSmaStat)),
              SMA_VID(pRSmaStat->pSma));
      ASSERT(0);
    }
    atomic_store_8(RSMA_RUNNING_STAT(pRSmaStat), 0);
    taosReleaseRef(smaMgmt.smaRef, pRSmaStat->refId);
  }

  taosThreadAttrDestroy(&thAttr);
}

/**
 * @brief trigger to persist rsma qTaskInfo
 *
 * @param param
 * @param tmrId
 */
static void tdRSmaPersistTrigger(void *param, void *tmrId) {
  SRSmaStat *rsmaStat = param;
  SRSmaStat *pRSmaStat = (SRSmaStat *)taosAcquireRef(smaMgmt.smaRef, rsmaStat->refId);

  if (!pRSmaStat) {
    smaDebug("rsma persistence task not start since already destroyed");
    return;
  }

  int8_t tmrStat =
      atomic_val_compare_exchange_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_ACTIVE, TASK_TRIGGER_STAT_INACTIVE);
  switch (tmrStat) {
    case TASK_TRIGGER_STAT_ACTIVE: {
      atomic_store_8(RSMA_RUNNING_STAT(pRSmaStat), 1);
      if (TASK_TRIGGER_STAT_CANCELLED != atomic_val_compare_exchange_8(RSMA_TRIGGER_STAT(pRSmaStat),
                                                                       TASK_TRIGGER_STAT_CANCELLED,
                                                                       TASK_TRIGGER_STAT_FINISHED)) {
        smaDebug("vgId:%d, rsma persistence start since active", SMA_VID(pRSmaStat->pSma));

        // start persist task
        tdRSmaPersistTask(pRSmaStat);

        taosTmrReset(tdRSmaPersistTrigger, RSMA_QTASKINFO_PERSIST_MS, pRSmaStat, pRSmaStat->tmrHandle,
                     &pRSmaStat->tmrId);
      } else {
        atomic_store_8(RSMA_RUNNING_STAT(pRSmaStat), 0);
      }
      return;
    } break;
    case TASK_TRIGGER_STAT_CANCELLED: {
      atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_FINISHED);
      smaDebug("rsma persistence not start since cancelled and finished");
    } break;
    case TASK_TRIGGER_STAT_INACTIVE: {
      smaDebug("rsma persistence not start since inactive");
    } break;
    case TASK_TRIGGER_STAT_INIT: {
      smaDebug("rsma persistence not start since init");
    } break;
    default: {
      smaWarn("rsma persistence not start since unknown stat %" PRIi8, tmrStat);
    } break;
  }
  taosReleaseRef(smaMgmt.smaRef, rsmaStat->refId);
}
