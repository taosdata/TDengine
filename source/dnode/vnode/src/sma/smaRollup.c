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
#include "tq.h"
#include "tstream.h"

#define RSMA_EXEC_SMOOTH_SIZE   (100)     // cnt
#define RSMA_EXEC_BATCH_SIZE    (1024)    // cnt
#define RSMA_FETCH_DELAY_MAX    (120000)  // ms
#define RSMA_FETCH_ACTIVE_MAX   (1000)    // ms
#define RSMA_FETCH_INTERVAL     (5000)    // ms
#define RSMA_EXEC_TASK_FLAG     "rsma"
#define RSMA_EXEC_MSG_HLEN      (13)  // type(int8_t) + len(int32_t) + version(int64_t)
#define RSMA_EXEC_MSG_TYPE(msg) (*(int8_t *)(msg))
#define RSMA_EXEC_MSG_LEN(msg)  (*(int32_t *)POINTER_SHIFT((msg), sizeof(int8_t)))
#define RSMA_EXEC_MSG_VER(msg)  (*(int64_t *)POINTER_SHIFT((msg), sizeof(int8_t) + sizeof(int32_t)))
#define RSMA_EXEC_MSG_BODY(msg) (POINTER_SHIFT((msg), RSMA_EXEC_MSG_HLEN))

#define RSMA_NEED_FETCH(r) (RSMA_INFO_ITEM((r), 0)->fetchLevel || RSMA_INFO_ITEM((r), 1)->fetchLevel)

SSmaMgmt smaMgmt = {
    .inited = 0,
    .rsetId = -1,
};

typedef struct SRSmaQTaskInfoItem SRSmaQTaskInfoItem;

static int32_t    tdUidStorePut(STbUidStore *pStore, tb_uid_t suid, tb_uid_t *uid);
static void       tdUidStoreDestory(STbUidStore *pStore);
static int32_t    tdUpdateTbUidListImpl(SSma *pSma, tb_uid_t *suid, SArray *tbUids, bool isAdd);
static int32_t    tdSetRSmaInfoItemParams(SSma *pSma, SRSmaParam *param, SRSmaStat *pStat, SRSmaInfo *pRSmaInfo,
                                          int8_t idx);
static int32_t    tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t msgSize, int64_t version, int32_t inputType,
                                    SRSmaInfo *pInfo, ERsmaExecType type, int8_t level);
static SRSmaInfo *tdAcquireRSmaInfoBySuid(SSma *pSma, int64_t suid);
static void       tdReleaseRSmaInfo(SSma *pSma, SRSmaInfo *pInfo);
static void       tdFreeRSmaSubmitItems(SArray *pItems, int32_t type);
static int32_t    tdRSmaFetchAllResult(SSma *pSma, SRSmaInfo *pInfo);
static int32_t    tdRSmaExecAndSubmitResult(SSma *pSma, qTaskInfo_t taskInfo, SRSmaInfoItem *pItem, SRSmaInfo *pInfo,
                                            int32_t execType, int8_t *streamFlushed);
static void       tdRSmaFetchTrigger(void *param, void *tmrId);
static void       tdRSmaQTaskInfoFree(qTaskInfo_t *taskHandle, int32_t vgId, int32_t level);
static int32_t    tdRSmaRestoreQTaskInfoInit(SSma *pSma, int64_t *nTables);
static int32_t    tdRSmaRestoreQTaskInfoReload(SSma *pSma, int8_t type, int64_t qTaskFileVer);
static int32_t    tdRSmaRestoreTSDataReload(SSma *pSma);

struct SRSmaQTaskInfoItem {
  int32_t len;
  int8_t  type;
  int64_t suid;
  void *  qTaskInfo;
};

static void tdRSmaQTaskInfoFree(qTaskInfo_t *taskHandle, int32_t vgId, int32_t level) {
  // Note: free/kill may in RC
  if (!taskHandle || !(*taskHandle)) return;
  qTaskInfo_t otaskHandle = atomic_load_ptr(taskHandle);
  if (otaskHandle && atomic_val_compare_exchange_ptr(taskHandle, otaskHandle, NULL)) {
    smaDebug("vgId:%d, free qTaskInfo_t %p of level %d", vgId, otaskHandle, level);
    qDestroyTask(otaskHandle);
  }
}

/**
 * @brief general function to free rsmaInfo
 *
 * @param pSma
 * @param pInfo
 * @return void*
 */
void *tdFreeRSmaInfo(SSma *pSma, SRSmaInfo *pInfo) {
  if (pInfo) {
    for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
      SRSmaInfoItem *pItem = &pInfo->items[i];

      if (pItem->tmrId) {
        smaDebug("vgId:%d, stop fetch timer %p for table %" PRIi64 " level %d", SMA_VID(pSma), pItem->tmrId,
                 pInfo->suid, i + 1);
        taosTmrStopA(&pItem->tmrId);
      }

      if (pItem->pStreamState) {
        streamStateClose(pItem->pStreamState, false);
      }

      if (pItem->pStreamTask) {
        tFreeStreamTask(pItem->pStreamTask);
      }
      taosArrayDestroy(pItem->pResList);
      tdRSmaQTaskInfoFree(&pInfo->taskInfo[i], SMA_VID(pSma), i + 1);
    }

    taosMemoryFreeClear(pInfo->pTSchema);

    if (pInfo->queue) {
      taosCloseQueue(pInfo->queue);
      pInfo->queue = NULL;
    }
    if (pInfo->qall) {
      taosFreeQall(pInfo->qall);
      pInfo->qall = NULL;
    }

    taosMemoryFree(pInfo);
  }

  return NULL;
}

static FORCE_INLINE int32_t tdUidStoreInit(STbUidStore **pStore) {
  *pStore = taosMemoryCalloc(1, sizeof(STbUidStore));
  if (*pStore == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tdUpdateTbUidListImpl(SSma *pSma, tb_uid_t *suid, SArray *tbUids, bool isAdd) {
  SRSmaInfo *pRSmaInfo = NULL;

  if (!suid || !tbUids) {
    terrno = TSDB_CODE_INVALID_PTR;
    smaError("vgId:%d, failed to get rsma info for uid:%" PRIi64 " since %s", SMA_VID(pSma), suid ? *suid : -1,
             terrstr());
    return TSDB_CODE_FAILED;
  }

  int32_t nTables = taosArrayGetSize(tbUids);

  if (0 == nTables) {
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
      if ((terrno = qUpdateTableListForStreamScanner(pRSmaInfo->taskInfo[i], tbUids, isAdd)) < 0) {
        tdReleaseRSmaInfo(pSma, pRSmaInfo);
        smaError("vgId:%d, update tbUidList failed for uid:%" PRIi64 " level %d since %s", SMA_VID(pSma), *suid, i,
                 terrstr());
        return TSDB_CODE_FAILED;
      }
      smaDebug("vgId:%d, update tbUidList succeed for qTaskInfo:%p. suid:%" PRIi64 " uid:%" PRIi64
               "nTables:%d level %d",
               SMA_VID(pSma), pRSmaInfo->taskInfo[i], *suid, *(int64_t *)TARRAY_GET_ELEM(tbUids, 0), nTables, i);
    }
  }

  tdReleaseRSmaInfo(pSma, pRSmaInfo);
  return TSDB_CODE_SUCCESS;
}

int32_t tdUpdateTbUidList(SSma *pSma, STbUidStore *pStore, bool isAdd) {
  if (!pStore || (taosArrayGetSize(pStore->tbUids) == 0)) {
    return TSDB_CODE_SUCCESS;
  }

  if (tdUpdateTbUidListImpl(pSma, &pStore->suid, pStore->tbUids, isAdd) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  void *pIter = NULL;
  while ((pIter = taosHashIterate(pStore->uidHash, pIter))) {
    tb_uid_t *pTbSuid = (tb_uid_t *)taosHashGetKey(pIter, NULL);
    SArray *  pTbUids = *(SArray **)pIter;

    if (tdUpdateTbUidListImpl(pSma, pTbSuid, pTbUids, isAdd) != TSDB_CODE_SUCCESS) {
      taosHashCancelIterate(pStore->uidHash, pIter);
      return TSDB_CODE_FAILED;
    }
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
  SHashObj * infoHash = NULL;
  if (!pStat || !(infoHash = RSMA_INFO_HASH(pStat))) {
    terrno = TSDB_CODE_RSMA_INVALID_STAT;
    return TSDB_CODE_FAILED;
  }

  // info cached when create rsma stable and return directly for non-rsma ctables
  if (!taosHashGet(infoHash, &suid, sizeof(tb_uid_t))) {
    return TSDB_CODE_SUCCESS;
  }

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

static void tdRSmaTaskInit(SStreamMeta *pMeta, SRSmaInfoItem *pItem, SStreamTaskId *pId) {
  STaskId id = {.streamId = pId->streamId, .taskId = pId->taskId};
  streamMetaRLock(pMeta);
  SStreamTask **ppTask = (SStreamTask **)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask && *ppTask) {
    pItem->submitReqVer = (*ppTask)->chkInfo.checkpointVer;
    pItem->fetchResultVer = (*ppTask)->info.triggerParam;
  }
  streamMetaRUnLock(pMeta);
}

static void tdRSmaTaskRemove(SStreamMeta *pMeta, int64_t streamId, int32_t taskId) {
  streamMetaUnregisterTask(pMeta, streamId, taskId);
  streamMetaWLock(pMeta);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  if (streamMetaCommit(pMeta) < 0) {
    // persist to disk
  }
  streamMetaWUnLock(pMeta);
  smaDebug("vgId:%d, rsma task:%" PRIi64 ",%d dropped, remain tasks:%d", pMeta->vgId, streamId, taskId, numOfTasks);
}

static int32_t tdSetRSmaInfoItemParams(SSma *pSma, SRSmaParam *param, SRSmaStat *pStat, SRSmaInfo *pRSmaInfo,
                                       int8_t idx) {
  if ((param->qmsgLen > 0) && param->qmsg[idx]) {
    SRSmaInfoItem *pItem = &(pRSmaInfo->items[idx]);
    SRetention *   pRetention = SMA_RETENTION(pSma);
    STsdbCfg *     pTsdbCfg = SMA_TSDB_CFG(pSma);
    SVnode *       pVnode = pSma->pVnode;
    char           taskInfDir[TSDB_FILENAME_LEN] = {0};
    void *         pStreamState = NULL;

    // set the backend of stream state
    tdRSmaQTaskInfoGetFullPath(pVnode, pRSmaInfo->suid, idx + 1, pVnode->pTfs, taskInfDir);

    if (!taosCheckExistFile(taskInfDir)) {
      char *s = taosStrdup(taskInfDir);
      if (taosMulMkDir(s) != 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        taosMemoryFree(s);
        return TSDB_CODE_FAILED;
      }
      taosMemoryFree(s);
    }

    SStreamTask *pStreamTask = taosMemoryCalloc(1, sizeof(*pStreamTask));
    if (!pStreamTask) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_FAILED;
    }
    pItem->pStreamTask = pStreamTask;
    pStreamTask->id.taskId = 0;
    pStreamTask->id.streamId = pRSmaInfo->suid + idx;
    pStreamTask->chkInfo.startTs = taosGetTimestampMs();
    pStreamTask->pMeta = pVnode->pTq->pStreamMeta;
    pStreamTask->exec.qmsg = taosMemoryMalloc(strlen(RSMA_EXEC_TASK_FLAG) + 1);
    sprintf(pStreamTask->exec.qmsg, "%s", RSMA_EXEC_TASK_FLAG);
    pStreamTask->chkInfo.checkpointId = streamMetaGetLatestCheckpointId(pStreamTask->pMeta);
    tdRSmaTaskInit(pStreamTask->pMeta, pItem, &pStreamTask->id);
    pStreamTask->status.pSM = streamCreateStateMachine(pStreamTask);

    pStreamState = streamStateOpen(taskInfDir, pStreamTask, true, -1, -1);
    if (!pStreamState) {
      terrno = TSDB_CODE_RSMA_STREAM_STATE_OPEN;
      return TSDB_CODE_FAILED;
    }
    pItem->pStreamState = pStreamState;

    tdRSmaTaskRemove(pStreamTask->pMeta, pStreamTask->id.streamId, pStreamTask->id.taskId);

    SReadHandle handle = {.vnode = pVnode, .initTqReader = 1, .skipRollup = 1, .pStateBackend = pStreamState};
    initStorageAPI(&handle.api);
    pRSmaInfo->taskInfo[idx] = qCreateStreamExecTaskInfo(param->qmsg[idx], &handle, TD_VID(pVnode), 0);
    if (!pRSmaInfo->taskInfo[idx]) {
      terrno = TSDB_CODE_RSMA_QTASKINFO_CREATE;
      return TSDB_CODE_FAILED;
    }

    if (!(pItem->pResList = taosArrayInit(1, POINTER_BYTES))) {
      return TSDB_CODE_FAILED;
    }

    if (pItem->fetchResultVer < pItem->submitReqVer) {
      // fetch the data when reboot
      pItem->triggerStat = TASK_TRIGGER_STAT_ACTIVE;
    }

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

    SRSmaRef rsmaRef = {.refId = pStat->refId, .suid = pRSmaInfo->suid};
    taosHashPut(smaMgmt.refHash, &pItem, POINTER_BYTES, &rsmaRef, sizeof(rsmaRef));

    taosTmrReset(tdRSmaFetchTrigger, RSMA_FETCH_INTERVAL, pItem, smaMgmt.tmrHandle, &pItem->tmrId);

    smaInfo("vgId:%d, open rsma task:%p table:%" PRIi64 " level:%" PRIi8 ", checkpointId:%" PRIi64
            ", submitReqVer:%" PRIi64 ", fetchResultVer:%" PRIi64 ", maxdelay:%" PRIi64 " watermark:%" PRIi64
            ", finally maxdelay:%" PRIi32,
            TD_VID(pVnode), pItem->pStreamTask, pRSmaInfo->suid, (int8_t)(idx + 1), pStreamTask->chkInfo.checkpointId,
            pItem->submitReqVer, pItem->fetchResultVer, param->maxdelay[idx], param->watermark[idx], pItem->maxDelay);
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
int32_t tdRSmaProcessCreateImpl(SSma *pSma, SRSmaParam *param, int64_t suid, const char *tbName) {
  if ((param->qmsgLen[0] == 0) && (param->qmsgLen[1] == 0)) {
    smaDebug("vgId:%d, no qmsg1/qmsg2 for rollup table %s %" PRIi64, SMA_VID(pSma), tbName, suid);
    return TSDB_CODE_SUCCESS;
  }

#if 0
  if (tdCheckAndInitSmaEnv(pSma, TSDB_SMA_TYPE_ROLLUP) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }
#endif

  SSmaEnv *  pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  pRSmaInfo = taosHashGet(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t));
  if (pRSmaInfo) {
    smaInfo("vgId:%d, rsma info already exists for table %s, %" PRIi64, SMA_VID(pSma), tbName, suid);
    return TSDB_CODE_SUCCESS;
  }

  // from write queue: single thead
  pRSmaInfo = (SRSmaInfo *)taosMemoryCalloc(1, sizeof(SRSmaInfo));
  if (!pRSmaInfo) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  STSchema *pTSchema = metaGetTbTSchema(SMA_META(pSma), suid, -1, 1);
  if (!pTSchema) {
    terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
    goto _err;
  }
  pRSmaInfo->pSma = pSma;
  pRSmaInfo->pTSchema = pTSchema;
  pRSmaInfo->suid = suid;
  T_REF_INIT_VAL(pRSmaInfo, 1);

  if (!(pRSmaInfo->queue = taosOpenQueue()) || !(pRSmaInfo->qall = taosAllocateQall()) ||
      tdSetRSmaInfoItemParams(pSma, param, pStat, pRSmaInfo, 0) < 0 ||
      tdSetRSmaInfoItemParams(pSma, param, pStat, pRSmaInfo, 1) < 0) {
    goto _err;
  }

  if (taosHashPut(RSMA_INFO_HASH(pStat), &suid, sizeof(tb_uid_t), &pRSmaInfo, sizeof(pRSmaInfo)) < 0) {
    goto _err;
  }

  smaDebug("vgId:%d, register rsma info succeed for table %" PRIi64, SMA_VID(pSma), suid);

  return TSDB_CODE_SUCCESS;
_err:
  tdFreeRSmaInfo(pSma, pRSmaInfo);
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
    smaWarn("vgId:%d, not create rsma for stable %s %" PRIi64 " since vnd is not rsma", TD_VID(pVnode), pReq->name,
            pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  return tdRSmaProcessCreateImpl(pSma, &pReq->rsmaParam, pReq->suid, pReq->name);
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

  SRSmaStat *pRSmaStat = (SRSmaStat *)SMA_ENV_STAT(pSmaEnv);

  SRSmaInfo *pRSmaInfo = tdAcquireRSmaInfoBySuid(pSma, pReq->suid);

  if (!pRSmaInfo) {
    smaWarn("vgId:%d, drop rsma for stable %s %" PRIi64 " failed no rsma in hash", TD_VID(pVnode), pReq->name,
            pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  // set del flag for data in mem
  atomic_store_8(&pRSmaStat->delFlag, 1);
  RSMA_INFO_SET_DEL(pRSmaInfo);
  tdUnRefRSmaInfo(pSma, pRSmaInfo);

  tdReleaseRSmaInfo(pSma, pRSmaInfo);

  // no need to save to file as triggered by dropping stable
  smaDebug("vgId:%d, drop rsma for stable %" PRIi64 " succeed", TD_VID(pVnode), pReq->suid);
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
          taosArrayDestroy(pUidArray);
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

static void tdUidStoreDestory(STbUidStore *pStore) {
  if (pStore) {
    if (pStore->uidHash) {
      if (pStore->tbUids) {
        // When pStore->tbUids not NULL, the pStore->uidHash has k/v; otherwise pStore->uidHash only has keys.
        void *pIter = NULL;
        while ((pIter = taosHashIterate(pStore->uidHash, pIter))) {
          SArray *arr = *(SArray **)pIter;
          taosArrayDestroy(arr);
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

/**
 * @brief The SubmitReq for rsma L2/L3 is inserted by tsdbInsertData method directly while not by WriteQ, as the queue
 * would be freed when close Vnode, thus lock should be used if with race condition.
 * @param pTsdb
 * @param version
 * @param pReq
 * @return int32_t
 */
static int32_t tdProcessSubmitReq(STsdb *pTsdb, int64_t version, void *pReq) {
  if (pReq) {
    SSubmitReq2 *pSubmitReq = (SSubmitReq2 *)pReq;
    // spin lock for race condition during insert data
    if (tsdbInsertData(pTsdb, version, pSubmitReq, NULL) < 0) {
      return TSDB_CODE_FAILED;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tdFetchSubmitReqSuids(SSubmitReq2 *pMsg, STbUidStore *pStore) {
  SArray *pSubmitTbData = pMsg ? pMsg->aSubmitTbData : NULL;
  int32_t size = taosArrayGetSize(pSubmitTbData);

  terrno = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < size; ++i) {
    SSubmitTbData *pData = TARRAY_GET_ELEM(pSubmitTbData, i);
    if ((terrno = tdUidStorePut(pStore, pData->suid, NULL)) < 0) {
      return -1;
    }
  }

  return 0;
}

/**
 * @brief retention of rsma1/rsma2
 *
 * @param pSma
 * @param now
 * @return int32_t
 */
int32_t smaRetention(SSma *pSma, int64_t now) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (!VND_IS_RSMA(pSma->pVnode)) {
    return code;
  }

  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pSma->pRSmaTsdb[i]) {
      code = tsdbRetention(pSma->pRSmaTsdb[i], now, pSma->pVnode->config.sttTrigger == 1);
      if (code) goto _end;
    }
  }

_end:
  return code;
}

static int32_t tdRSmaProcessDelReq(SSma *pSma, int64_t suid, int8_t level, SBatchDeleteReq *pDelReq) {
  int32_t code = 0;
  int32_t lino = 0;

  if (taosArrayGetSize(pDelReq->deleteReqs) > 0) {
    int32_t len = 0;
    tEncodeSize(tEncodeSBatchDeleteReq, pDelReq, len, code);
    TSDB_CHECK_CODE(code, lino, _exit);

    void *pBuf = rpcMallocCont(len + sizeof(SMsgHead));
    if (!pBuf) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    SEncoder encoder;
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SMsgHead)), len);
    tEncodeSBatchDeleteReq(&encoder, pDelReq);
    tEncoderClear(&encoder);

    ((SMsgHead *)pBuf)->vgId = TD_VID(pSma->pVnode);

    SRpcMsg delMsg = {.msgType = TDMT_VND_BATCH_DEL, .pCont = pBuf, .contLen = len + sizeof(SMsgHead)};
    code = tmsgPutToQueue(&pSma->pVnode->msgCb, WRITE_QUEUE, &delMsg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  taosArrayDestroy(pDelReq->deleteReqs);
  if (code) {
    smaError("vgId:%d, failed at line %d to process delete req for table:%" PRIi64 ", level:%" PRIi8 " since %s",
             SMA_VID(pSma), lino, suid, level, tstrerror(code));
  }

  return code;
}

static int32_t tdRSmaExecAndSubmitResult(SSma *pSma, qTaskInfo_t taskInfo, SRSmaInfoItem *pItem, SRSmaInfo *pInfo,
                                         int32_t execType, int8_t *streamFlushed) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSDataBlock *output = NULL;
  SArray *     pResList = pItem->pResList;
  STSchema *   pTSchema = pInfo->pTSchema;
  int64_t      suid = pInfo->suid;

  while (1) {
    uint64_t ts;
    bool     hasMore = false;
    code = qExecTaskOpt(taskInfo, pResList, &ts, &hasMore, NULL);
    if (code == TSDB_CODE_QRY_IN_EXEC) {
      code = 0;
      break;
    }
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosArrayGetSize(pResList) == 0) {
      break;
    }

    for (int32_t i = 0; i < taosArrayGetSize(pResList); ++i) {
      output = taosArrayGetP(pResList, i);
      if (output->info.type == STREAM_CHECKPOINT) {
        if (streamFlushed) *streamFlushed = 1;
        continue;
      } else if (output->info.type == STREAM_DELETE_RESULT) {
        SBatchDeleteReq deleteReq = {.suid = suid, .level = pItem->level};
        deleteReq.deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq));
        if (!deleteReq.deleteReqs) {
          code = terrno;
          TSDB_CHECK_CODE(code, lino, _exit);
        }
        code = tqBuildDeleteReq(pSma->pVnode->pTq, NULL, output, &deleteReq, "", true);
        TSDB_CHECK_CODE(code, lino, _exit);
        code = tdRSmaProcessDelReq(pSma, suid, pItem->level, &deleteReq);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }

      smaDebug("vgId:%d, result block, execType:%d, ver:%" PRIi64 ", submitReqVer:%" PRIi64 ", fetchResultVer:%" PRIi64
               ", suid:%" PRIi64 ", level:%" PRIi8 ", uid:%" PRIu64 ", groupid:%" PRIu64 ", rows:%" PRIi64,
               SMA_VID(pSma), execType, output->info.version, pItem->submitReqVer, pItem->fetchResultVer, suid,
               pItem->level, output->info.id.uid, output->info.id.groupId, output->info.rows);

      if (STREAM_GET_ALL == execType) {
        /**
         * 1. reset the output version when reboot
         * 2. delete msg version not updated from the result
         */
        if (output->info.version < pItem->submitReqVer) {
          // submitReqVer keeps unchanged since tdExecuteRSmaImpl and tdRSmaFetchAllResult are executed synchronously
          output->info.version = pItem->submitReqVer;
        } else if (output->info.version == pItem->fetchResultVer) {
          smaWarn("vgId:%d, result block, skip dup version, execType:%d, ver:%" PRIi64 ", submitReqVer:%" PRIi64
                  ", fetchResultVer:%" PRIi64 ", suid:%" PRIi64 ", level:%" PRIi8 ", uid:%" PRIu64 ", groupid:%" PRIu64
                  ", rows:%" PRIi64,
                  SMA_VID(pSma), execType, output->info.version, pItem->submitReqVer, pItem->fetchResultVer, suid,
                  pItem->level, output->info.id.uid, output->info.id.groupId, output->info.rows);
          continue;
        }
      }

      STsdb *      sinkTsdb = (pItem->level == TSDB_RETENTION_L1 ? pSma->pRSmaTsdb[0] : pSma->pRSmaTsdb[1]);
      SSubmitReq2 *pReq = NULL;

      if (buildSubmitReqFromDataBlock(&pReq, output, pTSchema, output->info.id.groupId, SMA_VID(pSma), suid) < 0) {
        code = terrno ? terrno : TSDB_CODE_RSMA_RESULT;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      if (pReq && tdProcessSubmitReq(sinkTsdb, output->info.version, pReq) < 0) {
        if (terrno == TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE) {
          // TODO: reconfigure SSubmitReq2
        } else {
          if (terrno == 0) terrno = TSDB_CODE_RSMA_RESULT;
          code = terrno;
        }
        tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
        taosMemoryFree(pReq);
        pReq = NULL;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      if (STREAM_GET_ALL == execType) {
        atomic_store_64(&pItem->fetchResultVer, output->info.version);
      }

      smaDebug("vgId:%d, process submit req for rsma suid:%" PRIu64 ",uid:%" PRIu64 ", level:%" PRIi8
               ", execType:%d, ver:%" PRIi64,
               SMA_VID(pSma), suid, output->info.id.groupId, pItem->level, execType, output->info.version);

      if (pReq) {
        tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
        taosMemoryFree(pReq);
      }
    }
  }
_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s, suid:%" PRIi64 ", level:%" PRIi8 ", uid:%" PRIi64
             ", ver:%" PRIi64,
             SMA_VID(pSma), __func__, lino, tstrerror(code), suid, pItem->level, output ? output->info.id.uid : -1,
             output ? output->info.version : -1);
  } else {
    smaDebug("vgId:%d, %s succeed, suid:%" PRIi64 ", level:%" PRIi8, SMA_VID(pSma), __func__, suid, pItem->level);
  }
  qCleanExecTaskBlockBuf(taskInfo);
  return code;
}

/**
 * @brief Copy msg to rsmaQueueBuffer for batch process
 *
 * @param pSma
 * @param version
 * @param pMsg
 * @param len
 * @param inputType
 * @param pInfo
 * @param suid
 * @return int32_t
 */
static int32_t tdExecuteRSmaImplAsync(SSma *pSma, int64_t version, const void *pMsg, int32_t len, int32_t inputType,
                                      SRSmaInfo *pInfo, tb_uid_t suid) {
  int32_t size = RSMA_EXEC_MSG_HLEN + len;  // header + payload
  void *  qItem = taosAllocateQitem(size, DEF_QITEM, 0);

  if (!qItem) {
    return TSDB_CODE_FAILED;
  }

  void *pItem = qItem;

  *(int8_t *)pItem = (int8_t)inputType;
  pItem = POINTER_SHIFT(pItem, sizeof(int8_t));
  *(int32_t *)pItem = len;
  pItem = POINTER_SHIFT(pItem, sizeof(int32_t));
  *(int64_t *)pItem = version;
  memcpy(POINTER_SHIFT(pItem, sizeof(int64_t)), pMsg, len);

  taosWriteQitem(pInfo->queue, qItem);

  pInfo->lastRecv = taosGetTimestampMs();

  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(pSma);

  int64_t nItems = atomic_fetch_add_64(&pRSmaStat->nBufItems, 1);

  if (atomic_load_8(&pInfo->assigned) == 0) {
    tsem_post(&(pRSmaStat->notEmpty));
  }

  // smoothing consume
  int32_t n = nItems / RSMA_EXEC_SMOOTH_SIZE;
  if (n > 1) {
    if (n > 10) {
      n = 10;
    }
    taosMsleep(n << 3);
    if (n > 5) {
      smaWarn("vgId:%d, pInfo->queue itemSize:%d, memSize:%" PRIi64 ", sleep %d ms", SMA_VID(pSma),
              taosQueueItemSize(pInfo->queue), taosQueueMemorySize(pInfo->queue), n << 3);
    }
  }

  return TSDB_CODE_SUCCESS;
}

#if 0
static int32_t tdRsmaPrintSubmitReq(SSma *pSma, SSubmitReq *pReq) {
  SSubmitMsgIter msgIter = {0};
  SSubmitBlkIter blkIter = {0};
  STSRow        *row = NULL;
  if (tInitSubmitMsgIter(pReq, &msgIter) < 0) return -1;
  while (true) {
    SSubmitBlk *pBlock = NULL;
    if (tGetSubmitMsgNext(&msgIter, &pBlock) < 0) return -1;
    if (pBlock == NULL) break;
    tInitSubmitBlkIter(&msgIter, pBlock, &blkIter);
    while ((row = tGetSubmitBlkNext(&blkIter)) != NULL) {
      smaDebug("vgId:%d, numOfRows:%d, suid:%" PRIi64 ", uid:%" PRIi64 ", version:%" PRIi64 ", ts:%" PRIi64,
               SMA_VID(pSma), msgIter.numOfRows, msgIter.suid, msgIter.uid, pReq->version, row->ts);
    }
  }
  return 0;
}
#endif

/**
 * @brief sync mode
 *
 * @param pSma
 * @param pMsg
 * @param msgSize
 * @param inputType
 * @param pInfo
 * @param type
 * @param level
 * @return int32_t
 */
static int32_t tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t msgSize, int64_t version, int32_t inputType,
                                 SRSmaInfo *pInfo, ERsmaExecType type, int8_t level) {
  int32_t        idx = level - 1;
  void *         qTaskInfo = RSMA_INFO_QTASK(pInfo, idx);
  SRSmaInfoItem *pItem = RSMA_INFO_ITEM(pInfo, idx);

  if (!qTaskInfo) {
    smaDebug("vgId:%d, no qTaskInfo to execute rsma %" PRIi8 " task for suid:%" PRIu64, SMA_VID(pSma), level,
             pInfo->suid);
    return TSDB_CODE_SUCCESS;
  }
  if (!pInfo->pTSchema) {
    terrno = TSDB_CODE_INVALID_PTR;
    smaWarn("vgId:%d, no schema to execute rsma %" PRIi8 " task for suid:%" PRIu64, SMA_VID(pSma), level, pInfo->suid);
    return TSDB_CODE_FAILED;
  }

  smaDebug("vgId:%d, execute rsma %" PRIi8 " task for qTaskInfo:%p, suid:%" PRIu64 ", nMsg:%d, submitReqVer:%" PRIi64
           ", inputType:%d",
           SMA_VID(pSma), level, RSMA_INFO_QTASK(pInfo, idx), pInfo->suid, msgSize, version, inputType);

  if ((terrno = qSetSMAInput(qTaskInfo, pMsg, msgSize, inputType)) < 0) {
    smaError("vgId:%d, rsma %" PRIi8 " qSetStreamInput failed since %s", SMA_VID(pSma), level, tstrerror(terrno));
    return TSDB_CODE_FAILED;
  }

  atomic_store_64(&pItem->submitReqVer, version);

  terrno = tdRSmaExecAndSubmitResult(pSma, qTaskInfo, pItem, pInfo, STREAM_NORMAL, NULL);

  return terrno ? TSDB_CODE_FAILED : TDB_CODE_SUCCESS;
}

/**
 * @brief During async commit, the SRSmaInfo object would be COW from iRSmaInfoHash and write lock should be applied.
 *
 * @param pSma
 * @param suid
 * @return SRSmaInfo*
 */
static SRSmaInfo *tdAcquireRSmaInfoBySuid(SSma *pSma, int64_t suid) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SSmaEnv *  pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = NULL;
  SRSmaInfo *pRSmaInfo = NULL;

  terrno = 0;

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
    if (ASSERTS(pRSmaInfo->suid == suid, "suid:%" PRIi64 " != %" PRIi64, pRSmaInfo->suid, suid)) {
      terrno = TSDB_CODE_APP_ERROR;
      return NULL;
    }
    return pRSmaInfo;
  }
  taosRUnLockLatch(SMA_ENV_LOCK(pEnv));

  return NULL;
}

static FORCE_INLINE void tdReleaseRSmaInfo(SSma *pSma, SRSmaInfo *pInfo) {
  if (pInfo) {
    tdUnRefRSmaInfo(pSma, pInfo);
  }
}

/**
 * @brief async mode
 *
 * @param pSma
 * @param version
 * @param pMsg
 * @param inputType
 * @param suid
 * @return int32_t
 */
static int32_t tdExecuteRSmaAsync(SSma *pSma, int64_t version, const void *pMsg, int32_t len, int32_t inputType,
                                  tb_uid_t suid) {
  SRSmaInfo *pRSmaInfo = tdAcquireRSmaInfoBySuid(pSma, suid);
  if (!pRSmaInfo) {
    smaDebug("vgId:%d, execute rsma, no rsma info for suid:%" PRIu64, SMA_VID(pSma), suid);
    return TSDB_CODE_SUCCESS;
  }

  if (inputType == STREAM_INPUT__DATA_SUBMIT || inputType == STREAM_INPUT__REF_DATA_BLOCK) {
    if (tdExecuteRSmaImplAsync(pSma, version, pMsg, len, inputType, pRSmaInfo, suid) < 0) {
      tdReleaseRSmaInfo(pSma, pRSmaInfo);
      return TSDB_CODE_FAILED;
    }
    if (smaMgmt.tmrHandle) {
      SRSmaInfoItem *pItem = RSMA_INFO_ITEM(pRSmaInfo, 0);
      if (pItem->level > 0) {
        atomic_store_8(&pItem->triggerStat, TASK_TRIGGER_STAT_ACTIVE);
      }
      pItem = RSMA_INFO_ITEM(pRSmaInfo, 1);
      if (pItem->level > 0) {
        atomic_store_8(&pItem->triggerStat, TASK_TRIGGER_STAT_ACTIVE);
      }
    }
  } else {
    terrno = TSDB_CODE_APP_ERROR;
    tdReleaseRSmaInfo(pSma, pRSmaInfo);
    smaError("vgId:%d, execute rsma, failed for suid:%" PRIu64 " since %s, type:%d", SMA_VID(pSma), suid,
             tstrerror(terrno), inputType);
    return TSDB_CODE_FAILED;
  }

  tdReleaseRSmaInfo(pSma, pRSmaInfo);
  return TSDB_CODE_SUCCESS;
}

int32_t tdProcessRSmaSubmit(SSma *pSma, int64_t version, void *pReq, void *pMsg, int32_t len) {
  if (!SMA_RSMA_ENV(pSma)) return TSDB_CODE_SUCCESS;

  if ((terrno = atomic_load_32(&SMA_RSMA_STAT(pSma)->execStat))) {
    smaError("vgId:%d, failed to process rsma submit since invalid exec code: %s", SMA_VID(pSma), terrstr());
    goto _err;
  }

  STbUidStore uidStore = {0};

  if (tdFetchSubmitReqSuids(pReq, &uidStore) < 0) {
    smaError("vgId:%d, failed to process rsma submit fetch suid since: %s", SMA_VID(pSma), terrstr());
    goto _err;
  }

  if (uidStore.suid != 0) {
    if (tdExecuteRSmaAsync(pSma, version, pMsg, len, STREAM_INPUT__DATA_SUBMIT, uidStore.suid) < 0) {
      smaError("vgId:%d, failed to process rsma submit exec 1 since: %s", SMA_VID(pSma), terrstr());
      goto _err;
    }

    void *pIter = NULL;
    while ((pIter = taosHashIterate(uidStore.uidHash, pIter))) {
      tb_uid_t *pTbSuid = (tb_uid_t *)taosHashGetKey(pIter, NULL);
      if (tdExecuteRSmaAsync(pSma, version, pMsg, len, STREAM_INPUT__DATA_SUBMIT, *pTbSuid) < 0) {
        smaError("vgId:%d, failed to process rsma submit exec 2 since: %s", SMA_VID(pSma), terrstr());
        taosHashCancelIterate(uidStore.uidHash, pIter);
        goto _err;
      }
    }
  }
  tdUidStoreDestory(&uidStore);
  return TSDB_CODE_SUCCESS;
_err:
  tdUidStoreDestory(&uidStore);
  return terrno;
}

int32_t tdProcessRSmaDelete(SSma *pSma, int64_t version, void *pReq, void *pMsg, int32_t len) {
  if (!SMA_RSMA_ENV(pSma)) return TSDB_CODE_SUCCESS;

  if ((terrno = atomic_load_32(&SMA_RSMA_STAT(pSma)->execStat))) {
    smaError("vgId:%d, failed to process rsma delete since invalid exec code: %s", SMA_VID(pSma), terrstr());
    goto _err;
  }

  SDeleteRes *pDelRes = pReq;
  if (tdExecuteRSmaAsync(pSma, version, pMsg, len, STREAM_INPUT__REF_DATA_BLOCK, pDelRes->suid) < 0) {
    smaError("vgId:%d, failed to process rsma submit exec 1 since: %s", SMA_VID(pSma), terrstr());
    goto _err;
  }
  return TSDB_CODE_SUCCESS;
_err:
  return terrno;
}

/**
 * @brief retrieve rsma meta and init
 *
 * @param pSma
 * @param nTables number of tables of rsma
 * @return int32_t
 */
static int32_t tdRSmaRestoreQTaskInfoInit(SSma *pSma, int64_t *nTables) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SVnode *    pVnode = pSma->pVnode;
  SArray *    suidList = NULL;
  STbUidStore uidStore = {0};
  SMetaReader mr = {0};
  tb_uid_t    suid = 0;

  if (!(suidList = taosArrayInit(1, sizeof(tb_uid_t)))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (vnodeGetStbIdList(pSma->pVnode, 0, suidList) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t arrSize = taosArrayGetSize(suidList);

  if (arrSize == 0) {
    if (nTables) {
      *nTables = 0;
    }
    taosArrayDestroy(suidList);
    smaDebug("vgId:%d, no need to restore rsma env since empty stb id list", TD_VID(pVnode));
    return TSDB_CODE_SUCCESS;
  }

  int64_t nRsmaTables = 0;
  metaReaderDoInit(&mr, SMA_META(pSma), 0);
  if (!(uidStore.tbUids = taosArrayInit(1024, sizeof(tb_uid_t)))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  for (int64_t i = 0; i < arrSize; ++i) {
    suid = *(tb_uid_t *)taosArrayGet(suidList, i);
    smaDebug("vgId:%d, rsma restore, suid is %" PRIi64, TD_VID(pVnode), suid);
    if (metaReaderGetTableEntryByUidCache(&mr, suid) < 0) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tDecoderClear(&mr.coder);
    if (mr.me.type != TSDB_SUPER_TABLE) {
      code = TSDB_CODE_RSMA_INVALID_SCHEMA;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (mr.me.uid != suid) {
      code = TSDB_CODE_RSMA_INVALID_SCHEMA;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (TABLE_IS_ROLLUP(mr.me.flags)) {
      ++nRsmaTables;
      SRSmaParam *param = &mr.me.stbEntry.rsmaParam;
      for (int i = 0; i < TSDB_RETENTION_L2; ++i) {
        smaDebug("vgId:%d, rsma restore, table:%" PRIi64 " level:%d, maxdelay:%" PRIi64 " watermark:%" PRIi64
                 " qmsgLen:%" PRIi32,
                 TD_VID(pVnode), suid, i, param->maxdelay[i], param->watermark[i], param->qmsgLen[i]);
      }
      if (tdRSmaProcessCreateImpl(pSma, &mr.me.stbEntry.rsmaParam, suid, mr.me.name) < 0) {
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
#if 0
      // reload all ctbUids for suid
      uidStore.suid = suid;
      if (vnodeGetCtbIdList(pVnode, suid, uidStore.tbUids) < 0) {
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      if (tdUpdateTbUidList(pVnode->pSma, &uidStore, true) < 0) {
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      taosArrayClear(uidStore.tbUids);
#endif
      smaDebug("vgId:%d, rsma restore env success for %" PRIi64, TD_VID(pVnode), suid);
    }
  }

  if (nTables) {
    *nTables = nRsmaTables;
  }
_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s, suid:%" PRIi64 ", type:%" PRIi8 ", uid:%" PRIi64, TD_VID(pVnode),
             __func__, lino, tstrerror(code), suid, mr.me.type, mr.me.uid);
  }
  metaReaderClear(&mr);
  taosArrayDestroy(suidList);
  tdUidStoreDestory(&uidStore);
  return code;
}

/**
 * N.B. the data would be restored from the unified WAL replay procedure
 */
int32_t tdRSmaProcessRestoreImpl(SSma *pSma, int8_t type, int64_t qtaskFileVer, int8_t rollback) {
  int32_t code = 0;
  int64_t nTables = 0;

  // step 1: init env
  if (tdCheckAndInitSmaEnv(pSma, TSDB_SMA_TYPE_ROLLUP) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_TDB_INIT_FAILED;
    goto _err;
  }

  // step 2: iterate all stables to restore the rsma env
  if ((code = tdRSmaRestoreQTaskInfoInit(pSma, &nTables)) < 0) {
    goto _err;
  }

_err:
  if (code) {
    smaError("vgId:%d, restore rsma task %" PRIi8 "from qtaskf %" PRIi64 " failed since %s", SMA_VID(pSma), type,
             qtaskFileVer, tstrerror(code));
  } else {
    smaInfo("vgId:%d, restore rsma task %" PRIi8 " from qtaskf %" PRIi64 " succeed, nTables:%" PRIi64, SMA_VID(pSma),
            type, qtaskFileVer, nTables);
  }

  return code;
}

int32_t tdRSmaPersistExecImpl(SRSmaStat *pRSmaStat, SHashObj *pInfoHash) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t nTaskInfo = 0;
  SSma *  pSma = pRSmaStat->pSma;
  SVnode *pVnode = pSma->pVnode;

  if (taosHashGetSize(pInfoHash) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  // stream state: trigger checkpoint
  do {
    void *infoHash = NULL;
    while ((infoHash = taosHashIterate(pInfoHash, infoHash))) {
      SRSmaInfo *pRSmaInfo = *(SRSmaInfo **)infoHash;
      if (RSMA_INFO_IS_DEL(pRSmaInfo)) {
        continue;
      }
      for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
        if (pRSmaInfo->taskInfo[i]) {
          code = qSetSMAInput(pRSmaInfo->taskInfo[i], pRSmaStat->blocks, 1, STREAM_INPUT__CHECKPOINT);
          if (code) {
            taosHashCancelIterate(pInfoHash, infoHash);
            TSDB_CHECK_CODE(code, lino, _exit);
          }
          pRSmaInfo->items[i].streamFlushed = 0;
          ++nTaskInfo;
        }
      }
    }
  } while (0);

  // stream state: wait checkpoint ready in async mode
  do {
    int32_t nStreamFlushed = 0;
    int32_t nSleep = 0;
    void *  infoHash = NULL;
    while (true) {
      while ((infoHash = taosHashIterate(pInfoHash, infoHash))) {
        SRSmaInfo *pRSmaInfo = *(SRSmaInfo **)infoHash;
        if (RSMA_INFO_IS_DEL(pRSmaInfo)) {
          continue;
        }
        for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
          if (pRSmaInfo->taskInfo[i] && (0 == pRSmaInfo->items[i].streamFlushed)) {
            int8_t streamFlushed = 0;
            code = tdRSmaExecAndSubmitResult(pSma, pRSmaInfo->taskInfo[i], &pRSmaInfo->items[i], pRSmaInfo,
                                             STREAM_CHECKPOINT, &streamFlushed);
            if (code) {
              taosHashCancelIterate(pInfoHash, infoHash);
              TSDB_CHECK_CODE(code, lino, _exit);
            }

            if (streamFlushed) {
              pRSmaInfo->items[i].streamFlushed = 1;
              if (++nStreamFlushed >= nTaskInfo) {
                smaInfo("vgId:%d, rsma commit, checkpoint ready, %d us consumed, received/total: %d/%d", TD_VID(pVnode),
                        nSleep * 10, nStreamFlushed, nTaskInfo);
                taosHashCancelIterate(pInfoHash, infoHash);
                goto _checkpoint;
              }
            }
          }
        }
      }
      taosUsleep(10);
      ++nSleep;
      smaDebug("vgId:%d, rsma commit, wait for checkpoint ready, %d us elapsed, received/total: %d/%d", TD_VID(pVnode),
               nSleep * 10, nStreamFlushed, nTaskInfo);
    }
  } while (0);

_checkpoint:
  // stream state: build checkpoint in backend
  do {
    SStreamMeta *pMeta = NULL;
    int64_t      checkpointId = taosGetTimestampNs();
    bool         checkpointBuilt = false;
    void *       infoHash = NULL;
    while ((infoHash = taosHashIterate(pInfoHash, infoHash))) {
      SRSmaInfo *pRSmaInfo = *(SRSmaInfo **)infoHash;
      if (RSMA_INFO_IS_DEL(pRSmaInfo)) {
        continue;
      }

      for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
        SRSmaInfoItem *pItem = RSMA_INFO_ITEM(pRSmaInfo, i);
        if (pItem && pItem->pStreamTask) {
          SStreamTask *pTask = pItem->pStreamTask;
          // atomic_store_32(&pTask->pMeta->chkptNotReadyTasks, 1);
          pTask->chkInfo.checkpointingId = checkpointId;
          pTask->chkInfo.checkpointId = checkpointId;  // 1pTask->checkpointingId;
          pTask->chkInfo.checkpointVer = pItem->submitReqVer;
          pTask->info.triggerParam = pItem->fetchResultVer;
          pTask->info.taskLevel = TASK_LEVEL_SMA;

          if (!checkpointBuilt) {
            // the stream states share one checkpoint
            code = streamTaskBuildCheckpoint(pTask);
            if (code) {
              taosHashCancelIterate(pInfoHash, infoHash);
              TSDB_CHECK_CODE(code, lino, _exit);
            }
            pMeta = pTask->pMeta;
            checkpointBuilt = true;
          }

          streamMetaWLock(pMeta);
          if (streamMetaSaveTask(pMeta, pTask)) {
            streamMetaWUnLock(pMeta);
            code = terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
            taosHashCancelIterate(pInfoHash, infoHash);
            TSDB_CHECK_CODE(code, lino, _exit);
          }
          streamMetaWUnLock(pMeta);
          smaDebug("vgId:%d, rsma commit, succeed to commit task:%p, submitReqVer:%" PRIi64 ", fetchResultVer:%" PRIi64
                   ", table:%" PRIi64 ", level:%d",
                   TD_VID(pVnode), pTask, pItem->submitReqVer, pItem->fetchResultVer, pRSmaInfo->suid, i + 1);
        }
      }
    }
    if (pMeta) {
      streamMetaWLock(pMeta);
      if (streamMetaCommit(pMeta)) {
        streamMetaWUnLock(pMeta);
        code = terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      streamMetaWUnLock(pMeta);
    }
    if (checkpointBuilt) {
      smaInfo("vgId:%d, rsma commit, succeed to commit checkpoint:%" PRIi64, TD_VID(pVnode), checkpointId);
    }
  } while (0);
_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }

  terrno = code;
  return code;
}

/**
 * @brief trigger to get rsma result in async mode
 *
 * @param param
 * @param tmrId
 */
static void tdRSmaFetchTrigger(void *param, void *tmrId) {
  SRSmaRef *     pRSmaRef = NULL;
  SSma *         pSma = NULL;
  SRSmaStat *    pStat = NULL;
  SRSmaInfo *    pRSmaInfo = NULL;
  SRSmaInfoItem *pItem = NULL;

  if (!(pRSmaRef = taosHashGet(smaMgmt.refHash, &param, POINTER_BYTES))) {
    smaDebug("rsma fetch task not start since rsma info item:%p not exist in refHash:%p, rsetId:%d", param,
             smaMgmt.refHash, smaMgmt.rsetId);
    return;
  }

  if (!(pStat = (SRSmaStat *)tdAcquireSmaRef(smaMgmt.rsetId, pRSmaRef->refId))) {
    smaWarn("rsma fetch task not start since rsma stat already destroyed, rsetId:%d refId:%" PRIi64 ")", smaMgmt.rsetId,
            pRSmaRef->refId);  // pRSmaRef freed in taosHashRemove
    taosHashRemove(smaMgmt.refHash, &param, POINTER_BYTES);
    return;
  }

  pSma = pStat->pSma;

  if (!(pRSmaInfo = tdAcquireRSmaInfoBySuid(pSma, pRSmaRef->suid))) {
    smaDebug("rsma fetch task not start since rsma info not exist, rsetId:%d refId:%" PRIi64 ")", smaMgmt.rsetId,
             pRSmaRef->refId);  // pRSmaRef freed in taosHashRemove
    tdReleaseSmaRef(smaMgmt.rsetId, pRSmaRef->refId);
    taosHashRemove(smaMgmt.refHash, &param, POINTER_BYTES);
    return;
  }

  if (RSMA_INFO_IS_DEL(pRSmaInfo)) {
    smaDebug("rsma fetch task not start since rsma info already deleted, rsetId:%d refId:%" PRIi64 ")", smaMgmt.rsetId,
             pRSmaRef->refId);  // pRSmaRef freed in taosHashRemove
    tdReleaseRSmaInfo(pSma, pRSmaInfo);
    tdReleaseSmaRef(smaMgmt.rsetId, pRSmaRef->refId);
    taosHashRemove(smaMgmt.refHash, &param, POINTER_BYTES);
    return;
  }

  pItem = *(SRSmaInfoItem **)&param;

  // if rsma trigger stat in paused, cancelled or finished, not start fetch task
  int8_t rsmaTriggerStat = atomic_load_8(RSMA_TRIGGER_STAT(pStat));
  switch (rsmaTriggerStat) {
    case TASK_TRIGGER_STAT_PAUSED:
    case TASK_TRIGGER_STAT_CANCELLED: {
      smaDebug("vgId:%d, rsma fetch task not start for level %" PRIi8 " since stat is %" PRIi8
               ", rsetId:%d refId:%" PRIi64,
               SMA_VID(pSma), pItem->level, rsmaTriggerStat, smaMgmt.rsetId, pRSmaRef->refId);
      if (rsmaTriggerStat == TASK_TRIGGER_STAT_PAUSED) {
        taosTmrReset(tdRSmaFetchTrigger, RSMA_FETCH_INTERVAL, pItem, smaMgmt.tmrHandle, &pItem->tmrId);
      }
      tdReleaseRSmaInfo(pSma, pRSmaInfo);
      tdReleaseSmaRef(smaMgmt.rsetId, pRSmaRef->refId);
      return;
    }
    default:
      break;
  }

  int8_t fetchTriggerStat =
      atomic_val_compare_exchange_8(&pItem->triggerStat, TASK_TRIGGER_STAT_ACTIVE, TASK_TRIGGER_STAT_INACTIVE);
  switch (fetchTriggerStat) {
    case TASK_TRIGGER_STAT_ACTIVE: {
      smaDebug("vgId:%d, rsma fetch task planned for level:%" PRIi8 " suid:%" PRIi64 " since stat is active",
               SMA_VID(pSma), pItem->level, pRSmaInfo->suid);
      // async process
      atomic_store_8(&pItem->fetchLevel, 1);

      if (atomic_load_8(&pRSmaInfo->assigned) == 0) {
        tsem_post(&(pStat->notEmpty));
      }
    } break;
    case TASK_TRIGGER_STAT_INACTIVE: {
      smaDebug("vgId:%d, rsma fetch task not start for level:%" PRIi8 " suid:%" PRIi64 " since stat is inactive ",
               SMA_VID(pSma), pItem->level, pRSmaInfo->suid);
    } break;
    case TASK_TRIGGER_STAT_INIT: {
      smaDebug("vgId:%d, rsma fetch task not start for level:%" PRIi8 " suid::%" PRIi64 " since stat is init",
               SMA_VID(pSma), pItem->level, pRSmaInfo->suid);
    } break;
    default: {
      smaDebug("vgId:%d, rsma fetch task not start for level:%" PRIi8 " suid:%" PRIi64 " since stat:%" PRIi8
               " is unknown",
               SMA_VID(pSma), pItem->level, pRSmaInfo->suid, fetchTriggerStat);
    } break;
  }

_end:
  taosTmrReset(tdRSmaFetchTrigger, pItem->maxDelay, pItem, smaMgmt.tmrHandle, &pItem->tmrId);
  tdReleaseRSmaInfo(pSma, pRSmaInfo);
  tdReleaseSmaRef(smaMgmt.rsetId, pRSmaRef->refId);
}

static void tdFreeRSmaSubmitItems(SArray *pItems, int32_t type) {
  int32_t arrSize = taosArrayGetSize(pItems);
  if (type == STREAM_INPUT__MERGED_SUBMIT) {
    for (int32_t i = 0; i < arrSize; ++i) {
      SPackedData *packData = TARRAY_GET_ELEM(pItems, i);
      taosFreeQitem(POINTER_SHIFT(packData->msgStr, -RSMA_EXEC_MSG_HLEN));
    }
  } else if (type == STREAM_INPUT__REF_DATA_BLOCK) {
    for (int32_t i = 0; i < arrSize; ++i) {
      SPackedData *packData = TARRAY_GET_ELEM(pItems, i);
      blockDataDestroy(packData->pDataBlock);
    }
  } else {
    ASSERTS(0, "unknown type:%d", type);
  }
  taosArrayClear(pItems);
}

/**
 * @brief fetch rsma result(consider the efficiency and functionality)
 *
 * @param pSma
 * @param pInfo
 * @return int32_t
 */
static int32_t tdRSmaFetchAllResult(SSma *pSma, SRSmaInfo *pInfo) {
  SSDataBlock dataBlock = {.info.type = STREAM_GET_ALL};
  for (int8_t i = 1; i <= TSDB_RETENTION_L2; ++i) {
    SRSmaInfoItem *pItem = RSMA_INFO_ITEM(pInfo, i - 1);

    if (1 == atomic_val_compare_exchange_8(&pItem->fetchLevel, 1, 0)) {
      qTaskInfo_t taskInfo = RSMA_INFO_QTASK(pInfo, i - 1);
      if (!taskInfo) {
        continue;
      }

      if ((++pItem->nScanned * pItem->maxDelay) > RSMA_FETCH_DELAY_MAX) {
        smaDebug("vgId:%d, suid:%" PRIi64 " level:%" PRIi8 " nScanned:%" PRIi32 " maxDelay:%d, fetch executed",
                 SMA_VID(pSma), pInfo->suid, i, pItem->nScanned, pItem->maxDelay);
      } else {
        int64_t curMs = taosGetTimestampMs();
        if ((curMs - pInfo->lastRecv) < RSMA_FETCH_ACTIVE_MAX) {
          smaTrace("vgId:%d, suid:%" PRIi64 " level:%" PRIi8 " curMs:%" PRIi64 " lastRecv:%" PRIi64 ", fetch skipped ",
                   SMA_VID(pSma), pInfo->suid, i, curMs, pInfo->lastRecv);
          atomic_store_8(&pItem->triggerStat, TASK_TRIGGER_STAT_ACTIVE);  // restore the active stat
          continue;
        } else {
          smaDebug("vgId:%d, suid:%" PRIi64 " level:%" PRIi8 " curMs:%" PRIi64 " lastRecv:%" PRIi64 ", fetch executed ",
                   SMA_VID(pSma), pInfo->suid, i, curMs, pInfo->lastRecv);
        }
      }

      pItem->nScanned = 0;

      if ((terrno = qSetSMAInput(taskInfo, &dataBlock, 1, STREAM_INPUT__DATA_BLOCK)) < 0) {
        goto _err;
      }
      if (tdRSmaExecAndSubmitResult(pSma, taskInfo, pItem, pInfo, STREAM_GET_ALL, NULL) < 0) {
        atomic_store_32(&SMA_RSMA_STAT(pSma)->execStat, terrno);
        goto _err;
      }

      smaDebug("vgId:%d, suid:%" PRIi64 " level:%" PRIi8 " nScanned:%" PRIi32 " maxDelay:%d, fetch finished",
               SMA_VID(pSma), pInfo->suid, i, pItem->nScanned, pItem->maxDelay);
    } else {
      smaDebug("vgId:%d, suid:%" PRIi64 " level:%" PRIi8 " nScanned:%" PRIi32
               " maxDelay:%d, fetch not executed as fetch level is %" PRIi8,
               SMA_VID(pSma), pInfo->suid, i, pItem->nScanned, pItem->maxDelay, pItem->fetchLevel);
    }
  }

_end:
  return TSDB_CODE_SUCCESS;
_err:
  return TSDB_CODE_FAILED;
}

static int32_t tdRSmaBatchExec(SSma *pSma, SRSmaInfo *pInfo, STaosQall *qall, SArray *pSubmitArr, ERsmaExecType type) {
  void *  msg = NULL;
  int8_t  resume = 0;
  int32_t nSubmit = 0;
  int32_t nDelete = 0;
  int64_t version = 0;

  SPackedData packData;

  taosArrayClear(pSubmitArr);

  // the submitReq/deleteReq msg may exsit alternately in the msg queue, consume them sequentially in batch mode
  while (1) {
    taosGetQitem(qall, (void **)&msg);
    if (msg) {
      int8_t inputType = RSMA_EXEC_MSG_TYPE(msg);
      if (inputType == STREAM_INPUT__DATA_SUBMIT) {
        if (nDelete > 0) {
          resume = 1;
          break;
        }
      _resume_submit:
        packData.msgLen = RSMA_EXEC_MSG_LEN(msg);
        packData.ver = RSMA_EXEC_MSG_VER(msg);
        packData.msgStr = RSMA_EXEC_MSG_BODY(msg);
        version = packData.ver;
        if (!taosArrayPush(pSubmitArr, &packData)) {
          taosFreeQitem(msg);
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }
        ++nSubmit;
      } else if (inputType == STREAM_INPUT__REF_DATA_BLOCK) {
        if (nSubmit > 0) {
          resume = 2;
          break;
        }
      _resume_delete:
        version = RSMA_EXEC_MSG_VER(msg);
        if ((terrno = tqExtractDelDataBlock(RSMA_EXEC_MSG_BODY(msg), RSMA_EXEC_MSG_LEN(msg), version,
                                          &packData.pDataBlock, 1))) {
          taosFreeQitem(msg);
          goto _err;
        }

        if (packData.pDataBlock && !taosArrayPush(pSubmitArr, &packData)) {
          taosFreeQitem(msg);
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }
        taosFreeQitem(msg);
        if (packData.pDataBlock) {
          // packData.pDataBlock is NULL if delete affects 0 row
          ++nDelete;
        }
      } else {
        ASSERTS(0, "unknown msg type:%d", inputType);
        break;
      }
    }

    if (nSubmit > 0 || nDelete > 0) {
      int32_t size = TARRAY_SIZE(pSubmitArr);
      ASSERTS(size > 0, "size is %d", size);
      int32_t inputType = nSubmit > 0 ? STREAM_INPUT__MERGED_SUBMIT : STREAM_INPUT__REF_DATA_BLOCK;
      for (int32_t i = 1; i <= TSDB_RETENTION_L2; ++i) {
        if (tdExecuteRSmaImpl(pSma, pSubmitArr->pData, size, version, inputType, pInfo, type, i) < 0) {
          goto _err;
        }
      }
      tdFreeRSmaSubmitItems(pSubmitArr, inputType);
      nSubmit = 0;
      nDelete = 0;
    } else {
      goto _rtn;
    }

    if (resume == 1) {
      resume = 0;
      goto _resume_submit;
    } else if (resume == 2) {
      resume = 0;
      goto _resume_delete;
    }
  }

_rtn:
  return TSDB_CODE_SUCCESS;
_err:
  atomic_store_32(&SMA_RSMA_STAT(pSma)->execStat, terrno);
  smaError("vgId:%d, batch exec for suid:%" PRIi64 " execType:%d size:%d failed since %s", SMA_VID(pSma), pInfo->suid,
           type, (int32_t)taosArrayGetSize(pSubmitArr), terrstr());
  tdFreeRSmaSubmitItems(pSubmitArr, nSubmit ? STREAM_INPUT__MERGED_SUBMIT : STREAM_INPUT__REF_DATA_BLOCK);
  while (1) {
    void *msg = NULL;
    taosGetQitem(qall, (void **)&msg);
    if (msg) {
      taosFreeQitem(msg);
    } else {
      break;
    }
  }
  return TSDB_CODE_FAILED;
}

/**
 * @brief
 *
 * @param pSma
 * @param type
 * @return int32_t
 */

int32_t tdRSmaProcessExecImpl(SSma *pSma, ERsmaExecType type) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SVnode *   pVnode = pSma->pVnode;
  SSmaEnv *  pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pRSmaStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SHashObj * infoHash = NULL;
  SArray *   pSubmitArr = NULL;
  bool       isFetchAll = false;

  if (!pRSmaStat || !(infoHash = RSMA_INFO_HASH(pRSmaStat))) {
    code = TSDB_CODE_RSMA_INVALID_STAT;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (!(pSubmitArr =
            taosArrayInit(TMIN(RSMA_EXEC_BATCH_SIZE, atomic_load_64(&pRSmaStat->nBufItems)), sizeof(SPackedData)))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  while (true) {
    // step 1: rsma exec - consume data in buffer queue for all suids
    if (type == RSMA_EXEC_OVERFLOW) {
      void *pIter = NULL;
      while ((pIter = taosHashIterate(infoHash, pIter))) {
        SRSmaInfo *pInfo = *(SRSmaInfo **)pIter;
        if (atomic_val_compare_exchange_8(&pInfo->assigned, 0, 1) == 0) {
          if ((taosQueueItemSize(pInfo->queue) > 0) || RSMA_NEED_FETCH(pInfo)) {
            int32_t batchCnt = -1;
            int32_t batchMax = taosHashGetSize(infoHash) / tsNumOfVnodeRsmaThreads;
            bool    occupied = (batchMax <= 1);
            if (batchMax > 1) {
              batchMax = 100 / batchMax;
              batchMax = TMAX(batchMax, 4);
            }
            while (occupied || (++batchCnt < batchMax)) {    // greedy mode
              taosReadAllQitems(pInfo->queue, pInfo->qall);  // queue has mutex lock
              int32_t qallItemSize = taosQallItemSize(pInfo->qall);
              if (qallItemSize > 0) {
                tdRSmaBatchExec(pSma, pInfo, pInfo->qall, pSubmitArr, type);
                smaDebug("vgId:%d, batchSize:%d, execType:%" PRIi32, SMA_VID(pSma), qallItemSize, type);
              }

              if (RSMA_NEED_FETCH(pInfo)) {
                int8_t oldStat = atomic_val_compare_exchange_8(RSMA_COMMIT_STAT(pRSmaStat), 0, 2);
                if (oldStat == 0 ||
                    ((oldStat == 2) && atomic_load_8(RSMA_TRIGGER_STAT(pRSmaStat)) < TASK_TRIGGER_STAT_PAUSED)) {
                  int32_t oldVal = atomic_fetch_add_32(&pRSmaStat->nFetchAll, 1);

                  if (ASSERTS(oldVal >= 0, "oldVal of nFetchAll: %d < 0", oldVal)) {
                    code = TSDB_CODE_APP_ERROR;
                    taosHashCancelIterate(infoHash, pIter);
                    TSDB_CHECK_CODE(code, lino, _exit);
                  }

                  tdRSmaFetchAllResult(pSma, pInfo);

                  if (0 == atomic_sub_fetch_32(&pRSmaStat->nFetchAll, 1)) {
                    atomic_store_8(RSMA_COMMIT_STAT(pRSmaStat), 0);
                  }
                }
              }

              if (qallItemSize > 0) {
                atomic_fetch_sub_64(&pRSmaStat->nBufItems, qallItemSize);
                continue;
              }
              if (RSMA_NEED_FETCH(pInfo)) {
                continue;
              }

              break;
            }
          }
          atomic_val_compare_exchange_8(&pInfo->assigned, 1, 0);
        }
      }
    } else {
      ASSERTS(0, "unknown rsma exec type:%d", (int32_t)type);
      code = TSDB_CODE_APP_ERROR;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (atomic_load_64(&pRSmaStat->nBufItems) <= 0) {
      if (pEnv->flag & SMA_ENV_FLG_CLOSE) {
        break;
      }

      tsem_wait(&pRSmaStat->notEmpty);

      if ((pEnv->flag & SMA_ENV_FLG_CLOSE) && (atomic_load_64(&pRSmaStat->nBufItems) <= 0)) {
        smaDebug("vgId:%d, exec task end, flag:%" PRIi8 ", nBufItems:%" PRIi64, SMA_VID(pSma), pEnv->flag,
                 atomic_load_64(&pRSmaStat->nBufItems));
        break;
      }
    }

  }  // end of while(true)

_exit:
  taosArrayDestroy(pSubmitArr);
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}