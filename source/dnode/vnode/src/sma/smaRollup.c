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
#include "tstream.h"

static FORCE_INLINE int32_t tdUidStorePut(STbUidStore *pStore, tb_uid_t suid, tb_uid_t *uid);
static FORCE_INLINE int32_t tdUpdateTbUidListImpl(SSma *pSma, tb_uid_t *suid, SArray *tbUids);
static FORCE_INLINE int32_t tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t inputType, SRSmaInfoItem *rsmaItem,
                                              tb_uid_t suid, int8_t level);

#define SET_RSMA_INFO_ITEM_PARAMS(__idx, __level)                                                               \
  if (param->qmsg[__idx]) {                                                                                     \
    pRSmaInfo->items[__idx].pRsmaInfo = pRSmaInfo;                                                              \
    pRSmaInfo->items[__idx].taskInfo = qCreateStreamExecTaskInfo(param->qmsg[0], &handle);                      \
    if (!pRSmaInfo->items[__idx].taskInfo) {                                                                    \
      goto _err;                                                                                                \
    }                                                                                                           \
    pRSmaInfo->items[__idx].triggerStatus = TASK_TRIGGER_STATUS__IN_ACTIVE;                                     \
    if (param->maxdelay[__idx] < 1) {                                                                           \
      int64_t msInterval =                                                                                      \
          convertTimeFromPrecisionToUnit(pRetention[__level].freq, pTsdbCfg->precision, TIME_UNIT_MILLISECOND); \
      pRSmaInfo->items[__idx].maxDelay = msInterval;                                                            \
    } else {                                                                                                    \
      pRSmaInfo->items[__idx].maxDelay = param->maxdelay[__idx];                                                \
    }                                                                                                           \
    if (pRSmaInfo->items[__idx].maxDelay > TSDB_MAX_ROLLUP_MAX_DELAY) {                                         \
      pRSmaInfo->items[__idx].maxDelay = TSDB_MAX_ROLLUP_MAX_DELAY;                                             \
    }                                                                                                           \
    pRSmaInfo->items[__idx].level = TSDB_RETENTION_L##__level;                                                  \
    pRSmaInfo->items[__idx].tmrHandle = taosTmrInit(10000, 100, 10000, "RSMA");                                 \
    if (!pRSmaInfo->items[__idx].tmrHandle) {                                                                   \
      goto _err;                                                                                                \
    }                                                                                                           \
  }

struct SRSmaInfoItem {
  SRSmaInfo *pRsmaInfo;
  void      *taskInfo;  // qTaskInfo_t
  void      *tmrHandle;
  tmr_h      tmrId;
  int8_t     level;
  int8_t     tmrInitFlag;
  int8_t     triggerStatus;  // TASK_TRIGGER_STATUS__IN_ACTIVE/TASK_TRIGGER_STATUS__ACTIVE
  int32_t    maxDelay;
};

typedef struct {
  int64_t        suid;
  SRSmaInfoItem *pItem;
  SSma          *pSma;
  STSchema      *pTSchema;
} SRSmaTriggerParam;

struct SRSmaInfo {
  STSchema     *pTSchema;
  SSma         *pSma;
  int64_t       suid;
  SRSmaInfoItem items[TSDB_RETENTION_L2];
};

static FORCE_INLINE void tdFreeTaskHandle(qTaskInfo_t *taskHandle) {
  // Note: free/kill may in RC
  qTaskInfo_t otaskHandle = atomic_load_ptr(taskHandle);
  if (otaskHandle && atomic_val_compare_exchange_ptr(taskHandle, otaskHandle, NULL)) {
    qDestroyTask(otaskHandle);
  }
}

void *tdFreeRSmaInfo(SRSmaInfo *pInfo) {
  if (pInfo) {
    for (int32_t i = 0; i < TSDB_RETENTION_MAX; ++i) {
      SRSmaInfoItem *pItem = &pInfo->items[i];
      if (pItem->taskInfo) {
        tdFreeTaskHandle(pItem->taskInfo);
      }
      if (pItem->tmrHandle) {
        taosTmrCleanUp(pItem->tmrHandle);
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

static FORCE_INLINE int32_t tdUpdateTbUidListImpl(SSma *pSma, tb_uid_t *suid, SArray *tbUids) {
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  if (!suid || !tbUids) {
    terrno = TSDB_CODE_INVALID_PTR;
    smaError("vgId:%d, failed to get rsma info for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  }

  pRSmaInfo = taosHashGet(SMA_STAT_INFO_HASH(pStat), suid, sizeof(tb_uid_t));
  if (!pRSmaInfo || !(pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    smaError("vgId:%d, failed to get rsma info for uid:%" PRIi64, SMA_VID(pSma), *suid);
    terrno = TSDB_CODE_RSMA_INVALID_STAT;
    return TSDB_CODE_FAILED;
  }

  if (pRSmaInfo->items[0].taskInfo && (qUpdateQualifiedTableId(pRSmaInfo->items[0].taskInfo, tbUids, true) < 0)) {
    smaError("vgId:%d, update tbUidList failed for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  } else {
    smaDebug("vgId:%d, update tbUidList succeed for qTaskInfo:%p with suid:%" PRIi64 ", uid:%" PRIi64, SMA_VID(pSma),
             pRSmaInfo->items[0].taskInfo, *suid, *(int64_t *)taosArrayGet(tbUids, 0));
  }

  if (pRSmaInfo->items[1].taskInfo && (qUpdateQualifiedTableId(pRSmaInfo->items[1].taskInfo, tbUids, true) < 0)) {
    smaError("vgId:%d, update tbUidList failed for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  } else {
    smaDebug("vgId:%d, update tbUidList succeed for qTaskInfo:%p with suid:%" PRIi64 ", uid:%" PRIi64, SMA_VID(pSma),
             pRSmaInfo->items[1].taskInfo, *suid, *(int64_t *)taosArrayGet(tbUids, 0));
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

  SSmaStat *pStat = SMA_ENV_STAT(pEnv);
  SHashObj *infoHash = NULL;
  if (!pStat || !(infoHash = SMA_STAT_INFO_HASH(pStat))) {
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

/**
 * @brief Check and init qTaskInfo_t, only applicable to stable with SRSmaParam.
 *
 * @param pTsdb
 * @param pMeta
 * @param pReq
 * @return int32_t
 */
int32_t tdProcessRSmaCreate(SVnode *pVnode, SVCreateStbReq *pReq) {
  SSma *pSma = pVnode->pSma;
  if (!pReq->rollup) {
    smaTrace("vgId:%d, return directly since no rollup for stable %s %" PRIi64, SMA_VID(pSma), pReq->name, pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  SMeta      *pMeta = pVnode->pMeta;
  SMsgCb     *pMsgCb = &pVnode->msgCb;
  SRSmaParam *param = &pReq->pRSmaParam;

  if ((param->qmsgLen[0] == 0) && (param->qmsgLen[1] == 0)) {
    smaWarn("vgId:%d, no qmsg1/qmsg2 for rollup stable %s %" PRIi64, SMA_VID(pSma), pReq->name, pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  if (tdCheckAndInitSmaEnv(pSma, TSDB_SMA_TYPE_ROLLUP) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  pRSmaInfo = taosHashGet(SMA_STAT_INFO_HASH(pStat), &pReq->suid, sizeof(tb_uid_t));
  if (pRSmaInfo) {
    ASSERT(0);  // TODO: free original pRSmaInfo is exists abnormally
    smaWarn("vgId:%d, rsma info already exists for stb: %s, %" PRIi64, SMA_VID(pSma), pReq->name, pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  // from write queue: single thead
  pRSmaInfo = (SRSmaInfo *)taosMemoryCalloc(1, sizeof(SRSmaInfo));
  if (!pRSmaInfo) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  STqReadHandle *pReadHandle = tqInitSubmitMsgScanner(pMeta);
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

  STSchema *pTSchema = metaGetTbTSchema(SMA_META(pSma), pReq->suid, -1);
  if (!pTSchema) {
    terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
    goto _err;
  }
  pRSmaInfo->pTSchema = pTSchema;
  pRSmaInfo->pSma = pSma;
  pRSmaInfo->suid = pReq->suid;

  SRetention *pRetention = SMA_RETENTION(pSma);
  STsdbCfg   *pTsdbCfg = SMA_TSDB_CFG(pSma);

  SET_RSMA_INFO_ITEM_PARAMS(0, 1);
  SET_RSMA_INFO_ITEM_PARAMS(1, 2);

  if (taosHashPut(SMA_STAT_INFO_HASH(pStat), &pReq->suid, sizeof(tb_uid_t), &pRSmaInfo, sizeof(pRSmaInfo)) !=
      TSDB_CODE_SUCCESS) {
    goto _err;
  } else {
    smaDebug("vgId:%d, register rsma info succeed for suid:%" PRIi64, SMA_VID(pSma), pReq->suid);
  }

  return TSDB_CODE_SUCCESS;
_err:
  tdFreeRSmaInfo(pRSmaInfo);
  taosMemoryFree(pReadHandle);
  return TSDB_CODE_FAILED;
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
    pStore->uid = msgIter.uid;  // TODO: remove, just for debugging
  }

  if (terrno != TSDB_CODE_SUCCESS) return -1;
  return 0;
}

static int32_t tdFetchAndSubmitRSmaResult(SRSmaInfoItem *pItem, int8_t blkType) {
  SArray    *pResult = NULL;
  SRSmaInfo *pRSmaInfo = pItem->pRsmaInfo;
  SSma      *pSma = pRSmaInfo->pSma;

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
      pResult = taosArrayInit(0, sizeof(SSDataBlock));
      if (!pResult) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return TSDB_CODE_FAILED;
      }
    }

    taosArrayPush(pResult, output);
  }

  if (taosArrayGetSize(pResult) > 0) {
#if 0
    char flag[10] = {0};
    snprintf(flag, 10, "level %" PRIi8, pItem->level);
    blockDebugShowData(pResult, flag);
#endif
    STsdb      *sinkTsdb = (pItem->level == TSDB_RETENTION_L1 ? pSma->pRSmaTsdb1 : pSma->pRSmaTsdb2);
    SSubmitReq *pReq = NULL;
    if (buildSubmitReqFromDataBlock(&pReq, pResult, pRSmaInfo->pTSchema, SMA_VID(pSma), pRSmaInfo->suid) < 0) {
      taosArrayDestroy(pResult);
      return TSDB_CODE_FAILED;
    }

    if (pReq && tdProcessSubmitReq(sinkTsdb, INT64_MAX, pReq) < 0) {
      taosArrayDestroy(pResult);
      taosMemoryFreeClear(pReq);
      return TSDB_CODE_FAILED;
    }

    taosMemoryFreeClear(pReq);
  } else {
    smaDebug("vgId:%d, no rsma % " PRIi8 " data generated since %s", SMA_VID(pSma), pItem->level, tstrerror(terrno));
  }

  if (blkType == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
    atomic_store_8(&pItem->triggerStatus, TASK_TRIGGER_STATUS__ACTIVE);
  }

  taosArrayDestroy(pResult);
  return 0;
}

/**
 * @brief trigger to get rsma result
 *
 * @param param
 * @param tmrId
 */
static void rsmaTriggerByTimer(void *param, void *tmrId) {
  // SRSmaTriggerParam *pParam = (SRSmaTriggerParam *)param;
  // SRSmaInfoItem     *pItem = pParam->pItem;
  SRSmaInfoItem *pItem = param;

  if (atomic_load_8(&pItem->triggerStatus) == TASK_TRIGGER_STATUS__ACTIVE) {
    smaTrace("level %" PRIi8 " status is active for tb suid:%" PRIi64, pItem->level, pItem->pRsmaInfo->suid);
    SSDataBlock dataBlock = {.info.type = STREAM_GET_ALL};

    atomic_store_8(&pItem->triggerStatus, TASK_TRIGGER_STATUS__IN_ACTIVE);
    qSetStreamInput(pItem->taskInfo, &dataBlock, STREAM_DATA_TYPE_SSDATA_BLOCK, false);

    tdFetchAndSubmitRSmaResult(pItem, STREAM_DATA_TYPE_SSDATA_BLOCK);
  } else {
    smaTrace("level %" PRIi8 " status is inactive for tb suid:%" PRIi64, pItem->level, pItem->pRsmaInfo->suid);
  }

  // taosTmrReset(rsmaTriggerByTimer, pItem->maxDelay, pItem, pItem->tmrHandle, &pItem->tmrId);
}

static FORCE_INLINE int32_t tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t inputType, SRSmaInfoItem *pItem,
                                              tb_uid_t suid, int8_t level) {
  if (!pItem || !pItem->taskInfo) {
    smaDebug("vgId:%d, no qTaskInfo to execute rsma %" PRIi8 " task for suid:%" PRIu64, SMA_VID(pSma), level, suid);
    return TSDB_CODE_SUCCESS;
  }

  smaDebug("vgId:%d, execute rsma %" PRIi8 " task for qTaskInfo:%p suid:%" PRIu64, SMA_VID(pSma), level,
           pItem->taskInfo, suid);

  // inputType = STREAM_DATA_TYPE_SUBMIT_BLOCK(1)
  if (qSetStreamInput(pItem->taskInfo, pMsg, inputType, true) < 0) {
    smaError("vgId:%d, rsma % " PRIi8 " qSetStreamInput failed since %s", SMA_VID(pSma), level, tstrerror(terrno));
    return TSDB_CODE_FAILED;
  }

  // SRSmaTriggerParam triggerParam = {.suid = suid, .pItem = pItem, .pSma = pSma, .pTSchema = pTSchema};
  tdFetchAndSubmitRSmaResult(pItem, STREAM_DATA_TYPE_SUBMIT_BLOCK);
  atomic_store_8(&pItem->triggerStatus, TASK_TRIGGER_STATUS__ACTIVE);
  taosTmrReset(rsmaTriggerByTimer, pItem->maxDelay, pItem, pItem->tmrHandle, &pItem->tmrId);

  return TSDB_CODE_SUCCESS;
}

static int32_t tdExecuteRSma(SSma *pSma, const void *pMsg, int32_t inputType, tb_uid_t suid) {
  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);
  if (!pEnv) {
    // only applicable when rsma env exists
    return TSDB_CODE_SUCCESS;
  }

  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  pRSmaInfo = taosHashGet(SMA_STAT_INFO_HASH(pStat), &suid, sizeof(tb_uid_t));

  if (!pRSmaInfo || !(pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    smaDebug("vgId:%d, return as no rsma info for suid:%" PRIu64, SMA_VID(pSma), suid);
    return TSDB_CODE_SUCCESS;
  }

  if (!pRSmaInfo->items[0].taskInfo) {
    smaDebug("vgId:%d, return as no rsma qTaskInfo for suid:%" PRIu64, SMA_VID(pSma), suid);
    return TSDB_CODE_SUCCESS;
  }

  if (inputType == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
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

  if (inputType == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
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
