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

static FORCE_INLINE int32_t tdUidStorePut(STbUidStore *pStore, tb_uid_t suid, tb_uid_t *uid);
static FORCE_INLINE int32_t tdUpdateTbUidListImpl(SSma *pSma, tb_uid_t *suid, SArray *tbUids);
static FORCE_INLINE int32_t tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t inputType, qTaskInfo_t *taskInfo,
                                              STSchema *pTSchema, tb_uid_t suid, tb_uid_t uid, int8_t level);

struct SRSmaInfo {
  void *taskInfo[TSDB_RETENTION_L2];  // qTaskInfo_t
};

static FORCE_INLINE void tdFreeTaskHandle(qTaskInfo_t *taskHandle) {
  // Note: free/kill may in RC
  qTaskInfo_t otaskHandle = atomic_load_ptr(taskHandle);
  if (otaskHandle && atomic_val_compare_exchange_ptr(taskHandle, otaskHandle, NULL)) {
    qDestroyTask(otaskHandle);
  }
}

void *tdFreeRSmaInfo(SRSmaInfo *pInfo) {
  for (int32_t i = 0; i < TSDB_RETENTION_MAX; ++i) {
    if (pInfo->taskInfo[i]) {
      tdFreeTaskHandle(pInfo->taskInfo[i]);
    }
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
    smaError("vgId:%d failed to get rsma info for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  }

  pRSmaInfo = taosHashGet(SMA_STAT_INFO_HASH(pStat), suid, sizeof(tb_uid_t));
  if (!pRSmaInfo || !(pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    smaError("vgId:%d failed to get rsma info for uid:%" PRIi64, SMA_VID(pSma), *suid);
    terrno = TSDB_CODE_TDB_INVALID_SMA_STAT;
    return TSDB_CODE_FAILED;
  }

  if (pRSmaInfo->taskInfo[0] && (qUpdateQualifiedTableId(pRSmaInfo->taskInfo[0], tbUids, true) != 0)) {
    smaError("vgId:%d update tbUidList failed for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  } else {
    smaDebug("vgId:%d update tbUidList succeed for qTaskInfo:%p with suid:%" PRIi64 ", uid:%" PRIi64, SMA_VID(pSma),
             pRSmaInfo->taskInfo[0], *suid, *(int64_t *)taosArrayGet(tbUids, 0));
  }

  if (pRSmaInfo->taskInfo[1] && (qUpdateQualifiedTableId(pRSmaInfo->taskInfo[1], tbUids, true) != 0)) {
    smaError("vgId:%d update tbUidList failed for uid:%" PRIi64 " since %s", SMA_VID(pSma), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  } else {
    smaDebug("vgId:%d update tbUidList succeed for qTaskInfo:%p with suid:%" PRIi64 ", uid:%" PRIi64, SMA_VID(pSma),
             pRSmaInfo->taskInfo[1], *suid, *(int64_t *)taosArrayGet(tbUids, 0));
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
    terrno = TSDB_CODE_TDB_INVALID_SMA_STAT;
    return TSDB_CODE_FAILED;
  }

  // info cached when create rsma stable and return directly for non-rsma ctables
  if (!taosHashGet(infoHash, &suid, sizeof(tb_uid_t))) {
    return TSDB_CODE_SUCCESS;
  }

  ASSERT(ppStore != NULL);

  if (!(*ppStore)) {
    if (tdUidStoreInit(ppStore) != 0) {
      return TSDB_CODE_FAILED;
    }
  }

  if (tdUidStorePut(*ppStore, suid, &uid) != 0) {
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
int32_t tdProcessRSmaCreate(SSma *pSma, SMeta *pMeta, SVCreateStbReq *pReq, SMsgCb *pMsgCb) {
  if (!pReq->rollup) {
    smaTrace("vgId:%d return directly since no rollup for stable %s %" PRIi64, SMA_VID(pSma), pReq->name, pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  SRSmaParam *param = &pReq->pRSmaParam;

  if ((param->qmsg1Len == 0) && (param->qmsg2Len == 0)) {
    smaWarn("vgId:%d no qmsg1/qmsg2 for rollup stable %s %" PRIi64, SMA_VID(pSma), pReq->name, pReq->suid);
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
    smaWarn("vgId:%d rsma info already exists for stb: %s, %" PRIi64, SMA_VID(pSma), pReq->name, pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  pRSmaInfo = (SRSmaInfo *)taosMemoryCalloc(1, sizeof(SRSmaInfo));
  if (!pRSmaInfo) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  STqReadHandle *pReadHandle = tqInitSubmitMsgScanner(pMeta);
  if (!pReadHandle) {
    taosMemoryFree(pRSmaInfo);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  SReadHandle handle = {
      .reader = pReadHandle,
      .meta = pMeta,
      .pMsgCb = pMsgCb,
  };

  if (param->qmsg1) {
    pRSmaInfo->taskInfo[0] = qCreateStreamExecTaskInfo(param->qmsg1, &handle);
    if (!pRSmaInfo->taskInfo[0]) {
      taosMemoryFree(pRSmaInfo);
      taosMemoryFree(pReadHandle);
      return TSDB_CODE_FAILED;
    }
  }

  if (param->qmsg2) {
    pRSmaInfo->taskInfo[1] = qCreateStreamExecTaskInfo(param->qmsg2, &handle);
    if (!pRSmaInfo->taskInfo[1]) {
      taosMemoryFree(pRSmaInfo);
      taosMemoryFree(pReadHandle);
      return TSDB_CODE_FAILED;
    }
  }

  if (taosHashPut(SMA_STAT_INFO_HASH(pStat), &pReq->suid, sizeof(tb_uid_t), &pRSmaInfo, sizeof(pRSmaInfo)) !=
      TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  } else {
    smaDebug("vgId:%d register rsma info succeed for suid:%" PRIi64, SMA_VID(pSma), pReq->suid);
  }

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
        if (taosHashPut(pStore->uidHash, &suid, sizeof(suid), &pUidArray, sizeof(pUidArray)) != 0) {
          return TSDB_CODE_FAILED;
        }
      }
    } else {
      if (taosHashPut(pStore->uidHash, &suid, sizeof(suid), NULL, 0) != 0) {
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

static FORCE_INLINE int32_t tdExecuteRSmaImpl(SSma *pSma, const void *pMsg, int32_t inputType, qTaskInfo_t *taskInfo,
                                              STSchema *pTSchema, tb_uid_t suid, tb_uid_t uid, int8_t level) {
  SArray *pResult = NULL;

  if (!taskInfo) {
    smaDebug("vgId:%d no qTaskInfo to execute rsma %" PRIi8 " task for suid:%" PRIu64, SMA_VID(pSma), level, suid);
    return TSDB_CODE_SUCCESS;
  }

  smaDebug("vgId:%d execute rsma %" PRIi8 " task for qTaskInfo:%p suid:%" PRIu64, SMA_VID(pSma), level, taskInfo, suid);

  qSetStreamInput(taskInfo, pMsg, inputType, true);
  while (1) {
    SSDataBlock *output = NULL;
    uint64_t     ts;
    if (qExecTask(taskInfo, &output, &ts) < 0) {
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
    blockDebugShowData(pResult);
    STsdb      *sinkTsdb = (level == TSDB_RETENTION_L1 ? pSma->pRSmaTsdb1 : pSma->pRSmaTsdb2);
    SSubmitReq *pReq = NULL;
    if (buildSubmitReqFromDataBlock(&pReq, pResult, pTSchema, SMA_VID(pSma), uid, suid) != 0) {
      taosArrayDestroy(pResult);
      return TSDB_CODE_FAILED;
    }
    if (tdProcessSubmitReq(sinkTsdb, INT64_MAX, pReq) != 0) {
      taosArrayDestroy(pResult);
      taosMemoryFreeClear(pReq);
      return TSDB_CODE_FAILED;
    }
    taosMemoryFreeClear(pReq);
  } else {
    smaWarn("vgId:%d no rsma % " PRIi8 " data generated since %s", SMA_VID(pSma), level, tstrerror(terrno));
  }

  taosArrayDestroy(pResult);

  return TSDB_CODE_SUCCESS;
}

static int32_t tdExecuteRSma(SSma *pSma, const void *pMsg, int32_t inputType, tb_uid_t suid, tb_uid_t uid) {
  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);
  if (!pEnv) {
    // only applicable when rsma env exists
    return TSDB_CODE_SUCCESS;
  }

  ASSERT(uid != 0);  // TODO: remove later

  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  pRSmaInfo = taosHashGet(SMA_STAT_INFO_HASH(pStat), &suid, sizeof(tb_uid_t));

  if (!pRSmaInfo || !(pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    smaDebug("vgId:%d no rsma info for suid:%" PRIu64, SMA_VID(pSma), suid);
    return TSDB_CODE_SUCCESS;
  }
  if (!pRSmaInfo->taskInfo[0]) {
    smaDebug("vgId:%d no rsma qTaskInfo for suid:%" PRIu64, SMA_VID(pSma), suid);
    return TSDB_CODE_SUCCESS;
  }

  if (inputType == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
    // TODO: use the proper schema instead of 0, and cache STSchema in cache
    STSchema *pTSchema = metaGetTbTSchema(SMA_META(pSma), suid, 1);
    if (!pTSchema) {
      terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
      return TSDB_CODE_FAILED;
    }
    tdExecuteRSmaImpl(pSma, pMsg, inputType, pRSmaInfo->taskInfo[0], pTSchema, suid, uid, TSDB_RETENTION_L1);
    tdExecuteRSmaImpl(pSma, pMsg, inputType, pRSmaInfo->taskInfo[1], pTSchema, suid, uid, TSDB_RETENTION_L2);
    taosMemoryFree(pTSchema);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tdProcessRSmaSubmit(SSma *pSma, void *pMsg, int32_t inputType) {
  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);
  if (!pEnv) {
    // only applicable when rsma env exists
    return TSDB_CODE_SUCCESS;
  }

  if (inputType == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
    STbUidStore uidStore = {0};
    tdFetchSubmitReqSuids(pMsg, &uidStore);

    if (uidStore.suid != 0) {
      tdExecuteRSma(pSma, pMsg, inputType, uidStore.suid, uidStore.uid);

      void *pIter = taosHashIterate(uidStore.uidHash, NULL);
      while (pIter) {
        tb_uid_t *pTbSuid = (tb_uid_t *)taosHashGetKey(pIter, NULL);
        tdExecuteRSma(pSma, pMsg, inputType, *pTbSuid, 0);
        pIter = taosHashIterate(uidStore.uidHash, pIter);
      }

      tdUidStoreDestory(&uidStore);
    }
  }
  return TSDB_CODE_SUCCESS;
}
