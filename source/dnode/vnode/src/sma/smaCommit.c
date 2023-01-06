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

extern SSmaMgmt smaMgmt;

#if 0
static int32_t tdProcessRSmaSyncPreCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaSyncCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaSyncPostCommitImpl(SSma *pSma);
#endif
static int32_t tdProcessRSmaAsyncPreCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaAsyncCommitImpl(SSma *pSma, SCommitInfo *pInfo);
static int32_t tdProcessRSmaAsyncPostCommitImpl(SSma *pSma);
static int32_t tdUpdateQTaskInfoFiles(SSma *pSma, SRSmaStat *pRSmaStat);

#if 0
/**
 * @brief Only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaSyncPreCommit(SSma *pSma) { return tdProcessRSmaSyncPreCommitImpl(pSma); }

/**
 * @brief Only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaSyncCommit(SSma *pSma) { return tdProcessRSmaSyncCommitImpl(pSma); }

/**
 * @brief Only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaSyncPostCommit(SSma *pSma) { return tdProcessRSmaSyncPostCommitImpl(pSma); }
#endif

/**
 * @brief async commit, only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaPrepareAsyncCommit(SSma *pSma) { return tdProcessRSmaAsyncPreCommitImpl(pSma); }

/**
 * @brief async commit, only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaCommit(SSma *pSma, SCommitInfo *pInfo) { return tdProcessRSmaAsyncCommitImpl(pSma, pInfo); }

/**
 * @brief async commit, only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaPostCommit(SSma *pSma) { return tdProcessRSmaAsyncPostCommitImpl(pSma); }

/**
 * @brief prepare rsma1/2, and set rsma trigger stat active
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaBegin(SSma *pSma) {
  int32_t code = 0;
  SVnode *pVnode = pSma->pVnode;

  if ((code = tsdbBegin(VND_RSMA1(pVnode))) < 0) {
    smaError("vgId:%d, failed to begin rsma1 since %s", TD_VID(pVnode), tstrerror(code));
    goto _exit;
  }

  if ((code = tsdbBegin(VND_RSMA2(pVnode))) < 0) {
    smaError("vgId:%d, failed to begin rsma2 since %s", TD_VID(pVnode), tstrerror(code));
    goto _exit;
  }

  // set trigger stat
  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    goto _exit;
  }
  SRSmaStat *pRSmaStat = (SRSmaStat *)SMA_ENV_STAT(pSmaEnv);
  int8_t     rsmaTriggerStat =
      atomic_val_compare_exchange_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_PAUSED, TASK_TRIGGER_STAT_ACTIVE);
  switch (rsmaTriggerStat) {
    case TASK_TRIGGER_STAT_PAUSED: {
      smaDebug("vgId:%d, rsma trigger stat from paused to active", TD_VID(pVnode));
      break;
    }
    case TASK_TRIGGER_STAT_INIT: {
      atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_ACTIVE);
      smaDebug("vgId:%d, rsma trigger stat from init to active", TD_VID(pVnode));
      break;
    }
    default: {
      atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_ACTIVE);
      smaWarn("vgId:%d, rsma trigger stat %" PRIi8 " is unexpected", TD_VID(pVnode), rsmaTriggerStat);
      break;
    }
  }
_exit:
  terrno = code;
  return code;
}

int32_t smaFinishCommit(SSma *pSma) {
  int32_t code = 0;
  SVnode *pVnode = pSma->pVnode;

  if (VND_RSMA1(pVnode) && (code = tsdbFinishCommit(VND_RSMA1(pVnode))) < 0) {
    smaError("vgId:%d, failed to finish commit tsdb rsma1 since %s", TD_VID(pVnode), tstrerror(code));
    goto _exit;
  }
  if (VND_RSMA2(pVnode) && (code = tsdbFinishCommit(VND_RSMA2(pVnode))) < 0) {
    smaError("vgId:%d, failed to finish commit tsdb rsma2 since %s", TD_VID(pVnode), tstrerror(code));
    goto _exit;
  }
_exit:
  terrno = code;
  return code;
}

#if 0
/**
 * @brief pre-commit for rollup sma(sync commit).
 *  1) set trigger stat of rsma timer TASK_TRIGGER_STAT_PAUSED.
 *  2) wait for all triggered fetch tasks to finish
 *  3) perform persist task for qTaskInfo
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaSyncPreCommitImpl(SSma *pSma) {
  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SSmaStat  *pStat = SMA_ENV_STAT(pSmaEnv);
  SRSmaStat *pRSmaStat = SMA_STAT_RSMA(pStat);

  // step 1: set rsma stat paused
  atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_PAUSED);

  // step 2: wait for all triggered fetch tasks to finish
  int32_t nLoops = 0;
  while (1) {
    if (T_REF_VAL_GET(pStat) == 0) {
      smaDebug("vgId:%d, rsma fetch tasks are all finished", SMA_VID(pSma));
      break;
    } else {
      smaDebug("vgId:%d, rsma fetch tasks are not all finished yet", SMA_VID(pSma));
    }
    ++nLoops;
    if (nLoops > 1000) {
      sched_yield();
      nLoops = 0;
    }
  }

  // step 3: perform persist task for qTaskInfo
  pRSmaStat->commitAppliedVer = pSma->pVnode->state.applied;
  tdRSmaPersistExecImpl(pRSmaStat, RSMA_INFO_HASH(pRSmaStat));

  smaDebug("vgId:%d, rsma pre commit success", SMA_VID(pSma));

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief commit for rollup sma
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaSyncCommitImpl(SSma *pSma) {
#if 0
  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    return TSDB_CODE_SUCCESS;
  }
#endif
  return TSDB_CODE_SUCCESS;
}
#endif

// SQTaskFile ======================================================

/**
 * @brief At most time, there is only one qtaskinfo file committed latest in aTaskFile. Sometimes, there would be
 * multiple qtaskinfo files supporting snapshot replication.
 *
 * @param pSma
 * @param pStat
 * @return int32_t
 */
static int32_t tdUpdateQTaskInfoFiles(SSma *pSma, SRSmaStat *pStat) {
  SVnode  *pVnode = pSma->pVnode;
  SRSmaFS *pFS = RSMA_FS(pStat);
  int64_t  committed = pStat->commitAppliedVer;
  int64_t  fsMaxVer = -1;
  char     qTaskInfoFullName[TSDB_FILENAME_LEN];

  taosWLockLatch(RSMA_FS_LOCK(pStat));

  for (int32_t i = 0; i < taosArrayGetSize(pFS->aQTaskInf);) {
    SQTaskFile *pTaskF = taosArrayGet(pFS->aQTaskInf, i);
    int32_t     oldVal = atomic_fetch_sub_32(&pTaskF->nRef, 1);
    if ((oldVal <= 1) && (pTaskF->version < committed)) {
      tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pTaskF->version, tfsGetPrimaryPath(pVnode->pTfs), qTaskInfoFullName);
      if (taosRemoveFile(qTaskInfoFullName) < 0) {
        smaWarn("vgId:%d, cleanup qinf, committed %" PRIi64 ", failed to remove %s since %s", TD_VID(pVnode), committed,
                qTaskInfoFullName, tstrerror(TAOS_SYSTEM_ERROR(errno)));
      } else {
        smaDebug("vgId:%d, cleanup qinf, committed %" PRIi64 ", success to remove %s", TD_VID(pVnode), committed,
                 qTaskInfoFullName);
      }
      taosArrayRemove(pFS->aQTaskInf, i);
      continue;
    }
    ++i;
  }

  if (taosArrayGetSize(pFS->aQTaskInf) > 0) {
    fsMaxVer = ((SQTaskFile *)taosArrayGetLast(pFS->aQTaskInf))->version;
  }

  if (fsMaxVer < committed) {
    tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), committed, tfsGetPrimaryPath(pVnode->pTfs), qTaskInfoFullName);
    if (taosCheckExistFile(qTaskInfoFullName)) {
      SQTaskFile qFile = {.nRef = 1, .padding = 0, .version = committed, .size = 0};
      if (!taosArrayPush(pFS->aQTaskInf, &qFile)) {
        taosWUnLockLatch(RSMA_FS_LOCK(pStat));
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return TSDB_CODE_FAILED;
      }
    }
  } else {
    smaDebug("vgId:%d, update qinf, no need as committed %" PRIi64 " not larger than fsMaxVer %" PRIi64, TD_VID(pVnode),
             committed, fsMaxVer);
  }

  taosWUnLockLatch(RSMA_FS_LOCK(pStat));
  return TSDB_CODE_SUCCESS;
}

#if 0
/**
 * @brief post-commit for rollup sma
 *  1) clean up the outdated qtaskinfo files
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaSyncPostCommitImpl(SSma *pSma) {
  SVnode *pVnode = pSma->pVnode;
  if (!VND_IS_RSMA(pVnode)) {
    return TSDB_CODE_SUCCESS;
  }

  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(pSma);

  tdUpdateQTaskInfoFiles(pSma, pRSmaStat);

  return TSDB_CODE_SUCCESS;
}
#endif

/**
 * @brief Rsma async commit implementation(only do some necessary light weighted task)
 *  1) set rsma stat TASK_TRIGGER_STAT_PAUSED
 *  2) Wait all running fetch task finish to fetch and put submitMsg into level 2/3 wQueue(blocking level 1 write)
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaAsyncPreCommitImpl(SSma *pSma) {
  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);
  if (!pEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaStat *pRSmaStat = SMA_STAT_RSMA(pStat);
  int32_t    nLoops = 0;

  // step 1: set rsma stat
  atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_PAUSED);
  while (atomic_val_compare_exchange_8(RSMA_COMMIT_STAT(pRSmaStat), 0, 1) != 0) {
    ++nLoops;
    if (nLoops > 1000) {
      sched_yield();
      nLoops = 0;
    }
  }
  pRSmaStat->commitAppliedVer = pSma->pVnode->state.applied;
  ASSERT(pRSmaStat->commitAppliedVer > 0);

  // step 2: wait for all triggered fetch tasks to finish
  nLoops = 0;
  while (1) {
    if (T_REF_VAL_GET(pStat) == 0) {
      smaDebug("vgId:%d, rsma commit, fetch tasks are all finished", SMA_VID(pSma));
      break;
    } else {
      smaDebug("vgId:%d, rsma commit, fetch tasks are not all finished yet", SMA_VID(pSma));
    }
    ++nLoops;
    if (nLoops > 1000) {
      sched_yield();
      nLoops = 0;
    }
  }

  /**
   * @brief step 3: commit should wait for all SubmitReq in buffer be consumed
   *  1) This is high cost task and should not put in asyncPreCommit originally.
   *  2) But, if put in asyncCommit, would trigger taskInfo cloning frequently.
   */
  smaInfo("vgId:%d, rsma commit, wait for all items to be consumed, TID:%p", SMA_VID(pSma),
          (void *)taosGetSelfPthreadId());
  nLoops = 0;
  while (atomic_load_64(&pRSmaStat->nBufItems) > 0) {
    ++nLoops;
    if (nLoops > 1000) {
      sched_yield();
      nLoops = 0;
    }
  }
  smaInfo("vgId:%d, rsma commit, all items are consumed, TID:%p", SMA_VID(pSma), (void *)taosGetSelfPthreadId());
  if (tdRSmaPersistExecImpl(pRSmaStat, RSMA_INFO_HASH(pRSmaStat)) < 0) {
    return TSDB_CODE_FAILED;
  }
  smaInfo("vgId:%d, rsma commit, operator state committed, TID:%p", SMA_VID(pSma), (void *)taosGetSelfPthreadId());

#if 0  // consuming task of qTaskInfo clone 
  // step 4:  swap queue/qall and iQueue/iQall
  // lock
  taosWLockLatch(SMA_ENV_LOCK(pEnv));

  ASSERT(RSMA_INFO_HASH(pRSmaStat));

  void *pIter = taosHashIterate(RSMA_INFO_HASH(pRSmaStat), NULL);

  while (pIter) {
    SRSmaInfo *pInfo = *(SRSmaInfo **)pIter;
    TSWAP(pInfo->iQall, pInfo->qall);
    TSWAP(pInfo->iQueue, pInfo->queue);
    TSWAP(pInfo->iTaskInfo[0], pInfo->taskInfo[0]);
    TSWAP(pInfo->iTaskInfo[1], pInfo->taskInfo[1]);
    pIter = taosHashIterate(RSMA_INFO_HASH(pRSmaStat), pIter);
  }

  // unlock
  taosWUnLockLatch(SMA_ENV_LOCK(pEnv));
#endif

  // all rsma results are written completely
  STsdb *pTsdb = NULL;
  if ((pTsdb = VND_RSMA1(pSma->pVnode))) tsdbPrepareCommit(pTsdb);
  if ((pTsdb = VND_RSMA2(pSma->pVnode))) tsdbPrepareCommit(pTsdb);

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief commit for rollup sma
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaAsyncCommitImpl(SSma *pSma, SCommitInfo *pInfo) {
  int32_t code = 0;
  SVnode *pVnode = pSma->pVnode;
  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    goto _exit;
  }
#if 0
  SRSmaStat *pRSmaStat = (SRSmaStat *)SMA_ENV_STAT(pSmaEnv);

  // perform persist task for qTaskInfo operator
  if (tdRSmaPersistExecImpl(pRSmaStat, RSMA_INFO_HASH(pRSmaStat)) < 0) {
    return TSDB_CODE_FAILED;
  }
#endif

  if ((code = tsdbCommit(VND_RSMA1(pVnode), pInfo)) < 0) {
    smaError("vgId:%d, failed to commit tsdb rsma1 since %s", TD_VID(pVnode), tstrerror(code));
    goto _exit;
  }
  if ((code = tsdbCommit(VND_RSMA2(pVnode), pInfo)) < 0) {
    smaError("vgId:%d, failed to commit tsdb rsma2 since %s", TD_VID(pVnode), tstrerror(code));
    goto _exit;
  }
_exit:
  terrno = code;
  return code;
}

/**
 * @brief Migrate rsmaInfo from iRsmaInfo to rsmaInfo if rsma infoHash not empty.
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaAsyncPostCommitImpl(SSma *pSma) {
  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);
  if (!pEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SRSmaStat *pRSmaStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);

  // step 1: merge qTaskInfo and iQTaskInfo
  // lock
  if (1 == atomic_val_compare_exchange_8(&pRSmaStat->delFlag, 1, 0)) {
    taosWLockLatch(SMA_ENV_LOCK(pEnv));

    void *pIter = NULL;
    while ((pIter = taosHashIterate(RSMA_INFO_HASH(pRSmaStat), pIter))) {
      tb_uid_t  *pSuid = (tb_uid_t *)taosHashGetKey(pIter, NULL);
      SRSmaInfo *pRSmaInfo = *(SRSmaInfo **)pIter;
      if (RSMA_INFO_IS_DEL(pRSmaInfo)) {
        int32_t refVal = T_REF_VAL_GET(pRSmaInfo);
        if (refVal == 0) {
          taosHashRemove(RSMA_INFO_HASH(pRSmaStat), pSuid, sizeof(*pSuid));
        } else {
          smaDebug(
              "vgId:%d, rsma async post commit, not free rsma info since ref is %d although already deleted for "
              "table:%" PRIi64,
              SMA_VID(pSma), refVal, *pSuid);
        }

        continue;
      }
#if 0
    if (pRSmaInfo->taskInfo[0]) {
      if (pRSmaInfo->iTaskInfo[0]) {
        SRSmaInfo *pRSmaInfo = *(SRSmaInfo **)pRSmaInfo->iTaskInfo[0];
        tdFreeRSmaInfo(pSma, pRSmaInfo, false);
        pRSmaInfo->iTaskInfo[0] = NULL;
      }
    } else {
      TSWAP(pRSmaInfo->taskInfo[0], pRSmaInfo->iTaskInfo[0]);
    }

    taosHashPut(RSMA_INFO_HASH(pRSmaStat), pSuid, sizeof(tb_uid_t), pIter, sizeof(pIter));
    smaDebug("vgId:%d, rsma async post commit, migrated from iRsmaInfoHash for table:%" PRIi64, SMA_VID(pSma), *pSuid);
#endif
    }

    // unlock
    taosWUnLockLatch(SMA_ENV_LOCK(pEnv));
  }

  tdUpdateQTaskInfoFiles(pSma, pRSmaStat);

  atomic_store_8(RSMA_COMMIT_STAT(pRSmaStat), 0);

  return TSDB_CODE_SUCCESS;
}
