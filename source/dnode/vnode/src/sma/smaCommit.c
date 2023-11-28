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

static int32_t tdProcessRSmaAsyncPreCommitImpl(SSma *pSma, bool isCommit);
static int32_t tdProcessRSmaAsyncCommitImpl(SSma *pSma, SCommitInfo *pInfo);
static int32_t tdProcessRSmaAsyncPostCommitImpl(SSma *pSma);
static int32_t tdUpdateQTaskInfoFiles(SSma *pSma, SRSmaStat *pRSmaStat);

/**
 * @brief only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaPreClose(SSma *pSma) { return tdProcessRSmaAsyncPreCommitImpl(pSma, false); }

/**
 * @brief async commit, only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaPrepareAsyncCommit(SSma *pSma) { return tdProcessRSmaAsyncPreCommitImpl(pSma, true); }

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

extern int32_t tsdbCommitCommit(STsdb *tsdb);
int32_t        smaFinishCommit(SSma *pSma) {
  int32_t code = 0;
  int32_t lino = 0;
  SVnode *pVnode = pSma->pVnode;

  if (VND_RSMA1(pVnode) && (code = tsdbCommitCommit(VND_RSMA1(pVnode))) < 0) {
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (VND_RSMA2(pVnode) && (code = tsdbCommitCommit(VND_RSMA2(pVnode))) < 0) {
    TSDB_CHECK_CODE(code, lino, _exit);
  }
_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

/**
 * @brief Rsma async commit implementation(only do some necessary light weighted task)
 *  1) set rsma stat TASK_TRIGGER_STAT_PAUSED
 *  2) Wait all running fetch task finish to fetch and put submitMsg into level 2/3 wQueue(blocking level 1 write)
 *
 * @param pSma
 * @param isCommit
 * @return int32_t
 */
extern int32_t tsdbPreCommit(STsdb *tsdb);
static int32_t tdProcessRSmaAsyncPreCommitImpl(SSma *pSma, bool isCommit) {
  int32_t code = 0;
  int32_t lino = 0;

  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);
  if (!pEnv) {
    return code;
  }

  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaStat *pRSmaStat = SMA_STAT_RSMA(pStat);
  int32_t    nLoops = 0;

  // step 1: set rsma stat
  atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_PAUSED);
  if (isCommit) {
    while (atomic_val_compare_exchange_8(RSMA_COMMIT_STAT(pRSmaStat), 0, 1) != 0) {
      TD_SMA_LOOPS_CHECK(nLoops, 1000)
    }
  }
  // step 2: wait for all triggered fetch tasks to finish
  nLoops = 0;
  while (1) {
    if (atomic_load_32(&pRSmaStat->nFetchAll) <= 0) {
      smaDebug("vgId:%d, rsma commit, type:%d, fetch tasks are all finished", SMA_VID(pSma), isCommit);
      break;
    } else {
      smaDebug("vgId:%d, rsma commit, type:%d, fetch tasks are not all finished yet", SMA_VID(pSma), isCommit);
    }
    TD_SMA_LOOPS_CHECK(nLoops, 1000);
  }

  /**
   * @brief step 3: commit should wait for all SubmitReq in buffer be consumed
   *  1) This is high cost task and should not put in asyncPreCommit originally.
   *  2) But, if put in asyncCommit, would trigger taskInfo cloning frequently.
   */
  smaInfo("vgId:%d, rsma commit, type:%d, wait for all items to be consumed, TID:%p", SMA_VID(pSma), isCommit,
          (void *)taosGetSelfPthreadId());
  nLoops = 0;
  while (atomic_load_64(&pRSmaStat->nBufItems) > 0) {
    TD_SMA_LOOPS_CHECK(nLoops, 1000);
  }
  smaInfo("vgId:%d, rsma commit, all items are consumed, TID:%p", SMA_VID(pSma), (void *)taosGetSelfPthreadId());

  if (!isCommit) goto _exit;

  code = atomic_load_32(&pRSmaStat->execStat);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tdRSmaPersistExecImpl(pRSmaStat, RSMA_INFO_HASH(pRSmaStat));
  TSDB_CHECK_CODE(code, lino, _exit);

  smaInfo("vgId:%d, rsma commit, operator state committed, TID:%p", SMA_VID(pSma), (void *)taosGetSelfPthreadId());

  // all rsma results are written completely
  STsdb *pTsdb = NULL;
  if ((pTsdb = VND_RSMA1(pSma->pVnode))) {
    code = tsdbPreCommit(pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if ((pTsdb = VND_RSMA2(pSma->pVnode))) {
    code = tsdbPreCommit(pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s(%d)", SMA_VID(pSma), __func__, lino, tstrerror(code), isCommit);
  }
  return code;
}

/**
 * @brief commit for rollup sma
 *
 * @param pSma
 * @return int32_t
 */
extern int32_t tsdbCommitBegin(STsdb *tsdb, SCommitInfo *info);
static int32_t tdProcessRSmaAsyncCommitImpl(SSma *pSma, SCommitInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  SVnode *pVnode = pSma->pVnode;

  if (!SMA_RSMA_ENV(pSma)) goto _exit;

  code = tsdbCommitBegin(VND_RSMA1(pVnode), pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitBegin(VND_RSMA2(pVnode), pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
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
    }

    // unlock
    taosWUnLockLatch(SMA_ENV_LOCK(pEnv));
  }

  atomic_store_8(RSMA_COMMIT_STAT(pRSmaStat), 0);

  return TSDB_CODE_SUCCESS;
}
