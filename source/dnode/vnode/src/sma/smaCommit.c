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

static int32_t tdProcessRSmaSyncPreCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaSyncCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaSyncPostCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaAsyncPreCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaAsyncCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaAsyncPostCommitImpl(SSma *pSma);
static int32_t tdCleanupQTaskInfoFiles(SSma *pSma, SRSmaStat *pRSmaStat);

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

/**
 * @brief Only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaAsyncPreCommit(SSma *pSma) { return tdProcessRSmaAsyncPreCommitImpl(pSma); }

/**
 * @brief Only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaAsyncCommit(SSma *pSma) { return tdProcessRSmaAsyncCommitImpl(pSma); }

/**
 * @brief Only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaAsyncPostCommit(SSma *pSma) { return tdProcessRSmaAsyncPostCommitImpl(pSma); }

/**
 * @brief set rsma trigger stat active
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaBegin(SSma *pSma) {
  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SSmaStat  *pStat = SMA_ENV_STAT(pSmaEnv);
  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(pStat);

  int8_t rsmaTriggerStat =
      atomic_val_compare_exchange_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_PAUSED, TASK_TRIGGER_STAT_ACTIVE);
  switch (rsmaTriggerStat) {
    case TASK_TRIGGER_STAT_PAUSED: {
      smaDebug("vgId:%d, rsma trigger stat from paused to active", SMA_VID(pSma));
      break;
    }
    case TASK_TRIGGER_STAT_INIT: {
      atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_ACTIVE);
      smaDebug("vgId:%d, rsma trigger stat from init to active", SMA_VID(pSma));
      break;
    }
    default: {
      atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_ACTIVE);
      smaError("vgId:%d, rsma trigger stat %" PRIi8 " is unexpected", SMA_VID(pSma), rsmaTriggerStat);
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief pre-commit for rollup sma(sync commit).
 *  1) set trigger stat of rsma timer TASK_TRIGGER_STAT_PAUSED.
 *  2) wait all triggered fetch tasks finished
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
  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(pStat);

  // step 1: set rsma stat paused
  atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_PAUSED);

  // step 2: wait all triggered fetch tasks finished
  int32_t nLoops = 0;
  while (1) {
    if (T_REF_VAL_GET(pStat) == 0) {
      smaDebug("vgId:%d, rsma fetch tasks all finished", SMA_VID(pSma));
      break;
    } else {
      smaDebug("vgId:%d, rsma fetch tasks not all finished yet", SMA_VID(pSma));
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
  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tdCleanupQTaskInfoFiles(SSma *pSma, SRSmaStat *pRSmaStat) {
  SVnode       *pVnode = pSma->pVnode;
  int64_t       committed = pRSmaStat->commitAppliedVer;
  TdDirPtr      pDir = NULL;
  TdDirEntryPtr pDirEntry = NULL;
  char          dir[TSDB_FILENAME_LEN];
  const char   *pattern = "v[0-9]+qtaskinfo\\.ver([0-9]+)?$";
  regex_t       regex;
  int           code = 0;

  tdGetVndDirName(TD_VID(pVnode), tfsGetPrimaryPath(pVnode->pTfs), VNODE_RSMA_DIR, true, dir);

  // Resource allocation and init
  if ((code = regcomp(&regex, pattern, REG_EXTENDED)) != 0) {
    char errbuf[128];
    regerror(code, &regex, errbuf, sizeof(errbuf));
    smaWarn("vgId:%d, rsma post commit, regcomp for %s failed since %s", TD_VID(pVnode), dir, errbuf);
    return TSDB_CODE_FAILED;
  }

  if ((pDir = taosOpenDir(dir)) == NULL) {
    regfree(&regex);
    terrno = TAOS_SYSTEM_ERROR(errno);
    smaDebug("vgId:%d, rsma post commit, open dir %s failed since %s", TD_VID(pVnode), dir, terrstr());
    return TSDB_CODE_FAILED;
  }

  int32_t    dirLen = strlen(dir);
  char      *dirEnd = POINTER_SHIFT(dir, dirLen);
  regmatch_t regMatch[2];
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char *entryName = taosGetDirEntryName(pDirEntry);
    if (!entryName) {
      continue;
    }

    code = regexec(&regex, entryName, 2, regMatch, 0);

    if (code == 0) {
      // match
      int64_t version = -1;
      sscanf((const char *)POINTER_SHIFT(entryName, regMatch[1].rm_so), "%" PRIi64, &version);
      if ((version < committed) && (version > -1)) {
        strncpy(dirEnd, entryName, TSDB_FILENAME_LEN - dirLen);
        if (taosRemoveFile(dir) != 0) {
          terrno = TAOS_SYSTEM_ERROR(errno);
          smaWarn("vgId:%d, committed version:%" PRIi64 ", failed to remove %s since %s", TD_VID(pVnode), committed,
                  dir, terrstr());
        } else {
          smaDebug("vgId:%d, committed version:%" PRIi64 ", success to remove %s", TD_VID(pVnode), committed, dir);
        }
      }
    } else if (code == REG_NOMATCH) {
      // not match
      smaTrace("vgId:%d, rsma post commit, not match %s", TD_VID(pVnode), entryName);
      continue;
    } else {
      // has other error
      char errbuf[128];
      regerror(code, &regex, errbuf, sizeof(errbuf));
      smaWarn("vgId:%d, rsma post commit, regexec failed since %s", TD_VID(pVnode), errbuf);

      taosCloseDir(&pDir);
      regfree(&regex);
      return TSDB_CODE_FAILED;
    }
  }

  taosCloseDir(&pDir);
  regfree(&regex);

  return TSDB_CODE_SUCCESS;
}

// SQTaskFile ======================================================
// int32_t tCmprQTaskFile(void const *lhs, void const *rhs) {
//   int64_t    *lCommitted = *(int64_t *)lhs;
//   SQTaskFile *rQTaskF = (SQTaskFile *)rhs;

//   if (lCommitted < rQTaskF->commitID) {
//     return -1;
//   } else if (lCommitted > rQTaskF->commitID) {
//     return 1;
//   }

//   return 0;
// }

#if 0
/**
 * @brief At most time, there is only one qtaskinfo file committed latest in aTaskFile. Sometimes, there would be
 * multiple qtaskinfo files supporting snapshot replication.
 *
 * @param pSma
 * @param pRSmaStat
 * @return int32_t
 */
static int32_t tdCleanupQTaskInfoFiles(SSma *pSma, SRSmaStat *pRSmaStat) {
  SVnode *pVnode = pSma->pVnode;
  int64_t committed = pRSmaStat->commitAppliedVer;
  SArray *aTaskFile = pRSmaStat->aTaskFile;

  void *qTaskFile = taosArraySearch(aTaskFile, committed, tCmprQTaskFile, TD_LE);
  

  return TSDB_CODE_SUCCESS;
}
#endif

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

  SSmaEnv   *pSmaEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(SMA_ENV_STAT(pSmaEnv));

  // cleanup outdated qtaskinfo files
  tdCleanupQTaskInfoFiles(pSma, pRSmaStat);

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Rsma async commit implementation
 *  1) set rsma stat TASK_TRIGGER_STAT_PAUSED
 *  2) Wait all running fetch task finish to fetch and put submitMsg into level 2/3 wQueue(blocking level 1 write)
 *  3)
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
  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(pStat);

  // step 1: set rsma stat
  atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_PAUSED);
  atomic_store_8(RSMA_COMMIT_STAT(pRSmaStat), 1);

  // step 2: wait all triggered fetch tasks finished
  int32_t nLoops = 0;
  while (1) {
    if (T_REF_VAL_GET(pStat) == 0) {
      smaDebug("vgId:%d, rsma fetch tasks all finished", SMA_VID(pSma));
      break;
    } else {
      smaDebug("vgId:%d, rsma fetch tasks not all finished yet", SMA_VID(pSma));
    }
    ++nLoops;
    if (nLoops > 1000) {
      sched_yield();
      nLoops = 0;
    }
  }

  // step 3:  swap rsmaInfoHash and iRsmaInfoHash
  // lock
  taosWLockLatch(SMA_ENV_LOCK(pEnv));

  ASSERT(RSMA_INFO_HASH(pRSmaStat));
  ASSERT(!RSMA_IMU_INFO_HASH(pRSmaStat));

  RSMA_IMU_INFO_HASH(pRSmaStat) = RSMA_INFO_HASH(pRSmaStat);
  RSMA_INFO_HASH(pRSmaStat) =
      taosHashInit(RSMA_TASK_INFO_HASH_SLOT, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);

  if (!RSMA_INFO_HASH(pRSmaStat)) {
    // unlock
    taosWUnLockLatch(SMA_ENV_LOCK(pEnv));
    smaError("vgId:%d, rsma async commit failed since %s", SMA_VID(pSma), terrstr());
    return TSDB_CODE_FAILED;
  }

  // unlock
  taosWUnLockLatch(SMA_ENV_LOCK(pEnv));

  // step 4: others
  pRSmaStat->commitAppliedVer = pSma->pVnode->state.applied;

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief commit for rollup sma
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaAsyncCommitImpl(SSma *pSma) {
  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SSmaStat  *pStat = SMA_ENV_STAT(pSmaEnv);
  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(pStat);

  // perform persist task for qTaskInfo
  tdRSmaPersistExecImpl(pRSmaStat, RSMA_IMU_INFO_HASH(pRSmaStat));

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Migrate rsmaInfo from iRsmaInfo to rsmaInfo if rsmaInfoHash not empty.
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaAsyncPostCommitImpl(SSma *pSma) {
  SSmaEnv *pEnv = SMA_RSMA_ENV(pSma);
  if (!pEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(pStat);

  // step 1: merge rsmaInfoHash and iRsmaInfoHash
  // lock
  taosWLockLatch(SMA_ENV_LOCK(pEnv));
#if 0
  if (taosHashGetSize(RSMA_INFO_HASH(pRSmaStat)) <= 0) {
    // just switch the hash pointer if rsmaInfoHash is empty
    if (taosHashGetSize(RSMA_IMU_INFO_HASH(pRSmaStat)) > 0) {
      SHashObj *infoHash = RSMA_INFO_HASH(pRSmaStat);
      RSMA_INFO_HASH(pRSmaStat) = RSMA_IMU_INFO_HASH(pRSmaStat);
      RSMA_IMU_INFO_HASH(pRSmaStat) = infoHash;
    }
  } else {
#endif
#if 1
  void *pIter = taosHashIterate(RSMA_IMU_INFO_HASH(pRSmaStat), NULL);
  while (pIter) {
    tb_uid_t *pSuid = (tb_uid_t *)taosHashGetKey(pIter, NULL);

    if (!taosHashGet(RSMA_INFO_HASH(pRSmaStat), pSuid, sizeof(tb_uid_t))) {
      SRSmaInfo *pRSmaInfo = *(SRSmaInfo **)pIter;
      if (RSMA_INFO_IS_DEL(pRSmaInfo)) {
        int32_t refVal = T_REF_VAL_GET(pRSmaInfo);
        if (refVal == 0) {
          tdFreeRSmaInfo(pSma, pRSmaInfo, true);
          smaDebug(
              "vgId:%d, rsma async post commit, free rsma info since already deleted and ref is 0 for "
              "table:%" PRIi64,
              SMA_VID(pSma), *pSuid);
        } else {
          smaDebug(
              "vgId:%d, rsma async post commit, not free rsma info since ref is %d although already deleted for "
              "table:%" PRIi64,
              SMA_VID(pSma), refVal, *pSuid);
        }

        pIter = taosHashIterate(RSMA_IMU_INFO_HASH(pRSmaStat), pIter);
        continue;
      }
      taosHashPut(RSMA_INFO_HASH(pRSmaStat), pSuid, sizeof(tb_uid_t), pIter, sizeof(pIter));
      smaDebug("vgId:%d, rsma async post commit, migrated from iRsmaInfoHash for table:%" PRIi64, SMA_VID(pSma),
               *pSuid);
    } else {
      // free the resources
      SRSmaInfo *pRSmaInfo = *(SRSmaInfo **)pIter;
      tdFreeRSmaInfo(pSma, pRSmaInfo, false);
      smaDebug("vgId:%d, rsma async post commit, free rsma info since already COW for table:%" PRIi64, SMA_VID(pSma),
               *pSuid);
    }

    pIter = taosHashIterate(RSMA_IMU_INFO_HASH(pRSmaStat), pIter);
  }
#endif
  // }

  taosHashCleanup(RSMA_IMU_INFO_HASH(pRSmaStat));
  RSMA_IMU_INFO_HASH(pRSmaStat) = NULL;

  // unlock
  taosWUnLockLatch(SMA_ENV_LOCK(pEnv));

  // step 2: cleanup outdated qtaskinfo files
  tdCleanupQTaskInfoFiles(pSma, pRSmaStat);

  atomic_store_8(RSMA_COMMIT_STAT(pRSmaStat), 0);

  return TSDB_CODE_SUCCESS;
}
