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

static int32_t tdProcessRSmaPreCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaCommitImpl(SSma *pSma);
static int32_t tdProcessRSmaPostCommitImpl(SSma *pSma);

/**
 * @brief Only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaPreCommit(SSma *pSma) { return tdProcessRSmaPreCommitImpl(pSma); }

/**
 * @brief Only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaCommit(SSma *pSma) { return tdProcessRSmaCommitImpl(pSma); }

/**
 * @brief Only applicable to Rollup SMA
 *
 * @param pSma
 * @return int32_t
 */
int32_t smaPostCommit(SSma *pSma) { return tdProcessRSmaPostCommitImpl(pSma); }

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
      smaDebug("vgId:%d rsma trigger stat from paused to active", SMA_VID(pSma));
      break;
    }
    case TASK_TRIGGER_STAT_INIT: {
      atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_ACTIVE);
      smaDebug("vgId:%d rsma trigger stat from init to active", SMA_VID(pSma));
      break;
    }
    default: {
      atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_ACTIVE);
      smaWarn("vgId:%d rsma trigger stat %" PRIi8 " is unexpected", SMA_VID(pSma), rsmaTriggerStat);
      ASSERT(0);
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief pre-commit for rollup sma.
 *  1) set trigger stat of rsma timer TASK_TRIGGER_STAT_PAUSED.
 *  2) perform persist task for qTaskInfo
 *  3) wait all triggered fetch tasks finished
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaPreCommitImpl(SSma *pSma) {
  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SSmaStat  *pStat = SMA_ENV_STAT(pSmaEnv);
  SRSmaStat *pRSmaStat = SMA_RSMA_STAT(pStat);

  smaDebug("vgId:%d, rsma pre commit", SMA_VID(pSma));

  // step 1: set persistence task paused
  atomic_store_8(RSMA_TRIGGER_STAT(pRSmaStat), TASK_TRIGGER_STAT_PAUSED);

  // step 2: perform persist task for qTaskInfo
  tdRSmaPersistExecImpl(pRSmaStat);

  // step 3: wait all triggered fetch tasks finished
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

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief commit for rollup sma
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaCommitImpl(SSma *pSma) {
  SSmaEnv *pSmaEnv = SMA_RSMA_ENV(pSma);
  if (!pSmaEnv) {
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief post-commit for rollup sma
 *  1) clean up the outdated qtaskinfo files
 *
 * @param pSma
 * @return int32_t
 */
static int32_t tdProcessRSmaPostCommitImpl(SSma *pSma) {
  SVnode *pVnode = pSma->pVnode;

  if (!VND_IS_RSMA(pVnode)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t       committed = pVnode->state.committed;
  TdDirPtr      pDir = NULL;
  TdDirEntryPtr pDirEntry = NULL;
  char          dir[TSDB_FILENAME_LEN];
  const char   *pattern = "^v[0-9]+qtaskinfo\\.ver([0-9]+)?$";
  regex_t       regex;

  tdGetVndDirName(TD_VID(pVnode), tfsGetPrimaryPath(pVnode->pTfs), VNODE_RSMA_DIR, dir);

  // Resource allocation and init
  regcomp(&regex, pattern, REG_EXTENDED);

  if ((pDir = taosOpenDir(dir)) == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    smaWarn("vgId:%d, rsma post commit, open dir %s failed since %s", TD_VID(pVnode), dir, terrstr());
    return TSDB_CODE_FAILED;
  }

  regmatch_t regMatch[2];
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char *entryName = taosGetDirEntryName(pDirEntry);
    if (!entryName) {
      continue;
    }
    char *fileName = taosDirEntryBaseName(entryName);
    int   code = regexec(&regex, fileName, 2, regMatch, 0);

    if (code == 0) {
      // match
      smaDebug("vgId:%d, matched = %s, %s", TD_VID(pVnode), (char *)POINTER_SHIFT(fileName, regMatch[0].rm_so),
               (const char *)POINTER_SHIFT(fileName, regMatch[1].rm_so));
      int64_t version = -1;
      sscanf((const char *)POINTER_SHIFT(fileName, regMatch[1].rm_so), "%" PRIi64, &version);
      if ((version < committed) && (version > -1)) {
        if (taosRemoveFile(entryName) != 0) {
          terrno = TAOS_SYSTEM_ERROR(errno);
          smaWarn("vgId:%d, committed version:%" PRIi64 ", failed to remove %s since %s", TD_VID(pVnode), committed,
                  entryName, terrstr());
        } else {
          smaDebug("vgId:%d, committed version:%" PRIi64 ", success to remove %s", TD_VID(pVnode), committed,
                   entryName);
        }
      }
    } else if (code == REG_NOMATCH) {
      // not match
      smaInfo("vgId:%d, rsma post commit, not match %s", TD_VID(pVnode), fileName);
      continue;
    } else {
      // has other error
      terrno = TAOS_SYSTEM_ERROR(code);
      smaWarn("vgId:%d, rsma post commit, regexec failed since %s", TD_VID(pVnode), terrstr());

      taosCloseDir(&pDir);
      regfree(&regex);
      return TSDB_CODE_FAILED;
    }
  }
  taosCloseDir(&pDir);
  return TSDB_CODE_SUCCESS;
}
