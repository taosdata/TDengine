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
#include "tsdb.h"

static int32_t smaEvalDays(SRetention *r, int8_t precision);
static int32_t smaSetKeepCfg(STsdbKeepCfg *pKeepCfg, STsdbCfg *pCfg, int type);
static int32_t rsmaRestore(SSma *pSma);

#define SMA_SET_KEEP_CFG(l)                                                                       \
  do {                                                                                            \
    SRetention *r = &pCfg->retentions[l];                                                         \
    pKeepCfg->keep2 = convertTimeFromPrecisionToUnit(r->keep, pCfg->precision, TIME_UNIT_MINUTE); \
    pKeepCfg->keep0 = pKeepCfg->keep2;                                                            \
    pKeepCfg->keep1 = pKeepCfg->keep2;                                                            \
    pKeepCfg->days = smaEvalDays(r, pCfg->precision);                                             \
  } while (0)

#define SMA_OPEN_RSMA_IMPL(v, l)                                                   \
  do {                                                                             \
    SRetention *r = (SRetention *)VND_RETENTIONS(v) + l;                           \
    if (!RETENTION_VALID(r)) {                                                     \
      if (l == 0) {                                                                \
        goto _err;                                                                 \
      }                                                                            \
      break;                                                                       \
    }                                                                              \
    smaSetKeepCfg(&keepCfg, pCfg, TSDB_TYPE_RSMA_L##l);                            \
    if (tsdbOpen(v, &SMA_RSMA_TSDB##l(pSma), VNODE_RSMA##l##_DIR, &keepCfg) < 0) { \
      goto _err;                                                                   \
    }                                                                              \
  } while (0)

#define RETENTION_DAYS_SPLIT_RATIO 10
#define RETENTION_DAYS_SPLIT_MIN   1
#define RETENTION_DAYS_SPLIT_MAX   30

static int32_t smaEvalDays(SRetention *r, int8_t precision) {
  int32_t keepDays = convertTimeFromPrecisionToUnit(r->keep, precision, TIME_UNIT_DAY);
  int32_t freqDays = convertTimeFromPrecisionToUnit(r->freq, precision, TIME_UNIT_DAY);

  int32_t days = keepDays / RETENTION_DAYS_SPLIT_RATIO;
  if (days <= RETENTION_DAYS_SPLIT_MIN) {
    days = RETENTION_DAYS_SPLIT_MIN;
    if (days < freqDays) {
      days = freqDays + 1;
    }
  } else {
    if (days > RETENTION_DAYS_SPLIT_MAX) {
      days = RETENTION_DAYS_SPLIT_MAX;
    }
    if (days < freqDays) {
      days = freqDays + 1;
    }
  }
  return days * 1440;
}

int smaSetKeepCfg(STsdbKeepCfg *pKeepCfg, STsdbCfg *pCfg, int type) {
  pKeepCfg->precision = pCfg->precision;
  switch (type) {
    case TSDB_TYPE_TSMA:
      ASSERT(0);
      break;
    case TSDB_TYPE_RSMA_L0:
      SMA_SET_KEEP_CFG(0);
      break;
    case TSDB_TYPE_RSMA_L1:
      SMA_SET_KEEP_CFG(1);
      break;
    case TSDB_TYPE_RSMA_L2:
      SMA_SET_KEEP_CFG(2);
      break;
    default:
      ASSERT(0);
      break;
  }
  return 0;
}

int32_t smaOpen(SVnode *pVnode) {
  STsdbCfg *pCfg = &pVnode->config.tsdbCfg;

  ASSERT(!pVnode->pSma);

  SSma *pSma = taosMemoryCalloc(1, sizeof(SSma));
  if (!pSma) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pVnode->pSma = pSma;

  pSma->pVnode = pVnode;
  taosThreadMutexInit(&pSma->mutex, NULL);
  pSma->locked = false;

  if (VND_IS_RSMA(pVnode)) {
    STsdbKeepCfg keepCfg = {0};
    for (int i = 0; i < TSDB_RETENTION_MAX; ++i) {
      if (i == TSDB_RETENTION_L0) {
        SMA_OPEN_RSMA_IMPL(pVnode, 0);
      } else if (i == TSDB_RETENTION_L1) {
        SMA_OPEN_RSMA_IMPL(pVnode, 1);
      } else if (i == TSDB_RETENTION_L2) {
        SMA_OPEN_RSMA_IMPL(pVnode, 2);
      } else {
        ASSERT(0);
      }
    }

    // restore the rsma
    if (tdRsmaRestore(pSma, RSMA_RESTORE_REBOOT, pVnode->state.committed) < 0) {
      goto _err;
    }
  }

  return 0;
_err:
  return -1;
}

int32_t smaClose(SSma *pSma) {
  if (pSma) {
    taosThreadMutexDestroy(&pSma->mutex);
    SMA_TSMA_ENV(pSma) = tdFreeSmaEnv(SMA_TSMA_ENV(pSma));
    SMA_RSMA_ENV(pSma) = tdFreeSmaEnv(SMA_RSMA_ENV(pSma));
    if SMA_RSMA_TSDB0 (pSma) tsdbClose(&SMA_RSMA_TSDB0(pSma));
    if SMA_RSMA_TSDB1 (pSma) tsdbClose(&SMA_RSMA_TSDB1(pSma));
    if SMA_RSMA_TSDB2 (pSma) tsdbClose(&SMA_RSMA_TSDB2(pSma));
    taosMemoryFreeClear(pSma);
  }
  return 0;
}

/**
 * @brief rsma env restore
 * 
 * @param pSma 
 * @param type 
 * @param committedVer 
 * @return int32_t 
 */
int32_t tdRsmaRestore(SSma *pSma, int8_t type, int64_t committedVer) {
  ASSERT(VND_IS_RSMA(pSma->pVnode));

  return tdProcessRSmaRestoreImpl(pSma, type, committedVer);
}