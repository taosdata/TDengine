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

static int32_t smaEvalDays(SVnode *pVnode, SRetention *r, int8_t level, int8_t precision, int32_t duration,
                           int32_t *days);
static int32_t smaSetKeepCfg(SVnode *pVnode, STsdbKeepCfg *pKeepCfg, STsdbCfg *pCfg, int type);
static int32_t rsmaRestore(SSma *pSma);

#define SMA_SET_KEEP_CFG(v, l)                                                                          \
  do {                                                                                                  \
    SRetention *r = &pCfg->retentions[l];                                                               \
    int64_t     keep = -1;                                                                              \
    TAOS_CHECK_EXIT(convertTimeFromPrecisionToUnit(r->keep, pCfg->precision, TIME_UNIT_MINUTE, &keep)); \
    pKeepCfg->keep2 = (int32_t)keep;                                                                    \
    pKeepCfg->keep0 = pKeepCfg->keep2;                                                                  \
    pKeepCfg->keep1 = pKeepCfg->keep2;                                                                  \
    TAOS_CHECK_EXIT(smaEvalDays(v, pCfg->retentions, l, pCfg->precision, pCfg->days, &pKeepCfg->days)); \
    pKeepCfg->keepTimeOffset = 0;                                                                       \
  } while (0)

#define SMA_OPEN_RSMA_IMPL(v, l, force)                                                                    \
  do {                                                                                                     \
    SRetention *r = (SRetention *)VND_RETENTIONS(v) + l;                                                   \
    if (!RETENTION_VALID(l, r)) {                                                                          \
      if (l == 0) {                                                                                        \
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);                                                           \
      }                                                                                                    \
      break;                                                                                               \
    }                                                                                                      \
    TAOS_CHECK_EXIT(smaSetKeepCfg(v, &keepCfg, pCfg, TSDB_TYPE_RSMA_L##l));                                \
    TAOS_CHECK_EXIT(tsdbOpen(v, &SMA_RSMA_TSDB##l(pSma), VNODE_RSMA##l##_DIR, &keepCfg, rollback, force)); \
  } while (0)

/**
 * @brief Evaluate days(duration) for rsma level 1/2/3.
 *  1) level 1: duration from "create database"
 *  2) level 2/3: duration * (freq/freqL1)
 * @param pVnode
 * @param r
 * @param level
 * @param precision
 * @param duration
 * @param days
 * @return int32_t
 */
static int32_t smaEvalDays(SVnode *pVnode, SRetention *r, int8_t level, int8_t precision, int32_t duration,
                           int32_t *days) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t freqDuration = -1;
  int64_t keepDuration = -1;
  TAOS_CHECK_EXIT(
      convertTimeFromPrecisionToUnit((r + TSDB_RETENTION_L0)->freq, precision, TIME_UNIT_MINUTE, &freqDuration));
  TAOS_CHECK_EXIT(
      convertTimeFromPrecisionToUnit((r + TSDB_RETENTION_L0)->keep, precision, TIME_UNIT_MINUTE, &keepDuration));
  *days = duration;  // min

  if (*days < freqDuration) {
    *days = freqDuration;
  }

  if (*days > keepDuration) {
    *days = keepDuration;
  }

  if (level < TSDB_RETENTION_L1 || level > TSDB_RETENTION_L2) {
    goto _exit;
  }

  TAOS_CHECK_EXIT(convertTimeFromPrecisionToUnit((r + level)->freq, precision, TIME_UNIT_MINUTE, &freqDuration));
  TAOS_CHECK_EXIT(convertTimeFromPrecisionToUnit((r + level)->keep, precision, TIME_UNIT_MINUTE, &keepDuration));

  int32_t nFreqTimes = (r + level)->freq / (60 * 1000);  // use 60s for freq of 1st level
  *days *= (nFreqTimes > 1 ? nFreqTimes : 1);

  if (*days < freqDuration) {
    *days = freqDuration;
  }

  int32_t maxKeepDuration = TMIN(keepDuration, TSDB_MAX_DURATION_PER_FILE);
  if (*days > maxKeepDuration) {
    *days = maxKeepDuration;
  }

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    smaInfo("vgId:%d, evaluated duration for level %d is %d, raw val:%d", TD_VID(pVnode), level + 1, *days, duration);
  }
  TAOS_RETURN(code);
}

int smaSetKeepCfg(SVnode *pVnode, STsdbKeepCfg *pKeepCfg, STsdbCfg *pCfg, int type) {
  int32_t code = 0;
  int32_t lino = 0;
  pKeepCfg->precision = pCfg->precision;
  switch (type) {
    case TSDB_TYPE_RSMA_L0:
      SMA_SET_KEEP_CFG(pVnode, 0);
      break;
    case TSDB_TYPE_RSMA_L1:
      SMA_SET_KEEP_CFG(pVnode, 1);
      break;
    case TSDB_TYPE_RSMA_L2:
      SMA_SET_KEEP_CFG(pVnode, 2);
      break;
    default:
      code = TSDB_CODE_APP_ERROR;
      break;
  }
_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t smaOpen(SVnode *pVnode, int8_t rollback, bool force) {
  int32_t   code = 0;
  int32_t   lino = 0;
  STsdbCfg *pCfg = &pVnode->config.tsdbCfg;

  SSma *pSma = taosMemoryCalloc(1, sizeof(SSma));
  if (!pSma) {
    TAOS_CHECK_EXIT(terrno);
  }

  pVnode->pSma = pSma;

  pSma->pVnode = pVnode;
  (void)taosThreadMutexInit(&pSma->mutex, NULL);
  pSma->locked = false;

  if (VND_IS_RSMA(pVnode)) {
    STsdbKeepCfg keepCfg = {0};
    for (int32_t i = 0; i < TSDB_RETENTION_MAX; ++i) {
      if (i == TSDB_RETENTION_L0) {
        SMA_OPEN_RSMA_IMPL(pVnode, 0, force);
      } else if (i == TSDB_RETENTION_L1) {
        SMA_OPEN_RSMA_IMPL(pVnode, 1, force);
      } else if (i == TSDB_RETENTION_L2) {
        SMA_OPEN_RSMA_IMPL(pVnode, 2, force);
      }
    }

    // restore the rsma
    TAOS_CHECK_EXIT(tdRSmaRestore(pSma, RSMA_RESTORE_REBOOT, pVnode->state.committed, rollback));
  }

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t smaClose(SSma *pSma) {
  if (pSma) {
    (void)smaPreClose(pSma);
    (void)taosThreadMutexDestroy(&pSma->mutex);
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
int32_t tdRSmaRestore(SSma *pSma, int8_t type, int64_t committedVer, int8_t rollback) {
  if (!VND_IS_RSMA(pSma->pVnode)) {
    TAOS_RETURN(TSDB_CODE_RSMA_INVALID_ENV);
  }

  return tdRSmaProcessRestoreImpl(pSma, type, committedVer, rollback);
}
