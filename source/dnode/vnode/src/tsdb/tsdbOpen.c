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

#include "tsdb.h"

#define TSDB_OPEN_RSMA_IMPL(v, l)                                                                \
  do {                                                                                           \
    SRetention *r = VND_RETENTIONS(v)[0];                                                        \
    if (RETENTION_VALID(r)) {                                                                    \
      return tsdbOpenImpl((v), type, &VND_RSMA##l(v), VNODE_RSMA##l##_DIR, TSDB_RETENTION_L##l); \
    }                                                                                            \
  } while (0)

#define TSDB_SET_KEEP_CFG(l)                                                                      \
  do {                                                                                            \
    SRetention *r = &pCfg->retentions[l];                                                         \
    pKeepCfg->keep2 = convertTimeFromPrecisionToUnit(r->keep, pCfg->precision, TIME_UNIT_MINUTE); \
    pKeepCfg->keep0 = pKeepCfg->keep2;                                                            \
    pKeepCfg->keep1 = pKeepCfg->keep2;                                                            \
    pKeepCfg->days = tsdbEvalDays(r, pCfg->precision);                                            \
  } while (0)

#define RETENTION_DAYS_SPLIT_RATIO 10
#define RETENTION_DAYS_SPLIT_MIN   1
#define RETENTION_DAYS_SPLIT_MAX   30

static int32_t tsdbSetKeepCfg(STsdbKeepCfg *pKeepCfg, STsdbCfg *pCfg, int8_t type);
static int32_t tsdbEvalDays(SRetention *r, int8_t precision);
static int32_t tsdbOpenImpl(SVnode *pVnode, int8_t type, STsdb **ppTsdb, const char *dir, int8_t level);

int tsdbOpen(SVnode *pVnode, int8_t type) {
  switch (type) {
    case TSDB_TYPE_TSDB:
      return tsdbOpenImpl(pVnode, type, &VND_TSDB(pVnode), VNODE_TSDB_DIR, TSDB_RETENTION_L0);
    case TSDB_TYPE_TSMA:
      ASSERT(0);
      break;
    case TSDB_TYPE_RSMA_L0:
      TSDB_OPEN_RSMA_IMPL(pVnode, 0);
      break;
    case TSDB_TYPE_RSMA_L1:
      TSDB_OPEN_RSMA_IMPL(pVnode, 1);
      break;
    case TSDB_TYPE_RSMA_L2:
      TSDB_OPEN_RSMA_IMPL(pVnode, 2);
      break;
    default:
      ASSERT(0);
      break;
  }
  return 0;
}

static int32_t tsdbEvalDays(SRetention *r, int8_t precision) {
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

static int32_t tsdbSetKeepCfg(STsdbKeepCfg *pKeepCfg, STsdbCfg *pCfg, int8_t type) {
  pKeepCfg->precision = pCfg->precision;
  switch (type) {
    case TSDB_TYPE_TSDB:
      pKeepCfg->days = pCfg->days;
      pKeepCfg->keep0 = pCfg->keep0;
      pKeepCfg->keep1 = pCfg->keep1;
      pKeepCfg->keep2 = pCfg->keep2;
      break;
    case TSDB_TYPE_TSMA:
      ASSERT(0);
      break;
    case TSDB_TYPE_RSMA_L0:
      TSDB_SET_KEEP_CFG(0);
      break;
    case TSDB_TYPE_RSMA_L1:
      TSDB_SET_KEEP_CFG(1);
      break;
    case TSDB_TYPE_RSMA_L2:
      TSDB_SET_KEEP_CFG(2);
      break;
    default:
      ASSERT(0);
      break;
  }
  return 0;
}

/**
 * @brief
 *
 * @param pVnode
 * @param type
 * @param ppTsdb
 * @param dir
 * @param level retention level
 * @return int
 */
int32_t tsdbOpenImpl(SVnode *pVnode, int8_t type, STsdb **ppTsdb, const char *dir, int8_t level) {
  STsdb *pTsdb = NULL;
  int    slen = 0;

  *ppTsdb = NULL;
  slen = strlen(tfsGetPrimaryPath(pVnode->pTfs)) + strlen(pVnode->path) + strlen(dir) + 3;

  // create handle
  pTsdb = (STsdb *)taosMemoryCalloc(1, sizeof(*pTsdb) + slen);
  if (pTsdb == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pTsdb->path = (char *)&pTsdb[1];
  sprintf(pTsdb->path, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP, dir);
  pTsdb->pVnode = pVnode;
  pTsdb->level = level;
  pTsdb->repoLocked = false;
  taosThreadMutexInit(&pTsdb->mutex, NULL);
  tsdbSetKeepCfg(REPO_KEEP_CFG(pTsdb), REPO_CFG(pTsdb), type);
  pTsdb->fs = tsdbNewFS(REPO_KEEP_CFG(pTsdb));

  // create dir (TODO: use tfsMkdir)
  taosMkDir(pTsdb->path);

  // open tsdb
  if (tsdbOpenFS(pTsdb) < 0) {
    goto _err;
  }

  tsdbDebug("vgId:%d tsdb is opened for %s", TD_VID(pVnode), pTsdb->path);

  *ppTsdb = pTsdb;
  return 0;

_err:
  taosMemoryFree(pTsdb);
  return -1;
}

int tsdbClose(STsdb *pTsdb) {
  if (pTsdb) {
    // TODO: destroy mem/imem
    tsdbCloseFS(pTsdb);
    tsdbFreeFS(pTsdb->fs);
    taosMemoryFree(pTsdb);
  }
  return 0;
}

int tsdbLockRepo(STsdb *pTsdb) {
  int code = taosThreadMutexLock(&pTsdb->mutex);
  if (code != 0) {
    tsdbError("vgId:%d failed to lock tsdb since %s", REPO_ID(pTsdb), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  pTsdb->repoLocked = true;
  return 0;
}

int tsdbUnlockRepo(STsdb *pTsdb) {
  ASSERT(IS_REPO_LOCKED(pTsdb));
  pTsdb->repoLocked = false;
  int code = taosThreadMutexUnlock(&pTsdb->mutex);
  if (code != 0) {
    tsdbError("vgId:%d failed to unlock tsdb since %s", REPO_ID(pTsdb), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}