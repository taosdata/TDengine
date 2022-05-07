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


static int tsdbSetKeepCfg(STsdbCfg *pCfg, STsdbKeepCfg *pKeepCfg, int8_t type);
static int tsdbOpenImpl(SVnode *pVnode, int8_t type, STsdb **ppTsdb, const char *dir, int8_t level);

int tsdbOpen(SVnode *pVnode, int8_t type) {
  switch (type) {
    case TSDB_TYPE_TSDB:
      return tsdbOpenImpl(pVnode, type, &VND_TSDB(pVnode), VNODE_TSDB_DIR, TSDB_RETENTION_L0);
    case TSDB_TYPE_TSMA:
      ASSERT(0);
      break;
    case TSDB_TYPE_RSMA_L0:
      return tsdbOpenImpl(pVnode, type, &VND_RSMA0(pVnode), VNODE_TSDB_DIR, TSDB_RETENTION_L0);
    case TSDB_TYPE_RSMA_L1:
      return tsdbOpenImpl(pVnode, type, &VND_RSMA1(pVnode), VNODE_RSMA1_DIR, TSDB_RETENTION_L1);
    case TSDB_TYPE_RSMA_L2:
      return tsdbOpenImpl(pVnode, type, &VND_RSMA2(pVnode), VNODE_RSMA2_DIR, TSDB_RETENTION_L2);
    default:
      ASSERT(0);
      break;
  }
  return 0;
}

static int tsdbSetKeepCfg(STsdbCfg *pCfg, STsdbKeepCfg *pKeepCfg, int8_t type) {
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
      pKeepCfg->days = pCfg->days;
      pKeepCfg->keep0 = pCfg->keep0;
      pKeepCfg->keep1 = pCfg->keep1;
      pKeepCfg->keep2 = pCfg->keep2;
      break;
    case TSDB_TYPE_RSMA_L1:
      pKeepCfg->days = pCfg->days;
      pKeepCfg->keep0 = pCfg->keep0;
      pKeepCfg->keep1 = pCfg->keep1;
      pKeepCfg->keep2 = pCfg->keep2;
      break;
    case TSDB_TYPE_RSMA_L2:
      pKeepCfg->days = pCfg->days;
      pKeepCfg->keep0 = pCfg->keep0;
      pKeepCfg->keep1 = pCfg->keep1;
      pKeepCfg->keep2 = pCfg->keep2;
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
int tsdbOpenImpl(SVnode *pVnode, int8_t type, STsdb **ppTsdb, const char *dir, int8_t level) {
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
  sprintf(pTsdb->path, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP,
          dir);
  pTsdb->pVnode = pVnode;
  pTsdb->level = level;
  pTsdb->repoLocked = false;
  taosThreadMutexInit(&pTsdb->mutex, NULL);
  tsdbSetKeepCfg(REPO_CFG(pTsdb), REPO_KEEP_CFG(pTsdb), type);
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