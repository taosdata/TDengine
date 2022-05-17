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

static int tsdbSetKeepCfg(STsdbKeepCfg *pKeepCfg, STsdbCfg *pCfg);


// implementation

static int tsdbSetKeepCfg(STsdbKeepCfg *pKeepCfg, STsdbCfg *pCfg) {
  pKeepCfg->precision = pCfg->precision;
  pKeepCfg->days = pCfg->days;
  pKeepCfg->keep0 = pCfg->keep0;
  pKeepCfg->keep1 = pCfg->keep1;
  pKeepCfg->keep2 = pCfg->keep2;
  return 0;
}

/**
 * @brief
 *
 * @param pVnode
 * @param ppTsdb
 * @param dir
 * @return int
 */
int tsdbOpen(SVnode *pVnode, STsdb **ppTsdb, const char *dir, STsdbKeepCfg *pKeepCfg) {
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

  ASSERT(strlen(dir) < TSDB_DATA_DIR_LEN);
  memcpy(pTsdb->dir, dir, strlen(dir));
  pTsdb->path = (char *)&pTsdb[1];
  sprintf(pTsdb->path, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP, dir);
  pTsdb->pVnode = pVnode;
  pTsdb->repoLocked = false;
  taosThreadMutexInit(&pTsdb->mutex, NULL);
  if (!pKeepCfg) {
    tsdbSetKeepCfg(&pTsdb->keepCfg, &pVnode->config.tsdbCfg);
  } else {
    memcpy(&pTsdb->keepCfg, pKeepCfg, sizeof(STsdbKeepCfg));
  }
  pTsdb->fs = tsdbNewFS(REPO_KEEP_CFG(pTsdb));

  // create dir (TODO: use tfsMkdir)
  taosMkDir(pTsdb->path);

  // open tsdb
  if (tsdbOpenFS(pTsdb) < 0) {
    goto _err;
  }

  tsdbDebug("vgId:%d tsdb is opened for %s, days:%d, keep:%d,%d,%d", TD_VID(pVnode), pTsdb->path, pTsdb->keepCfg.days,
            pTsdb->keepCfg.keep0, pTsdb->keepCfg.keep1, pTsdb->keepCfg.keep2);

  *ppTsdb = pTsdb;
  return 0;

_err:
  taosMemoryFree(pTsdb);
  return -1;
}

int tsdbClose(STsdb **pTsdb) {
  if (*pTsdb) {
    // TODO: destroy mem/imem
    taosThreadMutexDestroy(&(*pTsdb)->mutex);
    tsdbCloseFS(*pTsdb);
    tsdbFreeFS((*pTsdb)->fs);
    taosMemoryFreeClear(*pTsdb);
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