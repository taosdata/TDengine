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
#include "tsdbFS2.h"

extern int32_t tsdbOpenCompMonitor(STsdb *tsdb);
extern int32_t tsdbCloseCompMonitor(STsdb *tsdb);

void tsdbSetKeepCfg(STsdb *pTsdb, STsdbCfg *pCfg) {
  STsdbKeepCfg *pKeepCfg = &pTsdb->keepCfg;
  pKeepCfg->precision = pCfg->precision;
  pKeepCfg->days = pCfg->days;
  pKeepCfg->keep0 = pCfg->keep0;
  pKeepCfg->keep1 = pCfg->keep1;
  pKeepCfg->keep2 = pCfg->keep2;
  pKeepCfg->keepTimeOffset = pCfg->keepTimeOffset;
}

int64_t tsdbGetEarliestTs(STsdb *pTsdb) {
  STsdbKeepCfg *pCfg = &pTsdb->keepCfg;

  int64_t now = taosGetTimestamp(pCfg->precision);
  int64_t ts = now - (tsTickPerMin[pCfg->precision] * pCfg->keep2) + 1;  // needs to add one tick
  return ts;
}

int32_t tsdbOpen(SVnode *pVnode, STsdb **ppTsdb, const char *dir, STsdbKeepCfg *pKeepCfg, int8_t rollback, bool force) {
  STsdb  *pTsdb = NULL;
  int     slen = 0;
  int32_t code;
  int32_t lino;

  *ppTsdb = NULL;
  slen = strlen(pVnode->path) + strlen(dir) + 2;

  // create handle
  pTsdb = (STsdb *)taosMemoryCalloc(1, sizeof(*pTsdb) + slen);
  if (pTsdb == NULL) {
    TAOS_CHECK_RETURN(terrno);
  }

  pTsdb->path = (char *)&pTsdb[1];
  snprintf(pTsdb->path, TD_PATH_MAX, "%s%s%s", pVnode->path, TD_DIRSEP, dir);
  // taosRealPath(pTsdb->path, NULL, slen);
  pTsdb->pVnode = pVnode;
  (void)taosThreadMutexInit(&pTsdb->mutex, NULL);
  if (!pKeepCfg) {
    tsdbSetKeepCfg(pTsdb, &pVnode->config.tsdbCfg);
  } else {
    memcpy(&pTsdb->keepCfg, pKeepCfg, sizeof(STsdbKeepCfg));
  }

  // create dir
  if (pVnode->pTfs) {
    TAOS_CHECK_GOTO(tfsMkdir(pVnode->pTfs, pTsdb->path), &lino, _exit);
  } else {
    TAOS_CHECK_GOTO(taosMkDir(pTsdb->path), &lino, _exit);
  }

  // open tsdb
  TAOS_CHECK_GOTO(tsdbOpenFS(pTsdb, &pTsdb->pFS, rollback), &lino, _exit);

  if (pTsdb->pFS->fsstate == TSDB_FS_STATE_INCOMPLETE && force == false) {
    TAOS_CHECK_GOTO(TSDB_CODE_NEED_RETRY, &lino, _exit);
  }

  TAOS_CHECK_GOTO(tsdbOpenCache(pTsdb), &lino, _exit);

#ifdef TD_ENTERPRISE
  TAOS_CHECK_GOTO(tsdbOpenCompMonitor(pTsdb), &lino, _exit);
#endif

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, lino, tstrerror(code));
    tsdbCloseFS(&pTsdb->pFS);
    (void)taosThreadMutexDestroy(&pTsdb->mutex);
    taosMemoryFree(pTsdb);
  } else {
    tsdbDebug("vgId:%d, tsdb is opened at %s, days:%d, keep:%d,%d,%d, keepTimeoffset:%d", TD_VID(pVnode), pTsdb->path,
              pTsdb->keepCfg.days, pTsdb->keepCfg.keep0, pTsdb->keepCfg.keep1, pTsdb->keepCfg.keep2,
              pTsdb->keepCfg.keepTimeOffset);
    *ppTsdb = pTsdb;
  }
  return code;
}

int32_t tsdbClose(STsdb **pTsdb) {
  if (*pTsdb) {
    STsdb *pdb = *pTsdb;
    tsdbDebug("vgId:%d, tsdb is close at %s, days:%d, keep:%d,%d,%d, keepTimeOffset:%d", TD_VID(pdb->pVnode), pdb->path,
              pdb->keepCfg.days, pdb->keepCfg.keep0, pdb->keepCfg.keep1, pdb->keepCfg.keep2,
              pdb->keepCfg.keepTimeOffset);
    (void)taosThreadMutexLock(&(*pTsdb)->mutex);
    tsdbMemTableDestroy((*pTsdb)->mem, true);
    (*pTsdb)->mem = NULL;
    (void)taosThreadMutexUnlock(&(*pTsdb)->mutex);

    tsdbCloseFS(&(*pTsdb)->pFS);
    tsdbCloseCache(*pTsdb);
#ifdef TD_ENTERPRISE
    (void)tsdbCloseCompMonitor(*pTsdb);
#endif
    (void)taosThreadMutexDestroy(&(*pTsdb)->mutex);
    taosMemoryFreeClear(*pTsdb);
  }
  return 0;
}
