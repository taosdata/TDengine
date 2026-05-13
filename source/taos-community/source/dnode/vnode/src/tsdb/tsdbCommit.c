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

int32_t tRowInfoCmprFn(const void *p1, const void *p2) {
  SRowInfo *pInfo1 = (SRowInfo *)p1;
  SRowInfo *pInfo2 = (SRowInfo *)p2;

  if (pInfo1->suid < pInfo2->suid) {
    return -1;
  } else if (pInfo1->suid > pInfo2->suid) {
    return 1;
  }

  if (pInfo1->uid < pInfo2->uid) {
    return -1;
  } else if (pInfo1->uid > pInfo2->uid) {
    return 1;
  }

  return tsdbRowCompare(&pInfo1->row, &pInfo2->row);
}

int32_t tsdbBegin(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!pTsdb) return code;

  SMemTable *pMemTable;
  TAOS_CHECK_GOTO(tsdbMemTableCreate(pTsdb, &pMemTable), &lino, _exit);

  // lock
  if ((code = taosThreadMutexLock(&pTsdb->mutex))) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(code), &lino, _exit);
  }

  pTsdb->mem = pMemTable;

  // unlock
  if ((code = taosThreadMutexUnlock(&pTsdb->mutex))) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(code), &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}
