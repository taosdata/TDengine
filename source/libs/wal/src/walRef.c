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

#include "cJSON.h"
#include "os.h"
#include "taoserror.h"
#include "tutil.h"
#include "walInt.h"

SWalRef *walOpenRef(SWal *pWal) {
  SWalRef *pRef = taosMemoryCalloc(1, sizeof(SWalRef));
  if (pRef == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pRef->refId = tGenIdPI64();

  if (taosHashPut(pWal->pRefHash, &pRef->refId, sizeof(int64_t), &pRef, sizeof(void *))) {
    taosMemoryFree(pRef);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pRef->refVer = -1;
  pRef->pWal = pWal;

  return pRef;
}

void walCloseRef(SWal *pWal, int64_t refId) {
  SWalRef **ppRef = taosHashGet(pWal->pRefHash, &refId, sizeof(int64_t));
  if (ppRef) {
    SWalRef *pRef = *ppRef;

    if (pRef) {
      wDebug("vgId:%d, wal close ref %" PRId64 ", refId %" PRId64, pWal->cfg.vgId, pRef->refVer, pRef->refId);

      taosMemoryFree(pRef);
    } else {
      wDebug("vgId:%d, wal close ref null, refId %" PRId64, pWal->cfg.vgId, refId);
    }

    (void)taosHashRemove(pWal->pRefHash, &refId, sizeof(int64_t));
  }
}

int32_t walSetRefVer(SWalRef *pRef, int64_t ver) {
  SWal *pWal = pRef->pWal;
  wDebug("vgId:%d, wal ref version %" PRId64 ", refId %" PRId64, pWal->cfg.vgId, ver, pRef->refId);
  if (pRef->refVer != ver) {
    TAOS_UNUSED(walThreadRwlockWrlock(&pWal->mutex));
    if (ver < pWal->vers.firstVer || ver > pWal->vers.lastVer) {
      TAOS_UNUSED(walThreadRwlockUnlock(&pWal->mutex));

      TAOS_RETURN(TSDB_CODE_WAL_INVALID_VER);
    }

    pRef->refVer = ver;
    TAOS_UNUSED(walThreadRwlockUnlock(&pWal->mutex));
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

void walRefFirstVer(SWal *pWal, SWalRef *pRef) {
  TAOS_UNUSED(walThreadRwlockRdlock(&pWal->mutex));

  pRef->refVer = pWal->vers.firstVer;

  TAOS_UNUSED(walThreadRwlockUnlock(&pWal->mutex));
  wDebug("vgId:%d, wal ref version %" PRId64 " for first", pWal->cfg.vgId, pRef->refVer);
}

void walRefLastVer(SWal *pWal, SWalRef *pRef) {
  TAOS_UNUSED(walThreadRwlockRdlock(&pWal->mutex));

  pRef->refVer = pWal->vers.lastVer;

  TAOS_UNUSED(walThreadRwlockUnlock(&pWal->mutex));
  wDebug("vgId:%d, wal ref version %" PRId64 " for last", pWal->cfg.vgId, pRef->refVer);
}
