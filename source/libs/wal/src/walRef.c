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
    return NULL;
  }
  pRef->refId = tGenIdPI64();
  pRef->refVer = -1;
  pRef->refFile = -1;
  pRef->pWal = pWal;
  taosHashPut(pWal->pRefHash, &pRef->refId, sizeof(int64_t), &pRef, sizeof(void *));
  return pRef;
}

#if 1
void walCloseRef(SWal *pWal, int64_t refId) {
  SWalRef **ppRef = taosHashGet(pWal->pRefHash, &refId, sizeof(int64_t));
  if (ppRef == NULL) return;
  SWalRef *pRef = *ppRef;
  taosHashRemove(pWal->pRefHash, &refId, sizeof(int64_t));
  taosMemoryFree(pRef);
}
#endif

int32_t walRefVer(SWalRef *pRef, int64_t ver) {
  SWal *pWal = pRef->pWal;
  wDebug("vgId:%d, wal ref version %" PRId64 ", refId %" PRId64, pWal->cfg.vgId, ver, pRef->refId);
  if (pRef->refVer != ver) {
    taosThreadMutexLock(&pWal->mutex);
    if (ver < pWal->vers.firstVer || ver > pWal->vers.lastVer) {
      taosThreadMutexUnlock(&pWal->mutex);
      terrno = TSDB_CODE_WAL_INVALID_VER;
      return -1;
    }

    pRef->refVer = ver;
    // bsearch in fileSet
    SWalFileInfo tmpInfo;
    tmpInfo.firstVer = ver;
    SWalFileInfo *pRet = taosArraySearch(pWal->fileInfoSet, &tmpInfo, compareWalFileInfo, TD_LE);
    ASSERT(pRet != NULL);
    pRef->refFile = pRet->firstVer;

    taosThreadMutexUnlock(&pWal->mutex);
  }

  return 0;
}

#if 1
void walUnrefVer(SWalRef *pRef) {
  pRef->refId = -1;
  pRef->refFile = -1;
}
#endif

SWalRef *walRefCommittedVer(SWal *pWal) {
  SWalRef *pRef = walOpenRef(pWal);
  if (pRef == NULL) {
    return NULL;
  }
  taosThreadMutexLock(&pWal->mutex);

  int64_t ver = walGetCommittedVer(pWal);

  pRef->refVer = ver;
  // bsearch in fileSet
  SWalFileInfo tmpInfo;
  tmpInfo.firstVer = ver;
  SWalFileInfo *pRet = taosArraySearch(pWal->fileInfoSet, &tmpInfo, compareWalFileInfo, TD_LE);
  ASSERT(pRet != NULL);
  pRef->refFile = pRet->firstVer;

  taosThreadMutexUnlock(&pWal->mutex);
  return pRef;
}
