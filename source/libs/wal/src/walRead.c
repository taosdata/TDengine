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

#include "wal.h"
#include "tchecksum.h"

static int walValidateChecksum(SWalHead *pHead, void* body, int64_t bodyLen) {
  return taosCheckChecksum((uint8_t*)pHead, sizeof(SWalHead) - sizeof(uint32_t)*2, pHead->cksumHead) &&
    taosCheckChecksum(body, bodyLen, pHead->cksumBody);
}


int32_t walRead(SWal *pWal, SWalHead **ppHead, int64_t ver) {
  return 0;
}

int32_t walReadWithFp(SWal *pWal, FWalWrite writeFp, int64_t verStart, int32_t readNum) {
  return 0;
}

int64_t walGetFirstVer(SWal *pWal) {
  if (pWal == NULL) return 0;
  return pWal->firstVersion;
}

int64_t walGetSnaphostVer(SWal *pWal) {
  if (pWal == NULL) return 0;
  return pWal->snapshotVersion;
}

int64_t walGetLastVer(SWal *pWal) {
  if (pWal == NULL) return 0;
  return pWal->lastVersion;
}
