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

#include "walInt.h"
#include "tfile.h"
#include "tchecksum.h"

static inline int walValidHeadCksum(SWalHead* pHead) {
  return taosCheckChecksum((uint8_t*)pHead, sizeof(SWalHead) - sizeof(uint32_t)*2, pHead->cksumHead);
}

static inline int walValidBodyCksum(SWalHead* pHead) {
  return taosCheckChecksum((uint8_t*)pHead->cont, pHead->len, pHead->cksumBody);
}

static int walValidCksum(SWalHead *pHead, void* body, int64_t bodyLen) {
  return walValidHeadCksum(pHead) && walValidBodyCksum(pHead);
}

int32_t walRead(SWal *pWal, SWalHead **ppHead, int64_t ver) {
  int code;
  code = walSeekVer(pWal, ver);
  if(code != 0) {
    return code;
  }
  if(*ppHead == NULL) {
    void* ptr = realloc(*ppHead, sizeof(SWalHead));
    if(ptr == NULL) {
      return -1;
    }
    *ppHead = ptr;
  }
  if(tfRead(pWal->curLogTfd, *ppHead, sizeof(SWalHead)) != sizeof(SWalHead)) {
    return -1;
  }
  //TODO: endian compatibility processing after read
  if(walValidHeadCksum(*ppHead) != 0) {
    return -1;
  }
  void* ptr = realloc(*ppHead, sizeof(SWalHead) + (*ppHead)->len);
  if(ptr == NULL) {
    free(*ppHead);
    *ppHead = NULL;
    return -1;
  }
  if(tfRead(pWal->curLogTfd, (*ppHead)->cont, (*ppHead)->len) != (*ppHead)->len) {
    return -1;
  }
  //TODO: endian compatibility processing after read
  if(walValidBodyCksum(*ppHead) != 0) {
    return -1;
  }
  
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
