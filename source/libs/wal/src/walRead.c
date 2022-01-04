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

#include "tfile.h"
#include "walInt.h"

SWalReadHandle *walOpenReadHandle(SWal *pWal) {
  SWalReadHandle *pRead = malloc(sizeof(SWalReadHandle));
  if (pRead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pRead->pWal = pWal;
  pRead->readIdxTfd = -1;
  pRead->readLogTfd = -1;
  pRead->curVersion = -1;
  pRead->curFileFirstVer = -1;
  pRead->capacity = 0;
  pRead->status = 0;
  pRead->pHead = malloc(sizeof(SWalHead));
  if (pRead->pHead == NULL) {
    free(pRead);
    return NULL;
  }
  return pRead;
}

void walCloseReadHandle(SWalReadHandle *pRead) {
  tfClose(pRead->readIdxTfd);
  tfClose(pRead->readLogTfd);
  tfree(pRead->pHead);
  free(pRead);
}

int32_t walRegisterRead(SWalReadHandle *pRead, int64_t ver) { return 0; }

static int32_t walReadSeekFilePos(SWalReadHandle *pRead, int64_t fileFirstVer, int64_t ver) {
  int code = 0;

  int64_t idxTfd = pRead->readIdxTfd;
  int64_t logTfd = pRead->readLogTfd;

  // seek position
  int64_t offset = (ver - fileFirstVer) * sizeof(SWalIdxEntry);
  code = tfLseek(idxTfd, offset, SEEK_SET);
  if (code < 0) {
    return -1;
  }
  SWalIdxEntry entry;
  if (tfRead(idxTfd, &entry, sizeof(SWalIdxEntry)) != sizeof(SWalIdxEntry)) {
    return -1;
  }
  // TODO:deserialize
  ASSERT(entry.ver == ver);
  code = tfLseek(logTfd, entry.offset, SEEK_SET);
  if (code < 0) {
    return -1;
  }
  return code;
}

static int32_t walReadChangeFile(SWalReadHandle *pRead, int64_t fileFirstVer) {
  char fnameStr[WAL_FILE_LEN];

  tfClose(pRead->readIdxTfd);
  tfClose(pRead->readLogTfd);

  walBuildLogName(pRead->pWal, fileFirstVer, fnameStr);
  int64_t logTfd = tfOpenRead(fnameStr);
  if (logTfd < 0) {
    return -1;
  }

  walBuildIdxName(pRead->pWal, fileFirstVer, fnameStr);
  int64_t idxTfd = tfOpenRead(fnameStr);
  if (idxTfd < 0) {
    return -1;
  }

  pRead->readLogTfd = logTfd;
  pRead->readIdxTfd = idxTfd;
  return 0;
}

static int32_t walReadSeekVer(SWalReadHandle *pRead, int64_t ver) {
  int   code;
  SWal *pWal = pRead->pWal;
  if (ver == pRead->curVersion) {
    return 0;
  }
  if (ver > pWal->vers.lastVer || ver < pWal->vers.firstVer) {
    return -1;
  }
  if (ver < pWal->vers.snapshotVer) {
  }

  SWalFileInfo tmpInfo;
  tmpInfo.firstVer = ver;
  // bsearch in fileSet
  SWalFileInfo *pRet = taosArraySearch(pWal->fileInfoSet, &tmpInfo, compareWalFileInfo, TD_LE);
  ASSERT(pRet != NULL);
  if (pRead->curFileFirstVer != pRet->firstVer) {
    code = walReadChangeFile(pRead, pRet->firstVer);
    if (code < 0) {
      // TODO: set error flag
      return -1;
    }
  }

  code = walReadSeekFilePos(pRead, pRet->firstVer, ver);
  if (code < 0) {
    return -1;
  }
  pRead->curVersion = ver;

  return 0;
}

int32_t walReadWithHandle(SWalReadHandle *pRead, int64_t ver) {
  int code;
  // TODO: check wal life
  if (pRead->curVersion != ver) {
    code = walReadSeekVer(pRead, ver);
    if (code != 0) {
      return -1;
    }
  }

  if (!tfValid(pRead->readLogTfd)) return -1;

  code = tfRead(pRead->readLogTfd, pRead->pHead, sizeof(SWalHead));
  if (code != sizeof(SWalHead)) {
    return -1;
  }
  code = walValidHeadCksum(pRead->pHead);
  if (code != 0) {
    return -1;
  }
  if (pRead->capacity < pRead->pHead->head.len) {
    void *ptr = realloc(pRead->pHead, sizeof(SWalHead) + pRead->pHead->head.len);
    if (ptr == NULL) {
      return -1;
    }
    pRead->pHead = ptr;
    pRead->capacity = pRead->pHead->head.len;
  }
  if (pRead->pHead->head.len != tfRead(pRead->readLogTfd, pRead->pHead->head.body, pRead->pHead->head.len)) {
    return -1;
  }

  ASSERT(pRead->pHead->head.version == ver);

  code = walValidBodyCksum(pRead->pHead);
  if (code != 0) {
    return -1;
  }
  pRead->curVersion++;

  return 0;
}

#if 0
int32_t walRead(SWal *pWal, SWalHead **ppHead, int64_t ver) {
  int code;
  code = walSeekVer(pWal, ver);
  if (code != 0) {
    return code;
  }
  if (*ppHead == NULL) {
    void *ptr = realloc(*ppHead, sizeof(SWalHead));
    if (ptr == NULL) {
      return -1;
    }
    *ppHead = ptr;
  }
  if (tfRead(pWal->writeLogTfd, *ppHead, sizeof(SWalHead)) != sizeof(SWalHead)) {
    return -1;
  }
  // TODO: endian compatibility processing after read
  if (walValidHeadCksum(*ppHead) != 0) {
    return -1;
  }
  void *ptr = realloc(*ppHead, sizeof(SWalHead) + (*ppHead)->head.len);
  if (ptr == NULL) {
    free(*ppHead);
    *ppHead = NULL;
    return -1;
  }
  if (tfRead(pWal->writeLogTfd, (*ppHead)->head.body, (*ppHead)->head.len) != (*ppHead)->head.len) {
    return -1;
  }
  // TODO: endian compatibility processing after read
  if (walValidBodyCksum(*ppHead) != 0) {
    return -1;
  }

  return 0;
}

int32_t walReadWithFp(SWal *pWal, FWalWrite writeFp, int64_t verStart, int32_t readNum) {
return 0;
}
#endif
