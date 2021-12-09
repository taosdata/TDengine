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

#ifndef _TD_WAL_INT_H_
#define _TD_WAL_INT_H_

#include "wal.h"
#include "compare.h"

#ifdef __cplusplus
extern "C" {
#endif

//meta section begin
typedef struct WalFileInfo {
  int64_t firstVer;
  int64_t lastVer;
  int64_t createTs;
  int64_t closeTs;
  int64_t fileSize;
} WalFileInfo;

static inline int32_t compareWalFileInfo(const void* pLeft, const void* pRight) {
  WalFileInfo* pInfoLeft = (WalFileInfo*)pLeft;
  WalFileInfo* pInfoRight = (WalFileInfo*)pRight;
  return compareInt64Val(&pInfoLeft->firstVer, &pInfoRight->firstVer);
}

static inline int64_t walGetLastFileSize(SWal* pWal) {
  WalFileInfo* pInfo = (WalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
  return pInfo->fileSize;
}

static inline int64_t walGetLastFileFirstVer(SWal* pWal) {
  WalFileInfo* pInfo = (WalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
  return pInfo->firstVer;
}

static inline int64_t walGetCurFileFirstVer(SWal* pWal) {
  WalFileInfo* pInfo = (WalFileInfo*)taosArrayGet(pWal->fileInfoSet, pWal->fileCursor);
  return pInfo->firstVer;
}

static inline int64_t walGetCurFileLastVer(SWal* pWal) {
  WalFileInfo* pInfo = (WalFileInfo*)taosArrayGet(pWal->fileInfoSet, pWal->fileCursor);
  return pInfo->firstVer;
}

static inline int64_t walGetCurFileOffset(SWal* pWal) {
  WalFileInfo* pInfo = (WalFileInfo*)taosArrayGet(pWal->fileInfoSet, pWal->fileCursor);
  return pInfo->fileSize;
}

static inline bool walCurFileClosed(SWal* pWal) {
  return taosArrayGetSize(pWal->fileInfoSet) != pWal->fileCursor;
}

static inline WalFileInfo* walGetCurFileInfo(SWal* pWal) {
  return (WalFileInfo*)taosArrayGet(pWal->fileInfoSet, pWal->fileCursor);
}

static inline int walBuildLogName(SWal*pWal, int64_t fileFirstVer, char* buf) {
  return sprintf(buf, "%s/%" PRId64 "." WAL_LOG_SUFFIX, pWal->path, fileFirstVer);
}

static inline int walBuildIdxName(SWal*pWal, int64_t fileFirstVer, char* buf) {
  return sprintf(buf, "%s/%" PRId64 "." WAL_INDEX_SUFFIX, pWal->path, fileFirstVer);
}

int walReadMeta(SWal* pWal);
int walWriteMeta(SWal* pWal);
int walRollFileInfo(SWal* pWal);

char* walFileInfoSerialize(SWal* pWal);
SArray* walFileInfoDeserialize(const char* bytes);
//meta section end

int64_t walGetSeq();
int walSeekVer(SWal *pWal, int64_t ver);
int walRoll(SWal *pWal);

#ifdef __cplusplus
}
#endif

#endif /*_TD_WAL_INT_H_*/
