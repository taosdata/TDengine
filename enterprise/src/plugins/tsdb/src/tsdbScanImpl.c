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

#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "tsdbMain.h"
#include "tscompression.h"

#define tscanHeadF(h) &((h)->fGroup.files[TSDB_FILE_TYPE_HEAD])
#define tscanDataF(h) &((h)->fGroup.files[TSDB_FILE_TYPE_DATA])
#define tscanLastF(h) &((h)->fGroup.files[TSDB_FILE_TYPE_LAST])

static void tsdbScanReport(STsdbScanHandle *pScanHandle, const char *flag, const char *format, ...);
static void tsdbResetScanHandle(STsdbScanHandle *pScanHandle);

#define tsdbScanMsg(...) \
  { tsdbScanReport(pScanHandle, "SCAN MSG: ", __VA_ARGS__); }
#define tsdbScanWarn(...) \
  { tsdbScanReport(pScanHandle, "SCAN WARN: ", __VA_ARGS__); }
#define tsdbScanError(...) \
  { tsdbScanReport(pScanHandle, "SCAN ERROR: ", __VA_ARGS__); }

int tsdbScanFGroup(STsdbScanHandle *pScanHandle, char *rootDir, int fid) {
  if (pScanHandle == NULL) return -1;

  if (tsdbSetAndOpenScanFile(pScanHandle, rootDir, fid) < 0) return -1;

  if (tsdbScanSCompIdx(pScanHandle) < 0) return -1;

  for (int i = 0; i < pScanHandle->numOfIdx; i++) {
    if (tsdbScanSCompBlock(pScanHandle, i) < 0) return -1;
  }

  return 0;
}

STsdbScanHandle *tsdbNewScanHandle() {
  STsdbScanHandle *pScanHandle = (STsdbScanHandle *)calloc(1, sizeof(*pScanHandle));
  if (pScanHandle == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  // TODO

  return pScanHandle;

_err:
  tsdbFreeScanHandle(pScanHandle);
  return NULL;
}

void tsdbSetScanLogStream(STsdbScanHandle *pScanHandle, FILE *fLogStream) {
  if (pScanHandle != NULL) pScanHandle->tLogStream = fLogStream;
}

int tsdbSetAndOpenScanFile(STsdbScanHandle *pScanHandle, char *rootDir, int fid) {
  if (pScanHandle == NULL) return -1;

  tsdbResetScanHandle(pScanHandle);

  int  vnode = 0;  // TODO: get vnode from rootDir
  char fname[TSDB_FILENAME_LEN] = "\0";

  for (int type = TSDB_FILE_TYPE_NHEAD; type <= TSDB_FILE_TYPE_NSTAT; type++) {
    tsdbGetDataFileName(rootDir, vnode, fid, type, fname);
    if (access(fname, F_OK) == 0) {
      tsdbScanWarn("file %s exists", fname);
    }
  }

  pScanHandle->fGroup.fileId = fid;
  pScanHandle->fGroup.state = 0;

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &(pScanHandle->fGroup.files[type]);
    tsdbGetDataFileName(rootDir, vnode, fid, type, TSDB_FILE_NAME(pFile));

    if (tsdbOpenFile(pFile, O_RDONLY) < 0) {
      tsdbScanError("failed to open file %s since %s", TSDB_FILE_NAME(pFile), tstrerror(terrno));
      pScanHandle->fGroup.state = 1;
      continue;
    }

    uint32_t version = 0;

    if (tsdbLoadFileHeader(pFile, &version) < 0) {
      tsdbScanError("file %s header is broken since %s", TSDB_FILE_NAME(pFile), tstrerror(terrno));
      pScanHandle->fGroup.state = 1;
      continue;
    }

    struct stat tstat;
    fstat(pFile->fd, &tstat);

    if (pFile->info.size != tstat.st_size) {
      tsdbScanWarn("file %s saved size %" PRIu64 " is not the same as reas size %" PRId64, TSDB_FILE_NAME(pFile),
                   pFile->info.size, tstat.st_size);
      pFile->info.size = tstat.st_size;
    }

    if (type == TSDB_FILE_TYPE_HEAD && pFile->info.offset + pFile->info.len != pFile->info.size) {
      tsdbError("file %s has invalid offset %u len %u size %" PRId64, TSDB_FILE_NAME(pFile), pFile->info.offset, pFile->info.len,
                pFile->info.size);
      pScanHandle->fGroup.state = 1;
    }
  }

  if (pScanHandle->fGroup.state) return -1;

  return 0;
}

int tsdbScanSCompIdx(STsdbScanHandle *pScanHandle) {
  if (pScanHandle == NULL) return -1;

  SFile *pHeadFile = &(pScanHandle->fGroup.files[TSDB_FILE_TYPE_HEAD]);

  pScanHandle->pBuf = taosTRealloc(pScanHandle->pBuf, pHeadFile->info.len);
  if (pScanHandle->pBuf == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tsdbLoadCompIdxImpl(pHeadFile, pHeadFile->info.offset, pHeadFile->info.len, pScanHandle->pBuf) < 0) {
    tsdbScanError("SCompIdx part is broken while load, offest %u len %u reason %s", pHeadFile->info.offset,
                  pHeadFile->info.len, tstrerror(terrno));
    return -1;
  }

  if (tsdbDecodeSCompIdxImpl(pScanHandle->pBuf, pHeadFile->info.len, &(pScanHandle->pCompIdx),
                             &(pScanHandle->numOfIdx)) < 0) {
    tsdbScanError("SCompIdx part is broken while decode, offest %u len %u reason %s", pHeadFile->info.offset,
                  pHeadFile->info.len, tstrerror(terrno));
    return -1;
  }

  SCompIdx *pPrevIdx = NULL;
  for (int i = 0; i < pScanHandle->numOfIdx; i++) {
    SCompIdx *pCompIdx = pScanHandle->pCompIdx + i;
    if (pPrevIdx != NULL && pPrevIdx->tid >= pCompIdx->tid) {
      tsdbScanError("SCompIdx part is broken since tid %d at idx %d not larger than previous tid:%d", pCompIdx->tid, i,
                    pPrevIdx->tid);
      return -1;
    }

    pPrevIdx = pCompIdx;
  }

  return 0;
}

int tsdbScanSCompBlock(STsdbScanHandle *pScanHandle, int idx) {
  if (pScanHandle == NULL) return -1;

  ASSERT(idx < pScanHandle->numOfIdx);

  SFile *   pHeadFile = &(pScanHandle->fGroup.files[TSDB_FILE_TYPE_HEAD]);
  SCompIdx *pCompIdx = pScanHandle->pCompIdx + idx;

  if (pCompIdx->tid < 1) {
    tsdbScanError("SCompIdx at idx %d has invalid tid %d", idx, pCompIdx->tid);
    return -1;
  }

  if (pCompIdx->offset + pCompIdx->len > pHeadFile->info.size) {
    tsdbScanError("SCompIdx at idx %d has invalid offset %u len %u size %u", idx, pCompIdx->offset, pCompIdx->len,
                  pHeadFile->info.size);
    return -1;
  }

  if (tsdbLoadCompInfoImpl(pHeadFile, pCompIdx, &(pScanHandle->pCompInfo)) < 0) {
    tsdbScanError("SCompInfo/SCompBlock part is broken, offset %u len %u reason %s", pCompIdx->offset, pCompIdx->len,
                  tstrerror(terrno));
    return -1;
  }

  if (pScanHandle->pCompInfo->delimiter != TSDB_FILE_DELIMITER) {
    tsdbScanError("SCompInfo has invalid delimiter %d", pScanHandle->pCompInfo->delimiter);
    return -1;
  }

  if (pCompIdx->tid != pScanHandle->pCompInfo->tid || pCompIdx->uid != pScanHandle->pCompInfo->uid) {
    tsdbScanError("SCompInfo uid %" PRIu64 " tid %d is not the same as SCompIdx uid %" PRIu64 " tid %d",
                  pScanHandle->pCompInfo->uid, pScanHandle->pCompInfo->tid, pCompIdx->uid, pCompIdx->tid);
    return -1;
  }

  SCompBlock *pLastBlock = pScanHandle->pCompInfo->blocks + pCompIdx->numOfBlocks - 1;
  if (pLastBlock->numOfSubBlocks == 0) {
    tsdbScanError("SCompInfo last block is not super block, numOfBlocks %d", pCompIdx->numOfBlocks);
    return -1;
  }

  if ((pCompIdx->hasLast && !pLastBlock->last) && (!pCompIdx->hasLast && pLastBlock->last)) {
    tsdbScanError("SCompIdx last not match SCompInfo last");
    return -1;
  }

  if (pCompIdx->maxKey != pLastBlock->keyLast) {
    tsdbScanError("SCompIdx maxKey %" PRId64 "not match SCompInfo maxKey %" PRId64, pCompIdx->maxKey,
                  pLastBlock->keyLast);
    return -1;
  }

  SCompBlock *pPrevBlock = NULL;
  for (int i = 0; pCompIdx->numOfBlocks; i++) {
    SCompBlock *pCompBlock = pScanHandle->pCompInfo->blocks + i;
    if (i != pCompIdx->numOfBlocks - 1 && pCompBlock->last) {
      tsdbScanError("SCompBlock at idx %d has last set while it is not the last", i);
      return -1;
    }

    if (pCompBlock->algorithm != NO_COMPRESSION && pCompBlock->algorithm != ONE_STAGE_COMP && pCompBlock->algorithm != TWO_STAGE_COMP) {
      tsdbScanError("SCompBlock at idx %d has invalid compression %d", i, pCompBlock->algorithm);
      return -1;
    }

    if (pCompBlock->numOfRows <= 0) {
      tsdbScanError("SCompBlock at idx %d has invalid numOfRows %d", i, pCompBlock->numOfRows);
      return -1;
    }

    if (pCompBlock->len <= 0) {
      tsdbScanError("SCompBlock at idx %d has invalid len %d", i, pCompBlock->len);
      return -1;
    }

    if (pCompBlock->keyLen <= 0) {
      tsdbScanError("SCompBlock at idx %d has invalid keyLen %d", i, pCompBlock->keyLen);
      return -1;
    }

    if (pCompBlock->numOfSubBlocks < 1 || pCompBlock->numOfSubBlocks >= TSDB_MAX_SUBBLOCKS) {
      tsdbScanError("SCompBlock at idx %d has invalid numOfSubBlocks %d", i, pCompBlock->numOfSubBlocks);
      return -1;
    }

    if (pCompBlock->numOfCols < 1) {
      tsdbScanError("SCompBlock at idx %d has invalid numOfCols %d", i, pCompBlock->numOfCols);
      return -1;
    }

    if (pCompBlock->keyFirst > pCompBlock->keyLast) {
      tsdbScanError("SCompBlock at idx %d has invalid keyFirst %" PRId64 " and keyLast %" PRId64, i, pCompBlock->keyFirst, pCompBlock->keyLast);
      return -1;
    }

    if (pPrevBlock != NULL && pPrevBlock->keyLast >= pCompBlock->keyFirst) {
      tsdbScanError("SCompBlock at idx %d has keyFirst %" PRId64 " not larger then previouse block keyLast %" PRId64, i,
                    pCompBlock->keyFirst, pPrevBlock->keyLast);
      return -1;
    }

    pPrevBlock = pCompBlock;
  }

  return 0;
}

int tsdbCloseScanFile(STsdbScanHandle * pScanHandle) {
  if (pScanHandle == NULL) return -1;

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &(pScanHandle->fGroup.files[type]);
    tsdbCloseFile(pFile);
  }

  return 0;
}

void tsdbFreeScanHandle(STsdbScanHandle *pScanHandle) {
  if (pScanHandle) {
    taosTZfree(pScanHandle->pCompIdx);
    taosTZfree(pScanHandle->pCompInfo);
    taosTZfree(pScanHandle->pBuf);
  }
}

static void tsdbResetScanHandle(STsdbScanHandle *pScanHandle) {
  if (pScanHandle == NULL) return;

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &pScanHandle->fGroup.files[type];
    if (pFile->fd >= 0) close(pFile->fd);
    memset(pFile, 0, sizeof(SFile));
    pFile->fd = -1;
  }
  pScanHandle->numOfIdx = 0;
}

static void tsdbScanReport(STsdbScanHandle *pScanHandle, const char *flag, const char *format, ...) {
  if (pScanHandle == NULL || pScanHandle->tLogStream == NULL) return;

  const int bufSize = 1024;
  char      buffer[bufSize];
  char *    pBuf = buffer;

  pBuf += sprintf(pBuf, "%s", flag);

  va_list argpointer;
  va_start(argpointer, format);
  pBuf += vsnprintf(pBuf, bufSize - 1 - POINTER_DISTANCE(pBuf, buffer), format, argpointer);
  va_end(argpointer);

  fprintf(pScanHandle->tLogStream, "%s\n", buffer);
}