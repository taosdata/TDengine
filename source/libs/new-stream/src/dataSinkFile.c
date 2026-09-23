/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <stdint.h>
#include <stdio.h>
#include "dataSink.h"
#include "freeBlockMgr.h"
#include "osAtomic.h"
#include "osFile.h"
#include "osMemory.h"
#include "osSysinfo.h"
#include "osTime.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"
#include "thash.h"
#include "tutil.h"

char      gDataSinkFilePath[PATH_MAX] = {0};
const int gFileGroupBlockMaxSize = 64 * 1024;  // 64K

int32_t initDataSinkFileDir() {
  int32_t code = 0;
  int     ret = snprintf(gDataSinkFilePath, sizeof(gDataSinkFilePath), "%s/tdengine_stream_data/", tsTempDir);
  if (ret < 0 || ret >= sizeof(gDataSinkFilePath)) {
    stError("failed to get stream data sink path ret:%d", ret);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  code = taosEnsurePrivateDir(gDataSinkFilePath);
  if (code != 0) {
    return code;
  }
  stInfo("create stream data sink path %s", gDataSinkFilePath);
  return TSDB_CODE_SUCCESS;
}

int32_t createStreamDataSinkFile(char* fileName, size_t fileNameSize, TdFilePtr* ppFile) {
  if (fileName == NULL || fileNameSize == 0 || ppFile == NULL || *ppFile != NULL) return TSDB_CODE_INVALID_PARA;
  fileName[0] = '\0';
  int32_t code = taosEnsurePrivateDir(gDataSinkFilePath);
  if (code != 0) return code;
  char uuid[37] = {0};
  code = taosGetSystemUUIDLimit36(uuid, sizeof(uuid));
  if (code != 0) return code;
  for (int32_t attempt = 0; attempt < 32; ++attempt) {
    char temporary[PATH_MAX] = {0};
    char candidate[PATH_MAX] = {0};
    // The helper formats its directory argument; generate the basename using a literal directory.
    taosGetTmpfilePath(".", "stream", temporary);
    int32_t length = snprintf(candidate, sizeof(candidate), "%s/%s-%s", gDataSinkFilePath, temporary + 2, uuid);
    if (length < 0 || length >= sizeof(candidate) || length >= fileNameSize) return TSDB_CODE_INVALID_PARA;
    TdFilePtr file = taosOpenPrivateFile(candidate, TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_READ | TD_FILE_WRITE);
    if (file != NULL) {
      memcpy(fileName, candidate, length + 1);
      *ppFile = file;
      return TSDB_CODE_SUCCESS;
    }
    code = terrno;
#ifdef WINDOWS
    if (code != TAOS_SYSTEM_WINAPI_ERROR(ERROR_FILE_EXISTS) && code != TAOS_SYSTEM_WINAPI_ERROR(ERROR_ALREADY_EXISTS)) {
      return code;
    }
#else
    if (code != TAOS_SYSTEM_ERROR(EEXIST)) return code;
#endif
  }
  return code;
}

static int32_t createStreamDataSinkFileMgr(SDataSinkFileMgr** ppDaSinkFileMgr) {
  SDataSinkFileMgr* pFileMgr = NULL;
  pFileMgr = (SDataSinkFileMgr*)taosMemoryCalloc(1, sizeof(SDataSinkFileMgr));
  if (pFileMgr == NULL) {
    return terrno;
  }
  int32_t code = createStreamDataSinkFile(pFileMgr->fileName, sizeof(pFileMgr->fileName), &pFileMgr->writeFilePtr);
  if (code != 0) {
    taosMemoryFree(pFileMgr);
    return code;
  }

  pFileMgr->fileBlockCount = 0;
  pFileMgr->fileBlockUsedCount = 0;
  pFileMgr->fileSize = 0;
  tRBTreeCreate(&pFileMgr->pFreeFileBlockList, compareFreeBlock);
  pFileMgr->writingGroupId = -1;
  pFileMgr->readingGroupId = -1;
  pFileMgr->readFilePtr = NULL;

  *ppDaSinkFileMgr = pFileMgr;

  return TSDB_CODE_SUCCESS;
}

void destroyStreamDataSinkFile(SDataSinkFileMgr** ppDaSinkFileMgr) {
  if (ppDaSinkFileMgr == NULL || *ppDaSinkFileMgr == NULL) {
    return;
  }
  if ((*ppDaSinkFileMgr)) {
    if ((*ppDaSinkFileMgr)->writeFilePtr) {
      if(taosCloseFile(&(*ppDaSinkFileMgr)->writeFilePtr) != 0) {
        stError("failed to close file %s, lineno:%d", (*ppDaSinkFileMgr)->fileName, __LINE__);
      }
      (*ppDaSinkFileMgr)->writeFilePtr = NULL;
    }
    if ((*ppDaSinkFileMgr)->readFilePtr) {
      if(taosCloseFile(&(*ppDaSinkFileMgr)->readFilePtr) != 0) {
        stError("failed to close file %s, lineno:%d", (*ppDaSinkFileMgr)->fileName, __LINE__);
      }
      (*ppDaSinkFileMgr)->readFilePtr = NULL;
    }
    if (strlen((*ppDaSinkFileMgr)->fileName) > 0) {
      if(taosRemoveFile((*ppDaSinkFileMgr)->fileName) != 0) {
        stError("failed to remove file %s, lineno:%d", (*ppDaSinkFileMgr)->fileName, __LINE__);
      }
      (*ppDaSinkFileMgr)->fileName[0] = '\0';
    }

    clearAllFreeBlocks(&(*ppDaSinkFileMgr)->pFreeFileBlockList);
    taosMemoryFreeClear((*ppDaSinkFileMgr));
  }
}

static int32_t initStreamDataSinkFile(SSlidingTaskDSMgr* pStreamDataSink) {
  if (pStreamDataSink->pFileMgr == NULL) {
    return createStreamDataSinkFileMgr(&pStreamDataSink->pFileMgr);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t openFileForRead(SDataSinkFileMgr* pFileMgr) {
  void* existing = atomic_load_ptr(&pFileMgr->readFilePtr);

  if (existing == NULL) {
    TdFilePtr newPtr = taosOpenPrivateFile(pFileMgr->fileName, TD_FILE_READ);
    if (newPtr == NULL) {
      stError("open file %s failed, err: %s", pFileMgr->fileName, terrMsg);
      return terrno;
    }

    int64_t writeDev = 0, writeIno = 0, readDev = 0, readIno = 0;
    int32_t code = taosDevInoFile(pFileMgr->writeFilePtr, &writeDev, &writeIno);
    if (code == 0) code = taosDevInoFile(newPtr, &readDev, &readIno);
    if (code == 0 && (writeDev != readDev || writeIno != readIno)) code = TSDB_CODE_INVALID_MSG;
    if (code != 0) {
      (void)taosCloseFile(&newPtr);
      return code;
    }

    void* oldPtr = atomic_val_compare_exchange_ptr(&pFileMgr->readFilePtr, NULL, newPtr);
    if (oldPtr != NULL) {
      TdFilePtr fileToClose = (TdFilePtr)newPtr;
      if(taosCloseFile(&fileToClose) != 0) {
        stError("failed to close file %s, lineno:%d", pFileMgr->fileName, __LINE__);
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void getFreeBlock(SDataSinkFileMgr* pFileMgr, int32_t needSize, SFileBlockInfo* pGroupBlockOffset) {
  FreeBlock* pFreeBlock = popBestFitBlock(&pFileMgr->pFreeFileBlockList, needSize);
  if (pFreeBlock != NULL) {
    pGroupBlockOffset->size = pFreeBlock->length;
    pGroupBlockOffset->offset = pFreeBlock->start;
    destroyFreeBlock(pFreeBlock);
    return;
  }
  pGroupBlockOffset->offset = pFileMgr->fileSize;
  pGroupBlockOffset->size = needSize;
  pFileMgr->fileBlockCount++;
  pFileMgr->fileBlockUsedCount++;
  pFileMgr->fileSize += needSize;
  destroyFreeBlock(pFreeBlock);
  return;
}

static int32_t addToFreeBlock(SDataSinkFileMgr* pFileMgr, const SFileBlockInfo* pBlockInfo) {
  if (pBlockInfo->size <= 0) return TSDB_CODE_SUCCESS;
  FreeBlock* pFreeBlock = createFreeBlock(pBlockInfo->offset, pBlockInfo->size);
  if (pFreeBlock == NULL) {
    stError("failed to create free block, err: %s", terrMsg);
    return terrno;
  }
  insertFreeBlock(&pFileMgr->pFreeFileBlockList, pFreeBlock);
  return TSDB_CODE_SUCCESS;
}

bool setNextIteratorFromFile(SResultIter** ppResult) {
  SResultIter* pResult = *ppResult;
  if (pResult->cleanMode == DATA_CLEAN_EXPIRED) {
    SSlidingGrpMgr* pSlidingGrpMgr = (SSlidingGrpMgr*)pResult->groupData;
    if (++pResult->offset < pSlidingGrpMgr->blocksInFile->size) {
      return false;
    } else {
      return true;
    }
  } else {
    // pointer movement is completed while reading data
    SAlignGrpMgr* pAlignGrpMgr = (SAlignGrpMgr*)pResult->groupData;
    // todo
    return pAlignGrpMgr->blocksInMem->size == 0;
  }
  return true;
}

static int32_t appendTmpSBlocksInMem(SResultIter* pResult, SSDataBlock* pBlock) {
  if (pBlock == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  if (pResult->tmpBlocksInMem == NULL) {
    pResult->tmpBlocksInMem = taosArrayInit(1, sizeof(SSDataBlock*));
    if (pResult->tmpBlocksInMem == NULL) {
      return terrno;
    }
  }
  void* p = taosArrayPush(pResult->tmpBlocksInMem, &pBlock);
  if (p == NULL) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t readFileDataToSlidingWindows(SResultIter* pResult, SSlidingGrpMgr* pSlidingGrpMgr, int32_t tsColSlotId,
                                            SBlocksInfoFile* pBlockInfo, bool* finished) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*        buf = NULL;
  SSDataBlock* pBlock = NULL;
  if (pBlockInfo->dataLen <= 0 || pBlockInfo->dataLen > DS_FILE_BLOCK_SIZE ||
      pBlockInfo->dataLen > pBlockInfo->capacity || pBlockInfo->groupOffset < 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  buf = taosMemoryMalloc(pBlockInfo->dataLen);
  if (buf == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }
  code = openFileForRead(pResult->pFileMgr);
  QUERY_CHECK_CODE(code, lino, _exit);
  int64_t readLen = taosPReadFile(pResult->pFileMgr->readFilePtr, buf, pBlockInfo->dataLen, pBlockInfo->groupOffset);
  if (readLen != pBlockInfo->dataLen) {
    code = readLen < 0 && terrno != 0 ? terrno : TSDB_CODE_STREAM_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _exit);
  }

  *finished = false;
  const char* cursor = buf;
  size_t      remaining = pBlockInfo->dataLen;
  while (remaining > 0) {
    SSlidingWindowInMem window;
    if (remaining < sizeof(window)) {
      code = TSDB_CODE_INVALID_MSG;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
    memcpy(&window, cursor, sizeof(window));
    cursor += sizeof(window);
    remaining -= sizeof(window);
    if (window.dataLen < 0 || (uint64_t)window.dataLen > remaining) {
      code = TSDB_CODE_INVALID_MSG;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
    if (window.startTime > pResult->reqEndTime) {
      *finished = true;
      break;
    }
    if (window.endTime >= pResult->reqStartTime) {
      pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      if (pBlock == NULL) {
        code = terrno;
        QUERY_CHECK_CODE(code, lino, _exit);
      }
      code = blockSpecialDecodeLaterPartBounded(pBlock, cursor, window.dataLen, tsColSlotId, pResult->reqStartTime,
                                                pResult->reqEndTime, NULL);
      QUERY_CHECK_CODE(code, lino, _exit);
      if (pBlock->info.rows == 0) {
        blockDataDestroy(pBlock);
      } else {
        code = appendTmpSBlocksInMem(pResult, pBlock);
        QUERY_CHECK_CODE(code, lino, _exit);
      }
      pBlock = NULL;
    }
    cursor += window.dataLen;
    remaining -= window.dataLen;
  }
_exit:
  blockDataDestroy(pBlock);
  taosMemoryFree(buf);
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to read data from file, err: %s, lineno:%d", tstrerror(code), lino);
    taosArrayDestroyP(pResult->tmpBlocksInMem, (FDelete)blockDataDestroy);
    pResult->tmpBlocksInMem = NULL;
  }
  return code;
}

int32_t readSlidingDataFromFile(SResultIter* pResult, SSDataBlock** ppBlock, int32_t tsColSlotId) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SDataSinkFileMgr* pFileMgr = pResult->pFileMgr;

  SSlidingGrpMgr* pSlidingGrpMgr = (SSlidingGrpMgr*)pResult->groupData;

  while (pResult->offset < taosArrayGetSize(pSlidingGrpMgr->blocksInFile)) {
    SBlocksInfoFile* pBlockInfo = (SBlocksInfoFile*)taosArrayGet(pSlidingGrpMgr->blocksInFile, pResult->offset);
    if (pBlockInfo == NULL || pBlockInfo->dataLen <= 0) {
      stError("invalid block info at offset:%" PRId64 ", pBlockInfo:%p", pResult->offset, pBlockInfo);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }

    bool finished = false;
    code = readFileDataToSlidingWindows(pResult, pSlidingGrpMgr, tsColSlotId, pBlockInfo, &finished);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to read file data to sliding windows, err: %s, lineno:%d", terrMsg, lino);
      return code;
    }
    if ((pResult->tmpBlocksInMem == NULL || pResult->tmpBlocksInMem->size == 0) && !finished) {
      SFileBlockInfo fileBlockInfo = {.offset = pBlockInfo->groupOffset, .size = pBlockInfo->capacity};
      code = addToFreeBlock(pFileMgr, &fileBlockInfo);
      if (code != TSDB_CODE_SUCCESS) {
        stError("failed to add to free block, err: %s, lineno:%d", terrMsg, lino);
        return code;
      }
      taosArrayRemove(pSlidingGrpMgr->blocksInFile, pResult->offset);
      continue;
    }
    if (finished) {
      pResult->dataPos = DATA_SINK_ALL_TMP;
    }
    if (pResult->tmpBlocksInMem != NULL && pResult->tmpBlocksInMem->size > 0) {
      pResult->dataPos = (pResult->dataPos == DATA_SINK_ALL_TMP ? DATA_SINK_ALL_TMP : DATA_SINK_PART_TMP);
      pResult->winIndex = 0;
      *ppBlock = *(SSDataBlock**)taosArrayGet(pResult->tmpBlocksInMem, pResult->winIndex);
      break;
    }
    pResult->offset++;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to read data from file, err: %s, lineno:%d", terrMsg, lino);
  }
  return code;
}

int32_t readAlignDataFromFile(SResultIter* pResult, SSDataBlock** ppBlock, int32_t tsColSlotId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  return TSDB_CODE_SUCCESS;
}

int32_t readDataFromFile(SResultIter* pResult, SSDataBlock** ppBlock, int32_t tsColSlotId) {
  if (pResult->cleanMode == DATA_CLEAN_EXPIRED) {
    return readSlidingDataFromFile(pResult, ppBlock, tsColSlotId);
  } else {
    return readAlignDataFromFile(pResult, ppBlock, tsColSlotId);
  }
}

int32_t moveSlidingGrpMemCache(SSlidingTaskDSMgr* pSlidingTaskMgr, SSlidingGrpMgr* pSlidingGrp) {
  if (pSlidingGrp->winDataInMem == NULL || pSlidingGrp->winDataInMem->size == 0) return TSDB_CODE_SUCCESS;
  int32_t           code = 0;
  int32_t           lino = 0;
  TaosIOVec* iov = NULL;
  SDataSinkFileMgr* pFileMgr = NULL;
  SFileBlockInfo    groupBlockOffset = {0};
  bool              acquiredRange = false;
  bool              createdFile = pSlidingTaskMgr->pFileMgr == NULL;
  code = initStreamDataSinkFile(pSlidingTaskMgr);
  QUERY_CHECK_CODE(code, lino, _exit);
  pFileMgr = pSlidingTaskMgr->pFileMgr;

  int32_t nWin = taosArrayGetSize(pSlidingGrp->winDataInMem);
  iov = taosMemoryCalloc(nWin, sizeof(TaosIOVec));
  if (iov == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }
  int32_t moveWinCount = 0;
  int32_t needSize = 0;
  for (int32_t i = 0; i < nWin; ++i) {
    SSlidingWindowInMem* pSlidingWin = *(SSlidingWindowInMem**)taosArrayGet(pSlidingGrp->winDataInMem, i);
    if (pSlidingWin == NULL || pSlidingWin->dataLen <= 0) {
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
    int64_t remaining = (int64_t)DS_FILE_BLOCK_SIZE - needSize;
    if (remaining < (int64_t)sizeof(SSlidingWindowInMem) ||
        pSlidingWin->dataLen > remaining - (int64_t)sizeof(SSlidingWindowInMem))
      break;
    iov[i].iov_base = pSlidingWin;
    iov[i].iov_len = pSlidingWin->dataLen + sizeof(SSlidingWindowInMem);
    needSize += iov[i].iov_len;
    ++moveWinCount;
  }
  if (moveWinCount == 0) {
    code = TSDB_CODE_STREAM_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _exit);
  }
  if (pSlidingGrp->blocksInFile == NULL) {
    pSlidingGrp->blocksInFile = taosArrayInit(1, sizeof(SBlocksInfoFile));
    if (pSlidingGrp->blocksInFile == NULL) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
  }
  code = taosArrayEnsureCap(pSlidingGrp->blocksInFile, taosArrayGetSize(pSlidingGrp->blocksInFile) + 1);
  QUERY_CHECK_CODE(code, lino, _exit);
  getFreeBlock(pFileMgr, needSize, &groupBlockOffset);
  acquiredRange = true;
  SBlocksInfoFile fileBlockInfo = {
      .groupOffset = groupBlockOffset.offset, .dataLen = needSize, .capacity = groupBlockOffset.size};
  int64_t groupOffset;  // offset in file
  int64_t dataStartOffset;
  int64_t dataLen;
  int64_t capacity;  // size in file
  stDebug("move sliding group memory cache, groupId:%" PRId64
          ", moveWinCount:%d, needSize:%d, "
          "groupOffset:%" PRId64 ", capacity:%" PRId64 ", dataLen:%" PRId64,
          pSlidingGrp->groupId, moveWinCount, needSize, fileBlockInfo.groupOffset, fileBlockInfo.capacity,
          fileBlockInfo.dataLen);

  if (false) {  // append path may skip taosLSeekFile (todo)

  } else {  // first write
    if (taosLSeekFile(pFileMgr->writeFilePtr, fileBlockInfo.groupOffset, SEEK_SET) < 0) {
      code = terrno != 0 ? terrno : TSDB_CODE_STREAM_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
    int64_t writeLen = taosWritevFile(pFileMgr->writeFilePtr, iov, moveWinCount);
    if (writeLen != needSize) {
      code = writeLen < 0 && terrno != 0 ? terrno : TSDB_CODE_STREAM_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
  }
  if (taosArrayPush(pSlidingGrp->blocksInFile, &fileBlockInfo) == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }
  // Only the published file index transfers ownership away from the memory windows.
  taosArrayRemoveBatch(pSlidingGrp->winDataInMem, 0, moveWinCount, destroySlidingWindowInMemPP);
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to move sliding group memory cache, code: %d, lineno:%d", code, lino);
    if (createdFile) {
      destroyStreamDataSinkFile(&pSlidingTaskMgr->pFileMgr);
    } else if (acquiredRange) {
      (void)addToFreeBlock(pFileMgr, &groupBlockOffset);
    }
  }
  taosMemoryFree(iov);
  return code;
}
