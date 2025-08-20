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

#include <stdint.h>
#include <stdio.h>
#include "dataSink.h"
#include "freeBlockMgr.h"
#include "osAtomic.h"
#include "osFile.h"
#include "osMemory.h"
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
  int     ret = tsnprintf(gDataSinkFilePath, sizeof(gDataSinkFilePath), "%s/tdengine_stream_data/", tsTempDir);
  if (ret < 0) {
    stError("failed to get stream data sink path ret:%d", ret);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  if (!taosIsDir(gDataSinkFilePath)) {
    code = taosMulMkDir(gDataSinkFilePath);
  }
  if (code != 0) {
    return code;
  }
  stInfo("create stream data sink path %s", gDataSinkFilePath);
  return TSDB_CODE_SUCCESS;
}

static int32_t createStreamDataSinkFileMgr(int64_t streamId, SDataSinkFileMgr** ppDaSinkFileMgr) {
  SDataSinkFileMgr* pFileMgr = NULL;
  pFileMgr = (SDataSinkFileMgr*)taosMemoryCalloc(1, sizeof(SDataSinkFileMgr));
  if (pFileMgr == NULL) {
    return terrno;
  }
  int32_t now = taosGetTimestampSec();
  snprintf(pFileMgr->fileName, FILENAME_MAX, "%s//%s_%d_%" PRId64, gDataSinkFilePath, "stream", now, streamId);

  pFileMgr->fileBlockCount = 0;
  pFileMgr->fileBlockUsedCount = 0;
  pFileMgr->fileSize = 0;
  tRBTreeCreate(&pFileMgr->pFreeFileBlockList, compareFreeBlock);
  pFileMgr->writingGroupId = -1;
  pFileMgr->readingGroupId = -1;
  pFileMgr->writeFilePtr = NULL;
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
      taosCloseFile(&(*ppDaSinkFileMgr)->writeFilePtr);
      (*ppDaSinkFileMgr)->writeFilePtr = NULL;
    }
    if ((*ppDaSinkFileMgr)->readFilePtr) {
      taosCloseFile(&(*ppDaSinkFileMgr)->readFilePtr);
      (*ppDaSinkFileMgr)->readFilePtr = NULL;
    }
    if (strlen((*ppDaSinkFileMgr)->fileName) > 0) {
      taosRemoveFile((*ppDaSinkFileMgr)->fileName);
      (*ppDaSinkFileMgr)->fileName[0] = '\0';
    }

    clearAllFreeBlocks(&(*ppDaSinkFileMgr)->pFreeFileBlockList);
    taosMemoryFreeClear((*ppDaSinkFileMgr));
  }
}

static int32_t initStreamDataSinkFile(SSlidingTaskDSMgr* pStreamDataSink) {
  if (pStreamDataSink->pFileMgr == NULL) {
    return createStreamDataSinkFileMgr(pStreamDataSink->streamId, &pStreamDataSink->pFileMgr);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t openFileForWrite(SDataSinkFileMgr* pFileMgr) {
  void* existing = atomic_load_ptr(&pFileMgr->writeFilePtr);
  if (existing == NULL) {
    void* newPtr = taosOpenFile(pFileMgr->fileName, TD_FILE_CREATE | TD_FILE_WRITE);
    if (newPtr == NULL) {
      stError("open file %s failed, err: %s", pFileMgr->fileName, terrMsg);
      return terrno;
    }

    void* oldPtr = atomic_val_compare_exchange_ptr(&pFileMgr->writeFilePtr, NULL, newPtr);
    if (oldPtr != NULL) {
      TdFilePtr fileToClose = (TdFilePtr)newPtr;
      taosCloseFile(&fileToClose);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t openFileForRead(SDataSinkFileMgr* pFileMgr) {
  void* existing = atomic_load_ptr(&pFileMgr->readFilePtr);

  if (existing == NULL) {
    void* newPtr  = taosOpenFile(pFileMgr->fileName, TD_FILE_CREATE | TD_FILE_READ);
    if (newPtr == NULL) {
      stError("open file %s failed, err: %s", pFileMgr->fileName, terrMsg);
      return terrno;
    }

    void* oldPtr = atomic_val_compare_exchange_ptr(&pFileMgr->readFilePtr, NULL, newPtr);
    if (oldPtr != NULL) {
      TdFilePtr fileToClose = (TdFilePtr)newPtr;
      taosCloseFile(&fileToClose);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void getFreeBlock(SDataSinkFileMgr* pFileMgr, int32_t needSize, SFileBlockInfo* pGroupBlockOffset) {
  FreeBlock* pFreeBlock = popBestFitBlock(&pFileMgr->pFreeFileBlockList, needSize);
  if (pFreeBlock != NULL) {
    pGroupBlockOffset->size = pFreeBlock->length;
    pGroupBlockOffset->offset = pFreeBlock->start;
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
  if (pResult->dataMode & DATA_ALLOC_MODE_SLIDING) {
    SSlidingGrpMgr* pSlidingGrpMgr = (SSlidingGrpMgr*)pResult->groupData;
    if (++pResult->offset < pSlidingGrpMgr->blocksInFile->size) {
      return false;
    } else {
      return true;
    }
  } else if (pResult->dataMode & DATA_ALLOC_MODE_REORDER) {
    SReorderGrpMgr* pReorderGrpMgr = (SReorderGrpMgr*)pResult->groupData;
    SDatasInWindow* pWindowData = NULL;

    SListNode* pNode = (SListNode*)pResult->winIndex;
    SListIter  iter = {pNode->dl_next_, TD_LIST_FORWARD};

    pWindowData = (SDatasInWindow*)pNode->data;
    if (++pResult->offset < pWindowData->pBlockFileBuffer->size) {
      return false;
    } else {
      pResult->offset = 0;
      while ((pNode = tdListNext(&iter)) != NULL) {
        pWindowData = (SDatasInWindow*)pNode->data;
        if (pWindowData->timeRange.startTime >= pResult->reqStartTime &&
            pWindowData->timeRange.startTime <= pResult->reqEndTime) {
          if (!pWindowData->pBlockFileBuffer || pWindowData->pBlockFileBuffer->size <= 0) {
            continue;
          }
          pResult->winIndex = (int64_t)pNode;
          return false;
        }
        if (pWindowData->timeRange.startTime > pResult->reqEndTime) {
          pResult->winIndex = 0;
          return true;
        }
      }
    }
  } else {
    // 在读取数据时已完成指针移动
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

static int32_t readFileDataToReorderWindows(SResultIter* pResult, SSDataBlock** ppBlock, SReorderGrpMgr* pReorderGrpMgr,
                                            int32_t tsColSlotId, SBlockFileBuffer* pFileBuffer) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  char* buf = taosMemoryCalloc(1, pFileBuffer->len);
  if (buf == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }

  if (!pResult->pFileMgr->readFilePtr) {
    code = openFileForRead(pResult->pFileMgr);
    if (code != 0) {
      stError("failed to open file for read, err: %s", terrMsg);
      return code;
    }
  }

  int64_t readLen = taosPReadFile(pResult->pFileMgr->readFilePtr, buf, pFileBuffer->len, pFileBuffer->fileOffset);
  if (readLen < 0 || readLen != pFileBuffer->len) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }
  QUERY_CHECK_CODE(code, lino, _exit);
  code = blockDecode(pBlock, buf, NULL);
  QUERY_CHECK_CODE(code, lino, _exit);
  if (pBlock->info.rows == 0) {
    blockDataDestroy(pBlock);
  }
  *ppBlock = pBlock;

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to read data from file, err: %s, lineno:%d", terrMsg, lino);
    if (buf) {
      taosMemoryFreeClear(buf);
    }

    return code;
  }
  taosMemoryFree(buf);
  return TSDB_CODE_SUCCESS;
}

static int32_t readFileDataToSlidingWindows(SResultIter* pResult, SSlidingGrpMgr* pSlidingGrpMgr, int32_t tsColSlotId,
                                            SBlocksInfoFile* pBlockInfo, bool* finished) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  char* buf = taosMemoryCalloc(1, pBlockInfo->dataLen);
  if (buf == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }

  if (!pResult->pFileMgr->readFilePtr) {
    code = openFileForRead(pResult->pFileMgr);
    if (code != 0) {
      stError("failed to open file for read, err: %s", terrMsg);
      return code;
    }
  }

  int64_t readLen = taosPReadFile(pResult->pFileMgr->readFilePtr, buf, pBlockInfo->dataLen, pBlockInfo->groupOffset);
  if (readLen < 0 || readLen != pBlockInfo->dataLen) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }

  *finished = false;
  char* start = buf;
  while (true) {
    SWindowDataInMem* pWindowData = (SWindowDataInMem*)start;

    if (pWindowData->startTime > pResult->reqEndTime) {
      *finished = true;
      break;
    } else if (pWindowData->endTime < pResult->reqStartTime) {
      // do nothing
    } else {
      *finished = false;
      SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
      if (pBlock == NULL) {
        return terrno;
      }
      QUERY_CHECK_CODE(code, lino, _exit);
      code = blockSpecialDecodeLaterPart(pBlock, getWindowDataBuf(pWindowData), tsColSlotId, pResult->reqStartTime,
                                         pResult->reqEndTime);
      QUERY_CHECK_CODE(code, lino, _exit);
      if (pBlock->info.rows == 0) {
        blockDataDestroy(pBlock);
      } else {
        code = appendTmpSBlocksInMem(pResult, pBlock);
        QUERY_CHECK_CODE(code, lino, _exit);
      }
    }
    start += sizeof(SWindowDataInMem) + pWindowData->dataLen;
    if (start >= buf + pBlockInfo->dataLen) {
      break;  // 已经读取到数据末尾
    }
  }
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to read data from file, err: %s, lineno:%d", terrMsg, lino);
    if (buf) {
      taosMemoryFreeClear(buf);
    }
    if (pResult->tmpBlocksInMem != NULL) {
      taosArrayDestroy(pResult->tmpBlocksInMem);
      pResult->tmpBlocksInMem = NULL;
    }
    return code;
  }

  taosMemoryFree(buf);
  return TSDB_CODE_SUCCESS;
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
      addToFreeBlock(pFileMgr, &fileBlockInfo);
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

int32_t readReorderDataFromFile(SResultIter* pResult, SSDataBlock** ppBlock, int32_t tsColSlotId) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SDataSinkFileMgr* pFileMgr = pResult->pFileMgr;

  SReorderGrpMgr* pReorderGrpMgr = (SReorderGrpMgr*)pResult->groupData;
  SDatasInWindow* pWindowData = NULL;

  SListNode* pNode = (SListNode*)pResult->winIndex;
  SListIter  iter = {(void*)pResult->winIndex, TD_LIST_FORWARD};

  pWindowData = (SDatasInWindow*)pNode->data;
  if (pWindowData == NULL) {
    stError("getNextStreamDataCache failed, groupId: %" PRId64 " start:%" PRId64 " end:%" PRId64
            " dataPos: %d, winIndex: %" PRId64 ", offset: %" PRId64,
            pResult->groupId, pResult->reqStartTime, pResult->reqEndTime, pResult->dataPos, pResult->winIndex,
            pResult->offset);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  if (pResult->offset < pWindowData->pBlockFileBuffer->size) {
    SBlockFileBuffer* pFileBuffer = taosArrayGet(pWindowData->pBlockFileBuffer, pResult->offset);
    code = readFileDataToReorderWindows(pResult, ppBlock, pReorderGrpMgr, tsColSlotId, pFileBuffer);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
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
  if (pResult->dataMode & DATA_ALLOC_MODE_SLIDING) {
    return readSlidingDataFromFile(pResult, ppBlock, tsColSlotId);
  } else if (pResult->dataMode & DATA_ALLOC_MODE_REORDER) {
    return readReorderDataFromFile(pResult, ppBlock, tsColSlotId);
  } else {
    return readAlignDataFromFile(pResult, ppBlock, tsColSlotId);
  }
}

int32_t moveSlidingGrpMemCache(SSlidingTaskDSMgr* pSlidingTaskMgr, SSlidingGrpMgr* pSlidingGrp) {
  if (pSlidingGrp->winDataInMem == NULL || pSlidingGrp->winDataInMem->size == 0) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t    code = 0;
  int32_t    lino = 0;
  TaosIOVec* iov = NULL;

  if (!pSlidingTaskMgr->pFileMgr) {
    code = initStreamDataSinkFile(pSlidingTaskMgr);
    if (code != 0) {
      stError("failed to init stream data sink file, err: %s", terrMsg);
    }
    code = openFileForWrite(pSlidingTaskMgr->pFileMgr);
    if (code != 0) {
      destroyStreamDataSinkFile(&pSlidingTaskMgr->pFileMgr);
    }
  }
  SDataSinkFileMgr* pFileMgr = pSlidingTaskMgr->pFileMgr;

  int32_t nWin = taosArrayGetSize(pSlidingGrp->winDataInMem);
  iov = taosMemCalloc(nWin, sizeof(TaosIOVec));
  if (iov == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }
  int32_t moveWinCount = 0;
  int32_t needSize = 0;
  for (int i = 0; i < nWin; ++i) {
    needSize = 0;
    SWindowDataInMem* pSlidingWin = *(SWindowDataInMem**)taosArrayGet(pSlidingGrp->winDataInMem, i);
    if (pSlidingWin == NULL || pSlidingWin->dataLen < 0) {
      stError("sliding window in mem is NULL or dataLen < 0, i:%d, pSlidingWin:%p", i, pSlidingWin);
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
    if (pSlidingWin->dataLen == 0) {
      // todo
    }
    if (needSize + pSlidingWin->dataLen + sizeof(SWindowDataInMem) > DS_FILE_BLOCK_SIZE) {
      break;
    }
    ++moveWinCount;
    iov[i].iov_base = pSlidingWin;
    iov[i].iov_len = pSlidingWin->dataLen + sizeof(SWindowDataInMem);
    needSize += pSlidingWin->dataLen + sizeof(SWindowDataInMem);
  }

  if (pSlidingGrp->blocksInFile == NULL) {
    pSlidingGrp->blocksInFile = taosArrayInit(0, sizeof(SBlocksInfoFile));
    if (pSlidingGrp->blocksInFile == NULL) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
  }
  SBlocksInfoFile fileBlockInfo = {0};
  SFileBlockInfo  groupBlockOffset = {0};
  getFreeBlock(pFileMgr, needSize, &groupBlockOffset);
  int64_t groupOffset;  // offset in file
  int64_t dataStartOffset;
  int64_t dataLen;
  int64_t capacity;  // size in file
  fileBlockInfo.groupOffset = groupBlockOffset.offset;
  fileBlockInfo.capacity = groupBlockOffset.size;
  fileBlockInfo.dataLen = needSize;
  stDebug("move sliding group memory cache, groupId:%" PRId64
          ", moveWinCount:%d, needSize:%d, "
          "groupOffset:%" PRId64 ", capacity:%" PRId64 ", dataLen:%" PRId64,
          pSlidingGrp->groupId, moveWinCount, needSize, fileBlockInfo.groupOffset, fileBlockInfo.capacity,
          fileBlockInfo.dataLen);

  if (false) {  // 续写时， 可以不进行 taosLSeekFile, todo

  } else {  // 第一次写入
    int64_t ret = taosLSeekFile(pFileMgr->writeFilePtr, fileBlockInfo.groupOffset, SEEK_SET);
    if (ret < 0) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _exit);
    }

    int64_t writeLen = taosWritevFile(pFileMgr->writeFilePtr, iov, moveWinCount);
    if (writeLen != needSize) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
    QUERY_CHECK_CODE(code, lino, _exit);
  }

  taosArrayRemoveBatch(pSlidingGrp->winDataInMem, 0, moveWinCount, destroySlidingWindowInMemPP);

  void* pBlocksInFile = taosArrayPush(pSlidingGrp->blocksInFile, &fileBlockInfo);
  if (pBlocksInFile == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to move sliding group memory cache, code: 0x%0x, lineno:%d", code, lino);
    addToFreeBlock(pFileMgr, &groupBlockOffset);
  }
  taosMemoryFree(iov);
  return code;
}

int32_t writeReorderDataToFile(SDataSinkFileMgr* pFileMgr, SReorderGrpMgr* pReorderGrp, TaosIOVec* iov, int32_t iovNum,
                    int32_t needSize, int64_t* pGroupOffset) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlocksInfoFile fileBlockInfo = {0};
  SFileBlockInfo  groupBlockOffset = {0};
  getFreeBlock(pFileMgr, needSize, &groupBlockOffset);
  int64_t groupOffset;  // offset in file
  int64_t dataStartOffset;
  int64_t dataLen;
  int64_t capacity;  // size in file
  fileBlockInfo.groupOffset = groupBlockOffset.offset;
  *pGroupOffset = groupBlockOffset.offset;
  fileBlockInfo.capacity = groupBlockOffset.size;
  fileBlockInfo.dataLen = needSize;
  stDebug("move sliding group memory cache, groupId:%" PRId64
          ", moveWinCount:%d, needSize:%d, "
          "groupOffset:%" PRId64 ", capacity:%" PRId64 ", dataLen:%" PRId64,
          pReorderGrp->groupId, iovNum, needSize, fileBlockInfo.groupOffset, fileBlockInfo.capacity,
          fileBlockInfo.dataLen);

  int64_t writeLen = taosPWritevFile(pFileMgr->writeFilePtr, fileBlockInfo.groupOffset, SEEK_SET, iov, iovNum);
  if (writeLen != needSize) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to move sliding group memory cache, code: 0x%0x, lineno:%d", code, lino);
    addToFreeBlock(pFileMgr, &groupBlockOffset);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t moveReorderGrpMemCache(SSlidingTaskDSMgr* pSlidingTaskMgr, SReorderGrpMgr* pReorderGrp) {
  if (tdListGetTail(&pReorderGrp->winAllData) == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t    code = 0;
  int32_t    lino = 0;
  TaosIOVec* iov = NULL;

  if (!pSlidingTaskMgr->pFileMgr) {
    code = initStreamDataSinkFile(pSlidingTaskMgr);
    if (code != 0) {
      stError("failed to init stream data sink file, err: %s", terrMsg);
    }
    code = openFileForWrite(pSlidingTaskMgr->pFileMgr);
    if (code != 0) {
      destroyStreamDataSinkFile(&pSlidingTaskMgr->pFileMgr);
    }
  }
  SDataSinkFileMgr* pFileMgr = pSlidingTaskMgr->pFileMgr;

  SListIter  foundDataIter = {0};
  SListNode* pNode = NULL;
  int32_t moveWinCount = 0;
  int32_t needSize = 0;
  int32_t existFileBlockCount = 0;
  tdListInitIter(&pReorderGrp->winAllData, &foundDataIter, TD_LIST_FORWARD);
  while ((pNode = tdListNext(&foundDataIter)) != NULL) {
    needSize = 0;
    SDatasInWindow* pWinData = (SDatasInWindow*)pNode->data;
    moveWinCount = pWinData->datas->size;
    if (moveWinCount <= 0) continue;

    iov = taosMemCalloc(moveWinCount, sizeof(TaosIOVec));
    if (iov == NULL) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _exit);
    }

    existFileBlockCount = taosArrayGetSize(pWinData->pBlockFileBuffer);

    if (pWinData->pBlockFileBuffer) {
      void* tmp = taosArrayReserve(pWinData->pBlockFileBuffer, moveWinCount);
      QUERY_CHECK_NULL(tmp, code, lino, _exit, terrno);
    } else {
      pWinData->pBlockFileBuffer = taosArrayInit_s(sizeof(SBlockFileBuffer), moveWinCount);
      QUERY_CHECK_NULL(pWinData->pBlockFileBuffer, code, lino, _exit, terrno);
    }

    for (int i = 0; i < moveWinCount; ++i) {
      SBlockBuffer* pBlockBuffer = taosArrayGet(pWinData->datas, i);
      if (pBlockBuffer == NULL || pBlockBuffer->len < 0) {
        stError("sliding window in mem is NULL or dataLen < 0, i:%d, pSlidingWin:%p", i, pBlockBuffer);
        code = TSDB_CODE_STREAM_INTERNAL_ERROR;
        QUERY_CHECK_CODE(code, lino, _exit);
      }

      iov[i].iov_base = pBlockBuffer->data;
      iov[i].iov_len = pBlockBuffer->len;
      needSize += pBlockBuffer->len;

      SBlockFileBuffer* pFileBuffer = TARRAY_GET_ELEM(pWinData->pBlockFileBuffer, existFileBlockCount + i);
      pFileBuffer->len = pBlockBuffer->len;
      QUERY_CHECK_CODE(code, lino, _exit);
    }

    int64_t groupOffset = 0;
    code = writeReorderDataToFile(pFileMgr, pReorderGrp, iov, moveWinCount, needSize, &groupOffset);
    QUERY_CHECK_CODE(code, lino, _exit);

    for (int i = 0; i < moveWinCount; ++i) {
      SBlockFileBuffer* pFileBuffer = TARRAY_GET_ELEM(pWinData->pBlockFileBuffer, existFileBlockCount + i);
      pFileBuffer->fileOffset = groupOffset;
      groupOffset += pFileBuffer->len;
    }

    cleanReorderDataInMem(pWinData);
  }
  
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to move sliding group memory cache, code: 0x%0x, lineno:%d", code, lino);
  }
  taosMemoryFree(iov);
  return code;
}
