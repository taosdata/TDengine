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
#include "osFile.h"
#include "osTime.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"
#include "thash.h"

char      gDataSinkFilePath[PATH_MAX] = {0};
const int gFileGroupBlockMaxSize = 64 * 1024;  // 64K

static int32_t createSGroupFileDataMgr(SGroupFileDataMgr** ppSGroupFileDataMgr, int64_t groupId, int64_t fileOffset);
static void destroySGroupFileDataMgr(void* pData);

void syncWindowDataFileAdd(SWindowDataInFile* pWindowData);
void syncWindowDataFileDel(SWindowDataInFile* pWindowData);

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
  pFileMgr->fileGroupBlockMaxSize = gFileGroupBlockMaxSize;
  pFileMgr->freeBlockList = taosArrayInit(0, sizeof(int64_t));
  pFileMgr->writingGroupId = -1;
  pFileMgr->readingGroupId = -1;
  pFileMgr->writeFilePtr = NULL;
  pFileMgr->readFilePtr = NULL;
  if (pFileMgr->freeBlockList == NULL) {
    stError("failed to create free block list, err: %s", terrMsg);
    taosMemoryFreeClear(pFileMgr);
    return terrno;
  }
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
    if ((*ppDaSinkFileMgr)->freeBlockList) {
      taosArrayDestroy((*ppDaSinkFileMgr)->freeBlockList);
      (*ppDaSinkFileMgr)->freeBlockList = NULL;
    }
    if ((*ppDaSinkFileMgr)->groupBlockList) {
      taosHashCleanup((*ppDaSinkFileMgr)->groupBlockList);
      (*ppDaSinkFileMgr)->groupBlockList = NULL;
    }
    taosMemoryFreeClear((*ppDaSinkFileMgr));
  }
}

static int32_t initStreamDataSinkFile(SStreamTaskDSManager* pStreamDataSink) {
  if (pStreamDataSink->pFileMgr == NULL) {
    return createStreamDataSinkFileMgr(pStreamDataSink->streamId, &pStreamDataSink->pFileMgr);
  }
  return TSDB_CODE_SUCCESS;
}



static int32_t openFileForWrite(SDataSinkFileMgr* pFileMgr) {
  if (pFileMgr->writeFilePtr == NULL) {
    pFileMgr->writeFilePtr = taosOpenFile(pFileMgr->fileName, TD_FILE_CREATE | TD_FILE_WRITE);
    if (pFileMgr->writeFilePtr == NULL) {
      stError("open file %s failed, err: %s", pFileMgr->fileName, terrMsg);  
      return terrno;
    }
    if (pFileMgr->groupBlockList == NULL) {
      pFileMgr->groupBlockList = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
      if (pFileMgr->groupBlockList == NULL) {
        taosCloseFile(&pFileMgr->writeFilePtr);
        pFileMgr->writeFilePtr = NULL;
        stError("failed to create group block list, err: %s", terrMsg);
        return terrno;
      }
      taosHashSetFreeFp(pFileMgr->groupBlockList, destroySGroupFileDataMgr);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t openFileForRead(SDataSinkFileMgr* pFileMgr) {
  if (pFileMgr->readFilePtr == NULL) {
    pFileMgr->readFilePtr = taosOpenFile(pFileMgr->fileName, TD_FILE_CREATE | TD_FILE_READ);
    if (pFileMgr->readFilePtr == NULL) {
      stError("open file %s failed, err: %s", pFileMgr->fileName, terrMsg);  
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createSGroupFileDataMgr(SGroupFileDataMgr** ppSGroupFileDataMgr, int64_t groupId,
                                       int64_t groupBlockOffset) {
  *ppSGroupFileDataMgr = (SGroupFileDataMgr*)taosMemoryCalloc(1, sizeof(SGroupFileDataMgr));
  if (*ppSGroupFileDataMgr == NULL) {
    stError("failed to create group file data manager, err: %s", terrMsg);
    return terrno;
  }
  (*ppSGroupFileDataMgr)->groupId = groupId;
  (*ppSGroupFileDataMgr)->windowDataInFile = taosArrayInit(0, sizeof(SWindowDataInFile*));
  if ((*ppSGroupFileDataMgr)->windowDataInFile == NULL) {
    stError("failed to create window data in file, err: %s", terrMsg);
    taosMemoryFree(*ppSGroupFileDataMgr);
    return terrno;
  }
  (*ppSGroupFileDataMgr)->groupDataStartOffSet = groupBlockOffset;
  (*ppSGroupFileDataMgr)->allWindowDataLen = 0;
  (*ppSGroupFileDataMgr)->lastWstartInFile = 0;
  (*ppSGroupFileDataMgr)->hasDataInFile = false;
  return TSDB_CODE_SUCCESS;
}

static void destroySGroupFileDataMgr(void* pData) {
  SGroupFileDataMgr** ppSGroupFileDataMgr = (SGroupFileDataMgr**)pData;
  if (ppSGroupFileDataMgr == NULL || *ppSGroupFileDataMgr == NULL) {
    return;
  }
  SGroupFileDataMgr* pSGroupFileDataMgr = *ppSGroupFileDataMgr;
  if (pSGroupFileDataMgr->windowDataInFile) {
    taosArrayDestroy(pSGroupFileDataMgr->windowDataInFile);
    pSGroupFileDataMgr->windowDataInFile = NULL;
  }
  taosMemoryFree(pSGroupFileDataMgr);
  *ppSGroupFileDataMgr = NULL;
}

static int64_t getFreeBlock(SDataSinkFileMgr* pFileMgr) {
  if (pFileMgr->freeBlockList != NULL && pFileMgr->freeBlockList->size > 0) {
    int64_t* pFreeBlockOffset = (int64_t*)taosArrayPop(pFileMgr->freeBlockList);
    if (pFreeBlockOffset != NULL) {
      pFileMgr->fileBlockUsedCount++;
      return *pFreeBlockOffset;
    }
  }
  pFileMgr->fileBlockCount++;
  pFileMgr->fileBlockUsedCount++;
  return (pFileMgr->fileBlockCount - 1) * pFileMgr->fileGroupBlockMaxSize;
}

static int32_t createOrGetSGroupFileDataMgr(SDataSinkFileMgr* pFileMgr, int64_t groupId,
                                            SGroupFileDataMgr** ppSGroupFileDataMgr, int32_t needSize) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SGroupFileDataMgr*  pSGroupFileDataMgr = NULL;
  SGroupFileDataMgr** ppExistMgr =
      (SGroupFileDataMgr**)taosHashGet(pFileMgr->groupBlockList, &groupId, sizeof(groupId));
  if (needSize > pFileMgr->fileGroupBlockMaxSize) {
    stError("need size %d, groupId %" PRId64 " fileGroupBlockMaxSize %" PRId64, needSize, groupId,
            pFileMgr->fileGroupBlockMaxSize);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (ppExistMgr != NULL) {
    if ((*ppExistMgr) != NULL && (*ppExistMgr)->allWindowDataLen + needSize <= pFileMgr->fileGroupBlockMaxSize) {
      stDebug("need size %d, groupId %" PRId64 " allWindowDataLen %" PRId64, needSize, groupId,
              (*ppExistMgr)->allWindowDataLen);
      *ppSGroupFileDataMgr = *ppExistMgr;
      return TSDB_CODE_SUCCESS;
    }
  }
  int64_t groupBlockOffset = getFreeBlock(pFileMgr);
  code = createSGroupFileDataMgr(&pSGroupFileDataMgr, groupId, groupBlockOffset);
  if (code != 0) {
    stError("failed to create group file data manager, err: %s", terrMsg);
    return code;
  }

  code =
      taosHashPut(pFileMgr->groupBlockList, &groupId, sizeof(groupId), &pSGroupFileDataMgr, sizeof(SGroupFileDataMgr*));
  if (code != 0) {
    destroySGroupFileDataMgr(&pSGroupFileDataMgr);
    stError("failed to put group file data manager, err: %s", terrMsg);
    return code;
  }

  *ppSGroupFileDataMgr = pSGroupFileDataMgr;

  return code;
}

static int32_t writeWindowDataIntoGroupFile(SDataSinkFileMgr* pFileMgr, SGroupFileDataMgr* pSGroupFileDataMgr,
                                            const char* data, SWindowDataInFile* pWindowData) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  void* p = taosArrayPush(pSGroupFileDataMgr->windowDataInFile, &pWindowData);
  if (p == NULL) {
    stError("failed to push window data into group file data manager, err: %s", terrMsg);
    return terrno;
  }
  pWindowData->groupDataStartOffSet = pSGroupFileDataMgr->groupDataStartOffSet;

  if (pSGroupFileDataMgr->allWindowDataLen > 0) {  // 续写
    pWindowData->offset = pSGroupFileDataMgr->allWindowDataLen;

    if (pFileMgr->writingGroupId != pSGroupFileDataMgr->groupId) {
      stWarn("write data to groupId %" PRId64 " but writing groupId %" PRId64, pSGroupFileDataMgr->groupId,
             pFileMgr->writingGroupId);
      int64_t ret = taosLSeekFile(pFileMgr->writeFilePtr, pSGroupFileDataMgr->groupDataStartOffSet + pWindowData->offset, SEEK_SET);
      if (ret < 0) {
        code = terrno;
        QUERY_CHECK_CODE(code, lino, _exit);
      }
    }

    int writeLen = taosWriteFile(pFileMgr->writeFilePtr, data, pWindowData->dataLen);
    if (writeLen != pWindowData->dataLen) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
  } else {  // 第一次写入
    pWindowData->offset = 0;
    int64_t ret = taosLSeekFile(pFileMgr->writeFilePtr, pSGroupFileDataMgr->groupDataStartOffSet, SEEK_SET);
    if(ret < 0) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _exit);
    }

    int64_t writeLen = taosWriteFile(pFileMgr->writeFilePtr, data, pWindowData->dataLen);
    if(writeLen != pWindowData->dataLen) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
    QUERY_CHECK_CODE(code, lino, _exit);
  }
  pSGroupFileDataMgr->lastWstartInFile = pWindowData->wstart;
  pSGroupFileDataMgr->allWindowDataLen += pWindowData->dataLen;
  pFileMgr->writingGroupId = pSGroupFileDataMgr->groupId;
  return TSDB_CODE_SUCCESS;
  _exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to write data to file, err: %s lino:%d", terrMsg, lino);
    taosArrayPop(pSGroupFileDataMgr->windowDataInFile);
  }
  return code;
}

static int32_t writeBufInfoStreamTaskFile(SDataSinkFileMgr* pFileMgr, int64_t groupId, char* buf, SWindowDataInFile* pWindowData) {
  int32_t code = TSDB_CODE_SUCCESS;

  SGroupFileDataMgr* pSGroupFileDataMgr = NULL;
  code = createOrGetSGroupFileDataMgr(pFileMgr, groupId, &pSGroupFileDataMgr, pWindowData->dataLen);
  if (code != 0) {
    stError("failed to create or get group file data manager, err: %s", terrMsg);
    return code;
  }


  code = writeWindowDataIntoGroupFile(pFileMgr, pSGroupFileDataMgr, buf, pWindowData);
  if (code != 0) {
    stError("failed to get or create write block, err: %s", terrMsg);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t writeToFile(SStreamTaskDSManager* pStreamDataSink, int64_t groupId, TSKEY wstart, TSKEY wend,
                    SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex) {
  int32_t code = 0;
  int32_t lino = 0;
  if (!pStreamDataSink->pFileMgr) {
    code = initStreamDataSinkFile(pStreamDataSink);
    if (code != 0) {
      stError("failed to init stream data sink file, err: %s", terrMsg);
      return code;
    }
    code = openFileForWrite(pStreamDataSink->pFileMgr);
    if (code != 0) {
      destroyStreamDataSinkFile(&pStreamDataSink->pFileMgr);
      return code;
    }
  }

  SWindowDataInFile* pWindowData = NULL;

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  size_t dataEncodeBufSize = blockGetEncodeSizeOfRows(pBlock, startIndex, endIndex);
  char*  buf = taosMemoryCalloc(1, dataEncodeBufSize);
  char*  pStart = buf;

  if (pStart == NULL) {
    return terrno;
  }

  int32_t len = 0;
  code = blockEncodeAsRows(pBlock, pStart, dataEncodeBufSize, numOfCols, startIndex, endIndex, &len);
  QUERY_CHECK_CODE(code, lino, _end);

  pWindowData = (SWindowDataInFile*)taosMemoryCalloc(1, sizeof(SWindowDataInFile));
  if (pWindowData == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pWindowData->wstart = wstart;
  pWindowData->wend = wend;
  code = getStreamBlockTS(pBlock, startIndex, &pWindowData->start);
  QUERY_CHECK_CODE(code, lino, _end);
  code = getStreamBlockTS(pBlock, endIndex, &pWindowData->end);
  QUERY_CHECK_CODE(code, lino, _end);
  pWindowData->dataLen = dataEncodeBufSize;

  code = writeBufInfoStreamTaskFile(pStreamDataSink->pFileMgr, groupId, buf, pWindowData);
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to write data to file, err: %s", terrMsg);
    goto _end;
  }

  if (len < 0) {
    taosMemoryFree(buf);
    stError("failed to encode data since %s", tstrerror(terrno));
    return terrno;
  }

  pStreamDataSink->pFileMgr->fileSize += len;

  return TSDB_CODE_SUCCESS;
_end:
  if (pWindowData) {
    taosMemoryFree(pWindowData);
  }
  if (buf) {
    taosMemoryFree(buf);
  }
  return code;
}

void destorySWindowDataInFilePP(void* pData) {
  SWindowDataInFile** ppWindowData = (SWindowDataInFile**)pData;
  if (ppWindowData == NULL || (*ppWindowData) == NULL) {
    return;
  }
  syncWindowDataFileDel(*ppWindowData);
  taosMemoryFree((*ppWindowData));
}

void clearGroupExpiredDataFile(SGroupFileDataMgr* pGroupData, TSKEY start) {
  if (pGroupData->windowDataInFile == NULL) {
    return;
  }

  int32_t size = taosArrayGetSize(pGroupData->windowDataInFile);
  int     deleteCount = 0;
  for (int i = 0; i < size; ++i) {
    SWindowDataInFile* pWindowData = *(SWindowDataInFile**)taosArrayGet(pGroupData->windowDataInFile, i);
    if (pWindowData && pWindowData->wend <= start) {
      deleteCount++;
    } else {
      break;
    }
  }
  if (deleteCount > 0) {
    taosArrayRemoveBatch(pGroupData->windowDataInFile, 0, deleteCount, destorySWindowDataInFilePP);
  }
}

int32_t getFirstDataIterFromFile(SDataSinkFileMgr* pFileMgr, int64_t groupId, TSKEY start, TSKEY end,
                                 void** ppResult) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SResultIter* pResult = NULL;

  //SDataSinkFileMgr* pFileMgr = pStreamTaskDSMgr->pFileMgr;
  if (pFileMgr == NULL) {
    *ppResult = NULL;
    return TSDB_CODE_SUCCESS;
  }
  SGroupFileDataMgr** ppGroupData =
      (SGroupFileDataMgr**)taosHashGet(pFileMgr->groupBlockList, &groupId, sizeof(groupId));
  if (ppGroupData == NULL || *ppGroupData == NULL) {
    *ppResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SGroupFileDataMgr* pGroupData = *ppGroupData;

  clearGroupExpiredDataFile(pGroupData, start);
  // todo 每次 clear file data 之后，如果没有数据了，需要回收 file block

  if (pGroupData->windowDataInFile == NULL) {
    *ppResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SWindowDataInFile* pWindowData = *(SWindowDataInFile**)taosArrayGet(pGroupData->windowDataInFile, 0);
  if (pWindowData == NULL) {
    *ppResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (!pFileMgr->readFilePtr) {
    code = openFileForRead(pFileMgr);
    if (code != 0) {
      stError("failed to open file for read, err: %s", terrMsg);
      return code;
    }
  }

  int64_t ret = taosLSeekFile(pFileMgr->readFilePtr, pWindowData->groupDataStartOffSet + pWindowData->offset, SEEK_SET);
  if (ret < 0) {
    code = terrno;
    stError("failed to seek file, err: %s", terrMsg);
    return code;
  }
  pFileMgr->readingGroupId = groupId;

  pResult = taosMemoryCalloc(1, sizeof(SResultIter));
  if (pResult == NULL) {
    return terrno;
  }

  pResult->pFileMgr = pFileMgr;
  pResult->groupId = groupId;
  pResult->reqStartTime = start;
  pResult->reqEndTime = end;
  pResult->groupData = pGroupData;
  pResult->offset = 0;
  pResult->dataPos = DATA_SINK_FILE;
  *ppResult = pResult;

  return TSDB_CODE_SUCCESS;
}

void setNextIteratorFromFile(SResultIter** ppResult) {
  SResultIter*       pResult = *ppResult;
  SGroupFileDataMgr* pGroupData = (SGroupFileDataMgr*)pResult->groupData;

  if (pResult->offset == -1) {  // 查询内存结束，开始查询文件
    releaseDataIterator((void**)ppResult);
    *ppResult = NULL;
    getFirstDataIterFromFile(pResult->pFileMgr, pResult->groupId, pResult->reqStartTime, pResult->reqEndTime,
                             (void**)ppResult);
    return;
  } else {
    pResult->offset++;
  }
  if (pResult->offset < taosArrayGetSize(pGroupData->windowDataInFile)) {
    SWindowData* pWindowData = *(SWindowData**)taosArrayGet(pGroupData->windowDataInFile, pResult->offset);
    if ((pResult->reqStartTime >= pWindowData->start && pResult->reqStartTime <= pWindowData->end) ||
        (pResult->reqEndTime >= pWindowData->start && pResult->reqEndTime <= pWindowData->end) ||
        (pWindowData->start >= pResult->reqStartTime && pWindowData->end <= pResult->reqEndTime)) {
      return;
    } else {
      goto _end;
      return;
    }
  }
_end:
  releaseDataIterator((void**)ppResult);
  *ppResult = NULL;
  return;
}

void syncWindowDataFileAdd(SWindowDataInFile* pWindowData) {
  int32_t size = pWindowData->dataLen;
  // atomic_add_fetch_64(&pWindowData->pGroupDataInfoMgr->usedMemSize, size);
  // atomic_add_fetch_64(&pWindowData->pGroupDataInfoMgr->pSinkManager->usedMemSize, size);
  // atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, size);
}

void syncWindowDataFileDel(SWindowDataInFile* pWindowData) {
  int32_t size = pWindowData->dataLen;
  // atomic_sub_fetch_64(&pWindowData->pGroupDataInfoMgr->usedMemSize, size);
  // atomic_sub_fetch_64(&pWindowData->pGroupDataInfoMgr->pSinkManager->usedMemSize, size);
  // atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, size);
}

int32_t readDataFromFile(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SDataSinkFileMgr*  pFileMgr = pResult->pFileMgr;
  SGroupFileDataMgr* pGroupData = (SGroupFileDataMgr*)pResult->groupData;
  SWindowDataInFile* pWindowData = *(SWindowDataInFile**)taosArrayGet(pGroupData->windowDataInFile, pResult->offset);
  if (pWindowData == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pWindowData->start > pResult->reqEndTime) {
    *finished = true;
    return TSDB_CODE_SUCCESS;
  } else if (pWindowData->end < pResult->reqStartTime) {
    pFileMgr->readingGroupId = -1; // 跳过读取文件中的数据，下次需要重新 taosLSeekFile
    *finished = false;  // 查看下一个窗口
    return TSDB_CODE_SUCCESS;
  }

  if (pFileMgr->readingGroupId != pGroupData->groupId) {
    int64_t ret = taosLSeekFile(pFileMgr->readFilePtr, pWindowData->groupDataStartOffSet + pWindowData->offset, SEEK_SET);
    if (ret < 0) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _exit);
    }
  }

  char* buf = taosMemoryCalloc(1, pWindowData->dataLen);
  if (buf == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }
  int64_t readLen = taosReadFile(pFileMgr->readFilePtr, buf, pWindowData->dataLen);
  if (readLen < 0 || readLen != pWindowData->dataLen) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _exit);
  }

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }
  QUERY_CHECK_CODE(code, lino, _exit);
  code = blockSpecialDecodeLaterPart(pBlock, buf, pResult->reqStartTime, pResult->reqEndTime);
  QUERY_CHECK_CODE(code, lino, _exit);

  *ppBlock = pBlock;

  // todo
  // 如果内存使用不超过 90%，buf 转移到内存进行管理

  return TSDB_CODE_SUCCESS;
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to read data from file, err: %s, lineno:%d", terrMsg, lino);
    if (buf) {
      taosMemoryFreeClear(buf);
    }
    if (pBlock) {
      blockDataDestroy(pBlock);
    }
  }
  return code;
}
