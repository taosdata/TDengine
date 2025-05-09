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




static int32_t createSGroupFileDataMgr(SGroupFileDataMgr** ppSGroupFileDataMgr, int64_t groupId, const SFileBlockInfo* fileOffset);
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
  int32_t code = createFileBlockSkipList(&pFileMgr->pFreeBlockSkipList);
  if (code != 0) {
    stError("failed to create file block skip list, err: %s", terrMsg);
    taosMemoryFreeClear(pFileMgr);
    return code;
  }
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
    if ((*ppDaSinkFileMgr)->groupBlockList) {
      taosHashCleanup((*ppDaSinkFileMgr)->groupBlockList);
      (*ppDaSinkFileMgr)->groupBlockList = NULL;
    }
    if ((*ppDaSinkFileMgr)->pFreeBlockSkipList) {
      destroyFileBlockSkipList((*ppDaSinkFileMgr)->pFreeBlockSkipList);
      (*ppDaSinkFileMgr)->pFreeBlockSkipList = NULL;
    }
    if (strlen((*ppDaSinkFileMgr)->fileName) > 0) {
      taosRemoveFile((*ppDaSinkFileMgr)->fileName);
      (*ppDaSinkFileMgr)->fileName[0] = '\0';
    }
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
                                       const SFileBlockInfo* pBlockInfo) {
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
  (*ppSGroupFileDataMgr)->blockInfo = *pBlockInfo;
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

static void getFreeBlock(SDataSinkFileMgr* pFileMgr, int32_t needSize, SFileBlockInfo* pGroupBlockOffset) {
  if(pFileMgr->pFreeBlockSkipList != NULL && pFileMgr->pFreeBlockSkipList->size > 0) {
    SFileBlockInfo* pFileBlockInfo = findFirstGreaterThan(pFileMgr->pFreeBlockSkipList, needSize);
    if (pFileBlockInfo != NULL) {
      pGroupBlockOffset->size = pFileBlockInfo->size;
      pGroupBlockOffset->offset = pFileBlockInfo->offset;
      return;
    }
  }
  pGroupBlockOffset->offset = pFileMgr->fileSize;
  pGroupBlockOffset->size = needSize;
  pFileMgr->fileBlockCount++;
  pFileMgr->fileBlockUsedCount++;
  pFileMgr->fileSize += needSize;
  return;
}

static int32_t createOrGetSGroupFileDataMgr(SDataSinkFileMgr* pFileMgr, int64_t groupId,
                                            SGroupFileDataMgr** ppSGroupFileDataMgr, int32_t needSize) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SGroupFileDataMgr*  pSGroupFileDataMgr = NULL;
  SGroupFileDataMgr** ppExistMgr =
      (SGroupFileDataMgr**)taosHashGet(pFileMgr->groupBlockList, &groupId, sizeof(groupId));
  if (ppExistMgr != NULL) {
    *ppSGroupFileDataMgr = *ppExistMgr;
    if ((*ppExistMgr) != NULL && (*ppExistMgr)->allWindowDataLen + needSize <= (*ppExistMgr)->blockInfo.size) {
      stDebug("need size %d, groupId %" PRId64 " allWindowDataLen %" PRId64, needSize, groupId,
              (*ppExistMgr)->allWindowDataLen);
      return TSDB_CODE_SUCCESS;
    }
  } else {
    *ppSGroupFileDataMgr = pSGroupFileDataMgr;
  }
  SFileBlockInfo groupBlockOffset;

  // todo 
  // createSGroupFileDataMgr 不重复创建
  // winndowDataInFile offset 不能使用 group 内偏移, 而要使用 文件内偏移，group 的文件内偏移好像没用了
  getFreeBlock(pFileMgr, needSize, &groupBlockOffset);
  code = createSGroupFileDataMgr(&pSGroupFileDataMgr, groupId, &groupBlockOffset);
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
  pWindowData->blockInfo = pSGroupFileDataMgr->blockInfo;

  if (pSGroupFileDataMgr->allWindowDataLen > 0) {  // 续写
    pWindowData->offset = pSGroupFileDataMgr->allWindowDataLen;

    if (pFileMgr->writingGroupId != pSGroupFileDataMgr->groupId) {
      stWarn("write data to groupId %" PRId64 " but writing groupId %" PRId64, pSGroupFileDataMgr->groupId,
             pFileMgr->writingGroupId);
      int64_t ret = taosLSeekFile(pFileMgr->writeFilePtr, pSGroupFileDataMgr->blockInfo.offset + pWindowData->offset, SEEK_SET);
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
    int64_t ret = taosLSeekFile(pFileMgr->writeFilePtr, pSGroupFileDataMgr->blockInfo.offset, SEEK_SET);
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

int32_t writeToFile(SSlidingTaskDSMgr* pStreamDataSink, int64_t groupId, TSKEY wstart, TSKEY wend,
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

  int64_t ret = taosLSeekFile(pFileMgr->readFilePtr, pWindowData->blockInfo.offset + pWindowData->offset, SEEK_SET);
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
    int64_t ret = taosLSeekFile(pFileMgr->readFilePtr, pWindowData->blockInfo.offset + pWindowData->offset, SEEK_SET);
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
  // 移动到小的文件块区域，或者完全释放，或者不做处理

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
