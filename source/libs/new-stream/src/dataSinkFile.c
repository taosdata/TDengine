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
#include "os.h"
#include "osTime.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"

static SDataSinkManager2 g_pDataSinkManager = {0};
static char              dataSinkFilePath[256] = "/tmp/taosStreamDataSink";

int32_t initStreamDataSinkOnce() {
  static int8_t init = 0;
  int8_t        flag = atomic_val_compare_exchange_8(&init, 0, 1);
  if (flag != 0) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = 0;
  snprintf(dataSinkFilePath, sizeof(dataSinkFilePath), "%s", "/tmp/taosStreamDataSink");
  if (!taosIsDir(dataSinkFilePath)) {
    code = taosMulMkDir(dataSinkFilePath);
  }
  if (code != 0) {
    return code;
  }

  g_pDataSinkManager.memorySize = 0;
  g_pDataSinkManager.maxMemorySize = 0;
  g_pDataSinkManager.fileBlockSize = 0;
  g_pDataSinkManager.readDataFromMesTimes = 0;
  g_pDataSinkManager.readDataFromFileTimes = 0;
  g_pDataSinkManager.DataSinkStreamList =
      taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (g_pDataSinkManager.DataSinkStreamList == NULL) {
    return -1;
  }
  init = true;
  return TSDB_CODE_SUCCESS;
};

static int32_t createStreamDataSinkFile(SStreamDataSinkManager* pStreamDataSink) {
  if (pStreamDataSink->pFile == NULL) {
    pStreamDataSink->pFile = (DataSinkFileState*)taosMemoryCalloc(1, sizeof(DataSinkFileState));
    if (pStreamDataSink->pFile == NULL) {
      return terrno;
    }
    int32_t now = taosGetTimestampSec();
    snprintf(pStreamDataSink->pFile->fileName, FILENAME_MAX, "%s//%s_%d_%" PRId64, dataSinkFilePath, "stream", now,
             pStreamDataSink->streamId);

    pStreamDataSink->pFile->pFile =
        taosOpenFile(pStreamDataSink->pFile->fileName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);
    if (pStreamDataSink->pFile->pFile == NULL) {
      taosMemoryFreeClear(pStreamDataSink->pFile);
      stError("open file %s failed, err: %s", pStreamDataSink->pFile->fileName, terrMsg);
      return terrno;
    }
    pStreamDataSink->pFile->fileBlockCount = 0;
    pStreamDataSink->pFile->fileBlockUsedCount = 0;
    pStreamDataSink->pFile->fileSize = 0;
    pStreamDataSink->pFile->freeBlockList = taosArrayInit(0, sizeof(int64_t));
    if (pStreamDataSink->pFile->freeBlockList == NULL) {
      taosMemoryFreeClear(pStreamDataSink->pFile);
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}
static void destroyStreamDataSinkFile(SStreamDataSinkManager* pStreamDataSink) {
  if (pStreamDataSink->pFile) {
    taosCloseFile(pStreamDataSink->pFile->pFile);
    taosMemoryFreeClear(pStreamDataSink->pFile);
  }
}

static int32_t initStreamDataSinkFile(SStreamDataSinkManager* pStreamDataSink) {
  if (pStreamDataSink->pFile == NULL) {
    return createStreamDataSinkFile(pStreamDataSink);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t blockTo(SStreamDataSinkManager* pStreamDataSink) {
  if (pStreamDataSink->pFile) {
    taosCloseFile(pStreamDataSink->pFile->pFile);
    taosMemoryFreeClear(pStreamDataSink->pFile);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t writeToFile(SStreamDataSinkManager* pStreamDataSink, int64_t groupId, TSKEY wstart, TSKEY wend,
                    SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex) {
  int32_t code = 0;
  if (!pStreamDataSink->pFile) {
    code = initStreamDataSinkFile(pStreamDataSink);
    if (code != 0) {
      return code;
    }
  }

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  size_t dataEncodeBufSize = blockGetEncodeSize(pBlock);

  int32_t size = sizeof(SDataSinkFileHeader) + dataEncodeBufSize;
  char*   buf = taosMemoryCalloc(1, dataEncodeBufSize);
  char*   pStart = buf;
  if (pStart == NULL) {
    return terrno;
  }
  SDataSinkFileHeader* pHeader = (SDataSinkFileHeader*)pStart;
  pHeader->startTime = htobe64(wstart);
  pHeader->endTime = htobe64(wend);
  pHeader->dataLen = htobe64(dataEncodeBufSize);
  pStart += sizeof(SDataSinkFileHeader);

  int32_t len = blockEncode(pBlock, pStart, dataEncodeBufSize, numOfCols);

  if (len < 0) {
    taosMemoryFree(buf);
    stError("failed to encode data since %s", tstrerror(terrno));
    return terrno;
  }

  pStreamDataSink->pFile->fileBlockCount++;
  pStreamDataSink->pFile->fileBlockUsedCount++;
  pStreamDataSink->pFile->fileSize += len;

  return TSDB_CODE_SUCCESS;
}

int32_t getTableWindowData(void* pCache, const char* groupId, int64_t tableId, int64_t start, int64_t end,
                           void** pIter);

int32_t getNextTableWindwoData(void** pIter, SSDataBlock** ppBlock);
int32_t getTableWindowDataSize(void* pCache, const char* groupId, int64_t tableId, int64_t start, int64_t end,
                               int64_t* pSize);

SStreamGroupDataSink* getGroupData(SStreamDataSinkManager* pStreamData, const char* groupId) { return NULL; }
STableDataInfo*       getTableDataInfo(SStreamGroupDataSink* pGroupData, int64_t tableId);
void                  sortTableHash(SStreamGroupDataSink* pGroupData) { return; }

int32_t getFirstDataIter(SStreamGroupDataSink* pGroupData, TSKEY start, TSKEY end, void** ppResult) {
  ppResult = taosMemoryCalloc(1, sizeof(SResultIter));
  void* pIter = taosHashIterate(pGroupData->DataSinkTableList, NULL);
  while (pIter) {
    STableDataInfo** ppIter = pIter;
    ((SResultIter*)ppResult)->tableInfo = *ppIter;
    ((SResultIter*)ppResult)->tableId = (*ppIter)->tableId;
    ((SResultIter*)ppResult)->dataPos = 0;
    ((SResultIter*)ppResult)->blockNum = 0;

    // or in file
  }

  return TSDB_CODE_SUCCESS;
}

static bool memIsEnough() { return false; }
static bool isManagerReady() {
  if (g_pDataSinkManager.DataSinkStreamList != NULL) {
    return true;
  }
  return false;
}

static int32_t createSStreamDataSinkManager(int64_t streamId, int64_t taskId, int32_t cleanMode,
                                            SStreamDataSinkManager** ppStreamDataSink) {
  SStreamDataSinkManager* pStreamDataSink = taosMemoryCalloc(1, sizeof(SStreamDataSinkManager));
  if (pStreamDataSink == NULL) {
    return terrno;
  }
  pStreamDataSink->streamId = streamId;
  pStreamDataSink->taskId = taskId;
  pStreamDataSink->cleanMode = cleanMode;
  pStreamDataSink->pFile = NULL;

  *ppStreamDataSink = pStreamDataSink;
  return TSDB_CODE_SUCCESS;
}

static void destroySStreamDataSinkManager(SStreamDataSinkManager* pStreamDataSink) {
  if (pStreamDataSink->pFile) {
    taosCloseFile(pStreamDataSink->pFile->pFile);
    taosMemoryFreeClear(pStreamDataSink->pFile);
  }
  taosMemoryFreeClear(pStreamDataSink);
}

// @brief 初始化数据缓存
int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int32_t cleanMode, void** ppCache) {
  int32_t code = initStreamDataSinkOnce();
  if (code != 0) {
    return code;
  }

  char key[128] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64, streamId, taskId);
  SStreamDataSinkManager* pStreamDataSink = NULL;
  pStreamDataSink = (SStreamDataSinkManager*)taosHashGet(g_pDataSinkManager.DataSinkStreamList, key, strlen(key));
  if (pStreamDataSink == NULL) {
    code = createSStreamDataSinkManager(streamId, taskId, cleanMode, &pStreamDataSink);
    if (code != 0) {
      code = taosHashPut(g_pDataSinkManager.DataSinkStreamList, key, strlen(key), pStreamDataSink,
                         sizeof(SStreamDataSinkManager));
      if (code != 0) {
        destroySStreamDataSinkManager(pStreamDataSink);
        pStreamDataSink = NULL;
        return code;
      }
    }
  }
  *ppCache = pStreamDataSink;
  return TSDB_CODE_SUCCESS;
}

// @brief 销毁数据缓存
void destroyStreamDataCache(void* pCache) {}

int32_t putStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock,
                           int32_t startIndex, int32_t endIndex) {
  if (isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (!memIsEnough()) {
    return writeToFile((SStreamDataSinkManager*)pCache, groupId, wstart, wend, pBlock, startIndex, endIndex);
  } else {
    // todo
    return TSDB_CODE_SUCCESS;
  }
}

int32_t getStreamDataCache(void* pCache, const char* groupId, TSKEY start, TSKEY end, void** pIter) {
  if (isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  SStreamGroupDataSink* pGroupData = getGroupData((SStreamDataSinkManager*)pCache, groupId);
  sortTableHash(pGroupData);
  return getFirstDataIter(pGroupData, start, end, pIter);
}
// getGroupData
// sort table hash
// for each table:
//   return (pTableIter, getTableWindowData  )

int32_t getNextStreamDataCache(void** pIter, SSDataBlock** ppBlock);
// 读取过程数据会发生变化么？

int32_t destroyDataSinkManager2() {
  // todo: destroy all data sink

  if (g_pDataSinkManager.DataSinkStreamList) {
    taosHashCleanup(g_pDataSinkManager.DataSinkStreamList);
    g_pDataSinkManager.DataSinkStreamList = NULL;
  }
  return TSDB_CODE_SUCCESS;
}
