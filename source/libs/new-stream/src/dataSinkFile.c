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
#include "osTime.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"

char    gDataSinkFilePath[PATH_MAX] = {0};
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

static int32_t createStreamDataSinkFile(SStreamTaskDSManager* pStreamDataSink) {
  if (pStreamDataSink->pFile == NULL) {
    pStreamDataSink->pFile = (DataSinkFileState*)taosMemoryCalloc(1, sizeof(DataSinkFileState));
    if (pStreamDataSink->pFile == NULL) {
      return terrno;
    }
    int32_t now = taosGetTimestampSec();
    snprintf(pStreamDataSink->pFile->fileName, FILENAME_MAX, "%s//%s_%d_%" PRId64, gDataSinkFilePath, "stream", now,
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
static void destroyStreamDataSinkFile(SStreamTaskDSManager* pStreamDataSink) {
  if (pStreamDataSink->pFile) {
    taosCloseFile(pStreamDataSink->pFile->pFile);
    taosMemoryFreeClear(pStreamDataSink->pFile);
  }
}

static int32_t initStreamDataSinkFile(SStreamTaskDSManager* pStreamDataSink) {
  if (pStreamDataSink->pFile == NULL) {
    return createStreamDataSinkFile(pStreamDataSink);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t writeToFile(SStreamTaskDSManager* pStreamDataSink, SGroupDSManager* pGroupDataInfoMgr, TSKEY wstart, TSKEY wend,
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
  char*   buf = taosMemoryCalloc(1, size);
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
