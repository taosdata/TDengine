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
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"

int32_t writeToCache(SStreamTaskDSManager* pStreamDataSink, int64_t groupId, TSKEY wstart, TSKEY wend,
                     SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex) {
  int32_t code = 0;

  SGroupDSManager*  pGroupDataInfo = NULL;
  SGroupDSManager** ppGroupDataInfo =
      (SGroupDSManager**)taosHashGet(pStreamDataSink->DataSinkGroupList, &groupId, sizeof(groupId));
  if (ppGroupDataInfo == NULL) {
    pGroupDataInfo = (SGroupDSManager*)taosMemoryCalloc(1, sizeof(SGroupDSManager));
    if (pGroupDataInfo == NULL) {
      return terrno;
    }
    pGroupDataInfo->groupId = groupId;
    code = taosHashPut(pStreamDataSink->DataSinkGroupList, &groupId, sizeof(groupId), &pGroupDataInfo,
                       sizeof(SGroupDSManager*));
    if (code != 0) {
      taosMemoryFree(pGroupDataInfo);
      return code;
    }
  } else {
    pGroupDataInfo = *ppGroupDataInfo;
  }

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  size_t dataEncodeBufSize = blockGetEncodeSize(pBlock);
  char*  buf = taosMemoryCalloc(1, dataEncodeBufSize);
  char*  pStart = buf;

  if (pStart == NULL) {
    return terrno;
  }

  int32_t len = blockEncode(pBlock, pStart, dataEncodeBufSize, numOfCols);
  if (len < 0) {
    taosMemoryFree(buf);
    stError("failed to encode data since %s", tstrerror(terrno));
    return terrno;
  }
  SWindowData* pWindowData = (SWindowData*)taosMemoryCalloc(1, sizeof(SWindowData));
  if (pWindowData == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }
  pWindowData->wstart = wstart;
  pWindowData->wend = wend;
  pWindowData->dataLen = dataEncodeBufSize;
  pWindowData->pDataBuf = buf;
  if (pGroupDataInfo->windowDataInMem == NULL) {
    pGroupDataInfo->windowDataInMem = taosArrayInit(0, sizeof(SWindowData*));
    if (pGroupDataInfo->windowDataInMem == NULL) {
      taosMemoryFree(buf);
      return terrno;
    }
  }
  if (taosArrayPush(pGroupDataInfo->windowDataInMem, &pWindowData) == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }
  pGroupDataInfo->usedMemSize += dataEncodeBufSize;
  pGroupDataInfo->lastWstartInMem = wstart;
  return TSDB_CODE_SUCCESS;
}

int32_t readDataFromCache(SResultIter* pResult, SSDataBlock** ppBlock) {
  int32_t          code = 0;
  int32_t          lino = 0;
  SGroupDSManager* pGroupData = pResult->groupData;
  SWindowData**    ppWindowData = (SWindowData**)taosArrayGet(pGroupData->windowDataInMem, pResult->offset);
  if (ppWindowData == NULL || *ppWindowData == NULL) {
    stError("failed to get data from cache, offset:%" PRId64, pResult->offset);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }
  QUERY_CHECK_CODE(code, lino, _end);

  code = blockDecode(pBlock, (*ppWindowData)->pDataBuf, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to get data from cache since %s, lino:%d", tstrerror(code), lino);
    if (pBlock) {
      blockDataDestroy(pBlock);
    }
  } else {
    *ppBlock = pBlock;
  }
  return code;
}
