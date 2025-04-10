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

#include "cmdnodes.h"
#include "tq.h"

#ifndef WINDOWS
#include "curl/curl.h"
#endif

#define STREAM_EVENT_NOTIFY_RETRY_MS         50          // 50 ms
typedef struct SStreamNotifyHandle {
  TdThreadMutex mutex;
#ifndef WINDOWS
  CURL* curl;
#endif
  char* url;
} SStreamNotifyHandle;

struct SStreamNotifyHandleMap {
  TdThreadMutex gMutex;
  SHashObj*     handleMap;
};

static void stopStreamNotifyConn(SStreamNotifyHandle* pHandle) {
#ifndef WINDOWS
  if (pHandle == NULL || pHandle->curl == NULL) {
    return;
  }
  // status code 1000 means normal closure
  size_t   len = 0;
  uint16_t status = htons(1000);
  CURLcode res = curl_ws_send(pHandle->curl, &status, sizeof(status), &len, 0, CURLWS_CLOSE);
  if (res != CURLE_OK) {
    tqWarn("failed to send ws-close msg to %s for %d", pHandle->url ? pHandle->url : "", res);
  }
  // TODO: add wait mechanism for peer connection close response
  curl_easy_cleanup(pHandle->curl);
  pHandle->curl = NULL;
#endif
}

static void destroyStreamNotifyHandle(void* ptr) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamNotifyHandle** ppHandle = ptr;

  if (ppHandle == NULL || *ppHandle == NULL) {
    return;
  }
  code = taosThreadMutexDestroy(&(*ppHandle)->mutex);
  stopStreamNotifyConn(*ppHandle);
  taosMemoryFreeClear((*ppHandle)->url);
  taosMemoryFreeClear(*ppHandle);
}

static void releaseStreamNotifyHandle(SStreamNotifyHandle** ppHandle) {
  if (ppHandle == NULL || *ppHandle == NULL) {
    return;
  }
  (void)taosThreadMutexUnlock(&(*ppHandle)->mutex);
  *ppHandle = NULL;
}

static int32_t acquireStreamNotifyHandle(SStreamNotifyHandleMap* pMap, const char* url,
                                         SStreamNotifyHandle** ppHandle) {
#ifndef WINDOWS
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  bool                  gLocked = false;
  SStreamNotifyHandle** ppFindHandle = NULL;
  SStreamNotifyHandle*  pNewHandle = NULL;
  CURL*                 newCurl = NULL;
  CURLcode              res = CURLE_OK;

  TSDB_CHECK_NULL(pMap, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(url, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(ppHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *ppHandle = NULL;

  code = taosThreadMutexLock(&pMap->gMutex);
  TSDB_CHECK_CODE(code, lino, _end);
  gLocked = true;

  ppFindHandle = taosHashGet(pMap->handleMap, url, strlen(url));
  if (ppFindHandle == NULL) {
    pNewHandle = taosMemoryCalloc(1, sizeof(SStreamNotifyHandle));
    TSDB_CHECK_NULL(pNewHandle, code, lino, _end, terrno);
    code = taosThreadMutexInit(&pNewHandle->mutex, NULL);
    TSDB_CHECK_CODE(code, lino, _end);
    code = taosHashPut(pMap->handleMap, url, strlen(url), &pNewHandle, POINTER_BYTES);
    TSDB_CHECK_CODE(code, lino, _end);
    *ppHandle = pNewHandle;
    pNewHandle = NULL;
  } else {
    *ppHandle = *ppFindHandle;
  }

  code = taosThreadMutexLock(&(*ppHandle)->mutex);
  TSDB_CHECK_CODE(code, lino, _end);

  (void)taosThreadMutexUnlock(&pMap->gMutex);
  gLocked = false;

  if ((*ppHandle)->curl == NULL) {
    newCurl = curl_easy_init();
    TSDB_CHECK_NULL(newCurl, code, lino, _end, TSDB_CODE_FAILED);
    res = curl_easy_setopt(newCurl, CURLOPT_URL, url);
    TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
    res = curl_easy_setopt(newCurl, CURLOPT_SSL_VERIFYPEER, 0L);
    TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
    res = curl_easy_setopt(newCurl, CURLOPT_SSL_VERIFYHOST, 0L);
    TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
    res = curl_easy_setopt(newCurl, CURLOPT_TIMEOUT, 3L);
    TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
    res = curl_easy_setopt(newCurl, CURLOPT_CONNECT_ONLY, 2L);
    TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
    res = curl_easy_perform(newCurl);
    TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
    (*ppHandle)->curl = newCurl;
    newCurl = NULL;
  }

  if ((*ppHandle)->url == NULL) {
    (*ppHandle)->url = taosStrdup(url);
    TSDB_CHECK_NULL((*ppHandle)->url, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at line %d since %d, %s", __func__, lino, res, tstrerror(code));
    if (*ppHandle) {
      releaseStreamNotifyHandle(ppHandle);
    }
    *ppHandle = NULL;
  }
  if (newCurl) {
    curl_easy_cleanup(newCurl);
  }
  if (pNewHandle) {
    destroyStreamNotifyHandle(&pNewHandle);
  }
  if (gLocked) {
    (void)taosThreadMutexUnlock(&pMap->gMutex);
  }
  return code;
#else
  tqError("stream notify events is not supported on windows");
  return TSDB_CODE_NOT_SUPPORTTED_IN_WINDOWS;
#endif
}

int32_t tqInitNotifyHandleMap(SStreamNotifyHandleMap** ppMap) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SStreamNotifyHandleMap* pMap = NULL;

  TSDB_CHECK_NULL(ppMap, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *ppMap = NULL;
  pMap = taosMemoryCalloc(1, sizeof(SStreamNotifyHandleMap));
  TSDB_CHECK_NULL(pMap, code, lino, _end, terrno);
  code = taosThreadMutexInit(&pMap->gMutex, NULL);
  TSDB_CHECK_CODE(code, lino, _end);
  pMap->handleMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  TSDB_CHECK_NULL(pMap->handleMap, code, lino, _end, terrno);
  taosHashSetFreeFp(pMap->handleMap, destroyStreamNotifyHandle);
  *ppMap = pMap;
  pMap = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pMap != NULL) {
    tqDestroyNotifyHandleMap(&pMap);
  }
  return code;
}

void tqDestroyNotifyHandleMap(SStreamNotifyHandleMap** ppMap) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (*ppMap == NULL) {
    return;
  }
  taosHashCleanup((*ppMap)->handleMap);
  code = taosThreadMutexDestroy(&(*ppMap)->gMutex);
  taosMemoryFreeClear((*ppMap));
}

#define JSON_CHECK_ADD_ITEM(obj, str, item) \
  TSDB_CHECK_CONDITION(cJSON_AddItemToObjectCS(obj, str, item), code, lino, _end, TSDB_CODE_OUT_OF_MEMORY)

static int32_t getStreamNotifyEventHeader(const char* streamName, char** pHeader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cJSON*  obj = NULL;
  cJSON*  streams = NULL;
  cJSON*  stream = NULL;
  char    msgId[37];

  TSDB_CHECK_NULL(streamName, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pHeader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pHeader = NULL;

  code = taosGetSystemUUIDLimit36(msgId, sizeof(msgId));
  TSDB_CHECK_CODE(code, lino, _end);

  stream = cJSON_CreateObject();
  TSDB_CHECK_NULL(stream, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(stream, "streamName", cJSON_CreateStringReference(streamName));
  JSON_CHECK_ADD_ITEM(stream, "events", cJSON_CreateArray());

  streams = cJSON_CreateArray();
  TSDB_CHECK_CONDITION(cJSON_AddItemToArray(streams, stream), code, lino, _end, TSDB_CODE_OUT_OF_MEMORY)
  stream = NULL;

  obj = cJSON_CreateObject();
  TSDB_CHECK_NULL(obj, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
  JSON_CHECK_ADD_ITEM(obj, "messageId", cJSON_CreateStringReference(msgId));
  JSON_CHECK_ADD_ITEM(obj, "timestamp", cJSON_CreateNumber(taosGetTimestampMs()));
  JSON_CHECK_ADD_ITEM(obj, "streams", streams);
  streams = NULL;

  *pHeader = cJSON_PrintUnformatted(obj);
  TSDB_CHECK_NULL(*pHeader, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (stream != NULL) {
    cJSON_Delete(stream);
  }
  if (streams != NULL) {
    cJSON_Delete(streams);
  }
  if (obj != NULL) {
    cJSON_Delete(obj);
  }
  return code;
}

static int32_t packupStreamNotifyEvent(const char* streamName, const SArray* pBlocks, char** pMsg,
                                       int32_t* nNotifyEvents, STaskNotifyEventStat* pNotifyEventStat,
                                       int32_t* pBlockIdx) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  int32_t     numOfBlocks = 0;
  int32_t     msgHeaderLen = 0;
  int32_t     msgTailLen = 0;
  int32_t     msgLen = 0;
  char*       msgHeader = NULL;
  const char* msgTail = "]}]}";
  char*       msg = NULL;
  int64_t     startTime = 0;
  int64_t     endTime = 0;
  int32_t     nBlocks = 0;

  TSDB_CHECK_NULL(pMsg, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pNotifyEventStat, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pMsg = NULL;
  numOfBlocks = taosArrayGetSize(pBlocks);
  *nNotifyEvents = 0;

  for (int32_t i = *pBlockIdx; i < numOfBlocks; ++i) {
    SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
    nBlocks++;
    if (pDataBlock == NULL || pDataBlock->info.type != STREAM_NOTIFY_EVENT) {
      continue;
    }

    SColumnInfoData* pEventStrCol = taosArrayGet(pDataBlock->pDataBlock, NOTIFY_EVENT_STR_COLUMN_INDEX);
    for (int32_t j = 0; j < pDataBlock->info.rows; ++j) {
      char* val = colDataGetVarData(pEventStrCol, j);
      msgLen += varDataLen(val) + 1;
    }
    *nNotifyEvents += pDataBlock->info.rows;
    if (msgLen >= tsStreamNotifyMessageSize * 1024) {
      break;
    }
  }

  *pBlockIdx += nBlocks;

  if (msgLen == 0) {
    // skip since no notification events found
    goto _end;
  }

  startTime = taosGetMonoTimestampMs();
  code = getStreamNotifyEventHeader(streamName, &msgHeader);
  TSDB_CHECK_CODE(code, lino, _end);
  msgHeaderLen = strlen(msgHeader);
  msgTailLen = strlen(msgTail);
  msgLen += msgHeaderLen;

  msg = taosMemoryMalloc(msgLen);
  TSDB_CHECK_NULL(msg, code, lino, _end, terrno);
  char* p = msg;
  TAOS_STRNCPY(p, msgHeader, msgHeaderLen);
  p += msgHeaderLen - msgTailLen;

  for (int32_t i = *pBlockIdx - nBlocks; i < *pBlockIdx; ++i) {
    SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
    if (pDataBlock == NULL || pDataBlock->info.type != STREAM_NOTIFY_EVENT) {
      continue;
    }

    SColumnInfoData* pEventStrCol = taosArrayGet(pDataBlock->pDataBlock, NOTIFY_EVENT_STR_COLUMN_INDEX);
    for (int32_t j = 0; j < pDataBlock->info.rows; ++j) {
      char* val = colDataGetVarData(pEventStrCol, j);
      TAOS_STRNCPY(p, varDataVal(val), varDataLen(val));
      p += varDataLen(val);
      *(p++) = ',';
    }
  }

  p -= 1;
  TAOS_STRNCPY(p, msgTail, msgTailLen);
  *(p + msgTailLen) = '\0';

  *pMsg = msg;
  msg = NULL;

  endTime = taosGetMonoTimestampMs();
  pNotifyEventStat->notifyEventPackTimes++;
  pNotifyEventStat->notifyEventPackElems += *nNotifyEvents;
  pNotifyEventStat->notifyEventPackCostSec += (endTime - startTime) / 1000.0;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (msgHeader != NULL) {
    cJSON_free(msgHeader);
  }
  if (msg != NULL) {
    taosMemoryFreeClear(msg);
  }
  return code;
}

static int32_t sendSingleStreamNotify(SStreamNotifyHandle* pHandle, char* msg) {
#ifndef WINDOWS
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  CURLcode res = CURLE_OK;
  uint64_t sentLen = 0;
  uint64_t totalLen = 0;
  size_t   nbytes = 0;

  TSDB_CHECK_NULL(pHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pHandle->curl, code, lino, _end, TSDB_CODE_INVALID_PARA);

  totalLen = strlen(msg);
  if (totalLen > 0) {
    // send PING frame to check if the connection is still alive
    res = curl_ws_send(pHandle->curl, "", 0, (size_t*)&sentLen, 0, CURLWS_PING);
    TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  }
  sentLen = 0;
  while (sentLen < totalLen) {
    size_t chunkSize = TMIN(totalLen - sentLen, tsStreamNotifyFrameSize * 1024);
    if (sentLen == 0) {
      res = curl_ws_send(pHandle->curl, msg, chunkSize, &nbytes, totalLen, CURLWS_TEXT | CURLWS_OFFSET);
      TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
    } else {
      res = curl_ws_send(pHandle->curl, msg + sentLen, chunkSize, &nbytes, 0, CURLWS_TEXT | CURLWS_OFFSET);
      TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
    }
    sentLen += nbytes;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at line %d since %d, %s", __func__, lino, res, tstrerror(code));
    stopStreamNotifyConn(pHandle);
  }
  return code;
#else
  tqError("stream notify events is not supported on windows");
  return TSDB_CODE_NOT_SUPPORTTED_IN_WINDOWS;
#endif
}

int32_t tqSendAllNotifyEvents(const SArray* pBlocks, SStreamTask* pTask, SVnode* pVnode) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  char*                msg = NULL;
  int32_t              nNotifyAddr = 0;
  int32_t              nNotifyEvents = 0;
  SStreamNotifyHandle* pHandle = NULL;
  int64_t              startTime = 0;
  int64_t              endTime = 0;
  int32_t              blockIdx = 0;

  TSDB_CHECK_NULL(pTask, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pVnode, code, lino, _end, TSDB_CODE_INVALID_PARA);

  nNotifyAddr = taosArrayGetSize(pTask->notifyInfo.pNotifyAddrUrls);
  if (nNotifyAddr == 0) {
    goto _end;
  }

  while (blockIdx < taosArrayGetSize(pBlocks)) {
    code = packupStreamNotifyEvent(pTask->notifyInfo.streamName, pBlocks, &msg, &nNotifyEvents, &pTask->notifyEventStat,
                                   &blockIdx);
    TSDB_CHECK_CODE(code, lino, _end);
    if (msg == NULL) {
      continue;
    }

    tqDebug("stream task %s prepare to send %d notify events, total msg length: %" PRIu64, pTask->notifyInfo.streamName,
            nNotifyEvents, (uint64_t)strlen(msg));

    startTime = taosGetMonoTimestampMs();
    for (int32_t i = 0; i < nNotifyAddr; ++i) {
      if (streamTaskShouldStop(pTask)) {
        break;
      }
      const char* url = taosArrayGetP(pTask->notifyInfo.pNotifyAddrUrls, i);
      code = acquireStreamNotifyHandle(pVnode->pNotifyHandleMap, url, &pHandle);
      if (code != TSDB_CODE_SUCCESS) {
        tqError("failed to get stream notify handle of %s", url);
        if (pTask->notifyInfo.notifyErrorHandle == SNOTIFY_ERROR_HANDLE_PAUSE) {
          // retry for event message sending in PAUSE error handling mode
          taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
          --i;
          continue;
        } else {
          // simply ignore the failure in DROP error handling mode
          code = TSDB_CODE_SUCCESS;
          continue;
        }
      }
      code = sendSingleStreamNotify(pHandle, msg);
      if (code != TSDB_CODE_SUCCESS) {
        tqError("failed to send stream notify handle to %s since %s", url, tstrerror(code));
        if (pTask->notifyInfo.notifyErrorHandle == SNOTIFY_ERROR_HANDLE_PAUSE) {
          // retry for event message sending in PAUSE error handling mode
          taosMsleep(STREAM_EVENT_NOTIFY_RETRY_MS);
          --i;
        } else {
          // simply ignore the failure in DROP error handling mode
          code = TSDB_CODE_SUCCESS;
        }
      } else {
        tqDebug("stream task %s send %d notify events to %s successfully", pTask->notifyInfo.streamName, nNotifyEvents,
                url);
      }
      releaseStreamNotifyHandle(&pHandle);
    }

    endTime = taosGetMonoTimestampMs();
    pTask->notifyEventStat.notifyEventSendTimes++;
    pTask->notifyEventStat.notifyEventSendElems += nNotifyEvents;
    pTask->notifyEventStat.notifyEventSendCostSec += (endTime - startTime) / 1000.0;

    taosMemoryFreeClear(msg);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (msg) {
    taosMemoryFreeClear(msg);
  }
  return code;
}
