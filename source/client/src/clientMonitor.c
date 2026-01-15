#include "clientMonitor.h"
#include "cJSON.h"
#include "clientInt.h"
#include "clientLog.h"
#include "query.h"
#include "taos.h"
#include "taoserror.h"
#include "tarray.h"
#include "tglobal.h"
#include "thash.h"
#include "tmisce.h"
#include "tqueue.h"
#include "ttime.h"
#include "ttimer.h"

SRWLatch    monitorLock;
SRWLatch    monitorQueueLock;
void*       monitorTimer;
SHashObj*   monitorCounterHash;
int32_t     monitorFlag = 0;
int64_t     quitTime = 0;
int32_t     quitCnt = 0;
tsem2_t     monitorSem;
STaosQueue* monitorQueue;
SHashObj*   monitorSlowLogHash;
SHashObj*   monitorSlowLogHashPath;
char        tmpSlowLogPath[PATH_MAX] = {0};
TdThread    monitorThread;
extern bool tsEnableAuditDelete;

static int32_t getSlowLogTmpDir(char* tmpPath, int32_t size) {
  int ret = tsnprintf(tmpPath, size, "%stdengine_slow_log/", tsTempDir);
  if (ret < 0) {
    tscError("failed to get tmp path ret:%d", ret);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  return 0;
}

static void destroySlowLogClient(void* data) {
  if (data == NULL) {
    return;
  }
  SlowLogClient* slowLogClient = *(SlowLogClient**)data;
  if (taosCloseFile(&slowLogClient->pFile) != 0) {
    tscError("%s monitor failed to close file:%s, terrno:%d", __func__, slowLogClient->path, terrno);
  }
  slowLogClient->pFile = NULL;

  taosMemoryFree(slowLogClient);
}

static void destroyMonitorClient(void* data) {
  if (data == NULL) {
    return;
  }
  MonitorClient* pMonitor = *(MonitorClient**)data;
  if (pMonitor == NULL) {
    return;
  }
  if (!taosTmrStopA(&pMonitor->timer)) {
    tscError("failed to stop timer, pMonitor:%p", pMonitor);
  }
  taosHashCleanup(pMonitor->counters);
  int ret = taos_collector_registry_destroy(pMonitor->registry);
  if (ret) {
    tscError("failed to destroy registry, pMonitor:%p ret:%d", pMonitor, ret);
  }
  taosMemoryFree(pMonitor);
}

static void monitorFreeSlowLogData(void* paras) {
  MonitorSlowLogData* pData = (MonitorSlowLogData*)paras;
  if (pData == NULL) {
    return;
  }
  taosMemoryFreeClear(pData->data);
}

static void monitorFreeSlowLogDataEx(void* paras) {
  monitorFreeSlowLogData(paras);
  taosMemoryFree(paras);
}

static SAppInstInfo* getAppInstByClusterId(int64_t clusterId) {
  void* p = taosHashGet(appInfo.pInstMapByClusterId, &clusterId, LONG_BYTES);
  if (p == NULL) {
    tscError("failed to get app inst, clusterId:0x%" PRIx64, clusterId);
    return NULL;
  }
  return *(SAppInstInfo**)p;
}

static int32_t monitorReportAsyncCB(void* param, SDataBuf* pMsg, int32_t code) {
  if (TSDB_CODE_SUCCESS != code) {
    tscError("found error in monitorReport send callback, code:%d, please check the network.", code);
  }
  if (pMsg) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
  }
  if (param != NULL) {
    MonitorSlowLogData* p = (MonitorSlowLogData*)param;
    if (code != 0) {
      tscError("failed to send slow log:%s, clusterId:0x%" PRIx64, p->data, p->clusterId);
    }
    MonitorSlowLogData tmp = *p;
    tmp.data = NULL;
    (void)monitorPutData2MonitorQueue(tmp);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t sendReport(void* pTransporter, SEpSet* epSet, char* pCont, MONITOR_TYPE type, void* param) {
  int32_t    code = 0;
  void*      buf = NULL;
  SStatisReq sStatisReq;
  sStatisReq.pCont = pCont;
  sStatisReq.contLen = strlen(pCont);
  sStatisReq.type = type;

  int tlen = tSerializeSStatisReq(NULL, 0, &sStatisReq);
  if (tlen < 0) {
    code = terrno;
    goto FAILED;
  }
  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    tscError("sendReport failed, out of memory, len:%d", tlen);
    code = terrno;
    goto FAILED;
  }
  tlen = tSerializeSStatisReq(buf, tlen, &sStatisReq);
  if (tlen < 0) {
    code = terrno;
    goto FAILED;
  }

  SMsgSendInfo* pInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pInfo == NULL) {
    tscError("sendReport failed, out of memory send info");
    code = terrno;
    goto FAILED;
  }
  pInfo->fp = monitorReportAsyncCB;
  pInfo->msgInfo.pData = buf;
  pInfo->msgInfo.len = tlen;
  pInfo->msgType = TDMT_MND_STATIS;
  pInfo->param = param;
  pInfo->paramFreeFp = monitorFreeSlowLogDataEx;
  pInfo->requestId = tGenIdPI64();
  pInfo->requestObjRefId = 0;

  // int64_t transporterId = 0;
  return asyncSendMsgToServer(pTransporter, epSet, NULL, pInfo);

FAILED:
  taosMemoryFree(buf);
  monitorFreeSlowLogDataEx(param);
  return code;
}

static void generateClusterReport(taos_collector_registry_t* registry, void* pTransporter, SEpSet* epSet) {
  char ts[50] = {0};
  (void)snprintf(ts, sizeof(ts), "%" PRId64, taosGetTimestamp(TSDB_TIME_PRECISION_MILLI));
  char* pCont = (char*)taos_collector_registry_bridge_new(registry, ts, "%" PRId64, NULL);
  if (NULL == pCont) {
    tscError("generateClusterReport failed, get null content. since %s", tstrerror(terrno));
    return;
  }

  if (strlen(pCont) != 0 && sendReport(pTransporter, epSet, pCont, MONITOR_TYPE_COUNTER, NULL) == 0) {
    int ret = taos_collector_registry_clear_batch(registry);
    if (ret) {
      tscError("failed to clear registry, ret:%d", ret);
    }
  }
  taosMemoryFreeClear(pCont);
}

static void reportSendProcess(void* param, void* tmrId) {
  taosRLockLatch(&monitorLock);
  if (atomic_load_32(&monitorFlag) == 1) {
    taosRUnLockLatch(&monitorLock);
    return;
  }

  MonitorClient* pMonitor = (MonitorClient*)param;
  SAppInstInfo*  pInst = getAppInstByClusterId(pMonitor->clusterId);
  if (pInst == NULL) {
    taosRUnLockLatch(&monitorLock);
    return;
  }

  SEpSet ep = getEpSet_s(&pInst->mgmtEp);
  generateClusterReport(pMonitor->registry, pInst->pTransporter, &ep);
  bool reset = taosTmrReset(reportSendProcess, pInst->serverCfg.monitorParas.tsMonitorInterval * 1000, param,
                            monitorTimer, &tmrId);
  tscDebug("reset timer, pMonitor:%p, %d", pMonitor, reset);
  taosRUnLockLatch(&monitorLock);
}

static void sendAllCounter() {
  MonitorClient** ppMonitor = NULL;
  while ((ppMonitor = taosHashIterate(monitorCounterHash, ppMonitor))) {
    MonitorClient* pMonitor = *ppMonitor;
    if (pMonitor == NULL) {
      continue;
    }
    SAppInstInfo* pInst = getAppInstByClusterId(pMonitor->clusterId);
    if (pInst == NULL) {
      taosHashCancelIterate(monitorCounterHash, ppMonitor);
      break;
    }
    SEpSet ep = getEpSet_s(&pInst->mgmtEp);
    generateClusterReport(pMonitor->registry, pInst->pTransporter, &ep);
  }
}

void monitorCreateClient(int64_t clusterId) {
  MonitorClient* pMonitor = NULL;
  taosWLockLatch(&monitorLock);
  if (taosHashGet(monitorCounterHash, &clusterId, LONG_BYTES) == NULL) {
    tscInfo("clusterId:0x%" PRIx64 ", create monitor", clusterId);
    pMonitor = taosMemoryCalloc(1, sizeof(MonitorClient));
    if (pMonitor == NULL) {
      tscError("failed to create monitor client");
      goto fail;
    }
    pMonitor->clusterId = clusterId;
    char clusterKey[32] = {0};
    if (snprintf(clusterKey, sizeof(clusterKey), "%" PRId64, clusterId) < 0) {
      tscError("failed to create cluster key");
      goto fail;
    }
    pMonitor->registry = taos_collector_registry_new(clusterKey);
    if (pMonitor->registry == NULL) {
      tscError("failed to create registry");
      goto fail;
    }
    pMonitor->colector = taos_collector_new(clusterKey);
    if (pMonitor->colector == NULL) {
      tscError("failed to create collector");
      goto fail;
    }

    int r = taos_collector_registry_register_collector(pMonitor->registry, pMonitor->colector);
    if (r) {
      tscError("failed to register collector, ret:%d", r);
      goto fail;
    }
    pMonitor->counters =
        (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (pMonitor->counters == NULL) {
      tscError("failed to create monitor counters");
      goto fail;
    }

    if (taosHashPut(monitorCounterHash, &clusterId, LONG_BYTES, &pMonitor, POINTER_BYTES) != 0) {
      tscError("failed to put monitor client to hash");
      goto fail;
    }

    SAppInstInfo* pInst = getAppInstByClusterId(clusterId);
    if (pInst == NULL) {
      tscError("failed to get app instance by cluster id");
      pMonitor = NULL;
      goto fail;
    }
    pMonitor->timer = taosTmrStart(reportSendProcess, pInst->serverCfg.monitorParas.tsMonitorInterval * 1000,
                                   (void*)pMonitor, monitorTimer);
    if (pMonitor->timer == NULL) {
      tscError("failed to start timer");
      goto fail;
    }
    tscInfo("clusterId:0x%" PRIx64 ", create monitor finished, monitor:%p", clusterId, pMonitor);
  }
  taosWUnLockLatch(&monitorLock);

  return;

fail:
  destroyMonitorClient(&pMonitor);
  taosWUnLockLatch(&monitorLock);
}

void monitorCreateClientCounter(int64_t clusterId, const char* name, const char* help, size_t label_key_count,
                                const char** label_keys) {
  taosWLockLatch(&monitorLock);
  MonitorClient** ppMonitor = (MonitorClient**)taosHashGet(monitorCounterHash, &clusterId, LONG_BYTES);
  if (ppMonitor == NULL || *ppMonitor == NULL) {
    tscError("failed to get monitor client");
    goto end;
  }
  taos_counter_t* newCounter = taos_counter_new(name, help, label_key_count, label_keys);
  if (newCounter == NULL) return;
  MonitorClient* pMonitor = *ppMonitor;
  if (taos_collector_add_metric(pMonitor->colector, newCounter) != 0) {
    tscError("failed to add metric to collector");
    int r = taos_counter_destroy(newCounter);
    if (r) {
      tscError("failed to destroy counter, code:%d", r);
    }
    goto end;
  }
  if (taosHashPut(pMonitor->counters, name, strlen(name), &newCounter, POINTER_BYTES) != 0) {
    tscError("failed to put counter to monitor");
    int r = taos_counter_destroy(newCounter);
    if (r) {
      tscError("failed to destroy counter, code:%d", r);
    }
    goto end;
  }
  tscInfo("clusterId:0x%" PRIx64 ", monitor:%p, create counter:%s %p", pMonitor->clusterId, pMonitor, name, newCounter);

end:
  taosWUnLockLatch(&monitorLock);
}

void monitorCounterInc(int64_t clusterId, const char* counterName, const char** label_values) {
  taosWLockLatch(&monitorLock);
  if (atomic_load_32(&monitorFlag) == 1) {
    taosWUnLockLatch(&monitorLock);
    return;
  }

  MonitorClient** ppMonitor = (MonitorClient**)taosHashGet(monitorCounterHash, &clusterId, LONG_BYTES);
  if (ppMonitor == NULL || *ppMonitor == NULL) {
    tscError("clusterId:0x%" PRIx64 ", monitor not found", clusterId);
    goto end;
  }

  MonitorClient*   pMonitor = *ppMonitor;
  taos_counter_t** ppCounter = (taos_counter_t**)taosHashGet(pMonitor->counters, counterName, strlen(counterName));
  if (ppCounter == NULL || *ppCounter == NULL) {
    tscError("clusterId:0x%" PRIx64 ", monitor:%p counter:%s not found", clusterId, pMonitor, counterName);
    goto end;
  }
  if (taos_counter_inc(*ppCounter, label_values) != 0) {
    tscError("clusterId:0x%" PRIx64 ", monitor:%p counter:%s inc failed", clusterId, pMonitor, counterName);
    goto end;
  }
  tscTrace("clusterId:0x%" PRIx64 ", monitor:%p, counter:%s inc", pMonitor->clusterId, pMonitor, counterName);

end:
  taosWUnLockLatch(&monitorLock);
}

const char* monitorResultStr(SQL_RESULT_CODE code) {
  static const char* result_state[] = {"Success", "Failed", "Cancel"};
  return result_state[code];
}

static void monitorWriteSlowLog2File(MonitorSlowLogData* slowLogData, char* tmpPath) {
  TdFilePtr pFile = NULL;
  SlowLogClient* pClient = NULL;
  void*     tmp = taosHashGet(monitorSlowLogHash, &slowLogData->clusterId, LONG_BYTES);
  if (tmp == NULL) {
    char path[PATH_MAX] = {0};
    char clusterId[32] = {0};
    if (snprintf(clusterId, sizeof(clusterId), "%" PRIx64"-%"PRId64, slowLogData->clusterId, taosGetTimestampNs()) < 0) {
      tscError("failed to generate clusterId:0x%" PRIx64, slowLogData->clusterId);
      return;
    }
    taosGetTmpfilePath(tmpPath, clusterId, path);
    tscInfo("monitor create slow log file:%s", path);
    pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
    if (pFile == NULL) {
      tscError("failed to open file:%s since %d", path, terrno);
      return;
    }

    pClient = taosMemoryCalloc(1, sizeof(SlowLogClient));
    if (pClient == NULL) {
      tscError("failed to allocate memory for slow log client");
      int32_t ret = taosCloseFile(&pFile);
      if (ret != 0) {
        tscError("failed to close file:%p ret:%d", pFile, ret);
      }
      return;
    }
    tstrncpy(pClient->path, path, PATH_MAX);
    pClient->sendOffset = 0;
    pClient->pFile = pFile;
    pClient->clusterId = slowLogData->clusterId;
    pClient->type = SLOW_LOG_READ_RUNNING;
    if (taosHashPut(monitorSlowLogHash, &pClient->clusterId, LONG_BYTES, &pClient, POINTER_BYTES) != 0) {
      tscError("failed to put clusterId:0x%" PRIx64 " to hash table", pClient->clusterId);
      int32_t ret = taosCloseFile(&pFile);
      if (ret != 0) {
        tscError("failed to close file:%p ret:%d", pFile, ret);
      }
      taosMemoryFree(pClient);
      return;
    }

    if (taosLockFile(pFile) < 0) {
      tscError("failed to lock file:%p since %s", pFile, terrstr());
      return;
    }
  } else {
    pFile = (*(SlowLogClient**)tmp)->pFile;
    pClient = *(SlowLogClient**)tmp;
  }

  if (taosLSeekFile(pFile, 0, SEEK_END) < 0) {
    tscError("failed to seek file:%p code:%d", pFile, terrno);
    return;
  }
  if (taosWriteFile(pFile, slowLogData->data, strlen(slowLogData->data) + 1) < 0) {
    tscError("failed to write len to file:%p since %s", pFile, terrstr());
  }
  tscDebug("monitor write slow log to file:%s, clusterId:0x%" PRIx64 ", data:%s", pClient->path, pClient->clusterId, slowLogData->data);
}

static char* readFile(SlowLogClient* pClient) {
  tscDebug("monitor readFile slow begin file:%s, offset:%" PRId64 ", size:%" PRId64, pClient->path,
           pClient->sendOffset, pClient->size);
  if (taosLSeekFile(pClient->pFile, pClient->sendOffset, SEEK_SET) < 0) {
    tscError("failed to seek file:%p code:%d", pClient->path, terrno);
    return NULL;
  }

  if ((pClient->size <= pClient->sendOffset)) {
    tscError("invalid size:%" PRId64 ", offset:%" PRId64, pClient->size, pClient->sendOffset);
    terrno = TSDB_CODE_TSC_INTERNAL_ERROR;
    return NULL;
  }
  char*   pCont = NULL;
  int64_t totalSize = 0;
  if (pClient->size - pClient->sendOffset >= SLOW_LOG_SEND_SIZE_MAX) {
    totalSize = 4 + SLOW_LOG_SEND_SIZE_MAX;
  } else {
    totalSize = 4 + (pClient->size - pClient->sendOffset);
  }

  pCont = taosMemoryCalloc(1, totalSize);  // 4 reserved for []
  if (pCont == NULL) {
    tscError("failed to allocate memory for slow log, size:%" PRId64, totalSize);
    return NULL;
  }
  char* buf = pCont;
  (void)strncat(buf++, "[", totalSize - 1);
  int64_t readSize = taosReadFile(pClient->pFile, buf, totalSize - 4);  // 4 reserved for []
  if (readSize <= 0) {
    if (readSize < 0) {
      tscError("failed to read len from file:%s since %s", pClient->path, terrstr());
    }
    taosMemoryFree(pCont);
    return NULL;
  }

  totalSize = 0;
  while (1) {
    size_t len = strlen(buf);
    if (len == SLOW_LOG_SEND_SIZE_MAX) {  // one item is too long
      pClient->sendOffset = pClient->size;
      *buf = ']';
      *(buf + 1) = '\0';
      break;
    }

    totalSize += (len + 1);
    if (totalSize > readSize) {
      *(buf - 1) = ']';
      *buf = '\0';
      break;
    }

    if (len == 0) {             // one item is empty
      if (*(buf - 1) == '[') {  // data is "\0"
        // no data read
        *buf = ']';
        *(buf + 1) = '\0';
      } else {  // data is "ass\0\0"
        *(buf - 1) = ']';
        *buf = '\0';
      }
      pClient->sendOffset += 1;
      break;
    }
    buf[len] = ',';  // replace '\0' with ','
    buf += (len + 1);
    pClient->sendOffset += (len + 1);
  }

  tscDebugL("monitor readFile slow log end, data:%s, offset:%" PRId64, pCont, pClient->sendOffset);
  return pCont;
}

static void processFileRemoved(SlowLogClient* pClient) {
  int32_t ret = taosCloseFile(&(pClient->pFile));
  if (ret != 0) {
    tscError("%s failed to close file:%s ret:%d", __func__, pClient->path, ret);
  }
  pClient->pFile = NULL;

  TdFilePtr pFile =
      taosOpenFile(pClient->path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
  if (pFile == NULL) {
    tscError("%s failed to open file:%s since %d", __func__, pClient->path, terrno);
  } else {
    pClient->pFile = pFile;
  }
  pClient->size = 0;
}

static int32_t processFile(SlowLogClient* pClient) {
  int32_t code = 0;
  code = taosStatFile(pClient->path, &pClient->size, NULL, NULL);
  if (code < 0 && ERRNO == ENOENT) {
    processFileRemoved(pClient);
    tscError("%s monitor rebuild file:%s because of file not exist", __func__, pClient->path);
    code = 0;
  }
  return code;
}

static int32_t sendSlowLog(int64_t clusterId, char* data, SLOW_LOG_QUEUE_TYPE type, char* fileName, void* pTransporter,
                           SEpSet* epSet) {
  if (data == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  MonitorSlowLogData* pParam = taosMemoryCalloc(1, sizeof(MonitorSlowLogData));
  if (pParam == NULL) {
    taosMemoryFree(data);
    return terrno;
  }
  pParam->data = data;
  pParam->type = type;
  pParam->fileName = fileName;
  pParam->clusterId = clusterId;
  return sendReport(pTransporter, epSet, data, MONITOR_TYPE_SLOW_LOG, pParam);
}

static int32_t monitorReadSend(SlowLogClient* pClient) {
  SAppInstInfo* pInst = getAppInstByClusterId(pClient->clusterId);
  if (pInst == NULL) {
    tscError("%s failed to get app instance by clusterId:0x%" PRIx64, __func__, pClient->clusterId);
    return terrno;
  }
  SEpSet ep = getEpSet_s(&pInst->mgmtEp);
  char*  data = readFile(pClient);
  if (data == NULL) return terrno;
  int32_t code = sendSlowLog(pClient->clusterId, data, pClient->type, pClient->path, pInst->pTransporter, &ep);
  if (code != 0) {
    pClient->sendOffset = 0;  // set offset = 0 to send data at beginning if send data failed
    tscError("monitor send slow log failed, clusterId:0x%" PRIx64 ", ret:%d", pClient->clusterId, code);
  }
  return code;
}

static void sendOneClient(SlowLogClient* pClient) {
  int32_t code = processFile(pClient);
  if (code < 0) {
    tscError("failed to get file size for file:%s, code:%d, errno:%d", pClient->path, code, errno);
    return;
  }
  if (pClient->size <= pClient->sendOffset) {
    if (pClient->type == SLOW_LOG_READ_RUNNING && pClient->size > 0) {
      if (taosFtruncateFile(pClient->pFile, 0) < 0) {
        tscError("failed to truncate file:%s code:%d", pClient->path, terrno);
      }
      tscDebug("monitor truncate file to 0 file:%s", pClient->path);
    } else if (pClient->type == SLOW_LOG_READ_BEGINNIG || pClient->type == SLOW_LOG_READ_QUIT) {
      if (taosCloseFile(&pClient->pFile) != 0) {
        tscError("failed to close file:%s ret:%d", pClient->path, terrno);
      }
      pClient->pFile = NULL;
      if (taosRemoveFile(pClient->path) != 0) {
        tscError("failed to remove file:%s, terrno:%d", pClient->path, terrno);
      }
      tscInfo("monitor remove file:%s when send data out at beginning", pClient->path);
    }

    pClient->sendOffset = 0;
    if (pClient->type == SLOW_LOG_READ_QUIT) {
      pClient->closed = true;
      quitCnt++;
    }
  } else {
    if (pClient->closed) {
      tscWarn("%s client is closed, skip send slow log, file:%s", __func__, pClient->path);
      return;
    }
    code = monitorReadSend(pClient);
    tscDebug("%s monitor send slow log, file:%s code:%d", __func__, pClient->path, code);
  }
}

static void monitorSendSlowLogAtRunningCb(int64_t clusterId) {
  int32_t code = 0;
  void*   tmp = taosHashGet(monitorSlowLogHash, &clusterId, LONG_BYTES);
  if (tmp == NULL) {
    tscError("failed to get slow log client by clusterId:0x%" PRIx64, clusterId);
    return;
  }
  SlowLogClient* pClient = (*(SlowLogClient**)tmp);
  if (pClient == NULL) {
    tscError("failed to get slow log client by clusterId:0x%" PRIx64, clusterId);
    return;
  }
  sendOneClient(pClient);
}

static void monitorSendSlowLogAtBeginningCb(const char* fileName) {
  int32_t code = 0;
  void*   tmp = taosHashGet(monitorSlowLogHashPath, fileName, strlen(fileName));
  if (tmp == NULL) {
    tscError("failed to get slow log client by fileName:%s", fileName);
    return;
  }
  SlowLogClient* pClient = (*(SlowLogClient**)tmp);
  if (pClient == NULL) {
    tscError("failed to get slow log client by fileName:%s", fileName);
    return;
  }
  sendOneClient(pClient);
}

static void monitorSendAllSlowLog() {
  int32_t code = 0;
  int64_t t = taosGetMonoTimestampMs();
  void*   pIter = NULL;
  while ((pIter = taosHashIterate(monitorSlowLogHash, pIter))) {
    int64_t*       clusterId = (int64_t*)taosHashGetKey(pIter, NULL);
    SAppInstInfo*  pInst = getAppInstByClusterId(*clusterId);
    SlowLogClient* pClient = (*(SlowLogClient**)pIter);
    if (pClient == NULL || pInst == NULL) {
      taosHashCancelIterate(monitorSlowLogHash, pIter);
      return;
    }
    if (t - pClient->lastSendTime > pInst->serverCfg.monitorParas.tsMonitorInterval * 1000 ||
        atomic_load_32(&monitorFlag) == 1) {
      pClient->lastSendTime = t;
    } else {
      continue;
    }

    if (atomic_load_32(&monitorFlag) == 1) {  // change type to quit
      pClient->type = SLOW_LOG_READ_QUIT;
    }
    tscDebug("monitor send slow log for clusterId:0x%" PRIx64 ", file:%s, type:%d", *clusterId, pClient->path, pClient->type);
    if (pClient->sendOffset > 0) {  // already in sending process
      continue;
    }
    sendOneClient(pClient);
  }
}

static void monitorSendAllSlowLogFromTempDir(int64_t clusterId) {
  SAppInstInfo* pInst = getAppInstByClusterId((int64_t)clusterId);

  if (pInst == NULL || !pInst->serverCfg.monitorParas.tsEnableMonitor) {
    tscInfo("monitor is disabled, skip send slow log");
    return;
  }
  char namePrefix[PATH_MAX] = {0};
  if (snprintf(namePrefix, sizeof(namePrefix), "%s%" PRIx64, TD_TMP_FILE_PREFIX, clusterId) < 0) {
    tscError("failed to generate slow log file name prefix");
    return;
  }

  char tmpPath[PATH_MAX] = {0};
  if (getSlowLogTmpDir(tmpPath, sizeof(tmpPath)) < 0) {
    return;
  }

  TdDirPtr pDir = taosOpenDir(tmpPath);
  if (pDir == NULL) {
    return;
  }

  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir)) != NULL) {
    if (taosDirEntryIsDir(de)) {
      continue;
    }

    char* name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || strstr(name, namePrefix) == NULL) {
      tscInfo("skip file:%s, for cluster id:%" PRIx64, name, clusterId);
      continue;
    }

    char filename[PATH_MAX] = {0};
    (void)snprintf(filename, sizeof(filename), "%s%s", tmpPath, name);
    int64_t fileSize = 0;
    if (taosStatFile(filename, &fileSize, NULL, NULL) < 0) {
      tscError("failed to get file:%s status, since %s", filename, terrstr());
      continue;
    }
    if (fileSize == 0) {
      if (taosRemoveFile(filename) != 0) {
        tscError("failed to remove file:%s, terrno:%d", filename, terrno);
      }
      continue;
    }

    tscInfo("%s monitor opening file:%s at beginning", __func__, filename);
    TdFilePtr pFile = taosOpenFile(filename, TD_FILE_READ | TD_FILE_WRITE);
    if (pFile == NULL) {
      tscError("failed to open file:%s since %s", filename, terrstr());
      continue;
    }
    if (taosLockFile(pFile) < 0) {
      tscInfo("failed to lock file:%s since %s, maybe used by other process", filename, terrstr());
      int32_t ret = taosCloseFile(&pFile);
      if (ret != 0) {
        tscError("failed to close file:%p ret:%d", pFile, ret);
      }
      continue;
    }

    SlowLogClient* pClient = taosMemoryCalloc(1, sizeof(SlowLogClient));
    if (pClient == NULL) {
      tscError("failed to allocate memory for slow log client");
      int32_t ret = taosCloseFile(&pFile);
      if (ret != 0) {
        tscError("failed to close file:%p ret:%d", pFile, ret);
      }
      return;
    }
    tstrncpy(pClient->path, filename, PATH_MAX);
    pClient->sendOffset = 0;
    pClient->pFile = pFile;
    pClient->clusterId = clusterId;
    pClient->type = SLOW_LOG_READ_BEGINNIG;
    if (taosHashPut(monitorSlowLogHashPath, filename, strlen(filename), &pClient, POINTER_BYTES) != 0) {
      tscError("failed to put clusterId:0x%" PRIx64 " to hash table", pClient->clusterId);
      int32_t ret = taosCloseFile(&pFile);
      if (ret != 0) {
        tscError("failed to close file:%s ret:%d", filename, ret);
      }
      taosMemoryFree(pClient);
      return;
    }
    sendOneClient(pClient);
  }

  int32_t ret = taosCloseDir(&pDir);
  if (ret != 0) {
    tscError("failed to close dir, ret:%d", ret);
  }
}

static void* monitorThreadFunc(void* param) {
  setThreadName("client-monitor-slowlog");
  tscInfo("monitor update thread started");
  while (1) {
    if (atomic_load_32(&monitorFlag) == 1) {
      if (taosGetMonoTimestampMs() - quitTime > 1000 ||
          quitCnt == taosHashGetSize(monitorSlowLogHash)) {  // quit at most 1000ms or no data need to send
        tscInfo("monitorThreadFunc quit since timeout or quitcnt:%d", quitCnt);
        break;
      }
    }

    (void)tsem2_timewait(&monitorSem, 100);
    monitorSendAllSlowLog();

    MonitorSlowLogData* slowLogData = NULL;
    taosReadQitem(monitorQueue, (void**)&slowLogData);
    if (slowLogData == NULL) {
      continue;
    }

    if (slowLogData->type == SLOW_LOG_READ_ALL) {
      monitorSendAllSlowLogFromTempDir(slowLogData->clusterId);
    } else if (slowLogData->type == SLOW_LOG_READ_BEGINNIG) {
      monitorSendSlowLogAtBeginningCb(slowLogData->fileName);
    } else if (slowLogData->type == SLOW_LOG_WRITE) {
      monitorWriteSlowLog2File(slowLogData, tmpSlowLogPath);
    } else if (slowLogData->type == SLOW_LOG_READ_RUNNING || slowLogData->type == SLOW_LOG_READ_QUIT) {
      monitorSendSlowLogAtRunningCb(slowLogData->clusterId);
    }
    monitorFreeSlowLogData(slowLogData);
    taosFreeQitem(slowLogData);
  }

  return NULL;
}

static int32_t tscMonitortInit() {
  TdThreadAttr thAttr;
  if (taosThreadAttrInit(&thAttr) != 0) {
    tscError("failed to init thread attr since %s", strerror(ERRNO));
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE) != 0) {
    tscError("failed to set thread attr since %s", strerror(ERRNO));
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  if (taosThreadCreate(&monitorThread, &thAttr, monitorThreadFunc, NULL) != 0) {
    tscError("failed to create monitor thread since %s", strerror(ERRNO));
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  return 0;
}

static void tscMonitorStop() {
  if (taosCheckPthreadValid(monitorThread)) {
    (void)taosThreadJoin(monitorThread, NULL);
    taosThreadClear(&monitorThread);
  }
}

int32_t monitorInit() {
  int32_t code = 0;

  tscInfo("monitor init");
  monitorCounterHash =
      (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (monitorCounterHash == NULL) {
    tscError("failed to create monitorCounterHash");
    return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
  }
  taosHashSetFreeFp(monitorCounterHash, destroyMonitorClient);

  monitorSlowLogHashPath =
      (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (monitorSlowLogHashPath == NULL) {
    tscError("failed to create monitorSlowLogHashPath");
    return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
  }
  taosHashSetFreeFp(monitorSlowLogHashPath, destroySlowLogClient);

  monitorSlowLogHash =
      (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (monitorSlowLogHash == NULL) {
    tscError("failed to create monitorSlowLogHash");
    return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
  }
  taosHashSetFreeFp(monitorSlowLogHash, destroySlowLogClient);

  monitorTimer = taosTmrInit(0, 0, 0, "MONITOR");
  if (monitorTimer == NULL) {
    tscError("failed to create monitor timer");
    return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
  }

  code = getSlowLogTmpDir(tmpSlowLogPath, sizeof(tmpSlowLogPath));
  if (code != 0) {
    return code;
  }

  code = taosMulModeMkDir(tmpSlowLogPath, 0777, true);
  if (code != 0) {
    tscError("failed to create dir:%s since %s", tmpSlowLogPath, terrstr());
    return code;
  }

  if (tsem2_init(&monitorSem, 0, 0) != 0) {
    tscError("sem init error since %s", terrstr());
    return TAOS_SYSTEM_ERROR(ERRNO);
  }

  code = taosOpenQueue(&monitorQueue);
  if (code) {
    tscError("open queue error since %s", terrstr());
    return TAOS_GET_TERRNO(code);
  }

  taosInitRWLatch(&monitorLock);
  taosInitRWLatch(&monitorQueueLock);
  return tscMonitortInit();
}

void monitorClose() {
  tscInfo("monitor close");
  taosWLockLatch(&monitorLock);
  atomic_store_32(&monitorFlag, 1);
  atomic_store_64(&quitTime, taosGetMonoTimestampMs());
  tscMonitorStop();
  sendAllCounter();
  taosHashCleanup(monitorCounterHash);
  taosHashCleanup(monitorSlowLogHash);
  taosTmrCleanUp(monitorTimer);
  taosWUnLockLatch(&monitorLock);

  taosWLockLatch(&monitorQueueLock);
  taosCloseQueue(monitorQueue);
  monitorQueue = NULL;
  if (tsem2_destroy(&monitorSem) != 0) {
    tscError("failed to destroy semaphore");
  }
  taosWUnLockLatch(&monitorQueueLock);
}

int32_t monitorPutData2MonitorQueue(MonitorSlowLogData data) {
  int32_t             code = 0;
  MonitorSlowLogData* slowLogData = NULL;

  code = taosAllocateQitem(sizeof(MonitorSlowLogData), DEF_QITEM, 0, (void**)&slowLogData);
  if (code) {
    tscError("monitor failed to allocate slow log data");
    return code;
  }
  *slowLogData = data;
  tscDebug("monitor write slow log to queue, clusterId:0x%" PRIx64 " type:%s, data:%s", slowLogData->clusterId,
           queueTypeStr[slowLogData->type], slowLogData->data == NULL ? "null" : slowLogData->data);
  taosWLockLatch(&monitorQueueLock);
  if (monitorQueue == NULL) {
    tscError("monitor queue is null");
    taosWUnLockLatch(&monitorQueueLock);
    taosFreeQitem(slowLogData);
    return 0;
  }
  code = taosWriteQitem(monitorQueue, slowLogData);
  taosWUnLockLatch(&monitorQueueLock);

  if (code == 0) {
    if (tsem2_post(&monitorSem) != 0) {
      tscError("failed to post semaphore");
    }
  } else {
    taosFreeQitem(slowLogData);
  }
  return code;
}

int32_t reportCB(void* param, SDataBuf* pMsg, int32_t code) {
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  tscDebug("[del report]delete reportCB code:%d", code);
  return 0;
}

int32_t senAuditInfo(STscObj* pTscObj, void* pReq, int32_t len, uint64_t requestId) {
  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    tscError("[del report] failed to allocate memory for sendInfo");
    return terrno;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = len, .handle = NULL};

  sendInfo->requestId = requestId;
  sendInfo->requestObjRefId = 0;
  sendInfo->param = NULL;
  sendInfo->fp = reportCB;
  sendInfo->msgType = TDMT_MND_AUDIT;

  SEpSet epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  int32_t code = asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);
  if (code != 0) {
    tscError("[del report] failed to send msg to server, code:%d", code);
    taosMemoryFree(sendInfo);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t setDeleteStmtAuditReqTableInfo(SDeleteStmt* pStmt, SAuditReq* pReq) {
  if (nodeType(pStmt->pFromTable) != QUERY_NODE_REAL_TABLE) {
    tscDebug("[report] invalid from table node type:%s", nodesNodeName(pStmt->pFromTable->type));
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  SRealTableNode* pTableNode = (SRealTableNode*)pStmt->pFromTable;
  TAOS_UNUSED(tsnprintf(pReq->table, TSDB_TABLE_NAME_LEN, "%s", pTableNode->table.tableName));
  TAOS_UNUSED(tsnprintf(pReq->db, TSDB_DB_FNAME_LEN, "%s", pTableNode->table.dbName));
  return TSDB_CODE_SUCCESS;
}

static int32_t setModifyStmtAuditReqTableInfo(SVnodeModifyOpStmt* pStmt, SAuditReq* pReq) {
  if (pStmt->insertType != TSDB_QUERY_TYPE_INSERT && pStmt->insertType != TSDB_QUERY_TYPE_FILE_INSERT &&
      pStmt->insertType != TSDB_QUERY_TYPE_STMT_INSERT) {
    tscDebug("[report] invalid from table node type:%s", nodesNodeName(pStmt->sqlNodeType));
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  TAOS_UNUSED(tsnprintf(pReq->table, TSDB_TABLE_NAME_LEN, "%s", pStmt->targetTableName.tname));
  TAOS_UNUSED(tsnprintf(pReq->db, TSDB_DB_FNAME_LEN, "%s", pStmt->targetTableName.dbname));
  return TSDB_CODE_SUCCESS;
}

typedef struct SAuditTableListInfo {
  SArray* dbList;
  SArray* tableList;
} SAuditTableListInfo;

static int32_t initAuditTableListInfo(SAuditTableListInfo* pInfo) {
  if (pInfo->dbList) return TSDB_CODE_SUCCESS;

  pInfo->dbList = taosArrayInit(4, TSDB_DB_FNAME_LEN);
  if (pInfo->dbList == NULL) {
    tscError("[report] failed to create db list array");
    return terrno;
  }

  pInfo->tableList = taosArrayInit(4, TSDB_TABLE_NAME_LEN);
  if (pInfo->tableList == NULL) {
    tscError("[report] failed to create table list array");
    taosArrayDestroy(pInfo->dbList);
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static void destroyAuditTableListInfo(SAuditTableListInfo* pInfo) {
  if (pInfo->dbList) {
    taosArrayDestroy(pInfo->dbList);
    pInfo->dbList = NULL;
  }
  if (pInfo->tableList) {
    taosArrayDestroy(pInfo->tableList);
    pInfo->tableList = NULL;
  }
}

static void copyTableInfoToAuditReq(SAuditTableListInfo* pTbListInfo, SAuditReq* pReq) {
  if (pTbListInfo->dbList->size > 0) {
    char* dbName = (char*)taosArrayGet(pTbListInfo->dbList, 0);
    TAOS_UNUSED(tsnprintf(pReq->db, TSDB_DB_FNAME_LEN, "%s", dbName));
  }
  if (pTbListInfo->tableList->size > 0) {
    char* tableName = (char*)taosArrayGet(pTbListInfo->tableList, 0);
    TAOS_UNUSED(tsnprintf(pReq->table, TSDB_TABLE_NAME_LEN, "%s", tableName));
  }
}

static int32_t doSetSelectStmtAuditReqTableInfo(SNode* pFromTable, SAuditReq* pReq, SAuditTableListInfo* pTbListInfo);
static int32_t doSetOperatorTableInfo(SSetOperator* pSetOperator, SAuditTableListInfo* pTbListInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pSetOperator->pLeft) {
    SSelectStmt* pLeftSelect = (SSelectStmt*)pSetOperator->pLeft;
    if (nodeType(pSetOperator->pLeft) == QUERY_NODE_SELECT_STMT) {
      code = doSetSelectStmtAuditReqTableInfo(pLeftSelect->pFromTable, NULL, pTbListInfo);
    } else if (nodeType(pSetOperator->pLeft) == QUERY_NODE_SET_OPERATOR) {
      SSetOperator* pLeftSetOperator = (SSetOperator*)pSetOperator->pLeft;
      code = doSetOperatorTableInfo(pLeftSetOperator, pTbListInfo);
      TAOS_RETURN(code);
    }
  }
  if (pSetOperator->pRight) {
    SSelectStmt* pRightSelect = (SSelectStmt*)pSetOperator->pRight;
    if (nodeType(pSetOperator->pRight) == QUERY_NODE_SELECT_STMT) {
      code = doSetSelectStmtAuditReqTableInfo(pRightSelect->pFromTable, NULL, pTbListInfo);
      TAOS_RETURN(code);
    } else if (nodeType(pSetOperator->pRight) == QUERY_NODE_SET_OPERATOR) {
      SSetOperator* pRightSetOperator = (SSetOperator*)pSetOperator->pRight;
      code = doSetOperatorTableInfo(pRightSetOperator, pTbListInfo);
      TAOS_RETURN(code);
    }
  }
  return code;
}

static int32_t doSetSelectStmtAuditReqTableInfo(SNode* pFromTable, SAuditReq* pReq, SAuditTableListInfo* pTbListInfo) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  STableNode* pTable = NULL;

  if (!pFromTable) return TSDB_CODE_TSC_INVALID_OPERATION;

  if (nodeType(pFromTable) == QUERY_NODE_REAL_TABLE) {
    SRealTableNode* pTableNode = (SRealTableNode*)pFromTable;
    pTable = &pTableNode->table;

  } else if (nodeType(pFromTable) == QUERY_NODE_TEMP_TABLE && ((STempTableNode*)pFromTable)->pSubquery) {
    if (nodeType(((STempTableNode*)pFromTable)->pSubquery) == QUERY_NODE_SELECT_STMT) {
      SSelectStmt* pSubquery = (SSelectStmt*)((STempTableNode*)pFromTable)->pSubquery;
      return doSetSelectStmtAuditReqTableInfo(pSubquery->pFromTable, pReq, pTbListInfo);

    } else if (nodeType(((STempTableNode*)pFromTable)->pSubquery) == QUERY_NODE_SET_OPERATOR) {
      code = initAuditTableListInfo(pTbListInfo);
      TAOS_CHECK_GOTO(code, &lino, _exit);

      SSetOperator* pSetOperator = (SSetOperator*)((STempTableNode*)pFromTable)->pSubquery;
      code = doSetOperatorTableInfo(pSetOperator, pTbListInfo);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
  } else if (nodeType(pFromTable) == QUERY_NODE_VIRTUAL_TABLE && pFromTable) {
    SVirtualTableNode* pVtable = (SVirtualTableNode*)pFromTable;
    pTable = &pVtable->table;

  } else if (nodeType(pFromTable) == QUERY_NODE_JOIN_TABLE) {
    code = initAuditTableListInfo(pTbListInfo);
    TAOS_CHECK_GOTO(code, &lino, _exit);

    SJoinTableNode* pJoinTable = (SJoinTableNode*)pFromTable;
    code = doSetSelectStmtAuditReqTableInfo(pJoinTable->pLeft, NULL, pTbListInfo);
    TAOS_CHECK_GOTO(code, &lino, _exit);
    code = doSetSelectStmtAuditReqTableInfo(pJoinTable->pRight, NULL, pTbListInfo);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }
  if (pTbListInfo->dbList == NULL && pTable && pReq) {
    TAOS_UNUSED(tsnprintf(pReq->table, TSDB_TABLE_NAME_LEN, "%s", pTable->tableName));
    TAOS_UNUSED(tsnprintf(pReq->db, TSDB_DB_FNAME_LEN, "%s", pTable->dbName));
  } else if (pTbListInfo->dbList != NULL && pTable) {
    void* tmp = taosArrayPush(pTbListInfo->dbList, pTable->dbName);
    TSDB_CHECK_NULL(tmp, code, lino, _exit, terrno);
    tmp = taosArrayPush(pTbListInfo->tableList, pTable->tableName);
    TSDB_CHECK_NULL(tmp, code, lino, _exit, terrno);
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tscError("[report] failed to set select stmt audit req table info, code:%d, lino:%d", code, lino);
  }
  return code;
}

static int32_t setSelectStmtAuditReqTableInfo(SSelectStmt* pStmt, SAuditReq* pReq) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SAuditTableListInfo tableListInfo = {0};

  code = doSetSelectStmtAuditReqTableInfo(pStmt->pFromTable, pReq, &tableListInfo);
  if (code == TSDB_CODE_SUCCESS && tableListInfo.dbList) {
    copyTableInfoToAuditReq(&tableListInfo, pReq);
    destroyAuditTableListInfo(&tableListInfo);
  }
  return code;
}

static int32_t setOperatorTableInfo(SSetOperator* pSetOperator, SAuditReq* pReq) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SAuditTableListInfo tableListInfo = {0};
  code = initAuditTableListInfo(&tableListInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = doSetOperatorTableInfo(pSetOperator, &tableListInfo);
  if (code == TSDB_CODE_SUCCESS) {
    copyTableInfoToAuditReq(&tableListInfo, pReq);
  }
  destroyAuditTableListInfo(&tableListInfo);
  return code;
}

static int32_t setAuditReqTableInfo(SRequestObj* pRequest, ENodeType type, SAuditReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (QUERY_NODE_DELETE_STMT == type) {
    SDeleteStmt* pStmt = (SDeleteStmt*)pRequest->pQuery->pRoot;
    return setDeleteStmtAuditReqTableInfo(pStmt, pReq);
  } else if (QUERY_NODE_VNODE_MODIFY_STMT == type) {
    return setModifyStmtAuditReqTableInfo((SVnodeModifyOpStmt*)pRequest->pQuery->pRoot, pReq);
  } else if (QUERY_NODE_SELECT_STMT == type) {
    return setSelectStmtAuditReqTableInfo((SSelectStmt*)pRequest->pQuery->pRoot, pReq);
  } else if (QUERY_NODE_SET_OPERATOR == type) {
    return setOperatorTableInfo((SSetOperator*)pRequest->pQuery->pRoot, pReq);
  }
  tscError("[report]unsupported report type: %s", nodesNodeName(type));
  return code;
}

static void setAuditReqAffectedRows(SRequestObj* pRequest, ENodeType type, SAuditReq* pReq) {
  if (QUERY_NODE_DELETE_STMT == type || QUERY_NODE_VNODE_MODIFY_STMT == type) {
    pReq->affectedRows = pRequest->body.resInfo.numOfRows;
  } else if (QUERY_NODE_SELECT_STMT == type || QUERY_NODE_SET_OPERATOR == type) {
    pReq->affectedRows = 0;
  }
}

static void setAuditReqOperation(SRequestObj* pRequest, ENodeType type, SAuditReq* pReq) {
  if (QUERY_NODE_DELETE_STMT == type) {
    TAOS_UNUSED(tsnprintf(pReq->operation, AUDIT_OPERATION_LEN, "delete"));
  } else if (QUERY_NODE_VNODE_MODIFY_STMT == type) {
    TAOS_UNUSED(tsnprintf(pReq->operation, AUDIT_OPERATION_LEN, "insert"));
  } else if (QUERY_NODE_SELECT_STMT == type) {
    TAOS_UNUSED(tsnprintf(pReq->operation, AUDIT_OPERATION_LEN, "select"));
  }
}

static bool needSendReport(SAppInstServerCFG* pCfg, ENodeType type) {
  if (pCfg->auditLevel < AUDIT_LEVEL_DATA) {
    return false;
  }
  if (type == QUERY_NODE_SELECT_STMT) {
    return pCfg->enableAuditSelect != 0;
  } else if (type == QUERY_NODE_DELETE_STMT) {
    return pCfg->enableAuditDelete != 0;
  } else if (type == QUERY_NODE_VNODE_MODIFY_STMT) {
    return pCfg->enableAuditInsert != 0;
  } else if (type == QUERY_NODE_SET_OPERATOR) {
    return pCfg->enableAuditSelect != 0;
  }
  tscError("[report] unsupported report type: %s", nodesNodeName(type));

  return false;
}

static void reportSqlExecResult(SRequestObj* pRequest, ENodeType type) {
  int32_t  code = TSDB_CODE_SUCCESS;
  STscObj* pTscObj = pRequest->pTscObj;

  if (pTscObj == NULL || pTscObj->pAppInfo == NULL) {
    tscError("[report][%s] invalid tsc obj", nodesNodeName(type));
    return;
  }
  if (pRequest->code != TSDB_CODE_SUCCESS) {
    tscDebug("[report][%s] request result code:%d, skip audit", nodesNodeName(type), pRequest->code);
    return;
  }

  if (!needSendReport(&pTscObj->pAppInfo->serverCfg, type)) {
    tscTrace("[report][%s] audit is disabled", nodesNodeName(type));
    return;
  }

  SAuditReq req;
  req.pSql = pRequest->sqlstr;
  req.sqlLen = pRequest->sqlLen;
  setAuditReqAffectedRows(pRequest, type, &req);
  code = setAuditReqTableInfo(pRequest, type, &req);
  if (code == TSDB_CODE_TSC_INVALID_OPERATION) {
    return;
  } else if (code != TSDB_CODE_SUCCESS) {
    tscError("[report][%s] failed to set audit req table info, code:%d", nodesNodeName(type), code);
    return;
  }
  int64_t duration = taosGetTimestampUs() - pRequest->metric.start;
  req.duration = duration / 1000000.0;  // convert to seconds
  setAuditReqOperation(pRequest, type, &req);

  int32_t tlen = tSerializeSAuditReq(NULL, 0, &req);
  void*   pReq = taosMemoryCalloc(1, tlen);
  if (pReq == NULL) {
    tscError("[report][%s] failed to allocate memory for req", nodesNodeName(type));
    return;
  }

  if (tSerializeSAuditReq(pReq, tlen, &req) < 0) {
    tscError("[report][%s] failed to serialize req", nodesNodeName(type));
    taosMemoryFree(pReq);
    return;
  }

  code = senAuditInfo(pRequest->pTscObj, pReq, tlen, pRequest->requestId);
  if (code != 0) {
    tscError("[report][%s] failed to send audit info, code:%d", nodesNodeName(type), code);
    taosMemoryFree(pReq);
    return;
  }
  tscDebug("[report][%s] data, sql:%s", nodesNodeName(type), req.pSql);
}

void clientOperateReport(SRequestObj* pRequest) {
  if (pRequest == NULL || pRequest->pQuery == NULL || pRequest->pQuery->pRoot == NULL) {
    tscDebug("[report] invalid request");
    return;
  }
  ENodeType type = nodeType(pRequest->pQuery->pRoot);
  if (QUERY_NODE_DELETE_STMT == type || QUERY_NODE_SELECT_STMT == type || QUERY_NODE_VNODE_MODIFY_STMT == type ||
      QUERY_NODE_SET_OPERATOR) {
    reportSqlExecResult(pRequest, type);
  }
}
