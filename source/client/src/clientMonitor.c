#include "clientMonitor.h"
#include "cJSON.h"
#include "clientInt.h"
#include "clientLog.h"
#include "os.h"
#include "tglobal.h"
#include "tmisce.h"
#include "tqueue.h"
#include "ttime.h"
#include "ttimer.h"

SRWLatch    monitorLock;
void*       monitorTimer;
SHashObj*   monitorCounterHash;
int32_t     monitorFlag = 0;
int32_t     quitCnt = 0;
tsem2_t     monitorSem;
STaosQueue* monitorQueue;
SHashObj*   monitorSlowLogHash;
char        tmpSlowLogPath[PATH_MAX] = {0};
TdThread    monitorThread;

static int32_t getSlowLogTmpDir(char* tmpPath, int32_t size) {
  int ret = snprintf(tmpPath, size, "%s/tdengine_slow_log/", tsTempDir);
  if (ret < 0) {
    tscError("failed to get tmp path ret:%d", ret);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  return 0;
}

static void processFileInTheEnd(TdFilePtr pFile, char* path) {
  if (pFile == NULL) {
    return;
  }
  if (taosFtruncateFile(pFile, 0) != 0) {
    tscError("failed to truncate file:%s, errno:%d", path, terrno);
    return;
  }
  if (taosUnLockFile(pFile) != 0) {
    tscError("failed to unlock file:%s, errno:%d", path, terrno);
    return;
  }
  if (taosCloseFile(&(pFile)) != 0) {
    tscError("failed to close file:%s, errno:%d", path, errno);
    return;
  }
  if (taosRemoveFile(path) != 0) {
    tscError("failed to remove file:%s, errno:%d", path, errno);
    return;
  }
}

static void destroySlowLogClient(void* data) {
  if (data == NULL) {
    return;
  }
  SlowLogClient* slowLogClient = *(SlowLogClient**)data;
  processFileInTheEnd(slowLogClient->pFile, slowLogClient->path);
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
  if (ret){
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
  if (pData->type == SLOW_LOG_READ_BEGINNIG) {
    taosMemoryFree(pData->fileName);
  }
}

static void monitorFreeSlowLogDataEx(void* paras) {
  monitorFreeSlowLogData(paras);
  taosMemoryFree(paras);
}

static SAppInstInfo* getAppInstByClusterId(int64_t clusterId) {
  void* p = taosHashGet(appInfo.pInstMapByClusterId, &clusterId, LONG_BYTES);
  if (p == NULL) {
    tscError("failed to get app inst, clusterId:%" PRIx64, clusterId);
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
      tscError("failed to send slow log:%s, clusterId:%" PRIx64, p->data, p->clusterId);
    }
    MonitorSlowLogData tmp = {.clusterId = p->clusterId,
                              .type = p->type,
                              .fileName = p->fileName,
                              .pFile = p->pFile,
                              .offset = p->offset,
                              .data = NULL};
    if (monitorPutData2MonitorQueue(tmp) == 0) {
      p->fileName = NULL;
    } else {
      if (taosCloseFile(&(p->pFile)) != 0) {
        tscError("failed to close file:%p", p->pFile);
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t sendReport(void* pTransporter, SEpSet* epSet, char* pCont, MONITOR_TYPE type, void* param) {
  SStatisReq sStatisReq;
  sStatisReq.pCont = pCont;
  sStatisReq.contLen = strlen(pCont);
  sStatisReq.type = type;

  int tlen = tSerializeSStatisReq(NULL, 0, &sStatisReq);
  if (tlen < 0) {
    goto FAILED;
  }
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    tscError("sendReport failed, out of memory, len:%d", tlen);
    goto FAILED;
  }
  tlen = tSerializeSStatisReq(buf, tlen, &sStatisReq);
  if (tlen < 0) {
    taosMemoryFree(buf);
    goto FAILED;
  }

  SMsgSendInfo* pInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pInfo == NULL) {
    tscError("sendReport failed, out of memory send info");
    taosMemoryFree(buf);
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
  if (taosCloseFile(&(((MonitorSlowLogData*)param)->pFile)) != 0) {
    tscError("failed to close file:%p", ((MonitorSlowLogData*)param)->pFile);
  }
  monitorFreeSlowLogDataEx(param);
  return TAOS_GET_TERRNO(TSDB_CODE_TSC_INTERNAL_ERROR);
}

static void generateClusterReport(taos_collector_registry_t* registry, void* pTransporter, SEpSet* epSet) {
  char ts[50] = {0};
  (void)sprintf(ts, "%" PRId64, taosGetTimestamp(TSDB_TIME_PRECISION_MILLI));
  char* pCont = (char*)taos_collector_registry_bridge_new(registry, ts, "%" PRId64, NULL);
  if (NULL == pCont) {
    tscError("generateClusterReport failed, get null content.");
    return;
  }

  if (strlen(pCont) != 0 && sendReport(pTransporter, epSet, pCont, MONITOR_TYPE_COUNTER, NULL) == 0) {
    int ret = taos_collector_registry_clear_batch(registry);
    if (ret){
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
  bool reset = taosTmrReset(reportSendProcess, pInst->monitorParas.tsMonitorInterval * 1000, param, monitorTimer, &tmrId);
  if (!reset){
    tscError("failed to reset timer, pMonitor:%p", pMonitor);
  }
  taosRUnLockLatch(&monitorLock);
}

static void sendAllCounter() {
  MonitorClient** ppMonitor = NULL;
  while ((ppMonitor = taosHashIterate(monitorSlowLogHash, ppMonitor))) {
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
    tscInfo("[monitor] monitorCreateClient for %" PRIx64, clusterId);
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
    if (r){
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
    pMonitor->timer =
        taosTmrStart(reportSendProcess, pInst->monitorParas.tsMonitorInterval * 1000, (void*)pMonitor, monitorTimer);
    if (pMonitor->timer == NULL) {
      tscError("failed to start timer");
      goto fail;
    }
    tscInfo("[monitor] monitorCreateClient for %" PRIx64 "finished %p.", clusterId, pMonitor);
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
    if (r){
      tscError("failed to destroy counter, code: %d", r);
    }
    goto end;
  }
  if (taosHashPut(pMonitor->counters, name, strlen(name), &newCounter, POINTER_BYTES) != 0) {
    tscError("failed to put counter to monitor");
    int r = taos_counter_destroy(newCounter);
    if (r){
      tscError("failed to destroy counter, code: %d", r);
    }
    goto end;
  }
  tscInfo("[monitor] monitorCreateClientCounter %" PRIx64 "(%p):%s : %p.", pMonitor->clusterId, pMonitor, name,
          newCounter);

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
    tscError("monitorCounterInc not found pMonitor %" PRId64, clusterId);
    goto end;
  }

  MonitorClient*   pMonitor = *ppMonitor;
  taos_counter_t** ppCounter = (taos_counter_t**)taosHashGet(pMonitor->counters, counterName, strlen(counterName));
  if (ppCounter == NULL || *ppCounter == NULL) {
    tscError("monitorCounterInc not found pCounter %" PRIx64 ":%s.", clusterId, counterName);
    goto end;
  }
  if (taos_counter_inc(*ppCounter, label_values) != 0) {
    tscError("monitorCounterInc failed to inc %" PRIx64 ":%s.", clusterId, counterName);
    goto end;
  }
  tscDebug("[monitor] monitorCounterInc %" PRIx64 "(%p):%s", pMonitor->clusterId, pMonitor, counterName);

end:
  taosWUnLockLatch(&monitorLock);
}

const char* monitorResultStr(SQL_RESULT_CODE code) {
  static const char* result_state[] = {"Success", "Failed", "Cancel"};
  return result_state[code];
}

static void monitorWriteSlowLog2File(MonitorSlowLogData* slowLogData, char* tmpPath) {
  TdFilePtr pFile = NULL;
  void*     tmp = taosHashGet(monitorSlowLogHash, &slowLogData->clusterId, LONG_BYTES);
  if (tmp == NULL) {
    char path[PATH_MAX] = {0};
    char clusterId[32] = {0};
    if (snprintf(clusterId, sizeof(clusterId), "%" PRIx64, slowLogData->clusterId) < 0) {
      tscError("failed to generate clusterId:%" PRIx64, slowLogData->clusterId);
      return;
    }
    taosGetTmpfilePath(tmpPath, clusterId, path);
    tscInfo("[monitor] create slow log file:%s", path);
    pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
    if (pFile == NULL) {
      tscError("failed to open file:%s since %d", path, terrno);
      return;
    }

    SlowLogClient* pClient = taosMemoryCalloc(1, sizeof(SlowLogClient));
    if (pClient == NULL) {
      tscError("failed to allocate memory for slow log client");
      int32_t ret = taosCloseFile(&pFile);
      if (ret != 0){
        tscError("failed to close file:%p ret:%d", pFile, ret);
      }
      return;
    }
    pClient->lastCheckTime = taosGetMonoTimestampMs();
    (void)strcpy(pClient->path, path);
    pClient->offset = 0;
    pClient->pFile = pFile;
    if (taosHashPut(monitorSlowLogHash, &slowLogData->clusterId, LONG_BYTES, &pClient, POINTER_BYTES) != 0) {
      tscError("failed to put clusterId:%" PRId64 " to hash table", slowLogData->clusterId);
      int32_t ret = taosCloseFile(&pFile);
      if (ret != 0){
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
  }

  if (taosLSeekFile(pFile, 0, SEEK_END) < 0) {
    tscError("failed to seek file:%p code: %d", pFile, terrno);
    return;
  }
  if (taosWriteFile(pFile, slowLogData->data, strlen(slowLogData->data) + 1) < 0) {
    tscError("failed to write len to file:%p since %s", pFile, terrstr());
  }
  tscDebug("[monitor] write slow log to file:%p, clusterId:%" PRIx64, pFile, slowLogData->clusterId);
}

static char* readFile(TdFilePtr pFile, int64_t* offset, int64_t size) {
  tscDebug("[monitor] readFile slow begin pFile:%p, offset:%" PRId64 ", size:%" PRId64, pFile, *offset, size);
  if (taosLSeekFile(pFile, *offset, SEEK_SET) < 0) {
    tscError("failed to seek file:%p code: %d", pFile, terrno);
    return NULL;
  }

  if ((size <= *offset)) {
    tscError("invalid size:%" PRId64 ", offset:%" PRId64, size, *offset);
    terrno = TSDB_CODE_TSC_INTERNAL_ERROR;
    return NULL;
  }
  char*   pCont = NULL;
  int64_t totalSize = 0;
  if (size - *offset >= SLOW_LOG_SEND_SIZE_MAX) {
    pCont = taosMemoryCalloc(1, 4 + SLOW_LOG_SEND_SIZE_MAX);  // 4 reserved for []
    totalSize = 4 + SLOW_LOG_SEND_SIZE_MAX;
  } else {
    pCont = taosMemoryCalloc(1, 4 + (size - *offset));
    totalSize = 4 + (size - *offset);
  }

  if (pCont == NULL) {
    tscError("failed to allocate memory for slow log, size:%" PRId64, totalSize);
    return NULL;
  }
  char* buf = pCont;
  (void)strcat(buf++, "[");
  int64_t readSize = taosReadFile(pFile, buf, SLOW_LOG_SEND_SIZE_MAX);
  if (readSize <= 0) {
    if (readSize < 0) {
      tscError("failed to read len from file:%p since %s", pFile, terrstr());
    }
    taosMemoryFree(pCont);
    return NULL;
  }

  totalSize = 0;
  while (1) {
    size_t len = strlen(buf);
    totalSize += (len + 1);
    if (totalSize > readSize || len == 0) {
      *(buf - 1) = ']';
      *buf = '\0';
      break;
    }
    buf[len] = ',';  // replace '\0' with ','
    buf += (len + 1);
    *offset += (len + 1);
  }

  tscDebug("[monitor] readFile slow log end, data:%s, offset:%" PRId64, pCont, *offset);
  return pCont;
}

static int64_t getFileSize(char* path) {
  int64_t fileSize = 0;
  if (taosStatFile(path, &fileSize, NULL, NULL) < 0) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  return fileSize;
}

static int32_t sendSlowLog(int64_t clusterId, char* data, TdFilePtr pFile, int64_t offset, SLOW_LOG_QUEUE_TYPE type,
                           char* fileName, void* pTransporter, SEpSet* epSet) {
  if (data == NULL) {
    if (taosCloseFile(&pFile) != 0) {
      tscError("failed to close file:%p", pFile);
    }
    taosMemoryFree(fileName);
    return TSDB_CODE_INVALID_PARA;
  }
  MonitorSlowLogData* pParam = taosMemoryMalloc(sizeof(MonitorSlowLogData));
  if (pParam == NULL) {
    if (taosCloseFile(&pFile) != 0) {
      tscError("failed to close file:%p", pFile);
    }
    taosMemoryFree(data);
    taosMemoryFree(fileName);
    return terrno;
  }
  pParam->data = data;
  pParam->offset = offset;
  pParam->clusterId = clusterId;
  pParam->type = type;
  pParam->pFile = pFile;
  pParam->fileName = fileName;
  return sendReport(pTransporter, epSet, data, MONITOR_TYPE_SLOW_LOG, pParam);
}

static int32_t monitorReadSend(int64_t clusterId, TdFilePtr pFile, int64_t* offset, int64_t size,
                               SLOW_LOG_QUEUE_TYPE type, char* fileName) {
  SAppInstInfo* pInst = getAppInstByClusterId(clusterId);
  if (pInst == NULL) {
    tscError("failed to get app instance by clusterId:%" PRId64, clusterId);
    if (taosCloseFile(&pFile) != 0) {
      tscError("failed to close file:%p", pFile);
    }
    taosMemoryFree(fileName);
    return terrno;
  }
  SEpSet ep = getEpSet_s(&pInst->mgmtEp);
  char*  data = readFile(pFile, offset, size);
  if (data == NULL) return terrno;
  return sendSlowLog(clusterId, data, (type == SLOW_LOG_READ_BEGINNIG ? pFile : NULL), *offset, type, fileName,
                     pInst->pTransporter, &ep);
}

static void monitorSendSlowLogAtBeginning(int64_t clusterId, char** fileName, TdFilePtr pFile, int64_t offset) {
  if (fileName == NULL) {
    return;
  }
  int64_t size = getFileSize(*fileName);
  if (size <= offset) {
    processFileInTheEnd(pFile, *fileName);
    tscDebug("[monitor] monitorSendSlowLogAtBeginning delete file:%s", *fileName);
  } else {
    int32_t code = monitorReadSend(clusterId, pFile, &offset, size, SLOW_LOG_READ_BEGINNIG, *fileName);
    if (code == 0) {
      tscDebug("[monitor] monitorSendSlowLogAtBeginning send slow log succ, clusterId:%" PRId64, clusterId);
    } else {
      tscError("[monitor] monitorSendSlowLogAtBeginning send slow log failed, clusterId:%" PRId64 ",ret:%d", clusterId,
               code);
    }
    *fileName = NULL;
  }
}

static void monitorSendSlowLogAtRunning(int64_t clusterId) {
  void* tmp = taosHashGet(monitorSlowLogHash, &clusterId, LONG_BYTES);
  if (tmp == NULL) {
    tscError("failed to get slow log client by clusterId:%" PRId64, clusterId);
    return;
  }
  SlowLogClient* pClient = (*(SlowLogClient**)tmp);
  if (pClient == NULL) {
    tscError("failed to get slow log client by clusterId:%" PRId64, clusterId);
    return;
  }
  int64_t size = getFileSize(pClient->path);
  if (size <= pClient->offset) {
    if (taosFtruncateFile(pClient->pFile, 0) < 0) {
      tscError("failed to truncate file:%p code: %d", pClient->pFile, terrno);
    }
    tscDebug("[monitor] monitorSendSlowLogAtRunning truncate file to 0 file:%p", pClient->pFile);
    pClient->offset = 0;
  } else {
    int32_t code = monitorReadSend(clusterId, pClient->pFile, &pClient->offset, size, SLOW_LOG_READ_RUNNING, NULL);
    tscDebug("[monitor] monitorSendSlowLogAtRunning send slow log clusterId:%" PRId64 ",ret:%d", clusterId, code);
  }
}

static bool monitorSendSlowLogAtQuit(int64_t clusterId) {
  void* tmp = taosHashGet(monitorSlowLogHash, &clusterId, LONG_BYTES);
  if (tmp == NULL) {
    return true;
  }
  SlowLogClient* pClient = (*(SlowLogClient**)tmp);
  if (pClient == NULL) {
    return true;
  }
  int64_t size = getFileSize(pClient->path);
  if (size <= pClient->offset) {
    processFileInTheEnd(pClient->pFile, pClient->path);
    pClient->pFile = NULL;
    tscInfo("[monitor] monitorSendSlowLogAtQuit remove file:%s", pClient->path);
    if ((--quitCnt) == 0) {
      return true;
    }
  } else {
    int32_t code = monitorReadSend(clusterId, pClient->pFile, &pClient->offset, size, SLOW_LOG_READ_QUIT, NULL);
    tscDebug("[monitor] monitorSendSlowLogAtQuit send slow log clusterId:%" PRId64 ",ret:%d", clusterId, code);
  }
  return false;
}
static void monitorSendAllSlowLogAtQuit() {
  void* pIter = NULL;
  while ((pIter = taosHashIterate(monitorSlowLogHash, pIter))) {
    SlowLogClient* pClient = (*(SlowLogClient**)pIter);
    if (pClient == NULL) {
      continue;
    }
    int64_t size = getFileSize(pClient->path);
    if (size <= pClient->offset) {
      processFileInTheEnd(pClient->pFile, pClient->path);
      pClient->pFile = NULL;
    } else if (pClient->offset == 0) {
      int64_t* clusterId = (int64_t*)taosHashGetKey(pIter, NULL);
      int32_t  code = monitorReadSend(*clusterId, pClient->pFile, &pClient->offset, size, SLOW_LOG_READ_QUIT, NULL);
      tscDebug("[monitor] monitorSendAllSlowLogAtQuit send slow log clusterId:%" PRId64 ",ret:%d", *clusterId, code);
      if (code == 0) {
        quitCnt++;
      }
    }
  }
}

static void processFileRemoved(SlowLogClient* pClient) {
  if (taosUnLockFile(pClient->pFile) != 0) {
    tscError("failed to unlock file:%s since %d", pClient->path, terrno);
    return;
  }
  int32_t ret = taosCloseFile(&(pClient->pFile));
  if (ret != 0){
    tscError("failed to close file:%p ret:%d", pClient->pFile, ret);
    return;
  }

  TdFilePtr pFile =
      taosOpenFile(pClient->path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
  if (pFile == NULL) {
    tscError("failed to open file:%s since %d", pClient->path, terrno);
  } else {
    pClient->pFile = pFile;
  }
}

static void monitorSendAllSlowLog() {
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
    if (t - pClient->lastCheckTime > pInst->monitorParas.tsMonitorInterval * 1000) {
      pClient->lastCheckTime = t;
    } else {
      continue;
    }

    if (pClient->offset == 0) {
      int64_t size = getFileSize(pClient->path);
      if (size <= 0) {
        if (size < 0) {
          tscError("[monitor] monitorSendAllSlowLog failed to get file size:%s, err:%d", pClient->path, errno);
          if (errno == ENOENT) {
            processFileRemoved(pClient);
          }
        }
        continue;
      }
      int32_t code = monitorReadSend(*clusterId, pClient->pFile, &pClient->offset, size, SLOW_LOG_READ_RUNNING, NULL);
      tscDebug("[monitor] monitorSendAllSlowLog send slow log clusterId:%" PRId64 ",ret:%d", *clusterId, code);
    }
  }
}

static void monitorSendAllSlowLogFromTempDir(int64_t clusterId) {
  SAppInstInfo* pInst = getAppInstByClusterId((int64_t)clusterId);

  if (pInst == NULL || !pInst->monitorParas.tsEnableMonitor) {
    tscInfo("[monitor] monitor is disabled, skip send slow log");
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
    TdFilePtr pFile = taosOpenFile(filename, TD_FILE_READ | TD_FILE_WRITE);
    if (pFile == NULL) {
      tscError("failed to open file:%s since %s", filename, terrstr());
      continue;
    }
    if (taosLockFile(pFile) < 0) {
      tscInfo("failed to lock file:%s since %s, maybe used by other process", filename, terrstr());
      int32_t ret = taosCloseFile(&pFile);
      if (ret != 0){
        tscError("failed to close file:%p ret:%d", pFile, ret);
      }
      continue;
    }
    char* tmp = taosStrdup(filename);
    monitorSendSlowLogAtBeginning(clusterId, &tmp, pFile, 0);
    taosMemoryFree(tmp);
  }

  int32_t ret = taosCloseDir(&pDir);
  if (ret != 0){
    tscError("failed to close dir, ret:%d", ret);
  }
}

static void* monitorThreadFunc(void* param) {
  setThreadName("client-monitor-slowlog");
  tscDebug("monitorThreadFunc start");
  int64_t quitTime = 0;
  while (1) {
    if (atomic_load_32(&monitorFlag) == 1) {
      if (quitCnt == 0) {
        monitorSendAllSlowLogAtQuit();
        if (quitCnt == 0) {
          tscInfo("monitorThreadFunc quit since no slow log to send");
          break;
        }
        quitTime = taosGetMonoTimestampMs();
      }
      if (taosGetMonoTimestampMs() - quitTime > 500) {  // quit at most 500ms
        tscInfo("monitorThreadFunc quit since timeout");
        break;
      }
    }

    MonitorSlowLogData* slowLogData = NULL;
    if (taosReadQitem(monitorQueue, (void**)&slowLogData) != 0) {
      if (slowLogData->type == SLOW_LOG_READ_BEGINNIG && quitCnt == 0) {
        if (slowLogData->pFile != NULL) {
          monitorSendSlowLogAtBeginning(slowLogData->clusterId, &(slowLogData->fileName), slowLogData->pFile,
                                        slowLogData->offset);
        } else {
          monitorSendAllSlowLogFromTempDir(slowLogData->clusterId);
        }
      } else if (slowLogData->type == SLOW_LOG_WRITE) {
        monitorWriteSlowLog2File(slowLogData, tmpSlowLogPath);
      } else if (slowLogData->type == SLOW_LOG_READ_RUNNING) {
        monitorSendSlowLogAtRunning(slowLogData->clusterId);
      } else if (slowLogData->type == SLOW_LOG_READ_QUIT) {
        if (monitorSendSlowLogAtQuit(slowLogData->clusterId)) {
          tscInfo("monitorThreadFunc quit since all slow log sended");
          monitorFreeSlowLogData(slowLogData);
          taosFreeQitem(slowLogData);
          break;
        }
      }
      monitorFreeSlowLogData(slowLogData);
      taosFreeQitem(slowLogData);
    }

    if (quitCnt == 0) {
      monitorSendAllSlowLog();
    }
    (void)tsem2_timewait(&monitorSem, 100);
  }
  return NULL;
}

static int32_t tscMonitortInit() {
  TdThreadAttr thAttr;
  if (taosThreadAttrInit(&thAttr) != 0) {
    tscError("failed to init thread attr since %s", strerror(errno));
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE) != 0) {
    tscError("failed to set thread attr since %s", strerror(errno));
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  if (taosThreadCreate(&monitorThread, &thAttr, monitorThreadFunc, NULL) != 0) {
    tscError("failed to create monitor thread since %s", strerror(errno));
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  return 0;
}

static void tscMonitorStop() {
  if (taosCheckPthreadValid(monitorThread)) {
    (void)taosThreadJoin(monitorThread, NULL);
    (void)taosThreadClear(&monitorThread);
  }
}

int32_t monitorInit() {
  int32_t code = 0;

  tscInfo("[monitor] tscMonitor init");
  monitorCounterHash =
      (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (monitorCounterHash == NULL) {
    tscError("failed to create monitorCounterHash");
    return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
  }
  taosHashSetFreeFp(monitorCounterHash, destroyMonitorClient);

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
    return TAOS_SYSTEM_ERROR(errno);
  }

  code = taosOpenQueue(&monitorQueue);
  if (code) {
    tscError("open queue error since %s", terrstr());
    return TAOS_GET_TERRNO(code);
  }

  taosInitRWLatch(&monitorLock);
  return tscMonitortInit();
}

void monitorClose() {
  tscInfo("[monitor] tscMonitor close");
  taosWLockLatch(&monitorLock);
  atomic_store_32(&monitorFlag, 1);
  tscMonitorStop();
  sendAllCounter();
  taosHashCleanup(monitorCounterHash);
  taosHashCleanup(monitorSlowLogHash);
  taosTmrCleanUp(monitorTimer);
  taosCloseQueue(monitorQueue);
  if(tsem2_destroy(&monitorSem) != 0) {
    tscError("failed to destroy semaphore");
  }
  taosWUnLockLatch(&monitorLock);
}

int32_t monitorPutData2MonitorQueue(MonitorSlowLogData data) {
  int32_t             code = 0;
  MonitorSlowLogData* slowLogData = NULL;

  if (atomic_load_32(&monitorFlag) == 1) {
    tscError("[monitor] slow log thread is exiting");
    return -1;
  }

  code = taosAllocateQitem(sizeof(MonitorSlowLogData), DEF_QITEM, 0, (void**)&slowLogData);
  if (code) {
    tscError("[monitor] failed to allocate slow log data");
    return code;
  }
  *slowLogData = data;
  tscDebug("[monitor] write slow log to queue, clusterId:%" PRIx64 " type:%s, data:%s", slowLogData->clusterId,
           queueTypeStr[slowLogData->type], slowLogData->data);
  if (taosWriteQitem(monitorQueue, slowLogData) == 0) {
    if(tsem2_post(&monitorSem) != 0) {
      tscError("failed to post semaphore");
    }
  } else {
    if (taosCloseFile(&(slowLogData->pFile)) != 0) {
      tscError("failed to close file:%p", slowLogData->pFile);
    }
    monitorFreeSlowLogData(slowLogData);
    taosFreeQitem(slowLogData);
  }
  return 0;
}