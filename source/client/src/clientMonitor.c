#include "clientMonitor.h"
#include "os.h"
#include "tmisce.h"
#include "ttime.h"
#include "ttimer.h"
#include "tglobal.h"
#include "tqueue.h"
#include "cJSON.h"
#include "clientInt.h"

SRWLatch    monitorLock;
void*       monitorTimer;
SHashObj*   monitorCounterHash;
int32_t     slowLogFlag = -1;
int32_t     monitorFlag = -1;
int32_t     quitCnt = 0;
tsem2_t     monitorSem;
STaosQueue* monitorQueue;
SHashObj*   monitorSlowLogHash;

static int32_t getSlowLogTmpDir(char* tmpPath, int32_t size){
  if (tsTempDir == NULL) {
    return -1;
  }
  int ret = snprintf(tmpPath, size, "%s/tdengine_slow_log/", tsTempDir);
  if (ret < 0){
    uError("failed to get tmp path ret:%d", ret);
    return ret;
  }
  return 0;
}

//static void destroyCounter(void* data){
//  if (data == NULL) {
//    return;
//  }
//  taos_counter_t* conuter = *(taos_counter_t**)data;
//  if(conuter == NULL){
//    return;
//  }
//  taos_counter_destroy(conuter);
//}

static void destroySlowLogClient(void* data){
  if (data == NULL) {
    return;
  }
  SlowLogClient* slowLogClient = *(SlowLogClient**)data;
  taosMemoryFree(slowLogClient);
}

static void destroyMonitorClient(void* data){
  if (data == NULL) {
    return;
  }
  MonitorClient* pMonitor = *(MonitorClient**)data;
  if(pMonitor == NULL){
    return;
  }
  taosTmrStopA(&pMonitor->timer);
  taosHashCleanup(pMonitor->counters);
  taos_collector_registry_destroy(pMonitor->registry);
//  taos_collector_destroy(pMonitor->colector);
  taosMemoryFree(pMonitor);
}

static void monitorFreeSlowLogData(void *paras) {
  MonitorSlowLogData* pData = (MonitorSlowLogData*)paras;
  if (pData == NULL) {
    return;
  }
  if (pData->type == SLOW_LOG_WRITE){
    taosMemoryFree(pData->data);
  }
  if (pData->type == SLOW_LOG_READ_BEGINNIG){
    taosMemoryFree(pData->fileName);
  }
}

static SAppInstInfo* getAppInstByClusterId(int64_t clusterId) {
  void *p = taosHashGet(appInfo.pInstMapByClusterId, &clusterId, LONG_BYTES);
  if(p == NULL){
    uError("failed to get app inst, clusterId:%" PRIx64, clusterId);
    return NULL;
  }
  return *(SAppInstInfo**)p;
}

static int32_t monitorReportAsyncCB(void* param, SDataBuf* pMsg, int32_t code) {
  if (TSDB_CODE_SUCCESS != code) {
    uError("found error in monitorReport send callback, code:%d, please check the network.", code);
  }
  if (pMsg) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
  }
  if(param != NULL){
    MonitorSlowLogData* p = (MonitorSlowLogData*)param;
    if(code != 0){
      uError("failed to send slow log:%s, clusterId:%" PRIx64, p->data, p->clusterId);
    }
    if(monitorPutData2MonitorQueue(*p) == 0){
      p->fileName = NULL;
    }
    monitorFreeSlowLogData(p);
    taosMemoryFree(p);
  }
  return code;
}

static int32_t sendReport(void* pTransporter, SEpSet *epSet, char* pCont, MONITOR_TYPE type, void* param) {
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
    uError("sendReport failed, out of memory, len:%d", tlen);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto FAILED;
  }
  tSerializeSStatisReq(buf, tlen, &sStatisReq);

  SMsgSendInfo* pInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pInfo == NULL) {
    uError("sendReport failed, out of memory send info");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(buf);
    goto FAILED;
  }
  pInfo->fp = monitorReportAsyncCB;
  pInfo->msgInfo.pData = buf;
  pInfo->msgInfo.len = tlen;
  pInfo->msgType = TDMT_MND_STATIS;
  pInfo->param = param;
  pInfo->requestId = tGenIdPI64();
  pInfo->requestObjRefId = 0;

  int64_t transporterId = 0;
  int32_t code = asyncSendMsgToServer(pTransporter, epSet, &transporterId, pInfo);
  if (code == TSDB_CODE_SUCCESS) {
    return code;
  }

FAILED:
  uError("sendReport failed, code:%d", code);
  monitorFreeSlowLogData(param);
  taosMemoryFree(param);
  return -1;
}

static void generateClusterReport(taos_collector_registry_t* registry, void* pTransporter, SEpSet *epSet) {
  char ts[50] = {0};
  sprintf(ts, "%" PRId64, taosGetTimestamp(TSDB_TIME_PRECISION_MILLI));
  char* pCont = (char*)taos_collector_registry_bridge_new(registry, ts, "%" PRId64, NULL);
  if(NULL == pCont) {
    uError("generateClusterReport failed, get null content.");
    return;
  }

  if (strlen(pCont) != 0 && sendReport(pTransporter, epSet, pCont, MONITOR_TYPE_COUNTER, NULL) == 0) {
    taos_collector_registry_clear_batch(registry);
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
  SAppInstInfo* pInst = getAppInstByClusterId(pMonitor->clusterId);
  if(pInst == NULL){
    taosRUnLockLatch(&monitorLock);
    return;
  }

  SEpSet ep = getEpSet_s(&pInst->mgmtEp);
  generateClusterReport(pMonitor->registry, pInst->pTransporter, &ep);
  taosTmrReset(reportSendProcess, pInst->monitorParas.tsMonitorInterval * 1000, param, monitorTimer, &tmrId);
  taosRUnLockLatch(&monitorLock);
}

static void sendAllCounter(){
  MonitorClient** ppMonitor = NULL;
  while ((ppMonitor = taosHashIterate(monitorSlowLogHash, ppMonitor))) {
    MonitorClient* pMonitor = *ppMonitor;
    if (pMonitor == NULL){
      continue;
    }
    SAppInstInfo* pInst = getAppInstByClusterId(pMonitor->clusterId);
    if(pInst == NULL){
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
    uInfo("[monitor] monitorCreateClient for %" PRIx64, clusterId);
    pMonitor = taosMemoryCalloc(1, sizeof(MonitorClient));
    if (pMonitor == NULL) {
      uError("failed to create monitor client");
      goto fail;
    }
    pMonitor->clusterId = clusterId;
    char clusterKey[32] = {0};
    if(snprintf(clusterKey, sizeof(clusterKey), "%"PRId64, clusterId) < 0){
      uError("failed to create cluster key");
      goto fail;
    }
    pMonitor->registry = taos_collector_registry_new(clusterKey);
    if(pMonitor->registry == NULL){
      uError("failed to create registry");
      goto fail;
    }
    pMonitor->colector = taos_collector_new(clusterKey);
    if(pMonitor->colector == NULL){
      uError("failed to create collector");
      goto fail;
    }

    taos_collector_registry_register_collector(pMonitor->registry, pMonitor->colector);
    pMonitor->counters = (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (pMonitor->counters == NULL) {
      uError("failed to create monitor counters");
      goto fail;
    }
//    taosHashSetFreeFp(pMonitor->counters, destroyCounter);

    if(taosHashPut(monitorCounterHash, &clusterId, LONG_BYTES, &pMonitor, POINTER_BYTES) != 0){
      uError("failed to put monitor client to hash");
      goto fail;
    }

    SAppInstInfo* pInst = getAppInstByClusterId(clusterId);
    if(pInst == NULL){
      uError("failed to get app instance by cluster id");
      pMonitor = NULL;
      goto fail;
    }
    pMonitor->timer = taosTmrStart(reportSendProcess, pInst->monitorParas.tsMonitorInterval * 1000, (void*)pMonitor, monitorTimer);
    if(pMonitor->timer == NULL){
      uError("failed to start timer");
      goto fail;
    }
    uInfo("[monitor] monitorCreateClient for %"PRIx64 "finished %p.", clusterId, pMonitor);
  }
  taosWUnLockLatch(&monitorLock);
  if (-1 != atomic_val_compare_exchange_32(&monitorFlag, -1, 0)) {
    uDebug("[monitor] monitorFlag already is 0");
  }
  return;

fail:
  destroyMonitorClient(&pMonitor);
  taosWUnLockLatch(&monitorLock);
}

void monitorCreateClientCounter(int64_t clusterId, const char* name, const char* help, size_t label_key_count, const char** label_keys) {
  taosWLockLatch(&monitorLock);
  MonitorClient** ppMonitor = (MonitorClient**)taosHashGet(monitorCounterHash, &clusterId, LONG_BYTES);
  if (ppMonitor == NULL || *ppMonitor == NULL) {
    uError("failed to get monitor client");
    goto end;
  }
  taos_counter_t* newCounter = taos_counter_new(name, help, label_key_count, label_keys);
  if (newCounter == NULL)
    return;
  MonitorClient*   pMonitor = *ppMonitor;
  taos_collector_add_metric(pMonitor->colector, newCounter);
  if(taosHashPut(pMonitor->counters, name, strlen(name), &newCounter, POINTER_BYTES) != 0){
    uError("failed to put counter to monitor");
    taos_counter_destroy(newCounter);
    goto end;
  }
  uInfo("[monitor] monitorCreateClientCounter %"PRIx64"(%p):%s : %p.", pMonitor->clusterId, pMonitor, name, newCounter);

end:
  taosWUnLockLatch(&monitorLock);
}

void monitorCounterInc(int64_t clusterId, const char* counterName, const char** label_values) {
  taosWLockLatch(&monitorLock);
  MonitorClient** ppMonitor = (MonitorClient**)taosHashGet(monitorCounterHash, &clusterId, LONG_BYTES);
  if (ppMonitor == NULL || *ppMonitor == NULL) {
    uError("monitorCounterInc not found pMonitor %"PRId64, clusterId);
    goto end;
  }

  MonitorClient*   pMonitor = *ppMonitor;
  taos_counter_t** ppCounter = (taos_counter_t**)taosHashGet(pMonitor->counters, counterName, strlen(counterName));
  if (ppCounter == NULL || *ppCounter == NULL) {
    uError("monitorCounterInc not found pCounter %"PRIx64":%s.", clusterId, counterName);
    goto end;
  }
  taos_counter_inc(*ppCounter, label_values);
  uInfo("[monitor] monitorCounterInc %"PRIx64"(%p):%s", pMonitor->clusterId, pMonitor, counterName);

end:
  taosWUnLockLatch(&monitorLock);
}

const char* monitorResultStr(SQL_RESULT_CODE code) {
  static const char* result_state[] = {"Success", "Failed", "Cancel"};
  return result_state[code];
}

static void monitorThreadFuncUnexpectedStopped(void) { atomic_store_32(&slowLogFlag, -1); }

static void monitorWriteSlowLog2File(MonitorSlowLogData* slowLogData, char *tmpPath){
  TdFilePtr pFile = NULL;
  void* tmp = taosHashGet(monitorSlowLogHash, &slowLogData->clusterId, LONG_BYTES);
  if (tmp == NULL){
    char path[PATH_MAX] = {0};
    char clusterId[32] = {0};
    if (snprintf(clusterId, sizeof(clusterId), "%" PRIx64, slowLogData->clusterId) < 0){
      uError("failed to generate clusterId:%" PRIx64, slowLogData->clusterId);
      return;
    }
    taosGetTmpfilePath(tmpPath, clusterId, path);
    uInfo("[monitor] create slow log file:%s", path);
    pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
    if (pFile == NULL) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      uError("failed to open file:%s since %s", path, terrstr());
      return;
    }

    SlowLogClient *pClient = taosMemoryCalloc(1, sizeof(SlowLogClient));
    if (pClient == NULL){
      uError("failed to allocate memory for slow log client");
      taosCloseFile(&pFile);
      return;
    }
    pClient->lastCheckTime = taosGetMonoTimestampMs();
    strcpy(pClient->path, path);
    pClient->offset = 0;
    pClient->pFile = pFile;
    if (taosHashPut(monitorSlowLogHash, &slowLogData->clusterId, LONG_BYTES, &pClient, POINTER_BYTES) != 0){
      uError("failed to put clusterId:%" PRId64 " to hash table", slowLogData->clusterId);
      taosCloseFile(&pFile);
      taosMemoryFree(pClient);
      return;
    }

    if(taosLockFile(pFile) < 0){
      uError("failed to lock file:%p since %s", pFile, terrstr());
      return;
    }

    SAppInstInfo* pInst = getAppInstByClusterId(slowLogData->clusterId);
    if(pInst == NULL){
      uError("failed to get app instance by clusterId:%" PRId64, slowLogData->clusterId);
      return;
    }
  }else{
    pFile = (*(SlowLogClient**)tmp)->pFile;
  }

  if(taosLSeekFile(pFile, 0, SEEK_END) < 0){
    uError("failed to seek file:%p code: %d", pFile, errno);
    return;
  }
  if (taosWriteFile(pFile, slowLogData->data, strlen(slowLogData->data) + 1) < 0){
    uError("failed to write len to file:%p since %s", pFile, terrstr());
  }
  uDebug("[monitor] write slow log to file:%p, clusterId:%"PRIx64, pFile, slowLogData->clusterId);
}

static char* readFile(TdFilePtr pFile, int64_t *offset, int64_t size){
  uDebug("[monitor] readFile slow begin pFile:%p, offset:%"PRId64 ", size:%"PRId64, pFile, *offset, size);
  if(taosLSeekFile(pFile, *offset, SEEK_SET) < 0){
    uError("failed to seek file:%p code: %d", pFile, errno);
    return NULL;
  }

  ASSERT(size > *offset);
  char* pCont = NULL;
  int64_t totalSize = 0;
  if (size - *offset >= SLOW_LOG_SEND_SIZE_MAX) {
    pCont = taosMemoryCalloc(1, 2 * SLOW_LOG_SEND_SIZE_MAX);
    totalSize = 2 * SLOW_LOG_SEND_SIZE_MAX;
  }else{
    pCont = taosMemoryCalloc(1, 2 * (size - *offset));
    totalSize = 2 * (size - *offset);
  }

  if(pCont == NULL){
    uError("failed to allocate memory for slow log, size:%" PRId64, totalSize);
    return NULL;
  }
  char*  buf = pCont;
  strcat(buf++, "[");
  int64_t readSize = taosReadFile(pFile, buf, SLOW_LOG_SEND_SIZE_MAX);
  if (readSize <= 0) {
    if (readSize < 0){
      uError("failed to read len from file:%p since %s", pFile, terrstr());
    }
    taosMemoryFree(pCont);
    return NULL;
  }

  totalSize = 0;
  while(1){
    size_t len = strlen(buf);
    totalSize += (len+1);
    if (totalSize > readSize || len == 0) {
      *(buf-1) = ']';
      *buf = '\0';
      break;
    }
    buf[len] = ','; // replace '\0' with ','
    buf += (len + 1);
    *offset += (len+1);
  }

  uDebug("[monitor] readFile slow log end, data:%s, offset:%"PRId64, pCont, *offset);
  return pCont;
}

static int64_t getFileSize(char* path){
  int64_t fileSize = 0;
  if (taosStatFile(path, &fileSize, NULL, NULL) < 0) {
    return -1;
  }

  return fileSize;
}

static int32_t sendSlowLog(int64_t clusterId, char* data, TdFilePtr pFile, int64_t offset, SLOW_LOG_QUEUE_TYPE type, char* fileName, void* pTransporter, SEpSet *epSet){
  MonitorSlowLogData* pParam = taosMemoryMalloc(sizeof(MonitorSlowLogData));
  if(pParam == NULL){
    return -1;
  }
  pParam->data = data;
  pParam->offset = offset;
  pParam->clusterId = clusterId;
  pParam->type = type;
  pParam->pFile = pFile;
  pParam->fileName = fileName;
  return sendReport(pTransporter, epSet, data, MONITOR_TYPE_SLOW_LOG, pParam);
}

static void monitorSendSlowLogAtBeginning(int64_t clusterId, char* fileName, TdFilePtr pFile, int64_t offset, void* pTransporter, SEpSet *epSet){
  int64_t  size = getFileSize(fileName);
  if(size <= offset){
    taosFtruncateFile(pFile, 0);
    taosUnLockFile(pFile);
    taosCloseFile(&pFile);
    taosRemoveFile(fileName);
    uDebug("[monitor] monitorSendSlowLogAtBeginning delete file:%s", fileName);
  }else{
    char* data = readFile(pFile, &offset, size);
    if(data != NULL){
      sendSlowLog(clusterId, data, pFile, offset, SLOW_LOG_READ_BEGINNIG, taosStrdup(fileName), pTransporter, epSet);
    }
    uDebug("[monitor] monitorSendSlowLogAtBeginning send slow log file:%p, data:%s", pFile, data);
    taosMemoryFree(data);
  }
}

static void monitorSendSlowLogAtRunning(int64_t clusterId){
  void* tmp = taosHashGet(monitorSlowLogHash, &clusterId, LONG_BYTES);
  if (tmp == NULL){
    return;
  }
  SlowLogClient* pClient = (*(SlowLogClient**)tmp);
  if (pClient == NULL){
    return;
  }
  int64_t  size = getFileSize(pClient->path);
  if(size <= pClient->offset){
    if(taosFtruncateFile(pClient->pFile, 0) < 0){
      uError("failed to truncate file:%p code: %d", pClient->pFile, errno);
    }
    uDebug("[monitor] monitorSendSlowLogAtRunning truncate file to 0 file:%p", pClient->pFile);
    pClient->offset = 0;
  }else{
    SAppInstInfo* pInst = getAppInstByClusterId(clusterId);
    if(pInst == NULL){
      uError("failed to get app instance by clusterId:%" PRId64, clusterId);
      return;
    }
    SEpSet ep = getEpSet_s(&pInst->mgmtEp);
    char* data = readFile(pClient->pFile, &pClient->offset, size);
    if(data != NULL){
      sendSlowLog(clusterId, data, pClient->pFile, pClient->offset, SLOW_LOG_READ_RUNNING, NULL, pInst->pTransporter, &ep);
    }
    uDebug("[monitor] monitorSendSlowLogAtRunning send slow log:%s", data);
    taosMemoryFree(data);
  }
}

static bool monitorSendSlowLogAtQuit(int64_t clusterId) {
  void* tmp = taosHashGet(monitorSlowLogHash, &clusterId, LONG_BYTES);
  if (tmp == NULL){
    return true;
  }
  SlowLogClient* pClient = (*(SlowLogClient**)tmp);
  if (pClient == NULL){
    return true;
  }
  int64_t size = getFileSize(pClient->path);
  if(size <= pClient->offset){
    taosUnLockFile(pClient->pFile);
    taosCloseFile(&(pClient->pFile));
    taosRemoveFile(pClient->path);
    uInfo("[monitor] monitorSendSlowLogAtQuit remove file:%s", pClient->path);
    if((--quitCnt) == 0){
      return true;
    }
  }else{
    SAppInstInfo* pInst = getAppInstByClusterId(clusterId);
    if(pInst == NULL) {
      return true;
    }
    SEpSet ep = getEpSet_s(&pInst->mgmtEp);
    char* data = readFile(pClient->pFile, &pClient->offset, size);
    if(data != NULL){
      sendSlowLog(clusterId, data, pClient->pFile, pClient->offset, SLOW_LOG_READ_QUIT, NULL, pInst->pTransporter, &ep);
    }
    uInfo("[monitor] monitorSendSlowLogAtQuit send slow log:%s", data);
    taosMemoryFree(data);
  }
  return false;
}
static void   monitorSendAllSlowLogAtQuit(){
  void* pIter = NULL;
  while ((pIter = taosHashIterate(monitorSlowLogHash, pIter))) {
    SlowLogClient* pClient = (*(SlowLogClient**)pIter);
    if(pClient == NULL) {
      taosHashCancelIterate(monitorSlowLogHash, pIter);
      return;
    }
    int64_t size = getFileSize(pClient->path);
    if(size <= pClient->offset){
      taosUnLockFile(pClient->pFile);
      taosCloseFile(&(pClient->pFile));
      taosRemoveFile(pClient->path);
    }else if(pClient->offset == 0){
      int64_t* clusterId = (int64_t*)taosHashGetKey(pIter, NULL);
      SAppInstInfo* pInst = getAppInstByClusterId(*clusterId);
      if(pInst == NULL) {
        taosHashCancelIterate(monitorSlowLogHash, pIter);
        return;
      }
      SEpSet ep = getEpSet_s(&pInst->mgmtEp);
      char* data = readFile(pClient->pFile, &pClient->offset, size);
      if(data != NULL && sendSlowLog(*clusterId, data, NULL, pClient->offset, SLOW_LOG_READ_QUIT, NULL, pInst->pTransporter, &ep) == 0){
        quitCnt ++;
      }
      uInfo("[monitor] monitorSendAllSlowLogAtQuit send slow log :%s", data);
      taosMemoryFree(data);
    }
  }
}

static void processFileRemoved(SlowLogClient* pClient){
  taosUnLockFile(pClient->pFile);
  taosCloseFile(&(pClient->pFile));

  TdFilePtr pFile = taosOpenFile(pClient->path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to open file:%s since %s", pClient->path, terrstr());
  }else{
    pClient->pFile = pFile;
  }
}

static void monitorSendAllSlowLog(){
  int64_t t = taosGetMonoTimestampMs();
  void* pIter = NULL;
  while ((pIter = taosHashIterate(monitorSlowLogHash, pIter))) {
    int64_t*       clusterId = (int64_t*)taosHashGetKey(pIter, NULL);
    SAppInstInfo*  pInst = getAppInstByClusterId(*clusterId);
    SlowLogClient* pClient = (*(SlowLogClient**)pIter);
    if (pClient == NULL){
      taosHashCancelIterate(monitorSlowLogHash, pIter);
      return;
    }
    if (t - pClient->lastCheckTime > pInst->monitorParas.tsMonitorInterval * 1000){
      pClient->lastCheckTime = t;
    } else {
      continue;
    }

    if (pInst != NULL && pClient->offset == 0) {
      int64_t size = getFileSize(pClient->path);
      if(size <= 0){
        if(size < 0){
          uError("[monitor] monitorSendAllSlowLog failed to get file size:%s, err:%d", pClient->path, errno);
          if(errno == ENOENT){
            processFileRemoved(pClient);
          }
        }
        continue;
      }
      SEpSet ep = getEpSet_s(&pInst->mgmtEp);
      char* data = readFile(pClient->pFile, &pClient->offset, size);
      if(data != NULL){
        sendSlowLog(*clusterId, data, NULL, pClient->offset, SLOW_LOG_READ_RUNNING, NULL, pInst->pTransporter, &ep);
      }
      uDebug("[monitor] monitorSendAllSlowLog send slow log :%s", data);
      taosMemoryFree(data);
    }
  }
}

static void monitorSendAllSlowLogFromTempDir(int64_t clusterId){
  SAppInstInfo* pInst = getAppInstByClusterId((int64_t)clusterId);

  if(pInst == NULL || !pInst->monitorParas.tsEnableMonitor){
    uInfo("[monitor] monitor is disabled, skip send slow log");
    return;
  }
  char namePrefix[PATH_MAX] = {0};
  if (snprintf(namePrefix, sizeof(namePrefix), "%s%"PRIx64, TD_TMP_FILE_PREFIX, pInst->clusterId) < 0) {
    uError("failed to generate slow log file name prefix");
    return;
  }

  char          tmpPath[PATH_MAX] = {0};
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

    char *name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 ||
        strcmp(name, "..") == 0 ||
        strstr(name, namePrefix) == NULL) {
      uInfo("skip file:%s, for cluster id:%"PRIx64, name, pInst->clusterId);
      continue;
    }

    char filename[PATH_MAX] = {0};
    snprintf(filename, sizeof(filename), "%s%s", tmpPath, name);
    TdFilePtr pFile = taosOpenFile(filename, TD_FILE_READ | TD_FILE_WRITE);
    if (pFile == NULL) {
      uError("failed to open file:%s since %s", filename, terrstr());
      continue;
    }
    if (taosLockFile(pFile) < 0) {
      uError("failed to lock file:%s since %s, maybe used by other process", filename, terrstr());
      taosCloseFile(&pFile);
      continue;
    }
    SEpSet ep = getEpSet_s(&pInst->mgmtEp);
    monitorSendSlowLogAtBeginning(pInst->clusterId, filename, pFile, 0, pInst->pTransporter, &ep);
  }

  taosCloseDir(&pDir);
}

static void* monitorThreadFunc(void *param){
  setThreadName("client-monitor-slowlog");

#ifdef WINDOWS
  if (taosCheckCurrentInDll()) {
    atexit(monitorThreadFuncUnexpectedStopped);
  }
#endif

  char tmpPath[PATH_MAX] = {0};
  if (getSlowLogTmpDir(tmpPath, sizeof(tmpPath)) < 0){
    return NULL;
  }

  if (taosMulModeMkDir(tmpPath, 0777, true) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    printf("failed to create dir:%s since %s", tmpPath, terrstr());
    return NULL;
  }

  if (tsem2_init(&monitorSem, 0, 0) != 0) {
    uError("sem init error since %s", terrstr());
    return NULL;
  }

  monitorQueue = taosOpenQueue();
  if(monitorQueue == NULL){
    uError("open queue error since %s", terrstr());
    return NULL;
  }

  if (-1 != atomic_val_compare_exchange_32(&slowLogFlag, -1, 0)) {
    return NULL;
  }
  uDebug("monitorThreadFunc start");
  int64_t     quitTime = 0;
  while (1) {
    if (slowLogFlag > 0) {
      if(quitCnt == 0){
        monitorSendAllSlowLogAtQuit();
        if(quitCnt == 0){
          uInfo("monitorThreadFunc quit since no slow log to send");
          break;
        }
        quitTime = taosGetMonoTimestampMs();
      }
      if(taosGetMonoTimestampMs() - quitTime > 500){   //quit at most 500ms
        uInfo("monitorThreadFunc quit since timeout")
        break;
      }
    }

    MonitorSlowLogData* slowLogData = NULL;
    taosReadQitem(monitorQueue, (void**)&slowLogData);
    if (slowLogData != NULL) {
      if (slowLogData->type == SLOW_LOG_READ_BEGINNIG){
        if(slowLogData->pFile != NULL){
          SAppInstInfo* pInst = getAppInstByClusterId(slowLogData->clusterId);
          if(pInst != NULL) {
            SEpSet ep = getEpSet_s(&pInst->mgmtEp);
            monitorSendSlowLogAtBeginning(slowLogData->clusterId, slowLogData->fileName, slowLogData->pFile, slowLogData->offset, pInst->pTransporter, &ep);
          }
        }else{
          monitorSendAllSlowLogFromTempDir(slowLogData->clusterId);
        }
      } else if(slowLogData->type == SLOW_LOG_WRITE){
        monitorWriteSlowLog2File(slowLogData, tmpPath);
      } else if(slowLogData->type == SLOW_LOG_READ_RUNNING){
        monitorSendSlowLogAtRunning(slowLogData->clusterId);
      }else if(slowLogData->type == SLOW_LOG_READ_QUIT){
        if(monitorSendSlowLogAtQuit(slowLogData->clusterId)){
          uInfo("monitorThreadFunc quit since all slow log sended");
          monitorFreeSlowLogData(slowLogData);
          taosFreeQitem(slowLogData);
          break;
        }
      }
    }
    monitorFreeSlowLogData(slowLogData);
    taosFreeQitem(slowLogData);

    if (quitCnt == 0) {
      monitorSendAllSlowLog();
    }
    tsem2_timewait(&monitorSem, 100);
  }

  taosCloseQueue(monitorQueue);
  tsem2_destroy(&monitorSem);
  slowLogFlag = -2;
  return NULL;
}

static int32_t tscMonitortInit() {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  TdThread monitorThread;
  if (taosThreadCreate(&monitorThread, &thAttr, monitorThreadFunc, NULL) != 0) {
    uError("failed to create monitor thread since %s", strerror(errno));
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  return 0;
}

static void tscMonitorStop() {
  if (atomic_val_compare_exchange_32(&slowLogFlag, 0, 1)) {
    uDebug("monitor thread already stopped");
    return;
  }

  while (atomic_load_32(&slowLogFlag) > 0) {
    taosMsleep(100);
  }
}

void monitorInit() {
  uInfo("[monitor] tscMonitor init");
  monitorCounterHash = (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (monitorCounterHash == NULL) {
    uError("failed to create monitorCounterHash");
  }
  taosHashSetFreeFp(monitorCounterHash, destroyMonitorClient);

  monitorSlowLogHash = (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (monitorSlowLogHash == NULL) {
    uError("failed to create monitorSlowLogHash");
  }
  taosHashSetFreeFp(monitorSlowLogHash, destroySlowLogClient);

  monitorTimer = taosTmrInit(0, 0, 0, "MONITOR");
  if (monitorTimer == NULL) {
    uError("failed to create monitor timer");
  }

  taosInitRWLatch(&monitorLock);
  tscMonitortInit();
}

void monitorClose() {
  uInfo("[monitor] tscMonitor close");
  taosWLockLatch(&monitorLock);

  if (atomic_val_compare_exchange_32(&monitorFlag, 0, 1)) {
    uDebug("[monitor] monitorFlag is not 0");
  }
  tscMonitorStop();
  sendAllCounter();
  taosHashCleanup(monitorCounterHash);
  taosHashCleanup(monitorSlowLogHash);
  taosTmrCleanUp(monitorTimer);
  taosWUnLockLatch(&monitorLock);
}

int32_t monitorPutData2MonitorQueue(MonitorSlowLogData data){
  MonitorSlowLogData* slowLogData = taosAllocateQitem(sizeof(MonitorSlowLogData), DEF_QITEM, 0);
  if (slowLogData == NULL) {
    uError("[monitor] failed to allocate slow log data");
    return -1;
  }
  *slowLogData = data;
  uDebug("[monitor] write slow log to queue, clusterId:%"PRIx64 " type:%d", slowLogData->clusterId, slowLogData->type);
  while (atomic_load_32(&slowLogFlag) == -1) {
    taosMsleep(5);
  }
  if (taosWriteQitem(monitorQueue, slowLogData) == 0){
    tsem2_post(&monitorSem);
  }else{
    monitorFreeSlowLogData(slowLogData);
    taosFreeQitem(slowLogData);
  }
  return 0;
}