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
  if(slowLogClient == NULL){
    return;
  }
  taosTmrStopA(&(*(SlowLogClient**)data)->timer);

  TdFilePtr pFile = slowLogClient->pFile;
  if(pFile == NULL){
    taosMemoryFree(slowLogClient);
    return;
  }

  taosUnLockFile(pFile);
  taosCloseFile(&pFile);
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
  return code;
}

static int32_t sendReport(void* pTransporter, SEpSet *epSet, char* pCont, MONITOR_TYPE type) {
  SStatisReq sStatisReq;
  sStatisReq.pCont = pCont;
  sStatisReq.contLen = strlen(pCont);
  sStatisReq.type = type;

  int tlen = tSerializeSStatisReq(NULL, 0, &sStatisReq);
  if (tlen < 0) return 0;
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    uError("sendReport failed, out of memory, len:%d", tlen);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSStatisReq(buf, tlen, &sStatisReq);

  SMsgSendInfo* pInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pInfo == NULL) {
    uError("sendReport failed, out of memory send info");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pInfo->fp = monitorReportAsyncCB;
  pInfo->msgInfo.pData = buf;
  pInfo->msgInfo.len = tlen;
  pInfo->msgType = TDMT_MND_STATIS;
  // pInfo->param = taosMemoryMalloc(sizeof(int32_t));
  // *(int32_t*)pInfo->param = i;
  pInfo->paramFreeFp = taosMemoryFree;
  pInfo->requestId = tGenIdPI64();
  pInfo->requestObjRefId = 0;

  int64_t transporterId = 0;
  int32_t code = asyncSendMsgToServer(pTransporter, epSet, &transporterId, pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    uError("sendReport failed, code:%d", code);
  }
  return code;
}

static void monitorReadSendSlowLog(TdFilePtr pFile, void* pTransporter, SEpSet *epSet){
  char buf[SLOW_LOG_SEND_SIZE + 1] = {0};   // +1 for \0, for print log
  char pCont[SLOW_LOG_SEND_SIZE + 1] = {0};   // +1 for \0, for print log
  int32_t offset = 0;
  if(taosLSeekFile(pFile, 0, SEEK_SET) < 0){
    uError("failed to seek file:%p code: %d", pFile, errno);
    return;
  }
  while(1){
    int64_t readSize = taosReadFile(pFile, buf + offset, SLOW_LOG_SEND_SIZE - offset);
    if (readSize <= 0) {
      uError("failed to read len from file:%p since %s", pFile, terrstr());
      return;
    }

    memset(pCont, 0, sizeof(pCont));
    strcat(pCont, "[");
    char* string = buf;
    for(int i = 0; i < readSize + offset; i++){
      if (buf[i] == '\0') {
        if (string != buf) strcat(pCont, ",");
        strcat(pCont, string);
        uDebug("[monitor] monitorReadSendSlowLog slow log:%s", string);
        string = buf + i + 1;
      }
    }
    strcat(pCont, "]");
    if (pTransporter && pCont != NULL) {
      if(sendReport(pTransporter, epSet, pCont, MONITOR_TYPE_SLOW_LOG) != 0){
        if(taosLSeekFile(pFile, -readSize, SEEK_CUR) < 0){
          uError("failed to seek file:%p code: %d", pFile, errno);
        }
        uError("failed to send report:%s", pCont);
        return;
      }
      uDebug("[monitor] monitorReadSendSlowLog send slow log to mnode:%s", pCont)
    }

    if (readSize + offset < SLOW_LOG_SEND_SIZE) {
      break;
    }
    offset = SLOW_LOG_SEND_SIZE - (string - buf);
    if(buf != string && offset != 0){
      memmove(buf, string, offset);
      uDebug("[monitor] monitorReadSendSlowLog left slow log:%s", buf)
    }
  }
  if(taosFtruncateFile(pFile, 0) < 0){
    uError("failed to truncate file:%p code: %d", pFile, errno);
  }
  uDebug("[monitor] monitorReadSendSlowLog send slow log file:%p", pFile);
}

static void generateClusterReport(taos_collector_registry_t* registry, void* pTransporter, SEpSet *epSet) {
  char ts[50] = {0};
  sprintf(ts, "%" PRId64, taosGetTimestamp(TSDB_TIME_PRECISION_MILLI));
  char* pCont = (char*)taos_collector_registry_bridge_new(registry, ts, "%" PRId64, NULL);
  if(NULL == pCont) {
    uError("generateClusterReport failed, get null content.");
    return;
  }

  if (strlen(pCont) != 0 && sendReport(pTransporter, epSet, pCont, MONITOR_TYPE_COUNTER) == 0) {
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
  taosRUnLockLatch(&monitorLock);
  taosTmrReset(reportSendProcess, pInst->monitorParas.tsMonitorInterval * 1000, param, monitorTimer, &tmrId);
}

static void sendAllSlowLog(){
  void* data = taosHashIterate(monitorSlowLogHash, NULL);
  while (data != NULL) {
    TdFilePtr pFile = (*(SlowLogClient**)data)->pFile;
    if (pFile != NULL){
      int64_t clusterId = *(int64_t*)taosHashGetKey(data, NULL);
      SAppInstInfo* pInst = getAppInstByClusterId(clusterId);
      if(pInst == NULL){
        taosHashCancelIterate(monitorSlowLogHash, data);
        break;
      }
      SEpSet ep = getEpSet_s(&pInst->mgmtEp);
      monitorReadSendSlowLog(pFile, pInst->pTransporter, &ep);
    }
    data = taosHashIterate(monitorSlowLogHash, data);
  }
  uDebug("[monitor] sendAllSlowLog when client close");
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

  taosRLockLatch(&monitorLock);

  char          tmpPath[PATH_MAX] = {0};
  if (getSlowLogTmpDir(tmpPath, sizeof(tmpPath)) < 0) {
    goto END;
  }

  TdDirPtr pDir = taosOpenDir(tmpPath);
  if (pDir == NULL) {
    goto END;
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
    TdFilePtr pFile = taosOpenFile(filename, TD_FILE_READ);
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
    monitorReadSendSlowLog(pFile, pInst->pTransporter, &ep);
    taosUnLockFile(pFile);
    taosCloseFile(&pFile);
    taosRemoveFile(filename);
    uDebug("[monitor] send and delete slow log file when reveive connect rsp:%s", filename);

  }

  taosCloseDir(&pDir);

END:
  taosRUnLockLatch(&monitorLock);
}

static void sendAllCounter(){
  MonitorClient** ppMonitor = (MonitorClient**)taosHashIterate(monitorCounterHash, NULL);
  while (ppMonitor != NULL) {
    MonitorClient* pMonitor = *ppMonitor;
    if (pMonitor != NULL){
      SAppInstInfo* pInst = getAppInstByClusterId(pMonitor->clusterId);
      if(pInst == NULL){
        taosHashCancelIterate(monitorCounterHash, ppMonitor);
        break;
      }
      SEpSet ep = getEpSet_s(&pInst->mgmtEp);
      generateClusterReport(pMonitor->registry, pInst->pTransporter, &ep);
    }
    ppMonitor = taosHashIterate(monitorCounterHash, ppMonitor);
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
  if (ppCounter == NULL || *ppCounter != NULL) {
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

static void monitorFreeSlowLogData(MonitorSlowLogData* pData) {
  if (pData == NULL) {
    return;
  }
  taosMemoryFree(pData->value);
}

static void monitorThreadFuncUnexpectedStopped(void) { atomic_store_32(&slowLogFlag, -1); }

static void reportSlowLog(void* param, void* tmrId) {
  taosRLockLatch(&monitorLock);
  if (atomic_load_32(&monitorFlag) == 1) {
    taosRUnLockLatch(&monitorLock);
    return;
  }
  SAppInstInfo* pInst = getAppInstByClusterId((int64_t)param);
  if(pInst == NULL){
    uError("failed to get app inst, clusterId:%"PRIx64, (int64_t)param);
    taosRUnLockLatch(&monitorLock);
    return;
  }

  void* tmp = taosHashGet(monitorSlowLogHash, &param, LONG_BYTES);
  if(tmp == NULL){
    uError("failed to get file inst, clusterId:%"PRIx64, (int64_t)param);
    taosRUnLockLatch(&monitorLock);
    return;
  }

  SEpSet ep = getEpSet_s(&pInst->mgmtEp);
  monitorReadSendSlowLog((*(SlowLogClient**)tmp)->pFile, pInst->pTransporter, &ep);
  taosRUnLockLatch(&monitorLock);

  taosTmrReset(reportSlowLog, pInst->monitorParas.tsMonitorInterval * 1000, param, monitorTimer, &tmrId);
}

static void monitorWriteSlowLog2File(MonitorSlowLogData* slowLogData, char *tmpPath){
  taosWLockLatch(&monitorLock);
  TdFilePtr pFile = NULL;
  void* tmp = taosHashGet(monitorSlowLogHash, &slowLogData->clusterId, LONG_BYTES);
  if (tmp == NULL){
    char path[PATH_MAX] = {0};
    char clusterId[32] = {0};
    if (snprintf(clusterId, sizeof(clusterId), "%" PRIx64, slowLogData->clusterId) < 0){
      uError("failed to generate clusterId:%" PRIx64, slowLogData->clusterId);
      goto FAILED;
    }
    taosGetTmpfilePath(tmpPath, clusterId, path);
    uInfo("[monitor] create slow log file:%s", path);
    pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
    if (pFile == NULL) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      uError("failed to open file:%s since %s", path, terrstr());
      goto FAILED;
    }

    SlowLogClient *pClient = taosMemoryCalloc(1, sizeof(SlowLogClient));
    if (pClient == NULL){
      uError("failed to allocate memory for slow log client");
      taosCloseFile(&pFile);
      goto FAILED;
    }
    pClient->pFile = pFile;
    if (taosHashPut(monitorSlowLogHash, &slowLogData->clusterId, LONG_BYTES, &pClient, POINTER_BYTES) != 0){
      uError("failed to put clusterId:%" PRId64 " to hash table", slowLogData->clusterId);
      taosCloseFile(&pFile);
      taosMemoryFree(pClient);
      goto FAILED;
    }

    if(taosLockFile(pFile) < 0){
      uError("failed to lock file:%p since %s", pFile, terrstr());
      goto FAILED;
    }

    SAppInstInfo* pInst = getAppInstByClusterId(slowLogData->clusterId);
    if(pInst == NULL){
      uError("failed to get app instance by clusterId:%" PRId64, slowLogData->clusterId);
      goto FAILED;
    }

    pClient->timer = taosTmrStart(reportSlowLog, pInst->monitorParas.tsMonitorInterval * 1000, (void*)slowLogData->clusterId, monitorTimer);
  }else{
    pFile = (*(SlowLogClient**)tmp)->pFile;
  }

  if (taosWriteFile(pFile, slowLogData->value, strlen(slowLogData->value) + 1) < 0){
    uError("failed to write len to file:%p since %s", pFile, terrstr());
  }
  uDebug("[monitor] write slow log to file:%p, clusterId:%"PRIx64, pFile, slowLogData->clusterId);

FAILED:
  taosWUnLockLatch(&monitorLock);
}

static void* monitorThreadFunc(void *param){
  setThreadName("client-monitor-slowlog");

#ifdef WINDOWS
  if (taosCheckCurrentInDll()) {
    atexit(monitorThreadFuncUnexpectedStopped);
  }
#endif

  if (-1 != atomic_val_compare_exchange_32(&slowLogFlag, -1, 0)) {
    return NULL;
  }

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
  uDebug("monitorThreadFunc start");
  while (1) {
    if (slowLogFlag > 0) break;

    MonitorSlowLogData* slowLogData = NULL;
    taosReadQitem(monitorQueue, (void**)&slowLogData);
    if (slowLogData != NULL) {
      uDebug("[monitor] read slow log data from queue, clusterId:%" PRIx64 " value:%s", slowLogData->clusterId, slowLogData->value);
      if (slowLogData->value == NULL){
        monitorSendAllSlowLogFromTempDir(slowLogData->clusterId);
      }else{
        monitorWriteSlowLog2File(slowLogData, tmpPath);
      }
    }
    monitorFreeSlowLogData(slowLogData);
    taosFreeQitem(slowLogData);
    tsem2_timewait(&monitorSem, 500);
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
  sendAllSlowLog();
  sendAllCounter();
  taosHashCleanup(monitorCounterHash);
  taosHashCleanup(monitorSlowLogHash);
  taosTmrCleanUp(monitorTimer);
  taosWUnLockLatch(&monitorLock);
}

int32_t monitorPutData2MonitorQueue(int64_t clusterId, char* value){
  MonitorSlowLogData* slowLogData = taosAllocateQitem(sizeof(MonitorSlowLogData), DEF_QITEM, 0);
  if (slowLogData == NULL) {
    uError("[monitor] failed to allocate slow log data");
    return -1;
  }
  slowLogData->clusterId = clusterId;
  slowLogData->value = value;
  uDebug("[monitor] write slow log to queue, clusterId:%"PRIx64 " value:%s", slowLogData->clusterId, slowLogData->value);
  while (monitorQueue == NULL) {
    taosMsleep(100);
  }
  if (taosWriteQitem(monitorQueue, slowLogData) == 0){
    tsem2_post(&monitorSem);
  }else{
    monitorFreeSlowLogData(slowLogData);
    taosFreeQitem(slowLogData);
  }
  return 0;
}