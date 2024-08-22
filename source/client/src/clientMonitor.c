#include "clientMonitor.h"
#include "os.h"
#include "tmisce.h"
#include "ttime.h"
#include "ttimer.h"
#include "tglobal.h"
#include "clientInt.h"

SRWLatch  monitorLock;
void*     tmrClientMonitor;
tmr_h     tmrStartHandle;
SHashObj* clusterMonitorInfoTable;

static int sendBathchSize = 1;
static bool clientMonitorStopping = false;
static tsem_t  waitExitSem;

int32_t sendReport(ClientMonitor* pMonitor, char* pCont, void* param);
void    generateClusterReport(ClientMonitor* pMonitor, bool send, void* param) {
  char ts[50];
  sprintf(ts, "%" PRId64, taosGetTimestamp(TSDB_TIME_PRECISION_MILLI));
  char* pCont = (char*)taos_collector_registry_bridge_new(pMonitor->registry, ts, "%" PRId64, NULL);
  if(NULL == pCont) {
    uError("[monitor] generateClusterReport failed, get null content.");
    return;
  }
  if (send && strlen(pCont) != 0) {
    if (sendReport(pMonitor, pCont, param) == 0) {
      taos_collector_registry_clear_batch(pMonitor->registry);
    }
  }
  taosMemoryFreeClear(pCont);
}

void reportSendProcess(void* pExitSem, void* tmrId) {
  if (!clientMonitorStopping) {
    taosTmrReset(reportSendProcess, tsMonitorInterval * 1000, NULL, tmrClientMonitor, &tmrStartHandle);
  } else if (pExitSem != NULL) {
    uInfo("[monitor] reportSendProcess, send last report before exit.");
  } else {
    return;
  }
  taosRLockLatch(&monitorLock);

  static int index = 0;
  index++;
  ClientMonitor** ppMonitor = (ClientMonitor**)taosHashIterate(clusterMonitorInfoTable, NULL);
  while (ppMonitor != NULL && *ppMonitor != NULL) {
    ClientMonitor* pMonitor = *ppMonitor;
    generateClusterReport(*ppMonitor, index == sendBathchSize, pExitSem);
    ppMonitor = taosHashIterate(clusterMonitorInfoTable, ppMonitor);
  }

  if (index == sendBathchSize) index = 0;
  taosRUnLockLatch(&monitorLock);
}

void monitorClientInitOnce() {
  static int8_t init = 0;
  tsem_init(&waitExitSem, 0, 0);
  if (atomic_exchange_8(&init, 1) == 0) {
    uInfo("[monitor] tscMonitorInit once.");
    clusterMonitorInfoTable =
        (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);

    tmrClientMonitor = taosTmrInit(0, 0, 0, "MONITOR");
    tmrStartHandle = taosTmrStart(reportSendProcess, tsMonitorInterval * 1000, NULL, tmrClientMonitor);

    taosInitRWLatch(&monitorLock);
  }
}

void createMonitorClient(const char* clusterKey) {
  if (clusterKey == NULL || strlen(clusterKey) ==  0) {
    uError("[monitor] createMonitorClient failed, clusterKey is NULL");
    return;
  }
  taosWLockLatch(&monitorLock);
  ClientMonitor** ppMonitor = (ClientMonitor**)taosHashGet(clusterMonitorInfoTable, clusterKey, strlen(clusterKey));
  if (ppMonitor == NULL) {
    uInfo("[monitor] createMonitorClient for %s.", clusterKey);
    ClientMonitor* pMonitor = taosMemoryCalloc(1, sizeof(ClientMonitor));
    snprintf(pMonitor->clusterKey, sizeof(pMonitor->clusterKey), "%s", clusterKey);
    pMonitor->registry = taos_collector_registry_new(clusterKey);
    pMonitor->colector = taos_collector_new(clusterKey);

    taos_collector_registry_register_collector(pMonitor->registry, pMonitor->colector);
    pMonitor->counters =
        (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);

    taosHashPut(clusterMonitorInfoTable, clusterKey, strlen(clusterKey), &pMonitor, sizeof(ClientMonitor*));
    uInfo("[monitor] createMonitorClient for %s finished %p.", clusterKey, pMonitor);
  }
  
  taosWUnLockLatch(&monitorLock);
}

static int32_t monitorReportAsyncCB(void* param, SDataBuf* pMsg, int32_t code) {
  static int32_t emptyRspNum = 0;
  if (TSDB_CODE_SUCCESS != code) {
    uError("[monitor] found error in monitorReport send callback, code:%d, please check the network.", code);
  }
  if (pMsg) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
  }
  if(param != NULL) {
    tsem_post((tsem_t*)param);
    uInfo("[monitor] get last response, post sem for exit.");
  }
  return code;
}

int32_t sendReport(ClientMonitor* pMonitor, char* pCont, void* param) {
  SStatisReq sStatisReq;
  sStatisReq.pCont = pCont;
  sStatisReq.contLen = strlen(pCont);

  int tlen = tSerializeSStatisReq(NULL, 0, &sStatisReq);
  if (tlen < 0) return 0;
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSStatisReq(buf, tlen, &sStatisReq);

  SMsgSendInfo* pInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pInfo->fp = monitorReportAsyncCB;
  pInfo->msgInfo.pData = buf;
  pInfo->msgInfo.len = tlen;
  pInfo->msgType = TDMT_MND_STATIS;
  pInfo->param = param;
  pInfo->paramFreeFp = NULL;
  pInfo->requestId = tGenIdPI64();
  pInfo->requestObjRefId = 0;

  int64_t transporterId = 0;
  SAppInstInfo* pAppInstInfo = getAppInstInfo(pMonitor->clusterKey);
  return asyncSendMsgToServer(pAppInstInfo->pTransporter, &pAppInstInfo->mgmtEp.epSet, &transporterId, pInfo);
}

void clusterMonitorInit(const char* clusterKey) {
  monitorClientInitOnce();
  createMonitorClient(clusterKey);
}

taos_counter_t* createClusterCounter(const char* clusterKey, const char* name, const char* help, size_t label_key_count,
                                     const char** label_keys) {
  ClientMonitor** ppMonitor = (ClientMonitor**)taosHashGet(clusterMonitorInfoTable, clusterKey, strlen(clusterKey));

  if (ppMonitor != NULL && *ppMonitor != NULL) {
    ClientMonitor*   pMonitor = *ppMonitor;
    taos_counter_t** ppCounter = (taos_counter_t**)taosHashGet(pMonitor->counters, name, strlen(name));
    if (ppCounter != NULL && *ppCounter != NULL) {
      taosHashRemove(pMonitor->counters, name, strlen(name));
      uInfo("[monitor] createClusterCounter remove old counter: %s.", name);
    }

    taos_counter_t* newCounter = taos_counter_new(name, help, label_key_count, label_keys);
    if (newCounter != NULL) {
      taos_collector_add_metric(pMonitor->colector, newCounter);
      taosHashPut(pMonitor->counters, name, strlen(name), &newCounter, sizeof(taos_counter_t*));
      uInfo("[monitor] createClusterCounter %s(%p):%s : %p.", pMonitor->clusterKey, pMonitor, name, newCounter);
      return newCounter;
    } else {
      return NULL;
    }
  } else {
    return NULL;
  }
  return NULL;
}

int taosClusterCounterInc(const char* clusterKey, const char* counterName, const char** label_values) {
  taosRLockLatch(&monitorLock);
  ClientMonitor** ppMonitor = (ClientMonitor**)taosHashGet(clusterMonitorInfoTable, clusterKey, strlen(clusterKey));

  if (ppMonitor != NULL && *ppMonitor != NULL) {
    ClientMonitor*   pMonitor = *ppMonitor;
    taos_counter_t** ppCounter = (taos_counter_t**)taosHashGet(pMonitor->counters, counterName, strlen(counterName));
    if (ppCounter != NULL && *ppCounter != NULL) {
      taos_counter_inc(*ppCounter, label_values);
    } else {
      uError("[monitor] taosClusterCounterInc not found pCounter %s:%s.", clusterKey, counterName);
    }
  } else {
    uError("[monitor] taosClusterCounterInc not found pMonitor %s.", clusterKey);
  }
  taosRUnLockLatch(&monitorLock);
  return 0;
}

void closeAllClientMonitor() {
  taosWLockLatch(&monitorLock);

  ClientMonitor** ppMonitor = (ClientMonitor**)taosHashIterate(clusterMonitorInfoTable, NULL);
  while (ppMonitor != NULL && *ppMonitor != NULL) {
    ClientMonitor* pMonitor = *ppMonitor;
    uInfo("[monitor] clusterMonitorClose valule:%p  clusterKey:%s.", pMonitor, pMonitor->clusterKey);
    taosHashCleanup(pMonitor->counters);
    taos_collector_registry_destroy(pMonitor->registry);
    taosMemoryFree(pMonitor);

    ppMonitor = taosHashIterate(clusterMonitorInfoTable, ppMonitor);
  }
  taosHashCleanup(clusterMonitorInfoTable);

  taosWUnLockLatch(&monitorLock);
}

const char* resultStr(SQL_RESULT_CODE code) {
  static const char* result_state[] = {"Success", "Failed", "Cancel"};
  return result_state[code];
}

void cluster_monitor_stop() {
  uInfo("[monitor] tscMonitor close");
  clientMonitorStopping = true;
  reportSendProcess(&waitExitSem, NULL);
  tsem_timewait(&waitExitSem, 2000);
  closeAllClientMonitor();
  uInfo("[monitor] tscMonitor close finished.");
}
