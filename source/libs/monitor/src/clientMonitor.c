#include "clientMonitor.h"
#include "os.h"
#include "tmisce.h"
#include "ttime.h"
#include "ttimer.h"

SRWLatch  monitorLock;
void*     tmrClientMonitor;
tmr_h     tmrStartHandle;
SHashObj* clusterMonitorInfoTable;

static const int interval = 1000;  // ms
static const int sendSize = 10;
static const int sleepTimeMS = 100;

int32_t sendReport(ClientMonitor* pMonitor, char* pCont);
void    generateClusterReport(ClientMonitor* pMonitor, bool send) {
  char ts[50];
  sprintf(ts, "%" PRId64, taosGetTimestamp(TSDB_TIME_PRECISION_MILLI));
  char* pCont = (char*)taos_collector_registry_bridge(pMonitor->registry, ts, "%" PRId64);
  if (send && strlen(pCont) != TSDB_CODE_SUCCESS) {
    if (sendReport(pMonitor, pCont) == 0) {
      taos_collector_registry_clear_out(pMonitor->registry);
    }
  }
}

void reportSendProcess(void* param, void* tmrId) {
  taosRLockLatch(&monitorLock);
  taosTmrReset(reportSendProcess, interval, NULL, tmrClientMonitor, &tmrStartHandle);

  static int index = 0;
  index++;
  ClientMonitor** ppMonitor = (ClientMonitor**)taosHashIterate(clusterMonitorInfoTable, NULL);
  while (ppMonitor != NULL && *ppMonitor != NULL) {
    ClientMonitor* pMonitor = *ppMonitor;
    generateClusterReport(*ppMonitor, index == sendSize);
    ppMonitor = taosHashIterate(clusterMonitorInfoTable, ppMonitor);
  }

  if (index == sendSize) index = 0;
  taosRUnLockLatch(&monitorLock);
}

void monitorClientInitOnce() {
  static int8_t init = 0;
  if (atomic_exchange_8(&init, 1) == 0) {
    uInfo("tscMonitorInit once.");
    clusterMonitorInfoTable =
        (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);

    tmrClientMonitor = taosTmrInit(0, 0, 0, "MONITOR");
    tmrStartHandle = taosTmrStart(reportSendProcess, interval, NULL, tmrClientMonitor);
    taosInitRWLatch(&monitorLock);
  }
}

void createMonitorClient(const char* clusterKey, SEpSet epSet, void* pTransporter) {
  taosWLockLatch(&monitorLock);
  if (clusterKey == NULL || strlen(clusterKey) ==  0) {
    uError("createMonitorClient failed, clusterKey is NULL");
    return;
  }

  if (taosHashGet(clusterMonitorInfoTable, clusterKey, strlen(clusterKey)) == NULL) {
    uInfo("createMonitorClient for %s.", clusterKey);
    ClientMonitor* pMonitor = taosMemoryCalloc(1, sizeof(ClientMonitor));
    snprintf(pMonitor->clusterKey, sizeof(pMonitor->clusterKey), "%s", clusterKey);
    pMonitor->registry = taos_collector_registry_new(clusterKey);
    pMonitor->colector = taos_collector_new(clusterKey);
    pMonitor->epSet = epSet;
    pMonitor->pTransporter = pTransporter;

    taos_collector_registry_register_collector(pMonitor->registry, pMonitor->colector);
    pMonitor->counters =
        (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);

    taosHashPut(clusterMonitorInfoTable, clusterKey, strlen(clusterKey), &pMonitor, sizeof(ClientMonitor*));
    uInfo("createMonitorClient for %s finished %p.", clusterKey, pMonitor);
  }
  taosWUnLockLatch(&monitorLock);
}

static int32_t monitorReportAsyncCB(void* param, SDataBuf* pMsg, int32_t code) {
  static int32_t emptyRspNum = 0;
  if (TSDB_CODE_SUCCESS != code) {
    uError("found error in monitorReport send callback, code:%d, please check the network.", code);
  }
  return code;
}

int32_t sendReport(ClientMonitor* pMonitor, char* pCont) {
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
  // pInfo->param = taosMemoryMalloc(sizeof(int32_t));
  // *(int32_t*)pInfo->param = i;
  pInfo->paramFreeFp = taosMemoryFree;
  pInfo->requestId = tGenIdPI64();
  pInfo->requestObjRefId = 0;

  int64_t transporterId = 0;
  return asyncSendMsgToServer(pMonitor->pTransporter, &pMonitor->epSet, &transporterId, pInfo);
}

void clusterMonitorInit(const char* clusterKey, SEpSet epSet, void* pTransporter) {
  monitorClientInitOnce();
  createMonitorClient(clusterKey, epSet, pTransporter);
}

taos_counter_t* createClusterCounter(const char* clusterKey, const char* name, const char* help, size_t label_key_count,
                                     const char** label_keys) {
  ClientMonitor** ppMonitor = (ClientMonitor**)taosHashGet(clusterMonitorInfoTable, clusterKey, strlen(clusterKey));

  if (ppMonitor != NULL && *ppMonitor != NULL) {
    ClientMonitor*   pMonitor = *ppMonitor;
    taos_counter_t** ppCounter = (taos_counter_t**)taosHashGet(pMonitor->counters, name, strlen(name));
    if (ppCounter != NULL && *ppCounter != NULL) {
      taosHashRemove(pMonitor->counters, name, strlen(name));
      uInfo("createClusterCounter remove old counter: %s.", name);
    }

    taos_counter_t* newCounter = taos_counter_new(name, help, label_key_count, label_keys);
    if (newCounter != NULL) {
      taos_collector_add_metric(pMonitor->colector, newCounter);
      taosHashPut(pMonitor->counters, name, strlen(name), &newCounter, sizeof(taos_counter_t*));
      uInfo("createClusterCounter %s(%p):%s : %p.", pMonitor->clusterKey, pMonitor, name, newCounter);
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
      int res = taos_counter_inc(*ppCounter, label_values);
    } else {
      uError("taosClusterCounterInc not found pCounter %s:%s.", clusterKey, counterName);
    }
  } else {
    uError("taosClusterCounterInc not found pMonitor %s.", clusterKey);
  }
  taosRUnLockLatch(&monitorLock);
  return 0;
}

void clusterMonitorClose(const char* clusterKey) {
  taosWLockLatch(&monitorLock);
  ClientMonitor** ppMonitor = (ClientMonitor**)taosHashGet(clusterMonitorInfoTable, clusterKey, strlen(clusterKey));

  if (ppMonitor != NULL && *ppMonitor != NULL) {
    ClientMonitor* pMonitor = *ppMonitor;
    uInfo("clusterMonitorClose valule:%p  clusterKey:%s.", pMonitor, pMonitor->clusterKey);
    taosHashCleanup(pMonitor->counters);
    taos_collector_registry_destroy(pMonitor->registry);
    taosMemoryFree(pMonitor);
    taosHashRemove(clusterMonitorInfoTable, clusterKey, strlen(clusterKey));
  }
  taosWUnLockLatch(&monitorLock);
}
