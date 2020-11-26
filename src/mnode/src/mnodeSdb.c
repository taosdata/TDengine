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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taoserror.h"
#include "hash.h"
#include "tutil.h"
#include "tbalance.h"
#include "tqueue.h"
#include "twal.h"
#include "tsync.h"
#include "ttimer.h"
#include "tglobal.h"
#include "dnode.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeMnode.h"
#include "mnodeDnode.h"
#include "mnodeCluster.h"
#include "mnodeSdb.h"

#define SDB_TABLE_LEN 12
#define MAX_QUEUED_MSG_NUM 10000

typedef enum {
  SDB_ACTION_INSERT = 0,
  SDB_ACTION_DELETE = 1,
  SDB_ACTION_UPDATE = 2
} ESdbAction;

typedef enum {
  SDB_STATUS_OFFLINE = 0,
  SDB_STATUS_SERVING = 1,
  SDB_STATUS_CLOSING = 2
} ESdbStatus;

char *actStr[] = {
  "insert",
  "delete",
  "update",
  "invalid"
};

typedef struct SSdbTable {
  char      name[SDB_TABLE_LEN];
  ESdbTable id;
  ESdbKey   keyType;
  int32_t   hashSessions;
  int32_t   maxRowSize;
  int32_t   refCountPos;
  int32_t   autoIndex;
  int64_t   numOfRows;
  void *    iHandle;
  int32_t (*fpInsert)(SSdbRow *pRow);
  int32_t (*fpDelete)(SSdbRow *pRow);
  int32_t (*fpUpdate)(SSdbRow *pRow);
  int32_t (*fpDecode)(SSdbRow *pRow);
  int32_t (*fpEncode)(SSdbRow *pRow);
  int32_t (*fpDestroy)(SSdbRow *pRow);
  int32_t (*fpRestored)();
  pthread_mutex_t mutex;
} SSdbTable;

typedef struct {
  ESyncRole  role;
  ESdbStatus status;
  uint64_t   version;
  int64_t    sync;
  void *     wal;
  SSyncCfg   cfg;
  int32_t    queuedMsg;
  int32_t    numOfTables;
  SSdbTable *tableList[SDB_TABLE_MAX];
  pthread_mutex_t mutex;
} SSdbMgmt;

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SSdbWorker;

typedef struct {
  int32_t num;
  SSdbWorker *worker;
} SSdbWorkerPool;

extern void *     tsMnodeTmr;
static void *     tsSdbTmr;
static SSdbMgmt   tsSdbMgmt = {0};
static taos_qset  tsSdbWQset;
static taos_qall  tsSdbWQall;
static taos_queue tsSdbWQueue;
static SSdbWorkerPool tsSdbPool;

static int32_t sdbProcessWrite(void *pRow, void *pHead, int32_t qtype, void *unused);
static int32_t sdbWriteFwdToQueue(int32_t vgId, void *pHead, int32_t qtype, void *rparam);
static int32_t sdbWriteRowToQueue(SSdbRow *pRow, int32_t action);
static void    sdbFreeFromQueue(SSdbRow *pRow);
static void *  sdbWorkerFp(void *pWorker);
static int32_t sdbInitWorker();
static void    sdbCleanupWorker();
static int32_t sdbAllocQueue();
static void    sdbFreeQueue();
static int32_t sdbInsertHash(SSdbTable *pTable, SSdbRow *pRow);
static int32_t sdbUpdateHash(SSdbTable *pTable, SSdbRow *pRow);
static int32_t sdbDeleteHash(SSdbTable *pTable, SSdbRow *pRow);

int32_t sdbGetId(void *pTable) {
  return ((SSdbTable *)pTable)->autoIndex;
}

int64_t sdbGetNumOfRows(void *pTable) {
  return ((SSdbTable *)pTable)->numOfRows;
}

uint64_t sdbGetVersion() {
  return tsSdbMgmt.version;
}

bool sdbIsMaster() { 
  return tsSdbMgmt.role == TAOS_SYNC_ROLE_MASTER; 
}

bool sdbIsServing() {
  return tsSdbMgmt.status == SDB_STATUS_SERVING; 
}

static void *sdbGetObjKey(SSdbTable *pTable, void *key) {
  if (pTable->keyType == SDB_KEY_VAR_STRING) {
    return *(char **)key;
  }

  return key;
}

static char *sdbGetKeyStr(SSdbTable *pTable, void *key) {
  static char str[16];
  switch (pTable->keyType) {
    case SDB_KEY_STRING:
    case SDB_KEY_VAR_STRING:
      return (char *)key;
    case SDB_KEY_INT:
    case SDB_KEY_AUTO:
      sprintf(str, "%d", *(int32_t *)key);
      return str;
    default:
      return "invalid";
  }
}

static char *sdbGetRowStr(SSdbTable *pTable, void *key) {
  return sdbGetKeyStr(pTable, sdbGetObjKey(pTable, key));
}

static void *sdbGetTableFromId(int32_t tableId) {
  return tsSdbMgmt.tableList[tableId];
}

static int32_t sdbInitWal() {
  SWalCfg walCfg = {.vgId = 1, .walLevel = TAOS_WAL_FSYNC, .keep = TAOS_WAL_KEEP, .fsyncPeriod = 0};
  char    temp[TSDB_FILENAME_LEN] = {0};
  sprintf(temp, "%s/wal", tsMnodeDir);
  tsSdbMgmt.wal = walOpen(temp, &walCfg);
  if (tsSdbMgmt.wal == NULL) {
    sdbError("vgId:1, failed to open wal in %s", tsMnodeDir);
    return -1;
  }

  sdbInfo("vgId:1, open wal for restore");
  int32_t code = walRestore(tsSdbMgmt.wal, NULL, sdbProcessWrite);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, failed to open wal for restore since %s", tstrerror(code));
    return -1;
  }
  return 0;
}

static void sdbRestoreTables() {
  int32_t totalRows = 0;
  int32_t numOfTables = 0;
  for (int32_t tableId = 0; tableId < SDB_TABLE_MAX; ++tableId) {
    SSdbTable *pTable = sdbGetTableFromId(tableId);
    if (pTable == NULL) continue;
    if (pTable->fpRestored) {
      (*pTable->fpRestored)();
    }

    totalRows += pTable->numOfRows;
    numOfTables++;
    sdbDebug("vgId:1, sdb:%s is restored, rows:%" PRId64, pTable->name, pTable->numOfRows);
  }

  sdbInfo("vgId:1, sdb is restored, mver:%" PRIu64 " rows:%d tables:%d", tsSdbMgmt.version, totalRows, numOfTables);
}

void sdbUpdateMnodeRoles() {
  if (tsSdbMgmt.sync <= 0) return;

  SNodesRole roles = {0};
  if (syncGetNodesRole(tsSdbMgmt.sync, &roles) != 0) return;

  sdbInfo("vgId:1, update mnodes role, replica:%d", tsSdbMgmt.cfg.replica);
  for (int32_t i = 0; i < tsSdbMgmt.cfg.replica; ++i) {
    SMnodeObj *pMnode = mnodeGetMnode(roles.nodeId[i]);
    if (pMnode != NULL) {
      pMnode->role = roles.role[i];
      sdbInfo("vgId:1, mnode:%d, role:%s", pMnode->mnodeId, syncRole[pMnode->role]);
      if (pMnode->mnodeId == dnodeGetDnodeId()) tsSdbMgmt.role = pMnode->role;
      mnodeDecMnodeRef(pMnode);
    }
  }

  mnodeUpdateClusterId();
  mnodeUpdateMnodeEpSet();
}

static uint32_t sdbGetFileInfo(int32_t vgId, char *name, uint32_t *index, uint32_t eindex, int64_t *size, uint64_t *fversion) {
  sdbUpdateMnodeRoles();
  return 0;
}

static int32_t sdbGetWalInfo(int32_t vgId, char *fileName, int64_t *fileId) {
  return walGetWalFile(tsSdbMgmt.wal, fileName, fileId);
}

static void sdbNotifyRole(int32_t vgId, int8_t role) {
  sdbInfo("vgId:1, mnode role changed from %s to %s", syncRole[tsSdbMgmt.role], syncRole[role]);

  if (role == TAOS_SYNC_ROLE_MASTER && tsSdbMgmt.role != TAOS_SYNC_ROLE_MASTER) {
    balanceReset();
  }
  tsSdbMgmt.role = role;

  sdbUpdateMnodeRoles();
}

// failed to forward, need revert insert
static void sdbHandleFailedConfirm(SSdbRow *pRow) {
  SWalHead *pHead = pRow->pHead;
  int32_t   action = pHead->msgType % 10;

  sdbError("vgId:1, row:%p:%s hver:%" PRIu64 " action:%s, failed to foward since %s", pRow->pObj,
           sdbGetKeyStr(pRow->pTable, pHead->cont), pHead->version, actStr[action], tstrerror(pRow->code));

  // It's better to create a table in two stages, create it first and then set it success
  if (action == SDB_ACTION_INSERT) {
    SSdbRow row = {.type = SDB_OPER_GLOBAL, .pTable = pRow->pTable, .pObj = pRow->pObj};
    sdbDeleteRow(&row);
  }
}

FORCE_INLINE
static void sdbConfirmForward(int32_t vgId, void *wparam, int32_t code) {
  if (wparam == NULL) return;
  SSdbRow *pRow = wparam;
  SMnodeMsg * pMsg = pRow->pMsg;

  if (code <= 0) pRow->code = code;
  int32_t count = atomic_add_fetch_32(&pRow->processedCount, 1);
  if (count <= 1) {
    if (pMsg != NULL) sdbTrace("vgId:1, msg:%p waiting for confirm, count:%d code:%x", pMsg, count, code);
    return;
  } else {
    if (pMsg != NULL) sdbTrace("vgId:1, msg:%p is confirmed, code:%x", pMsg, code);
  }

  if (pRow->code != TSDB_CODE_SUCCESS) sdbHandleFailedConfirm(pRow);

  if (pRow->fpRsp != NULL) {
    pRow->code = (*pRow->fpRsp)(pMsg, pRow->code);
  }

  dnodeSendRpcMWriteRsp(pMsg, pRow->code);
  sdbFreeFromQueue(pRow);
}

static void sdbUpdateSyncTmrFp(void *param, void *tmrId) { sdbUpdateSync(NULL); }

void sdbUpdateAsync() {
  taosTmrReset(sdbUpdateSyncTmrFp, 200, NULL, tsMnodeTmr, &tsSdbTmr);
}

void sdbUpdateSync(void *pMnodes) {
  SMnodeInfos *mnodes = pMnodes;
  if (!mnodeIsRunning()) {
    mDebug("vgId:1, mnode not start yet, update sync config later");
    return;
  }

  mDebug("vgId:1, update sync config in sync module, mnodes:%p", pMnodes);

  SSyncCfg syncCfg = {0};
  int32_t  index = 0;

  if (mnodes == NULL) {
    void *pIter = NULL;
    while (1) {
      SMnodeObj *pMnode = NULL;
      pIter = mnodeGetNextMnode(pIter, &pMnode);
      if (pMnode == NULL) break;

      syncCfg.nodeInfo[index].nodeId = pMnode->mnodeId;

      SDnodeObj *pDnode = mnodeGetDnode(pMnode->mnodeId);
      if (pDnode != NULL) {
        syncCfg.nodeInfo[index].nodePort = pDnode->dnodePort + TSDB_PORT_SYNC;
        tstrncpy(syncCfg.nodeInfo[index].nodeFqdn, pDnode->dnodeFqdn, TSDB_FQDN_LEN);
        index++;
      }

      mnodeDecDnodeRef(pDnode);
      mnodeDecMnodeRef(pMnode);
    }
    sdbFreeIter(pIter);
    syncCfg.replica = index;
    mDebug("vgId:1, mnodes info not input, use infos in sdb, numOfMnodes:%d", syncCfg.replica);
  } else {
    for (index = 0; index < mnodes->mnodeNum; ++index) {
      SMnodeInfo *node = &mnodes->mnodeInfos[index];
      syncCfg.nodeInfo[index].nodeId = node->mnodeId;
      taosGetFqdnPortFromEp(node->mnodeEp, syncCfg.nodeInfo[index].nodeFqdn, &syncCfg.nodeInfo[index].nodePort);
      syncCfg.nodeInfo[index].nodePort += TSDB_PORT_SYNC;
    }
    syncCfg.replica = index;
    mDebug("vgId:1, mnodes info input, numOfMnodes:%d", syncCfg.replica);
  }

  syncCfg.quorum = (syncCfg.replica == 1) ? 1 : 2;

  bool hasThisDnode = false;
  for (int32_t i = 0; i < syncCfg.replica; ++i) {
    if (syncCfg.nodeInfo[i].nodeId == dnodeGetDnodeId()) {
      hasThisDnode = true;
      break;
    }
  }

  if (!hasThisDnode) {
    sdbDebug("vgId:1, update sync config, this dnode not exist");
    return;
  }

  if (memcmp(&syncCfg, &tsSdbMgmt.cfg, sizeof(SSyncCfg)) == 0) {
    sdbDebug("vgId:1, update sync config, info not changed");
    return;
  }

  sdbInfo("vgId:1, work as mnode, replica:%d", syncCfg.replica);
  for (int32_t i = 0; i < syncCfg.replica; ++i) {
    sdbInfo("vgId:1, mnode:%d, %s:%d", syncCfg.nodeInfo[i].nodeId, syncCfg.nodeInfo[i].nodeFqdn,
            syncCfg.nodeInfo[i].nodePort);
  }

  SSyncInfo syncInfo = {0};
  syncInfo.vgId = 1;
  syncInfo.version = sdbGetVersion();
  syncInfo.syncCfg = syncCfg;
  sprintf(syncInfo.path, "%s", tsMnodeDir);
  syncInfo.getWalInfo = sdbGetWalInfo;
  syncInfo.getFileInfo = sdbGetFileInfo;
  syncInfo.writeToCache = sdbWriteFwdToQueue;
  syncInfo.confirmForward = sdbConfirmForward;
  syncInfo.notifyRole = sdbNotifyRole;
  tsSdbMgmt.cfg = syncCfg;

  if (tsSdbMgmt.sync) {
    syncReconfig(tsSdbMgmt.sync, &syncCfg);
  } else {
    tsSdbMgmt.sync = syncStart(&syncInfo);
  }

  sdbUpdateMnodeRoles();
}

int32_t sdbInit() {
  pthread_mutex_init(&tsSdbMgmt.mutex, NULL);

  if (sdbInitWorker() != 0) {
    return -1;
  }

  if (sdbInitWal() != 0) {
    return -1;
  }

  sdbRestoreTables();

  if (mnodeGetMnodesNum() == 1) {
    tsSdbMgmt.role = TAOS_SYNC_ROLE_MASTER;
  }

  tsSdbMgmt.status = SDB_STATUS_SERVING;
  return TSDB_CODE_SUCCESS;
}

void sdbCleanUp() {
  if (tsSdbMgmt.status != SDB_STATUS_SERVING) return;

  tsSdbMgmt.status = SDB_STATUS_CLOSING;

  sdbCleanupWorker();
  sdbDebug("vgId:1, sdb will be closed, mver:%" PRIu64, tsSdbMgmt.version);

  if (tsSdbMgmt.sync) {
    syncStop(tsSdbMgmt.sync);
    tsSdbMgmt.sync = -1;
  }

  if (tsSdbMgmt.wal) {
    walClose(tsSdbMgmt.wal);
    tsSdbMgmt.wal = NULL;
  }
  
  pthread_mutex_destroy(&tsSdbMgmt.mutex);
}

void sdbIncRef(void *tparam, void *pRow) {
  if (pRow == NULL || tparam == NULL) return;

  SSdbTable *pTable = tparam;
  int32_t *  pRefCount = (int32_t *)(pRow + pTable->refCountPos);
  int32_t    refCount = atomic_add_fetch_32(pRefCount, 1);
  sdbTrace("vgId:1, sdb:%s, inc ref to row:%p:%s:%d", pTable->name, pRow, sdbGetRowStr(pTable, pRow), refCount);
}

void sdbDecRef(void *tparam, void *pRow) {
  if (pRow == NULL || tparam == NULL) return;

  SSdbTable *pTable = tparam;
  int32_t *  pRefCount = (int32_t *)(pRow + pTable->refCountPos);
  int32_t    refCount = atomic_sub_fetch_32(pRefCount, 1);
  sdbTrace("vgId:1, sdb:%s, dec ref to row:%p:%s:%d", pTable->name, pRow, sdbGetRowStr(pTable, pRow), refCount);

  int32_t *updateEnd = pRow + pTable->refCountPos - 4;
  if (refCount <= 0 && *updateEnd) {
    sdbTrace("vgId:1, sdb:%s, row:%p:%s:%d destroyed", pTable->name, pRow, sdbGetRowStr(pTable, pRow), refCount);
    SSdbRow row = {.pObj = pRow};
    (*pTable->fpDestroy)(&row);
  }
}

static void *sdbGetRowMeta(SSdbTable *pTable, void *key) {
  if (pTable == NULL) return NULL;

  int32_t keySize = sizeof(int32_t);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  void **ppRow = (void **)taosHashGet(pTable->iHandle, key, keySize);
  if (ppRow != NULL) return *ppRow;

  return NULL;
}

static void *sdbGetRowMetaFromObj(SSdbTable *pTable, void *key) {
  return sdbGetRowMeta(pTable, sdbGetObjKey(pTable, key));
}

void *sdbGetRow(void *tparam, void *key) {
  SSdbTable *pTable = tparam;

  pthread_mutex_lock(&pTable->mutex);
  void *pRow = sdbGetRowMeta(pTable, key);
  if (pRow) sdbIncRef(pTable, pRow);
  pthread_mutex_unlock(&pTable->mutex);

  return pRow;
}

static void *sdbGetRowFromObj(SSdbTable *pTable, void *key) {
  return sdbGetRow(pTable, sdbGetObjKey(pTable, key));
}

static int32_t sdbInsertHash(SSdbTable *pTable, SSdbRow *pRow) {
  void *  key = sdbGetObjKey(pTable, pRow->pObj);
  int32_t keySize = sizeof(int32_t);

  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  pthread_mutex_lock(&pTable->mutex);
  taosHashPut(pTable->iHandle, key, keySize, &pRow->pObj, sizeof(int64_t));
  pthread_mutex_unlock(&pTable->mutex);

  sdbIncRef(pTable, pRow->pObj);
  atomic_add_fetch_32(&pTable->numOfRows, 1);

  if (pTable->keyType == SDB_KEY_AUTO) {
    pTable->autoIndex = MAX(pTable->autoIndex, *((uint32_t *)pRow->pObj));
  } else {
    atomic_add_fetch_32(&pTable->autoIndex, 1);
  }

  sdbDebug("vgId:1, sdb:%s, insert key:%s to hash, rowSize:%d rows:%" PRId64 ", msg:%p", pTable->name,
           sdbGetRowStr(pTable, pRow->pObj), pRow->rowSize, pTable->numOfRows, pRow->pMsg);

  int32_t code = (*pTable->fpInsert)(pRow);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, sdb:%s, failed to insert key:%s to hash, remove it", pTable->name,
             sdbGetRowStr(pTable, pRow->pObj));
    sdbDeleteHash(pTable, pRow);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t sdbDeleteHash(SSdbTable *pTable, SSdbRow *pRow) {
  int32_t *updateEnd = pRow->pObj + pTable->refCountPos - 4;
  bool set = atomic_val_compare_exchange_32(updateEnd, 0, 1) == 0;
  if (!set) {
    sdbError("vgId:1, sdb:%s, failed to delete key:%s from hash, for it already removed", pTable->name,
             sdbGetRowStr(pTable, pRow->pObj));
    return TSDB_CODE_MND_SDB_OBJ_NOT_THERE;
  }

  (*pTable->fpDelete)(pRow);
  
  void *  key = sdbGetObjKey(pTable, pRow->pObj);
  int32_t keySize = sizeof(int32_t);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  pthread_mutex_lock(&pTable->mutex);
  taosHashRemove(pTable->iHandle, key, keySize);
  pthread_mutex_unlock(&pTable->mutex);

  atomic_sub_fetch_32(&pTable->numOfRows, 1);

  sdbDebug("vgId:1, sdb:%s, delete key:%s from hash, numOfRows:%" PRId64 ", msg:%p", pTable->name,
           sdbGetRowStr(pTable, pRow->pObj), pTable->numOfRows, pRow->pMsg);

  sdbDecRef(pTable, pRow->pObj);

  return TSDB_CODE_SUCCESS;
}

static int32_t sdbUpdateHash(SSdbTable *pTable, SSdbRow *pRow) {
  sdbDebug("vgId:1, sdb:%s, update key:%s in hash, numOfRows:%" PRId64 ", msg:%p", pTable->name,
           sdbGetRowStr(pTable, pRow->pObj), pTable->numOfRows, pRow->pMsg);

  (*pTable->fpUpdate)(pRow);
  return TSDB_CODE_SUCCESS;
}

static int32_t sdbPerformInsertAction(SWalHead *pHead, SSdbTable *pTable) {
  SSdbRow row = {.rowSize = pHead->len, .rowData = pHead->cont, .pTable = pTable};
  (*pTable->fpDecode)(&row);
  return sdbInsertHash(pTable, &row);
}

static int32_t sdbPerformDeleteAction(SWalHead *pHead, SSdbTable *pTable) {
  void *pObj = sdbGetRowMeta(pTable, pHead->cont);
  if (pObj == NULL) {
    sdbDebug("vgId:1, sdb:%s, object:%s not exist in hash, ignore delete action", pTable->name,
             sdbGetKeyStr(pTable, pHead->cont));
    return TSDB_CODE_SUCCESS;
  }
  SSdbRow row = {.pTable = pTable, .pObj = pObj};
  return sdbDeleteHash(pTable, &row);
}

static int32_t sdbPerformUpdateAction(SWalHead *pHead, SSdbTable *pTable) {
  void *pObj = sdbGetRowMeta(pTable, pHead->cont);
  if (pObj == NULL) {
    sdbDebug("vgId:1, sdb:%s, object:%s not exist in hash, ignore update action", pTable->name,
             sdbGetKeyStr(pTable, pHead->cont));
    return TSDB_CODE_SUCCESS;
  }
  SSdbRow row = {.rowSize = pHead->len, .rowData = pHead->cont, .pTable = pTable};
  (*pTable->fpDecode)(&row);
  return sdbUpdateHash(pTable, &row);
}

static int32_t sdbProcessWrite(void *wparam, void *hparam, int32_t qtype, void *unused) {
  SSdbRow *pRow = wparam;
  SWalHead *pHead = hparam;
  int32_t tableId = pHead->msgType / 10;
  int32_t action = pHead->msgType % 10;

  SSdbTable *pTable = sdbGetTableFromId(tableId);
  assert(pTable != NULL);

  if (qtype == TAOS_QTYPE_QUERY) return sdbPerformDeleteAction(pHead, pTable);

  pthread_mutex_lock(&tsSdbMgmt.mutex);
  
  if (pHead->version == 0) {
    // assign version
    tsSdbMgmt.version++;
    pHead->version = tsSdbMgmt.version;
  } else {
    // for data from WAL or forward, version may be smaller
    if (pHead->version <= tsSdbMgmt.version) {
      pthread_mutex_unlock(&tsSdbMgmt.mutex);
      sdbDebug("vgId:1, sdb:%s, failed to restore %s key:%s from source(%d), hver:%" PRIu64 " too large, mver:%" PRIu64,
               pTable->name, actStr[action], sdbGetKeyStr(pTable, pHead->cont), qtype, pHead->version, tsSdbMgmt.version);
      return TSDB_CODE_SUCCESS;
    } else if (pHead->version != tsSdbMgmt.version + 1) {
      pthread_mutex_unlock(&tsSdbMgmt.mutex);
      sdbError("vgId:1, sdb:%s, failed to restore %s key:%s from source(%d), hver:%" PRIu64 " too large, mver:%" PRIu64,
               pTable->name, actStr[action], sdbGetKeyStr(pTable, pHead->cont), qtype, pHead->version, tsSdbMgmt.version);
      return TSDB_CODE_SYN_INVALID_VERSION;
    } else {
      tsSdbMgmt.version = pHead->version;
    }
  }

  int32_t code = walWrite(tsSdbMgmt.wal, pHead);
  if (code < 0) {
    pthread_mutex_unlock(&tsSdbMgmt.mutex);
    return code;
  }

  pthread_mutex_unlock(&tsSdbMgmt.mutex);

  // from app, row is created
  if (pRow != NULL) {
    // forward to peers
    pRow->processedCount = 0;
    int32_t syncCode = syncForwardToPeer(tsSdbMgmt.sync, pHead, pRow, TAOS_QTYPE_RPC);
    if (syncCode <= 0) pRow->processedCount = 1;

    if (syncCode < 0) {
      sdbError("vgId:1, sdb:%s, failed to forward req since %s action:%s key:%s hver:%" PRIu64 ", msg:%p", pTable->name,
               tstrerror(syncCode), actStr[action], sdbGetKeyStr(pTable, pHead->cont), pHead->version, pRow->pMsg);
    } else if (syncCode > 0) {
      sdbDebug("vgId:1, sdb:%s, forward req is sent, action:%s key:%s hver:%" PRIu64 ", msg:%p", pTable->name,
               actStr[action], sdbGetKeyStr(pTable, pHead->cont), pHead->version, pRow->pMsg);
    } else {
      sdbTrace("vgId:1, sdb:%s, no need to send fwd req, action:%s key:%s hver:%" PRIu64 ", msg:%p", pTable->name,
               actStr[action], sdbGetKeyStr(pTable, pHead->cont), pHead->version, pRow->pMsg);
    }
    return syncCode;
  }

  sdbDebug("vgId:1, sdb:%s, record from wal/fwd is disposed, action:%s key:%s hver:%" PRIu64, pTable->name,
           actStr[action], sdbGetKeyStr(pTable, pHead->cont), pHead->version);

  // even it is WAL/FWD, it shall be called to update version in sync
  syncForwardToPeer(tsSdbMgmt.sync, pHead, pRow, TAOS_QTYPE_RPC);

  // from wal or forward msg, row not created, should add into hash
  if (action == SDB_ACTION_INSERT) {
    return sdbPerformInsertAction(pHead, pTable);
  } else if (action == SDB_ACTION_DELETE) {
    if (qtype == TAOS_QTYPE_FWD) {
      // Drop database/stable may take a long time and cause a timeout, so we confirm first then reput it into queue
      sdbWriteFwdToQueue(1, hparam, TAOS_QTYPE_QUERY, unused);
      return TSDB_CODE_SUCCESS;
    } else {
      return sdbPerformDeleteAction(pHead, pTable);
    }
  } else if (action == SDB_ACTION_UPDATE) {
    return sdbPerformUpdateAction(pHead, pTable);
  } else {
    return TSDB_CODE_MND_INVALID_MSG_TYPE;
  }
}

int32_t sdbInsertRow(SSdbRow *pRow) {
  SSdbTable *pTable = pRow->pTable;
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  if (sdbGetRowFromObj(pTable, pRow->pObj)) {
    sdbError("vgId:1, sdb:%s, failed to insert:%s since it exist", pTable->name, sdbGetRowStr(pTable, pRow->pObj));
    sdbDecRef(pTable, pRow->pObj);
    return TSDB_CODE_MND_SDB_OBJ_ALREADY_THERE;
  }

  if (pTable->keyType == SDB_KEY_AUTO) {
    *((uint32_t *)pRow->pObj) = atomic_add_fetch_32(&pTable->autoIndex, 1);

    // let vgId increase from 2
    if (pTable->autoIndex == 1 && pTable->id == SDB_TABLE_VGROUP) {
      *((uint32_t *)pRow->pObj) = atomic_add_fetch_32(&pTable->autoIndex, 1);
    }
  }

  int32_t code = sdbInsertHash(pTable, pRow);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, sdb:%s, failed to insert:%s into hash", pTable->name, sdbGetRowStr(pTable, pRow->pObj));
    return code;
  }

  // just insert data into memory
  if (pRow->type != SDB_OPER_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pRow->fpReq) {
    return (*pRow->fpReq)(pRow->pMsg);
  } else {
    return sdbWriteRowToQueue(pRow, SDB_ACTION_INSERT);
  }
}

bool sdbCheckRowDeleted(void *tparam, void *pRow) {
  SSdbTable *pTable = tparam;
  if (pTable == NULL) return false;

  int32_t *updateEnd = pRow + pTable->refCountPos - 4;
  return atomic_val_compare_exchange_32(updateEnd, 1, 1) == 1;
}

int32_t sdbDeleteRow(SSdbRow *pRow) {
  SSdbTable *pTable = pRow->pTable;
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  void *pObj = sdbGetRowMetaFromObj(pTable, pRow->pObj);
  if (pObj == NULL) {
    sdbDebug("vgId:1, sdb:%s, record is not there, delete failed", pTable->name);
    return TSDB_CODE_MND_SDB_OBJ_NOT_THERE;
  }

  int32_t code = sdbDeleteHash(pTable, pRow);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, sdb:%s, failed to delete from hash", pTable->name);
    return code;
  }

  // just delete data from memory
  if (pRow->type != SDB_OPER_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pRow->fpReq) {
    return (*pRow->fpReq)(pRow->pMsg);
  } else {
    return sdbWriteRowToQueue(pRow, SDB_ACTION_DELETE);
  }
}

int32_t sdbUpdateRow(SSdbRow *pRow) {
  SSdbTable *pTable = pRow->pTable;
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  void *pObj = sdbGetRowMetaFromObj(pTable, pRow->pObj);
  if (pObj == NULL) {
    sdbDebug("vgId:1, sdb:%s, record is not there, update failed", pTable->name);
    return TSDB_CODE_MND_SDB_OBJ_NOT_THERE;
  }

  int32_t code = sdbUpdateHash(pTable, pRow);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, sdb:%s, failed to update hash", pTable->name);
    return code;
  }

  // just update data in memory
  if (pRow->type != SDB_OPER_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pRow->fpReq) {
    return (*pRow->fpReq)(pRow->pMsg);
  } else {
    return sdbWriteRowToQueue(pRow, SDB_ACTION_UPDATE);
  }
}

void *sdbFetchRow(void *tparam, void *pNode, void **ppRow) {
  SSdbTable *pTable = tparam;
  *ppRow = NULL;
  if (pTable == NULL) return NULL;

  SHashMutableIterator *pIter = pNode;
  if (pIter == NULL) {
    pIter = taosHashCreateIter(pTable->iHandle);
  }

  if (!taosHashIterNext(pIter)) {
    taosHashDestroyIter(pIter);
    return NULL;
  }

  void **ppMetaRow = taosHashIterGet(pIter);
  if (ppMetaRow == NULL) {
    taosHashDestroyIter(pIter);
    return NULL;
  }

  *ppRow = *ppMetaRow;
  sdbIncRef(pTable, *ppMetaRow);

  return pIter;
}

void sdbFreeIter(void *pIter) {
  if (pIter != NULL) {
    taosHashDestroyIter(pIter);
  }
}

void *sdbOpenTable(SSdbTableDesc *pDesc) {
  SSdbTable *pTable = (SSdbTable *)calloc(1, sizeof(SSdbTable));
  
  if (pTable == NULL) return NULL;

  pthread_mutex_init(&pTable->mutex, NULL);
  tstrncpy(pTable->name, pDesc->name, SDB_TABLE_LEN);
  pTable->keyType      = pDesc->keyType;
  pTable->id           = pDesc->id;
  pTable->hashSessions = pDesc->hashSessions;
  pTable->maxRowSize   = pDesc->maxRowSize;
  pTable->refCountPos  = pDesc->refCountPos;
  pTable->fpInsert     = pDesc->fpInsert;
  pTable->fpDelete     = pDesc->fpDelete;
  pTable->fpUpdate     = pDesc->fpUpdate;
  pTable->fpEncode     = pDesc->fpEncode;
  pTable->fpDecode     = pDesc->fpDecode;
  pTable->fpDestroy    = pDesc->fpDestroy;
  pTable->fpRestored   = pDesc->fpRestored;

  _hash_fn_t hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  }
  pTable->iHandle = taosHashInit(pTable->hashSessions, hashFp, true, HASH_ENTRY_LOCK);

  tsSdbMgmt.numOfTables++;
  tsSdbMgmt.tableList[pTable->id] = pTable;
  return pTable;
}

void sdbCloseTable(void *handle) {
  SSdbTable *pTable = (SSdbTable *)handle;
  if (pTable == NULL) return;
  
  tsSdbMgmt.numOfTables--;
  tsSdbMgmt.tableList[pTable->id] = NULL;

  SHashMutableIterator *pIter = taosHashCreateIter(pTable->iHandle);
  while (taosHashIterNext(pIter)) {
    void **ppRow = taosHashIterGet(pIter);
    if (ppRow == NULL) continue;

    SSdbRow row = {
      .pObj = *ppRow,
      .pTable = pTable,
    };

    (*pTable->fpDestroy)(&row);
  }

  taosHashDestroyIter(pIter);
  taosHashCleanup(pTable->iHandle);
  pthread_mutex_destroy(&pTable->mutex);

  sdbDebug("vgId:1, sdb:%s, is closed, numOfTables:%d", pTable->name, tsSdbMgmt.numOfTables);
  free(pTable);
}

static int32_t sdbInitWorker() {
  tsSdbPool.num = 1;
  tsSdbPool.worker = calloc(sizeof(SSdbWorker), tsSdbPool.num);

  if (tsSdbPool.worker == NULL) return -1;
  for (int32_t i = 0; i < tsSdbPool.num; ++i) {
    SSdbWorker *pWorker = tsSdbPool.worker + i;
    pWorker->workerId = i;
  }

  sdbAllocQueue();
  
  mInfo("vgId:1, sdb write is opened");
  return 0;
}

static void sdbCleanupWorker() {
  for (int32_t i = 0; i < tsSdbPool.num; ++i) {
    SSdbWorker *pWorker = tsSdbPool.worker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(tsSdbWQset);
    }
  }

  for (int32_t i = 0; i < tsSdbPool.num; ++i) {
    SSdbWorker *pWorker = tsSdbPool.worker + i;
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  sdbFreeQueue();
  tfree(tsSdbPool.worker);

  mInfo("vgId:1, sdb write is closed");
}

static int32_t sdbAllocQueue() {
  tsSdbWQueue = taosOpenQueue();
  if (tsSdbWQueue == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  tsSdbWQset = taosOpenQset();
  if (tsSdbWQset == NULL) {
    taosCloseQueue(tsSdbWQueue);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  taosAddIntoQset(tsSdbWQset, tsSdbWQueue, NULL);

  tsSdbWQall = taosAllocateQall();
  if (tsSdbWQall == NULL) {
    taosCloseQset(tsSdbWQset);
    taosCloseQueue(tsSdbWQueue);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  
  for (int32_t i = 0; i < tsSdbPool.num; ++i) {
    SSdbWorker *pWorker = tsSdbPool.worker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, sdbWorkerFp, pWorker) != 0) {
      mError("failed to create thread to process sdb write queue, reason:%s", strerror(errno));
      taosFreeQall(tsSdbWQall);
      taosCloseQset(tsSdbWQset);
      taosCloseQueue(tsSdbWQueue);
      return TSDB_CODE_MND_OUT_OF_MEMORY;
    }

    pthread_attr_destroy(&thAttr);
    mDebug("sdb write worker:%d is launched, total:%d", pWorker->workerId, tsSdbPool.num);
  }

  mDebug("sdb write queue:%p is allocated", tsSdbWQueue);
  return TSDB_CODE_SUCCESS;
}

static void sdbFreeQueue() {
  taosCloseQueue(tsSdbWQueue);
  taosFreeQall(tsSdbWQall);
  taosCloseQset(tsSdbWQset);
  tsSdbWQall = NULL;
  tsSdbWQset = NULL;
  tsSdbWQueue = NULL;
}

static int32_t sdbWriteToQueue(SSdbRow *pRow, int32_t qtype) {
  SWalHead *pHead = pRow->pHead;

  if (pHead->len > TSDB_MAX_WAL_SIZE) {
    sdbError("vgId:1, wal len:%d exceeds limit, hver:%" PRIu64, pHead->len, pHead->version);
    taosFreeQitem(pRow);
    return TSDB_CODE_WAL_SIZE_LIMIT;
  }

  int32_t queued = atomic_add_fetch_32(&tsSdbMgmt.queuedMsg, 1);
  if (queued > MAX_QUEUED_MSG_NUM) {
    sdbDebug("vgId:1, too many msg:%d in sdb queue, flow control", queued);
    taosMsleep(1);
  }

  sdbIncRef(pRow->pTable, pRow->pObj);

  sdbTrace("vgId:1, msg:%p qtype:%s write into to sdb queue, queued:%d", pRow->pMsg, qtypeStr[qtype], queued);
  taosWriteQitem(tsSdbWQueue, qtype, pRow);

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static void sdbFreeFromQueue(SSdbRow *pRow) {
  int32_t queued = atomic_sub_fetch_32(&tsSdbMgmt.queuedMsg, 1);
  sdbTrace("vgId:1, msg:%p free from sdb queue, queued:%d", pRow->pMsg, queued);

  sdbDecRef(pRow->pTable, pRow->pObj);
  taosFreeQitem(pRow);
}

static int32_t sdbWriteFwdToQueue(int32_t vgId, void *wparam, int32_t qtype, void *rparam) {
  SWalHead *pHead = wparam;

  int32_t  size = sizeof(SSdbRow) + sizeof(SWalHead) + pHead->len;
  SSdbRow *pRow = taosAllocateQitem(size);
  if (pRow == NULL) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  memcpy(pRow->pHead, pHead, sizeof(SWalHead) + pHead->len);
  pRow->rowData = pRow->pHead->cont;

  return sdbWriteToQueue(pRow, qtype);
}

static int32_t sdbWriteRowToQueue(SSdbRow *pInputRow, int32_t action) {
  SSdbTable *pTable = pInputRow->pTable;
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  int32_t  size = sizeof(SSdbRow) + sizeof(SWalHead) + pTable->maxRowSize;
  SSdbRow *pRow = taosAllocateQitem(size);
  if (pRow == NULL) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  memcpy(pRow, pInputRow, sizeof(SSdbRow));
  pRow->processedCount = 1;

  SWalHead *pHead = pRow->pHead;
  pRow->rowData = pHead->cont;
  (*pTable->fpEncode)(pRow);

  pHead->len = pRow->rowSize;
  pHead->version = 0;
  pHead->msgType = pTable->id * 10 + action;

  return sdbWriteToQueue(pRow, TAOS_QTYPE_RPC);
}

int32_t sdbInsertRowToQueue(SSdbRow *pRow) { return sdbWriteRowToQueue(pRow, SDB_ACTION_INSERT); }

static void *sdbWorkerFp(void *pWorker) {
  SSdbRow *pRow;
  int32_t  qtype;
  void *   unUsed;

  while (1) {
    int32_t numOfMsgs = taosReadAllQitemsFromQset(tsSdbWQset, tsSdbWQall, &unUsed);
    if (numOfMsgs == 0) {
      sdbDebug("qset:%p, sdb got no message from qset, exiting", tsSdbWQset);
      break;
    }

    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(tsSdbWQall, &qtype, (void **)&pRow);
      sdbTrace("vgId:1, msg:%p, row:%p hver:%" PRIu64 ", will be processed in sdb queue", pRow->pMsg, pRow->pObj,
               pRow->pHead->version);

      pRow->code = sdbProcessWrite((qtype == TAOS_QTYPE_RPC) ? pRow : NULL, pRow->pHead, qtype, NULL);
      if (pRow->code > 0) pRow->code = 0;

      sdbTrace("vgId:1, msg:%p is processed in sdb queue, code:%x", pRow->pMsg, pRow->code);
    }

    walFsync(tsSdbMgmt.wal, true);

    // browse all items, and process them one by one
    taosResetQitems(tsSdbWQall);
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(tsSdbWQall, &qtype, (void **)&pRow);

      if (qtype == TAOS_QTYPE_RPC) {
        sdbConfirmForward(1, pRow, pRow->code);
      } else {
        if (qtype == TAOS_QTYPE_FWD) {
          syncConfirmForward(tsSdbMgmt.sync, pRow->pHead->version, pRow->code);
        }
        sdbFreeFromQueue(pRow);
      }
    }
  }

  return NULL;
}
