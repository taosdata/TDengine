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
#include "tglobal.h"
#include "dnode.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeMnode.h"
#include "mnodeDnode.h"
#include "mnodeSdb.h"

#define SDB_TABLE_LEN 12
#define SDB_SYNC_HACK 16

typedef enum {
  SDB_ACTION_INSERT,
  SDB_ACTION_DELETE,
  SDB_ACTION_UPDATE
} ESdbAction;

typedef enum {
  SDB_STATUS_OFFLINE,
  SDB_STATUS_SERVING,
  SDB_STATUS_CLOSING
} ESdbStatus;

typedef struct _SSdbTable {
  char      tableName[SDB_TABLE_LEN];
  ESdbTable tableId;
  ESdbKey   keyType;
  int32_t   hashSessions;
  int32_t   maxRowSize;
  int32_t   refCountPos;
  int32_t   autoIndex;
  int64_t   numOfRows;
  void *    iHandle;
  int32_t (*insertFp)(SSdbOper *pDesc);
  int32_t (*deleteFp)(SSdbOper *pOper);
  int32_t (*updateFp)(SSdbOper *pOper);
  int32_t (*decodeFp)(SSdbOper *pOper);
  int32_t (*encodeFp)(SSdbOper *pOper);
  int32_t (*destroyFp)(SSdbOper *pOper);
  int32_t (*restoredFp)();
} SSdbTable;

typedef struct {
  ESyncRole  role;
  ESdbStatus status;
  int64_t    version;
  void *     sync;
  void *     wal;
  SSyncCfg   cfg;
  sem_t      sem;
  int32_t    code;
  int32_t    numOfTables;
  SSdbTable *tableList[SDB_TABLE_MAX];
  pthread_mutex_t mutex;
} SSdbObject;

typedef struct {
  int32_t rowSize;
  void *  row;
} SSdbRow;

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SSdbWriteWorker;

typedef struct {
  int32_t num;
  SSdbWriteWorker *writeWorker;
} SSdbWriteWorkerPool;

static SSdbObject tsSdbObj = {0};
static taos_qset  tsSdbWriteQset;
static taos_qall  tsSdbWriteQall;
static taos_queue tsSdbWriteQueue;
static SSdbWriteWorkerPool tsSdbPool;

static int     sdbWrite(void *param, void *data, int type);
static int     sdbWriteToQueue(void *param, void *data, int type);
static void *  sdbWorkerFp(void *param);
static int32_t sdbInitWriteWorker();
static void    sdbCleanupWriteWorker();
static int32_t sdbAllocWriteQueue();
static void    sdbFreeWritequeue();

int32_t sdbGetId(void *handle) {
  return ((SSdbTable *)handle)->autoIndex;
}

int64_t sdbGetNumOfRows(void *handle) {
  return ((SSdbTable *)handle)->numOfRows;
}

uint64_t sdbGetVersion() {
  return tsSdbObj.version;
}

bool sdbIsMaster() { 
  return tsSdbObj.role == TAOS_SYNC_ROLE_MASTER; 
}

bool sdbIsServing() {
  return tsSdbObj.status == SDB_STATUS_SERVING; 
}

static void *sdbGetObjKey(SSdbTable *pTable, void *key) {
  if (pTable->keyType == SDB_KEY_VAR_STRING) {
    return *(char **)key;
  }

  return key;
}

static char *sdbGetActionStr(int32_t action) {
  switch (action) {
    case SDB_ACTION_INSERT:
      return "insert";
    case SDB_ACTION_DELETE:
      return "delete";
    case SDB_ACTION_UPDATE:
      return "update";
  }
  return "invalid";
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

static char *sdbGetKeyStrFromObj(SSdbTable *pTable, void *key) {
  return sdbGetKeyStr(pTable, sdbGetObjKey(pTable, key));
}

static void *sdbGetTableFromId(int32_t tableId) {
  return tsSdbObj.tableList[tableId];
}

static int32_t sdbInitWal() {
  SWalCfg walCfg = {.walLevel = 2, .wals = 2, .keep = 1};
  char temp[TSDB_FILENAME_LEN];
  sprintf(temp, "%s/wal", tsMnodeDir);
  tsSdbObj.wal = walOpen(temp, &walCfg);
  if (tsSdbObj.wal == NULL) {
    sdbError("failed to open sdb wal in %s", tsMnodeDir);
    return -1;
  }

  sdbInfo("open sdb wal for restore");
  walRestore(tsSdbObj.wal, NULL, sdbWrite);
  return 0;
}

static void sdbRestoreTables() {
  int32_t totalRows = 0;
  int32_t numOfTables = 0;
  for (int32_t tableId = 0; tableId < SDB_TABLE_MAX; ++tableId) {
    SSdbTable *pTable = sdbGetTableFromId(tableId);
    if (pTable == NULL) continue;
    if (pTable->restoredFp) {
      (*pTable->restoredFp)();
    }

    totalRows += pTable->numOfRows;
    numOfTables++;
    sdbDebug("table:%s, is restored, numOfRows:%" PRId64, pTable->tableName, pTable->numOfRows);
  }

  sdbInfo("sdb is restored, version:%" PRId64 " totalRows:%d numOfTables:%d", tsSdbObj.version, totalRows, numOfTables);
}

void sdbUpdateMnodeRoles() {
  if (tsSdbObj.sync == NULL) return;

  SNodesRole roles = {0};
  syncGetNodesRole(tsSdbObj.sync, &roles);

  sdbInfo("update mnodes sync roles, total:%d", tsSdbObj.cfg.replica);
  for (int32_t i = 0; i < tsSdbObj.cfg.replica; ++i) {
    SMnodeObj *pMnode = mnodeGetMnode(roles.nodeId[i]);
    if (pMnode != NULL) {
      pMnode->role = roles.role[i];
      sdbInfo("mnode:%d, role:%s", pMnode->mnodeId, mnodeGetMnodeRoleStr(pMnode->role));
      if (pMnode->mnodeId == dnodeGetDnodeId()) tsSdbObj.role = pMnode->role;
      mnodeDecMnodeRef(pMnode);
    }
  }

  mnodeUpdateMnodeIpSet();
}

static uint32_t sdbGetFileInfo(void *ahandle, char *name, uint32_t *index, uint32_t eindex, int32_t *size, uint64_t *fversion) {
  sdbUpdateMnodeRoles();
  return 0;
}

static int sdbGetWalInfo(void *ahandle, char *name, uint32_t *index) {
  return walGetWalFile(tsSdbObj.wal, name, index);
}

static void sdbNotifyRole(void *ahandle, int8_t role) {
  sdbInfo("mnode role changed from %s to %s", mnodeGetMnodeRoleStr(tsSdbObj.role), mnodeGetMnodeRoleStr(role));

  if (role == TAOS_SYNC_ROLE_MASTER && tsSdbObj.role != TAOS_SYNC_ROLE_MASTER) {
    balanceReset();
  }
  tsSdbObj.role = role;

  sdbUpdateMnodeRoles();
}

static void sdbConfirmForward(void *ahandle, void *param, int32_t code) {
  tsSdbObj.code = code;
  sem_post(&tsSdbObj.sem);
  sdbDebug("forward request confirmed, version:%" PRIu64 ", result:%s", (int64_t)param, tstrerror(code));
}

 static int32_t sdbForwardToPeer(SWalHead *pHead) {
  if (tsSdbObj.sync == NULL) return TSDB_CODE_SUCCESS;

  int32_t code = syncForwardToPeer(tsSdbObj.sync, pHead, (void*)pHead->version, TAOS_QTYPE_RPC);
  if (code > 0) {
    sdbDebug("forward request is sent, version:%" PRIu64 ", code:%d", pHead->version, code);
    sem_wait(&tsSdbObj.sem);
    return tsSdbObj.code;
  } 
  return code;
}

void sdbUpdateSync() {
  SSyncCfg syncCfg = {0};
  int32_t index = 0;

  SDMMnodeInfos *mnodes = dnodeGetMnodeInfos();
  for (int32_t i = 0; i < mnodes->nodeNum; ++i) {
    SDMMnodeInfo *node = &mnodes->nodeInfos[i];
    syncCfg.nodeInfo[i].nodeId = node->nodeId;
    taosGetFqdnPortFromEp(node->nodeEp, syncCfg.nodeInfo[i].nodeFqdn, &syncCfg.nodeInfo[i].nodePort);
    syncCfg.nodeInfo[i].nodePort += TSDB_PORT_SYNC;
    index++;
  }

  if (index == 0) {
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
  }

  syncCfg.replica = index;
  syncCfg.quorum = (syncCfg.replica == 1) ? 1:2;

  bool hasThisDnode = false;
  for (int32_t i = 0; i < syncCfg.replica; ++i) {
    if (syncCfg.nodeInfo[i].nodeId == dnodeGetDnodeId()) {
      hasThisDnode = true;
      break;
    }
  }

  if (!hasThisDnode) return;
  if (memcmp(&syncCfg, &tsSdbObj.cfg, sizeof(SSyncCfg)) == 0) return;

  sdbInfo("work as mnode, replica:%d", syncCfg.replica);
  for (int32_t i = 0; i < syncCfg.replica; ++i) {
    sdbInfo("mnode:%d, %s:%d", syncCfg.nodeInfo[i].nodeId, syncCfg.nodeInfo[i].nodeFqdn, syncCfg.nodeInfo[i].nodePort);
  }

  SSyncInfo syncInfo = {0};
  syncInfo.vgId = 1;
  syncInfo.version = sdbGetVersion();
  syncInfo.syncCfg = syncCfg;
  sprintf(syncInfo.path, "%s", tsMnodeDir);
  syncInfo.ahandle = NULL;
  syncInfo.getWalInfo = sdbGetWalInfo;
  syncInfo.getFileInfo = sdbGetFileInfo;
  syncInfo.writeToCache = sdbWriteToQueue;
  syncInfo.confirmForward = sdbConfirmForward; 
  syncInfo.notifyRole = sdbNotifyRole;
  tsSdbObj.cfg = syncCfg;
  
  if (tsSdbObj.sync) {
    syncReconfig(tsSdbObj.sync, &syncCfg);
  } else {
    tsSdbObj.sync = syncStart(&syncInfo);
  }
  sdbUpdateMnodeRoles();
}

int32_t sdbInit() {
  pthread_mutex_init(&tsSdbObj.mutex, NULL);
  sem_init(&tsSdbObj.sem, 0, 0);

  if (sdbInitWriteWorker() != 0) {
    return -1;
  }

  if (sdbInitWal() != 0) {
    return -1;
  }

  sdbRestoreTables();

  if (mnodeGetMnodesNum() == 1) {
    tsSdbObj.role = TAOS_SYNC_ROLE_MASTER;
  }

  sdbUpdateSync();

  tsSdbObj.status = SDB_STATUS_SERVING;
  return TSDB_CODE_SUCCESS;
}

void sdbCleanUp() {
  if (tsSdbObj.status != SDB_STATUS_SERVING) return;

  tsSdbObj.status = SDB_STATUS_CLOSING;
  
  sdbCleanupWriteWorker();
  sdbDebug("sdb will be closed, version:%" PRId64, tsSdbObj.version);

  if (tsSdbObj.sync) {
    syncStop(tsSdbObj.sync);
    tsSdbObj.sync = NULL;
  }

  if (tsSdbObj.wal) {
    walClose(tsSdbObj.wal);
    tsSdbObj.wal = NULL;
  }
  
  sem_destroy(&tsSdbObj.sem);
  pthread_mutex_destroy(&tsSdbObj.mutex);
}

void sdbIncRef(void *handle, void *pObj) {
  if (pObj == NULL) return;

  SSdbTable *pTable = handle;
  int32_t *  pRefCount = (int32_t *)(pObj + pTable->refCountPos);
  atomic_add_fetch_32(pRefCount, 1);
  sdbTrace("add ref to table:%s record:%p:%s:%d", pTable->tableName, pObj, sdbGetKeyStrFromObj(pTable, pObj), *pRefCount);
}

void sdbDecRef(void *handle, void *pObj) {
  if (pObj == NULL) return;

  SSdbTable *pTable = handle;
  int32_t *  pRefCount = (int32_t *)(pObj + pTable->refCountPos);
  int32_t    refCount = atomic_sub_fetch_32(pRefCount, 1);
  sdbTrace("def ref of table:%s record:%p:%s:%d", pTable->tableName, pObj, sdbGetKeyStrFromObj(pTable, pObj), *pRefCount);

  int8_t *updateEnd = pObj + pTable->refCountPos - 1;
  if (refCount <= 0 && *updateEnd) {
    sdbTrace("table:%s, record:%p:%s:%d is destroyed", pTable->tableName, pObj, sdbGetKeyStrFromObj(pTable, pObj), *pRefCount);
    SSdbOper oper = {.pObj = pObj};
    (*pTable->destroyFp)(&oper);
  }
}

static SSdbRow *sdbGetRowMeta(SSdbTable *pTable, void *key) {
  if (pTable == NULL) return NULL;

  int32_t keySize = sizeof(int32_t);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }
  
  return taosHashGet(pTable->iHandle, key, keySize);
}

static SSdbRow *sdbGetRowMetaFromObj(SSdbTable *pTable, void *key) {
  return sdbGetRowMeta(pTable, sdbGetObjKey(pTable, key));
}

void *sdbGetRow(void *handle, void *key) {
  SSdbTable *pTable = (SSdbTable *)handle;
  int32_t keySize = sizeof(int32_t);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }
  
  SSdbRow *pMeta = taosHashGet(pTable->iHandle, key, keySize);
  if (pMeta) {
    sdbIncRef(pTable, pMeta->row);
    return pMeta->row;
  } else {
    return NULL;
  }
}

static void *sdbGetRowFromObj(SSdbTable *pTable, void *key) {
  return sdbGetRow(pTable, sdbGetObjKey(pTable, key));
}

static int32_t sdbInsertHash(SSdbTable *pTable, SSdbOper *pOper) {
  SSdbRow rowMeta;
  rowMeta.rowSize = pOper->rowSize;
  rowMeta.row = pOper->pObj;

  void *  key = sdbGetObjKey(pTable, pOper->pObj);
  int32_t keySize = sizeof(int32_t);

  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  taosHashPut(pTable->iHandle, key, keySize, &rowMeta, sizeof(SSdbRow));

  sdbIncRef(pTable, pOper->pObj);
  atomic_add_fetch_32(&pTable->numOfRows, 1);

  if (pTable->keyType == SDB_KEY_AUTO) {
    pTable->autoIndex = MAX(pTable->autoIndex, *((uint32_t *)pOper->pObj));
  } else {
    atomic_add_fetch_32(&pTable->autoIndex, 1);
  }

  sdbDebug("table:%s, insert record:%s to hash, rowSize:%d numOfRows:%" PRId64 " version:%" PRIu64, pTable->tableName,
           sdbGetKeyStrFromObj(pTable, pOper->pObj), pOper->rowSize, pTable->numOfRows, sdbGetVersion());

  (*pTable->insertFp)(pOper);
  return TSDB_CODE_SUCCESS;
}

static int32_t sdbDeleteHash(SSdbTable *pTable, SSdbOper *pOper) {
  (*pTable->deleteFp)(pOper);
  
  void *  key = sdbGetObjKey(pTable, pOper->pObj);
  int32_t keySize = sizeof(int32_t);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  taosHashRemove(pTable->iHandle, key, keySize);
  atomic_sub_fetch_32(&pTable->numOfRows, 1);
  
  sdbDebug("table:%s, delete record:%s from hash, numOfRows:%" PRId64 " version:%" PRIu64, pTable->tableName,
           sdbGetKeyStrFromObj(pTable, pOper->pObj), pTable->numOfRows, sdbGetVersion());

  int8_t *updateEnd = pOper->pObj + pTable->refCountPos - 1;
  *updateEnd = 1;
  sdbDecRef(pTable, pOper->pObj);

  return TSDB_CODE_SUCCESS;
}

static int32_t sdbUpdateHash(SSdbTable *pTable, SSdbOper *pOper) {
  sdbDebug("table:%s, update record:%s in hash, numOfRows:%" PRId64 " version:%" PRIu64, pTable->tableName,
           sdbGetKeyStrFromObj(pTable, pOper->pObj), pTable->numOfRows, sdbGetVersion());

  (*pTable->updateFp)(pOper);
  return TSDB_CODE_SUCCESS;
}

static int sdbWrite(void *param, void *data, int type) {
  SSdbOper *pOper = param;
  SWalHead *pHead = data;
  int32_t tableId = pHead->msgType / 10;
  int32_t action = pHead->msgType % 10;

  SSdbTable *pTable = sdbGetTableFromId(tableId);
  assert(pTable != NULL);

  pthread_mutex_lock(&tsSdbObj.mutex);
  if (pHead->version == 0) {
     // assign version
    tsSdbObj.version++;
    pHead->version = tsSdbObj.version;
  } else {
    // for data from WAL or forward, version may be smaller
    if (pHead->version <= tsSdbObj.version) {
      pthread_mutex_unlock(&tsSdbObj.mutex);
      if (type == TAOS_QTYPE_FWD && tsSdbObj.sync != NULL) {
        sdbDebug("forward request is received, version:%" PRIu64 " confirm it", pHead->version);
        syncConfirmForward(tsSdbObj.sync, pHead->version, TSDB_CODE_SUCCESS);
      }
      return TSDB_CODE_SUCCESS;
    } else if (pHead->version != tsSdbObj.version + 1) {
      pthread_mutex_unlock(&tsSdbObj.mutex);
      sdbError("table:%s, failed to restore %s record:%s from wal, version:%" PRId64 " too large, sdb version:%" PRId64,
               pTable->tableName, sdbGetActionStr(action), sdbGetKeyStr(pTable, pHead->cont), pHead->version,
               tsSdbObj.version);
      return TSDB_CODE_MND_APP_ERROR;
    } else {
      tsSdbObj.version = pHead->version;
    }
  }

  int32_t code = walWrite(tsSdbObj.wal, pHead);
  if (code < 0) {
    pthread_mutex_unlock(&tsSdbObj.mutex);
    return code;
  }
  
  code = sdbForwardToPeer(pHead);
  pthread_mutex_unlock(&tsSdbObj.mutex);

  // from app, oper is created
  if (pOper != NULL) {
    sdbTrace("record from app is disposed, table:%s action:%s record:%s version:%" PRIu64 " result:%s",
             pTable->tableName, sdbGetActionStr(action), sdbGetKeyStr(pTable, pHead->cont), pHead->version,
             tstrerror(code));
    return code;
  }

  // from wal or forward msg, oper not created, should add into hash
  if (tsSdbObj.sync != NULL) {
    sdbTrace("record from wal forward is disposed, table:%s action:%s record:%s version:%" PRIu64 " confirm it",
             pTable->tableName, sdbGetActionStr(action), sdbGetKeyStr(pTable, pHead->cont), pHead->version);
    syncConfirmForward(tsSdbObj.sync, pHead->version, code);
  } else {
    sdbTrace("record from wal restore is disposed, table:%s action:%s record:%s version:%" PRIu64, pTable->tableName,
             sdbGetActionStr(action), sdbGetKeyStr(pTable, pHead->cont), pHead->version);
  }

  if (action == SDB_ACTION_INSERT) {
    SSdbOper oper = {.rowSize = pHead->len, .rowData = pHead->cont, .table = pTable};
    code = (*pTable->decodeFp)(&oper);
    return sdbInsertHash(pTable, &oper);
  } else if (action == SDB_ACTION_DELETE) {
    SSdbRow *rowMeta = sdbGetRowMeta(pTable, pHead->cont);
    assert(rowMeta != NULL && rowMeta->row != NULL);
    SSdbOper oper = {.table = pTable, .pObj = rowMeta->row};
    return sdbDeleteHash(pTable, &oper);
  } else if (action == SDB_ACTION_UPDATE) {
    SSdbRow *rowMeta = sdbGetRowMeta(pTable, pHead->cont);
    assert(rowMeta != NULL && rowMeta->row != NULL);
    SSdbOper oper = {.rowSize = pHead->len, .rowData = pHead->cont, .table = pTable};
    code = (*pTable->decodeFp)(&oper);
    return sdbUpdateHash(pTable, &oper);
  } else { return TSDB_CODE_MND_INVALID_MSG_TYPE; }
}

int32_t sdbInsertRow(SSdbOper *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  if (sdbGetRowFromObj(pTable, pOper->pObj)) {
    sdbError("table:%s, failed to insert record:%s, already exist", pTable->tableName, sdbGetKeyStrFromObj(pTable, pOper->pObj));
    sdbDecRef(pTable, pOper->pObj);
    return TSDB_CODE_MND_SDB_OBJ_ALREADY_THERE;
  }

  if (pTable->keyType == SDB_KEY_AUTO) {
    *((uint32_t *)pOper->pObj) = atomic_add_fetch_32(&pTable->autoIndex, 1);

    // let vgId increase from 2
    if (pTable->autoIndex == 1 && strcmp(pTable->tableName, "vgroups") == 0) {
      *((uint32_t *)pOper->pObj) = atomic_add_fetch_32(&pTable->autoIndex, 1);
    }
  }

  int32_t code = sdbInsertHash(pTable, pOper);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("table:%s, failed to insert into hash", pTable->tableName);
    return code;
  }

  // just insert data into memory
  if (pOper->type != SDB_OPER_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t size = sizeof(SSdbOper) + sizeof(SWalHead) + pTable->maxRowSize + SDB_SYNC_HACK;
  SSdbOper *pNewOper = taosAllocateQitem(size);
  
  SWalHead *pHead = (void *)pNewOper + sizeof(SSdbOper) + SDB_SYNC_HACK;
  pHead->version = 0;
  pHead->len = pOper->rowSize;
  pHead->msgType = pTable->tableId * 10 + SDB_ACTION_INSERT;

  pOper->rowData = pHead->cont;
  (*pTable->encodeFp)(pOper);
  pHead->len = pOper->rowSize;

  memcpy(pNewOper, pOper, sizeof(SSdbOper));

  if (pNewOper->pMsg != NULL) {
    sdbDebug("app:%p:%p, table:%s record:%p:%s, insert action is add to sdb queue, ", pNewOper->pMsg->rpcMsg.ahandle,
             pNewOper->pMsg, pTable->tableName, pOper->pObj, sdbGetKeyStrFromObj(pTable, pOper->pObj));
  }

  sdbIncRef(pNewOper->table, pNewOper->pObj);
  taosWriteQitem(tsSdbWriteQueue, TAOS_QTYPE_RPC, pNewOper);
  return TSDB_CODE_SUCCESS;
}

int32_t sdbDeleteRow(SSdbOper *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  SSdbRow *pMeta = sdbGetRowMetaFromObj(pTable, pOper->pObj);
  if (pMeta == NULL) {
    sdbDebug("table:%s, record is not there, delete failed", pTable->tableName);
    return TSDB_CODE_MND_SDB_OBJ_NOT_THERE;
  }

  void *pMetaRow = pMeta->row;
  if (pMetaRow == NULL) {
    sdbError("table:%s, record meta is null", pTable->tableName);
    return TSDB_CODE_MND_SDB_INVAID_META_ROW;
  }

  int32_t code = sdbDeleteHash(pTable, pOper);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("table:%s, failed to delete from hash", pTable->tableName);
    return code;
  }

  // just delete data from memory
  if (pOper->type != SDB_OPER_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t size = sizeof(SSdbOper) + sizeof(SWalHead) + pTable->maxRowSize + SDB_SYNC_HACK;
  SSdbOper *pNewOper = taosAllocateQitem(size);

  SWalHead *pHead = (void *)pNewOper + sizeof(SSdbOper) + SDB_SYNC_HACK;
  pHead->version = 0;
  pHead->msgType = pTable->tableId * 10 + SDB_ACTION_DELETE;
  
  pOper->rowData = pHead->cont;
  (*pTable->encodeFp)(pOper);
  pHead->len = pOper->rowSize;

  memcpy(pNewOper, pOper, sizeof(SSdbOper));

  if (pNewOper->pMsg != NULL) {
    sdbDebug("app:%p:%p, table:%s record:%p:%s, delete action is add to sdb queue, ", pNewOper->pMsg->rpcMsg.ahandle,
             pNewOper->pMsg, pTable->tableName, pOper->pObj, sdbGetKeyStrFromObj(pTable, pOper->pObj));
  }

  sdbIncRef(pNewOper->table, pNewOper->pObj);
  taosWriteQitem(tsSdbWriteQueue, TAOS_QTYPE_RPC, pNewOper);
  return TSDB_CODE_SUCCESS;
}

int32_t sdbUpdateRow(SSdbOper *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  SSdbRow *pMeta = sdbGetRowMetaFromObj(pTable, pOper->pObj);
  if (pMeta == NULL) {
    sdbDebug("table:%s, record is not there, update failed", pTable->tableName);
    return TSDB_CODE_MND_SDB_OBJ_NOT_THERE;
  }

  void *pMetaRow = pMeta->row;
  if (pMetaRow == NULL) {
    sdbError("table:%s, record meta is null", pTable->tableName);
    return TSDB_CODE_MND_SDB_INVAID_META_ROW;
  }

  int32_t code = sdbUpdateHash(pTable, pOper);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("table:%s, failed to update hash", pTable->tableName);
    return code;
  }

  // just update data in memory
  if (pOper->type != SDB_OPER_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t size = sizeof(SSdbOper) + sizeof(SWalHead) + pTable->maxRowSize + SDB_SYNC_HACK;
  SSdbOper *pNewOper = taosAllocateQitem(size);

  SWalHead *pHead = (void *)pNewOper + sizeof(SSdbOper) + SDB_SYNC_HACK;
  pHead->version = 0;
  pHead->msgType = pTable->tableId * 10 + SDB_ACTION_UPDATE;

  pOper->rowData = pHead->cont;
  (*pTable->encodeFp)(pOper);
  pHead->len = pOper->rowSize;

  memcpy(pNewOper, pOper, sizeof(SSdbOper));

  if (pNewOper->pMsg != NULL) {
    sdbDebug("app:%p:%p, table:%s record:%p:%s, update action is add to sdb queue, ", pNewOper->pMsg->rpcMsg.ahandle,
             pNewOper->pMsg, pTable->tableName, pOper->pObj, sdbGetKeyStrFromObj(pTable, pOper->pObj));
  }

  sdbIncRef(pNewOper->table, pNewOper->pObj);
  taosWriteQitem(tsSdbWriteQueue, TAOS_QTYPE_RPC, pNewOper);
  return TSDB_CODE_SUCCESS;
}

void *sdbFetchRow(void *handle, void *pNode, void **ppRow) {
  SSdbTable *pTable = (SSdbTable *)handle;
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

  SSdbRow *pMeta = taosHashIterGet(pIter);
  if (pMeta == NULL) {
    taosHashDestroyIter(pIter);
    return NULL;
  }

  *ppRow = pMeta->row;
  sdbIncRef(handle, pMeta->row);

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

  tstrncpy(pTable->tableName, pDesc->tableName, SDB_TABLE_LEN);
  pTable->keyType      = pDesc->keyType;
  pTable->tableId      = pDesc->tableId;
  pTable->hashSessions = pDesc->hashSessions;
  pTable->maxRowSize   = pDesc->maxRowSize;
  pTable->refCountPos  = pDesc->refCountPos;
  pTable->insertFp     = pDesc->insertFp;
  pTable->deleteFp     = pDesc->deleteFp;
  pTable->updateFp     = pDesc->updateFp;
  pTable->encodeFp     = pDesc->encodeFp;
  pTable->decodeFp     = pDesc->decodeFp;
  pTable->destroyFp    = pDesc->destroyFp;
  pTable->restoredFp   = pDesc->restoredFp;

  _hash_fn_t hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  }
  pTable->iHandle = taosHashInit(pTable->hashSessions, hashFp, true);

  tsSdbObj.numOfTables++;
  tsSdbObj.tableList[pTable->tableId] = pTable;
  return pTable;
}

void sdbCloseTable(void *handle) {
  SSdbTable *pTable = (SSdbTable *)handle;
  if (pTable == NULL) return;
  
  tsSdbObj.numOfTables--;
  tsSdbObj.tableList[pTable->tableId] = NULL;

  SHashMutableIterator *pIter = taosHashCreateIter(pTable->iHandle);
  while (taosHashIterNext(pIter)) {
    SSdbRow *pMeta = taosHashIterGet(pIter);
    if (pMeta == NULL) continue;

    SSdbOper oper = {
      .pObj = pMeta->row,
      .table = pTable,
    };

    (*pTable->destroyFp)(&oper);
  }

  taosHashDestroyIter(pIter);
  taosHashCleanup(pTable->iHandle);

  sdbDebug("table:%s, is closed, numOfTables:%d", pTable->tableName, tsSdbObj.numOfTables);
  free(pTable);
}

int32_t sdbInitWriteWorker() {
  tsSdbPool.num = 1;
  tsSdbPool.writeWorker = (SSdbWriteWorker *)calloc(sizeof(SSdbWriteWorker), tsSdbPool.num);

  if (tsSdbPool.writeWorker == NULL) return -1;
  for (int32_t i = 0; i < tsSdbPool.num; ++i) {
    SSdbWriteWorker *pWorker = tsSdbPool.writeWorker + i;
    pWorker->workerId = i;
  }

  sdbAllocWriteQueue();
  
  mInfo("sdb write is opened");
  return 0;
}

void sdbCleanupWriteWorker() {
  for (int32_t i = 0; i < tsSdbPool.num; ++i) {
    SSdbWriteWorker *pWorker = tsSdbPool.writeWorker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(tsSdbWriteQset);
    }
  }

  for (int32_t i = 0; i < tsSdbPool.num; ++i) {
    SSdbWriteWorker *pWorker = tsSdbPool.writeWorker + i;
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  sdbFreeWritequeue();
  tfree(tsSdbPool.writeWorker);

  mInfo("sdb write is closed");
}

int32_t sdbAllocWriteQueue() {
  tsSdbWriteQueue = taosOpenQueue();
  if (tsSdbWriteQueue == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  tsSdbWriteQset = taosOpenQset();
  if (tsSdbWriteQset == NULL) {
    taosCloseQueue(tsSdbWriteQueue);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  taosAddIntoQset(tsSdbWriteQset, tsSdbWriteQueue, NULL);

  tsSdbWriteQall = taosAllocateQall();
  if (tsSdbWriteQall == NULL) {
    taosCloseQset(tsSdbWriteQset);
    taosCloseQueue(tsSdbWriteQueue);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  
  for (int32_t i = 0; i < tsSdbPool.num; ++i) {
    SSdbWriteWorker *pWorker = tsSdbPool.writeWorker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, sdbWorkerFp, pWorker) != 0) {
      mError("failed to create thread to process sdb write queue, reason:%s", strerror(errno));
      taosFreeQall(tsSdbWriteQall);
      taosCloseQset(tsSdbWriteQset);
      taosCloseQueue(tsSdbWriteQueue);
      return TSDB_CODE_MND_OUT_OF_MEMORY;
    }

    pthread_attr_destroy(&thAttr);
    mDebug("sdb write worker:%d is launched, total:%d", pWorker->workerId, tsSdbPool.num);
  }

  mDebug("sdb write queue:%p is allocated", tsSdbWriteQueue);
  return TSDB_CODE_SUCCESS;
}

void sdbFreeWritequeue() {
  taosCloseQset(tsSdbWriteQueue);
  taosFreeQall(tsSdbWriteQall);
  taosCloseQset(tsSdbWriteQset);
  tsSdbWriteQall = NULL;
  tsSdbWriteQset = NULL;
  tsSdbWriteQueue = NULL;
}

int sdbWriteToQueue(void *param, void *data, int type) {
  SWalHead *pHead = data;
  int size = sizeof(SWalHead) + pHead->len;
  SWalHead *pWal = (SWalHead *)taosAllocateQitem(size);
  memcpy(pWal, pHead, size);

  taosWriteQitem(tsSdbWriteQueue, type, pWal);
  return 0;
}

static void *sdbWorkerFp(void *param) {
  SWalHead *pHead;
  SSdbOper *pOper;
  int32_t   type;
  int32_t   numOfMsgs;
  void *    item;
  void *    unUsed;

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(tsSdbWriteQset, tsSdbWriteQall, &unUsed);
    if (numOfMsgs == 0) {
      sdbDebug("sdbWorkerFp: got no message from qset, exiting...");
      break;
    }

    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(tsSdbWriteQall, &type, &item);
      if (type == TAOS_QTYPE_RPC) {
        pOper = (SSdbOper *)item;
        pHead = (void *)pOper + sizeof(SSdbOper) + SDB_SYNC_HACK;
      } else {
        pHead = (SWalHead *)item;
        pOper = NULL;
      }

      if (pOper != NULL && pOper->pMsg != NULL) {
        sdbDebug("app:%p:%p, table:%s record:%p:%s version:%" PRIu64 ", will be processed in sdb queue",
                 pOper->pMsg->rpcMsg.ahandle, pOper->pMsg, ((SSdbTable *)pOper->table)->tableName, pOper->pObj,
                 sdbGetKeyStr(pOper->table, pHead->cont), pHead->version);
      }

      int32_t code = sdbWrite(pOper, pHead, type);
      if (pOper) pOper->retCode = code;
    }

    walFsync(tsSdbObj.wal);

    // browse all items, and process them one by one
    taosResetQitems(tsSdbWriteQall);
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(tsSdbWriteQall, &type, &item);
      if (type == TAOS_QTYPE_RPC) {
        pOper = (SSdbOper *)item;
        if (pOper != NULL && pOper->cb != NULL) {
          sdbTrace("app:%p:%p, will do callback func, index:%d", pOper->pMsg->rpcMsg.ahandle, pOper->pMsg, i);
          pOper->retCode = (*pOper->cb)(pOper->pMsg, pOper->retCode);
        }

        if (pOper != NULL && pOper->pMsg != NULL) {
          sdbTrace("app:%p:%p, msg is processed, result:%s", pOper->pMsg->rpcMsg.ahandle, pOper->pMsg,
                   tstrerror(pOper->retCode));
        }

        if (pOper != NULL) {
          sdbDecRef(pOper->table, pOper->pObj);
        }

        dnodeSendRpcMnodeWriteRsp(pOper->pMsg, pOper->retCode);
      }
      taosFreeQitem(item);
    }
  }

  return NULL;
}
