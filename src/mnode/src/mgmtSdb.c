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
#include "trpc.h"
#include "tutil.h"
#include "tbalance.h"
#include "tqueue.h"
#include "twal.h"
#include "tsync.h"
#include "tglobal.h"
#include "dnode.h"
#include "mgmtDef.h"
#include "mgmtInt.h"
#include "mgmtMnode.h"
#include "mgmtDnode.h"
#include "mgmtSdb.h"

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
  char      tableName[TSDB_DB_NAME_LEN + 1];
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
  pthread_mutex_t mutex;
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

static SSdbObject tsSdbObj = {0};
static int sdbWrite(void *param, void *data, int type);

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

  sdbTrace("open sdb wal for restore");
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
    sdbTrace("table:%s, is restored, numOfRows:%d", pTable->tableName, pTable->numOfRows);
  }

  sdbTrace("sdb is restored, version:%d totalRows:%d numOfTables:%d", tsSdbObj.version, totalRows, numOfTables);
}

void sdbUpdateMnodeRoles() {
  if (tsSdbObj.sync == NULL) return;

  SNodesRole roles = {0};
  syncGetNodesRole(tsSdbObj.sync, &roles);

  sdbPrint("update mnodes:%d sync roles", tsSdbObj.cfg.replica);
  for (int32_t i = 0; i < tsSdbObj.cfg.replica; ++i) {
    SMnodeObj *pMnode = mgmtGetMnode(roles.nodeId[i]);
    if (pMnode != NULL) {
      pMnode->role = roles.role[i];
      sdbPrint("mnode:%d, role:%s", pMnode->mnodeId, mgmtGetMnodeRoleStr(pMnode->role));
      if (pMnode->mnodeId == dnodeGetDnodeId()) tsSdbObj.role = pMnode->role;
      mgmtDecMnodeRef(pMnode);
    }
  }

  mgmtUpdateMnodeIpSet();
}

static uint32_t sdbGetFileInfo(void *ahandle, char *name, uint32_t *index, int32_t *size, uint64_t *fversion) {
  sdbUpdateMnodeRoles();
  return 0;
}

static int sdbGetWalInfo(void *ahandle, char *name, uint32_t *index) {
  return walGetWalFile(tsSdbObj.wal, name, index);
}

static void sdbNotifyRole(void *ahandle, int8_t role) {
  sdbPrint("mnode role changed from %s to %s", mgmtGetMnodeRoleStr(tsSdbObj.role), mgmtGetMnodeRoleStr(role));

  if (role == TAOS_SYNC_ROLE_MASTER && tsSdbObj.role != TAOS_SYNC_ROLE_MASTER) {
    balanceReset();
  }
  tsSdbObj.role = role;

  sdbUpdateMnodeRoles();
}

static void sdbConfirmForward(void *ahandle, void *param, int32_t code) {
  tsSdbObj.code = code;
  sem_post(&tsSdbObj.sem);
  sdbTrace("forward request confirmed, version:%" PRIu64 ", result:%s", (int64_t)param, tstrerror(code));
}

static int32_t sdbForwardToPeer(SWalHead *pHead) {
  if (tsSdbObj.sync == NULL) return TSDB_CODE_SUCCESS;

  int32_t code = syncForwardToPeer(tsSdbObj.sync, pHead, (void*)pHead->version, TAOS_QTYPE_RPC);
  if (code > 0) {
    sdbTrace("forward request is sent, version:%" PRIu64 ", code:%d", pHead->version, code);
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
      pIter = mgmtGetNextMnode(pIter, &pMnode);
      if (pMnode == NULL) break;

      syncCfg.nodeInfo[index].nodeId = pMnode->mnodeId;

      SDnodeObj *pDnode = mgmtGetDnode(pMnode->mnodeId);
      if (pDnode != NULL) {
        syncCfg.nodeInfo[index].nodePort = pDnode->dnodePort + TSDB_PORT_SYNC;
        strcpy(syncCfg.nodeInfo[index].nodeFqdn, pDnode->dnodeEp);
        index++;
      }

      mgmtDecDnodeRef(pDnode);
      mgmtDecMnodeRef(pMnode);
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

  sdbPrint("work as mnode, replica:%d", syncCfg.replica);
  for (int32_t i = 0; i < syncCfg.replica; ++i) {
    sdbPrint("mnode:%d, %s:%d", syncCfg.nodeInfo[i].nodeId, syncCfg.nodeInfo[i].nodeFqdn, syncCfg.nodeInfo[i].nodePort);
  }

  SSyncInfo syncInfo = {0};
  syncInfo.vgId = 1;
  syncInfo.version = sdbGetVersion();
  syncInfo.syncCfg = syncCfg;
  sprintf(syncInfo.path, "%s", tsMnodeDir);
  syncInfo.ahandle = NULL;
  syncInfo.getWalInfo = sdbGetWalInfo;
  syncInfo.getFileInfo = sdbGetFileInfo;
  syncInfo.writeToCache = sdbWrite;
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

  if (sdbInitWal() != 0) {
    return -1;
  }
  
  sdbRestoreTables();

  if (mgmtGetMnodesNum() == 1) {
    tsSdbObj.role = TAOS_SYNC_ROLE_MASTER;
  }

  sdbUpdateSync();

  tsSdbObj.status = SDB_STATUS_SERVING;
  return TSDB_CODE_SUCCESS;
}

void sdbCleanUp() {
  if (tsSdbObj.status != SDB_STATUS_SERVING) return;

  tsSdbObj.status = SDB_STATUS_CLOSING;
  
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
  if (0 && (pTable->tableId == SDB_TABLE_MNODE || pTable->tableId == SDB_TABLE_DNODE)) {
    sdbTrace("add ref to table:%s record:%s:%d", pTable->tableName, sdbGetKeyStrFromObj(pTable, pObj), *pRefCount);
  }
}

void sdbDecRef(void *handle, void *pObj) {
  if (pObj == NULL) return;

  SSdbTable *pTable = handle;
  int32_t *  pRefCount = (int32_t *)(pObj + pTable->refCountPos);
  int32_t    refCount = atomic_sub_fetch_32(pRefCount, 1);
  if (0 && (pTable->tableId == SDB_TABLE_MNODE || pTable->tableId == SDB_TABLE_DNODE)) {
    sdbTrace("def ref of table:%s record:%s:%d", pTable->tableName, sdbGetKeyStrFromObj(pTable, pObj), *pRefCount);
  }

  int8_t *updateEnd = pObj + pTable->refCountPos - 1;
  if (refCount <= 0 && *updateEnd) {
    sdbTrace("table:%s, record:%s:%d is destroyed", pTable->tableName, sdbGetKeyStrFromObj(pTable, pObj), *pRefCount);
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
  SSdbRow * pMeta;

  if (handle == NULL) return NULL;

  pthread_mutex_lock(&pTable->mutex);

  int32_t keySize = sizeof(int32_t);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }
  pMeta = taosHashGet(pTable->iHandle, key, keySize);

  if (pMeta) sdbIncRef(pTable, pMeta->row);
  pthread_mutex_unlock(&pTable->mutex);

  if (pMeta == NULL) return NULL;

  return pMeta->row;
}

static void *sdbGetRowFromObj(SSdbTable *pTable, void *key) {
  return sdbGetRow(pTable, sdbGetObjKey(pTable, key));
}

static int32_t sdbInsertHash(SSdbTable *pTable, SSdbOper *pOper) {
  SSdbRow rowMeta;
  rowMeta.rowSize = pOper->rowSize;
  rowMeta.row = pOper->pObj;

  pthread_mutex_lock(&pTable->mutex);

  void *  key = sdbGetObjKey(pTable, pOper->pObj);
  int32_t keySize = sizeof(int32_t);

  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  taosHashPut(pTable->iHandle, key, keySize, &rowMeta, sizeof(SSdbRow));

  sdbIncRef(pTable, pOper->pObj);
  pTable->numOfRows++;

  if (pTable->keyType == SDB_KEY_AUTO) {
    pTable->autoIndex = MAX(pTable->autoIndex, *((uint32_t *)pOper->pObj));
  } else {
    pTable->autoIndex++;
  }

  pthread_mutex_unlock(&pTable->mutex);

  sdbTrace("table:%s, insert record:%s to hash, rowSize:%d vnumOfRows:%d version:%" PRIu64, pTable->tableName,
           sdbGetKeyStrFromObj(pTable, pOper->pObj), pOper->rowSize, pTable->numOfRows, sdbGetVersion());

  (*pTable->insertFp)(pOper);
  return TSDB_CODE_SUCCESS;
}

static int32_t sdbDeleteHash(SSdbTable *pTable, SSdbOper *pOper) {
  (*pTable->deleteFp)(pOper);
  
  pthread_mutex_lock(&pTable->mutex);

  void *  key = sdbGetObjKey(pTable, pOper->pObj);
  int32_t keySize = sizeof(int32_t);

  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  taosHashRemove(pTable->iHandle, key, keySize);

  pTable->numOfRows--;
  pthread_mutex_unlock(&pTable->mutex);

  sdbTrace("table:%s, delete record:%s from hash, numOfRows:%d version:%" PRIu64, pTable->tableName,
           sdbGetKeyStrFromObj(pTable, pOper->pObj), pTable->numOfRows, sdbGetVersion());

  int8_t *updateEnd = pOper->pObj + pTable->refCountPos - 1;
  *updateEnd = 1;
  sdbDecRef(pTable, pOper->pObj);

  return TSDB_CODE_SUCCESS;
}

static int32_t sdbUpdateHash(SSdbTable *pTable, SSdbOper *pOper) {
  sdbTrace("table:%s, update record:%s in hash, numOfRows:%d version:%" PRIu64, pTable->tableName,
           sdbGetKeyStrFromObj(pTable, pOper->pObj), pTable->numOfRows, sdbGetVersion());

  (*pTable->updateFp)(pOper);
  return TSDB_CODE_SUCCESS;
}

static int sdbWrite(void *param, void *data, int type) {
  SWalHead *pHead = data;
  int32_t   tableId = pHead->msgType / 10;
  int32_t   action = pHead->msgType % 10;

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
        sdbTrace("forward request is received, version:%" PRIu64 " confirm it", pHead->version);
        syncConfirmForward(tsSdbObj.sync, pHead->version, TSDB_CODE_SUCCESS);
      }
      return TSDB_CODE_SUCCESS;
    } else if (pHead->version != tsSdbObj.version + 1) {
      pthread_mutex_unlock(&tsSdbObj.mutex);
      sdbError("table:%s, failed to restore %s record:%s from wal, version:%" PRId64 " too large, sdb version:%" PRId64,
               pTable->tableName, sdbGetActionStr(action), sdbGetKeyStr(pTable, pHead->cont), pHead->version,
               tsSdbObj.version);
      return TSDB_CODE_OTHERS;
    } else {
      tsSdbObj.version = pHead->version;
    }
  }

  int32_t code = walWrite(tsSdbObj.wal, pHead);
  if (code < 0) {
    pthread_mutex_unlock(&tsSdbObj.mutex);
    return code;
  }
  walFsync(tsSdbObj.wal);

  code = sdbForwardToPeer(pHead);
  pthread_mutex_unlock(&tsSdbObj.mutex);

  // from app, oper is created
  if (param != NULL) {
    //sdbTrace("request from app is disposed, version:%" PRIu64 " code:%s", pHead->version, tstrerror(code));
    return code;
  }
  
  // from wal or forward msg, oper not created, should add into hash
  if (tsSdbObj.sync != NULL) {
    sdbTrace("forward request is received, version:%" PRIu64 " result:%s, confirm it", pHead->version, tstrerror(code));
    syncConfirmForward(tsSdbObj.sync, pHead->version, code);
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
  } else { return TSDB_CODE_INVALID_MSG_TYPE; }
}

int32_t sdbInsertRow(SSdbOper *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return -1;

  if (sdbGetRowFromObj(pTable, pOper->pObj)) {
    sdbError("table:%s, failed to insert record:%s, already exist", pTable->tableName, sdbGetKeyStrFromObj(pTable, pOper->pObj));
    sdbDecRef(pTable, pOper->pObj);
    return TSDB_CODE_ALREADY_THERE;
  }

  if (pTable->keyType == SDB_KEY_AUTO) {
    pthread_mutex_lock(&pTable->mutex);
    *((uint32_t *)pOper->pObj) = ++pTable->autoIndex;

    // let vgId increase from 2
    if (pTable->autoIndex == 1 && strcmp(pTable->tableName, "vgroups") == 0) {
      *((uint32_t *)pOper->pObj) = ++pTable->autoIndex;
    }
    pthread_mutex_unlock(&pTable->mutex);
  }

  if (pOper->type == SDB_OPER_GLOBAL) {
    int32_t   size = sizeof(SWalHead) + pTable->maxRowSize;
    SWalHead *pHead = taosAllocateQitem(size);
    pHead->version = 0;
    pHead->len = pOper->rowSize;
    pHead->msgType = pTable->tableId * 10 + SDB_ACTION_INSERT;

    pOper->rowData = pHead->cont;
    (*pTable->encodeFp)(pOper);
    pHead->len = pOper->rowSize;

    int32_t code = sdbWrite(pOper, pHead, pHead->msgType);
    taosFreeQitem(pHead);
    if (code < 0) return code;
  }

  return sdbInsertHash(pTable, pOper);
}

int32_t sdbDeleteRow(SSdbOper *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return -1;

  SSdbRow *pMeta = sdbGetRowMetaFromObj(pTable, pOper->pObj);
  if (pMeta == NULL) {
    sdbTrace("table:%s, record is not there, delete failed", pTable->tableName);
    return -1;
  }

  void * pMetaRow = pMeta->row;
  assert(pMetaRow != NULL);

  if (pOper->type == SDB_OPER_GLOBAL) {
    void *  key = sdbGetObjKey(pTable, pOper->pObj);
    int32_t keySize = 0;
    switch (pTable->keyType) {
      case SDB_KEY_STRING:
      case SDB_KEY_VAR_STRING:
        keySize = strlen((char *)key) + 1;
        break;
      case SDB_KEY_INT:
      case SDB_KEY_AUTO:
        keySize = sizeof(uint32_t);
        break;
      default:
        return -1;
    }

    int32_t   size = sizeof(SWalHead) + keySize;
    SWalHead *pHead = taosAllocateQitem(size);
    pHead->version = 0;
    pHead->len = keySize;
    pHead->msgType = pTable->tableId * 10 + SDB_ACTION_DELETE;
    memcpy(pHead->cont, key, keySize);

    int32_t code = sdbWrite(pOper, pHead, pHead->msgType);
    taosFreeQitem(pHead);
    if (code < 0) return code;
  }

  return sdbDeleteHash(pTable, pOper);
}

int32_t sdbUpdateRow(SSdbOper *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return -1;

  SSdbRow *pMeta = sdbGetRowMetaFromObj(pTable, pOper->pObj);
  if (pMeta == NULL) {
    sdbTrace("table:%s, record is not there, delete failed", pTable->tableName);
    return -1;
  }

  void * pMetaRow = pMeta->row;
  assert(pMetaRow != NULL);

  if (pOper->type == SDB_OPER_GLOBAL) {
    int32_t   size = sizeof(SWalHead) + pTable->maxRowSize;
    SWalHead *pHead = taosAllocateQitem(size);
    pHead->version = 0;
    pHead->msgType = pTable->tableId * 10 + SDB_ACTION_UPDATE;

    pOper->rowData = pHead->cont;
    (*pTable->encodeFp)(pOper);
    pHead->len = pOper->rowSize;

    int32_t code = sdbWrite(pOper, pHead, pHead->msgType);
    taosFreeQitem(pHead);
    if (code < 0) return code;
  } 
  
  return sdbUpdateHash(pTable, pOper);
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

  strcpy(pTable->tableName, pDesc->tableName);
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
  if (pTable->keyType == SDB_KEY_STRING) {
    hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  }
  pTable->iHandle = taosHashInit(pTable->hashSessions, hashFp, true);

  pthread_mutex_init(&pTable->mutex, NULL);

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

  pthread_mutex_destroy(&pTable->mutex);
  
  sdbTrace("table:%s, is closed, numOfTables:%d", pTable->tableName, tsSdbObj.numOfTables);
  free(pTable);
}

