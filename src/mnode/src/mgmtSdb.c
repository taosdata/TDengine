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
#include "tlog.h"
#include "trpc.h"
#include "treplica.h"
#include "tqueue.h"
#include "twal.h"
#include "hashint.h"
#include "hashstr.h"
#include "mgmtLog.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"

typedef struct _SSdbTable {
  char        tableName[TSDB_DB_NAME_LEN + 1];
  ESdbTable   tableId;
  ESdbKeyType keyType;
  int32_t     hashSessions;
  int32_t     maxRowSize;
  int32_t     refCountPos;
  int32_t     autoIndex;
  int64_t     numOfRows;
  void *      iHandle;
  int32_t   (*insertFp)(SSdbOperDesc *pDesc);
  int32_t   (*deleteFp)(SSdbOperDesc *pOper);
  int32_t   (*updateFp)(SSdbOperDesc *pOper);
  int32_t   (*decodeFp)(SSdbOperDesc *pOper);
  int32_t   (*encodeFp)(SSdbOperDesc *pOper);
  int32_t   (*destroyFp)(SSdbOperDesc *pOper);
  int32_t   (*restoredFp)();
  pthread_mutex_t mutex;
} SSdbTable;

typedef struct {
  int32_t rowSize;
  void *  row;
} SRowMeta;

typedef enum {
  SDB_ACTION_INSERT,
  SDB_ACTION_DELETE,
  SDB_ACTION_UPDATE
} ESdbActionType;

static SSdbTable *tsSdbTableList[SDB_TABLE_MAX] = {0};
static int32_t    tsSdbNumOfTables = 0;
static SSdbObject * tsSdbObj;

static void *(*sdbInitIndexFp[])(int32_t maxRows, int32_t dataSize) = {sdbOpenStrHash, sdbOpenIntHash, sdbOpenIntHash};
static void *(*sdbAddIndexFp[])(void *handle, void *key, void *data) = {sdbAddStrHash, sdbAddIntHash, sdbAddIntHash};
static void  (*sdbDeleteIndexFp[])(void *handle, void *key) = {sdbDeleteStrHash, sdbDeleteIntHash, sdbDeleteIntHash};
static void *(*sdbGetIndexFp[])(void *handle, void *key) = {sdbGetStrHashData, sdbGetIntHashData, sdbGetIntHashData};
static void  (*sdbCleanUpIndexFp[])(void *handle) = {sdbCloseStrHash, sdbCloseIntHash, sdbCloseIntHash};
static void *(*sdbFetchRowFp[])(void *handle, void *ptr, void **ppRow) = {sdbFetchStrHashData, sdbFetchIntHashData, sdbFetchIntHashData};

int32_t  sdbGetId(void *handle) { return ((SSdbTable *)handle)->autoIndex; }
int64_t  sdbGetNumOfRows(void *handle) { return ((SSdbTable *)handle)->numOfRows; }

uint64_t sdbGetVersion() {
  if (tsSdbObj)
    return tsSdbObj->version;
  else
    return 0;
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

static char *sdbGetkeyStr(SSdbTable *pTable, void *row) {
  static char str[16];
  switch (pTable->keyType) {
    case SDB_KEY_STRING:
      return (char *)row;
    case SDB_KEY_INT:
    case SDB_KEY_AUTO:
      sprintf(str, "%d", *(int32_t *)row);
      return str;
    default:
      return "invalid";
  }
}

static void *sdbGetTableFromId(int32_t tableId) {
  return tsSdbTableList[tableId];
}

int32_t sdbInit() {
  tsSdbObj = calloc(1, sizeof(SSdbObject));
  pthread_mutex_init(&tsSdbObj->mutex, NULL);

  SWalCfg walCfg = {.commitLog = 2, .wals = 2, .keep = 1};
  tsSdbObj->wal = walOpen(tsMnodeDir, &walCfg);
  if (tsSdbObj->wal == NULL) {
    sdbError("failed to open sdb in %s", tsMnodeDir);
    return -1;
  }

  sdbTrace("open sdb file for read");
  walRestore(tsSdbObj->wal, tsSdbObj, sdbProcessWrite);

  int32_t totalRows = 0;
  int32_t numOfTables = 0;
  for (int32_t tableId = SDB_TABLE_DNODE; tableId < SDB_TABLE_MAX; ++tableId) {
    SSdbTable *pTable = sdbGetTableFromId(tableId);
    if (pTable == NULL) continue;
    if (pTable->restoredFp) {
      (*pTable->restoredFp)();
    }

    totalRows += pTable->numOfRows;
    numOfTables++;
    sdbTrace("table:%s, is initialized, numOfRows:%d", pTable->tableName, pTable->numOfRows);
  }

  sdbTrace("sdb is initialized, version:%d totalRows:%d numOfTables:%d", tsSdbObj->version, totalRows, numOfTables);
  
  replicaNotify();

  return TSDB_CODE_SUCCESS;
}

void sdbCleanUp() {
  if (tsSdbObj) {
    pthread_mutex_destroy(&tsSdbObj->mutex);
    walClose(tsSdbObj->wal);
    free(tsSdbObj);
    tsSdbObj = NULL;
  }
}

void sdbIncRef(void *handle, void *pRow) {
  if (pRow) {
    SSdbTable *pTable = handle;
    int32_t *  pRefCount = (int32_t *)(pRow + pTable->refCountPos);
    atomic_add_fetch_32(pRefCount, 1);
    if (0 && strcmp(pTable->tableName, "dnodes") == 0) {
      sdbTrace("table:%s, add ref to record:%s:%s:%d", pTable->tableName, pTable->tableName, sdbGetkeyStr(pTable, pRow),
               *pRefCount);
    }
  }
}

void sdbDecRef(void *handle, void *pRow) {
  if (pRow) {
    SSdbTable *pTable = handle;
    int32_t *  pRefCount = (int32_t *)(pRow + pTable->refCountPos);
    int32_t    refCount = atomic_sub_fetch_32(pRefCount, 1);
    if (0 && strcmp(pTable->tableName, "dnodes") == 0) {
      sdbTrace("table:%s, def ref of record:%s:%s:%d", pTable->tableName, pTable->tableName, sdbGetkeyStr(pTable, pRow),
               *pRefCount);
    }
    int8_t *updateEnd = pRow + pTable->refCountPos - 1;
    if (refCount <= 0 && *updateEnd) {
      sdbTrace("table:%s, record:%s:%s:%d is destroyed", pTable->tableName, pTable->tableName,
               sdbGetkeyStr(pTable, pRow), *pRefCount);
      SSdbOperDesc oper = {.pObj = pRow};
      (*pTable->destroyFp)(&oper);
    }
  }
}

static SRowMeta *sdbGetRowMeta(void *handle, void *key) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta * pMeta;

  if (handle == NULL) return NULL;

  pMeta = (*sdbGetIndexFp[pTable->keyType])(pTable->iHandle, key);

  return pMeta;
}

void *sdbGetRow(void *handle, void *key) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta * pMeta;

  if (handle == NULL) return NULL;

  pthread_mutex_lock(&pTable->mutex);
  pMeta = (*sdbGetIndexFp[pTable->keyType])(pTable->iHandle, key);
  if (pMeta) sdbIncRef(pTable, pMeta->row);
  pthread_mutex_unlock(&pTable->mutex);

  if (pMeta == NULL) {
    return NULL;
  }

  return pMeta->row;
}

static int32_t sdbInsertLocal(SSdbTable *pTable, SSdbOperDesc *pOper) {
  SRowMeta rowMeta;
  rowMeta.rowSize = pOper->rowSize;
  rowMeta.row = pOper->pObj;

  pthread_mutex_lock(&pTable->mutex);
  (*sdbAddIndexFp[pTable->keyType])(pTable->iHandle, pOper->pObj, &rowMeta);
  sdbIncRef(pTable, pOper->pObj);
  pTable->numOfRows++;

  if (pTable->keyType == SDB_KEY_AUTO) {
    pTable->autoIndex = MAX(pTable->autoIndex, *((uint32_t *)pOper->pObj));
  }

  pthread_mutex_unlock(&pTable->mutex);

  sdbTrace("table:%s, insert record:%s, numOfRows:%d", pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj),
           pTable->numOfRows);

  (*pTable->insertFp)(pOper);
  return TSDB_CODE_SUCCESS;
}

static int32_t sdbDeleteLocal(SSdbTable *pTable, SSdbOperDesc *pOper) {
  pthread_mutex_lock(&pTable->mutex);
  (*sdbDeleteIndexFp[pTable->keyType])(pTable->iHandle, pOper->pObj);
  pTable->numOfRows--;
  pthread_mutex_unlock(&pTable->mutex);

  sdbTrace("table:%s, delete record:%s, numOfRows:%d", pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj),
           pTable->numOfRows);

  (*pTable->deleteFp)(pOper);
  int8_t *updateEnd = pOper->pObj + pTable->refCountPos - 1;
  *updateEnd = 1;
  sdbDecRef(pTable, pOper->pObj);

  return TSDB_CODE_SUCCESS;
}

static int32_t sdbUpdateLocal(SSdbTable *pTable, SSdbOperDesc *pOper) {
  sdbTrace("table:%s, update record:%s, numOfRows:%d", pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj),
           pTable->numOfRows);

  (*pTable->updateFp)(pOper);
  return TSDB_CODE_SUCCESS;
}

static int32_t sdbProcessWriteFromApp(SSdbTable *pTable, SWalHead *pHead, int32_t action) {
  int32_t code = 0;

  pthread_mutex_lock(&tsSdbObj->mutex);
  tsSdbObj->version++;
  pHead->version = tsSdbObj->version;

  code = replicaForwardReqToPeer(pHead);
  if (code != TSDB_CODE_SUCCESS) {
    pthread_mutex_unlock(&tsSdbObj->mutex);
    sdbError("table:%s, failed to forward %s record:%s from file, version:%" PRId64 ", reason:%s", pTable->tableName,
             sdbGetActionStr(action), sdbGetkeyStr(pTable, pHead->cont), pHead->version, tstrerror(code));
    return code;
  }

  code = walWrite(tsSdbObj->wal, pHead);
  pthread_mutex_unlock(&tsSdbObj->mutex);

  if (code < 0) {
    sdbError("table:%s, failed to %s record:%s to file, version:%" PRId64 ", reason:%s", pTable->tableName,
             sdbGetActionStr(action), sdbGetkeyStr(pTable, pHead->cont), pHead->version, tstrerror(code));
  } else {
    sdbTrace("table:%s, success to %s record:%s to file, version:%" PRId64, pTable->tableName, sdbGetActionStr(action),
             sdbGetkeyStr(pTable, pHead->cont), pHead->version);
  }

  walFsync(tsSdbObj->wal);
  taosFreeQitem(pHead);
  return code;
}

static int32_t sdbProcessWriteFromWal(SSdbTable *pTable, SWalHead *pHead, int32_t action) {
  pthread_mutex_lock(&tsSdbObj->mutex);
  if (pHead->version <= tsSdbObj->version) {
    pthread_mutex_unlock(&tsSdbObj->mutex);
    return TSDB_CODE_SUCCESS;
  } else if (pHead->version != tsSdbObj->version + 1) {
    pthread_mutex_unlock(&tsSdbObj->mutex);
    sdbError("table:%s, failed to restore %s record:%s from file, version:%" PRId64 " too large, sdb version:%" PRId64,
             pTable->tableName, sdbGetActionStr(action), sdbGetkeyStr(pTable, pHead->cont), pHead->version,
             tsSdbObj->version);
    return TSDB_CODE_OTHERS;
  }

  tsSdbObj->version = pHead->version;
  sdbTrace("table:%s, success to restore %s record:%s from file, version:%" PRId64, pTable->tableName,
           sdbGetActionStr(action), sdbGetkeyStr(pTable, pHead->cont), pHead->version);

  int32_t code = -1;
  if (action == SDB_ACTION_INSERT) {
    SSdbOperDesc oper = {
        .rowSize = pHead->len,
        .rowData = pHead->cont,
        .table = pTable,
    };
    code = (*pTable->decodeFp)(&oper);
    if (code < 0) {
      sdbTrace("table:%s, failed to decode %s record:%s from file, version:%" PRId64, pTable->tableName,
               sdbGetActionStr(action), sdbGetkeyStr(pTable, pHead->cont), pHead->version);
      pthread_mutex_unlock(&tsSdbObj->mutex);
      return code;
    }

    code = sdbInsertLocal(pTable, &oper);
  } else if (action == SDB_ACTION_DELETE) {
    SRowMeta *rowMeta = sdbGetRowMeta(pTable, pHead->cont);
    assert(rowMeta != NULL && rowMeta->row != NULL);

    SSdbOperDesc oper = {
        .table = pTable,
        .pObj = rowMeta->row,
    };

    code = sdbDeleteLocal(pTable, &oper);
  } else if (action == SDB_ACTION_UPDATE) {
    SRowMeta *rowMeta = sdbGetRowMeta(pTable, pHead->cont);
    assert(rowMeta != NULL && rowMeta->row != NULL);

    SSdbOperDesc oper = {
        .rowSize = pHead->len,
        .rowData = pHead->cont,
        .table = pTable,
    };
    code = (*pTable->decodeFp)(&oper);
    if (code < 0) {
      sdbTrace("table:%s, failed to decode %s record:%s from file, version:%" PRId64, pTable->tableName,
               sdbGetActionStr(action), sdbGetkeyStr(pTable, pHead->cont), pHead->version);
      pthread_mutex_unlock(&tsSdbObj->mutex);
      return code;
    }
    code = sdbUpdateLocal(pTable, &oper);
  }

  pthread_mutex_unlock(&tsSdbObj->mutex);
  return code;
}

int sdbProcessWrite(void *param, void *data, int type) {
  SWalHead *pHead = data;
  int32_t   tableId = pHead->msgType / 10;
  int32_t   action = pHead->msgType % 10;

  SSdbTable *pTable = sdbGetTableFromId(tableId);
  assert(pTable != NULL);

  if (pHead->version == 0) {
    return sdbProcessWriteFromApp(pTable, pHead, action);
  } else {
    return sdbProcessWriteFromWal(pTable, pHead, action);
  }
}

int32_t sdbInsertRow(SSdbOperDesc *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return -1;

  if (sdbGetRow(pTable, pOper->pObj)) {
    sdbError("table:%s, failed to insert record:%s, already exist", pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj));
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

    int32_t code = sdbProcessWrite(tsSdbObj, pHead, pHead->msgType);
    if (code < 0) return code;
  } 
  
  return sdbInsertLocal(pTable, pOper);
}

// row here can be object or null-terminated string
int32_t sdbDeleteRow(SSdbOperDesc *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return -1;

  SRowMeta *pMeta = sdbGetRowMeta(pTable, pOper->pObj);
  if (pMeta == NULL) {
    sdbTrace("table:%s, record is not there, delete failed", pTable->tableName);
    return -1;
  }

  void * pMetaRow = pMeta->row;
  assert(pMetaRow != NULL);

  if (pOper->type == SDB_OPER_GLOBAL) {
    int32_t rowSize = 0;
    switch (pTable->keyType) {
      case SDB_KEY_STRING:
        rowSize = strlen((char *)pOper->pObj) + 1;
        break;
      case SDB_KEY_INT:
      case SDB_KEY_AUTO:
        rowSize = sizeof(uint64_t);
        break;
      default:
        return -1;
    }

    int32_t   size = sizeof(SWalHead) + rowSize;
    SWalHead *pHead = taosAllocateQitem(size);
    pHead->version = 0;
    pHead->len = rowSize;
    pHead->msgType = pTable->tableId * 10 + SDB_ACTION_DELETE;
    memcpy(pHead->cont, pOper->pObj, rowSize);

    int32_t code = sdbProcessWrite(tsSdbObj, pHead, pHead->msgType);
    if (code < 0) return code;
  } 
  
  return sdbDeleteLocal(pTable, pOper);
}

int32_t sdbUpdateRow(SSdbOperDesc *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return -1;

  SRowMeta *pMeta = sdbGetRowMeta(pTable, pOper->pObj);
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

    int32_t code = sdbProcessWrite(tsSdbObj, pHead, pHead->msgType);
    if (code < 0) return code;
  }
  
  return sdbUpdateLocal(pTable, pOper);
}

void *sdbFetchRow(void *handle, void *pNode, void **ppRow) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta * pMeta;

  *ppRow = NULL;
  if (pTable == NULL) return NULL;

  pNode = (*sdbFetchRowFp[pTable->keyType])(pTable->iHandle, pNode, (void **)&pMeta);
  if (pMeta == NULL) return NULL;

  *ppRow = pMeta->row;
  sdbIncRef(handle, pMeta->row);

  return pNode;
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
  
  if (sdbInitIndexFp[pTable->keyType] != NULL) {
    pTable->iHandle = (*sdbInitIndexFp[pTable->keyType])(pTable->maxRowSize, sizeof(SRowMeta));
  }

  pthread_mutex_init(&pTable->mutex, NULL);

  tsSdbNumOfTables++;
  tsSdbTableList[pTable->tableId] = pTable;
  return pTable;
}

void sdbCloseTable(void *handle) {
  SSdbTable *pTable = (SSdbTable *)handle;
  if (pTable == NULL) return;
  
  tsSdbNumOfTables--;
  tsSdbTableList[pTable->tableId] = NULL;

  void *pNode = NULL;
  while (1) {
    SRowMeta *pMeta;
    pNode = (*sdbFetchRowFp[pTable->keyType])(pTable->iHandle, pNode, (void **)&pMeta);
    if (pMeta == NULL) break;

    SSdbOperDesc oper = {
      .pObj = pMeta->row,
      .table = pTable,
    };

    (*pTable->destroyFp)(&oper);
  }

  if (sdbCleanUpIndexFp[pTable->keyType]) {
    (*sdbCleanUpIndexFp[pTable->keyType])(pTable->iHandle);
  }

  pthread_mutex_destroy(&pTable->mutex);
  
  sdbTrace("table:%s, is closed, numOfTables:%d", pTable->tableName, tsSdbNumOfTables);
  free(pTable);
}
