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
#include "sdb.h"

static void sdbCheckRow(SSdb *pSdb, SSdbRow *pRow);

const char *sdbTableName(ESdbType type) {
  switch (type) {
    case SDB_TRANS:
      return "trans";
    case SDB_CLUSTER:
      return "cluster";
    case SDB_MNODE:
      return "mnode";
    case SDB_QNODE:
      return "qnode";
    case SDB_SNODE:
      return "snode";
    case SDB_DNODE:
      return "dnode";
    case SDB_USER:
      return "user";
    case SDB_AUTH:
      return "auth";
    case SDB_ACCT:
      return "acct";
    case SDB_STREAM_CK:
      return "stream_ck";
    case SDB_STREAM:
      return "stream";
    case SDB_OFFSET:
      return "offset";
    case SDB_SUBSCRIBE:
      return "subscribe";
    case SDB_CONSUMER:
      return "consumer";
    case SDB_TOPIC:
      return "topic";
    case SDB_VGROUP:
      return "vgroup";
    case SDB_SMA:
      return "sma";
    case SDB_STB:
      return "stb";
    case SDB_DB:
      return "db";
    case SDB_FUNC:
      return "func";
    case SDB_IDX:
      return "idx";
    case SDB_VIEW:
      return "view";
    case SDB_STREAM_SEQ:
      return "stream_seq";
    case SDB_COMPACT:
      return "compact";
    case SDB_COMPACT_DETAIL:
      return "compact_detail";
     default:
      return "undefine";
  }
}

const char *sdbStatusName(ESdbStatus status) {
  switch (status) {
    case SDB_STATUS_CREATING:
      return "creating";
    case SDB_STATUS_DROPPING:
      return "dropping";
    case SDB_STATUS_READY:
      return "ready";
    case SDB_STATUS_DROPPED:
      return "dropped";
    case SDB_STATUS_INIT:
      return "init";
    case SDB_STATUS_UPDATE:
      return "update";
    default:
      return "undefine";
  }
}

void sdbPrintOper(SSdb *pSdb, SSdbRow *pRow, const char *oper) {
#if 1
  EKeyType keyType = pSdb->keyTypes[pRow->type];

  if (keyType == SDB_KEY_BINARY) {
    mTrace("%s:%s, ref:%d oper:%s row:%p status:%s", sdbTableName(pRow->type), (char *)pRow->pObj, pRow->refCount, oper,
           pRow->pObj, sdbStatusName(pRow->status));
  } else if (keyType == SDB_KEY_INT32) {
    mTrace("%s:%d, ref:%d oper:%s row:%p status:%s", sdbTableName(pRow->type), *(int32_t *)pRow->pObj, pRow->refCount,
           oper, pRow->pObj, sdbStatusName(pRow->status));
  } else if (keyType == SDB_KEY_INT64) {
    mTrace("%s:%" PRId64 ", ref:%d oper:%s row:%p status:%s", sdbTableName(pRow->type), *(int64_t *)pRow->pObj,
           pRow->refCount, oper, pRow->pObj, sdbStatusName(pRow->status));
  } else {
  }
#endif
}

static SHashObj *sdbGetHash(SSdb *pSdb, int32_t type) {
  if (type >= SDB_MAX || type < 0) {
    terrno = TSDB_CODE_SDB_INVALID_TABLE_TYPE;
    return NULL;
  }

  SHashObj *hash = pSdb->hashObjs[type];
  if (hash == NULL) {
    terrno = TSDB_CODE_APP_ERROR;
    return NULL;
  }

  return hash;
}

static int32_t sdbGetkeySize(SSdb *pSdb, ESdbType type, const void *pKey) {
  int32_t  keySize = 0;
  EKeyType keyType = pSdb->keyTypes[type];

  if (keyType == SDB_KEY_INT32) {
    keySize = sizeof(int32_t);
  } else if (keyType == SDB_KEY_BINARY) {
    keySize = strlen(pKey) + 1;
  } else {
    keySize = sizeof(int64_t);
  }

  return keySize;
}

static int32_t sdbInsertRow(SSdb *pSdb, SHashObj *hash, SSdbRaw *pRaw, SSdbRow *pRow, int32_t keySize) {
  int32_t type = pRow->type;
  sdbWriteLock(pSdb, type);

  SSdbRow *pOldRow = taosHashGet(hash, pRow->pObj, keySize);
  if (pOldRow != NULL) {
    sdbUnLock(pSdb, type);
    sdbFreeRow(pSdb, pRow, false);
    terrno = TSDB_CODE_SDB_OBJ_ALREADY_THERE;
    return terrno;
  }

  pRow->refCount = 0;
  pRow->status = pRaw->status;
  sdbPrintOper(pSdb, pRow, "insert");

  if (taosHashPut(hash, pRow->pObj, keySize, &pRow, sizeof(void *)) != 0) {
    sdbUnLock(pSdb, type);
    sdbFreeRow(pSdb, pRow, false);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  int32_t     code = 0;
  SdbInsertFp insertFp = pSdb->insertFps[pRow->type];
  if (insertFp != NULL) {
    code = (*insertFp)(pSdb, pRow->pObj);
    if (code != 0) {
      if (terrno == 0) terrno = TSDB_CODE_MND_TRANS_UNKNOW_ERROR;
      code = terrno;
      taosHashRemove(hash, pRow->pObj, keySize);
      sdbFreeRow(pSdb, pRow, false);
      terrno = code;
      sdbUnLock(pSdb, type);
      return terrno;
    }
  }

  sdbUnLock(pSdb, type);

  if (pSdb->keyTypes[pRow->type] == SDB_KEY_INT32) {
    pSdb->maxId[pRow->type] = TMAX(pSdb->maxId[pRow->type], *((int32_t *)pRow->pObj));
  }
  if (pSdb->keyTypes[pRow->type] == SDB_KEY_INT64) {
    pSdb->maxId[pRow->type] = TMAX(pSdb->maxId[pRow->type], *((int64_t *)pRow->pObj));
  }
  pSdb->tableVer[pRow->type]++;

  return 0;
}

static int32_t sdbUpdateRow(SSdb *pSdb, SHashObj *hash, SSdbRaw *pRaw, SSdbRow *pNewRow, int32_t keySize) {
  int32_t type = pNewRow->type;
  sdbWriteLock(pSdb, type);

  SSdbRow **ppOldRow = taosHashGet(hash, pNewRow->pObj, keySize);
  if (ppOldRow == NULL || *ppOldRow == NULL) {
    sdbUnLock(pSdb, type);
    return sdbInsertRow(pSdb, hash, pRaw, pNewRow, keySize);
  }

  SSdbRow *pOldRow = *ppOldRow;
  pOldRow->status = pRaw->status;
  sdbPrintOper(pSdb, pOldRow, "update");

  int32_t     code = 0;
  SdbUpdateFp updateFp = pSdb->updateFps[type];
  if (updateFp != NULL) {
    code = (*updateFp)(pSdb, pOldRow->pObj, pNewRow->pObj);
  }
  sdbUnLock(pSdb, type);

  // sdbUnLock(pSdb, type);
  sdbFreeRow(pSdb, pNewRow, false);

  pSdb->tableVer[pOldRow->type]++;
  return code;
}

static int32_t sdbDeleteRow(SSdb *pSdb, SHashObj *hash, SSdbRaw *pRaw, SSdbRow *pRow, int32_t keySize) {
  int32_t type = pRow->type;
  sdbWriteLock(pSdb, type);

  SSdbRow **ppOldRow = taosHashGet(hash, pRow->pObj, keySize);
  if (ppOldRow == NULL || *ppOldRow == NULL) {
    sdbUnLock(pSdb, type);
    sdbFreeRow(pSdb, pRow, false);
    terrno = TSDB_CODE_SDB_OBJ_NOT_THERE;
    return terrno;
  }
  SSdbRow *pOldRow = *ppOldRow;
  pOldRow->status = pRaw->status;

  atomic_add_fetch_32(&pOldRow->refCount, 1);
  sdbPrintOper(pSdb, pOldRow, "delete");

  taosHashRemove(hash, pOldRow->pObj, keySize);
  pSdb->tableVer[pOldRow->type]++;
  sdbUnLock(pSdb, type);

  sdbFreeRow(pSdb, pRow, false);

  sdbCheckRow(pSdb, pOldRow);
  return 0;
}

int32_t sdbWriteWithoutFree(SSdb *pSdb, SSdbRaw *pRaw) {
  SHashObj *hash = sdbGetHash(pSdb, pRaw->type);
  if (hash == NULL) return terrno;

  SdbDecodeFp decodeFp = pSdb->decodeFps[pRaw->type];
  SSdbRow    *pRow = (*decodeFp)(pRaw);
  if (pRow == NULL) return terrno;

  pRow->type = pRaw->type;

  int32_t keySize = sdbGetkeySize(pSdb, pRow->type, pRow->pObj);
  int32_t code = TSDB_CODE_SDB_INVALID_ACTION_TYPE;

  switch (pRaw->status) {
    case SDB_STATUS_CREATING:
      code = sdbInsertRow(pSdb, hash, pRaw, pRow, keySize);
      break;
    case SDB_STATUS_READY:
    case SDB_STATUS_UPDATE:
    case SDB_STATUS_DROPPING:
      code = sdbUpdateRow(pSdb, hash, pRaw, pRow, keySize);
      break;
    case SDB_STATUS_DROPPED:
      code = sdbDeleteRow(pSdb, hash, pRaw, pRow, keySize);
      break;
  }

  return code;
}

int32_t sdbWrite(SSdb *pSdb, SSdbRaw *pRaw) {
  int32_t code = sdbWriteWithoutFree(pSdb, pRaw);
  sdbFreeRaw(pRaw);
  return code;
}

void *sdbAcquireAll(SSdb *pSdb, ESdbType type, const void *pKey, bool onlyReady) {
  terrno = 0;

  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return NULL;

  void   *pRet = NULL;
  int32_t keySize = sdbGetkeySize(pSdb, type, pKey);

  sdbReadLock(pSdb, type);

  SSdbRow **ppRow = taosHashGet(hash, pKey, keySize);
  if (ppRow == NULL || *ppRow == NULL) {
    sdbUnLock(pSdb, type);
    terrno = TSDB_CODE_SDB_OBJ_NOT_THERE;
    return NULL;
  }

  SSdbRow *pRow = *ppRow;
  switch (pRow->status) {
    case SDB_STATUS_READY:
      atomic_add_fetch_32(&pRow->refCount, 1);
      pRet = pRow->pObj;
      sdbPrintOper(pSdb, pRow, "acquire");
      break;
    case SDB_STATUS_CREATING:
      terrno = TSDB_CODE_SDB_OBJ_CREATING;
      break;
    case SDB_STATUS_DROPPING:
      terrno = TSDB_CODE_SDB_OBJ_DROPPING;
      break;
    default:
      terrno = TSDB_CODE_APP_ERROR;
      break;
  }

  if (pRet == NULL) {
    if (!onlyReady) {
      terrno = 0;
      atomic_add_fetch_32(&pRow->refCount, 1);
      pRet = pRow->pObj;
      sdbPrintOper(pSdb, pRow, "acquire");
    }
  }

  sdbUnLock(pSdb, type);
  return pRet;
}

void *sdbAcquire(SSdb *pSdb, ESdbType type, const void *pKey) { return sdbAcquireAll(pSdb, type, pKey, true); }
void *sdbAcquireNotReadyObj(SSdb *pSdb, ESdbType type, const void *pKey) {
  return sdbAcquireAll(pSdb, type, pKey, false);
}

static void sdbCheckRow(SSdb *pSdb, SSdbRow *pRow) {
  int32_t type = pRow->type;
  sdbWriteLock(pSdb, type);

  int32_t ref = atomic_sub_fetch_32(&pRow->refCount, 1);
  sdbPrintOper(pSdb, pRow, "check");
  if (ref <= 0 && pRow->status == SDB_STATUS_DROPPED) {
    sdbFreeRow(pSdb, pRow, true);
  }

  sdbUnLock(pSdb, type);
}

void sdbReleaseLock(SSdb *pSdb, void *pObj, bool lock) {
  if (pObj == NULL) return;

  SSdbRow *pRow = (SSdbRow *)((char *)pObj - sizeof(SSdbRow));
  if (pRow->type >= SDB_MAX) return;

  int32_t type = pRow->type;
  if (lock) {
    sdbWriteLock(pSdb, type);
  }

  int32_t ref = atomic_sub_fetch_32(&pRow->refCount, 1);
  sdbPrintOper(pSdb, pRow, "release");
  if (ref <= 0 && pRow->status == SDB_STATUS_DROPPED) {
    sdbFreeRow(pSdb, pRow, true);
  }

  if (lock) {
    sdbUnLock(pSdb, type);
  }
}

void sdbRelease(SSdb *pSdb, void *pObj) { sdbReleaseLock(pSdb, pObj, true); }

void *sdbFetch(SSdb *pSdb, ESdbType type, void *pIter, void **ppObj) {
  *ppObj = NULL;

  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return NULL;

  sdbReadLock(pSdb, type);

  SSdbRow **ppRow = taosHashIterate(hash, pIter);
  while (ppRow != NULL) {
    SSdbRow *pRow = *ppRow;
    if (pRow == NULL || pRow->status != SDB_STATUS_READY) {
      ppRow = taosHashIterate(hash, ppRow);
      continue;
    }

    atomic_add_fetch_32(&pRow->refCount, 1);
    sdbPrintOper(pSdb, pRow, "fetch");
    *ppObj = pRow->pObj;
    break;
  }
  sdbUnLock(pSdb, type);

  return ppRow;
}

void *sdbFetchAll(SSdb *pSdb, ESdbType type, void *pIter, void **ppObj, ESdbStatus *status, bool lock) {
  *ppObj = NULL;

  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return NULL;

  if (lock) {
    sdbReadLock(pSdb, type);
  }

  SSdbRow **ppRow = taosHashIterate(hash, pIter);
  while (ppRow != NULL) {
    SSdbRow *pRow = *ppRow;
    if (pRow == NULL) {
      ppRow = taosHashIterate(hash, ppRow);
      continue;
    }

    atomic_add_fetch_32(&pRow->refCount, 1);
    sdbPrintOper(pSdb, pRow, "fetch");
    *ppObj = pRow->pObj;
    *status = pRow->status;
    break;
  }
  if (lock) {
    sdbUnLock(pSdb, type);
  }

  return ppRow;
}

void sdbCancelFetch(SSdb *pSdb, void *pIter) {
  if (pIter == NULL) return;
  SSdbRow  *pRow = *(SSdbRow **)pIter;
  SHashObj *hash = sdbGetHash(pSdb, pRow->type);
  if (hash == NULL) return;

  int32_t type = pRow->type;
  sdbReadLock(pSdb, type);
  taosHashCancelIterate(hash, pIter);
  sdbUnLock(pSdb, type);
}

void sdbTraverse(SSdb *pSdb, ESdbType type, sdbTraverseFp fp, void *p1, void *p2, void *p3) {
  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return;

  sdbReadLock(pSdb, type);

  SSdbRow **ppRow = taosHashIterate(hash, NULL);
  while (ppRow != NULL) {
    SSdbRow *pRow = *ppRow;
    if (pRow->status == SDB_STATUS_READY) {
      bool isContinue = (*fp)(pSdb->pMnode, pRow->pObj, p1, p2, p3);
      if (!isContinue) {
        taosHashCancelIterate(hash, ppRow);
        break;
      }
    }

    ppRow = taosHashIterate(hash, ppRow);
  }

  sdbUnLock(pSdb, type);
}

int32_t sdbGetSize(SSdb *pSdb, ESdbType type) {
  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return 0;

  sdbReadLock(pSdb, type);
  int32_t size = taosHashGetSize(hash);
  sdbUnLock(pSdb, type);

  return size;
}

int32_t sdbGetMaxId(SSdb *pSdb, ESdbType type) {
  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return -1;

  if (pSdb->keyTypes[type] != SDB_KEY_INT32) return -1;

  int32_t maxId = 0;
  sdbReadLock(pSdb, type);

  SSdbRow **ppRow = taosHashIterate(hash, NULL);
  while (ppRow != NULL) {
    SSdbRow *pRow = *ppRow;
    int32_t  id = *(int32_t *)pRow->pObj;
    maxId = TMAX(id, maxId);
    ppRow = taosHashIterate(hash, ppRow);
  }

  sdbUnLock(pSdb, type);
  maxId = TMAX(maxId, pSdb->maxId[type]);
  return maxId + 1;
}

int64_t sdbGetTableVer(SSdb *pSdb, ESdbType type) {
  if (type >= SDB_MAX || type < 0) {
    terrno = TSDB_CODE_SDB_INVALID_TABLE_TYPE;
    return -1;
  }

  return pSdb->tableVer[type];
}
