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
#include "sdbInt.h"

static SHashObj *sdbGetHash(SSdb *pSdb, int32_t type) {
  if (type >= SDB_MAX || type <= SDB_START) {
    terrno = TSDB_CODE_SDB_INVALID_TABLE_TYPE;
    return NULL;
  }

  SHashObj *hash = pSdb->hashObjs[type];
  if (hash == NULL) {
    terrno = TSDB_CODE_SDB_APP_ERROR;
    return NULL;
  }

  return hash;
}

static int32_t sdbGetkeySize(SSdb *pSdb, ESdbType type, void *pKey) {
  int32_t  keySize;
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
  int32_t code = 0;

  SRWLatch *pLock = &pSdb->locks[pRow->type];
  taosWLockLatch(pLock);

  SSdbRow *pOldRow = taosHashGet(hash, pRow->pObj, keySize);
  if (pOldRow != NULL) {
    taosWUnLockLatch(pLock);
    sdbFreeRow(pRow);
    terrno = TSDB_CODE_SDB_OBJ_ALREADY_THERE;
    return terrno;
  }

  pRow->refCount = 1;
  pRow->status = pRaw->status;

  if (taosHashPut(hash, pRow->pObj, keySize, &pRow, sizeof(void *)) != 0) {
    taosWUnLockLatch(pLock);
    sdbFreeRow(pRow);
    terrno = TSDB_CODE_SDB_OBJ_ALREADY_THERE;
    return terrno;
  }

  taosWUnLockLatch(pLock);

  if (pSdb->keyTypes[pRow->type] == SDB_KEY_INT32) {
    pSdb->maxId[pRow->type] = MAX(pSdb->maxId[pRow->type], *((int32_t *)pRow->pObj));
  }

  SdbInsertFp insertFp = pSdb->insertFps[pRow->type];
  if (insertFp != NULL) {
    code = (*insertFp)(pSdb, pRow->pObj);
    if (code != 0) {
      taosWLockLatch(pLock);
      taosHashRemove(hash, pRow->pObj, keySize);
      taosWUnLockLatch(pLock);
      sdbFreeRow(pRow);
      terrno = code;
      return terrno;
    }
  }

  return 0;
}

static int32_t sdbUpdateRow(SSdb *pSdb, SHashObj *hash, SSdbRaw *pRaw, SSdbRow *pNewRow, int32_t keySize) {
  int32_t code = 0;

  SRWLatch *pLock = &pSdb->locks[pNewRow->type];
  taosRLockLatch(pLock);

  SSdbRow **ppOldRow = taosHashGet(hash, pNewRow->pObj, keySize);
  if (ppOldRow == NULL || *ppOldRow == NULL) {
    taosRUnLockLatch(pLock);
    return sdbInsertRow(pSdb, hash, pRaw, pNewRow, keySize);
  }
  SSdbRow *pOldRow = *ppOldRow;

  pOldRow->status = pRaw->status;
  taosRUnLockLatch(pLock);

  SdbUpdateFp updateFp = pSdb->updateFps[pNewRow->type];
  if (updateFp != NULL) {
    code = (*updateFp)(pSdb, pOldRow->pObj, pNewRow->pObj);
  }

  sdbFreeRow(pNewRow);
  return code;
}

static int32_t sdbDeleteRow(SSdb *pSdb, SHashObj *hash, SSdbRaw *pRaw, SSdbRow *pRow, int32_t keySize) {
  int32_t code = 0;

  SRWLatch *pLock = &pSdb->locks[pRow->type];
  taosWLockLatch(pLock);

  // remove attached object such as trans
  SdbDeleteFp deleteFp = pSdb->deleteFps[pRow->type];
  if (deleteFp != NULL) (*deleteFp)(pSdb, pRow->pObj);

  SSdbRow **ppOldRow = taosHashGet(hash, pRow->pObj, keySize);
  if (ppOldRow == NULL || *ppOldRow == NULL) {
    taosWUnLockLatch(pLock);
    sdbFreeRow(pRow);
    terrno = TSDB_CODE_SDB_OBJ_NOT_THERE;
    return terrno;
  }
  SSdbRow *pOldRow = *ppOldRow;

  pOldRow->status = pRaw->status;
  taosHashRemove(hash, pOldRow->pObj, keySize);
  taosWUnLockLatch(pLock);

  sdbRelease(pSdb, pOldRow->pObj);
  sdbFreeRow(pRow);
  return code;
}

int32_t sdbWriteNotFree(SSdb *pSdb, SSdbRaw *pRaw) {
  SHashObj *hash = sdbGetHash(pSdb, pRaw->type);
  if (hash == NULL) return terrno;

  SdbDecodeFp decodeFp = pSdb->decodeFps[pRaw->type];
  SSdbRow    *pRow = (*decodeFp)(pRaw);
  if (pRow == NULL) {
    return terrno;
  }

  pRow->type = pRaw->type;

  int32_t keySize = sdbGetkeySize(pSdb, pRow->type, pRow->pObj);
  int32_t code = TSDB_CODE_SDB_INVALID_ACTION_TYPE;

  switch (pRaw->status) {
    case SDB_STATUS_CREATING:
      code = sdbInsertRow(pSdb, hash, pRaw, pRow, keySize);
      break;
    case SDB_STATUS_UPDATING:
    case SDB_STATUS_READY:
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
  int32_t code = sdbWriteNotFree(pSdb, pRaw);
  sdbFreeRaw(pRaw);
  return code;
}

void *sdbAcquire(SSdb *pSdb, ESdbType type, void *pKey) {
  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return NULL;

  void   *pRet = NULL;
  int32_t keySize = sdbGetkeySize(pSdb, type, pKey);

  SRWLatch *pLock = &pSdb->locks[type];
  taosRLockLatch(pLock);

  SSdbRow **ppRow = taosHashGet(hash, pKey, keySize);
  if (ppRow == NULL || *ppRow == NULL) {
    taosRUnLockLatch(pLock);
    terrno = TSDB_CODE_SDB_OBJ_NOT_THERE;
    return NULL;
  }

  SSdbRow *pRow = *ppRow;
  switch (pRow->status) {
    case SDB_STATUS_READY:
    case SDB_STATUS_UPDATING:
      atomic_add_fetch_32(&pRow->refCount, 1);
      pRet = pRow->pObj;
      break;
    case SDB_STATUS_CREATING:
      terrno = TSDB_CODE_SDB_OBJ_CREATING;
      break;
    case SDB_STATUS_DROPPING:
      terrno = TSDB_CODE_SDB_OBJ_DROPPING;
      break;
    default:
      terrno = TSDB_CODE_SDB_APP_ERROR;
      break;
  }

  taosRUnLockLatch(pLock);
  return pRet;
}

void sdbRelease(SSdb *pSdb, void *pObj) {
  if (pObj == NULL) return;

  SSdbRow *pRow = (SSdbRow *)((char *)pObj - sizeof(SSdbRow));
  if (pRow->type >= SDB_MAX || pRow->type <= SDB_START) return;

  SRWLatch *pLock = &pSdb->locks[pRow->type];
  taosRLockLatch(pLock);

  int32_t ref = atomic_sub_fetch_32(&pRow->refCount, 1);
  if (ref <= 0 && pRow->status == SDB_STATUS_DROPPED) {
    SdbDeleteFp deleteFp = pSdb->deleteFps[pRow->type];
    if (deleteFp != NULL) {
      (*deleteFp)(pSdb, pRow->pObj);
    }

    sdbFreeRow(pRow);
  }

  taosRUnLockLatch(pLock);
}

void *sdbFetch(SSdb *pSdb, ESdbType type, void *pIter, void **ppObj) {
  *ppObj = NULL;

  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return NULL;

  SRWLatch *pLock = &pSdb->locks[type];
  taosRLockLatch(pLock);

  if (pIter != NULL) {
    SSdbRow *pLastRow = *(SSdbRow **)pIter;
    int32_t  ref = atomic_sub_fetch_32(&pLastRow->refCount, 1);
    if (ref <= 0 && pLastRow->status == SDB_STATUS_DROPPED) {
      sdbFreeRow(pLastRow);
    }
  }

  SSdbRow **ppRow = taosHashIterate(hash, pIter);
  while (ppRow != NULL) {
    SSdbRow *pRow = *ppRow;
    if (pRow == NULL || pRow->status != SDB_STATUS_READY) {
      ppRow = taosHashIterate(hash, ppRow);
      continue;
    }

    atomic_add_fetch_32(&pRow->refCount, 1);
    *ppObj = pRow->pObj;
    break;
  }
  taosRUnLockLatch(pLock);

  return ppRow;
}

void sdbCancelFetch(SSdb *pSdb, void *pIter) {
  if (pIter == NULL) return;
  SSdbRow  *pRow = *(SSdbRow **)pIter;
  SHashObj *hash = sdbGetHash(pSdb, pRow->type);
  if (hash == NULL) return;

  SRWLatch *pLock = &pSdb->locks[pRow->type];
  taosRLockLatch(pLock);
  taosHashCancelIterate(hash, pIter);
  taosRUnLockLatch(pLock);
}

int32_t sdbGetSize(SSdb *pSdb, ESdbType type) {
  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return 0;

  SRWLatch *pLock = &pSdb->locks[type];
  taosRLockLatch(pLock);
  int32_t size = taosHashGetSize(hash);
  taosRUnLockLatch(pLock);

  return size;
}

int32_t sdbGetMaxId(SSdb *pSdb, ESdbType type) {
  SHashObj *hash = sdbGetHash(pSdb, type);
  if (hash == NULL) return -1;

  if (pSdb->keyTypes[type] != SDB_KEY_INT32) return -1;

  int32_t maxId = 0;

  SRWLatch *pLock = &pSdb->locks[type];
  taosRLockLatch(pLock);

  SSdbRow **ppRow = taosHashIterate(hash, NULL);
  while (ppRow != NULL) {
    SSdbRow *pRow = *ppRow;
    int32_t  id = *(int32_t *)pRow->pObj;
    maxId = MAX(id, maxId);
    ppRow = taosHashIterate(hash, ppRow);
  }

  taosRUnLockLatch(pLock);

  maxId = MAX(maxId, pSdb->maxId[type]);
  return maxId + 1;
}
