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
#include "tglobal.h"

static SHashObj *sdbGetHash(int32_t sdb) {
  if (sdb >= SDB_MAX || sdb <= SDB_START) {
    terrno = TSDB_CODE_SDB_INVALID_TABLE_TYPE;
    return NULL;
  }

  SHashObj *hash = tsSdb.hashObjs[sdb];
  if (hash == NULL) {
    terrno = TSDB_CODE_SDB_APP_ERROR;
    return NULL;
  }

  return hash;
}

static int32_t sdbInsertRow(SHashObj *hash, SSdbRaw *pRaw, SSdbRow *pRow, int32_t keySize) {
  SSdbRow *pDstRow = NULL;
  if (pDstRow != NULL) {
    terrno = TSDB_CODE_SDB_OBJ_ALREADY_THERE;
    return -1;
  }

  pRow->refCount = 0;
  pRow->status = pRaw->status;

  if (taosHashPut(hash, pRow->pObj, keySize, &pRow, sizeof(void *)) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SdbInsertFp insertFp = tsSdb.insertFps[pRaw->sdb];
  if (insertFp != NULL) {
    if ((*insertFp)(pRow->pObj) != 0) {
      taosHashRemove(hash, pRow->pObj, keySize);
      return -1;
    }
  }

  return 0;
}

static int32_t sdbUpdateRow(SHashObj *hash, SSdbRaw *pRaw, SSdbRow *pRow, int32_t keySize) {
  SSdbRow *pDstRow = NULL;
  if (pDstRow == NULL) {
    terrno = TSDB_CODE_SDB_OBJ_NOT_THERE;
    return -1;
  }

  SdbUpdateFp updateFp = tsSdb.updateFps[pRaw->sdb];
  if (updateFp != NULL) {
    if ((*updateFp)(pRow->pObj, pDstRow->pObj) != 0) {
      return -1;
    }
  }

  return 0;
}

static int32_t sdbDeleteRow(SHashObj *hash, SSdbRaw *pRaw, SSdbRow *pRow, int32_t keySize) {
  SSdbRow *pDstRow = NULL;
  if (pDstRow == NULL) {
    terrno = TSDB_CODE_SDB_OBJ_NOT_THERE;
    return -1;
  }

  SdbDeleteFp deleteFp = tsSdb.deleteFps[pRaw->sdb];
  if (deleteFp != NULL) {
    if ((*deleteFp)(pRow->pObj) != 0) {
      return -1;
    }
  }

  taosHashRemove(hash, pRow->pObj, keySize);
  return 0;
}

int32_t sdbWrite(SSdbRaw *pRaw) {
  SHashObj *hash = sdbGetHash(pRaw->sdb);
  if (hash == NULL) return -1;

  SdbDecodeFp decodeFp = tsSdb.decodeFps[pRaw->sdb];
  SSdbRow    *pRow = (*decodeFp)(pRaw);
  if (pRow == NULL) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_CONTENT;
    return -1;
  }

  int32_t  keySize;
  EKeyType keyType = tsSdb.keyTypes[pRaw->sdb];
  if (keyType == SDB_KEY_INT32) {
    keySize = sizeof(int32_t);
  } else if (keyType == SDB_KEY_BINARY) {
    keySize = strlen(pRow->pObj) + 1;
  } else {
    keySize = sizeof(int64_t);
  }

  int32_t code = -1;
  if (pRaw->action == SDB_ACTION_INSERT) {
    code = sdbInsertRow(hash, pRaw, pRow, keySize);
  } else if (pRaw->action == SDB_ACTION_UPDATE) {
    code = sdbUpdateRow(hash, pRaw, pRow, keySize);
  } else if (pRaw->action == SDB_ACTION_DELETE) {
    code = sdbDeleteRow(hash, pRaw, pRow, keySize);
  } else {
    terrno = TSDB_CODE_SDB_INVALID_ACTION_TYPE;
  }

  if (code != 0) {
    sdbFreeRow(pRow);
  }
  return 0;
}

void *sdbAcquire(ESdbType sdb, void *pKey) {
  terrno = 0;

  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return NULL;
  }

  int32_t  keySize;
  EKeyType keyType = tsSdb.keyTypes[sdb];

  switch (keyType) {
    case SDB_KEY_INT32:
      keySize = sizeof(int32_t);
      break;
    case SDB_KEY_INT64:
      keySize = sizeof(int64_t);
      break;
    case SDB_KEY_BINARY:
      keySize = strlen(pKey) + 1;
      break;
    default:
      keySize = sizeof(int32_t);
  }

  SSdbRow *pRow = taosHashGet(hash, pKey, keySize);
  if (pRow == NULL) return NULL;

  if (pRow->status == SDB_STATUS_READY) {
    atomic_add_fetch_32(&pRow->refCount, 1);
    return pRow->pObj;
  } else {
    terrno = -1;  // todo
    return NULL;
  }
}

void sdbRelease(void *pObj) {
  SSdbRow *pRow = (SSdbRow *)((char *)pObj - sizeof(SSdbRow));
  atomic_sub_fetch_32(&pRow->refCount, 1);
}

void *sdbFetchRow(ESdbType sdb, void *pIter) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return NULL;
  }

  return taosHashIterate(hash, pIter);
}

void sdbCancelFetch(ESdbType sdb, void *pIter) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return;
  }
  taosHashCancelIterate(hash, pIter);
}

int32_t sdbGetSize(ESdbType sdb) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return 0;
  }
  return taosHashGetSize(hash);
}
