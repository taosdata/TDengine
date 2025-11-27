/*
 * Copyright (c) 2024 TAOS Data, Inc.
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
#include <inttypes.h>
#include "mndInstance.h"
#include "mndShow.h"
#include "tcompare.h"
#include "ttime.h"

#define MND_INSTANCE_VER_NUMBER 1

static SSdbRaw *mndInstanceEncode(const SInstanceObj *pInstance);
static SSdbRow *mndInstanceDecode(SSdbRaw *pRaw);
static int32_t  mndInstanceInsert(SSdb *pSdb, SInstanceObj *pInstance);
static int32_t  mndInstanceUpdate(SSdb *pSdb, SInstanceObj *pOld, SInstanceObj *pNew);
static int32_t  mndInstanceDelete(SSdb *pSdb, SInstanceObj *pInstance);
static int32_t  mndRetrieveInstance(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t  mndProcessInstanceRegister(SRpcMsg *pReq);
static int32_t  mndProcessInstanceList(SRpcMsg *pReq);
static int32_t  mndProcessInstanceTimer(SRpcMsg *pReq);

static inline int32_t mndInstanceDataSize(void) {
  return TSDB_INSTANCE_ID_LEN + TSDB_INSTANCE_TYPE_LEN + TSDB_INSTANCE_DESC_LEN + sizeof(int64_t) * 2 + sizeof(int32_t);
}

int32_t mndInitInstance(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_INSTANCE,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = NULL,
      .decodeFp = (SdbDecodeFp)mndInstanceDecode,
      .insertFp = (SdbInsertFp)mndInstanceInsert,
      .updateFp = (SdbUpdateFp)mndInstanceUpdate,
      .deleteFp = (SdbDeleteFp)mndInstanceDelete,
  };

  TAOS_CHECK_RETURN(sdbSetTable(pMnode->pSdb, table));

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_INSTANCE, mndRetrieveInstance);
  mndSetMsgHandle(pMnode, TDMT_MND_REGISTER_INSTANCE, mndProcessInstanceRegister);
  mndSetMsgHandle(pMnode, TDMT_MND_LIST_INSTANCES, mndProcessInstanceList);
  mndSetMsgHandle(pMnode, TDMT_MND_INSTANCE_TIMER, mndProcessInstanceTimer);

  return TSDB_CODE_SUCCESS;
}

void mndCleanupInstance(SMnode *pMnode) {
  (void)pMnode;
}

SInstanceObj *mndInstanceAcquire(SMnode *pMnode, const char *id) {
  if (id == NULL || id[0] == 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  return sdbAcquire(pMnode->pSdb, SDB_INSTANCE, id);
}

void mndInstanceRelease(SMnode *pMnode, SInstanceObj *pInstance) {
  if (pInstance == NULL) return;
  sdbRelease(pMnode->pSdb, pInstance);
}

int32_t mndInstanceUpsert(SMnode *pMnode, const char *id, const char *type, const char *desc, int64_t regTime,
                          int32_t expire) {
  if (id == NULL || id[0] == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SInstanceObj instance = {0};
  tstrncpy(instance.id, id, sizeof(instance.id));
  if (type != NULL) {
    tstrncpy(instance.type, type, sizeof(instance.type));
  }
  if (desc != NULL) {
    tstrncpy(instance.desc, desc, sizeof(instance.desc));
  }
  instance.lastRegTime = regTime;
  instance.expire = expire;

  SInstanceObj *pOld = mndInstanceAcquire(pMnode, id);
  if (pOld != NULL) {
    instance.firstRegTime = pOld->firstRegTime;
    if (instance.type[0] == 0) {
      tstrncpy(instance.type, pOld->type, sizeof(instance.type));
    }
    if (instance.desc[0] == 0) {
      tstrncpy(instance.desc, pOld->desc, sizeof(instance.desc));
    }
    mndInstanceRelease(pMnode, pOld);
  } else {
    instance.firstRegTime = regTime;
  }

  SSdbRaw *pRaw = mndInstanceEncode(&instance);
  if (pRaw == NULL) {
    return terrno;
  }

  int32_t code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) {
    sdbFreeRaw(pRaw);
    return code;
  }

  code = sdbWrite(pMnode->pSdb, pRaw);
  if (code != TSDB_CODE_SUCCESS) {
    mError("instance:%s, failed to upsert into sdb since %s", id, tstrerror(code));
  }
  return code;
}

int32_t mndInstanceRemove(SMnode *pMnode, const char *id) {
  if (id == NULL || id[0] == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SInstanceObj instance = {0};
  tstrncpy(instance.id, id, sizeof(instance.id));

  SSdbRaw *pRaw = mndInstanceEncode(&instance);
  if (pRaw == NULL) {
    return terrno;
  }

  int32_t code = sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);
  if (code != TSDB_CODE_SUCCESS) {
    sdbFreeRaw(pRaw);
    return code;
  }

  code = sdbWrite(pMnode->pSdb, pRaw);
  if (code != TSDB_CODE_SUCCESS) {
    mError("instance:%s, failed to remove from sdb since %s", id, tstrerror(code));
  }
  return code;
}

static SSdbRaw *mndInstanceEncode(const SInstanceObj *pInstance) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_INSTANCE, MND_INSTANCE_VER_NUMBER, mndInstanceDataSize());
  if (pRaw == NULL) {
    return NULL;
  }

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pInstance->id, TSDB_INSTANCE_ID_LEN, _OVER);
  SDB_SET_BINARY(pRaw, dataPos, pInstance->type, TSDB_INSTANCE_TYPE_LEN, _OVER);
  SDB_SET_BINARY(pRaw, dataPos, pInstance->desc, TSDB_INSTANCE_DESC_LEN, _OVER);
  SDB_SET_INT64(pRaw, dataPos, pInstance->firstRegTime, _OVER);
  SDB_SET_INT64(pRaw, dataPos, pInstance->lastRegTime, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pInstance->expire, _OVER);
  SDB_SET_DATALEN(pRaw, dataPos, _OVER);

  return pRaw;

_OVER:
  sdbFreeRaw(pRaw);
  terrno = code != TSDB_CODE_SUCCESS ? code : terrno;
  return NULL;
}

static SSdbRow *mndInstanceDecode(SSdbRaw *pRaw) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    return NULL;
  }
  if (sver != MND_INSTANCE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SInstanceObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  SInstanceObj *pInstance = sdbGetRowObj(pRow);
  int32_t       dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pInstance->id, TSDB_INSTANCE_ID_LEN, _OVER);
  SDB_GET_BINARY(pRaw, dataPos, pInstance->type, TSDB_INSTANCE_TYPE_LEN, _OVER);
  SDB_GET_BINARY(pRaw, dataPos, pInstance->desc, TSDB_INSTANCE_DESC_LEN, _OVER);
  SDB_GET_INT64(pRaw, dataPos, &pInstance->firstRegTime, _OVER);
  SDB_GET_INT64(pRaw, dataPos, &pInstance->lastRegTime, _OVER);
  SDB_GET_INT32(pRaw, dataPos, &pInstance->expire, _OVER);

  return pRow;

_OVER:
  taosMemoryFreeClear(pRow);
  if (code != TSDB_CODE_SUCCESS) terrno = code;
  return NULL;
}

static int32_t mndInstanceInsert(SSdb *pSdb, SInstanceObj *pInstance) {
  (void)pSdb;
  (void)pInstance;
  return TSDB_CODE_SUCCESS;
}

static int32_t mndInstanceUpdate(SSdb *pSdb, SInstanceObj *pOld, SInstanceObj *pNew) {
  (void)pSdb;
  if (pOld == NULL || pNew == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  memcpy(pOld, pNew, sizeof(SInstanceObj));
  return TSDB_CODE_SUCCESS;
}

static int32_t mndInstanceDelete(SSdb *pSdb, SInstanceObj *pInstance) {
  (void)pSdb;
  (void)pInstance;
  return TSDB_CODE_SUCCESS;
}

static int32_t mndProcessInstanceRegister(SRpcMsg *pReq) {
  SMnode              *pMnode = pReq->info.node;
  SInstanceRegisterReq req = {0};
  int32_t              code = tDeserializeSInstanceRegisterReq(pReq->pCont, pReq->contLen, &req);
  if (code != TSDB_CODE_SUCCESS) {
    mError("instance register, failed to decode since %s", tstrerror(code));
    return code;
  }

  if (req.id[0] == 0) {
    mError("instance register, id is empty");
    return TSDB_CODE_INVALID_PARA;
  }

  if (req.expire < 0) {
    code = mndInstanceRemove(pMnode, req.id);
    if (code == TSDB_CODE_SUCCESS) {
      mInfo("instance:%s unregistered", req.id);
    }
  } else {
    int64_t now = taosGetTimestampMs();
    const char *type = req.type[0] ? req.type : NULL;
    const char *desc = req.desc[0] ? req.desc : NULL;
    code = mndInstanceUpsert(pMnode, req.id, type, desc, now, req.expire);
    if (code == TSDB_CODE_SUCCESS) {
      mDebug("instance:%s registered type:%s expire:%d", req.id, req.type, req.expire);
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    mError("instance:%s, failed to process register request since %s", req.id, tstrerror(code));
  }
  return code;
}

static int32_t mndProcessInstanceList(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  SSdb            *pSdb = pMnode->pSdb;
  SInstanceListReq req = {0};
  int32_t          code = tDeserializeSInstanceListReq(pReq->pCont, pReq->contLen, &req);
  if (code != TSDB_CODE_SUCCESS) {
    mError("instance list, failed to decode since %s", tstrerror(code));
    return code;
  }

  // Collect instance IDs (keep instances referenced until serialization)
  int32_t        capacity = 32;
  int32_t        count = 0;
  SInstanceObj **instances = taosMemoryCalloc(capacity, sizeof(SInstanceObj *));
  const char   **ids = taosMemoryCalloc(capacity, sizeof(char *));
  void          *iter = NULL;
  SInstanceObj  *pInstance = NULL;
  int64_t        now = taosGetTimestampMs();
  int32_t        filterTypeLen = (req.filter_type[0] != 0) ? (int32_t)strlen(req.filter_type) : 0;

  if (instances == NULL || ids == NULL) {
    taosMemoryFree(instances);
    taosMemoryFree(ids);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  while (1) {
    iter = sdbFetch(pSdb, SDB_INSTANCE, iter, (void **)&pInstance);
    if (iter == NULL) {
      break;
    }

    // Filter by type if specified
    if (filterTypeLen > 0) {
      int32_t typeLen = (int32_t)strlen(pInstance->type);
      if (typeLen != filterTypeLen || strncasecmp(pInstance->type, req.filter_type, (size_t)filterTypeLen) != 0) {
        sdbRelease(pSdb, pInstance);
        continue;
      }
    }

    // Filter by expiration
    if (pInstance->expire > 0 && pInstance->lastRegTime > 0) {
      int64_t delta = now - pInstance->lastRegTime;
      if (delta > (int64_t)pInstance->expire * 1000) {
        sdbRelease(pSdb, pInstance);
        continue;
      }
    }

    // Add to list (keep reference)
    if (count >= capacity) {
      int32_t newCap = capacity * 2;
      SInstanceObj **newInstances = taosMemoryRealloc(instances, newCap * sizeof(SInstanceObj *));
      if (newInstances == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _cleanup;
      }
      const char   **newIds = taosMemoryRealloc(ids, newCap * sizeof(char *));
      if (newIds == NULL) {
        // If memory was moved, free newInstances; otherwise, keep instances unchanged
        if (newInstances != instances) {
          taosMemoryFree(newInstances);
        }
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _cleanup;
      }
      instances = newInstances;
      ids = newIds;
      capacity = newCap;
    }

    instances[count] = pInstance;
    ids[count] = pInstance->id;
    count++;
  }

  // Serialize response
  SInstanceListRsp rsp = {0};
  rsp.count = count;
  rsp.ids = (char **)ids;
  int32_t contLen = tSerializeSInstanceListRsp(NULL, 0, &rsp);
  if (contLen <= 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_TSC_INTERNAL_ERROR;
    goto _cleanup;
  }

  pReq->info.rsp = rpcMallocCont(contLen);
  if (pReq->info.rsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _cleanup;
  }

  if (tSerializeSInstanceListRsp(pReq->info.rsp, contLen, &rsp) < 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_TSC_INTERNAL_ERROR;
    rpcFreeCont(pReq->info.rsp);
    pReq->info.rsp = NULL;
    goto _cleanup;
  }

  pReq->info.rspLen = contLen;
  code = TSDB_CODE_SUCCESS;

_cleanup:
  // Release all instances
  for (int32_t i = 0; i < count; i++) {
    if (instances[i] != NULL) {
      sdbRelease(pSdb, instances[i]);
    }
  }
  taosMemoryFree(instances);
  taosMemoryFree(ids);
  if (pInstance != NULL) {
    sdbRelease(pSdb, pInstance);
  }

  if (code != TSDB_CODE_SUCCESS) {
    mError("instance list, failed to process request since %s", tstrerror(code));
  } else {
    mDebug("instance list, returned %d instances", count);
  }

  return code;
}

static int32_t mndProcessInstanceTimer(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SSdb         *pSdb = pMnode->pSdb;
  void         *iter = NULL;
  SInstanceObj *pInstance = NULL;
  int64_t       now = taosGetTimestampMs();

  while (1) {
    iter = sdbFetch(pSdb, SDB_INSTANCE, iter, (void **)&pInstance);
    if (iter == NULL) {
      break;
    }

    bool expired =
        (pInstance->expire > 0) && (now - pInstance->lastRegTime > ((int64_t)pInstance->expire * 1000));
    if (expired) {
      mInfo("instance:%s expired(last:%" PRId64 "ms, expire:%ds), removing", pInstance->id, pInstance->lastRegTime,
            pInstance->expire);
      (void)mndInstanceRemove(pMnode, pInstance->id);
    }

    sdbRelease(pSdb, pInstance);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mndRetrieveInstance(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t        numOfRows = 0;
  SInstanceObj  *pInstance = NULL;
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_INSTANCE, pShow->pIter, (void **)&pInstance);
    if (pShow->pIter == NULL) break;

    if (pShow->filterTb[0] != 0) {
      if (rawStrPatternMatch(pInstance->id, pShow->filterTb) != TSDB_PATTERN_MATCH) {
        sdbRelease(pSdb, pInstance);
        pInstance = NULL;
        continue;
      }
    }

    int32_t cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);

    char idBuf[TSDB_INSTANCE_ID_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(idBuf, pInstance->id, sizeof(idBuf));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, idBuf, false), pInstance, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    char typeBuf[TSDB_INSTANCE_TYPE_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(typeBuf, pInstance->type, sizeof(typeBuf));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, typeBuf, false), pInstance, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    char descBuf[TSDB_INSTANCE_DESC_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(descBuf, pInstance->desc, sizeof(descBuf));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, descBuf, false), pInstance, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pInstance->firstRegTime, false), pInstance,
                        &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pInstance->lastRegTime, false), pInstance,
                        &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pInstance->expire, false), pInstance, &lino,
                        _OVER);

    numOfRows++;
    sdbRelease(pSdb, pInstance);
    pInstance = NULL;
  }

_OVER:
  pShow->numOfRows += numOfRows;
  pBlock->info.rows += numOfRows;

  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to retrieve instance rows at line:%d since %s", lino, tstrerror(code));
  }

  return numOfRows;
}

