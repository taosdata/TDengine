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

#include "mndView.h"
#include "mndTrans.h"
#include "mndDb.h"
#include "audit.h"

#define MND_VIEW_VER_NUMBER 1

void tFreeViewObj(SViewObj *pView) {
  taosMemoryFree(pView->querySql);
  taosMemoryFree(pView->pSchema);
}

int32_t tSerializeSViewObj(void *buf, int32_t bufLen, const SViewObj *pObj) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->fullname) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->name) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->dbFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->querySql) < 0) return -1;
  if (NULL != pObj->parameters) {
    if (tEncodeI8(&encoder, 1) < 0) return -1;
    if (tEncodeCStr(&encoder, pObj->parameters) < 0) return -1;
  } else {
    if (tEncodeI8(&encoder, 0) < 0) return -1;
  }
  if (NULL != pObj->defaultValues) {
    if (tEncodeI8(&encoder, 1) < 0) return -1;
    //TODO
  } else {
    if (tEncodeI8(&encoder, 0) < 0) return -1;
  }
  if (NULL != pObj->targetTable) {
    if (tEncodeI8(&encoder, 1) < 0) return -1;
    if (tEncodeCStr(&encoder, pObj->targetTable) < 0) return -1;
  } else {
    if (tEncodeI8(&encoder, 0) < 0) return -1;
  }
  if (tEncodeU64(&encoder, pObj->viewId) < 0) return -1;
  if (tEncodeU64(&encoder, pObj->dbId) < 0) return -1;
  if (tEncodeI64(&encoder, pObj->createdTime) < 0) return -1;
  if (tEncodeI32(&encoder, pObj->version) < 0) return -1;
  if (tEncodeI8(&encoder, pObj->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pObj->type) < 0) return -1;
  if (tEncodeI32(&encoder, pObj->numOfCols) < 0) return -1;
  for (int32_t i = 0; i < pObj->numOfCols; ++i) {
    SSchema *pSchema = &pObj->pSchema[i];
    if (tEncodeSSchema(&encoder, pSchema) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSViewObj(void *buf, int32_t bufLen, SViewObj *pObj) {
  int8_t ex = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->fullname) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->name) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->dbFName) < 0) return -1;
  if (tDecodeCStrAlloc(&decoder, &pObj->querySql) < 0) return -1;
  if (tDecodeI8(&decoder, &ex) < 0) return -1;
  if (0 != ex) {
    if (tDecodeCStrAlloc(&decoder, &pObj->parameters) < 0) return -1;
  } else {
    pObj->parameters = NULL;
  }
  if (tDecodeI8(&decoder, &ex) < 0) return -1;
  if (0 != ex) {
    //TODO
  } else {
    pObj->defaultValues = NULL;
  }
  if (tDecodeI8(&decoder, &ex) < 0) return -1;
  if (0 != ex) {
    if (tDecodeCStrAlloc(&decoder, &pObj->targetTable) < 0) return -1;
  } else {
    pObj->targetTable = NULL;
  }
  if (tDecodeU64(&decoder, &pObj->viewId) < 0) return -1;
  if (tDecodeU64(&decoder, &pObj->dbId) < 0) return -1;
  if (tDecodeI64(&decoder, &pObj->createdTime) < 0) return -1;
  if (tDecodeI32(&decoder, &pObj->version) < 0) return -1;
  if (tDecodeI8(&decoder, &pObj->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pObj->type) < 0) return -1;
  if (tDecodeI32(&decoder, &pObj->numOfCols) < 0) return -1;

  if (pObj->numOfCols > 0) {
    pObj->pSchema = taosMemoryCalloc(pObj->numOfCols, sizeof(SSchema));
    if (pObj->pSchema == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    for (int32_t i = 0; i < pObj->numOfCols; ++i) {
      SSchema* pSchema = pObj->pSchema + i;
      if (tDecodeSSchema(&decoder, pSchema) < 0) return -1;
    }
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}



SSdbRaw *mndViewActionEncode(SViewObj *pView) {
  terrno = TSDB_CODE_SUCCESS;
  void *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t tlen = tSerializeSViewObj(NULL, 0, pView);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }
  
  int32_t  size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_VIEW, MND_VIEW_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }

  tlen = tSerializeSViewObj(buf, tlen, pView);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, VIEW_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, VIEW_ENCODE_OVER);
  SDB_SET_DATALEN(pRaw, dataPos, VIEW_ENCODE_OVER);


VIEW_ENCODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("view:%s, failed to encode to raw:%p since %s", pView->fullname, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("view:%s, encode to raw:%p, row:%p", pView->fullname, pRaw, pView);
  return pRaw;
}

SSdbRow *mndViewActionDecode(SSdbRaw *pRaw) {
  SSdbRow    *pRow = NULL;
  SViewObj   *pView = NULL;
  void       *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto VIEW_DECODE_OVER;
  }

  if (sver != MND_VIEW_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("view read invalid ver, data ver: %d, curr ver: %d", sver, MND_VIEW_VER_NUMBER);
    goto VIEW_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(SViewObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_DECODE_OVER;
  }

  pView = sdbGetRowObj(pRow);
  if (pView == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_DECODE_OVER;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, VIEW_DECODE_OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_DECODE_OVER;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, VIEW_DECODE_OVER);

  if (tDeserializeSViewObj(buf, tlen, pView) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_DECODE_OVER;
  }

  taosInitRWLatch(&pView->lock);

VIEW_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("view:%s, failed to decode from raw:%p since %s", pView == NULL ? "null" : pView->fullname, pRaw,
           terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("view:%s, decode from raw:%p, row:%p", pView->fullname, pRaw, pView);
  return pRow;
}

int32_t mndViewActionInsert(SSdb *pSdb, SViewObj *pView) {
  mTrace("view:%s, perform insert action", pView->fullname);
  return 0;
}

int32_t mndViewActionDelete(SSdb *pSdb, SViewObj *pView) {
  mTrace("view:%s, perform delete action", pView->fullname);
  tFreeViewObj(pView);
  return 0;
}

int32_t mndViewActionUpdate(SSdb *pSdb, SViewObj *pOldView, SViewObj *pNewView) {
  taosWLockLatch(&pOldView->lock);

  mTrace("view:%s, perform update action, old row:%p new row:%p", pOldView->fullname, pOldView, pNewView);

  pOldView->viewId = pNewView->viewId;
  pOldView->dbId = pNewView->dbId;
  pOldView->version = pNewView->version;
  pOldView->precision = pNewView->precision;
  pOldView->numOfCols = pNewView->numOfCols;
  TSWAP(pOldView->querySql, pNewView->querySql);
  TSWAP(pOldView->pSchema, pNewView->pSchema);

  taosWUnLockLatch(&pOldView->lock);

  return 0;
}

SViewObj *mndAcquireView(SMnode *pMnode, char *viewName) {
  SSdb       *pSdb = pMnode->pSdb;
  SViewObj   *pView = sdbAcquire(pSdb, SDB_VIEW, viewName);
  if (pView == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pView;
}

void mndReleaseView(SMnode *pMnode, SViewObj *pView) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pView);
}

static int32_t mndCreateViewObj(SMnode *pMnode, SViewObj* pView, SCMCreateViewReq* pCreate, SViewObj *pOldView) {
  SDbObj* pDb = mndAcquireDb(pMnode, pCreate->dbFName);
  if (NULL == pDb) {
    return -1;
  }

  pView->createdTime = taosGetTimestampMs();
  pView->viewId = mndGenerateUid(pCreate->fullname, strlen(pCreate->fullname));
  pView->dbId = pDb->uid;
  pView->querySql = strdup(pCreate->querySql);
  if (NULL == pView) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }
  pView->pSchema = taosMemoryMalloc(pCreate->numOfCols * sizeof(SSchema));
  if (NULL == pView->pSchema) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }
  memcpy(pView->pSchema, pCreate->pSchema, pCreate->numOfCols * sizeof(SSchema));
  tstrncpy(pView->fullname, pCreate->fullname, sizeof(pView->fullname));
  tstrncpy(pView->name, pCreate->name, sizeof(pView->name));
  tstrncpy(pView->dbFName, pCreate->dbFName, sizeof(pView->dbFName));
  pView->precision = pCreate->precision;
  pView->numOfCols = pCreate->numOfCols;
  if (NULL != pOldView) {
    pView->version = pOldView->version + 1;
  } else {
    pView->version = 1;
  }

  mndReleaseDb(pMnode, pDb);

  return TSDB_CODE_SUCCESS;
  
_OVER:

  tFreeViewObj(pView);
  mndReleaseDb(pMnode, pDb);
  return -1;
}

static int32_t mndCreateView(SMnode *pMnode, SCMCreateViewReq *pCreate, SRpcMsg *pReq, SViewObj *pOldView) {
  SViewObj view = {0};
  int32_t code = -1;
  if (mndCreateViewObj(pMnode, &view, pCreate, pOldView) != 0) {
    return -1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-view");
  if (pTrans == NULL) {
    mError("view:%s, failed to create since %s", pCreate->fullname, terrstr());
    goto _OVER;
  }

  mInfo("trans:%d, used to create view:%s", pTrans->id, pCreate->fullname);

  SSdbRaw *pCommitRaw = mndViewActionEncode(&view);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    goto _OVER;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  mndTransDrop(pTrans);
  code = 0;

_OVER:
  
  tFreeViewObj(&view);
  
  return 0;
}

static int32_t mndDropView(SMnode *pMnode, SRpcMsg *pReq, SViewObj *pView) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "drop-view");
  if (pTrans == NULL) {
    mError("view:%s, failed to drop since %s", pView->fullname, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to drop view:%s", pTrans->id, pView->fullname);

  SSdbRaw *pCommitRaw = mndViewActionEncode(pView);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}


static void mndLogCreateViewAudit(SRpcMsg *pReq, SMnode *pMnode, SCMCreateViewReq* pCreateViewReq) {
  char detail[2000] = {0};
  sprintf(detail, "orReplace:%d, precision:%d, numOfCols:%d",
          pCreateViewReq->orReplace, pCreateViewReq->precision, pCreateViewReq->numOfCols);

  auditRecord(pReq, pMnode->clusterId, "createView", pCreateViewReq->dbFName, pCreateViewReq->name, detail);
}

static void mndLogDropViewAudit(SRpcMsg *pReq, SMnode *pMnode, SCMDropViewReq* pDropViewReq) {
  char detail[100] = {0};
  sprintf(detail, "igNotExists:%d", pDropViewReq->igNotExists);

  auditRecord(pReq, pMnode->clusterId, "dropView", pDropViewReq->dbFName, pDropViewReq->name, detail);
}


int32_t mndProcessCreateViewReqImpl(SCMCreateViewReq* pCreateView, SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = -1;
  SViewObj          *pOldView = NULL;
  SDbObj            *pDb = NULL;
  SViewObj           newObj = {0};

  pOldView = mndAcquireView(pMnode, pCreateView->fullname);
  if (pOldView != NULL) {
    if (!pCreateView->orReplace) {
      terrno = TSDB_CODE_MND_VIEW_ALREADY_EXIST;
      goto _OVER;
    } else {
      mInfo("view %s already exist, or replace is set", pCreateView->fullname);
    }
  } else if (terrno != TSDB_CODE_SUCCESS) {
    goto _OVER;
  }

  if (mndCreateView(pMnode, pCreateView, pReq, pOldView) < 0) {
    mError("view:%s, failed to create since %s", pCreateView->fullname, terrstr());
    goto _OVER;
  }

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  mndLogCreateViewAudit(pReq, pMnode, pCreateView);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to create view %s since %s", pCreateView->fullname, terrstr());
  }

  mndReleaseView(pMnode, pOldView);

  tFreeSCMCreateViewReq(pCreateView);
  return code;
}

int32_t mndProcessDropViewReqImpl(SCMDropViewReq* pDropView, SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  int32_t     code = -1;
  SViewObj   *pView = mndAcquireView(pMnode, pDropView->fullname);

  if (pView == NULL) {
    if (pDropView->igNotExists) {
      mInfo("view:%s, not exist, ignore not exist is set", pDropView->name);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_VIEW_NOT_EXIST;
      return -1;
    }
  }

  if (mndDropView(pMnode, pReq, pView) < 0) {
    goto _OVER;
  }

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  mndLogDropViewAudit(pReq, pMnode, pDropView);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to drop view %s since %s", pDropView->fullname, terrstr());
  }

  sdbRelease(pMnode->pSdb, pView);

  return code;
}

int32_t mndProcessViewMetaReqImpl(SViewMetaReq* pMetaReq, SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  int32_t     code = -1;
  SViewObj   *pView = mndAcquireView(pMnode, pMetaReq->fullname);
  if (pView == NULL) {
    terrno = TSDB_CODE_MND_VIEW_NOT_EXIST;
    return -1;
  }

  SViewMetaRsp rsp = {0};
  tstrncpy(rsp.name, pView->name, sizeof(rsp.name));
  tstrncpy(rsp.dbFName, pView->dbFName, sizeof(rsp.dbFName));
  rsp.dbId = pView->dbId;
  rsp.viewId = pView->viewId;
  rsp.querySql = strdup(pView->querySql);
  rsp.version = pView->version;

  int32_t rspLen = tSerializeSViewMetaRsp(NULL, 0, &rsp);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  tSerializeSViewMetaRsp(pRsp, rspLen, &rsp);
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = rspLen;
  code = 0;

  mTrace("view %s meta is retrieved", pMetaReq->fullname);

_OVER:
  if (code != 0) {
    mError("view:%s, failed to retrieve meta since %s", pMetaReq->fullname, terrstr());
  }

  mndReleaseView(pMnode, pView);
  tFreeSViewMetaRsp(&rsp);
  return code;
}

static void mndGenerateViewTypeStr(char* buf, int8_t type) {
  if (0 == type) {
    strcpy(buf, "NORMAL ");
    return;
  }

  *buf = 0;
  if (type | VIEW_TYPE_UPDATABLE) {
    strcpy(buf, "UPDATABLE ");
  }
  if (type | VIEW_TYPE_MATERIALIZED) {
    strcat(buf, "MATERIALIZED ");
  }
}

static void mndGenerateViewColListStr(char* buf, int32_t bufSize, int32_t colNum, SSchema* pSchema) {
  int32_t offset = 0;
  for (int32_t i = 0; i < colNum; ++i) {
    SSchema* pCol = pSchema + i;
    if (IS_VAR_DATA_TYPE(pCol->type)) {
      if (i > 0) {
        offset += snprintf(buf + offset, bufSize - offset, ", `%s` %s(%lu)", pCol->name, tDataTypes[pCol->type].name, pCol->bytes - VARSTR_HEADER_SIZE);
      } else {
        offset += snprintf(buf + offset, bufSize - offset, "`%s` %s(%lu)", pCol->name, tDataTypes[pCol->type].name, pCol->bytes - VARSTR_HEADER_SIZE);
      }
    } else {
      if (i > 0) {
        offset += snprintf(buf + offset, bufSize - offset, ", `%s` %s", pCol->name, tDataTypes[pCol->type].name);
      } else {
        offset += snprintf(buf + offset, bufSize - offset, "`%s` %s", pCol->name, tDataTypes[pCol->type].name);
      }
    }

    if (offset >= bufSize) {
      break;
    }
  }
}


static void mndGenerateViewDefValsListStr(char* buf, int32_t bufSize, int32_t colNum, void** pDefVals) {
  //TODO
}


int32_t mndRetrieveViewImpl(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SViewObj   *pView = NULL;

  SDbObj *pDb = NULL;
  if (strlen(pShow->db) > 0) {
    char *p = strchr(pShow->db, '.');
    if (p && ((0 == strcmp(p + 1, TSDB_INFORMATION_SCHEMA_DB) || (0 == strcmp(p + 1, TSDB_PERFORMANCE_SCHEMA_DB))))) {
      return 0;
    }
    
    pDb = mndAcquireDb(pMnode, pShow->db);
    if (pDb == NULL) return terrno;
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_VIEW, pShow->pIter, (void **)&pView);
    if (pShow->pIter == NULL) break;

    if (pDb != NULL && pView->dbId != pDb->uid) {
      sdbRelease(pSdb, pView);
      continue;
    }

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->name, sizeof(tmpBuf));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    SName name = {0};
    tNameFromString(&name, pView->dbFName, T_NAME_ACCT | T_NAME_DB);
    tNameGetDbName(&name, varDataVal(tmpBuf));
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pView->createdTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    mndGenerateViewTypeStr(varDataVal(tmpBuf), pView->type);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->querySql, sizeof(tmpBuf));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    mndGenerateViewColListStr(varDataVal(tmpBuf), sizeof(tmpBuf) - VARSTR_HEADER_SIZE, pView->numOfCols, pView->pSchema);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL != pView->parameters) {
      STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->parameters, sizeof(tmpBuf));
      colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);
    } else {
      colDataSetVal(pColInfo, numOfRows, NULL, true);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL != pView->defaultValues) {
      mndGenerateViewDefValsListStr(varDataVal(tmpBuf), sizeof(tmpBuf) - VARSTR_HEADER_SIZE, pView->numOfCols, pView->defaultValues);
      colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);
    } else {
      colDataSetVal(pColInfo, numOfRows, NULL, true);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL != pView->targetTable) {
      STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->targetTable, sizeof(tmpBuf));
      colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);
    } else {
      colDataSetVal(pColInfo, numOfRows, NULL, true);
    }

    numOfRows++;
    sdbRelease(pSdb, pView);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextViewImpl(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}


int32_t mndSetDropViewCommitLogs(SMnode *pMnode, STrans *pTrans, SViewObj *pView) {
  SSdbRaw *pCommitRaw = mndViewActionEncode(pView);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;

  return 0;
}

int32_t mndDropViewByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;
  SViewObj *pView = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VIEW, pIter, (void **)&pView);
    if (pIter == NULL) break;

    if (pView->dbId == pDb->uid) {
      if (mndSetDropViewCommitLogs(pMnode, pTrans, pView) != 0) {
        sdbRelease(pSdb, pView);
        sdbCancelFetch(pSdb, pIter);
        return -1;
      }
    }

    sdbRelease(pSdb, pView);
  }

  return 0;
}




