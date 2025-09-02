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
#include "mndUser.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "audit.h"

#define MND_VIEW_VER_NUMBER 1

SDynViewVersion gViewVer = {0};

void initDynViewVersion(void) {
  gViewVer.svrBootTs = taosGetTimestampMs();
  gViewVer.dynViewVer = 1;
}

void tFreeViewObj(SViewObj *pView) {
  taosMemoryFree(pView->querySql);
  taosMemoryFree(pView->pSchema);
}

int32_t tSerializeSViewObj(void *buf, int32_t bufLen, const SViewObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->fullname));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->name));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->dbFName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->user));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->querySql));
  if (NULL != pObj->parameters) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 1));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->parameters));
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));
  }
  if (NULL != pObj->defaultValues) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 1));
    // TODO
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));
  }
  if (NULL != pObj->targetTable) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 1));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->targetTable));
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));
  }
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pObj->viewId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pObj->dbId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->createdTime));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->version));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->precision));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->type));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->numOfCols));
  for (int32_t i = 0; i < pObj->numOfCols; ++i) {
    SSchema *pSchema = &pObj->pSchema[i];
    TAOS_CHECK_EXIT(tEncodeSSchema(&encoder, pSchema));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSViewObj(void *buf, int32_t bufLen, SViewObj *pObj) {
  int8_t   ex = 0;
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->fullname));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->dbFName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->user));
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pObj->querySql));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &ex));
  if (0 != ex) {
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pObj->parameters));
  } else {
    pObj->parameters = NULL;
  }
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &ex));
  if (0 != ex) {
    // TODO
  } else {
    pObj->defaultValues = NULL;
  }
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &ex));
  if (0 != ex) {
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pObj->targetTable));
  } else {
    pObj->targetTable = NULL;
  }
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pObj->viewId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pObj->dbId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->createdTime));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->version));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->precision));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->type));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->numOfCols));

  if (pObj->numOfCols > 0) {
    pObj->pSchema = taosMemoryCalloc(pObj->numOfCols, sizeof(SSchema));
    if (pObj->pSchema == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < pObj->numOfCols; ++i) {
      SSchema *pSchema = pObj->pSchema + i;
      TAOS_CHECK_EXIT(tDecodeSSchema(&decoder, pSchema));
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}



SSdbRaw *mndViewActionEncode(SViewObj *pView) {
  int32_t code = 0;
  int32_t lino = 0;
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
  int32_t   code = 0;
  int32_t   lino = 0;
  SSdbRow  *pRow = NULL;
  SViewObj *pView = NULL;
  void     *buf = NULL;
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
  (void)atomic_add_fetch_64(&gViewVer.dynViewVer, 1);
  return 0;
}

int32_t mndViewActionDelete(SSdb *pSdb, SViewObj *pView) {
  mTrace("view:%s, perform delete action", pView->fullname);
  tFreeViewObj(pView);
  (void)atomic_add_fetch_64(&gViewVer.dynViewVer, 1);
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
  pOldView->createdTime = pNewView->createdTime;
  pOldView->type = pNewView->type;
  TSWAP(pOldView->querySql, pNewView->querySql);
  TSWAP(pOldView->parameters, pNewView->parameters);
  TSWAP(pOldView->defaultValues, pNewView->defaultValues);
  TSWAP(pOldView->targetTable, pNewView->targetTable);
  TSWAP(pOldView->pSchema, pNewView->pSchema);
  tstrncpy(pOldView->user, pNewView->user, sizeof(pOldView->user));

  taosWUnLockLatch(&pOldView->lock);

  (void)atomic_add_fetch_64(&gViewVer.dynViewVer, 1);

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

static int32_t mndCreateViewObj(SMnode *pMnode, SViewObj* pView, SCMCreateViewReq* pCreate, SViewObj *pOldView, char* user) {
  char* dbFName = pCreate->dbFName;
  char* sep = strchr(pCreate->dbFName, '.');
  if (NULL != sep && IS_SYS_DBNAME(sep + 1)) {
    pView->dbId = 0;
    dbFName = sep + 1;
  } else {
    SDbObj* pDb = mndAcquireDb(pMnode, pCreate->dbFName);
    if (NULL == pDb) {
      return -1;
    }
    pView->dbId = pDb->uid;
    mndReleaseDb(pMnode, pDb);
  }

  pView->createdTime = taosGetTimestampMs();
  pView->viewId = mndGenerateUid(pCreate->fullname, strlen(pCreate->fullname));
  pView->querySql = tstrdup(pCreate->querySql);
  if (NULL == pView->querySql) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }
  pView->pSchema = taosMemoryMalloc(pCreate->numOfCols * sizeof(SSchema));
  if (NULL == pView->pSchema) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }
  TAOS_MEMCPY(pView->pSchema, pCreate->pSchema, pCreate->numOfCols * sizeof(SSchema));
  tstrncpy(pView->fullname, pCreate->fullname, sizeof(pView->fullname));
  tstrncpy(pView->name, pCreate->name, sizeof(pView->name));
  tstrncpy(pView->dbFName, dbFName, sizeof(pView->dbFName));
  tstrncpy(pView->user, user, sizeof(pView->user));
  pView->precision = pCreate->precision;
  pView->numOfCols = pCreate->numOfCols;
  if (NULL != pOldView) {
    pView->version = pOldView->version + 1;
  } else {
    pView->version = 1;
  }


  return TSDB_CODE_SUCCESS;
  
_OVER:

  tFreeViewObj(pView);
  return terrno;
}

static int32_t mndCreateView(SMnode *pMnode, SCMCreateViewReq *pCreate, SRpcMsg *pReq, SViewObj *pOldView) {
  SViewObj view = {0};
  int32_t code = -1;
  SUserObj *pUser = NULL;
  SUserObj newUserObj = {0}, *pNewUserDuped = NULL;

  TAOS_CHECK_RETURN(mndAcquireUser(pMnode, pReq->info.conn.user, &pUser));

  if (mndCreateViewObj(pMnode, &view, pCreate, pOldView, pReq->info.conn.user) != 0) {
    code = terrno;
    goto _OVER;
  }

  // add view privileges for user
  if (!pUser->superUser) {
    code = mndUserDupObj(pUser, &newUserObj);
    if (code != 0) {
      terrno = code;
      goto _OVER;
    }
    if (taosHashPut(newUserObj.readViews, pCreate->fullname, strlen(pCreate->fullname) + 1, "v", 2)) {
      code = terrno;
      goto _OVER;
    }
    if (taosHashPut(newUserObj.writeViews, pCreate->fullname, strlen(pCreate->fullname) + 1, "v", 2)) {
      code = terrno;
      goto _OVER;
    }
    if (taosHashPut(newUserObj.alterViews, pCreate->fullname, strlen(pCreate->fullname) + 1, "v", 2)) {
      code = terrno;
      goto _OVER;
    }
    int32_t  dbKeyLen = strlen(pCreate->dbFName) + 1;
    int32_t  ref = 3;
    int32_t *currRef = taosHashGet(newUserObj.useDbs, pCreate->dbFName, dbKeyLen);
    if (NULL != currRef) {
      ref += (*currRef);
    }
    if (taosHashPut(newUserObj.useDbs, pCreate->dbFName, dbKeyLen, &ref, sizeof(ref)) != 0) {
      code = terrno;
      goto _OVER;
    }

    pNewUserDuped = &newUserObj;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-view");
  if (pTrans == NULL) {
    mError("view:%s, failed to create since %s", pCreate->fullname, terrstr());
    code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to create view:%s", pTrans->id, pCreate->fullname);

  SSdbRaw *pCommitRaw = mndViewActionEncode(&view);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    code = terrno;
    mError("trans:%d, failed to append view commit log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    goto _OVER;
  }
  TAOS_CHECK_GOTO(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY), NULL, _OVER);

  if (NULL != pNewUserDuped) {
    SSdbRaw *pUserRaw = mndUserActionEncode(pNewUserDuped);
    if (pUserRaw == NULL || mndTransAppendCommitlog(pTrans, pUserRaw) != 0) {
      mError("trans:%d, failed to append user commit log since %s", pTrans->id, terrstr());
      sdbFreeRaw(pUserRaw);
      mndTransDrop(pTrans);
      goto _OVER;
    }    
    TAOS_CHECK_GOTO(sdbSetRawStatus(pUserRaw, SDB_STATUS_READY), NULL, _OVER);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    code = terrno;
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  mndTransDrop(pTrans);
  code = 0;

_OVER:

  mndReleaseUser(pMnode, pUser);

  mndUserFreeObj(&newUserObj);  
  tFreeViewObj(&view);
  
  return code;
}

static int32_t mndDropView(SMnode *pMnode, SRpcMsg *pReq, SViewObj *pView) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "drop-view");
  if (pTrans == NULL) {
    mError("view:%s, failed to drop since %s", pView->fullname, terrstr());
    return terrno;
  }
  mInfo("trans:%d, used to drop view:%s", pTrans->id, pView->fullname);

  SSdbRaw *pCommitRaw = mndViewActionEncode(pView);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return terrno;
  }
  int32_t code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);
  if (code) {
    mError("trans:%d, failed to sdbSetRawStatus SDB_STATUS_DROPPED, error:%s", pTrans->id, tstrerror(code));
    mndTransDrop(pTrans);
    return code;
  }

  if (mndUserRemoveView(pMnode, pTrans, pView->fullname)) {
    mError("trans:%d, failed to append user dropo view commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return terrno;
  }
  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return terrno;
  }

  mndTransDrop(pTrans);
  return 0;
}


static void mndLogCreateViewAudit(SRpcMsg *pReq, SMnode *pMnode, SCMCreateViewReq* pCreateViewReq) {
  auditRecord(pReq, pMnode->clusterId, "createView", pCreateViewReq->dbFName, pCreateViewReq->name, pCreateViewReq->sql, strlen(pCreateViewReq->sql));
}

static void mndLogDropViewAudit(SRpcMsg *pReq, SMnode *pMnode, SCMDropViewReq* pDropViewReq) {
  auditRecord(pReq, pMnode->clusterId, "dropView", pDropViewReq->dbFName, pDropViewReq->name, pDropViewReq->sql, strlen(pDropViewReq->sql));
}

static int32_t dumpViewMetaRspFromView(SViewMetaRsp *pRsp, SViewObj* pView) {
  tstrncpy(pRsp->name, pView->name, sizeof(pRsp->name));
  tstrncpy(pRsp->dbFName, pView->dbFName, sizeof(pRsp->dbFName));
  pRsp->user = tstrdup(pView->user);
  if (pRsp->user == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  pRsp->dbId = pView->dbId;
  pRsp->viewId = pView->viewId;
  pRsp->querySql = tstrdup(pView->querySql);
  if (pRsp->querySql == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  pRsp->precision = pView->precision;
  pRsp->type = pView->type;
  pRsp->version = pView->version;
  pRsp->numOfCols = pView->numOfCols;
  pRsp->pSchema = taosMemoryMalloc(pView->numOfCols * sizeof(SSchema));
  if (pRsp->pSchema == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  TAOS_MEMCPY(pRsp->pSchema, pView->pSchema, pView->numOfCols * sizeof(SSchema));

  return TSDB_CODE_SUCCESS;
}

int32_t mndProcessCreateViewReqImpl(SCMCreateViewReq* pCreateView, SRpcMsg *pReq) {
  SMnode   *pMnode = pReq->info.node;
  int32_t   code = -1;
  SViewObj *pOldView = NULL;
  SViewObj  newObj = {0};
  SDbObj   *pDb = NULL;
  char* dbFName = pCreateView->dbFName;

  if ((terrno = grantCheck(TSDB_GRANT_VIEW)) != 0) {
    code = terrno;
    goto _OVER;
  }

  char* sep = strchr(pCreateView->dbFName, '.');
  if (NULL != sep && IS_SYS_DBNAME(sep + 1)) {
    //DO NOTHING
  } else {
    pDb = mndAcquireDb(pMnode, pCreateView->dbFName);
    if (NULL == pDb) {
      code = terrno;
      goto _OVER;
    }
    
    if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb) != 0) {
      code = terrno;
      goto _OVER;
    }
  }

  pOldView = mndAcquireView(pMnode, pCreateView->fullname);
  if (pOldView != NULL) {
    if (!pCreateView->orReplace) {
      terrno = TSDB_CODE_MND_VIEW_ALREADY_EXIST;
      goto _OVER;
    } else {
      mInfo("view %s already exist, or replace is set", pCreateView->fullname);
    }
    
    if (0 != mndCheckViewPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_VIEW, pCreateView->fullname)) {
      code = terrno;
      goto _OVER;
    }
  } else if (terrno != TSDB_CODE_SUCCESS) {
    code = terrno;
    goto _OVER;
  }

  if (mndCreateView(pMnode, pCreateView, pReq, pOldView) < 0) {
    code = terrno;
    mError("view:%s, failed to create since %s", pCreateView->fullname, terrstr());
    goto _OVER;
  }

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  mndLogCreateViewAudit(pReq, pMnode, pCreateView);

_OVER:

  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to create view %s since %s", pCreateView->fullname, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
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
      tFreeSCMDropViewReq(pDropView);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_VIEW_NOT_EXIST;
      tFreeSCMDropViewReq(pDropView);
      return terrno;
    }
  }

  if (0 != mndCheckViewPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_VIEW, pDropView->fullname)) {
    code = terrno;
    goto _OVER;
  }

  if (mndDropView(pMnode, pReq, pView) < 0) {
    code = terrno;
    goto _OVER;
  }

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  mndLogDropViewAudit(pReq, pMnode, pDropView);

_OVER:

  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to drop view %s since %s", pDropView->fullname, terrstr());
  }

  tFreeSCMDropViewReq(pDropView);

  sdbRelease(pMnode->pSdb, pView);

  return code;
}

int32_t mndProcessViewMetaReqImpl(SViewMetaReq* pMetaReq, SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  int32_t     code = -1;
  SViewObj   *pView = mndAcquireView(pMnode, pMetaReq->fullname);
  if (pView == NULL) {
    terrno = TSDB_CODE_MND_VIEW_NOT_EXIST;
    return terrno;
  }

  SViewMetaRsp rsp = {0};
  code = dumpViewMetaRspFromView(&rsp, pView);
  if (TSDB_CODE_SUCCESS != code) {
    code = terrno;
    goto _OVER;
  }

  int32_t rspLen = tSerializeSViewMetaRsp(NULL, 0, &rsp);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    code = terrno;
    goto _OVER;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = terrno;
    goto _OVER;
  }

  if (tSerializeSViewMetaRsp(pRsp, rspLen, &rsp) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = terrno;
    goto _OVER;
  }
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
    TAOS_STRCPY(buf, "NORMAL");
    return;
  }

  *buf = 0;
  if (type | VIEW_TYPE_UPDATABLE) {
    TAOS_STRCPY(buf, "UPDATABLE");
  }
  if (type | VIEW_TYPE_MATERIALIZED) {
    TAOS_STRCAT(buf, buf[0] ? " MATERIALIZED" : "MATERIALIZED");
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
  char       *sep = NULL;
  SDbObj     *pDb = NULL;
  int32_t     code = 0;
  
  if (strlen(pShow->db) > 0) {
    sep = strchr(pShow->db, '.');
    if (sep && ((0 == strcmp(sep + 1, TSDB_INFORMATION_SCHEMA_DB) || (0 == strcmp(sep + 1, TSDB_PERFORMANCE_SCHEMA_DB))))) {
      sep++;
    } else {
      pDb = mndAcquireDb(pMnode, pShow->db);
      if (pDb == NULL) return terrno;
    }
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_VIEW, pShow->pIter, (void **)&pView);
    if (pShow->pIter == NULL) break;

    if (pDb != NULL) {
      if (pView->dbId != pDb->uid) {
        sdbRelease(pSdb, pView);
        continue;
      }
    } else if (NULL != sep && 0 != strcmp(pView->dbFName, sep)) {
      sdbRelease(pSdb, pView);
      continue;
    }

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->name, sizeof(tmpBuf));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL == pColInfo) {
      TAOS_CHECK_GOTO(TSDB_CODE_QRY_INVALID_INPUT, NULL, _return);
    }
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), NULL, _return);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL == pColInfo) {
      TAOS_CHECK_GOTO(TSDB_CODE_QRY_INVALID_INPUT, NULL, _return);
    }
    if (pDb != NULL || !IS_SYS_DBNAME(pView->dbFName)) {
      SName name = {0};
      TAOS_CHECK_GOTO(tNameFromString(&name, pView->dbFName, T_NAME_ACCT | T_NAME_DB), NULL, _return);
      (void)tNameGetDbName(&name, varDataVal(tmpBuf));
    } else {
      tstrncpy(varDataVal(tmpBuf), pView->dbFName, TSDB_SHOW_SQL_LEN);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), NULL, _return);

    STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->user, sizeof(tmpBuf));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL == pColInfo) {
      TAOS_CHECK_GOTO(TSDB_CODE_QRY_INVALID_INPUT, NULL, _return);
    }
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), NULL, _return);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL == pColInfo) {
      TAOS_CHECK_GOTO(TSDB_CODE_QRY_INVALID_INPUT, NULL, _return);
    }
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pView->createdTime, false), NULL, _return);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL == pColInfo) {
      TAOS_CHECK_GOTO(TSDB_CODE_QRY_INVALID_INPUT, NULL, _return);
    }
    mndGenerateViewTypeStr(varDataVal(tmpBuf), pView->type);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), NULL, _return);

    STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->querySql, sizeof(tmpBuf));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL == pColInfo) {
      sdbRelease(pSdb, pView);
      TAOS_CHECK_GOTO(TSDB_CODE_QRY_INVALID_INPUT, NULL, _return);
    }
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), NULL, _return);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL == pColInfo) {
      TAOS_CHECK_GOTO(TSDB_CODE_QRY_INVALID_INPUT, NULL, _return);
    }
    if (NULL != pView->parameters) {
      STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->parameters, sizeof(tmpBuf));
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), NULL, _return);
    } else {
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, NULL, true), NULL, _return);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL == pColInfo) {
      TAOS_CHECK_GOTO(TSDB_CODE_QRY_INVALID_INPUT, NULL, _return);
    }
    if (NULL != pView->defaultValues) {
      mndGenerateViewDefValsListStr(varDataVal(tmpBuf), sizeof(tmpBuf) - VARSTR_HEADER_SIZE, pView->numOfCols, pView->defaultValues);
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), NULL, _return);
    } else {
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, NULL, true), NULL, _return);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (NULL == pColInfo) {
      TAOS_CHECK_GOTO(TSDB_CODE_QRY_INVALID_INPUT, NULL, _return);
    }
    if (NULL != pView->targetTable) {
      STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->targetTable, sizeof(tmpBuf));
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), NULL, _return);
    } else {
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, NULL, true), NULL, _return);
    }

/*
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    mndGenerateViewColListStr(varDataVal(tmpBuf), sizeof(tmpBuf) - VARSTR_HEADER_SIZE, pView->numOfCols, pView->pSchema);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);
*/

    numOfRows++;
    sdbRelease(pSdb, pView);
    pView = NULL;
  }

  pShow->numOfRows += numOfRows;

_return:

  if (pSdb && pView) {
    sdbRelease(pSdb, pView);
  }

  if (pMnode && pDb) {
    mndReleaseDb(pMnode, pDb);
  }
  
  return numOfRows;
}

static void mndCancelGetNextViewImpl(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_VIEW);
}


int32_t mndSetDropViewCommitLogs(SMnode *pMnode, STrans *pTrans, SViewObj *pView) {
  SSdbRaw *pCommitRaw = mndViewActionEncode(pView);
  if (pCommitRaw == NULL) return terrno;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return terrno;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return terrno;

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
        return terrno;
      }
    }

    sdbRelease(pSdb, pView);
  }

  return 0;
}

int32_t mndValidateDynViewVersion(SMnode *pMnode, SDynViewVersion* pReqVer, bool *needCheck, SDynViewVersion** ppRspVer) {
  if (pReqVer->svrBootTs != gViewVer.svrBootTs || pReqVer->dynViewVer != gViewVer.dynViewVer) {
    *needCheck = true;
    *ppRspVer = taosMemoryMalloc(sizeof(SDynViewVersion));
    if (NULL == *ppRspVer) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return terrno;
    }
    
    (*ppRspVer)->svrBootTs = gViewVer.svrBootTs;
    (*ppRspVer)->dynViewVer = atomic_load_64(&gViewVer.dynViewVer);

    return TSDB_CODE_SUCCESS;
  }

  *needCheck = false;
  
  return TSDB_CODE_SUCCESS;
}

int32_t mndValidateViewInfo(SMnode *pMnode, SViewVersion *pViewVersions, int32_t numOfViews, void **ppRsp,
                           int32_t *pRspLen) {
  char viewFName[TSDB_VIEW_FNAME_LEN] = {0};
  int32_t rspLen = 0;
  void *pRsp = NULL;
  int32_t code = -1;
  SViewHbRsp hbRsp = {0};
  hbRsp.pViewRsp = taosArrayInit(numOfViews, POINTER_BYTES);
  if (hbRsp.pViewRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  for (int32_t i = 0; i < numOfViews; ++i) {
    SViewVersion *pViewVersion = &pViewVersions[i];
    pViewVersion->dbId = be64toh(pViewVersion->dbId);
    pViewVersion->viewId = be64toh(pViewVersion->viewId);
    pViewVersion->version = ntohl(pViewVersion->version);

    (void)snprintf(viewFName, sizeof(viewFName), "%s.%s", pViewVersion->dbFName, pViewVersion->viewName);

    SViewObj *pView = mndAcquireView(pMnode, viewFName);
    if (pView == NULL) {
      mTrace("view %s not exists", viewFName);
      SViewMetaRsp *metaRsp = taosMemoryCalloc(1, sizeof(SViewMetaRsp));
      if (NULL == metaRsp) {
        code = terrno;
        goto _OVER;
      }
      metaRsp->numOfCols = -1;
      metaRsp->viewId = pViewVersion->viewId;
      metaRsp->dbId = pViewVersion->dbId;
      metaRsp->user = taosMemoryCalloc(1, 1);
      if (NULL == metaRsp->user) {
        code = terrno;
        tFreeSViewMetaRsp(metaRsp);
        goto _OVER;
      }
      metaRsp->querySql = taosMemoryCalloc(1, 1);
      if (NULL == metaRsp->querySql) {
        code = terrno;
        tFreeSViewMetaRsp(metaRsp);
        goto _OVER;
      }
      tstrncpy(metaRsp->dbFName, pViewVersion->dbFName, sizeof(metaRsp->dbFName));
      tstrncpy(metaRsp->name, pViewVersion->viewName, sizeof(metaRsp->name));
      if (NULL == taosArrayPush(hbRsp.pViewRsp, &metaRsp)) {
        code = terrno;
        tFreeSViewMetaRsp(metaRsp);
        goto _OVER;
      }
      continue;
    }

    if (pView->viewId != pViewVersion->viewId) {
      mTrace("view %s,%" PRIx64 " viewId mismatch with current %" PRIx64, viewFName, pViewVersion->viewId, pView->viewId);
      
      SViewMetaRsp *metaRsp = taosMemoryCalloc(1, sizeof(SViewMetaRsp));
      if (NULL == metaRsp) {
        code = terrno;
        mndReleaseView(pMnode, pView);      
        goto _OVER;
      }
      metaRsp->numOfCols = -1;
      metaRsp->viewId = pViewVersion->viewId;
      metaRsp->dbId = pViewVersion->dbId;
      metaRsp->user = taosMemoryCalloc(1, 1);
      if (NULL == metaRsp->user) {
        code = terrno;
        tFreeSViewMetaRsp(metaRsp);
        mndReleaseView(pMnode, pView);      
        goto _OVER;
      }
      metaRsp->querySql = taosMemoryCalloc(1, 1);
      if (NULL == metaRsp->querySql) {
        code = terrno;
        tFreeSViewMetaRsp(metaRsp);
        mndReleaseView(pMnode, pView);      
        goto _OVER;
      }
      tstrncpy(metaRsp->dbFName, pViewVersion->dbFName, sizeof(metaRsp->dbFName));
      tstrncpy(metaRsp->name, pViewVersion->viewName, sizeof(metaRsp->name));
      if (NULL == taosArrayPush(hbRsp.pViewRsp, &metaRsp)) {
        code = terrno;
        tFreeSViewMetaRsp(metaRsp);
        mndReleaseView(pMnode, pView);      
        goto _OVER;
      }      
    } else if (pView->version == pViewVersion->version) {
      mTrace("view %s version %d match with current", viewFName, pViewVersion->version);
      mndReleaseView(pMnode, pView);
      continue;
    }
    
    SViewMetaRsp* rsp = taosMemoryCalloc(1, sizeof(SViewMetaRsp));
    if (NULL == rsp) {
      code = terrno;
      mndReleaseView(pMnode, pView);      
      goto _OVER;
    }
    int32_t code = dumpViewMetaRspFromView(rsp, pView);
    if (TSDB_CODE_SUCCESS != code) {
      mndReleaseView(pMnode, pView);      
      tFreeSViewMetaRsp(rsp);
      taosMemoryFree(rsp);
      goto _OVER;
    }

    mTrace("view %s,%" PRIx64 " got lastest meta, current ver:%d, recv ver:%d", viewFName, pView->viewId, pView->version, pViewVersion->version);

    if (NULL == taosArrayPush(hbRsp.pViewRsp, &rsp)) {
      code = terrno;
      mndReleaseView(pMnode, pView);      
      tFreeSViewMetaRsp(rsp);
      taosMemoryFree(rsp);
      goto _OVER;
    }
    
    mndReleaseView(pMnode, pView);
  }

  rspLen = tSerializeSViewHbRsp(NULL, 0, &hbRsp);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    code = terrno;
    goto _OVER;
  }

  pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = terrno;
    rspLen = 0;
    goto _OVER;
  }

  if (tSerializeSViewHbRsp(pRsp, rspLen, &hbRsp) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    code = terrno;
    goto _OVER;
  }
  code = 0;

_OVER:

  tFreeSViewHbRsp(&hbRsp);
  *ppRsp = pRsp;
  *pRspLen = rspLen;
  return code;
}


