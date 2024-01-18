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

#include "mndGrant.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "audit.h"

#define MND_GRANT_VER_NUMBER 1

void tFreeGrantObj(SGrantObj *pGrant) { taosArrayDestroy(pGrant->pMachines); }

int32_t mndProcessConfigClusterReq(SRpcMsg *pReq) {
#if 0
  SMnode         *pMnode = pReq->info.node;
  SMCfgClusterReq cfgReq = {0};
  if (tDeserializeSMCfgClusterReq(pReq->pCont, pReq->contLen, &cfgReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t code = 0;
  mInfo("cluster: start to config, option:%s, value:%s", cfgReq.config, cfgReq.value);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CONFIG_CLUSTER) != 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_MND_NO_RIGHTS;
    goto _exit;
  }

  SClusterObj  clusterObj = {0};
  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (!pCluster || pCluster->id <= 0) {
    code = TSDB_CODE_APP_IS_STARTING;
    if (pCluster) mndReleaseCluster(pMnode, pCluster, pIter);
    goto _exit;
  }
  memcpy(&clusterObj, pCluster, sizeof(SClusterObj));
  mndReleaseCluster(pMnode, pCluster, pIter);

  if (strncmp(cfgReq.config, GRANT_ACTIVE_CODE, 11) == 0) {
#ifdef TD_ENTERPRISE
    if (strlen(cfgReq.config) >= TSDB_DNODE_CONFIG_LEN) {
      code = TSDB_CODE_INVALID_CFG;
      goto _exit;
    }
    if (strlen(cfgReq.value) >= TSDB_DNODE_VALUE_LEN) {
      code = TSDB_CODE_INVALID_CFG_VALUE;
      goto _exit;
    }
    char *newActive = NULL;
    if ((code = grantAlterActiveCode(cfgReq.value, &newActive)) != 0) {
      goto _exit;
    }
#else
    code = TSDB_CODE_OPS_NOT_SUPPORT;
    goto _exit;
#endif
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-cluster");
  if (pTrans == NULL) return -1;

  SSdbRaw *pCommitRaw = mndClusterActionEncode(&clusterObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    code = terrno;
    goto _exit;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    code = terrno;
    goto _exit;
  }

  mndTransDrop(pTrans);

  {  // audit
    auditRecord(pReq, pMnode->clusterId, "alterCluster", "", "", cfgReq.sql, cfgReq.sqlLen);
  }
_exit:
  tFreeSMCfgClusterReq(&cfgReq);
  if (code != 0) {
    terrno = code;
    mError("cluster: failed to config:%s %s since %s", cfgReq.config, cfgReq.value, terrstr());
  } else {
    mInfo("cluster: success to config:%s %s", cfgReq.config, cfgReq.value);
  }
  return code;
#endif
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return 0;
}

int32_t mndProcessConfigClusterRsp(SRpcMsg *pRsp) {
  mInfo("config rsp from cluster");
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return 0;
}

int32_t mndRetrieveGrantLog(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
#if 0
  SMnode *pMnode = pReq->info.node;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char   *pWrite = NULL;
  char    tmp[192] = {0};
  char    tmp1[192] = {0};
  char    ts[GRANT_TS_SEC_LEN] = {0};

  if (pShow->numOfRows < 1) {
    SGrantDataIns *pDataIn = NULL;
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src = GRANT_VERSION;
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    GRANT_EXPIRE_SHOW(gStatus.basicExpireSec);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = gStatus.basicExpired || (gStatus.multiTierExpired && tsDiskCfgNum > 1) ? "true" : "false";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    GRANT_ITEM_SHOW(gStatus.curTimeSeries, gStatus.limitTimeSeries, 64);
    GRANT_ITEM_SHOW(gStatus.curDnodes, gStatus.limitDnodes, 16);
    GRANT_ITEM_SHOW(gStatus.curStreams, gStatus.limitStreams, 16);
    GRANT_ITEM_SHOW(gStatus.curSubscriptions, gStatus.limitSubscriptions, 16);
    GRANT_ITEM_SHOW(gStatus.curCpuCores, gStatus.limitCpuCores, 32);

    GRANT_EXPIRE_SHOW(gStatus.multiTierExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.streamExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.subscriptionExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.auditExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.csvExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.bakRstExpireSec);

    // connectors
    // for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
    //   GRANT_DATA_IN_SHOW(i);
    // }

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
#endif
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return 0;
}

void    mndCancelGetNextGrantLog(SMnode *pMnode, void *pIter) {
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t tSerializeSGrantObj(void *buf, int32_t bufLen, const SGrantObj *pObj) {

#if 0
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->fullname) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->name) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->dbFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->user) < 0) return -1;
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
#endif
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return 0;
}

int32_t tDeserializeSGrantObj(void *buf, int32_t bufLen, SGrantObj *pObj) {
#if 0
  int8_t ex = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->fullname) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->name) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->dbFName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->user) < 0) return -1;
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
#endif
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return 0;
}



SSdbRaw *mndGrantActionEncode(SGrantObj *pGrant) {
#if 0
  terrno = TSDB_CODE_SUCCESS;
  void *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t tlen = tSerializeSGrantObj(NULL, 0, pGrant);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }
  
  int32_t  size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_VIEW, MND_GRANT_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }

  tlen = tSerializeSGrantObj(buf, tlen, pView);
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
    mError("grant, failed to encode to raw:%p since %s", pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("grant, encode to raw:%p, row:%p", pRaw, pGrant);
  return pRaw;
#endif
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return NULL;
}

SSdbRow *mndGrantActionDecode(SSdbRaw *pRaw) {
#if 0
  SSdbRow    *pRow = NULL;
  SGrantObj   *pGrant = NULL;
  void       *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto VIEW_DECODE_OVER;
  }

  if (sver != MND_GRANT_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("grant read invalid ver, data ver: %d, curr ver: %d", sver, MND_GARNT_VER_NUMBER);
    goto _exit;
  }

  pRow = sdbAllocRow(sizeof(SGrantObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pView = sdbGetRowObj(pRow);
  if (pView == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, _exit);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _exit);

  if (tDeserializeSGrantObj(buf, tlen, pGrant) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  taosInitRWLatch(&pGrant->lock);

_exit:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("view, failed to decode from raw:%p since %s", pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("view, decode from raw:%p, row:%p", pRaw, pGrant);
  return pRow;
#endif
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return NULL;
}

int32_t mndGrantActionInsert(SSdb *pSdb, SGrantObj *pGrant) {
  mTrace("grant:%d, perform insert action", pGrant->id);
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return 0;
}

int32_t mndGrantActionDelete(SSdb *pSdb, SGrantObj *pGrant) {
  mTrace("grant:%d, perform delete action", pGrant->id);
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  tFreeGrantObj(pGrant);
  return 0;
}

int32_t mndGrantActionUpdate(SSdb *pSdb, SGrantObj *pOldGrant, SGrantObj *pNewGrant) {
  taosWLockLatch(&pOldGrant->lock);

  mTrace("grant:%d, perform update action, old row:%p new row:%p", pOldGrant->id, pOldGrant, pNewGrant);

  pOldGrant->id = pNewGrant->id;
  pOldGrant->createTime = pNewGrant->createTime;
  pOldGrant->updateTime = pNewGrant->updateTime;
  TSWAP(pOldGrant->state, pNewGrant->state);
  TSWAP(pOldGrant->active, pNewGrant->active);
  taosArrayClear(pOldGrant->pMachines);
  taosArrayAddAll(pOldGrant->pMachines, pNewGrant->pMachines);

  taosWUnLockLatch(&pOldGrant->lock);

  return 0;
}

SGrantObj *mndAcquireGrant(SMnode *pMnode, int32_t id) {
  SSdb       *pSdb = pMnode->pSdb;
  SGrantObj   *pView = sdbAcquire(pSdb, SDB_GRANT, &id);
  if (pView == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pView;
}

void mndReleaseGrant(SMnode *pMnode, SGrantObj *pGrant) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pGrant);
}

#if 0
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
  return -1;
}

static int32_t mndCreateView(SMnode *pMnode, SCMCreateViewReq *pCreate, SRpcMsg *pReq, SViewObj *pOldView) {
  SViewObj view = {0};
  int32_t code = -1;
  SUserObj *pUser = NULL;
  SUserObj newUserObj = {0}, *pNewUserDuped = NULL;

  pUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pUser == NULL) {
    return -1;
  }

  

  if (mndCreateViewObj(pMnode, &view, pCreate, pOldView, pReq->info.conn.user) != 0) {
    goto _OVER;
  }

  // add view privileges for user
  if (!pUser->superUser) {
    if (mndUserDupObj(pUser, &newUserObj) != 0) goto _OVER;
    taosHashPut(newUserObj.readViews, pCreate->fullname, strlen(pCreate->fullname) + 1, "v", 2);
    taosHashPut(newUserObj.writeViews, pCreate->fullname, strlen(pCreate->fullname) + 1, "v", 2);
    taosHashPut(newUserObj.alterViews, pCreate->fullname, strlen(pCreate->fullname) + 1, "v", 2);
    int32_t  dbKeyLen = strlen(pCreate->dbFName) + 1;
    int32_t  ref = 3;
    int32_t *currRef = taosHashGet(newUserObj.useDbs, pCreate->dbFName, dbKeyLen);
    if (NULL != currRef) {
      ref += (*currRef);
    }
    if (taosHashPut(newUserObj.useDbs, pCreate->dbFName, dbKeyLen, &ref, sizeof(ref)) != 0) {
      goto _OVER;
    }

    pNewUserDuped = &newUserObj;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-view");
  if (pTrans == NULL) {
    mError("view:%s, failed to create since %s", pCreate->fullname, terrstr());
    goto _OVER;
  }

  mInfo("trans:%d, used to create view:%s", pTrans->id, pCreate->fullname);

  SSdbRaw *pCommitRaw = mndViewActionEncode(&view);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append view commit log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    goto _OVER;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (NULL != pNewUserDuped) {
    SSdbRaw *pUserRaw = mndUserActionEncode(pNewUserDuped);
    if (pUserRaw == NULL || mndTransAppendCommitlog(pTrans, pUserRaw) != 0) {
      mError("trans:%d, failed to append user commit log since %s", pTrans->id, terrstr());
      sdbFreeRaw(pUserRaw);
      mndTransDrop(pTrans);
      goto _OVER;
    }    
    (void)sdbSetRawStatus(pUserRaw, SDB_STATUS_READY);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
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
  
  return 0;
}


int32_t mndProcessCreateViewReqImpl(SCMCreateViewReq* pCreateView, SRpcMsg *pReq) {
  SMnode   *pMnode = pReq->info.node;
  int32_t   code = -1;
  SViewObj *pOldView = NULL;
  SViewObj  newObj = {0};
  SDbObj   *pDb = NULL;
  char* dbFName = pCreateView->dbFName;
  char* sep = strchr(pCreateView->dbFName, '.');
  if (NULL != sep && IS_SYS_DBNAME(sep + 1)) {
    //DO NOTHING
  } else {
    pDb = mndAcquireDb(pMnode, pCreateView->dbFName);
    if (NULL == pDb) {
      goto _OVER;
    }
    
    if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb) != 0) {
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
      goto _OVER;
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

  mndReleaseDb(pMnode, pDb);
  mndReleaseView(pMnode, pOldView);

  tFreeSCMCreateViewReq(pCreateView);
  return code;
}

int32_t mndRetrieveViewImpl(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SViewObj   *pView = NULL;
  char       *sep = NULL;
  SDbObj     *pDb = NULL;
  
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
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pDb != NULL || !IS_SYS_DBNAME(pView->dbFName)) {
      SName name = {0};
      tNameFromString(&name, pView->dbFName, T_NAME_ACCT | T_NAME_DB);
      tNameGetDbName(&name, varDataVal(tmpBuf));
    } else {
      tstrncpy(varDataVal(tmpBuf), pView->dbFName, TSDB_SHOW_SQL_LEN);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pView->user, sizeof(tmpBuf));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
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

/*
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    mndGenerateViewColListStr(varDataVal(tmpBuf), sizeof(tmpBuf) - VARSTR_HEADER_SIZE, pView->numOfCols, pView->pSchema);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);
*/

    numOfRows++;
    sdbRelease(pSdb, pView);
  }

  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
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
#endif