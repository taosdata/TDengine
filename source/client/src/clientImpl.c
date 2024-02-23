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

#include "cJSON.h"
#include "clientInt.h"
#include "clientMonitor.h"
#include "clientLog.h"
#include "command.h"
#include "scheduler.h"
#include "tdatablock.h"
#include "tdataformat.h"
#include "tdef.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tpagedbuf.h"
#include "tref.h"
#include "tsched.h"
#include "tversion.h"
static int32_t       initEpSetFromCfg(const char* firstEp, const char* secondEp, SCorEpSet* pEpSet);
static SMsgSendInfo* buildConnectMsg(SRequestObj* pRequest);

static bool stringLengthCheck(const char* str, size_t maxsize) {
  if (str == NULL) {
    return false;
  }

  size_t len = strlen(str);
  if (len <= 0 || len > maxsize) {
    return false;
  }

  return true;
}

static bool validateUserName(const char* user) { return stringLengthCheck(user, TSDB_USER_LEN - 1); }

static bool validatePassword(const char* passwd) { return stringLengthCheck(passwd, TSDB_PASSWORD_LEN - 1); }

static bool validateDbName(const char* db) { return stringLengthCheck(db, TSDB_DB_NAME_LEN - 1); }

static char* getClusterKey(const char* user, const char* auth, const char* ip, int32_t port) {
  char key[512] = {0};
  snprintf(key, sizeof(key), "%s:%s:%s:%d", user, auth, ip, port);
  return taosStrdup(key);
}

bool chkRequestKilled(void* param) {
  bool         killed = false;
  SRequestObj* pRequest = acquireRequest((int64_t)param);
  if (NULL == pRequest || pRequest->killed) {
    killed = true;
  }

  releaseRequest((int64_t)param);

  return killed;
}

static STscObj* taosConnectImpl(const char* user, const char* auth, const char* db, __taos_async_fn_t fp, void* param,
                                SAppInstInfo* pAppInfo, int connType);

STscObj* taos_connect_internal(const char* ip, const char* user, const char* pass, const char* auth, const char* db,
                               uint16_t port, int connType) {
  if (taos_init() != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  if (!validateUserName(user)) {
    terrno = TSDB_CODE_TSC_INVALID_USER_LENGTH;
    return NULL;
  }

  char localDb[TSDB_DB_NAME_LEN] = {0};
  if (db != NULL && strlen(db) > 0) {
    if (!validateDbName(db)) {
      terrno = TSDB_CODE_TSC_INVALID_DB_LENGTH;
      return NULL;
    }

    tstrncpy(localDb, db, sizeof(localDb));
    strdequote(localDb);
  }

  char secretEncrypt[TSDB_PASSWORD_LEN + 1] = {0};
  if (auth == NULL) {
    if (!validatePassword(pass)) {
      terrno = TSDB_CODE_TSC_INVALID_PASS_LENGTH;
      return NULL;
    }

    taosEncryptPass_c((uint8_t*)pass, strlen(pass), secretEncrypt);
  } else {
    tstrncpy(secretEncrypt, auth, tListLen(secretEncrypt));
  }

  SCorEpSet epSet = {0};
  if (ip) {
    if (initEpSetFromCfg(ip, NULL, &epSet) < 0) {
      return NULL;
    }
  } else {
    if (initEpSetFromCfg(tsFirst, tsSecond, &epSet) < 0) {
      return NULL;
    }
  }

  if (port) {
    epSet.epSet.eps[0].port = port;
    epSet.epSet.eps[1].port = port;
  }

  char* key = getClusterKey(user, secretEncrypt, ip, port);

  tscInfo("connecting to server, numOfEps:%d inUse:%d user:%s db:%s key:%s", epSet.epSet.numOfEps, epSet.epSet.inUse,
          user, db, key);
  for (int32_t i = 0; i < epSet.epSet.numOfEps; ++i) {
    tscInfo("ep:%d, %s:%u", i, epSet.epSet.eps[i].fqdn, epSet.epSet.eps[i].port);
  }

  SAppInstInfo** pInst = NULL;
  taosThreadMutexLock(&appInfo.mutex);

  pInst = taosHashGet(appInfo.pInstMap, key, strlen(key));
  SAppInstInfo* p = NULL;
  if (pInst == NULL) {
    p = taosMemoryCalloc(1, sizeof(struct SAppInstInfo));
    p->mgmtEp = epSet;
    taosThreadMutexInit(&p->qnodeMutex, NULL);
    p->pTransporter = openTransporter(user, secretEncrypt, tsNumOfCores / 2);
    if (p->pTransporter == NULL) {
      taosThreadMutexUnlock(&appInfo.mutex);
      taosMemoryFreeClear(key);
      taosMemoryFree(p);
      return NULL;
    }
    p->pAppHbMgr = appHbMgrInit(p, key);
    if (NULL == p->pAppHbMgr) {
      destroyAppInst(p);
      taosThreadMutexUnlock(&appInfo.mutex);
      taosMemoryFreeClear(key);
      return NULL;
    }
    taosHashPut(appInfo.pInstMap, key, strlen(key), &p, POINTER_BYTES);
    p->instKey = key;
    key = NULL;
    tscDebug("new app inst mgr %p, user:%s, ip:%s, port:%d", p, user, epSet.epSet.eps[0].fqdn, epSet.epSet.eps[0].port);

    pInst = &p;

    clientSlowQueryMonitorInit(p->instKey);
    clientSQLReqMonitorInit(p->instKey);
  } else {
    ASSERTS((*pInst) && (*pInst)->pAppHbMgr, "*pInst:%p, pAppHgMgr:%p", *pInst, (*pInst) ? (*pInst)->pAppHbMgr : NULL);
    // reset to 0 in case of conn with duplicated user key but its user has ever been dropped.
    atomic_store_8(&(*pInst)->pAppHbMgr->connHbFlag, 0);
  }

  taosThreadMutexUnlock(&appInfo.mutex);

  taosMemoryFreeClear(key);

  return taosConnectImpl(user, &secretEncrypt[0], localDb, NULL, NULL, *pInst, connType);
}

SAppInstInfo* getAppInstInfo(const char* clusterKey) {
  SAppInstInfo** ppAppInstInfo = taosHashGet(appInfo.pInstMap, clusterKey, strlen(clusterKey));
  if (ppAppInstInfo != NULL && *ppAppInstInfo != NULL) {
    return *ppAppInstInfo;
  } else {
    return NULL;
  }
}

void freeQueryParam(SSyncQueryParam* param) {
  if (param == NULL) return;
  tsem_destroy(&param->sem);
  taosMemoryFree(param);
}

int32_t buildRequest(uint64_t connId, const char* sql, int sqlLen, void* param, bool validateSql,
                     SRequestObj** pRequest, int64_t reqid) {
  *pRequest = createRequest(connId, TSDB_SQL_SELECT, reqid);
  if (*pRequest == NULL) {
    tscError("failed to malloc sqlObj, %s", sql);
    return terrno;
  }

  (*pRequest)->sqlstr = taosMemoryMalloc(sqlLen + 1);
  if ((*pRequest)->sqlstr == NULL) {
    tscError("0x%" PRIx64 " failed to prepare sql string buffer, %s", (*pRequest)->self, sql);
    destroyRequest(*pRequest);
    *pRequest = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  strntolower((*pRequest)->sqlstr, sql, (int32_t)sqlLen);
  (*pRequest)->sqlstr[sqlLen] = 0;
  (*pRequest)->sqlLen = sqlLen;
  (*pRequest)->validateOnly = validateSql;

  ((SSyncQueryParam*)(*pRequest)->body.interParam)->userParam = param;

  STscObj* pTscObj = (*pRequest)->pTscObj;
  int32_t  err = taosHashPut(pTscObj->pRequests, &(*pRequest)->self, sizeof((*pRequest)->self), &(*pRequest)->self,
                             sizeof((*pRequest)->self));
  if (err) {
    tscError("%" PRId64 " failed to add to request container, reqId:0x%" PRIx64 ", conn:%" PRId64 ", %s",
             (*pRequest)->self, (*pRequest)->requestId, pTscObj->id, sql);
    destroyRequest(*pRequest);
    *pRequest = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pRequest)->allocatorRefId = -1;
  if (tsQueryUseNodeAllocator && !qIsInsertValuesSql((*pRequest)->sqlstr, (*pRequest)->sqlLen)) {
    if (TSDB_CODE_SUCCESS !=
        nodesCreateAllocator((*pRequest)->requestId, tsQueryNodeChunkSize, &((*pRequest)->allocatorRefId))) {
      tscError("%" PRId64 " failed to create node allocator, reqId:0x%" PRIx64 ", conn:%" PRId64 ", %s",
               (*pRequest)->self, (*pRequest)->requestId, pTscObj->id, sql);
      destroyRequest(*pRequest);
      *pRequest = NULL;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  tscDebugL("0x%" PRIx64 " SQL: %s, reqId:0x%" PRIx64, (*pRequest)->self, (*pRequest)->sqlstr, (*pRequest)->requestId);
  return TSDB_CODE_SUCCESS;
}

int32_t buildPreviousRequest(SRequestObj* pRequest, const char* sql, SRequestObj** pNewRequest) {
  int32_t code =
      buildRequest(pRequest->pTscObj->id, sql, strlen(sql), pRequest, pRequest->validateOnly, pNewRequest, 0);
  if (TSDB_CODE_SUCCESS == code) {
    pRequest->relation.prevRefId = (*pNewRequest)->self;
    (*pNewRequest)->relation.nextRefId = pRequest->self;
    (*pNewRequest)->relation.userRefId = pRequest->self;
    (*pNewRequest)->isSubReq = true;
  }
  return code;
}

int32_t parseSql(SRequestObj* pRequest, bool topicQuery, SQuery** pQuery, SStmtCallback* pStmtCb) {
  STscObj* pTscObj = pRequest->pTscObj;

  SParseContext cxt = {.requestId = pRequest->requestId,
                       .requestRid = pRequest->self,
                       .acctId = pTscObj->acctId,
                       .db = pRequest->pDb,
                       .topicQuery = topicQuery,
                       .pSql = pRequest->sqlstr,
                       .sqlLen = pRequest->sqlLen,
                       .pMsg = pRequest->msgBuf,
                       .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
                       .pTransporter = pTscObj->pAppInfo->pTransporter,
                       .pStmtCb = pStmtCb,
                       .pUser = pTscObj->user,
                       .isSuperUser = (0 == strcmp(pTscObj->user, TSDB_DEFAULT_USER)),
                       .enableSysInfo = pTscObj->sysInfo,
                       .svrVer = pTscObj->sVer,
                       .nodeOffline = (pTscObj->pAppInfo->onlineDnodes < pTscObj->pAppInfo->totalDnodes)};

  cxt.mgmtEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  int32_t code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &cxt.pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = qParseSql(&cxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    if ((*pQuery)->haveResultSet) {
      setResSchemaInfo(&pRequest->body.resInfo, (*pQuery)->pResSchema, (*pQuery)->numOfResCols);
      setResPrecision(&pRequest->body.resInfo, (*pQuery)->precision);
    }
  }

  if (TSDB_CODE_SUCCESS == code || NEED_CLIENT_HANDLE_ERROR(code)) {
    TSWAP(pRequest->dbList, (*pQuery)->pDbList);
    TSWAP(pRequest->tableList, (*pQuery)->pTableList);
    TSWAP(pRequest->targetTableList, (*pQuery)->pTargetTableList);
  }

  taosArrayDestroy(cxt.pTableMetaPos);
  taosArrayDestroy(cxt.pTableVgroupPos);

  return code;
}

int32_t execLocalCmd(SRequestObj* pRequest, SQuery* pQuery) {
  SRetrieveTableRsp* pRsp = NULL;
  int8_t             biMode = atomic_load_8(&pRequest->pTscObj->biMode);
  int32_t code = qExecCommand(&pRequest->pTscObj->id, pRequest->pTscObj->sysInfo, pQuery->pRoot, &pRsp, biMode);
  if (TSDB_CODE_SUCCESS == code && NULL != pRsp) {
    code = setQueryResultFromRsp(&pRequest->body.resInfo, pRsp, false, true);
  }

  return code;
}

int32_t execDdlQuery(SRequestObj* pRequest, SQuery* pQuery) {
  // drop table if exists not_exists_table
  if (NULL == pQuery->pCmdMsg) {
    return TSDB_CODE_SUCCESS;
  }

  SCmdMsgInfo* pMsgInfo = pQuery->pCmdMsg;
  pRequest->type = pMsgInfo->msgType;
  pRequest->body.requestMsg = (SDataBuf){.pData = pMsgInfo->pMsg, .len = pMsgInfo->msgLen, .handle = NULL};
  pMsgInfo->pMsg = NULL;  // pMsg transferred to SMsgSendInfo management

  STscObj*      pTscObj = pRequest->pTscObj;
  SMsgSendInfo* pSendMsg = buildMsgInfoImpl(pRequest);

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &pMsgInfo->epSet, &transporterId, pSendMsg);

  tsem_wait(&pRequest->body.rspSem);
  return TSDB_CODE_SUCCESS;
}

static SAppInstInfo* getAppInfo(SRequestObj* pRequest) { return pRequest->pTscObj->pAppInfo; }

void asyncExecLocalCmd(SRequestObj* pRequest, SQuery* pQuery) {
  SRetrieveTableRsp* pRsp = NULL;
  if (pRequest->validateOnly) {
    doRequestCallback(pRequest, 0);
    return;
  }

  int32_t code = qExecCommand(&pRequest->pTscObj->id, pRequest->pTscObj->sysInfo, pQuery->pRoot, &pRsp,
                              atomic_load_8(&pRequest->pTscObj->biMode));
  if (TSDB_CODE_SUCCESS == code && NULL != pRsp) {
    code = setQueryResultFromRsp(&pRequest->body.resInfo, pRsp, false, true);
  }

  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;
  pRequest->code = code;

  if (pRequest->code != TSDB_CODE_SUCCESS) {
    pResultInfo->numOfRows = 0;
    tscError("0x%" PRIx64 " fetch results failed, code:%s, reqId:0x%" PRIx64, pRequest->self, tstrerror(code),
             pRequest->requestId);
  } else {
    tscDebug("0x%" PRIx64 " fetch results, numOfRows:%" PRId64 " total Rows:%" PRId64 ", complete:%d, reqId:0x%" PRIx64,
             pRequest->self, pResultInfo->numOfRows, pResultInfo->totalRows, pResultInfo->completed,
             pRequest->requestId);
  }

  doRequestCallback(pRequest, code);
}

int32_t asyncExecDdlQuery(SRequestObj* pRequest, SQuery* pQuery) {
  if (pRequest->validateOnly) {
    doRequestCallback(pRequest, 0);
    return TSDB_CODE_SUCCESS;
  }

  // drop table if exists not_exists_table
  if (NULL == pQuery->pCmdMsg) {
    doRequestCallback(pRequest, 0);
    return TSDB_CODE_SUCCESS;
  }

  SCmdMsgInfo* pMsgInfo = pQuery->pCmdMsg;
  pRequest->type = pMsgInfo->msgType;
  pRequest->body.requestMsg = (SDataBuf){.pData = pMsgInfo->pMsg, .len = pMsgInfo->msgLen, .handle = NULL};
  pMsgInfo->pMsg = NULL;  // pMsg transferred to SMsgSendInfo management

  SAppInstInfo* pAppInfo = getAppInfo(pRequest);
  SMsgSendInfo* pSendMsg = buildMsgInfoImpl(pRequest);

  int64_t transporterId = 0;
  int32_t code = asyncSendMsgToServer(pAppInfo->pTransporter, &pMsgInfo->epSet, &transporterId, pSendMsg);
  if (code) {
    doRequestCallback(pRequest, code);
  }
  return code;
}

int compareQueryNodeLoad(const void* elem1, const void* elem2) {
  SQueryNodeLoad* node1 = (SQueryNodeLoad*)elem1;
  SQueryNodeLoad* node2 = (SQueryNodeLoad*)elem2;

  if (node1->load < node2->load) {
    return -1;
  }

  return node1->load > node2->load;
}

int32_t updateQnodeList(SAppInstInfo* pInfo, SArray* pNodeList) {
  taosThreadMutexLock(&pInfo->qnodeMutex);
  if (pInfo->pQnodeList) {
    taosArrayDestroy(pInfo->pQnodeList);
    pInfo->pQnodeList = NULL;
    tscDebug("QnodeList cleared in cluster 0x%" PRIx64, pInfo->clusterId);
  }

  if (pNodeList) {
    pInfo->pQnodeList = taosArrayDup(pNodeList, NULL);
    taosArraySort(pInfo->pQnodeList, compareQueryNodeLoad);
    tscDebug("QnodeList updated in cluster 0x%" PRIx64 ", num:%ld", pInfo->clusterId,
             taosArrayGetSize(pInfo->pQnodeList));
  }
  taosThreadMutexUnlock(&pInfo->qnodeMutex);

  return TSDB_CODE_SUCCESS;
}

bool qnodeRequired(SRequestObj* pRequest) {
  if (QUERY_POLICY_VNODE == tsQueryPolicy || QUERY_POLICY_CLIENT == tsQueryPolicy) {
    return false;
  }

  SAppInstInfo* pInfo = pRequest->pTscObj->pAppInfo;
  bool          required = false;

  taosThreadMutexLock(&pInfo->qnodeMutex);
  required = (NULL == pInfo->pQnodeList);
  taosThreadMutexUnlock(&pInfo->qnodeMutex);

  return required;
}

int32_t getQnodeList(SRequestObj* pRequest, SArray** pNodeList) {
  SAppInstInfo* pInfo = pRequest->pTscObj->pAppInfo;
  int32_t       code = 0;

  taosThreadMutexLock(&pInfo->qnodeMutex);
  if (pInfo->pQnodeList) {
    *pNodeList = taosArrayDup(pInfo->pQnodeList, NULL);
  }
  taosThreadMutexUnlock(&pInfo->qnodeMutex);

  if (NULL == *pNodeList) {
    SCatalog* pCatalog = NULL;
    code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
    if (TSDB_CODE_SUCCESS == code) {
      *pNodeList = taosArrayInit(5, sizeof(SQueryNodeLoad));
      SRequestConnInfo conn = {.pTrans = pRequest->pTscObj->pAppInfo->pTransporter,
                               .requestId = pRequest->requestId,
                               .requestObjRefId = pRequest->self,
                               .mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp)};
      code = catalogGetQnodeList(pCatalog, &conn, *pNodeList);
    }

    if (TSDB_CODE_SUCCESS == code && *pNodeList) {
      code = updateQnodeList(pInfo, *pNodeList);
    }
  }

  return code;
}

int32_t getPlan(SRequestObj* pRequest, SQuery* pQuery, SQueryPlan** pPlan, SArray* pNodeList) {
  pRequest->type = pQuery->msgType;
  SAppInstInfo* pAppInfo = getAppInfo(pRequest);

  SPlanContext cxt = {.queryId = pRequest->requestId,
                      .acctId = pRequest->pTscObj->acctId,
                      .mgmtEpSet = getEpSet_s(&pAppInfo->mgmtEp),
                      .pAstRoot = pQuery->pRoot,
                      .showRewrite = pQuery->showRewrite,
                      .pMsg = pRequest->msgBuf,
                      .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
                      .pUser = pRequest->pTscObj->user,
                      .sysInfo = pRequest->pTscObj->sysInfo};

  return qCreateQueryPlan(&cxt, pPlan, pNodeList);
}

void setResSchemaInfo(SReqResultInfo* pResInfo, const SSchema* pSchema, int32_t numOfCols) {
  if (pResInfo == NULL || pSchema == NULL || numOfCols <= 0) {
    tscError("invalid paras, pResInfo == NULL || pSchema == NULL || numOfCols <= 0");
    return;
  }

  pResInfo->numOfCols = numOfCols;
  if (pResInfo->fields != NULL) {
    taosMemoryFree(pResInfo->fields);
  }
  if (pResInfo->userFields != NULL) {
    taosMemoryFree(pResInfo->userFields);
  }
  pResInfo->fields = taosMemoryCalloc(numOfCols, sizeof(TAOS_FIELD));
  pResInfo->userFields = taosMemoryCalloc(numOfCols, sizeof(TAOS_FIELD));
  if (numOfCols != pResInfo->numOfCols) {
    tscError("numOfCols:%d != pResInfo->numOfCols:%d", numOfCols, pResInfo->numOfCols);
    return;
  }

  for (int32_t i = 0; i < pResInfo->numOfCols; ++i) {
    pResInfo->fields[i].bytes = pSchema[i].bytes;
    pResInfo->fields[i].type = pSchema[i].type;

    pResInfo->userFields[i].bytes = pSchema[i].bytes;
    pResInfo->userFields[i].type = pSchema[i].type;

    if (pSchema[i].type == TSDB_DATA_TYPE_VARCHAR || pSchema[i].type == TSDB_DATA_TYPE_VARBINARY ||
        pSchema[i].type == TSDB_DATA_TYPE_GEOMETRY) {
      pResInfo->userFields[i].bytes -= VARSTR_HEADER_SIZE;
    } else if (pSchema[i].type == TSDB_DATA_TYPE_NCHAR || pSchema[i].type == TSDB_DATA_TYPE_JSON) {
      pResInfo->userFields[i].bytes = (pResInfo->userFields[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
    }

    tstrncpy(pResInfo->fields[i].name, pSchema[i].name, tListLen(pResInfo->fields[i].name));
    tstrncpy(pResInfo->userFields[i].name, pSchema[i].name, tListLen(pResInfo->userFields[i].name));
  }
}

void setResPrecision(SReqResultInfo* pResInfo, int32_t precision) {
  if (precision != TSDB_TIME_PRECISION_MILLI && precision != TSDB_TIME_PRECISION_MICRO &&
      precision != TSDB_TIME_PRECISION_NANO) {
    return;
  }

  pResInfo->precision = precision;
}

int32_t buildVnodePolicyNodeList(SRequestObj* pRequest, SArray** pNodeList, SArray* pMnodeList, SArray* pDbVgList) {
  SArray* nodeList = taosArrayInit(4, sizeof(SQueryNodeLoad));
  char*   policy = (tsQueryPolicy == QUERY_POLICY_VNODE) ? "vnode" : "client";

  int32_t dbNum = taosArrayGetSize(pDbVgList);
  for (int32_t i = 0; i < dbNum; ++i) {
    SArray* pVg = taosArrayGetP(pDbVgList, i);
    int32_t vgNum = taosArrayGetSize(pVg);
    if (vgNum <= 0) {
      continue;
    }

    for (int32_t j = 0; j < vgNum; ++j) {
      SVgroupInfo*   pInfo = taosArrayGet(pVg, j);
      SQueryNodeLoad load = {0};
      load.addr.nodeId = pInfo->vgId;
      load.addr.epSet = pInfo->epSet;

      taosArrayPush(nodeList, &load);
    }
  }

  int32_t vnodeNum = taosArrayGetSize(nodeList);
  if (vnodeNum > 0) {
    tscDebug("0x%" PRIx64 " %s policy, use vnode list, num:%d", pRequest->requestId, policy, vnodeNum);
    goto _return;
  }

  int32_t mnodeNum = taosArrayGetSize(pMnodeList);
  if (mnodeNum <= 0) {
    tscDebug("0x%" PRIx64 " %s policy, empty node list", pRequest->requestId, policy);
    goto _return;
  }

  void* pData = taosArrayGet(pMnodeList, 0);
  taosArrayAddBatch(nodeList, pData, mnodeNum);

  tscDebug("0x%" PRIx64 " %s policy, use mnode list, num:%d", pRequest->requestId, policy, mnodeNum);

_return:

  *pNodeList = nodeList;

  return TSDB_CODE_SUCCESS;
}

int32_t buildQnodePolicyNodeList(SRequestObj* pRequest, SArray** pNodeList, SArray* pMnodeList, SArray* pQnodeList) {
  SArray* nodeList = taosArrayInit(4, sizeof(SQueryNodeLoad));

  int32_t qNodeNum = taosArrayGetSize(pQnodeList);
  if (qNodeNum > 0) {
    void* pData = taosArrayGet(pQnodeList, 0);
    taosArrayAddBatch(nodeList, pData, qNodeNum);
    tscDebug("0x%" PRIx64 " qnode policy, use qnode list, num:%d", pRequest->requestId, qNodeNum);
    goto _return;
  }

  int32_t mnodeNum = taosArrayGetSize(pMnodeList);
  if (mnodeNum <= 0) {
    tscDebug("0x%" PRIx64 " qnode policy, empty node list", pRequest->requestId);
    goto _return;
  }

  void* pData = taosArrayGet(pMnodeList, 0);
  taosArrayAddBatch(nodeList, pData, mnodeNum);

  tscDebug("0x%" PRIx64 " qnode policy, use mnode list, num:%d", pRequest->requestId, mnodeNum);

_return:

  *pNodeList = nodeList;

  return TSDB_CODE_SUCCESS;
}

int32_t buildAsyncExecNodeList(SRequestObj* pRequest, SArray** pNodeList, SArray* pMnodeList, SMetaData* pResultMeta) {
  SArray* pDbVgList = NULL;
  SArray* pQnodeList = NULL;
  int32_t code = 0;

  switch (tsQueryPolicy) {
    case QUERY_POLICY_VNODE:
    case QUERY_POLICY_CLIENT: {
      if (pResultMeta) {
        pDbVgList = taosArrayInit(4, POINTER_BYTES);

        int32_t dbNum = taosArrayGetSize(pResultMeta->pDbVgroup);
        for (int32_t i = 0; i < dbNum; ++i) {
          SMetaRes* pRes = taosArrayGet(pResultMeta->pDbVgroup, i);
          if (pRes->code || NULL == pRes->pRes) {
            continue;
          }

          taosArrayPush(pDbVgList, &pRes->pRes);
        }
      }

      code = buildVnodePolicyNodeList(pRequest, pNodeList, pMnodeList, pDbVgList);
      break;
    }
    case QUERY_POLICY_HYBRID:
    case QUERY_POLICY_QNODE: {
      if (pResultMeta && taosArrayGetSize(pResultMeta->pQnodeList) > 0) {
        SMetaRes* pRes = taosArrayGet(pResultMeta->pQnodeList, 0);
        if (pRes->code) {
          pQnodeList = NULL;
        } else {
          pQnodeList = taosArrayDup((SArray*)pRes->pRes, NULL);
        }
      } else {
        SAppInstInfo* pInst = pRequest->pTscObj->pAppInfo;
        taosThreadMutexLock(&pInst->qnodeMutex);
        if (pInst->pQnodeList) {
          pQnodeList = taosArrayDup(pInst->pQnodeList, NULL);
        }
        taosThreadMutexUnlock(&pInst->qnodeMutex);
      }

      code = buildQnodePolicyNodeList(pRequest, pNodeList, pMnodeList, pQnodeList);
      break;
    }
    default:
      tscError("unknown query policy: %d", tsQueryPolicy);
      return TSDB_CODE_APP_ERROR;
  }

  taosArrayDestroy(pDbVgList);
  taosArrayDestroy(pQnodeList);

  return code;
}

void freeVgList(void* list) {
  SArray* pList = *(SArray**)list;
  taosArrayDestroy(pList);
}

int32_t buildSyncExecNodeList(SRequestObj* pRequest, SArray** pNodeList, SArray* pMnodeList) {
  SArray* pDbVgList = NULL;
  SArray* pQnodeList = NULL;
  int32_t code = 0;

  switch (tsQueryPolicy) {
    case QUERY_POLICY_VNODE:
    case QUERY_POLICY_CLIENT: {
      int32_t dbNum = taosArrayGetSize(pRequest->dbList);
      if (dbNum > 0) {
        SCatalog*     pCtg = NULL;
        SAppInstInfo* pInst = pRequest->pTscObj->pAppInfo;
        code = catalogGetHandle(pInst->clusterId, &pCtg);
        if (code != TSDB_CODE_SUCCESS) {
          goto _return;
        }

        pDbVgList = taosArrayInit(dbNum, POINTER_BYTES);
        SArray* pVgList = NULL;
        for (int32_t i = 0; i < dbNum; ++i) {
          char*            dbFName = taosArrayGet(pRequest->dbList, i);
          SRequestConnInfo conn = {.pTrans = pInst->pTransporter,
                                   .requestId = pRequest->requestId,
                                   .requestObjRefId = pRequest->self,
                                   .mgmtEps = getEpSet_s(&pInst->mgmtEp)};

          code = catalogGetDBVgList(pCtg, &conn, dbFName, &pVgList);
          if (code) {
            goto _return;
          }

          taosArrayPush(pDbVgList, &pVgList);
        }
      }

      code = buildVnodePolicyNodeList(pRequest, pNodeList, pMnodeList, pDbVgList);
      break;
    }
    case QUERY_POLICY_HYBRID:
    case QUERY_POLICY_QNODE: {
      getQnodeList(pRequest, &pQnodeList);

      code = buildQnodePolicyNodeList(pRequest, pNodeList, pMnodeList, pQnodeList);
      break;
    }
    default:
      tscError("unknown query policy: %d", tsQueryPolicy);
      return TSDB_CODE_APP_ERROR;
  }

_return:

  taosArrayDestroyEx(pDbVgList, freeVgList);
  taosArrayDestroy(pQnodeList);

  return code;
}

int32_t scheduleQuery(SRequestObj* pRequest, SQueryPlan* pDag, SArray* pNodeList) {
  void* pTransporter = pRequest->pTscObj->pAppInfo->pTransporter;

  SExecResult      res = {0};
  SRequestConnInfo conn = {.pTrans = pRequest->pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self};
  SSchedulerReq    req = {
         .syncReq = true,
         .localReq = (tsQueryPolicy == QUERY_POLICY_CLIENT),
         .pConn = &conn,
         .pNodeList = pNodeList,
         .pDag = pDag,
         .sql = pRequest->sqlstr,
         .startTs = pRequest->metric.start,
         .execFp = NULL,
         .cbParam = NULL,
         .chkKillFp = chkRequestKilled,
         .chkKillParam = (void*)pRequest->self,
         .pExecRes = &res,
  };

  int32_t code = schedulerExecJob(&req, &pRequest->body.queryJob);

  destroyQueryExecRes(&pRequest->body.resInfo.execRes);
  memcpy(&pRequest->body.resInfo.execRes, &res, sizeof(res));

  if (code != TSDB_CODE_SUCCESS) {
    schedulerFreeJob(&pRequest->body.queryJob, 0);

    pRequest->code = code;
    terrno = code;
    return pRequest->code;
  }

  if (TDMT_VND_SUBMIT == pRequest->type || TDMT_VND_DELETE == pRequest->type ||
      TDMT_VND_CREATE_TABLE == pRequest->type) {
    pRequest->body.resInfo.numOfRows = res.numOfRows;
    if (TDMT_VND_SUBMIT == pRequest->type) {
      STscObj*            pTscObj = pRequest->pTscObj;
      SAppClusterSummary* pActivity = &pTscObj->pAppInfo->summary;
      atomic_add_fetch_64((int64_t*)&pActivity->numOfInsertRows, res.numOfRows);
    }

    schedulerFreeJob(&pRequest->body.queryJob, 0);
  }

  pRequest->code = res.code;
  terrno = res.code;
  return pRequest->code;
}

int32_t handleSubmitExecRes(SRequestObj* pRequest, void* res, SCatalog* pCatalog, SEpSet* epset) {
  SArray*      pArray = NULL;
  SSubmitRsp2* pRsp = (SSubmitRsp2*)res;
  if (NULL == pRsp->aCreateTbRsp) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t tbNum = taosArrayGetSize(pRsp->aCreateTbRsp);
  for (int32_t i = 0; i < tbNum; ++i) {
    SVCreateTbRsp* pTbRsp = (SVCreateTbRsp*)taosArrayGet(pRsp->aCreateTbRsp, i);
    if (pTbRsp->pMeta) {
      handleCreateTbExecRes(pTbRsp->pMeta, pCatalog);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t handleQueryExecRes(SRequestObj* pRequest, void* res, SCatalog* pCatalog, SEpSet* epset) {
  int32_t code = 0;
  SArray* pArray = NULL;
  SArray* pTbArray = (SArray*)res;
  int32_t tbNum = taosArrayGetSize(pTbArray);
  if (tbNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  pArray = taosArrayInit(tbNum, sizeof(STbSVersion));
  if (NULL == pArray) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < tbNum; ++i) {
    STbVerInfo* tbInfo = taosArrayGet(pTbArray, i);
    STbSVersion tbSver = {.tbFName = tbInfo->tbFName, .sver = tbInfo->sversion, .tver = tbInfo->tversion};
    taosArrayPush(pArray, &tbSver);
  }

  SRequestConnInfo conn = {.pTrans = pRequest->pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = *epset};

  code = catalogChkTbMetaVersion(pCatalog, &conn, pArray);

_return:

  taosArrayDestroy(pArray);
  return code;
}

int32_t handleAlterTbExecRes(void* res, SCatalog* pCatalog) {
  return catalogUpdateTableMeta(pCatalog, (STableMetaRsp*)res);
}

int32_t handleCreateTbExecRes(void* res, SCatalog* pCatalog) {
  return catalogAsyncUpdateTableMeta(pCatalog, (STableMetaRsp*)res);
}

int32_t handleQueryExecRsp(SRequestObj* pRequest) {
  if (NULL == pRequest->body.resInfo.execRes.res) {
    return pRequest->code;
  }

  SCatalog*     pCatalog = NULL;
  SAppInstInfo* pAppInfo = getAppInfo(pRequest);

  int32_t code = catalogGetHandle(pAppInfo->clusterId, &pCatalog);
  if (code) {
    return code;
  }

  SEpSet       epset = getEpSet_s(&pAppInfo->mgmtEp);
  SExecResult* pRes = &pRequest->body.resInfo.execRes;

  switch (pRes->msgType) {
    case TDMT_VND_ALTER_TABLE:
    case TDMT_MND_ALTER_STB: {
      code = handleAlterTbExecRes(pRes->res, pCatalog);
      break;
    }
    case TDMT_VND_CREATE_TABLE: {
      SArray* pList = (SArray*)pRes->res;
      int32_t num = taosArrayGetSize(pList);
      for (int32_t i = 0; i < num; ++i) {
        void* res = taosArrayGetP(pList, i);
        code = handleCreateTbExecRes(res, pCatalog);
      }
      break;
    }
    case TDMT_MND_CREATE_STB: {
      code = handleCreateTbExecRes(pRes->res, pCatalog);
      break;
    }
    case TDMT_VND_SUBMIT: {
      atomic_add_fetch_64((int64_t*)&pAppInfo->summary.insertBytes, pRes->numOfBytes);

      code = handleSubmitExecRes(pRequest, pRes->res, pCatalog, &epset);
      break;
    }
    case TDMT_SCH_QUERY:
    case TDMT_SCH_MERGE_QUERY: {
      code = handleQueryExecRes(pRequest, pRes->res, pCatalog, &epset);
      break;
    }
    default:
      tscError("0x%" PRIx64 ", invalid exec result for request type %d, reqId:0x%" PRIx64, pRequest->self,
               pRequest->type, pRequest->requestId);
      code = TSDB_CODE_APP_ERROR;
  }

  return code;
}

static bool incompletaFileParsing(SNode* pStmt) {
  return QUERY_NODE_VNODE_MODIFY_STMT != nodeType(pStmt) ? false : ((SVnodeModifyOpStmt*)pStmt)->fileProcessing;
}

void continuePostSubQuery(SRequestObj* pRequest, SSDataBlock* pBlock) {
  SSqlCallbackWrapper* pWrapper = pRequest->pWrapper;

  int32_t code = nodesAcquireAllocator(pWrapper->pParseCtx->allocatorId);
  if (TSDB_CODE_SUCCESS == code) {
    int64_t analyseStart = taosGetTimestampUs();
    code = qContinueParsePostQuery(pWrapper->pParseCtx, pRequest->pQuery, pBlock);
    pRequest->metric.analyseCostUs += taosGetTimestampUs() - analyseStart;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = qContinuePlanPostQuery(pRequest->pPostPlan);
  }

  nodesReleaseAllocator(pWrapper->pParseCtx->allocatorId);
  handleQueryAnslyseRes(pWrapper, NULL, code);
}

void returnToUser(SRequestObj* pRequest) {
  if (pRequest->relation.userRefId == pRequest->self || 0 == pRequest->relation.userRefId) {
    // return to client
    doRequestCallback(pRequest, pRequest->code);
    return;
  }

  SRequestObj* pUserReq = acquireRequest(pRequest->relation.userRefId);
  if (pUserReq) {
    pUserReq->code = pRequest->code;
    // return to client
    doRequestCallback(pUserReq, pUserReq->code);
    releaseRequest(pRequest->relation.userRefId);
    return;
  } else {
    tscError("0x%" PRIx64 ", user ref 0x%" PRIx64 " is not there, reqId:0x%" PRIx64, pRequest->self,
             pRequest->relation.userRefId, pRequest->requestId);
  }
}

static SSDataBlock* createResultBlock(TAOS_RES* pRes, int32_t numOfRows) {
  int64_t lastTs = 0;

  TAOS_FIELD* pResFields = taos_fetch_fields(pRes);
  int32_t numOfFields = taos_num_fields(pRes);

  SSDataBlock* pBlock = createDataBlock();

  for(int32_t i = 0; i < numOfFields; ++i) {
    SColumnInfoData colInfoData = createColumnInfoData(pResFields[i].type, pResFields[i].bytes, i + 1);
    blockDataAppendColInfo(pBlock, &colInfoData);
  }

  blockDataEnsureCapacity(pBlock, numOfRows);

  for (int32_t i = 0; i < numOfRows; ++i) {
    TAOS_ROW pRow = taos_fetch_row(pRes);
    int64_t  ts = *(int64_t*)pRow[0];
    if (lastTs < ts) {
      lastTs = ts;
    }

    for(int32_t j = 0; j < numOfFields; ++j) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, j);
      colDataSetVal(pColInfoData, i, pRow[j], false);
    }

    tscDebug("lastKey:%" PRId64 " vgId:%d, vgVer:%" PRId64, ts, *(int32_t*)pRow[1], *(int64_t*)pRow[2]);
  }

  pBlock->info.window.ekey = lastTs;
  pBlock->info.rows = numOfRows;

  tscDebug("lastKey:%"PRId64" numOfRows:%d from all vgroups", lastTs, numOfRows);
  return pBlock;
}

void postSubQueryFetchCb(void* param, TAOS_RES* res, int32_t rowNum) {
  SRequestObj* pRequest = (SRequestObj*)res;
  if (pRequest->code) {
    returnToUser(pRequest);
    return;
  }

  SSDataBlock* pBlock = createResultBlock(res, rowNum);
  SRequestObj* pNextReq = acquireRequest(pRequest->relation.nextRefId);
  if (pNextReq) {
    continuePostSubQuery(pNextReq, pBlock);
    releaseRequest(pRequest->relation.nextRefId);
  } else {
    tscError("0x%" PRIx64 ", next req ref 0x%" PRIx64 " is not there, reqId:0x%" PRIx64, pRequest->self,
             pRequest->relation.nextRefId, pRequest->requestId);
  }

  blockDataDestroy(pBlock);
}

void handlePostSubQuery(SSqlCallbackWrapper* pWrapper) {
  SRequestObj* pRequest = pWrapper->pRequest;
  if (TD_RES_QUERY(pRequest)) {
    taosAsyncFetchImpl(pRequest, postSubQueryFetchCb, pWrapper);
    return;
  }

  SRequestObj* pNextReq = acquireRequest(pRequest->relation.nextRefId);
  if (pNextReq) {
    continuePostSubQuery(pNextReq, NULL);
    releaseRequest(pRequest->relation.nextRefId);
  } else {
    tscError("0x%" PRIx64 ", next req ref 0x%" PRIx64 " is not there, reqId:0x%" PRIx64, pRequest->self,
             pRequest->relation.nextRefId, pRequest->requestId);
  }
}

// todo refacto the error code  mgmt
void schedulerExecCb(SExecResult* pResult, void* param, int32_t code) {
  SSqlCallbackWrapper* pWrapper = param;
  SRequestObj*         pRequest = pWrapper->pRequest;
  STscObj*             pTscObj = pRequest->pTscObj;

  pRequest->code = code;
  if (pResult) {
    destroyQueryExecRes(&pRequest->body.resInfo.execRes);
    memcpy(&pRequest->body.resInfo.execRes, pResult, sizeof(*pResult));
  }

  int32_t type = pRequest->type;
  if (TDMT_VND_SUBMIT == type || TDMT_VND_DELETE == type || TDMT_VND_CREATE_TABLE == type) {
    if (pResult) {
      pRequest->body.resInfo.numOfRows += pResult->numOfRows;

      // record the insert rows
      if (TDMT_VND_SUBMIT == type) {
        SAppClusterSummary* pActivity = &pTscObj->pAppInfo->summary;
        atomic_add_fetch_64((int64_t*)&pActivity->numOfInsertRows, pResult->numOfRows);
      }
    }

    schedulerFreeJob(&pRequest->body.queryJob, 0);
  }

  taosMemoryFree(pResult);
  tscDebug("0x%" PRIx64 " enter scheduler exec cb, code:%s, reqId:0x%" PRIx64, pRequest->self, tstrerror(code),
           pRequest->requestId);

  if (code != TSDB_CODE_SUCCESS && NEED_CLIENT_HANDLE_ERROR(code) && pRequest->sqlstr != NULL) {
    tscDebug("0x%" PRIx64 " client retry to handle the error, code:%s, tryCount:%d, reqId:0x%" PRIx64, pRequest->self,
             tstrerror(code), pRequest->retry, pRequest->requestId);
    restartAsyncQuery(pRequest, code);
    return;
  }

  tscDebug("schedulerExecCb request type %s", TMSG_INFO(pRequest->type));
  if (NEED_CLIENT_RM_TBLMETA_REQ(pRequest->type) && NULL == pRequest->body.resInfo.execRes.res) {
    removeMeta(pTscObj, pRequest->targetTableList, IS_VIEW_REQUEST(pRequest->type));
  }

  pRequest->metric.execCostUs = taosGetTimestampUs() - pRequest->metric.execStart;
  int32_t code1 = handleQueryExecRsp(pRequest);
  if (pRequest->code == TSDB_CODE_SUCCESS && pRequest->code != code1) {
    pRequest->code = code1;
  }

  if (pRequest->code == TSDB_CODE_SUCCESS && NULL != pRequest->pQuery &&
      incompletaFileParsing(pRequest->pQuery->pRoot)) {
    continueInsertFromCsv(pWrapper, pRequest);
    return;
  }

  if (pRequest->relation.nextRefId) {
    handlePostSubQuery(pWrapper);
  } else {
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;

    // return to client
    doRequestCallback(pRequest, code);
  }
}

SRequestObj* launchQueryImpl(SRequestObj* pRequest, SQuery* pQuery, bool keepQuery, void** res) {
  int32_t code = 0;

  if (pQuery->pRoot) {
    pRequest->stmtType = pQuery->pRoot->type;
  }

  if (pQuery->pRoot && !pRequest->inRetry) {
    STscObj*            pTscObj = pRequest->pTscObj;
    SAppClusterSummary* pActivity = &pTscObj->pAppInfo->summary;
    if (QUERY_NODE_VNODE_MODIFY_STMT == pQuery->pRoot->type) {
      atomic_add_fetch_64((int64_t*)&pActivity->numOfInsertsReq, 1);
    } else if (QUERY_NODE_SELECT_STMT == pQuery->pRoot->type) {
      atomic_add_fetch_64((int64_t*)&pActivity->numOfQueryReq, 1);
    }
  }

  pRequest->body.execMode = pQuery->execMode;
  switch (pQuery->execMode) {
    case QUERY_EXEC_MODE_LOCAL:
      if (!pRequest->validateOnly) {
        if (NULL == pQuery->pRoot) {
          terrno = TSDB_CODE_INVALID_PARA;
          code = terrno;
        } else {
          code = execLocalCmd(pRequest, pQuery);
        }
      }
      break;
    case QUERY_EXEC_MODE_RPC:
      if (!pRequest->validateOnly) {
        code = execDdlQuery(pRequest, pQuery);
      }
      break;
    case QUERY_EXEC_MODE_SCHEDULE: {
      SArray*     pMnodeList = taosArrayInit(4, sizeof(SQueryNodeLoad));
      SQueryPlan* pDag = NULL;
      code = getPlan(pRequest, pQuery, &pDag, pMnodeList);
      if (TSDB_CODE_SUCCESS == code) {
        pRequest->body.subplanNum = pDag->numOfSubplans;
        if (!pRequest->validateOnly) {
          SArray* pNodeList = NULL;
          buildSyncExecNodeList(pRequest, &pNodeList, pMnodeList);

          code = scheduleQuery(pRequest, pDag, pNodeList);
          taosArrayDestroy(pNodeList);
        }
      }
      taosArrayDestroy(pMnodeList);
      break;
    }
    case QUERY_EXEC_MODE_EMPTY_RESULT:
      pRequest->type = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      break;
    default:
      break;
  }

  if (!keepQuery) {
    qDestroyQuery(pQuery);
  }

  if (NEED_CLIENT_RM_TBLMETA_REQ(pRequest->type) && NULL == pRequest->body.resInfo.execRes.res) {
    removeMeta(pRequest->pTscObj, pRequest->targetTableList, IS_VIEW_REQUEST(pRequest->type));
  }

  handleQueryExecRsp(pRequest);

  if (TSDB_CODE_SUCCESS != code) {
    pRequest->code = terrno;
  }

  if (res) {
    *res = pRequest->body.resInfo.execRes.res;
    pRequest->body.resInfo.execRes.res = NULL;
  }

  return pRequest;
}

static int32_t asyncExecSchQuery(SRequestObj* pRequest, SQuery* pQuery, SMetaData* pResultMeta,
                                 SSqlCallbackWrapper* pWrapper) {
  int32_t code = TSDB_CODE_SUCCESS;
  pRequest->type = pQuery->msgType;
  SArray*     pMnodeList = NULL;
  SQueryPlan* pDag = NULL;
  int64_t     st = taosGetTimestampUs();

  if (!pRequest->parseOnly) {
    pMnodeList = taosArrayInit(4, sizeof(SQueryNodeLoad));

    SPlanContext cxt = {.queryId = pRequest->requestId,
                        .acctId = pRequest->pTscObj->acctId,
                        .mgmtEpSet = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp),
                        .pAstRoot = pQuery->pRoot,
                        .showRewrite = pQuery->showRewrite,
                        .isView = pWrapper->pParseCtx->isView,
                        .isAudit = pWrapper->pParseCtx->isAudit,
                        .pMsg = pRequest->msgBuf,
                        .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
                        .pUser = pRequest->pTscObj->user,
                        .sysInfo = pRequest->pTscObj->sysInfo,
                        .allocatorId = pRequest->allocatorRefId};

    code = qCreateQueryPlan(&cxt, &pDag, pMnodeList);
    if (code) {
      tscError("0x%" PRIx64 " failed to create query plan, code:%s 0x%" PRIx64, pRequest->self, tstrerror(code),
               pRequest->requestId);
    } else {
      pRequest->body.subplanNum = pDag->numOfSubplans;
      TSWAP(pRequest->pPostPlan, pDag->pPostPlan);
    }
  }

  pRequest->metric.execStart = taosGetTimestampUs();
  pRequest->metric.planCostUs = pRequest->metric.execStart - st;

  if (TSDB_CODE_SUCCESS == code && !pRequest->validateOnly) {
    SArray* pNodeList = NULL;
    if (QUERY_NODE_VNODE_MODIFY_STMT != nodeType(pQuery->pRoot)) {
      buildAsyncExecNodeList(pRequest, &pNodeList, pMnodeList, pResultMeta);
    }

    SRequestConnInfo conn = {.pTrans = getAppInfo(pRequest)->pTransporter,
                             .requestId = pRequest->requestId,
                             .requestObjRefId = pRequest->self};
    SSchedulerReq    req = {
           .syncReq = false,
           .localReq = (tsQueryPolicy == QUERY_POLICY_CLIENT),
           .pConn = &conn,
           .pNodeList = pNodeList,
           .pDag = pDag,
           .allocatorRefId = pRequest->allocatorRefId,
           .sql = pRequest->sqlstr,
           .startTs = pRequest->metric.start,
           .execFp = schedulerExecCb,
           .cbParam = pWrapper,
           .chkKillFp = chkRequestKilled,
           .chkKillParam = (void*)pRequest->self,
           .pExecRes = NULL,
    };
    code = schedulerExecJob(&req, &pRequest->body.queryJob);
    taosArrayDestroy(pNodeList);
  } else {
    qDestroyQueryPlan(pDag);
    tscDebug("0x%" PRIx64 " plan not executed, code:%s 0x%" PRIx64, pRequest->self, tstrerror(code),
             pRequest->requestId);
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
    if (TSDB_CODE_SUCCESS != code) {
      pRequest->code = terrno;
    }

    doRequestCallback(pRequest, code);
  }

  // todo not to be released here
  taosArrayDestroy(pMnodeList);

  return code;
}

void launchAsyncQuery(SRequestObj* pRequest, SQuery* pQuery, SMetaData* pResultMeta, SSqlCallbackWrapper* pWrapper) {
  int32_t code = 0;

  if (pRequest->parseOnly) {
    doRequestCallback(pRequest, 0);
    return;
  }

  pRequest->body.execMode = pQuery->execMode;
  if (QUERY_EXEC_MODE_SCHEDULE != pRequest->body.execMode) {
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
  }

  if (pQuery->pRoot && !pRequest->inRetry) {
    STscObj*            pTscObj = pRequest->pTscObj;
    SAppClusterSummary* pActivity = &pTscObj->pAppInfo->summary;
    if (QUERY_NODE_VNODE_MODIFY_STMT == pQuery->pRoot->type &&
        (0 == ((SVnodeModifyOpStmt*)pQuery->pRoot)->sqlNodeType)) {
      atomic_add_fetch_64((int64_t*)&pActivity->numOfInsertsReq, 1);
    } else if (QUERY_NODE_SELECT_STMT == pQuery->pRoot->type) {
      atomic_add_fetch_64((int64_t*)&pActivity->numOfQueryReq, 1);
    }
  }

  switch (pQuery->execMode) {
    case QUERY_EXEC_MODE_LOCAL:
      asyncExecLocalCmd(pRequest, pQuery);
      break;
    case QUERY_EXEC_MODE_RPC:
      code = asyncExecDdlQuery(pRequest, pQuery);
      break;
    case QUERY_EXEC_MODE_SCHEDULE: {
      code = asyncExecSchQuery(pRequest, pQuery, pResultMeta, pWrapper);
      break;
    }
    case QUERY_EXEC_MODE_EMPTY_RESULT:
      pRequest->type = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      doRequestCallback(pRequest, 0);
      break;
    default:
      tscError("0x%" PRIx64 " invalid execMode %d", pRequest->self, pQuery->execMode);
      doRequestCallback(pRequest, -1);
      break;
  }
}

int32_t refreshMeta(STscObj* pTscObj, SRequestObj* pRequest) {
  SCatalog* pCatalog = NULL;
  int32_t   code = 0;
  int32_t   dbNum = taosArrayGetSize(pRequest->dbList);
  int32_t   tblNum = taosArrayGetSize(pRequest->tableList);

  if (dbNum <= 0 && tblNum <= 0) {
    return TSDB_CODE_APP_ERROR;
  }

  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};

  for (int32_t i = 0; i < dbNum; ++i) {
    char* dbFName = taosArrayGet(pRequest->dbList, i);

    code = catalogRefreshDBVgInfo(pCatalog, &conn, dbFName);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  for (int32_t i = 0; i < tblNum; ++i) {
    SName* tableName = taosArrayGet(pRequest->tableList, i);

    code = catalogRefreshTableMeta(pCatalog, &conn, tableName, -1);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return code;
}

int32_t removeMeta(STscObj* pTscObj, SArray* tbList, bool isView) {
  SCatalog* pCatalog = NULL;
  int32_t   tbNum = taosArrayGetSize(tbList);
  int32_t   code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (isView) {
    for (int32_t i = 0; i < tbNum; ++i) {
      SName* pViewName = taosArrayGet(tbList, i);
      char   dbFName[TSDB_DB_FNAME_LEN];
      tNameGetFullDbName(pViewName, dbFName);
      catalogRemoveViewMeta(pCatalog, dbFName, 0, pViewName->tname, 0);
    }
  } else {
    for (int32_t i = 0; i < tbNum; ++i) {
      SName* pTbName = taosArrayGet(tbList, i);
      catalogRemoveTableMeta(pCatalog, pTbName);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int initEpSetFromCfg(const char* firstEp, const char* secondEp, SCorEpSet* pEpSet) {
  pEpSet->version = 0;

  // init mnode ip set
  SEpSet* mgmtEpSet = &(pEpSet->epSet);
  mgmtEpSet->numOfEps = 0;
  mgmtEpSet->inUse = 0;

  if (firstEp && firstEp[0] != 0) {
    if (strlen(firstEp) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }

    int32_t code = taosGetFqdnPortFromEp(firstEp, &mgmtEpSet->eps[mgmtEpSet->numOfEps]);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return terrno;
    }
    uint32_t addr = taosGetIpv4FromFqdn(mgmtEpSet->eps[mgmtEpSet->numOfEps].fqdn);
    if (addr == 0xffffffff) {
      tscError("failed to resolve firstEp fqdn: %s, code:%s", mgmtEpSet->eps[mgmtEpSet->numOfEps].fqdn,
               tstrerror(TSDB_CODE_TSC_INVALID_FQDN));
      memset(&(mgmtEpSet->eps[mgmtEpSet->numOfEps]), 0, sizeof(mgmtEpSet->eps[mgmtEpSet->numOfEps]));
    } else {
      mgmtEpSet->numOfEps++;
    }
  }

  if (secondEp && secondEp[0] != 0) {
    if (strlen(secondEp) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }

    taosGetFqdnPortFromEp(secondEp, &mgmtEpSet->eps[mgmtEpSet->numOfEps]);
    uint32_t addr = taosGetIpv4FromFqdn(mgmtEpSet->eps[mgmtEpSet->numOfEps].fqdn);
    if (addr == 0xffffffff) {
      tscError("failed to resolve secondEp fqdn: %s, code:%s", mgmtEpSet->eps[mgmtEpSet->numOfEps].fqdn,
               tstrerror(TSDB_CODE_TSC_INVALID_FQDN));
      memset(&(mgmtEpSet->eps[mgmtEpSet->numOfEps]), 0, sizeof(mgmtEpSet->eps[mgmtEpSet->numOfEps]));
    } else {
      mgmtEpSet->numOfEps++;
    }
  }

  if (mgmtEpSet->numOfEps == 0) {
    terrno = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    return TSDB_CODE_RPC_NETWORK_UNAVAIL;
  }

  return 0;
}

STscObj* taosConnectImpl(const char* user, const char* auth, const char* db, __taos_async_fn_t fp, void* param,
                         SAppInstInfo* pAppInfo, int connType) {
  STscObj* pTscObj = createTscObj(user, auth, db, connType, pAppInfo);
  if (NULL == pTscObj) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return pTscObj;
  }

  SRequestObj* pRequest = createRequest(pTscObj->id, TDMT_MND_CONNECT, 0);
  if (pRequest == NULL) {
    destroyTscObj(pTscObj);
    return NULL;
  }

  pRequest->sqlstr = taosStrdup("taos_connect");
  if (pRequest->sqlstr) {
    pRequest->sqlLen = strlen(pRequest->sqlstr);
  }

  SMsgSendInfo* body = buildConnectMsg(pRequest);

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &pTscObj->pAppInfo->mgmtEp.epSet, &transporterId, body);

  tsem_wait(&pRequest->body.rspSem);
  if (pRequest->code != TSDB_CODE_SUCCESS) {
    const char* errorMsg =
        (pRequest->code == TSDB_CODE_RPC_FQDN_ERROR) ? taos_errstr(pRequest) : tstrerror(pRequest->code);
    tscError("failed to connect to server, reason: %s", errorMsg);

    terrno = pRequest->code;
    destroyRequest(pRequest);
    taos_close_internal(pTscObj);
    pTscObj = NULL;
  } else {
    tscDebug("0x%" PRIx64 " connection is opening, connId:%u, dnodeConn:%p, reqId:0x%" PRIx64, pTscObj->id,
             pTscObj->connId, pTscObj->pAppInfo->pTransporter, pRequest->requestId);
    destroyRequest(pRequest);
  }

  return pTscObj;
}

static SMsgSendInfo* buildConnectMsg(SRequestObj* pRequest) {
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pMsgSendInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pMsgSendInfo->msgType = TDMT_MND_CONNECT;

  pMsgSendInfo->requestObjRefId = pRequest->self;
  pMsgSendInfo->requestId = pRequest->requestId;
  pMsgSendInfo->fp = getMsgRspHandle(pMsgSendInfo->msgType);
  pMsgSendInfo->param = taosMemoryCalloc(1, sizeof(pRequest->self));

  *(int64_t*)pMsgSendInfo->param = pRequest->self;

  SConnectReq connectReq = {0};
  STscObj*    pObj = pRequest->pTscObj;

  char* db = getDbOfConnection(pObj);
  if (db != NULL) {
    tstrncpy(connectReq.db, db, sizeof(connectReq.db));
  }
  taosMemoryFreeClear(db);

  connectReq.connType = pObj->connType;
  connectReq.pid = appInfo.pid;
  connectReq.startTime = appInfo.startTime;

  tstrncpy(connectReq.app, appInfo.appName, sizeof(connectReq.app));
  tstrncpy(connectReq.user, pObj->user, sizeof(connectReq.user));
  tstrncpy(connectReq.passwd, pObj->pass, sizeof(connectReq.passwd));
  tstrncpy(connectReq.sVer, version, sizeof(connectReq.sVer));

  int32_t contLen = tSerializeSConnectReq(NULL, 0, &connectReq);
  void*   pReq = taosMemoryMalloc(contLen);
  tSerializeSConnectReq(pReq, contLen, &connectReq);

  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgInfo.pData = pReq;
  return pMsgSendInfo;
}

void updateTargetEpSet(SMsgSendInfo* pSendInfo, STscObj* pTscObj, SRpcMsg* pMsg, SEpSet* pEpSet) {
  if (NULL == pEpSet) {
    return;
  }

  switch (pSendInfo->target.type) {
    case TARGET_TYPE_MNODE:
      if (NULL == pTscObj) {
        tscError("mnode epset changed but not able to update it, msg:%s, reqObjRefId:%" PRIx64,
                 TMSG_INFO(pMsg->msgType), pSendInfo->requestObjRefId);
        return;
      }

      SEpSet* pOrig = &pTscObj->pAppInfo->mgmtEp.epSet;
      SEp*    pOrigEp = &pOrig->eps[pOrig->inUse];
      SEp*    pNewEp = &pEpSet->eps[pEpSet->inUse];
      tscDebug("mnode epset updated from %d/%d=>%s:%d to %d/%d=>%s:%d in client", pOrig->inUse, pOrig->numOfEps,
               pOrigEp->fqdn, pOrigEp->port, pEpSet->inUse, pEpSet->numOfEps, pNewEp->fqdn, pNewEp->port);
      updateEpSet_s(&pTscObj->pAppInfo->mgmtEp, pEpSet);
      break;
    case TARGET_TYPE_VNODE: {
      if (NULL == pTscObj) {
        tscError("vnode epset changed but not able to update it, msg:%s, reqObjRefId:%" PRIx64,
                 TMSG_INFO(pMsg->msgType), pSendInfo->requestObjRefId);
        return;
      }

      SCatalog* pCatalog = NULL;
      int32_t   code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
      if (code != TSDB_CODE_SUCCESS) {
        tscError("fail to get catalog handle, clusterId:%" PRIx64 ", error %s", pTscObj->pAppInfo->clusterId,
                 tstrerror(code));
        return;
      }

      catalogUpdateVgEpSet(pCatalog, pSendInfo->target.dbFName, pSendInfo->target.vgId, pEpSet);
      taosMemoryFreeClear(pSendInfo->target.dbFName);
      break;
    }
    default:
      tscDebug("epset changed, not updated, msgType %s", TMSG_INFO(pMsg->msgType));
      break;
  }
}

int32_t doProcessMsgFromServer(void* param) {
  AsyncArg* arg = (AsyncArg*)param;
  SRpcMsg*  pMsg = &arg->msg;
  SEpSet*   pEpSet = arg->pEpset;

  SMsgSendInfo* pSendInfo = (SMsgSendInfo*)pMsg->info.ahandle;
  if (pMsg->info.ahandle == NULL) {
    tscError("doProcessMsgFromServer pMsg->info.ahandle == NULL");
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  STscObj* pTscObj = NULL;

  STraceId* trace = &pMsg->info.traceId;
  char      tbuf[40] = {0};
  TRACE_TO_STR(trace, tbuf);

  tscDebug("processMsgFromServer handle %p, message: %s, size:%d, code: %s, gtid: %s", pMsg->info.handle,
           TMSG_INFO(pMsg->msgType), pMsg->contLen, tstrerror(pMsg->code), tbuf);

  if (pSendInfo->requestObjRefId != 0) {
    SRequestObj* pRequest = (SRequestObj*)taosAcquireRef(clientReqRefPool, pSendInfo->requestObjRefId);
    if (pRequest) {
      if (pRequest->self != pSendInfo->requestObjRefId) {
        tscError("doProcessMsgFromServer pRequest->self:%" PRId64 " != pSendInfo->requestObjRefId:%" PRId64,
                 pRequest->self, pSendInfo->requestObjRefId);
        return TSDB_CODE_TSC_INTERNAL_ERROR;
      }
      pTscObj = pRequest->pTscObj;
    }
  }

  updateTargetEpSet(pSendInfo, pTscObj, pMsg, pEpSet);

  SDataBuf buf = {.msgType = pMsg->msgType,
                  .len = pMsg->contLen,
                  .pData = NULL,
                  .handle = pMsg->info.handle,
                  .handleRefId = pMsg->info.refId,
                  .pEpSet = pEpSet};

  if (pMsg->contLen > 0) {
    buf.pData = taosMemoryCalloc(1, pMsg->contLen);
    if (buf.pData == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      memcpy(buf.pData, pMsg->pCont, pMsg->contLen);
    }
  }

  pSendInfo->fp(pSendInfo->param, &buf, pMsg->code);

  if (pTscObj) {
    taosReleaseRef(clientReqRefPool, pSendInfo->requestObjRefId);
  }

  rpcFreeCont(pMsg->pCont);
  destroySendMsgInfo(pSendInfo);

  taosMemoryFree(arg);
  return TSDB_CODE_SUCCESS;
}

void processMsgFromServer(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SEpSet* tEpSet = NULL;
  if (pEpSet != NULL) {
    tEpSet = taosMemoryCalloc(1, sizeof(SEpSet));
    memcpy((void*)tEpSet, (void*)pEpSet, sizeof(SEpSet));
  }

  // pMsg is response msg
  if (pMsg->msgType == TDMT_MND_CONNECT + 1) {
    // restore origin code
    if (pMsg->code == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED) {
      pMsg->code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    } else if (pMsg->code == TSDB_CODE_RPC_SOMENODE_BROKEN_LINK) {
      pMsg->code = TSDB_CODE_RPC_BROKEN_LINK;
    }
  } else {
    // uniform to one error code: TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED
    if (pMsg->code == TSDB_CODE_RPC_SOMENODE_BROKEN_LINK) {
      pMsg->code = TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED;
    }
  }

  AsyncArg* arg = taosMemoryCalloc(1, sizeof(AsyncArg));
  arg->msg = *pMsg;
  arg->pEpset = tEpSet;

  if (0 != taosAsyncExec(doProcessMsgFromServer, arg, NULL)) {
    tscError("failed to sched msg to tsc, tsc ready to quit");
    rpcFreeCont(pMsg->pCont);
    taosMemoryFree(arg->pEpset);
    destroySendMsgInfo(pMsg->info.ahandle);
    taosMemoryFree(arg);
  }
}

TAOS* taos_connect_auth(const char* ip, const char* user, const char* auth, const char* db, uint16_t port) {
  tscDebug("try to connect to %s:%u by auth, user:%s db:%s", ip, port, user, db);
  if (user == NULL) {
    user = TSDB_DEFAULT_USER;
  }

  if (auth == NULL) {
    tscError("No auth info is given, failed to connect to server");
    return NULL;
  }

  STscObj* pObj = taos_connect_internal(ip, user, NULL, auth, db, port, CONN_TYPE__QUERY);
  if (pObj) {
    int64_t* rid = taosMemoryCalloc(1, sizeof(int64_t));
    *rid = pObj->id;
    return (TAOS*)rid;
  }

  return NULL;
}

TAOS* taos_connect_l(const char* ip, int ipLen, const char* user, int userLen, const char* pass, int passLen,
                     const char* db, int dbLen, uint16_t port) {
  char ipStr[TSDB_EP_LEN] = {0};
  char dbStr[TSDB_DB_NAME_LEN] = {0};
  char userStr[TSDB_USER_LEN] = {0};
  char passStr[TSDB_PASSWORD_LEN] = {0};

  strncpy(ipStr, ip, TMIN(TSDB_EP_LEN - 1, ipLen));
  strncpy(userStr, user, TMIN(TSDB_USER_LEN - 1, userLen));
  strncpy(passStr, pass, TMIN(TSDB_PASSWORD_LEN - 1, passLen));
  strncpy(dbStr, db, TMIN(TSDB_DB_NAME_LEN - 1, dbLen));
  return taos_connect(ipStr, userStr, passStr, dbStr, port);
}

void doSetOneRowPtr(SReqResultInfo* pResultInfo) {
  for (int32_t i = 0; i < pResultInfo->numOfCols; ++i) {
    SResultColumn* pCol = &pResultInfo->pCol[i];

    int32_t type = pResultInfo->fields[i].type;
    int32_t bytes = pResultInfo->fields[i].bytes;

    if (IS_VAR_DATA_TYPE(type)) {
      if (!IS_VAR_NULL_TYPE(type, bytes) && pCol->offset[pResultInfo->current] != -1) {
        char* pStart = pResultInfo->pCol[i].offset[pResultInfo->current] + pResultInfo->pCol[i].pData;

        pResultInfo->length[i] = varDataLen(pStart);
        pResultInfo->row[i] = varDataVal(pStart);
      } else {
        pResultInfo->row[i] = NULL;
        pResultInfo->length[i] = 0;
      }
    } else {
      if (!colDataIsNull_f(pCol->nullbitmap, pResultInfo->current)) {
        pResultInfo->row[i] = pResultInfo->pCol[i].pData + bytes * pResultInfo->current;
        pResultInfo->length[i] = bytes;
      } else {
        pResultInfo->row[i] = NULL;
        pResultInfo->length[i] = 0;
      }
    }
  }
}

void* doFetchRows(SRequestObj* pRequest, bool setupOneRowPtr, bool convertUcs4) {
  if (pRequest == NULL) {
    return NULL;
  }

  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;
  if (pResultInfo->pData == NULL || pResultInfo->current >= pResultInfo->numOfRows) {
    // All data has returned to App already, no need to try again
    if (pResultInfo->completed) {
      pResultInfo->numOfRows = 0;
      return NULL;
    }

    SReqResultInfo* pResInfo = &pRequest->body.resInfo;
    SSchedulerReq   req = {
          .syncReq = true,
          .pFetchRes = (void**)&pResInfo->pData,
    };
    pRequest->code = schedulerFetchRows(pRequest->body.queryJob, &req);
    if (pRequest->code != TSDB_CODE_SUCCESS) {
      pResultInfo->numOfRows = 0;
      return NULL;
    }

    pRequest->code =
        setQueryResultFromRsp(&pRequest->body.resInfo, (const SRetrieveTableRsp*)pResInfo->pData, convertUcs4, true);
    if (pRequest->code != TSDB_CODE_SUCCESS) {
      pResultInfo->numOfRows = 0;
      return NULL;
    }

    tscDebug("0x%" PRIx64 " fetch results, numOfRows:%" PRId64 " total Rows:%" PRId64 ", complete:%d, reqId:0x%" PRIx64,
             pRequest->self, pResInfo->numOfRows, pResInfo->totalRows, pResInfo->completed, pRequest->requestId);

    STscObj*            pTscObj = pRequest->pTscObj;
    SAppClusterSummary* pActivity = &pTscObj->pAppInfo->summary;
    atomic_add_fetch_64((int64_t*)&pActivity->fetchBytes, pRequest->body.resInfo.payloadLen);

    if (pResultInfo->numOfRows == 0) {
      return NULL;
    }
  }

  if (setupOneRowPtr) {
    doSetOneRowPtr(pResultInfo);
    pResultInfo->current += 1;
  }

  return pResultInfo->row;
}

static void syncFetchFn(void* param, TAOS_RES* res, int32_t numOfRows) {
  tsem_t* sem = param;
  tsem_post(sem);
}

void* doAsyncFetchRows(SRequestObj* pRequest, bool setupOneRowPtr, bool convertUcs4) {
  if (pRequest == NULL) {
    return NULL;
  }

  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;
  if (pResultInfo->pData == NULL || pResultInfo->current >= pResultInfo->numOfRows) {
    // All data has returned to App already, no need to try again
    if (pResultInfo->completed) {
      pResultInfo->numOfRows = 0;
      return NULL;
    }

    // convert ucs4 to native multi-bytes string
    pResultInfo->convertUcs4 = convertUcs4;
    tsem_t sem;
    tsem_init(&sem, 0, 0);
    taos_fetch_rows_a(pRequest, syncFetchFn, &sem);
    tsem_wait(&sem);
    tsem_destroy(&sem);
    pRequest->inCallback = false;
  }

  if (pResultInfo->numOfRows == 0 || pRequest->code != TSDB_CODE_SUCCESS) {
    return NULL;
  } else {
    if (setupOneRowPtr) {
      doSetOneRowPtr(pResultInfo);
      pResultInfo->current += 1;
    }

    return pResultInfo->row;
  }
}

static int32_t doPrepareResPtr(SReqResultInfo* pResInfo) {
  if (pResInfo->row == NULL) {
    pResInfo->row = taosMemoryCalloc(pResInfo->numOfCols, POINTER_BYTES);
    pResInfo->pCol = taosMemoryCalloc(pResInfo->numOfCols, sizeof(SResultColumn));
    pResInfo->length = taosMemoryCalloc(pResInfo->numOfCols, sizeof(int32_t));
    pResInfo->convertBuf = taosMemoryCalloc(pResInfo->numOfCols, POINTER_BYTES);

    if (pResInfo->row == NULL || pResInfo->pCol == NULL || pResInfo->length == NULL || pResInfo->convertBuf == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doConvertUCS4(SReqResultInfo* pResultInfo, int32_t numOfRows, int32_t numOfCols, int32_t* colLength) {
  for (int32_t i = 0; i < numOfCols; ++i) {
    int32_t type = pResultInfo->fields[i].type;
    int32_t bytes = pResultInfo->fields[i].bytes;

    if (type == TSDB_DATA_TYPE_NCHAR && colLength[i] > 0) {
      char* p = taosMemoryRealloc(pResultInfo->convertBuf[i], colLength[i]);
      if (p == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pResultInfo->convertBuf[i] = p;

      SResultColumn* pCol = &pResultInfo->pCol[i];
      for (int32_t j = 0; j < numOfRows; ++j) {
        if (pCol->offset[j] != -1) {
          char* pStart = pCol->offset[j] + pCol->pData;

          int32_t len = taosUcs4ToMbs((TdUcs4*)varDataVal(pStart), varDataLen(pStart), varDataVal(p));
          if (len > bytes || (p + len) >= (pResultInfo->convertBuf[i] + colLength[i])) {
            tscError(
                "doConvertUCS4 error, invalid data. len:%d, bytes:%d, (p + len):%p, (pResultInfo->convertBuf[i] + "
                "colLength[i]):%p",
                len, bytes, (p + len), (pResultInfo->convertBuf[i] + colLength[i]));
            return TSDB_CODE_TSC_INTERNAL_ERROR;
          }

          varDataSetLen(p, len);
          pCol->offset[j] = (p - pResultInfo->convertBuf[i]);
          p += (len + VARSTR_HEADER_SIZE);
        }
      }

      pResultInfo->pCol[i].pData = pResultInfo->convertBuf[i];
      pResultInfo->row[i] = pResultInfo->pCol[i].pData;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getVersion1BlockMetaSize(const char* p, int32_t numOfCols) {
  return sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) * 3 + sizeof(uint64_t) +
         numOfCols * (sizeof(int8_t) + sizeof(int32_t));
}

static int32_t estimateJsonLen(SReqResultInfo* pResultInfo, int32_t numOfCols, int32_t numOfRows) {
  char* p = (char*)pResultInfo->pData;
  int32_t blockVersion = *(int32_t*)p;

  // | version | total length | total rows | total columns | flag seg| block group id | column schema | each column
  // length |
  int32_t cols = *(int32_t*)(p + sizeof(int32_t) * 3);
  if (ASSERT(numOfCols == cols)) {
    tscError("estimateJsonLen error: numOfCols:%d != cols:%d", numOfCols, cols);
    return -1;
  }

  int32_t  len = getVersion1BlockMetaSize(p, numOfCols);
  int32_t* colLength = (int32_t*)(p + len);
  len += sizeof(int32_t) * numOfCols;

  char* pStart = p + len;
  for (int32_t i = 0; i < numOfCols; ++i) {
    int32_t colLen = (blockVersion == BLOCK_VERSION_1) ? htonl(colLength[i]) : colLength[i];

    if (pResultInfo->fields[i].type == TSDB_DATA_TYPE_JSON) {
      int32_t* offset = (int32_t*)pStart;
      int32_t  lenTmp = numOfRows * sizeof(int32_t);
      len += lenTmp;
      pStart += lenTmp;

      int32_t estimateColLen = 0;
      for (int32_t j = 0; j < numOfRows; ++j) {
        if (offset[j] == -1) {
          continue;
        }
        char* data = offset[j] + pStart;

        int32_t jsonInnerType = *data;
        char*   jsonInnerData = data + CHAR_BYTES;
        if (jsonInnerType == TSDB_DATA_TYPE_NULL) {
          estimateColLen += (VARSTR_HEADER_SIZE + strlen(TSDB_DATA_NULL_STR_L));
        } else if (tTagIsJson(data)) {
          estimateColLen += (VARSTR_HEADER_SIZE + ((const STag*)(data))->len);
        } else if (jsonInnerType == TSDB_DATA_TYPE_NCHAR) {  // value -> "value"
          estimateColLen += varDataTLen(jsonInnerData) + CHAR_BYTES * 2;
        } else if (jsonInnerType == TSDB_DATA_TYPE_DOUBLE) {
          estimateColLen += (VARSTR_HEADER_SIZE + 32);
        } else if (jsonInnerType == TSDB_DATA_TYPE_BOOL) {
          estimateColLen += (VARSTR_HEADER_SIZE + 5);
        } else {
          tscError("estimateJsonLen error: invalid type:%d", jsonInnerType);
          return -1;
        }
      }
      len += TMAX(colLen, estimateColLen);
    } else if (IS_VAR_DATA_TYPE(pResultInfo->fields[i].type)) {
      int32_t lenTmp = numOfRows * sizeof(int32_t);
      len += (lenTmp + colLen);
      pStart += lenTmp;
    } else {
      int32_t lenTmp = BitmapLen(pResultInfo->numOfRows);
      len += (lenTmp + colLen);
      pStart += lenTmp;
    }
    pStart += colLen;
  }
  return len;
}

static int32_t doConvertJson(SReqResultInfo* pResultInfo, int32_t numOfCols, int32_t numOfRows) {
  bool needConvert = false;
  for (int32_t i = 0; i < numOfCols; ++i) {
    if (pResultInfo->fields[i].type == TSDB_DATA_TYPE_JSON) {
      needConvert = true;
      break;
    }
  }

  if (!needConvert) {
    return TSDB_CODE_SUCCESS;
  }

  tscDebug("start to convert form json format string");

  char*   p = (char*)pResultInfo->pData;
  int32_t blockVersion = *(int32_t*)p;
  int32_t dataLen = estimateJsonLen(pResultInfo, numOfCols, numOfRows);
  if (dataLen <= 0) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  taosMemoryFreeClear(pResultInfo->convertJson);
  pResultInfo->convertJson = taosMemoryCalloc(1, dataLen);
  if (pResultInfo->convertJson == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  char* p1 = pResultInfo->convertJson;

  int32_t totalLen = 0;
  int32_t cols = *(int32_t*)(p + sizeof(int32_t) * 3);
  if (ASSERT(numOfCols == cols)) {
    tscError("doConvertJson error: numOfCols:%d != cols:%d", numOfCols, cols);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  int32_t len = getVersion1BlockMetaSize(p, numOfCols);
  memcpy(p1, p, len);

  p += len;
  p1 += len;
  totalLen += len;

  len = sizeof(int32_t) * numOfCols;
  int32_t* colLength = (int32_t*)p;
  int32_t* colLength1 = (int32_t*)p1;
  memcpy(p1, p, len);
  p += len;
  p1 += len;
  totalLen += len;

  char* pStart = p;
  char* pStart1 = p1;
  for (int32_t i = 0; i < numOfCols; ++i) {
    int32_t colLen = (blockVersion == BLOCK_VERSION_1) ? htonl(colLength[i]) : colLength[i];
    int32_t colLen1 = (blockVersion == BLOCK_VERSION_1) ? htonl(colLength1[i]) : colLength1[i];
    if (ASSERT(colLen < dataLen)) {
      tscError("doConvertJson error: colLen:%d >= dataLen:%d", colLen, dataLen);
      return TSDB_CODE_TSC_INTERNAL_ERROR;
    }
    if (pResultInfo->fields[i].type == TSDB_DATA_TYPE_JSON) {
      int32_t* offset = (int32_t*)pStart;
      int32_t* offset1 = (int32_t*)pStart1;
      len = numOfRows * sizeof(int32_t);
      memcpy(pStart1, pStart, len);
      pStart += len;
      pStart1 += len;
      totalLen += len;

      len = 0;
      for (int32_t j = 0; j < numOfRows; ++j) {
        if (offset[j] == -1) {
          continue;
        }
        char* data = offset[j] + pStart;

        int32_t jsonInnerType = *data;
        char*   jsonInnerData = data + CHAR_BYTES;
        char    dst[TSDB_MAX_JSON_TAG_LEN] = {0};
        if (jsonInnerType == TSDB_DATA_TYPE_NULL) {
          sprintf(varDataVal(dst), "%s", TSDB_DATA_NULL_STR_L);
          varDataSetLen(dst, strlen(varDataVal(dst)));
        } else if (tTagIsJson(data)) {
          char* jsonString = parseTagDatatoJson(data);
          STR_TO_VARSTR(dst, jsonString);
          taosMemoryFree(jsonString);
        } else if (jsonInnerType == TSDB_DATA_TYPE_NCHAR) {  // value -> "value"
          *(char*)varDataVal(dst) = '\"';
          int32_t length = taosUcs4ToMbs((TdUcs4*)varDataVal(jsonInnerData), varDataLen(jsonInnerData),
                                         varDataVal(dst) + CHAR_BYTES);
          if (length <= 0) {
            tscError("charset:%s to %s. convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset);
            length = 0;
          }
          varDataSetLen(dst, length + CHAR_BYTES * 2);
          *(char*)POINTER_SHIFT(varDataVal(dst), length + CHAR_BYTES) = '\"';
        } else if (jsonInnerType == TSDB_DATA_TYPE_DOUBLE) {
          double jsonVd = *(double*)(jsonInnerData);
          sprintf(varDataVal(dst), "%.9lf", jsonVd);
          varDataSetLen(dst, strlen(varDataVal(dst)));
        } else if (jsonInnerType == TSDB_DATA_TYPE_BOOL) {
          sprintf(varDataVal(dst), "%s", (*((char*)jsonInnerData) == 1) ? "true" : "false");
          varDataSetLen(dst, strlen(varDataVal(dst)));
        } else {
          tscError("doConvertJson error: invalid type:%d", jsonInnerType);
          return TSDB_CODE_TSC_INTERNAL_ERROR;
        }

        offset1[j] = len;
        memcpy(pStart1 + len, dst, varDataTLen(dst));
        len += varDataTLen(dst);
      }
      colLen1 = len;
      totalLen += colLen1;
      colLength1[i] = (blockVersion == BLOCK_VERSION_1) ? htonl(len) : len;
    } else if (IS_VAR_DATA_TYPE(pResultInfo->fields[i].type)) {
      len = numOfRows * sizeof(int32_t);
      memcpy(pStart1, pStart, len);
      pStart += len;
      pStart1 += len;
      totalLen += len;
      totalLen += colLen;
      memcpy(pStart1, pStart, colLen);
    } else {
      len = BitmapLen(pResultInfo->numOfRows);
      memcpy(pStart1, pStart, len);
      pStart += len;
      pStart1 += len;
      totalLen += len;
      totalLen += colLen;
      memcpy(pStart1, pStart, colLen);
    }
    pStart += colLen;
    pStart1 += colLen1;
  }

  *(int32_t*)(pResultInfo->convertJson + 4) = totalLen;
  pResultInfo->pData = pResultInfo->convertJson;
  return TSDB_CODE_SUCCESS;
}

int32_t setResultDataPtr(SReqResultInfo* pResultInfo, TAOS_FIELD* pFields, int32_t numOfCols, int32_t numOfRows,
                         bool convertUcs4) {
  if (ASSERT(numOfCols > 0 && pFields != NULL && pResultInfo != NULL)) {
    tscError("setResultDataPtr paras error");
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (numOfRows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = doPrepareResPtr(pResultInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = doConvertJson(pResultInfo, numOfCols, numOfRows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  char* p = (char*)pResultInfo->pData;

  // version:
  int32_t blockVersion = *(int32_t*)p;
  p += sizeof(int32_t);

  int32_t dataLen = *(int32_t*)p;
  p += sizeof(int32_t);

  int32_t rows = *(int32_t*)p;
  p += sizeof(int32_t);

  int32_t cols = *(int32_t*)p;
  p += sizeof(int32_t);

  if (ASSERT(rows == numOfRows && cols == numOfCols)) {
    tscError("setResultDataPtr paras error:rows;%d numOfRows:%d cols:%d numOfCols:%d", rows, numOfRows, cols,
             numOfCols);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  int32_t hasColumnSeg = *(int32_t*)p;
  p += sizeof(int32_t);

  uint64_t groupId = *(uint64_t*)p;
  p += sizeof(uint64_t);

  // check fields
  for (int32_t i = 0; i < numOfCols; ++i) {
    int8_t type = *(int8_t*)p;
    p += sizeof(int8_t);

    int32_t bytes = *(int32_t*)p;
    p += sizeof(int32_t);

    /*ASSERT(type == pFields[i].type && bytes == pFields[i].bytes);*/
  }

  int32_t* colLength = (int32_t*)p;
  p += sizeof(int32_t) * numOfCols;

  char* pStart = p;
  for (int32_t i = 0; i < numOfCols; ++i) {
    if(blockVersion == BLOCK_VERSION_1){
      colLength[i] = htonl(colLength[i]);
    }
    if (colLength[i] >= dataLen) {
      tscError("invalid colLength %d, dataLen %d", colLength[i], dataLen);
      return TSDB_CODE_TSC_INTERNAL_ERROR;
    }

    if (IS_VAR_DATA_TYPE(pResultInfo->fields[i].type)) {
      pResultInfo->pCol[i].offset = (int32_t*)pStart;
      pStart += numOfRows * sizeof(int32_t);
    } else {
      pResultInfo->pCol[i].nullbitmap = pStart;
      pStart += BitmapLen(pResultInfo->numOfRows);
    }

    pResultInfo->pCol[i].pData = pStart;
    pResultInfo->length[i] = pResultInfo->fields[i].bytes;
    pResultInfo->row[i] = pResultInfo->pCol[i].pData;

    pStart += colLength[i];
  }

  // bool blankFill = *(bool*)p;
  p += sizeof(bool);

  if (convertUcs4) {
    code = doConvertUCS4(pResultInfo, numOfRows, numOfCols, colLength);
  }

  return code;
}

char* getDbOfConnection(STscObj* pObj) {
  char* p = NULL;
  taosThreadMutexLock(&pObj->mutex);
  size_t len = strlen(pObj->db);
  if (len > 0) {
    p = strndup(pObj->db, tListLen(pObj->db));
  }

  taosThreadMutexUnlock(&pObj->mutex);
  return p;
}

void setConnectionDB(STscObj* pTscObj, const char* db) {
  if (db == NULL || pTscObj == NULL) {
    tscError("setConnectionDB para is NULL");
    return;
  }

  taosThreadMutexLock(&pTscObj->mutex);
  tstrncpy(pTscObj->db, db, tListLen(pTscObj->db));
  taosThreadMutexUnlock(&pTscObj->mutex);
}

void resetConnectDB(STscObj* pTscObj) {
  if (pTscObj == NULL) {
    return;
  }

  taosThreadMutexLock(&pTscObj->mutex);
  pTscObj->db[0] = 0;
  taosThreadMutexUnlock(&pTscObj->mutex);
}

int32_t setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp, bool convertUcs4,
                              bool freeAfterUse) {
  if (pResultInfo == NULL || pRsp == NULL) {
    tscError("setQueryResultFromRsp paras is null");
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  if (freeAfterUse) taosMemoryFreeClear(pResultInfo->pRspMsg);

  pResultInfo->pRspMsg = (const char*)pRsp;
  pResultInfo->pData = (void*)pRsp->data;
  pResultInfo->numOfRows = htobe64(pRsp->numOfRows);
  pResultInfo->current = 0;
  pResultInfo->completed = (pRsp->completed == 1);
  pResultInfo->payloadLen = htonl(pRsp->compLen);
  pResultInfo->precision = pRsp->precision;

  // TODO handle the compressed case
  pResultInfo->totalRows += pResultInfo->numOfRows;
  return setResultDataPtr(pResultInfo, pResultInfo->fields, pResultInfo->numOfCols, pResultInfo->numOfRows,
                          convertUcs4);
}

TSDB_SERVER_STATUS taos_check_server_status(const char* fqdn, int port, char* details, int maxlen) {
  TSDB_SERVER_STATUS code = TSDB_SRV_STATUS_UNAVAILABLE;
  void*              clientRpc = NULL;
  SServerStatusRsp   statusRsp = {0};
  SEpSet             epSet = {.inUse = 0, .numOfEps = 1};
  SRpcMsg            rpcMsg = {.info.ahandle = (void*)0x9526, .msgType = TDMT_DND_SERVER_STATUS};
  SRpcMsg            rpcRsp = {0};
  SRpcInit           rpcInit = {0};
  char               pass[TSDB_PASSWORD_LEN + 1] = {0};

  rpcInit.label = "CHK";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = NULL;
  rpcInit.sessions = 16;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.compressSize = tsCompressMsgSize;
  rpcInit.user = "_dnd";

  int32_t connLimitNum = tsNumOfRpcSessions / (tsNumOfRpcThreads * 3);
  connLimitNum = TMAX(connLimitNum, 10);
  connLimitNum = TMIN(connLimitNum, 500);
  rpcInit.connLimitNum = connLimitNum;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  taosVersionStrToInt(version, &(rpcInit.compatibilityVer));

  clientRpc = rpcOpen(&rpcInit);
  if (clientRpc == NULL) {
    tscError("failed to init server status client");
    goto _OVER;
  }

  if (fqdn == NULL) {
    fqdn = tsLocalFqdn;
  }

  if (port == 0) {
    port = tsServerPort;
  }

  tstrncpy(epSet.eps[0].fqdn, fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = (uint16_t)port;
  rpcSendRecv(clientRpc, &epSet, &rpcMsg, &rpcRsp);

  if (rpcRsp.code != 0 || rpcRsp.contLen <= 0 || rpcRsp.pCont == NULL) {
    tscError("failed to send server status req since %s", terrstr());
    goto _OVER;
  }

  if (tDeserializeSServerStatusRsp(rpcRsp.pCont, rpcRsp.contLen, &statusRsp) != 0) {
    tscError("failed to parse server status rsp since %s", terrstr());
    goto _OVER;
  }

  code = statusRsp.statusCode;
  if (details != NULL) {
    tstrncpy(details, statusRsp.details, maxlen);
  }

_OVER:
  if (clientRpc != NULL) {
    rpcClose(clientRpc);
  }
  if (rpcRsp.pCont != NULL) {
    rpcFreeCont(rpcRsp.pCont);
  }
  return code;
}

int32_t appendTbToReq(SHashObj* pHash, int32_t pos1, int32_t len1, int32_t pos2, int32_t len2, const char* str,
                      int32_t acctId, char* db) {
  SName name = {0};

  if (len1 <= 0) {
    return -1;
  }

  const char* dbName = db;
  const char* tbName = NULL;
  int32_t     dbLen = 0;
  int32_t     tbLen = 0;
  if (len2 > 0) {
    dbName = str + pos1;
    dbLen = len1;
    tbName = str + pos2;
    tbLen = len2;
  } else {
    dbLen = strlen(db);
    tbName = str + pos1;
    tbLen = len1;
  }

  if (dbLen <= 0 || tbLen <= 0) {
    return -1;
  }

  if (tNameSetDbName(&name, acctId, dbName, dbLen)) {
    return -1;
  }

  if (tNameAddTbName(&name, tbName, tbLen)) {
    return -1;
  }

  char dbFName[TSDB_DB_FNAME_LEN];
  sprintf(dbFName, "%d.%.*s", acctId, dbLen, dbName);

  STablesReq* pDb = taosHashGet(pHash, dbFName, strlen(dbFName));
  if (pDb) {
    taosArrayPush(pDb->pTables, &name);
  } else {
    STablesReq db;
    db.pTables = taosArrayInit(20, sizeof(SName));
    strcpy(db.dbFName, dbFName);
    taosArrayPush(db.pTables, &name);
    taosHashPut(pHash, dbFName, strlen(dbFName), &db, sizeof(db));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t transferTableNameList(const char* tbList, int32_t acctId, char* dbName, SArray** pReq) {
  SHashObj* pHash = taosHashInit(3, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pHash) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  bool    inEscape = false;
  int32_t code = 0;
  void*   pIter = NULL;

  int32_t vIdx = 0;
  int32_t vPos[2];
  int32_t vLen[2];

  memset(vPos, -1, sizeof(vPos));
  memset(vLen, 0, sizeof(vLen));

  for (int32_t i = 0;; ++i) {
    if (0 == *(tbList + i)) {
      if (vPos[vIdx] >= 0 && vLen[vIdx] <= 0) {
        vLen[vIdx] = i - vPos[vIdx];
      }

      code = appendTbToReq(pHash, vPos[0], vLen[0], vPos[1], vLen[1], tbList, acctId, dbName);
      if (code) {
        goto _return;
      }

      break;
    }

    if ('`' == *(tbList + i)) {
      inEscape = !inEscape;
      if (!inEscape) {
        if (vPos[vIdx] >= 0) {
          vLen[vIdx] = i - vPos[vIdx];
        } else {
          goto _return;
        }
      }

      continue;
    }

    if (inEscape) {
      if (vPos[vIdx] < 0) {
        vPos[vIdx] = i;
      }
      continue;
    }

    if ('.' == *(tbList + i)) {
      if (vPos[vIdx] < 0) {
        goto _return;
      }
      if (vLen[vIdx] <= 0) {
        vLen[vIdx] = i - vPos[vIdx];
      }
      vIdx++;
      if (vIdx >= 2) {
        goto _return;
      }
      continue;
    }

    if (',' == *(tbList + i)) {
      if (vPos[vIdx] < 0) {
        goto _return;
      }
      if (vLen[vIdx] <= 0) {
        vLen[vIdx] = i - vPos[vIdx];
      }

      code = appendTbToReq(pHash, vPos[0], vLen[0], vPos[1], vLen[1], tbList, acctId, dbName);
      if (code) {
        goto _return;
      }

      memset(vPos, -1, sizeof(vPos));
      memset(vLen, 0, sizeof(vLen));
      vIdx = 0;
      continue;
    }

    if (' ' == *(tbList + i) || '\r' == *(tbList + i) || '\t' == *(tbList + i) || '\n' == *(tbList + i)) {
      if (vPos[vIdx] >= 0 && vLen[vIdx] <= 0) {
        vLen[vIdx] = i - vPos[vIdx];
      }
      continue;
    }

    if (('a' <= *(tbList + i) && 'z' >= *(tbList + i)) || ('A' <= *(tbList + i) && 'Z' >= *(tbList + i)) ||
        ('0' <= *(tbList + i) && '9' >= *(tbList + i)) || ('_' == *(tbList + i))) {
      if (vLen[vIdx] > 0) {
        goto _return;
      }
      if (vPos[vIdx] < 0) {
        vPos[vIdx] = i;
      }
      continue;
    }

    goto _return;
  }

  int32_t dbNum = taosHashGetSize(pHash);
  *pReq = taosArrayInit(dbNum, sizeof(STablesReq));
  pIter = taosHashIterate(pHash, NULL);
  while (pIter) {
    STablesReq* pDb = (STablesReq*)pIter;
    taosArrayPush(*pReq, pDb);
    pIter = taosHashIterate(pHash, pIter);
  }

  taosHashCleanup(pHash);

  return TSDB_CODE_SUCCESS;

_return:

  terrno = TSDB_CODE_TSC_INVALID_OPERATION;

  pIter = taosHashIterate(pHash, NULL);
  while (pIter) {
    STablesReq* pDb = (STablesReq*)pIter;
    taosArrayDestroy(pDb->pTables);
    pIter = taosHashIterate(pHash, pIter);
  }

  taosHashCleanup(pHash);

  return terrno;
}

void syncCatalogFn(SMetaData* pResult, void* param, int32_t code) {
  SSyncQueryParam* pParam = param;
  pParam->pRequest->code = code;

  tsem_post(&pParam->sem);
}

void syncQueryFn(void* param, void* res, int32_t code) {
  SSyncQueryParam* pParam = param;
  pParam->pRequest = res;

  if (pParam->pRequest) {
    pParam->pRequest->code = code;
  }

  tsem_post(&pParam->sem);
}

void taosAsyncQueryImpl(uint64_t connId, const char* sql, __taos_async_fn_t fp, void* param, bool validateOnly) {
  if (sql == NULL || NULL == fp) {
    terrno = TSDB_CODE_INVALID_PARA;
    if (fp) {
      fp(param, NULL, terrno);
    }

    return;
  }

  size_t sqlLen = strlen(sql);
  if (sqlLen > (size_t)TSDB_MAX_ALLOWED_SQL_LEN) {
    tscError("sql string exceeds max length:%d", TSDB_MAX_ALLOWED_SQL_LEN);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    fp(param, NULL, terrno);
    return;
  }

  SRequestObj* pRequest = NULL;
  int32_t      code = buildRequest(connId, sql, sqlLen, param, validateOnly, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    fp(param, NULL, terrno);
    return;
  }

  pRequest->body.queryFp = fp;
  doAsyncQuery(pRequest, false);
}
void taosAsyncQueryImplWithReqid(uint64_t connId, const char* sql, __taos_async_fn_t fp, void* param, bool validateOnly,
                                 int64_t reqid) {
  if (sql == NULL || NULL == fp) {
    terrno = TSDB_CODE_INVALID_PARA;
    if (fp) {
      fp(param, NULL, terrno);
    }

    return;
  }

  size_t sqlLen = strlen(sql);
  if (sqlLen > (size_t)TSDB_MAX_ALLOWED_SQL_LEN) {
    tscError("sql string exceeds max length:%d", TSDB_MAX_ALLOWED_SQL_LEN);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    fp(param, NULL, terrno);
    return;
  }

  SRequestObj* pRequest = NULL;
  int32_t      code = buildRequest(connId, sql, sqlLen, param, validateOnly, &pRequest, reqid);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    fp(param, NULL, terrno);
    return;
  }

  pRequest->body.queryFp = fp;
  doAsyncQuery(pRequest, false);
}

TAOS_RES* taosQueryImpl(TAOS* taos, const char* sql, bool validateOnly) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  tscDebug("taos_query start with sql:%s", sql);

  SSyncQueryParam* param = taosMemoryCalloc(1, sizeof(SSyncQueryParam));
  if (NULL == param) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  tsem_init(&param->sem, 0, 0);

  taosAsyncQueryImpl(*(int64_t*)taos, sql, syncQueryFn, param, validateOnly);
  tsem_wait(&param->sem);

  SRequestObj* pRequest = NULL;
  if (param->pRequest != NULL) {
    param->pRequest->syncQuery = true;
    pRequest = param->pRequest;
    param->pRequest->inCallback = false;
  }
  taosMemoryFree(param);

  tscDebug("taos_query end with sql:%s", sql);

  return pRequest;
}

TAOS_RES* taosQueryImplWithReqid(TAOS* taos, const char* sql, bool validateOnly, int64_t reqid) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  SSyncQueryParam* param = taosMemoryCalloc(1, sizeof(SSyncQueryParam));
  if (param == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  tsem_init(&param->sem, 0, 0);

  taosAsyncQueryImplWithReqid(*(int64_t*)taos, sql, syncQueryFn, param, validateOnly, reqid);
  tsem_wait(&param->sem);
  SRequestObj* pRequest = NULL;
  if (param->pRequest != NULL) {
    param->pRequest->syncQuery = true;
    pRequest = param->pRequest;
  }
  taosMemoryFree(param);
  return pRequest;
}

static void fetchCallback(void* pResult, void* param, int32_t code) {
  SRequestObj* pRequest = (SRequestObj*)param;

  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;

  tscDebug("0x%" PRIx64 " enter scheduler fetch cb, code:%d - %s, reqId:0x%" PRIx64, pRequest->self, code,
           tstrerror(code), pRequest->requestId);

  pResultInfo->pData = pResult;
  pResultInfo->numOfRows = 0;

  if (code != TSDB_CODE_SUCCESS) {
    pRequest->code = code;
    taosMemoryFreeClear(pResultInfo->pData);
    pRequest->body.fetchFp(((SSyncQueryParam*)pRequest->body.interParam)->userParam, pRequest, 0);
    return;
  }

  if (pRequest->code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(pResultInfo->pData);
    pRequest->body.fetchFp(((SSyncQueryParam*)pRequest->body.interParam)->userParam, pRequest, 0);
    return;
  }

  pRequest->code =
      setQueryResultFromRsp(pResultInfo, (const SRetrieveTableRsp*)pResultInfo->pData, pResultInfo->convertUcs4, true);
  if (pRequest->code != TSDB_CODE_SUCCESS) {
    pResultInfo->numOfRows = 0;
    pRequest->code = code;
    tscError("0x%" PRIx64 " fetch results failed, code:%s, reqId:0x%" PRIx64, pRequest->self, tstrerror(code),
             pRequest->requestId);
  } else {
    tscDebug("0x%" PRIx64 " fetch results, numOfRows:%" PRId64 " total Rows:%" PRId64 ", complete:%d, reqId:0x%" PRIx64,
             pRequest->self, pResultInfo->numOfRows, pResultInfo->totalRows, pResultInfo->completed,
             pRequest->requestId);

    STscObj*            pTscObj = pRequest->pTscObj;
    SAppClusterSummary* pActivity = &pTscObj->pAppInfo->summary;
    atomic_add_fetch_64((int64_t*)&pActivity->fetchBytes, pRequest->body.resInfo.payloadLen);
  }

  pRequest->body.fetchFp(((SSyncQueryParam*)pRequest->body.interParam)->userParam, pRequest, pResultInfo->numOfRows);
}

void taosAsyncFetchImpl(SRequestObj* pRequest, __taos_async_fn_t fp, void* param) {
  pRequest->body.fetchFp = fp;
  ((SSyncQueryParam*)pRequest->body.interParam)->userParam = param;

  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;

  // this query has no results or error exists, return directly
  if (taos_num_fields(pRequest) == 0 || pRequest->code != TSDB_CODE_SUCCESS) {
    pResultInfo->numOfRows = 0;
    pRequest->body.fetchFp(param, pRequest, pResultInfo->numOfRows);
    return;
  }

  // all data has returned to App already, no need to try again
  if (pResultInfo->completed) {
    // it is a local executed query, no need to do async fetch
    if (QUERY_EXEC_MODE_SCHEDULE != pRequest->body.execMode) {
      if (pResultInfo->localResultFetched) {
        pResultInfo->numOfRows = 0;
        pResultInfo->current = 0;
      } else {
        pResultInfo->localResultFetched = true;
      }
    } else {
      pResultInfo->numOfRows = 0;
    }

    pRequest->body.fetchFp(param, pRequest, pResultInfo->numOfRows);
    return;
  }

  SSchedulerReq req = {
      .syncReq = false,
      .fetchFp = fetchCallback,
      .cbParam = pRequest,
  };

  schedulerFetchRows(pRequest->body.queryJob, &req);
}

void doRequestCallback(SRequestObj* pRequest, int32_t code) {
  pRequest->inCallback = true;
  int64_t this = pRequest->self;
  pRequest->body.queryFp(((SSyncQueryParam*)pRequest->body.interParam)->userParam, pRequest, code);
  SRequestObj* pReq = acquireRequest(this);
  if (pReq != NULL) {
    pReq->inCallback = false;
    releaseRequest(this);
  }
}

int32_t clientParseSql(void* param, const char* dbName, const char* sql, bool parseOnly, const char* effectiveUser,
                       SParseSqlRes* pRes) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_SUCCESS;
#else
  return clientParseSqlImpl(param, dbName, sql, parseOnly, effectiveUser, pRes);
#endif
}
