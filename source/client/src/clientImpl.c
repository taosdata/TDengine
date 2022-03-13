
#include "clientInt.h"
#include "clientLog.h"
#include "parser.h"
#include "planner.h"
#include "scheduler.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tpagedbuf.h"
#include "tref.h"

static int32_t       initEpSetFromCfg(const char* firstEp, const char* secondEp, SCorEpSet* pEpSet);
static SMsgSendInfo* buildConnectMsg(SRequestObj* pRequest);
static void          destroySendMsgInfo(SMsgSendInfo* pMsgBody);
static void          setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp);

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
  return strdup(key);
}

static STscObj* taosConnectImpl(const char* user, const char* auth, const char* db, __taos_async_fn_t fp, void* param,
                                SAppInstInfo* pAppInfo);
static void     setResSchemaInfo(SReqResultInfo* pResInfo, const SSchema* pSchema, int32_t numOfCols);

TAOS* taos_connect_internal(const char* ip, const char* user, const char* pass, const char* auth, const char* db,
                            uint16_t port) {
  if (taos_init() != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  if (!validateUserName(user)) {
    terrno = TSDB_CODE_TSC_INVALID_USER_LENGTH;
    return NULL;
  }

  char localDb[TSDB_DB_NAME_LEN] = {0};
  if (db != NULL) {
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

    if (port) {
      epSet.epSet.eps[0].port = port;
    }
  } else {
    if (initEpSetFromCfg(tsFirst, tsSecond, &epSet) < 0) {
      return NULL;
    }
  }

  char*          key = getClusterKey(user, secretEncrypt, ip, port);
  SAppInstInfo** pInst = NULL;

  pthread_mutex_lock(&appInfo.mutex);

  pInst = taosHashGet(appInfo.pInstMap, key, strlen(key));
  SAppInstInfo* p = NULL;
  if (pInst == NULL) {
    p = calloc(1, sizeof(struct SAppInstInfo));
    p->mgmtEp = epSet;
    p->pTransporter = openTransporter(user, secretEncrypt, tsNumOfCores);
    p->pAppHbMgr = appHbMgrInit(p, key);
    taosHashPut(appInfo.pInstMap, key, strlen(key), &p, POINTER_BYTES);

    pInst = &p;
  }

  pthread_mutex_unlock(&appInfo.mutex);

  tfree(key);
  return taosConnectImpl(user, &secretEncrypt[0], localDb, NULL, NULL, *pInst);
}

int32_t buildRequest(STscObj* pTscObj, const char* sql, int sqlLen, SRequestObj** pRequest) {
  *pRequest = createRequest(pTscObj, NULL, NULL, TSDB_SQL_SELECT);
  if (*pRequest == NULL) {
    tscError("failed to malloc sqlObj");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  (*pRequest)->sqlstr = malloc(sqlLen + 1);
  if ((*pRequest)->sqlstr == NULL) {
    tscError("0x%" PRIx64 " failed to prepare sql string buffer", (*pRequest)->self);
    (*pRequest)->msgBuf = strdup("failed to prepare sql string buffer");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  strntolower((*pRequest)->sqlstr, sql, (int32_t)sqlLen);
  (*pRequest)->sqlstr[sqlLen] = 0;
  (*pRequest)->sqlLen = sqlLen;

  tscDebugL("0x%" PRIx64 " SQL: %s, reqId:0x%" PRIx64, (*pRequest)->self, (*pRequest)->sqlstr, (*pRequest)->requestId);
  return TSDB_CODE_SUCCESS;
}

int32_t parseSql(SRequestObj* pRequest, SQuery** pQuery) {
  STscObj* pTscObj = pRequest->pTscObj;

  SParseContext cxt = {
      .requestId = pRequest->requestId,
      .acctId = pTscObj->acctId,
      .db = getDbOfConnection(pTscObj),
      .pSql = pRequest->sqlstr,
      .sqlLen = pRequest->sqlLen,
      .pMsg = pRequest->msgBuf,
      .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
      .pTransporter = pTscObj->pAppInfo->pTransporter,
  };

  cxt.mgmtEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  int32_t code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &cxt.pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(cxt.db);
    return code;
  }

  code = qParseQuerySql(&cxt, pQuery);
  if (TSDB_CODE_SUCCESS == code && ((*pQuery)->haveResultSet)) {
    setResSchemaInfo(&pRequest->body.resInfo, (*pQuery)->pResSchema, (*pQuery)->numOfResCols);
  }

  tfree(cxt.db);
  return code;
}

int32_t execDdlQuery(SRequestObj* pRequest, SQuery* pQuery) {
  SCmdMsgInfo* pMsgInfo = pQuery->pCmdMsg;
  pRequest->type = pMsgInfo->msgType;
  pRequest->body.requestMsg = (SDataBuf){.pData = pMsgInfo->pMsg, .len = pMsgInfo->msgLen, .handle = NULL};
  pMsgInfo->pMsg = NULL; // pMsg transferred to SMsgSendInfo management

  STscObj*      pTscObj = pRequest->pTscObj;
  SMsgSendInfo* pSendMsg = buildMsgInfoImpl(pRequest);

  if (pMsgInfo->msgType == TDMT_VND_SHOW_TABLES) {
    SShowReqInfo* pShowReqInfo = &pRequest->body.showInfo;
    if (pShowReqInfo->pArray == NULL) {
      pShowReqInfo->currentIndex = 0;  // set the first vnode/ then iterate the next vnode
      pShowReqInfo->pArray = pMsgInfo->pExtension;
    }
  }
  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &pMsgInfo->epSet, &transporterId, pSendMsg);

  tsem_wait(&pRequest->body.rspSem);
  return TSDB_CODE_SUCCESS;
}

int32_t getPlan(SRequestObj* pRequest, SQuery* pQuery, SQueryPlan** pPlan, SArray* pNodeList) {
  pRequest->type = pQuery->msgType;
  SPlanContext cxt = { .queryId = pRequest->requestId, .pAstRoot = pQuery->pRoot, .acctId = pRequest->pTscObj->acctId };
  return qCreateQueryPlan(&cxt, pPlan, pNodeList);
}

void setResSchemaInfo(SReqResultInfo* pResInfo, const SSchema* pSchema, int32_t numOfCols) {
  assert(pSchema != NULL && numOfCols > 0);

  pResInfo->numOfCols = numOfCols;
  pResInfo->fields = calloc(numOfCols, sizeof(pSchema[0]));

  for (int32_t i = 0; i < pResInfo->numOfCols; ++i) {
    pResInfo->fields[i].bytes = pSchema[i].bytes;
    pResInfo->fields[i].type = pSchema[i].type;
    tstrncpy(pResInfo->fields[i].name, pSchema[i].name, tListLen(pResInfo->fields[i].name));
  }
}


int32_t scheduleQuery(SRequestObj* pRequest, SQueryPlan* pDag, SArray* pNodeList) {
  void* pTransporter = pRequest->pTscObj->pAppInfo->pTransporter;

  SQueryResult res = {.code = 0, .numOfRows = 0, .msgSize = ERROR_MSG_BUF_DEFAULT_SIZE, .msg = pRequest->msgBuf};
  int32_t      code = schedulerExecJob(pTransporter, pNodeList, pDag, &pRequest->body.queryJob, pRequest->sqlstr, &res);
  if (code != TSDB_CODE_SUCCESS) {
    if (pRequest->body.queryJob != 0) {
      schedulerFreeJob(pRequest->body.queryJob);
    }

    pRequest->code = code;
    return pRequest->code;
  }

  if (TDMT_VND_SUBMIT == pRequest->type || TDMT_VND_CREATE_TABLE == pRequest->type) {
    pRequest->body.resInfo.numOfRows = res.numOfRows;

    if (pRequest->body.queryJob != 0) {
      schedulerFreeJob(pRequest->body.queryJob);
    }
  }

  pRequest->code = res.code;
  return pRequest->code;
}

TAOS_RES* taos_query_l(TAOS* taos, const char* sql, int sqlLen) {
  STscObj* pTscObj = (STscObj*)taos;
  if (sqlLen > (size_t)TSDB_MAX_ALLOWED_SQL_LEN) {
    tscError("sql string exceeds max length:%d", TSDB_MAX_ALLOWED_SQL_LEN);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    return NULL;
  }

  SRequestObj* pRequest = NULL;
  SQuery* pQuery = NULL;
  SArray* pNodeList = taosArrayInit(4, sizeof(struct SQueryNodeAddr));

  terrno = TSDB_CODE_SUCCESS;
  CHECK_CODE_GOTO(buildRequest(pTscObj, sql, sqlLen, &pRequest), _return);
  CHECK_CODE_GOTO(parseSql(pRequest, &pQuery), _return);

  if (pQuery->directRpc) {
    CHECK_CODE_GOTO(execDdlQuery(pRequest, pQuery), _return);
  } else {
    CHECK_CODE_GOTO(getPlan(pRequest, pQuery, &pRequest->body.pDag, pNodeList), _return);
    CHECK_CODE_GOTO(scheduleQuery(pRequest, pRequest->body.pDag, pNodeList), _return);
    pRequest->code = terrno;
  }

_return:
  taosArrayDestroy(pNodeList);
  qDestroyQuery(pQuery);
  if (NULL != pRequest && TSDB_CODE_SUCCESS != terrno) {
    pRequest->code = terrno;
  }

  return pRequest;
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

    taosGetFqdnPortFromEp(firstEp, &mgmtEpSet->eps[0]);
    mgmtEpSet->numOfEps++;
  }

  if (secondEp && secondEp[0] != 0) {
    if (strlen(secondEp) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }

    taosGetFqdnPortFromEp(secondEp, &mgmtEpSet->eps[mgmtEpSet->numOfEps]);
    mgmtEpSet->numOfEps++;
  }

  if (mgmtEpSet->numOfEps == 0) {
    terrno = TSDB_CODE_TSC_INVALID_FQDN;
    return -1;
  }

  return 0;
}

STscObj* taosConnectImpl(const char* user, const char* auth, const char* db, __taos_async_fn_t fp, void* param,
                         SAppInstInfo* pAppInfo) {
  STscObj* pTscObj = createTscObj(user, auth, db, pAppInfo);
  if (NULL == pTscObj) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pTscObj;
  }

  SRequestObj* pRequest = createRequest(pTscObj, fp, param, TDMT_MND_CONNECT);
  if (pRequest == NULL) {
    destroyTscObj(pTscObj);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  SMsgSendInfo* body = buildConnectMsg(pRequest);

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &pTscObj->pAppInfo->mgmtEp.epSet, &transporterId, body);

  tsem_wait(&pRequest->body.rspSem);
  if (pRequest->code != TSDB_CODE_SUCCESS) {
    const char* errorMsg =
        (pRequest->code == TSDB_CODE_RPC_FQDN_ERROR) ? taos_errstr(pRequest) : tstrerror(pRequest->code);
    printf("failed to connect to server, reason: %s\n\n", errorMsg);

    destroyRequest(pRequest);
    taos_close(pTscObj);
    pTscObj = NULL;
  } else {
    tscDebug("0x%" PRIx64 " connection is opening, connId:%d, dnodeConn:%p, reqId:0x%" PRIx64, pTscObj->id,
             pTscObj->connId, pTscObj->pAppInfo->pTransporter, pRequest->requestId);
    destroyRequest(pRequest);
  }

  return pTscObj;
}

static SMsgSendInfo* buildConnectMsg(SRequestObj* pRequest) {
  SMsgSendInfo* pMsgSendInfo = calloc(1, sizeof(SMsgSendInfo));
  if (pMsgSendInfo == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pMsgSendInfo->msgType = TDMT_MND_CONNECT;

  pMsgSendInfo->requestObjRefId = pRequest->self;
  pMsgSendInfo->requestId = pRequest->requestId;
  pMsgSendInfo->fp = handleRequestRspFp[TMSG_INDEX(pMsgSendInfo->msgType)];
  pMsgSendInfo->param = pRequest;

  SConnectReq connectReq = {0};
  STscObj*    pObj = pRequest->pTscObj;

  char* db = getDbOfConnection(pObj);
  if (db != NULL) {
    tstrncpy(connectReq.db, db, sizeof(connectReq.db));
  }
  tfree(db);

  connectReq.pid = htonl(appInfo.pid);
  connectReq.startTime = htobe64(appInfo.startTime);
  tstrncpy(connectReq.app, appInfo.appName, sizeof(connectReq.app));

  int32_t contLen = tSerializeSConnectReq(NULL, 0, &connectReq);
  void*   pReq = malloc(contLen);
  tSerializeSConnectReq(pReq, contLen, &connectReq);

  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgInfo.pData = pReq;
  return pMsgSendInfo;
}

static void destroySendMsgInfo(SMsgSendInfo* pMsgBody) {
  assert(pMsgBody != NULL);
  tfree(pMsgBody->msgInfo.pData);
  tfree(pMsgBody);
}
bool persistConnForSpecificMsg(void* parenct, tmsg_t msgType) {
  return msgType == TDMT_VND_QUERY_RSP || msgType == TDMT_VND_FETCH_RSP || msgType == TDMT_VND_RES_READY_RSP;
}
void processMsgFromServer(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SMsgSendInfo* pSendInfo = (SMsgSendInfo*)pMsg->ahandle;
  assert(pMsg->ahandle != NULL);

  if (pSendInfo->requestObjRefId != 0) {
    SRequestObj* pRequest = (SRequestObj*)taosAcquireRef(clientReqRefPool, pSendInfo->requestObjRefId);
    assert(pRequest->self == pSendInfo->requestObjRefId);

    pRequest->metric.rsp = taosGetTimestampMs();
    pRequest->code = pMsg->code;

    STscObj* pTscObj = pRequest->pTscObj;
    if (pEpSet) {
      if (!isEpsetEqual(&pTscObj->pAppInfo->mgmtEp.epSet, pEpSet)) {
        updateEpSet_s(&pTscObj->pAppInfo->mgmtEp, pEpSet);
      }
    }

    /*
     * There is not response callback function for submit response.
     * The actual inserted number of points is the first number.
     */
    int32_t elapsed = pRequest->metric.rsp - pRequest->metric.start;
    if (pMsg->code == TSDB_CODE_SUCCESS) {
      tscDebug("0x%" PRIx64 " message:%s, code:%s rspLen:%d, elapsed:%d ms, reqId:0x%" PRIx64, pRequest->self,
               TMSG_INFO(pMsg->msgType), tstrerror(pMsg->code), pMsg->contLen, elapsed, pRequest->requestId);
    } else {
      tscError("0x%" PRIx64 " SQL cmd:%s, code:%s rspLen:%d, elapsed time:%d ms, reqId:0x%" PRIx64, pRequest->self,
               TMSG_INFO(pMsg->msgType), tstrerror(pMsg->code), pMsg->contLen, elapsed, pRequest->requestId);
    }

    taosReleaseRef(clientReqRefPool, pSendInfo->requestObjRefId);
  }

  SDataBuf buf = {.len = pMsg->contLen, .pData = NULL, .handle = pMsg->handle};

  if (pMsg->contLen > 0) {
    buf.pData = calloc(1, pMsg->contLen);
    if (buf.pData == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      memcpy(buf.pData, pMsg->pCont, pMsg->contLen);
    }
  }

  pSendInfo->fp(pSendInfo->param, &buf, pMsg->code);
  rpcFreeCont(pMsg->pCont);
  destroySendMsgInfo(pSendInfo);
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

  return taos_connect_internal(ip, user, NULL, auth, db, port);
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

void* doFetchRow(SRequestObj* pRequest) {
  assert(pRequest != NULL);
  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;

  SEpSet epSet = {0};

  if (pResultInfo->pData == NULL || pResultInfo->current >= pResultInfo->numOfRows) {
    if (pRequest->type == TDMT_VND_QUERY) {
      // All data has returned to App already, no need to try again
      if (pResultInfo->completed) {
        return NULL;
      }

      SReqResultInfo* pResInfo = &pRequest->body.resInfo;
      int32_t         code = schedulerFetchRows(pRequest->body.queryJob, (void**)&pResInfo->pData);
      if (code != TSDB_CODE_SUCCESS) {
        pRequest->code = code;
        return NULL;
      }

      setQueryResultFromRsp(&pRequest->body.resInfo, (SRetrieveTableRsp*)pResInfo->pData);
      tscDebug("0x%" PRIx64 " fetch results, numOfRows:%d total Rows:%" PRId64 ", complete:%d, reqId:0x%" PRIx64,
               pRequest->self, pResInfo->numOfRows, pResInfo->totalRows, pResInfo->completed, pRequest->requestId);

      if (pResultInfo->numOfRows == 0) {
        return NULL;
      }

      goto _return;
    } else if (pRequest->type == TDMT_MND_SHOW) {
      pRequest->type = TDMT_MND_SHOW_RETRIEVE;
      epSet = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);
    } else if (pRequest->type == TDMT_VND_SHOW_TABLES) {
      pRequest->type = TDMT_VND_SHOW_TABLES_FETCH;
      SShowReqInfo* pShowReqInfo = &pRequest->body.showInfo;
      SVgroupInfo*  pVgroupInfo = taosArrayGet(pShowReqInfo->pArray, pShowReqInfo->currentIndex);

      epSet = pVgroupInfo->epSet;
    } else if (pRequest->type == TDMT_VND_SHOW_TABLES_FETCH) {
      pRequest->type = TDMT_VND_SHOW_TABLES;
      SShowReqInfo* pShowReqInfo = &pRequest->body.showInfo;
      pShowReqInfo->currentIndex += 1;
      if (pShowReqInfo->currentIndex >= taosArrayGetSize(pShowReqInfo->pArray)) {
        return NULL;
      }

      SVgroupInfo*     pVgroupInfo = taosArrayGet(pShowReqInfo->pArray, pShowReqInfo->currentIndex);
      SVShowTablesReq* pShowReq = calloc(1, sizeof(SVShowTablesReq));
      pShowReq->head.vgId = htonl(pVgroupInfo->vgId);

      pRequest->body.requestMsg.len = sizeof(SVShowTablesReq);
      pRequest->body.requestMsg.pData = pShowReq;

      SMsgSendInfo* body = buildMsgInfoImpl(pRequest);
      epSet = pVgroupInfo->epSet;

      int64_t  transporterId = 0;
      STscObj* pTscObj = pRequest->pTscObj;
      asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, body);
      tsem_wait(&pRequest->body.rspSem);

      pRequest->type = TDMT_VND_SHOW_TABLES_FETCH;
    } else if (pRequest->type == TDMT_MND_SHOW_RETRIEVE) {
      epSet = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

      if (pResultInfo->completed) {
        return NULL;
      }
    }

    SMsgSendInfo* body = buildMsgInfoImpl(pRequest);

    int64_t  transporterId = 0;
    STscObj* pTscObj = pRequest->pTscObj;
    asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, body);

    tsem_wait(&pRequest->body.rspSem);

    pResultInfo->current = 0;
    if (pResultInfo->numOfRows <= pResultInfo->current) {
      return NULL;
    }
  }

_return:

  for (int32_t i = 0; i < pResultInfo->numOfCols; ++i) {
    pResultInfo->row[i] = pResultInfo->pCol[i] + pResultInfo->fields[i].bytes * pResultInfo->current;
    if (IS_VAR_DATA_TYPE(pResultInfo->fields[i].type)) {
      pResultInfo->length[i] = varDataLen(pResultInfo->row[i]);
      pResultInfo->row[i] = varDataVal(pResultInfo->row[i]);
    }
  }

  pResultInfo->current += 1;
  return pResultInfo->row;
}

static void doPrepareResPtr(SReqResultInfo* pResInfo) {
  if (pResInfo->row == NULL) {
    pResInfo->row = calloc(pResInfo->numOfCols, POINTER_BYTES);
    pResInfo->pCol = calloc(pResInfo->numOfCols, POINTER_BYTES);
    pResInfo->length = calloc(pResInfo->numOfCols, sizeof(int32_t));
  }
}

void setResultDataPtr(SReqResultInfo* pResultInfo, TAOS_FIELD* pFields, int32_t numOfCols, int32_t numOfRows) {
  assert(numOfCols > 0 && pFields != NULL && pResultInfo != NULL);
  if (numOfRows == 0) {
    return;
  }

  // todo check for the failure of malloc
  doPrepareResPtr(pResultInfo);

  int32_t offset = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    pResultInfo->length[i] = pResultInfo->fields[i].bytes;
    pResultInfo->row[i] = (char*)(pResultInfo->pData + offset * pResultInfo->numOfRows);
    pResultInfo->pCol[i] = pResultInfo->row[i];
    offset += pResultInfo->fields[i].bytes;
  }
}

char* getDbOfConnection(STscObj* pObj) {
  char* p = NULL;
  pthread_mutex_lock(&pObj->mutex);
  size_t len = strlen(pObj->db);
  if (len > 0) {
    p = strndup(pObj->db, tListLen(pObj->db));
  }

  pthread_mutex_unlock(&pObj->mutex);
  return p;
}

void setConnectionDB(STscObj* pTscObj, const char* db) {
  assert(db != NULL && pTscObj != NULL);
  pthread_mutex_lock(&pTscObj->mutex);
  tstrncpy(pTscObj->db, db, tListLen(pTscObj->db));
  pthread_mutex_unlock(&pTscObj->mutex);
}

void setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp) {
  assert(pResultInfo != NULL && pRsp != NULL);

  pResultInfo->pRspMsg = (const char*)pRsp;
  pResultInfo->pData = (void*)pRsp->data;
  pResultInfo->numOfRows = htonl(pRsp->numOfRows);
  pResultInfo->current = 0;
  pResultInfo->completed = (pRsp->completed == 1);

  pResultInfo->totalRows += pResultInfo->numOfRows;
  setResultDataPtr(pResultInfo, pResultInfo->fields, pResultInfo->numOfCols, pResultInfo->numOfRows);
}
