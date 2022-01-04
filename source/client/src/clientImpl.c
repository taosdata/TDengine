
#include "clientInt.h"
#include "clientLog.h"
#include "tdef.h"
#include "tep.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tnote.h"
#include "tpagedfile.h"
#include "tref.h"
#include "parser.h"
#include "planner.h"
#include "scheduler.h"

#define CHECK_CODE_GOTO(expr, lable) \
  do {                               \
    int32_t code = expr;             \
    if (TSDB_CODE_SUCCESS != code) { \
      terrno = code;                 \
      goto lable;                    \
    }                                \
  } while (0)

static int32_t initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet);
static SMsgSendInfo* buildConnectMsg(SRequestObj *pRequest);
static void destroySendMsgInfo(SMsgSendInfo* pMsgBody);

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

static bool validateUserName(const char* user) {
  return stringLengthCheck(user, TSDB_USER_LEN - 1);
}

static bool validatePassword(const char* passwd) {
  return stringLengthCheck(passwd, TSDB_PASSWORD_LEN - 1);
}

static bool validateDbName(const char* db) {
  return stringLengthCheck(db, TSDB_DB_NAME_LEN - 1);
}

static char* getClusterKey(const char* user, const char* auth, const char* ip, int32_t port) {
  char key[512] = {0};
  snprintf(key, sizeof(key), "%s:%s:%s:%d", user, auth, ip, port);
  return strdup(key);
}

static STscObj* taosConnectImpl(const char *ip, const char *user, const char *auth, const char *db, uint16_t port, __taos_async_fn_t fp, void *param, SAppInstInfo* pAppInfo);

TAOS *taos_connect_internal(const char *ip, const char *user, const char *pass, const char *auth, const char *db, uint16_t port) {
  if (taos_init() != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  if (!validateUserName(user)) {
    terrno = TSDB_CODE_TSC_INVALID_USER_LENGTH;
    return NULL;
  }

  char tmp[TSDB_DB_NAME_LEN] = {0};
  if (db != NULL) {
    if(!validateDbName(db)) {
      terrno = TSDB_CODE_TSC_INVALID_DB_LENGTH;
      return NULL;
    }

    tstrncpy(tmp, db, sizeof(tmp));
    strdequote(tmp);
  }

  char secretEncrypt[32] = {0};
  if (auth == NULL) {
    if (!validatePassword(pass)) {
      terrno = TSDB_CODE_TSC_INVALID_PASS_LENGTH;
      return NULL;
    }

    taosEncryptPass_c((uint8_t *)pass, strlen(pass), secretEncrypt);
  } else {
    tstrncpy(secretEncrypt, auth, tListLen(secretEncrypt));
  }

  SCorEpSet epSet = {0};
  if (ip) {
    if (initEpSetFromCfg(ip, NULL, &epSet) < 0) {
      return NULL;
    }

    if (port) {
      epSet.epSet.port[0] = port;
    }
  } else {
    if (initEpSetFromCfg(tsFirst, tsSecond, &epSet) < 0) {
      return NULL;
    }
  }

  char* key = getClusterKey(user, secretEncrypt, ip, port);

  SAppInstInfo** pInst = taosHashGet(appInfo.pInstMap, key, strlen(key));
  if (pInst == NULL) {
    SAppInstInfo* p = calloc(1, sizeof(struct SAppInstInfo));
    p->mgmtEp       = epSet;
    p->pTransporter = openTransporter(user, secretEncrypt, tsNumOfCores);
    taosHashPut(appInfo.pInstMap, key, strlen(key), &p, POINTER_BYTES);

    pInst = &p;
  }

  tfree(key);
  return taosConnectImpl(ip, user, &secretEncrypt[0], db, port, NULL, NULL, *pInst);
}

int32_t buildRequest(STscObj *pTscObj, const char *sql, int sqlLen, SRequestObj** pRequest) {
  *pRequest = createRequest(pTscObj, NULL, NULL, TSDB_SQL_SELECT);
  if (*pRequest == NULL) {
    tscError("failed to malloc sqlObj");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  (*pRequest)->sqlstr = malloc(sqlLen + 1);
  if ((*pRequest)->sqlstr == NULL) {
    tscError("0x%"PRIx64" failed to prepare sql string buffer", (*pRequest)->self);
    (*pRequest)->msgBuf = strdup("failed to prepare sql string buffer");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  strntolower((*pRequest)->sqlstr, sql, (int32_t)sqlLen);
  (*pRequest)->sqlstr[sqlLen] = 0;
  (*pRequest)->sqlLen = sqlLen;

  tscDebugL("0x%"PRIx64" SQL: %s, reqId:0x"PRIx64, (*pRequest)->self, (*pRequest)->sqlstr, (*pRequest)->requestId);
  return TSDB_CODE_SUCCESS;
}

int32_t parseSql(SRequestObj* pRequest, SQueryNode** pQuery) {
  STscObj* pTscObj = pRequest->pTscObj;

  SParseContext cxt = {
    .ctx = {.requestId = pRequest->requestId, .acctId = pTscObj->acctId, .db = getConnectionDB(pTscObj), .pTransporter = pTscObj->pTransporter},
    .pSql   = pRequest->sqlstr,
    .sqlLen = pRequest->sqlLen,
    .pMsg   = pRequest->msgBuf,
    .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE
  };

  cxt.ctx.mgmtEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  int32_t code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &cxt.ctx.pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(cxt.ctx.db);
    return code;
  }

  code = qParseQuerySql(&cxt, pQuery);

  tfree(cxt.ctx.db);
  return code;
}

int32_t execDdlQuery(SRequestObj* pRequest, SQueryNode* pQuery) {
  SDclStmtInfo* pDcl = (SDclStmtInfo*)pQuery;
  pRequest->type = pDcl->msgType;
  pRequest->body.requestMsg = (SDataBuf){.pData = pDcl->pMsg, .len = pDcl->msgLen};

  STscObj* pTscObj = pRequest->pTscObj;
  SMsgSendInfo* pSendMsg = buildMsgInfoImpl(pRequest);

  int64_t transporterId = 0;
  if (pDcl->msgType == TDMT_VND_CREATE_TABLE || pDcl->msgType == TDMT_VND_SHOW_TABLES) {
    if (pDcl->msgType == TDMT_VND_SHOW_TABLES) {
      SShowReqInfo* pShowReqInfo = &pRequest->body.showInfo;
      if (pShowReqInfo->pArray == NULL) {
        pShowReqInfo->currentIndex = 0;
        pShowReqInfo->pArray = pDcl->pExtension;
      }
    }
    asyncSendMsgToServer(pTscObj->pTransporter, &pDcl->epSet, &transporterId, pSendMsg);
  } else {
    SEpSet* pEpSet = &pTscObj->pAppInfo->mgmtEp.epSet;
    asyncSendMsgToServer(pTscObj->pTransporter, pEpSet, &transporterId, pSendMsg);
  }

  tsem_wait(&pRequest->body.rspSem);
  destroySendMsgInfo(pSendMsg);
  return TSDB_CODE_SUCCESS;
}

int32_t getPlan(SRequestObj* pRequest, SQueryNode* pQuery, SQueryDag** pDag) {
  pRequest->type = pQuery->type;
  return qCreateQueryDag(pQuery, pDag, pRequest->requestId);
}

int32_t scheduleQuery(SRequestObj* pRequest, SQueryDag* pDag, void** pJob) {
  if (TSDB_SQL_INSERT == pRequest->type || TSDB_SQL_CREATE_TABLE == pRequest->type) {
    return scheduleExecJob(pRequest->pTscObj->pTransporter, NULL/*todo appInfo.xxx*/, pDag, pJob, &pRequest->affectedRows);
  }

  return scheduleAsyncExecJob(pRequest->pTscObj->pTransporter, NULL/*todo appInfo.xxx*/, pDag, pJob);
}

TAOS_RES *tmq_create_topic(TAOS* taos, const char* name, const char* sql, int sqlLen) {
  STscObj* pTscObj = (STscObj*)taos;
  SRequestObj* pRequest = NULL;
  SQueryNode*  pQuery = NULL;
  SQueryDag*   pDag = NULL;
  char *dagStr = NULL;

  terrno = TSDB_CODE_SUCCESS;

  CHECK_CODE_GOTO(buildRequest(pTscObj, sql, sqlLen, &pRequest), _return);

//temporary disabled until planner ready
#if 0
  CHECK_CODE_GOTO(parseSql(pRequest, &pQuery), _return);
  //TODO: check sql valid

  CHECK_CODE_GOTO(qCreateQueryDag(pQuery, &pDag), _return);

  dagStr = qDagToString(pDag);
  if(dagStr == NULL) {
    //TODO
  }
#endif

  SCMCreateTopicReq req = {
    .name = (char*)name,
    .igExists = 0,
    /*.physicalPlan = dagStr,*/
    .physicalPlan = (char*)sql,
    .logicalPlan = "",
  };

  int tlen = tSerializeSCMCreateTopicReq(NULL, &req);
  void* buf = malloc(tlen);
  if(buf == NULL) {
    goto _return;
  }
  void* abuf = buf;
  tSerializeSCMCreateTopicReq(&abuf, &req);
  /*printf("formatted: %s\n", dagStr);*/

  pRequest->body.requestMsg = (SDataBuf){ .pData = buf, .len = tlen };

  SMsgSendInfo* body = buildMsgInfoImpl(pRequest);
  SEpSet* pEpSet = &pTscObj->pAppInfo->mgmtEp.epSet;

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pTransporter, pEpSet, &transporterId, body);

  tsem_wait(&pRequest->body.rspSem);

_return:
  qDestroyQuery(pQuery);
  qDestroyQueryDag(pDag); 
  destroySendMsgInfo(body);
  if (pRequest != NULL && terrno != TSDB_CODE_SUCCESS) {
    pRequest->code = terrno;
  }
  return pRequest;
}

TAOS_RES *taos_query_l(TAOS *taos, const char *sql, int sqlLen) {
  STscObj *pTscObj = (STscObj *)taos;
  if (sqlLen > (size_t) tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    return NULL;
  }

  nPrintTsc("%s", sql)

  SRequestObj* pRequest = NULL;
  SQueryNode* pQuery = NULL;
  SQueryDag* pDag = NULL;
  void* pJob = NULL;

  terrno = TSDB_CODE_SUCCESS;
  CHECK_CODE_GOTO(buildRequest(pTscObj, sql, sqlLen, &pRequest), _return);
  CHECK_CODE_GOTO(parseSql(pRequest, &pQuery), _return);

  if (qIsDdlQuery(pQuery)) {
    CHECK_CODE_GOTO(execDdlQuery(pRequest, pQuery), _return);
  } else {
    CHECK_CODE_GOTO(getPlan(pRequest, pQuery, &pDag), _return);
    CHECK_CODE_GOTO(scheduleQuery(pRequest, pDag, &pJob), _return);
  }

_return:
  qDestroyQuery(pQuery);
  qDestroyQueryDag(pDag);
  if (NULL != pRequest && TSDB_CODE_SUCCESS != terrno) {
    pRequest->code = terrno;
  }
  return pRequest;
}

int initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet) {
  pEpSet->version = 0;

  // init mnode ip set
  SEpSet *mgmtEpSet   = &(pEpSet->epSet);
  mgmtEpSet->numOfEps = 0;
  mgmtEpSet->inUse    = 0;

  if (firstEp && firstEp[0] != 0) {
    if (strlen(firstEp) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }

    taosGetFqdnPortFromEp(firstEp, mgmtEpSet->fqdn[0], &(mgmtEpSet->port[0]));
    mgmtEpSet->numOfEps++;
  }

  if (secondEp && secondEp[0] != 0) {
    if (strlen(secondEp) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }

    taosGetFqdnPortFromEp(secondEp, mgmtEpSet->fqdn[mgmtEpSet->numOfEps], &(mgmtEpSet->port[mgmtEpSet->numOfEps]));
    mgmtEpSet->numOfEps++;
  }

  if (mgmtEpSet->numOfEps == 0) {
    terrno = TSDB_CODE_TSC_INVALID_FQDN;
    return -1;
  }

  return 0;
}

STscObj* taosConnectImpl(const char *ip, const char *user, const char *auth, const char *db, uint16_t port, __taos_async_fn_t fp, void *param, SAppInstInfo* pAppInfo) {
  STscObj *pTscObj = createTscObj(user, auth, db, pAppInfo);
  if (NULL == pTscObj) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pTscObj;
  }

  SRequestObj *pRequest = createRequest(pTscObj, fp, param, TDMT_MND_CONNECT);
  if (pRequest == NULL) {
    destroyTscObj(pTscObj);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  SMsgSendInfo* body = buildConnectMsg(pRequest);

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pTransporter, &pTscObj->pAppInfo->mgmtEp.epSet, &transporterId, body);

  tsem_wait(&pRequest->body.rspSem);
  destroySendMsgInfo(body);

  if (pRequest->code != TSDB_CODE_SUCCESS) {
    const char *errorMsg = (pRequest->code == TSDB_CODE_RPC_FQDN_ERROR) ? taos_errstr(pRequest) : tstrerror(terrno);
    printf("failed to connect to server, reason: %s\n\n", errorMsg);

    destroyRequest(pRequest);
    taos_close(pTscObj);
    pTscObj = NULL;
  } else {
    tscDebug("0x%"PRIx64" connection is opening, connId:%d, dnodeConn:%p, reqId:0x%"PRIx64, pTscObj->id, pTscObj->connId, pTscObj->pTransporter, pRequest->requestId);
    destroyRequest(pRequest);
  }

  return pTscObj;
}

static SMsgSendInfo* buildConnectMsg(SRequestObj *pRequest) {
  SMsgSendInfo *pMsgSendInfo = calloc(1, sizeof(SMsgSendInfo));
  if (pMsgSendInfo == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pMsgSendInfo->msgType         = TDMT_MND_CONNECT;
  pMsgSendInfo->msgInfo.len     = sizeof(SConnectMsg);
  pMsgSendInfo->requestObjRefId = pRequest->self;
  pMsgSendInfo->requestId       = pRequest->requestId;
  pMsgSendInfo->fp              = handleRequestRspFp[TMSG_INDEX(pMsgSendInfo->msgType)];
  pMsgSendInfo->param           = pRequest;

  SConnectMsg *pConnect = calloc(1, sizeof(SConnectMsg));
  if (pConnect == NULL) {
    tfree(pMsgSendInfo);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  STscObj *pObj = pRequest->pTscObj;

  char* db = getConnectionDB(pObj);
  tstrncpy(pConnect->db, db, sizeof(pConnect->db));
  tfree(db);

  pConnect->pid = htonl(appInfo.pid);
  pConnect->startTime = htobe64(appInfo.startTime);
  tstrncpy(pConnect->app, appInfo.appName, tListLen(pConnect->app));

  pMsgSendInfo->msgInfo.pData = pConnect;
  return pMsgSendInfo;
}

static void destroySendMsgInfo(SMsgSendInfo* pMsgBody) {
  assert(pMsgBody != NULL);
  tfree(pMsgBody->msgInfo.pData);
  tfree(pMsgBody);
}

void processMsgFromServer(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SMsgSendInfo *pSendInfo = (SMsgSendInfo *) pMsg->ahandle;
  assert(pMsg->ahandle != NULL);

  if (pSendInfo->requestObjRefId != 0) {
    SRequestObj *pRequest = (SRequestObj *)taosAcquireRef(clientReqRefPool, pSendInfo->requestObjRefId);
    assert(pRequest->self == pSendInfo->requestObjRefId);

    pRequest->metric.rsp = taosGetTimestampMs();
    pRequest->code = pMsg->code;

    STscObj *pTscObj = pRequest->pTscObj;
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
      tscDebug("0x%" PRIx64 " message:%s, code:%s rspLen:%d, elapsed:%d ms, reqId:0x%"PRIx64, pRequest->self,
          TMSG_INFO(pMsg->msgType), tstrerror(pMsg->code), pMsg->contLen, elapsed, pRequest->requestId);
    } else {
      tscError("0x%" PRIx64 " SQL cmd:%s, code:%s rspLen:%d, elapsed time:%d ms, reqId:0x%"PRIx64, pRequest->self,
          TMSG_INFO(pMsg->msgType), tstrerror(pMsg->code), pMsg->contLen, elapsed, pRequest->requestId);
    }

    taosReleaseRef(clientReqRefPool, pSendInfo->requestObjRefId);
  }

  SDataBuf buf = {.len = pMsg->contLen};
  buf.pData = calloc(1, pMsg->contLen);
  if (buf.pData == NULL) {
    terrno     = TSDB_CODE_OUT_OF_MEMORY;
    pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    memcpy(buf.pData, pMsg->pCont, pMsg->contLen);
  }

  pSendInfo->fp(pSendInfo->param, &buf, pMsg->code);
  rpcFreeCont(pMsg->pCont);
}

TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) {
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

TAOS *taos_connect_l(const char *ip, int ipLen, const char *user, int userLen, const char *pass, int passLen, const char *db, int dbLen, uint16_t port) {
  char ipStr[TSDB_EP_LEN]      = {0};
  char dbStr[TSDB_DB_NAME_LEN] = {0};
  char userStr[TSDB_USER_LEN]  = {0};
  char passStr[TSDB_PASSWORD_LEN]   = {0};

  strncpy(ipStr,   ip,   MIN(TSDB_EP_LEN - 1, ipLen));
  strncpy(userStr, user, MIN(TSDB_USER_LEN - 1, userLen));
  strncpy(passStr, pass, MIN(TSDB_PASSWORD_LEN - 1, passLen));
  strncpy(dbStr,   db,   MIN(TSDB_DB_NAME_LEN - 1, dbLen));
  return taos_connect(ipStr, userStr, passStr, dbStr, port);
}

void* doFetchRow(SRequestObj* pRequest) {
  assert(pRequest != NULL);
  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;

  if (pResultInfo->pData == NULL || pResultInfo->current >= pResultInfo->numOfRows) {
    if (pRequest->type == TDMT_MND_SHOW) {
      pRequest->type = TDMT_MND_SHOW_RETRIEVE;
    } else if (pRequest->type == TDMT_VND_SHOW_TABLES) {
      pRequest->type = TDMT_VND_SHOW_TABLES_FETCH;
    } else if (pRequest->type == TDMT_VND_SHOW_TABLES_FETCH) {
      pRequest->type = TDMT_VND_SHOW_TABLES;
      SShowReqInfo* pShowReqInfo = &pRequest->body.showInfo;
      pShowReqInfo->currentIndex += 1;
      if (pShowReqInfo->currentIndex >= taosArrayGetSize(pShowReqInfo->pArray)) {
        return NULL;
      }

      SVgroupInfo* pVgroupInfo = taosArrayGet(pShowReqInfo->pArray, pShowReqInfo->currentIndex);
      SVShowTablesReq* pShowReq = calloc(1, sizeof(SVShowTablesReq));
      pShowReq->head.vgId = htonl(pVgroupInfo->vgId);

      pRequest->body.requestMsg.len = sizeof(SVShowTablesReq);
      pRequest->body.requestMsg.pData = pShowReq;

      SMsgSendInfo* body = buildMsgInfoImpl(pRequest);

      int64_t  transporterId = 0;
      STscObj *pTscObj = pRequest->pTscObj;
      asyncSendMsgToServer(pTscObj->pTransporter, &pTscObj->pAppInfo->mgmtEp.epSet, &transporterId, body);

      tsem_wait(&pRequest->body.rspSem);
      destroySendMsgInfo(body);

      pRequest->type = TDMT_VND_SHOW_TABLES_FETCH;
    }

    SMsgSendInfo* body = buildMsgInfoImpl(pRequest);

    int64_t  transporterId = 0;
    STscObj *pTscObj = pRequest->pTscObj;
    asyncSendMsgToServer(pTscObj->pTransporter, &pTscObj->pAppInfo->mgmtEp.epSet, &transporterId, body);

    tsem_wait(&pRequest->body.rspSem);
    destroySendMsgInfo(body);

    pResultInfo->current = 0;
    if (pResultInfo->numOfRows <= pResultInfo->current) {
      return NULL;
    }
  }

  for(int32_t i = 0; i < pResultInfo->numOfCols; ++i) {
    pResultInfo->row[i] = pResultInfo->pCol[i] + pResultInfo->fields[i].bytes * pResultInfo->current;
    if (IS_VAR_DATA_TYPE(pResultInfo->fields[i].type)) {
      pResultInfo->length[i] = varDataLen(pResultInfo->row[i]);
      pResultInfo->row[i] = varDataVal(pResultInfo->row[i]);
    }
  }

  pResultInfo->current += 1;
  return pResultInfo->row;
}

void setResultDataPtr(SReqResultInfo* pResultInfo, TAOS_FIELD* pFields, int32_t numOfCols, int32_t numOfRows) {
  assert(numOfCols > 0 && pFields != NULL && pResultInfo != NULL);
  if (numOfRows == 0) {
    return;
  }

  int32_t offset = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    pResultInfo->length[i] = pResultInfo->fields[i].bytes;
    pResultInfo->row[i]    = (char*) (pResultInfo->pData + offset * pResultInfo->numOfRows);
    pResultInfo->pCol[i]   = pResultInfo->row[i];
    offset += pResultInfo->fields[i].bytes;
  }
}

char* getConnectionDB(STscObj* pObj) {
  char *p = NULL;
  pthread_mutex_lock(&pObj->mutex);
  p = strndup(pObj->db, tListLen(pObj->db));
  pthread_mutex_unlock(&pObj->mutex);

  return p;
}

void setConnectionDB(STscObj* pTscObj, const char* db) {
  assert(db != NULL && pTscObj != NULL);
  pthread_mutex_lock(&pTscObj->mutex);
  tstrncpy(pTscObj->db, db, tListLen(pTscObj->db));
  pthread_mutex_unlock(&pTscObj->mutex);
}

