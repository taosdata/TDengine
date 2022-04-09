
#include "clientInt.h"
#include "clientLog.h"
#include "command.h"
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
static int32_t       setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp);

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

  taosThreadMutexLock(&appInfo.mutex);

  pInst = taosHashGet(appInfo.pInstMap, key, strlen(key));
  SAppInstInfo* p = NULL;
  if (pInst == NULL) {
    p = taosMemoryCalloc(1, sizeof(struct SAppInstInfo));
    p->mgmtEp = epSet;
    p->pTransporter = openTransporter(user, secretEncrypt, tsNumOfCores);
    p->pAppHbMgr = appHbMgrInit(p, key);
    taosHashPut(appInfo.pInstMap, key, strlen(key), &p, POINTER_BYTES);

    pInst = &p;
  }

  taosThreadMutexUnlock(&appInfo.mutex);

  taosMemoryFreeClear(key);
  return taosConnectImpl(user, &secretEncrypt[0], localDb, NULL, NULL, *pInst);
}

int32_t buildRequest(STscObj* pTscObj, const char* sql, int sqlLen, SRequestObj** pRequest) {
  *pRequest = createRequest(pTscObj, NULL, NULL, TSDB_SQL_SELECT);
  if (*pRequest == NULL) {
    tscError("failed to malloc sqlObj");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  (*pRequest)->sqlstr = taosMemoryMalloc(sqlLen + 1);
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

int32_t parseSql(SRequestObj* pRequest, bool topicQuery, SQuery** pQuery) {
  STscObj* pTscObj = pRequest->pTscObj;

  SParseContext cxt = {
      .requestId = pRequest->requestId,
      .acctId = pTscObj->acctId,
      .db = pRequest->pDb,
      .topicQuery = topicQuery,
      .pSql = pRequest->sqlstr,
      .sqlLen = pRequest->sqlLen,
      .pMsg = pRequest->msgBuf,
      .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
      .pTransporter = pTscObj->pAppInfo->pTransporter,
  };

  cxt.mgmtEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  int32_t code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &cxt.pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = qParseQuerySql(&cxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    if ((*pQuery)->haveResultSet) {
      setResSchemaInfo(&pRequest->body.resInfo, (*pQuery)->pResSchema, (*pQuery)->numOfResCols);
    }

    TSWAP(pRequest->dbList, (*pQuery)->pDbList, SArray*);
    TSWAP(pRequest->tableList, (*pQuery)->pTableList, SArray*);
  }

  return code;
}

int32_t execLocalCmd(SRequestObj* pRequest, SQuery* pQuery) {
  SRetrieveTableRsp* pRsp = NULL;
  int32_t code = qExecCommand(pQuery->pRoot, &pRsp);
  if (TSDB_CODE_SUCCESS == code && NULL != pRsp) {
    code = setQueryResultFromRsp(&pRequest->body.resInfo, pRsp);
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
  SPlanContext cxt = {
    .queryId = pRequest->requestId,
    .acctId = pRequest->pTscObj->acctId,
    .mgmtEpSet = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp),
    .pAstRoot = pQuery->pRoot,
    .showRewrite = pQuery->showRewrite
  };
  int32_t  code = qCreateQueryPlan(&cxt, pPlan, pNodeList);
  if (code != 0) {
    return code;
  }
  return code;
}

void setResSchemaInfo(SReqResultInfo* pResInfo, const SSchema* pSchema, int32_t numOfCols) {
  assert(pSchema != NULL && numOfCols > 0);

  pResInfo->numOfCols = numOfCols;
  pResInfo->fields = taosMemoryCalloc(numOfCols, sizeof(TAOS_FIELD));
  pResInfo->userFields = taosMemoryCalloc(numOfCols, sizeof(TAOS_FIELD));

  for (int32_t i = 0; i < pResInfo->numOfCols; ++i) {
    pResInfo->fields[i].bytes = pSchema[i].bytes;
    pResInfo->fields[i].type  = pSchema[i].type;

    pResInfo->userFields[i].bytes = pSchema[i].bytes;
    pResInfo->userFields[i].type  = pSchema[i].type;

    if (pSchema[i].type == TSDB_DATA_TYPE_VARCHAR) {
      pResInfo->userFields[i].bytes -= VARSTR_HEADER_SIZE;
    } else if (pSchema[i].type == TSDB_DATA_TYPE_NCHAR) {
      pResInfo->userFields[i].bytes = (pResInfo->userFields[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
    }

    tstrncpy(pResInfo->fields[i].name, pSchema[i].name, tListLen(pResInfo->fields[i].name));
    tstrncpy(pResInfo->userFields[i].name, pSchema[i].name, tListLen(pResInfo->userFields[i].name));
  }
}

int32_t scheduleQuery(SRequestObj* pRequest, SQueryPlan* pDag, SArray* pNodeList) {
  void* pTransporter = pRequest->pTscObj->pAppInfo->pTransporter;

  SQueryResult res = {.code = 0, .numOfRows = 0, .msgSize = ERROR_MSG_BUF_DEFAULT_SIZE, .msg = pRequest->msgBuf};
  int32_t      code = schedulerExecJob(pTransporter, pNodeList, pDag, &pRequest->body.queryJob, pRequest->sqlstr, pRequest->metric.start, &res);
  if (code != TSDB_CODE_SUCCESS) {
    if (pRequest->body.queryJob != 0) {
      schedulerFreeJob(pRequest->body.queryJob);
    }

    pRequest->code = code;
    terrno = code;
    return pRequest->code;
  }

  if (TDMT_VND_SUBMIT == pRequest->type || TDMT_VND_CREATE_TABLE == pRequest->type) {
    pRequest->body.resInfo.numOfRows = res.numOfRows;

    if (pRequest->body.queryJob != 0) {
      schedulerFreeJob(pRequest->body.queryJob);
    }
  }

  pRequest->code = res.code;
  terrno = res.code;  
  return pRequest->code;
}

SRequestObj* execQueryImpl(STscObj* pTscObj, const char* sql, int sqlLen) {
  SRequestObj* pRequest = NULL;
  SQuery* pQuery = NULL;
  SArray* pNodeList = taosArrayInit(4, sizeof(struct SQueryNodeAddr));

  int32_t code = buildRequest(pTscObj, sql, sqlLen, &pRequest);
  if (TSDB_CODE_SUCCESS == code) {
    code = parseSql(pRequest, false, &pQuery);
  }

  if (TSDB_CODE_SUCCESS == code) {
    switch (pQuery->execMode) {
      case QUERY_EXEC_MODE_LOCAL:
        code = execLocalCmd(pRequest, pQuery);
        break;
      case QUERY_EXEC_MODE_RPC:
        code = execDdlQuery(pRequest, pQuery);
        break;
      case QUERY_EXEC_MODE_SCHEDULE:
        code = getPlan(pRequest, pQuery, &pRequest->body.pDag, pNodeList);
        if (TSDB_CODE_SUCCESS == code) {
          code = scheduleQuery(pRequest, pRequest->body.pDag, pNodeList);
        }
        break;
      case QUERY_EXEC_MODE_EMPTY_RESULT:
        pRequest->type = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
        break;
      default:
        break;
    }
  }

  taosArrayDestroy(pNodeList);
  qDestroyQuery(pQuery);
  if (NULL != pRequest && TSDB_CODE_SUCCESS != code) {
    pRequest->code = terrno;
  }

  return pRequest;
}

int32_t refreshMeta(STscObj* pTscObj, SRequestObj* pRequest) {
  SCatalog *pCatalog = NULL;
  int32_t code = 0;
  int32_t dbNum = taosArrayGetSize(pRequest->dbList);
  int32_t tblNum = taosArrayGetSize(pRequest->tableList);

  if (dbNum <= 0 && tblNum <= 0) {
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SEpSet epset = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  for (int32_t i = 0; i < dbNum; ++i) {
    char *dbFName = taosArrayGet(pRequest->dbList, i);
    
    code = catalogRefreshDBVgInfo(pCatalog, pTscObj->pAppInfo->pTransporter, &epset, dbFName);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  for (int32_t i = 0; i < tblNum; ++i) {
    SName *tableName = taosArrayGet(pRequest->tableList, i);

    code = catalogRefreshTableMeta(pCatalog, pTscObj->pAppInfo->pTransporter, &epset, tableName, -1);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return code;
}


SRequestObj* execQuery(STscObj* pTscObj, const char* sql, int sqlLen) {
  SRequestObj* pRequest = NULL;
  int32_t retryNum = 0;
  int32_t code = 0;

  while (retryNum++ < REQUEST_MAX_TRY_TIMES) {
    pRequest = execQueryImpl(pTscObj, sql, sqlLen);
    if (TSDB_CODE_SUCCESS == pRequest->code || !NEED_CLIENT_HANDLE_ERROR(pRequest->code)) {
      break;
    }

    code = refreshMeta(pTscObj, pRequest);
    if (code) {
      pRequest->code = code;
      break;
    }

    destroyRequest(pRequest);
  }
  
  return pRequest;
}

TAOS_RES* taos_query_l(TAOS* taos, const char* sql, int sqlLen) {
  STscObj* pTscObj = (STscObj*)taos;
  if (sqlLen > (size_t)TSDB_MAX_ALLOWED_SQL_LEN) {
    tscError("sql string exceeds max length:%d", TSDB_MAX_ALLOWED_SQL_LEN);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    return NULL;
  }

  return execQuery(pTscObj, sql, sqlLen);
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
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
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
  taosMemoryFreeClear(db);

  connectReq.pid = htonl(appInfo.pid);
  connectReq.startTime = htobe64(appInfo.startTime);
  tstrncpy(connectReq.app, appInfo.appName, sizeof(connectReq.app));

  int32_t contLen = tSerializeSConnectReq(NULL, 0, &connectReq);
  void*   pReq = taosMemoryMalloc(contLen);
  tSerializeSConnectReq(pReq, contLen, &connectReq);

  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgInfo.pData = pReq;
  return pMsgSendInfo;
}

static void destroySendMsgInfo(SMsgSendInfo* pMsgBody) {
  assert(pMsgBody != NULL);
  taosMemoryFreeClear(pMsgBody->msgInfo.pData);
  taosMemoryFreeClear(pMsgBody);
}

bool persistConnForSpecificMsg(void* parenct, tmsg_t msgType) {
  return msgType == TDMT_VND_QUERY_RSP || msgType == TDMT_VND_FETCH_RSP || msgType == TDMT_VND_RES_READY_RSP || msgType == TDMT_VND_QUERY_HEARTBEAT_RSP;
}

void processMsgFromServer(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SMsgSendInfo* pSendInfo = (SMsgSendInfo*)pMsg->ahandle;
  assert(pMsg->ahandle != NULL);

  if (pSendInfo->requestObjRefId != 0) {
    SRequestObj* pRequest = (SRequestObj*)taosAcquireRef(clientReqRefPool, pSendInfo->requestObjRefId);
    assert(pRequest->self == pSendInfo->requestObjRefId);

    pRequest->metric.rsp = taosGetTimestampMs();

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
    buf.pData = taosMemoryCalloc(1, pMsg->contLen);
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

static void doSetOneRowPtr(SReqResultInfo* pResultInfo) {
  for (int32_t i = 0; i < pResultInfo->numOfCols; ++i) {
    SResultColumn* pCol = &pResultInfo->pCol[i];

    int32_t type = pResultInfo->fields[i].type;
    int32_t bytes = pResultInfo->fields[i].bytes;

    if (IS_VAR_DATA_TYPE(type)) {
      if (pCol->offset[pResultInfo->current] != -1) {
        char* pStart = pResultInfo->pCol[i].offset[pResultInfo->current] + pResultInfo->pCol[i].pData;

        pResultInfo->length[i] = varDataLen(pStart);
        pResultInfo->row[i] = varDataVal(pStart);
      } else {
        pResultInfo->row[i] = NULL;
      }
    } else {
      if (!colDataIsNull_f(pCol->nullbitmap, pResultInfo->current)) {
        pResultInfo->row[i] = pResultInfo->pCol[i].pData + bytes * pResultInfo->current;
      } else {
        pResultInfo->row[i] = NULL;
      }
    }
  }
}

void* doFetchRow(SRequestObj* pRequest, bool setupOneRowPtr) {
  assert(pRequest != NULL);
  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;

  SEpSet epSet = {0};

  if (pResultInfo->pData == NULL || pResultInfo->current >= pResultInfo->numOfRows) {
    if (pRequest->type == TDMT_VND_QUERY) {
      // All data has returned to App already, no need to try again
      if (pResultInfo->completed) {
        pResultInfo->numOfRows = 0;
        return NULL;
      }

      SReqResultInfo* pResInfo = &pRequest->body.resInfo;
      pRequest->code = schedulerFetchRows(pRequest->body.queryJob, (void**)&pResInfo->pData);
      if (pRequest->code != TSDB_CODE_SUCCESS) {
        pResultInfo->numOfRows = 0;
        return NULL;
      }

      pRequest->code = setQueryResultFromRsp(&pRequest->body.resInfo, (SRetrieveTableRsp*)pResInfo->pData);
      if (pRequest->code != TSDB_CODE_SUCCESS) {
        pResultInfo->numOfRows = 0;
        return NULL;
      }

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
      SVShowTablesReq* pShowReq = taosMemoryCalloc(1, sizeof(SVShowTablesReq));
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

    if (pResultInfo->completed) {
      pResultInfo->numOfRows = 0;
      return NULL;
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
  if (setupOneRowPtr) {
    doSetOneRowPtr(pResultInfo);
    pResultInfo->current += 1;
  }

  return pResultInfo->row;
}

static int32_t doPrepareResPtr(SReqResultInfo* pResInfo) {
  if (pResInfo->row == NULL) {
    pResInfo->row    = taosMemoryCalloc(pResInfo->numOfCols, POINTER_BYTES);
    pResInfo->pCol   = taosMemoryCalloc(pResInfo->numOfCols, sizeof(SResultColumn));
    pResInfo->length = taosMemoryCalloc(pResInfo->numOfCols, sizeof(int32_t));
    pResInfo->convertBuf = taosMemoryCalloc(pResInfo->numOfCols, POINTER_BYTES);

    if (pResInfo->row == NULL || pResInfo->pCol == NULL || pResInfo->length == NULL || pResInfo->convertBuf == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setResultDataPtr(SReqResultInfo* pResultInfo, TAOS_FIELD* pFields, int32_t numOfCols, int32_t numOfRows) {
  assert(numOfCols > 0 && pFields != NULL && pResultInfo != NULL);
  if (numOfRows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = doPrepareResPtr(pResultInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t* colLength = (int32_t*)pResultInfo->pData;
  char*    pStart = ((char*)pResultInfo->pData) + sizeof(int32_t) * numOfCols;
  for (int32_t i = 0; i < numOfCols; ++i) {
    colLength[i] = htonl(colLength[i]);

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

  // convert UCS4-LE encoded character to native multi-bytes character in current data block.
  for (int32_t i = 0; i < numOfCols; ++i) {
    int32_t type = pResultInfo->fields[i].type;
    int32_t bytes = pResultInfo->fields[i].bytes;

    if (type == TSDB_DATA_TYPE_NCHAR) {
      char* p = taosMemoryRealloc(pResultInfo->convertBuf[i], colLength[i]);
      if (p == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pResultInfo->convertBuf[i] = p;

      SResultColumn* pCol = &pResultInfo->pCol[i];
      for (int32_t j = 0; j < numOfRows; ++j) {
        if (pCol->offset[j] != -1) {
          pStart = pCol->offset[j] + pCol->pData;

          int32_t len = taosUcs4ToMbs((TdUcs4*)varDataVal(pStart), varDataLen(pStart), varDataVal(p));
          ASSERT(len <= bytes);

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
  assert(db != NULL && pTscObj != NULL);
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

int32_t setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp) {
  assert(pResultInfo != NULL && pRsp != NULL);

  pResultInfo->pRspMsg    = (const char*)pRsp;
  pResultInfo->pData      = (void*)pRsp->data;
  pResultInfo->numOfRows  = htonl(pRsp->numOfRows);
  pResultInfo->current    = 0;
  pResultInfo->completed  = (pRsp->completed == 1);
  pResultInfo->payloadLen = htonl(pRsp->compLen);
  pResultInfo->precision  = pRsp->precision;

  // TODO handle the compressed case
  pResultInfo->totalRows += pResultInfo->numOfRows;
  return setResultDataPtr(pResultInfo, pResultInfo->fields, pResultInfo->numOfCols, pResultInfo->numOfRows);
}
