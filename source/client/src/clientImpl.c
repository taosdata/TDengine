
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

static int32_t initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet);
static int32_t buildConnectMsg(SRequestObj *pRequest, SRequestMsgBody* pMsgBody);
static void destroyConnectMsg(SRequestMsgBody* pMsgBody);

static int32_t sendMsgToServer(void *pTransporter, SEpSet* epSet, const SRequestMsgBody *pBody, int64_t* pTransporterId);

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

  SAppInstInfo* pInst = taosHashGet(appInfo.pInstMap, key, strlen(key));
  if (pInst == NULL) {
    pInst = calloc(1, sizeof(struct SAppInstInfo));

    pInst->mgmtEp       = epSet;
    pInst->pTransporter = openTransporter(user, secretEncrypt);

    taosHashPut(appInfo.pInstMap, key, strlen(key), &pInst, POINTER_BYTES);
  }

  return taosConnectImpl(ip, user, &secretEncrypt[0], db, port, NULL, NULL, pInst);
}

TAOS_RES *taos_query_l(TAOS *taos, const char *sql, int sqlLen) {
  STscObj *pTscObj = (STscObj *)taos;
  if (sqlLen > (size_t) tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    return NULL;
  }

  nPrintTsc("%s", sql)

  SRequestObj* pRequest = createRequest(pTscObj, NULL, NULL, TSDB_SQL_SELECT);
  if (pRequest == NULL) {
    tscError("failed to malloc sqlObj");
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pRequest->sqlstr = malloc(sqlLen + 1);
  if (pRequest->sqlstr == NULL) {
    tscError("0x%"PRIx64" failed to prepare sql string buffer", pRequest->self);

    pRequest->msgBuf = strdup("failed to prepare sql string buffer");
    terrno = pRequest->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pRequest;
  }

  strntolower(pRequest->sqlstr, sql, (int32_t)sqlLen);
  pRequest->sqlstr[sqlLen] = 0;

  tscDebugL("0x%"PRIx64" SQL: %s", pRequest->requestId, pRequest->sqlstr);

  int32_t code = 0;
  if (qIsInsertSql(pRequest->sqlstr, sqlLen)) {
    // todo add
  } else {
    int32_t type = 0;
    void*   output = NULL;
    int32_t outputLen = 0;
    code = qParseQuerySql(pRequest->sqlstr, sqlLen, pRequest->requestId, &type, &output, &outputLen, pRequest->msgBuf, ERROR_MSG_BUF_DEFAULT_SIZE);
    if (type == TSDB_SQL_CREATE_USER || type == TSDB_SQL_SHOW) {
      pRequest->type = type;
      pRequest->body.param = output;
      pRequest->body.paramLen = outputLen;

      SRequestMsgBody body = {0};
      buildRequestMsgFp[type](pRequest, &body);

      int64_t transporterId = 0;
      sendMsgToServer(pTscObj->pTransporter, &pTscObj->pAppInfo->mgmtEp.epSet, &body, &transporterId);

      tsem_wait(&pRequest->body.rspSem);
      destroyConnectMsg(&body);
    } else {
      assert(0);
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    pRequest->code = code;
    return pRequest;
  }

  return pRequest;
}

int initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet) {
  pEpSet->version = 0;

  // init mgmt ip set
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
  STscObj *pTscObj = createTscObj(user, auth, ip, port, pAppInfo);
  if (NULL == pTscObj) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pTscObj;
  }

  SRequestObj *pRequest = createRequest(pTscObj, fp, param, TSDB_SQL_CONNECT);
  if (pRequest == NULL) {
    destroyTscObj(pTscObj);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  SRequestMsgBody body = {0};
  buildConnectMsg(pRequest, &body);

  int64_t transporterId = 0;
  sendMsgToServer(pTscObj->pTransporter, &pTscObj->pAppInfo->mgmtEp.epSet, &body, &transporterId);

  tsem_wait(&pRequest->body.rspSem);
  destroyConnectMsg(&body);

  if (pRequest->code != TSDB_CODE_SUCCESS) {
    const char *errorMsg = (pRequest->code == TSDB_CODE_RPC_FQDN_ERROR) ? taos_errstr(pRequest) : tstrerror(terrno);
    printf("failed to connect to server, reason: %s\n\n", errorMsg);

    destroyRequest(pRequest);
    taos_close(pTscObj);
    pTscObj = NULL;
  } else {
    tscDebug("0x%"PRIx64" connection is opening, connId:%d, dnodeConn:%p", pTscObj->id, pTscObj->connId, pTscObj->pTransporter);
    destroyRequest(pRequest);
  }

  return pTscObj;
}

static int32_t buildConnectMsg(SRequestObj *pRequest, SRequestMsgBody* pMsgBody) {
  pMsgBody->msgType         = TSDB_MSG_TYPE_CONNECT;
  pMsgBody->msgLen          = sizeof(SConnectMsg);
  pMsgBody->requestObjRefId = pRequest->self;

  SConnectMsg *pConnect = calloc(1, sizeof(SConnectMsg));
  if (pConnect == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return -1;
  }

  // TODO refactor full_name
  char *db;  // ugly code to move the space

  STscObj *pObj = pRequest->pTscObj;
  pthread_mutex_lock(&pObj->mutex);
  db = strstr(pObj->db, TS_PATH_DELIMITER);

  db = (db == NULL) ? pObj->db : db + 1;
  tstrncpy(pConnect->db, db, sizeof(pConnect->db));
  pthread_mutex_unlock(&pObj->mutex);

  pConnect->pid = htonl(appInfo.pid);
  pConnect->startTime = htobe64(appInfo.startTime);
  tstrncpy(pConnect->app, appInfo.appName, tListLen(pConnect->app));

  pMsgBody->pData = pConnect;
  return 0;
}

static void destroyConnectMsg(SRequestMsgBody* pMsgBody) {
  assert(pMsgBody != NULL);
  tfree(pMsgBody->pData);
}

int32_t sendMsgToServer(void *pTransporter, SEpSet* epSet, const SRequestMsgBody *pBody, int64_t* pTransporterId) {
  char *pMsg = rpcMallocCont(pBody->msgLen);
  if (NULL == pMsg) {
    tscError("0x%"PRIx64" msg:%s malloc failed", pBody->requestId, taosMsg[pBody->msgType]);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return -1;
  }

  memcpy(pMsg, pBody->pData, pBody->msgLen);
  SRpcMsg rpcMsg = {
      .msgType = pBody->msgType,
      .pCont   = pMsg,
      .contLen = pBody->msgLen,
      .ahandle = (void*) pBody->requestObjRefId,
      .handle  = NULL,
      .code    = 0
  };

  rpcSendRequest(pTransporter, epSet, &rpcMsg, pTransporterId);
  return TSDB_CODE_SUCCESS;
}

void processMsgFromServer(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  int64_t requestRefId = (int64_t)pMsg->ahandle;

  SRequestObj *pRequest = (SRequestObj *)taosAcquireRef(tscReqRef, requestRefId);
  if (pRequest == NULL) {
    rpcFreeCont(pMsg->pCont);
    return;
  }

  assert(pRequest->self == requestRefId);
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
  if (pMsg->code == TSDB_CODE_SUCCESS) {
    tscDebug("0x%" PRIx64 " message:%s, code:%s rspLen:%d, elapsed:%"PRId64 " ms", pRequest->requestId, taosMsg[pMsg->msgType],
             tstrerror(pMsg->code), pMsg->contLen, pRequest->metric.rsp - pRequest->metric.start);
    if (handleRequestRspFp[pRequest->type]) {
      pMsg->code = (*handleRequestRspFp[pRequest->type])(pRequest, pMsg->pCont, pMsg->contLen);
    }
  } else {
    tscError("0x%" PRIx64 " SQL cmd:%s, code:%s rspLen:%d, elapsed time:%"PRId64" ms", pRequest->requestId, taosMsg[pMsg->msgType],
             tstrerror(pMsg->code), pMsg->contLen, pRequest->metric.rsp - pRequest->metric.start);
  }

  taosReleaseRef(tscReqRef, requestRefId);
  rpcFreeCont(pMsg->pCont);

  sem_post(&pRequest->body.rspSem);
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
  SClientResultInfo* pResultInfo = pRequest->body.pResInfo;

  if (pResultInfo == NULL || pResultInfo->current >= pResultInfo->pData->info.rows) {
    if (pResultInfo == NULL) {
      pRequest->body.pResInfo = calloc(1, sizeof(SClientResultInfo));
//      pRequest->body.pResInfo.
    }
    // current data set are exhausted, fetch more result from node
//    if (pRes->row >= pRes->numOfRows && needToFetchNewBlock(pSql)) {
//      taos_fetch_rows_a(res, waitForRetrieveRsp, pSql->pTscObj);
//      tsem_wait(&pSql->rspSem);
//    }
  }
}