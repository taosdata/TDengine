#include <tpagedfile.h>
#include "clientInt.h"
#include "tdef.h"
#include "tep.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tref.h"
#include "tscLog.h"

static int32_t initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet);
static int32_t buildConnectMsg(SRequestObj *pRequest, SRequestMsgBody* pMsgBody);
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
  if (pRequest->code != TSDB_CODE_SUCCESS) {
    const char *errorMsg = (pRequest->code == TSDB_CODE_RPC_FQDN_ERROR) ? taos_errstr(pRequest) : tstrerror(terrno);
    printf("failed to connect to server, reason: %s\n\n", errorMsg);

    destroyRequest(pRequest);
    taos_close(pTscObj);
    pTscObj = NULL;
  } else {
    tscDebug("%p connection is opening, dnodeConn:%p", pTscObj, pTscObj->pTransporter);
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

//  tstrncpy(pConnect->clientVersion, version, sizeof(pConnect->clientVersion));
//  tstrncpy(pConnect->msgVersion, "", sizeof(pConnect->msgVersion));

//  pConnect->pid = htonl(taosGetPId());
//  taosGetCurrentAPPName(pConnect->appName, NULL);
  pMsgBody->pData = pConnect;
  return 0;
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

//
//int tscBuildAndSendRequest(SRequestObj *pRequest) {
//  assert(pRequest != NULL);
//  char name[TSDB_TABLE_FNAME_LEN] = {0};
//
//  uint32_t type = 0;
//  tscDebug("0x%"PRIx64" SQL cmd:%s will be processed, name:%s, type:%d", pRequest->requestId, taosMsg[pRequest->type], name, type);
//  if (pRequest->type < TSDB_SQL_MGMT) { // the pTableMetaInfo cannot be NULL
//
//  } else if (pCmd->command >= TSDB_SQL_LOCAL) {
//    return (*tscProcessMsgRsp[pCmd->command])(pSql);
//  }
//
//  return buildConnectMsg(pRequest);
//}


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
    tscDebug("0x%" PRIx64 " SQL cmd:%s, code:%s rspLen:%d", pRequest->requestId, taosMsg[pRequest->type],
             tstrerror(pMsg->code), pMsg->contLen);
    if (handleRequestRspFp[pRequest->type]) {
      pMsg->code = (*handleRequestRspFp[pRequest->type])(pRequest, pMsg->pCont, pMsg->contLen);
    }
  } else {
    tscError("0x%" PRIx64 " SQL cmd:%s, code:%s rspLen:%d", pRequest->requestId, taosMsg[pRequest->type],
             tstrerror(pMsg->code), pMsg->contLen);
  }

  taosReleaseRef(requestRefId, requestRefId);
  rpcFreeCont(pMsg->pCont);
}
