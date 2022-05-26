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

static int32_t       initEpSetFromCfg(const char* firstEp, const char* secondEp, SCorEpSet* pEpSet);
static SMsgSendInfo* buildConnectMsg(SRequestObj* pRequest);
static void          destroySendMsgInfo(SMsgSendInfo* pMsgBody);

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
                                SAppInstInfo* pAppInfo, int connType);

TAOS* taos_connect_internal(const char* ip, const char* user, const char* pass, const char* auth, const char* db,
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
  return taosConnectImpl(user, &secretEncrypt[0], localDb, NULL, NULL, *pInst, connType);
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

  if (taosHashPut(pTscObj->pRequests, &(*pRequest)->self, sizeof((*pRequest)->self), &(*pRequest)->self,
                  sizeof((*pRequest)->self))) {
    destroyRequest(*pRequest);
    *pRequest = NULL;
    tscError("put request to request hash failed");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  tscDebugL("0x%" PRIx64 " SQL: %s, reqId:0x%" PRIx64, (*pRequest)->self, (*pRequest)->sqlstr, (*pRequest)->requestId);
  return TSDB_CODE_SUCCESS;
}

int32_t parseSql(SRequestObj* pRequest, bool topicQuery, SQuery** pQuery, SStmtCallback* pStmtCb) {
  STscObj* pTscObj = pRequest->pTscObj;

  SParseContext cxt = {.requestId = pRequest->requestId,
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
                       .isSuperUser = (0 == strcmp(pTscObj->user, TSDB_DEFAULT_USER))};

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
  }

  return code;
}

int32_t execLocalCmd(SRequestObj* pRequest, SQuery* pQuery) {
  SRetrieveTableRsp* pRsp = NULL;
  int32_t            code = qExecCommand(pQuery->pRoot, &pRsp);
  if (TSDB_CODE_SUCCESS == code && NULL != pRsp) {
    code = setQueryResultFromRsp(&pRequest->body.resInfo, pRsp, false);
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

int32_t getPlan(SRequestObj* pRequest, SQuery* pQuery, SQueryPlan** pPlan, SArray* pNodeList) {
  pRequest->type = pQuery->msgType;
  SPlanContext cxt = {.queryId = pRequest->requestId,
                      .acctId = pRequest->pTscObj->acctId,
                      .mgmtEpSet = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp),
                      .pAstRoot = pQuery->pRoot,
                      .showRewrite = pQuery->showRewrite,
                      .pMsg = pRequest->msgBuf,
                      .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE};
  SEpSet       mgmtEpSet = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);
  SCatalog*    pCatalog = NULL;
  int32_t      code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (TSDB_CODE_SUCCESS == code) {
    code = catalogGetQnodeList(pCatalog, pRequest->pTscObj->pAppInfo->pTransporter, &mgmtEpSet, pNodeList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = qCreateQueryPlan(&cxt, pPlan, pNodeList);
  }
  return code;
}

void setResSchemaInfo(SReqResultInfo* pResInfo, const SSchema* pSchema, int32_t numOfCols) {
  ASSERT(pSchema != NULL && numOfCols > 0);

  pResInfo->numOfCols = numOfCols;
  if (pResInfo->fields != NULL) {
    taosMemoryFree(pResInfo->fields);
  }
  if (pResInfo->userFields != NULL) {
    taosMemoryFree(pResInfo->userFields);
  }
  pResInfo->fields = taosMemoryCalloc(numOfCols, sizeof(TAOS_FIELD));
  pResInfo->userFields = taosMemoryCalloc(numOfCols, sizeof(TAOS_FIELD));

  for (int32_t i = 0; i < pResInfo->numOfCols; ++i) {
    pResInfo->fields[i].bytes = pSchema[i].bytes;
    pResInfo->fields[i].type = pSchema[i].type;

    pResInfo->userFields[i].bytes = pSchema[i].bytes;
    pResInfo->userFields[i].type = pSchema[i].type;

    if (pSchema[i].type == TSDB_DATA_TYPE_VARCHAR) {
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

int32_t scheduleQuery(SRequestObj* pRequest, SQueryPlan* pDag, SArray* pNodeList, void** pRes) {
  void* pTransporter = pRequest->pTscObj->pAppInfo->pTransporter;

  SQueryResult res = {.code = 0, .numOfRows = 0};
  int32_t      code = schedulerExecJob(pTransporter, pNodeList, pDag, &pRequest->body.queryJob, pRequest->sqlstr,
                                       pRequest->metric.start, &res);
  if (code != TSDB_CODE_SUCCESS) {
    if (pRequest->body.queryJob != 0) {
      schedulerFreeJob(pRequest->body.queryJob);
    }

    *pRes = res.res;

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

  *pRes = res.res;

  pRequest->code = res.code;
  terrno = res.code;
  return pRequest->code;
}

int32_t getQueryPlan(SRequestObj* pRequest, SQuery* pQuery, SArray** pNodeList) {
  *pNodeList = taosArrayInit(4, sizeof(struct SQueryNodeAddr));
  return getPlan(pRequest, pQuery, &pRequest->body.pDag, *pNodeList);
}

int32_t validateSversion(SRequestObj* pRequest, void* res) {
  SArray* pArray = NULL;
  int32_t code = 0;

  if (TDMT_VND_SUBMIT == pRequest->type) {
    SSubmitRsp* pRsp = (SSubmitRsp*)res;
    if (pRsp->nBlocks <= 0) {
      return TSDB_CODE_SUCCESS;
    }

    pArray = taosArrayInit(pRsp->nBlocks, sizeof(STbSVersion));
    if (NULL == pArray) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    for (int32_t i = 0; i < pRsp->nBlocks; ++i) {
      SSubmitBlkRsp* blk = pRsp->pBlocks + i;
      if (NULL == blk->tblFName || 0 == blk->tblFName[0]) {
        continue;
      }
      
      STbSVersion    tbSver = {.tbFName = blk->tblFName, .sver = blk->sver};
      taosArrayPush(pArray, &tbSver);
    }
  } else if (TDMT_VND_QUERY == pRequest->type) {
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
      STbSVersion tbSver = {.tbFName = tbInfo->tbFName, .sver = tbInfo->sversion};
      taosArrayPush(pArray, &tbSver);
    }
  }

  SCatalog* pCatalog = NULL;
  CHECK_CODE_GOTO(catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog), _return);

  SEpSet epset = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  code = catalogChkTbMetaVersion(pCatalog, pRequest->pTscObj->pAppInfo->pTransporter, &epset, pArray);

_return:

  taosArrayDestroy(pArray);

  return code;
}

void freeRequestRes(SRequestObj* pRequest, void* res) {
  if (NULL == pRequest || NULL == res) {
    return;
  }

  if (TDMT_VND_SUBMIT == pRequest->type) {
    tFreeSSubmitRsp((SSubmitRsp*)res);
  } else if (TDMT_VND_QUERY == pRequest->type) {
    taosArrayDestroy((SArray*)res);
  }
}

SRequestObj* launchQueryImpl(SRequestObj* pRequest, SQuery* pQuery, int32_t code, bool keepQuery, void** res) {
  void* pRes = NULL;

  if (TSDB_CODE_SUCCESS == code) {
    switch (pQuery->execMode) {
      case QUERY_EXEC_MODE_LOCAL:
        code = execLocalCmd(pRequest, pQuery);
        break;
      case QUERY_EXEC_MODE_RPC:
        code = execDdlQuery(pRequest, pQuery);
        break;
      case QUERY_EXEC_MODE_SCHEDULE: {
        SArray* pNodeList = taosArrayInit(4, sizeof(struct SQueryNodeAddr));
        code = getPlan(pRequest, pQuery, &pRequest->body.pDag, pNodeList);
        if (TSDB_CODE_SUCCESS == code) {
          code = scheduleQuery(pRequest, pRequest->body.pDag, pNodeList, &pRes);
          if (NULL != pRes) {
            code = validateSversion(pRequest, pRes);
          }
        }
        taosArrayDestroy(pNodeList);
        break;
      }
      case QUERY_EXEC_MODE_EMPTY_RESULT:
        pRequest->type = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
        break;
      default:
        break;
    }
  }

  if (!keepQuery) {
    qDestroyQuery(pQuery);
  }

  if (NULL != pRequest && TSDB_CODE_SUCCESS != code) {
    pRequest->code = terrno;
  }

  if (res) {
    *res = pRes;
  } else {
    freeRequestRes(pRequest, pRes);
    pRes = NULL;
  }

  return pRequest;
}

SRequestObj* launchQuery(STscObj* pTscObj, const char* sql, int sqlLen) {
  SRequestObj* pRequest = NULL;
  SQuery*      pQuery = NULL;

  int32_t code = buildRequest(pTscObj, sql, sqlLen, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  code = parseSql(pRequest, false, &pQuery, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    pRequest->code = code;
    return pRequest;
  }

  return launchQueryImpl(pRequest, pQuery, code, false, NULL);
}

int32_t refreshMeta(STscObj* pTscObj, SRequestObj* pRequest) {
  SCatalog* pCatalog = NULL;
  int32_t   code = 0;
  int32_t   dbNum = taosArrayGetSize(pRequest->dbList);
  int32_t   tblNum = taosArrayGetSize(pRequest->tableList);

  if (dbNum <= 0 && tblNum <= 0) {
    return TSDB_CODE_QRY_APP_ERROR;
  }

  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SEpSet epset = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  for (int32_t i = 0; i < dbNum; ++i) {
    char* dbFName = taosArrayGet(pRequest->dbList, i);

    code = catalogRefreshDBVgInfo(pCatalog, pTscObj->pAppInfo->pTransporter, &epset, dbFName);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  for (int32_t i = 0; i < tblNum; ++i) {
    SName* tableName = taosArrayGet(pRequest->tableList, i);

    code = catalogRefreshTableMeta(pCatalog, pTscObj->pAppInfo->pTransporter, &epset, tableName, -1);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return code;
}

int32_t removeMeta(STscObj* pTscObj, SArray* tbList) {
  SCatalog* pCatalog = NULL;
  int32_t tbNum = taosArrayGetSize(tbList);
  int32_t code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  
  for (int32_t i = 0; i < tbNum; ++i) {
    SName* pTbName = taosArrayGet(tbList, i);
    catalogRemoveTableMeta(pCatalog, pTbName);
  }

  return TSDB_CODE_SUCCESS;
}


SRequestObj* execQuery(STscObj* pTscObj, const char* sql, int sqlLen) {
  SRequestObj* pRequest = NULL;
  int32_t      retryNum = 0;
  int32_t      code = 0;

  do {
    destroyRequest(pRequest);
    pRequest = launchQuery(pTscObj, sql, sqlLen);
    if (pRequest == NULL || TSDB_CODE_SUCCESS == pRequest->code || !NEED_CLIENT_HANDLE_ERROR(pRequest->code)) {
      break;
    }

    code = refreshMeta(pTscObj, pRequest);
    if (code) {
      pRequest->code = code;
      break;
    }
  } while (retryNum++ < REQUEST_MAX_TRY_TIMES);

  if (NEED_CLIENT_RM_TBLMETA_REQ(pRequest->type)) {
    removeMeta(pTscObj, pRequest->tableList);
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

    int32_t code = taosGetFqdnPortFromEp(firstEp, &mgmtEpSet->eps[0]);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return terrno;
    }

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
                         SAppInstInfo* pAppInfo, int connType) {
  STscObj* pTscObj = createTscObj(user, auth, db, connType, pAppInfo);
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
    fprintf(stderr, "failed to connect to server, reason: %s\n\n", errorMsg);

    terrno = pRequest->code;
    destroyRequest(pRequest);
    taos_close(pTscObj);
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

  connectReq.connType = pObj->connType;
  connectReq.pid = htonl(appInfo.pid);
  connectReq.startTime = htobe64(appInfo.startTime);

  tstrncpy(connectReq.app, appInfo.appName, sizeof(connectReq.app));
  tstrncpy(connectReq.user, pObj->user, sizeof(connectReq.user));
  tstrncpy(connectReq.passwd, pObj->pass, sizeof(connectReq.passwd));

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
  return msgType == TDMT_VND_QUERY_RSP || msgType == TDMT_VND_FETCH_RSP || msgType == TDMT_VND_RES_READY_RSP ||
         msgType == TDMT_VND_QUERY_HEARTBEAT_RSP;
}

void processMsgFromServer(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SMsgSendInfo* pSendInfo = (SMsgSendInfo*)pMsg->info.ahandle;
  assert(pMsg->info.ahandle != NULL);

  if (pSendInfo->requestObjRefId != 0) {
    SRequestObj* pRequest = (SRequestObj*)taosAcquireRef(clientReqRefPool, pSendInfo->requestObjRefId);
    assert(pRequest->self == pSendInfo->requestObjRefId);

    pRequest->metric.rsp = taosGetTimestampUs();

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
               TMSG_INFO(pMsg->msgType), tstrerror(pMsg->code), pMsg->contLen, elapsed / 1000, pRequest->requestId);
    } else {
      tscError("0x%" PRIx64 " SQL cmd:%s, code:%s rspLen:%d, elapsed time:%d ms, reqId:0x%" PRIx64, pRequest->self,
               TMSG_INFO(pMsg->msgType), tstrerror(pMsg->code), pMsg->contLen, elapsed / 1000, pRequest->requestId);
    }

    taosReleaseRef(clientReqRefPool, pSendInfo->requestObjRefId);
  }

  SDataBuf buf = {.len = pMsg->contLen, .pData = NULL, .handle = pMsg->info.handle};

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

  return taos_connect_internal(ip, user, NULL, auth, db, port, CONN_TYPE__QUERY);
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
      if (pCol->offset[pResultInfo->current] != -1) {
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
  assert(pRequest != NULL);

  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;
  if (pResultInfo->pData == NULL || pResultInfo->current >= pResultInfo->numOfRows) {
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

    pRequest->code = setQueryResultFromRsp(&pRequest->body.resInfo, (SRetrieveTableRsp*)pResInfo->pData, convertUcs4);
    if (pRequest->code != TSDB_CODE_SUCCESS) {
      pResultInfo->numOfRows = 0;
      return NULL;
    }

    tscDebug("0x%" PRIx64 " fetch results, numOfRows:%d total Rows:%" PRId64 ", complete:%d, reqId:0x%" PRIx64,
             pRequest->self, pResInfo->numOfRows, pResInfo->totalRows, pResInfo->completed, pRequest->requestId);

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

static char* parseTagDatatoJson(void* p) {
  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    goto end;
  }

  int16_t nCols = kvRowNCols(p);
  char    tagJsonKey[256] = {0};
  for (int j = 0; j < nCols; ++j) {
    SColIdx* pColIdx = kvRowColIdxAt(p, j);
    char*    val = (char*)(kvRowColVal(p, pColIdx));
    if (j == 0) {
      if (*val == TSDB_DATA_TYPE_NULL) {
        string = taosMemoryCalloc(1, 8);
        sprintf(string, "%s", TSDB_DATA_NULL_STR_L);
        goto end;
      }
      continue;
    }

    // json key  encode by binary
    memset(tagJsonKey, 0, sizeof(tagJsonKey));
    memcpy(tagJsonKey, varDataVal(val), varDataLen(val));
    // json value
    val += varDataTLen(val);
    char* realData = POINTER_SHIFT(val, CHAR_BYTES);
    char  type = *val;
    if (type == TSDB_DATA_TYPE_NULL) {
      cJSON* value = cJSON_CreateNull();
      if (value == NULL) {
        goto end;
      }
      cJSON_AddItemToObject(json, tagJsonKey, value);
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      cJSON* value = NULL;
      if (varDataLen(realData) > 0) {
        char*   tagJsonValue = taosMemoryCalloc(varDataLen(realData), 1);
        int32_t length = taosUcs4ToMbs((TdUcs4*)varDataVal(realData), varDataLen(realData), tagJsonValue);
        if (length < 0) {
          tscError("charset:%s to %s. val:%s convert json value failed.", DEFAULT_UNICODE_ENCODEC, tsCharset, val);
          taosMemoryFree(tagJsonValue);
          goto end;
        }
        value = cJSON_CreateString(tagJsonValue);
        taosMemoryFree(tagJsonValue);
        if (value == NULL) {
          goto end;
        }
      } else if (varDataLen(realData) == 0) {
        value = cJSON_CreateString("");
      } else {
        ASSERT(0);
      }

      cJSON_AddItemToObject(json, tagJsonKey, value);
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double jsonVd = *(double*)(realData);
      cJSON* value = cJSON_CreateNumber(jsonVd);
      if (value == NULL) {
        goto end;
      }
      cJSON_AddItemToObject(json, tagJsonKey, value);
      //    }else if(type == TSDB_DATA_TYPE_BIGINT){
      //      int64_t jsonVd = *(int64_t*)(realData);
      //      cJSON* value = cJSON_CreateNumber((double)jsonVd);
      //      if (value == NULL)
      //      {
      //        goto end;
      //      }
      //      cJSON_AddItemToObject(json, tagJsonKey, value);
    } else if (type == TSDB_DATA_TYPE_BOOL) {
      char   jsonVd = *(char*)(realData);
      cJSON* value = cJSON_CreateBool(jsonVd);
      if (value == NULL) {
        goto end;
      }
      cJSON_AddItemToObject(json, tagJsonKey, value);
    } else {
      ASSERT(0);
    }
  }
  string = cJSON_PrintUnformatted(json);
end:
  cJSON_Delete(json);
  return string;
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
          ASSERT(len <= bytes);
          ASSERT((p + len) < (pResultInfo->convertBuf[i] + colLength[i]));

          varDataSetLen(p, len);
          pCol->offset[j] = (p - pResultInfo->convertBuf[i]);
          p += (len + VARSTR_HEADER_SIZE);
        }
      }

      pResultInfo->pCol[i].pData = pResultInfo->convertBuf[i];
      pResultInfo->row[i] = pResultInfo->pCol[i].pData;
    } else if (type == TSDB_DATA_TYPE_JSON && colLength[i] > 0) {
      char* p = taosMemoryRealloc(pResultInfo->convertBuf[i], colLength[i]);
      if (p == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pResultInfo->convertBuf[i] = p;
      int32_t        len = 0;
      SResultColumn* pCol = &pResultInfo->pCol[i];
      for (int32_t j = 0; j < numOfRows; ++j) {
        if (pCol->offset[j] != -1) {
          char* pStart = pCol->offset[j] + pCol->pData;

          int32_t jsonInnerType = *pStart;
          char*   jsonInnerData = pStart + CHAR_BYTES;
          char    dst[TSDB_MAX_JSON_TAG_LEN] = {0};
          if (jsonInnerType == TSDB_DATA_TYPE_NULL) {
            sprintf(varDataVal(dst), "%s", TSDB_DATA_NULL_STR_L);
            varDataSetLen(dst, strlen(varDataVal(dst)));
          } else if (jsonInnerType == TSDB_DATA_TYPE_JSON) {
            char* jsonString = parseTagDatatoJson(jsonInnerData);
            STR_TO_VARSTR(dst, jsonString);
            taosMemoryFree(jsonString);
          } else if (jsonInnerType == TSDB_DATA_TYPE_NCHAR) {  // value -> "value"
            *(char*)varDataVal(dst) = '\"';
            int32_t length = taosUcs4ToMbs((TdUcs4*)varDataVal(jsonInnerData), varDataLen(jsonInnerData),
                                           varDataVal(dst) + CHAR_BYTES);
            if (length <= 0) {
              tscError("charset:%s to %s. val:%s convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset,
                       varDataVal(jsonInnerData));
              length = 0;
            }
            varDataSetLen(dst, length + CHAR_BYTES * 2);
            *(char*)POINTER_SHIFT(varDataVal(dst), length + CHAR_BYTES) = '\"';
          } else if (jsonInnerType == TSDB_DATA_TYPE_DOUBLE) {
            double jsonVd = *(double*)(jsonInnerData);
            sprintf(varDataVal(dst), "%.9lf", jsonVd);
            varDataSetLen(dst, strlen(varDataVal(dst)));
          } else if (jsonInnerType == TSDB_DATA_TYPE_BIGINT) {
            int64_t jsonVd = *(int64_t*)(jsonInnerData);
            sprintf(varDataVal(dst), "%" PRId64, jsonVd);
            varDataSetLen(dst, strlen(varDataVal(dst)));
          } else if (jsonInnerType == TSDB_DATA_TYPE_BOOL) {
            sprintf(varDataVal(dst), "%s", (*((char*)jsonInnerData) == 1) ? "true" : "false");
            varDataSetLen(dst, strlen(varDataVal(dst)));
          } else {
            ASSERT(0);
          }

          if (len + varDataTLen(dst) > colLength[i]) {
            p = taosMemoryRealloc(pResultInfo->convertBuf[i], len + varDataTLen(dst));
            if (p == NULL) {
              return TSDB_CODE_OUT_OF_MEMORY;
            }

            pResultInfo->convertBuf[i] = p;
          }
          p = pResultInfo->convertBuf[i] + len;
          memcpy(p, dst, varDataTLen(dst));
          pCol->offset[j] = len;
          len += varDataTLen(dst);
        }
      }

      pResultInfo->pCol[i].pData = pResultInfo->convertBuf[i];
      pResultInfo->row[i] = pResultInfo->pCol[i].pData;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setResultDataPtr(SReqResultInfo* pResultInfo, TAOS_FIELD* pFields, int32_t numOfCols, int32_t numOfRows,
                         bool convertUcs4) {
  assert(numOfCols > 0 && pFields != NULL && pResultInfo != NULL);
  if (numOfRows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = doPrepareResPtr(pResultInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  char* p = (char*)pResultInfo->pData;

  int32_t dataLen = *(int32_t*)p;
  p += sizeof(int32_t);

  uint64_t groupId = *(uint64_t*)p;
  p += sizeof(uint64_t);

  int32_t* colLength = (int32_t*)p;
  p += sizeof(int32_t) * numOfCols;

  char* pStart = p;
  for (int32_t i = 0; i < numOfCols; ++i) {
    colLength[i] = htonl(colLength[i]);
    ASSERT(colLength[i] < dataLen);

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

int32_t setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp, bool convertUcs4) {
  assert(pResultInfo != NULL && pRsp != NULL);

  pResultInfo->pRspMsg = (const char*)pRsp;
  pResultInfo->pData = (void*)pRsp->data;
  pResultInfo->numOfRows = htonl(pRsp->numOfRows);
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
  rpcInit.user = "_dnd";

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
  if (details != NULL && statusRsp.details != NULL) {
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
