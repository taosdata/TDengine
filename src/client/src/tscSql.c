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

#include "hash.h"
#include "os.h"
#include "texpr.h"
#include "tkey.h"
#include "tcache.h"
#include "tnote.h"
#include "trpc.h"
#include "tscLog.h"
#include "tscSubquery.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "ttokendef.h"
#include "tutil.h"
#include "ttimer.h"
#include "tscProfile.h"

static bool validImpl(const char* str, size_t maxsize) {
  if (str == NULL) {
    return false;
  }
  
  size_t len = strlen(str);
  if (len <= 0 || len > maxsize) {
    return false;
  }
  
  return true;
}

static bool validUserName(const char* user) {
  return validImpl(user, TSDB_USER_LEN - 1);
}

static bool validPassword(const char* passwd) {
  return validImpl(passwd, TSDB_KEY_LEN - 1);
}

static SSqlObj *taosConnectImpl(const char *ip, const char *user, const char *pass, const char *auth, const char *db,
                         uint16_t port, void (*fp)(void *, TAOS_RES *, int), void *param, TAOS **taos) {
  if (taos_init()) {
    return NULL;
  }

  if (!validUserName(user)) {
    terrno = TSDB_CODE_TSC_INVALID_USER_LENGTH;
    return NULL;
  }
  SRpcCorEpSet corMgmtEpSet;

  char secretEncrypt[32] = {0};
  int  secretEncryptLen = 0;
  if (auth == NULL) {
    if (!validPassword(pass)) {
      terrno = TSDB_CODE_TSC_INVALID_PASS_LENGTH;
      return NULL;
    }
    taosEncryptPass((uint8_t *)pass, strlen(pass), secretEncrypt);
  } else {
    int   outlen = 0;
    int   len = (int)strlen(auth);
    char *base64 = (char *)base64_decode(auth, len, &outlen);
    if (base64 == NULL || outlen == 0) {
      tscError("invalid auth info:%s", auth);
      free(base64);
      terrno = TSDB_CODE_TSC_INVALID_PASS_LENGTH;
      return NULL;
    } else {
      memcpy(secretEncrypt, base64, outlen);
      free(base64);
    }
    secretEncryptLen = outlen;
  }
  
  if (ip) {
    if (tscSetMgmtEpSetFromCfg(ip, NULL, &corMgmtEpSet) < 0) return NULL;
    if (port) corMgmtEpSet.epSet.port[0] = port;
  } else {
    if (tscSetMgmtEpSetFromCfg(tsFirst, tsSecond, &corMgmtEpSet) < 0) return NULL;
  }
  char rpcKey[512] = {0};
  snprintf(rpcKey, sizeof(rpcKey), "%s:%s:%s:%d", user, pass, ip, port);
 
  void *pRpcObj = NULL;
  if (tscAcquireRpc(rpcKey, user, secretEncrypt, &pRpcObj) != 0) {
    terrno = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    return NULL;
  }
 
  STscObj *pObj = (STscObj *)calloc(1, sizeof(STscObj));
  if (NULL == pObj) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscReleaseRpc(pRpcObj);
    return NULL;
  }

  pObj->tscCorMgmtEpSet = malloc(sizeof(SRpcCorEpSet));
  if (pObj->tscCorMgmtEpSet == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscReleaseRpc(pRpcObj);
    free(pObj);
    return NULL;
  }
  memcpy(pObj->tscCorMgmtEpSet, &corMgmtEpSet, sizeof(corMgmtEpSet));
  
  pObj->signature = pObj;
  pObj->pRpcObj = (SRpcObj *)pRpcObj;
  tstrncpy(pObj->user, user, sizeof(pObj->user));
  secretEncryptLen = MIN(secretEncryptLen, sizeof(pObj->pass));
  memcpy(pObj->pass, secretEncrypt, secretEncryptLen);

  if (db) {
    int32_t len = (int32_t)strlen(db);
    /* db name is too long */
    if (len >= TSDB_DB_NAME_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_DB_LENGTH;
      tscReleaseRpc(pRpcObj);
      free(pObj);
      return NULL;
    }

    char tmp[TSDB_DB_NAME_LEN] = {0};
    tstrncpy(tmp, db, sizeof(tmp));

    strdequote(tmp);
    strtolower(pObj->db, tmp);
  }

  pthread_mutex_init(&pObj->mutex, NULL);

  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (NULL == pSql) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscReleaseRpc(pRpcObj);
    free(pObj);
    return NULL;
  }

  pSql->pTscObj   = pObj;
  pSql->signature = pSql;
  pSql->maxRetry  = TSDB_MAX_REPLICA;
  pSql->fp        = fp;
  pSql->param     = param;
  pSql->cmd.command = TSDB_SQL_CONNECT;

  tsem_init(&pSql->rspSem, 0, 0);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pSql->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscReleaseRpc(pRpcObj);
    free(pSql);
    free(pObj);
    return NULL;
  }

  if (taos != NULL) {
    *taos = pObj;
  }
  pObj->rid = taosAddRef(tscRefId, pObj);
  registerSqlObj(pSql);

  return pSql;
}

static void syncConnCallback(void *param, TAOS_RES *tres, int code) {
  SSqlObj *pSql = (SSqlObj *) tres;
  assert(pSql != NULL);
  
  tsem_post(&pSql->rspSem);
}

TAOS *taos_connect_internal(const char *ip, const char *user, const char *pass, const char *auth, const char *db,
                            uint16_t port) {
  STscObj *pObj = NULL;
  SSqlObj *pSql = taosConnectImpl(ip, user, pass, auth, db, port, syncConnCallback, NULL, (void **)&pObj);
  if (pSql != NULL) {
    pSql->fp = syncConnCallback;
    pSql->param = pSql;

    tscBuildAndSendRequest(pSql, NULL);
    tsem_wait(&pSql->rspSem);

    if (pSql->res.code != TSDB_CODE_SUCCESS) {
      terrno = pSql->res.code;
      taos_free_result(pSql);
      taos_close(pObj);
      return NULL;
    }
    
    tscDebug("%p DB connection is opening, rpcObj: %p, dnodeConn:%p", pObj, pObj->pRpcObj, pObj->pRpcObj->pDnodeConn);
    taos_free_result(pSql);
  
    // version compare only requires the first 3 segments of the version string
    int code = taosCheckVersion(version, taos_get_server_info(pObj), 3);
    if (code != 0) {
      terrno = code;
      taos_close(pObj);
      return NULL;
    } else {
      return pObj;
    }
  }

  return NULL;
}

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  tscDebug("try to create a connection to %s:%u, user:%s db:%s", ip, port != 0 ? port : tsServerPort , user, db);
  if (user == NULL) user = TSDB_DEFAULT_USER;
  if (pass == NULL) pass = TSDB_DEFAULT_PASS;

  return taos_connect_internal(ip, user, pass, NULL, db, port);
}

TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) {
  tscDebug("try to create a connection to %s:%u by auth, user:%s db:%s", ip, port, user, db);
  if (user == NULL) user = TSDB_DEFAULT_USER;
  if (auth == NULL) return NULL;

  return taos_connect_internal(ip, user, NULL, auth, db, port);
}

TAOS *taos_connect_c(const char *ip, uint8_t ipLen, const char *user, uint8_t userLen, const char *pass,
                     uint8_t passLen, const char *db, uint8_t dbLen, uint16_t port) {
  char ipBuf[TSDB_EP_LEN] = {0};
  char userBuf[TSDB_USER_LEN] = {0};
  char passBuf[TSDB_KEY_LEN] = {0};
  char dbBuf[TSDB_DB_NAME_LEN] = {0};
  strncpy(ipBuf, ip, MIN(TSDB_EP_LEN - 1, ipLen));
  strncpy(userBuf, user, MIN(TSDB_USER_LEN - 1, userLen));
  strncpy(passBuf, pass, MIN(TSDB_KEY_LEN - 1, passLen));
  strncpy(dbBuf, db, MIN(TSDB_DB_NAME_LEN - 1, dbLen));
  return taos_connect(ipBuf, userBuf, passBuf, dbBuf, port);
}

static void asyncConnCallback(void *param, TAOS_RES *tres, int code) {
  SSqlObj *pSql = (SSqlObj *) tres;
  assert(pSql != NULL);
  
  pSql->fetchFp(pSql->param, tres, code);
}

TAOS *taos_connect_a(char *ip, char *user, char *pass, char *db, uint16_t port, void (*fp)(void *, TAOS_RES *, int),
                     void *param, TAOS **taos) {
  STscObj *pObj = NULL;
  SSqlObj *pSql = taosConnectImpl(ip, user, pass, NULL, db, port, asyncConnCallback, param, (void **)&pObj);
  if (pSql == NULL) {
    return NULL;
  }

  if (taos) *taos = pObj;

  pSql->fetchFp = fp;
  pSql->res.code = tscBuildAndSendRequest(pSql, NULL);
  tscDebug("%p DB async connection is opening", taos);
  return pObj;
}

void taos_close(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;

  if (pObj == NULL) {
    tscDebug("(null) try to free tscObj and close dnodeConn");
    return;
  }

  tscDebug("%p try to free tscObj", pObj);
  if (pObj->signature != pObj) {
    tscDebug("%p already closed or invalid tscObj", pObj);
    return;
  }

  if (RID_VALID(pObj->hbrid)) {
    SSqlObj* pHb = (SSqlObj*)taosAcquireRef(tscObjRef, pObj->hbrid);
    if (pHb != NULL) {
      if (RID_VALID(pHb->rpcRid)) {  // wait for rsp from dnode
        rpcCancelRequest(pHb->rpcRid);
        pHb->rpcRid = -1;
      }

      tscDebug("0x%"PRIx64" HB is freed", pHb->self);
      taosReleaseRef(tscObjRef, pHb->self);
#ifdef __APPLE__
      // to satisfy later tsem_destroy in taos_free_result
      tsem_init(&pHb->rspSem, 0, 0);
#endif // __APPLE__
      taos_free_result(pHb);
    }
  }

  tscDebug("%p all sqlObj are freed, free tscObj", pObj);
  taosRemoveRef(tscRefId, pObj->rid);
}

void waitForQueryRsp(void *param, TAOS_RES *tres, int code) {
  assert(tres != NULL);
  
  SSqlObj *pSql = (SSqlObj *) tres;
  tsem_post(&pSql->rspSem);
}

static void waitForRetrieveRsp(void *param, TAOS_RES *tres, int numOfRows) {
  SSqlObj* pSql = (SSqlObj*) tres;
  tsem_post(&pSql->rspSem);
}

TAOS_RES* taos_query_c(TAOS *taos, const char *sqlstr, uint32_t sqlLen, int64_t* res) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }
  
  if (sqlLen > (uint32_t)tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    return NULL;
  }

  nPrintTsc("%s", sqlstr);

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    tscError("failed to malloc sqlObj");
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }
  
  tsem_init(&pSql->rspSem, 0, 0);
  doAsyncQuery(pObj, pSql, waitForQueryRsp, taos, sqlstr, sqlLen);

  if (res != NULL) {
    atomic_store_64(res, pSql->self);
  }

  tsem_wait(&pSql->rspSem);
  return pSql; 
}

TAOS_RES* taos_query(TAOS *taos, const char *sqlstr) {
  return taos_query_c(taos, sqlstr, (uint32_t)strlen(sqlstr), NULL);
}

TAOS_RES* taos_query_h(TAOS* taos, const char *sqlstr, int64_t* res) {
  return taos_query_c(taos, sqlstr, (uint32_t) strlen(sqlstr), res);
}

int taos_result_precision(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) return 0;

  return pSql->res.precision;
}

int taos_num_rows(TAOS_RES *res) { return 0; }

int taos_num_fields(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) return 0;

  int32_t num = 0;
  SQueryInfo *pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  if (pQueryInfo == NULL) {
    return num;
  }

  while(pQueryInfo->pDownstream != NULL) {
    pQueryInfo = pQueryInfo->pDownstream;
  }

  size_t numOfCols = tscNumOfFields(pQueryInfo);
  for(int32_t i = 0; i < numOfCols; ++i) {
    SInternalField* pInfo = taosArrayGet(pQueryInfo->fieldsInfo.internalField, i);
    if (pInfo->visible) {
      num++;
    }
  }
  
  return num;
}

int taos_field_count(TAOS_RES *tres) {
  SSqlObj* pSql = (SSqlObj*) tres;
  if (pSql == NULL || pSql->signature != pSql) return 0;

  return taos_num_fields(pSql);
}

int taos_affected_rows(TAOS_RES *tres) {
  SSqlObj* pSql = (SSqlObj*) tres;
  if (pSql == NULL || pSql->signature != pSql) return 0;

  return pSql->res.numOfRows;
}

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlRes *pRes = &pSql->res;
  if (pSql == NULL || pSql->signature != pSql) return 0;

  SQueryInfo *pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  if (pQueryInfo == NULL) {
    return NULL;
  }
  
  size_t numOfCols = tscNumOfFields(pQueryInfo);
  if (numOfCols == 0) {
    return NULL;
  }

  SFieldInfo *pFieldInfo = &pQueryInfo->fieldsInfo;

  if (pRes->final == NULL) {
    TAOS_FIELD* f = calloc(pFieldInfo->numOfOutput, sizeof(TAOS_FIELD));

    int32_t j = 0;
    for(int32_t i = 0; i < pFieldInfo->numOfOutput; ++i) {
      SInternalField* pField = tscFieldInfoGetInternalField(pFieldInfo, i);
      if (pField->visible) {
        f[j] = pField->field;

        // revise the length for binary and nchar fields
        if (f[j].type == TSDB_DATA_TYPE_BINARY) {
          f[j].bytes -= VARSTR_HEADER_SIZE;
        } else if (f[j].type == TSDB_DATA_TYPE_NCHAR) {
          f[j].bytes = (f[j].bytes - VARSTR_HEADER_SIZE)/TSDB_NCHAR_SIZE;
        }

        j += 1;
      }
    }

    pRes->final = f;
  }

  return pRes->final;
}

static bool needToFetchNewBlock(SSqlObj* pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  return (pRes->completed != true || hasMoreVnodesToTry(pSql) || hasMoreClauseToTry(pSql)) &&
         (pCmd->command == TSDB_SQL_RETRIEVE ||
          pCmd->command == TSDB_SQL_RETRIEVE_LOCALMERGE ||
          pCmd->command == TSDB_SQL_TABLE_JOIN_RETRIEVE ||
          pCmd->command == TSDB_SQL_FETCH ||
          pCmd->command == TSDB_SQL_SHOW ||
          pCmd->command == TSDB_SQL_SHOW_CREATE_TABLE ||
          pCmd->command == TSDB_SQL_SHOW_CREATE_STABLE ||
          pCmd->command == TSDB_SQL_SHOW_CREATE_DATABASE ||
          pCmd->command == TSDB_SQL_SELECT ||
          pCmd->command == TSDB_SQL_DESCRIBE_TABLE ||
          pCmd->command == TSDB_SQL_SERV_STATUS ||
          pCmd->command == TSDB_SQL_CURRENT_DB ||
          pCmd->command == TSDB_SQL_SERV_VERSION ||
          pCmd->command == TSDB_SQL_CLI_VERSION ||
          pCmd->command == TSDB_SQL_CURRENT_USER);
}

TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }
  
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  
  if (pRes->qId == 0 ||
      pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED ||
      pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      pCmd->command == TSDB_SQL_INSERT) {
    return NULL;
  }

  // set the sql object owner
  tscSetSqlOwner(pSql);

  // current data set are exhausted, fetch more result from node
  if (pRes->row >= pRes->numOfRows && needToFetchNewBlock(pSql)) {
    taos_fetch_rows_a(res, waitForRetrieveRsp, pSql->pTscObj);
    tsem_wait(&pSql->rspSem);
  }

  void* data = doSetResultRowData(pSql);

  tscClearSqlOwner(pSql);
  return data;
}

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return 0;
  }

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pRes->qId == 0 ||
      pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED ||
      pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      pCmd->command == TSDB_SQL_INSERT) {
    return 0;
  }

  tscResetForNextRetrieve(pRes);

  // set the sql object owner
  tscSetSqlOwner(pSql);

  // current data set are exhausted, fetch more data from node
  if (needToFetchNewBlock(pSql)) {
    taos_fetch_rows_a(res, waitForRetrieveRsp, pSql->pTscObj);
    tsem_wait(&pSql->rspSem);
  }

  *rows = pRes->urow;

  tscClearSqlOwner(pSql);
  return pRes->numOfRows;
}

int taos_select_db(TAOS *taos, const char *db) {
  char sql[256] = {0};

  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  snprintf(sql, tListLen(sql), "use %s", db);
  SSqlObj* pSql = taos_query(taos, sql);
  int32_t code = pSql->res.code;
  taos_free_result(pSql);
  
  return code;
}

// send free message to vnode to free qhandle and corresponding resources in vnode
static bool tscKillQueryInDnode(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  if (pRes == NULL || pRes->qId == 0) {
    return true;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);

  if ((pQueryInfo == NULL) || tscIsTwoStageSTableQuery(pQueryInfo, 0)) {
    return true;
  }

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  tscRemoveFromSqlList(pSql);

  int32_t cmd = pCmd->command;
  if (pRes->code == TSDB_CODE_SUCCESS && pRes->completed == false && pSql->pStream == NULL && (pTableMetaInfo->pTableMeta != NULL) &&
      (cmd == TSDB_SQL_SELECT ||
       cmd == TSDB_SQL_SHOW ||
       cmd == TSDB_SQL_RETRIEVE ||
       cmd == TSDB_SQL_FETCH)) {
    pQueryInfo->type = TSDB_QUERY_TYPE_FREE_RESOURCE;
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
    tscDebug("0x%"PRIx64" send msg to dnode to free qhandle ASAP before free sqlObj, command:%s", pSql->self, sqlCmd[pCmd->command]);

    tscBuildAndSendRequest(pSql, NULL);
    return false;
  }

  return true;
}

void taos_free_result(TAOS_RES *res) {
  SSqlObj* pSql = (SSqlObj*) res;
  if (pSql == NULL || pSql->signature != pSql) {
    tscError("0x%"PRIx64" already released sqlObj", pSql ? pSql->self : -1);
    return;
  }

  bool freeNow = tscKillQueryInDnode(pSql);
  if (freeNow) {
    tscDebug("0x%"PRIx64" free sqlObj in cache", pSql->self);
    taosReleaseRef(tscObjRef, pSql->self);
  }
}

int taos_errno(TAOS_RES *tres) {
  SSqlObj *pSql = (SSqlObj *) tres;
  if (pSql == NULL || pSql->signature != pSql) {
    return terrno;
  }

  return pSql->res.code;
}

/*
 * In case of invalid sql/sql syntax error, additional information is attached to explain
 * why the sql is invalid
 */
static bool hasAdditionalErrorInfo(int32_t code, SSqlCmd *pCmd) {
  if (code != TSDB_CODE_TSC_INVALID_OPERATION
      && code != TSDB_CODE_TSC_SQL_SYNTAX_ERROR) {
    return false;
  }

  size_t len = strlen(pCmd->payload);

  char *z = NULL;
  if (len > 0) {
      z = strstr(pCmd->payload, "invalid SQL");
      if (z == NULL) {
        z = strstr(pCmd->payload, "syntax error");
      }
  }
  return z != NULL;
}

// todo should not be used in async model
char *taos_errstr(TAOS_RES *tres) {
  SSqlObj *pSql = (SSqlObj *) tres;

  if (pSql == NULL || pSql->signature != pSql) {
    return (char*) tstrerror(terrno);
  }

  if (hasAdditionalErrorInfo(pSql->res.code, &pSql->cmd)) {
    return pSql->cmd.payload;
  } else {
    return (char*)tstrerror(pSql->res.code);
  }
}

void taos_config(int debug, char *log_path) {
  uDebugFlag = debug;
  tstrncpy(tsLogDir, log_path, TSDB_FILENAME_LEN);
}

char *taos_get_server_info(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;

  if (pObj == NULL) return NULL;

  return pObj->sversion;
}

int* taos_fetch_lengths(TAOS_RES *res) {
  SSqlObj* pSql = (SSqlObj* ) res;
  if (pSql == NULL || pSql->signature != pSql) {
    return NULL;
  }
  
  return pSql->res.length;
}

char *taos_get_client_info() { return version; }

static void tscKillSTableQuery(SSqlObj *pSql) {
  SSqlCmd* pCmd = &pSql->cmd;

  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);

  if (!tscIsTwoStageSTableQuery(pQueryInfo, 0)) {
    return;
  }

  // set the master sqlObj flag to cancel query
  pSql->res.code = TSDB_CODE_TSC_QUERY_CANCELLED;

  tscLockByThread(&pSql->squeryLock);
  
  for (int i = 0; i < pSql->subState.numOfSub; ++i) {
    // NOTE: pSub may have been released already here
    SSqlObj *pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }

    SSqlObj* pSubObj = pSub;

    pSubObj->res.code = TSDB_CODE_TSC_QUERY_CANCELLED;
    if (pSubObj->rpcRid > 0) {
      rpcCancelRequest(pSubObj->rpcRid);
      pSubObj->rpcRid = -1;
    }

    tscAsyncResultOnError(pSubObj);
    // taosRelekaseRef(tscObjRef, pSubObj->self);
  }

  if (pSql->subState.numOfSub <= 0) {
    tscAsyncResultOnError(pSql);
  }

  tscUnlockByThread(&pSql->squeryLock);

  tscDebug("0x%"PRIx64" super table query cancelled", pSql->self);
}

void taos_stop_query(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) {
    return;
  }

  tscDebug("0x%"PRIx64" start to cancel query", pSql->self);
  SSqlCmd *pCmd = &pSql->cmd;

  // set the error code for master pSqlObj firstly
  pSql->res.code = TSDB_CODE_TSC_QUERY_CANCELLED;

  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);

  if (tscIsTwoStageSTableQuery(pQueryInfo, 0)) {
    assert(pSql->rpcRid <= 0);
    tscKillSTableQuery(pSql);
  } else {
    if (pSql->cmd.command < TSDB_SQL_LOCAL) {
      /*
       * There is multi-thread problem here, since pSql->pRpcCtx may have been
       * reset and freed in the processMsgFromServer function, and causes the invalid
       * write problem for rpcCancelRequest.
       */
      if (pSql->rpcRid > 0) {
        rpcCancelRequest(pSql->rpcRid);
        pSql->rpcRid = -1;
      }

      tscAsyncResultOnError(pSql);
    }
  }

  tscDebug("0x%"PRIx64" query is cancelled", pSql->self);
}

bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) {
    return true;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  if (pQueryInfo == NULL) {
    return true;
  }

  SInternalField* pInfo = (SInternalField*)TARRAY_GET_ELEM(pQueryInfo->fieldsInfo.internalField, col);
  if (col < 0 || col >= tscNumOfFields(pQueryInfo) || row < 0 || row > pSql->res.numOfRows) {
    return true;
  }

  return isNull(((char*) pSql->res.urow[col]) + row * pInfo->field.bytes, pInfo->field.type);
}

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  int len = 0;

  for (int i = 0; i < num_fields; ++i) {
    if (i > 0) {
      str[len++] = ' ';
    }

    if (row[i] == NULL) {
      len += sprintf(str + len, "%s", TSDB_DATA_NULL_STR);
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_TINYINT:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UTINYINT:
        len += sprintf(str + len, "%u", *((uint8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        len += sprintf(str + len, "%d", *((int16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_USMALLINT:
        len += sprintf(str + len, "%u", *((uint16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_INT:
        len += sprintf(str + len, "%d", *((int32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UINT:
        len += sprintf(str + len, "%u", *((uint32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BIGINT:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UBIGINT:
        len += sprintf(str + len, "%" PRIu64, *((uint64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_FLOAT: {
        float fv = 0;
        fv = GET_FLOAT_VAL(row[i]);
        len += sprintf(str + len, "%f", fv);
      } break;

      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = 0;
        dv = GET_DOUBLE_VAL(row[i]);
        len += sprintf(str + len, "%lf", dv);
      } break;

      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR: {
        int32_t charLen = varDataLen((char*)row[i] - VARSTR_HEADER_SIZE);
        if (fields[i].type == TSDB_DATA_TYPE_BINARY) {
          assert(charLen <= fields[i].bytes && charLen >= 0);
        } else {
          assert(charLen <= fields[i].bytes * TSDB_NCHAR_SIZE && charLen >= 0);
        }

        memcpy(str + len, row[i], charLen);
        len += charLen;
      } break;

      case TSDB_DATA_TYPE_TIMESTAMP:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BOOL:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
      default:
        break;
    }
  }

  return len;
}

static void asyncCallback(void *param, TAOS_RES *tres, int code) {
  assert(param != NULL);
  SSqlObj *pSql = ((SSqlObj *)param);
  pSql->res.code = code;
  tsem_post(&pSql->rspSem);
}

int taos_validate_sql(TAOS *taos, const char *sql) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));

  pSql->pTscObj  = taos;
  pSql->signature = pSql;
  SSqlCmd *pCmd = &pSql->cmd;
  
  pCmd->resColumnId = TSDB_RES_COL_ID;

  tscDebug("0x%"PRIx64" Valid SQL: %s pObj:%p", pSql->self, sql, pObj);

  int32_t sqlLen = (int32_t)strlen(sql);
  if (sqlLen > tsMaxSQLStringLen) {
    tscError("0x%"PRIx64" sql too long", pSql->self);
    tfree(pSql);
    return TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
  }

  pSql->sqlstr = realloc(pSql->sqlstr, sqlLen + 1);
  if (pSql->sqlstr == NULL) {
    tscError("0x%"PRIx64" failed to malloc sql string buffer", pSql->self);
    tfree(pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  strtolower(pSql->sqlstr, sql);

//  pCmd->curSql = NULL;
  if (NULL != pCmd->insertParam.pTableBlockHashList) {
    taosHashCleanup(pCmd->insertParam.pTableBlockHashList);
    pCmd->insertParam.pTableBlockHashList = NULL;
  }

  pSql->fp = asyncCallback;
  pSql->fetchFp = asyncCallback;
  pSql->param = pSql;

  registerSqlObj(pSql);
  int code = tsParseSql(pSql, true);
  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    tsem_wait(&pSql->rspSem);
    code = pSql->res.code;
  }

  if (code != TSDB_CODE_SUCCESS) {
    tscError("0x%"PRIx64" invalid SQL result:%d, %s pObj:%p", pSql->self, code, taos_errstr(pSql), pObj);
  }

  taos_free_result(pSql);
  return code;
}

void loadMultiTableMetaCallback(void *param, TAOS_RES *res, int code) {
  SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, (int64_t)param);
  if (pSql == NULL) {
    return;
  }

  taosReleaseRef(tscObjRef, pSql->self);
  pSql->res.code = code;
  tsem_post(&pSql->rspSem);
}

static void freeElem(void* p) {
  tfree(*(char**)p);
}

int taos_load_table_info(TAOS *taos, const char *tableNameList) {
  const int32_t MAX_TABLE_NAME_LENGTH = 12 * 1024 * 1024;  // 12MB list

  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
  pSql->pTscObj = taos;
  pSql->signature = pSql;

  pSql->fp = NULL;        // todo set the correct callback function pointer
  pSql->cmd.pTableMetaMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);

  int32_t length = (int32_t)strlen(tableNameList);
  if (length > MAX_TABLE_NAME_LENGTH) {
    tscError("0x%"PRIx64" tableNameList too long, length:%d, maximum allowed:%d", pSql->self, length, MAX_TABLE_NAME_LENGTH);
    tscFreeSqlObj(pSql);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  char *str = calloc(1, length + 1);
  if (str == NULL) {
    tscError("0x%"PRIx64" failed to allocate sql string buffer", pSql->self);
    tscFreeSqlObj(pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  strtolower(str, tableNameList);
  SArray* plist = taosArrayInit(4, POINTER_BYTES);
  SArray* vgroupList = taosArrayInit(4, POINTER_BYTES);

  int32_t code = (uint8_t) tscTransferTableNameList(pSql, str, length, plist);
  free(str);

  if (code != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pSql);
    return code;
  }

  registerSqlObj(pSql);
  tscDebug("0x%"PRIx64" load multiple table meta, tableNameList: %s pObj:%p", pSql->self, tableNameList, pObj);

  code = getMultiTableMetaFromMnode(pSql, plist, vgroupList, loadMultiTableMetaCallback);
  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    code = TSDB_CODE_SUCCESS;
  }

  taosArrayDestroyEx(plist, freeElem);
  taosArrayDestroyEx(vgroupList, freeElem);

  if (code != TSDB_CODE_SUCCESS) {
    tscFreeRegisteredSqlObj(pSql);
    return code;
  }

  tsem_wait(&pSql->rspSem);
  tscFreeRegisteredSqlObj(pSql);
  return code;
}
