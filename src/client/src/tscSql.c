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
#include "qAst.h"
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
  return validImpl(passwd, TSDB_PASSWORD_LEN - 1);
}

SSqlObj *taosConnectImpl(const char *ip, const char *user, const char *pass, const char *auth, const char *db,
                         uint16_t port, void (*fp)(void *, TAOS_RES *, int), void *param, void **taos) {
  taos_init();

  if (!validUserName(user)) {
    terrno = TSDB_CODE_TSC_INVALID_USER_LENGTH;
    return NULL;
  }

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
    if (tscSetMgmtEpSetFromCfg(ip, NULL) < 0) return NULL;
    if (port) tscMgmtEpSet.epSet.port[0] = port;
  } 
 
  void *pDnodeConn = NULL;
  if (tscInitRpc(user, secretEncrypt, &pDnodeConn) != 0) {
    terrno = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    return NULL;
  }
 
  STscObj *pObj = (STscObj *)calloc(1, sizeof(STscObj));
  if (NULL == pObj) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    rpcClose(pDnodeConn);
    return NULL;
  }

  pObj->signature = pObj;

  tstrncpy(pObj->user, user, sizeof(pObj->user));
  secretEncryptLen = MIN(secretEncryptLen, sizeof(pObj->pass));
  memcpy(pObj->pass, secretEncrypt, secretEncryptLen);

  if (db) {
    int32_t len = (int32_t)strlen(db);
    /* db name is too long */
    if (len >= TSDB_DB_NAME_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_DB_LENGTH;
      rpcClose(pDnodeConn);
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
    rpcClose(pDnodeConn);
    free(pObj);
    return NULL;
  }

  pSql->pTscObj = pObj;
  pSql->signature = pSql;
  pSql->maxRetry = TSDB_MAX_REPLICA;
  tsem_init(&pSql->rspSem, 0, 0);
  
  pObj->pDnodeConn = pDnodeConn;
  
  pSql->fp = fp;
  pSql->param = param;
  if (taos != NULL) {
    *taos = pObj;
  }

  pSql->cmd.command = TSDB_SQL_CONNECT;
  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pSql->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    rpcClose(pDnodeConn);
    free(pSql);
    free(pObj);
    return NULL;
  }

  tsInsertHeadSize = sizeof(SMsgDesc) + sizeof(SSubmitMsg);
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

    tscProcessSql(pSql);
    tsem_wait(&pSql->rspSem);

    if (pSql->res.code != TSDB_CODE_SUCCESS) {
      terrno = pSql->res.code;
      taos_free_result(pSql);
      taos_close(pObj);
      return NULL;
    }
    
    tscDebug("%p DB connection is opening, dnodeConn:%p", pObj, pObj->pDnodeConn);
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
  tscDebug("try to create a connection to %s:%u, user:%s db:%s", ip, port, user, db);
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
  char passBuf[TSDB_PASSWORD_LEN] = {0};
  char dbBuf[TSDB_DB_NAME_LEN] = {0};
  strncpy(ipBuf, ip, MIN(TSDB_EP_LEN - 1, ipLen));
  strncpy(userBuf, user, MIN(TSDB_USER_LEN - 1, userLen));
  strncpy(passBuf, pass, MIN(TSDB_PASSWORD_LEN - 1, passLen));
  strncpy(dbBuf, db, MIN(TSDB_DB_NAME_LEN - 1, dbLen));
  return taos_connect(ipBuf, userBuf, passBuf, dbBuf, port);
}

TAOS *taos_connect_a(char *ip, char *user, char *pass, char *db, uint16_t port, void (*fp)(void *, TAOS_RES *, int),
                     void *param, void **taos) {
  SSqlObj* pSql = taosConnectImpl(ip, user, pass, NULL, db, port, fp, param, taos);
  if (pSql == NULL) {
    return NULL;
  }
  
  pSql->res.code = tscProcessSql(pSql);
  tscDebug("%p DB async connection is opening", taos);
  return taos;
}

void taos_close(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;

  if (pObj == NULL || pObj->signature != pObj)  {
    return;
  }

  if (pObj->pHb != NULL) {
    if (pObj->pHb->pRpcCtx != NULL) {  // wait for rsp from dnode
      rpcCancelRequest(pObj->pHb->pRpcCtx);
    }

    tscSetFreeHeatBeat(pObj);
    tscFreeSqlObj(pObj->pHb);
  }

  tscCloseTscObj(pObj);
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

TAOS_RES* taos_query_c(TAOS *taos, const char *sqlstr, uint32_t sqlLen) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }
  
  if (sqlLen > (uint32_t)tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_TSC_INVALID_SQL;
    return NULL;
  }

  taosNotePrintTsc(sqlstr);

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    tscError("failed to malloc sqlObj");
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }
  
  tsem_init(&pSql->rspSem, 0, 0);
  doAsyncQuery(pObj, pSql, waitForQueryRsp, taos, sqlstr, sqlLen);

  tsem_wait(&pSql->rspSem);
  return pSql; 
}

TAOS_RES* taos_query(TAOS *taos, const char *sqlstr) {
  return taos_query_c(taos, sqlstr, (uint32_t)strlen(sqlstr));
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
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  if (pQueryInfo == NULL) {
    return num;
  }

  size_t numOfCols = tscNumOfFields(pQueryInfo);
  for(int32_t i = 0; i < numOfCols; ++i) {
    SFieldSupInfo* pInfo = taosArrayGet(pQueryInfo->fieldsInfo.pSupportInfo, i);
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

  return (int)(pSql->res.numOfRows);
}

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) return 0;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  if (pQueryInfo == NULL) {
    return NULL;
  }
  
  size_t numOfCols = tscNumOfFields(pQueryInfo);
  if (numOfCols == 0) {
    return NULL;
  }
  
  return pQueryInfo->fieldsInfo.pFields->pData;
}

int taos_retrieve(TAOS_RES *res) {
  if (res == NULL) return 0;
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  if (pSql == NULL || pSql->signature != pSql) return 0;
  if (pRes->qhandle == 0) return 0;

  tscResetForNextRetrieve(pRes);

  if (pCmd->command < TSDB_SQL_LOCAL) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
  }
  tscProcessSql(pSql);

  return (int)pRes->numOfRows;
}

int taos_fetch_block_impl(TAOS_RES *res, TAOS_ROW *rows) {
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pRes->qhandle == 0 || pSql->signature != pSql) {
    *rows = NULL;
    return 0;
  }

  // Retrieve new block
  tscResetForNextRetrieve(pRes);
  if (pCmd->command < TSDB_SQL_LOCAL) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
  }

  tscProcessSql(pSql);
  if (pRes->numOfRows == 0) {
    *rows = NULL;
    return 0;
  }

  // secondary merge has handle this situation
  if (pCmd->command != TSDB_SQL_RETRIEVE_LOCALMERGE) {
    pRes->numOfClauseTotal += pRes->numOfRows;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  if (pQueryInfo == NULL)
    return 0;

  assert(0);
  for (int i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    tscGetResultColumnChr(pRes, &pQueryInfo->fieldsInfo, i);
  }

  *rows = pRes->tsrow;

  return (int)((pQueryInfo->order.order == TSDB_ORDER_DESC) ? pRes->numOfRows : -pRes->numOfRows);
}

TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }
  
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  
  if (pRes->qhandle == 0 ||
      pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      pCmd->command == TSDB_SQL_INSERT) {
    return NULL;
  }

  // set the sql object owner
  tscSetSqlOwner(pSql);

  // current data set are exhausted, fetch more data from node
  if (pRes->row >= pRes->numOfRows && (pRes->completed != true || hasMoreVnodesToTry(pSql) || hasMoreClauseToTry(pSql)) &&
      (pCmd->command == TSDB_SQL_RETRIEVE ||
       pCmd->command == TSDB_SQL_RETRIEVE_LOCALMERGE ||
       pCmd->command == TSDB_SQL_TABLE_JOIN_RETRIEVE ||
       pCmd->command == TSDB_SQL_FETCH ||
       pCmd->command == TSDB_SQL_SHOW ||
       pCmd->command == TSDB_SQL_SELECT ||
       pCmd->command == TSDB_SQL_DESCRIBE_TABLE ||
       pCmd->command == TSDB_SQL_SERV_STATUS ||
       pCmd->command == TSDB_SQL_CURRENT_DB ||
       pCmd->command == TSDB_SQL_SERV_VERSION ||
       pCmd->command == TSDB_SQL_CLI_VERSION ||
       pCmd->command == TSDB_SQL_CURRENT_USER )) {
    taos_fetch_rows_a(res, waitForRetrieveRsp, pSql->pTscObj);
    tsem_wait(&pSql->rspSem);
  }

  void* data = doSetResultRowData(pSql, true);

  tscClearSqlOwner(pSql);
  return data;
}

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
#if 0
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  int nRows = 0;

  if (pSql == NULL || pSql->signature != pSql) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    *rows = NULL;
    return 0;
  }

  // projection query on metric, pipeline retrieve data from vnode list,
  // instead of two-stage mergednodeProcessMsgFromShell free qhandle
  nRows = taos_fetch_block_impl(res, rows);

  // current subclause is completed, try the next subclause
  while (rows == NULL && pCmd->clauseIndex < pCmd->numOfClause - 1) {
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

    pSql->cmd.command = pQueryInfo->command;
    pCmd->clauseIndex++;

    pRes->numOfTotal += pRes->numOfClauseTotal;
    pRes->numOfClauseTotal = 0;
    pRes->rspType = 0;

    pSql->numOfSubs = 0;
    taosTFree(pSql->pSubs);

    assert(pSql->fp == NULL);

    tscDebug("%p try data in the next subclause:%d, total subclause:%d", pSql, pCmd->clauseIndex, pCmd->numOfClause);
    tscProcessSql(pSql);

    nRows = taos_fetch_block_impl(res, rows);
  }

  return nRows;
#endif

  (*rows) = taos_fetch_row(res);
  return ((*rows) != NULL)? 1:0;
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
static bool tscKillQueryInVnode(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  if (pRes->code == TSDB_CODE_SUCCESS && pRes->completed == false && !tscIsTwoStageSTableQuery(pQueryInfo, 0) &&
      (pCmd->command == TSDB_SQL_SELECT ||
       pCmd->command == TSDB_SQL_SHOW ||
       pCmd->command == TSDB_SQL_RETRIEVE ||
       pCmd->command == TSDB_SQL_FETCH) &&
      (pSql->pStream == NULL && pTableMetaInfo->pTableMeta != NULL)) {

    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
    tscDebug("%p send msg to dnode to free qhandle ASAP, command:%s, ", pSql, sqlCmd[pCmd->command]);
    tscProcessSql(pSql);
    return true;
  }

  return false;
}

void taos_free_result(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;

  if (pSql == NULL || pSql->signature != pSql) {
    tscDebug("%p sqlObj has been freed", pSql);
    return;
  }
  
  // The semaphore can not be changed while freeing async sub query objects.
  SSqlRes *pRes = &pSql->res;
  if (pRes == NULL || pRes->qhandle == 0) {
    tscFreeSqlObj(pSql);
    tscDebug("%p SqlObj is freed by app, qhandle is null", pSql);
    return;
  }

  // set freeFlag to 1 in retrieve message if there are un-retrieved results data in node
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  if (pQueryInfo == NULL) {
    tscFreeSqlObj(pSql);
    tscDebug("%p SqlObj is freed by app", pSql);
    return;
  }

  pQueryInfo->type = TSDB_QUERY_TYPE_FREE_RESOURCE;
  if (!tscKillQueryInVnode(pSql)) {
    tscFreeSqlObj(pSql);
    tscDebug("%p sqlObj is freed by app", pSql);
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
 * In case of invalid sql error, additional information is attached to explain
 * why the sql is invalid
 */
static bool hasAdditionalErrorInfo(int32_t code, SSqlCmd *pCmd) {
  if (code != TSDB_CODE_TSC_INVALID_SQL) {
    return false;
  }

  size_t len = strlen(pCmd->payload);

  char *z = NULL;
  if (len > 0) {
    z = strstr(pCmd->payload, "invalid SQL");
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

void taos_stop_query(TAOS_RES *res) {
  if (res == NULL) {
    return;
  }

  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;

  if (pSql->signature != pSql) return;
  tscDebug("%p start to cancel query", res);


  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (tscIsTwoStageSTableQuery(pQueryInfo, 0)) {
    tscKillSTableQuery(pSql);
  }

  if (pSql->cmd.command < TSDB_SQL_LOCAL) {
    rpcCancelRequest(pSql->pRpcCtx);
  }
  pSql->res.code = TSDB_CODE_TSC_QUERY_CANCELLED;
  tscQueueAsyncRes(pSql);

  tscDebug("%p query is cancelled", res);
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
        len += sprintf(str + len, "%d", *((char *)row[i]));
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        len += sprintf(str + len, "%d", *((short *)row[i]));
        break;

      case TSDB_DATA_TYPE_INT:
        len += sprintf(str + len, "%d", *((int *)row[i]));
        break;

      case TSDB_DATA_TYPE_BIGINT:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
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
        size_t xlen = 0;
        for (xlen = 0; xlen < fields[i].bytes - VARSTR_HEADER_SIZE; xlen++) {
          char c = ((char *)row[i])[xlen];
          if (c == 0) break;
          str[len++] = c;
        }
        str[len] = 0;
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
  pSql->pTscObj = taos;
  pSql->signature = pSql;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  pRes->numOfTotal = 0;
  pRes->numOfClauseTotal = 0;

  tscDebug("%p Valid SQL: %s pObj:%p", pSql, sql, pObj);

  int32_t sqlLen = (int32_t)strlen(sql);
  if (sqlLen > tsMaxSQLStringLen) {
    tscError("%p sql too long", pSql);
    pRes->code = TSDB_CODE_TSC_INVALID_SQL;
    taosTFree(pSql);
    return pRes->code;
  }

  pSql->sqlstr = realloc(pSql->sqlstr, sqlLen + 1);
  if (pSql->sqlstr == NULL) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("%p failed to malloc sql string buffer", pSql);
    tscDebug("%p Valid SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
    taosTFree(pSql);
    return pRes->code;
  }

  strtolower(pSql->sqlstr, sql);

  pCmd->curSql = NULL;
  if (NULL != pCmd->pTableList) {
    taosHashCleanup(pCmd->pTableList);
    pCmd->pTableList = NULL;
  }

  pSql->fp = asyncCallback;
  pSql->fetchFp = asyncCallback;
  pSql->param = pSql;
  int code = tsParseSql(pSql, true);
  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    tsem_wait(&pSql->rspSem);
    code = pSql->res.code;
  }
  if (code != TSDB_CODE_SUCCESS) {
    tscDebug("%p Valid SQL result:%d, %s pObj:%p", pSql, code, taos_errstr(taos), pObj);
  }
  taos_free_result(pSql);

  return code;
}

static int tscParseTblNameList(SSqlObj *pSql, const char *tblNameList, int32_t tblListLen) {
  // must before clean the sqlcmd object
  tscResetSqlCmdObj(&pSql->cmd);

  SSqlCmd *pCmd = &pSql->cmd;

  pCmd->command = TSDB_SQL_MULTI_META;
  pCmd->count = 0;

  int   code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  char *str = (char *)tblNameList;

  SQueryInfo *pQueryInfo = NULL;
  tscGetQueryInfoDetailSafely(pCmd, pCmd->clauseIndex, &pQueryInfo);

  STableMetaInfo *pTableMetaInfo = tscAddEmptyMetaInfo(pQueryInfo);

  if ((code = tscAllocPayload(pCmd, tblListLen + 16)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  char *nextStr;
  char  tblName[TSDB_TABLE_FNAME_LEN];
  int   payloadLen = 0;
  char *pMsg = pCmd->payload;
  while (1) {
    nextStr = strchr(str, ',');
    if (nextStr == NULL) {
      break;
    }

    memcpy(tblName, str, nextStr - str);
    int32_t len = (int32_t)(nextStr - str);
    tblName[len] = '\0';

    str = nextStr + 1;
    len = (int32_t)strtrim(tblName);

    SSQLToken sToken = {.n = len, .type = TK_ID, .z = tblName};
    tSQLGetToken(tblName, &sToken.type);

    // Check if the table name available or not
    if (tscValidateName(&sToken) != TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
      sprintf(pCmd->payload, "table name is invalid");
      return code;
    }

    if ((code = tscSetTableFullName(pTableMetaInfo, &sToken, pSql)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (++pCmd->count > TSDB_MULTI_METERMETA_MAX_NUM) {
      code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
      sprintf(pCmd->payload, "tables over the max number");
      return code;
    }

    if (payloadLen + strlen(pTableMetaInfo->name) + 128 >= pCmd->allocSize) {
      char *pNewMem = realloc(pCmd->payload, pCmd->allocSize + tblListLen);
      if (pNewMem == NULL) {
        code = TSDB_CODE_TSC_OUT_OF_MEMORY;
        sprintf(pCmd->payload, "failed to allocate memory");
        return code;
      }

      pCmd->payload = pNewMem;
      pCmd->allocSize = pCmd->allocSize + tblListLen;
      pMsg = pCmd->payload;
    }

    payloadLen += sprintf(pMsg + payloadLen, "%s,", pTableMetaInfo->name);
  }

  *(pMsg + payloadLen) = '\0';
  pCmd->payloadLen = payloadLen + 1;

  return TSDB_CODE_SUCCESS;
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
  SSqlRes *pRes = &pSql->res;

  pRes->numOfTotal = 0;  // the number of getting table meta from server
  pRes->numOfClauseTotal = 0;

  pRes->code = 0;

  assert(pSql->fp == NULL);
  tscDebug("%p tableNameList: %s pObj:%p", pSql, tableNameList, pObj);

  int32_t tblListLen = (int32_t)strlen(tableNameList);
  if (tblListLen > MAX_TABLE_NAME_LENGTH) {
    tscError("%p tableNameList too long, length:%d, maximum allowed:%d", pSql, tblListLen, MAX_TABLE_NAME_LENGTH);
    pRes->code = TSDB_CODE_TSC_INVALID_SQL;
    taosTFree(pSql);
    return pRes->code;
  }

  char *str = calloc(1, tblListLen + 1);
  if (str == NULL) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("%p failed to malloc sql string buffer", pSql);
    taosTFree(pSql);
    return pRes->code;
  }

  strtolower(str, tableNameList);
  pRes->code = (uint8_t)tscParseTblNameList(pSql, str, tblListLen);

  /*
   * set the qhandle to 0 before return in order to erase the qhandle value assigned in the previous successful query.
   * If qhandle is NOT set 0, the function of taos_free_result() will send message to server by calling tscProcessSql()
   * to free connection, which may cause segment fault, when the parse phrase is not even successfully executed.
   */
  pRes->qhandle = 0;
  free(str);

  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pSql);
    return pRes->code;
  }

  tscDoQuery(pSql);

  tscDebug("%p load multi metermeta result:%d %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscPartiallyFreeSqlObj(pSql);
  }

  return pRes->code;
}
