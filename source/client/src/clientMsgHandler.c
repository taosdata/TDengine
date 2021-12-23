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

#include "os.h"
#include "tdef.h"
#include "tname.h"
#include "clientInt.h"
#include "clientLog.h"
#include "tmsgtype.h"
#include "trpc.h"

int (*handleRequestRspFp[TSDB_MSG_TYPE_MAX])(SRequestObj *pRequest, const char* pMsg, int32_t msgLen);

int32_t buildConnectMsg(SRequestObj *pRequest, SRequestMsgBody* pMsgBody) {
  pMsgBody->msgType         = TSDB_MSG_TYPE_CONNECT;
  pMsgBody->msgInfo.len     = sizeof(SConnectMsg);
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

  pMsgBody->msgInfo.pMsg = pConnect;
  return 0;
}

int processConnectRsp(SRequestObj *pRequest, const char* pMsg, int32_t msgLen) {
  STscObj *pTscObj = pRequest->pTscObj;

  SConnectRsp *pConnect = (SConnectRsp *)pMsg;
  pConnect->acctId    = htonl(pConnect->acctId);
  pConnect->connId    = htonl(pConnect->connId);
  pConnect->clusterId = htonl(pConnect->clusterId);

  assert(pConnect->epSet.numOfEps > 0);
  for(int32_t i = 0; i < pConnect->epSet.numOfEps; ++i) {
    pConnect->epSet.port[i] = htons(pConnect->epSet.port[i]);
  }

  if (!isEpsetEqual(&pTscObj->pAppInfo->mgmtEp.epSet, &pConnect->epSet)) {
    updateEpSet_s(&pTscObj->pAppInfo->mgmtEp, &pConnect->epSet);
  }

  for (int i = 0; i < pConnect->epSet.numOfEps; ++i) {
    tscDebug("0x%" PRIx64 " epSet.fqdn[%d]:%s port:%d, connObj:0x%"PRIx64, pRequest->requestId, i, pConnect->epSet.fqdn[i], pConnect->epSet.port[i], pTscObj->id);
  }

  pTscObj->connId = pConnect->connId;
  pTscObj->acctId = pConnect->acctId;

  // update the appInstInfo
  pTscObj->pAppInfo->clusterId = pConnect->clusterId;
  atomic_add_fetch_64(&pTscObj->pAppInfo->numOfConns, 1);

  pRequest->body.resInfo.pRspMsg = pMsg;
  tscDebug("0x%" PRIx64 " clusterId:%d, totalConn:%"PRId64, pRequest->requestId, pConnect->clusterId, pTscObj->pAppInfo->numOfConns);
  return 0;
}

static int32_t buildRetrieveMnodeMsg(SRequestObj *pRequest, SRequestMsgBody* pMsgBody) {
  pMsgBody->msgType = TSDB_MSG_TYPE_SHOW_RETRIEVE;
  pMsgBody->msgInfo.len = sizeof(SRetrieveTableMsg);
  pMsgBody->requestObjRefId = pRequest->self;

  SRetrieveTableMsg *pRetrieveMsg = calloc(1, sizeof(SRetrieveTableMsg));
  if (pRetrieveMsg == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pRetrieveMsg->showId  = htonl(pRequest->body.execId);
  pMsgBody->msgInfo.pMsg = pRetrieveMsg;
  return TSDB_CODE_SUCCESS;
}

SRequestMsgBody buildRequestMsgImpl(SRequestObj *pRequest) {
  if (pRequest->type == TSDB_MSG_TYPE_SHOW_RETRIEVE) {
    SRequestMsgBody body = {0};
    buildRetrieveMnodeMsg(pRequest, &body);
    return body;
  } else {
    assert(pRequest != NULL);
    SRequestMsgBody body = {
        .requestObjRefId = pRequest->self,
        .msgInfo = pRequest->body.requestMsg,
        .msgType = pRequest->type,
        .requestId = pRequest->requestId,
    };
    return body;
  }
}

int32_t processShowRsp(SRequestObj *pRequest, const char* pMsg, int32_t msgLen) {
  SShowRsp* pShow = (SShowRsp *)pMsg;
  pShow->showId   = htonl(pShow->showId);

  STableMetaMsg *pMetaMsg = &(pShow->tableMeta);
  pMetaMsg->numOfColumns = htonl(pMetaMsg->numOfColumns);

  SSchema* pSchema = pMetaMsg->pSchema;
  pMetaMsg->tuid = htobe64(pMetaMsg->tuid);
  for (int i = 0; i < pMetaMsg->numOfColumns; ++i) {
    pSchema->bytes = htonl(pSchema->bytes);
    pSchema++;
  }

  pSchema = pMetaMsg->pSchema;
  TAOS_FIELD* pFields = calloc(pMetaMsg->numOfColumns, sizeof(TAOS_FIELD));
  for (int32_t i = 0; i < pMetaMsg->numOfColumns; ++i) {
    tstrncpy(pFields[i].name, pSchema[i].name, tListLen(pFields[i].name));
    pFields[i].type  = pSchema[i].type;
    pFields[i].bytes = pSchema[i].bytes;
  }

  pRequest->body.resInfo.pRspMsg = pMsg;
  SReqResultInfo* pResInfo = &pRequest->body.resInfo;

  pResInfo->fields    = pFields;
  pResInfo->numOfCols = pMetaMsg->numOfColumns;
  pResInfo->row       = calloc(pResInfo->numOfCols, POINTER_BYTES);
  pResInfo->pCol      = calloc(pResInfo->numOfCols, POINTER_BYTES);
  pResInfo->length    = calloc(pResInfo->numOfCols, sizeof(int32_t));

  pRequest->body.execId = pShow->showId;
  return 0;
}

int32_t processRetrieveMnodeRsp(SRequestObj *pRequest, const char* pMsg, int32_t msgLen) {
  assert(msgLen >= sizeof(SRetrieveTableRsp));

  tfree(pRequest->body.resInfo.pRspMsg);
  pRequest->body.resInfo.pRspMsg = pMsg;

  SRetrieveTableRsp *pRetrieve = (SRetrieveTableRsp *) pMsg;
  pRetrieve->numOfRows  = htonl(pRetrieve->numOfRows);
  pRetrieve->precision  = htons(pRetrieve->precision);

  SReqResultInfo* pResInfo = &pRequest->body.resInfo;
  pResInfo->numOfRows = pRetrieve->numOfRows;
  pResInfo->pData = pRetrieve->data;              // todo fix this in async model

  pResInfo->current = 0;
  setResultDataPtr(pResInfo, pResInfo->fields, pResInfo->numOfCols, pResInfo->numOfRows);

  tscDebug("0x%"PRIx64" numOfRows:%d, complete:%d, qId:0x%"PRIx64, pRequest->self, pRetrieve->numOfRows,
           pRetrieve->completed, pRequest->body.execId);
  return 0;
}

int32_t processCreateDbRsp(SRequestObj *pRequest, const char* pMsg, int32_t msgLen) {
  // todo rsp with the vnode id list
}

int32_t processUseDbRsp(SRequestObj *pRequest, const char* pMsg, int32_t msgLen) {
  SUseDbRsp* pUseDbRsp = (SUseDbRsp*) pMsg;
  SName name = {0};
  tNameFromString(&name, pUseDbRsp->db, T_NAME_ACCT|T_NAME_DB);

  char db[TSDB_DB_NAME_LEN] = {0};
  tNameGetDbName(&name, db);
  setConnectionDB(pRequest->pTscObj, db);
}

int32_t processCreateTableRsp(SRequestObj *pRequest, const char* pMsg, int32_t msgLen) {
  assert(pMsg != NULL);
}

int32_t processDropDbRsp(SRequestObj *pRequest, const char* pMsg, int32_t msgLen) {
  // todo: Remove cache in catalog cache.
}

void initMsgHandleFp() {
#if 0
  tscBuildMsg[TSDB_SQL_SELECT] = tscBuildQueryMsg;
  tscBuildMsg[TSDB_SQL_INSERT] = tscBuildSubmitMsg;
  tscBuildMsg[TSDB_SQL_FETCH] = tscBuildFetchMsg;

  tscBuildMsg[TSDB_SQL_CREATE_DB] = tscBuildCreateDbMsg;
  tscBuildMsg[TSDB_SQL_CREATE_USER] = tscBuildUserMsg;
  tscBuildMsg[TSDB_SQL_CREATE_FUNCTION] = tscBuildCreateFuncMsg;

  tscBuildMsg[TSDB_SQL_CREATE_ACCT] = tscBuildAcctMsg;
  tscBuildMsg[TSDB_SQL_ALTER_ACCT] = tscBuildAcctMsg;

  tscBuildMsg[TSDB_SQL_CREATE_TABLE] = tscBuildCreateTableMsg;
  tscBuildMsg[TSDB_SQL_DROP_USER] = tscBuildDropUserAcctMsg;
  tscBuildMsg[TSDB_SQL_DROP_ACCT] = tscBuildDropUserAcctMsg;
  tscBuildMsg[TSDB_SQL_DROP_DB] = tscBuildDropDbMsg;
  tscBuildMsg[TSDB_SQL_DROP_FUNCTION] = tscBuildDropFuncMsg;
  tscBuildMsg[TSDB_SQL_SYNC_DB_REPLICA] = tscBuildSyncDbReplicaMsg;
  tscBuildMsg[TSDB_SQL_DROP_TABLE] = tscBuildDropTableMsg;
  tscBuildMsg[TSDB_SQL_ALTER_USER] = tscBuildUserMsg;
  tscBuildMsg[TSDB_SQL_CREATE_DNODE] = tscBuildCreateDnodeMsg;
  tscBuildMsg[TSDB_SQL_DROP_DNODE] = tscBuildDropDnodeMsg;
  tscBuildMsg[TSDB_SQL_CFG_DNODE] = tscBuildCfgDnodeMsg;
  tscBuildMsg[TSDB_SQL_ALTER_TABLE] = tscBuildAlterTableMsg;
  tscBuildMsg[TSDB_SQL_UPDATE_TAG_VAL] = tscBuildUpdateTagMsg;
  tscBuildMsg[TSDB_SQL_ALTER_DB] = tscAlterDbMsg;
  tscBuildMsg[TSDB_SQL_COMPACT_VNODE] = tscBuildCompactMsg;


  tscBuildMsg[TSDB_SQL_USE_DB] = tscBuildUseDbMsg;
  tscBuildMsg[TSDB_SQL_STABLEVGROUP] = tscBuildSTableVgroupMsg;
  tscBuildMsg[TSDB_SQL_RETRIEVE_FUNC] = tscBuildRetrieveFuncMsg;

  tscBuildMsg[TSDB_SQL_HB] = tscBuildHeartBeatMsg;
  tscBuildMsg[TSDB_SQL_SHOW] = tscBuildShowMsg;
  tscBuildMsg[TSDB_SQL_RETRIEVE_MNODE] = tscBuildRetrieveFromMgmtMsg;
  tscBuildMsg[TSDB_SQL_KILL_QUERY] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_STREAM] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_CONNECTION] = tscBuildKillMsg;

  tscProcessMsgRsp[TSDB_SQL_SELECT] = tscProcessQueryRsp;
  tscProcessMsgRsp[TSDB_SQL_FETCH] = tscProcessRetrieveRspFromNode;

  tscProcessMsgRsp[TSDB_SQL_DROP_DB] = tscProcessDropDbRsp;
  tscProcessMsgRsp[TSDB_SQL_DROP_TABLE] = tscProcessDropTableRsp;

  tscProcessMsgRsp[TSDB_SQL_USE_DB] = tscProcessUseDbRsp;
  tscProcessMsgRsp[TSDB_SQL_META] = tscProcessTableMetaRsp;
  tscProcessMsgRsp[TSDB_SQL_STABLEVGROUP] = tscProcessSTableVgroupRsp;
  tscProcessMsgRsp[TSDB_SQL_MULTI_META] = tscProcessMultiTableMetaRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_FUNC] = tscProcessRetrieveFuncRsp;

  tscProcessMsgRsp[TSDB_SQL_SHOW] = tscProcessShowRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_MNODE] = tscProcessRetrieveRspFromNode;  // rsp handled by same function.
  tscProcessMsgRsp[TSDB_SQL_DESCRIBE_TABLE] = tscProcessDescribeTableRsp;

  tscProcessMsgRsp[TSDB_SQL_CURRENT_DB]   = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CURRENT_USER] = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_VERSION] = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CLI_VERSION]  = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_STATUS]  = tscProcessLocalRetrieveRsp;

  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_EMPTY_RESULT] = tscProcessEmptyResultRsp;

  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_GLOBALMERGE] = tscProcessRetrieveGlobalMergeRsp;

  tscProcessMsgRsp[TSDB_SQL_ALTER_TABLE] = tscProcessAlterTableMsgRsp;
  tscProcessMsgRsp[TSDB_SQL_ALTER_DB] = tscProcessAlterDbMsgRsp;
  tscProcessMsgRsp[TSDB_SQL_COMPACT_VNODE] = tscProcessCompactRsp;

  tscProcessMsgRsp[TSDB_SQL_SHOW_CREATE_TABLE] = tscProcessShowCreateRsp;
  tscProcessMsgRsp[TSDB_SQL_SHOW_CREATE_STABLE] = tscProcessShowCreateRsp;
  tscProcessMsgRsp[TSDB_SQL_SHOW_CREATE_DATABASE] = tscProcessShowCreateRsp;
#endif

//  buildRequestMsgFp[TSDB_MSG_TYPE_CONNECT]  = buildConnectMsg;
//  buildRequestMsgFp[TSDB_MSG_TYPE_SHOW_RETRIEVE] = buildRetrieveMnodeMsg;

  handleRequestRspFp[TSDB_MSG_TYPE_CONNECT]       = processConnectRsp;
  handleRequestRspFp[TSDB_MSG_TYPE_SHOW]          = processShowRsp;
  handleRequestRspFp[TSDB_MSG_TYPE_SHOW_RETRIEVE] = processRetrieveMnodeRsp;
  handleRequestRspFp[TSDB_MSG_TYPE_CREATE_DB]     = processCreateDbRsp;
  handleRequestRspFp[TSDB_MSG_TYPE_USE_DB]        = processUseDbRsp;
  handleRequestRspFp[TSDB_MSG_TYPE_CREATE_TABLE]  = processCreateTableRsp;
  handleRequestRspFp[TSDB_MSG_TYPE_DROP_DB]       = processDropDbRsp;
}