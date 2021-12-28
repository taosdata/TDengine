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
#include "trpc.h"

int (*handleRequestRspFp[TDMT_MAX])(void*, const SDataBuf* pMsg, int32_t code);

int genericRspCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  pRequest->code = code;
  sem_post(&pRequest->body.rspSem);
  return 0;
}

int processConnectRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  if (code != TSDB_CODE_SUCCESS) {
    pRequest->code = code;
    terrno         = code;

    sem_post(&pRequest->body.rspSem);
    return code;
  }

  STscObj *pTscObj = pRequest->pTscObj;

  SConnectRsp *pConnect = (SConnectRsp *)pMsg->pData;
  pConnect->acctId    = htonl(pConnect->acctId);
  pConnect->connId    = htonl(pConnect->connId);
  pConnect->clusterId = htobe64(pConnect->clusterId);

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

  //  pRequest->body.resInfo.pRspMsg = pMsg->pData;
  tscDebug("0x%" PRIx64 " clusterId:%" PRId64 ", totalConn:%" PRId64, pRequest->requestId, pConnect->clusterId,
           pTscObj->pAppInfo->numOfConns);

  sem_post(&pRequest->body.rspSem);
  return 0;
}

static int32_t buildRetrieveMnodeMsg(SRequestObj *pRequest, SMsgSendInfo* pMsgSendInfo) {
  pMsgSendInfo->msgType         = TDMT_MND_SHOW_RETRIEVE;
  pMsgSendInfo->msgInfo.len     = sizeof(SRetrieveTableMsg);
  pMsgSendInfo->requestObjRefId = pRequest->self;
  pMsgSendInfo->param           = pRequest;
  pMsgSendInfo->fp              = handleRequestRspFp[TMSG_INDEX(pMsgSendInfo->msgType)];

  SRetrieveTableMsg *pRetrieveMsg = calloc(1, sizeof(SRetrieveTableMsg));
  if (pRetrieveMsg == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pRetrieveMsg->showId  = htonl(pRequest->body.execId);
  pMsgSendInfo->msgInfo.pData = pRetrieveMsg;
  return TSDB_CODE_SUCCESS;
}

SMsgSendInfo* buildSendMsgInfoImpl(SRequestObj *pRequest) {
  SMsgSendInfo* pMsgSendInfo = calloc(1, sizeof(SMsgSendInfo));

  if (pRequest->type == TDMT_MND_SHOW_RETRIEVE) {
    buildRetrieveMnodeMsg(pRequest, pMsgSendInfo);
  } else {
    assert(pRequest != NULL);
    pMsgSendInfo->requestObjRefId = pRequest->self;
    pMsgSendInfo->msgInfo   = pRequest->body.requestMsg;
    pMsgSendInfo->msgType   = pRequest->type;
    pMsgSendInfo->requestId = pRequest->requestId;
    pMsgSendInfo->param     = pRequest;

    pMsgSendInfo->fp = (handleRequestRspFp[TMSG_INDEX(pRequest->type)] == NULL)? genericRspCallback:handleRequestRspFp[TMSG_INDEX(pRequest->type)];
  }

  return pMsgSendInfo;
}

int32_t processShowRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  if (code != TSDB_CODE_SUCCESS) {
    pRequest->code = code;
    tsem_post(&pRequest->body.rspSem);
    return code;
  }

  SShowRsp* pShow = (SShowRsp *)pMsg->pData;
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

  pRequest->body.resInfo.pRspMsg = pMsg->pData;
  SReqResultInfo* pResInfo = &pRequest->body.resInfo;

  pResInfo->fields    = pFields;
  pResInfo->numOfCols = pMetaMsg->numOfColumns;
  pResInfo->row       = calloc(pResInfo->numOfCols, POINTER_BYTES);
  pResInfo->pCol      = calloc(pResInfo->numOfCols, POINTER_BYTES);
  pResInfo->length    = calloc(pResInfo->numOfCols, sizeof(int32_t));

  pRequest->body.execId = pShow->showId;
  tsem_post(&pRequest->body.rspSem);
  return 0;
}

int32_t processRetrieveMnodeRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  assert(pMsg->len >= sizeof(SRetrieveTableRsp));

  SRequestObj* pRequest = param;
//  tfree(pRequest->body.resInfo.pRspMsg);
//  pRequest->body.resInfo.pRspMsg = pMsg->pData;

  SRetrieveTableRsp *pRetrieve = (SRetrieveTableRsp *) pMsg->pData;
  pRetrieve->numOfRows  = htonl(pRetrieve->numOfRows);
  pRetrieve->precision  = htons(pRetrieve->precision);

  SReqResultInfo* pResInfo = &pRequest->body.resInfo;

  tfree(pResInfo->pRspMsg);
  pResInfo->pRspMsg   = pMsg->pData;
  pResInfo->numOfRows = pRetrieve->numOfRows;
  pResInfo->pData     = pRetrieve->data;              // todo fix this in async model

  pResInfo->current = 0;
  setResultDataPtr(pResInfo, pResInfo->fields, pResInfo->numOfCols, pResInfo->numOfRows);

  tscDebug("0x%"PRIx64" numOfRows:%d, complete:%d, qId:0x%"PRIx64, pRequest->self, pRetrieve->numOfRows,
           pRetrieve->completed, pRequest->body.execId);

  tsem_post(&pRequest->body.rspSem);
  return 0;
}

int32_t processCreateDbRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  // todo rsp with the vnode id list
  SRequestObj* pRequest = param;
  tsem_post(&pRequest->body.rspSem);
}

int32_t processUseDbRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  SUseDbRsp* pUseDbRsp = (SUseDbRsp*) pMsg->pData;
  SName name = {0};
  tNameFromString(&name, pUseDbRsp->db, T_NAME_ACCT|T_NAME_DB);

  char db[TSDB_DB_NAME_LEN] = {0};
  tNameGetDbName(&name, db);

  SRequestObj* pRequest = param;
  setConnectionDB(pRequest->pTscObj, db);

  tsem_post(&pRequest->body.rspSem);
  return 0;
}

int32_t processCreateTableRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  assert(pMsg != NULL);
  SRequestObj* pRequest = param;
  tsem_post(&pRequest->body.rspSem);
}

int32_t processDropDbRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  // todo: Remove cache in catalog cache.
  SRequestObj* pRequest = param;
  tsem_post(&pRequest->body.rspSem);
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

  handleRequestRspFp[TMSG_INDEX(TDMT_MND_CONNECT)]       = processConnectRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_SHOW)]          = processShowRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_SHOW_RETRIEVE)] = processRetrieveMnodeRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_CREATE_DB)]     = processCreateDbRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_USE_DB)]        = processUseDbRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_CREATE_STB)]    = processCreateTableRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_DROP_DB)]       = processDropDbRsp;
}