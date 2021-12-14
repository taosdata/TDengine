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

#include "taosmsg.h"
#include "commonint.h"


int32_t (*tscBuildMsg[TSDB_MSG_TYPE_MAX])(void* input, char **msg, int32_t msgSize, int32_t *msgLen) = {0};

int32_t (*tscProcessMsgRsp[TSDB_MSG_TYPE_MAX])(void* output, char *msg, int32_t msgSize) = {0};


int32_t tscBuildVgroupListReqMsg(void* input, char **msg, int32_t msgSize, int32_t *msgLen) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  *msgLen = 0;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildTableMetaReqMsg(void* input, char **msg, int32_t msgSize, int32_t *msgLen) {
  if (NULL == input || NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SBuildTableMetaInput* bInput = (SBuildTableMetaInput *)input;

  int32_t estimateSize = sizeof(STableInfoMsg);
  if (NULL == *msg || msgSize < estimateSize) {
    tfree(*msg);
    *msg = calloc(1, estimateSize);
    if (NULL == *msg) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  STableInfoMsg *bMsg = (STableInfoMsg *)*msg;

  bMsg->msgHead.vgId = bInput->vgId;

  strncpy(bMsg->tableFname, bInput->tableFullName, sizeof(bMsg->tableFname));
  bMsg->tableFname[sizeof(bMsg->tableFname) - 1] = 0;

  *msgLen = (int32_t)sizeof(*bMsg);

  return TSDB_CODE_SUCCESS;
}


int32_t tscProcessVgroupListRsp(void* output, char *msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SVgroupListRspMsg *pRsp = (SVgroupListRspMsg *)msg;

  pRsp->vgroupNum = htonl(pRsp->vgroupNum);
  pRsp->vgroupVersion = htonl(pRsp->vgroupVersion);

  if (pRsp->vgroupNum < 0) {
    tscError("vgroup number[%d] in rsp is invalid", pRsp->vgroupNum);
    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
  }

  if (pRsp->vgroupVersion < 0) {
    tscError("vgroup vgroupVersion[%d] in rsp is invalid", pRsp->vgroupVersion);
    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
  }

  if (msgSize != (pRsp->vgroupNum * sizeof(pRsp->vgroupInfo[0]) + sizeof(*pRsp))) {
    tscError("vgroup list msg size mis-match, msgSize:%d, vgroup number:%d", msgSize, pRsp->vgroupNum);
    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
  }

  // keep SVgroupListInfo/SVgroupListRspMsg the same
  *(SVgroupListInfo **)output = (SVgroupListInfo *)msg;

  if (pRsp->vgroupNum == 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < pRsp->vgroupNum; ++i) {
    pRsp->vgroupInfo[i].vgId = htonl(pRsp->vgroupInfo[i].vgId);
    for (int32_t n = 0; n < pRsp->vgroupInfo[i].numOfEps; ++n) {
      pRsp->vgroupInfo[i].epAddr[n].port = htonl(pRsp->vgroupInfo[i].epAddr[n].port);
    }
  }

  return TSDB_CODE_SUCCESS;
}

void msgInit() {
  tscBuildMsg[TSDB_MSG_TYPE_TABLE_META] = tscBuildTableMetaReqMsg;
  tscBuildMsg[TSDB_MSG_TYPE_VGROUP_LIST] = tscBuildVgroupListReqMsg;


  //tscProcessMsgRsp[TSDB_MSG_TYPE_TABLE_META] = tscProcessTableMetaRsp;
  tscProcessMsgRsp[TSDB_MSG_TYPE_VGROUP_LIST] = tscProcessVgroupListRsp;

/*
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
  tscBuildMsg[TSDB_SQL_UPDATE_TAGS_VAL] = tscBuildUpdateTagMsg;
  tscBuildMsg[TSDB_SQL_ALTER_DB] = tscAlterDbMsg;
  tscBuildMsg[TSDB_SQL_COMPACT_VNODE] = tscBuildCompactMsg;  

  tscBuildMsg[TSDB_SQL_CONNECT] = tscBuildConnectMsg;
  tscBuildMsg[TSDB_SQL_USE_DB] = tscBuildUseDbMsg;
  tscBuildMsg[TSDB_SQL_STABLEVGROUP] = tscBuildSTableVgroupMsg;
  tscBuildMsg[TSDB_SQL_RETRIEVE_FUNC] = tscBuildRetrieveFuncMsg;

  tscBuildMsg[TSDB_SQL_HB] = tscBuildHeartBeatMsg;
  tscBuildMsg[TSDB_SQL_SHOW] = tscBuildShowMsg;
  tscBuildMsg[TSDB_SQL_RETRIEVE] = tscBuildRetrieveFromMgmtMsg;
  tscBuildMsg[TSDB_SQL_KILL_QUERY] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_STREAM] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_CONNECTION] = tscBuildKillMsg;

  tscProcessMsgRsp[TSDB_SQL_SELECT] = tscProcessQueryRsp;
  tscProcessMsgRsp[TSDB_SQL_FETCH] = tscProcessRetrieveRspFromNode;

  tscProcessMsgRsp[TSDB_SQL_DROP_DB] = tscProcessDropDbRsp;
  tscProcessMsgRsp[TSDB_SQL_DROP_TABLE] = tscProcessDropTableRsp;
  tscProcessMsgRsp[TSDB_SQL_CONNECT] = tscProcessConnectRsp;
  tscProcessMsgRsp[TSDB_SQL_USE_DB] = tscProcessUseDbRsp;
  tscProcessMsgRsp[TSDB_SQL_META] = tscProcessTableMetaRsp;
  tscProcessMsgRsp[TSDB_SQL_STABLEVGROUP] = tscProcessSTableVgroupRsp;
  tscProcessMsgRsp[TSDB_SQL_MULTI_META] = tscProcessMultiTableMetaRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_FUNC] = tscProcessRetrieveFuncRsp;

  tscProcessMsgRsp[TSDB_SQL_SHOW] = tscProcessShowRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE] = tscProcessRetrieveRspFromNode;  // rsp handled by same function.
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
*/
}





