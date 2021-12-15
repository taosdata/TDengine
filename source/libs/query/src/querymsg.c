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
#include "queryInt.h"
#include "query.h"

int32_t (*queryBuildMsg[TSDB_MSG_TYPE_MAX])(void* input, char **msg, int32_t msgSize, int32_t *msgLen) = {0};

int32_t (*queryProcessMsgRsp[TSDB_MSG_TYPE_MAX])(void* output, char *msg, int32_t msgSize) = {0};


int32_t queryBuildVgroupListReqMsg(void* input, char **msg, int32_t msgSize, int32_t *msgLen) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  *msgLen = 0;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildTableMetaReqMsg(void* input, char **msg, int32_t msgSize, int32_t *msgLen) {
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

int32_t queryBuildUseDbMsg(void* input, char **msg, int32_t msgSize, int32_t *msgLen) {
  if (NULL == input || NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SBuildUseDBInput* bInput = (SBuildUseDBInput *)input;

  int32_t estimateSize = sizeof(SUseDbMsg);
  if (NULL == *msg || msgSize < estimateSize) {
    tfree(*msg);
    *msg = calloc(1, estimateSize);
    if (NULL == *msg) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  SUseDbMsg *bMsg = (SUseDbMsg *)*msg;

  strncpy(bMsg->db, bInput->db, sizeof(bMsg->db));
  bMsg->db[sizeof(bMsg->db) - 1] = 0;

  bMsg->vgroupVersion = bInput->vgroupVersion;
  bMsg->dbGroupVersion = bInput->dbGroupVersion;

  *msgLen = (int32_t)sizeof(*bMsg);

  return TSDB_CODE_SUCCESS;  
}



int32_t queryProcessVgroupListRsp(void* output, char *msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SVgroupListRspMsg *pRsp = (SVgroupListRspMsg *)msg;

  pRsp->vgroupNum = htonl(pRsp->vgroupNum);
  pRsp->vgroupVersion = htonl(pRsp->vgroupVersion);

  if (pRsp->vgroupNum < 0) {
    qError("vgroup number[%d] in rsp is invalid", pRsp->vgroupNum);
    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
  }

  if (pRsp->vgroupVersion < 0) {
    qError("vgroup vgroupVersion[%d] in rsp is invalid", pRsp->vgroupVersion);
    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
  }

  if (msgSize != (pRsp->vgroupNum * sizeof(pRsp->vgroupInfo[0]) + sizeof(*pRsp))) {
    qError("vgroup list msg size mis-match, msgSize:%d, vgroup number:%d", msgSize, pRsp->vgroupNum);
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




int32_t queryProcessUseDBRsp(void* output, char *msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SUseDbRspMsg *pRsp = (SUseDbRspMsg *)msg;
  SUseDbOutput *pOut = (SUseDbOutput *)output;
  int32_t code = 0;

  if (msgSize <= sizeof(*pRsp)) {
    qError("invalid use db rsp msg size, msgSize:%d", msgSize);
    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
  }
  
  pRsp->vgroupVersion = htonl(pRsp->vgroupVersion);
  pRsp->dbVgroupVersion = htonl(pRsp->dbVgroupVersion);

  pRsp->vgroupNum = htonl(pRsp->vgroupNum);
  pRsp->dbVgroupNum = htonl(pRsp->dbVgroupNum);

  if (pRsp->vgroupNum < 0) {
    qError("invalid vgroup number[%d]", pRsp->vgroupNum);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pRsp->dbVgroupNum < 0) {
    qError("invalid db vgroup number[%d]", pRsp->dbVgroupNum);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  int32_t expectSize = pRsp->vgroupNum * sizeof(pRsp->vgroupInfo[0]) + pRsp->dbVgroupNum * sizeof(int32_t) + sizeof(*pRsp);
  if (msgSize != expectSize) {
    qError("vgroup list msg size mis-match, msgSize:%d, expected:%d, vgroup number:%d, db vgroup number:%d", msgSize, expectSize, pRsp->vgroupNum, pRsp->dbVgroupNum);
    return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
  }

  if (pRsp->vgroupVersion < 0) {
    qInfo("no new vgroup list info");
    if (pRsp->vgroupNum != 0) {
      qError("invalid vgroup number[%d] for no new vgroup list case", pRsp->vgroupNum);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }
  } else {
    int32_t s = sizeof(*pOut->vgroupList) + sizeof(pOut->vgroupList->vgroupInfo[0]) * pRsp->vgroupNum;
    pOut->vgroupList = calloc(1, s);
    if (NULL == pOut->vgroupList) {
      qError("calloc size[%d] failed", s);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    pOut->vgroupList->vgroupNum = pRsp->vgroupNum;
    pOut->vgroupList->vgroupVersion = pRsp->vgroupVersion;

    for (int32_t i = 0; i < pRsp->vgroupNum; ++i) {
      pRsp->vgroupInfo[i].vgId = htonl(pRsp->vgroupInfo[i].vgId);
      for (int32_t n = 0; n < pRsp->vgroupInfo[i].numOfEps; ++n) {
        pRsp->vgroupInfo[i].epAddr[n].port = htonl(pRsp->vgroupInfo[i].epAddr[n].port);
      }

      memcpy(&pOut->vgroupList->vgroupInfo[i], &pRsp->vgroupInfo[i], sizeof(pRsp->vgroupInfo[i]));
    }
  }

  int32_t *vgIdList = (int32_t *)((char *)pRsp->vgroupInfo + sizeof(pRsp->vgroupInfo[0]) * pRsp->vgroupNum);

  memcpy(pOut->db, pRsp->db, sizeof(pOut->db));
  
  if (pRsp->dbVgroupVersion < 0) {
    qInfo("no new vgroup info for db[%s]", pRsp->db);
  } else {
    pOut->dbVgroup = calloc(1, sizeof(*pOut->dbVgroup));
    if (NULL == pOut->dbVgroup) {
      qError("calloc size[%d] failed", (int32_t)sizeof(*pOut->dbVgroup));
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _exit;
    }

    pOut->dbVgroup->vgId = taosArrayInit(pRsp->dbVgroupNum, sizeof(int32_t));
    if (NULL == pOut->dbVgroup->vgId) {
      qError("taosArrayInit size[%d] failed", pRsp->dbVgroupNum);
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _exit;
    }
    
    pOut->dbVgroup->vgroupVersion = pRsp->dbVgroupVersion;
    pOut->dbVgroup->hashRange = htonl(pRsp->dbHashRange);

    for (int32_t i = 0; i < pRsp->dbVgroupNum; ++i) {
      *(vgIdList + i) = htonl(*(vgIdList + i));
      
      taosArrayPush(pOut->dbVgroup->vgId, vgIdList + i) ;
    }
  }

  return code;

_exit:
  if (pOut->dbVgroup && pOut->dbVgroup->vgId) {
    taosArrayDestroy(pOut->dbVgroup->vgId);
    pOut->dbVgroup->vgId = NULL;
  }
  
  tfree(pOut->dbVgroup);
  tfree(pOut->vgroupList);

  return code;
}


void msgInit() {
  queryBuildMsg[TSDB_MSG_TYPE_TABLE_META] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TSDB_MSG_TYPE_VGROUP_LIST] = queryBuildVgroupListReqMsg;
  queryBuildMsg[TSDB_MSG_TYPE_USE_DB] = queryBuildUseDbMsg;

  //tscProcessMsgRsp[TSDB_MSG_TYPE_TABLE_META] = tscProcessTableMetaRsp;
  queryProcessMsgRsp[TSDB_MSG_TYPE_VGROUP_LIST] = queryProcessVgroupListRsp;
  queryProcessMsgRsp[TSDB_MSG_TYPE_USE_DB] = queryProcessUseDBRsp;

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





