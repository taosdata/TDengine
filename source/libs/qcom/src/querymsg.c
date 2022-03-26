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

#include "tmsg.h"
#include "queryInt.h"
#include "query.h"
#include "trpc.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"

int32_t (*queryBuildMsg[TDMT_MAX])(void *input, char **msg, int32_t msgSize, int32_t *msgLen) = {0};
int32_t (*queryProcessMsgRsp[TDMT_MAX])(void *output, char *msg, int32_t msgSize) = {0};

int32_t queryBuildUseDbOutput(SUseDbOutput *pOut, SUseDbRsp *usedbRsp) {
  memcpy(pOut->db, usedbRsp->db, TSDB_DB_FNAME_LEN);
  pOut->dbId = usedbRsp->uid;

  pOut->dbVgroup = taosMemoryCalloc(1, sizeof(SDBVgInfo));
  if (NULL == pOut->dbVgroup) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pOut->dbVgroup->vgVersion = usedbRsp->vgVersion;
  pOut->dbVgroup->hashMethod = usedbRsp->hashMethod;

  if (usedbRsp->vgNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }
  
  pOut->dbVgroup->vgHash =
      taosHashInit(usedbRsp->vgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (NULL == pOut->dbVgroup->vgHash) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < usedbRsp->vgNum; ++i) {
    SVgroupInfo *pVgInfo = taosArrayGet(usedbRsp->pVgroupInfos, i);
    pOut->dbVgroup->numOfTable += pVgInfo->numOfTable;
    if (0 != taosHashPut(pOut->dbVgroup->vgHash, &pVgInfo->vgId, sizeof(int32_t), pVgInfo, sizeof(SVgroupInfo))) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildTableMetaReqMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen) {
  SBuildTableMetaInput *pInput = input;
  if (NULL == input || NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  STableInfoReq infoReq = {0};
  infoReq.header.vgId = pInput->vgId;
  if (pInput->dbFName) {
    tstrncpy(infoReq.dbFName, pInput->dbFName, TSDB_DB_FNAME_LEN);
  }
  tstrncpy(infoReq.tbName, pInput->tbName, TSDB_TABLE_NAME_LEN);

  int32_t bufLen = tSerializeSTableInfoReq(NULL, 0, &infoReq);
  void   *pBuf = rpcMallocCont(bufLen);
  tSerializeSTableInfoReq(pBuf, bufLen, &infoReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildUseDbMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen) {
  SBuildUseDBInput *pInput = input;
  if (NULL == pInput || NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SUseDbReq usedbReq = {0};
  strncpy(usedbReq.db, pInput->db, sizeof(usedbReq.db));
  usedbReq.db[sizeof(usedbReq.db) - 1] = 0;
  usedbReq.vgVersion = pInput->vgVersion;
  usedbReq.dbId = pInput->dbId;
  usedbReq.numOfTable = pInput->numOfTable;

  int32_t bufLen = tSerializeSUseDbReq(NULL, 0, &usedbReq);
  void   *pBuf = rpcMallocCont(bufLen);
  tSerializeSUseDbReq(pBuf, bufLen, &usedbReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildQnodeListMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SQnodeListReq qnodeListReq = {0};
  qnodeListReq.rowNum = -1;

  int32_t bufLen = tSerializeSQnodeListReq(NULL, 0, &qnodeListReq);
  void   *pBuf = rpcMallocCont(bufLen);
  tSerializeSQnodeListReq(pBuf, bufLen, &qnodeListReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}


int32_t queryProcessUseDBRsp(void *output, char *msg, int32_t msgSize) {
  SUseDbOutput *pOut = output;
  SUseDbRsp     usedbRsp = {0};
  int32_t       code = -1;

  if (NULL == output || NULL == msg || msgSize <= 0) {
    code = TSDB_CODE_TSC_INVALID_INPUT;
    goto PROCESS_USEDB_OVER;
  }

  if (tDeserializeSUseDbRsp(msg, msgSize, &usedbRsp) != 0) {
    qError("invalid use db rsp msg, msgSize:%d", msgSize);
    code = TSDB_CODE_INVALID_MSG;
    goto PROCESS_USEDB_OVER;
  }

  if (usedbRsp.vgNum < 0) {
    qError("invalid db[%s] vgroup number[%d]", usedbRsp.db, usedbRsp.vgNum);
    code = TSDB_CODE_TSC_INVALID_VALUE;
    goto PROCESS_USEDB_OVER;
  }

  code = queryBuildUseDbOutput(pOut, &usedbRsp);

PROCESS_USEDB_OVER:

  if (code != 0) {
    if (pOut) {
      if (pOut->dbVgroup) taosHashCleanup(pOut->dbVgroup->vgHash);
      taosMemoryFreeClear(pOut->dbVgroup);
    }
    qError("failed to process usedb rsp since %s", terrstr());
  }

  tFreeSUsedbRsp(&usedbRsp);
  return code;
}

static int32_t queryConvertTableMetaMsg(STableMetaRsp *pMetaMsg) {
  if (pMetaMsg->numOfTags < 0 || pMetaMsg->numOfTags > TSDB_MAX_TAGS) {
    qError("invalid numOfTags[%d] in table meta rsp msg", pMetaMsg->numOfTags);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->numOfColumns > TSDB_MAX_COLUMNS || pMetaMsg->numOfColumns <= 0) {
    qError("invalid numOfColumns[%d] in table meta rsp msg", pMetaMsg->numOfColumns);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->tableType != TSDB_SUPER_TABLE && pMetaMsg->tableType != TSDB_CHILD_TABLE &&
      pMetaMsg->tableType != TSDB_NORMAL_TABLE && pMetaMsg->tableType != TSDB_SYSTEM_TABLE) {
    qError("invalid tableType[%d] in table meta rsp msg", pMetaMsg->tableType);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->sversion < 0) {
    qError("invalid sversion[%d] in table meta rsp msg", pMetaMsg->sversion);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->tversion < 0) {
    qError("invalid tversion[%d] in table meta rsp msg", pMetaMsg->tversion);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->pSchemas[0].colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
    qError("invalid colId[%d] for the first column in table meta rsp msg", pMetaMsg->pSchemas[0].colId);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryCreateTableMetaFromMsg(STableMetaRsp *msg, bool isSuperTable, STableMeta **pMeta) {
  int32_t total = msg->numOfColumns + msg->numOfTags;
  int32_t metaSize = sizeof(STableMeta) + sizeof(SSchema) * total;

  STableMeta *pTableMeta = taosMemoryCalloc(1, metaSize);
  if (NULL == pTableMeta) {
    qError("calloc size[%d] failed", metaSize);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pTableMeta->vgId = isSuperTable ? 0 : msg->vgId;
  pTableMeta->tableType = isSuperTable ? TSDB_SUPER_TABLE : msg->tableType;
  pTableMeta->uid = isSuperTable ? msg->suid : msg->tuid;
  pTableMeta->suid = msg->suid;
  pTableMeta->sversion = msg->sversion;
  pTableMeta->tversion = msg->tversion;

  pTableMeta->tableInfo.numOfTags = msg->numOfTags;
  pTableMeta->tableInfo.precision = msg->precision;
  pTableMeta->tableInfo.numOfColumns = msg->numOfColumns;

  memcpy(pTableMeta->schema, msg->pSchemas, sizeof(SSchema) * total);

  for (int32_t i = 0; i < msg->numOfColumns; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
  }

  *pMeta = pTableMeta;
  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessTableMetaRsp(void *output, char *msg, int32_t msgSize) {
  int32_t       code = 0;
  STableMetaRsp metaRsp = {0};

  if (NULL == output || NULL == msg || msgSize <= 0) {
    code = TSDB_CODE_TSC_INVALID_INPUT;
    goto PROCESS_META_OVER;
  }

  if (tDeserializeSTableMetaRsp(msg, msgSize, &metaRsp) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto PROCESS_META_OVER;
  }

  code = queryConvertTableMetaMsg(&metaRsp);
  if (code != TSDB_CODE_SUCCESS) {
    goto PROCESS_META_OVER;
  }

  if (0 != strcmp(metaRsp.dbFName, TSDB_INFORMATION_SCHEMA_DB) && !tIsValidSchema(metaRsp.pSchemas, metaRsp.numOfColumns, metaRsp.numOfTags)) {
    code = TSDB_CODE_TSC_INVALID_VALUE;
    goto PROCESS_META_OVER;
  }

  STableMetaOutput *pOut = output;
  strcpy(pOut->dbFName, metaRsp.dbFName);
  pOut->dbId = metaRsp.dbId;

  if (metaRsp.tableType == TSDB_CHILD_TABLE) {
    SET_META_TYPE_BOTH_TABLE(pOut->metaType);

    strcpy(pOut->ctbName, metaRsp.tbName);
    strcpy(pOut->tbName, metaRsp.stbName);

    pOut->ctbMeta.vgId = metaRsp.vgId;
    pOut->ctbMeta.tableType = metaRsp.tableType;
    pOut->ctbMeta.uid = metaRsp.tuid;
    pOut->ctbMeta.suid = metaRsp.suid;

    code = queryCreateTableMetaFromMsg(&metaRsp, true, &pOut->tbMeta);
  } else {
    SET_META_TYPE_TABLE(pOut->metaType);
    strcpy(pOut->tbName, metaRsp.tbName);
    code = queryCreateTableMetaFromMsg(&metaRsp, (metaRsp.tableType == TSDB_SUPER_TABLE), &pOut->tbMeta);
  }

PROCESS_META_OVER:
  if (code != 0) {
    qError("failed to process table meta rsp since %s", tstrerror(code));
  }

  tFreeSTableMetaRsp(&metaRsp);
  return code;
}


int32_t queryProcessQnodeListRsp(void *output, char *msg, int32_t msgSize) {
  SQnodeListRsp out = {0};
  int32_t       code = -1;

  if (NULL == output || NULL == msg || msgSize <= 0) {
    code = TSDB_CODE_TSC_INVALID_INPUT;
    goto PROCESS_QLIST_OVER;
  }

  if (tDeserializeSQnodeListRsp(msg, msgSize, &out) != 0) {
    qError("invalid qnode list rsp msg, msgSize:%d", msgSize);
    code = TSDB_CODE_INVALID_MSG;
    goto PROCESS_QLIST_OVER;
  }

PROCESS_QLIST_OVER:

  if (code != 0) {
    tFreeSQnodeListRsp(&out);
    out.epSetList = NULL;
  }

  *(SArray **)output = out.epSetList;

  return code;
}


void initQueryModuleMsgHandle() {
  queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_META)] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_TABLE_META)] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_USE_DB)] = queryBuildUseDbMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_QNODE_LIST)] = queryBuildQnodeListMsg;

  queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_META)] = queryProcessTableMetaRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_TABLE_META)] = queryProcessTableMetaRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_USE_DB)] = queryProcessUseDBRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_QNODE_LIST)] = queryProcessQnodeListRsp;
}

#pragma GCC diagnostic pop
