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

int32_t (*queryBuildMsg[TDMT_MAX])(void* input, char **msg, int32_t msgSize, int32_t *msgLen) = {0};

int32_t (*queryProcessMsgRsp[TDMT_MAX])(void* output, char *msg, int32_t msgSize) = {0};

int32_t queryBuildTableMetaReqMsg(void* input, char **msg, int32_t msgSize, int32_t *msgLen) {
  if (NULL == input || NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SBuildTableMetaInput* bInput = (SBuildTableMetaInput *)input;

  int32_t estimateSize = sizeof(STableInfoReq);
  if (NULL == *msg || msgSize < estimateSize) {
    tfree(*msg);
    *msg = rpcMallocCont(estimateSize);
    if (NULL == *msg) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  STableInfoReq *bMsg = (STableInfoReq *)*msg;

  bMsg->header.vgId = htonl(bInput->vgId);

  if (bInput->dbFName) {
    tstrncpy(bMsg->dbFName, bInput->dbFName, tListLen(bMsg->dbFName));
  }

  tstrncpy(bMsg->tbName, bInput->tbName, tListLen(bMsg->tbName));

  *msgLen = (int32_t)sizeof(*bMsg);
  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildUseDbMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen) {
  if (NULL == input || NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SBuildUseDBInput *bInput = input;

  SUseDbReq usedbReq = {0};
  strncpy(usedbReq.db, bInput->db, sizeof(usedbReq.db));
  usedbReq.db[sizeof(usedbReq.db) - 1] = 0;
  usedbReq.vgVersion = bInput->vgVersion;

  int32_t bufLen = tSerializeSUseDbReq(NULL, 0, &usedbReq);
  void   *pBuf = rpcMallocCont(bufLen);
  tSerializeSUseDbReq(pBuf, bufLen, &usedbReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessUseDBRsp(void *output, char *msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SUseDbOutput *pOut = (SUseDbOutput *)output;
  int32_t       code = 0;

  SUseDbRsp usedbRsp = {0};
  if (tDeserializeSUseDbRsp(msg, msgSize, &usedbRsp) != 0) {
    qError("invalid use db rsp msg, msgSize:%d", msgSize);
    return TSDB_CODE_INVALID_MSG;
  }

  if (usedbRsp.vgNum < 0) {
    qError("invalid db[%s] vgroup number[%d]", usedbRsp.db, usedbRsp.vgNum);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  pOut->dbVgroup = calloc(1, sizeof(SDBVgInfo));
  if (NULL == pOut->dbVgroup) {
    qError("calloc %d failed", (int32_t)sizeof(SDBVgInfo));
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pOut->dbId = usedbRsp.uid;
  pOut->dbVgroup->vgVersion = usedbRsp.vgVersion;
  pOut->dbVgroup->hashMethod = usedbRsp.hashMethod;
  pOut->dbVgroup->vgHash =
      taosHashInit(usedbRsp.vgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (NULL == pOut->dbVgroup->vgHash) {
    qError("taosHashInit %d failed", usedbRsp.vgNum);
    tfree(pOut->dbVgroup);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < usedbRsp.vgNum; ++i) {
    SVgroupInfo *pVgInfo = taosArrayGet(usedbRsp.pVgroupInfos, i);

    if (0 != taosHashPut(pOut->dbVgroup->vgHash, &pVgInfo->vgId, sizeof(int32_t), pVgInfo, sizeof(SVgroupInfo))) {
      qError("taosHashPut failed");
      goto _return;
    }
  }

  memcpy(pOut->db, usedbRsp.db, TSDB_DB_FNAME_LEN);

  return code;

_return:
  tFreeSUsedbRsp(&usedbRsp);

  if (pOut) {
    taosHashCleanup(pOut->dbVgroup->vgHash);
    tfree(pOut->dbVgroup);
  }

  return code;
}

static int32_t queryConvertTableMetaMsg(STableMetaRsp* pMetaMsg) {
  pMetaMsg->dbId = be64toh(pMetaMsg->dbId);
  pMetaMsg->numOfTags = ntohl(pMetaMsg->numOfTags);
  pMetaMsg->numOfColumns = ntohl(pMetaMsg->numOfColumns);
  pMetaMsg->sversion = ntohl(pMetaMsg->sversion);
  pMetaMsg->tversion = ntohl(pMetaMsg->tversion);
  pMetaMsg->tuid = be64toh(pMetaMsg->tuid);
  pMetaMsg->suid = be64toh(pMetaMsg->suid);
  pMetaMsg->vgId = ntohl(pMetaMsg->vgId);

  if (pMetaMsg->numOfTags < 0 || pMetaMsg->numOfTags > TSDB_MAX_TAGS) {
    qError("invalid numOfTags[%d] in table meta rsp msg", pMetaMsg->numOfTags);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->numOfColumns > TSDB_MAX_COLUMNS || pMetaMsg->numOfColumns <= 0) {
    qError("invalid numOfColumns[%d] in table meta rsp msg", pMetaMsg->numOfColumns);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->tableType != TSDB_SUPER_TABLE && pMetaMsg->tableType != TSDB_CHILD_TABLE && pMetaMsg->tableType != TSDB_NORMAL_TABLE) {
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
  
  SSchema* pSchema = pMetaMsg->pSchema;

  int32_t numOfTotalCols = pMetaMsg->numOfColumns + pMetaMsg->numOfTags;
  for (int i = 0; i < numOfTotalCols; ++i) {
    pSchema->bytes = ntohl(pSchema->bytes);
    pSchema->colId = ntohl(pSchema->colId);

    pSchema++;
  }

  if (pMetaMsg->pSchema[0].colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
    qError("invalid colId[%d] for the first column in table meta rsp msg", pMetaMsg->pSchema[0].colId);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryCreateTableMetaFromMsg(STableMetaRsp* msg, bool isSuperTable, STableMeta **pMeta) {
  int32_t total = msg->numOfColumns + msg->numOfTags;
  int32_t metaSize = sizeof(STableMeta) + sizeof(SSchema) * total;
  
  STableMeta* pTableMeta = calloc(1, metaSize);
  if (NULL == pTableMeta) {
    qError("calloc size[%d] failed", metaSize);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pTableMeta->vgId = isSuperTable ? 0 : msg->vgId;
  pTableMeta->tableType = isSuperTable ? TSDB_SUPER_TABLE : msg->tableType;
  pTableMeta->uid  = isSuperTable ? msg->suid : msg->tuid;
  pTableMeta->suid = msg->suid;
  pTableMeta->sversion = msg->sversion;
  pTableMeta->tversion = msg->tversion;

  pTableMeta->tableInfo.numOfTags = msg->numOfTags;
  pTableMeta->tableInfo.precision = msg->precision;
  pTableMeta->tableInfo.numOfColumns = msg->numOfColumns;

  memcpy(pTableMeta->schema, msg->pSchema, sizeof(SSchema) * total);

  for(int32_t i = 0; i < msg->numOfColumns; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
  }

  *pMeta = pTableMeta;
  
  return TSDB_CODE_SUCCESS;
}


int32_t queryProcessTableMetaRsp(void* output, char *msg, int32_t msgSize) {
  STableMetaRsp *pMetaMsg = (STableMetaRsp *)msg;
  int32_t code = queryConvertTableMetaMsg(pMetaMsg);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  STableMetaOutput *pOut = (STableMetaOutput *)output;
  
  if (!tIsValidSchema(pMetaMsg->pSchema, pMetaMsg->numOfColumns, pMetaMsg->numOfTags)) {
    qError("validate table meta schema in rsp msg failed");
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  strcpy(pOut->dbFName, pMetaMsg->dbFName);
  
  pOut->dbId = pMetaMsg->dbId;

  if (pMetaMsg->tableType == TSDB_CHILD_TABLE) {
    SET_META_TYPE_BOTH_TABLE(pOut->metaType);

    strcpy(pOut->ctbName, pMetaMsg->tbName);
    strcpy(pOut->tbName, pMetaMsg->stbName);
    
    pOut->ctbMeta.vgId = pMetaMsg->vgId;
    pOut->ctbMeta.tableType = pMetaMsg->tableType;
    pOut->ctbMeta.uid = pMetaMsg->tuid;
    pOut->ctbMeta.suid = pMetaMsg->suid;

    code = queryCreateTableMetaFromMsg(pMetaMsg, true, &pOut->tbMeta);
  } else {
    SET_META_TYPE_TABLE(pOut->metaType);
    
    strcpy(pOut->tbName, pMetaMsg->tbName);
    
    code = queryCreateTableMetaFromMsg(pMetaMsg, (pMetaMsg->tableType == TSDB_SUPER_TABLE), &pOut->tbMeta);
  }
  
  return code;
}


void initQueryModuleMsgHandle() {
  queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_META)] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_STB_META)] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_USE_DB)] = queryBuildUseDbMsg;

  queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_META)] = queryProcessTableMetaRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_STB_META)] = queryProcessTableMetaRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_USE_DB)] = queryProcessUseDBRsp;
}

#pragma GCC diagnostic pop
