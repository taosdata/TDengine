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

#include "query.h"
#include "queryInt.h"
#include "systable.h"
#include "tmsg.h"
#include "trpc.h"

#pragma GCC diagnostic push
#ifdef COMPILER_SUPPORTS_CXX13
#pragma GCC diagnostic ignored "-Wformat-truncation"
#endif

int32_t (*queryBuildMsg[TDMT_MAX])(void *input, char **msg, int32_t msgSize, int32_t *msgLen, 
                                   void *(*mallocFp)(int64_t)) = {0};
int32_t (*queryProcessMsgRsp[TDMT_MAX])(void *output, char *msg, int32_t msgSize) = {0};

int32_t queryBuildUseDbOutput(SUseDbOutput *pOut, SUseDbRsp *usedbRsp) {
  memcpy(pOut->db, usedbRsp->db, TSDB_DB_FNAME_LEN);
  pOut->dbId = usedbRsp->uid;

  pOut->dbVgroup = taosMemoryCalloc(1, sizeof(SDBVgInfo));
  if (NULL == pOut->dbVgroup) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pOut->dbVgroup->vgVersion = usedbRsp->vgVersion;
  pOut->dbVgroup->hashMethod = usedbRsp->hashMethod;
  pOut->dbVgroup->hashPrefix = usedbRsp->hashPrefix;
  pOut->dbVgroup->hashSuffix = usedbRsp->hashSuffix;
  pOut->dbVgroup->stateTs = usedbRsp->stateTs;

  qDebug("Got %d vgroup for db %s, vgVersion:%d, stateTs:%" PRId64, usedbRsp->vgNum, usedbRsp->db, usedbRsp->vgVersion, usedbRsp->stateTs);

  if (usedbRsp->vgNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  pOut->dbVgroup->vgHash =
      taosHashInit(usedbRsp->vgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (NULL == pOut->dbVgroup->vgHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < usedbRsp->vgNum; ++i) {
    SVgroupInfo *pVgInfo = taosArrayGet(usedbRsp->pVgroupInfos, i);
    pOut->dbVgroup->numOfTable += pVgInfo->numOfTable;
    qDebug("the %dth vgroup, id %d, epNum %d, current %s port %d", i, pVgInfo->vgId, pVgInfo->epSet.numOfEps,
           pVgInfo->epSet.eps[pVgInfo->epSet.inUse].fqdn, pVgInfo->epSet.eps[pVgInfo->epSet.inUse].port);
    if (0 != taosHashPut(pOut->dbVgroup->vgHash, &pVgInfo->vgId, sizeof(int32_t), pVgInfo, sizeof(SVgroupInfo))) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildTableMetaReqMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, 
                                  void *(*mallcFp)(int64_t)) {
  SBuildTableInput *pInput = input;
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
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSTableInfoReq(pBuf, bufLen, &infoReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildUseDbMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
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
  usedbReq.stateTs = pInput->stateTs;

  int32_t bufLen = tSerializeSUseDbReq(NULL, 0, &usedbReq);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSUseDbReq(pBuf, bufLen, &usedbReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildQnodeListMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SQnodeListReq qnodeListReq = {0};
  qnodeListReq.rowNum = -1;

  int32_t bufLen = tSerializeSQnodeListReq(NULL, 0, &qnodeListReq);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSQnodeListReq(pBuf, bufLen, &qnodeListReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildDnodeListMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SDnodeListReq dnodeListReq = {0};
  dnodeListReq.rowNum = -1;

  int32_t bufLen = tSerializeSDnodeListReq(NULL, 0, &dnodeListReq);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSDnodeListReq(pBuf, bufLen, &dnodeListReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetSerVerMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SServerVerReq req = {0};

  int32_t bufLen = tSerializeSServerVerReq(NULL, 0, &req);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSServerVerReq(pBuf, bufLen, &req);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetDBCfgMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SDbCfgReq dbCfgReq = {0};
  strncpy(dbCfgReq.db, input, sizeof(dbCfgReq.db) - 1);

  int32_t bufLen = tSerializeSDbCfgReq(NULL, 0, &dbCfgReq);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSDbCfgReq(pBuf, bufLen, &dbCfgReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetIndexMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SUserIndexReq indexReq = {0};
  strncpy(indexReq.indexFName, input, sizeof(indexReq.indexFName) - 1);

  int32_t bufLen = tSerializeSUserIndexReq(NULL, 0, &indexReq);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSUserIndexReq(pBuf, bufLen, &indexReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildRetrieveFuncMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, 
                                  void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SRetrieveFuncReq funcReq = {0};
  funcReq.numOfFuncs = 1;
  funcReq.ignoreCodeComment = true;
  funcReq.pFuncNames = taosArrayInit(1, strlen(input) + 1);
  taosArrayPush(funcReq.pFuncNames, input);

  int32_t bufLen = tSerializeSRetrieveFuncReq(NULL, 0, &funcReq);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSRetrieveFuncReq(pBuf, bufLen, &funcReq);

  taosArrayDestroy(funcReq.pFuncNames);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetUserAuthMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SGetUserAuthReq req = {0};
  strncpy(req.user, input, sizeof(req.user) - 1);

  int32_t bufLen = tSerializeSGetUserAuthReq(NULL, 0, &req);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSGetUserAuthReq(pBuf, bufLen, &req);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetTbIndexMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  STableIndexReq indexReq = {0};
  strncpy(indexReq.tbFName, input, sizeof(indexReq.tbFName) - 1);

  int32_t bufLen = tSerializeSTableIndexReq(NULL, 0, &indexReq);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSTableIndexReq(pBuf, bufLen, &indexReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetTbCfgMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SBuildTableInput *pInput = input;
  STableCfgReq      cfgReq = {0};
  cfgReq.header.vgId = pInput->vgId;
  strncpy(cfgReq.dbFName, pInput->dbFName, sizeof(cfgReq.dbFName) - 1);
  strncpy(cfgReq.tbName, pInput->tbName, sizeof(cfgReq.tbName) - 1);

  int32_t bufLen = tSerializeSTableCfgReq(NULL, 0, &cfgReq);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSTableCfgReq(pBuf, bufLen, &cfgReq);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetViewMetaMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  if (NULL == msg || NULL == msgLen) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SViewMetaReq  req = {0};
  strncpy(req.fullname, input, sizeof(req.fullname) - 1);

  int32_t bufLen = tSerializeSViewMetaReq(NULL, 0, &req);
  void   *pBuf = (*mallcFp)(bufLen);
  tSerializeSViewMetaReq(pBuf, bufLen, &req);

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

  qTrace("db:%s, usedbRsp received, numOfVgroups:%d", usedbRsp.db, usedbRsp.vgNum);
  for (int32_t i = 0; i < usedbRsp.vgNum; ++i) {
    SVgroupInfo *pInfo = taosArrayGet(usedbRsp.pVgroupInfos, i);
    qTrace("vgId:%d, numOfEps:%d inUse:%d ", pInfo->vgId, pInfo->epSet.numOfEps, pInfo->epSet.inUse);
    for (int32_t j = 0; j < pInfo->epSet.numOfEps; ++j) {
      qTrace("vgId:%d, index:%d epset:%s:%u", pInfo->vgId, j, pInfo->epSet.eps[j].fqdn, pInfo->epSet.eps[j].port);
    }
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
    qError("invalid colId[%" PRIi16 "] for the first column in table meta rsp msg", pMetaMsg->pSchemas[0].colId);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryCreateCTableMetaFromMsg(STableMetaRsp *msg, SCTableMeta *pMeta) {
  pMeta->vgId = msg->vgId;
  pMeta->tableType = msg->tableType;
  pMeta->uid = msg->tuid;
  pMeta->suid = msg->suid;

  qDebug("ctable %s uid %" PRIx64 " meta returned, type %d vgId:%d db %s suid %" PRIx64, msg->tbName, pMeta->uid,
         pMeta->tableType, pMeta->vgId, msg->dbFName, pMeta->suid);

  return TSDB_CODE_SUCCESS;
}

int32_t queryCreateTableMetaFromMsg(STableMetaRsp *msg, bool isStb, STableMeta **pMeta) {
  int32_t total = msg->numOfColumns + msg->numOfTags;
  int32_t metaSize = sizeof(STableMeta) + sizeof(SSchema) * total;

  STableMeta *pTableMeta = taosMemoryCalloc(1, metaSize);
  if (NULL == pTableMeta) {
    qError("calloc size[%d] failed", metaSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTableMeta->vgId = isStb ? 0 : msg->vgId;
  pTableMeta->tableType = isStb ? TSDB_SUPER_TABLE : msg->tableType;
  pTableMeta->uid = isStb ? msg->suid : msg->tuid;
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

  qDebug("table %s uid %" PRIx64 " meta returned, type %d vgId:%d db %s stb %s suid %" PRIx64 " sver %d tver %d"
         " tagNum %d colNum %d precision %d rowSize %d",
         msg->tbName, pTableMeta->uid, pTableMeta->tableType, pTableMeta->vgId, msg->dbFName, msg->stbName,
         pTableMeta->suid, pTableMeta->sversion, pTableMeta->tversion, pTableMeta->tableInfo.numOfTags,
         pTableMeta->tableInfo.numOfColumns, pTableMeta->tableInfo.precision, pTableMeta->tableInfo.rowSize);

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

  if (0 != strcmp(metaRsp.dbFName, TSDB_INFORMATION_SCHEMA_DB) &&
      !tIsValidSchema(metaRsp.pSchemas, metaRsp.numOfColumns, metaRsp.numOfTags)) {
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
  int32_t       code = 0;

  if (NULL == output || NULL == msg || msgSize <= 0) {
    code = TSDB_CODE_TSC_INVALID_INPUT;
    return code;
  }

  out.qnodeList = (SArray *)output;
  if (tDeserializeSQnodeListRsp(msg, msgSize, &out) != 0) {
    qError("invalid qnode list rsp msg, msgSize:%d", msgSize);
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  return code;
}

int32_t queryProcessDnodeListRsp(void *output, char *msg, int32_t msgSize) {
  SDnodeListRsp out = {0};
  int32_t       code = 0;

  if (NULL == output || NULL == msg || msgSize <= 0) {
    code = TSDB_CODE_TSC_INVALID_INPUT;
    return code;
  }

  if (tDeserializeSDnodeListRsp(msg, msgSize, &out) != 0) {
    qError("invalid dnode list rsp msg, msgSize:%d", msgSize);
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  *(SArray **)output = out.dnodeList;

  return code;
}

int32_t queryProcessGetSerVerRsp(void *output, char *msg, int32_t msgSize) {
  SServerVerRsp out = {0};
  int32_t       code = 0;

  if (NULL == output || NULL == msg || msgSize <= 0) {
    code = TSDB_CODE_TSC_INVALID_INPUT;
    return code;
  }

  if (tDeserializeSServerVerRsp(msg, msgSize, &out) != 0) {
    qError("invalid svr ver rsp msg, msgSize:%d", msgSize);
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  *(char **)output = taosStrdup(out.ver);

  return code;
}

int32_t queryProcessGetDbCfgRsp(void *output, char *msg, int32_t msgSize) {
  SDbCfgRsp out = {0};

  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  if (tDeserializeSDbCfgRsp(msg, msgSize, &out) != 0) {
    qError("tDeserializeSDbCfgRsp failed, msgSize:%d,dbCfgRsp:%lu", msgSize, sizeof(out));
    return TSDB_CODE_INVALID_MSG;
  }

  memcpy(output, &out, sizeof(out));

  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessGetIndexRsp(void *output, char *msg, int32_t msgSize) {
  SUserIndexRsp out = {0};

  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  if (tDeserializeSUserIndexRsp(msg, msgSize, &out) != 0) {
    qError("tDeserializeSUserIndexRsp failed, msgSize:%d", msgSize);
    return TSDB_CODE_INVALID_MSG;
  }

  memcpy(output, &out, sizeof(out));

  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessRetrieveFuncRsp(void *output, char *msg, int32_t msgSize) {
  SRetrieveFuncRsp out = {0};

  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  if (tDeserializeSRetrieveFuncRsp(msg, msgSize, &out) != 0) {
    qError("tDeserializeSRetrieveFuncRsp failed, msgSize:%d", msgSize);
    return TSDB_CODE_INVALID_MSG;
  }

  if (1 != out.numOfFuncs) {
    qError("invalid func num returned, numOfFuncs:%d", out.numOfFuncs);
    return TSDB_CODE_INVALID_MSG;
  }

  SFuncInfo *funcInfo = taosArrayGet(out.pFuncInfos, 0);

  memcpy(output, funcInfo, sizeof(*funcInfo));
  taosArrayDestroy(out.pFuncInfos);
  taosArrayDestroy(out.pFuncExtraInfos);
  
  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessGetUserAuthRsp(void *output, char *msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  if (tDeserializeSGetUserAuthRsp(msg, msgSize, (SGetUserAuthRsp *)output) != 0) {
    qError("tDeserializeSGetUserAuthRsp failed, msgSize:%d", msgSize);
    return TSDB_CODE_INVALID_MSG;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessGetTbIndexRsp(void *output, char *msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  STableIndexRsp *out = (STableIndexRsp *)output;
  if (tDeserializeSTableIndexRsp(msg, msgSize, out) != 0) {
    qError("tDeserializeSTableIndexRsp failed, msgSize:%d", msgSize);
    return TSDB_CODE_INVALID_MSG;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessGetTbCfgRsp(void *output, char *msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  STableCfgRsp *out = taosMemoryCalloc(1, sizeof(STableCfgRsp));
  if (tDeserializeSTableCfgRsp(msg, msgSize, out) != 0) {
    qError("tDeserializeSTableCfgRsp failed, msgSize:%d", msgSize);
    tFreeSTableCfgRsp(out);
    taosMemoryFree(out);
    return TSDB_CODE_INVALID_MSG;
  }

  *(STableCfgRsp **)output = out;

  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessGetViewMetaRsp(void *output, char *msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SViewMetaRsp *out = taosMemoryCalloc(1, sizeof(SViewMetaRsp));
  if (tDeserializeSViewMetaRsp(msg, msgSize, out) != 0) {
    qError("tDeserializeSViewMetaRsp failed, msgSize:%d", msgSize);
    tFreeSViewMetaRsp(out);
    taosMemoryFree(out);
    return TSDB_CODE_INVALID_MSG;
  }

  qDebugL("view meta recved, dbFName:%s, view:%s, querySQL:%s", out->dbFName, out->name, out->querySql);

  *(SViewMetaRsp **)output = out;

  return TSDB_CODE_SUCCESS;
}


void initQueryModuleMsgHandle() {
  queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_META)] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_TABLE_META)] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_USE_DB)] = queryBuildUseDbMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_QNODE_LIST)] = queryBuildQnodeListMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_DNODE_LIST)] = queryBuildDnodeListMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_GET_DB_CFG)] = queryBuildGetDBCfgMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_GET_INDEX)] = queryBuildGetIndexMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_RETRIEVE_FUNC)] = queryBuildRetrieveFuncMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_GET_USER_AUTH)] = queryBuildGetUserAuthMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_GET_TABLE_INDEX)] = queryBuildGetTbIndexMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_CFG)] = queryBuildGetTbCfgMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_TABLE_CFG)] = queryBuildGetTbCfgMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_SERVER_VERSION)] = queryBuildGetSerVerMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_VIEW_META)] = queryBuildGetViewMetaMsg;

  queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_META)] = queryProcessTableMetaRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_TABLE_META)] = queryProcessTableMetaRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_USE_DB)] = queryProcessUseDBRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_QNODE_LIST)] = queryProcessQnodeListRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_DNODE_LIST)] = queryProcessDnodeListRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_GET_DB_CFG)] = queryProcessGetDbCfgRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_GET_INDEX)] = queryProcessGetIndexRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_RETRIEVE_FUNC)] = queryProcessRetrieveFuncRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_GET_USER_AUTH)] = queryProcessGetUserAuthRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_GET_TABLE_INDEX)] = queryProcessGetTbIndexRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_CFG)] = queryProcessGetTbCfgRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_TABLE_CFG)] = queryProcessGetTbCfgRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_SERVER_VERSION)] = queryProcessGetSerVerRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_VIEW_META)] = queryProcessGetViewMetaRsp;
}

#pragma GCC diagnostic pop
