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
#include "streamMsg.h"
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
  QUERY_PARAM_CHECK(pOut);
  QUERY_PARAM_CHECK(usedbRsp);
  memcpy(pOut->db, usedbRsp->db, TSDB_DB_FNAME_LEN);
  pOut->dbId = usedbRsp->uid;

  pOut->dbVgroup = taosMemoryCalloc(1, sizeof(SDBVgInfo));
  if (NULL == pOut->dbVgroup) {
    return terrno;
  }

  pOut->dbVgroup->vgVersion = usedbRsp->vgVersion;
  pOut->dbVgroup->hashMethod = usedbRsp->hashMethod;
  pOut->dbVgroup->hashPrefix = usedbRsp->hashPrefix;
  pOut->dbVgroup->hashSuffix = usedbRsp->hashSuffix;
  pOut->dbVgroup->stateTs = usedbRsp->stateTs;
  pOut->dbVgroup->flags = usedbRsp->flags;

  qDebug("db:%s, get %d vgroup, vgVersion:%d, stateTs:%" PRId64, usedbRsp->db, usedbRsp->vgNum, usedbRsp->vgVersion,
         usedbRsp->stateTs);

  if (usedbRsp->vgNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  pOut->dbVgroup->vgHash =
      taosHashInit(usedbRsp->vgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (NULL == pOut->dbVgroup->vgHash) {
    return terrno;
  }

  for (int32_t i = 0; i < usedbRsp->vgNum; ++i) {
    SVgroupInfo *pVgInfo = taosArrayGet(usedbRsp->pVgroupInfos, i);
    pOut->dbVgroup->numOfTable += pVgInfo->numOfTable;
    qDebug("db:%s, vgId:%d, epNum:%d, current ep:%s:%u", usedbRsp->db, pVgInfo->vgId, pVgInfo->epSet.numOfEps,
           pVgInfo->epSet.eps[pVgInfo->epSet.inUse].fqdn, pVgInfo->epSet.eps[pVgInfo->epSet.inUse].port);
    if (0 != taosHashPut(pOut->dbVgroup->vgHash, &pVgInfo->vgId, sizeof(int32_t), pVgInfo, sizeof(SVgroupInfo))) {
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildTableMetaReqMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen,
                                  void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);
  SBuildTableInput *pInput = input;

  STableInfoReq infoReq = {0};
  infoReq.option = pInput->option;
  infoReq.header.vgId = pInput->vgId;
  infoReq.autoCreateCtb = pInput->autoCreateCtb;

  if (pInput->dbFName) {
    tstrncpy(infoReq.dbFName, pInput->dbFName, TSDB_DB_FNAME_LEN);
  }
  tstrncpy(infoReq.tbName, pInput->tbName, TSDB_TABLE_NAME_LEN);

  int32_t bufLen = tSerializeSTableInfoReq(NULL, 0, &infoReq);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  int32_t ret = tSerializeSTableInfoReq(pBuf, bufLen, &infoReq);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildUseDbMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);
  SBuildUseDBInput *pInput = input;

  SUseDbReq usedbReq = {0};
  tstrncpy(usedbReq.db, pInput->db, TSDB_DB_FNAME_LEN);
  usedbReq.db[sizeof(usedbReq.db) - 1] = 0;
  usedbReq.vgVersion = pInput->vgVersion;
  usedbReq.dbId = pInput->dbId;
  usedbReq.numOfTable = pInput->numOfTable;
  usedbReq.stateTs = pInput->stateTs;

  int32_t bufLen = tSerializeSUseDbReq(NULL, 0, &usedbReq);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  int32_t ret = tSerializeSUseDbReq(pBuf, bufLen, &usedbReq);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildQnodeListMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SQnodeListReq qnodeListReq = {0};
  qnodeListReq.rowNum = -1;

  int32_t bufLen = tSerializeSQnodeListReq(NULL, 0, &qnodeListReq);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }

  int32_t ret = tSerializeSQnodeListReq(pBuf, bufLen, &qnodeListReq);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildDnodeListMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SDnodeListReq dnodeListReq = {0};
  dnodeListReq.rowNum = -1;

  int32_t bufLen = tSerializeSDnodeListReq(NULL, 0, &dnodeListReq);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  int32_t ret = tSerializeSDnodeListReq(pBuf, bufLen, &dnodeListReq);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetSerVerMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SServerVerReq req = {0};

  int32_t bufLen = tSerializeSServerVerReq(NULL, 0, &req);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  int32_t ret = tSerializeSServerVerReq(pBuf, bufLen, &req);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetDBCfgMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SDbCfgReq dbCfgReq = {0};
  tstrncpy(dbCfgReq.db, input, TSDB_DB_FNAME_LEN);

  int32_t bufLen = tSerializeSDbCfgReq(NULL, 0, &dbCfgReq);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  int32_t ret = tSerializeSDbCfgReq(pBuf, bufLen, &dbCfgReq);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildRetrieveFuncMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen,
                                  void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SRetrieveFuncReq funcReq = {0};
  funcReq.numOfFuncs = 1;
  funcReq.ignoreCodeComment = true;
  funcReq.pFuncNames = taosArrayInit(1, strlen(input) + 1);
  if (NULL == funcReq.pFuncNames) {
    return terrno;
  }
  if (taosArrayPush(funcReq.pFuncNames, input) == NULL) {
    taosArrayDestroy(funcReq.pFuncNames);
    return terrno;
  }

  int32_t bufLen = tSerializeSRetrieveFuncReq(NULL, 0, &funcReq);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    taosArrayDestroy(funcReq.pFuncNames);
    return terrno;
  }
  int32_t ret = tSerializeSRetrieveFuncReq(pBuf, bufLen, &funcReq);
  if (ret < 0) {
    taosArrayDestroy(funcReq.pFuncNames);
    return ret;
  }

  taosArrayDestroy(funcReq.pFuncNames);

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetUserAuthMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SGetUserAuthReq req = {0};
  tstrncpy(req.user, input, TSDB_USER_LEN);

  int32_t bufLen = tSerializeSGetUserAuthReq(NULL, 0, &req);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  int32_t ret = tSerializeSGetUserAuthReq(pBuf, bufLen, &req);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetTbCfgMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SBuildTableInput *pInput = input;
  STableCfgReq      cfgReq = {0};
  cfgReq.header.vgId = pInput->vgId;
  tstrncpy(cfgReq.dbFName, pInput->dbFName, TSDB_DB_FNAME_LEN);
  tstrncpy(cfgReq.tbName, pInput->tbName, TSDB_TABLE_NAME_LEN);

  int32_t bufLen = tSerializeSTableCfgReq(NULL, 0, &cfgReq);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  int32_t ret = tSerializeSTableCfgReq(pBuf, bufLen, &cfgReq);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetViewMetaMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen, void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SViewMetaReq req = {0};
  tstrncpy(req.fullname, input, TSDB_VIEW_FNAME_LEN);

  int32_t bufLen = tSerializeSViewMetaReq(NULL, 0, &req);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  int32_t ret = tSerializeSViewMetaReq(pBuf, bufLen, &req);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetTableTSMAMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen,
                                  void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  STableTSMAInfoReq req = {0};
  tstrncpy(req.name, input, TSDB_TABLE_FNAME_LEN);

  int32_t bufLen = tSerializeTableTSMAInfoReq(NULL, 0, &req);
  void *  pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  int32_t ret = tSerializeTableTSMAInfoReq(pBuf, bufLen, &req);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;
  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetTSMAMsg(void *input, char **msg, int32_t msgSize, int32_t *msgLen,
                                  void *(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  STableTSMAInfoReq req = {0};
  req.fetchingWithTsmaName = true;
  tstrncpy(req.name, input, TSDB_TABLE_FNAME_LEN);

  int32_t bufLen = tSerializeTableTSMAInfoReq(NULL, 0, &req);
  void *  pBuf = (*mallcFp)(bufLen);
  if(pBuf == NULL)
  {
    return terrno;
  }
  int32_t ret = tSerializeTableTSMAInfoReq(pBuf, bufLen, &req);
  if(ret < 0) return ret;

  *msg = pBuf;
  *msgLen = bufLen;
  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildGetStreamProgressMsg(void* input, char** msg, int32_t msgSize, int32_t *msgLen, void*(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  int32_t len = tSerializeStreamProgressReq(NULL, 0, input);
  void* pBuf = (*mallcFp)(len);
  if (NULL == pBuf) {
    return terrno;
  }

  int32_t ret = tSerializeStreamProgressReq(pBuf, len, input);
  if (ret < 0) return ret;

  *msg = pBuf;
  *msgLen = len;
  return TSDB_CODE_SUCCESS;
}


int32_t queryBuildVSubTablesMsg(void* input, char** msg, int32_t msgSize, int32_t *msgLen, void*(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SVSubTablesReq req = {0};
  req.suid = *(int64_t*)input;

  int32_t bufLen = tSerializeSVSubTablesReq(NULL, 0, &req);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  if(tSerializeSVSubTablesReq(pBuf, bufLen, &req) < 0)   {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  *msg = pBuf;
  *msgLen = bufLen;

  return TSDB_CODE_SUCCESS;
}

int32_t queryBuildVStbRefDBsMsg(void* input, char** msg, int32_t msgSize, int32_t *msgLen, void*(*mallcFp)(int64_t)) {
  QUERY_PARAM_CHECK(input);
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(msgLen);

  SVStbRefDbsReq req = {0};
  req.suid = *(int64_t*)input;

  int32_t bufLen = tSerializeSVStbRefDbsReq(NULL, 0, &req);
  void   *pBuf = (*mallcFp)(bufLen);
  if (NULL == pBuf) {
    return terrno;
  }
  if(tSerializeSVStbRefDbsReq(pBuf, bufLen, &req) < 0)   {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

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
    qError("invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
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
  QUERY_PARAM_CHECK(pMetaMsg);
  if (pMetaMsg->numOfTags < 0 || pMetaMsg->numOfTags > TSDB_MAX_TAGS) {
    qError("invalid numOfTags[%d] in table meta rsp msg", pMetaMsg->numOfTags);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->numOfColumns > TSDB_MAX_COLUMNS || pMetaMsg->numOfColumns <= 0) {
    qError("invalid numOfColumns[%d] in table meta rsp msg", pMetaMsg->numOfColumns);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->tableType != TSDB_SUPER_TABLE && pMetaMsg->tableType != TSDB_CHILD_TABLE &&
      pMetaMsg->tableType != TSDB_NORMAL_TABLE && pMetaMsg->tableType != TSDB_SYSTEM_TABLE &&
      pMetaMsg->tableType != TSDB_VIRTUAL_NORMAL_TABLE && pMetaMsg->tableType != TSDB_VIRTUAL_CHILD_TABLE) {
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

  if (pMetaMsg->rversion < 0) {
    qError("invalid rversion[%d] in table meta rsp msg", pMetaMsg->rversion);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->pSchemas[0].colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
    qError("invalid colId[%" PRIi16 "] for the first column in table meta rsp msg", pMetaMsg->pSchemas[0].colId);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryCreateCTableMetaFromMsg(STableMetaRsp *msg, SCTableMeta *pMeta) {
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(pMeta);
  pMeta->vgId = msg->vgId;
  pMeta->tableType = msg->tableType;
  pMeta->uid = msg->tuid;
  pMeta->suid = msg->suid;

  qDebug("ctb:%s, uid:0x%" PRIx64 " meta returned, type:%d vgId:%d db:%s suid:%" PRIx64, msg->tbName, pMeta->uid,
         pMeta->tableType, pMeta->vgId, msg->dbFName, pMeta->suid);

  return TSDB_CODE_SUCCESS;
}

int32_t queryCreateVCTableMetaFromMsg(STableMetaRsp *msg, SVCTableMeta **pMeta) {
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(pMeta);
  QUERY_PARAM_CHECK(msg->pColRefs);

  int32_t pColRefSize = sizeof(SColRef) * msg->numOfColRefs;

  SVCTableMeta *pTableMeta = taosMemoryCalloc(1, sizeof(SVCTableMeta) + pColRefSize);
  if (NULL == pTableMeta) {
    qError("calloc size[%d] failed", (int32_t)sizeof(SVCTableMeta) + pColRefSize);
    return terrno;
  }

  pTableMeta->vgId = msg->vgId;
  pTableMeta->tableType = msg->tableType;
  pTableMeta->uid = msg->tuid;
  pTableMeta->suid = msg->suid;
  pTableMeta->numOfColRefs = msg->numOfColRefs;
  pTableMeta->rversion = msg->rversion;

  pTableMeta->colRef = (SColRef *)((char *)pTableMeta + sizeof(SVCTableMeta));
  memcpy(pTableMeta->colRef, msg->pColRefs, pColRefSize);

  qDebug("ctable %s uid %" PRIx64 " meta returned, type %d vgId:%d db %s suid %" PRIx64, msg->tbName, (pTableMeta)->uid,
         (pTableMeta)->tableType, (pTableMeta)->vgId, msg->dbFName, (pTableMeta)->suid);

  *pMeta = pTableMeta;
  return TSDB_CODE_SUCCESS;
}

int32_t queryCreateTableMetaFromMsg(STableMetaRsp *msg, bool isStb, STableMeta **pMeta) {
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(pMeta);
  int32_t total = msg->numOfColumns + msg->numOfTags;
  int32_t metaSize = sizeof(STableMeta) + sizeof(SSchema) * total;
  int32_t schemaExtSize = (withExtSchema(msg->tableType) && msg->pSchemaExt) ? sizeof(SSchemaExt) * msg->numOfColumns : 0;
  int32_t pColRefSize = (hasRefCol(msg->tableType) && msg->pColRefs && !isStb) ? sizeof(SColRef) * msg->numOfColRefs : 0;

  STableMeta *pTableMeta = taosMemoryCalloc(1, metaSize + schemaExtSize + pColRefSize);
  if (NULL == pTableMeta) {
    qError("calloc size[%d] failed", metaSize);
    return terrno;
  }
  SSchemaExt *pSchemaExt = (SSchemaExt *)((char *)pTableMeta + metaSize);
  SColRef    *pColRef = (SColRef *)((char *)pTableMeta + metaSize + schemaExtSize);

  pTableMeta->vgId = isStb ? 0 : msg->vgId;
  pTableMeta->tableType = isStb ? TSDB_SUPER_TABLE : msg->tableType;
  pTableMeta->uid = isStb ? msg->suid : msg->tuid;
  pTableMeta->suid = msg->suid;
  pTableMeta->sversion = msg->sversion;
  pTableMeta->tversion = msg->tversion;
  pTableMeta->rversion = msg->rversion;
  if (msg->virtualStb) {
    pTableMeta->virtualStb = 1;
    pTableMeta->numOfColRefs = 0;
  } else {
    if (msg->tableType == TSDB_VIRTUAL_CHILD_TABLE && isStb) {
      pTableMeta->virtualStb = 1;
      pTableMeta->numOfColRefs = 0;
    } else {
      pTableMeta->virtualStb = 0;
      pTableMeta->numOfColRefs = msg->numOfColRefs;
    }
  }

  pTableMeta->tableInfo.numOfTags = msg->numOfTags;
  pTableMeta->tableInfo.precision = msg->precision;
  pTableMeta->tableInfo.numOfColumns = msg->numOfColumns;

  memcpy(pTableMeta->schema, msg->pSchemas, sizeof(SSchema) * total);
  if (withExtSchema(msg->tableType) && msg->pSchemaExt) {
    pTableMeta->schemaExt = pSchemaExt;
    memcpy(pSchemaExt, msg->pSchemaExt, schemaExtSize);
  } else {
    pTableMeta->schemaExt = NULL;
  }

  if (hasRefCol(msg->tableType) && msg->pColRefs && !isStb) {
    pTableMeta->colRef = (SColRef *)((char *)pTableMeta + metaSize + schemaExtSize);
    memcpy(pTableMeta->colRef, msg->pColRefs, pColRefSize);
  } else {
    pTableMeta->colRef = NULL;
  }

  bool hasPK = (msg->numOfColumns > 1) && (pTableMeta->schema[1].flags & COL_IS_KEY);
  for (int32_t i = 0; i < msg->numOfColumns; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
    if (hasPK && (i > 0)) {
      if ((pTableMeta->schema[i].flags & COL_IS_KEY)) {
        ++pTableMeta->tableInfo.numOfPKs;
      } else {
        hasPK = false;
      }
    }
  }

  qDebug("tb:%s, uid:%" PRIx64 " meta returned, type:%d vgId:%d db:%s stb:%s suid:%" PRIx64
         " sver:%d tver:%d"
         " tagNum:%d colNum:%d precision:%d rowSize:%d",
         msg->tbName, pTableMeta->uid, pTableMeta->tableType, pTableMeta->vgId, msg->dbFName, msg->stbName,
         pTableMeta->suid, pTableMeta->sversion, pTableMeta->tversion, pTableMeta->tableInfo.numOfTags,
         pTableMeta->tableInfo.numOfColumns, pTableMeta->tableInfo.precision, pTableMeta->tableInfo.rowSize);

  *pMeta = pTableMeta;
  return TSDB_CODE_SUCCESS;
}

int32_t queryCreateTableMetaExFromMsg(STableMetaRsp *msg, bool isStb, STableMeta **pMeta) {
  QUERY_PARAM_CHECK(msg);
  QUERY_PARAM_CHECK(pMeta);
  int32_t total = msg->numOfColumns + msg->numOfTags;
  int32_t metaSize = sizeof(STableMeta) + sizeof(SSchema) * total;
  int32_t schemaExtSize = (withExtSchema(msg->tableType) && msg->pSchemaExt) ? sizeof(SSchemaExt) * msg->numOfColumns : 0;
  int32_t pColRefSize = (hasRefCol(msg->tableType) && msg->pColRefs) ? sizeof(SColRef) * msg->numOfColRefs : 0;
  int32_t tbNameSize = strlen(msg->tbName) + 1;

  STableMeta *pTableMeta = taosMemoryCalloc(1, metaSize + schemaExtSize + pColRefSize + tbNameSize);
  if (NULL == pTableMeta) {
    qError("calloc size[%d] failed", metaSize);
    return terrno;
  }
  SSchemaExt *pSchemaExt = (SSchemaExt *)((char *)pTableMeta + metaSize);
  SColRef    *pColRef = (SColRef *)((char *)pTableMeta + metaSize + schemaExtSize);

  pTableMeta->vgId = isStb ? 0 : msg->vgId;
  pTableMeta->tableType = isStb ? TSDB_SUPER_TABLE : msg->tableType;
  pTableMeta->uid = isStb ? msg->suid : msg->tuid;
  pTableMeta->suid = msg->suid;
  pTableMeta->sversion = msg->sversion;
  pTableMeta->tversion = msg->tversion;
  pTableMeta->rversion = msg->rversion;
  pTableMeta->virtualStb = msg->virtualStb;
  pTableMeta->numOfColRefs = msg->numOfColRefs;

  pTableMeta->tableInfo.numOfTags = msg->numOfTags;
  pTableMeta->tableInfo.precision = msg->precision;
  pTableMeta->tableInfo.numOfColumns = msg->numOfColumns;

  TAOS_MEMCPY(pTableMeta->schema, msg->pSchemas, sizeof(SSchema) * total);
  if (withExtSchema(msg->tableType) && msg->pSchemaExt) {
    pTableMeta->schemaExt = pSchemaExt;
    TAOS_MEMCPY(pSchemaExt, msg->pSchemaExt, schemaExtSize);
  } else {
    pTableMeta->schemaExt = NULL;
  }

  if (hasRefCol(msg->tableType) && msg->pColRefs) {
    pTableMeta->colRef = (SColRef *)((char *)pTableMeta + metaSize + schemaExtSize);
    memcpy(pTableMeta->colRef, msg->pColRefs, pColRefSize);
  } else {
    pTableMeta->colRef = NULL;
  }

  bool hasPK = (msg->numOfColumns > 1) && (pTableMeta->schema[1].flags & COL_IS_KEY);
  for (int32_t i = 0; i < msg->numOfColumns; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
    if (hasPK && (i > 0)) {
      if ((pTableMeta->schema[i].flags & COL_IS_KEY)) {
        ++pTableMeta->tableInfo.numOfPKs;
      } else {
        hasPK = false;
      }
    }
  }

  char *pTbName = (char *)pTableMeta + metaSize + schemaExtSize + pColRefSize;
  tstrncpy(pTbName, msg->tbName, tbNameSize);

  qDebug("tb:%s, uid:%" PRIx64 " meta returned, type:%d vgId:%d db:%s stb:%s suid:%" PRIx64
         " sver:%d tver:%d"
         " tagNum:%d colNum:%d precision:%d rowSize:%d",
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
    qError("queryProcessTableMetaRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
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

  if (!IS_SYS_DBNAME(metaRsp.dbFName) &&
      !tIsValidSchema(metaRsp.pSchemas, metaRsp.numOfColumns, metaRsp.numOfTags)) {
    code = TSDB_CODE_TSC_INVALID_VALUE;
    goto PROCESS_META_OVER;
  }

  STableMetaOutput *pOut = output;
  tstrncpy(pOut->dbFName, metaRsp.dbFName, TSDB_DB_FNAME_LEN);
  pOut->dbId = metaRsp.dbId;

  if (metaRsp.tableType == TSDB_CHILD_TABLE) {
    SET_META_TYPE_BOTH_TABLE(pOut->metaType);

    tstrncpy(pOut->ctbName, metaRsp.tbName, TSDB_TABLE_NAME_LEN);
    tstrncpy(pOut->tbName, metaRsp.stbName, TSDB_TABLE_NAME_LEN);

    pOut->ctbMeta.vgId = metaRsp.vgId;
    pOut->ctbMeta.tableType = metaRsp.tableType;
    pOut->ctbMeta.uid = metaRsp.tuid;
    pOut->ctbMeta.suid = metaRsp.suid;

    code = queryCreateTableMetaFromMsg(&metaRsp, true, &pOut->tbMeta);
  } else if (metaRsp.tableType == TSDB_VIRTUAL_CHILD_TABLE) {
    SET_META_TYPE_BOTH_VTABLE(pOut->metaType);

    tstrncpy(pOut->ctbName, metaRsp.tbName, TSDB_TABLE_NAME_LEN);
    tstrncpy(pOut->tbName, metaRsp.stbName, TSDB_TABLE_NAME_LEN);

    code = queryCreateVCTableMetaFromMsg(&metaRsp, &pOut->vctbMeta);
    if (TSDB_CODE_SUCCESS != code) {
      goto PROCESS_META_OVER;
    }
    code = queryCreateTableMetaFromMsg(&metaRsp, true, &pOut->tbMeta);
  } else {
    SET_META_TYPE_TABLE(pOut->metaType);
    tstrncpy(pOut->tbName, metaRsp.tbName, TSDB_TABLE_NAME_LEN);
    code = queryCreateTableMetaFromMsg(&metaRsp, (metaRsp.tableType == TSDB_SUPER_TABLE), &pOut->tbMeta);
  }

PROCESS_META_OVER:
  if (code != 0) {
    qError("failed to process table meta rsp since %s", tstrerror(code));
  }

  tFreeSTableMetaRsp(&metaRsp);
  return code;
}

static int32_t queryProcessTableNameRsp(void *output, char *msg, int32_t msgSize) {
  int32_t       code = 0;
  STableMetaRsp metaRsp = {0};

  if (NULL == output || NULL == msg || msgSize <= 0) {
    qError("queryProcessTableNameRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    code = TSDB_CODE_TSC_INVALID_INPUT;
    goto PROCESS_NAME_OVER;
  }

  if (tDeserializeSTableMetaRsp(msg, msgSize, &metaRsp) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto PROCESS_NAME_OVER;
  }

  code = queryConvertTableMetaMsg(&metaRsp);
  if (code != TSDB_CODE_SUCCESS) {
    goto PROCESS_NAME_OVER;
  }

  if (!IS_SYS_DBNAME(metaRsp.dbFName) &&
      !tIsValidSchema(metaRsp.pSchemas, metaRsp.numOfColumns, metaRsp.numOfTags)) {
    code = TSDB_CODE_TSC_INVALID_VALUE;
    goto PROCESS_NAME_OVER;
  }

  STableMetaOutput *pOut = output;
  tstrncpy(pOut->dbFName, metaRsp.dbFName, TSDB_DB_FNAME_LEN);
  pOut->dbId = metaRsp.dbId;

  if (metaRsp.tableType == TSDB_CHILD_TABLE) {
    SET_META_TYPE_BOTH_TABLE(pOut->metaType);

    tstrncpy(pOut->ctbName, metaRsp.tbName, TSDB_TABLE_NAME_LEN);
    tstrncpy(pOut->tbName, metaRsp.stbName, TSDB_TABLE_NAME_LEN);

    pOut->ctbMeta.vgId = metaRsp.vgId;
    pOut->ctbMeta.tableType = metaRsp.tableType;
    pOut->ctbMeta.uid = metaRsp.tuid;
    pOut->ctbMeta.suid = metaRsp.suid;

    code = queryCreateTableMetaExFromMsg(&metaRsp, true, &pOut->tbMeta);
  } else if (metaRsp.tableType == TSDB_VIRTUAL_CHILD_TABLE) {
    SET_META_TYPE_BOTH_VTABLE(pOut->metaType);

    tstrncpy(pOut->ctbName, metaRsp.tbName, TSDB_TABLE_NAME_LEN);
    tstrncpy(pOut->tbName, metaRsp.stbName, TSDB_TABLE_NAME_LEN);

    code = queryCreateVCTableMetaFromMsg(&metaRsp, &pOut->vctbMeta);
    if (TSDB_CODE_SUCCESS != code) {
      goto PROCESS_NAME_OVER;
    }

    code = queryCreateTableMetaExFromMsg(&metaRsp, true, &pOut->tbMeta);
  } else {
    SET_META_TYPE_TABLE(pOut->metaType);
    tstrncpy(pOut->tbName, metaRsp.tbName, TSDB_TABLE_NAME_LEN);
    code = queryCreateTableMetaExFromMsg(&metaRsp, (metaRsp.tableType == TSDB_SUPER_TABLE), &pOut->tbMeta);
  }

PROCESS_NAME_OVER:
  if (code != 0) {
    qError("failed to process table name rsp since %s", tstrerror(code));
  }

  tFreeSTableMetaRsp(&metaRsp);
  return code;
}

int32_t queryProcessQnodeListRsp(void *output, char *msg, int32_t msgSize) {
  SQnodeListRsp out = {0};
  int32_t       code = 0;

  if (NULL == output || NULL == msg || msgSize <= 0) {
    qError("queryProcessQnodeListRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
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
    qError("queryProcessDnodeListRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
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
    qError("queryProcessGetSerVerRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    code = TSDB_CODE_TSC_INVALID_INPUT;
    return code;
  }

  if (tDeserializeSServerVerRsp(msg, msgSize, &out) != 0) {
    qError("invalid svr ver rsp msg, msgSize:%d", msgSize);
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  *(char **)output = taosStrdup(out.ver);
  if (NULL == *(char **)output) {
    return terrno;
  }

  return code;
}

int32_t queryProcessGetDbCfgRsp(void *output, char *msg, int32_t msgSize) {
  SDbCfgRsp out = {0};

  if (NULL == output || NULL == msg || msgSize <= 0) {
    qError("queryProcessGetDbCfgRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  if (tDeserializeSDbCfgRsp(msg, msgSize, &out) != 0) {
    qError("tDeserializeSDbCfgRsp failed, msgSize:%d, dbCfgRsp:%lu", msgSize, sizeof(out));
    return TSDB_CODE_INVALID_MSG;
  }

  memcpy(output, &out, sizeof(out));

  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessGetIndexRsp(void *output, char *msg, int32_t msgSize) {
  SUserIndexRsp out = {0};

  if (NULL == output || NULL == msg || msgSize <= 0) {
    qError("queryProcessGetIndexRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
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
    qError("queryProcessRetrieveFuncRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
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
    qError("queryProcessGetUserAuthRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  if (tDeserializeSGetUserAuthRsp(msg, msgSize, (SGetUserAuthRsp *)output) != 0) {
    qError("tDeserializeSGetUserAuthRsp failed, msgSize:%d", msgSize);
    return TSDB_CODE_INVALID_MSG;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessGetTbCfgRsp(void *output, char *msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    qError("queryProcessGetTbCfgRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  STableCfgRsp *out = taosMemoryCalloc(1, sizeof(STableCfgRsp));
  if(out == NULL) {
    return terrno;
  }
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
    qError("queryProcessGetViewMetaRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SViewMetaRsp *out = taosMemoryCalloc(1, sizeof(SViewMetaRsp));
  if (out == NULL) {
    return terrno;
  }
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

int32_t queryProcessGetTbTSMARsp(void* output, char* msg, int32_t msgSize) {
  if (NULL == output || NULL == msg || msgSize <= 0) {
    qError("queryProcessGetTbTSMARsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  if (tDeserializeTableTSMAInfoRsp(msg, msgSize, output) != 0) {
    qError("tDeserializeSViewMetaRsp failed, msgSize:%d", msgSize);
    return TSDB_CODE_INVALID_MSG;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessStreamProgressRsp(void* output, char* msg, int32_t msgSize) {
  if (!output || !msg || msgSize <= 0) {
    qError("queryProcessStreamProgressRsp: invalid input param, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  if (tDeserializeSStreamProgressRsp(msg, msgSize, output) != 0) {
    qError("tDeserializeStreamProgressRsp failed, msgSize:%d", msgSize);
    return TSDB_CODE_INVALID_MSG;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessVSubTablesRsp(void* output, char* msg, int32_t msgSize) {
  if (!output || !msg || msgSize <= 0) {
    qError("queryProcessVSubTablesRsp input error, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SVSubTablesRsp* pRsp = (SVSubTablesRsp*)output;
  int32_t code = tDeserializeSVSubTablesRsp(msg, msgSize, pRsp);
  if (code != 0) {
    qError("tDeserializeSVSubTablesRsp failed, msgSize: %d, error:%d", msgSize, code);
    return code;
  }
  
  return TSDB_CODE_SUCCESS;
}

int32_t queryProcessVStbRefDbsRsp(void* output, char* msg, int32_t msgSize) {
  if (!output || !msg || msgSize <= 0) {
    qError("queryProcessVStbRefDbsRsp input error, output:%p, msg:%p, msgSize:%d", output, msg, msgSize);
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  SVStbRefDbsRsp * pRsp = (SVStbRefDbsRsp*)output;
  int32_t code = tDeserializeSVStbRefDbsRsp(msg, msgSize, pRsp);
  if (code != 0) {
    qError("tDeserializeSVStbRefDbsRsp failed, msgSize: %d, error:%d", msgSize, code);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

void initQueryModuleMsgHandle() {
  queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_META)] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_NAME)] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_TABLE_META)] = queryBuildTableMetaReqMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_USE_DB)] = queryBuildUseDbMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_QNODE_LIST)] = queryBuildQnodeListMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_DNODE_LIST)] = queryBuildDnodeListMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_GET_DB_CFG)] = queryBuildGetDBCfgMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_RETRIEVE_FUNC)] = queryBuildRetrieveFuncMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_GET_USER_AUTH)] = queryBuildGetUserAuthMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_CFG)] = queryBuildGetTbCfgMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_TABLE_CFG)] = queryBuildGetTbCfgMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_SERVER_VERSION)] = queryBuildGetSerVerMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_VIEW_META)] = queryBuildGetViewMetaMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_GET_TABLE_TSMA)] = queryBuildGetTableTSMAMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_GET_TSMA)] = queryBuildGetTSMAMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_MND_GET_STREAM_PROGRESS)] = queryBuildGetStreamProgressMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_VND_VSUBTABLES_META)] = queryBuildVSubTablesMsg;
  queryBuildMsg[TMSG_INDEX(TDMT_VND_VSTB_REF_DBS)] = queryBuildVStbRefDBsMsg;

  queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_META)] = queryProcessTableMetaRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_NAME)] = queryProcessTableNameRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_TABLE_META)] = queryProcessTableMetaRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_USE_DB)] = queryProcessUseDBRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_QNODE_LIST)] = queryProcessQnodeListRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_DNODE_LIST)] = queryProcessDnodeListRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_GET_DB_CFG)] = queryProcessGetDbCfgRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_RETRIEVE_FUNC)] = queryProcessRetrieveFuncRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_GET_USER_AUTH)] = queryProcessGetUserAuthRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_CFG)] = queryProcessGetTbCfgRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_TABLE_CFG)] = queryProcessGetTbCfgRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_SERVER_VERSION)] = queryProcessGetSerVerRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_VIEW_META)] = queryProcessGetViewMetaRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_GET_TABLE_TSMA)] = queryProcessGetTbTSMARsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_GET_TSMA)] = queryProcessGetTbTSMARsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_GET_STREAM_PROGRESS)] = queryProcessStreamProgressRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_VSUBTABLES_META)] = queryProcessVSubTablesRsp;
  queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_VSTB_REF_DBS)] = queryProcessVStbRefDbsRsp;
}

#pragma GCC diagnostic pop
