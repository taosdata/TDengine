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

#include "tsdb.h"
#include "vnd.h"

#define VNODE_GET_LOAD_RESET_VALS(pVar, oVal, vType, tags)                                                    \
  do {                                                                                                        \
    int##vType##_t newVal = atomic_sub_fetch_##vType(&(pVar), (oVal));                                        \
    ASSERT(newVal >= 0);                                                                                      \
    if (newVal < 0) {                                                                                         \
      vWarn("vgId:%d, %s, abnormal val:%" PRIi64 ", old val:%" PRIi64, TD_VID(pVnode), tags, newVal, (oVal)); \
    }                                                                                                         \
  } while (0)

int vnodeQueryOpen(SVnode *pVnode) {
  return qWorkerInit(NODE_TYPE_VNODE, TD_VID(pVnode), (void **)&pVnode->pQuery, &pVnode->msgCb);
}

void vnodeQueryPreClose(SVnode *pVnode) { qWorkerStopAllTasks((void *)pVnode->pQuery); }

void vnodeQueryClose(SVnode *pVnode) { qWorkerDestroy((void **)&pVnode->pQuery); }

int vnodeGetTableMeta(SVnode *pVnode, SRpcMsg *pMsg, bool direct) {
  STableInfoReq  infoReq = {0};
  STableMetaRsp  metaRsp = {0};
  SMetaReader    mer1 = {0};
  SMetaReader    mer2 = {0};
  char           tableFName[TSDB_TABLE_FNAME_LEN];
  SRpcMsg        rpcMsg = {0};
  int32_t        code = 0;
  int32_t        rspLen = 0;
  void          *pRsp = NULL;
  SSchemaWrapper schema = {0};
  SSchemaWrapper schemaTag = {0};

  // decode req
  if (tDeserializeSTableInfoReq(pMsg->pCont, pMsg->contLen, &infoReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit4;
  }

  metaRsp.dbId = pVnode->config.dbId;
  strcpy(metaRsp.tbName, infoReq.tbName);
  memcpy(metaRsp.dbFName, infoReq.dbFName, sizeof(metaRsp.dbFName));

  sprintf(tableFName, "%s.%s", infoReq.dbFName, infoReq.tbName);
  code = vnodeValidateTableHash(pVnode, tableFName);
  if (code) {
    goto _exit4;
  }

  // query meta
  metaReaderDoInit(&mer1, pVnode->pMeta, 0);

  if (metaGetTableEntryByName(&mer1, infoReq.tbName) < 0) {
    code = terrno;
    goto _exit3;
  }

  metaRsp.tableType = mer1.me.type;
  metaRsp.vgId = TD_VID(pVnode);
  metaRsp.tuid = mer1.me.uid;

  if (mer1.me.type == TSDB_SUPER_TABLE) {
    strcpy(metaRsp.stbName, mer1.me.name);
    schema = mer1.me.stbEntry.schemaRow;
    schemaTag = mer1.me.stbEntry.schemaTag;
    metaRsp.suid = mer1.me.uid;
  } else if (mer1.me.type == TSDB_CHILD_TABLE) {
    metaReaderDoInit(&mer2, pVnode->pMeta, META_READER_NOLOCK);
    if (metaReaderGetTableEntryByUid(&mer2, mer1.me.ctbEntry.suid) < 0) goto _exit2;

    strcpy(metaRsp.stbName, mer2.me.name);
    metaRsp.suid = mer2.me.uid;
    schema = mer2.me.stbEntry.schemaRow;
    schemaTag = mer2.me.stbEntry.schemaTag;
  } else if (mer1.me.type == TSDB_NORMAL_TABLE) {
    schema = mer1.me.ntbEntry.schemaRow;
  } else {
    ASSERT(0);
  }

  metaRsp.numOfTags = schemaTag.nCols;
  metaRsp.numOfColumns = schema.nCols;
  metaRsp.precision = pVnode->config.tsdbCfg.precision;
  metaRsp.sversion = schema.version;
  metaRsp.tversion = schemaTag.version;
  metaRsp.pSchemas = (SSchema *)taosMemoryMalloc(sizeof(SSchema) * (metaRsp.numOfColumns + metaRsp.numOfTags));

  memcpy(metaRsp.pSchemas, schema.pSchema, sizeof(SSchema) * schema.nCols);
  if (schemaTag.nCols) {
    memcpy(metaRsp.pSchemas + schema.nCols, schemaTag.pSchema, sizeof(SSchema) * schemaTag.nCols);
  }

  // encode and send response
  rspLen = tSerializeSTableMetaRsp(NULL, 0, &metaRsp);
  if (rspLen < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (direct) {
    pRsp = rpcMallocCont(rspLen);
  } else {
    pRsp = taosMemoryCalloc(1, rspLen);
  }

  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  tSerializeSTableMetaRsp(pRsp, rspLen, &metaRsp);

_exit:
  taosMemoryFree(metaRsp.pSchemas);
_exit2:
  metaReaderClear(&mer2);
_exit3:
  metaReaderClear(&mer1);
_exit4:
  rpcMsg.info = pMsg->info;
  rpcMsg.pCont = pRsp;
  rpcMsg.contLen = rspLen;
  rpcMsg.code = code;
  rpcMsg.msgType = pMsg->msgType;

  if (code) {
    qError("get table %s meta failed cause of %s", infoReq.tbName, tstrerror(code));
  }

  if (direct) {
    tmsgSendRsp(&rpcMsg);
  } else {
    *pMsg = rpcMsg;
  }

  return TSDB_CODE_SUCCESS;
}

int vnodeGetTableCfg(SVnode *pVnode, SRpcMsg *pMsg, bool direct) {
  STableCfgReq   cfgReq = {0};
  STableCfgRsp   cfgRsp = {0};
  SMetaReader    mer1 = {0};
  SMetaReader    mer2 = {0};
  char           tableFName[TSDB_TABLE_FNAME_LEN];
  SRpcMsg        rpcMsg = {0};
  int32_t        code = 0;
  int32_t        rspLen = 0;
  void          *pRsp = NULL;
  SSchemaWrapper schema = {0};
  SSchemaWrapper schemaTag = {0};

  // decode req
  if (tDeserializeSTableCfgReq(pMsg->pCont, pMsg->contLen, &cfgReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  strcpy(cfgRsp.tbName, cfgReq.tbName);
  memcpy(cfgRsp.dbFName, cfgReq.dbFName, sizeof(cfgRsp.dbFName));

  sprintf(tableFName, "%s.%s", cfgReq.dbFName, cfgReq.tbName);
  code = vnodeValidateTableHash(pVnode, tableFName);
  if (code) {
    goto _exit;
  }

  // query meta
  metaReaderDoInit(&mer1, pVnode->pMeta, 0);

  if (metaGetTableEntryByName(&mer1, cfgReq.tbName) < 0) {
    code = terrno;
    goto _exit;
  }

  cfgRsp.tableType = mer1.me.type;

  if (mer1.me.type == TSDB_SUPER_TABLE) {
    code = TSDB_CODE_VND_HASH_MISMATCH;
    goto _exit;
  } else if (mer1.me.type == TSDB_CHILD_TABLE) {
    metaReaderDoInit(&mer2, pVnode->pMeta, 0);
    if (metaReaderGetTableEntryByUid(&mer2, mer1.me.ctbEntry.suid) < 0) goto _exit;

    strcpy(cfgRsp.stbName, mer2.me.name);
    schema = mer2.me.stbEntry.schemaRow;
    schemaTag = mer2.me.stbEntry.schemaTag;
    cfgRsp.ttl = mer1.me.ctbEntry.ttlDays;
    cfgRsp.commentLen = mer1.me.ctbEntry.commentLen;
    if (mer1.me.ctbEntry.commentLen > 0) {
      cfgRsp.pComment = taosStrdup(mer1.me.ctbEntry.comment);
    }
    STag *pTag = (STag *)mer1.me.ctbEntry.pTags;
    cfgRsp.tagsLen = pTag->len;
    cfgRsp.pTags = taosMemoryMalloc(cfgRsp.tagsLen);
    memcpy(cfgRsp.pTags, pTag, cfgRsp.tagsLen);
  } else if (mer1.me.type == TSDB_NORMAL_TABLE) {
    schema = mer1.me.ntbEntry.schemaRow;
    cfgRsp.ttl = mer1.me.ntbEntry.ttlDays;
    cfgRsp.commentLen = mer1.me.ntbEntry.commentLen;
    if (mer1.me.ntbEntry.commentLen > 0) {
      cfgRsp.pComment = taosStrdup(mer1.me.ntbEntry.comment);
    }
  } else {
    ASSERT(0);
  }

  cfgRsp.numOfTags = schemaTag.nCols;
  cfgRsp.numOfColumns = schema.nCols;
  cfgRsp.pSchemas = (SSchema *)taosMemoryMalloc(sizeof(SSchema) * (cfgRsp.numOfColumns + cfgRsp.numOfTags));

  memcpy(cfgRsp.pSchemas, schema.pSchema, sizeof(SSchema) * schema.nCols);
  if (schemaTag.nCols) {
    memcpy(cfgRsp.pSchemas + schema.nCols, schemaTag.pSchema, sizeof(SSchema) * schemaTag.nCols);
  }

  // encode and send response
  rspLen = tSerializeSTableCfgRsp(NULL, 0, &cfgRsp);
  if (rspLen < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (direct) {
    pRsp = rpcMallocCont(rspLen);
  } else {
    pRsp = taosMemoryCalloc(1, rspLen);
  }

  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  tSerializeSTableCfgRsp(pRsp, rspLen, &cfgRsp);

_exit:
  rpcMsg.info = pMsg->info;
  rpcMsg.pCont = pRsp;
  rpcMsg.contLen = rspLen;
  rpcMsg.code = code;
  rpcMsg.msgType = pMsg->msgType;

  if (code) {
    qError("get table %s cfg failed cause of %s", cfgReq.tbName, tstrerror(code));
  }

  if (direct) {
    tmsgSendRsp(&rpcMsg);
  } else {
    *pMsg = rpcMsg;
  }

  tFreeSTableCfgRsp(&cfgRsp);
  metaReaderClear(&mer2);
  metaReaderClear(&mer1);
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void vnodeFreeSBatchRspMsg(void *p) {
  if (NULL == p) {
    return;
  }

  SBatchRspMsg *pRsp = (SBatchRspMsg *)p;
  rpcFreeCont(pRsp->msg);
}

int32_t vnodeGetBatchMeta(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t      code = 0;
  int32_t      rspSize = 0;
  SBatchReq    batchReq = {0};
  SBatchMsg   *req = NULL;
  SBatchRspMsg rsp = {0};
  SBatchRsp    batchRsp = {0};
  SRpcMsg      reqMsg = *pMsg;
  SRpcMsg      rspMsg = {0};
  void        *pRsp = NULL;

  if (tDeserializeSBatchReq(pMsg->pCont, pMsg->contLen, &batchReq)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    qError("tDeserializeSBatchReq failed");
    goto _exit;
  }

  int32_t msgNum = taosArrayGetSize(batchReq.pMsgs);
  if (msgNum >= MAX_META_MSG_IN_BATCH) {
    code = TSDB_CODE_INVALID_MSG;
    qError("too many msgs %d in vnode batch meta req", msgNum);
    goto _exit;
  }

  batchRsp.pRsps = taosArrayInit(msgNum, sizeof(SBatchRspMsg));
  if (NULL == batchRsp.pRsps) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    qError("taosArrayInit %d SBatchRspMsg failed", msgNum);
    goto _exit;
  }

  for (int32_t i = 0; i < msgNum; ++i) {
    req = taosArrayGet(batchReq.pMsgs, i);

    reqMsg.msgType = req->msgType;
    reqMsg.pCont = req->msg;
    reqMsg.contLen = req->msgLen;

    switch (req->msgType) {
      case TDMT_VND_TABLE_META:
        vnodeGetTableMeta(pVnode, &reqMsg, false);
        break;
      case TDMT_VND_TABLE_CFG:
        vnodeGetTableCfg(pVnode, &reqMsg, false);
        break;
      default:
        qError("invalid req msgType %d", req->msgType);
        reqMsg.code = TSDB_CODE_INVALID_MSG;
        reqMsg.pCont = NULL;
        reqMsg.contLen = 0;
        break;
    }

    rsp.msgIdx = req->msgIdx;
    rsp.reqType = reqMsg.msgType;
    rsp.msgLen = reqMsg.contLen;
    rsp.rspCode = reqMsg.code;
    rsp.msg = reqMsg.pCont;

    taosArrayPush(batchRsp.pRsps, &rsp);
  }

  rspSize = tSerializeSBatchRsp(NULL, 0, &batchRsp);
  if (rspSize < 0) {
    qError("tSerializeSBatchRsp failed");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pRsp = rpcMallocCont(rspSize);
  if (pRsp == NULL) {
    qError("rpcMallocCont %d failed", rspSize);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  if (tSerializeSBatchRsp(pRsp, rspSize, &batchRsp) < 0) {
    qError("tSerializeSBatchRsp %d failed", rspSize);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:

  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = pMsg->msgType;

  if (code) {
    qError("vnd get batch meta failed cause of %s", tstrerror(code));
  }

  taosArrayDestroyEx(batchReq.pMsgs, tFreeSBatchReqMsg);
  taosArrayDestroyEx(batchRsp.pRsps, tFreeSBatchRspMsg);

  tmsgSendRsp(&rspMsg);

  return code;
}

int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad) {
  SSyncState state = syncGetState(pVnode->sync);

  pLoad->vgId = TD_VID(pVnode);
  pLoad->syncState = state.state;
  pLoad->syncRestore = state.restored;
  pLoad->syncTerm = state.term;
  pLoad->roleTimeMs = state.roleTimeMs;
  pLoad->startTimeMs = state.startTimeMs;
  pLoad->syncCanRead = state.canRead;
  pLoad->learnerProgress = state.progress;
  pLoad->cacheUsage = tsdbCacheGetUsage(pVnode);
  pLoad->numOfCachedTables = tsdbCacheGetElems(pVnode);
  pLoad->numOfTables = metaGetTbNum(pVnode->pMeta);
  pLoad->numOfTimeSeries = metaGetTimeSeriesNum(pVnode->pMeta, 1);
  pLoad->totalStorage = (int64_t)3 * 1073741824;
  pLoad->compStorage = (int64_t)2 * 1073741824;
  pLoad->pointsWritten = 100;
  pLoad->numOfSelectReqs = 1;
  pLoad->numOfInsertReqs = atomic_load_64(&pVnode->statis.nInsert);
  pLoad->numOfInsertSuccessReqs = atomic_load_64(&pVnode->statis.nInsertSuccess);
  pLoad->numOfBatchInsertReqs = atomic_load_64(&pVnode->statis.nBatchInsert);
  pLoad->numOfBatchInsertSuccessReqs = atomic_load_64(&pVnode->statis.nBatchInsertSuccess);
  return 0;
}

int32_t vnodeGetLoadLite(SVnode *pVnode, SVnodeLoadLite *pLoad) {
  SSyncState syncState = syncGetState(pVnode->sync);
  if (syncState.state == TAOS_SYNC_STATE_LEADER) {
    pLoad->vgId = TD_VID(pVnode);
    pLoad->nTimeSeries = metaGetTimeSeriesNum(pVnode->pMeta, 1);
    return 0;
  }
  return -1;
}
/**
 * @brief Reset the statistics value by monitor interval
 *
 * @param pVnode
 * @param pLoad
 */
void vnodeResetLoad(SVnode *pVnode, SVnodeLoad *pLoad) {
  VNODE_GET_LOAD_RESET_VALS(pVnode->statis.nInsert, pLoad->numOfInsertReqs, 64, "nInsert");
  VNODE_GET_LOAD_RESET_VALS(pVnode->statis.nInsertSuccess, pLoad->numOfInsertSuccessReqs, 64, "nInsertSuccess");
  VNODE_GET_LOAD_RESET_VALS(pVnode->statis.nBatchInsert, pLoad->numOfBatchInsertReqs, 64, "nBatchInsert");
  VNODE_GET_LOAD_RESET_VALS(pVnode->statis.nBatchInsertSuccess, pLoad->numOfBatchInsertSuccessReqs, 64,
                            "nBatchInsertSuccess");
}

void vnodeGetInfo(void *pVnode, const char **dbname, int32_t *vgId, int64_t *numOfTables, int64_t *numOfNormalTables) {
  SVnode    *pVnodeObj = pVnode;
  SVnodeCfg *pConf = &pVnodeObj->config;

  if (dbname) {
    *dbname = pConf->dbname;
  }

  if (vgId) {
    *vgId = TD_VID(pVnodeObj);
  }

  if (numOfTables) {
    *numOfTables = pConf->vndStats.numOfNTables + pConf->vndStats.numOfCTables;
  }

  if (numOfNormalTables) {
    *numOfNormalTables = pConf->vndStats.numOfNTables;
  }
}

int32_t vnodeGetTableList(void *pVnode, int8_t type, SArray *pList) {
  if (type == TSDB_SUPER_TABLE) {
    return vnodeGetStbIdList(pVnode, 0, pList);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int32_t vnodeGetAllTableList(SVnode *pVnode, uint64_t uid, SArray *list) {
  SMCtbCursor *pCur = metaOpenCtbCursor(pVnode, uid, 1);

  while (1) {
    tb_uid_t id = metaCtbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    STableKeyInfo info = {uid = id};
    taosArrayPush(list, &info);
  }

  metaCloseCtbCursor(pCur);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetCtbIdListByFilter(SVnode *pVnode, int64_t suid, SArray *list, bool (*filter)(void *arg), void *arg) {
  return 0;
}

int32_t vnodeGetCtbIdList(void *pVnode, int64_t suid, SArray *list) {
  SVnode      *pVnodeObj = pVnode;
  SMCtbCursor *pCur = metaOpenCtbCursor(pVnodeObj, suid, 1);

  while (1) {
    tb_uid_t id = metaCtbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    taosArrayPush(list, &id);
  }

  metaCloseCtbCursor(pCur);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetStbIdList(SVnode *pVnode, int64_t suid, SArray *list) {
  SMStbCursor *pCur = metaOpenStbCursor(pVnode->pMeta, suid);
  if (!pCur) {
    return TSDB_CODE_FAILED;
  }

  while (1) {
    tb_uid_t id = metaStbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    taosArrayPush(list, &id);
  }

  metaCloseStbCursor(pCur);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetStbIdListByFilter(SVnode *pVnode, int64_t suid, SArray *list, bool (*filter)(void *arg, void *arg1),
                                  void *arg) {
  SMStbCursor *pCur = metaOpenStbCursor(pVnode->pMeta, suid);
  if (!pCur) {
    return TSDB_CODE_FAILED;
  }

  while (1) {
    tb_uid_t id = metaStbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    if ((*filter) && (*filter)(arg, &id)) {
      continue;
    }

    taosArrayPush(list, &id);
  }

  metaCloseStbCursor(pCur);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetCtbNum(SVnode *pVnode, int64_t suid, int64_t *num) {
  SMCtbCursor *pCur = metaOpenCtbCursor(pVnode, suid, 0);
  if (!pCur) {
    return TSDB_CODE_FAILED;
  }

  *num = 0;
  while (1) {
    tb_uid_t id = metaCtbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    ++(*num);
  }

  metaCloseCtbCursor(pCur);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetStbColumnNum(SVnode *pVnode, tb_uid_t suid, int *num) {
  SSchemaWrapper *pSW = metaGetTableSchema(pVnode->pMeta, suid, -1, 0);
  if (pSW) {
    *num = pSW->nCols;
    tDeleteSchemaWrapper(pSW);
  } else {
    *num = 2;
  }

  return TSDB_CODE_SUCCESS;
}

#ifdef TD_ENTERPRISE
const char *tkLogStb[] = {"cluster_info",
                          "data_dir",
                          "dnodes_info",
                          "d_info",
                          "grants_info",
                          "keeper_monitor",
                          "logs",
                          "log_dir",
                          "log_summary",
                          "m_info",
                          "taosadapter_restful_http_request_fail",
                          "taosadapter_restful_http_request_in_flight",
                          "taosadapter_restful_http_request_summary_milliseconds",
                          "taosadapter_restful_http_request_total",
                          "taosadapter_system_cpu_percent",
                          "taosadapter_system_mem_percent",
                          "temp_dir",
                          "vgroups_info",
                          "vnodes_role"};
const char *tkAuditStb[] = {"operations"};
const int   tkLogStbNum = ARRAY_SIZE(tkLogStb);
const int   tkAuditStbNum = ARRAY_SIZE(tkAuditStb);

// exclude stbs of taoskeeper log
static int32_t vnodeGetTimeSeriesBlackList(SVnode *pVnode) {
  int32_t      tbSize = 0;
  int32_t      tbNum = 0;
  const char **pTbArr = NULL;
  const char  *dbName = NULL;

  if (!(dbName = strchr(pVnode->config.dbname, '.'))) return 0;
  if (0 == strncmp(++dbName, "log", TSDB_DB_NAME_LEN)) {
    tbNum = tkLogStbNum;
    pTbArr = (const char **)&tkLogStb;
  } else if (0 == strncmp(dbName, "audit", TSDB_DB_NAME_LEN)) {
    tbNum = tkAuditStbNum;
    pTbArr = (const char **)&tkAuditStb;
  }
  if (tbNum && pTbArr) {
    tbSize = metaSizeOfTbFilterCache(pVnode->pMeta, 0);
    if (tbSize < tbNum) {
      for (int32_t i = 0; i < tbNum; ++i) {
        tb_uid_t suid = metaGetTableEntryUidByName(pVnode->pMeta, pTbArr[i]);
        if (suid != 0) {
          metaPutTbToFilterCache(pVnode->pMeta, &suid, 0);
        }
      }
      tbSize = metaSizeOfTbFilterCache(pVnode->pMeta, 0);
    }
  }

  return tbSize;
}
#endif

static bool vnodeTimeSeriesFilter(void *arg1, void *arg2) {
  SVnode *pVnode = (SVnode *)arg1;

  if (metaTbInFilterCache(pVnode->pMeta, arg2, 0)) {
    return true;
  }
  return false;
}

int32_t vnodeGetTimeSeriesNum(SVnode *pVnode, int64_t *num) {
  SArray *suidList = NULL;

  if (!(suidList = taosArrayInit(1, sizeof(tb_uid_t)))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  int32_t tbFilterSize = 0;
#ifdef TD_ENTERPRISE
  tbFilterSize = vnodeGetTimeSeriesBlackList(pVnode);
#endif

  if ((!tbFilterSize && vnodeGetStbIdList(pVnode, 0, suidList) < 0) ||
      (tbFilterSize && vnodeGetStbIdListByFilter(pVnode, 0, suidList, vnodeTimeSeriesFilter, pVnode) < 0)) {
    qError("vgId:%d, failed to get stb id list error: %s", TD_VID(pVnode), terrstr());
    taosArrayDestroy(suidList);
    return TSDB_CODE_FAILED;
  }

  *num = 0;
  int64_t arrSize = taosArrayGetSize(suidList);
  for (int64_t i = 0; i < arrSize; ++i) {
    tb_uid_t suid = *(tb_uid_t *)taosArrayGet(suidList, i);

    int64_t ctbNum = 0;
    int32_t numOfCols = 0;
    metaGetStbStats(pVnode, suid, &ctbNum, &numOfCols);

    *num += ctbNum * (numOfCols - 1);
  }

  taosArrayDestroy(suidList);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetAllCtbNum(SVnode *pVnode, int64_t *num) {
  SMStbCursor *pCur = metaOpenStbCursor(pVnode->pMeta, 0);
  if (!pCur) {
    return TSDB_CODE_FAILED;
  }

  *num = 0;
  while (1) {
    tb_uid_t id = metaStbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    int64_t ctbNum = 0;
    vnodeGetCtbNum(pVnode, id, &ctbNum);

    *num += ctbNum;
  }

  metaCloseStbCursor(pCur);
  return TSDB_CODE_SUCCESS;
}

void *vnodeGetIdx(void *pVnode) {
  if (pVnode == NULL) {
    return NULL;
  }

  return metaGetIdx(((SVnode *)pVnode)->pMeta);
}

void *vnodeGetIvtIdx(void *pVnode) {
  if (pVnode == NULL) {
    return NULL;
  }
  return metaGetIvtIdx(((SVnode *)pVnode)->pMeta);
}

int32_t vnodeGetTableSchema(void *pVnode, int64_t uid, STSchema **pSchema, int64_t *suid) {
  return tsdbGetTableSchema(((SVnode *)pVnode)->pMeta, uid, pSchema, suid);
}
