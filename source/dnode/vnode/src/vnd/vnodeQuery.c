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
    if (newVal < 0) {                                                                                         \
      vWarn("vgId:%d, %s, abnormal val:%" PRIi64 ", old val:%" PRIi64, TD_VID(pVnode), tags, newVal, (oVal)); \
    }                                                                                                         \
  } while (0)

int vnodeQueryOpen(SVnode *pVnode) {
  return qWorkerInit(NODE_TYPE_VNODE, TD_VID(pVnode), (void **)&pVnode->pQuery, &pVnode->msgCb);
}

void vnodeQueryPreClose(SVnode *pVnode) { qWorkerStopAllTasks((void *)pVnode->pQuery); }

void vnodeQueryClose(SVnode *pVnode) { qWorkerDestroy((void **)&pVnode->pQuery); }

int32_t fillTableColCmpr(SMetaReader *reader, SSchemaExt *pExt, int32_t numOfCol) {
  int8_t tblType = reader->me.type;
  if (useCompress(tblType)) {
    SColCmprWrapper *p = &(reader->me.colCmpr);
    if (numOfCol != p->nCols) {
      vError("fillTableColCmpr table type:%d, col num:%d, col cmpr num:%d mismatch", tblType, numOfCol, p->nCols);
      return TSDB_CODE_APP_ERROR;
    }
    for (int i = 0; i < p->nCols; i++) {
      SColCmpr *pCmpr = &p->pColCmpr[i];
      pExt[i].colId = pCmpr->id;
      pExt[i].compress = pCmpr->alg;
    }
  }
  return 0;
}

int32_t vnodeGetTableMeta(SVnode *pVnode, SRpcMsg *pMsg, bool direct) {
  STableInfoReq  infoReq = {0};
  STableMetaRsp  metaRsp = {0};
  SMetaReader    mer1 = {0};
  SMetaReader    mer2 = {0};
  char           tableFName[TSDB_TABLE_FNAME_LEN];
  bool           reqTbUid = false;
  SRpcMsg        rpcMsg = {0};
  int32_t        code = 0;
  int32_t        rspLen = 0;
  void          *pRsp = NULL;
  SSchemaWrapper schema = {0};
  SSchemaWrapper schemaTag = {0};

  // decode req
  if (tDeserializeSTableInfoReq(pMsg->pCont, pMsg->contLen, &infoReq) != 0) {
    code = terrno;
    goto _exit4;
  }

  if (infoReq.option == REQ_OPT_TBUID) reqTbUid = true;
  metaRsp.dbId = pVnode->config.dbId;
  (void)strcpy(metaRsp.tbName, infoReq.tbName);
  (void)memcpy(metaRsp.dbFName, infoReq.dbFName, sizeof(metaRsp.dbFName));

  if (!reqTbUid) {
    TAOS_UNUSED(sprintf(tableFName, "%s.%s", infoReq.dbFName, infoReq.tbName));
    code = vnodeValidateTableHash(pVnode, tableFName);
    if (code) {
      goto _exit4;
    }
  }

  // query meta
  metaReaderDoInit(&mer1, pVnode->pMeta, META_READER_LOCK);
  if (reqTbUid) {
    errno = 0;
    uint64_t tbUid = taosStr2UInt64(infoReq.tbName, NULL, 10);
    if (errno == ERANGE || tbUid == 0) {
      code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
      goto _exit3;
    }
    char tbName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    TAOS_CHECK_GOTO(metaGetTableNameByUid(pVnode, tbUid, tbName), NULL, _exit3);
    tstrncpy(metaRsp.tbName, varDataVal(tbName), TSDB_TABLE_NAME_LEN);
    TAOS_CHECK_GOTO(metaGetTableEntryByName(&mer1, varDataVal(tbName)), NULL, _exit3);
  } else if (metaGetTableEntryByName(&mer1, infoReq.tbName) < 0) {
    code = terrno;
    goto _exit3;
  }

  metaRsp.tableType = mer1.me.type;
  metaRsp.vgId = TD_VID(pVnode);
  metaRsp.tuid = mer1.me.uid;

  if (mer1.me.type == TSDB_SUPER_TABLE) {
    (void)strcpy(metaRsp.stbName, mer1.me.name);
    schema = mer1.me.stbEntry.schemaRow;
    schemaTag = mer1.me.stbEntry.schemaTag;
    metaRsp.suid = mer1.me.uid;
  } else if (mer1.me.type == TSDB_CHILD_TABLE) {
    metaReaderDoInit(&mer2, pVnode->pMeta, META_READER_NOLOCK);
    if (metaReaderGetTableEntryByUid(&mer2, mer1.me.ctbEntry.suid) < 0) goto _exit2;

    (void)strcpy(metaRsp.stbName, mer2.me.name);
    metaRsp.suid = mer2.me.uid;
    schema = mer2.me.stbEntry.schemaRow;
    schemaTag = mer2.me.stbEntry.schemaTag;
  } else if (mer1.me.type == TSDB_NORMAL_TABLE) {
    schema = mer1.me.ntbEntry.schemaRow;
  } else {
    vError("vnodeGetTableMeta get invalid table type:%d", mer1.me.type);
    goto _exit3;
  }

  metaRsp.numOfTags = schemaTag.nCols;
  metaRsp.numOfColumns = schema.nCols;
  metaRsp.precision = pVnode->config.tsdbCfg.precision;
  metaRsp.sversion = schema.version;
  metaRsp.tversion = schemaTag.version;
  metaRsp.pSchemas = (SSchema *)taosMemoryMalloc(sizeof(SSchema) * (metaRsp.numOfColumns + metaRsp.numOfTags));
  metaRsp.pSchemaExt = (SSchemaExt *)taosMemoryCalloc(metaRsp.numOfColumns, sizeof(SSchemaExt));
  if (NULL == metaRsp.pSchemas || NULL == metaRsp.pSchemaExt) {
    code = terrno;
    goto _exit;
  }
  (void)memcpy(metaRsp.pSchemas, schema.pSchema, sizeof(SSchema) * schema.nCols);
  if (schemaTag.nCols) {
    (void)memcpy(metaRsp.pSchemas + schema.nCols, schemaTag.pSchema, sizeof(SSchema) * schemaTag.nCols);
  }
  if (metaRsp.pSchemaExt) {
    SMetaReader *pReader = mer1.me.type == TSDB_CHILD_TABLE ? &mer2 : &mer1;
    code = fillTableColCmpr(pReader, metaRsp.pSchemaExt, metaRsp.numOfColumns);
    if (code < 0) {
      goto _exit;
    }
  } else {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // encode and send response
  rspLen = tSerializeSTableMetaRsp(NULL, 0, &metaRsp);
  if (rspLen < 0) {
    code = terrno;
    goto _exit;
  }

  if (direct) {
    pRsp = rpcMallocCont(rspLen);
  } else {
    pRsp = taosMemoryCalloc(1, rspLen);
  }

  if (pRsp == NULL) {
    code = terrno;
    goto _exit;
  }

  rspLen = tSerializeSTableMetaRsp(pRsp, rspLen, &metaRsp);
  if (rspLen < 0) {
    code = terrno;
    goto _exit;
  }

_exit:
  taosMemoryFree(metaRsp.pSchemas);
  taosMemoryFree(metaRsp.pSchemaExt);
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
    qError("get table %s meta with %" PRIu8 " failed cause of %s", infoReq.tbName, infoReq.option, tstrerror(code));
  }

  if (direct) {
    tmsgSendRsp(&rpcMsg);
  } else {
    *pMsg = rpcMsg;
  }

  return code;
}

int32_t vnodeGetTableCfg(SVnode *pVnode, SRpcMsg *pMsg, bool direct) {
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
    code = terrno;
    goto _exit;
  }

  (void)strcpy(cfgRsp.tbName, cfgReq.tbName);
  (void)memcpy(cfgRsp.dbFName, cfgReq.dbFName, sizeof(cfgRsp.dbFName));

  (void)sprintf(tableFName, "%s.%s", cfgReq.dbFName, cfgReq.tbName);
  code = vnodeValidateTableHash(pVnode, tableFName);
  if (code) {
    goto _exit;
  }

  // query meta
  metaReaderDoInit(&mer1, pVnode->pMeta, META_READER_LOCK);

  if (metaGetTableEntryByName(&mer1, cfgReq.tbName) < 0) {
    code = terrno;
    goto _exit;
  }

  cfgRsp.tableType = mer1.me.type;

  if (mer1.me.type == TSDB_SUPER_TABLE) {
    code = TSDB_CODE_VND_HASH_MISMATCH;
    goto _exit;
  } else if (mer1.me.type == TSDB_CHILD_TABLE) {
    metaReaderDoInit(&mer2, pVnode->pMeta, META_READER_LOCK);
    if (metaReaderGetTableEntryByUid(&mer2, mer1.me.ctbEntry.suid) < 0) goto _exit;

    (void)strcpy(cfgRsp.stbName, mer2.me.name);
    schema = mer2.me.stbEntry.schemaRow;
    schemaTag = mer2.me.stbEntry.schemaTag;
    cfgRsp.ttl = mer1.me.ctbEntry.ttlDays;
    cfgRsp.commentLen = mer1.me.ctbEntry.commentLen;
    if (mer1.me.ctbEntry.commentLen > 0) {
      cfgRsp.pComment = taosStrdup(mer1.me.ctbEntry.comment);
      if (NULL == cfgRsp.pComment) {
        code = terrno;
        goto _exit;
      }
    }
    STag *pTag = (STag *)mer1.me.ctbEntry.pTags;
    cfgRsp.tagsLen = pTag->len;
    cfgRsp.pTags = taosMemoryMalloc(cfgRsp.tagsLen);
    if (NULL == cfgRsp.pTags) {
      code = terrno;
      goto _exit;
    }
    (void)memcpy(cfgRsp.pTags, pTag, cfgRsp.tagsLen);
  } else if (mer1.me.type == TSDB_NORMAL_TABLE) {
    schema = mer1.me.ntbEntry.schemaRow;
    cfgRsp.ttl = mer1.me.ntbEntry.ttlDays;
    cfgRsp.commentLen = mer1.me.ntbEntry.commentLen;
    if (mer1.me.ntbEntry.commentLen > 0) {
      cfgRsp.pComment = taosStrdup(mer1.me.ntbEntry.comment);
      if (NULL == cfgRsp.pComment) {
        code = terrno;
        goto _exit;
      }
    }
  } else {
    vError("vnodeGetTableCfg get invalid table type:%d", mer1.me.type);
    return TSDB_CODE_APP_ERROR;
  }

  cfgRsp.numOfTags = schemaTag.nCols;
  cfgRsp.numOfColumns = schema.nCols;
  cfgRsp.pSchemas = (SSchema *)taosMemoryMalloc(sizeof(SSchema) * (cfgRsp.numOfColumns + cfgRsp.numOfTags));
  cfgRsp.pSchemaExt = (SSchemaExt *)taosMemoryMalloc(cfgRsp.numOfColumns * sizeof(SSchemaExt));

  if (NULL == cfgRsp.pSchemas || NULL == cfgRsp.pSchemaExt) {
    code = terrno;
    goto _exit;
  }
  (void)memcpy(cfgRsp.pSchemas, schema.pSchema, sizeof(SSchema) * schema.nCols);
  if (schemaTag.nCols) {
    (void)memcpy(cfgRsp.pSchemas + schema.nCols, schemaTag.pSchema, sizeof(SSchema) * schemaTag.nCols);
  }

  // if (useCompress(cfgRsp.tableType)) {

  SMetaReader     *pReader = mer1.me.type == TSDB_CHILD_TABLE ? &mer2 : &mer1;
  SColCmprWrapper *pColCmpr = &pReader->me.colCmpr;

  for (int32_t i = 0; i < cfgRsp.numOfColumns; i++) {
    SColCmpr   *pCmpr = &pColCmpr->pColCmpr[i];
    SSchemaExt *pSchExt = cfgRsp.pSchemaExt + i;
    pSchExt->colId = pCmpr->id;
    pSchExt->compress = pCmpr->alg;
  }
  //}

  // encode and send response
  rspLen = tSerializeSTableCfgRsp(NULL, 0, &cfgRsp);
  if (rspLen < 0) {
    code = terrno;
    goto _exit;
  }

  if (direct) {
    pRsp = rpcMallocCont(rspLen);
  } else {
    pRsp = taosMemoryCalloc(1, rspLen);
  }

  if (pRsp == NULL) {
    code = terrno;
    goto _exit;
  }

  rspLen = tSerializeSTableCfgRsp(pRsp, rspLen, &cfgRsp);
  if (rspLen < 0) {
    code = terrno;
    goto _exit;
  }

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
  return code;
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
    code = terrno;
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
    code = terrno;
    qError("taosArrayInit %d SBatchRspMsg failed", msgNum);
    goto _exit;
  }

  for (int32_t i = 0; i < msgNum; ++i) {
    req = taosArrayGet(batchReq.pMsgs, i);
    if (req == NULL) {
      code = terrno;
      goto _exit;
    }

    reqMsg.msgType = req->msgType;
    reqMsg.pCont = req->msg;
    reqMsg.contLen = req->msgLen;

    switch (req->msgType) {
      case TDMT_VND_TABLE_META:
        // error code has been set into reqMsg, no need to handle it here.
        if (TSDB_CODE_SUCCESS != vnodeGetTableMeta(pVnode, &reqMsg, false)) {
          qWarn("vnodeGetBatchMeta failed, msgType:%d", req->msgType);
        }
        break;
      case TDMT_VND_TABLE_NAME:
        // error code has been set into reqMsg, no need to handle it here.
        if (TSDB_CODE_SUCCESS != vnodeGetTableMeta(pVnode, &reqMsg, false)) {
          qWarn("vnodeGetBatchName failed, msgType:%d", req->msgType);
        }
        break;
      case TDMT_VND_TABLE_CFG:
        // error code has been set into reqMsg, no need to handle it here.
        if (TSDB_CODE_SUCCESS != vnodeGetTableCfg(pVnode, &reqMsg, false)) {
          qWarn("vnodeGetBatchMeta failed, msgType:%d", req->msgType);
        }
        break;
      case TDMT_VND_GET_STREAM_PROGRESS:
        // error code has been set into reqMsg, no need to handle it here.
        if (TSDB_CODE_SUCCESS != vnodeGetStreamProgress(pVnode, &reqMsg, false)) {
          qWarn("vnodeGetBatchMeta failed, msgType:%d", req->msgType);
        }
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

    if (NULL == taosArrayPush(batchRsp.pRsps, &rsp)) {
      qError("taosArrayPush failed");
      code = terrno;
      goto _exit;
    }
  }

  rspSize = tSerializeSBatchRsp(NULL, 0, &batchRsp);
  if (rspSize < 0) {
    qError("tSerializeSBatchRsp failed");
    code = terrno;
    goto _exit;
  }
  pRsp = rpcMallocCont(rspSize);
  if (pRsp == NULL) {
    qError("rpcMallocCont %d failed", rspSize);
    code = terrno;
    goto _exit;
  }
  if (tSerializeSBatchRsp(pRsp, rspSize, &batchRsp) < 0) {
    qError("tSerializeSBatchRsp %d failed", rspSize);
    code = terrno;
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
  if (syncState.state == TAOS_SYNC_STATE_LEADER || syncState.state == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
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
  int32_t      code = TSDB_CODE_SUCCESS;
  SMCtbCursor *pCur = metaOpenCtbCursor(pVnode, uid, 1);
  if (NULL == pCur) {
    qError("vnode get all table list failed");
    return terrno;
  }

  while (1) {
    tb_uid_t id = metaCtbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    STableKeyInfo info = {uid = id};
    if (NULL == taosArrayPush(list, &info)) {
      qError("taosArrayPush failed");
      code = terrno;
      goto _exit;
    }
  }
_exit:
  metaCloseCtbCursor(pCur);
  return code;
}

int32_t vnodeGetCtbIdListByFilter(SVnode *pVnode, int64_t suid, SArray *list, bool (*filter)(void *arg), void *arg) {
  return 0;
}

int32_t vnodeGetCtbIdList(void *pVnode, int64_t suid, SArray *list) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SVnode      *pVnodeObj = pVnode;
  SMCtbCursor *pCur = metaOpenCtbCursor(pVnodeObj, suid, 1);
  if (NULL == pCur) {
    qError("vnode get all table list failed");
    return terrno;
  }

  while (1) {
    tb_uid_t id = metaCtbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    if (NULL == taosArrayPush(list, &id)) {
      qError("taosArrayPush failed");
      code = terrno;
      goto _exit;
    }
  }

_exit:
  metaCloseCtbCursor(pCur);
  return code;
}

int32_t vnodeGetStbIdList(SVnode *pVnode, int64_t suid, SArray *list) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SMStbCursor *pCur = metaOpenStbCursor(pVnode->pMeta, suid);
  if (!pCur) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  while (1) {
    tb_uid_t id = metaStbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    if (NULL == taosArrayPush(list, &id)) {
      qError("taosArrayPush failed");
      code = terrno;
      goto _exit;
    }
  }

_exit:
  metaCloseStbCursor(pCur);
  return code;
}

int32_t vnodeGetStbIdListByFilter(SVnode *pVnode, int64_t suid, SArray *list, bool (*filter)(void *arg, void *arg1),
                                  void *arg) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SMStbCursor *pCur = metaOpenStbCursor(pVnode->pMeta, suid);
  if (!pCur) {
    return terrno;
  }

  while (1) {
    tb_uid_t id = metaStbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    if ((*filter) && (*filter)(arg, &id)) {
      continue;
    }

    if (NULL == taosArrayPush(list, &id)) {
      qError("taosArrayPush failed");
      code = terrno;
      goto _exit;
    }
  }

_exit:
  metaCloseStbCursor(pCur);
  return code;
}

int32_t vnodeGetCtbNum(SVnode *pVnode, int64_t suid, int64_t *num) {
  SMCtbCursor *pCur = metaOpenCtbCursor(pVnode, suid, 0);
  if (!pCur) {
    return terrno;
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
  SSchemaWrapper *pSW = metaGetTableSchema(pVnode->pMeta, suid, -1, 0, NULL);
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
static int32_t vnodeGetTimeSeriesBlackList(SVnode *pVnode, int32_t *tbSize) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      tbNum = 0;
  const char **pTbArr = NULL;
  const char  *dbName = NULL;
  *tbSize = 0;

  if (!(dbName = strchr(pVnode->config.dbname, '.'))) return 0;
  if (0 == strncmp(++dbName, "log", TSDB_DB_NAME_LEN)) {
    tbNum = tkLogStbNum;
    pTbArr = (const char **)&tkLogStb;
  } else if (0 == strncmp(dbName, "audit", TSDB_DB_NAME_LEN)) {
    tbNum = tkAuditStbNum;
    pTbArr = (const char **)&tkAuditStb;
  }
  if (tbNum && pTbArr) {
    *tbSize = metaSizeOfTbFilterCache(pVnode->pMeta, 0);
    if (*tbSize < tbNum) {
      for (int32_t i = 0; i < tbNum; ++i) {
        tb_uid_t suid = metaGetTableEntryUidByName(pVnode->pMeta, pTbArr[i]);
        if (suid != 0) {
          code = metaPutTbToFilterCache(pVnode->pMeta, &suid, 0);
          if (TSDB_CODE_SUCCESS != code) {
            return code;
          }
        }
      }
      *tbSize = metaSizeOfTbFilterCache(pVnode->pMeta, 0);
    }
  }

  return code;
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
    return terrno;
  }

  int32_t tbFilterSize = 0;
  int32_t code = TSDB_CODE_SUCCESS;
#ifdef TD_ENTERPRISE
  code = vnodeGetTimeSeriesBlackList(pVnode, &tbFilterSize);
  if (TSDB_CODE_SUCCESS != code) {
    goto _exit;
  }
#endif

  if ((!tbFilterSize && vnodeGetStbIdList(pVnode, 0, suidList) < 0) ||
      (tbFilterSize && vnodeGetStbIdListByFilter(pVnode, 0, suidList, vnodeTimeSeriesFilter, pVnode) < 0)) {
    qError("vgId:%d, failed to get stb id list error: %s", TD_VID(pVnode), terrstr());
    taosArrayDestroy(suidList);
    return terrno;
  }

  *num = 0;
  int64_t arrSize = taosArrayGetSize(suidList);
  for (int64_t i = 0; i < arrSize; ++i) {
    tb_uid_t suid = *(tb_uid_t *)taosArrayGet(suidList, i);

    int64_t ctbNum = 0;
    int32_t numOfCols = 0;
    code = metaGetStbStats(pVnode, suid, &ctbNum, &numOfCols);
    if (TSDB_CODE_SUCCESS != code) {
      goto _exit;
    }
    *num += ctbNum * (numOfCols - 1);
  }

_exit:
  taosArrayDestroy(suidList);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetAllCtbNum(SVnode *pVnode, int64_t *num) {
  SMStbCursor *pCur = metaOpenStbCursor(pVnode->pMeta, 0);
  if (!pCur) {
    return terrno;
  }

  *num = 0;
  while (1) {
    tb_uid_t id = metaStbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    int64_t ctbNum = 0;
    int32_t code = vnodeGetCtbNum(pVnode, id, &ctbNum);
    if (TSDB_CODE_SUCCESS != code) {
      metaCloseStbCursor(pCur);
      return code;
    }

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

int32_t vnodeGetDBSize(void *pVnode, int64_t *dataSize, int64_t *walSize, int64_t *metaSize) {
  SVnode *pVnodeObj = pVnode;
  if (pVnodeObj == NULL) {
    return TSDB_CODE_VND_NOT_EXIST;
  }
  int32_t code = 0;
  char    path[TSDB_FILENAME_LEN] = {0};

  char   *dirName[] = {VNODE_TSDB_DIR, VNODE_WAL_DIR, VNODE_META_DIR};
  int64_t dirSize[3];

  vnodeGetPrimaryDir(pVnodeObj->path, pVnodeObj->diskPrimary, pVnodeObj->pTfs, path, TSDB_FILENAME_LEN);
  int32_t offset = strlen(path);

  SDiskSize size = {0};
  for (int i = 0; i < sizeof(dirName) / sizeof(dirName[0]); i++) {
    (void)snprintf(path + offset, TSDB_FILENAME_LEN, "%s%s", TD_DIRSEP, dirName[i]);
    code = taosGetDiskSize(path, &size);
    if (code != 0) {
      return code;
    }
    path[offset] = 0;
    dirSize[i] = size.used;
    memset(&size, 0, sizeof(size));
  }

  *dataSize = dirSize[0];
  *walSize = dirSize[1];
  *metaSize = dirSize[2];
  return 0;
}

int32_t vnodeGetStreamProgress(SVnode *pVnode, SRpcMsg *pMsg, bool direct) {
  int32_t            code = 0;
  SStreamProgressReq req;
  SStreamProgressRsp rsp = {0};
  SRpcMsg            rpcMsg = {.info = pMsg->info, .code = 0};
  char              *buf = NULL;
  int32_t            rspLen = 0;
  code = tDeserializeStreamProgressReq(pMsg->pCont, pMsg->contLen, &req);

  if (code == TSDB_CODE_SUCCESS) {
    rsp.fetchIdx = req.fetchIdx;
    rsp.subFetchIdx = req.subFetchIdx;
    rsp.vgId = req.vgId;
    rsp.streamId = req.streamId;
    rspLen = tSerializeStreamProgressRsp(0, 0, &rsp);
    if (rspLen < 0) {
      code = terrno;
      goto _OVER;
    }
    if (direct) {
      buf = rpcMallocCont(rspLen);
    } else {
      buf = taosMemoryCalloc(1, rspLen);
    }
    if (!buf) {
      code = terrno;
      goto _OVER;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = tqGetStreamExecInfo(pVnode, req.streamId, &rsp.progressDelay, &rsp.fillHisFinished);
  }
  if (code == TSDB_CODE_SUCCESS) {
    rspLen = tSerializeStreamProgressRsp(buf, rspLen, &rsp);
    if (rspLen < 0) {
      code = terrno;
      goto _OVER;
    }
    rpcMsg.pCont = buf;
    buf = NULL;
    rpcMsg.contLen = rspLen;
    rpcMsg.code = code;
    rpcMsg.msgType = pMsg->msgType;
    if (direct) {
      tmsgSendRsp(&rpcMsg);
    } else {
      *pMsg = rpcMsg;
    }
  }

_OVER:
  if (buf) {
    taosMemoryFree(buf);
  }
  return code;
}
