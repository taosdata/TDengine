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
#include "tutil.h"
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
  if (withExtSchema(tblType)) {
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

void vnodePrintTableMeta(STableMetaRsp *pMeta) {
  if (!(qDebugFlag & DEBUG_DEBUG)) {
    return;
  }

  qDebug("tbName:%s", pMeta->tbName);
  qDebug("stbName:%s", pMeta->stbName);
  qDebug("dbFName:%s", pMeta->dbFName);
  qDebug("dbId:%" PRId64, pMeta->dbId);
  qDebug("numOfTags:%d", pMeta->numOfTags);
  qDebug("numOfColumns:%d", pMeta->numOfColumns);
  qDebug("precision:%d", pMeta->precision);
  qDebug("tableType:%d", pMeta->tableType);
  qDebug("sversion:%d", pMeta->sversion);
  qDebug("tversion:%d", pMeta->tversion);
  qDebug("suid:%" PRIu64, pMeta->suid);
  qDebug("tuid:%" PRIu64, pMeta->tuid);
  qDebug("vgId:%d", pMeta->vgId);
  qDebug("sysInfo:%d", pMeta->sysInfo);
  if (pMeta->pSchemas) {
    for (int32_t i = 0; i < (pMeta->numOfColumns + pMeta->numOfTags); ++i) {
      SSchema *pSchema = pMeta->pSchemas + i;
      qDebug("%d col/tag: type:%d, flags:%d, colId:%d, bytes:%d, name:%s", i, pSchema->type, pSchema->flags,
             pSchema->colId, pSchema->bytes, pSchema->name);
    }
  }
}

int32_t fillTableColRef(SMetaReader *reader, SColRef *pRef, int32_t numOfCol) {
  int8_t tblType = reader->me.type;
  if (hasRefCol(tblType)) {
    SColRefWrapper *p = &(reader->me.colRef);
    if (numOfCol != p->nCols) {
      vError("fillTableColRef table type:%d, col num:%d, col cmpr num:%d mismatch", tblType, numOfCol, p->nCols);
      return TSDB_CODE_APP_ERROR;
    }
    for (int i = 0; i < p->nCols; i++) {
      SColRef *pColRef = &p->pColRef[i];
      pRef[i].hasRef = pColRef->hasRef;
      pRef[i].id = pColRef->id;
      if(pRef[i].hasRef) {
        tstrncpy(pRef[i].refDbName, pColRef->refDbName, TSDB_DB_NAME_LEN);
        tstrncpy(pRef[i].refTableName, pColRef->refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(pRef[i].refColName, pColRef->refColName, TSDB_COL_NAME_LEN);
      }
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
  uint8_t        autoCreateCtb = 0;

  // decode req
  if (tDeserializeSTableInfoReq(pMsg->pCont, pMsg->contLen, &infoReq) != 0) {
    code = terrno;
    goto _exit4;
  }
  autoCreateCtb = infoReq.autoCreateCtb;

  if (infoReq.option == REQ_OPT_TBUID) reqTbUid = true;
  metaRsp.dbId = pVnode->config.dbId;
  tstrncpy(metaRsp.tbName, infoReq.tbName, TSDB_TABLE_NAME_LEN);
  (void)memcpy(metaRsp.dbFName, infoReq.dbFName, sizeof(metaRsp.dbFName));

  if (!reqTbUid) {
    (void)tsnprintf(tableFName, TSDB_TABLE_FNAME_LEN, "%s.%s", infoReq.dbFName, infoReq.tbName);
    if (pVnode->mounted) tTrimMountPrefix(tableFName);
    code = vnodeValidateTableHash(pVnode, tableFName);
    if (code) {
      goto _exit4;
    }
  }

  // query meta
  metaReaderDoInit(&mer1, pVnode->pMeta, META_READER_LOCK);
  if (reqTbUid) {
    SET_ERRNO(0);
    uint64_t tbUid = taosStr2UInt64(infoReq.tbName, NULL, 10);
    if (ERRNO == ERANGE || tbUid == 0) {
      code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
      goto _exit3;
    }
    SMetaReader mr3 = {0};
    metaReaderDoInit(&mr3, ((SVnode *)pVnode)->pMeta, META_READER_NOLOCK);
    if ((code = metaReaderGetTableEntryByUid(&mr3, tbUid)) < 0) {
      metaReaderClear(&mr3);
      TAOS_CHECK_GOTO(code, NULL, _exit3);
    }
    tstrncpy(metaRsp.tbName, mr3.me.name, TSDB_TABLE_NAME_LEN);
    metaReaderClear(&mr3);
    TAOS_CHECK_GOTO(metaGetTableEntryByName(&mer1, metaRsp.tbName), NULL, _exit3);
  } else if (metaGetTableEntryByName(&mer1, infoReq.tbName) < 0) {
    code = terrno;
    goto _exit3;
  }

  metaRsp.tableType = mer1.me.type;
  metaRsp.vgId = TD_VID(pVnode);
  metaRsp.tuid = mer1.me.uid;

  switch (mer1.me.type) {
    case TSDB_SUPER_TABLE: {
      (void)strcpy(metaRsp.stbName, mer1.me.name);
      schema = mer1.me.stbEntry.schemaRow;
      schemaTag = mer1.me.stbEntry.schemaTag;
      metaRsp.suid = mer1.me.uid;
      break;
    }
    case TSDB_CHILD_TABLE:
    case TSDB_VIRTUAL_CHILD_TABLE:{
      metaReaderDoInit(&mer2, pVnode->pMeta, META_READER_NOLOCK);
      if (metaReaderGetTableEntryByUid(&mer2, mer1.me.ctbEntry.suid) < 0) goto _exit2;

      (void)strcpy(metaRsp.stbName, mer2.me.name);
      metaRsp.suid = mer2.me.uid;
      schema = mer2.me.stbEntry.schemaRow;
      schemaTag = mer2.me.stbEntry.schemaTag;
      break;
    }
    case TSDB_NORMAL_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {
      schema = mer1.me.ntbEntry.schemaRow;
      break;
    }
    default: {
      vError("vnodeGetTableMeta get invalid table type:%d", mer1.me.type);
      goto _exit3;
    }
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
    for (int32_t i = 0; i < metaRsp.numOfColumns && pReader->me.pExtSchemas; i++) {
      metaRsp.pSchemaExt[i].typeMod = pReader->me.pExtSchemas[i].typeMod;
    }
  } else {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  if (hasRefCol(mer1.me.type)) {
    metaRsp.rversion = mer1.me.colRef.version;
    metaRsp.pColRefs = (SColRef*)taosMemoryMalloc(sizeof(SColRef) * metaRsp.numOfColumns);
    if (metaRsp.pColRefs) {
      code = fillTableColRef(&mer1, metaRsp.pColRefs, metaRsp.numOfColumns);
      if (code < 0) {
        goto _exit;
      }
    }
    metaRsp.numOfColRefs = metaRsp.numOfColumns;
  } else {
    metaRsp.pColRefs = NULL;
    metaRsp.numOfColRefs = 0;
  }

  vnodePrintTableMeta(&metaRsp);

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
  taosMemoryFree(metaRsp.pColRefs);
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

  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST && autoCreateCtb == 1) {
    code = TSDB_CODE_SUCCESS;
  }

  if (code) {
    qError("vgId:%d, get table %s meta with %" PRIu8 " failed cause of %s", pVnode->config.vgId, infoReq.tbName,
           infoReq.option, tstrerror(code));
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

  tstrncpy(cfgRsp.tbName, cfgReq.tbName, TSDB_TABLE_NAME_LEN);
  (void)memcpy(cfgRsp.dbFName, cfgReq.dbFName, sizeof(cfgRsp.dbFName));

  (void)tsnprintf(tableFName, TSDB_TABLE_FNAME_LEN, "%s.%s", cfgReq.dbFName, cfgReq.tbName);
  if (pVnode->mounted) tTrimMountPrefix(tableFName);
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
  } else if (mer1.me.type == TSDB_CHILD_TABLE || mer1.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
    metaReaderDoInit(&mer2, pVnode->pMeta, META_READER_NOLOCK);
    if (metaReaderGetTableEntryByUid(&mer2, mer1.me.ctbEntry.suid) < 0) goto _exit;

    tstrncpy(cfgRsp.stbName, mer2.me.name, TSDB_TABLE_NAME_LEN);
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
  } else if (mer1.me.type == TSDB_NORMAL_TABLE || mer1.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
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
    code = TSDB_CODE_APP_ERROR;
    goto _exit;
  }

  cfgRsp.numOfTags = schemaTag.nCols;
  cfgRsp.numOfColumns = schema.nCols;
  cfgRsp.virtualStb = false; // vnode don't have super table, so it's always false
  cfgRsp.pSchemas = (SSchema *)taosMemoryMalloc(sizeof(SSchema) * (cfgRsp.numOfColumns + cfgRsp.numOfTags));
  cfgRsp.pSchemaExt = (SSchemaExt *)taosMemoryMalloc(cfgRsp.numOfColumns * sizeof(SSchemaExt));
  cfgRsp.pColRefs = (SColRef *)taosMemoryMalloc(sizeof(SColRef) * cfgRsp.numOfColumns);

  if (NULL == cfgRsp.pSchemas || NULL == cfgRsp.pSchemaExt || NULL == cfgRsp.pColRefs) {
    code = terrno;
    goto _exit;
  }
  (void)memcpy(cfgRsp.pSchemas, schema.pSchema, sizeof(SSchema) * schema.nCols);
  if (schemaTag.nCols) {
    (void)memcpy(cfgRsp.pSchemas + schema.nCols, schemaTag.pSchema, sizeof(SSchema) * schemaTag.nCols);
  }

  SMetaReader     *pReader = (mer1.me.type == TSDB_CHILD_TABLE || mer1.me.type == TSDB_VIRTUAL_CHILD_TABLE) ? &mer2 : &mer1;
  SColCmprWrapper *pColCmpr = &pReader->me.colCmpr;
  SColRefWrapper  *pColRef = &mer1.me.colRef;

  if (withExtSchema(cfgRsp.tableType)) {
    for (int32_t i = 0; i < cfgRsp.numOfColumns; i++) {
      SColCmpr   *pCmpr = &pColCmpr->pColCmpr[i];
      SSchemaExt *pSchExt = cfgRsp.pSchemaExt + i;
      pSchExt->colId = pCmpr->id;
      pSchExt->compress = pCmpr->alg;
      if (pReader->me.pExtSchemas)
        pSchExt->typeMod = pReader->me.pExtSchemas[i].typeMod;
      else
        pSchExt->typeMod = 0;
    }
  }

  cfgRsp.virtualStb = false;
  if (hasRefCol(cfgRsp.tableType)) {
    for (int32_t i = 0; i < cfgRsp.numOfColumns; i++) {
      SColRef *pRef = &pColRef->pColRef[i];
      cfgRsp.pColRefs[i].hasRef = pRef->hasRef;
      cfgRsp.pColRefs[i].id = pRef->id;
      if (cfgRsp.pColRefs[i].hasRef) {
        tstrncpy(cfgRsp.pColRefs[i].refDbName, pRef->refDbName, TSDB_DB_NAME_LEN);
        tstrncpy(cfgRsp.pColRefs[i].refTableName, pRef->refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(cfgRsp.pColRefs[i].refColName, pRef->refColName, TSDB_COL_NAME_LEN);
      }
    }
  }

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
      case TDMT_VND_VSUBTABLES_META:
        // error code has been set into reqMsg, no need to handle it here.
        if (TSDB_CODE_SUCCESS != vnodeGetVSubtablesMeta(pVnode, &reqMsg)) {
          qWarn("vnodeGetVSubtablesMeta failed, msgType:%d", req->msgType);
        }
        break;
      case TDMT_VND_VSTB_REF_DBS:
        // error code has been set into reqMsg, no need to handle it here.
        if (TSDB_CODE_SUCCESS != vnodeGetVStbRefDbs(pVnode, &reqMsg)) {
          qWarn("vnodeGetVStbRefDbs failed, msgType:%d", req->msgType);
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

#define VNODE_DO_META_QUERY(pVnode, cmd)                 \
  do {                                                   \
    (void)taosThreadRwlockRdlock(&(pVnode)->metaRWLock); \
    cmd;                                                 \
    (void)taosThreadRwlockUnlock(&(pVnode)->metaRWLock); \
  } while (0)

int32_t vnodeReadVSubtables(SReadHandle* pHandle, int64_t suid, SArray** ppRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SMetaReader                mr = {0};
  bool                       readerInit = false;
  SVCTableRefCols*           pTb = NULL;
  int32_t                    refColsNum = 0;
  char                       tbFName[TSDB_TABLE_FNAME_LEN];
  SSHashObj*                 pSrcTbls = NULL;

  SArray *pList = taosArrayInit(10, sizeof(uint64_t));
  QUERY_CHECK_NULL(pList, code, line, _return, terrno);

  QUERY_CHECK_CODE(pHandle->api.metaFn.getChildTableList(pHandle->vnode, suid, pList), line, _return);

  size_t num = taosArrayGetSize(pList);
  *ppRes = taosArrayInit(num, POINTER_BYTES);
  QUERY_CHECK_NULL(*ppRes, code, line, _return, terrno);
  pSrcTbls = tSimpleHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  QUERY_CHECK_NULL(pSrcTbls, code, line, _return, terrno);

  for (int32_t i = 0; i < num; ++i) {
    uint64_t* id = taosArrayGet(pList, i);
    QUERY_CHECK_NULL(id, code, line, _return, terrno);
    pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, META_READER_LOCK, &pHandle->api.metaFn);
    QUERY_CHECK_CODE(pHandle->api.metaReaderFn.getTableEntryByUid(&mr, *id), line, _return);
    readerInit = true;

    refColsNum = 0;
    for (int32_t j = 0; j < mr.me.colRef.nCols; j++) {
      if (mr.me.colRef.pColRef[j].hasRef) {
        refColsNum++;
      }
    }

    if (refColsNum <= 0) {
      pHandle->api.metaReaderFn.clearReader(&mr);
      readerInit = false;
      continue;
    }

    pTb = taosMemoryCalloc(1, refColsNum * sizeof(SRefColInfo) + sizeof(*pTb));
    QUERY_CHECK_NULL(pTb, code, line, _return, terrno);

    pTb->uid = mr.me.uid;
    pTb->numOfColRefs = refColsNum;
    pTb->refCols = (SRefColInfo*)(pTb + 1);

    refColsNum = 0;
    tSimpleHashClear(pSrcTbls);
    for (int32_t j = 0; j < mr.me.colRef.nCols; j++) {
      if (!mr.me.colRef.pColRef[j].hasRef) {
        continue;
      }

      pTb->refCols[refColsNum].colId = mr.me.colRef.pColRef[j].id;
      tstrncpy(pTb->refCols[refColsNum].refColName, mr.me.colRef.pColRef[j].refColName, TSDB_COL_NAME_LEN);
      tstrncpy(pTb->refCols[refColsNum].refTableName, mr.me.colRef.pColRef[j].refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pTb->refCols[refColsNum].refDbName, mr.me.colRef.pColRef[j].refDbName, TSDB_DB_NAME_LEN);

      snprintf(tbFName, sizeof(tbFName), "%s.%s", pTb->refCols[refColsNum].refDbName, pTb->refCols[refColsNum].refTableName);

      if (NULL == tSimpleHashGet(pSrcTbls, tbFName, strlen(tbFName))) {
        QUERY_CHECK_CODE(tSimpleHashPut(pSrcTbls, tbFName, strlen(tbFName), &code, sizeof(code)), line, _return);
      }

      refColsNum++;
    }

    pTb->numOfSrcTbls = tSimpleHashGetSize(pSrcTbls);
    QUERY_CHECK_NULL(taosArrayPush(*ppRes, &pTb), code, line, _return, terrno);
    pTb = NULL;

    pHandle->api.metaReaderFn.clearReader(&mr);
    readerInit = false;
  }

_return:

  if (readerInit) {
    pHandle->api.metaReaderFn.clearReader(&mr);
  }

  taosArrayDestroy(pList);
  taosMemoryFree(pTb);
  tSimpleHashCleanup(pSrcTbls);

  if (code) {
    qError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

int32_t vnodeReadVStbRefDbs(SReadHandle* pHandle, int64_t suid, SArray** ppRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    line = 0;
  SMetaReader                mr = {0};
  bool                       readerInit = false;
  SSHashObj*                 pDbNameHash = NULL;
  SArray*                    pList = NULL;

  pList = taosArrayInit(10, sizeof(uint64_t));
  QUERY_CHECK_NULL(pList, code, line, _return, terrno);

  *ppRes = taosArrayInit(10, POINTER_BYTES);
  QUERY_CHECK_NULL(*ppRes, code, line, _return, terrno)
  
  // lookup in cache
  code = pHandle->api.metaFn.metaGetCachedRefDbs(pHandle->vnode, suid, *ppRes);
  QUERY_CHECK_CODE(code, line, _return);

  if (taosArrayGetSize(*ppRes) > 0) {
    // found in cache
    goto _return;
  } else {
    code = pHandle->api.metaFn.getChildTableList(pHandle->vnode, suid, pList);
    QUERY_CHECK_CODE(code, line, _return);

    size_t num = taosArrayGetSize(pList);
    pDbNameHash = tSimpleHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    QUERY_CHECK_NULL(pDbNameHash, code, line, _return, terrno);

    for (int32_t i = 0; i < num; ++i) {
      uint64_t* id = taosArrayGet(pList, i);
      QUERY_CHECK_NULL(id, code, line, _return, terrno);

      pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, META_READER_LOCK, &pHandle->api.metaFn);
      readerInit = true;

      code = pHandle->api.metaReaderFn.getTableEntryByUid(&mr, *id);
      QUERY_CHECK_CODE(code, line, _return);

      for (int32_t j = 0; j < mr.me.colRef.nCols; j++) {
        if (mr.me.colRef.pColRef[j].hasRef) {
          if (NULL == tSimpleHashGet(pDbNameHash, mr.me.colRef.pColRef[j].refDbName, strlen(mr.me.colRef.pColRef[j].refDbName))) {
            char *refDbName = taosStrdup(mr.me.colRef.pColRef[j].refDbName);
            QUERY_CHECK_NULL(refDbName, code, line, _return, terrno);

            QUERY_CHECK_NULL(taosArrayPush(*ppRes, &refDbName), code, line, _return, terrno);

            code = tSimpleHashPut(pDbNameHash, refDbName, strlen(refDbName), NULL, 0);
            QUERY_CHECK_CODE(code, line, _return);
          }
        }
      }

      pHandle->api.metaReaderFn.clearReader(&mr);
      readerInit = false;
    }

    code = pHandle->api.metaFn.metaPutRefDbsToCache(pHandle->vnode, suid, *ppRes);
    QUERY_CHECK_CODE(code, line, _return);
  }

_return:

  if (readerInit) {
    pHandle->api.metaReaderFn.clearReader(&mr);
  }

  taosArrayDestroy(pList);
  tSimpleHashCleanup(pDbNameHash);

  if (code) {
    qError("%s failed since %s", __func__, tstrerror(code));
  }
  return code;
}

int32_t vnodeGetVSubtablesMeta(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t        code = 0;
  int32_t        rspSize = 0;
  SVSubTablesReq req = {0};
  SVSubTablesRsp rsp = {0};
  SRpcMsg      rspMsg = {0};
  void        *pRsp = NULL;
  int32_t      line = 0;

  if (tDeserializeSVSubTablesReq(pMsg->pCont, pMsg->contLen, &req)) {
    code = terrno;
    qError("tDeserializeSVSubTablesReq failed");
    goto _return;
  }

  SReadHandle handle = {0};
  handle.vnode = pVnode;
  initStorageAPI(&handle.api);

  QUERY_CHECK_CODE(vnodeReadVSubtables(&handle, req.suid, &rsp.pTables), line, _return);
  rsp.vgId = TD_VID(pVnode);

  rspSize = tSerializeSVSubTablesRsp(NULL, 0, &rsp);
  if (rspSize < 0) {
    code = rspSize;
    qError("tSerializeSVSubTablesRsp failed, error:%d", rspSize);
    goto _return;
  }
  pRsp = taosMemoryCalloc(1, rspSize);
  if (pRsp == NULL) {
    code = terrno;
    qError("rpcMallocCont %d failed, error:%d", rspSize, terrno);
    goto _return;
  }
  rspSize = tSerializeSVSubTablesRsp(pRsp, rspSize, &rsp);
  if (rspSize < 0) {
    code = rspSize;
    qError("tSerializeSVSubTablesRsp failed, error:%d", rspSize);
    goto _return;
  }

_return:

  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = pMsg->msgType;

  if (code) {
    qError("vnd get virtual subtables failed cause of %s", tstrerror(code));
  }

  *pMsg = rspMsg;
  
  tDestroySVSubTablesRsp(&rsp);

  //tmsgSendRsp(&rspMsg);

  return code;
}

int32_t vnodeGetVStbRefDbs(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t        code = 0;
  int32_t        rspSize = 0;
  SVStbRefDbsReq req = {0};
  SVStbRefDbsRsp rsp = {0};
  SRpcMsg        rspMsg = {0};
  void          *pRsp = NULL;
  int32_t        line = 0;

  if (tDeserializeSVStbRefDbsReq(pMsg->pCont, pMsg->contLen, &req)) {
    code = terrno;
    qError("tDeserializeSVSubTablesReq failed");
    goto _return;
  }

  SReadHandle handle = {0};
  handle.vnode = pVnode;
  initStorageAPI(&handle.api);

  code = vnodeReadVStbRefDbs(&handle, req.suid, &rsp.pDbs);
  QUERY_CHECK_CODE(code, line, _return);
  rsp.vgId = TD_VID(pVnode);

  rspSize = tSerializeSVStbRefDbsRsp(NULL, 0, &rsp);
  if (rspSize < 0) {
    code = rspSize;
    qError("tSerializeSVStbRefDbsRsp failed, error:%d", rspSize);
    goto _return;
  }
  pRsp = taosMemoryCalloc(1, rspSize);
  if (pRsp == NULL) {
    code = terrno;
    qError("rpcMallocCont %d failed, error:%d", rspSize, terrno);
    goto _return;
  }
  rspSize = tSerializeSVStbRefDbsRsp(pRsp, rspSize, &rsp);
  if (rspSize < 0) {
    code = rspSize;
    qError("tSerializeSVStbRefDbsRsp failed, error:%d", rspSize);
    goto _return;
  }

_return:

  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = pMsg->msgType;

  if (code) {
    qError("vnd get virtual stb ref db failed cause of %s", tstrerror(code));
  }

  *pMsg = rspMsg;

  tDestroySVStbRefDbsRsp(&rsp);

  return code;
}

static int32_t vnodeGetCompStorage(SVnode *pVnode, int64_t *output) {
  int32_t code = 0;
#ifdef TD_ENTERPRISE
  int32_t now = taosGetTimestampSec();
  if (llabs(now - pVnode->config.vndStats.storageLastUpd) >= 30) {
    pVnode->config.vndStats.storageLastUpd = now;

    SDbSizeStatisInfo info = {0};
    if (0 == (code = vnodeGetDBSize(pVnode, &info))) {
      int64_t compSize =
          info.l1Size + info.l2Size + info.l3Size + info.cacheSize + info.walSize + info.metaSize + +info.ssSize;
      if (compSize >= 0) {
        pVnode->config.vndStats.compStorage = compSize;
      } else {
        vError("vnode get comp storage failed since compSize is negative:%" PRIi64, compSize);
        code = TSDB_CODE_APP_ERROR;
      }
    } else {
      vWarn("vnode get comp storage failed since %s", tstrerror(code));
    }
  }
  if (output) *output = pVnode->config.vndStats.compStorage;
#endif
  return code;
}

static void vnodeGetBufferInfo(SVnode *pVnode, int64_t *bufferSegmentUsed, int64_t *bufferSegmentSize) {
  *bufferSegmentUsed = 0;
  *bufferSegmentSize = 0;
  if (pVnode) {
    (void)taosThreadMutexLock(&pVnode->mutex);

    if (pVnode->inUse) {
      *bufferSegmentUsed = pVnode->inUse->size;
    }
    *bufferSegmentSize = pVnode->config.szBuf / VNODE_BUFPOOL_SEGMENTS;

    (void)taosThreadMutexUnlock(&pVnode->mutex);
  }
}

int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad) {
  SSyncState state = syncGetState(pVnode->sync);
  pLoad->syncAppliedIndex = pVnode->state.applied;
  syncGetCommitIndex(pVnode->sync, &pLoad->syncCommitIndex);

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
  VNODE_DO_META_QUERY(pVnode, pLoad->numOfTables = metaGetTbNum(pVnode->pMeta));
  VNODE_DO_META_QUERY(pVnode, pLoad->numOfTimeSeries = metaGetTimeSeriesNum(pVnode->pMeta, 1));
  pLoad->totalStorage = (int64_t)3 * 1073741824;  // TODO
  (void)vnodeGetCompStorage(pVnode, &pLoad->compStorage);
  pLoad->pointsWritten = 100;
  pLoad->numOfSelectReqs = 1;
  pLoad->numOfInsertReqs = atomic_load_64(&pVnode->statis.nInsert);
  pLoad->numOfInsertSuccessReqs = atomic_load_64(&pVnode->statis.nInsertSuccess);
  pLoad->numOfBatchInsertReqs = atomic_load_64(&pVnode->statis.nBatchInsert);
  pLoad->numOfBatchInsertSuccessReqs = atomic_load_64(&pVnode->statis.nBatchInsertSuccess);
  vnodeGetBufferInfo(pVnode, &pLoad->bufferSegmentUsed, &pLoad->bufferSegmentSize);
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
    *numOfTables = pConf->vndStats.numOfNTables + pConf->vndStats.numOfCTables +
                   pConf->vndStats.numOfVTables + pConf->vndStats.numOfVCTables;
  }

  if (numOfNormalTables) {
    *numOfNormalTables = pConf->vndStats.numOfNTables +
                         pConf->vndStats.numOfVTables;
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
    qTrace("vnodeGetCtbIdList: got ctb id %" PRId64 " for suid %" PRId64, id, suid);
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
  SSchemaWrapper *pSW = metaGetTableSchema(pVnode->pMeta, suid, -1, 0, NULL, 0);
  if (pSW) {
    *num = pSW->nCols;
    tDeleteSchemaWrapper(pSW);
  } else {
    *num = 2;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetStbInfo(SVnode *pVnode, tb_uid_t suid, int64_t *keep, int8_t *flags) {
  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_NOLOCK);

  int32_t code = metaReaderGetTableEntryByUid(&mr, suid);
  if (code == TSDB_CODE_SUCCESS) {
    if (keep) *keep = mr.me.stbEntry.keep;
    if (flags) *flags = mr.me.flags;
  } else {
    if (keep) *keep = 0;
    if (flags) *flags = 0;
  }

  metaReaderClear(&mr);
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
    int8_t  flags = 0;
    code = metaGetStbStats(pVnode, suid, &ctbNum, &numOfCols, &flags);
    if (TSDB_CODE_SUCCESS != code) {
      goto _exit;
    }
    if (!TABLE_IS_VIRTUAL(flags)) {
      *num += ctbNum * (numOfCols - 1);
    }
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

int32_t vnodeGetTableSchema(void *pVnode, int64_t uid, STSchema **pSchema, int64_t *suid, SSchemaWrapper **pTagSchema) {
  return tsdbGetTableSchema(((SVnode *)pVnode)->pMeta, uid, pSchema, suid, pTagSchema);
}

static FORCE_INLINE int32_t vnodeGetDBPrimaryInfo(SVnode *pVnode, SDbSizeStatisInfo *pInfo) {
  int32_t code = 0;
  char    path[TSDB_FILENAME_LEN] = {0};

  char   *dirName[] = {VNODE_TSDB_DIR, VNODE_WAL_DIR, VNODE_META_DIR, VNODE_TSDB_CACHE_DIR};
  int64_t dirSize[4];

  vnodeGetPrimaryPath(pVnode, false, path, TSDB_FILENAME_LEN);
  int32_t offset = strlen(path);

  for (int i = 0; i < sizeof(dirName) / sizeof(dirName[0]); i++) {
    int64_t size = {0};
    (void)snprintf(path + offset, TSDB_FILENAME_LEN - offset, "%s%s", TD_DIRSEP, dirName[i]);
    code = taosGetDirSize(path, &size);
    if (code != 0) {
      uWarn("vnode %d get dir %s %s size failed since %s", TD_VID(pVnode), path, dirName[i], tstrerror(code));
    }
    path[offset] = 0;
    dirSize[i] = size;
  }

  pInfo->l1Size = 0;
  pInfo->walSize = dirSize[1];
  pInfo->metaSize = dirSize[2];
  pInfo->cacheSize = dirSize[3];
  return code;
}
int32_t vnodeGetDBSize(void *pVnode, SDbSizeStatisInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  SVnode *pVnodeObj = pVnode;
  if (pVnodeObj == NULL) {
    return TSDB_CODE_VND_NOT_EXIST;
  }
  code = vnodeGetDBPrimaryInfo(pVnode, pInfo);
  if (code != 0) goto _exit;

  code = tsdbGetFsSize(pVnodeObj->pTsdb, pInfo);
_exit:
  return code;
}

/*
 * Get raw write metrics for a vnode
 */
int32_t vnodeGetRawWriteMetrics(void *pVnode, SRawWriteMetrics *pRawMetrics) {
  if (pVnode == NULL || pRawMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SVnode      *pVnode1 = (SVnode *)pVnode;
  SSyncMetrics syncMetrics = syncGetMetrics(pVnode1->sync);

  // Copy values following SRawWriteMetrics structure order
  pRawMetrics->total_requests = atomic_load_64(&pVnode1->writeMetrics.total_requests);
  pRawMetrics->total_rows = atomic_load_64(&pVnode1->writeMetrics.total_rows);
  pRawMetrics->total_bytes = atomic_load_64(&pVnode1->writeMetrics.total_bytes);
  pRawMetrics->fetch_batch_meta_time = atomic_load_64(&pVnode1->writeMetrics.fetch_batch_meta_time);
  pRawMetrics->fetch_batch_meta_count = atomic_load_64(&pVnode1->writeMetrics.fetch_batch_meta_count);
  pRawMetrics->preprocess_time = atomic_load_64(&pVnode1->writeMetrics.preprocess_time);
  pRawMetrics->wal_write_bytes = atomic_load_64(&syncMetrics.wal_write_bytes);
  pRawMetrics->wal_write_time = atomic_load_64(&syncMetrics.wal_write_time);
  pRawMetrics->apply_bytes = atomic_load_64(&pVnode1->writeMetrics.apply_bytes);
  pRawMetrics->apply_time = atomic_load_64(&pVnode1->writeMetrics.apply_time);
  pRawMetrics->commit_count = atomic_load_64(&pVnode1->writeMetrics.commit_count);
  pRawMetrics->commit_time = atomic_load_64(&pVnode1->writeMetrics.commit_time);
  pRawMetrics->memtable_wait_time = atomic_load_64(&pVnode1->writeMetrics.memtable_wait_time);
  pRawMetrics->blocked_commit_count = atomic_load_64(&pVnode1->writeMetrics.blocked_commit_count);
  pRawMetrics->blocked_commit_time = atomic_load_64(&pVnode1->writeMetrics.block_commit_time);
  pRawMetrics->merge_count = atomic_load_64(&pVnode1->writeMetrics.merge_count);
  pRawMetrics->merge_time = atomic_load_64(&pVnode1->writeMetrics.merge_time);
  pRawMetrics->last_cache_commit_time = atomic_load_64(&pVnode1->writeMetrics.last_cache_commit_time);
  pRawMetrics->last_cache_commit_count = atomic_load_64(&pVnode1->writeMetrics.last_cache_commit_count);

  return 0;
}

/*
 * Reset raw write metrics for a vnode by subtracting old values
 */
int32_t vnodeResetRawWriteMetrics(void *pVnode, const SRawWriteMetrics *pOldMetrics) {
  if (pVnode == NULL || pOldMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SVnode *pVnode1 = (SVnode *)pVnode;

  // Reset vnode write metrics using atomic operations to subtract old values
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.total_requests, pOldMetrics->total_requests);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.total_rows, pOldMetrics->total_rows);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.total_bytes, pOldMetrics->total_bytes);

  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.fetch_batch_meta_time, pOldMetrics->fetch_batch_meta_time);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.fetch_batch_meta_count, pOldMetrics->fetch_batch_meta_count);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.preprocess_time, pOldMetrics->preprocess_time);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.apply_bytes, pOldMetrics->apply_bytes);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.apply_time, pOldMetrics->apply_time);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.commit_count, pOldMetrics->commit_count);

  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.commit_time, pOldMetrics->commit_time);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.merge_time, pOldMetrics->merge_time);

  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.memtable_wait_time, pOldMetrics->memtable_wait_time);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.blocked_commit_count, pOldMetrics->blocked_commit_count);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.block_commit_time, pOldMetrics->blocked_commit_time);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.merge_count, pOldMetrics->merge_count);

  // Reset new cache metrics
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.last_cache_commit_time, pOldMetrics->last_cache_commit_time);
  (void)atomic_sub_fetch_64(&pVnode1->writeMetrics.last_cache_commit_count, pOldMetrics->last_cache_commit_count);

  // Reset sync metrics
  SSyncMetrics syncMetrics = {
      .wal_write_bytes = pOldMetrics->wal_write_bytes,
      .wal_write_time = pOldMetrics->wal_write_time,
  };
  syncResetMetrics(pVnode1->sync, &syncMetrics);

  return 0;
}
