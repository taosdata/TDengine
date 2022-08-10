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

#include "vnd.h"

int vnodeQueryOpen(SVnode *pVnode) {
  return qWorkerInit(NODE_TYPE_VNODE, TD_VID(pVnode), NULL, (void **)&pVnode->pQuery, &pVnode->msgCb);
}

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
    goto _exit;
  }

  metaRsp.dbId = pVnode->config.dbId;
  strcpy(metaRsp.tbName, infoReq.tbName);
  memcpy(metaRsp.dbFName, infoReq.dbFName, sizeof(metaRsp.dbFName));

  sprintf(tableFName, "%s.%s", infoReq.dbFName, infoReq.tbName);
  code = vnodeValidateTableHash(pVnode, tableFName);
  if (code) {
    goto _exit;
  }

  // query meta
  metaReaderInit(&mer1, pVnode->pMeta, 0);

  if (metaGetTableEntryByName(&mer1, infoReq.tbName) < 0) {
    code = terrno;
    goto _exit;
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
    metaReaderInit(&mer2, pVnode->pMeta, 0);
    if (metaGetTableEntryByUid(&mer2, mer1.me.ctbEntry.suid) < 0) goto _exit;

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

  taosMemoryFree(metaRsp.pSchemas);
  metaReaderClear(&mer2);
  metaReaderClear(&mer1);
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
  metaReaderInit(&mer1, pVnode->pMeta, 0);

  if (metaGetTableEntryByName(&mer1, cfgReq.tbName) < 0) {
    code = terrno;
    goto _exit;
  }

  cfgRsp.tableType = mer1.me.type;

  if (mer1.me.type == TSDB_SUPER_TABLE) {
    code = TSDB_CODE_VND_HASH_MISMATCH;
    goto _exit;
  } else if (mer1.me.type == TSDB_CHILD_TABLE) {
    metaReaderInit(&mer2, pVnode->pMeta, 0);
    if (metaGetTableEntryByUid(&mer2, mer1.me.ctbEntry.suid) < 0) goto _exit;

    strcpy(cfgRsp.stbName, mer2.me.name);
    schema = mer2.me.stbEntry.schemaRow;
    schemaTag = mer2.me.stbEntry.schemaTag;
    cfgRsp.ttl = mer1.me.ctbEntry.ttlDays;
    cfgRsp.commentLen = mer1.me.ctbEntry.commentLen;
    if (mer1.me.ctbEntry.commentLen > 0) {
      cfgRsp.pComment = strdup(mer1.me.ctbEntry.comment);
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
      cfgRsp.pComment = strdup(mer1.me.ntbEntry.comment);
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

int32_t vnodeGetBatchMeta(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t    code = 0;
  int32_t    offset = 0;
  int32_t    rspSize = 0;
  SBatchReq *batchReq = (SBatchReq *)pMsg->pCont;
  int32_t    msgNum = ntohl(batchReq->msgNum);
  offset += sizeof(SBatchReq);
  SBatchMsg req = {0};
  SBatchRsp rsp = {0};
  SRpcMsg   reqMsg = *pMsg;
  SRpcMsg   rspMsg = {0};
  void     *pRsp = NULL;

  SArray *batchRsp = taosArrayInit(msgNum, sizeof(SBatchRsp));
  if (NULL == batchRsp) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  for (int32_t i = 0; i < msgNum; ++i) {
    req.msgIdx = ntohl(*(int32_t *)((char *)pMsg->pCont + offset));
    offset += sizeof(req.msgIdx);

    req.msgType = ntohl(*(int32_t *)((char *)pMsg->pCont + offset));
    offset += sizeof(req.msgType);

    req.msgLen = ntohl(*(int32_t *)((char *)pMsg->pCont + offset));
    offset += sizeof(req.msgLen);

    req.msg = (char *)pMsg->pCont + offset;
    offset += req.msgLen;

    reqMsg.msgType = req.msgType;
    reqMsg.pCont = req.msg;
    reqMsg.contLen = req.msgLen;

    switch (req.msgType) {
      case TDMT_VND_TABLE_META:
        vnodeGetTableMeta(pVnode, &reqMsg, false);
        break;
      case TDMT_VND_TABLE_CFG:
        vnodeGetTableCfg(pVnode, &reqMsg, false);
        break;
      default:
        qError("invalid req msgType %d", req.msgType);
        reqMsg.code = TSDB_CODE_INVALID_MSG;
        reqMsg.pCont = NULL;
        reqMsg.contLen = 0;
        break;
    }

    rsp.msgIdx = req.msgIdx;
    rsp.reqType = reqMsg.msgType;
    rsp.msgLen = reqMsg.contLen;
    rsp.rspCode = reqMsg.code;
    rsp.msg = reqMsg.pCont;

    taosArrayPush(batchRsp, &rsp);

    rspSize += sizeof(rsp) + rsp.msgLen - POINTER_BYTES;
  }

  rspSize += sizeof(int32_t);
  offset = 0;

  pRsp = rpcMallocCont(rspSize);
  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  *(int32_t *)((char *)pRsp + offset) = htonl(msgNum);
  offset += sizeof(msgNum);
  for (int32_t i = 0; i < msgNum; ++i) {
    SBatchRsp *p = taosArrayGet(batchRsp, i);

    *(int32_t *)((char *)pRsp + offset) = htonl(p->reqType);
    offset += sizeof(p->reqType);
    *(int32_t *)((char *)pRsp + offset) = htonl(p->msgIdx);
    offset += sizeof(p->msgIdx);
    *(int32_t *)((char *)pRsp + offset) = htonl(p->msgLen);
    offset += sizeof(p->msgLen);
    *(int32_t *)((char *)pRsp + offset) = htonl(p->rspCode);
    offset += sizeof(p->rspCode);
    memcpy((char *)pRsp + offset, p->msg, p->msgLen);
    offset += p->msgLen;

    taosMemoryFreeClear(p->msg);
  }

  taosArrayDestroy(batchRsp);
  batchRsp = NULL;

_exit:

  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = pMsg->msgType;

  if (code) {
    qError("vnd get batch meta failed cause of %s", tstrerror(code));
  }

  taosArrayDestroyEx(batchRsp, tFreeSBatchRsp);

  tmsgSendRsp(&rspMsg);

  return code;
}

int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad) {
  pLoad->vgId = TD_VID(pVnode);
  pLoad->syncState = syncGetMyRole(pVnode->sync);
  pLoad->numOfTables = metaGetTbNum(pVnode->pMeta);
  pLoad->numOfTimeSeries = metaGetTimeSeriesNum(pVnode->pMeta);
  pLoad->totalStorage = (int64_t)3 * 1073741824;
  pLoad->compStorage = (int64_t)2 * 1073741824;
  pLoad->pointsWritten = 100;
  pLoad->numOfSelectReqs = 1;
  pLoad->numOfInsertReqs = 3;
  pLoad->numOfInsertSuccessReqs = 2;
  pLoad->numOfBatchInsertReqs = 5;
  pLoad->numOfBatchInsertSuccessReqs = 4;
  return 0;
}

void vnodeGetInfo(SVnode *pVnode, const char **dbname, int32_t *vgId) {
  if (dbname) {
    *dbname = pVnode->config.dbname;
  }

  if (vgId) {
    *vgId = TD_VID(pVnode);
  }
}

int32_t vnodeGetAllTableList(SVnode *pVnode, uint64_t uid, SArray *list) {
  SMCtbCursor *pCur = metaOpenCtbCursor(pVnode->pMeta, uid);

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

int32_t vnodeGetCtbIdList(SVnode *pVnode, int64_t suid, SArray *list) {
  SMCtbCursor *pCur = metaOpenCtbCursor(pVnode->pMeta, suid);

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

int32_t vnodeGetCtbNum(SVnode *pVnode, int64_t suid, int64_t *num) {
  SMCtbCursor *pCur = metaOpenCtbCursor(pVnode->pMeta, suid);
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

static int32_t vnodeGetStbColumnNum(SVnode *pVnode, tb_uid_t suid, int *num) {
  STSchema *pTSchema = metaGetTbTSchema(pVnode->pMeta, suid, -1);
  // metaGetTbTSchemaEx(pVnode->pMeta, suid, suid, -1, &pTSchema);

  *num = pTSchema->numOfCols;

  taosMemoryFree(pTSchema);

  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetTimeSeriesNum(SVnode *pVnode, int64_t *num) {
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
    int numOfCols = 0;
    vnodeGetStbColumnNum(pVnode, id, &numOfCols);

    *num += ctbNum * (numOfCols - 1);
  }

  metaCloseStbCursor(pCur);
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

void *vnodeGetIdx(SVnode *pVnode) {
  if (pVnode == NULL) {
    return NULL;
  }
  return metaGetIdx(pVnode->pMeta);
}

void *vnodeGetIvtIdx(SVnode *pVnode) {
  if (pVnode == NULL) {
    return NULL;
  }
  return metaGetIvtIdx(pVnode->pMeta);
}
