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

int vnodeGetTableMeta(SVnode *pVnode, SRpcMsg *pMsg) {
  STableInfoReq  infoReq = {0};
  STableMetaRsp  metaRsp = {0};
  SMetaReader    mer1 = {0};
  SMetaReader    mer2 = {0};
  char           tableFName[TSDB_TABLE_FNAME_LEN];
  SRpcMsg        rpcMsg = {0};
  int32_t        code = 0;
  int32_t        rspLen = 0;
  void *         pRsp = NULL;
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

  pRsp = rpcMallocCont(rspLen);
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

  tmsgSendRsp(&rpcMsg);

  taosMemoryFree(metaRsp.pSchemas);
  metaReaderClear(&mer2);
  metaReaderClear(&mer1);
  return TSDB_CODE_SUCCESS;
}

int vnodeGetTableCfg(SVnode *pVnode, SRpcMsg *pMsg) {
  STableCfgReq   cfgReq = {0};
  STableCfgRsp   cfgRsp = {0};
  SMetaReader    mer1 = {0};
  SMetaReader    mer2 = {0};
  char           tableFName[TSDB_TABLE_FNAME_LEN];
  SRpcMsg        rpcMsg = {0};
  int32_t        code = 0;
  int32_t        rspLen = 0;
  void *         pRsp = NULL;
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

  pRsp = rpcMallocCont(rspLen);
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

  tmsgSendRsp(&rpcMsg);

  tFreeSTableCfgRsp(&cfgRsp);
  metaReaderClear(&mer2);
  metaReaderClear(&mer1);
  return TSDB_CODE_SUCCESS;
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
