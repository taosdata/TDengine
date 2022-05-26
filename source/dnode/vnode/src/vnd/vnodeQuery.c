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
  SRpcMsg        rpcMsg;
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

  tmsgSendRsp(&rpcMsg);

  taosMemoryFree(metaRsp.pSchemas);
  metaReaderClear(&mer2);
  metaReaderClear(&mer1);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad) {
  pLoad->vgId = TD_VID(pVnode);
  pLoad->syncState = syncGetMyRole(pVnode->sync);
  pLoad->numOfTables = metaGetTbNum(pVnode->pMeta);
  pLoad->numOfTimeSeries = 400;
  pLoad->totalStorage = 300;
  pLoad->compStorage = 200;
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

// wrapper of tsdb read interface
tsdbReaderT tsdbQueryCacheLast(SVnode *pVnode, SQueryTableDataCond *pCond, STableGroupInfo *groupList, uint64_t qId,
                               void *pMemRef) {
#if 0
  return tsdbQueryCacheLastT(pVnode->pTsdb, pCond, groupList, qId, pMemRef);
#endif
  return 0;
}
int32_t tsdbGetTableGroupFromIdList(SVnode *pVnode, SArray *pTableIdList, STableGroupInfo *pGroupInfo) {
#if 0
  return tsdbGetTableGroupFromIdListT(pVnode->pTsdb, pTableIdList, pGroupInfo);
#endif
  return 0;
}