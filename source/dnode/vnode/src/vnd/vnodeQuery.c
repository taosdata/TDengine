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

#include "vnodeInt.h"

int vnodeQueryOpen(SVnode *pVnode) {
  return qWorkerInit(NODE_TYPE_VNODE, TD_VID(pVnode), NULL, (void **)&pVnode->pQuery, &pVnode->msgCb);
}

void vnodeQueryClose(SVnode *pVnode) { qWorkerDestroy((void **)&pVnode->pQuery); }

int vnodeGetTableMeta(SVnode *pVnode, SRpcMsg *pMsg) {
  STableInfoReq    infoReq = {0};
  STableMetaRsp    metaRsp = {0};
  SMetaEntryReader meReader1 = {0};
  SMetaEntryReader meReader2 = {0};
  char             tableFName[TSDB_TABLE_FNAME_LEN];
  SRpcMsg          rpcMsg;
  int32_t          code = 0;
  int32_t          rspLen = 0;
  void            *pRsp = NULL;

  // decode req
  if (tDeserializeSTableInfoReq(pMsg->pCont, pMsg->contLen, &infoReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  strcpy(metaRsp.tbName, infoReq.tbName);
  memcpy(metaRsp.dbFName, infoReq.dbFName, sizeof(metaRsp.dbFName));
  metaRsp.dbId = pVnode->config.dbId;
  sprintf(tableFName, "%s.%s", infoReq.dbFName, infoReq.tbName);
  code = vnodeValidateTableHash(&pVnode->config, tableFName);
  if (code) {
    goto _exit;
  }

  // query meta
  metaEntryReaderInit(&meReader1);

  if (metaGetTableEntryByName(pVnode->pMeta, &meReader1, infoReq.tbName) < 0) {
    goto _exit;
  }

  if (meReader1.me.type == TSDB_CHILD_TABLE) {
    metaEntryReaderInit(&meReader2);
    if (metaGetTableEntryByUid(pVnode->pMeta, &meReader2, meReader1.me.ctbEntry.suid) < 0) goto _exit;
  }

  // fill response
  metaRsp.tableType = meReader1.me.type;
  metaRsp.vgId = TD_VID(pVnode);
  metaRsp.tuid = meReader1.me.uid;
  if (meReader1.me.type == TSDB_SUPER_TABLE) {
    strcpy(metaRsp.stbName, meReader1.me.name);
    metaRsp.numOfTags = meReader1.me.stbEntry.nTags;
    metaRsp.numOfColumns = meReader1.me.stbEntry.nCols;
    metaRsp.suid = meReader1.me.uid;
    metaRsp.pSchemas = taosMemoryMalloc((metaRsp.numOfTags + metaRsp.numOfColumns) * sizeof(SSchema));
    if (metaRsp.pSchemas == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    memcpy(metaRsp.pSchemas, meReader1.me.stbEntry.pSchema, sizeof(SSchema) * metaRsp.numOfColumns);
    memcpy(metaRsp.pSchemas + metaRsp.numOfColumns, meReader1.me.stbEntry.pSchemaTg,
           sizeof(SSchema) * metaRsp.numOfTags);
  } else if (meReader1.me.type == TSDB_CHILD_TABLE) {
    strcpy(metaRsp.stbName, meReader2.me.name);
    metaRsp.numOfTags = meReader2.me.stbEntry.nTags;
    metaRsp.numOfColumns = meReader2.me.stbEntry.nCols;
    metaRsp.suid = meReader2.me.uid;
    metaRsp.pSchemas = taosMemoryMalloc((metaRsp.numOfTags + metaRsp.numOfColumns) * sizeof(SSchema));
    if (metaRsp.pSchemas == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    memcpy(metaRsp.pSchemas, meReader2.me.stbEntry.pSchema, sizeof(SSchema) * metaRsp.numOfColumns);
    memcpy(metaRsp.pSchemas + metaRsp.numOfColumns, meReader2.me.stbEntry.pSchemaTg,
           sizeof(SSchema) * metaRsp.numOfTags);
  } else if (meReader1.me.type == TSDB_NORMAL_TABLE) {
    metaRsp.numOfTags = 0;
    metaRsp.numOfColumns = meReader1.me.ntbEntry.nCols;
    metaRsp.suid = 0;
    metaRsp.pSchemas = meReader1.me.ntbEntry.pSchema;
  } else {
    ASSERT(0);
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

  rpcMsg.handle = pMsg->handle;
  rpcMsg.ahandle = pMsg->ahandle;
  rpcMsg.pCont = pRsp;
  rpcMsg.contLen = rspLen;
  rpcMsg.code = code;

  tmsgSendRsp(&rpcMsg);

_exit:
  if (meReader1.me.type == TSDB_SUPER_TABLE || meReader1.me.type == TSDB_CHILD_TABLE) {
    taosMemoryFree(metaRsp.pSchemas);
  }
  metaEntryReaderClear(&meReader2);
  metaEntryReaderClear(&meReader1);
  return code;
}

int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad) {
  pLoad->vgId = TD_VID(pVnode);
  pLoad->syncState = TAOS_SYNC_STATE_LEADER;
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
