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
  SMetaEntryReader meReader = {0};
  int32_t          code = 0;

  // decode req
  if (tDeserializeSTableInfoReq(pMsg->pCont, pMsg->contLen, &infoReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  // query meta
  metaEntryReaderInit(&meReader);

  if (metaGetTableEntryByName(pVnode->pMeta, &meReader, NULL) < 0) {
    goto _exit;
  }

  // fill response

_exit:
  return 0;
#if 0
  STbCfg         *pTbCfg = NULL;
  STbCfg         *pStbCfg = NULL;
  tb_uid_t        uid;
  int32_t         nCols;
  int32_t         nTagCols;
  SSchemaWrapper *pSW = NULL;
  STableMetaRsp  *pTbMetaMsg = NULL;
  STableMetaRsp   metaRsp = {0};
  SSchema        *pTagSchema;
  SRpcMsg         rpcMsg;
  int             msgLen = 0;
  int32_t         code = 0;
  char            tableFName[TSDB_TABLE_FNAME_LEN];
  int32_t         rspLen = 0;
  void           *pRsp = NULL;

  STableInfoReq infoReq = {0};
  if (tDeserializeSTableInfoReq(pMsg->pCont, pMsg->contLen, &infoReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  metaRsp.dbId = pVnode->config.dbId;
  memcpy(metaRsp.dbFName, infoReq.dbFName, sizeof(metaRsp.dbFName));
  strcpy(metaRsp.tbName, infoReq.tbName);

  sprintf(tableFName, "%s.%s", infoReq.dbFName, infoReq.tbName);
  code = vnodeValidateTableHash(&pVnode->config, tableFName);
  if (code) {
    goto _exit;
  }

  pTbCfg = metaGetTbInfoByName(pVnode->pMeta, infoReq.tbName, &uid);
  if (pTbCfg == NULL) {
    code = TSDB_CODE_VND_TB_NOT_EXIST;
    goto _exit;
  }

  if (pTbCfg->type == META_CHILD_TABLE) {
    pStbCfg = metaGetTbInfoByUid(pVnode->pMeta, pTbCfg->ctbCfg.suid);
    if (pStbCfg == NULL) {
      code = TSDB_CODE_VND_TB_NOT_EXIST;
      goto _exit;
    }

    pSW = metaGetTableSchema(pVnode->pMeta, pTbCfg->ctbCfg.suid, 0, true);
  } else {
    pSW = metaGetTableSchema(pVnode->pMeta, uid, 0, true);
  }

  nCols = pSW->nCols;
  if (pTbCfg->type == META_SUPER_TABLE) {
    // nTagCols = pTbCfg->stbCfg.nTagCols;
    // pTagSchema = pTbCfg->stbCfg.pTagSchema;
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    // nTagCols = pStbCfg->stbCfg.nTagCols;
    // pTagSchema = pStbCfg->stbCfg.pTagSchema;
  } else {
    nTagCols = 0;
    pTagSchema = NULL;
  }

  metaRsp.pSchemas = taosMemoryCalloc(nCols + nTagCols, sizeof(SSchema));
  if (metaRsp.pSchemas == NULL) {
    code = TSDB_CODE_VND_OUT_OF_MEMORY;
    goto _exit;
  }

  if (pTbCfg->type == META_CHILD_TABLE) {
    strcpy(metaRsp.stbName, pStbCfg->name);
    metaRsp.suid = pTbCfg->ctbCfg.suid;
  } else if (pTbCfg->type == META_SUPER_TABLE) {
    strcpy(metaRsp.stbName, pTbCfg->name);
    metaRsp.suid = uid;
  }
  metaRsp.numOfTags = nTagCols;
  metaRsp.numOfColumns = nCols;
  metaRsp.tableType = pTbCfg->type;
  metaRsp.tuid = uid;
  metaRsp.vgId = TD_VID(pVnode);

  memcpy(metaRsp.pSchemas, pSW->pSchema, sizeof(SSchema) * pSW->nCols);
  if (nTagCols) {
    memcpy(POINTER_SHIFT(metaRsp.pSchemas, sizeof(SSchema) * pSW->nCols), pTagSchema, sizeof(SSchema) * nTagCols);
  }

_exit:

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

  tFreeSTableMetaRsp(&metaRsp);
  if (pSW != NULL) {
    taosMemoryFreeClear(pSW->pSchema);
    taosMemoryFreeClear(pSW);
  }

  if (pTbCfg) {
    taosMemoryFreeClear(pTbCfg->name);
    if (pTbCfg->type == META_SUPER_TABLE) {
      // taosMemoryFree(pTbCfg->stbCfg.pTagSchema);
    } else if (pTbCfg->type == META_SUPER_TABLE) {
      kvRowFree(pTbCfg->ctbCfg.pTag);
    }

    taosMemoryFreeClear(pTbCfg);
  }

  rpcMsg.handle = pMsg->handle;
  rpcMsg.ahandle = pMsg->ahandle;
  rpcMsg.pCont = pRsp;
  rpcMsg.contLen = rspLen;
  rpcMsg.code = code;

  tmsgSendRsp(&rpcMsg);
#endif
  return TSDB_CODE_SUCCESS;
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
