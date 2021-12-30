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

#include "vnodeQuery.h"
#include "vnodeDef.h"

int vnodeQueryOpen(SVnode *pVnode) { return qWorkerInit(NULL, &pVnode->pQuery); }

int vnodeProcessQueryReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  vInfo("query message is processed");
  return qWorkerProcessQueryMsg(pVnode, pVnode->pQuery, pMsg);
}

int vnodeProcessFetchReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  vInfo("fetch message is processed");
  switch (pMsg->msgType) {
    case TDMT_VND_FETCH:
      return qWorkerProcessFetchMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_RES_READY:
      return qWorkerProcessReadyMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_TASKS_STATUS:
      return qWorkerProcessStatusMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_CANCEL_TASK:
      return qWorkerProcessCancelMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_DROP_TASK:
      return qWorkerProcessDropMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_SHOW_TABLES:
      return qWorkerProcessShowMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_SHOW_TABLES_FETCH:
      return qWorkerProcessShowFetchMsg(pVnode, pVnode->pQuery, pMsg);
    default:
      vError("unknown msg type:%d in fetch queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
}

static int vnodeGetTableMeta(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  STableInfoMsg * pReq = (STableInfoMsg *)(pMsg->pCont);
  STbCfg *        pTbCfg = NULL;
  STbCfg *        pStbCfg = NULL;
  tb_uid_t        uid;
  int32_t         nCols;
  int32_t         nTagCols;
  SSchemaWrapper *pSW;
  STableMetaMsg * pTbMetaMsg;
  SSchema *       pTagSchema;

  pTbCfg = metaGetTbInfoByName(pVnode->pMeta, pReq->tableFname, &uid);
  if (pTbCfg == NULL) {
    return -1;
  }

  if (pTbCfg->type == META_CHILD_TABLE) {
    pStbCfg = metaGetTbInfoByUid(pVnode->pMeta, pTbCfg->ctbCfg.suid);
    if (pStbCfg == NULL) {
      return -1;
    }

    pSW = metaGetTableSchema(pVnode->pMeta, pTbCfg->ctbCfg.suid, 0, true);
  } else {
    pSW = metaGetTableSchema(pVnode->pMeta, uid, 0, true);
  }

  nCols = pSW->nCols;
  if (pTbCfg->type == META_SUPER_TABLE) {
    nTagCols = pTbCfg->stbCfg.nTagCols;
    pTagSchema = pTbCfg->stbCfg.pTagSchema;
  } else if (pTbCfg->type == META_SUPER_TABLE) {
    nTagCols = pStbCfg->stbCfg.nTagCols;
    pTagSchema = pStbCfg->stbCfg.pTagSchema;
  } else {
    nTagCols = 0;
    pTagSchema = NULL;
  }

  pTbMetaMsg = (STableMetaMsg *)calloc(1, sizeof(STableMetaMsg) + sizeof(SSchema) * (nCols + nTagCols));
  if (pTbMetaMsg == NULL) {
    return -1;
  }

  strcpy(pTbMetaMsg->tbFname, pTbCfg->name);
  if (pTbCfg->type == META_CHILD_TABLE) {
    strcpy(pTbMetaMsg->stbFname, pStbCfg->name);
    pTbMetaMsg->suid = htobe64(pTbCfg->ctbCfg.suid);
  }
  pTbMetaMsg->numOfTags = htonl(nTagCols);
  pTbMetaMsg->numOfColumns = htonl(nCols);
  pTbMetaMsg->tableType = pTbCfg->type;
  pTbMetaMsg->tuid = htobe64(uid);
  pTbMetaMsg->vgId = htonl(pVnode->vgId);

  memcpy(pTbMetaMsg->pSchema, pSW->pSchema, sizeof(SSchema) * pSW->nCols);
  if (nTagCols) {
    memcpy(POINTER_SHIFT(pTbMetaMsg->pSchema, sizeof(SSchema) * pSW->nCols), pTagSchema, sizeof(SSchema) * nTagCols);
  }

  for (int i = 0; i < nCols + nTagCols; i++) {
    SSchema *pSch = pTbMetaMsg->pSchema + i;
    pSch->colId = htonl(pSch->colId);
    pSch->bytes = htonl(pSch->bytes);
  }

  return 0;
}