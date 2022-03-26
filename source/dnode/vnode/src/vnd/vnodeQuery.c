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
#include "executor.h"
#include "vnd.h"

static int32_t vnodeGetTableList(SVnode *pVnode, SRpcMsg *pMsg);
static int     vnodeGetTableMeta(SVnode *pVnode, SRpcMsg *pMsg);

int vnodeQueryOpen(SVnode *pVnode) {
  return qWorkerInit(NODE_TYPE_VNODE, pVnode->vgId, NULL, (void **)&pVnode->pQuery, &pVnode->msgCb);
}

void vnodeQueryClose(SVnode *pVnode) { qWorkerDestroy((void **)&pVnode->pQuery); }

int vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  vTrace("message in query queue is processing");
  SReadHandle handle = {.reader = pVnode->pTsdb, .meta = pVnode->pMeta, .config = &pVnode->config};

  switch (pMsg->msgType) {
    case TDMT_VND_QUERY: {
      return qWorkerProcessQueryMsg(&handle, pVnode->pQuery, pMsg);
    }
    case TDMT_VND_QUERY_CONTINUE:
      return qWorkerProcessCQueryMsg(&handle, pVnode->pQuery, pMsg);
    default:
      vError("unknown msg type:%d in query queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
}

int vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  vTrace("message in fetch queue is processing");
  switch (pMsg->msgType) {
    case TDMT_VND_FETCH:
      return qWorkerProcessFetchMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_FETCH_RSP:
      return qWorkerProcessFetchRsp(pVnode, pVnode->pQuery, pMsg);
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
      return vnodeGetTableList(pVnode, pMsg);
      //      return qWorkerProcessShowFetchMsg(pVnode->pMeta, pVnode->pQuery, pMsg);
    case TDMT_VND_TABLE_META:
      return vnodeGetTableMeta(pVnode, pMsg);
    case TDMT_VND_CONSUME:
      return tqProcessPollReq(pVnode->pTq, pMsg);
    case TDMT_VND_TASK_EXEC:
      return tqProcessTaskExec(pVnode->pTq, pMsg);
    case TDMT_VND_STREAM_TRIGGER:
      return tqProcessStreamTrigger(pVnode->pTq, pMsg->pCont, pMsg->contLen);
    case TDMT_VND_QUERY_HEARTBEAT:
      return qWorkerProcessHbMsg(pVnode, pVnode->pQuery, pMsg);
    default:
      vError("unknown msg type:%d in fetch queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
}

static int vnodeGetTableMeta(SVnode *pVnode, SRpcMsg *pMsg) {
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
    nTagCols = pTbCfg->stbCfg.nTagCols;
    pTagSchema = pTbCfg->stbCfg.pTagSchema;
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    nTagCols = pStbCfg->stbCfg.nTagCols;
    pTagSchema = pStbCfg->stbCfg.pTagSchema;
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
  metaRsp.vgId = pVnode->vgId;

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
      taosMemoryFree(pTbCfg->stbCfg.pTagSchema);
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

  rpcSendResponse(&rpcMsg);

  return code;
}

static void freeItemHelper(void *pItem) {
  char *p = *(char **)pItem;
  taosMemoryFree(p);
}

/**
 * @param pVnode
 * @param pMsg
 * @param pRsp
 */
static int32_t vnodeGetTableList(SVnode *pVnode, SRpcMsg *pMsg) {
  SMTbCursor *pCur = metaOpenTbCursor(pVnode->pMeta);
  SArray     *pArray = taosArrayInit(10, POINTER_BYTES);

  char   *name = NULL;
  int32_t totalLen = 0;
  int32_t numOfTables = 0;
  while ((name = metaTbCursorNext(pCur)) != NULL) {
    if (numOfTables < 10000) {  // TODO: temp get tables of vnode, and should del when show tables commad ok.
      taosArrayPush(pArray, &name);
      totalLen += strlen(name);
    } else {
      taosMemoryFreeClear(name);
    }

    numOfTables++;
  }

  // TODO: temp debug, and should del when show tables command ok
  vInfo("====vgId:%d, numOfTables: %d", pVnode->vgId, numOfTables);
  if (numOfTables > 10000) {
    numOfTables = 10000;
  }

  metaCloseTbCursor(pCur);

  int32_t rowLen =
      (TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE) + 8 + 2 + (TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE) + 8 + 4;
  // int32_t numOfTables = (int32_t)taosArrayGetSize(pArray);

  int32_t payloadLen = rowLen * numOfTables;
  //  SVShowTablesFetchReq *pFetchReq = pMsg->pCont;

  SVShowTablesFetchRsp *pFetchRsp = (SVShowTablesFetchRsp *)rpcMallocCont(sizeof(SVShowTablesFetchRsp) + payloadLen);
  memset(pFetchRsp, 0, sizeof(SVShowTablesFetchRsp) + payloadLen);

  char *p = pFetchRsp->data;
  for (int32_t i = 0; i < numOfTables; ++i) {
    char *n = taosArrayGetP(pArray, i);
    STR_TO_VARSTR(p, n);

    p += (TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE);
    // taosMemoryFree(n);
  }

  pFetchRsp->numOfRows = htonl(numOfTables);
  pFetchRsp->precision = 0;

  SRpcMsg rpcMsg = {
      .handle = pMsg->handle,
      .ahandle = pMsg->ahandle,
      .pCont = pFetchRsp,
      .contLen = sizeof(SVShowTablesFetchRsp) + payloadLen,
      .code = 0,
  };

  rpcSendResponse(&rpcMsg);

  taosArrayDestroyEx(pArray, freeItemHelper);
  return 0;
}
