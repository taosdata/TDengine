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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosmsg.h"
#include "tlog.h"
#include "trpc.h"
#include "tsched.h"
#include "tsystem.h"
#include "mnode.h"
#include "dnode.h"
#include "dnodeSystem.h"
#include "dnodeMgmt.h"
#include "dnodeWrite.h"
#include "dnodeVnodeMgmt.h"

void    (*dnodeInitMgmtIpFp)() = NULL;
int32_t (*dnodeInitMgmtFp)() = NULL;
void    (*dnodeProcessStatusRspFp)(int8_t *pCont, int32_t contLen, int8_t msgType, void *pConn) = NULL;
void    (*dnodeSendMsgToMnodeFp)(int8_t msgType, void *pCont, int32_t contLen) = NULL;
void    (*dnodeSendRspToMnodeFp)(void *handle, int32_t code, void *pCont, int contLen) = NULL;

static void (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(void *pCont, int32_t contLen, int8_t msgType, void *pConn);
static void dnodeInitProcessShellMsg();

static void dnodeSendMsgToMnodeQueueFp(SSchedMsg *sched) {
  int32_t contLen = *(int32_t *) (sched->msg - 4);
  int32_t code    = *(int32_t *) (sched->msg - 8);
  int8_t  msgType = *(int8_t  *) (sched->msg - 9);
  void    *handle = sched->ahandle;
  int8_t  *pCont  = sched->msg;

  mgmtProcessMsgFromDnode(msgType, pCont, contLen, handle, code);
}

void dnodeSendMsgToMnode(int8_t msgType, void *pCont, int32_t contLen) {
  dTrace("msg:%d:%s is sent to mnode", msgType, taosMsg[msgType]);
  if (dnodeSendMsgToMnodeFp) {
    dnodeSendMsgToMnodeFp(msgType, pCont, contLen);
  } else {
    if (pCont == NULL) {
      pCont = rpcMallocCont(1);
      contLen = 0;
    }
    SSchedMsg schedMsg = {0};
    schedMsg.fp      = dnodeSendMsgToMnodeQueueFp;
    schedMsg.msg     = pCont;
    *(int32_t *) (pCont - 4) = contLen;
    *(int32_t *) (pCont - 8) = TSDB_CODE_SUCCESS;
    *(int8_t *)  (pCont - 9) = msgType;
    taosScheduleTask(tsDnodeMgmtQhandle, &schedMsg);
  }
}

void dnodeSendRspToMnode(void *pConn, int8_t msgType, int32_t code, void *pCont, int32_t contLen) {
  dTrace("rsp:%d:%s is sent to mnode, pConn:%p", msgType, taosMsg[msgType], pConn);
  if (dnodeSendRspToMnodeFp) {
    dnodeSendRspToMnodeFp(pConn, code, pCont, contLen);
  } else {
    //hack way
    if (pCont == NULL) {
      pCont = rpcMallocCont(1);
      contLen = 0;
    }
    SSchedMsg schedMsg = {0};
    schedMsg.fp      = dnodeSendMsgToMnodeQueueFp;
    schedMsg.msg     = pCont;
    schedMsg.ahandle = pConn;
    *(int32_t *) (pCont - 4) = contLen;
    *(int32_t *) (pCont - 8) = code;
    *(int8_t *)  (pCont - 9) = msgType;
    taosScheduleTask(tsDnodeMgmtQhandle, &schedMsg);
  }
}

int32_t dnodeInitMgmt() {
  if (dnodeInitMgmtFp) {
    dnodeInitMgmtFp();
  }

  dnodeInitProcessShellMsg();
  return 0;
}

void dnodeInitMgmtIp() {
  if (dnodeInitMgmtIpFp) {
    dnodeInitMgmtIpFp();
  }
}

void dnodeProcessMsgFromMgmt(int8_t msgType, void *pCont, int32_t contLen, void *pConn, int32_t code) {
  if (msgType < 0 || msgType >= TSDB_MSG_TYPE_MAX) {
    dError("invalid msg type:%d", msgType);
    return;
  }

  dTrace("msg:%d:%s is received from mgmt, pConn:%p", msgType, taosMsg[msgType], pConn);

  if (msgType == TSDB_MSG_TYPE_STATUS_RSP && dnodeProcessStatusRspFp != NULL) {
    dnodeProcessStatusRspFp(pCont, contLen, msgType, pConn);
  }
  if (dnodeProcessMgmtMsgFp[msgType]) {
    (*dnodeProcessMgmtMsgFp[msgType])(pCont, contLen, msgType, pConn);
  } else {
    dError("%s is not processed", taosMsg[msgType]);
  }

  //rpcFreeCont(pCont);
}

void dnodeProcessCreateTableRequest(void *pCont, int32_t contLen, int8_t msgType, void *pConn) {
  SDCreateTableMsg *pTable = pCont;
  pTable->numOfColumns  = htons(pTable->numOfColumns);
  pTable->numOfTags     = htons(pTable->numOfTags);
  pTable->sid           = htonl(pTable->sid);
  pTable->sversion      = htonl(pTable->sversion);
  pTable->tagDataLen    = htonl(pTable->tagDataLen);
  pTable->sqlDataLen    = htonl(pTable->sqlDataLen);
  pTable->contLen       = htonl(pTable->contLen);
  pTable->numOfVPeers   = htonl(pTable->numOfVPeers);
  pTable->uid           = htobe64(pTable->uid);
  pTable->superTableUid = htobe64(pTable->superTableUid);
  pTable->createdTime   = htobe64(pTable->createdTime);

  for (int i = 0; i < pTable->numOfVPeers; ++i) {
    pTable->vpeerDesc[i].ip    = htonl(pTable->vpeerDesc[i].ip);
    pTable->vpeerDesc[i].vnode = htonl(pTable->vpeerDesc[i].vnode);
  }

  int32_t totalCols = pTable->numOfColumns + pTable->numOfTags;
  SSchema *pSchema = (SSchema *) pTable->data;
  for (int32_t col = 0; col < totalCols; ++col) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema->colId = htons(pSchema->colId);
    pSchema++;
  }

  int32_t code = dnodeCreateTable(pTable);
  dnodeSendRspToMnode(pConn, msgType + 1, code, NULL, 0);
}

void dnodeProcessAlterStreamRequest(void *pCont, int32_t contLen, int8_t msgType, void *pConn) {
  SDAlterStreamMsg *pStream = pCont;
  pStream->uid    = htobe64(pStream->uid);
  pStream->stime  = htobe64(pStream->stime);
  pStream->vnode  = htonl(pStream->vnode);
  pStream->sid    = htonl(pStream->sid);
  pStream->status = htonl(pStream->status);

  int32_t code = dnodeCreateStream(pStream);
  dnodeSendRspToMnode(pConn, msgType + 1, code, NULL, 0);
}

void dnodeProcessRemoveTableRequest(void *pCont, int32_t contLen, int8_t msgType, void *pConn) {
  SDRemoveTableMsg *pTable = pCont;
  pTable->sid         = htonl(pTable->sid);
  pTable->numOfVPeers = htonl(pTable->numOfVPeers);
  pTable->uid         = htobe64(pTable->uid);

  for (int i = 0; i < pTable->numOfVPeers; ++i) {
    pTable->vpeerDesc[i].ip    = htonl(pTable->vpeerDesc[i].ip);
    pTable->vpeerDesc[i].vnode = htonl(pTable->vpeerDesc[i].vnode);
  }

  int32_t code = dnodeDropTable(pTable);
  dnodeSendRspToMnode(pConn, msgType + 1, code, NULL, 0);
}

void dnodeProcessVPeerCfgRsp(void *pCont, int32_t contLen, int8_t msgType, void *pConn) {
  int32_t code = htonl(*((int32_t *) pCont));

  if (code == TSDB_CODE_SUCCESS) {
    SCreateVnodeMsg *pVnode = (SCreateVnodeMsg *) (pCont + sizeof(int32_t));
    dnodeCreateVnode(pVnode);
  } else if (code == TSDB_CODE_INVALID_VNODE_ID) {
    SFreeVnodeMsg *vpeer = (SFreeVnodeMsg *) (pCont + sizeof(int32_t));
    int32_t vnode = htonl(vpeer->vnode);
    dError("vnode:%d, not exist, remove it", vnode);
    dnodeDropVnode(vnode);
  } else {
    dError("code:%d invalid message", code);
  }
}

void dnodeProcessTableCfgRsp(void *pCont, int32_t contLen, int8_t msgType, void *pConn) {
  int32_t code = htonl(*((int32_t *) pCont));

  if (code == TSDB_CODE_SUCCESS) {
    SDCreateTableMsg *table = (SDCreateTableMsg *) (pCont + sizeof(int32_t));
    dnodeCreateTable(table);
  } else if (code == TSDB_CODE_INVALID_TABLE_ID) {
    SDRemoveTableMsg *pTable = (SDRemoveTableMsg *) (pCont + sizeof(int32_t));
    pTable->sid   = htonl(pTable->sid);
    pTable->uid   = htobe64(pTable->uid);
    dError("table:%s, sid:%d table is not configured, remove it", pTable->tableId, pTable->sid);
    dnodeDropTable(pTable);
  } else {
    dError("code:%d invalid message", code);
  }
}

void dnodeProcessCreateVnodeRequest(void *pCont, int32_t contLen, int8_t msgType, void *pConn) {
  SCreateVnodeMsg *pVnode = (SCreateVnodeMsg *) pCont;

  int32_t code = dnodeCreateVnode(pVnode);
  dnodeSendRspToMnode(pConn, msgType + 1, code, NULL, 0);
}

void dnodeProcessFreeVnodeRequest(void *pCont, int32_t contLen, int8_t msgType, void *pConn) {
  SFreeVnodeMsg *pVnode = (SFreeVnodeMsg *) pCont;
  int32_t vnode = htonl(pVnode->vnode);

  int32_t code = dnodeDropVnode(vnode);
  dnodeSendRspToMnode(pConn, msgType + 1, code, NULL, 0);
}

void dnodeProcessDnodeCfgRequest(void *pCont, int32_t contLen, int8_t msgType, void *pConn) {
  SCfgDnodeMsg *pCfg = (SCfgDnodeMsg *)pCont;

  int32_t code = tsCfgDynamicOptions(pCfg->config);
  dnodeSendRspToMnode(pConn, msgType + 1, code, NULL, 0);
}

void dnodeSendVnodeCfgMsg(int32_t vnode) {
  SVpeerCfgMsg *cfg = (SVpeerCfgMsg *) rpcMallocCont(sizeof(SVpeerCfgMsg));
  if (cfg == NULL) {
    return;
  }

  cfg->vnode = htonl(vnode);
  dnodeSendMsgToMnode(TSDB_MSG_TYPE_VNODE_CFG, cfg, sizeof(SVpeerCfgMsg));
}

void dnodeSendTableCfgMsg(int32_t vnode, int32_t sid) {
  STableCfgMsg *cfg = (STableCfgMsg *) rpcMallocCont(sizeof(STableCfgMsg));
  if (cfg == NULL) {
    return;
  }

  cfg->vnode = htonl(vnode);
  dnodeSendMsgToMnode(TSDB_MSG_TYPE_TABLE_CFG, cfg, sizeof(STableCfgMsg));
}

void dnodeInitProcessShellMsg() {
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_DNODE_CREATE_TABLE] = dnodeProcessCreateTableRequest;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_DNODE_REMOVE_TABLE] = dnodeProcessRemoveTableRequest;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_CREATE_VNODE]       = dnodeProcessCreateVnodeRequest;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_FREE_VNODE]   = dnodeProcessFreeVnodeRequest;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_DNODE_CFG]          = dnodeProcessDnodeCfgRequest;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_ALTER_STREAM]       = dnodeProcessAlterStreamRequest;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_VNODE_CFG_RSP]      = dnodeProcessVPeerCfgRsp;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_TABLE_CFG_RSP]      = dnodeProcessTableCfgRsp;
}