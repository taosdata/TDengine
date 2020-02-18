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

static int (*dnodeProcessShellMsgFp[TSDB_MSG_TYPE_MAX])(int8_t *pCont, int32_t contLen, void *pConn);
static void dnodeInitProcessShellMsg();

void dnodeSendMsgToMnodeImpFp(SSchedMsg *sched) {
  int8_t  msgType = *(sched->msg - 1);
  int8_t  *pCont  = sched->msg;
  int32_t contLen = (int32_t) sched->ahandle;
  void    *pConn  = NULL;

  mgmtProcessMsgFromDnode(pCont, contLen, msgType, pConn);
  rpcFreeCont(sched->msg);
}

int32_t dnodeSendMsgToMnodeImp(int8_t *pCont, int32_t contLen, int8_t msgType) {
  dTrace("msg:%s is sent to mnode", taosMsg[msgType]);
  *(pCont-1) = msgType;

  SSchedMsg schedMsg;
  schedMsg.fp      = dnodeSendMsgToMnodeImpFp;
  schedMsg.msg     = pCont;
  schedMsg.ahandle = (void*)contLen;
  schedMsg.thandle = NULL;
  taosScheduleTask(tsDnodeMgmtQhandle, &schedMsg);

  return TSDB_CODE_SUCCESS;
}

int32_t (*dnodeSendMsgToMnode)(int8_t *pCont, int32_t contLen, int8_t msgType) = dnodeSendMsgToMnodeImp;

int32_t dnodeSendSimpleRspToMnodeImp(int32_t msgType, int32_t code) {
  int8_t *pCont = rpcMallocCont(sizeof(int32_t));
  *(int32_t *) pCont = code;

  dnodeSendMsgToMnodeImp(pCont, sizeof(int32_t), msgType);
  return TSDB_CODE_SUCCESS;
}

int32_t (*dnodeSendSimpleRspToMnode)(int32_t msgType, int32_t code) = dnodeSendSimpleRspToMnodeImp;

int32_t dnodeInitMgmtImp() {
  dnodeInitProcessShellMsg();
  return 0;
}

int32_t (*dnodeInitMgmt)() = dnodeInitMgmtImp;

void dnodeInitMgmtIpImp() {}

void (*dnodeInitMgmtIp)() = dnodeInitMgmtIpImp;

void dnodeProcessMsgFromMgmt(int8_t *pCont, int32_t contLen, int32_t msgType, void *pConn) {
  if (msgType < 0 || msgType >= TSDB_MSG_TYPE_MAX) {
    dError("invalid msg type:%d", msgType);
  } else {
    if (dnodeProcessShellMsgFp[msgType]) {
      (*dnodeProcessShellMsgFp[msgType])(pConn, contLen, pConn);
    } else {
      dError("%s is not processed", taosMsg[msgType]);
    }
  }
}

int32_t dnodeProcessTableCfgRsp(char *pMsg, int msgLen, SMgmtObj *pObj) {
  int32_t code = htonl(*((int32_t *) pMsg));

  if (code == TSDB_CODE_SUCCESS) {
    SDCreateTableMsg *table = (SDCreateTableMsg *) (pMsg + sizeof(int32_t));
    return dnodeCreateTable(table);
  } else if (code == TSDB_CODE_INVALID_TABLE_ID) {
    SDRemoveTableMsg *table = (SDRemoveTableMsg *) (pMsg + sizeof(int32_t));
    int32_t vnode = htonl(table->vnode);
    int32_t sid = htonl(table->sid);
    uint64_t uid = htobe64(table->uid);
    dError("vnode:%d, sid:%d table is not configured, remove it", vnode, sid);
    return dnodeDropTable(vnode, sid, uid);
  } else {
    dError("code:%d invalid message", code);
    return TSDB_CODE_INVALID_MSG;
  }
}

int32_t dnodeProcessCreateTableRequest(int8_t *pCont, int32_t contLen, void *pConn) {
  SDCreateTableMsg *table = (SDCreateTableMsg *) pCont;
  int32_t code = dnodeCreateTable(table);
  rpcSendSimpleRsp(pConn, code);
  return code;
}

int32_t dnodeProcessAlterStreamRequest(int8_t *pCont, int32_t contLen, void *pConn) {
  SDCreateTableMsg *table = (SDCreateTableMsg *) pCont;
  int32_t code = dnodeCreateTable(table);
  rpcSendSimpleRsp(pConn, code);
  return code;
}

int32_t dnodeProcessRemoveTableRequest(int8_t *pCont, int32_t contLen, void *pConn) {
  SDRemoveTableMsg *table = (SDRemoveTableMsg *) pCont;
  int32_t vnode = htonl(table->vnode);
  int32_t sid = htonl(table->sid);
  uint64_t uid = htobe64(table->uid);

  dPrint("vnode:%d, sid:%d table is not configured, remove it", vnode, sid);
  int32_t code = dnodeDropTable(vnode, sid, uid);
  rpcSendSimpleRsp(pConn, code);
  return code;
}

int32_t dnodeProcessVPeerCfgRsp(int8_t *pCont, int32_t contLen, void *pConn) {
  int32_t code = htonl(*((int32_t *) pCont));

  if (code == TSDB_CODE_SUCCESS) {
    SVPeersMsg *vpeer = (SVPeersMsg *) (pCont + sizeof(int32_t));
    int32_t vnode  = htonl(vpeer->vnode);
    return dnodeCreateVnode(vnode, vpeer);
  } else if (code == TSDB_CODE_INVALID_VNODE_ID) {
    SFreeVnodeMsg *vpeer = (SFreeVnodeMsg *) (pCont + sizeof(int32_t));
    int32_t vnode = htonl(vpeer->vnode);
    dError("vnode:%d, not exist, remove it", vnode);
    return dnodeDropVnode(vnode);
  } else {
    dError("code:%d invalid message", code);
    return TSDB_CODE_INVALID_MSG;
  }
}

int32_t dnodeProcessVPeersMsg(int8_t *pCont, int32_t contLen, void *pConn) {
  SVPeersMsg *vpeer = (SVPeersMsg *) pCont;
  int32_t vnode = htonl(vpeer->vnode);

  dPrint("vnode:%d, start to config", vnode);

  int32_t code = dnodeCreateVnode(vnode, vpeer);
  rpcSendSimpleRsp(pConn, code);
  return code;
}

int32_t dnodeProcessFreeVnodeRequest(int8_t *pCont, int32_t contLen, void *pConn) {
  SFreeVnodeMsg *vpeer = (SFreeVnodeMsg *) pCont;
  int32_t vnode = htonl(vpeer->vnode);

  dPrint("vnode:%d, remove it", vnode);

  int32_t code = dnodeDropVnode(vnode);
  rpcSendSimpleRsp(pConn, code);

  return code;
}

int32_t dnodeProcessDnodeCfgRequest(int8_t *pCont, int32_t contLen, void *pConn) {
  SCfgMsg *pCfg = (SCfgMsg *)pCont;
  int32_t code = tsCfgDynamicOptions(pCfg->config);
  rpcSendSimpleRsp(pConn, code);
  return code;
}

void dnodeSendVpeerCfgMsg(int32_t vnode) {
  SVpeerCfgMsg *cfg = (SVpeerCfgMsg *) rpcMallocCont(sizeof(SVpeerCfgMsg));
  if (cfg == NULL) {
    return;
  }

  cfg->vnode = htonl(vnode);
  dnodeSendMsgToMnode(cfg, sizeof(SMeterCfgMsg));
}

void dnodeSendMeterCfgMsg(int32_t vnode, int32_t sid) {
  SMeterCfgMsg *cfg = (SMeterCfgMsg *) rpcMallocCont(sizeof(SMeterCfgMsg));
  if (cfg == NULL) {
    return;
  }

  cfg->vnode = htonl(vnode);
  dnodeSendMsgToMnode(cfg, sizeof(SMeterCfgMsg));
}

void dnodeInitProcessShellMsg() {
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_DNODE_CREATE_CHILD_TABLE] = dnodeProcessCreateTableRequest;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_DNODE_CREATE_NORMAL_TABLE] = dnodeProcessCreateTableRequest;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_DNODE_CREATE_STREAM_TABLE] = dnodeProcessCreateTableRequest;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_DNODE_REMOVE_CHILD_TABLE] = dnodeProcessRemoveTableRequest;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_DNODE_REMOVE_NORMAL_TABLE] = dnodeProcessRemoveTableRequest;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_DNODE_REMOVE_STREAM_TABLE] = dnodeProcessRemoveTableRequest;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_DNODE_VPEERS] = dnodeProcessVPeersMsg;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_DNODE_FREE_VNODE] = dnodeProcessFreeVnodeRequest;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_DNODE_CFG] = dnodeProcessDnodeCfgRequest;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_ALTER_STREAM] = dnodeProcessAlterStreamRequest;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_VNODE_CFG_RSP] = dnodeProcessVPeerCfgRsp;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_TABLE_CFG_RSP] = dnodeProcessTableCfgRsp;
}