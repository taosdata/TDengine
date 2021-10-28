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
#include "tstep.h"
#include "vnodeMain.h"
#include "vnodeMgmt.h"
#include "vnodeRead.h"
#include "vnodeWrite.h"

static struct {
  SSteps  *steps;
  SVnodeFp fp;
  void (*msgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
} tsVint;

void vnodeGetDnodeEp(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port) {
  return (*tsVint.fp.GetDnodeEp)(dnodeId, ep, fqdn, port);
}

void vnodeSendMsgToDnode(struct SRpcEpSet *epSet, struct SRpcMsg *rpcMsg) {
  (*tsVint.fp.SendMsgToDnode)(epSet, rpcMsg);
}

void vnodeSendMsgToMnode(struct SRpcMsg *rpcMsg) { return (*tsVint.fp.SendMsgToMnode)(rpcMsg); }

void vnodeProcessMsg(SRpcMsg *pMsg) {
  if (tsVint.msgFp[pMsg->msgType]) {
    (*tsVint.msgFp[pMsg->msgType])(pMsg);
  } else {
    assert(0);
  }
}

static void vnodeInitMsgFp() {
  tsVint.msgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = vnodeProcessMgmtMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE] = vnodeProcessMgmtMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MD_SYNC_VNODE] = vnodeProcessMgmtMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE] = vnodeProcessMgmtMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MD_DROP_VNODE] = vnodeProcessMgmtMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = vnodeProcessMgmtMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = vnodeProcessWriteMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MD_DROP_TABLE] = vnodeProcessWriteMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE] = vnodeProcessWriteMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MD_DROP_STABLE] = vnodeProcessWriteMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_SUBMIT] = vnodeProcessWriteMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL] = vnodeProcessWriteMsg;
  // mq related
  tsVint.msgFp[TSDB_MSG_TYPE_MQ_CONNECT] = vnodeProcessWriteMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MQ_DISCONNECT] = vnodeProcessWriteMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MQ_ACK] = vnodeProcessWriteMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MQ_RESET] = vnodeProcessWriteMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MQ_QUERY] = vnodeProcessReadMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_MQ_CONSUME] = vnodeProcessReadMsg;
  // mq related end
  tsVint.msgFp[TSDB_MSG_TYPE_QUERY] = vnodeProcessReadMsg;
  tsVint.msgFp[TSDB_MSG_TYPE_FETCH] = vnodeProcessReadMsg;
}

int32_t vnodeInit(SVnodePara para) {
  vnodeInitMsgFp();
  tsVint.fp = para.fp;

  struct SSteps *steps = taosStepInit(8, NULL);
  if (steps == NULL) return -1;

  taosStepAdd(steps, "vnode-main", vnodeInitMain, vnodeCleanupMain);
  taosStepAdd(steps, "vnode-read", vnodeInitRead, vnodeCleanupRead);
  taosStepAdd(steps, "vnode-mgmt", vnodeInitMgmt, vnodeCleanupMgmt);
  taosStepAdd(steps, "vnode-write", vnodeInitWrite, vnodeCleanupWrite);

  tsVint.steps = steps;
  return taosStepExec(tsVint.steps);
}

void vnodeCleanup() { taosStepCleanup(tsVint.steps); }
