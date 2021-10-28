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
} tsVint;

void vnodeGetDnodeEp(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port) {
  return (*tsVint.fp.GetDnodeEp)(dnodeId, ep, fqdn, port);
}

void vnodeSendMsgToDnode(struct SRpcEpSet *epSet, struct SRpcMsg *rpcMsg) {
  (*tsVint.fp.SendMsgToDnode)(epSet, rpcMsg);
}

void vnodeSendMsgToMnode(struct SRpcMsg *rpcMsg) { return (*tsVint.fp.SendMsgToMnode)(rpcMsg); }

int32_t vnodeInit(SVnodePara para) {
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
