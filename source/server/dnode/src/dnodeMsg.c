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
#include "dnodeMsg.h"
#include "dnodeEps.h"
#include "mnode.h"
#include "tthread.h"
#include "ttime.h"
#include "vnode.h"

static struct {
  pthread_t *threadId;
  bool       stop;
  uint32_t   rebootTime;
} tsMsg;

static void dnodeSendStatusMsg() {
  int32_t     contLen = sizeof(SStatusMsg) + TSDB_MAX_VNODES * sizeof(SVnodeLoad);
  SStatusMsg *pStatus = rpcMallocCont(contLen);
  if (pStatus == NULL) {
    dError("failed to malloc status message");
    return;
  }

  pStatus->version = htonl(tsVersion);
  pStatus->dnodeId = htonl(dnodeGetDnodeId());
  tstrncpy(pStatus->dnodeEp, tsLocalEp, TSDB_EP_LEN);
  pStatus->clusterId = htobe64(dnodeGetClusterId());
  pStatus->lastReboot = htonl(tsMsg.rebootTime);
  pStatus->numOfCores = htonl(tsNumOfCores);
  pStatus->diskAvailable = tsAvailDataDirGB;

  // fill cluster cfg parameters
  pStatus->clusterCfg.statusInterval = htonl(tsStatusInterval);
  pStatus->clusterCfg.checkTime = 0;
  tstrncpy(pStatus->clusterCfg.timezone, tsTimezone, 64);
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pStatus->clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  tstrncpy(pStatus->clusterCfg.locale, tsLocale, TSDB_LOCALE_LEN);
  tstrncpy(pStatus->clusterCfg.charset, tsCharset, TSDB_LOCALE_LEN);

  vnodeGetStatus(pStatus);
  contLen = sizeof(SStatusMsg) + pStatus->openVnodes * sizeof(SVnodeLoad);
  pStatus->openVnodes = htons(pStatus->openVnodes);

  SRpcMsg rpcMsg = {.ahandle = NULL, .pCont = pStatus, .contLen = contLen, .msgType = TSDB_MSG_TYPE_DM_STATUS};

  dnodeSendMsgToMnode(&rpcMsg);
}

void dnodeProcessStatusRsp(SRpcMsg *pMsg) {
  dTrace("status rsp is received, code:%s", tstrerror(pMsg->code));
  if (pMsg->code != TSDB_CODE_SUCCESS) return;

  SStatusRsp *pStatusRsp = pMsg->pCont;

  SDnodeCfg *pCfg = &pStatusRsp->dnodeCfg;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->clusterId = htobe64(pCfg->clusterId);
  pCfg->numOfVnodes = htonl(pCfg->numOfVnodes);
  pCfg->numOfDnodes = htonl(pCfg->numOfDnodes);
  dnodeUpdateCfg(pCfg);

  if (pCfg->dropped) {
    dError("status rsp is received, and set dnode to drop status");
    return;
  }

  vnodeSetAccess(pStatusRsp->vgAccess, pCfg->numOfVnodes);

  SDnodeEps *eps = (SDnodeEps *)((char *)pStatusRsp->vgAccess + pCfg->numOfVnodes * sizeof(SVgroupAccess));
  eps->dnodeNum = htonl(eps->dnodeNum);
  for (int32_t i = 0; i < eps->dnodeNum; ++i) {
    eps->dnodeEps[i].dnodeId = htonl(eps->dnodeEps[i].dnodeId);
    eps->dnodeEps[i].dnodePort = htons(eps->dnodeEps[i].dnodePort);
  }

  dnodeUpdateDnodeEps(eps);
}

static void *dnodeThreadRoutine(void *param) {
  int32_t ms = tsStatusInterval * 1000;
  while (!tsMsg.stop) {
    taosMsleep(ms);
    dnodeSendStatusMsg();
  }
}

int32_t dnodeInitMsg() {
  tsMsg.stop = false;
  tsMsg.rebootTime = taosGetTimestampSec();
  tsMsg.threadId = taosCreateThread(dnodeThreadRoutine, NULL);
  if (tsMsg.threadId == NULL) {
    return -1;
  }

  dInfo("dnode msg is initialized");
  return 0;
}

void dnodeCleanupMsg() {
  if (tsMsg.threadId != NULL) {
    tsMsg.stop = true;
    taosDestoryThread(tsMsg.threadId);
    tsMsg.threadId = NULL;
  }

  dInfo("dnode msg is cleanuped");
}

static int32_t dnodeStartMnode(SRpcMsg *pMsg) {
  SCreateMnodeMsg *pCfg = pMsg->pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->mnodeNum = htonl(pCfg->mnodeNum);
  for (int32_t i = 0; i < pCfg->mnodeNum; ++i) {
    pCfg->mnodeEps[i].dnodeId = htonl(pCfg->mnodeEps[i].dnodeId);
    pCfg->mnodeEps[i].dnodePort = htons(pCfg->mnodeEps[i].dnodePort);
  }

  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    dDebug("dnode:%d, in create meps msg is not equal with saved dnodeId:%d", pCfg->dnodeId, dnodeGetDnodeId());
    return TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED;
  }

  if (mnodeGetStatus() == MN_STATUS_READY) return 0;

  return mnodeDeploy();
}

void dnodeProcessCreateMnodeReq(SRpcMsg *pMsg) {
  int32_t code = dnodeStartMnode(pMsg);

  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code};

  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

void dnodeProcessConfigDnodeReq(SRpcMsg *pMsg) {
  SCfgDnodeMsg *pCfg = pMsg->pCont;

  int32_t code = taosCfgDynamicOptions(pCfg->config);

  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code};

  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

void dnodeProcessStartupReq(SRpcMsg *pMsg) {
  dInfo("startup msg is received, cont:%s", (char *)pMsg->pCont);

  SStartupStep *pStep = rpcMallocCont(sizeof(SStartupStep));
  dnodeGetStartup(pStep);

  dDebug("startup msg is sent, step:%s desc:%s finished:%d", pStep->name, pStep->desc, pStep->finished);

  SRpcMsg rpcRsp = {.handle = pMsg->handle, .pCont = pStep, .contLen = sizeof(SStartupStep)};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
}
