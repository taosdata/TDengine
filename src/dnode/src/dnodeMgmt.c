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
#include "ihash.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "tlog.h"
#include "trpc.h"
#include "tsdb.h"
#include "twal.h"
#include "vnode.h"
#include "dnodeMClient.h"
#include "dnodeMgmt.h"
#include "dnodeRead.h"
#include "dnodeWrite.h"

static int32_t  dnodeOpenVnodes();
static void     dnodeCloseVnodes();
static int32_t  dnodeProcessCreateVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessDropVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessAlterVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessAlterStreamMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg);
static int32_t (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);

int32_t dnodeInitMgmt() {
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeProcessCreateVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeProcessDropVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE]  = dnodeProcessAlterVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = dnodeProcessAlterStreamMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeProcessConfigDnodeMsg;

  int32_t code = dnodeOpenVnodes();
  if (code != TSDB_CODE_SUCCESS) {
    return -1;
  }

  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupMgmt() {
  dnodeCloseVnodes();
}

void dnodeMgmt(SRpcMsg *pMsg) {
  SRpcMsg rsp;

  if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
    rsp.code = (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
  } else {
    rsp.code = TSDB_CODE_MSG_NOT_PROCESSED;
  }

  rsp.handle = pMsg->handle;
  rsp.pCont  = NULL;
  rpcSendResponse(&rsp);

  rpcFreeCont(pMsg->pCont);
}

static int32_t dnodeGetVnodeList(int32_t vnodeList[]) {
  DIR *dir = opendir(tsVnodeDir);
  if (dir == NULL) {
    return TSDB_CODE_NO_WRITE_ACCESS;
  }

  int32_t numOfVnodes = 0;
  struct dirent *de = NULL;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
    if (de->d_type & DT_DIR) {
      if (strncmp("vnode", de->d_name, 5) != 0) continue;
      int32_t vnode = atoi(de->d_name + 5);
      if (vnode == 0) continue;

      vnodeList[numOfVnodes] = vnode;
      numOfVnodes++;
    }
  }
  closedir(dir);

  return numOfVnodes;
}

static int32_t dnodeOpenVnodes() {
  char vnodeDir[TSDB_FILENAME_LEN * 3];
  int32_t failed = 0;

  int32_t *vnodeList = (int32_t *)malloc(sizeof(int32_t) * TSDB_MAX_VNODES);
  int32_t  numOfVnodes = dnodeGetVnodeList(vnodeList);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    snprintf(vnodeDir, TSDB_FILENAME_LEN * 3, "%s/vnode%d", tsVnodeDir, vnodeList[i]);
    if (vnodeOpen(vnodeList[i], vnodeDir) < 0) failed++;
  }

  free(vnodeList);

  dPrint("there are total vnodes:%d, failed to open:%d", numOfVnodes, failed);
  return TSDB_CODE_SUCCESS;
}

static void dnodeCloseVnodes() {
  int32_t *vnodeList = (int32_t *)malloc(sizeof(int32_t) * TSDB_MAX_VNODES);
  int32_t  numOfVnodes = dnodeGetVnodeList(vnodeList);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    vnodeClose(vnodeList[i]);
  }

  free(vnodeList);
  dPrint("total vnodes:%d are all closed", numOfVnodes);
}

static int32_t dnodeProcessCreateVnodeMsg(SRpcMsg *rpcMsg) {
  SMDCreateVnodeMsg *pCreate = rpcMsg->pCont;
  pCreate->cfg.vgId            = htonl(pCreate->cfg.vgId);
  pCreate->cfg.maxSessions     = htonl(pCreate->cfg.maxSessions);
  pCreate->cfg.cacheBlockSize  = htonl(pCreate->cfg.cacheBlockSize);
  pCreate->cfg.daysPerFile     = htonl(pCreate->cfg.daysPerFile);
  pCreate->cfg.daysToKeep1     = htonl(pCreate->cfg.daysToKeep1);
  pCreate->cfg.daysToKeep2     = htonl(pCreate->cfg.daysToKeep2);
  pCreate->cfg.daysToKeep      = htonl(pCreate->cfg.daysToKeep);
  pCreate->cfg.commitTime      = htonl(pCreate->cfg.commitTime);
  pCreate->cfg.rowsInFileBlock = htonl(pCreate->cfg.rowsInFileBlock);
  pCreate->cfg.blocksPerTable  = htons(pCreate->cfg.blocksPerTable);
  pCreate->cfg.cacheNumOfBlocks.totalBlocks = htonl(pCreate->cfg.cacheNumOfBlocks.totalBlocks);
  
  for (int32_t j = 0; j < pCreate->cfg.replications; ++j) {
    pCreate->vpeerDesc[j].vgId    = htonl(pCreate->vpeerDesc[j].vgId);
    pCreate->vpeerDesc[j].dnodeId = htonl(pCreate->vpeerDesc[j].dnodeId);
    pCreate->vpeerDesc[j].ip      = htonl(pCreate->vpeerDesc[j].ip);
  }
  
  return vnodeCreate(pCreate);
}

static int32_t dnodeProcessDropVnodeMsg(SRpcMsg *rpcMsg) {
  SMDDropVnodeMsg *pDrop = rpcMsg->pCont;
  pDrop->vgId = htonl(pDrop->vgId);

  return vnodeDrop(pDrop->vgId);
}

static int32_t dnodeProcessAlterVnodeMsg(SRpcMsg *rpcMsg) {
  SMDCreateVnodeMsg *pCreate = rpcMsg->pCont;
  pCreate->cfg.vgId        = htonl(pCreate->cfg.vgId);
  pCreate->cfg.maxSessions = htonl(pCreate->cfg.maxSessions);
  pCreate->cfg.daysPerFile = htonl(pCreate->cfg.daysPerFile);

  return 0;
}

static int32_t dnodeProcessAlterStreamMsg(SRpcMsg *pMsg) {
//  SMDAlterStreamMsg *pStream = pCont;
//  pStream->uid    = htobe64(pStream->uid);
//  pStream->stime  = htobe64(pStream->stime);
//  pStream->vnode  = htonl(pStream->vnode);
//  pStream->sid    = htonl(pStream->sid);
//  pStream->status = htonl(pStream->status);
//
//  int32_t code = dnodeCreateStream(pStream);

  return 0;
}

static int32_t dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg) {
  SMDCfgDnodeMsg *pCfg = (SMDCfgDnodeMsg *)pMsg->pCont;
  return tsCfgDynamicOptions(pCfg->config);
}
