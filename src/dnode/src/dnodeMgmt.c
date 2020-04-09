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
#include "tstatus.h"
#include "tsdb.h"
#include "ttime.h"
#include "ttimer.h"
#include "twal.h"
#include "dnodeMClient.h"
#include "dnodeMgmt.h"
#include "dnodeRead.h"
#include "dnodeWrite.h"
#include "vnode.h"

static int32_t  dnodeOpenVnodes();
static void     dnodeCloseVnodes();
static int32_t  dnodeProcessCreateVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessDropVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessAlterVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessAlterStreamMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg);
static int32_t (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);
static void     dnodeSendStatusMsg(void *handle, void *tmrId);
static void     dnodeReadDnodeId();

static void    *tsDnodeTmr = NULL;
static void    *tsStatusTimer = NULL;
static uint32_t tsRebootTime;
static int32_t  tsDnodeId = 0;
static char     tsDnodeName[TSDB_DNODE_NAME_LEN];

int32_t dnodeInitMgmt() {
  dnodeReadDnodeId();

  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeProcessCreateVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeProcessDropVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE]  = dnodeProcessAlterVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = dnodeProcessAlterStreamMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeProcessConfigDnodeMsg;

  tsRebootTime = taosGetTimestampSec();

  tsDnodeTmr = taosTmrInit(100, 200, 60000, "DND-DM");
  if (tsDnodeTmr == NULL) {
    dError("failed to init dnode timer");
    return -1;
  }

  int32_t code = dnodeOpenVnodes();
  if (code != TSDB_CODE_SUCCESS) {
    return -1;
  }

  taosTmrReset(dnodeSendStatusMsg, 500, NULL, tsDnodeTmr, &tsStatusTimer);
  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupMgmt() {
  if (tsStatusTimer != NULL) {
    taosTmrStopA(&tsStatusTimer);
    tsStatusTimer = NULL;
  }

  if (tsDnodeTmr != NULL) {
    taosTmrCleanUp(tsDnodeTmr);
    tsDnodeTmr = NULL;
  }

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

  int32_t *vnodeList = (int32_t *)malloc(sizeof(int32_t) * 10000);
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
  int32_t *vnodeList = (int32_t *)malloc(sizeof(int32_t) * 10000);
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

static void dnodeSendStatusMsg(void *handle, void *tmrId) {
  if (tsDnodeTmr == NULL) {
    dError("dnode timer is already released");
    return;
  }

  if (tsStatusTimer == NULL) {
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
    dError("failed to start status timer");
    return;
  }

  int32_t contLen = sizeof(SDMStatusMsg) + TSDB_MAX_VNODES * sizeof(SVnodeLoad);
  SDMStatusMsg *pStatus = rpcMallocCont(contLen);
  if (pStatus == NULL) {
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
    dError("failed to malloc status message");
    return;
  }

  strcpy(pStatus->dnodeName, tsDnodeName);
  pStatus->version          = htonl(tsVersion);
  pStatus->dnodeId          = htonl(tsDnodeId);
  pStatus->privateIp        = htonl(inet_addr(tsPrivateIp));
  pStatus->publicIp         = htonl(inet_addr(tsPublicIp));
  pStatus->lastReboot       = htonl(tsRebootTime);
  pStatus->numOfTotalVnodes = htons((uint16_t) tsNumOfTotalVnodes);
  pStatus->numOfCores       = htons((uint16_t) tsNumOfCores);
  pStatus->diskAvailable    = tsAvailDataDirGB;
  pStatus->alternativeRole  = (uint8_t) tsAlternativeRole;

  vnodeBuildStatusMsg(pStatus);
  contLen = sizeof(SDMStatusMsg) + pStatus->openVnodes * sizeof(SVnodeLoad);
  pStatus->openVnodes = htons(pStatus->openVnodes);
  
  SRpcMsg rpcMsg = {
    .pCont   = pStatus,
    .contLen = contLen,
    .msgType = TSDB_MSG_TYPE_DM_STATUS
  };

  dnodeSendMsgToMnode(&rpcMsg);
  taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
}

static void dnodeReadDnodeId() {
  char dnodeIdFile[TSDB_FILENAME_LEN] = {0};
  sprintf(dnodeIdFile, "%s/dnodeId", tsDnodeDir);

  FILE *fp = fopen(dnodeIdFile, "r");
  if (!fp) return;
  
  char option[32] = {0};
  int32_t value = 0;
  int32_t num = 0;
  
  num = fscanf(fp, "%s %d", option, &value);
  if (num != 2) return;
  if (strcmp(option, "dnodeId") != 0) return;
  tsDnodeId = value;;

  fclose(fp);
  dPrint("read dnodeId:%d successed", tsDnodeId);
}

static void dnodeSaveDnodeId() {
  char dnodeIdFile[TSDB_FILENAME_LEN] = {0};
  sprintf(dnodeIdFile, "%s/dnodeId", tsDnodeDir);

  FILE *fp = fopen(dnodeIdFile, "w");
  if (!fp) return;

  fprintf(fp, "dnodeId %d\n", tsDnodeId);

  fclose(fp);
  dPrint("save dnodeId successed");
}

void dnodeUpdateDnodeId(int32_t dnodeId) {
  if (tsDnodeId == 0) {
    dPrint("dnodeId is set to %d", dnodeId);  
    tsDnodeId = dnodeId;
    dnodeSaveDnodeId();
  }
}
