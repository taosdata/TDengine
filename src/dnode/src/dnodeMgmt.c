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
#include "dnodeMClient.h"
#include "dnodeMgmt.h"
#include "dnodeRead.h"
#include "dnodeWrite.h"

typedef struct {
  int32_t      vgId;      // global vnode group ID
  int32_t      refCount;  // reference count
  EVnodeStatus status;    // status: master, slave, notready, deleting
  int64_t      version;
  void *       wworker;
  void *       rworker;
  void *       wal;
  void *       tsdb;
  void *       replica;
  void *       events;
  void *       cq;  // continuous query
} SVnodeObj;

static int32_t  dnodeOpenVnodes();
static void     dnodeCleanupVnodes();
static int32_t  dnodeOpenVnode(int32_t vnode, char *rootDir);
static void     dnodeCleanupVnode(SVnodeObj *pVnode);
static void     dnodeDoCleanupVnode(SVnodeObj *pVnode);
static int32_t  dnodeCreateVnode(SMDCreateVnodeMsg *cfg);
static void     dnodeDropVnode(SVnodeObj *pVnode);
static void     dnodeDoDropVnode(SVnodeObj *pVnode);
static void     dnodeProcessCreateVnodeMsg(SRpcMsg *pMsg);
static void     dnodeProcessDropVnodeMsg(SRpcMsg *pMsg);
static void     dnodeProcessAlterVnodeMsg(SRpcMsg *pMsg);
static void     dnodeProcessAlterStreamMsg(SRpcMsg *pMsg);
static void     dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg);
static void   (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);
static void     dnodeSendStatusMsg(void *handle, void *tmrId);
static void     dnodeReadDnodeId();

static void    *tsDnodeVnodesHash = NULL;
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

  tsDnodeVnodesHash = taosInitIntHash(TSDB_MAX_VNODES, sizeof(SVnodeObj), taosHashInt);
  if (tsDnodeVnodesHash == NULL) {
    dError("failed to init vnode list");
    return -1;
  }

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

  dnodeCleanupVnodes();
  if (tsDnodeVnodesHash == NULL) {
    taosCleanUpIntHash(tsDnodeVnodesHash);
    tsDnodeVnodesHash = NULL;
  }
}

void dnodeMgmt(SRpcMsg *pMsg) {
  terrno = 0;

  if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
    (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
  } else {
    SRpcMsg rsp;
    rsp.handle = pMsg->handle;
    rsp.code   = TSDB_CODE_MSG_NOT_PROCESSED;
    rsp.pCont  = NULL;
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
}

void *dnodeGetVnode(int32_t vgId) {
  SVnodeObj *pVnode = (SVnodeObj *) taosGetIntHashData(tsDnodeVnodesHash, vgId);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_INVALID_VGROUP_ID;
    return NULL;
  }

  if (pVnode->status != TSDB_VN_STATUS_MASTER && pVnode->status == TSDB_VN_STATUS_SLAVE) {
    terrno = TSDB_CODE_INVALID_VNODE_STATUS;
    return NULL;
  }

  atomic_add_fetch_32(&pVnode->refCount, 1);
  dTrace("pVnode:%p, vgroup:%d, get vnode, refCount:%d", pVnode, pVnode->vgId, pVnode->refCount);

  return pVnode;
}

int32_t dnodeGetVnodeStatus(void *pVnode) {
  return ((SVnodeObj *)pVnode)->status;
}

void *dnodeGetVnodeWworker(void *pVnode) {
  return ((SVnodeObj *)pVnode)->wworker;
}
 
void *dnodeGetVnodeRworker(void *pVnode) {
  return ((SVnodeObj *)pVnode)->rworker;
}
 
void *dnodeGetVnodeWal(void *pVnode) {
  return ((SVnodeObj *)pVnode)->wal;
}

void *dnodeGetVnodeTsdb(void *pVnode) {
  return ((SVnodeObj *)pVnode)->tsdb;
}

void dnodeReleaseVnode(void *pVnodeRaw) {
  SVnodeObj *pVnode = pVnodeRaw;

  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  if (pVnode->status == TSDB_VN_STATUS_DELETING) {
    if (refCount <= 0) {
      dPrint("pVnode:%p, vgroup:%d, drop vnode, refCount:%d", pVnode, pVnode->vgId, refCount);
      dnodeDoDropVnode(pVnode);
    } else {
      dTrace("pVnode:%p, vgroup:%d, vnode will be dropped until refCount:%d is 0", pVnode, pVnode->vgId, refCount);
    }
  } else if (pVnode->status == TSDB_VN_STATUS_CLOSING) {
    if (refCount <= 0) {
      dPrint("pVnode:%p, vgroup:%d, cleanup vnode, refCount:%d", pVnode, pVnode->vgId, refCount);
      dnodeDoCleanupVnode(pVnode);
    } else {
      dTrace("pVnode:%p, vgroup:%d, vnode will cleanup until refCount:%d is 0", pVnode, pVnode->vgId, refCount);
    }
  } else {
    dTrace("pVnode:%p, vgroup:%d, release vnode, refCount:%d", pVnode, pVnode->vgId, refCount);
  }
}

static int32_t dnodeOpenVnodes() {
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

      char vnodeDir[TSDB_FILENAME_LEN * 3];
      snprintf(vnodeDir, TSDB_FILENAME_LEN * 3, "%s/%s", tsVnodeDir, de->d_name);
      int32_t code = dnodeOpenVnode(vnode, vnodeDir);
      if (code == 0) {
        numOfVnodes++;
      }
    }
  }
  closedir(dir);

  dPrint("dnode mgmt is opened, vnodes:%d", numOfVnodes);
  return TSDB_CODE_SUCCESS;
}

typedef void (*CleanupFp)(char *);
static void dnodeCleanupVnodes() {
  int32_t num = taosGetIntHashSize(tsDnodeVnodesHash);
  taosCleanUpIntHashWithFp(tsDnodeVnodesHash, (CleanupFp)dnodeCleanupVnode);
  dPrint("dnode mgmt is closed, vnodes:%d", num);
}

static int32_t dnodeOpenVnode(int32_t vnode, char *rootDir) {
  SVnodeObj vnodeObj = {0};
  vnodeObj.vgId     = vnode;
  vnodeObj.status   = TSDB_VN_STATUS_NOT_READY;
  vnodeObj.refCount = 1;
  vnodeObj.version  = 0;  
  SVnodeObj *pVnode = (SVnodeObj *)taosAddIntHash(tsDnodeVnodesHash, vnodeObj.vgId, (char *)(&vnodeObj));

  char tsdbDir[TSDB_FILENAME_LEN];
  sprintf(tsdbDir, "%s/tsdb", rootDir);
  void *pTsdb = tsdbOpenRepo(tsdbDir);
  if (pTsdb == NULL) {
    dError("pVnode:%p, vgroup:%d, failed to open tsdb in %s, reason:%s", pVnode, pVnode->vgId, tsdbDir, tstrerror(terrno));
    taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);
    return terrno;
  }

  pVnode->wal      = NULL;
  pVnode->tsdb     = pTsdb;
  pVnode->replica  = NULL;
  pVnode->events   = NULL;
  pVnode->cq       = NULL;
  pVnode->wworker = dnodeAllocateWriteWorker(pVnode);
  pVnode->rworker = dnodeAllocateReadWorker(pVnode);

  //TODO: jude status while replca is not null
  if (pVnode->replica == NULL) {
    pVnode->status = TSDB_VN_STATUS_MASTER;
  }

  dTrace("pVnode:%p, vgroup:%d, vnode is opened in %s", pVnode, pVnode->vgId, rootDir);
  return TSDB_CODE_SUCCESS;
}

static void dnodeDoCleanupVnode(SVnodeObj *pVnode) {
  dTrace("pVnode:%p, vgroup:%d, cleanup vnode", pVnode, pVnode->vgId);
  
  // remove replica

  // remove read queue
  dnodeFreeReadWorker(pVnode->rworker);
  pVnode->rworker = NULL;

  // remove write queue
  dnodeFreeWriteWorker(pVnode->wworker);
  pVnode->wworker = NULL;

  // remove wal

  // remove tsdb
  if (pVnode->tsdb) {
    tsdbCloseRepo(pVnode->tsdb);
    pVnode->tsdb = NULL;
  }
}

static void dnodeCleanupVnode(SVnodeObj *pVnode) {
  pVnode->status = TSDB_VN_STATUS_CLOSING;
  dnodeReleaseVnode(pVnode);
}

static int32_t dnodeCreateVnode(SMDCreateVnodeMsg *pVnodeCfg) {
  STsdbCfg tsdbCfg = {0};
  tsdbCfg.precision           = pVnodeCfg->cfg.precision;
  tsdbCfg.tsdbId              = pVnodeCfg->cfg.vgId;
  tsdbCfg.maxTables           = pVnodeCfg->cfg.maxSessions;
  tsdbCfg.daysPerFile         = pVnodeCfg->cfg.daysPerFile;
  tsdbCfg.minRowsPerFileBlock = -1;
  tsdbCfg.maxRowsPerFileBlock = -1;
  tsdbCfg.keep                = -1;
  tsdbCfg.maxCacheSize        = -1;

  char rootDir[TSDB_FILENAME_LEN] = {0};
  sprintf(rootDir, "%s/vnode%d", tsVnodeDir, pVnodeCfg->cfg.vgId);
  if (mkdir(rootDir, 0755) != 0) {
    if (errno == EACCES) {
      return TSDB_CODE_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_SERV_NO_DISKSPACE;
    } else if (errno == EEXIST) {
    } else {
      return TSDB_CODE_VG_INIT_FAILED;
    }
  }

  sprintf(rootDir, "%s/vnode%d/tsdb", tsVnodeDir, pVnodeCfg->cfg.vgId);
  if (mkdir(rootDir, 0755) != 0) {
    if (errno == EACCES) {
      return TSDB_CODE_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_SERV_NO_DISKSPACE;
    } else if (errno == EEXIST) {
    } else {
      return TSDB_CODE_VG_INIT_FAILED;
    }
  }

  void *pTsdb = tsdbCreateRepo(rootDir, &tsdbCfg, NULL);
  if (pTsdb == NULL) {
    dError("vgroup:%d, failed to create tsdb in vnode, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(terrno));
    return terrno;
  }

  SVnodeObj vnodeObj = {0};
  vnodeObj.vgId     = pVnodeCfg->cfg.vgId;
  vnodeObj.status   = TSDB_VN_STATUS_CREATING;
  vnodeObj.refCount = 1;
  vnodeObj.version  = 0;
  vnodeObj.wal      = NULL;
  vnodeObj.tsdb     = pTsdb;
  vnodeObj.replica  = NULL;
  vnodeObj.events   = NULL;
  vnodeObj.cq       = NULL;

  SVnodeObj *pVnode = (SVnodeObj *)taosAddIntHash(tsDnodeVnodesHash, vnodeObj.vgId, (char *)(&vnodeObj));
  pVnode->wworker = dnodeAllocateWriteWorker(pVnode);
  pVnode->rworker = dnodeAllocateReadWorker(pVnode);
  if (pVnode->replica == NULL) {
    pVnode->status = TSDB_VN_STATUS_MASTER;
  }

  dPrint("vgroup:%d, vnode:%d is created", pVnode->vgId, pVnode->vgId);
  return TSDB_CODE_SUCCESS;
}

static void dnodeDoDropVnode(SVnodeObj *pVnode) {
  dnodeDoCleanupVnode(pVnode);
  taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);

  char rootDir[TSDB_FILENAME_LEN] = {0};
  sprintf(rootDir, "%s/vnode%d", tsVnodeDir, pVnode->vgId);
  dPrint("pVnode:%p, vgroup:%d, drop file:%s from disk", pVnode, pVnode->vgId, rootDir);
  // rmdir(rootDir);
}

static void dnodeDropVnode(SVnodeObj *pVnode) {
  pVnode->status = TSDB_VN_STATUS_DELETING;
  dnodeReleaseVnode(pVnode);
}

static void dnodeProcessCreateVnodeMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  SMDCreateVnodeMsg *pCreate = rpcMsg->pCont;
  pCreate->cfg.vgId        = htonl(pCreate->cfg.vgId);
  pCreate->cfg.maxSessions = htonl(pCreate->cfg.maxSessions);
  pCreate->cfg.daysPerFile = htonl(pCreate->cfg.daysPerFile);

  dTrace("vgroup:%d, start to create vnode in dnode", pCreate->cfg.vgId);

  SVnodeObj *pVnodeObj = (SVnodeObj *) taosGetIntHashData(tsDnodeVnodesHash, pCreate->cfg.vgId);
  if (pVnodeObj != NULL) {
    rpcRsp.code = TSDB_CODE_SUCCESS;
    dPrint("vgroup:%d, vnode is already exist", pCreate->cfg.vgId);
  } else {
    rpcRsp.code = dnodeCreateVnode(pCreate);
  }

  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessDropVnodeMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  SMDDropVnodeMsg *pDrop = rpcMsg->pCont;
  pDrop->vgId = htonl(pDrop->vgId);

  SVnodeObj *pVnodeObj = (SVnodeObj *) taosGetIntHashData(tsDnodeVnodesHash, pDrop->vgId);
  if (pVnodeObj != NULL) {
    dPrint("pVnode:%p, vgroup:%d, start to drop vnode in dnode", pVnodeObj, pDrop->vgId);
    dnodeDropVnode(pVnodeObj);
    rpcRsp.code = TSDB_CODE_SUCCESS;
  } else {
    dTrace("vgroup:%d, failed drop vnode in dnode, vgroup not exist", pDrop->vgId);
    rpcRsp.code = TSDB_CODE_INVALID_VGROUP_ID;
  }

  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessAlterVnodeMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  SMDCreateVnodeMsg *pCreate = rpcMsg->pCont;
  pCreate->cfg.vgId        = htonl(pCreate->cfg.vgId);
  pCreate->cfg.maxSessions = htonl(pCreate->cfg.maxSessions);
  pCreate->cfg.daysPerFile = htonl(pCreate->cfg.daysPerFile);

  dTrace("vgroup:%d, start to alter vnode in dnode", pCreate->cfg.vgId);

  SVnodeObj *pVnodeObj = (SVnodeObj *) taosGetIntHashData(tsDnodeVnodesHash, pCreate->cfg.vgId);
  if (pVnodeObj != NULL) {
    dPrint("pVnode:%p, vgroup:%d, start to alter vnode in dnode", pVnodeObj, pCreate->cfg.vgId);
    rpcRsp.code = TSDB_CODE_SUCCESS;
  } else {
    dTrace("vgroup:%d, alter vnode msg received, start to create vnode", pCreate->cfg.vgId);
    rpcRsp.code = dnodeCreateVnode(pCreate);;
  }

  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessAlterStreamMsg(SRpcMsg *pMsg) {
//  SMDAlterStreamMsg *pStream = pCont;
//  pStream->uid    = htobe64(pStream->uid);
//  pStream->stime  = htobe64(pStream->stime);
//  pStream->vnode  = htonl(pStream->vnode);
//  pStream->sid    = htonl(pStream->sid);
//  pStream->status = htonl(pStream->status);
//
//  int32_t code = dnodeCreateStream(pStream);
}

static void dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg) {
  SMDCfgDnodeMsg *pCfg = (SMDCfgDnodeMsg *)pMsg->pCont;
  int32_t code = tsCfgDynamicOptions(pCfg->config);

  SRpcMsg rpcRsp = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code, .msgType = 0};
  rpcSendResponse(&rpcRsp);
}

static void dnodeBuildVloadMsg(char *pNode, void * param) {
  SVnodeObj *pVnode = (SVnodeObj *) pNode;
  dPrint("===> pVnode:%p, vgroup:%d status:%s", pVnode, pVnode->vgId, taosGetVnodeStatusStr(pVnode->status));
  if (pVnode->status == TSDB_VN_STATUS_DELETING) return;
  
  SDMStatusMsg *pStatus = param;
  if (pStatus->openVnodes >= TSDB_MAX_VNODES) return;
  
  SVnodeLoad *pLoad = &pStatus->load[pStatus->openVnodes++];
  pLoad->vgId = htonl(pVnode->vgId);
  pLoad->vnode = htonl(pVnode->vgId);
  pLoad->status = pVnode->status;
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

  taosVisitIntHashWithFp(tsDnodeVnodesHash, dnodeBuildVloadMsg, pStatus);
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
