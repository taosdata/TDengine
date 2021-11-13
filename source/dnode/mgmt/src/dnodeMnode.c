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
#include "dnodeMnode.h"
#include "cJSON.h"
#include "dnodeDnode.h"
#include "dnodeTransport.h"
#include "mnode.h"
#include "tlockfree.h"
#include "tqueue.h"
#include "tstep.h"
#include "tworker.h"

static struct {
  int32_t     refCount;
  int8_t      deployed;
  int8_t      dropped;
  SWorkerPool mgmtPool;
  SWorkerPool readPool;
  SWorkerPool writePool;
  SWorkerPool syncPool;
  taos_queue  pReadQ;
  taos_queue  pWriteQ;
  taos_queue  pApplyQ;
  taos_queue  pSyncQ;
  taos_queue  pMgmtQ;
  SSteps     *pSteps;
  SRWLatch    latch;
} tsMnode = {0};

static int32_t dnodeAllocMnodeReadQueue();
static void    dnodeFreeMnodeReadQueue();
static int32_t dnodeAllocMnodeWriteQueue();
static void    dnodeFreeMnodeWriteQueue();
static int32_t dnodeAllocMnodeApplyQueue();
static void    dnodeFreeMnodeApplyQueue();
static int32_t dnodeAllocMnodeSyncQueue();
static void    dnodeFreeMnodeSyncQueue();

static int32_t dnodeAcquireMnode() {
  taosRLockLatch(&tsMnode.latch);

  int32_t code = tsMnode.deployed ? 0 : TSDB_CODE_DND_MNODE_NOT_DEPLOYED;
  if (code == 0) {
    atomic_add_fetch_32(&tsMnode.refCount, 1);
  }

  taosRUnLockLatch(&tsMnode.latch);
  return code;
}

static void dnodeReleaseMnode() { atomic_sub_fetch_32(&tsMnode.refCount, 1); }

static int32_t dnodeReadMnodeFile() {
  int32_t code = TSDB_CODE_DND_READ_MNODE_FILE_ERROR;
  int32_t len = 0;
  int32_t maxLen = 300;
  char   *content = calloc(1, maxLen + 1);
  cJSON  *root = NULL;
  FILE   *fp = NULL;
  char    file[PATH_MAX + 20] = {0};

  snprintf(file, sizeof(file), "%s/mnode.json", tsDnodeDir);
  fp = fopen(file, "r");
  if (!fp) {
    dDebug("file %s not exist", file);
    code = 0;
    goto PRASE_MNODE_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", file);
    goto PRASE_MNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", file);
    goto PRASE_MNODE_OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_String) {
    dError("failed to read %s since deployed not found", file);
    goto PRASE_MNODE_OVER;
  }
  tsMnode.deployed = atoi(deployed->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_String) {
    dError("failed to read %s since dropped not found", file);
    goto PRASE_MNODE_OVER;
  }
  tsMnode.dropped = atoi(dropped->valuestring);

  code = 0;
  dInfo("succcessed to read file %s", file);

PRASE_MNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  return code;
}

static int32_t dnodeWriteMnodeFile() {
  char file[PATH_MAX + 20] = {0};
  char realfile[PATH_MAX + 20] = {0};
  snprintf(file, sizeof(file), "%s/mnode.json.bak", tsDnodeDir);
  snprintf(realfile, sizeof(realfile), "%s/mnode.json", tsDnodeDir);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    dError("failed to write %s since %s", file, strerror(errno));
    return TSDB_CODE_DND_WRITE_MNODE_FILE_ERROR;
  }

  int32_t len = 0;
  int32_t maxLen = 300;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"deployed\": \"%d\",\n", tsMnode.deployed);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": \"%d\"\n", tsMnode.dropped);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsyncFile(fileno(fp));
  fclose(fp);
  free(content);

  int32_t code = taosRenameFile(file, realfile);
  if (code != 0) {
    dError("failed to rename %s since %s", file, tstrerror(code));
    return TSDB_CODE_DND_WRITE_MNODE_FILE_ERROR;
  }

  dInfo("successed to write %s", realfile);
  return 0;
}

static int32_t dnodeStartMnode() {
  int32_t code = dnodeAllocMnodeReadQueue();
  if (code != 0) {
    return code;
  }

  code = dnodeAllocMnodeWriteQueue();
  if (code != 0) {
    return code;
  }

  code = dnodeAllocMnodeApplyQueue();
  if (code != 0) {
    return code;
  }

  code = dnodeAllocMnodeSyncQueue();
  if (code != 0) {
    return code;
  }

  taosWLockLatch(&tsMnode.latch);
  tsMnode.deployed = 1;
  taosWUnLockLatch(&tsMnode.latch);

  return mnodeStart(NULL);
}

static void dnodeStopMnode() {
  taosWLockLatch(&tsMnode.latch);
  tsMnode.deployed = 0;
  taosWUnLockLatch(&tsMnode.latch);

  dnodeReleaseMnode();

  while (tsMnode.refCount > 0) taosMsleep(10);
  while (!taosQueueEmpty(tsMnode.pReadQ)) taosMsleep(10);
  while (!taosQueueEmpty(tsMnode.pApplyQ)) taosMsleep(10);
  while (!taosQueueEmpty(tsMnode.pWriteQ)) taosMsleep(10);
  while (!taosQueueEmpty(tsMnode.pSyncQ)) taosMsleep(10);

  dnodeFreeMnodeReadQueue();
  dnodeFreeMnodeWriteQueue();
  dnodeFreeMnodeApplyQueue();
  dnodeFreeMnodeSyncQueue();
}

static int32_t dnodeUnDeployMnode() {
  tsMnode.dropped = 1;
  int32_t code = dnodeWriteMnodeFile();
  if (code != 0) {
    tsMnode.dropped = 0;
    dError("failed to undeploy mnode since %s", tstrerror(code));
    return code;
  }

  dnodeStopMnode();
  mnodeUnDeploy();
  dnodeWriteMnodeFile();

  return code;
}

static int32_t dnodeDeployMnode(SMnodeCfg *pCfg) {
  int32_t code = mnodeDeploy(pCfg);
  if (code != 0) {
    dError("failed to deploy mnode since %s", tstrerror(code));
    return code;
  }

  code = dnodeStartMnode();
  if (code != 0) {
    dnodeUnDeployMnode();
    dError("failed to deploy mnode since %s", tstrerror(code));
    return code;
  }

  code = dnodeWriteMnodeFile();
  if (code != 0) {
    dnodeUnDeployMnode();
    dError("failed to deploy mnode since %s", tstrerror(code));
    return code;
  }

  dInfo("deploy mnode success");
  return code;
}

static int32_t dnodeAlterMnode(SMnodeCfg *pCfg) {
  int32_t code = dnodeAcquireMnode();
  if (code == 0) {
    code = mnodeAlter(pCfg);
    dnodeReleaseMnode();
  }
  return code;
}

static SCreateMnodeMsg *dnodeParseCreateMnodeMsg(SRpcMsg *pRpcMsg) {
  SCreateMnodeMsg *pMsg = pRpcMsg->pCont;
  pMsg->dnodeId = htonl(pMsg->dnodeId);
  for (int32_t i = 0; i < pMsg->replica; ++i) {
    pMsg->replicas[i].port = htons(pMsg->replicas[i].port);
  }
  return pMsg;
}

static int32_t dnodeProcessCreateMnodeReq(SRpcMsg *pRpcMsg) {
  SAlterMnodeMsg *pMsg = (SAlterMnodeMsg *)dnodeParseCreateMnodeMsg(pRpcMsg->pCont);

  if (pMsg->dnodeId != dnodeGetDnodeId()) {
    return TSDB_CODE_DND_MNODE_ID_NOT_MATCH_DNODE;
  } else {
    SMnodeCfg cfg = {0};
    cfg.replica = pMsg->replica;
    memcpy(cfg.replicas, pMsg->replicas, sizeof(SReplica) * sizeof(TSDB_MAX_REPLICA));
    return dnodeDeployMnode(&cfg);
  }
}

static int32_t dnodeProcessAlterMnodeReq(SRpcMsg *pRpcMsg) {
  SAlterMnodeMsg *pMsg = (SAlterMnodeMsg *)dnodeParseCreateMnodeMsg(pRpcMsg->pCont);
  if (pMsg->dnodeId != dnodeGetDnodeId()) {
    return TSDB_CODE_DND_MNODE_ID_NOT_MATCH_DNODE;
  } else {
    SMnodeCfg cfg = {0};
    cfg.replica = pMsg->replica;
    memcpy(cfg.replicas, pMsg->replicas, sizeof(SReplica) * sizeof(TSDB_MAX_REPLICA));
    return dnodeAlterMnode(&cfg);
  }
}

static int32_t dnodeProcessDropMnodeReq(SRpcMsg *pMsg) {
  SAlterMnodeMsg *pCfg = pMsg->pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);

  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    return TSDB_CODE_DND_MNODE_ID_NOT_MATCH_DNODE;
  } else {
    return dnodeUnDeployMnode();
  }
}

static void dnodeProcessMnodeMgmtQueue(void *unused, SRpcMsg *pMsg) {
  int32_t code = 0;

  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_CREATE_MNODE_IN:
      code = dnodeProcessCreateMnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_ALTER_MNODE_IN:
      code = dnodeProcessAlterMnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_DROP_MNODE_IN:
      code = dnodeProcessDropMnodeReq(pMsg);
      break;
    default:
      code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
      break;
  }

  SRpcMsg rsp = {.code = code, .handle = pMsg->handle};
  rpcSendResponse(&rsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void dnodeProcessMnodeReadQueue(void *unused, SMnodeMsg *pMsg) { mnodeProcessMsg(pMsg, MN_MSG_TYPE_READ); }

static void dnodeProcessMnodeWriteQueue(void *unused, SMnodeMsg *pMsg) { mnodeProcessMsg(pMsg, MN_MSG_TYPE_WRITE); }

static void dnodeProcessMnodeApplyQueue(void *unused, SMnodeMsg *pMsg) { mnodeProcessMsg(pMsg, MN_MSG_TYPE_APPLY); }

static void dnodeProcessMnodeSyncQueue(void *unused, SMnodeMsg *pMsg) { mnodeProcessMsg(pMsg, MN_MSG_TYPE_SYNC); }

static int32_t dnodeWriteMnodeMsgToQueue(taos_queue pQueue, SRpcMsg *pRpcMsg) {
  int32_t code = 0;

  if (pQueue == NULL) {
    code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
  } else {
    SMnodeMsg *pMsg = mnodeInitMsg(pRpcMsg);
    if (pMsg == NULL) {
      code = terrno;
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {.handle = pRpcMsg->handle, .code = code};
    rpcSendResponse(&rsp);
    rpcFreeCont(pRpcMsg->pCont);
  }
}

void dnodeProcessMnodeMgmtMsg(SRpcMsg *pMsg, SEpSet *pEpSet) { dnodeWriteMnodeMsgToQueue(tsMnode.pMgmtQ, pMsg); }

void dnodeProcessMnodeWriteMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  if (dnodeAcquireMnode() == 0) {
    dnodeWriteMnodeMsgToQueue(tsMnode.pWriteQ, pMsg);
    dnodeReleaseMnode();
  } else {
    dnodeSendRedirectMsg(pMsg, 0);
  }
}

void dnodeProcessMnodeSyncMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  int32_t code = dnodeAcquireMnode();
  if (code == 0) {
    dnodeWriteMnodeMsgToQueue(tsMnode.pSyncQ, pMsg);
    dnodeReleaseMnode();
  } else {
    SRpcMsg rsp = {.handle = pMsg->handle, .code = code};
    rpcSendResponse(&rsp);
    rpcFreeCont(pMsg->pCont);
  }
}

void dnodeProcessMnodeReadMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  if (dnodeAcquireMnode() == 0) {
    dnodeWriteMnodeMsgToQueue(tsMnode.pReadQ, pMsg);
    dnodeReleaseMnode();
  } else {
    dnodeSendRedirectMsg(pMsg, 0);
  }
}

static int32_t dnodePutMsgIntoMnodeApplyQueue(SMnodeMsg *pMsg) {
  int32_t code = dnodeAcquireMnode();
  if (code != 0) return code;

  code = taosWriteQitem(tsMnode.pApplyQ, pMsg);
  dnodeReleaseMnode();
  return code;
}

static int32_t dnodeAllocMnodeMgmtQueue() {
  tsMnode.pMgmtQ = tWorkerAllocQueue(&tsMnode.mgmtPool, NULL, (FProcessItem)dnodeProcessMnodeMgmtQueue);
  if (tsMnode.pMgmtQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeMnodeMgmtQueue() {
  tWorkerFreeQueue(&tsMnode.mgmtPool, tsMnode.pMgmtQ);
  tsMnode.pMgmtQ = NULL;
}

static int32_t dnodeInitMnodeMgmtWorker() {
  SWorkerPool *pPool = &tsMnode.mgmtPool;
  pPool->name = "mnode-mgmt";
  pPool->min = 1;
  pPool->max = 1;
  return tWorkerInit(pPool);
}

static void dnodeCleanupMnodeMgmtWorker() { tWorkerCleanup(&tsMnode.mgmtPool); }

static int32_t dnodeAllocMnodeReadQueue() {
  tsMnode.pReadQ = tWorkerAllocQueue(&tsMnode.readPool, NULL, (FProcessItem)dnodeProcessMnodeReadQueue);
  if (tsMnode.pReadQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeMnodeReadQueue() {
  tWorkerFreeQueue(&tsMnode.readPool, tsMnode.pReadQ);
  tsMnode.pReadQ = NULL;
}

static int32_t dnodeInitMnodeReadWorker() {
  SWorkerPool *pPool = &tsMnode.readPool;
  pPool->name = "mnode-read";
  pPool->min = 0;
  pPool->max = 1;
  return tWorkerInit(pPool);
}

static void dnodeCleanupMnodeReadWorker() { tWorkerCleanup(&tsMnode.readPool); }

static int32_t dnodeAllocMnodeWriteQueue() {
  tsMnode.pWriteQ = tWorkerAllocQueue(&tsMnode.writePool, NULL, (FProcessItem)dnodeProcessMnodeWriteQueue);
  if (tsMnode.pWriteQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeMnodeWriteQueue() {
  tWorkerFreeQueue(&tsMnode.writePool, tsMnode.pWriteQ);
  tsMnode.pWriteQ = NULL;
}

static int32_t dnodeAllocMnodeApplyQueue() {
  tsMnode.pApplyQ = tWorkerAllocQueue(&tsMnode.writePool, NULL, (FProcessItem)dnodeProcessMnodeApplyQueue);
  if (tsMnode.pApplyQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeMnodeApplyQueue() {
  tWorkerFreeQueue(&tsMnode.writePool, tsMnode.pApplyQ);
  tsMnode.pApplyQ = NULL;
}

static int32_t dnodeInitMnodeWriteWorker() {
  SWorkerPool *pPool = &tsMnode.writePool;
  pPool->name = "mnode-write";
  pPool->min = 0;
  pPool->max = 1;
  return tWorkerInit(pPool);
}

static void dnodeCleanupMnodeWriteWorker() { tWorkerCleanup(&tsMnode.writePool); }

static int32_t dnodeAllocMnodeSyncQueue() {
  tsMnode.pSyncQ = tWorkerAllocQueue(&tsMnode.syncPool, NULL, (FProcessItem)dnodeProcessMnodeSyncQueue);
  if (tsMnode.pSyncQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeMnodeSyncQueue() {
  tWorkerFreeQueue(&tsMnode.syncPool, tsMnode.pSyncQ);
  tsMnode.pSyncQ = NULL;
}

static int32_t dnodeInitMnodeSyncWorker() {
  SWorkerPool *pPool = &tsMnode.syncPool;
  pPool->name = "mnode-sync";
  pPool->min = 0;
  pPool->max = 1;
  return tWorkerInit(pPool);
}

static void dnodeCleanupMnodeSyncWorker() { tWorkerCleanup(&tsMnode.syncPool); }

static int32_t dnodeInitMnodeModule() {
  taosInitRWLatch(&tsMnode.latch);

  SMnodePara para;
  para.dnodeId = dnodeGetDnodeId();
  para.clusterId = dnodeGetClusterId();
  para.SendMsgToDnode = dnodeSendMsgToDnode;
  para.SendMsgToMnode = dnodeSendMsgToMnode;
  para.SendRedirectMsg = dnodeSendRedirectMsg;

  return mnodeInit(para);
}

static void dnodeCleanupMnodeModule() { mnodeCleanup(); }

static bool dnodeNeedDeployMnode() {
  if (dnodeGetDnodeId() > 0) return false;
  if (dnodeGetClusterId() > 0) return false;
  if (strcmp(tsFirst, tsLocalEp) != 0) return false;
  return true;
}

static int32_t dnodeOpenMnode() {
  int32_t code = dnodeReadMnodeFile();
  if (code != 0) {
    dError("failed to read open mnode since %s", tstrerror(code));
    return code;
  }

  if (tsMnode.dropped) {
    dInfo("mnode already dropped, undeploy it");
    return dnodeUnDeployMnode();
  }

  if (!tsMnode.deployed) {
    bool needDeploy = dnodeNeedDeployMnode();
    if (!needDeploy) return 0;

    dInfo("start to deploy mnode");
    SMnodeCfg cfg = {.replica = 1};
    cfg.replicas[0].port = tsServerPort;
    tstrncpy(cfg.replicas[0].fqdn, tsLocalFqdn, TSDB_FQDN_LEN);
    code = dnodeDeployMnode(&cfg);
  } else {
    dInfo("start to open mnode");
    return dnodeStartMnode();
  }
}

static void dnodeCloseMnode() {
  if (dnodeAcquireMnode() == 0) {
    dnodeStopMnode();
  }
}

int32_t dnodeInitMnode() {
  dInfo("dnode-mnode start to init");

  SSteps *pSteps = taosStepInit(6, dnodeReportStartup);
  taosStepAdd(pSteps, "dnode-mnode-env", dnodeInitMnodeModule, dnodeCleanupMnodeModule);
  taosStepAdd(pSteps, "dnode-mnode-mgmt", dnodeInitMnodeMgmtWorker, dnodeCleanupMnodeMgmtWorker);
  taosStepAdd(pSteps, "dnode-mnode-read", dnodeInitMnodeReadWorker, dnodeCleanupMnodeReadWorker);
  taosStepAdd(pSteps, "dnode-mnode-write", dnodeInitMnodeWriteWorker, dnodeCleanupMnodeWriteWorker);
  taosStepAdd(pSteps, "dnode-mnode-sync", dnodeInitMnodeSyncWorker, dnodeCleanupMnodeSyncWorker);
  taosStepAdd(pSteps, "dnode-mnode", dnodeOpenMnode, dnodeCloseMnode);

  tsMnode.pSteps = pSteps;
  int32_t code = taosStepExec(pSteps);

  if (code != 0) {
    dError("dnode-mnode init failed since %s", tstrerror(code));
  } else {
    dInfo("dnode-mnode is initialized");
  }
}

void dnodeCleanupMnode() {
  if (tsMnode.pSteps == NULL) {
    dInfo("dnode-mnode start to clean up");
    taosStepCleanup(tsMnode.pSteps);
    tsMnode.pSteps = NULL;
    dInfo("dnode-mnode is cleaned up");
  }
}

int32_t dnodeGetUserAuthFromMnode(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  int32_t code = dnodeAcquireMnode();
  if (code != 0) {
    dTrace("failed to get user auth since mnode not deployed");
    return code;
  }

  code = mnodeRetriveAuth(user, spi, encrypt, secret, ckey);
  dnodeReleaseMnode();
  return code;
}