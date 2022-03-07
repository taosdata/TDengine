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
#include "dndMgmt.h"
#include "dndBnode.h"
#include "dndMnode.h"
#include "dndQnode.h"
#include "dndSnode.h"
#include "dndTransport.h"
#include "dndVnodes.h"
#include "dndWorker.h"
#include "monitor.h"

static void dndProcessMgmtQueue(SDnode *pDnode, SRpcMsg *pMsg);

static int32_t dndReadDnodes(SDnode *pDnode);
static int32_t dndWriteDnodes(SDnode *pDnode);
static void   *dnodeThreadRoutine(void *param);

static int32_t dndProcessConfigDnodeReq(SDnode *pDnode, SRpcMsg *pReq);
static void    dndProcessStatusRsp(SDnode *pDnode, SRpcMsg *pRsp);
static void    dndProcessAuthRsp(SDnode *pDnode, SRpcMsg *pRsp);
static void    dndProcessGrantRsp(SDnode *pDnode, SRpcMsg *pRsp);

int32_t dndGetDnodeId(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);
  int32_t dnodeId = pMgmt->dnodeId;
  taosRUnLockLatch(&pMgmt->latch);
  return dnodeId;
}

int64_t dndGetClusterId(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);
  int64_t clusterId = pMgmt->clusterId;
  taosRUnLockLatch(&pMgmt->latch);
  return clusterId;
}

void dndGetDnodeEp(SDnode *pDnode, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);

  SDnodeEp *pDnodeEp = taosHashGet(pMgmt->dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    if (pPort != NULL) {
      *pPort = pDnodeEp->ep.port;
    }
    if (pFqdn != NULL) {
      tstrncpy(pFqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
    }
    if (pEp != NULL) {
      snprintf(pEp, TSDB_EP_LEN, "%s:%u", pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    }
  }

  taosRUnLockLatch(&pMgmt->latch);
}

void dndGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);
  *pEpSet = pMgmt->mnodeEpSet;
  taosRUnLockLatch(&pMgmt->latch);
}

void dndSendRedirectRsp(SDnode *pDnode, SRpcMsg *pReq) {
  tmsg_t msgType = pReq->msgType;

  SEpSet epSet = {0};
  dndGetMnodeEpSet(pDnode, &epSet);

  dDebug("RPC %p, req:%s is redirected, num:%d use:%d", pReq->handle, TMSG_INFO(msgType), epSet.numOfEps, epSet.inUse);
  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%u", i, epSet.eps[i].fqdn, epSet.eps[i].port);
    if (strcmp(epSet.eps[i].fqdn, pDnode->cfg.localFqdn) == 0 && epSet.eps[i].port == pDnode->cfg.serverPort) {
      epSet.inUse = (i + 1) % epSet.numOfEps;
    }

    epSet.eps[i].port = htons(epSet.eps[i].port);
  }

  rpcSendRedirectRsp(pReq->handle, &epSet);
}

static void dndUpdateMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);

  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosWLockLatch(&pMgmt->latch);

  pMgmt->mnodeEpSet = *pEpSet;
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dInfo("mnode index:%d %s:%u", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }

  taosWUnLockLatch(&pMgmt->latch);
}

static void dndPrintDnodes(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  int32_t numOfEps = (int32_t)taosArrayGetSize(pMgmt->pDnodeEps);
  dDebug("print dnode ep list, num:%d", numOfEps);
  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pEp = taosArrayGet(pMgmt->pDnodeEps, i);
    dDebug("dnode:%d, fqdn:%s port:%u isMnode:%d", pEp->id, pEp->ep.fqdn, pEp->ep.port, pEp->isMnode);
  }
}

static void dndResetDnodes(SDnode *pDnode, SArray *pDnodeEps) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  if (pMgmt->pDnodeEps != pDnodeEps) {
    SArray *tmp = pMgmt->pDnodeEps;
    pMgmt->pDnodeEps = taosArrayDup(pDnodeEps);
    taosArrayDestroy(tmp);
  }

  pMgmt->mnodeEpSet.inUse = 0;
  pMgmt->mnodeEpSet.numOfEps = 0;

  int32_t mIndex = 0;
  int32_t numOfEps = (int32_t)taosArrayGetSize(pDnodeEps);

  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pDnodeEp = taosArrayGet(pDnodeEps, i);
    if (!pDnodeEp->isMnode) continue;
    if (mIndex >= TSDB_MAX_REPLICA) continue;
    pMgmt->mnodeEpSet.numOfEps++;

    pMgmt->mnodeEpSet.eps[mIndex] = pDnodeEp->ep;
    mIndex++;
  }

  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pDnodeEp = taosArrayGet(pDnodeEps, i);
    taosHashPut(pMgmt->dnodeHash, &pDnodeEp->id, sizeof(int32_t), pDnodeEp, sizeof(SDnodeEp));
  }

  dndPrintDnodes(pDnode);
}

static bool dndIsEpChanged(SDnode *pDnode, int32_t dnodeId, char *pEp) {
  bool changed = false;

  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);

  SDnodeEp *pDnodeEp = taosHashGet(pMgmt->dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    char epstr[TSDB_EP_LEN + 1];
    snprintf(epstr, TSDB_EP_LEN, "%s:%u", pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    changed = strcmp(pEp, epstr) != 0;
  }

  taosRUnLockLatch(&pMgmt->latch);
  return changed;
}

static int32_t dndReadDnodes(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  pMgmt->pDnodeEps = taosArrayInit(1, sizeof(SDnodeEp));
  if (pMgmt->pDnodeEps == NULL) {
    dError("failed to calloc dnodeEp array since %s", strerror(errno));
    goto PRASE_DNODE_OVER;
  }

  int32_t code = TSDB_CODE_DND_DNODE_READ_FILE_ERROR;
  int32_t len = 0;
  int32_t maxLen = 256 * 1024;
  char   *content = calloc(1, maxLen + 1);
  cJSON  *root = NULL;

  // fp = fopen(pMgmt->file, "r");
  TdFilePtr pFile = taosOpenFile(pMgmt->file, TD_FILE_READ);
  if (pFile == NULL) {
    dDebug("file %s not exist", pMgmt->file);
    code = 0;
    goto PRASE_DNODE_OVER;
  }

  len = (int32_t)taosReadFile(pFile, content, maxLen);
  if (len <= 0) {
    dError("failed to read %s since content is null", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }

  cJSON *dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_Number) {
    dError("failed to read %s since dnodeId not found", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }
  pMgmt->dnodeId = dnodeId->valueint;

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s since clusterId not found", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }
  pMgmt->clusterId = atoll(clusterId->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    dError("failed to read %s since dropped not found", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }
  pMgmt->dropped = dropped->valueint;

  cJSON *dnodes = cJSON_GetObjectItem(root, "dnodes");
  if (!dnodes || dnodes->type != cJSON_Array) {
    dError("failed to read %s since dnodes not found", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }

  int32_t numOfDnodes = cJSON_GetArraySize(dnodes);
  if (numOfDnodes <= 0) {
    dError("failed to read %s since numOfDnodes:%d invalid", pMgmt->file, numOfDnodes);
    goto PRASE_DNODE_OVER;
  }

  for (int32_t i = 0; i < numOfDnodes; ++i) {
    cJSON *node = cJSON_GetArrayItem(dnodes, i);
    if (node == NULL) break;

    SDnodeEp dnodeEp = {0};

    cJSON *did = cJSON_GetObjectItem(node, "id");
    if (!did || did->type != cJSON_Number) {
      dError("failed to read %s since dnodeId not found", pMgmt->file);
      goto PRASE_DNODE_OVER;
    }

    dnodeEp.id = dnodeId->valueint;

    cJSON *dnodeFqdn = cJSON_GetObjectItem(node, "fqdn");
    if (!dnodeFqdn || dnodeFqdn->type != cJSON_String || dnodeFqdn->valuestring == NULL) {
      dError("failed to read %s since dnodeFqdn not found", pMgmt->file);
      goto PRASE_DNODE_OVER;
    }
    tstrncpy(dnodeEp.ep.fqdn, dnodeFqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *dnodePort = cJSON_GetObjectItem(node, "port");
    if (!dnodePort || dnodePort->type != cJSON_Number) {
      dError("failed to read %s since dnodePort not found", pMgmt->file);
      goto PRASE_DNODE_OVER;
    }

    dnodeEp.ep.port = dnodePort->valueint;

    cJSON *isMnode = cJSON_GetObjectItem(node, "isMnode");
    if (!isMnode || isMnode->type != cJSON_Number) {
      dError("failed to read %s since isMnode not found", pMgmt->file);
      goto PRASE_DNODE_OVER;
    }
    dnodeEp.isMnode = isMnode->valueint;

    taosArrayPush(pMgmt->pDnodeEps, &dnodeEp);
  }

  code = 0;
  dInfo("succcessed to read file %s", pMgmt->file);
  dndPrintDnodes(pDnode);

PRASE_DNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (dndIsEpChanged(pDnode, pMgmt->dnodeId, pDnode->cfg.localEp)) {
    dError("localEp %s different with %s and need reconfigured", pDnode->cfg.localEp, pMgmt->file);
    return -1;
  }

  if (taosArrayGetSize(pMgmt->pDnodeEps) == 0) {
    SDnodeEp dnodeEp = {0};
    dnodeEp.isMnode = 1;
    taosGetFqdnPortFromEp(pDnode->cfg.firstEp, &dnodeEp.ep);
    taosArrayPush(pMgmt->pDnodeEps, &dnodeEp);
  }

  dndResetDnodes(pDnode, pMgmt->pDnodeEps);

  terrno = 0;
  return 0;
}

static int32_t dndWriteDnodes(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  // FILE *fp = fopen(pMgmt->file, "w");
  TdFilePtr pFile = taosOpenFile(pMgmt->file, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    dError("failed to write %s since %s", pMgmt->file, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 256 * 1024;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": %d,\n", pMgmt->dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%" PRId64 "\",\n", pMgmt->clusterId);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": %d,\n", pMgmt->dropped);
  len += snprintf(content + len, maxLen - len, "  \"dnodes\": [{\n");

  int32_t numOfEps = (int32_t)taosArrayGetSize(pMgmt->pDnodeEps);
  for (int32_t i = 0; i < numOfEps; ++i) {
    SDnodeEp *pDnodeEp = taosArrayGet(pMgmt->pDnodeEps, i);
    len += snprintf(content + len, maxLen - len, "    \"id\": %d,\n", pDnodeEp->id);
    len += snprintf(content + len, maxLen - len, "    \"fqdn\": \"%s\",\n", pDnodeEp->ep.fqdn);
    len += snprintf(content + len, maxLen - len, "    \"port\": %u,\n", pDnodeEp->ep.port);
    len += snprintf(content + len, maxLen - len, "    \"isMnode\": %d\n", pDnodeEp->isMnode);
    if (i < numOfEps - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  taosWriteFile(pFile, content, len);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  free(content);
  terrno = 0;

  pMgmt->updateTime = taosGetTimestampMs();
  dDebug("successed to write %s", pMgmt->file);
  return 0;
}

void dndSendStatusReq(SDnode *pDnode) {
  SStatusReq req = {0};

  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);
  req.sver = tsVersion;
  req.dver = pMgmt->dver;
  req.dnodeId = pMgmt->dnodeId;
  req.clusterId = pMgmt->clusterId;
  req.rebootTime = pMgmt->rebootTime;
  req.updateTime = pMgmt->updateTime;
  req.numOfCores = tsNumOfCores;
  req.numOfSupportVnodes = pDnode->cfg.numOfSupportVnodes;
  memcpy(req.dnodeEp, pDnode->cfg.localEp, TSDB_EP_LEN);

  req.clusterCfg.statusInterval = tsStatusInterval;
  req.clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &req.clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  memcpy(req.clusterCfg.timezone, tsTimezone, TD_TIMEZONE_LEN);
  memcpy(req.clusterCfg.locale, tsLocale, TD_LOCALE_LEN);
  memcpy(req.clusterCfg.charset, tsCharset, TD_LOCALE_LEN);
  taosRUnLockLatch(&pMgmt->latch);

  req.pVloads = taosArrayInit(TSDB_MAX_VNODES, sizeof(SVnodeLoad));
  dndGetVnodeLoads(pDnode, req.pVloads);

  int32_t contLen = tSerializeSStatusReq(NULL, 0, &req);
  void   *pHead = rpcMallocCont(contLen);
  tSerializeSStatusReq(pHead, contLen, &req);
  taosArrayDestroy(req.pVloads);

  SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_MND_STATUS, .ahandle = (void *)9527};
  pMgmt->statusSent = 1;

  dTrace("pDnode:%p, send status req to mnode", pDnode);
  dndSendReqToMnode(pDnode, &rpcMsg);
}

static void dndUpdateDnodeCfg(SDnode *pDnode, SDnodeCfg *pCfg) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  if (pMgmt->dnodeId == 0) {
    dInfo("set dnodeId:%d clusterId:%" PRId64, pCfg->dnodeId, pCfg->clusterId);
    taosWLockLatch(&pMgmt->latch);
    pMgmt->dnodeId = pCfg->dnodeId;
    pMgmt->clusterId = pCfg->clusterId;
    dndWriteDnodes(pDnode);
    taosWUnLockLatch(&pMgmt->latch);
  }
}

static void dndUpdateDnodeEps(SDnode *pDnode, SArray *pDnodeEps) {
  int32_t numOfEps = taosArrayGetSize(pDnodeEps);
  if (numOfEps <= 0) return;

  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosWLockLatch(&pMgmt->latch);

  int32_t numOfEpsOld = (int32_t)taosArrayGetSize(pMgmt->pDnodeEps);
  if (numOfEps != numOfEpsOld) {
    dndResetDnodes(pDnode, pDnodeEps);
    dndWriteDnodes(pDnode);
  } else {
    int32_t size = numOfEps * sizeof(SDnodeEp);
    if (memcmp(pMgmt->pDnodeEps->pData, pDnodeEps->pData, size) != 0) {
      dndResetDnodes(pDnode, pDnodeEps);
      dndWriteDnodes(pDnode);
    }
  }

  taosWUnLockLatch(&pMgmt->latch);
}

static void dndProcessStatusRsp(SDnode *pDnode, SRpcMsg *pRsp) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  if (pRsp->code != TSDB_CODE_SUCCESS) {
    if (pRsp->code == TSDB_CODE_MND_DNODE_NOT_EXIST && !pMgmt->dropped && pMgmt->dnodeId > 0) {
      dInfo("dnode:%d, set to dropped since not exist in mnode", pMgmt->dnodeId);
      pMgmt->dropped = 1;
      dndWriteDnodes(pDnode);
    }
  } else {
    SStatusRsp statusRsp = {0};
    if (pRsp->pCont != NULL && pRsp->contLen != 0 &&
        tDeserializeSStatusRsp(pRsp->pCont, pRsp->contLen, &statusRsp) == 0) {
      pMgmt->dver = statusRsp.dver;
      dndUpdateDnodeCfg(pDnode, &statusRsp.dnodeCfg);
      dndUpdateDnodeEps(pDnode, statusRsp.pDnodeEps);
    }
    taosArrayDestroy(statusRsp.pDnodeEps);
  }

  pMgmt->statusSent = 0;
}

static void dndProcessAuthRsp(SDnode *pDnode, SRpcMsg *pReq) { dError("auth rsp is received, but not supported yet"); }

static void dndProcessGrantRsp(SDnode *pDnode, SRpcMsg *pReq) {
  dError("grant rsp is received, but not supported yet");
}

static int32_t dndProcessConfigDnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
  dError("config req is received, but not supported yet");
  SDCfgDnodeReq *pCfg = pReq->pCont;
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pReq) {
  dDebug("startup req is received");

  SStartupReq *pStartup = rpcMallocCont(sizeof(SStartupReq));
  dndGetStartup(pDnode, pStartup);

  dDebug("startup req is sent, step:%s desc:%s finished:%d", pStartup->name, pStartup->desc, pStartup->finished);

  SRpcMsg rpcRsp = {.handle = pReq->handle, .pCont = pStartup, .contLen = sizeof(SStartupReq)};
  rpcSendResponse(&rpcRsp);
}

static void dndGetMonitorBasicInfo(SDnode *pDnode, SMonBasicInfo *pInfo) {
  pInfo->dnode_id = dndGetDnodeId(pDnode);
  tstrncpy(pInfo->dnode_ep, tsLocalEp, TSDB_EP_LEN);
  pInfo->cluster_id = dndGetClusterId(pDnode);
  pInfo->protocol = 1;
}

static void dndGetMonitorDnodeInfo(SDnode *pDnode, SMonDnodeInfo *pInfo) {
  pInfo->uptime = (taosGetTimestampMs() - pDnode->dmgmt.rebootTime) / (86400000.0f);
  taosGetCpuUsage(&pInfo->cpu_engine, &pInfo->cpu_system);
  pInfo->cpu_cores = tsNumOfCores;
  taosGetProcMemory(&pInfo->mem_engine);
  taosGetSysMemory(&pInfo->mem_system);
  pInfo->mem_total = tsTotalMemoryKB;
  pInfo->disk_engine = 0;
  pInfo->disk_used = tsDataSpace.size.used;
  pInfo->disk_total = tsDataSpace.size.total;
  taosGetCardInfo(&pInfo->net_in, &pInfo->net_out);
  taosGetProcIO(&pInfo->io_read, &pInfo->io_write, &pInfo->io_read_disk, &pInfo->io_write_disk);

  SVnodesStat *pStat = &pDnode->vmgmt.stat;
  pInfo->req_select = pStat->numOfSelectReqs;
  pInfo->req_insert = pStat->numOfInsertReqs;
  pInfo->req_insert_success = pStat->numOfInsertSuccessReqs;
  pInfo->req_insert_batch = pStat->numOfBatchInsertReqs;
  pInfo->req_insert_batch_success = pStat->numOfBatchInsertSuccessReqs;
  pInfo->errors = tsNumOfErrorLogs;
  pInfo->vnodes_num = pStat->totalVnodes;
  pInfo->masters = pStat->masterNum;
  pInfo->has_mnode = pDnode->mmgmt.deployed;
}

static void dndSendMonitorReport(SDnode *pDnode) {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  dTrace("pDnode:%p, send monitor report to %s:%u", pDnode, tsMonitorFqdn, tsMonitorPort);

  SMonInfo *pMonitor = monCreateMonitorInfo();
  if (pMonitor == NULL) return;

  SMonBasicInfo basicInfo = {0};
  dndGetMonitorBasicInfo(pDnode, &basicInfo);
  monSetBasicInfo(pMonitor, &basicInfo);

  SMonClusterInfo clusterInfo = {0};
  SMonVgroupInfo  vgroupInfo = {0};
  SMonGrantInfo   grantInfo = {0};
  if (dndGetMnodeMonitorInfo(pDnode, &clusterInfo, &vgroupInfo, &grantInfo) == 0) {
    monSetClusterInfo(pMonitor, &clusterInfo);
    monSetVgroupInfo(pMonitor, &vgroupInfo);
    monSetGrantInfo(pMonitor, &grantInfo);
  }

  SMonDnodeInfo dnodeInfo = {0};
  dndGetMonitorDnodeInfo(pDnode, &dnodeInfo);
  monSetDnodeInfo(pMonitor, &dnodeInfo);

  SMonDiskInfo diskInfo = {0};
  if (dndGetMonitorDiskInfo(pDnode, &diskInfo) == 0) {
    monSetDiskInfo(pMonitor, &diskInfo);
  }

  taosArrayDestroy(clusterInfo.dnodes);
  taosArrayDestroy(clusterInfo.mnodes);
  taosArrayDestroy(vgroupInfo.vgroups);
  taosArrayDestroy(diskInfo.datadirs);

  monSendReport(pMonitor);
  monCleanupMonitorInfo(pMonitor);
}

static void *dnodeThreadRoutine(void *param) {
  SDnode     *pDnode = param;
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  int64_t     lastStatusTime = taosGetTimestampMs();
  int64_t     lastMonitorTime = lastStatusTime;

  setThreadName("dnode-hb");

  while (true) {
    pthread_testcancel();
    taosMsleep(200);
    if (dndGetStat(pDnode) != DND_STAT_RUNNING || pMgmt->dropped) {
      continue;
    }

    int64_t curTime = taosGetTimestampMs();

    float statusInterval = (curTime - lastStatusTime) / 1000.0f;
    if (statusInterval >= tsStatusInterval && !pMgmt->statusSent) {
      dndSendStatusReq(pDnode);
      lastStatusTime = curTime;
    }

    float monitorInterval = (curTime - lastMonitorTime) / 1000.0f;
    if (monitorInterval >= tsMonitorInterval) {
      dndSendMonitorReport(pDnode);
      lastMonitorTime = curTime;
    }
  }
}

int32_t dndInitMgmt(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  pMgmt->dnodeId = 0;
  pMgmt->rebootTime = taosGetTimestampMs();
  pMgmt->dropped = 0;
  pMgmt->clusterId = 0;
  taosInitRWLatch(&pMgmt->latch);

  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s/dnode.json", pDnode->dir.dnode);
  pMgmt->file = strdup(path);
  if (pMgmt->file == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMgmt->dnodeHash == NULL) {
    dError("failed to init dnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dndReadDnodes(pDnode) != 0) {
    dError("failed to read file:%s since %s", pMgmt->file, terrstr());
    return -1;
  }

  if (pMgmt->dropped) {
    dError("dnode not start since its already dropped");
    return -1;
  }

  if (dndInitWorker(pDnode, &pMgmt->mgmtWorker, DND_WORKER_SINGLE, "dnode-mgmt", 1, 1, dndProcessMgmtQueue) != 0) {
    dError("failed to start dnode mgmt worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pDnode, &pMgmt->statusWorker, DND_WORKER_SINGLE, "dnode-status", 1, 1, dndProcessMgmtQueue) != 0) {
    dError("failed to start dnode mgmt worker since %s", terrstr());
    return -1;
  }

  pMgmt->threadId = taosCreateThread(dnodeThreadRoutine, pDnode);
  if (pMgmt->threadId == NULL) {
    dError("failed to init dnode thread");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dInfo("dnode-mgmt is initialized");
  return 0;
}

void dndStopMgmt(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  dndCleanupWorker(&pMgmt->mgmtWorker);
  dndCleanupWorker(&pMgmt->statusWorker);

  if (pMgmt->threadId != NULL) {
    taosDestoryThread(pMgmt->threadId);
    pMgmt->threadId = NULL;
  }
}

void dndCleanupMgmt(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosWLockLatch(&pMgmt->latch);

  if (pMgmt->pDnodeEps != NULL) {
    taosArrayDestroy(pMgmt->pDnodeEps);
    pMgmt->pDnodeEps = NULL;
  }

  if (pMgmt->dnodeHash != NULL) {
    taosHashCleanup(pMgmt->dnodeHash);
    pMgmt->dnodeHash = NULL;
  }

  if (pMgmt->file != NULL) {
    free(pMgmt->file);
    pMgmt->file = NULL;
  }

  taosWUnLockLatch(&pMgmt->latch);
  dInfo("dnode-mgmt is cleaned up");
}

void dndProcessMgmtMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  if (pEpSet && pEpSet->numOfEps > 0 && pMsg->msgType == TDMT_MND_STATUS_RSP) {
    dndUpdateMnodeEpSet(pDnode, pEpSet);
  }

  SDnodeWorker *pWorker = &pMgmt->mgmtWorker;
  if (pMsg->msgType == TDMT_MND_STATUS_RSP) {
    pWorker = &pMgmt->statusWorker;
  }

  if (dndWriteMsgToWorker(pWorker, pMsg, sizeof(SRpcMsg)) != 0) {
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .code = TSDB_CODE_OUT_OF_MEMORY};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static void dndProcessMgmtQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  switch (pMsg->msgType) {
    case TDMT_DND_CREATE_MNODE:
      code = dndProcessCreateMnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_ALTER_MNODE:
      code = dndProcessAlterMnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_MNODE:
      code = dndProcessDropMnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_QNODE:
      code = dndProcessCreateQnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_QNODE:
      code = dndProcessDropQnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_SNODE:
      code = dndProcessCreateSnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_SNODE:
      code = dndProcessDropSnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_BNODE:
      code = dndProcessCreateBnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_BNODE:
      code = dndProcessDropBnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CONFIG_DNODE:
      code = dndProcessConfigDnodeReq(pDnode, pMsg);
      break;
    case TDMT_MND_STATUS_RSP:
      dndProcessStatusRsp(pDnode, pMsg);
      break;
    case TDMT_MND_AUTH_RSP:
      dndProcessAuthRsp(pDnode, pMsg);
      break;
    case TDMT_MND_GRANT_RSP:
      dndProcessGrantRsp(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_VNODE:
      code = dndProcessCreateVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_ALTER_VNODE:
      code = dndProcessAlterVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_VNODE:
      code = dndProcessDropVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_SYNC_VNODE:
      code = dndProcessSyncVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_COMPACT_VNODE:
      code = dndProcessCompactVnodeReq(pDnode, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      code = -1;
      dError("RPC %p, dnode msg:%s not processed", pMsg->handle, TMSG_INFO(pMsg->msgType));
      break;
  }

  if (pMsg->msgType & 1u) {
    if (code != 0) code = terrno;
    SRpcMsg rsp = {.code = code, .handle = pMsg->handle, .ahandle = pMsg->ahandle};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
  taosFreeQitem(pMsg);
}
