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
#include "dndDnode.h"
#include "dndTransport.h"
#include "dndVnodes.h"

static inline void dndRLockDnode(SDnode *pDnode) { taosRLockLatch(&pDnode->dmgmt.latch); }

static inline void dndRUnLockDnode(SDnode *pDnode) { taosRUnLockLatch(&pDnode->dmgmt.latch); }

static inline void dndWLockDnode(SDnode *pDnode) { taosWLockLatch(&pDnode->dmgmt.latch); }

static inline void dndWUnLockDnode(SDnode *pDnode) { taosWUnLockLatch(&pDnode->dmgmt.latch); }

int32_t dndGetDnodeId(SDnode *pDnode) {
  dndRLockDnode(pDnode);
  int32_t dnodeId = pDnode->dmgmt.dnodeId;
  dndRUnLockDnode(pDnode);
  return dnodeId;
}

int64_t dndGetClusterId(SDnode *pDnode) {
  dndRLockDnode(pDnode);
  int64_t clusterId = pDnode->dmgmt.clusterId;
  dndRUnLockDnode(pDnode);
  return clusterId;
}

void dndGetDnodeEp(SDnode *pDnode, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort) {
  dndRLockDnode(pDnode);

  SDnodeEp *pDnodeEp = taosHashGet(pDnode->dmgmt.dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    if (pPort != NULL) {
      *pPort = pDnodeEp->port;
    }
    if (pFqdn != NULL) {
      tstrncpy(pFqdn, pDnodeEp->fqdn, TSDB_FQDN_LEN);
    }
    if (pEp != NULL) {
      snprintf(pEp, TSDB_EP_LEN, "%s:%u", pDnodeEp->fqdn, pDnodeEp->port);
    }
  }

  dndRUnLockDnode(pDnode);
}

void dndGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  dndRLockDnode(pDnode);
  *pEpSet = pDnode->dmgmt.mnodeEpSet;
  dndRUnLockDnode(pDnode);
}

void dndSendRedirectMsg(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t msgType = pMsg->msgType;

  SEpSet epSet = {0};
  dndGetMnodeEpSet(pDnode, &epSet);

  dDebug("RPC %p, msg:%s is redirected, num:%d inUse:%d", pMsg->handle, taosMsg[msgType], epSet.numOfEps, epSet.inUse);
  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%u", i, epSet.fqdn[i], epSet.port[i]);
    if (strcmp(epSet.fqdn[i], pDnode->opt.localFqdn) == 0 && epSet.port[i] == pDnode->opt.serverPort) {
      epSet.inUse = (i + 1) % epSet.numOfEps;
    }

    epSet.port[i] = htons(epSet.port[i]);
  }

  rpcSendRedirectRsp(pMsg->handle, &epSet);
}

static void dndUpdateMnodeEpSet(SDnode *pDnd, SEpSet *pEpSet) {
  dInfo("mnode is changed, num:%d inUse:%d", pEpSet->numOfEps, pEpSet->inUse);

  dndWLockDnode(pDnd);

  pDnd->dmgmt.mnodeEpSet = *pEpSet;
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dInfo("mnode index:%d %s:%u", i, pEpSet->fqdn[i], pEpSet->port[i]);
  }

  dndWUnLockDnode(pDnd);
}

static void dndPrintDnodes(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  dDebug("print dnode ep list, num:%d", pMgmt->dnodeEps->num);
  for (int32_t i = 0; i < pMgmt->dnodeEps->num; i++) {
    SDnodeEp *pEp = &pMgmt->dnodeEps->eps[i];
    dDebug("dnode:%d, fqdn:%s port:%u isMnode:%d", pEp->id, pEp->fqdn, pEp->port, pEp->isMnode);
  }
}

static void dndResetDnodes(SDnode *pDnode, SDnodeEps *pDnodeEps) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  int32_t size = sizeof(SDnodeEps) + pDnodeEps->num * sizeof(SDnodeEp);
  if (pDnodeEps->num > pMgmt->dnodeEps->num) {
    SDnodeEps *tmp = calloc(1, size);
    if (tmp == NULL) return;

    tfree(pMgmt->dnodeEps);
    pMgmt->dnodeEps = tmp;
  }

  if (pMgmt->dnodeEps != pDnodeEps) {
    memcpy(pMgmt->dnodeEps, pDnodeEps, size);
  }

  pMgmt->mnodeEpSet.inUse = 0;

  int32_t mIndex = 0;
  for (int32_t i = 0; i < pMgmt->dnodeEps->num; i++) {
    SDnodeEp *pDnodeEp = &pMgmt->dnodeEps->eps[i];
    if (!pDnodeEp->isMnode) continue;
    if (mIndex >= TSDB_MAX_REPLICA) continue;
    strcpy(pMgmt->mnodeEpSet.fqdn[mIndex], pDnodeEp->fqdn);
    pMgmt->mnodeEpSet.port[mIndex] = pDnodeEp->port;
    mIndex++;
  }

  for (int32_t i = 0; i < pMgmt->dnodeEps->num; ++i) {
    SDnodeEp *pDnodeEp = &pMgmt->dnodeEps->eps[i];
    taosHashPut(pMgmt->dnodeHash, &pDnodeEp->id, sizeof(int32_t), pDnodeEp, sizeof(SDnodeEp));
  }

  dndPrintDnodes(pDnode);
}

static bool dndIsEpChanged(SDnode *pDnode, int32_t dnodeId, char *pEp) {
  bool changed = false;
  dndRLockDnode(pDnode);

  SDnodeEp *pDnodeEp = taosHashGet(pDnode->dmgmt.dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    char epstr[TSDB_EP_LEN + 1];
    snprintf(epstr, TSDB_EP_LEN, "%s:%u", pDnodeEp->fqdn, pDnodeEp->port);
    changed = strcmp(pEp, epstr) != 0;
  }

  dndRUnLockDnode(pDnode);
  return changed;
}

static int32_t dndReadDnodes(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  int32_t code = TSDB_CODE_DND_DNODE_READ_FILE_ERROR;
  int32_t len = 0;
  int32_t maxLen = 30000;
  char   *content = calloc(1, maxLen + 1);
  cJSON  *root = NULL;
  FILE   *fp = NULL;

  fp = fopen(pMgmt->file, "r");
  if (fp == NULL) {
    dDebug("file %s not exist", pMgmt->file);
    code = 0;
    goto PRASE_DNODE_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
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
  if (!dnodeId || dnodeId->type != cJSON_String) {
    dError("failed to read %s since dnodeId not found", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }
  pMgmt->dnodeId = atoi(dnodeId->valuestring);

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s since clusterId not found", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }
  pMgmt->clusterId = atoll(clusterId->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_String) {
    dError("failed to read %s since dropped not found", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }
  pMgmt->dropped = atoi(dropped->valuestring);

  cJSON *dnodeInfos = cJSON_GetObjectItem(root, "dnodeInfos");
  if (!dnodeInfos || dnodeInfos->type != cJSON_Array) {
    dError("failed to read %s since dnodeInfos not found", pMgmt->file);
    goto PRASE_DNODE_OVER;
  }

  int32_t dnodeInfosSize = cJSON_GetArraySize(dnodeInfos);
  if (dnodeInfosSize <= 0) {
    dError("failed to read %s since dnodeInfos size:%d invalid", pMgmt->file, dnodeInfosSize);
    goto PRASE_DNODE_OVER;
  }

  pMgmt->dnodeEps = calloc(1, dnodeInfosSize * sizeof(SDnodeEp) + sizeof(SDnodeEps));
  if (pMgmt->dnodeEps == NULL) {
    dError("failed to calloc dnodeEpList since %s", strerror(errno));
    goto PRASE_DNODE_OVER;
  }
  pMgmt->dnodeEps->num = dnodeInfosSize;

  for (int32_t i = 0; i < dnodeInfosSize; ++i) {
    cJSON *dnodeInfo = cJSON_GetArrayItem(dnodeInfos, i);
    if (dnodeInfo == NULL) break;

    SDnodeEp *pDnodeEp = &pMgmt->dnodeEps->eps[i];

    cJSON *dnodeId = cJSON_GetObjectItem(dnodeInfo, "dnodeId");
    if (!dnodeId || dnodeId->type != cJSON_String) {
      dError("failed to read %s, dnodeId not found", pMgmt->file);
      goto PRASE_DNODE_OVER;
    }
    pDnodeEp->id = atoi(dnodeId->valuestring);

    cJSON *isMnode = cJSON_GetObjectItem(dnodeInfo, "isMnode");
    if (!isMnode || isMnode->type != cJSON_String) {
      dError("failed to read %s, isMnode not found", pMgmt->file);
      goto PRASE_DNODE_OVER;
    }
    pDnodeEp->isMnode = atoi(isMnode->valuestring);

    cJSON *dnodeFqdn = cJSON_GetObjectItem(dnodeInfo, "dnodeFqdn");
    if (!dnodeFqdn || dnodeFqdn->type != cJSON_String || dnodeFqdn->valuestring == NULL) {
      dError("failed to read %s, dnodeFqdn not found", pMgmt->file);
      goto PRASE_DNODE_OVER;
    }
    tstrncpy(pDnodeEp->fqdn, dnodeFqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *dnodePort = cJSON_GetObjectItem(dnodeInfo, "dnodePort");
    if (!dnodePort || dnodePort->type != cJSON_String) {
      dError("failed to read %s, dnodePort not found", pMgmt->file);
      goto PRASE_DNODE_OVER;
    }
    pDnodeEp->port = atoi(dnodePort->valuestring);
  }

  code = 0;
  dInfo("succcessed to read file %s", pMgmt->file);
  dndPrintDnodes(pDnode);

PRASE_DNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  if (dndIsEpChanged(pDnode, pMgmt->dnodeId, pDnode->opt.localEp)) {
    dError("localEp %s different with %s and need reconfigured", pDnode->opt.localEp, pMgmt->file);
    return -1;
  }

  if (pMgmt->dnodeEps == NULL) {
    pMgmt->dnodeEps = calloc(1, sizeof(SDnodeEps) + sizeof(SDnodeEp));
    pMgmt->dnodeEps->num = 1;
    pMgmt->dnodeEps->eps[0].port = pDnode->opt.serverPort;
    tstrncpy(pMgmt->dnodeEps->eps[0].fqdn, pDnode->opt.localFqdn, TSDB_FQDN_LEN);
  }

  dndResetDnodes(pDnode, pMgmt->dnodeEps);

  terrno = 0;
  return 0;
}

static int32_t dndWriteDnodes(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  FILE *fp = fopen(pMgmt->file, "w");
  if (fp == NULL) {
    dError("failed to write %s since %s", pMgmt->file, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 30000;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": \"%d\",\n", pMgmt->dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%" PRId64 "\",\n", pMgmt->clusterId);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": \"%d\",\n", pMgmt->dropped);
  len += snprintf(content + len, maxLen - len, "  \"dnodeInfos\": [{\n");
  for (int32_t i = 0; i < pMgmt->dnodeEps->num; ++i) {
    SDnodeEp *pDnodeEp = &pMgmt->dnodeEps->eps[i];
    len += snprintf(content + len, maxLen - len, "    \"dnodeId\": \"%d\",\n", pDnodeEp->id);
    len += snprintf(content + len, maxLen - len, "    \"isMnode\": \"%d\",\n", pDnodeEp->isMnode);
    len += snprintf(content + len, maxLen - len, "    \"dnodeFqdn\": \"%s\",\n", pDnodeEp->fqdn);
    len += snprintf(content + len, maxLen - len, "    \"dnodePort\": \"%u\"\n", pDnodeEp->port);
    if (i < pMgmt->dnodeEps->num - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsyncFile(fileno(fp));
  fclose(fp);
  free(content);
  terrno = 0;

  dInfo("successed to write %s", pMgmt->file);
  return 0;
}

static void dndSendStatusMsg(SDnode *pDnode) {
  int32_t contLen = sizeof(SStatusMsg) + TSDB_MAX_VNODES * sizeof(SVnodeLoad);

  SStatusMsg *pStatus = rpcMallocCont(contLen);
  if (pStatus == NULL) {
    dError("failed to malloc status message");
    return;
  }

  dndRLockDnode(pDnode);
  pStatus->sversion = htonl(pDnode->opt.sver);
  pStatus->dnodeId = htonl(pDnode->dmgmt.dnodeId);
  pStatus->clusterId = htobe64(pDnode->dmgmt.clusterId);
  pStatus->rebootTime = htonl(pDnode->dmgmt.rebootTime);
  pStatus->numOfCores = htonl(pDnode->opt.numOfCores);
  tstrncpy(pStatus->dnodeEp, pDnode->opt.localEp, TSDB_EP_LEN);
  pStatus->clusterCfg.statusInterval = htonl(pDnode->opt.statusInterval);
  tstrncpy(pStatus->clusterCfg.timezone, pDnode->opt.timezone, TSDB_TIMEZONE_LEN);
  tstrncpy(pStatus->clusterCfg.locale, pDnode->opt.locale, TSDB_LOCALE_LEN);
  tstrncpy(pStatus->clusterCfg.charset, pDnode->opt.charset, TSDB_LOCALE_LEN);
  pStatus->clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pStatus->clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  dndRUnLockDnode(pDnode);

  dndGetVnodeLoads(pDnode, &pStatus->vnodeLoads);
  contLen = sizeof(SStatusMsg) + pStatus->vnodeLoads.num * sizeof(SVnodeLoad);

  SRpcMsg rpcMsg = {.pCont = pStatus, .contLen = contLen, .msgType = TSDB_MSG_TYPE_STATUS};
  dndSendMsgToMnode(pDnode, &rpcMsg);
}

static void dndUpdateDnodeCfg(SDnode *pDnode, SDnodeCfg *pCfg) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  if (pMgmt->dnodeId == 0 || pMgmt->dropped != pCfg->dropped) {
    dInfo("set dnodeId:%d clusterId:%" PRId64 " dropped:%d", pCfg->dnodeId, pCfg->clusterId, pCfg->dropped);

    dndWLockDnode(pDnode);
    pMgmt->dnodeId = pCfg->dnodeId;
    pMgmt->clusterId = pCfg->clusterId;
    pMgmt->dropped = pCfg->dropped;
    (void)dndWriteDnodes(pDnode);
    dndWUnLockDnode(pDnode);
  }
}

static void dndUpdateDnodeEps(SDnode *pDnode, SDnodeEps *pDnodeEps) {
  if (pDnodeEps == NULL || pDnodeEps->num <= 0) return;

  dndWLockDnode(pDnode);

  if (pDnodeEps->num != pDnode->dmgmt.dnodeEps->num) {
    dndResetDnodes(pDnode, pDnodeEps);
    dndWriteDnodes(pDnode);
  } else {
    int32_t size = pDnodeEps->num * sizeof(SDnodeEp) + sizeof(SDnodeEps);
    if (memcmp(pDnode->dmgmt.dnodeEps, pDnodeEps, size) != 0) {
      dndResetDnodes(pDnode, pDnodeEps);
      dndWriteDnodes(pDnode);
    }
  }

  dndWUnLockDnode(pDnode);
}

static void dndProcessStatusRsp(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  if (pEpSet && pEpSet->numOfEps > 0) {
    dndUpdateMnodeEpSet(pDnode, pEpSet);
  }

  if (pMsg->code != TSDB_CODE_SUCCESS) return;

  SStatusRsp *pRsp = pMsg->pCont;
  SDnodeCfg  *pCfg = &pRsp->dnodeCfg;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->clusterId = htobe64(pCfg->clusterId);
  dndUpdateDnodeCfg(pDnode, pCfg);

  if (pCfg->dropped) return;

  SDnodeEps *pDnodeEps = &pRsp->dnodeEps;
  pDnodeEps->num = htonl(pDnodeEps->num);
  for (int32_t i = 0; i < pDnodeEps->num; ++i) {
    pDnodeEps->eps[i].id = htonl(pDnodeEps->eps[i].id);
    pDnodeEps->eps[i].port = htons(pDnodeEps->eps[i].port);
  }

  dndUpdateDnodeEps(pDnode, pDnodeEps);
}

static void dndProcessAuthRsp(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) { assert(1); }

static void dndProcessGrantRsp(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) { assert(1); }

static void dndProcessConfigDnodeReq(SDnode *pDnode, SRpcMsg *pMsg) {
  SCfgDnodeMsg *pCfg = pMsg->pCont;

  int32_t code = TSDB_CODE_OPS_NOT_SUPPORT;
  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code};
  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

static void dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pMsg) {
  dInfo("startup msg is received");

  SStartupMsg *pStartup = rpcMallocCont(sizeof(SStartupMsg));
  dndGetStartup(pDnode, pStartup);

  dInfo("startup msg is sent, step:%s desc:%s finished:%d", pStartup->name, pStartup->desc, pStartup->finished);

  SRpcMsg rpcRsp = {.handle = pMsg->handle, .pCont = pStartup, .contLen = sizeof(SStartupMsg)};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
}

static void *dnodeThreadRoutine(void *param) {
  SDnode *pDnode = param;
  int32_t ms = pDnode->opt.statusInterval * 1000;

  while (true) {
    taosMsleep(ms);
    if (dndGetStat(pDnode) != DND_STAT_RUNNING) {
      continue;
    }

    pthread_testcancel();
    dndSendStatusMsg(pDnode);
  }
}

int32_t dndInitDnode(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  pMgmt->dnodeId = 0;
  pMgmt->rebootTime = taosGetTimestampSec();
  pMgmt->dropped = 0;
  pMgmt->clusterId = 0;

  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s/dnode.json", pDnode->dir.dnode);
  pMgmt->file = strdup(path);
  if (pMgmt->file == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pMgmt->dnodeHash == NULL) {
    dError("failed to init dnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dndReadDnodes(pDnode) != 0) {
    dError("failed to read file:%s since %s", pMgmt->file, terrstr());
    return -1;
  }

  taosInitRWLatch(&pMgmt->latch);

  pMgmt->threadId = taosCreateThread(dnodeThreadRoutine, pDnode);
  if (pMgmt->threadId == NULL) {
    dError("failed to init dnode thread");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dInfo("dnd-dnode is initialized");
  return 0;
}

void dndCleanupDnode(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  if (pMgmt->threadId != NULL) {
    taosDestoryThread(pMgmt->threadId);
    pMgmt->threadId = NULL;
  }

  dndWLockDnode(pDnode);

  if (pMgmt->dnodeEps != NULL) {
    free(pMgmt->dnodeEps);
    pMgmt->dnodeEps = NULL;
  }

  if (pMgmt->dnodeHash != NULL) {
    taosHashCleanup(pMgmt->dnodeHash);
    pMgmt->dnodeHash = NULL;
  }

  if (pMgmt->file != NULL) {
    free(pMgmt->file);
    pMgmt->file = NULL;
  }

  dndWUnLockDnode(pDnode);
  dInfo("dnd-dnode is cleaned up");
}

void dndProcessDnodeReq(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_NETWORK_TEST:
      dndProcessStartupReq(pDnode, pMsg);
      break;
    case TSDB_MSG_TYPE_CONFIG_DNODE_IN:
      dndProcessConfigDnodeReq(pDnode, pMsg);
      break;
    default:
      dError("RPC %p, dnode req:%s not processed", pMsg->handle, taosMsg[pMsg->msgType]);
      SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_MSG_NOT_PROCESSED};
      rpcSendResponse(&rspMsg);
      rpcFreeCont(pMsg->pCont);
  }
}

void dndProcessDnodeRsp(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_STATUS_RSP:
      dndProcessStatusRsp(pDnode, pMsg, pEpSet);
      break;
    case TSDB_MSG_TYPE_AUTH_RSP:
      dndProcessAuthRsp(pDnode, pMsg, pEpSet);
      break;
    case TSDB_MSG_TYPE_GRANT_RSP:
      dndProcessGrantRsp(pDnode, pMsg, pEpSet);
      break;
    default:
      dError("RPC %p, dnode rsp:%s not processed", pMsg->handle, taosMsg[pMsg->msgType]);
  }
}
