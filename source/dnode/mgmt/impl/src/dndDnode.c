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

static inline void dndLockDnode(SDnode *pDnd) { pthread_mutex_lock(&pDnd->d.mutex); }

static inline void dndUnLockDnode(SDnode *pDnd) { pthread_mutex_unlock(&pDnd->d.mutex); }

int32_t dndGetDnodeId(SDnode *pDnd) {
  dndLockDnode(pDnd);
  int32_t dnodeId = pDnd->d.dnodeId;
  dndUnLockDnode(pDnd);
  return dnodeId;
}

int64_t dndGetClusterId(SDnode *pDnd) {
  dndLockDnode(pDnd);
  int64_t clusterId = pDnd->d.clusterId;
  dndUnLockDnode(pDnd);
  return clusterId;
}

void dndGetDnodeEp(SDnode *pDnd, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort) {
  dndLockDnode(pDnd);

  SDnodeEp *pDnodeEp = taosHashGet(pDnd->d.dnodeHash, &dnodeId, sizeof(int32_t));
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

  dndUnLockDnode(pDnd);
}

void dndGetMnodeEpSet(SDnode *pDnd, SEpSet *pEpSet) {
  dndLockDnode(pDnd);
  *pEpSet = pDnd->d.peerEpSet;
  dndUnLockDnode(pDnd);
}

void dndGetShellEpSet(SDnode *pDnd, SEpSet *pEpSet) {
  dndLockDnode(pDnd);
  *pEpSet = pDnd->d.shellEpSet;
  dndUnLockDnode(pDnd);
}

void dndSendRedirectMsg(SDnode *pDnd, SRpcMsg *pMsg, bool forShell) {
  int32_t msgType = pMsg->msgType;

  SEpSet epSet = {0};
  if (forShell) {
    dndGetShellEpSet(pDnd, &epSet);
  } else {
    dndGetMnodeEpSet(pDnd, &epSet);
  }

  dDebug("RPC %p, msg:%s is redirected, num:%d use:%d", pMsg->handle, taosMsg[msgType], epSet.numOfEps, epSet.inUse);

  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%u", i, epSet.fqdn[i], epSet.port[i]);
    if (strcmp(epSet.fqdn[i], pDnd->opt.localFqdn) == 0) {
      if ((epSet.port[i] == pDnd->opt.serverPort + TSDB_PORT_DNODEDNODE && !forShell) ||
          (epSet.port[i] == pDnd->opt.serverPort && forShell)) {
        epSet.inUse = (i + 1) % epSet.numOfEps;
        dDebug("mnode index:%d %s:%d set inUse to %d", i, epSet.fqdn[i], epSet.port[i], epSet.inUse);
      }
    }

    epSet.port[i] = htons(epSet.port[i]);
  }

  rpcSendRedirectRsp(pMsg->handle, &epSet);
}

static void dndUpdateMnodeEpSet(SDnode *pDnd, SEpSet *pEpSet) {
  dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);

  dndLockDnode(pDnd);

  pDnd->d.peerEpSet = *pEpSet;
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    pEpSet->port[i] -= TSDB_PORT_DNODEDNODE;
    dInfo("mnode index:%d %s:%u", i, pEpSet->fqdn[i], pEpSet->port[i]);
  }
  pDnd->d.shellEpSet = *pEpSet;

  dndUnLockDnode(pDnd);
}

static void dndPrintDnodes(SDnode *pDnd) {
  SDnodeMgmt *pDnode = &pDnd->d;

  dDebug("print dnode endpoint list, num:%d", pDnode->dnodeEps->num);
  for (int32_t i = 0; i < pDnode->dnodeEps->num; i++) {
    SDnodeEp *pEp = &pDnode->dnodeEps->eps[i];
    dDebug("dnode:%d, fqdn:%s port:%u isMnode:%d", pEp->id, pEp->fqdn, pEp->port, pEp->isMnode);
  }
}

static void dndResetDnodes(SDnode *pDnd, SDnodeEps *pDnodeEps) {
  SDnodeMgmt *pDnode = &pDnd->d;

  int32_t size = sizeof(SDnodeEps) + pDnodeEps->num * sizeof(SDnodeEp);

  if (pDnodeEps->num > pDnode->dnodeEps->num) {
    SDnodeEps *tmp = calloc(1, size);
    if (tmp == NULL) return;

    tfree(pDnode->dnodeEps);
    pDnode->dnodeEps = tmp;
  }

  if (pDnode->dnodeEps != pDnodeEps) {
    memcpy(pDnode->dnodeEps, pDnodeEps, size);
  }

  pDnode->peerEpSet.inUse = 0;
  pDnode->shellEpSet.inUse = 0;

  int32_t mIndex = 0;
  for (int32_t i = 0; i < pDnode->dnodeEps->num; i++) {
    SDnodeEp *pDnodeEp = &pDnode->dnodeEps->eps[i];
    if (!pDnodeEp->isMnode) continue;
    if (mIndex >= TSDB_MAX_REPLICA) continue;
    strcpy(pDnode->shellEpSet.fqdn[mIndex], pDnodeEp->fqdn);
    strcpy(pDnode->peerEpSet.fqdn[mIndex], pDnodeEp->fqdn);
    pDnode->shellEpSet.port[mIndex] = pDnodeEp->port;
    pDnode->shellEpSet.port[mIndex] = pDnodeEp->port + TSDB_PORT_DNODEDNODE;
    mIndex++;
  }

  for (int32_t i = 0; i < pDnode->dnodeEps->num; ++i) {
    SDnodeEp *pDnodeEp = &pDnode->dnodeEps->eps[i];
    taosHashPut(pDnode->dnodeHash, &pDnodeEp->id, sizeof(int32_t), pDnodeEp, sizeof(SDnodeEp));
  }

  dndPrintDnodes(pDnd);
}

static bool dndIsEpChanged(SDnode *pDnd, int32_t dnodeId) {
  bool changed = false;
  dndLockDnode(pDnd);

  SDnodeEp *pDnodeEp = taosHashGet(pDnd->d.dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    char epstr[TSDB_EP_LEN + 1];
    snprintf(epstr, TSDB_EP_LEN, "%s:%u", pDnodeEp->fqdn, pDnodeEp->port);
    changed = strcmp(pDnd->opt.localEp, epstr) != 0;
  }

  dndUnLockDnode(pDnd);
  return changed;
}

static int32_t dndReadDnodes(SDnode *pDnd) {
  SDnodeMgmt *pDnode = &pDnd->d;

  int32_t len = 0;
  int32_t maxLen = 30000;
  char   *content = calloc(1, maxLen + 1);
  cJSON  *root = NULL;
  FILE   *fp = NULL;

  fp = fopen(pDnode->file, "r");
  if (!fp) {
    dDebug("file %s not exist", pDnode->file);
    goto PRASE_DNODE_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", pDnode->file);
    goto PRASE_DNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", pDnode->file);
    goto PRASE_DNODE_OVER;
  }

  cJSON *dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_String) {
    dError("failed to read %s since dnodeId not found", pDnode->file);
    goto PRASE_DNODE_OVER;
  }
  pDnode->dnodeId = atoi(dnodeId->valuestring);

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s since clusterId not found", pDnode->file);
    goto PRASE_DNODE_OVER;
  }
  pDnode->clusterId = atoll(clusterId->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_String) {
    dError("failed to read %s since dropped not found", pDnode->file);
    goto PRASE_DNODE_OVER;
  }
  pDnode->dropped = atoi(dropped->valuestring);

  cJSON *dnodeInfos = cJSON_GetObjectItem(root, "dnodeInfos");
  if (!dnodeInfos || dnodeInfos->type != cJSON_Array) {
    dError("failed to read %s since dnodeInfos not found", pDnode->file);
    goto PRASE_DNODE_OVER;
  }

  int32_t dnodeInfosSize = cJSON_GetArraySize(dnodeInfos);
  if (dnodeInfosSize <= 0) {
    dError("failed to read %s since dnodeInfos size:%d invalid", pDnode->file, dnodeInfosSize);
    goto PRASE_DNODE_OVER;
  }

  pDnode->dnodeEps = calloc(1, dnodeInfosSize * sizeof(SDnodeEp) + sizeof(SDnodeEps));
  if (pDnode->dnodeEps == NULL) {
    dError("failed to calloc dnodeEpList since %s", strerror(errno));
    goto PRASE_DNODE_OVER;
  }
  pDnode->dnodeEps->num = dnodeInfosSize;

  for (int32_t i = 0; i < dnodeInfosSize; ++i) {
    cJSON *dnodeInfo = cJSON_GetArrayItem(dnodeInfos, i);
    if (dnodeInfo == NULL) break;

    SDnodeEp *pDnodeEp = &pDnode->dnodeEps->eps[i];

    cJSON *dnodeId = cJSON_GetObjectItem(dnodeInfo, "dnodeId");
    if (!dnodeId || dnodeId->type != cJSON_String) {
      dError("failed to read %s, dnodeId not found", pDnode->file);
      goto PRASE_DNODE_OVER;
    }
    pDnodeEp->id = atoi(dnodeId->valuestring);

    cJSON *isMnode = cJSON_GetObjectItem(dnodeInfo, "isMnode");
    if (!isMnode || isMnode->type != cJSON_String) {
      dError("failed to read %s, isMnode not found", pDnode->file);
      goto PRASE_DNODE_OVER;
    }
    pDnodeEp->isMnode = atoi(isMnode->valuestring);

    cJSON *dnodeFqdn = cJSON_GetObjectItem(dnodeInfo, "dnodeFqdn");
    if (!dnodeFqdn || dnodeFqdn->type != cJSON_String || dnodeFqdn->valuestring == NULL) {
      dError("failed to read %s, dnodeFqdn not found", pDnode->file);
      goto PRASE_DNODE_OVER;
    }
    tstrncpy(pDnodeEp->fqdn, dnodeFqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *dnodePort = cJSON_GetObjectItem(dnodeInfo, "dnodePort");
    if (!dnodePort || dnodePort->type != cJSON_String) {
      dError("failed to read %s, dnodePort not found", pDnode->file);
      goto PRASE_DNODE_OVER;
    }
    pDnodeEp->port = atoi(dnodePort->valuestring);
  }

  dInfo("succcessed to read file %s", pDnode->file);
  dndPrintDnodes(pDnd);

PRASE_DNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  if (dndIsEpChanged(pDnd, pDnode->dnodeId)) {
    dError("localEp %s different with %s and need reconfigured", pDnd->opt.localEp, pDnode->file);
    return -1;
  }

  if (pDnode->dnodeEps == NULL) {
    pDnode->dnodeEps = calloc(1, sizeof(SDnodeEps) + sizeof(SDnodeEp));
    pDnode->dnodeEps->num = 1;
    pDnode->dnodeEps->eps[0].port = pDnd->opt.serverPort;
    tstrncpy(pDnode->dnodeEps->eps[0].fqdn, pDnd->opt.localFqdn, TSDB_FQDN_LEN);
  }

  dndResetDnodes(pDnd, pDnode->dnodeEps);

  terrno = 0;
  return 0;
}

static int32_t dndWriteDnodes(SDnode *pDnd) {
  SDnodeMgmt *pDnode = &pDnd->d;

  FILE *fp = fopen(pDnode->file, "w");
  if (!fp) {
    dError("failed to write %s since %s", pDnode->file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 30000;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": \"%d\",\n", pDnode->dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%" PRId64 "\",\n", pDnode->clusterId);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": \"%d\",\n", pDnode->dropped);
  len += snprintf(content + len, maxLen - len, "  \"dnodeInfos\": [{\n");
  for (int32_t i = 0; i < pDnode->dnodeEps->num; ++i) {
    SDnodeEp *pDnodeEp = &pDnode->dnodeEps->eps[i];
    len += snprintf(content + len, maxLen - len, "    \"dnodeId\": \"%d\",\n", pDnodeEp->id);
    len += snprintf(content + len, maxLen - len, "    \"isMnode\": \"%d\",\n", pDnodeEp->isMnode);
    len += snprintf(content + len, maxLen - len, "    \"dnodeFqdn\": \"%s\",\n", pDnodeEp->fqdn);
    len += snprintf(content + len, maxLen - len, "    \"dnodePort\": \"%u\"\n", pDnodeEp->port);
    if (i < pDnode->dnodeEps->num - 1) {
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

  dInfo("successed to write %s", pDnode->file);
  return 0;
}

static void dndSendStatusMsg(SDnode *pDnd) {
  int32_t     contLen = sizeof(SStatusMsg) + TSDB_MAX_VNODES * sizeof(SVnodeLoad);
  SStatusMsg *pStatus = rpcMallocCont(contLen);
  if (pStatus == NULL) {
    dError("failed to malloc status message");
    return;
  }

  dndLockDnode(pDnd);
  pStatus->sversion = htonl(pDnd->opt.sver);
  pStatus->dnodeId = htonl(pDnd->d.dnodeId);
  pStatus->clusterId = htobe64(pDnd->d.clusterId);
  pStatus->rebootTime = htonl(pDnd->d.rebootTime);
  pStatus->numOfCores = htonl(pDnd->opt.numOfCores);
  tstrncpy(pStatus->dnodeEp, pDnd->opt.localEp, TSDB_EP_LEN);
  pStatus->clusterCfg.statusInterval = htonl(pDnd->opt.statusInterval);
  tstrncpy(pStatus->clusterCfg.timezone, pDnd->opt.timezone, TSDB_TIMEZONE_LEN);
  tstrncpy(pStatus->clusterCfg.locale, pDnd->opt.locale, TSDB_LOCALE_LEN);
  tstrncpy(pStatus->clusterCfg.charset, pDnd->opt.charset, TSDB_LOCALE_LEN);
  pStatus->clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pStatus->clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  dndUnLockDnode(pDnd);

  dndGetVnodeLoads(pDnd, &pStatus->vnodeLoads);
  contLen = sizeof(SStatusMsg) + pStatus->vnodeLoads.num * sizeof(SVnodeLoad);

  SRpcMsg rpcMsg = {.pCont = pStatus, .contLen = contLen, .msgType = TSDB_MSG_TYPE_STATUS};
  dndSendMsgToMnode(pDnd, &rpcMsg);
}

static void dndUpdateDnodeCfg(SDnode *pDnd, SDnodeCfg *pCfg) {
  SDnodeMgmt *pDnode = &pDnd->d;
  if (pDnode->dnodeId != 0 && pDnode->dropped != pCfg->dropped) return;

  dndLockDnode(pDnd);

  pDnode->dnodeId = pCfg->dnodeId;
  pDnode->clusterId = pCfg->clusterId;
  pDnode->dropped = pCfg->dropped;
  dInfo("set dnodeId:%d clusterId:%" PRId64 " dropped:%d", pCfg->dnodeId, pCfg->clusterId, pCfg->dropped);

  dndWriteDnodes(pDnd);
  dndUnLockDnode(pDnd);
}

static void dndUpdateDnodeEps(SDnode *pDnd, SDnodeEps *pDnodeEps) {
  if (pDnodeEps == NULL || pDnodeEps->num <= 0) return;

  dndLockDnode(pDnd);

  if (pDnodeEps->num != pDnd->d.dnodeEps->num) {
    dndResetDnodes(pDnd, pDnodeEps);
    dndWriteDnodes(pDnd);
  } else {
    int32_t size = pDnodeEps->num * sizeof(SDnodeEp) + sizeof(SDnodeEps);
    if (memcmp(pDnd->d.dnodeEps, pDnodeEps, size) != 0) {
      dndResetDnodes(pDnd, pDnodeEps);
      dndWriteDnodes(pDnd);
    }
  }

  dndUnLockDnode(pDnd);
}

static void dndProcessStatusRsp(SDnode *pDnd, SRpcMsg *pMsg, SEpSet *pEpSet) {
  if (pEpSet && pEpSet->numOfEps > 0) {
    dndUpdateMnodeEpSet(pDnd, pEpSet);
  }

  if (pMsg->code != TSDB_CODE_SUCCESS) return;

  SStatusRsp *pStatusRsp = pMsg->pCont;
  SDnodeCfg  *pCfg = &pStatusRsp->dnodeCfg;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->clusterId = htobe64(pCfg->clusterId);
  dndUpdateDnodeCfg(pDnd, pCfg);

  if (pCfg->dropped) return;

  SDnodeEps *pDnodeEps = &pStatusRsp->dnodeEps;
  pDnodeEps->num = htonl(pDnodeEps->num);
  for (int32_t i = 0; i < pDnodeEps->num; ++i) {
    pDnodeEps->eps[i].id = htonl(pDnodeEps->eps[i].id);
    pDnodeEps->eps[i].port = htons(pDnodeEps->eps[i].port);
  }

  dndUpdateDnodeEps(pDnd, pDnodeEps);
}

static void dndProcessAuthRsp(SDnode *pDnd, SRpcMsg *pMsg, SEpSet *pEpSet) { assert(1); }

static void dndProcessGrantRsp(SDnode *pDnd, SRpcMsg *pMsg, SEpSet *pEpSet) { assert(1); }

static void dndProcessConfigDnodeReq(SDnode *pDnd, SRpcMsg *pMsg) {
  SCfgDnodeMsg *pCfg = pMsg->pCont;

  int32_t code = TSDB_CODE_OPS_NOT_SUPPORT;
  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code};
  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

static void dndProcessStartupReq(SDnode *pDnd, SRpcMsg *pMsg) {
  dInfo("startup msg is received");

  SStartupMsg *pStartup = rpcMallocCont(sizeof(SStartupMsg));
  dndGetStartup(pDnd, pStartup);

  dInfo("startup msg is sent, step:%s desc:%s finished:%d", pStartup->name, pStartup->desc, pStartup->finished);

  SRpcMsg rpcRsp = {.handle = pMsg->handle, .pCont = pStartup, .contLen = sizeof(SStartupMsg)};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
}

static void *dnodeThreadRoutine(void *param) {
  SDnode *pDnd = param;
  int32_t ms = pDnd->opt.statusInterval * 1000;

  while (true) {
    taosMsleep(ms);
    if (dndGetStat(pDnd) != DND_STAT_RUNNING) {
      continue;
    }

    pthread_testcancel();
    dndSendStatusMsg(pDnd);
  }
}

int32_t dndInitDnode(SDnode *pDnd) {
  SDnodeMgmt *pDnode = &pDnd->d;

  pDnode->dnodeId = 0;
  pDnode->rebootTime = taosGetTimestampSec();
  pDnode->dropped = 0;
  pDnode->clusterId = 0;

  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s/dnode.json", pDnd->dir.dnode);
  pDnode->file = strdup(path);
  if (pDnode->file == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pDnode->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pDnode->dnodeHash == NULL) {
    dError("failed to init dnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dndReadDnodes(pDnd) != 0) {
    dError("failed to read file:%s since %s", pDnode->file, terrstr());
    return -1;
  }

  pthread_mutex_init(&pDnode->mutex, NULL);

  pDnode->threadId = taosCreateThread(dnodeThreadRoutine, pDnd);
  if (pDnode->threadId == NULL) {
    dError("failed to init dnode thread");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dInfo("dnd-dnode is initialized");
  return 0;
}

void dndCleanupDnode(SDnode *pDnd) {
  SDnodeMgmt *pDnode = &pDnd->d;

  if (pDnode->threadId != NULL) {
    taosDestoryThread(pDnode->threadId);
    pDnode->threadId = NULL;
  }

  dndLockDnode(pDnd);

  if (pDnode->dnodeEps != NULL) {
    free(pDnode->dnodeEps);
    pDnode->dnodeEps = NULL;
  }

  if (pDnode->dnodeHash != NULL) {
    taosHashCleanup(pDnode->dnodeHash);
    pDnode->dnodeHash = NULL;
  }

  if (pDnode->file != NULL) {
    free(pDnode->file);
    pDnode->file = NULL;
  }

  dndUnLockDnode(pDnd);
  pthread_mutex_destroy(&pDnode->mutex);

  dInfo("dnd-dnode is cleaned up");
}

void dndProcessDnodeReq(SDnode *pDnd, SRpcMsg *pMsg, SEpSet *pEpSet) {
  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_NETWORK_TEST:
      dndProcessStartupReq(pDnd, pMsg);
      break;
    case TSDB_MSG_TYPE_CONFIG_DNODE_IN:
      dndProcessConfigDnodeReq(pDnd, pMsg);
      break;
    default:
      dError("RPC %p, dnode req:%s not processed", pMsg->handle, taosMsg[pMsg->msgType]);
      SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_MSG_NOT_PROCESSED};
      rpcSendResponse(&rspMsg);
      rpcFreeCont(pMsg->pCont);
  }
}

void dndProcessDnodeRsp(SDnode *pDnd, SRpcMsg *pMsg, SEpSet *pEpSet) {
  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_STATUS_RSP:
      dndProcessStatusRsp(pDnd, pMsg, pEpSet);
      break;
    case TSDB_MSG_TYPE_AUTH_RSP:
      dndProcessAuthRsp(pDnd, pMsg, pEpSet);
      break;
    case TSDB_MSG_TYPE_GRANT_RSP:
      dndProcessGrantRsp(pDnd, pMsg, pEpSet);
      break;
    default:
      dError("RPC %p, dnode rsp:%s not processed", pMsg->handle, taosMsg[pMsg->msgType]);
  }
}
