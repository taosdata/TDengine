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
#include "dnodeDnode.h"
#include "dnodeTransport.h"
#include "dnodeVnodes.h"
#include "cJSON.h"

int32_t dnodeGetDnodeId() {
  int32_t dnodeId = 0;
  pthread_mutex_lock(&pDnode->mutex);
  dnodeId = pDnode->dnodeId;
  pthread_mutex_unlock(&pDnode->mutex);
  return dnodeId;
}

int64_t dnodeGetClusterId() {
  int64_t clusterId = 0;
  pthread_mutex_lock(&pDnode->mutex);
  clusterId = pDnode->clusterId;
  pthread_mutex_unlock(&pDnode->mutex);
  return clusterId;
}

void dnodeGetDnodeEp(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port) {
  pthread_mutex_lock(&pDnode->mutex);

  SDnodeEp *pEp = taosHashGet(pDnode->dnodeHash, &dnodeId, sizeof(int32_t));
  if (pEp != NULL) {
    if (port) *port = pEp->dnodePort;
    if (fqdn) tstrncpy(fqdn, pEp->dnodeFqdn, TSDB_FQDN_LEN);
    if (ep) snprintf(ep, TSDB_EP_LEN, "%s:%u", pEp->dnodeFqdn, pEp->dnodePort);
  }

  pthread_mutex_unlock(&pDnode->mutex);
}

void dnodeGetMnodeEpSetForPeer(SEpSet *pEpSet) {
  pthread_mutex_lock(&pDnode->mutex);
  *pEpSet = pDnode->mnodeEpSetForPeer;
  pthread_mutex_unlock(&pDnode->mutex);
}

void dnodeGetMnodeEpSetForShell(SEpSet *pEpSet) {
  pthread_mutex_lock(&pDnode->mutex);
  *pEpSet = pDnode->mnodeEpSetForShell;
  pthread_mutex_unlock(&pDnode->mutex);
}

void dnodeSendRedirectMsg(SDnode *pDnode, SRpcMsg *pMsg, bool forShell) {
  int32_t msgType = pMsg->msgType;

  SEpSet epSet = {0};
  if (forShell) {
    dnodeGetMnodeEpSetForShell(&epSet);
  } else {
    dnodeGetMnodeEpSetForPeer(&epSet);
  }

  dDebug("RPC %p, msg:%s is redirected, num:%d use:%d", pMsg->handle, taosMsg[msgType], epSet.numOfEps, epSet.inUse);

  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%u", i, epSet.fqdn[i], epSet.port[i]);
    if (strcmp(epSet.fqdn[i], tsLocalFqdn) == 0) {
      if ((epSet.port[i] == tsServerPort + TSDB_PORT_DNODEDNODE && !forShell) ||
          (epSet.port[i] == tsServerPort && forShell)) {
        epSet.inUse = (i + 1) % epSet.numOfEps;
        dDebug("mnode index:%d %s:%d set inUse to %d", i, epSet.fqdn[i], epSet.port[i], epSet.inUse);
      }
    }

    epSet.port[i] = htons(epSet.port[i]);
  }

  rpcSendRedirectRsp(pMsg->handle, &epSet);
}

static void dnodeUpdateMnodeEpSet(SDnodeDnode *pDnode, SEpSet *pEpSet) {
  if (pEpSet == NULL || pEpSet->numOfEps <= 0) {
    dError("mnode is changed, but content is invalid, discard it");
    return;
  } else {
    dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);
  }

  pthread_mutex_lock(&pDnode->mutex);

  pDnode->mnodeEpSetForPeer = *pEpSet;
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    pEpSet->port[i] -= TSDB_PORT_DNODEDNODE;
    dInfo("mnode index:%d %s:%u", i, pEpSet->fqdn[i], pEpSet->port[i]);
  }
  pDnode->mnodeEpSetForShell = *pEpSet;

  pthread_mutex_unlock(&pDnode->mutex);
}

static void dnodePrintDnodes() {
  dDebug("print dnode endpoint list, num:%d", pDnode->dnodeEps->dnodeNum);
  for (int32_t i = 0; i < pDnode->dnodeEps->dnodeNum; i++) {
    SDnodeEp *ep = &pDnode->dnodeEps->dnodeEps[i];
    dDebug("dnode:%d, fqdn:%s port:%u isMnode:%d", ep->dnodeId, ep->dnodeFqdn, ep->dnodePort, ep->isMnode);
  }
}

static void dnodeResetDnodes(SDnodeEps *pEps) {
  assert(pEps != NULL);
  int32_t size = sizeof(SDnodeEps) + pEps->dnodeNum * sizeof(SDnodeEp);

  if (pEps->dnodeNum > pDnode->dnodeEps->dnodeNum) {
    SDnodeEps *tmp = calloc(1, size);
    if (tmp == NULL) return;

    tfree(pDnode->dnodeEps);
    pDnode->dnodeEps = tmp;
  }

  if (pDnode->dnodeEps != pEps) {
    memcpy(pDnode->dnodeEps, pEps, size);
  }

  pDnode->mnodeEpSetForPeer.inUse = 0;
  pDnode->mnodeEpSetForShell.inUse = 0;

  int32_t mIndex = 0;
  for (int32_t i = 0; i < pDnode->dnodeEps->dnodeNum; i++) {
    SDnodeEp *ep = &pDnode->dnodeEps->dnodeEps[i];
    if (!ep->isMnode) continue;
    if (mIndex >= TSDB_MAX_REPLICA) continue;
    strcpy(pDnode->mnodeEpSetForShell.fqdn[mIndex], ep->dnodeFqdn);
    strcpy(pDnode->mnodeEpSetForPeer.fqdn[mIndex], ep->dnodeFqdn);
    pDnode->mnodeEpSetForShell.port[mIndex] = ep->dnodePort;
    pDnode->mnodeEpSetForShell.port[mIndex] = ep->dnodePort + tsDnodeDnodePort;
    mIndex++;
  }

  for (int32_t i = 0; i < pDnode->dnodeEps->dnodeNum; ++i) {
    SDnodeEp *ep = &pDnode->dnodeEps->dnodeEps[i];
    taosHashPut(pDnode->dnodeHash, &ep->dnodeId, sizeof(int32_t), ep, sizeof(SDnodeEp));
  }

  dnodePrintDnodes();
}

static bool dnodeIsEpChanged(int32_t dnodeId, char *epStr) {
  bool changed = false;
  pthread_mutex_lock(&pDnode->mutex);

  SDnodeEp *pEp = taosHashGet(pDnode->dnodeHash, &dnodeId, sizeof(int32_t));
  if (pEp != NULL) {
    char epSaved[TSDB_EP_LEN + 1];
    snprintf(epSaved, TSDB_EP_LEN, "%s:%u", pEp->dnodeFqdn, pEp->dnodePort);
    changed = strcmp(epStr, epSaved) != 0;
  }

  pthread_mutex_unlock(&pDnode->mutex);
  return changed;
}

static int32_t dnodeReadDnodes() {
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
  pDnode->dnodeEps->dnodeNum = dnodeInfosSize;

  for (int32_t i = 0; i < dnodeInfosSize; ++i) {
    cJSON *dnodeInfo = cJSON_GetArrayItem(dnodeInfos, i);
    if (dnodeInfo == NULL) break;

    SDnodeEp *pEp = &pDnode->dnodeEps->dnodeEps[i];

    cJSON *dnodeId = cJSON_GetObjectItem(dnodeInfo, "dnodeId");
    if (!dnodeId || dnodeId->type != cJSON_String) {
      dError("failed to read %s, dnodeId not found", pDnode->file);
      goto PRASE_DNODE_OVER;
    }
    pEp->dnodeId = atoi(dnodeId->valuestring);

    cJSON *isMnode = cJSON_GetObjectItem(dnodeInfo, "isMnode");
    if (!isMnode || isMnode->type != cJSON_String) {
      dError("failed to read %s, isMnode not found", pDnode->file);
      goto PRASE_DNODE_OVER;
    }
    pEp->isMnode = atoi(isMnode->valuestring);

    cJSON *dnodeFqdn = cJSON_GetObjectItem(dnodeInfo, "dnodeFqdn");
    if (!dnodeFqdn || dnodeFqdn->type != cJSON_String || dnodeFqdn->valuestring == NULL) {
      dError("failed to read %s, dnodeFqdn not found", pDnode->file);
      goto PRASE_DNODE_OVER;
    }
    tstrncpy(pEp->dnodeFqdn, dnodeFqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *dnodePort = cJSON_GetObjectItem(dnodeInfo, "dnodePort");
    if (!dnodePort || dnodePort->type != cJSON_String) {
      dError("failed to read %s, dnodePort not found", pDnode->file);
      goto PRASE_DNODE_OVER;
    }
    pEp->dnodePort = atoi(dnodePort->valuestring);
  }

  dInfo("succcessed to read file %s", pDnode->file);
  dnodePrintDnodes();

PRASE_DNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  if (dnodeIsEpChanged(pDnode->dnodeId, tsLocalEp)) {
    dError("localEp %s different with %s and need reconfigured", tsLocalEp, pDnode->file);
    return -1;
  }

  if (pDnode->dnodeEps == NULL) {
    pDnode->dnodeEps = calloc(1, sizeof(SDnodeEps) + sizeof(SDnodeEp));
    pDnode->dnodeEps->dnodeNum = 1;
    pDnode->dnodeEps->dnodeEps[0].dnodePort = tsServerPort;
    tstrncpy(pDnode->dnodeEps->dnodeEps[0].dnodeFqdn, tsLocalFqdn, TSDB_FQDN_LEN);
  }

  dnodeResetDnodes(pDnode->dnodeEps);

  terrno = 0;
  return 0;
}

static int32_t dnodeWriteDnodes() {
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
  for (int32_t i = 0; i < pDnode->dnodeEps->dnodeNum; ++i) {
    SDnodeEp *ep = &pDnode->dnodeEps->dnodeEps[i];
    len += snprintf(content + len, maxLen - len, "    \"dnodeId\": \"%d\",\n", ep->dnodeId);
    len += snprintf(content + len, maxLen - len, "    \"isMnode\": \"%d\",\n", ep->isMnode);
    len += snprintf(content + len, maxLen - len, "    \"dnodeFqdn\": \"%s\",\n", ep->dnodeFqdn);
    len += snprintf(content + len, maxLen - len, "    \"dnodePort\": \"%u\"\n", ep->dnodePort);
    if (i < pDnode->dnodeEps->dnodeNum - 1) {
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

static void dnodeSendStatusMsg() {
  int32_t contLen = sizeof(SStatusMsg) + TSDB_MAX_VNODES * sizeof(SVnodeLoad);

  SStatusMsg *pStatus = rpcMallocCont(contLen);
  if (pStatus == NULL) {
    dError("failed to malloc status message");
    return;
  }

  pStatus->sversion = htonl(tsVersion);
  pStatus->dnodeId = htonl(dnodeGetDnodeId());
  pStatus->clusterId = htobe64(dnodeGetClusterId());
  pStatus->rebootTime = htonl(pDnode->rebootTime);
  pStatus->numOfCores = htonl(tsNumOfCores);
  tstrncpy(pStatus->dnodeEp, tsLocalEp, TSDB_EP_LEN);

  pStatus->clusterCfg.statusInterval = htonl(tsStatusInterval);
  pStatus->clusterCfg.checkTime = 0;
  tstrncpy(pStatus->clusterCfg.timezone, tsTimezone, TSDB_TIMEZONE_LEN);
  tstrncpy(pStatus->clusterCfg.locale, tsLocale, TSDB_LOCALE_LEN);
  tstrncpy(pStatus->clusterCfg.charset, tsCharset, TSDB_LOCALE_LEN);
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pStatus->clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);

  dnodeGetVnodeLoads(&pStatus->vnodeLoads);
  contLen = sizeof(SStatusMsg) + pStatus->vnodeLoads.num * sizeof(SVnodeLoad);

  SRpcMsg rpcMsg = {.pCont = pStatus, .contLen = contLen, .msgType = TSDB_MSG_TYPE_STATUS};
  dnodeSendMsgToMnode(NULL, &rpcMsg);
}

static void dnodeUpdateCfg(SDnodeCfg *pCfg) {
  if (pDnode->dnodeId == 0) return;
  if (pDnode->dropped) return;

  pthread_mutex_lock(&pDnode->mutex);

  pDnode->dnodeId = pCfg->dnodeId;
  pDnode->clusterId = pCfg->clusterId;
  pDnode->dropped = pCfg->dropped;
  dInfo("dnodeId is set to %d, clusterId is set to %" PRId64, pCfg->dnodeId, pCfg->clusterId);

  dnodeWriteDnodes();
  pthread_mutex_unlock(&pDnode->mutex);
}

static void dnodeUpdateDnodeEps(SDnodeEps *pEps) {
  if (pEps == NULL || pEps->dnodeNum <= 0) return;

  pthread_mutex_lock(&pDnode->mutex);

  if (pEps->dnodeNum != pDnode->dnodeEps->dnodeNum) {
    dnodeResetDnodes(pEps);
    dnodeWriteDnodes();
  } else {
    int32_t size = pEps->dnodeNum * sizeof(SDnodeEp) + sizeof(SDnodeEps);
    if (memcmp(pDnode->dnodeEps, pEps, size) != 0) {
      dnodeResetDnodes(pEps);
      dnodeWriteDnodes();
    }
  }

  pthread_mutex_unlock(&pDnode->mutex);
}

static void dnodeProcessStatusRsp(SRpcMsg *pMsg) {
  if (pMsg->code != TSDB_CODE_SUCCESS) return;

  SStatusRsp *pStatusRsp = pMsg->pCont;

  SDnodeCfg *pCfg = &pStatusRsp->dnodeCfg;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->clusterId = htobe64(pCfg->clusterId);
  dnodeUpdateCfg(pCfg);

  if (pCfg->dropped) return;

  SDnodeEps *pEps = &pStatusRsp->dnodeEps;
  pEps->dnodeNum = htonl(pEps->dnodeNum);
  for (int32_t i = 0; i < pEps->dnodeNum; ++i) {
    pEps->dnodeEps[i].dnodeId = htonl(pEps->dnodeEps[i].dnodeId);
    pEps->dnodeEps[i].dnodePort = htons(pEps->dnodeEps[i].dnodePort);
  }

  dnodeUpdateDnodeEps(pEps);
}

static void dnodeProcessConfigDnodeReq(SRpcMsg *pMsg) {
  SCfgDnodeMsg *pCfg = pMsg->pCont;

  int32_t code = taosCfgDynamicOptions(pCfg->config);
  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code};
  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

static void dnodeProcessStartupReq(SRpcMsg *pMsg) {
  dInfo("startup msg is received, cont:%s", (char *)pMsg->pCont);

  SStartupMsg *pStartup = rpcMallocCont(sizeof(SStartupMsg));
  dnodeGetStartup(NULL, pStartup);

  dInfo("startup msg is sent, step:%s desc:%s finished:%d", pStartup->name, pStartup->desc, pStartup->finished);

  SRpcMsg rpcRsp = {.handle = pMsg->handle, .pCont = pStartup, .contLen = sizeof(SStartupMsg)};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
}

static void *dnodeThreadRoutine(void *param) {
  int32_t ms = tsStatusInterval * 1000;

  while (!pDnode->threadStop) {
    if (dnodeGetStat() != DN_STAT_RUNNING) {
      continue;
    } else {
      dnodeSendStatusMsg();
    }
    taosMsleep(ms);
  }
}

int32_t dnodeInitDnode(SDnode *pServer) {
  SDnodeDnode *pDnode = &pServer->dnode;

  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s/dnode.json", pServer->dir.dnode);
  pDnode->file = strdup(path);
  if (pDnode->file == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pDnode->dnodeId = 0;
  pDnode->clusterId = 0;
  pDnode->dnodeEps = NULL;

  pDnode->rebootTime = taosGetTimestampSec();
  pDnode->dropped = 0;
  pthread_mutex_init(&pDnode->mutex, NULL);
  pDnode->threadStop = false;

  pDnode->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pDnode->dnodeHash == NULL) {
    dError("failed to init dnode hash");
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }

  pDnode->threadId = taosCreateThread(dnodeThreadRoutine, NULL);
  if (pDnode->threadId == NULL) {
    dError("failed to init dnode thread");
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }

  int32_t code = dnodeReadDnodes();
  if (code != 0) {
    dError("failed to read file:%s since %s", pDnode->file, tstrerror(code));
    return code;
  }

  dInfo("dnode-dnode is initialized");
  return 0;
}

void dnodeCleanupDnode(SDnode *pServer) {
  SDnodeDnode *pDnode = &pServer->dnode;

  if (pDnode->threadId != NULL) {
    pDnode->threadStop = true;
    taosDestoryThread(pDnode->threadId);
    pDnode->threadId = NULL;
  }

  pthread_mutex_lock(&pDnode->mutex);

  if (pDnode->dnodeEps != NULL) {
    free(pDnode->dnodeEps);
    pDnode->dnodeEps = NULL;
  }

  if (pDnode->dnodeHash) {
    taosHashCleanup(pDnode->dnodeHash);
    pDnode->dnodeHash = NULL;
  }

  pthread_mutex_unlock(&pDnode->mutex);
  pthread_mutex_destroy(&pDnode->mutex);

  dInfo("dnode-dnode is cleaned up");
}

void dnodeProcessDnodeMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  int32_t msgType = pMsg->msgType;

  if (msgType == TSDB_MSG_TYPE_STATUS_RSP && pEpSet) {
    dnodeUpdateMnodeEpSet(&pDnode->dnode, pEpSet);
  }

  switch (msgType) {
    case TSDB_MSG_TYPE_NETWORK_TEST:
      dnodeProcessStartupReq(pMsg);
      break;
    case TSDB_MSG_TYPE_CONFIG_DNODE_IN:
      dnodeProcessConfigDnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_STATUS_RSP:
      dnodeProcessStatusRsp(pMsg);
      break;
    default:
      dError("RPC %p, %s not processed", pMsg->handle, taosMsg[msgType]);
      SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_DND_MSG_NOT_PROCESSED};
      rpcSendResponse(&rspMsg);
      rpcFreeCont(pMsg->pCont);
  }
}
