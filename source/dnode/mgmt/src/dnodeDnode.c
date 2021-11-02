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
#include "thash.h"
#include "tthread.h"
#include "ttime.h"

static struct {
  int32_t         dnodeId;
  int64_t         clusterId;
  SDnodeEps      *dnodeEps;
  SHashObj       *dnodeHash;
  SEpSet          mnodeEpSetForShell;
  SEpSet          mnodeEpSetForPeer;
  char            file[PATH_MAX + 20];
  uint32_t        rebootTime;
  int8_t          dropped;
  int8_t          threadStop;
  pthread_t      *threadId;
  pthread_mutex_t mutex;
  MsgFp           msgFp[TSDB_MSG_TYPE_MAX];
} tsDnode = {0};

int32_t dnodeGetDnodeId() {
  int32_t dnodeId = 0;
  pthread_mutex_lock(&tsDnode.mutex);
  dnodeId = tsDnode.dnodeId;
  pthread_mutex_unlock(&tsDnode.mutex);
  return dnodeId;
}

int64_t dnodeGetClusterId() {
  int64_t clusterId = 0;
  pthread_mutex_lock(&tsDnode.mutex);
  clusterId = tsDnode.clusterId;
  pthread_mutex_unlock(&tsDnode.mutex);
  return clusterId;
}

void dnodeGetDnodeEp(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port) {
  pthread_mutex_lock(&tsDnode.mutex);

  SDnodeEp *pEp = taosHashGet(tsDnode.dnodeHash, &dnodeId, sizeof(int32_t));
  if (pEp != NULL) {
    if (port) *port = pEp->dnodePort;
    if (fqdn) tstrncpy(fqdn, pEp->dnodeFqdn, TSDB_FQDN_LEN);
    if (ep) snprintf(ep, TSDB_EP_LEN, "%s:%u", pEp->dnodeFqdn, pEp->dnodePort);
  }

  pthread_mutex_unlock(&tsDnode.mutex);
}

void dnodeGetMnodeEpSetForPeer(SEpSet *pEpSet) {
  pthread_mutex_lock(&tsDnode.mutex);
  *pEpSet = tsDnode.mnodeEpSetForPeer;
  pthread_mutex_unlock(&tsDnode.mutex);
}

void dnodeGetMnodeEpSetForShell(SEpSet *pEpSet) {
  pthread_mutex_lock(&tsDnode.mutex);
  *pEpSet = tsDnode.mnodeEpSetForShell;
  pthread_mutex_unlock(&tsDnode.mutex);
}

void dnodeSendRedirectMsg(SRpcMsg *pMsg, bool forShell) {
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

static void dnodeUpdateMnodeEpSet(SEpSet *pEpSet) {
  if (pEpSet == NULL || pEpSet->numOfEps <= 0) {
    dError("mnode is changed, but content is invalid, discard it");
    return;
  } else {
    dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);
  }

  pthread_mutex_lock(&tsDnode.mutex);

  tsDnode.mnodeEpSetForPeer = *pEpSet;
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    pEpSet->port[i] -= TSDB_PORT_DNODEDNODE;
    dInfo("mnode index:%d %s:%u", i, pEpSet->fqdn[i], pEpSet->port[i]);
  }
  tsDnode.mnodeEpSetForShell = *pEpSet;

  pthread_mutex_unlock(&tsDnode.mutex);
}

static void dnodePrintEps() {
  dDebug("print dnode endpoint list, num:%d", tsDnode.dnodeEps->dnodeNum);
  for (int32_t i = 0; i < tsDnode.dnodeEps->dnodeNum; i++) {
    SDnodeEp *ep = &tsDnode.dnodeEps->dnodeEps[i];
    dDebug("dnode:%d, fqdn:%s port:%u isMnode:%d", ep->dnodeId, ep->dnodeFqdn, ep->dnodePort, ep->isMnode);
  }
}

static void dnodeResetEps(SDnodeEps *pEps) {
  assert(pEps != NULL);
  int32_t size = sizeof(SDnodeEps) + pEps->dnodeNum * sizeof(SDnodeEp);

  if (pEps->dnodeNum > tsDnode.dnodeEps->dnodeNum) {
    SDnodeEps *tmp = calloc(1, size);
    if (tmp == NULL) return;

    tfree(tsDnode.dnodeEps);
    tsDnode.dnodeEps = tmp;
  }

  if (tsDnode.dnodeEps != pEps) {
    memcpy(tsDnode.dnodeEps, pEps, size);
  }

  tsDnode.mnodeEpSetForPeer.inUse = 0;
  tsDnode.mnodeEpSetForShell.inUse = 0;

  int32_t mIndex = 0;
  for (int32_t i = 0; i < tsDnode.dnodeEps->dnodeNum; i++) {
    SDnodeEp *ep = &tsDnode.dnodeEps->dnodeEps[i];
    if (!ep->isMnode) continue;
    if (mIndex >= TSDB_MAX_REPLICA) continue;
    strcpy(tsDnode.mnodeEpSetForShell.fqdn[mIndex], ep->dnodeFqdn);
    strcpy(tsDnode.mnodeEpSetForPeer.fqdn[mIndex], ep->dnodeFqdn);
    tsDnode.mnodeEpSetForShell.port[mIndex] = ep->dnodePort;
    tsDnode.mnodeEpSetForShell.port[mIndex] = ep->dnodePort + tsDnodeDnodePort;
    mIndex++;
  }

  for (int32_t i = 0; i < tsDnode.dnodeEps->dnodeNum; ++i) {
    SDnodeEp *ep = &tsDnode.dnodeEps->dnodeEps[i];
    taosHashPut(tsDnode.dnodeHash, &ep->dnodeId, sizeof(int32_t), ep, sizeof(SDnodeEp));
  }

  dnodePrintEps();
}

static bool dnodeIsEpChanged(int32_t dnodeId, char *epStr) {
  bool changed = false;
  pthread_mutex_lock(&tsDnode.mutex);

  SDnodeEp *pEp = taosHashGet(tsDnode.dnodeHash, &dnodeId, sizeof(int32_t));
  if (pEp != NULL) {
    char epSaved[TSDB_EP_LEN + 1];
    snprintf(epSaved, TSDB_EP_LEN, "%s:%u", pEp->dnodeFqdn, pEp->dnodePort);
    changed = strcmp(epStr, epSaved) != 0;
  }

  pthread_mutex_unlock(&tsDnode.mutex);
  return changed;
}

static int32_t dnodeReadEps() {
  int32_t len = 0;
  int32_t maxLen = 30000;
  char   *content = calloc(1, maxLen + 1);
  cJSON  *root = NULL;
  FILE   *fp = NULL;

  fp = fopen(tsDnode.file, "r");
  if (!fp) {
    dDebug("file %s not exist", tsDnode.file);
    goto PRASE_EPS_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", tsDnode.file);
    goto PRASE_EPS_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", tsDnode.file);
    goto PRASE_EPS_OVER;
  }

  cJSON *dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_String) {
    dError("failed to read %s since dnodeId not found", tsDnode.file);
    goto PRASE_EPS_OVER;
  }
  tsDnode.dnodeId = atoi(dnodeId->valuestring);

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s since clusterId not found", tsDnode.file);
    goto PRASE_EPS_OVER;
  }
  tsDnode.clusterId = atoll(clusterId->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_String) {
    dError("failed to read %s since dropped not found", tsDnode.file);
    goto PRASE_EPS_OVER;
  }
  tsDnode.dropped = atoi(dropped->valuestring);

  cJSON *dnodeInfos = cJSON_GetObjectItem(root, "dnodeInfos");
  if (!dnodeInfos || dnodeInfos->type != cJSON_Array) {
    dError("failed to read %s since dnodeInfos not found", tsDnode.file);
    goto PRASE_EPS_OVER;
  }

  int32_t dnodeInfosSize = cJSON_GetArraySize(dnodeInfos);
  if (dnodeInfosSize <= 0) {
    dError("failed to read %s since dnodeInfos size:%d invalid", tsDnode.file, dnodeInfosSize);
    goto PRASE_EPS_OVER;
  }

  tsDnode.dnodeEps = calloc(1, dnodeInfosSize * sizeof(SDnodeEp) + sizeof(SDnodeEps));
  if (tsDnode.dnodeEps == NULL) {
    dError("failed to calloc dnodeEpList since %s", strerror(errno));
    goto PRASE_EPS_OVER;
  }
  tsDnode.dnodeEps->dnodeNum = dnodeInfosSize;

  for (int32_t i = 0; i < dnodeInfosSize; ++i) {
    cJSON *dnodeInfo = cJSON_GetArrayItem(dnodeInfos, i);
    if (dnodeInfo == NULL) break;

    SDnodeEp *pEp = &tsDnode.dnodeEps->dnodeEps[i];

    cJSON *dnodeId = cJSON_GetObjectItem(dnodeInfo, "dnodeId");
    if (!dnodeId || dnodeId->type != cJSON_String) {
      dError("failed to read %s, dnodeId not found", tsDnode.file);
      goto PRASE_EPS_OVER;
    }
    pEp->dnodeId = atoi(dnodeId->valuestring);

    cJSON *isMnode = cJSON_GetObjectItem(dnodeInfo, "isMnode");
    if (!isMnode || isMnode->type != cJSON_String) {
      dError("failed to read %s, isMnode not found", tsDnode.file);
      goto PRASE_EPS_OVER;
    }
    pEp->isMnode = atoi(isMnode->valuestring);

    cJSON *dnodeFqdn = cJSON_GetObjectItem(dnodeInfo, "dnodeFqdn");
    if (!dnodeFqdn || dnodeFqdn->type != cJSON_String || dnodeFqdn->valuestring == NULL) {
      dError("failed to read %s, dnodeFqdn not found", tsDnode.file);
      goto PRASE_EPS_OVER;
    }
    tstrncpy(pEp->dnodeFqdn, dnodeFqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *dnodePort = cJSON_GetObjectItem(dnodeInfo, "dnodePort");
    if (!dnodePort || dnodePort->type != cJSON_String) {
      dError("failed to read %s, dnodePort not found", tsDnode.file);
      goto PRASE_EPS_OVER;
    }
    pEp->dnodePort = atoi(dnodePort->valuestring);
  }

  dInfo("succcessed to read file %s", tsDnode.file);
  dnodePrintEps();

PRASE_EPS_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  if (dnodeIsEpChanged(tsDnode.dnodeId, tsLocalEp)) {
    dError("localEp %s different with %s and need reconfigured", tsLocalEp, tsDnode.file);
    return -1;
  }

  dnodeResetEps(tsDnode.dnodeEps);

  terrno = 0;
  return 0;
}

static int32_t dnodeWriteEps() {
  FILE *fp = fopen(tsDnode.file, "w");
  if (!fp) {
    dError("failed to write %s since %s", tsDnode.file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 30000;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": \"%d\",\n", tsDnode.dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%" PRId64 "\",\n", tsDnode.clusterId);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": \"%d\",\n", tsDnode.dropped);
  len += snprintf(content + len, maxLen - len, "  \"dnodeInfos\": [{\n");
  for (int32_t i = 0; i < tsDnode.dnodeEps->dnodeNum; ++i) {
    SDnodeEp *ep = &tsDnode.dnodeEps->dnodeEps[i];
    len += snprintf(content + len, maxLen - len, "    \"dnodeId\": \"%d\",\n", ep->dnodeId);
    len += snprintf(content + len, maxLen - len, "    \"isMnode\": \"%d\",\n", ep->isMnode);
    len += snprintf(content + len, maxLen - len, "    \"dnodeFqdn\": \"%s\",\n", ep->dnodeFqdn);
    len += snprintf(content + len, maxLen - len, "    \"dnodePort\": \"%u\"\n", ep->dnodePort);
    if (i < tsDnode.dnodeEps->dnodeNum - 1) {
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

  dInfo("successed to write %s", tsDnode.file);
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
  pStatus->rebootTime = htonl(tsDnode.rebootTime);
  pStatus->numOfCores = htonl(tsNumOfCores);
  tstrncpy(pStatus->dnodeEp, tsLocalEp, TSDB_EP_LEN);

  pStatus->clusterCfg.statusInterval = htonl(tsStatusInterval);
  pStatus->clusterCfg.checkTime = 0;
  tstrncpy(pStatus->clusterCfg.timezone, tsTimezone, TSDB_TIMEZONE_LEN);
  tstrncpy(pStatus->clusterCfg.locale, tsLocale, TSDB_LOCALE_LEN);
  tstrncpy(pStatus->clusterCfg.charset, tsCharset, TSDB_LOCALE_LEN);
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pStatus->clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);

  dnodeGetVnodes(&pStatus->vnodeLoads);
  contLen = sizeof(SStatusMsg) + pStatus->vnodeLoads.vnodeNum * sizeof(SVnodeLoad);

  SRpcMsg rpcMsg = {.pCont = pStatus, .contLen = contLen, .msgType = TSDB_MSG_TYPE_STATUS};
  dnodeSendMsgToMnode(&rpcMsg);
}

static void dnodeUpdateCfg(SDnodeCfg *pCfg) {
  if (tsDnode.dnodeId == 0) return;
  if (tsDnode.dropped) return;

  pthread_mutex_lock(&tsDnode.mutex);

  tsDnode.dnodeId = pCfg->dnodeId;
  tsDnode.clusterId = pCfg->clusterId;
  tsDnode.dropped = pCfg->dropped;
  dInfo("dnodeId is set to %d, clusterId is set to %" PRId64, pCfg->dnodeId, pCfg->clusterId);

  dnodeWriteEps();
  pthread_mutex_unlock(&tsDnode.mutex);
}

static void dnodeUpdateDnodeEps(SDnodeEps *pEps) {
  if (pEps == NULL || pEps->dnodeNum <= 0) return;

  pthread_mutex_lock(&tsDnode.mutex);

  if (pEps->dnodeNum != tsDnode.dnodeEps->dnodeNum) {
    dnodeResetEps(pEps);
    dnodeWriteEps();
  } else {
    int32_t size = pEps->dnodeNum * sizeof(SDnodeEp) + sizeof(SDnodeEps);
    if (memcmp(tsDnode.dnodeEps, pEps, size) != 0) {
      dnodeResetEps(pEps);
      dnodeWriteEps();
    }
  }

  pthread_mutex_unlock(&tsDnode.mutex);
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
  dnodeGetStartup(pStartup);

  dInfo("startup msg is sent, step:%s desc:%s finished:%d", pStartup->name, pStartup->desc, pStartup->finished);

  SRpcMsg rpcRsp = {.handle = pMsg->handle, .pCont = pStartup, .contLen = sizeof(SStartupMsg)};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
}

static void *dnodeThreadRoutine(void *param) {
  int32_t ms = tsStatusInterval * 1000;

  while (!tsDnode.threadStop) {
    if (dnodeGetRunStat() != DN_RUN_STAT_RUNNING) {
      continue;
    } else {
      dnodeSendStatusMsg();
    }
    taosMsleep(ms);
  }
}

int32_t dnodeInitDnode() {
  tsDnode.dnodeId = 0;
  tsDnode.clusterId = 0;
  tsDnode.dnodeEps = NULL;
  snprintf(tsDnode.file, sizeof(tsDnode.file), "%s/dnode.json", tsDnodeDir);
  tsDnode.rebootTime = taosGetTimestampSec();
  tsDnode.dropped = 0;
  pthread_mutex_init(&tsDnode.mutex, NULL);
  tsDnode.threadStop = false;

  tsDnode.dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsDnode.dnodeHash == NULL) {
    dError("failed to init dnode hash");
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }

  tsDnode.threadId = taosCreateThread(dnodeThreadRoutine, NULL);
  if (tsDnode.threadId == NULL) {
    dError("failed to init dnode thread");
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }

  int32_t code = dnodeReadEps();
  if (code != 0) {
    dError("failed to read dnode endpoint file since %s", tstrerror(code));
    return code;
  }

  dInfo("dnode-dnode is initialized");
  return 0;
}

void dnodeCleanupDnode() {
  if (tsDnode.threadId != NULL) {
    tsDnode.threadStop = true;
    taosDestoryThread(tsDnode.threadId);
    tsDnode.threadId = NULL;
  }

  pthread_mutex_lock(&tsDnode.mutex);

  if (tsDnode.dnodeEps != NULL) {
    free(tsDnode.dnodeEps);
    tsDnode.dnodeEps = NULL;
  }

  if (tsDnode.dnodeHash) {
    taosHashCleanup(tsDnode.dnodeHash);
    tsDnode.dnodeHash = NULL;
  }

  pthread_mutex_unlock(&tsDnode.mutex);
  pthread_mutex_destroy(&tsDnode.mutex);

  dInfo("dnode-dnode is cleaned up");
}

void dnodeProcessDnodeMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  int32_t msgType = pMsg->msgType;

  if (msgType == TSDB_MSG_TYPE_STATUS_RSP && pEpSet) {
    dnodeUpdateMnodeEpSet(pEpSet);
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
