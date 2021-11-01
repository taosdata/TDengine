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
#include "dnodeEps.h"
#include "cJSON.h"
#include "thash.h"

static struct {
  int32_t    dnodeId;
  int32_t    dropped;
  int64_t    clusterId;
  SDnodeEps *dnodeEps;
  SHashObj  *dnodeHash;
  SRpcEpSet  mnodeEpSetForShell;
  SRpcEpSet  mnodeEpSetForPeer;
  char       file[PATH_MAX + 20];
  pthread_mutex_t mutex;
} tsEps;

void dnodeGetEpSetForPeer(SRpcEpSet *epSet) {
  pthread_mutex_lock(&tsEps.mutex);
  *epSet = tsEps.mnodeEpSetForPeer;
  pthread_mutex_unlock(&tsEps.mutex);
}

void dnodeGetEpSetForShell(SRpcEpSet *epSet) {
  pthread_mutex_lock(&tsEps.mutex);
  *epSet = tsEps.mnodeEpSetForShell;
  pthread_mutex_unlock(&tsEps.mutex);
}

void dnodeUpdateMnodeEps(SRpcEpSet *ep) {
  if (ep != NULL || ep->numOfEps <= 0) {
    dError("mnode is changed, but content is invalid, discard it");
    return;
  }

  pthread_mutex_lock(&tsEps.mutex);

  dInfo("mnode is changed, num:%d use:%d", ep->numOfEps, ep->inUse);

  tsEps.mnodeEpSetForPeer = *ep;
  for (int32_t i = 0; i < ep->numOfEps; ++i) {
    ep->port[i] -= TSDB_PORT_DNODEDNODE;
    dInfo("mnode index:%d %s:%u", i, ep->fqdn[i], ep->port[i]);
  }
  tsEps.mnodeEpSetForShell = *ep;

  pthread_mutex_unlock(&tsEps.mutex);
}

void dnodeSendRedirectMsg(SRpcMsg *rpcMsg, bool forShell) {
  SRpcConnInfo connInfo = {0};
  rpcGetConnInfo(rpcMsg->handle, &connInfo);

  SRpcEpSet epSet = {0};
  if (forShell) {
    dnodeGetEpSetForShell(&epSet);
  } else {
    dnodeGetEpSetForPeer(&epSet);
  }

  dDebug("msg:%s will be redirected, num:%d use:%d", taosMsg[rpcMsg->msgType], epSet.numOfEps, epSet.inUse);

  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%d", i, epSet.fqdn[i], epSet.port[i]);
    if (strcmp(epSet.fqdn[i], tsLocalFqdn) == 0) {
      if ((epSet.port[i] == tsServerPort + TSDB_PORT_DNODEDNODE && !forShell) ||
          (epSet.port[i] == tsServerPort && forShell)) {
        epSet.inUse = (i + 1) % epSet.numOfEps;
        dDebug("mnode index:%d %s:%d set inUse to %d", i, epSet.fqdn[i], epSet.port[i], epSet.inUse);
      }
    }

    epSet.port[i] = htons(epSet.port[i]);
  }

  rpcSendRedirectRsp(rpcMsg->handle, &epSet);
}

static void dnodePrintEps() {
  dDebug("print dnode list, num:%d", tsEps.dnodeEps->dnodeNum);
  for (int32_t i = 0; i < tsEps.dnodeEps->dnodeNum; i++) {
    SDnodeEp *ep = &tsEps.dnodeEps->dnodeEps[i];
    dDebug("dnode:%d, fqdn:%s port:%u isMnode:%d", ep->dnodeId, ep->dnodeFqdn, ep->dnodePort, ep->isMnode);
  }
}

static void dnodeResetEps(SDnodeEps *data) {
  assert(data != NULL);

  int32_t size = sizeof(SDnodeEps) + data->dnodeNum * sizeof(SDnodeEp);

  if (data->dnodeNum > tsEps.dnodeEps->dnodeNum) {
    SDnodeEps *tmp = calloc(1, size);
    if (tmp == NULL) return;

    tfree(tsEps.dnodeEps);
    tsEps.dnodeEps = tmp;
  }

  if (tsEps.dnodeEps != data) {
    memcpy(tsEps.dnodeEps, data, size);
  }

  tsEps.mnodeEpSetForPeer.inUse = 0;
  tsEps.mnodeEpSetForShell.inUse = 0;
  int32_t index = 0;
  for (int32_t i = 0; i < tsEps.dnodeEps->dnodeNum; i++) {
    SDnodeEp *ep = &tsEps.dnodeEps->dnodeEps[i];
    if (!ep->isMnode) continue;
    if (index >= TSDB_MAX_REPLICA) continue;
    strcpy(tsEps.mnodeEpSetForShell.fqdn[index], ep->dnodeFqdn);
    strcpy(tsEps.mnodeEpSetForPeer.fqdn[index], ep->dnodeFqdn);
    tsEps.mnodeEpSetForShell.port[index] = ep->dnodePort;
    tsEps.mnodeEpSetForShell.port[index] = ep->dnodePort + tsDnodeDnodePort;
    index++;
  }

  for (int32_t i = 0; i < tsEps.dnodeEps->dnodeNum; ++i) {
    SDnodeEp *ep = &tsEps.dnodeEps->dnodeEps[i];
    taosHashPut(tsEps.dnodeHash, &ep->dnodeId, sizeof(int32_t), ep, sizeof(SDnodeEp));
  }

  dnodePrintEps();
}

static bool dnodeIsDnodeEpChanged(int32_t dnodeId, char *epstr) {
  bool changed = false;

  pthread_mutex_lock(&tsEps.mutex);

  SDnodeEp *ep = taosHashGet(tsEps.dnodeHash, &dnodeId, sizeof(int32_t));
  if (ep != NULL) {
    char epSaved[TSDB_EP_LEN + 1];
    snprintf(epSaved, TSDB_EP_LEN, "%s:%u", ep->dnodeFqdn, ep->dnodePort);
    changed = strcmp(epstr, epSaved) != 0;
    tstrncpy(epstr, epSaved, TSDB_EP_LEN);
  }

  pthread_mutex_unlock(&tsEps.mutex);

  return changed;
}

static int32_t dnodeReadEps() {
  int32_t len = 0;
  int32_t maxLen = 30000;
  char   *content = calloc(1, maxLen + 1);
  cJSON  *root = NULL;
  FILE   *fp = NULL;

  fp = fopen(tsEps.file, "r");
  if (!fp) {
    dDebug("file %s not exist", tsEps.file);
    goto PRASE_EPS_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", tsEps.file);
    goto PRASE_EPS_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", tsEps.file);
    goto PRASE_EPS_OVER;
  }

  cJSON *dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_String) {
    dError("failed to read %s since dnodeId not found", tsEps.file);
    goto PRASE_EPS_OVER;
  }
  tsEps.dnodeId = atoi(dnodeId->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_String) {
    dError("failed to read %s since dropped not found", tsEps.file);
    goto PRASE_EPS_OVER;
  }
  tsEps.dropped = atoi(dropped->valuestring);

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s since clusterId not found", tsEps.file);
    goto PRASE_EPS_OVER;
  }
  tsEps.clusterId = atoll(clusterId->valuestring);

  cJSON *dnodeInfos = cJSON_GetObjectItem(root, "dnodeInfos");
  if (!dnodeInfos || dnodeInfos->type != cJSON_Array) {
    dError("failed to read %s since dnodeInfos not found", tsEps.file);
    goto PRASE_EPS_OVER;
  }

  int32_t dnodeInfosSize = cJSON_GetArraySize(dnodeInfos);
  if (dnodeInfosSize <= 0) {
    dError("failed to read %s since dnodeInfos size:%d invalid", tsEps.file, dnodeInfosSize);
    goto PRASE_EPS_OVER;
  }

  tsEps.dnodeEps = calloc(1, dnodeInfosSize * sizeof(SDnodeEp) + sizeof(SDnodeEps));
  if (tsEps.dnodeEps == NULL) {
    dError("failed to calloc dnodeEpList since %s", strerror(errno));
    goto PRASE_EPS_OVER;
  }
  tsEps.dnodeEps->dnodeNum = dnodeInfosSize;

  for (int32_t i = 0; i < dnodeInfosSize; ++i) {
    cJSON *dnodeInfo = cJSON_GetArrayItem(dnodeInfos, i);
    if (dnodeInfo == NULL) break;

    SDnodeEp *ep = &tsEps.dnodeEps->dnodeEps[i];

    cJSON *dnodeId = cJSON_GetObjectItem(dnodeInfo, "dnodeId");
    if (!dnodeId || dnodeId->type != cJSON_String) {
      dError("failed to read %s, dnodeId not found", tsEps.file);
      goto PRASE_EPS_OVER;
    }
    ep->dnodeId = atoi(dnodeId->valuestring);

    cJSON *isMnode = cJSON_GetObjectItem(dnodeInfo, "isMnode");
    if (!isMnode || isMnode->type != cJSON_String) {
      dError("failed to read %s, isMnode not found", tsEps.file);
      goto PRASE_EPS_OVER;
    }
    ep->isMnode = atoi(isMnode->valuestring);

    cJSON *dnodeFqdn = cJSON_GetObjectItem(dnodeInfo, "dnodeFqdn");
    if (!dnodeFqdn || dnodeFqdn->type != cJSON_String || dnodeFqdn->valuestring == NULL) {
      dError("failed to read %s, dnodeFqdn not found", tsEps.file);
      goto PRASE_EPS_OVER;
    }
    tstrncpy(ep->dnodeFqdn, dnodeFqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *dnodePort = cJSON_GetObjectItem(dnodeInfo, "dnodePort");
    if (!dnodePort || dnodePort->type != cJSON_String) {
      dError("failed to read %s, dnodePort not found", tsEps.file);
      goto PRASE_EPS_OVER;
    }
    ep->dnodePort = atoi(dnodePort->valuestring);
  }

  dInfo("succcessed to read file %s", tsEps.file);
  dnodePrintEps();

PRASE_EPS_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  if (dnodeIsDnodeEpChanged(tsEps.dnodeId, tsLocalEp)) {
    dError("dnode:%d, localEp %s different with dnodeEps.json and need reconfigured", tsEps.dnodeId, tsLocalEp);
    return -1;
  }

  dnodeResetEps(tsEps.dnodeEps);

  terrno = 0;
  return 0;
}

static int32_t dnodeWriteEps() {
  FILE *fp = fopen(tsEps.file, "w");
  if (!fp) {
    dError("failed to write %s since %s", tsEps.file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 30000;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": \"%d\",\n", tsEps.dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": \"%d\",\n", tsEps.dropped);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%" PRId64 "\",\n", tsEps.clusterId);
  len += snprintf(content + len, maxLen - len, "  \"dnodeInfos\": [{\n");
  for (int32_t i = 0; i < tsEps.dnodeEps->dnodeNum; ++i) {
    SDnodeEp *ep = &tsEps.dnodeEps->dnodeEps[i];
    len += snprintf(content + len, maxLen - len, "    \"dnodeId\": \"%d\",\n", ep->dnodeId);
    len += snprintf(content + len, maxLen - len, "    \"isMnode\": \"%d\",\n", ep->isMnode);
    len += snprintf(content + len, maxLen - len, "    \"dnodeFqdn\": \"%s\",\n", ep->dnodeFqdn);
    len += snprintf(content + len, maxLen - len, "    \"dnodePort\": \"%u\"\n", ep->dnodePort);
    if (i < tsEps.dnodeEps->dnodeNum - 1) {
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

  dInfo("successed to write %s", tsEps.file);
  return 0;
}

int32_t dnodeInitEps() {
  tsEps.dnodeId = 0;
  tsEps.dropped = 0;
  tsEps.clusterId = 0;
  tsEps.dnodeEps = NULL;
  snprintf(tsEps.file, sizeof(tsEps.file), "%s/dnodeEps.json", tsDnodeDir);
  pthread_mutex_init(&tsEps.mutex, NULL);

  tsEps.dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsEps.dnodeHash == NULL) return -1;

  int32_t ret = dnodeReadEps();
  if (ret == 0) {
    dInfo("dnode eps is initialized");
  }

  return ret;
}

void dnodeCleanupEps() {
  pthread_mutex_lock(&tsEps.mutex);

  if (tsEps.dnodeEps != NULL) {
    free(tsEps.dnodeEps);
    tsEps.dnodeEps = NULL;
  }

  if (tsEps.dnodeHash) {
    taosHashCleanup(tsEps.dnodeHash);
    tsEps.dnodeHash = NULL;
  }

  pthread_mutex_unlock(&tsEps.mutex);
  pthread_mutex_destroy(&tsEps.mutex);
}

void dnodeUpdateDnodeEps(SDnodeEps *data) {
  if (data == NULL || data->dnodeNum <= 0) return;

  pthread_mutex_lock(&tsEps.mutex);

  if (data->dnodeNum != tsEps.dnodeEps->dnodeNum) {
    dnodeResetEps(data);
    dnodeWriteEps();
  } else {
    int32_t size = data->dnodeNum * sizeof(SDnodeEp) + sizeof(SDnodeEps);
    if (memcmp(tsEps.dnodeEps, data, size) != 0) {
      dnodeResetEps(data);
      dnodeWriteEps();
    }
  }

  pthread_mutex_unlock(&tsEps.mutex);
}

void dnodeGetEp(int32_t dnodeId, char *epstr, char *fqdn, uint16_t *port) {
  pthread_mutex_lock(&tsEps.mutex);

  SDnodeEp *ep = taosHashGet(tsEps.dnodeHash, &dnodeId, sizeof(int32_t));
  if (ep != NULL) {
    if (port) *port = ep->dnodePort;
    if (fqdn) tstrncpy(fqdn, ep->dnodeFqdn, TSDB_FQDN_LEN);
    if (epstr) snprintf(epstr, TSDB_EP_LEN, "%s:%u", ep->dnodeFqdn, ep->dnodePort);
  }

  pthread_mutex_unlock(&tsEps.mutex);
}

void dnodeUpdateCfg(SDnodeCfg *data) {
  if (tsEps.dnodeId != 0 && !data->dropped) return;

  pthread_mutex_lock(&tsEps.mutex);

  tsEps.dnodeId = data->dnodeId;
  tsEps.clusterId = data->clusterId;
  tsEps.dropped = data->dropped;
  dInfo("dnodeId is set to %d, clusterId is set to %" PRId64, data->dnodeId, data->clusterId);

  dnodeWriteEps();
  pthread_mutex_unlock(&tsEps.mutex);
}

int32_t dnodeGetDnodeId() {
  int32_t dnodeId = 0;
  pthread_mutex_lock(&tsEps.mutex);
  dnodeId = tsEps.dnodeId;
  pthread_mutex_unlock(&tsEps.mutex);
  return dnodeId;
}

int64_t dnodeGetClusterId() {
  int64_t clusterId = 0;
  pthread_mutex_lock(&tsEps.mutex);
  clusterId = tsEps.clusterId;
  pthread_mutex_unlock(&tsEps.mutex);
  return clusterId;
}
