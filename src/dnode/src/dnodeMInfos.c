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
#include "cJSON.h"
#include "mnode.h"
#include "dnodeMInfos.h"

static SMInfos   tsMInfos;
static SRpcEpSet tsMEpSet;
static pthread_mutex_t tsMInfosMutex;

static void    dnodeResetMInfos(SMInfos *minfos);
static void    dnodePrintMInfos(SMInfos *minfos);
static int32_t dnodeReadMInfos();
static int32_t dnodeWriteMInfos();

int32_t dnodeInitMInfos() {
  pthread_mutex_init(&tsMInfosMutex, NULL);
  dnodeResetMInfos(NULL);
  int32_t ret = dnodeReadMInfos();
  if (ret == 0) {
    dInfo("dnode minfos is initialized");
  }

  return ret;
}

void dnodeCleanupMInfos() { pthread_mutex_destroy(&tsMInfosMutex); }

void dnodeUpdateMInfos(SMInfos *pMinfos) {
  if (pMinfos->mnodeNum <= 0 || pMinfos->mnodeNum > 3) {
    dError("invalid mnode infos, mnodeNum:%d", pMinfos->mnodeNum);
    return;
  }

  for (int32_t i = 0; i < pMinfos->mnodeNum; ++i) {
    SMInfo *minfo = &pMinfos->mnodeInfos[i];
    minfo->mnodeId = htonl(minfo->mnodeId);
    if (minfo->mnodeId <= 0 || strlen(minfo->mnodeEp) <= 5) {
      dError("invalid mnode info:%d, mnodeId:%d mnodeEp:%s", i, minfo->mnodeId, minfo->mnodeEp);
      return;
    }
  }

  pthread_mutex_lock(&tsMInfosMutex);
  if (pMinfos->mnodeNum != tsMInfos.mnodeNum) {
    dnodeResetMInfos(pMinfos);
    dnodeWriteMInfos();
    sdbUpdateAsync();
  } else {
    int32_t size = sizeof(SMInfos);
    if (memcmp(pMinfos, &tsMInfos, size) != 0) {
      dnodeResetMInfos(pMinfos);
      dnodeWriteMInfos();
      sdbUpdateAsync();
    }
  }
  pthread_mutex_unlock(&tsMInfosMutex);
}

void dnodeUpdateEpSetForPeer(SRpcEpSet *ep) {
  if (ep->numOfEps <= 0) {
    dError("minfos is changed, but content is invalid, discard it");
    return;
  }

  pthread_mutex_lock(&tsMInfosMutex);
  dInfo("minfos is changed, numOfEps:%d inUse:%d", ep->numOfEps, ep->inUse);
  for (int i = 0; i < ep->numOfEps; ++i) {
    ep->port[i] -= TSDB_PORT_DNODEDNODE;
    dInfo("minfo:%d %s:%u", i, ep->fqdn[i], ep->port[i]);
  }
  tsMEpSet = *ep;
  pthread_mutex_unlock(&tsMInfosMutex);
}

bool dnodeIsMasterEp(char *ep) {
  pthread_mutex_lock(&tsMInfosMutex);
  bool isMaster = strcmp(ep, tsMInfos.mnodeInfos[tsMEpSet.inUse].mnodeEp) == 0;
  pthread_mutex_unlock(&tsMInfosMutex);

  return isMaster;
}

void dnodeGetMInfos(SMInfos *pMinfos) {
  pthread_mutex_lock(&tsMInfosMutex);
  memcpy(pMinfos, &tsMInfos, sizeof(SMInfos));
  for (int32_t i = 0; i < tsMInfos.mnodeNum; ++i) {
    pMinfos->mnodeInfos[i].mnodeId = htonl(tsMInfos.mnodeInfos[i].mnodeId);
  }
  pthread_mutex_unlock(&tsMInfosMutex);
}

void dnodeGetEpSetForPeer(SRpcEpSet *epSet) {
  pthread_mutex_lock(&tsMInfosMutex);
  *epSet = tsMEpSet;
  for (int i = 0; i < epSet->numOfEps; ++i) {
    epSet->port[i] += TSDB_PORT_DNODEDNODE;
  }
  pthread_mutex_unlock(&tsMInfosMutex);
}

void dnodeGetEpSetForShell(SRpcEpSet *epSet) {
  pthread_mutex_lock(&tsMInfosMutex);
  *epSet = tsMEpSet;
  pthread_mutex_unlock(&tsMInfosMutex);
}

static void dnodePrintMInfos(SMInfos *pMinfos) {
  dInfo("print minfos, mnodeNum:%d inUse:%d", pMinfos->mnodeNum, pMinfos->inUse);
  for (int32_t i = 0; i < pMinfos->mnodeNum; i++) {
    dInfo("mnode index:%d, %s", pMinfos->mnodeInfos[i].mnodeId, pMinfos->mnodeInfos[i].mnodeEp);
  }
}

static void dnodeResetMInfos(SMInfos *pMinfos) {
  if (pMinfos == NULL) {
    tsMEpSet.numOfEps = 1;
    taosGetFqdnPortFromEp(tsFirst, tsMEpSet.fqdn[0], &tsMEpSet.port[0]);

    if (strcmp(tsSecond, tsFirst) != 0) {
      tsMEpSet.numOfEps = 2;
      taosGetFqdnPortFromEp(tsSecond, tsMEpSet.fqdn[1], &tsMEpSet.port[1]);
    }
    return;
  }

  if (pMinfos->mnodeNum == 0) return;

  int32_t size = sizeof(SMInfos);
  memcpy(&tsMInfos, pMinfos, size);

  tsMEpSet.inUse = tsMInfos.inUse;
  tsMEpSet.numOfEps = tsMInfos.mnodeNum;
  for (int32_t i = 0; i < tsMInfos.mnodeNum; i++) {
    taosGetFqdnPortFromEp(tsMInfos.mnodeInfos[i].mnodeEp, tsMEpSet.fqdn[i], &tsMEpSet.port[i]);
  }

  dnodePrintMInfos(pMinfos);
}

static int32_t dnodeReadMInfos() {
  int32_t len = 0;
  int32_t maxLen = 2000;
  char *  content = calloc(1, maxLen + 1);
  cJSON * root = NULL;
  FILE *  fp = NULL;
  SMInfos minfos = {0};
  bool    nodeChanged = false;

  char file[TSDB_FILENAME_LEN + 20] = {0};
  sprintf(file, "%s/mnodeEpSet.json", tsDnodeDir);

  fp = fopen(file, "r");
  if (!fp) {
    dDebug("failed to read %s, file not exist", file);
    goto PARSE_MINFOS_OVER;
  }

  len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s, content is null", file);
    goto PARSE_MINFOS_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s, invalid json format", file);
    goto PARSE_MINFOS_OVER;
  }

  cJSON *inUse = cJSON_GetObjectItem(root, "inUse");
  if (!inUse || inUse->type != cJSON_Number) {
    dError("failed to read mnodeEpSet.json, inUse not found");
    goto PARSE_MINFOS_OVER;
  }
  tsMInfos.inUse = inUse->valueint;

  cJSON *nodeNum = cJSON_GetObjectItem(root, "nodeNum");
  if (!nodeNum || nodeNum->type != cJSON_Number) {
    dError("failed to read mnodeEpSet.json, nodeNum not found");
    goto PARSE_MINFOS_OVER;
  }
  minfos.mnodeNum = nodeNum->valueint;

  cJSON *nodeInfos = cJSON_GetObjectItem(root, "nodeInfos");
  if (!nodeInfos || nodeInfos->type != cJSON_Array) {
    dError("failed to read mnodeEpSet.json, nodeInfos not found");
    goto PARSE_MINFOS_OVER;
  }

  int size = cJSON_GetArraySize(nodeInfos);
  if (size != minfos.mnodeNum) {
    dError("failed to read mnodeEpSet.json, nodeInfos size not matched");
    goto PARSE_MINFOS_OVER;
  }

  for (int i = 0; i < size; ++i) {
    cJSON *nodeInfo = cJSON_GetArrayItem(nodeInfos, i);
    if (nodeInfo == NULL) continue;

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "nodeId");
    if (!nodeId || nodeId->type != cJSON_Number) {
      dError("failed to read mnodeEpSet.json, nodeId not found");
      goto PARSE_MINFOS_OVER;
    }

    cJSON *nodeEp = cJSON_GetObjectItem(nodeInfo, "nodeEp");
    if (!nodeEp || nodeEp->type != cJSON_String || nodeEp->valuestring == NULL) {
      dError("failed to read mnodeEpSet.json, nodeName not found");
      goto PARSE_MINFOS_OVER;
    }

    SMInfo *pMinfo = &minfos.mnodeInfos[i];
    pMinfo->mnodeId = nodeId->valueint;
    tstrncpy(pMinfo->mnodeEp, nodeEp->valuestring, TSDB_EP_LEN);

    bool changed = dnodeCheckEpChanged(pMinfo->mnodeId, pMinfo->mnodeEp);
    if (changed) nodeChanged = changed;
  }

  dInfo("read file %s successed", file);
  dnodePrintMInfos(&minfos);

PARSE_MINFOS_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);
  terrno = 0;

  for (int32_t i = 0; i < minfos.mnodeNum; ++i) {
    SMInfo *mInfo = &minfos.mnodeInfos[i];
    dnodeUpdateEp(mInfo->mnodeId, mInfo->mnodeEp, NULL, NULL);
  }
  dnodeResetMInfos(&minfos);

  if (nodeChanged) {
    dnodeWriteMInfos();
  }

  return 0;
}

static int32_t dnodeWriteMInfos() {
  char file[TSDB_FILENAME_LEN + 20] = {0};
  sprintf(file, "%s/mnodeEpSet.json", tsDnodeDir);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    dError("failed to write %s, reason:%s", file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 2000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"inUse\": %d,\n", tsMInfos.inUse);
  len += snprintf(content + len, maxLen - len, "  \"nodeNum\": %d,\n", tsMInfos.mnodeNum);
  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");
  for (int32_t i = 0; i < tsMInfos.mnodeNum; i++) {
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", tsMInfos.mnodeInfos[i].mnodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s\"\n", tsMInfos.mnodeInfos[i].mnodeEp);
    if (i < tsMInfos.mnodeNum - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fflush(fp);
  fclose(fp);
  free(content);
  terrno = 0;

  dInfo("successed to write %s", file);
  return 0;
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
  
  dDebug("msg:%s will be redirected, dnodeIp:%s user:%s, numOfEps:%d inUse:%d", taosMsg[rpcMsg->msgType],
         taosIpStr(connInfo.clientIp), connInfo.user, epSet.numOfEps, epSet.inUse);

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
