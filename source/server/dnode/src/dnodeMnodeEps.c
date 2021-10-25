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
#include "tglobal.h"
#include "dnodeCfg.h"
#include "dnodeDnodeEps.h"
#include "dnodeMnodeEps.h"
#include "mnode.h"

static struct {
  SRpcEpSet       mnodeEpSet;
  SMInfos         mnodeInfos;
  char            file[PATH_MAX + 20];
  pthread_mutex_t mutex;
} tsDmeps;


static void dnodePrintMnodeEps() {
  SRpcEpSet *epset = &tsDmeps.mnodeEpSet;
  dInfo("print mnode eps, num:%d inuse:%d", epset->numOfEps, epset->inUse);
  for (int32_t i = 0; i < epset->numOfEps; i++) {
    dInfo("ep index:%d, %s:%u", i, epset->fqdn[i], epset->port[i]);
  }
}

static void dnodeResetMnodeEps(SMInfos *mInfos) {
  if (mInfos == NULL || mInfos->mnodeNum == 0) {
    tsDmeps.mnodeEpSet.numOfEps = 1;
    taosGetFqdnPortFromEp(tsFirst, tsDmeps.mnodeEpSet.fqdn[0], &tsDmeps.mnodeEpSet.port[0]);

    if (strcmp(tsSecond, tsFirst) != 0) {
      tsDmeps.mnodeEpSet.numOfEps = 2;
      taosGetFqdnPortFromEp(tsSecond, tsDmeps.mnodeEpSet.fqdn[1], &tsDmeps.mnodeEpSet.port[1]);
    }
    dnodePrintMnodeEps();
    return;
  }

  int32_t size = sizeof(SMInfos);
  memcpy(&tsDmeps.mnodeInfos, mInfos, size);

  tsDmeps.mnodeEpSet.inUse = tsDmeps.mnodeInfos.inUse;
  tsDmeps.mnodeEpSet.numOfEps = tsDmeps.mnodeInfos.mnodeNum;
  for (int32_t i = 0; i < tsDmeps.mnodeInfos.mnodeNum; i++) {
    taosGetFqdnPortFromEp(tsDmeps.mnodeInfos.mnodeInfos[i].mnodeEp, tsDmeps.mnodeEpSet.fqdn[i], &tsDmeps.mnodeEpSet.port[i]);
  }

  dnodePrintMnodeEps();
}

static int32_t dnodeWriteMnodeEps() {
  FILE *fp = fopen(tsDmeps.file, "w");
  if (!fp) {
    dError("failed to write %s since %s", tsDmeps.file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 2000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"inUse\": %d,\n", tsDmeps.mnodeInfos.inUse);
  len += snprintf(content + len, maxLen - len, "  \"nodeNum\": %d,\n", tsDmeps.mnodeInfos.mnodeNum);
  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");
  for (int32_t i = 0; i < tsDmeps.mnodeInfos.mnodeNum; i++) {
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", tsDmeps.mnodeInfos.mnodeInfos[i].mnodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s\"\n", tsDmeps.mnodeInfos.mnodeInfos[i].mnodeEp);
    if (i < tsDmeps.mnodeInfos.mnodeNum - 1) {
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

  dInfo("successed to write %s", tsDmeps.file);
  return 0;
}

static int32_t dnodeReadMnodeEps() {
  int32_t len = 0;
  int32_t maxLen = 2000;
  char *  content = calloc(1, maxLen + 1);
  cJSON * root = NULL;
  FILE *  fp = NULL;
  SMInfos mInfos = {0};
  bool    nodeChanged = false;

  fp = fopen(tsDmeps.file, "r");
  if (!fp) {
    dDebug("file %s not exist", tsDmeps.file);
    goto PARSE_MINFOS_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", tsDmeps.file);
    goto PARSE_MINFOS_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", tsDmeps.file);
    goto PARSE_MINFOS_OVER;
  }

  cJSON *inUse = cJSON_GetObjectItem(root, "inUse");
  if (!inUse || inUse->type != cJSON_Number) {
    dError("failed to read mnodeEpSet.json since inUse not found");
    goto PARSE_MINFOS_OVER;
  }
  tsDmeps.mnodeInfos.inUse = (int8_t)inUse->valueint;

  cJSON *nodeNum = cJSON_GetObjectItem(root, "nodeNum");
  if (!nodeNum || nodeNum->type != cJSON_Number) {
    dError("failed to read mnodeEpSet.json since nodeNum not found");
    goto PARSE_MINFOS_OVER;
  }
  mInfos.mnodeNum = (int8_t)nodeNum->valueint;

  cJSON *nodeInfos = cJSON_GetObjectItem(root, "nodeInfos");
  if (!nodeInfos || nodeInfos->type != cJSON_Array) {
    dError("failed to read mnodeEpSet.json since nodeInfos not found");
    goto PARSE_MINFOS_OVER;
  }

  int32_t size = cJSON_GetArraySize(nodeInfos);
  if (size != mInfos.mnodeNum) {
    dError("failed to read mnodeEpSet.json since nodeInfos size not matched");
    goto PARSE_MINFOS_OVER;
  }

  for (int32_t i = 0; i < size; ++i) {
    cJSON *nodeInfo = cJSON_GetArrayItem(nodeInfos, i);
    if (nodeInfo == NULL) continue;

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "nodeId");
    if (!nodeId || nodeId->type != cJSON_Number) {
      dError("failed to read mnodeEpSet.json since nodeId not found");
      goto PARSE_MINFOS_OVER;
    }

    cJSON *nodeEp = cJSON_GetObjectItem(nodeInfo, "nodeEp");
    if (!nodeEp || nodeEp->type != cJSON_String || nodeEp->valuestring == NULL) {
      dError("failed to read mnodeEpSet.json since nodeName not found");
      goto PARSE_MINFOS_OVER;
    }

    SMInfo *mInfo = &mInfos.mnodeInfos[i];
    mInfo->mnodeId = (int32_t)nodeId->valueint;
    tstrncpy(mInfo->mnodeEp, nodeEp->valuestring, TSDB_EP_LEN);

    bool changed = dnodeIsDnodeEpChanged(mInfo->mnodeId, mInfo->mnodeEp);
    if (changed) nodeChanged = changed;
  }

  dInfo("successed to read file %s", tsDmeps.file);

PARSE_MINFOS_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);
  terrno = 0;

  for (int32_t i = 0; i < mInfos.mnodeNum; ++i) {
    SMInfo *mInfo = &mInfos.mnodeInfos[i];
    dnodeGetDnodeEp(mInfo->mnodeId, mInfo->mnodeEp, NULL, NULL);
  }

  dnodeResetMnodeEps(&mInfos);

  if (nodeChanged) {
    dnodeWriteMnodeEps();
  }

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

int32_t dnodeInitMnodeEps() {
  snprintf(tsDmeps.file, sizeof(tsDmeps.file), "%s/mnodeEpSet.json", tsDnodeDir);
  pthread_mutex_init(&tsDmeps.mutex, NULL);

  dnodeResetMnodeEps(NULL);
  int32_t ret = dnodeReadMnodeEps();
  if (ret == 0) {
    dInfo("dnode mInfos is initialized");
  }

  return ret;
}

void dnodeCleanupMnodeEps() {
  pthread_mutex_destroy(&tsDmeps.mutex);
}

void dnodeUpdateMnodeFromStatus(SMInfos *mInfos) {
  if (mInfos->mnodeNum <= 0 || mInfos->mnodeNum > TSDB_MAX_REPLICA) {
    dError("invalid mInfos since num:%d invalid", mInfos->mnodeNum);
    return;
  }

  for (int32_t i = 0; i < mInfos->mnodeNum; ++i) {
    SMInfo *minfo = &mInfos->mnodeInfos[i];
    minfo->mnodeId = htonl(minfo->mnodeId);
    if (minfo->mnodeId <= 0 || strlen(minfo->mnodeEp) <= 5) {
      dError("invalid mInfo:%d since id:%d and ep:%s invalid", i, minfo->mnodeId, minfo->mnodeEp);
      return;
    }
  }

  pthread_mutex_lock(&tsDmeps.mutex);
  if (mInfos->mnodeNum != tsDmeps.mnodeInfos.mnodeNum) {
    dnodeResetMnodeEps(mInfos);
    dnodeWriteMnodeEps();
  } else {
    int32_t size = sizeof(SMInfos);
    if (memcmp(mInfos, &tsDmeps.mnodeInfos, size) != 0) {
      dnodeResetMnodeEps(mInfos);
      dnodeWriteMnodeEps();
    }
  }
  pthread_mutex_unlock(&tsDmeps.mutex);
}

void dnodeUpdateMnodeFromPeer(SRpcEpSet *ep) {
  if (ep->numOfEps <= 0) {
    dError("mInfos is changed, but content is invalid, discard it");
    return;
  }

  pthread_mutex_lock(&tsDmeps.mutex);

  dInfo("mInfos is changed, numOfEps:%d inUse:%d", ep->numOfEps, ep->inUse);
  for (int32_t i = 0; i < ep->numOfEps; ++i) {
    ep->port[i] -= TSDB_PORT_DNODEDNODE;
    dInfo("minfo:%d %s:%u", i, ep->fqdn[i], ep->port[i]);
  }
  tsDmeps.mnodeEpSet = *ep;

  pthread_mutex_unlock(&tsDmeps.mutex);
}

void dnodeGetEpSetForPeer(SRpcEpSet *epSet) {
  pthread_mutex_lock(&tsDmeps.mutex);

  *epSet = tsDmeps.mnodeEpSet;
  for (int32_t i = 0; i < epSet->numOfEps; ++i) {
    epSet->port[i] += TSDB_PORT_DNODEDNODE;
  }

  pthread_mutex_unlock(&tsDmeps.mutex);
}

void dnodeGetEpSetForShell(SRpcEpSet *epSet) {
  pthread_mutex_lock(&tsDmeps.mutex);
  *epSet = tsDmeps.mnodeEpSet;
  pthread_mutex_unlock(&tsDmeps.mutex);
}
