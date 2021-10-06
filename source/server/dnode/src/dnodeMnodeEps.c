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
#include "dnodeEps.h"
#include "dnodeMnodeEps.h"
#include "mnode.h"

static void dnodePrintMnodeEps(SDnMnEps *meps) {
  SRpcEpSet *epset = &meps->mnodeEpSet;
  dInfo("print mnode eps, num:%d inuse:%d", epset->numOfEps, epset->inUse);
  for (int32_t i = 0; i < epset->numOfEps; i++) {
    dInfo("ep index:%d, %s:%u", i, epset->fqdn[i], epset->port[i]);
  }
}

static void dnodeResetMnodeEps(SDnMnEps *meps, SMInfos *mInfos) {
  if (mInfos == NULL || mInfos->mnodeNum == 0) {
    meps->mnodeEpSet.numOfEps = 1;
    taosGetFqdnPortFromEp(tsFirst, meps->mnodeEpSet.fqdn[0], &meps->mnodeEpSet.port[0]);

    if (strcmp(tsSecond, tsFirst) != 0) {
      meps->mnodeEpSet.numOfEps = 2;
      taosGetFqdnPortFromEp(tsSecond, meps->mnodeEpSet.fqdn[1], &meps->mnodeEpSet.port[1]);
    }
    dnodePrintMnodeEps(meps);
    return;
  }

    int32_t size = sizeof(SMInfos);
  memcpy(&meps->mnodeInfos, mInfos, size);

  meps->mnodeEpSet.inUse = meps->mnodeInfos.inUse;
  meps->mnodeEpSet.numOfEps = meps->mnodeInfos.mnodeNum;
  for (int32_t i = 0; i < meps->mnodeInfos.mnodeNum; i++) {
    taosGetFqdnPortFromEp(meps->mnodeInfos.mnodeInfos[i].mnodeEp, meps->mnodeEpSet.fqdn[i], &meps->mnodeEpSet.port[i]);
  }

  dnodePrintMnodeEps(meps);
}

static int32_t dnodeWriteMnodeEps(SDnMnEps *meps) {
  FILE *fp = fopen(meps->file, "w");
  if (!fp) {
    dError("failed to write %s since %s", meps->file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 2000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"inUse\": %d,\n", meps->mnodeInfos.inUse);
  len += snprintf(content + len, maxLen - len, "  \"nodeNum\": %d,\n", meps->mnodeInfos.mnodeNum);
  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");
  for (int32_t i = 0; i < meps->mnodeInfos.mnodeNum; i++) {
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", meps->mnodeInfos.mnodeInfos[i].mnodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s\"\n", meps->mnodeInfos.mnodeInfos[i].mnodeEp);
    if (i < meps->mnodeInfos.mnodeNum - 1) {
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

  dInfo("successed to write %s", meps->file);
  return 0;
}

static int32_t dnodeReadMnodeEps(SDnMnEps *meps, SDnEps *deps) {
  int32_t len = 0;
  int32_t maxLen = 2000;
  char *  content = calloc(1, maxLen + 1);
  cJSON * root = NULL;
  FILE *  fp = NULL;
  SMInfos mInfos = {0};
  bool    nodeChanged = false;

  fp = fopen(meps->file, "r");
  if (!fp) {
    dDebug("file %s not exist", meps->file);
    goto PARSE_MINFOS_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", meps->file);
    goto PARSE_MINFOS_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", meps->file);
    goto PARSE_MINFOS_OVER;
  }

  cJSON *inUse = cJSON_GetObjectItem(root, "inUse");
  if (!inUse || inUse->type != cJSON_Number) {
    dError("failed to read mnodeEpSet.json since inUse not found");
    goto PARSE_MINFOS_OVER;
  }
  meps->mnodeInfos.inUse = (int8_t)inUse->valueint;

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

    bool changed = dnodeIsDnodeEpChanged(deps, mInfo->mnodeId, mInfo->mnodeEp);
    if (changed) nodeChanged = changed;
  }

  dInfo("successed to read file %s", meps->file);

PARSE_MINFOS_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);
  terrno = 0;

  for (int32_t i = 0; i < mInfos.mnodeNum; ++i) {
    SMInfo *mInfo = &mInfos.mnodeInfos[i];
    dnodeGetDnodeEp(mInfo->mnodeId, mInfo->mnodeEp, NULL, NULL);
  }

  dnodeResetMnodeEps(meps, &mInfos);

  if (nodeChanged) {
    dnodeWriteMnodeEps(meps);
  }

  return 0;
}

void dnodeSendRedirectMsg(SRpcMsg *rpcMsg, bool forShell) {
  SDnMnEps *meps = dnodeInst()->meps;
  SRpcConnInfo connInfo = {0};
  rpcGetConnInfo(rpcMsg->handle, &connInfo);

  SRpcEpSet epSet = {0};
  if (forShell) {
    dnodeGetEpSetForShell(meps, &epSet);
  } else {
    dnodeGetEpSetForPeer(meps, &epSet);
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

int32_t dnodeInitMnodeEps(SDnMnEps **out) {
  SDnMnEps *meps = calloc(1, sizeof(SDnMnEps));
  if (meps == NULL) return -1;

  snprintf(meps->file, sizeof(meps->file), "%s/mnodeEpSet.json", tsDnodeDir);
  pthread_mutex_init(&meps->mutex, NULL);
  *out = meps;

  dnodeResetMnodeEps(meps, NULL);
  int32_t ret = dnodeReadMnodeEps(meps, dnodeInst()->eps);
  if (ret == 0) {
    dInfo("dnode mInfos is initialized");
  }

  return ret;
}

void dnodeCleanupMnodeEps(SDnMnEps **out) {
  SDnMnEps *meps = *out;
  *out = NULL;

  if (meps != NULL) {
    pthread_mutex_destroy(&meps->mutex);
    free(meps);
  }
}

void dnodeUpdateMnodeFromStatus(SDnMnEps *meps, SMInfos *mInfos) {
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

  pthread_mutex_lock(&meps->mutex);
  if (mInfos->mnodeNum != meps->mnodeInfos.mnodeNum) {
    dnodeResetMnodeEps(meps, mInfos);
    dnodeWriteMnodeEps(meps);
  } else {
    int32_t size = sizeof(SMInfos);
    if (memcmp(mInfos, &meps->mnodeInfos, size) != 0) {
      dnodeResetMnodeEps(meps, mInfos);
      dnodeWriteMnodeEps(meps);
    }
  }
  pthread_mutex_unlock(&meps->mutex);
}

void dnodeUpdateMnodeFromPeer(SDnMnEps *meps, SRpcEpSet *ep) {
  if (ep->numOfEps <= 0) {
    dError("mInfos is changed, but content is invalid, discard it");
    return;
  }

  pthread_mutex_lock(&meps->mutex);

  dInfo("mInfos is changed, numOfEps:%d inUse:%d", ep->numOfEps, ep->inUse);
  for (int32_t i = 0; i < ep->numOfEps; ++i) {
    ep->port[i] -= TSDB_PORT_DNODEDNODE;
    dInfo("minfo:%d %s:%u", i, ep->fqdn[i], ep->port[i]);
  }
  meps->mnodeEpSet = *ep;

  pthread_mutex_unlock(&meps->mutex);
}

void dnodeGetEpSetForPeer(SDnMnEps *meps, SRpcEpSet *epSet) {
  pthread_mutex_lock(&meps->mutex);

  *epSet = meps->mnodeEpSet;
  for (int32_t i = 0; i < epSet->numOfEps; ++i) {
    epSet->port[i] += TSDB_PORT_DNODEDNODE;
  }

  pthread_mutex_unlock(&meps->mutex);
}

void dnodeGetEpSetForShell(SDnMnEps *meps, SRpcEpSet *epSet) {
  pthread_mutex_lock(&meps->mutex);

  *epSet = meps->mnodeEpSet;

  pthread_mutex_unlock(&meps->mutex);
}
