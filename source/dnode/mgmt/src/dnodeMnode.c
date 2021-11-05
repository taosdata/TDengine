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
#include "dnodeDnode.h"
#include "dnodeTransport.h"
#include "cJSON.h"
#include "mnode.h"

static struct {
  int8_t          deployed;
  int8_t          dropped;
  char            file[PATH_MAX + 20];
  pthread_mutex_t mutex;
} tsMnode = {0};

static int32_t dnodeReadMnode() {
  int32_t len = 0;
  int32_t maxLen = 300;
  char   *content = calloc(1, maxLen + 1);
  cJSON  *root = NULL;
  FILE   *fp = NULL;

  fp = fopen(tsMnode.file, "r");
  if (!fp) {
    dDebug("file %s not exist", tsMnode.file);
    goto PRASE_MNODE_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", tsMnode.file);
    goto PRASE_MNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", tsMnode.file);
    goto PRASE_MNODE_OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_String) {
    dError("failed to read %s since deployed not found", tsMnode.file);
    goto PRASE_MNODE_OVER;
  }
  tsMnode.deployed = atoi(deployed->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_String) {
    dError("failed to read %s since dropped not found", tsMnode.file);
    goto PRASE_MNODE_OVER;
  }
  tsMnode.dropped = atoi(dropped->valuestring);

  dInfo("succcessed to read file %s", tsMnode.file);

PRASE_MNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  return 0;
}

static int32_t dnodeWriteMnode() {
  FILE *fp = fopen(tsMnode.file, "w");
  if (!fp) {
    dError("failed to write %s since %s", tsMnode.file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 300;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"deployed\": \"%d\",\n", tsMnode.dropped);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": \"%d\",\n", tsMnode.dropped);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsyncFile(fileno(fp));
  fclose(fp);
  free(content);
  terrno = 0;

  dInfo("successed to write %s", tsMnode.file);
  return 0;
}

static int32_t dnodeStartMnode(SCreateMnodeMsg *pCfg) {
  int32_t code = 0;

  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    code = TSDB_CODE_DND_DNODE_ID_NOT_MATCHED;
    dError("failed to start mnode since %s", tstrerror(code));
    return code;
  }

  if (tsMnode.dropped) {
    code = TSDB_CODE_DND_MNODE_ALREADY_DROPPED;
    dError("failed to start mnode since %s", tstrerror(code));
    return code;
  }

  if (tsMnode.deployed) {
    dError("failed to start mnode since its already deployed");
    return 0;
  }

  tsMnode.deployed = 1;
  tsMnode.dropped = 0;

  code = dnodeWriteMnode();
  if (code != 0) {
    tsMnode.deployed = 0;
    dError("failed to start mnode since %s", tstrerror(code));
    return code;
  }

  code = mnodeDeploy();
  if (code != 0) {
    tsMnode.deployed = 0;
    dError("failed to start mnode since %s", tstrerror(code));
    return code;
  }

  code = mnodeStart();
  if (code != 0) {
    tsMnode.deployed = 0;
    dError("failed to start mnode since %s", tstrerror(code));
    return code;
  }

  tsMnode.deployed = 1;
  return 0;
}

static int32_t dnodeDropMnode(SDropMnodeMsg *pCfg) {
  int32_t code = 0;

  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    code = TSDB_CODE_DND_DNODE_ID_NOT_MATCHED;
    dError("failed to drop mnode since %s", tstrerror(code));
    return code;
  }

  if (tsMnode.dropped) {
    code = TSDB_CODE_DND_MNODE_ALREADY_DROPPED;
    dError("failed to drop mnode since %s", tstrerror(code));
    return code;
  }

  if (!tsMnode.deployed) {
    dError("failed to drop mnode since not deployed");
    return 0;
  }

  mnodeStop();

  tsMnode.deployed = 0;
  tsMnode.dropped = 1;

  code = dnodeWriteMnode();
  if (code != 0) {
    tsMnode.deployed = 1;
    tsMnode.dropped = 0;
    dError("failed to drop mnode since %s", tstrerror(code));
    return code;
  }

  mnodeUnDeploy();

  tsMnode.deployed = 0;
  return 0;
}

static void dnodeProcessCreateMnodeReq(SRpcMsg *pMsg) {
  SCreateMnodeMsg *pCfg = pMsg->pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);

  int32_t code = dnodeStartMnode(pCfg);
  SRpcMsg rspMsg = {.handle = pMsg->handle, .code = code};
  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

static void dnodeProcessDropMnodeReq(SRpcMsg *pMsg) {
  SDropMnodeMsg *pCfg = pMsg->pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);

  int32_t code = dnodeDropMnode(pCfg);
  SRpcMsg rspMsg = {.handle = pMsg->handle, .code = code};
  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

static bool dnodeNeedDeployMnode() {
  if (dnodeGetDnodeId() > 0) return false;
  if (dnodeGetClusterId() > 0) return false;
  if (strcmp(tsFirst, tsLocalEp) != 0) return false;
  return true;
}

int32_t dnodeInitMnode() {
  tsMnode.dropped = 0;
  tsMnode.deployed = 0;
  snprintf(tsMnode.file, sizeof(tsMnode.file), "%s/mnode.json", tsDnodeDir);

  SMnodePara para;
  para.fp.GetDnodeEp = dnodeGetDnodeEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;
  para.fp.SendRedirectMsg = dnodeSendRedirectMsg;
  para.dnodeId = dnodeGetDnodeId();
  para.clusterId = dnodeGetClusterId();

  int32_t code = mnodeInit(para);
  if (code != 0) {
    dError("failed to init mnode module since %s", tstrerror(code));
    return code;
  }

  code = dnodeReadMnode();
  if (code != 0) {
    dError("failed to read file:%s since %s", tsMnode.file, tstrerror(code));
    return code;
  }

  if (tsMnode.dropped) {
    dError("mnode already dropped, undeploy it");
    mnodeUnDeploy();
    return 0;
  }

  if (!tsMnode.deployed) {
    bool needDeploy = dnodeNeedDeployMnode();
    if (needDeploy) {
      code = mnodeDeploy();
    } else {
      return 0;
    }

    if (code != 0) {
      dError("failed to deploy mnode since %s", tstrerror(code));
      return code;
    }

    tsMnode.deployed = 1;
  }

  return mnodeStart();
}

void dnodeCleanupMnode() {
  if (tsMnode.deployed) {
    mnodeStop();
  }

  mnodeCleanup();
}

void dnodeProcessMnodeMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_CREATE_MNODE_IN:
      dnodeProcessCreateMnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_DROP_MNODE_IN:
      dnodeProcessDropMnodeReq(pMsg);
      break;
    default:
      mnodeProcessMsg(pMsg);
  }
}

int32_t dnodeGetUserAuthFromMnode(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return mnodeRetriveAuth(user, spi, encrypt, secret, ckey);
}