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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "dndSnode.h"
#include "dndMgmt.h"
#include "dndTransport.h"
#include "dndWorker.h"

static void dndProcessSnodeQueue(SDnode *pDnode, SRpcMsg *pMsg);

static SSnode *dndAcquireSnode(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;
  SSnode     *pSnode = NULL;
  int32_t     refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pMgmt->deployed && !pMgmt->dropped && pMgmt->pSnode != NULL) {
    refCount = atomic_add_fetch_32(&pMgmt->refCount, 1);
    pSnode = pMgmt->pSnode;
  } else {
    terrno = TSDB_CODE_DND_SNODE_NOT_DEPLOYED;
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pSnode != NULL) {
    dTrace("acquire snode, refCount:%d", refCount);
  }
  return pSnode;
}

static void dndReleaseSnode(SDnode *pDnode, SSnode *pSnode) {
  if (pSnode == NULL) return;

  SSnodeMgmt *pMgmt = &pDnode->smgmt;
  taosRLockLatch(&pMgmt->latch);
  int32_t refCount = atomic_sub_fetch_32(&pMgmt->refCount, 1);
  taosRUnLockLatch(&pMgmt->latch);
  dTrace("release snode, refCount:%d", refCount);
}

static int32_t dndReadSnodeFile(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;
  int32_t     code = TSDB_CODE_DND_SNODE_READ_FILE_ERROR;
  int32_t     len = 0;
  int32_t     maxLen = 1024;
  char       *content = calloc(1, maxLen + 1);
  cJSON      *root = NULL;

  char file[PATH_MAX + 20];
  snprintf(file, PATH_MAX + 20, "%s/snode.json", pDnode->dir.dnode);

  FILE *fp = fopen(file, "r");
  if (fp == NULL) {
    dDebug("file %s not exist", file);
    code = 0;
    goto PRASE_SNODE_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", file);
    goto PRASE_SNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", file);
    goto PRASE_SNODE_OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_Number) {
    dError("failed to read %s since deployed not found", file);
    goto PRASE_SNODE_OVER;
  }
  pMgmt->deployed = deployed->valueint;

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    dError("failed to read %s since dropped not found", file);
    goto PRASE_SNODE_OVER;
  }
  pMgmt->dropped = dropped->valueint;

  code = 0;
  dDebug("succcessed to read file %s, deployed:%d dropped:%d", file, pMgmt->deployed, pMgmt->dropped);

PRASE_SNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  terrno = code;
  return code;
}

static int32_t dndWriteSnodeFile(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;

  char file[PATH_MAX + 20];
  snprintf(file, PATH_MAX + 20, "%s/snode.json", pDnode->dir.dnode);

  FILE *fp = fopen(file, "w");
  if (fp == NULL) {
    terrno = TSDB_CODE_DND_SNODE_WRITE_FILE_ERROR;
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 1024;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"deployed\": %d,\n", pMgmt->deployed);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": %d\n", pMgmt->dropped);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsyncFile(fileno(fp));
  fclose(fp);
  free(content);

  char realfile[PATH_MAX + 20];
  snprintf(realfile, PATH_MAX + 20, "%s/snode.json", pDnode->dir.dnode);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TSDB_CODE_DND_SNODE_WRITE_FILE_ERROR;
    dError("failed to rename %s since %s", file, terrstr());
    return -1;
  }

  dInfo("successed to write %s, deployed:%d dropped:%d", realfile, pMgmt->deployed, pMgmt->dropped);
  return 0;
}

static int32_t dndStartSnodeWorker(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;
  if (dndInitWorker(pDnode, &pMgmt->writeWorker, DND_WORKER_SINGLE, "snode-write", 0, 1, dndProcessSnodeQueue) != 0) {
    dError("failed to start snode write worker since %s", terrstr());
    return -1;
  }

  return 0;
}

static void dndStopSnodeWorker(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;

  taosWLockLatch(&pMgmt->latch);
  pMgmt->deployed = 0;
  taosWUnLockLatch(&pMgmt->latch);

  while (pMgmt->refCount > 0) {
    taosMsleep(10);
  } 

  dndCleanupWorker(&pMgmt->writeWorker);
}

static void dndBuildSnodeOption(SDnode *pDnode, SSnodeOpt *pOption) {
  pOption->pDnode = pDnode;
  pOption->sendReqToDnodeFp = dndSendReqToDnode;
  pOption->sendReqToMnodeFp = dndSendReqToMnode;
  pOption->sendRedirectRspFp = dndSendRedirectRsp;
  pOption->dnodeId = dndGetDnodeId(pDnode);
  pOption->clusterId = dndGetClusterId(pDnode);
  pOption->sver = pDnode->env.sver;
}

static int32_t dndOpenSnode(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;
  SSnode     *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    dndReleaseSnode(pDnode, pSnode);
    terrno = TSDB_CODE_DND_SNODE_ALREADY_DEPLOYED;
    dError("failed to create snode since %s", terrstr());
    return -1;
  }

  SSnodeOpt option = {0};
  dndBuildSnodeOption(pDnode, &option);

  pSnode = sndOpen(pDnode->dir.snode, &option);
  if (pSnode == NULL) {
    dError("failed to open snode since %s", terrstr());
    return -1;
  }

  if (dndStartSnodeWorker(pDnode) != 0) {
    dError("failed to start snode worker since %s", terrstr());
    sndClose(pSnode);
    return -1;
  }

  pMgmt->deployed = 1;
  if (dndWriteSnodeFile(pDnode) != 0) {
    pMgmt->deployed = 0;
    dError("failed to write snode file since %s", terrstr());
    dndStopSnodeWorker(pDnode);
    sndClose(pSnode);
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  pMgmt->pSnode = pSnode;
  taosWUnLockLatch(&pMgmt->latch);

  dInfo("snode open successfully");
  return 0;
}

static int32_t dndDropSnode(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode == NULL) {
    dError("failed to drop snode since %s", terrstr());
    return -1;
  }

  taosRLockLatch(&pMgmt->latch);
  pMgmt->dropped = 1;
  taosRUnLockLatch(&pMgmt->latch);

  if (dndWriteSnodeFile(pDnode) != 0) {
    taosRLockLatch(&pMgmt->latch);
    pMgmt->dropped = 0;
    taosRUnLockLatch(&pMgmt->latch);

    dndReleaseSnode(pDnode, pSnode);
    dError("failed to drop snode since %s", terrstr());
    return -1;
  }

  dndReleaseSnode(pDnode, pSnode);
  dndStopSnodeWorker(pDnode);
  pMgmt->deployed = 0;
  dndWriteSnodeFile(pDnode);
  sndClose(pSnode);
  pMgmt->pSnode = NULL;
  sndDestroy(pDnode->dir.snode);

  return 0;
}

int32_t dndProcessCreateSnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
  SDCreateSnodeReq createReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.dnodeId != dndGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_SNODE_INVALID_OPTION;
    dError("failed to create snode since %s", terrstr());
    return -1;
  } else {
    return dndOpenSnode(pDnode);
  }
}

int32_t dndProcessDropSnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
  SDDropSnodeReq dropReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != dndGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_SNODE_INVALID_OPTION;
    dError("failed to drop snode since %s", terrstr());
    return -1;
  } else {
    return dndDropSnode(pDnode);
  }
}

static void dndProcessSnodeQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;
  SRpcMsg    *pRsp = NULL;
  int32_t     code = TSDB_CODE_DND_SNODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    code = sndProcessMsg(pSnode, pMsg, &pRsp);
  }
  dndReleaseSnode(pDnode, pSnode);

  if (pMsg->msgType & 1u) {
    if (pRsp != NULL) {
      pRsp->ahandle = pMsg->ahandle;
      rpcSendResponse(pRsp);
      free(pRsp);
    } else {
      if (code != 0) code = terrno;
      SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rpcRsp);
    }
  }

  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void dndWriteSnodeMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_DND_SNODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    code = dndWriteMsgToWorker(pWorker, pMsg, sizeof(SRpcMsg));
  }
  dndReleaseSnode(pDnode, pSnode);

  if (code != 0) {
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
  }
}

void dndProcessSnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteSnodeMsgToWorker(pDnode, &pDnode->smgmt.writeWorker, pMsg);
}

int32_t dndInitSnode(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;
  taosInitRWLatch(&pMgmt->latch);

  if (dndReadSnodeFile(pDnode) != 0) {
    return -1;
  }

  if (pMgmt->dropped) {
    dInfo("snode has been deployed and needs to be deleted");
    sndDestroy(pDnode->dir.snode);
    return 0;
  }

  if (!pMgmt->deployed) return 0;

  return dndOpenSnode(pDnode);
}

void dndCleanupSnode(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;
  if (pMgmt->pSnode) {
    dndStopSnodeWorker(pDnode);
    sndClose(pMgmt->pSnode);
    pMgmt->pSnode = NULL;
  }
}
