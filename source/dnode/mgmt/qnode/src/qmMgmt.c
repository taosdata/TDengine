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
// #include "dndQnode.h"
// #include "dm.h"
// #include "dndTransport.h"
// #include "dndWorker.h"

#if 0
static void dndProcessQnodeQueue(SDnode *pDnode, SRpcMsg *pMsg);

static SQnode *dndAcquireQnode(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  SQnode     *pQnode = NULL;
  int32_t     refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pMgmt->deployed && !pMgmt->dropped && pMgmt->pQnode != NULL) {
    refCount = atomic_add_fetch_32(&pMgmt->refCount, 1);
    pQnode = pMgmt->pQnode;
  } else {
    terrno = TSDB_CODE_DND_QNODE_NOT_DEPLOYED;
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pQnode != NULL) {
    dTrace("acquire qnode, refCount:%d", refCount);
  }
  return pQnode;
}

static void dndReleaseQnode(SDnode *pDnode, SQnode *pQnode) {
  if (pQnode == NULL) return;

  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  taosRLockLatch(&pMgmt->latch);
  int32_t refCount = atomic_sub_fetch_32(&pMgmt->refCount, 1);
  taosRUnLockLatch(&pMgmt->latch);
  dTrace("release qnode, refCount:%d", refCount);
}

static int32_t dndReadQnodeFile(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  int32_t     code = TSDB_CODE_DND_QNODE_READ_FILE_ERROR;
  int32_t     len = 0;
  int32_t     maxLen = 1024;
  char       *content = calloc(1, maxLen + 1);
  cJSON      *root = NULL;

  char file[PATH_MAX + 20];
  snprintf(file, PATH_MAX + 20, "%s/qnode.json", pDnode->dir.dnode);

  // FILE *fp = fopen(file, "r");
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    dDebug("file %s not exist", file);
    code = 0;
    goto PRASE_QNODE_OVER;
  }

  len = (int32_t)taosReadFile(pFile, content, maxLen);
  if (len <= 0) {
    dError("failed to read %s since content is null", file);
    goto PRASE_QNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", file);
    goto PRASE_QNODE_OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_Number) {
    dError("failed to read %s since deployed not found", file);
    goto PRASE_QNODE_OVER;
  }
  pMgmt->deployed = deployed->valueint;

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    dError("failed to read %s since dropped not found", file);
    goto PRASE_QNODE_OVER;
  }
  pMgmt->dropped = dropped->valueint;

  code = 0;
  dDebug("succcessed to read file %s, deployed:%d dropped:%d", file, pMgmt->deployed, pMgmt->dropped);

PRASE_QNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  terrno = code;
  return code;
}

static int32_t dndWriteQnodeFile(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;

  char file[PATH_MAX + 20];
  snprintf(file, PATH_MAX + 20, "%s/qnode.json", pDnode->dir.dnode);

  // FILE *fp = fopen(file, "w");
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TSDB_CODE_DND_QNODE_WRITE_FILE_ERROR;
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

  taosWriteFile(pFile, content, len);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  free(content);

  char realfile[PATH_MAX + 20];
  snprintf(realfile, PATH_MAX + 20, "%s/qnode.json", pDnode->dir.dnode);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TSDB_CODE_DND_QNODE_WRITE_FILE_ERROR;
    dError("failed to rename %s since %s", file, terrstr());
    return -1;
  }

  dInfo("successed to write %s, deployed:%d dropped:%d", realfile, pMgmt->deployed, pMgmt->dropped);
  return 0;
}

static int32_t dndStartQnodeWorker(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  if (dndInitWorker(pDnode, &pMgmt->queryWorker, DND_WORKER_SINGLE, "qnode-query", 0, 1, dndProcessQnodeQueue) != 0) {
    dError("failed to start qnode query worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pDnode, &pMgmt->fetchWorker, DND_WORKER_SINGLE, "qnode-fetch", 0, 1, dndProcessQnodeQueue) != 0) {
    dError("failed to start qnode fetch worker since %s", terrstr());
    return -1;
  }

  return 0;
}

static void dndStopQnodeWorker(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;

  taosWLockLatch(&pMgmt->latch);
  pMgmt->deployed = 0;
  taosWUnLockLatch(&pMgmt->latch);

  while (pMgmt->refCount > 0) {
    taosMsleep(10);
  }

  dndCleanupWorker(&pMgmt->queryWorker);
  dndCleanupWorker(&pMgmt->fetchWorker);
}

static void dndBuildQnodeOption(SDnode *pDnode, SQnodeOpt *pOption) {
  pOption->pDnode = pDnode;
  pOption->sendReqFp = dndSendReqToDnode;
  pOption->sendMnodeReqFp = dndSendReqToMnode;
  pOption->sendRedirectRspFp = dndSendRedirectRsp;
  pOption->dnodeId = pDnode->dnodeId;
  pOption->clusterId = pDnode->clusterId;
  pOption->sver = tsVersion;
}

static int32_t dndOpenQnode(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;

  SQnode *pQnode = dndAcquireQnode(pDnode);
  if (pQnode != NULL) {
    dndReleaseQnode(pDnode, pQnode);
    terrno = TSDB_CODE_DND_QNODE_ALREADY_DEPLOYED;
    dError("failed to create qnode since %s", terrstr());
    return -1;
  }

  SQnodeOpt option = {0};
  dndBuildQnodeOption(pDnode, &option);

  pQnode = qndOpen(&option);
  if (pQnode == NULL) {
    dError("failed to open qnode since %s", terrstr());
    return -1;
  }

  if (dndStartQnodeWorker(pDnode) != 0) {
    dError("failed to start qnode worker since %s", terrstr());
    qndClose(pQnode);
    return -1;
  }

  pMgmt->deployed = 1;
  if (dndWriteQnodeFile(pDnode) != 0) {
    pMgmt->deployed = 0;
    dError("failed to write qnode file since %s", terrstr());
    dndStopQnodeWorker(pDnode);
    qndClose(pQnode);
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  pMgmt->pQnode = pQnode;
  taosWUnLockLatch(&pMgmt->latch);

  dInfo("qnode open successfully");
  return 0;
}

static int32_t dndDropQnode(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;

  SQnode *pQnode = dndAcquireQnode(pDnode);
  if (pQnode == NULL) {
    dError("failed to drop qnode since %s", terrstr());
    return -1;
  }

  taosRLockLatch(&pMgmt->latch);
  pMgmt->dropped = 1;
  taosRUnLockLatch(&pMgmt->latch);

  if (dndWriteQnodeFile(pDnode) != 0) {
    taosRLockLatch(&pMgmt->latch);
    pMgmt->dropped = 0;
    taosRUnLockLatch(&pMgmt->latch);

    dndReleaseQnode(pDnode, pQnode);
    dError("failed to drop qnode since %s", terrstr());
    return -1;
  }

  dndReleaseQnode(pDnode, pQnode);
  dndStopQnodeWorker(pDnode);
  pMgmt->deployed = 0;
  dndWriteQnodeFile(pDnode);
  qndClose(pQnode);
  pMgmt->pQnode = NULL;

  return 0;
}

int32_t qmProcessCreateReq(SDnode *pDnode, SRpcMsg *pReq) {
  SDCreateQnodeReq createReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_DND_QNODE_INVALID_OPTION;
    dError("failed to create qnode since %s", terrstr());
    return -1;
  } else {
    return dndOpenQnode(pDnode);
  }
}

int32_t qmProcessDropReq(SDnode *pDnode, SRpcMsg *pReq) {
  SDDropQnodeReq dropReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_DND_QNODE_INVALID_OPTION;
    dError("failed to drop qnode since %s", terrstr());
    return -1;
  } else {
    return dndDropQnode(pDnode);
  }
}

static void dndProcessQnodeQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  SRpcMsg    *pRsp = NULL;
  int32_t     code = TSDB_CODE_DND_QNODE_NOT_DEPLOYED;

  SQnode *pQnode = dndAcquireQnode(pDnode);
  if (pQnode != NULL) {
    code = qndProcessMsg(pQnode, pMsg, &pRsp);
  }
  dndReleaseQnode(pDnode, pQnode);

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

static void dndWriteQnodeMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_DND_QNODE_NOT_DEPLOYED;

  SQnode *pQnode = dndAcquireQnode(pDnode);
  if (pQnode != NULL) {
    code = dndWriteMsgToWorker(pWorker, pMsg, sizeof(SRpcMsg));
  }
  dndReleaseQnode(pDnode, pQnode);

  if (code != 0) {
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
  }
}

void dndProcessQnodeQueryMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteQnodeMsgToWorker(pDnode, &pDnode->qmgmt.queryWorker, pMsg);
}

void dndProcessQnodeFetchMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteQnodeMsgToWorker(pDnode, &pDnode->qmgmt.queryWorker, pMsg);
}

int32_t dndInitQnode(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  taosInitRWLatch(&pMgmt->latch);

  if (dndReadQnodeFile(pDnode) != 0) {
    return -1;
  }

  if (pMgmt->dropped) return 0;
  if (!pMgmt->deployed) return 0;

  return dndOpenQnode(pDnode);
}

void dndCleanupQnode(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  if (pMgmt->pQnode) {
    dndStopQnodeWorker(pDnode);
    qndClose(pMgmt->pQnode);
    pMgmt->pQnode = NULL;
  }
}

#endif