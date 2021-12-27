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
#include "dndQnode.h"
#include "dndDnode.h"
#include "dndTransport.h"

static int32_t dndInitQnodeQueryWorker(SDnode *pDnode);
static int32_t dndInitQnodeFetchWorker(SDnode *pDnode);
static void    dndCleanupQnodeQueryWorker(SDnode *pDnode);
static void    dndCleanupQnodeFetchWorker(SDnode *pDnode);
static int32_t dndAllocQnodeQueryQueue(SDnode *pDnode);
static int32_t dndAllocQnodeFetchQueue(SDnode *pDnode);
static void    dndFreeQnodeQueryQueue(SDnode *pDnode);
static void    dndFreeQnodeFetchQueue(SDnode *pDnode);

static void    dndProcessQnodeQueue(SDnode *pDnode, SRpcMsg *pMsg);
static int32_t dndWriteQnodeMsgToQueue(SQnode *pQnode, taos_queue pQueue, SRpcMsg *pRpcMsg);

static int32_t dndStartQnodeWorker(SDnode *pDnode);
static void    dndStopQnodeWorker(SDnode *pDnode);

static SQnode *dndAcquireQnode(SDnode *pDnode);
static void    dndReleaseQnode(SDnode *pDnode, SQnode *pQnode);

static int32_t dndReadQnodeFile(SDnode *pDnode);
static int32_t dndWriteQnodeFile(SDnode *pDnode);

static int32_t dndOpenQnode(SDnode *pDnode);
static int32_t dndDropQnode(SDnode *pDnode);

static SQnode *dndAcquireQnode(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  SQnode     *pQnode = NULL;
  int32_t     refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pMgmt->deployed && !pMgmt->dropped) {
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
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  int32_t     refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pQnode != NULL) {
    refCount = atomic_sub_fetch_32(&pMgmt->refCount, 1);
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pQnode != NULL) {
    dTrace("release qnode, refCount:%d", refCount);
  }
}

static int32_t dndReadQnodeFile(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  int32_t     code = TSDB_CODE_DND_QNODE_READ_FILE_ERROR;
  int32_t     len = 0;
  int32_t     maxLen = 4096;
  char       *content = calloc(1, maxLen + 1);
  cJSON      *root = NULL;

  FILE *fp = fopen(pMgmt->file, "r");
  if (fp == NULL) {
    dDebug("file %s not exist", pMgmt->file);
    code = 0;
    goto PRASE_MNODE_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", pMgmt->file);
    goto PRASE_MNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", pMgmt->file);
    goto PRASE_MNODE_OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_Number) {
    dError("failed to read %s since deployed not found", pMgmt->file);
    goto PRASE_MNODE_OVER;
  }
  pMgmt->deployed = deployed->valueint;

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    dError("failed to read %s since dropped not found", pMgmt->file);
    goto PRASE_MNODE_OVER;
  }
  pMgmt->dropped = dropped->valueint;

  code = 0;
  dDebug("succcessed to read file %s, deployed:%d dropped:%d", pMgmt->file, pMgmt->deployed, pMgmt->dropped);

PRASE_MNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  terrno = code;
  return code;
}

static int32_t dndWriteQnodeFile(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;

  char file[PATH_MAX + 20] = {0};
  snprintf(file, sizeof(file), "%s.bak", pMgmt->file);

  FILE *fp = fopen(file, "w");
  if (fp == NULL) {
    terrno = TSDB_CODE_DND_QNODE_WRITE_FILE_ERROR;
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 4096;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"deployed\": %d,\n", pMgmt->deployed);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": %d\n", pMgmt->dropped);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFfetchFile(fileno(fp));
  fclose(fp);
  free(content);

  if (taosRenameFile(file, pMgmt->file) != 0) {
    terrno = TSDB_CODE_DND_QNODE_WRITE_FILE_ERROR;
    dError("failed to rename %s since %s", pMgmt->file, terrstr());
    return -1;
  }

  dInfo("successed to write %s, deployed:%d dropped:%d", pMgmt->file, pMgmt->deployed, pMgmt->dropped);
  return 0;
}

static int32_t dndStartQnodeWorker(SDnode *pDnode) {
  if (dndInitQnodeQueryWorker(pDnode) != 0) {
    dError("failed to start qnode query worker since %s", terrstr());
    return -1;
  }

  if (dndInitQnodeFetchWorker(pDnode) != 0) {
    dError("failed to start qnode fetch worker since %s", terrstr());
    return -1;
  }

  if (dndAllocQnodeQueryQueue(pDnode) != 0) {
    dError("failed to alloc qnode query queue since %s", terrstr());
    return -1;
  }

  if (dndAllocQnodeFetchQueue(pDnode) != 0) {
    dError("failed to alloc qnode fetch queue since %s", terrstr());
    return -1;
  }

  return 0;
}

static void dndStopQnodeWorker(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;

  taosWLockLatch(&pMgmt->latch);
  pMgmt->deployed = 0;
  taosWUnLockLatch(&pMgmt->latch);

  while (pMgmt->refCount > 1) taosMsleep(10);
  while (!taosQueueEmpty(pMgmt->pQueryQ)) taosMsleep(10);
  while (!taosQueueEmpty(pMgmt->pFetchQ)) taosMsleep(10);

  dndCleanupQnodeQueryWorker(pDnode);
  dndCleanupQnodeFetchWorker(pDnode);

  dndFreeQnodeQueryQueue(pDnode);
  dndFreeQnodeFetchQueue(pDnode);
}

static void dndBuildQnodeOption(SDnode *pDnode, SQnodeOpt *pOption) {
  pOption->pDnode = pDnode;
  pOption->sendMsgToDnodeFp = dndSendMsgToDnode;
  pOption->sendMsgToMnodeFp = dndSendMsgToMnode;
  pOption->sendRedirectMsgFp = dndSendRedirectMsg;
  pOption->dnodeId = dndGetDnodeId(pDnode);
  pOption->clusterId = dndGetClusterId(pDnode);
  pOption->cfg.sver = pDnode->opt.sver;
}

static int32_t dndOpenQnode(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  SQnodeOpt   option = {0};
  dndBuildQnodeOption(pDnode, &option);

  SQnode *pQnode = qndOpen(&option);
  if (pQnode == NULL) {
    dError("failed to open qnode since %s", terrstr());
    return -1;
  }
  pMgmt->deployed = 1;

  int32_t code = dndWriteQnodeFile(pDnode);
  if (code != 0) {
    dError("failed to write qnode file since %s", terrstr());
    code = terrno;
    pMgmt->deployed = 0;
    qndClose(pQnode);
    // qndDestroy(pDnode->dir.qnode);
    terrno = code;
    return -1;
  }

  code = dndStartQnodeWorker(pDnode);
  if (code != 0) {
    dError("failed to start qnode worker since %s", terrstr());
    code = terrno;
    pMgmt->deployed = 0;
    dndStopQnodeWorker(pDnode);
    qndClose(pQnode);
    // qndDestroy(pDnode->dir.qnode);
    terrno = code;
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  pMgmt->pQnode = pQnode;
  pMgmt->deployed = 1;
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
  dndWriteQnodeFile(pDnode);
  qndClose(pQnode);
  pMgmt->pQnode = NULL;
  // qndDestroy(pDnode->dir.qnode);

  return 0;
}

int32_t dndProcessCreateQnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg) {
  SCreateQnodeInMsg *pMsg = pRpcMsg->pCont;
  pMsg->dnodeId = htonl(pMsg->dnodeId);

  if (pMsg->dnodeId != dndGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_QNODE_ID_INVALID;
    return -1;
  } else {
    return dndOpenQnode(pDnode);
  }
}

int32_t dndProcessDropQnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg) {
  SDropQnodeInMsg *pMsg = pRpcMsg->pCont;
  pMsg->dnodeId = htonl(pMsg->dnodeId);

  if (pMsg->dnodeId != dndGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_QNODE_ID_INVALID;
    return -1;
  } else {
    return dndDropQnode(pDnode);
  }
}

static void dndProcessQnodeQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  SRpcMsg    *pRsp = NULL;
  int32_t     code = 0;

  SQnode *pQnode = dndAcquireQnode(pDnode);
  if (pQnode == NULL) {
    code = -1;
  } else {
    code = qndProcessQueryReq(pQnode, pMsg, &pRsp);
  }

  if (pRsp != NULL) {
    pRsp->ahandle = pMsg->ahandle;
    rpcSendResponse(pRsp);
    free(pRsp);
  } else {
    if (code != 0) code = terrno;
    SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
    rpcSendResponse(&rpcRsp);
  }

  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static int32_t dndWriteQnodeMsgToQueue(SQnode *pQnode, taos_queue pQueue, SRpcMsg *pRpcMsg) {
  int32_t code = 0;

  if (pQnode == NULL || pQueue == NULL) {
    code = TSDB_CODE_DND_QNODE_NOT_DEPLOYED;
  } else {
    SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg));
    if (pMsg == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      *pMsg = *pRpcMsg;
      if (taosWriteQitem(pQueue, pMsg) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }

  if (code != 0) {
    if (pRpcMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pRpcMsg->handle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pRpcMsg->pCont);
  }
}

void dndProcessQnodeQueryMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  SQnode     *pQnode = dndAcquireQnode(pDnode);
  dndWriteQnodeMsgToQueue(pQnode, pMgmt->pQueryQ, pMsg);
  dndReleaseQnode(pDnode, pQnode);
}

void dndProcessQnodeFetchMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  SQnode     *pQnode = dndAcquireQnode(pDnode);
  dndWriteQnodeMsgToQueue(pQnode, pMgmt->pFetchQ, pMsg);
  dndReleaseQnode(pDnode, pQnode);
}

static int32_t dndAllocQnodeQueryQueue(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  pMgmt->pQueryQ = tWorkerAllocQueue(&pMgmt->queryPool, pDnode, (FProcessItem)dndProcessQnodeQueue);
  if (pMgmt->pQueryQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeQnodeQueryQueue(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  tWorkerFreeQueue(&pMgmt->queryPool, pMgmt->pQueryQ);
  pMgmt->pQueryQ = NULL;
}

static int32_t dndInitQnodeQueryWorker(SDnode *pDnode) {
  SQnodeMgmt  *pMgmt = &pDnode->qmgmt;
  SWorkerPool *pPool = &pMgmt->queryPool;
  pPool->name = "qnode-query";
  pPool->min = 0;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dDebug("qnode query worker is initialized");
  return 0;
}

static void dndCleanupQnodeQueryWorker(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  tWorkerCleanup(&pMgmt->queryPool);
  dDebug("qnode query worker is closed");
}

static int32_t dndAllocQnodeFetchQueue(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  pMgmt->pFetchQ = tWorkerAllocQueue(&pMgmt->queryPool, pDnode, (FProcessItem)dndProcessQnodeQueue);
  if (pMgmt->pFetchQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeQnodeFetchQueue(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  tWorkerFreeQueue(&pMgmt->fetchPool, pMgmt->pFetchQ);
  pMgmt->pFetchQ = NULL;
}

static int32_t dndInitQnodeFetchWorker(SDnode *pDnode) {
  SQnodeMgmt  *pMgmt = &pDnode->qmgmt;
  SWorkerPool *pPool = &pMgmt->fetchPool;
  pPool->name = "qnode-fetch";
  pPool->min = 0;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dDebug("qnode fetch worker is initialized");
  return 0;
}

static void dndCleanupQnodeFetchWorker(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  tWorkerCleanup(&pMgmt->fetchPool);
  dDebug("qnode fetch worker is closed");
}

int32_t dndInitQnode(SDnode *pDnode) {
  dInfo("dnode-qnode start to init");
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;
  taosInitRWLatch(&pMgmt->latch);

  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s/qnode.json", pDnode->dir.dnode);
  pMgmt->file = strdup(path);
  if (pMgmt->file == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dndReadQnodeFile(pDnode) != 0) {
    return -1;
  }

  if (pMgmt->dropped) return 0;
  if (!pMgmt->deployed) return 0;

  return dndOpenQnode(pDnode);
}

void dndCleanupQnode(SDnode *pDnode) {
  SQnodeMgmt *pMgmt = &pDnode->qmgmt;

  dInfo("dnode-qnode start to clean up");
  if (pMgmt->pQnode) dndStopQnodeWorker(pDnode);
  tfree(pMgmt->file);
  qndClose(pMgmt->pQnode);
  pMgmt->pQnode = NULL;
  dInfo("dnode-qnode is cleaned up");
}
