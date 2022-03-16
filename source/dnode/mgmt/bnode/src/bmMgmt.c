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
// #include "dndBnode.h"
// #include "dndTransport.h"
// #include "dndWorker.h"

#if 0
static void dndProcessBnodeQueue(SDnode *pDnode, STaosQall *qall, int32_t numOfMsgs);

static SBnode *dndAcquireBnode(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  SBnode     *pBnode = NULL;
  int32_t     refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pMgmt->deployed && !pMgmt->dropped && pMgmt->pBnode != NULL) {
    refCount = atomic_add_fetch_32(&pMgmt->refCount, 1);
    pBnode = pMgmt->pBnode;
  } else {
    terrno = TSDB_CODE_DND_BNODE_NOT_DEPLOYED;
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pBnode != NULL) {
    dTrace("acquire bnode, refCount:%d", refCount);
  }
  return pBnode;
}

static void dndReleaseBnode(SDnode *pDnode, SBnode *pBnode) {
  if (pBnode == NULL) return;

  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  taosRLockLatch(&pMgmt->latch);
  int32_t refCount = atomic_sub_fetch_32(&pMgmt->refCount, 1);
  taosRUnLockLatch(&pMgmt->latch);
  dTrace("release bnode, refCount:%d", refCount);
}

static int32_t dndReadBnodeFile(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  int32_t     code = TSDB_CODE_DND_BNODE_READ_FILE_ERROR;
  int32_t     len = 0;
  int32_t     maxLen = 1024;
  char       *content = calloc(1, maxLen + 1);
  cJSON      *root = NULL;

  char file[PATH_MAX + 20];
  snprintf(file, PATH_MAX + 20, "%s/bnode.json", pDnode->dir.dnode);

  // FILE *fp = fopen(file, "r");
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    dDebug("file %s not exist", file);
    code = 0;
    goto PRASE_BNODE_OVER;
  }

  len = (int32_t)taosReadFile(pFile, content, maxLen);
  if (len <= 0) {
    dError("failed to read %s since content is null", file);
    goto PRASE_BNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", file);
    goto PRASE_BNODE_OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_Number) {
    dError("failed to read %s since deployed not found", file);
    goto PRASE_BNODE_OVER;
  }
  pMgmt->deployed = deployed->valueint;

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    dError("failed to read %s since dropped not found", file);
    goto PRASE_BNODE_OVER;
  }
  pMgmt->dropped = dropped->valueint;

  code = 0;
  dDebug("succcessed to read file %s, deployed:%d dropped:%d", file, pMgmt->deployed, pMgmt->dropped);

PRASE_BNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  terrno = code;
  return code;
}

static int32_t dndWriteBnodeFile(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;

  char file[PATH_MAX + 20];
  snprintf(file, PATH_MAX + 20, "%s/bnode.json", pDnode->dir.dnode);

  // FILE *fp = fopen(file, "w");
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TSDB_CODE_DND_BNODE_WRITE_FILE_ERROR;
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
  snprintf(realfile, PATH_MAX + 20, "%s/bnode.json", pDnode->dir.dnode);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TSDB_CODE_DND_BNODE_WRITE_FILE_ERROR;
    dError("failed to rename %s since %s", file, terrstr());
    return -1;
  }

  dInfo("successed to write %s, deployed:%d dropped:%d", realfile, pMgmt->deployed, pMgmt->dropped);
  return 0;
}

static int32_t dndStartBnodeWorker(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  if (dndInitWorker(pDnode, &pMgmt->writeWorker, DND_WORKER_MULTI, "bnode-write", 0, 1, dndProcessBnodeQueue) != 0) {
    dError("failed to start bnode write worker since %s", terrstr());
    return -1;
  }

  return 0;
}

static void dndStopBnodeWorker(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;

  taosWLockLatch(&pMgmt->latch);
  pMgmt->deployed = 0;
  taosWUnLockLatch(&pMgmt->latch);

  while (pMgmt->refCount > 0) {
    taosMsleep(10);
  }

  dndCleanupWorker(&pMgmt->writeWorker);
}

static void dndBuildBnodeOption(SDnode *pDnode, SBnodeOpt *pOption) {
  pOption->pDnode = pDnode;
  pOption->sendReqFp = dndSendReqToDnode;
  pOption->sendReqToMnodeFp = dndSendReqToMnode;
  pOption->sendRedirectRspFp = dmSendRedirectRsp;
  pOption->dnodeId = pDnode->dnodeId;
  pOption->clusterId = pDnode->clusterId;
  pOption->sver = tsVersion;
}

static int32_t dndOpenBnode(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  SBnode     *pBnode = dndAcquireBnode(pDnode);
  if (pBnode != NULL) {
    dndReleaseBnode(pDnode, pBnode);
    terrno = TSDB_CODE_DND_BNODE_ALREADY_DEPLOYED;
    dError("failed to create bnode since %s", terrstr());
    return -1;
  }

  SBnodeOpt option = {0};
  dndBuildBnodeOption(pDnode, &option);

  pBnode = bndOpen(pDnode->dir.bnode, &option);
  if (pBnode == NULL) {
    dError("failed to open bnode since %s", terrstr());
    return -1;
  }

  if (dndStartBnodeWorker(pDnode) != 0) {
    dError("failed to start bnode worker since %s", terrstr());
    bndClose(pBnode);
    return -1;
  }

  pMgmt->deployed = 1;
  if (dndWriteBnodeFile(pDnode) != 0) {
    pMgmt->deployed = 0;
    dError("failed to write bnode file since %s", terrstr());
    dndStopBnodeWorker(pDnode);
    bndClose(pBnode);
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  pMgmt->pBnode = pBnode;
  taosWUnLockLatch(&pMgmt->latch);

  dInfo("bnode open successfully");
  return 0;
}

static int32_t dndDropBnode(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;

  SBnode *pBnode = dndAcquireBnode(pDnode);
  if (pBnode == NULL) {
    dError("failed to drop bnode since %s", terrstr());
    return -1;
  }

  taosRLockLatch(&pMgmt->latch);
  pMgmt->dropped = 1;
  taosRUnLockLatch(&pMgmt->latch);

  if (dndWriteBnodeFile(pDnode) != 0) {
    taosRLockLatch(&pMgmt->latch);
    pMgmt->dropped = 0;
    taosRUnLockLatch(&pMgmt->latch);

    dndReleaseBnode(pDnode, pBnode);
    dError("failed to drop bnode since %s", terrstr());
    return -1;
  }

  dndReleaseBnode(pDnode, pBnode);
  dndStopBnodeWorker(pDnode);
  pMgmt->deployed = 0;
  dndWriteBnodeFile(pDnode);
  bndClose(pBnode);
  pMgmt->pBnode = NULL;
  bndDestroy(pDnode->dir.bnode);

  return 0;
}

int32_t bmProcessCreateReq(SDnode *pDnode, SRpcMsg *pReq) {
  SDCreateBnodeReq createReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_DND_BNODE_INVALID_OPTION;
    dError("failed to create bnode since %s", terrstr());
    return -1;
  } else {
    return dndOpenBnode(pDnode);
  }
}

int32_t bmProcessDropReq(SDnode *pDnode, SRpcMsg *pReq) {
  SDDropBnodeReq dropReq = {0};
  if (tDeserializeSMCreateDropQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_DND_BNODE_INVALID_OPTION;
    dError("failed to drop bnode since %s", terrstr());
    return -1;
  } else {
    return dndDropBnode(pDnode);
  }
}

static void dndSendBnodeErrorRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void dndSendBnodeErrorRsps(STaosQall *qall, int32_t numOfMsgs, int32_t code) {
  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    dndSendBnodeErrorRsp(pMsg, code);
  }
}

static void dndProcessBnodeQueue(SDnode *pDnode, STaosQall *qall, int32_t numOfMsgs) {
  SBnode *pBnode = dndAcquireBnode(pDnode);
  if (pBnode == NULL) {
    dndSendBnodeErrorRsps(qall, numOfMsgs, TSDB_CODE_OUT_OF_MEMORY);
    return;
  }

  SArray *pArray = taosArrayInit(numOfMsgs, sizeof(SRpcMsg *));
  if (pArray == NULL) {
    dndReleaseBnode(pDnode, pBnode);
    dndSendBnodeErrorRsps(qall, numOfMsgs, TSDB_CODE_OUT_OF_MEMORY);
    return;
  }

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    void *ptr = taosArrayPush(pArray, &pMsg);
    if (ptr == NULL) {
      dndSendBnodeErrorRsp(pMsg, TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  bndProcessWMsgs(pBnode, pArray);

  for (size_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg *pMsg = *(SRpcMsg **)taosArrayGet(pArray, i);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
  taosArrayDestroy(pArray);
  dndReleaseBnode(pDnode, pBnode);
}

static void dndWriteBnodeMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_DND_BNODE_NOT_DEPLOYED;

  SBnode *pBnode = dndAcquireBnode(pDnode);
  if (pBnode != NULL) {
    code = dndWriteMsgToWorker(pWorker, pMsg, sizeof(SRpcMsg));
  }
  dndReleaseBnode(pDnode, pBnode);

  if (code != 0) {
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
  }
}

void dndProcessBnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteBnodeMsgToWorker(pDnode, &pDnode->bmgmt.writeWorker, pMsg);
}

int32_t dndInitBnode(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  taosInitRWLatch(&pMgmt->latch);

  if (dndReadBnodeFile(pDnode) != 0) {
    return -1;
  }

  if (pMgmt->dropped) {
    dInfo("bnode has been deployed and needs to be deleted");
    bndDestroy(pDnode->dir.bnode);
    return 0;
  }

  if (!pMgmt->deployed) return 0;

  return dndOpenBnode(pDnode);
}

void dndCleanupBnode(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  if (pMgmt->pBnode) {
    dndStopBnodeWorker(pDnode);
    bndClose(pMgmt->pBnode);
    pMgmt->pBnode = NULL;
  }
}

#endif