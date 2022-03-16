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

typedef struct {
  int32_t     vgId;
  int32_t     refCount;
  int32_t     snVersion;
  int8_t      dropped;
  char       *path;
  SSnode     *pImpl;
  STaosQueue *pSharedQ;
  STaosQueue *pUniqueQ;
} SSnodeObj;

static void dndProcessSnodeSharedQueue(SDnode *pDnode, SRpcMsg *pMsg);

static void dndProcessSnodeUniqueQueue(SDnode *pDnode, STaosQall *qall, int32_t numOfMsgs);

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

  // FILE *fp = fopen(file, "r");
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    dDebug("file %s not exist", file);
    code = 0;
    goto PRASE_SNODE_OVER;
  }

  len = (int32_t)taosReadFile(pFile, content, maxLen);
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
  if (pFile != NULL) taosCloseFile(&pFile);

  terrno = code;
  return code;
}

static int32_t dndWriteSnodeFile(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;

  char file[PATH_MAX + 20];
  snprintf(file, PATH_MAX + 20, "%s/snode.json", pDnode->dir.dnode);

  // FILE *fp = fopen(file, "w");
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
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

  taosWriteFile(pFile, content, len);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
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
  pMgmt->uniqueWorkers = taosArrayInit(0, sizeof(void *));
  for (int32_t i = 0; i < SND_UNIQUE_THREAD_NUM; i++) {
    SDnodeWorker *pUniqueWorker = malloc(sizeof(SDnodeWorker));
    if (pUniqueWorker == NULL) {
      return -1;
    }
    if (dndInitWorker(pDnode, pUniqueWorker, DND_WORKER_MULTI, "snode-unique", 1, 1, dndProcessSnodeSharedQueue) != 0) {
      dError("failed to start snode unique worker since %s", terrstr());
      return -1;
    }
    taosArrayPush(pMgmt->uniqueWorkers, &pUniqueWorker);
  }
  if (dndInitWorker(pDnode, &pMgmt->sharedWorker, DND_WORKER_SINGLE, "snode-shared", SND_SHARED_THREAD_NUM,
                    SND_SHARED_THREAD_NUM, dndProcessSnodeSharedQueue)) {
    dError("failed to start snode shared worker since %s", terrstr());
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

  for (int32_t i = 0; i < taosArrayGetSize(pMgmt->uniqueWorkers); i++) {
    SDnodeWorker *worker = taosArrayGetP(pMgmt->uniqueWorkers, i);
    dndCleanupWorker(worker);
  }
  taosArrayDestroy(pMgmt->uniqueWorkers);
}

static void dndBuildSnodeOption(SDnode *pDnode, SSnodeOpt *pOption) {
  pOption->pDnode = pDnode;
  pOption->sendReqToDnodeFp = dndSendReqToDnode;
  pOption->sendReqToMnodeFp = dndSendReqToMnode;
  pOption->sendRedirectRspFp = dndSendRedirectRsp;
  pOption->dnodeId = dndGetDnodeId(pDnode);
  pOption->clusterId = dndGetClusterId(pDnode);
  pOption->sver = tsVersion;
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

static void dndProcessSnodeUniqueQueue(SDnode *pDnode, STaosQall *qall, int32_t numOfMsgs) {
  /*SSnodeMgmt *pMgmt = &pDnode->smgmt;*/
  int32_t code = TSDB_CODE_DND_SNODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    for (int32_t i = 0; i < numOfMsgs; i++) {
      SRpcMsg *pMsg = NULL;
      taosGetQitem(qall, (void **)&pMsg);

      sndProcessUMsg(pSnode, pMsg);

      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
    }
    dndReleaseSnode(pDnode, pSnode);
  } else {
    for (int32_t i = 0; i < numOfMsgs; i++) {
      SRpcMsg *pMsg = NULL;
      taosGetQitem(qall, (void **)&pMsg);
      SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rpcRsp);

      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
    }
  }
}

static void dndProcessSnodeSharedQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  /*SSnodeMgmt *pMgmt = &pDnode->smgmt;*/
  int32_t code = TSDB_CODE_DND_SNODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    sndProcessSMsg(pSnode, pMsg);
    dndReleaseSnode(pDnode, pSnode);
  } else {
    SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
    rpcSendResponse(&rpcRsp);
  }

#if 0
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
#endif

  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static FORCE_INLINE int32_t dndGetSWIdFromMsg(SRpcMsg *pMsg) {
  SMsgHead *pHead = pMsg->pCont;
  pHead->streamTaskId = htonl(pHead->streamTaskId);
  return pHead->streamTaskId % SND_UNIQUE_THREAD_NUM;
}

static void dndWriteSnodeMsgToWorkerByMsg(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_DND_SNODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    int32_t       index = dndGetSWIdFromMsg(pMsg);
    SDnodeWorker *pWorker = taosArrayGetP(pDnode->smgmt.uniqueWorkers, index);
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

static void dndWriteSnodeMsgToMgmtWorker(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_DND_SNODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    SDnodeWorker *pWorker = taosArrayGet(pDnode->smgmt.uniqueWorkers, 0);
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

void dndProcessSnodeMgmtMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteSnodeMsgToMgmtWorker(pDnode, pMsg);
}

void dndProcessSnodeUniqueMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteSnodeMsgToWorkerByMsg(pDnode, pMsg);
}

void dndProcessSnodeSharedMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteSnodeMsgToWorker(pDnode, &pDnode->smgmt.sharedWorker, pMsg);
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
