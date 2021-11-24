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
#include "dndVnodes.h"
#include "dndTransport.h"

typedef struct {
  int32_t    vgId;
  int32_t    refCount;
  int8_t     dropped;
  int8_t     accessState;
  SVnode    *pImpl;
  taos_queue pWriteQ;
  taos_queue pSyncQ;
  taos_queue pApplyQ;
  taos_queue pQueryQ;
  taos_queue pFetchQ;
} SVnodeObj;

typedef struct {
  int32_t    vnodeNum;
  int32_t    opened;
  int32_t    failed;
  int32_t    threadIndex;
  pthread_t *pThreadId;
  SVnodeObj *pVnodes;
  SDnode    *pDnode;
} SVnodeThread;

static int32_t dndInitVnodeReadWorker(SDnode *pDnode);
static int32_t dndInitVnodeWriteWorker(SDnode *pDnode);
static int32_t dndInitVnodeSyncWorker(SDnode *pDnode);
static int32_t dndInitVnodeMgmtWorker(SDnode *pDnode);
static void    dndCleanupVnodeReadWorker(SDnode *pDnode);
static void    dndCleanupVnodeWriteWorker(SDnode *pDnode);
static void    dndCleanupVnodeSyncWorker(SDnode *pDnode);
static void    dndCleanupVnodeMgmtWorker(SDnode *pDnode);
static int32_t dndAllocVnodeQueryQueue(SDnode *pDnode, SVnodeObj *pVnode);
static int32_t dndAllocVnodeFetchQueue(SDnode *pDnode, SVnodeObj *pVnode);
static int32_t dndAllocVnodeWriteQueue(SDnode *pDnode, SVnodeObj *pVnode);
static int32_t dndAllocVnodeApplyQueue(SDnode *pDnode, SVnodeObj *pVnode);
static int32_t dndAllocVnodeSyncQueue(SDnode *pDnode, SVnodeObj *pVnode);
static void    dndFreeVnodeQueryQueue(SDnode *pDnode, SVnodeObj *pVnode);
static void    dndFreeVnodeFetchQueue(SDnode *pDnode, SVnodeObj *pVnode);
static void    dndFreeVnodeWriteQueue(SDnode *pDnode, SVnodeObj *pVnode);
static void    dndFreeVnodeApplyQueue(SDnode *pDnode, SVnodeObj *pVnode);
static void    dndFreeVnodeSyncQueue(SDnode *pDnode, SVnodeObj *pVnode);

static void    dndProcessVnodeQueryQueue(SVnodeObj *pVnode, SVnodeMsg *pMsg);
static void    dndProcessVnodeFetchQueue(SVnodeObj *pVnode, SVnodeMsg *pMsg);
static void    dndProcessVnodeWriteQueue(SVnodeObj *pVnode, taos_qall qall, int32_t numOfMsgs);
static void    dndProcessVnodeApplyQueue(SVnodeObj *pVnode, taos_qall qall, int32_t numOfMsgs);
static void    dndProcessVnodeSyncQueue(SVnodeObj *pVnode, taos_qall qall, int32_t numOfMsgs);
static void    dndProcessVnodeMgmtQueue(SDnode *pDnode, SRpcMsg *pMsg);
void           dndProcessVnodeQueryMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessVnodeFetchMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessVnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessVnodeSyncMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessVnodeMgmtMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
static int32_t dndPutMsgIntoVnodeApplyQueue(SDnode *pDnode, int32_t vgId, SVnodeMsg *pMsg);

static SVnodeObj  *dndAcquireVnode(SDnode *pDnode, int32_t vgId);
static void        dndReleaseVnode(SDnode *pDnode, SVnodeObj *pVnode);
static int32_t     dndCreateVnodeWrapper(SDnode *pDnode, int32_t vgId, SVnode *pImpl);
static void        dndDropVnodeWrapper(SDnode *pDnode, SVnodeObj *pVnode);
static SVnodeObj **dndGetVnodesFromHash(SDnode *pDnode, int32_t *numOfVnodes);
static int32_t     dndGetVnodesFromFile(SDnode *pDnode, SVnodeObj **ppVnodes, int32_t *numOfVnodes);
static int32_t     dndWriteVnodesToFile(SDnode *pDnode);

static int32_t dndCreateVnode(SDnode *pDnode, int32_t vgId, SVnodeCfg *pCfg);
static int32_t dndDropVnode(SDnode *pDnode, SVnodeObj *pVnode);
static int32_t dndOpenVnodes(SDnode *pDnode);
static void    dndCloseVnodes(SDnode *pDnode);

static int32_t dndProcessCreateVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg);
static int32_t dndProcessAlterVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg);
static int32_t dndProcessDropVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg);
static int32_t dndProcessAuthVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg);
static int32_t dndProcessSyncVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg);
static int32_t dndProcessCompactVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg);

static SVnodeObj *dndAcquireVnode(SDnode *pDnode, int32_t vgId) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  SVnodeObj   *pVnode = NULL;
  int32_t      refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  taosHashGetClone(pMgmt->hash, &vgId, sizeof(int32_t), (void *)&pVnode);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
  } else {
    refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
  }
  taosRUnLockLatch(&pMgmt->latch);

  dTrace("vgId:%d, acquire vnode, refCount:%d", pVnode->vgId, refCount);
  return pVnode;
}

static void dndReleaseVnode(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  int32_t      refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pVnode != NULL) {
    refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pVnode != NULL) {
    dTrace("vgId:%d, release vnode, refCount:%d", pVnode->vgId, refCount);
  }
}

static int32_t dndCreateVnodeWrapper(SDnode *pDnode, int32_t vgId, SVnode *pImpl) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  SVnodeObj   *pVnode = calloc(1, sizeof(SVnodeObj));
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pVnode->vgId = vgId;
  pVnode->refCount = 0;
  pVnode->dropped = 0;
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  pVnode->pImpl = pImpl;

  if (dndAllocVnodeQueryQueue(pDnode, pVnode) != 0) {
    return -1;
  }

  if (dndAllocVnodeFetchQueue(pDnode, pVnode) != 0) {
    return -1;
  }

  if (dndAllocVnodeWriteQueue(pDnode, pVnode) != 0) {
    return -1;
  }

  if (dndAllocVnodeApplyQueue(pDnode, pVnode) != 0) {
    return -1;
  }

  if (dndAllocVnodeSyncQueue(pDnode, pVnode) != 0) {
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  int32_t code = taosHashPut(pMgmt->hash, &vgId, sizeof(int32_t), &pVnode, sizeof(SVnodeObj *));
  taosWUnLockLatch(&pMgmt->latch);

  if (code != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return code;
}

static void dndDropVnodeWrapper(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  taosWLockLatch(&pMgmt->latch);
  taosHashRemove(pMgmt->hash, &pVnode->vgId, sizeof(int32_t));
  taosWUnLockLatch(&pMgmt->latch);

  dndReleaseVnode(pDnode, pVnode);
  while (pVnode->refCount > 0) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pWriteQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pSyncQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pApplyQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pQueryQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pFetchQ)) taosMsleep(10);

  dndFreeVnodeQueryQueue(pDnode, pVnode);
  dndFreeVnodeFetchQueue(pDnode, pVnode);
  dndFreeVnodeWriteQueue(pDnode, pVnode);
  dndFreeVnodeApplyQueue(pDnode, pVnode);
  dndFreeVnodeSyncQueue(pDnode, pVnode);
}

static SVnodeObj **dndGetVnodesFromHash(SDnode *pDnode, int32_t *numOfVnodes) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  taosRLockLatch(&pMgmt->latch);

  int32_t     num = 0;
  int32_t     size = taosHashGetSize(pMgmt->hash);
  SVnodeObj **pVnodes = calloc(size, sizeof(SVnodeObj *));

  void *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    SVnodeObj  *pVnode = *ppVnode;
    if (pVnode) {
      num++;
      if (num < size) {
        int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
        dTrace("vgId:%d, acquire vnode, refCount:%d", pVnode->vgId, refCount);
        pVnodes[num] = (*ppVnode);
      }
    }
    pIter = taosHashIterate(pMgmt->hash, pIter);
  }

  taosRUnLockLatch(&pMgmt->latch);
  *numOfVnodes = num;

  return pVnodes;
}

static int32_t dndGetVnodesFromFile(SDnode *pDnode, SVnodeObj **ppVnodes, int32_t *numOfVnodes) {
  int32_t    code = TSDB_CODE_DND_VNODE_READ_FILE_ERROR;
  int32_t    len = 0;
  int32_t    maxLen = 30000;
  char      *content = calloc(1, maxLen + 1);
  cJSON     *root = NULL;
  FILE      *fp = NULL;
  char       file[PATH_MAX + 20] = {0};
  SVnodeObj *pVnodes = NULL;

  snprintf(file, PATH_MAX + 20, "%s/vnodes.json", pDnode->dir.vnodes);

  fp = fopen(file, "r");
  if (!fp) {
    dDebug("file %s not exist", file);
    code = 0;
    goto PRASE_VNODE_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", file);
    goto PRASE_VNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", file);
    goto PRASE_VNODE_OVER;
  }

  cJSON *vnodes = cJSON_GetObjectItem(root, "vnodes");
  if (!vnodes || vnodes->type != cJSON_Array) {
    dError("failed to read %s since vnodes not found", file);
    goto PRASE_VNODE_OVER;
  }

  int32_t vnodesNum = cJSON_GetArraySize(vnodes);
  if (vnodesNum <= 0) {
    dError("failed to read %s since vnodes size:%d invalid", file, vnodesNum);
    goto PRASE_VNODE_OVER;
  }

  pVnodes = calloc(vnodesNum, sizeof(SVnodeObj));
  if (pVnodes == NULL) {
    dError("failed to read %s since out of memory", file);
    goto PRASE_VNODE_OVER;
  }

  for (int32_t i = 0; i < vnodesNum; ++i) {
    cJSON     *vnode = cJSON_GetArrayItem(vnodes, i);
    SVnodeObj *pVnode = &pVnodes[i];

    cJSON *vgId = cJSON_GetObjectItem(vnode, "vgId");
    if (!vgId || vgId->type != cJSON_String) {
      dError("failed to read %s since vgId not found", file);
      goto PRASE_VNODE_OVER;
    }
    pVnode->vgId = atoi(vgId->valuestring);

    cJSON *dropped = cJSON_GetObjectItem(vnode, "dropped");
    if (!dropped || dropped->type != cJSON_String) {
      dError("failed to read %s since dropped not found", file);
      goto PRASE_VNODE_OVER;
    }
    pVnode->dropped = atoi(vnode->valuestring);
  }

  code = 0;
  dInfo("succcessed to read file %s", file);

PRASE_VNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  return code;
}

static int32_t dndWriteVnodesToFile(SDnode *pDnode) {
  char file[PATH_MAX + 20] = {0};
  char realfile[PATH_MAX + 20] = {0};
  snprintf(file, PATH_MAX + 20, "%s/vnodes.json.bak", pDnode->dir.vnodes);
  snprintf(realfile, PATH_MAX + 20, "%s/vnodes.json", pDnode->dir.vnodes);

  FILE *fp = fopen(file, "w");
  if (fp != NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }

  int32_t     len = 0;
  int32_t     maxLen = 30000;
  char       *content = calloc(1, maxLen + 1);
  int32_t     numOfVnodes = 0;
  SVnodeObj **pVnodes = dndGetVnodesFromHash(pDnode, &numOfVnodes);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"vnodes\": [{\n");
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    SVnodeObj *pVnode = pVnodes[i];
    len += snprintf(content + len, maxLen - len, "    \"vgId\": \"%d\",\n", pVnode->vgId);
    len += snprintf(content + len, maxLen - len, "    \"dropped\": \"%d\"\n", pVnode->dropped);
    if (i < numOfVnodes - 1) {
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

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    SVnodeObj *pVnode = pVnodes[i];
    dndReleaseVnode(pDnode, pVnode);
  }

  if (pVnodes != NULL) {
    free(pVnodes);
  }

  dInfo("successed to write %s", file);
  return taosRenameFile(file, realfile);
}

static int32_t dndCreateVnode(SDnode *pDnode, int32_t vgId, SVnodeCfg *pCfg) {
  char path[PATH_MAX + 20] = {0};
  snprintf(path, sizeof(path), "%s/vnode%d", pDnode->dir.vnodes, vgId);
  SVnode *pImpl = vnodeCreate(vgId, path, pCfg);

  if (pImpl == NULL) {
    return -1;
  }

  int32_t code = dndCreateVnodeWrapper(pDnode, vgId, pImpl);
  if (code != 0) {
    vnodeDrop(pImpl);
    terrno = code;
    return code;
  }

  code = dndWriteVnodesToFile(pDnode);
  if (code != 0) {
    vnodeDrop(pImpl);
    terrno = code;
    return code;
  }

  return 0;
}

static int32_t dndDropVnode(SDnode *pDnode, SVnodeObj *pVnode) {
  pVnode->dropped = 1;
  if (dndWriteVnodesToFile(pDnode) != 0) {
    pVnode->dropped = 0;
    return -1;
  }

  dndDropVnodeWrapper(pDnode, pVnode);
  vnodeDrop(pVnode->pImpl);
  dndWriteVnodesToFile(pDnode);
  return 0;
}

static void *dnodeOpenVnodeFunc(void *param) {
  SVnodeThread *pThread = param;
  SDnode       *pDnode = pThread->pDnode;
  SVnodesMgmt  *pMgmt = &pDnode->vmgmt;

  dDebug("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("open-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SVnodeObj *pVnode = &pThread->pVnodes[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been opened", pVnode->vgId,
             pMgmt->openVnodes, pMgmt->totalVnodes);
    dndReportStartup(pDnode, "open-vnodes", stepDesc);

    char path[PATH_MAX + 20] = {0};
    snprintf(path, sizeof(path), "%s/vnode%d", pDnode->dir.vnodes, pVnode->vgId);
    SVnode *pImpl = vnodeOpen(path, NULL);
    if (pImpl == NULL) {
      dError("vgId:%d, failed to open vnode by thread:%d", pVnode->vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      dndCreateVnodeWrapper(pDnode, pVnode->vgId, pImpl);
      dDebug("vgId:%d, is opened by thread:%d", pVnode->vgId, pThread->threadIndex);
      pThread->opened++;
    }

    atomic_add_fetch_32(&pMgmt->openVnodes, 1);
  }

  dDebug("thread:%d, total vnodes:%d, opened:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
         pThread->failed);
  return NULL;
}

static int32_t dndOpenVnodes(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  taosInitRWLatch(&pMgmt->latch);

  pMgmt->hash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMgmt->hash == NULL) {
    dError("failed to init vnode hash");
    terrno = TSDB_CODE_VND_OUT_OF_MEMORY;
    return -1;
  }

  SVnodeObj *pVnodes = NULL;
  int32_t    numOfVnodes = 0;
  if (dndGetVnodesFromFile(pDnode, &pVnodes, &numOfVnodes) != 0) {
    dInfo("failed to get vnode list from disk since %s", terrstr());
    return -1;
  }

  pMgmt->totalVnodes = numOfVnodes;

  int32_t threadNum = tsNumOfCores;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SVnodeThread *threads = calloc(threadNum, sizeof(SVnodeThread));
  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].pVnodes = calloc(vnodesPerThread, sizeof(SVnodeObj));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t       t = v % threadNum;
    SVnodeThread *pThread = &threads[t];
    pThread->pVnodes[pThread->vnodeNum++] = pVnodes[v];
  }

  dInfo("start %d threads to open %d vnodes", threadNum, numOfVnodes);

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    pThread->pThreadId = taosCreateThread(dnodeOpenVnodeFunc, pThread);
    if (pThread->pThreadId == NULL) {
      dError("thread:%d, failed to create thread to open vnode, reason:%s", pThread->threadIndex, strerror(errno));
    }
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    taosDestoryThread(pThread->pThreadId);
    pThread->pThreadId = NULL;
    free(pThread->pVnodes);
  }
  free(threads);

  if (pMgmt->openVnodes != pMgmt->totalVnodes) {
    dError("there are total vnodes:%d, opened:%d", pMgmt->totalVnodes, pMgmt->openVnodes);
    return -1;
  } else {
    dInfo("total vnodes:%d open successfully", pMgmt->totalVnodes);
    return 0;
  }
}

static void dndCloseVnodes(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;

  int32_t     numOfVnodes = 0;
  SVnodeObj **pVnodes = dndGetVnodesFromHash(pDnode, &numOfVnodes);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    dndDropVnodeWrapper(pDnode, pVnodes[i]);
  }

  if (pVnodes != NULL) {
    free(pVnodes);
  }

  if (pMgmt->hash != NULL) {
    taosHashCleanup(pMgmt->hash);
    pMgmt->hash = NULL;
  }

  dInfo("total vnodes:%d are all closed", numOfVnodes);
}

static int32_t dndParseCreateVnodeReq(SRpcMsg *rpcMsg, int32_t *vgId, SVnodeCfg *pCfg) {
  SCreateVnodeMsg *pCreate = rpcMsg->pCont;
  *vgId = htonl(pCreate->vgId);

  tstrncpy(pCfg->db, pCreate->db, TSDB_FULL_DB_NAME_LEN);
  pCfg->cacheBlockSize = htonl(pCreate->cacheBlockSize);
  pCfg->totalBlocks = htonl(pCreate->totalBlocks);
  pCfg->daysPerFile = htonl(pCreate->daysPerFile);
  pCfg->daysToKeep0 = htonl(pCreate->daysToKeep0);
  pCfg->daysToKeep1 = htonl(pCreate->daysToKeep1);
  pCfg->daysToKeep2 = htonl(pCreate->daysToKeep2);
  pCfg->minRowsPerFileBlock = htonl(pCreate->minRowsPerFileBlock);
  pCfg->maxRowsPerFileBlock = htonl(pCreate->maxRowsPerFileBlock);
  pCfg->precision = pCreate->precision;
  pCfg->compression = pCreate->compression;
  pCfg->cacheLastRow = pCreate->cacheLastRow;
  pCfg->update = pCreate->update;
  pCfg->quorum = pCreate->quorum;
  pCfg->replica = pCreate->replica;
  pCfg->walLevel = pCreate->walLevel;
  pCfg->fsyncPeriod = htonl(pCreate->fsyncPeriod);

  for (int32_t i = 0; i < pCfg->replica; ++i) {
    pCfg->replicas[i].port = htons(pCreate->replicas[i].port);
    tstrncpy(pCfg->replicas[i].fqdn, pCreate->replicas[i].fqdn, TSDB_FQDN_LEN);
  }

  return 0;
}

static SDropVnodeMsg *vnodeParseDropVnodeReq(SRpcMsg *rpcMsg) {
  SDropVnodeMsg *pDrop = rpcMsg->pCont;
  pDrop->vgId = htonl(pDrop->vgId);
  return pDrop;
}

static SAuthVnodeMsg *vnodeParseAuthVnodeReq(SRpcMsg *rpcMsg) {
  SAuthVnodeMsg *pAuth = rpcMsg->pCont;
  pAuth->vgId = htonl(pAuth->vgId);
  return pAuth;
}

static int32_t dndProcessCreateVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg) {
  SVnodeCfg vnodeCfg = {0};
  int32_t   vgId = 0;

  dndParseCreateVnodeReq(rpcMsg, &vgId, &vnodeCfg);
  dDebug("vgId:%d, create vnode req is received", vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, already exist, return success", vgId);
    dndReleaseVnode(pDnode, pVnode);
    return 0;
  }

  if (dndCreateVnode(pDnode, vgId, &vnodeCfg) != 0) {
    dError("vgId:%d, failed to create vnode since %s", vgId, terrstr());
    return terrno;
  }

  return 0;
}

static int32_t dndProcessAlterVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg) {
  SVnodeCfg vnodeCfg = {0};
  int32_t   vgId = 0;

  dndParseCreateVnodeReq(rpcMsg, &vgId, &vnodeCfg);
  dDebug("vgId:%d, alter vnode req is received", vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to alter vnode since %s", vgId, terrstr());
    return terrno;
  }

  if (vnodeAlter(pVnode->pImpl, &vnodeCfg) != 0) {
    dError("vgId:%d, failed to alter vnode since %s", vgId, terrstr());
    dndReleaseVnode(pDnode, pVnode);
    return terrno;
  }

  dndReleaseVnode(pDnode, pVnode);
  return 0;
}

static int32_t dndProcessDropVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg) {
  SDropVnodeMsg *pDrop = vnodeParseDropVnodeReq(rpcMsg);

  int32_t vgId = pDrop->vgId;
  dDebug("vgId:%d, drop vnode req is received", vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to drop since %s", vgId, terrstr());
    return terrno;
  }

  if (dndDropVnode(pDnode, pVnode) != 0) {
    dError("vgId:%d, failed to drop vnode since %s", vgId, terrstr());
    dndReleaseVnode(pDnode, pVnode);
    return terrno;
  }

  return 0;
}

static int32_t dndProcessAuthVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg) {
  SAuthVnodeMsg *pAuth = (SAuthVnodeMsg *)vnodeParseAuthVnodeReq(rpcMsg);

  int32_t code = 0;
  int32_t vgId = pAuth->vgId;
  dDebug("vgId:%d, auth vnode req is received", vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to auth since %s", vgId, terrstr());
    return terrno;
  }

  pVnode->accessState = pAuth->accessState;
  dndReleaseVnode(pDnode, pVnode);
  return 0;
}

static int32_t dndProcessSyncVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg) {
  SAuthVnodeMsg *pAuth = (SAuthVnodeMsg *)vnodeParseAuthVnodeReq(rpcMsg);

  int32_t vgId = pAuth->vgId;
  dDebug("vgId:%d, auth vnode req is received", vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to auth since %s", vgId, terrstr());
    return terrno;
  }

  if (vnodeSync(pVnode->pImpl) != 0) {
    dError("vgId:%d, failed to auth vnode since %s", vgId, terrstr());
    dndReleaseVnode(pDnode, pVnode);
    return terrno;
  }

  dndReleaseVnode(pDnode, pVnode);
  return 0;
}

static int32_t dndProcessCompactVnodeReq(SDnode *pDnode, SRpcMsg *rpcMsg) {
  SCompactVnodeMsg *pCompact = (SCompactVnodeMsg *)vnodeParseDropVnodeReq(rpcMsg);

  int32_t vgId = pCompact->vgId;
  dDebug("vgId:%d, compact vnode req is received", vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to compact since %s", vgId, terrstr());
    return terrno;
  }

  if (vnodeCompact(pVnode->pImpl) != 0) {
    dError("vgId:%d, failed to compact vnode since %s", vgId, terrstr());
    dndReleaseVnode(pDnode, pVnode);
    return terrno;
  }

  dndReleaseVnode(pDnode, pVnode);
  return 0;
}

static void dndProcessVnodeMgmtQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_CREATE_VNODE_IN:
      code = dndProcessCreateVnodeReq(pDnode, pMsg);
      break;
    case TSDB_MSG_TYPE_ALTER_VNODE_IN:
      code = dndProcessAlterVnodeReq(pDnode, pMsg);
      break;
    case TSDB_MSG_TYPE_DROP_VNODE_IN:
      code = dndProcessDropVnodeReq(pDnode, pMsg);
      break;
    case TSDB_MSG_TYPE_AUTH_VNODE_IN:
      code = dndProcessAuthVnodeReq(pDnode, pMsg);
      break;
    case TSDB_MSG_TYPE_SYNC_VNODE_IN:
      code = dndProcessSyncVnodeReq(pDnode, pMsg);
      break;
    case TSDB_MSG_TYPE_COMPACT_VNODE_IN:
      code = dndProcessCompactVnodeReq(pDnode, pMsg);
      break;
    default:
      code = TSDB_CODE_MSG_NOT_PROCESSED;
      break;
  }

  if (code != 0) {
    SRpcMsg rsp = {.code = code, .handle = pMsg->handle};
    rpcSendResponse(&rsp);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static void dndProcessVnodeQueryQueue(SVnodeObj *pVnode, SVnodeMsg *pMsg) {
  vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_QUERY);
}

static void dndProcessVnodeFetchQueue(SVnodeObj *pVnode, SVnodeMsg *pMsg) {
  vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_FETCH);
}

static void dndProcessVnodeWriteQueue(SVnodeObj *pVnode, taos_qall qall, int32_t numOfMsgs) {
  SVnodeMsg *pMsg = vnodeInitMsg(numOfMsgs);
  SRpcMsg   *pRpcMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pRpcMsg);
    vnodeAppendMsg(pMsg, pRpcMsg);
    taosFreeQitem(pRpcMsg);
  }

  vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_WRITE);
}

static void dndProcessVnodeApplyQueue(SVnodeObj *pVnode, taos_qall qall, int32_t numOfMsgs) {
  SVnodeMsg *pMsg = NULL;
  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);
    vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_APPLY);
  }
}

static void dndProcessVnodeSyncQueue(SVnodeObj *pVnode, taos_qall qall, int32_t numOfMsgs) {
  SVnodeMsg *pMsg = NULL;
  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);
    vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_SYNC);
  }
}

static int32_t dndWriteRpcMsgToVnodeQueue(taos_queue pQueue, SRpcMsg *pRpcMsg) {
  int32_t code = 0;

  if (pQueue == NULL) {
    code = TSDB_CODE_MSG_NOT_PROCESSED;
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

  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {.handle = pRpcMsg->handle, .code = code};
    rpcSendResponse(&rsp);
    rpcFreeCont(pRpcMsg->pCont);
  }
}

static int32_t dndWriteVnodeMsgToVnodeQueue(taos_queue pQueue, SRpcMsg *pRpcMsg) {
  int32_t code = 0;

  if (pQueue == NULL) {
    code = TSDB_CODE_MSG_NOT_PROCESSED;
  } else {
    SVnodeMsg *pMsg = vnodeInitMsg(1);
    if (pMsg == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      if (vnodeAppendMsg(pMsg, pRpcMsg) != 0) {
        code = terrno;
      } else {
        if (taosWriteQitem(pQueue, pMsg) != 0) {
          code = TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {.handle = pRpcMsg->handle, .code = code};
    rpcSendResponse(&rsp);
    rpcFreeCont(pRpcMsg->pCont);
  }
}

static SVnodeObj *dndAcquireVnodeFromMsg(SDnode *pDnode, SRpcMsg *pMsg) {
  SMsgHead *pHead = (SMsgHead *)pMsg->pCont;
  pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, pHead->vgId);
  if (pVnode == NULL) {
    SRpcMsg rsp = {.handle = pMsg->handle, .code = terrno};
    rpcSendResponse(&rsp);
    rpcFreeCont(pMsg->pCont);
  }

  return pVnode;
}

void dndProcessVnodeMgmtMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  dndWriteRpcMsgToVnodeQueue(pMgmt->pMgmtQ, pMsg);
}

void dndProcessVnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dndAcquireVnodeFromMsg(pDnode, pMsg);
  if (pVnode != NULL) {
    dndWriteRpcMsgToVnodeQueue(pVnode->pWriteQ, pMsg);
    dndReleaseVnode(pDnode, pVnode);
  }
}

void dndProcessVnodeSyncMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dndAcquireVnodeFromMsg(pDnode, pMsg);
  if (pVnode != NULL) {
    dndWriteVnodeMsgToVnodeQueue(pVnode->pSyncQ, pMsg);
    dndReleaseVnode(pDnode, pVnode);
  }
}

void dndProcessVnodeQueryMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dndAcquireVnodeFromMsg(pDnode, pMsg);
  if (pVnode != NULL) {
    dndWriteVnodeMsgToVnodeQueue(pVnode->pQueryQ, pMsg);
    dndReleaseVnode(pDnode, pVnode);
  }
}

void dndProcessVnodeFetchMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dndAcquireVnodeFromMsg(pDnode, pMsg);
  if (pVnode != NULL) {
    dndWriteVnodeMsgToVnodeQueue(pVnode->pFetchQ, pMsg);
    dndReleaseVnode(pDnode, pVnode);
  }
}

static int32_t dndPutMsgIntoVnodeApplyQueue(SDnode *pDnode, int32_t vgId, SVnodeMsg *pMsg) {
  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) {
    return -1;
  }

  int32_t code = taosWriteQitem(pVnode->pApplyQ, pMsg);
  dndReleaseVnode(pDnode, pVnode);
  return code;
}

static int32_t dndInitVnodeMgmtWorker(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  SWorkerPool *pPool = &pMgmt->mgmtPool;
  pPool->name = "vnode-mgmt";
  pPool->min = 1;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    terrno = TSDB_CODE_VND_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->pMgmtQ = tWorkerAllocQueue(pPool, pDnode, (FProcessItem)dndProcessVnodeMgmtQueue);
  if (pMgmt->pMgmtQ == NULL) {
    terrno = TSDB_CODE_VND_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndCleanupVnodeMgmtWorker(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tWorkerFreeQueue(&pMgmt->mgmtPool, pMgmt->pMgmtQ);
  tWorkerCleanup(&pMgmt->mgmtPool);
  pMgmt->pMgmtQ = NULL;
}

static int32_t dndAllocVnodeQueryQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  pVnode->pQueryQ = tWorkerAllocQueue(&pMgmt->queryPool, pVnode, (FProcessItem)dndProcessVnodeQueryQueue);
  if (pVnode->pQueryQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeVnodeQueryQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tWorkerFreeQueue(&pMgmt->queryPool, pVnode->pQueryQ);
  pVnode->pQueryQ = NULL;
}

static int32_t dndAllocVnodeFetchQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  pVnode->pFetchQ = tWorkerAllocQueue(&pMgmt->fetchPool, pVnode, (FProcessItem)dndProcessVnodeFetchQueue);
  if (pVnode->pFetchQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return 0;
}

static void dndFreeVnodeFetchQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tWorkerFreeQueue(&pMgmt->fetchPool, pVnode->pFetchQ);
  pVnode->pFetchQ = NULL;
}

static int32_t dndInitVnodeReadWorker(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;

  int32_t maxFetchThreads = 4;
  float   threadsForQuery = MAX(pDnode->opt.numOfCores * pDnode->opt.ratioOfQueryCores, 1);

  SWorkerPool *pPool = &pMgmt->queryPool;
  pPool->name = "vnode-query";
  pPool->min = (int32_t)threadsForQuery;
  pPool->max = pPool->min;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  pPool = &pMgmt->fetchPool;
  pPool->name = "vnode-fetch";
  pPool->min = MIN(maxFetchThreads, pDnode->opt.numOfCores);
  pPool->max = pPool->min;
  if (tWorkerInit(pPool) != 0) {
    TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  return 0;
}

static void dndCleanupVnodeReadWorker(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tWorkerCleanup(&pMgmt->fetchPool);
  tWorkerCleanup(&pMgmt->queryPool);
}

static int32_t dndAllocVnodeWriteQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  pVnode->pWriteQ = tMWorkerAllocQueue(&pMgmt->writePool, pVnode, (FProcessItems)dndProcessVnodeWriteQueue);
  if (pVnode->pWriteQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeVnodeWriteQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tMWorkerFreeQueue(&pMgmt->writePool, pVnode->pWriteQ);
  pVnode->pWriteQ = NULL;
}

static int32_t dndAllocVnodeApplyQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  pVnode->pApplyQ = tMWorkerAllocQueue(&pMgmt->writePool, pVnode, (FProcessItems)dndProcessVnodeApplyQueue);
  if (pVnode->pApplyQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeVnodeApplyQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tMWorkerFreeQueue(&pMgmt->writePool, pVnode->pApplyQ);
  pVnode->pApplyQ = NULL;
}

static int32_t dndInitVnodeWriteWorker(SDnode *pDnode) {
  SVnodesMgmt  *pMgmt = &pDnode->vmgmt;
  SMWorkerPool *pPool = &pMgmt->writePool;
  pPool->name = "vnode-write";
  pPool->max = tsNumOfCores;
  if (tMWorkerInit(pPool) != 0) {
    terrno = TSDB_CODE_VND_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndCleanupVnodeWriteWorker(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tMWorkerCleanup(&pMgmt->writePool);
}

static int32_t dndAllocVnodeSyncQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  pVnode->pSyncQ = tMWorkerAllocQueue(&pMgmt->writePool, pVnode, (FProcessItems)dndProcessVnodeSyncQueue);
  if (pVnode->pSyncQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeVnodeSyncQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tMWorkerFreeQueue(&pMgmt->writePool, pVnode->pSyncQ);
  pVnode->pSyncQ = NULL;
}

static int32_t dndInitVnodeSyncWorker(SDnode *pDnode) {
  int32_t maxThreads = tsNumOfCores / 2;
  if (maxThreads < 1) maxThreads = 1;

  SVnodesMgmt  *pMgmt = &pDnode->vmgmt;
  SMWorkerPool *pPool = &pMgmt->writePool;
  pPool->name = "vnode-sync";
  pPool->max = maxThreads;
  if (tMWorkerInit(pPool) != 0) {
    terrno = TSDB_CODE_VND_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndCleanupVnodeSyncWorker(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tMWorkerCleanup(&pMgmt->syncPool);
}

int32_t dndInitVnodes(SDnode *pDnode) {
  dInfo("dnode-vnodes start to init");

  if (dndInitVnodeReadWorker(pDnode) != 0) {
    dError("failed to init vnodes read worker since %s", terrstr());
    return -1;
  }

  if (dndInitVnodeWriteWorker(pDnode) != 0) {
    dError("failed to init vnodes write worker since %s", terrstr());
    return -1;
  }

  if (dndInitVnodeSyncWorker(pDnode) != 0) {
    dError("failed to init vnodes sync worker since %s", terrstr());
    return -1;
  }

  if (dndInitVnodeMgmtWorker(pDnode) != 0) {
    dError("failed to init vnodes mgmt worker since %s", terrstr());
    return -1;
  }

  if (dndOpenVnodes(pDnode) != 0) {
    dError("failed to open vnodes since %s", terrstr());
    return -1;
  }

  dInfo("dnode-vnodes is initialized");
  return 0;
}

void dndCleanupVnodes(SDnode *pDnode) {
  dInfo("dnode-vnodes start to clean up");
  dndCloseVnodes(pDnode);
  dndCleanupVnodeReadWorker(pDnode);
  dndCleanupVnodeWriteWorker(pDnode);
  dndCleanupVnodeSyncWorker(pDnode);
  dndCleanupVnodeMgmtWorker(pDnode);
  dInfo("dnode-vnodes is cleaned up");
}

void dndGetVnodeLoads(SDnode *pDnode, SVnodeLoads *pLoads) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;

  taosRLockLatch(&pMgmt->latch);
  pLoads->num = taosHashGetSize(pMgmt->hash);

  int32_t v = 0;
  void   *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj  *pVnode = *ppVnode;
    SVnodeLoad *pLoad = &pLoads->data[v++];

    vnodeGetLoad(pVnode->pImpl, pLoad);
    pLoad->vgId = htonl(pLoad->vgId);
    pLoad->totalStorage = htobe64(pLoad->totalStorage);
    pLoad->compStorage = htobe64(pLoad->compStorage);
    pLoad->pointsWritten = htobe64(pLoad->pointsWritten);
    pLoad->tablesNum = htobe64(pLoad->tablesNum);

    pIter = taosHashIterate(pMgmt->hash, pIter);
  }

  taosRUnLockLatch(&pMgmt->latch);
}
