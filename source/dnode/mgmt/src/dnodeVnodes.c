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
#include "dnodeVnodes.h"
#include "cJSON.h"
#include "dnodeTransport.h"
#include "thash.h"
#include "tlockfree.h"
#include "tqueue.h"
#include "tstep.h"
#include "tthread.h"
#include "tworker.h"
#include "vnode.h"

typedef struct {
  int32_t    vgId;
  int32_t    refCount;
  int8_t     dropped;
  int8_t     accessState;
  SVnode *   pImpl;
  taos_queue pWriteQ;
  taos_queue pSyncQ;
  taos_queue pApplyQ;
  taos_queue pQueryQ;
  taos_queue pFetchQ;
} SVnodeObj;

typedef struct {
  pthread_t *threadId;
  int32_t    threadIndex;
  int32_t    failed;
  int32_t    opened;
  int32_t    vnodeNum;
  SVnodeObj *pVnodes;
} SVThread;

static struct {
  SHashObj *   hash;
  SWorkerPool  mgmtPool;
  SWorkerPool  queryPool;
  SWorkerPool  fetchPool;
  SMWorkerPool syncPool;
  SMWorkerPool writePool;
  taos_queue   pMgmtQ;
  SSteps *     pSteps;
  int32_t      openVnodes;
  int32_t      totalVnodes;
  SRWLatch     latch;
} tsVnodes;

static int32_t dnodeAllocVnodeQueryQueue(SVnodeObj *pVnode);
static void    dnodeFreeVnodeQueryQueue(SVnodeObj *pVnode);
static int32_t dnodeAllocVnodeFetchQueue(SVnodeObj *pVnode);
static void    dnodeFreeVnodeFetchQueue(SVnodeObj *pVnode);
static int32_t dnodeAllocVnodeWriteQueue(SVnodeObj *pVnode);
static void    dnodeFreeVnodeWriteQueue(SVnodeObj *pVnode);
static int32_t dnodeAllocVnodeApplyQueue(SVnodeObj *pVnode);
static void    dnodeFreeVnodeApplyQueue(SVnodeObj *pVnode);
static int32_t dnodeAllocVnodeSyncQueue(SVnodeObj *pVnode);
static void    dnodeFreeVnodeSyncQueue(SVnodeObj *pVnode);

static SVnodeObj *dnodeAcquireVnode(int32_t vgId) {
  SVnodeObj *pVnode = NULL;
  int32_t    refCount = 0;

  taosRLockLatch(&tsVnodes.latch);
  taosHashGetClone(tsVnodes.hash, &vgId, sizeof(int32_t), (void *)&pVnode);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
  } else {
    refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
  }
  taosRUnLockLatch(&tsVnodes.latch);

  dTrace("vgId:%d, accquire vnode, refCount:%d", pVnode->vgId, refCount);
  return pVnode;
}

static void dnodeReleaseVnode(SVnodeObj *pVnode) {
  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  dTrace("vgId:%d, release vnode, refCount:%d", pVnode->vgId, refCount);
}

static int32_t dnodeCreateVnodeWrapper(int32_t vgId, SVnode *pImpl) {
  SVnodeObj *pVnode = calloc(1, sizeof(SVnodeObj));
  if (pVnode == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }

  pVnode->vgId = vgId;
  pVnode->refCount = 0;
  pVnode->dropped = 0;
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  pVnode->pImpl = pImpl;

  int32_t code = dnodeAllocVnodeQueryQueue(pVnode);
  if (code != 0) {
    return code;
  }

  code = dnodeAllocVnodeFetchQueue(pVnode);
  if (code != 0) {
    return code;
  }

  code = dnodeAllocVnodeWriteQueue(pVnode);
  if (code != 0) {
    return code;
  }

  code = dnodeAllocVnodeApplyQueue(pVnode);
  if (code != 0) {
    return code;
  }

  code = dnodeAllocVnodeSyncQueue(pVnode);
  if (code != 0) {
    return code;
  }

  taosWLockLatch(&tsVnodes.latch);
  code = taosHashPut(tsVnodes.hash, &vgId, sizeof(int32_t), &pVnode, sizeof(SVnodeObj *));
  taosWUnLockLatch(&tsVnodes.latch);

  return code;
}

static void dnodeDropVnodeWrapper(SVnodeObj *pVnode) {
  taosWLockLatch(&tsVnodes.latch);
  taosHashRemove(tsVnodes.hash, &pVnode->vgId, sizeof(int32_t));
  taosWUnLockLatch(&tsVnodes.latch);

  // wait all queue empty
  dnodeReleaseVnode(pVnode);
  while (pVnode->refCount > 0) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pWriteQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pSyncQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pApplyQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pQueryQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pFetchQ)) taosMsleep(10);

  dnodeFreeVnodeQueryQueue(pVnode);
  dnodeFreeVnodeFetchQueue(pVnode);
  dnodeFreeVnodeWriteQueue(pVnode);
  dnodeFreeVnodeApplyQueue(pVnode);
  dnodeFreeVnodeSyncQueue(pVnode);
}

static SVnodeObj **dnodeGetVnodesFromHash(int32_t *numOfVnodes) {
  taosRLockLatch(&tsVnodes.latch);

  int32_t     num = 0;
  int32_t     size = taosHashGetSize(tsVnodes.hash);
  SVnodeObj **pVnodes = calloc(size, sizeof(SVnodeObj *));

  void *pIter = taosHashIterate(tsVnodes.hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    SVnodeObj * pVnode = *ppVnode;
    if (pVnode) {
      num++;
      if (num < size) {
        int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
        dTrace("vgId:%d, accquire vnode, refCount:%d", pVnode->vgId, refCount);
        pVnodes[num] = (*ppVnode);
      }
    }
    pIter = taosHashIterate(tsVnodes.hash, pIter);
  }

  taosRUnLockLatch(&tsVnodes.latch);
  *numOfVnodes = num;

  return pVnodes;
}

static int32_t dnodeGetVnodesFromFile(SVnodeObj **ppVnodes, int32_t *numOfVnodes) {
  int32_t    code = TSDB_CODE_DND_PARSE_VNODE_FILE_ERROR;
  int32_t    len = 0;
  int32_t    maxLen = 30000;
  char *     content = calloc(1, maxLen + 1);
  cJSON *    root = NULL;
  FILE *     fp = NULL;
  char       file[PATH_MAX + 20] = {0};
  SVnodeObj *pVnodes = NULL;

  snprintf(file, PATH_MAX + 20, "%s/vnodes.json", tsVnodeDir);

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
    cJSON *    vnode = cJSON_GetArrayItem(vnodes, i);
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

static int32_t dnodeWriteVnodesToFile() {
  char file[PATH_MAX + 20] = {0};
  char realfile[PATH_MAX + 20] = {0};
  snprintf(file, PATH_MAX + 20, "%s/vnodes.json.bak", tsVnodeDir);
  snprintf(realfile, PATH_MAX + 20, "%s/vnodes.json", tsVnodeDir);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    dError("failed to write %s since %s", file, strerror(errno));
    return -1;
  }

  int32_t     len = 0;
  int32_t     maxLen = 30000;
  char *      content = calloc(1, maxLen + 1);
  int32_t     numOfVnodes = 0;
  SVnodeObj **pVnodes = dnodeGetVnodesFromHash(&numOfVnodes);

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
    dnodeReleaseVnode(pVnode);
  }

  if (pVnodes != NULL) {
    free(pVnodes);
  }

  dInfo("successed to write %s", file);
  return taosRenameFile(file, realfile);
}

static int32_t dnodeCreateVnode(int32_t vgId, SVnodeCfg *pCfg) {
  int32_t code = 0;

  char path[PATH_MAX + 20] = {0};
  snprintf(path, sizeof(path), "%s/vnode%d", tsVnodeDir, vgId);
  SVnode *pImpl = vnodeCreate(vgId, path, pCfg);

  if (pImpl == NULL) {
    code = terrno;
    return code;
  }

  code = dnodeCreateVnodeWrapper(vgId, pImpl);
  if (code != 0) {
    vnodeDrop(pImpl);
    return code;
  }

  code = dnodeWriteVnodesToFile();
  if (code != 0) {
    vnodeDrop(pImpl);
    return code;
  }

  return code;
}

static int32_t dnodeDropVnode(SVnodeObj *pVnode) {
  pVnode->dropped = 1;

  int32_t code = dnodeWriteVnodesToFile();
  if (code != 0) {
    pVnode->dropped = 0;
    return code;
  }

  dnodeDropVnodeWrapper(pVnode);
  vnodeDrop(pVnode->pImpl);
  dnodeWriteVnodesToFile();
  return 0;
}

static void *dnodeOpenVnodeFunc(void *param) {
  SVThread *pThread = param;

  dDebug("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("open-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SVnodeObj *pVnode = &pThread->pVnodes[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been opened", pVnode->vgId,
             tsVnodes.openVnodes, tsVnodes.totalVnodes);
    dnodeReportStartup("open-vnodes", stepDesc);

    char path[PATH_MAX + 20] = {0};
    snprintf(path, sizeof(path), "%s/vnode%d", tsVnodeDir, pVnode->vgId);
    SVnode *pImpl = vnodeOpen(path, NULL);
    if (pImpl == NULL) {
      dError("vgId:%d, failed to open vnode by thread:%d", pVnode->vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      dnodeCreateVnodeWrapper(pVnode->vgId, pImpl);
      dDebug("vgId:%d, is opened by thread:%d", pVnode->vgId, pThread->threadIndex);
      pThread->opened++;
    }

    atomic_add_fetch_32(&tsVnodes.openVnodes, 1);
  }

  dDebug("thread:%d, total vnodes:%d, opened:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
         pThread->failed);
  return NULL;
}

static int32_t dnodeOpenVnodes() {
  taosInitRWLatch(&tsVnodes.latch);

  tsVnodes.hash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsVnodes.hash == NULL) {
    dError("failed to init vnode hash");
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  SVnodeObj *pVnodes = NULL;
  int32_t    numOfVnodes = 0;
  int32_t    code = dnodeGetVnodesFromFile(&pVnodes, &numOfVnodes);
  if (code != TSDB_CODE_SUCCESS) {
    dInfo("failed to get vnode list from disk since %s", tstrerror(code));
    return code;
  }

  tsVnodes.totalVnodes = numOfVnodes;

  int32_t threadNum = tsNumOfCores;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SVThread *threads = calloc(threadNum, sizeof(SVThread));
  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].pVnodes = calloc(vnodesPerThread, sizeof(SVnodeObj));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t   t = v % threadNum;
    SVThread *pThread = &threads[t];
    pThread->pVnodes[pThread->vnodeNum++] = pVnodes[v];
  }

  dInfo("start %d threads to open %d vnodes", threadNum, numOfVnodes);

  for (int32_t t = 0; t < threadNum; ++t) {
    SVThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    pThread->threadId = taosCreateThread(dnodeOpenVnodeFunc, pThread);
    if (pThread->threadId == NULL) {
      dError("thread:%d, failed to create thread to open vnode, reason:%s", pThread->threadIndex, strerror(errno));
    }
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    SVThread *pThread = &threads[t];
    taosDestoryThread(pThread->threadId);
    pThread->threadId = NULL;
    free(pThread->pVnodes);
  }
  free(threads);

  if (tsVnodes.openVnodes != tsVnodes.totalVnodes) {
    dError("there are total vnodes:%d, opened:%d", tsVnodes.totalVnodes, tsVnodes.openVnodes);
    return -1;
  } else {
    dInfo("total vnodes:%d open successfully", tsVnodes.totalVnodes);
  }

  return TSDB_CODE_SUCCESS;
}

static void dnodeCloseVnodes() {
  int32_t     numOfVnodes = 0;
  SVnodeObj **pVnodes = dnodeGetVnodesFromHash(&numOfVnodes);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    dnodeDropVnodeWrapper(pVnodes[i]);
  }
  if (pVnodes != NULL) {
    free(pVnodes);
  }

  if (tsVnodes.hash != NULL) {
    taosHashCleanup(tsVnodes.hash);
    tsVnodes.hash = NULL;
  }

  dInfo("total vnodes:%d are all closed", numOfVnodes);
}

static int32_t dnodeParseCreateVnodeReq(SRpcMsg *rpcMsg, int32_t *vgId, SVnodeCfg *pCfg) {
  SCreateVnodeMsg *pCreate = rpcMsg->pCont;
  *vgId = htonl(pCreate->vgId);

#if 0
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
#endif

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

static int32_t vnodeProcessCreateVnodeReq(SRpcMsg *rpcMsg) {
  SVnodeCfg vnodeCfg = {0};
  int32_t   vgId = 0;

  dnodeParseCreateVnodeReq(rpcMsg, &vgId, &vnodeCfg);
  dDebug("vgId:%d, create vnode req is received", vgId);

  SVnodeObj *pVnode = dnodeAcquireVnode(vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, already exist, return success", vgId);
    dnodeReleaseVnode(pVnode);
    return 0;
  }

  int32_t code = dnodeCreateVnode(vgId, &vnodeCfg);
  if (code != 0) {
    dError("vgId:%d, failed to create vnode since %s", vgId, tstrerror(code));
  }

  return code;
}

static int32_t vnodeProcessAlterVnodeReq(SRpcMsg *rpcMsg) {
  SVnodeCfg vnodeCfg = {0};
  int32_t   vgId = 0;
  int32_t   code = 0;

  dnodeParseCreateVnodeReq(rpcMsg, &vgId, &vnodeCfg);
  dDebug("vgId:%d, alter vnode req is received", vgId);

  SVnodeObj *pVnode = dnodeAcquireVnode(vgId);
  if (pVnode == NULL) {
    code = terrno;
    dDebug("vgId:%d, failed to alter vnode since %s", vgId, tstrerror(code));
    return code;
  }

  code = vnodeAlter(pVnode->pImpl, &vnodeCfg);
  if (code != 0) {
    dError("vgId:%d, failed to alter vnode since %s", vgId, tstrerror(code));
  }

  dnodeReleaseVnode(pVnode);
  return code;
}

static int32_t vnodeProcessDropVnodeReq(SRpcMsg *rpcMsg) {
  SDropVnodeMsg *pDrop = vnodeParseDropVnodeReq(rpcMsg);

  int32_t code = 0;
  int32_t vgId = pDrop->vgId;
  dDebug("vgId:%d, drop vnode req is received", vgId);

  SVnodeObj *pVnode = dnodeAcquireVnode(vgId);
  if (pVnode == NULL) {
    code = terrno;
    dDebug("vgId:%d, failed to drop since %s", vgId, tstrerror(code));
    return code;
  }

  code = dnodeDropVnode(pVnode);
  if (code != 0) {
    dnodeReleaseVnode(pVnode);
    dError("vgId:%d, failed to drop vnode since %s", vgId, tstrerror(code));
  }

  return code;
}

static int32_t vnodeProcessAuthVnodeReq(SRpcMsg *rpcMsg) {
  SAuthVnodeMsg *pAuth = (SAuthVnodeMsg *)vnodeParseAuthVnodeReq(rpcMsg);

  int32_t code = 0;
  int32_t vgId = pAuth->vgId;
  dDebug("vgId:%d, auth vnode req is received", vgId);

  SVnodeObj *pVnode = dnodeAcquireVnode(vgId);
  if (pVnode == NULL) {
    code = terrno;
    dDebug("vgId:%d, failed to auth since %s", vgId, tstrerror(code));
    return code;
  }

  pVnode->accessState = pAuth->accessState;
  dnodeReleaseVnode(pVnode);
  return code;
}

static int32_t vnodeProcessSyncVnodeReq(SRpcMsg *rpcMsg) {
  SAuthVnodeMsg *pAuth = (SAuthVnodeMsg *)vnodeParseAuthVnodeReq(rpcMsg);

  int32_t code = 0;
  int32_t vgId = pAuth->vgId;
  dDebug("vgId:%d, auth vnode req is received", vgId);

  SVnodeObj *pVnode = dnodeAcquireVnode(vgId);
  if (pVnode == NULL) {
    code = terrno;
    dDebug("vgId:%d, failed to auth since %s", vgId, tstrerror(code));
    return code;
  }

  code = vnodeSync(pVnode->pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to auth vnode since %s", vgId, tstrerror(code));
  }

  dnodeReleaseVnode(pVnode);
  return code;
}

static int32_t vnodeProcessCompactVnodeReq(SRpcMsg *rpcMsg) {
  SCompactVnodeMsg *pCompact = (SCompactVnodeMsg *)vnodeParseDropVnodeReq(rpcMsg);

  int32_t code = 0;
  int32_t vgId = pCompact->vgId;
  dDebug("vgId:%d, compact vnode req is received", vgId);

  SVnodeObj *pVnode = dnodeAcquireVnode(vgId);
  if (pVnode == NULL) {
    code = terrno;
    dDebug("vgId:%d, failed to compact since %s", vgId, tstrerror(code));
    return code;
  }

  code = vnodeCompact(pVnode->pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to compact vnode since %s", vgId, tstrerror(code));
  }

  dnodeReleaseVnode(pVnode);
  return code;
}

static void dnodeProcessVnodeMgmtQueue(void *unused, SRpcMsg *pMsg) {
  int32_t code = 0;

  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_CREATE_VNODE_IN:
      code = vnodeProcessCreateVnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_ALTER_VNODE_IN:
      code = vnodeProcessAlterVnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_DROP_VNODE_IN:
      code = vnodeProcessDropVnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_AUTH_VNODE_IN:
      code = vnodeProcessAuthVnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_SYNC_VNODE_IN:
      code = vnodeProcessSyncVnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_COMPACT_VNODE_IN:
      code = vnodeProcessCompactVnodeReq(pMsg);
      break;
    default:
      code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
      break;
  }

  SRpcMsg rsp = {.code = code, .handle = pMsg->handle};
  rpcSendResponse(&rsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void dnodeProcessVnodeQueryQueue(SVnodeObj *pVnode, SVnodeMsg *pMsg) {
  vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_QUERY);
}

static void dnodeProcessVnodeFetchQueue(SVnodeObj *pVnode, SVnodeMsg *pMsg) {
  vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_FETCH);
}

static void dnodeProcessVnodeWriteQueue(SVnodeObj *pVnode, taos_qall qall, int32_t numOfMsgs) {
  SVnodeMsg *pMsg = vnodeInitMsg(numOfMsgs);
  SRpcMsg *  pRpcMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pRpcMsg);
    vnodeAppendMsg(pMsg, pRpcMsg);
    taosFreeQitem(pRpcMsg);
  }

  vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_WRITE);
}

static void dnodeProcessVnodeApplyQueue(SVnodeObj *pVnode, taos_qall qall, int32_t numOfMsgs) {
  SVnodeMsg *pMsg = NULL;
  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);
    vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_APPLY);
  }
}

static void dnodeProcessVnodeSyncQueue(SVnodeObj *pVnode, taos_qall qall, int32_t numOfMsgs) {
  SVnodeMsg *pMsg = NULL;
  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);
    vnodeProcessMsg(pVnode->pImpl, pMsg, VN_MSG_TYPE_SYNC);
  }
}

static int32_t dnodeWriteRpcMsgToVnodeQueue(taos_queue pQueue, SRpcMsg *pRpcMsg) {
  int32_t code = 0;

  if (pQueue == NULL) {
    code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
  } else {
    SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg));
    if (pMsg == NULL) {
      code = TSDB_CODE_DND_OUT_OF_MEMORY;
    } else {
      *pMsg = *pRpcMsg;
      code = taosWriteQitem(pQueue, pMsg);
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {.handle = pRpcMsg->handle, .code = code};
    rpcSendResponse(&rsp);
    rpcFreeCont(pRpcMsg->pCont);
  }
}

static int32_t dnodeWriteVnodeMsgToVnodeQueue(taos_queue pQueue, SRpcMsg *pRpcMsg) {
  int32_t code = 0;

  if (pQueue == NULL) {
    code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
  } else {
    SVnodeMsg *pMsg = vnodeInitMsg(1);
    if (pMsg == NULL) {
      code = TSDB_CODE_DND_OUT_OF_MEMORY;
    } else {
      vnodeAppendMsg(pMsg, pRpcMsg);
      code = taosWriteQitem(pQueue, pMsg);
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {.handle = pRpcMsg->handle, .code = code};
    rpcSendResponse(&rsp);
    rpcFreeCont(pRpcMsg->pCont);
  }
}

static SVnodeObj *dnodeAcquireVnodeFromMsg(SRpcMsg *pMsg) {
  SMsgHead *pHead = (SMsgHead *)pMsg->pCont;
  pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = dnodeAcquireVnode(pHead->vgId);
  if (pVnode == NULL) {
    SRpcMsg rsp = {.handle = pMsg->handle, .code = terrno};
    rpcSendResponse(&rsp);
    rpcFreeCont(pMsg->pCont);
  }

  return pVnode;
}

void dnodeProcessVnodeMgmtMsg(SRpcMsg *pMsg, SEpSet *pEpSet) { dnodeWriteRpcMsgToVnodeQueue(tsVnodes.pMgmtQ, pMsg); }

void dnodeProcessVnodeWriteMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dnodeAcquireVnodeFromMsg(pMsg);
  if (pVnode != NULL) {
    dnodeWriteRpcMsgToVnodeQueue(pVnode->pWriteQ, pMsg);
    dnodeReleaseVnode(pVnode);
  }
}

void dnodeProcessVnodeSyncMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dnodeAcquireVnodeFromMsg(pMsg);
  if (pVnode != NULL) {
    dnodeWriteVnodeMsgToVnodeQueue(pVnode->pSyncQ, pMsg);
    dnodeReleaseVnode(pVnode);
  }
}

void dnodeProcessVnodeQueryMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dnodeAcquireVnodeFromMsg(pMsg);
  if (pVnode != NULL) {
    dnodeWriteVnodeMsgToVnodeQueue(pVnode->pQueryQ, pMsg);
    dnodeReleaseVnode(pVnode);
  }
}

void dnodeProcessVnodeFetchMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dnodeAcquireVnodeFromMsg(pMsg);
  if (pVnode != NULL) {
    dnodeWriteVnodeMsgToVnodeQueue(pVnode->pFetchQ, pMsg);
    dnodeReleaseVnode(pVnode);
  }
}

static int32_t dnodePutMsgIntoVnodeApplyQueue(int32_t vgId, SVnodeMsg *pMsg) {
  SVnodeObj *pVnode = dnodeAcquireVnode(vgId);
  if (pVnode == NULL) {
    return terrno;
  }

  int32_t code = taosWriteQitem(pVnode->pApplyQ, pMsg);
  dnodeReleaseVnode(pVnode);
  return code;
}

static int32_t dnodeInitVnodeMgmtWorker() {
  SWorkerPool *pPool = &tsVnodes.mgmtPool;
  pPool->name = "vnode-mgmt";
  pPool->min = 1;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  tsVnodes.pMgmtQ = tWorkerAllocQueue(pPool, NULL, (FProcessItem)dnodeProcessVnodeMgmtQueue);
  if (tsVnodes.pMgmtQ == NULL) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  return 0;
}

static void dnodeCleanupVnodeMgmtWorker() {
  tWorkerFreeQueue(&tsVnodes.mgmtPool, tsVnodes.pMgmtQ);
  tWorkerCleanup(&tsVnodes.mgmtPool);
  tsVnodes.pMgmtQ = NULL;
}

static int32_t dnodeAllocVnodeQueryQueue(SVnodeObj *pVnode) {
  pVnode->pQueryQ = tWorkerAllocQueue(&tsVnodes.queryPool, pVnode, (FProcessItem)dnodeProcessVnodeQueryQueue);
  if (pVnode->pQueryQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeVnodeQueryQueue(SVnodeObj *pVnode) {
  tWorkerFreeQueue(&tsVnodes.queryPool, pVnode->pQueryQ);
  pVnode->pQueryQ = NULL;
}

static int32_t dnodeAllocVnodeFetchQueue(SVnodeObj *pVnode) {
  pVnode->pFetchQ = tWorkerAllocQueue(&tsVnodes.fetchPool, pVnode, (FProcessItem)dnodeProcessVnodeFetchQueue);
  if (pVnode->pFetchQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeVnodeFetchQueue(SVnodeObj *pVnode) {
  tWorkerFreeQueue(&tsVnodes.fetchPool, pVnode->pFetchQ);
  pVnode->pFetchQ = NULL;
}

static int32_t dnodeInitVnodeReadWorker() {
  int32_t maxFetchThreads = 4;
  float   threadsForQuery = MAX(tsNumOfCores * tsRatioOfQueryCores, 1);

  SWorkerPool *pPool = &tsVnodes.queryPool;
  pPool->name = "vnode-query";
  pPool->min = (int32_t)threadsForQuery;
  pPool->max = pPool->min;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  pPool = &tsVnodes.fetchPool;
  pPool->name = "vnode-fetch";
  pPool->min = MIN(maxFetchThreads, tsNumOfCores);
  pPool->max = pPool->min;
  if (tWorkerInit(pPool) != 0) {
    TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  return 0;
}

static void dnodeCleanupVnodeReadWorker() {
  tWorkerCleanup(&tsVnodes.fetchPool);
  tWorkerCleanup(&tsVnodes.queryPool);
}

static int32_t dnodeAllocVnodeWriteQueue(SVnodeObj *pVnode) {
  pVnode->pWriteQ = tMWorkerAllocQueue(&tsVnodes.writePool, pVnode, (FProcessItems)dnodeProcessVnodeWriteQueue);
  if (pVnode->pWriteQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeVnodeWriteQueue(SVnodeObj *pVnode) {
  tMWorkerFreeQueue(&tsVnodes.writePool, pVnode->pWriteQ);
  pVnode->pWriteQ = NULL;
}

static int32_t dnodeAllocVnodeApplyQueue(SVnodeObj *pVnode) {
  pVnode->pApplyQ = tMWorkerAllocQueue(&tsVnodes.writePool, pVnode, (FProcessItems)dnodeProcessVnodeApplyQueue);
  if (pVnode->pApplyQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeVnodeApplyQueue(SVnodeObj *pVnode) {
  tMWorkerFreeQueue(&tsVnodes.writePool, pVnode->pApplyQ);
  pVnode->pApplyQ = NULL;
}

static int32_t dnodeInitVnodeWriteWorker() {
  SMWorkerPool *pPool = &tsVnodes.writePool;
  pPool->name = "vnode-write";
  pPool->max = tsNumOfCores;
  if (tMWorkerInit(pPool) != 0) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  return 0;
}

static void dnodeCleanupVnodeWriteWorker() { tMWorkerCleanup(&tsVnodes.writePool); }

static int32_t dnodeAllocVnodeSyncQueue(SVnodeObj *pVnode) {
  pVnode->pSyncQ = tMWorkerAllocQueue(&tsVnodes.writePool, pVnode, (FProcessItems)dnodeProcessVnodeSyncQueue);
  if (pVnode->pSyncQ == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }
  return 0;
}

static void dnodeFreeVnodeSyncQueue(SVnodeObj *pVnode) {
  tMWorkerFreeQueue(&tsVnodes.writePool, pVnode->pSyncQ);
  pVnode->pSyncQ = NULL;
}

static int32_t dnodeInitVnodeSyncWorker() {
  int32_t maxThreads = tsNumOfCores / 2;
  if (maxThreads < 1) maxThreads = 1;

  SMWorkerPool *pPool = &tsVnodes.writePool;
  pPool->name = "vnode-sync";
  pPool->max = maxThreads;
  if (tMWorkerInit(pPool) != 0) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  return 0;
}

static void dnodeCleanupVnodeSyncWorker() { tMWorkerCleanup(&tsVnodes.syncPool); }

static int32_t dnodeInitVnodeModule() {
  SVnodePara para;
  para.SendMsgToDnode = dnodeSendMsgToDnode;
  para.SendMsgToMnode = dnodeSendMsgToMnode;
  para.PutMsgIntoApplyQueue = dnodePutMsgIntoVnodeApplyQueue;

  return vnodeInit(para);
}

int32_t dnodeInitVnodes() {
  dInfo("dnode-vnodes start to init");

  SSteps *pSteps = taosStepInit(6, dnodeReportStartup);
  taosStepAdd(pSteps, "dnode-vnode-env", dnodeInitVnodeModule, vnodeCleanup);
  taosStepAdd(pSteps, "dnode-vnode-mgmt", dnodeInitVnodeMgmtWorker, dnodeCleanupVnodeMgmtWorker);
  taosStepAdd(pSteps, "dnode-vnode-read", dnodeInitVnodeReadWorker, dnodeCleanupVnodeReadWorker);
  taosStepAdd(pSteps, "dnode-vnode-write", dnodeInitVnodeWriteWorker, dnodeCleanupVnodeWriteWorker);
  taosStepAdd(pSteps, "dnode-vnode-sync", dnodeInitVnodeSyncWorker, dnodeCleanupVnodeSyncWorker);
  taosStepAdd(pSteps, "dnode-vnodes", dnodeOpenVnodes, dnodeCleanupVnodes);

  tsVnodes.pSteps = pSteps;
  return taosStepExec(pSteps);
}

void dnodeCleanupVnodes() {
  if (tsVnodes.pSteps != NULL) {
    dInfo("dnode-vnodes start to clean up");
    taosStepCleanup(tsVnodes.pSteps);
    tsVnodes.pSteps = NULL;
    dInfo("dnode-vnodes is cleaned up");
  }
}

void dnodeGetVnodeLoads(SVnodeLoads *pLoads) {
  pLoads->num = taosHashGetSize(tsVnodes.hash);

  int32_t v = 0;
  void *  pIter = taosHashIterate(tsVnodes.hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL) continue;

    SVnodeObj *pVnode = *ppVnode;
    if (pVnode == NULL) continue;

    SVnodeLoad *pLoad = &pLoads->data[v++];
    vnodeGetLoad(pVnode->pImpl, pLoad);
    pLoad->vgId = htonl(pLoad->vgId);
    pLoad->totalStorage = htobe64(pLoad->totalStorage);
    pLoad->compStorage = htobe64(pLoad->compStorage);
    pLoad->pointsWritten = htobe64(pLoad->pointsWritten);
    pLoad->tablesNum = htobe64(pLoad->tablesNum);

    pIter = taosHashIterate(tsVnodes.hash, pIter);
  }
}
