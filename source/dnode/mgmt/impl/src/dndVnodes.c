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
#include "dndMgmt.h"
#include "dndTransport.h"
#include "sync.h"

typedef struct {
  int32_t  vgId;
  int32_t  vgVersion;
  int8_t   dropped;
  uint64_t dbUid;
  char     db[TSDB_DB_FNAME_LEN];
  char     path[PATH_MAX + 20];
} SWrapperCfg;

typedef struct {
  int32_t     vgId;
  int32_t     refCount;
  int32_t     vgVersion;
  int8_t      dropped;
  int8_t      accessState;
  uint64_t    dbUid;
  char       *db;
  char       *path;
  SVnode     *pImpl;
  STaosQueue *pWriteQ;
  STaosQueue *pSyncQ;
  STaosQueue *pApplyQ;
  STaosQueue *pQueryQ;
  STaosQueue *pFetchQ;
} SVnodeObj;

typedef struct {
  int32_t      vnodeNum;
  int32_t      opened;
  int32_t      failed;
  int32_t      threadIndex;
  pthread_t    thread;
  SDnode      *pDnode;
  SWrapperCfg *pCfgs;
} SVnodeThread;

static int32_t dndAllocVnodeQueue(SDnode *pDnode, SVnodeObj *pVnode);
static void    dndFreeVnodeQueue(SDnode *pDnode, SVnodeObj *pVnode);

static void    dndProcessVnodeQueryQueue(SVnodeObj *pVnode, SRpcMsg *pMsg);
static void    dndProcessVnodeFetchQueue(SVnodeObj *pVnode, SRpcMsg *pMsg);
static void    dndProcessVnodeWriteQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs);
static void    dndProcessVnodeApplyQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs);
static void    dndProcessVnodeSyncQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs);
void           dndProcessVnodeQueryMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessVnodeFetchMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessVnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessVnodeSyncMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
static int32_t dndPutMsgIntoVnodeApplyQueue(SDnode *pDnode, int32_t vgId, SRpcMsg *pMsg);

static SVnodeObj  *dndAcquireVnode(SDnode *pDnode, int32_t vgId);
static void        dndReleaseVnode(SDnode *pDnode, SVnodeObj *pVnode);
static int32_t     dndOpenVnode(SDnode *pDnode, SWrapperCfg *pCfg, SVnode *pImpl);
static void        dndCloseVnode(SDnode *pDnode, SVnodeObj *pVnode);
static SVnodeObj **dndGetVnodesFromHash(SDnode *pDnode, int32_t *numOfVnodes);
static int32_t     dndGetVnodesFromFile(SDnode *pDnode, SWrapperCfg **ppCfgs, int32_t *numOfVnodes);
static int32_t     dndWriteVnodesToFile(SDnode *pDnode);

static int32_t dndOpenVnodes(SDnode *pDnode);
static void    dndCloseVnodes(SDnode *pDnode);

static SVnodeObj *dndAcquireVnode(SDnode *pDnode, int32_t vgId) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  SVnodeObj   *pVnode = NULL;
  int32_t      refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  taosHashGetDup(pMgmt->hash, &vgId, sizeof(int32_t), (void *)&pVnode);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
  } else {
    refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pVnode != NULL) {
    dTrace("vgId:%d, acquire vnode, refCount:%d", pVnode->vgId, refCount);
  }

  return pVnode;
}

static void dndReleaseVnode(SDnode *pDnode, SVnodeObj *pVnode) {
  if (pVnode == NULL) return;

  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  taosRLockLatch(&pMgmt->latch);
  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  taosRUnLockLatch(&pMgmt->latch);
  dTrace("vgId:%d, release vnode, refCount:%d", pVnode->vgId, refCount);
}

static int32_t dndOpenVnode(SDnode *pDnode, SWrapperCfg *pCfg, SVnode *pImpl) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  SVnodeObj   *pVnode = calloc(1, sizeof(SVnodeObj));
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pVnode->vgId = pCfg->vgId;
  pVnode->refCount = 0;
  pVnode->dropped = 0;
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  pVnode->pImpl = pImpl;
  pVnode->vgVersion = pCfg->vgVersion;
  pVnode->dbUid = pCfg->dbUid;
  pVnode->db = tstrdup(pCfg->db);
  pVnode->path = tstrdup(pCfg->path);

  if (pVnode->path == NULL || pVnode->db == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dndAllocVnodeQueue(pDnode, pVnode) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  int32_t code = taosHashPut(pMgmt->hash, &pVnode->vgId, sizeof(int32_t), &pVnode, sizeof(SVnodeObj *));
  taosWUnLockLatch(&pMgmt->latch);

  if (code != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return code;
}

static void dndCloseVnode(SDnode *pDnode, SVnodeObj *pVnode) {
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

  dndFreeVnodeQueue(pDnode, pVnode);
  vnodeClose(pVnode->pImpl);
  pVnode->pImpl = NULL;

  dDebug("vgId:%d, vnode is closed", pVnode->vgId);

  if (pVnode->dropped) {
    dDebug("vgId:%d, vnode is destroyed for dropped:%d", pVnode->vgId, pVnode->dropped);
    vnodeDestroy(pVnode->path);
  }

  free(pVnode->path);
  free(pVnode->db);
  free(pVnode);
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
    if (pVnode && num < size) {
      int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
      dTrace("vgId:%d, acquire vnode, refCount:%d", pVnode->vgId, refCount);
      pVnodes[num] = (*ppVnode);
      num++;
      pIter = taosHashIterate(pMgmt->hash, pIter);
    } else {
      taosHashCancelIterate(pMgmt->hash, pIter);
    }
  }

  taosRUnLockLatch(&pMgmt->latch);
  *numOfVnodes = num;

  return pVnodes;
}

static int32_t dndGetVnodesFromFile(SDnode *pDnode, SWrapperCfg **ppCfgs, int32_t *numOfVnodes) {
  int32_t      code = TSDB_CODE_DND_VNODE_READ_FILE_ERROR;
  int32_t      len = 0;
  int32_t      maxLen = 30000;
  char        *content = calloc(1, maxLen + 1);
  cJSON       *root = NULL;
  FILE        *fp = NULL;
  char         file[PATH_MAX + 20] = {0};
  SWrapperCfg *pCfgs = NULL;

  snprintf(file, PATH_MAX + 20, "%s/vnodes.json", pDnode->dir.vnodes);

  // fp = fopen(file, "r");
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    dDebug("file %s not exist", file);
    code = 0;
    goto PRASE_VNODE_OVER;
  }

  len = (int32_t)taosReadFile(pFile, content, maxLen);
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
  if (vnodesNum > 0) {
    pCfgs = calloc(vnodesNum, sizeof(SWrapperCfg));
    if (pCfgs == NULL) {
      dError("failed to read %s since out of memory", file);
      goto PRASE_VNODE_OVER;
    }

    for (int32_t i = 0; i < vnodesNum; ++i) {
      cJSON       *vnode = cJSON_GetArrayItem(vnodes, i);
      SWrapperCfg *pCfg = &pCfgs[i];

      cJSON *vgId = cJSON_GetObjectItem(vnode, "vgId");
      if (!vgId || vgId->type != cJSON_Number) {
        dError("failed to read %s since vgId not found", file);
        goto PRASE_VNODE_OVER;
      }
      pCfg->vgId = vgId->valueint;
      snprintf(pCfg->path, sizeof(pCfg->path), "%s/vnode%d", pDnode->dir.vnodes, pCfg->vgId);

      cJSON *dropped = cJSON_GetObjectItem(vnode, "dropped");
      if (!dropped || dropped->type != cJSON_Number) {
        dError("failed to read %s since dropped not found", file);
        goto PRASE_VNODE_OVER;
      }
      pCfg->dropped = dropped->valueint;

      cJSON *vgVersion = cJSON_GetObjectItem(vnode, "vgVersion");
      if (!vgVersion || vgVersion->type != cJSON_Number) {
        dError("failed to read %s since vgVersion not found", file);
        goto PRASE_VNODE_OVER;
      }
      pCfg->vgVersion = vgVersion->valueint;

      cJSON *dbUid = cJSON_GetObjectItem(vnode, "dbUid");
      if (!dbUid || dbUid->type != cJSON_String) {
        dError("failed to read %s since dbUid not found", file);
        goto PRASE_VNODE_OVER;
      }
      pCfg->dbUid = atoll(dbUid->valuestring);

      cJSON *db = cJSON_GetObjectItem(vnode, "db");
      if (!db || db->type != cJSON_String) {
        dError("failed to read %s since db not found", file);
        goto PRASE_VNODE_OVER;
      }
      tstrncpy(pCfg->db, db->valuestring, TSDB_DB_FNAME_LEN);
    }

    *ppCfgs = pCfgs;
  }

  *numOfVnodes = vnodesNum;
  code = 0;
  dInfo("succcessed to read file %s", file);

PRASE_VNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  return code;
}

static int32_t dndWriteVnodesToFile(SDnode *pDnode) {
  char file[PATH_MAX + 20] = {0};
  char realfile[PATH_MAX + 20] = {0};
  snprintf(file, PATH_MAX + 20, "%s/vnodes.json.bak", pDnode->dir.vnodes);
  snprintf(realfile, PATH_MAX + 20, "%s/vnodes.json", pDnode->dir.vnodes);

  // FILE *fp = fopen(file, "w");
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }
  int32_t     numOfVnodes = 0;
  SVnodeObj **pVnodes = dndGetVnodesFromHash(pDnode, &numOfVnodes);

  int32_t len = 0;
  int32_t maxLen = 65536;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"vnodes\": [\n");
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    SVnodeObj *pVnode = pVnodes[i];
    len += snprintf(content + len, maxLen - len, "    {\n");
    len += snprintf(content + len, maxLen - len, "      \"vgId\": %d,\n", pVnode->vgId);
    len += snprintf(content + len, maxLen - len, "      \"dropped\": %d,\n", pVnode->dropped);
    len += snprintf(content + len, maxLen - len, "      \"vgVersion\": %d,\n", pVnode->vgVersion);
    len += snprintf(content + len, maxLen - len, "      \"dbUid\": \"%" PRIu64 "\",\n", pVnode->dbUid);
    len += snprintf(content + len, maxLen - len, "      \"db\": \"%s\"\n", pVnode->db);
    if (i < numOfVnodes - 1) {
      len += snprintf(content + len, maxLen - len, "    },\n");
    } else {
      len += snprintf(content + len, maxLen - len, "    }\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "  ]\n");
  len += snprintf(content + len, maxLen - len, "}\n");

  taosWriteFile(pFile, content, len);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  free(content);
  terrno = 0;

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    SVnodeObj *pVnode = pVnodes[i];
    dndReleaseVnode(pDnode, pVnode);
  }

  if (pVnodes != NULL) {
    free(pVnodes);
  }

  dDebug("successed to write %s", realfile);
  return taosRenameFile(file, realfile);
}

static void *dnodeOpenVnodeFunc(void *param) {
  SVnodeThread *pThread = param;
  SDnode       *pDnode = pThread->pDnode;
  SVnodesMgmt  *pMgmt = &pDnode->vmgmt;

  dDebug("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("open-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SWrapperCfg *pCfg = &pThread->pCfgs[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been opened", pCfg->vgId,
             pMgmt->stat.openVnodes, pMgmt->stat.totalVnodes);
    dndReportStartup(pDnode, "open-vnodes", stepDesc);

    SVnodeCfg cfg = {.pDnode = pDnode, .pTfs = pDnode->pTfs, .vgId = pCfg->vgId, .dbId = pCfg->dbUid};
    SVnode   *pImpl = vnodeOpen(pCfg->path, &cfg);
    if (pImpl == NULL) {
      dError("vgId:%d, failed to open vnode by thread:%d", pCfg->vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      dndOpenVnode(pDnode, pCfg, pImpl);
      dDebug("vgId:%d, is opened by thread:%d", pCfg->vgId, pThread->threadIndex);
      pThread->opened++;
    }

    atomic_add_fetch_32(&pMgmt->stat.openVnodes, 1);
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
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SWrapperCfg *pCfgs = NULL;
  int32_t      numOfVnodes = 0;
  if (dndGetVnodesFromFile(pDnode, &pCfgs, &numOfVnodes) != 0) {
    dInfo("failed to get vnode list from disk since %s", terrstr());
    return -1;
  }

  pMgmt->stat.totalVnodes = numOfVnodes;

  int32_t threadNum = tsNumOfCores;
#if 1
  threadNum = 1;
#endif

  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SVnodeThread *threads = calloc(threadNum, sizeof(SVnodeThread));
  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].pDnode = pDnode;
    threads[t].pCfgs = calloc(vnodesPerThread, sizeof(SWrapperCfg));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t       t = v % threadNum;
    SVnodeThread *pThread = &threads[t];
    pThread->pCfgs[pThread->vnodeNum++] = pCfgs[v];
  }

  dInfo("start %d threads to open %d vnodes", threadNum, numOfVnodes);

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&pThread->thread, &thAttr, dnodeOpenVnodeFunc, pThread) != 0) {
      dError("thread:%d, failed to create thread to open vnode, reason:%s", pThread->threadIndex, strerror(errno));
    }

    pthread_attr_destroy(&thAttr);
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      pthread_join(pThread->thread, NULL);
    }
    free(pThread->pCfgs);
  }
  free(threads);
  free(pCfgs);

  if (pMgmt->stat.openVnodes != pMgmt->stat.totalVnodes) {
    dError("there are total vnodes:%d, opened:%d", pMgmt->stat.totalVnodes, pMgmt->stat.openVnodes);
    return -1;
  } else {
    dInfo("total vnodes:%d open successfully", pMgmt->stat.totalVnodes);
    return 0;
  }
}

static void dndCloseVnodes(SDnode *pDnode) {
  dInfo("start to close all vnodes");
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;

  int32_t     numOfVnodes = 0;
  SVnodeObj **pVnodes = dndGetVnodesFromHash(pDnode, &numOfVnodes);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    dndCloseVnode(pDnode, pVnodes[i]);
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

static void dndGenerateVnodeCfg(SCreateVnodeReq *pCreate, SVnodeCfg *pCfg) {
  pCfg->vgId = pCreate->vgId;
  pCfg->wsize = pCreate->cacheBlockSize;
  pCfg->ssize = pCreate->cacheBlockSize;
  pCfg->lsize = pCreate->cacheBlockSize;
  pCfg->isHeapAllocator = true;
  pCfg->ttl = 4;
  pCfg->keep = pCreate->daysToKeep0;
  pCfg->streamMode = pCreate->streamMode;
  pCfg->isWeak = true;
  pCfg->tsdbCfg.keep = pCreate->daysToKeep0;
  pCfg->tsdbCfg.keep1 = pCreate->daysToKeep2;
  pCfg->tsdbCfg.keep2 = pCreate->daysToKeep0;
  pCfg->tsdbCfg.lruCacheSize = pCreate->cacheBlockSize;
  pCfg->metaCfg.lruSize = pCreate->cacheBlockSize;
  pCfg->walCfg.fsyncPeriod = pCreate->fsyncPeriod;
  pCfg->walCfg.level = pCreate->walLevel;
  pCfg->walCfg.retentionPeriod = 10;
  pCfg->walCfg.retentionSize = 128;
  pCfg->walCfg.rollPeriod = 128;
  pCfg->walCfg.segSize = 128;
  pCfg->walCfg.vgId = pCreate->vgId;
  pCfg->hashBegin = pCreate->hashBegin;
  pCfg->hashEnd = pCreate->hashEnd;
  pCfg->hashMethod = pCreate->hashMethod;
}

static void dndGenerateWrapperCfg(SDnode *pDnode, SCreateVnodeReq *pCreate, SWrapperCfg *pCfg) {
  memcpy(pCfg->db, pCreate->db, TSDB_DB_FNAME_LEN);
  pCfg->dbUid = pCreate->dbUid;
  pCfg->dropped = 0;
  snprintf(pCfg->path, sizeof(pCfg->path), "%s/vnode%d", pDnode->dir.vnodes, pCreate->vgId);
  pCfg->vgId = pCreate->vgId;
  pCfg->vgVersion = pCreate->vgVersion;
}

int32_t dndProcessCreateVnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
  SCreateVnodeReq createReq = {0};
  if (tDeserializeSCreateVnodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dDebug("vgId:%d, create vnode req is received", createReq.vgId);

  SVnodeCfg vnodeCfg = {0};
  dndGenerateVnodeCfg(&createReq, &vnodeCfg);

  SWrapperCfg wrapperCfg = {0};
  dndGenerateWrapperCfg(pDnode, &createReq, &wrapperCfg);

  if (createReq.dnodeId != dndGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_VNODE_INVALID_OPTION;
    dDebug("vgId:%d, failed to create vnode since %s", createReq.vgId, terrstr());
    return -1;
  }

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, createReq.vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, already exist", createReq.vgId);
    dndReleaseVnode(pDnode, pVnode);
    terrno = TSDB_CODE_DND_VNODE_ALREADY_DEPLOYED;
    return -1;
  }

  vnodeCfg.pDnode = pDnode;
  vnodeCfg.pTfs = pDnode->pTfs;
  vnodeCfg.dbId = wrapperCfg.dbUid;
  SVnode *pImpl = vnodeOpen(wrapperCfg.path, &vnodeCfg);
  if (pImpl == NULL) {
    dError("vgId:%d, failed to create vnode since %s", createReq.vgId, terrstr());
    return -1;
  }

  int32_t code = dndOpenVnode(pDnode, &wrapperCfg, pImpl);
  if (code != 0) {
    dError("vgId:%d, failed to open vnode since %s", createReq.vgId, terrstr());
    vnodeClose(pImpl);
    vnodeDestroy(wrapperCfg.path);
    terrno = code;
    return code;
  }

  code = dndWriteVnodesToFile(pDnode);
  if (code != 0) {
    vnodeClose(pImpl);
    vnodeDestroy(wrapperCfg.path);
    terrno = code;
    return code;
  }

  return 0;
}

int32_t dndProcessAlterVnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
  SAlterVnodeReq alterReq = {0};
  if (tDeserializeSCreateVnodeReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dDebug("vgId:%d, alter vnode req is received", alterReq.vgId);

  SVnodeCfg vnodeCfg = {0};
  dndGenerateVnodeCfg(&alterReq, &vnodeCfg);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, alterReq.vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to alter vnode since %s", alterReq.vgId, terrstr());
    return -1;
  }

  if (alterReq.vgVersion == pVnode->vgVersion) {
    dndReleaseVnode(pDnode, pVnode);
    dDebug("vgId:%d, no need to alter vnode cfg for version unchanged ", alterReq.vgId);
    return 0;
  }

  if (vnodeAlter(pVnode->pImpl, &vnodeCfg) != 0) {
    dError("vgId:%d, failed to alter vnode since %s", alterReq.vgId, terrstr());
    dndReleaseVnode(pDnode, pVnode);
    return -1;
  }

  int32_t oldVersion = pVnode->vgVersion;
  pVnode->vgVersion = alterReq.vgVersion;
  int32_t code = dndWriteVnodesToFile(pDnode);
  if (code != 0) {
    pVnode->vgVersion = oldVersion;
  }

  dndReleaseVnode(pDnode, pVnode);
  return code;
}

int32_t dndProcessDropVnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
  SDropVnodeReq dropReq = {0};
  if (tDeserializeSDropVnodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t vgId = dropReq.vgId;
  dDebug("vgId:%d, drop vnode req is received", vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to drop since %s", vgId, terrstr());
    terrno = TSDB_CODE_DND_VNODE_NOT_DEPLOYED;
    return -1;
  }

  pVnode->dropped = 1;
  if (dndWriteVnodesToFile(pDnode) != 0) {
    pVnode->dropped = 0;
    dndReleaseVnode(pDnode, pVnode);
    return -1;
  }

  dndCloseVnode(pDnode, pVnode);
  dndWriteVnodesToFile(pDnode);

  return 0;
}

int32_t dndProcessSyncVnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
  SSyncVnodeReq syncReq = {0};
  tDeserializeSDropVnodeReq(pReq->pCont, pReq->contLen, &syncReq);

  int32_t vgId = syncReq.vgId;
  dDebug("vgId:%d, sync vnode req is received", vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to sync since %s", vgId, terrstr());
    return -1;
  }

  if (vnodeSync(pVnode->pImpl) != 0) {
    dError("vgId:%d, failed to sync vnode since %s", vgId, terrstr());
    dndReleaseVnode(pDnode, pVnode);
    return -1;
  }

  dndReleaseVnode(pDnode, pVnode);
  return 0;
}

int32_t dndProcessCompactVnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
  SCompactVnodeReq compatcReq = {0};
  tDeserializeSDropVnodeReq(pReq->pCont, pReq->contLen, &compatcReq);

  int32_t vgId = compatcReq.vgId;
  dDebug("vgId:%d, compact vnode req is received", vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) {
    dDebug("vgId:%d, failed to compact since %s", vgId, terrstr());
    return -1;
  }

  if (vnodeCompact(pVnode->pImpl) != 0) {
    dError("vgId:%d, failed to compact vnode since %s", vgId, terrstr());
    dndReleaseVnode(pDnode, pVnode);
    return -1;
  }

  dndReleaseVnode(pDnode, pVnode);
  return 0;
}

static void dndProcessVnodeQueryQueue(SVnodeObj *pVnode, SRpcMsg *pMsg) { vnodeProcessQueryMsg(pVnode->pImpl, pMsg); }

static void dndProcessVnodeFetchQueue(SVnodeObj *pVnode, SRpcMsg *pMsg) { vnodeProcessFetchMsg(pVnode->pImpl, pMsg); }

static void dndProcessVnodeWriteQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SArray *pArray = taosArrayInit(numOfMsgs, sizeof(SRpcMsg *));

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    void *ptr = taosArrayPush(pArray, &pMsg);
    assert(ptr != NULL);
  }

  vnodeProcessWMsgs(pVnode->pImpl, pArray);

  for (size_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg *pRsp = NULL;
    SRpcMsg *pMsg = *(SRpcMsg **)taosArrayGet(pArray, i);
    int32_t  code = vnodeApplyWMsg(pVnode->pImpl, pMsg, &pRsp);
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

  for (size_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg *pMsg = *(SRpcMsg **)taosArrayGet(pArray, i);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }

  taosArrayDestroy(pArray);
}

static void dndProcessVnodeApplyQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SRpcMsg *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);

    // todo
    SRpcMsg *pRsp = NULL;
    (void)vnodeApplyWMsg(pVnode->pImpl, pMsg, &pRsp);
  }
}

static void dndProcessVnodeSyncQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SRpcMsg *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);

    // todo
    SRpcMsg *pRsp = NULL;
    (void)vnodeProcessSyncReq(pVnode->pImpl, pMsg, &pRsp);
  }
}

static int32_t dndWriteRpcMsgToVnodeQueue(STaosQueue *pQueue, SRpcMsg *pRpcMsg, bool sendRsp) {
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

  if (code != TSDB_CODE_SUCCESS && sendRsp) {
    if (pRpcMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pRpcMsg->handle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pRpcMsg->pCont);
  }

  return code;
}

static SVnodeObj *dndAcquireVnodeFromMsg(SDnode *pDnode, SRpcMsg *pMsg) {
  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, pHead->vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to acquire vnode while process req", pHead->vgId);
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .code = TSDB_CODE_VND_INVALID_VGROUP_ID};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
  }

  return pVnode;
}

void dndProcessVnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dndAcquireVnodeFromMsg(pDnode, pMsg);
  if (pVnode != NULL) {
    (void)dndWriteRpcMsgToVnodeQueue(pVnode->pWriteQ, pMsg, true);
    dndReleaseVnode(pDnode, pVnode);
  }
}

void dndProcessVnodeSyncMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dndAcquireVnodeFromMsg(pDnode, pMsg);
  if (pVnode != NULL) {
    (void)dndWriteRpcMsgToVnodeQueue(pVnode->pSyncQ, pMsg, true);
    dndReleaseVnode(pDnode, pVnode);
  }
}

void dndProcessVnodeQueryMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dndAcquireVnodeFromMsg(pDnode, pMsg);
  if (pVnode != NULL) {
    (void)dndWriteRpcMsgToVnodeQueue(pVnode->pQueryQ, pMsg, true);
    dndReleaseVnode(pDnode, pVnode);
  }
}

void dndProcessVnodeFetchMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SVnodeObj *pVnode = dndAcquireVnodeFromMsg(pDnode, pMsg);
  if (pVnode != NULL) {
    (void)dndWriteRpcMsgToVnodeQueue(pVnode->pFetchQ, pMsg, true);
    dndReleaseVnode(pDnode, pVnode);
  }
}

int32_t dndPutReqToVQueryQ(SDnode *pDnode, SRpcMsg *pMsg) {
  SMsgHead *pHead = pMsg->pCont;
  // pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = dndAcquireVnode(pDnode, pHead->vgId);
  if (pVnode == NULL) return -1;

  int32_t code = dndWriteRpcMsgToVnodeQueue(pVnode->pQueryQ, pMsg, false);
  dndReleaseVnode(pDnode, pVnode);
  return code;
}

static int32_t dndPutMsgIntoVnodeApplyQueue(SDnode *pDnode, int32_t vgId, SRpcMsg *pMsg) {
  SVnodeObj *pVnode = dndAcquireVnode(pDnode, vgId);
  if (pVnode == NULL) return -1;

  int32_t code = taosWriteQitem(pVnode->pApplyQ, pMsg);
  dndReleaseVnode(pDnode, pVnode);
  return code;
}

static int32_t dndInitVnodeWorkers(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;

  int32_t maxFetchThreads = 4;
  int32_t minFetchThreads = TMIN(maxFetchThreads, tsNumOfCores);
  int32_t minQueryThreads = TMAX((int32_t)(tsNumOfCores * tsRatioOfQueryCores), 1);
  int32_t maxQueryThreads = minQueryThreads;
  int32_t maxWriteThreads = TMAX(tsNumOfCores, 1);
  int32_t maxSyncThreads = TMAX(tsNumOfCores / 2, 1);

  SQWorkerPool *pQPool = &pMgmt->queryPool;
  pQPool->name = "vnode-query";
  pQPool->min = minQueryThreads;
  pQPool->max = maxQueryThreads;
  if (tQWorkerInit(pQPool) != 0) return -1;

  SFWorkerPool *pFPool = &pMgmt->fetchPool;
  pFPool->name = "vnode-fetch";
  pFPool->min = minFetchThreads;
  pFPool->max = maxFetchThreads;
  if (tFWorkerInit(pFPool) != 0) return -1;

  SWWorkerPool *pWPool = &pMgmt->writePool;
  pWPool->name = "vnode-write";
  pWPool->max = maxWriteThreads;
  if (tWWorkerInit(pWPool) != 0) return -1;

  pWPool = &pMgmt->syncPool;
  pWPool->name = "vnode-sync";
  pWPool->max = maxSyncThreads;
  if (tWWorkerInit(pWPool) != 0) return -1;

  dDebug("vnode workers is initialized");
  return 0;
}

static void dndCleanupVnodeWorkers(SDnode *pDnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tFWorkerCleanup(&pMgmt->fetchPool);
  tQWorkerCleanup(&pMgmt->queryPool);
  tWWorkerCleanup(&pMgmt->writePool);
  tWWorkerCleanup(&pMgmt->syncPool);
  dDebug("vnode workers is closed");
}

static int32_t dndAllocVnodeQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;

  pVnode->pWriteQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode, (FItems)dndProcessVnodeWriteQueue);
  pVnode->pApplyQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode, (FItems)dndProcessVnodeApplyQueue);
  pVnode->pSyncQ = tWWorkerAllocQueue(&pMgmt->syncPool, pVnode, (FItems)dndProcessVnodeSyncQueue);
  pVnode->pFetchQ = tFWorkerAllocQueue(&pMgmt->fetchPool, pVnode, (FItem)dndProcessVnodeFetchQueue);
  pVnode->pQueryQ = tQWorkerAllocQueue(&pMgmt->queryPool, pVnode, (FItem)dndProcessVnodeQueryQueue);

  if (pVnode->pApplyQ == NULL || pVnode->pWriteQ == NULL || pVnode->pSyncQ == NULL || pVnode->pFetchQ == NULL ||
      pVnode->pQueryQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeVnodeQueue(SDnode *pDnode, SVnodeObj *pVnode) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  tQWorkerFreeQueue(&pMgmt->queryPool, pVnode->pQueryQ);
  tFWorkerFreeQueue(&pMgmt->fetchPool, pVnode->pFetchQ);
  tWWorkerFreeQueue(&pMgmt->writePool, pVnode->pWriteQ);
  tWWorkerFreeQueue(&pMgmt->writePool, pVnode->pApplyQ);
  tWWorkerFreeQueue(&pMgmt->syncPool, pVnode->pSyncQ);
  pVnode->pWriteQ = NULL;
  pVnode->pApplyQ = NULL;
  pVnode->pSyncQ = NULL;
  pVnode->pFetchQ = NULL;
  pVnode->pQueryQ = NULL;
}

int32_t dndInitVnodes(SDnode *pDnode) {
  dInfo("dnode-vnodes start to init");

  if (dndInitVnodeWorkers(pDnode) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    dError("failed to init vnode workers since %s", terrstr());
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
  dndCleanupVnodeWorkers(pDnode);
  dInfo("dnode-vnodes is cleaned up");
}

void dndGetVnodeLoads(SDnode *pDnode, SArray *pLoads) {
  SVnodesMgmt *pMgmt = &pDnode->vmgmt;
  SVnodesStat *pStat = &pMgmt->stat;
  int32_t      totalVnodes = 0;
  int32_t      masterNum = 0;
  int64_t      numOfSelectReqs = 0;
  int64_t      numOfInsertReqs = 0;
  int64_t      numOfInsertSuccessReqs = 0;
  int64_t      numOfBatchInsertReqs = 0;
  int64_t      numOfBatchInsertSuccessReqs = 0;

  taosRLockLatch(&pMgmt->latch);

  void *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj *pVnode = *ppVnode;
    SVnodeLoad vload = {0};
    vnodeGetLoad(pVnode->pImpl, &vload);
    taosArrayPush(pLoads, &vload);

    numOfSelectReqs += vload.numOfSelectReqs;
    numOfInsertReqs += vload.numOfInsertReqs;
    numOfInsertSuccessReqs += vload.numOfInsertSuccessReqs;
    numOfBatchInsertReqs += vload.numOfBatchInsertReqs;
    numOfBatchInsertSuccessReqs += vload.numOfBatchInsertSuccessReqs;
    totalVnodes++;
    if (vload.role == TAOS_SYNC_STATE_LEADER) masterNum++;

    pIter = taosHashIterate(pMgmt->hash, pIter);
  }

  taosRUnLockLatch(&pMgmt->latch);

  pStat->totalVnodes = totalVnodes;
  pStat->masterNum = masterNum;
  pStat->numOfSelectReqs = numOfSelectReqs;
  pStat->numOfInsertReqs = numOfInsertReqs;
  pStat->numOfInsertSuccessReqs = numOfInsertSuccessReqs;
  pStat->numOfBatchInsertReqs = numOfBatchInsertReqs;
  pStat->numOfBatchInsertSuccessReqs = numOfBatchInsertSuccessReqs;
}
