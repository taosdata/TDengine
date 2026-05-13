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

/*
 * mndSnapSend.c — mnode side of the snapshot-send progress system tables:
 *   ins_snap_send_vnodes   (one row per vnode currently sending a snapshot)
 *   ins_snap_send_filesets (one row per fileset of an active snapshot send)
 *
 * Design overview:
 *   1. mndSnapSendPullup() is called every tsSnapSendPullupInterval seconds from
 *      mndDoTimerPullupTask(). It scans all SVgObj in SDB, finds those whose
 *      snapRestoring==1 (set by the dnode heartbeat), and sends
 *      TDMT_DND_QUERY_SNAP_SEND_PROGRESS to the leader dnode.
 *   2. mndProcessDnodeSnapSendProgressRsp() receives the response and updates
 *      the in-memory hash pSnapSendHash (key=vgId, value=SSnapSendVnodeInfo).
 *   3. mndRetrieveSnapSendVnodes / mndRetrieveSnapSendFilesets iterate the hash
 *      and fill the result block for SQL queries.
 *   4. When snapRestoring turns 0, the entry is removed from the hash on the
 *      next pullup cycle.
 */

#include "mndDnode.h"
#include "mndShow.h"
#include "mndSnapSend.h"
#include "mndVgroup.h"
#include "systable.h"
#include "tmisce.h"
#include "tmsgcb.h"

/* transferType constant — matches SNAP_DATA_RAW in vnodeInt.h */
#define SNAP_TRANSFER_TYPE_RAW 14

/* ====================================================================
 * Module-level state (singleton, protected by snapSendMutex)
 * ==================================================================== */

typedef struct {
  SHashObj      *pHash;   /* key: int32_t vgId → value: SSnapSendVnodeInfo (deep copy) */
  TdThreadMutex  mutex;
} SSnapSendMgmt;

static SSnapSendMgmt gSnapSendMgmt = {0};

/* ====================================================================
 * Helper: elapsed string  "HH:MM:SS"
 * ==================================================================== */
static void snapSendFmtElapsed(int64_t startTimeMs, char *buf, int32_t bufLen) {
  if (startTimeMs <= 0) {
    tsnprintf(buf, bufLen, "0:00:00");
    return;
  }
  int64_t elapsedSec = (taosGetTimestampMs() - startTimeMs) / 1000;
  if (elapsedSec < 0) elapsedSec = 0;
  int64_t h = elapsedSec / 3600;
  int64_t m = (elapsedSec % 3600) / 60;
  int64_t s = elapsedSec % 60;
  tsnprintf(buf, bufLen, "%" PRId64 ":%02" PRId64 ":%02" PRId64, h, m, s);
}

/* ====================================================================
 * Helper: free a deep-copied SSnapSendVnodeInfo
 * ==================================================================== */
static void snapSendFreeVnodeInfo(SSnapSendVnodeInfo *pInfo) {
  if (pInfo) {
    taosMemoryFree(pInfo->pFileSetInfos);
    pInfo->pFileSetInfos = NULL;
  }
}

/* ====================================================================
 * RSP handler: TDMT_DND_QUERY_SNAP_SEND_PROGRESS_RSP
 * ==================================================================== */
static int32_t mndProcessDnodeSnapSendProgressRsp(SRpcMsg *pReq) {
  int32_t                       code = 0;
  SDnodeQuerySnapSendProgressRsp rsp = {0};

  if (pReq->code != 0) {
    mDebug("snap-send-progress rsp from dnode with error: %s", tstrerror(pReq->code));
    TAOS_RETURN(0);  // non-fatal: ignore, will retry next cycle
  }

  code = tDeserializeSDnodeQuerySnapSendProgressRsp(pReq->pCont, pReq->contLen, &rsp);
  if (code != 0) {
    mError("failed to deserialize snap-send-progress rsp, code:%s", tstrerror(code));
    TAOS_RETURN(code);
  }

  (void)taosThreadMutexLock(&gSnapSendMgmt.mutex);

  for (int32_t i = 0; i < rsp.numOfVnodes; i++) {
    SSnapSendVnodeInfo *pSrc = &rsp.pVnodeInfos[i];

    // Deep-copy pFileSetInfos
    SSnapSendFileSetInfo *pFsCopy = NULL;
    if (pSrc->fileSetCount > 0 && pSrc->pFileSetInfos != NULL) {
      pFsCopy = taosMemoryMalloc(pSrc->fileSetCount * sizeof(SSnapSendFileSetInfo));
      if (pFsCopy == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      memcpy(pFsCopy, pSrc->pFileSetInfos, pSrc->fileSetCount * sizeof(SSnapSendFileSetInfo));
    }

    SSnapSendVnodeInfo copy = *pSrc;
    copy.pFileSetInfos = pFsCopy;

    // Save old pFileSetInfos pointer so we can free it AFTER a successful put.
    // Do NOT call snapSendFreeVnodeInfo before taosHashPut: if the put fails,
    // it would leave an entry with fileSetCount>0 but pFileSetInfos=NULL in the
    // hash, causing a NULL-dereference in mndRetrieveSnapSendFilesets.
    SSnapSendVnodeInfo   *pOld = taosHashGet(gSnapSendMgmt.pHash, &pSrc->vgId, sizeof(int32_t));
    SSnapSendFileSetInfo *pOldFs = (pOld != NULL) ? pOld->pFileSetInfos : NULL;

    // Upsert — only free old memory after a successful put
    if (taosHashPut(gSnapSendMgmt.pHash, &pSrc->vgId, sizeof(int32_t), &copy, sizeof(copy)) != 0) {
      taosMemoryFree(pFsCopy);
      code = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }
    taosMemoryFree(pOldFs);
  }

  (void)taosThreadMutexUnlock(&gSnapSendMgmt.mutex);

  tFreeSDnodeQuerySnapSendProgressRsp(&rsp);
  TAOS_RETURN(code);
}

/* ====================================================================
 * Pullup: scan SDB for active snapshot senders, query each leader dnode
 * ==================================================================== */
void mndSnapSendPullup(SMnode *pMnode) {
  SSdb  *pSdb = pMnode->pSdb;
  void  *pIter = NULL;

  mDebug("snap-send-progress pullup started");

  // Build a set of vgIds that are still snapRestoring
  SHashObj *pActiveVgIds =
      taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (pActiveVgIds == NULL) return;

  // Track which dnodes need a query (key=dnodeId of leader)
  // We send one request per leader-dnode that has >=1 snapshotSending vnode.
  SHashObj *pDnodesToQuery =
      taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (pDnodesToQuery == NULL) {
    taosHashCleanup(pActiveVgIds);
    return;
  }

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->snapRestoring) {
      mDebug("snap-send-progress pullup: found snapRestoring vgroup vgId:%d", pVgroup->vgId);

      if (taosHashPut(pActiveVgIds, &pVgroup->vgId, sizeof(int32_t), &pVgroup->vgId, sizeof(int32_t)) != 0) {
        // Must skip: without this entry in pActiveVgIds the cleanup pass would
        // incorrectly treat the vgroup as stale and evict its progress cache entry.
        mError("snap-send-progress pullup: failed to track active vgId:%d, skipping", pVgroup->vgId);
        sdbRelease(pSdb, pVgroup);
        continue;
      }

      // Find the current leader dnode for this vgroup
      for (int8_t r = 0; r < pVgroup->replica; r++) {
        if (pVgroup->vnodeGid[r].syncState == TAOS_SYNC_STATE_LEADER) {
          int32_t leaderId = pVgroup->vnodeGid[r].dnodeId;
          if (taosHashPut(pDnodesToQuery, &leaderId, sizeof(int32_t), &leaderId, sizeof(int32_t)) != 0) {
            mError("snap-send-progress pullup: failed to track dnode:%d", leaderId);
          }
          break;
        }
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  // Remove stale hash entries (vgIds that are no longer snapRestoring).
  // Collect keys first — modifying the hash during taosHashIterate corrupts the walk.
  SArray *pStaleVgIds = taosArrayInit(8, sizeof(int32_t));

  (void)taosThreadMutexLock(&gSnapSendMgmt.mutex);

  if (pStaleVgIds != NULL) {
    void *pHashIter = taosHashIterate(gSnapSendMgmt.pHash, NULL);
    while (pHashIter != NULL) {
      SSnapSendVnodeInfo *pInfo = (SSnapSendVnodeInfo *)pHashIter;
      if (taosHashGet(pActiveVgIds, &pInfo->vgId, sizeof(int32_t)) == NULL) {
        if (taosArrayPush(pStaleVgIds, &pInfo->vgId) == NULL) {
          mError("snap-send-progress pullup: failed to collect stale vgId:%d", pInfo->vgId);
        }
      }
      pHashIter = taosHashIterate(gSnapSendMgmt.pHash, pHashIter);
    }

    for (int32_t k = 0; k < (int32_t)taosArrayGetSize(pStaleVgIds); k++) {
      int32_t            *pVgId = taosArrayGet(pStaleVgIds, k);
      SSnapSendVnodeInfo *pOld  = taosHashGet(gSnapSendMgmt.pHash, pVgId, sizeof(int32_t));
      if (pOld != NULL) snapSendFreeVnodeInfo(pOld);
      taosHashRemove(gSnapSendMgmt.pHash, pVgId, sizeof(int32_t));
    }
    taosArrayDestroy(pStaleVgIds);
  }

  (void)taosThreadMutexUnlock(&gSnapSendMgmt.mutex);

  taosHashCleanup(pActiveVgIds);

  // Send TDMT_DND_QUERY_SNAP_SEND_PROGRESS to each leader dnode
  void *pDnodeIter = taosHashIterate(pDnodesToQuery, NULL);
  while (pDnodeIter != NULL) {
    int32_t    dnodeId = *(int32_t *)pDnodeIter;
    SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
    if (pDnode == NULL) {
      pDnodeIter = taosHashIterate(pDnodesToQuery, pDnodeIter);
      continue;
    }

    // Empty request body (no fields needed — dnode returns all vnode stats)
    int32_t   contLen = sizeof(SMsgHead);
    SMsgHead *pHead   = rpcMallocCont(contLen);
    if (pHead != NULL) {
      pHead->contLen = htonl(contLen);
      pHead->vgId    = htonl(0);

      SEpSet  epSet  = mndGetDnodeEpset(pDnode);
      SRpcMsg rpcMsg = {
          .msgType = TDMT_DND_QUERY_SNAP_SEND_PROGRESS,
          .pCont   = pHead,
          .contLen = contLen,
      };

      mDebug("snap-send-progress: send progress query to dnode:%d", dnodeId);
      if (tmsgSendReq(&epSet, &rpcMsg) < 0) {
        mError("snap-send-progress: failed to send to dnode:%d", dnodeId);
      }
    }

    mndReleaseDnode(pMnode, pDnode);
    pDnodeIter = taosHashIterate(pDnodesToQuery, pDnodeIter);
  }

  taosHashCleanup(pDnodesToQuery);
}

/* ====================================================================
 * Retrieve: ins_snap_send_vnodes
 * ==================================================================== */
int32_t mndRetrieveSnapSendVnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t numOfRows = 0;
  int32_t code = 0;

  (void)taosThreadMutexLock(&gSnapSendMgmt.mutex);

  void *pIter = taosHashIterate(gSnapSendMgmt.pHash, NULL);
  while (pIter != NULL && numOfRows < rows) {
    SSnapSendVnodeInfo *pInfo = (SSnapSendVnodeInfo *)pIter;
    SColumnInfoData    *pColInfo;
    int32_t             cols = 0;
    char                elapsedBuf[32];

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (colDataSetVal(pColInfo, numOfRows, (const char *)&pInfo->vgId, false) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY; break;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (colDataSetVal(pColInfo, numOfRows, (const char *)&pInfo->dnodeId, false) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY; break;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (colDataSetVal(pColInfo, numOfRows, (const char *)&pInfo->totalFileSets, false) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY; break;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (colDataSetVal(pColInfo, numOfRows, (const char *)&pInfo->finishedFileSets, false) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY; break;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (colDataSetVal(pColInfo, numOfRows, (const char *)&pInfo->startTime, false) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY; break;
    }

    // elapsed (VARCHAR "HH:MM:SS")
    snapSendFmtElapsed(pInfo->startTime, elapsedBuf, sizeof(elapsedBuf));
    char varElapsed[32 + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(varElapsed, elapsedBuf);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (colDataSetVal(pColInfo, numOfRows, varElapsed, false) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY; break;
    }

    numOfRows++;
    pIter = taosHashIterate(gSnapSendMgmt.pHash, pIter);
  }

  (void)taosThreadMutexUnlock(&gSnapSendMgmt.mutex);

  pShow->numOfRows += numOfRows;
  if (code != 0) mError("snap-send-progress: retrieve vnodes failed, code:%s", tstrerror(code));
  return numOfRows;
}

/* ====================================================================
 * Retrieve: ins_snap_send_filesets
 * ==================================================================== */
int32_t mndRetrieveSnapSendFilesets(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t numOfRows = 0;
  int32_t code = 0;

  (void)taosThreadMutexLock(&gSnapSendMgmt.mutex);

  void *pIter = taosHashIterate(gSnapSendMgmt.pHash, NULL);
  while (pIter != NULL && numOfRows < rows) {
    SSnapSendVnodeInfo *pInfo = (SSnapSendVnodeInfo *)pIter;

    for (int32_t fi = 0; fi < pInfo->fileSetCount && numOfRows < rows; fi++) {
      SSnapSendFileSetInfo *pFs = &pInfo->pFileSetInfos[fi];
      SColumnInfoData      *pColInfo;
      int32_t               cols = 0;
      char                  elapsedBuf[32];

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, (const char *)&pInfo->vgId, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, (const char *)&pFs->fid, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, (const char *)&pFs->fileCount, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _done;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, (const char *)&pFs->finishedFileCount, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, (const char *)&pFs->totalSize, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, (const char *)&pFs->readSize, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, (const char *)&pFs->startTime, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      // elapsed
      snapSendFmtElapsed(pFs->startTime, elapsedBuf, sizeof(elapsedBuf));
      char varElapsed[32 + VARSTR_HEADER_SIZE];
      STR_TO_VARSTR(varElapsed, elapsedBuf);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, varElapsed, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, (const char *)&pFs->sver, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, (const char *)&pFs->ever, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      // transfer_type: "raw" or "row"
      const char *typeStr = (pFs->transferType == SNAP_TRANSFER_TYPE_RAW) ? "raw" : "row";
      char        varType[8 + VARSTR_HEADER_SIZE];
      STR_TO_VARSTR(varType, typeStr);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (colDataSetVal(pColInfo, numOfRows, varType, false) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY; goto _done;
      }

      numOfRows++;
    }

    pIter = taosHashIterate(gSnapSendMgmt.pHash, pIter);
  }

_done:
  (void)taosThreadMutexUnlock(&gSnapSendMgmt.mutex);
  pShow->numOfRows += numOfRows;
  if (code != 0) mError("snap-send-progress: retrieve filesets failed, code:%s", tstrerror(code));
  return numOfRows;
}

/* ====================================================================
 * Init / Cleanup
 * ==================================================================== */
int32_t mndInitSnapSend(SMnode *pMnode) {
  gSnapSendMgmt.pHash = taosHashInit(
      64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (gSnapSendMgmt.pHash == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  if (taosThreadMutexInit(&gSnapSendMgmt.mutex, NULL) != 0) {
    taosHashCleanup(gSnapSendMgmt.pHash);
    gSnapSendMgmt.pHash = NULL;
    return TSDB_CODE_FAILED;
  }

  mndSetMsgHandle(pMnode, TDMT_DND_QUERY_SNAP_SEND_PROGRESS_RSP,
                  mndProcessDnodeSnapSendProgressRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SNAP_SEND_VNODES,
                           mndRetrieveSnapSendVnodes);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SNAP_SEND_FILESETS,
                           mndRetrieveSnapSendFilesets);

  mDebug("mnd snap-send progress module initialized");
  return 0;
}

void mndCleanupSnapSend(SMnode *pMnode) {
  if (gSnapSendMgmt.pHash == NULL) return;

  (void)taosThreadMutexLock(&gSnapSendMgmt.mutex);
  void *pIter = taosHashIterate(gSnapSendMgmt.pHash, NULL);
  while (pIter != NULL) {
    SSnapSendVnodeInfo *pInfo = (SSnapSendVnodeInfo *)pIter;
    snapSendFreeVnodeInfo(pInfo);
    pIter = taosHashIterate(gSnapSendMgmt.pHash, pIter);
  }
  taosHashCleanup(gSnapSendMgmt.pHash);
  gSnapSendMgmt.pHash = NULL;
  (void)taosThreadMutexUnlock(&gSnapSendMgmt.mutex);
  (void)taosThreadMutexDestroy(&gSnapSendMgmt.mutex);

  mDebug("mnd snap-send progress module cleaned up");
}
