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

#include "tsdbint.h"

typedef struct {
  bool            stop;
  pthread_mutex_t lock;
  pthread_cond_t  queueNotEmpty;
  int             nthreads;
  int             refCount;
  SList *         queue;
  pthread_t *     threads;
} SCommitQueue;

typedef struct {
  TSDB_REQ_T req;
  STsdbRepo *pRepo;
} SReq;

static void *tsdbLoopCommit(void *arg);

static SCommitQueue tsCommitQueue = {0};

int tsdbInitCommitQueue() {
  int nthreads = tsNumOfCommitThreads;
  SCommitQueue *pQueue = &tsCommitQueue;

  if (nthreads < 1) nthreads = 1;

  pQueue->stop = false;
  pQueue->nthreads = nthreads;

  pQueue->queue = tdListNew(0);
  if (pQueue->queue == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  pQueue->threads = (pthread_t *)calloc(nthreads, sizeof(pthread_t));
  if (pQueue->threads == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tdListFree(pQueue->queue);
    return -1;
  }

  pthread_mutex_init(&(pQueue->lock), NULL);
  pthread_cond_init(&(pQueue->queueNotEmpty), NULL);

  for (int i = 0; i < nthreads; i++) {
    pthread_create(pQueue->threads + i, NULL, tsdbLoopCommit, NULL);
  }

  return 0;
}

void tsdbDestroyCommitQueue() {
  SCommitQueue *pQueue = &tsCommitQueue;

  pthread_mutex_lock(&(pQueue->lock));

  if (pQueue->stop) {
    pthread_mutex_unlock(&(pQueue->lock));
    return;
  }

  pQueue->stop = true;
  pthread_cond_broadcast(&(pQueue->queueNotEmpty));

  pthread_mutex_unlock(&(pQueue->lock));

  for (size_t i = 0; i < pQueue->nthreads; i++) {
    pthread_join(pQueue->threads[i], NULL);
  }

  free(pQueue->threads);
  tdListFree(pQueue->queue);
  pthread_cond_destroy(&(pQueue->queueNotEmpty));
  pthread_mutex_destroy(&(pQueue->lock));
}

int tsdbScheduleCommit(STsdbRepo *pRepo, TSDB_REQ_T req) {
  SCommitQueue *pQueue = &tsCommitQueue;

  SListNode *pNode = (SListNode *)calloc(1, sizeof(SListNode) + sizeof(SReq));
  if (pNode == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  ((SReq *)pNode->data)->req = req;
  ((SReq *)pNode->data)->pRepo = pRepo;

  pthread_mutex_lock(&(pQueue->lock));

  // ASSERT(pQueue->stop);

  tdListAppendNode(pQueue->queue, pNode);
  pthread_cond_signal(&(pQueue->queueNotEmpty));

  pthread_mutex_unlock(&(pQueue->lock));
  return 0;
}

static void tsdbApplyRepoConfig(STsdbRepo *pRepo) {
  pthread_mutex_lock(&pRepo->save_mutex);

  pRepo->config_changed = false;
  STsdbCfg * pSaveCfg = &pRepo->save_config;
  STsdbCfg oldCfg;
  int32_t oldTotalBlocks = pRepo->config.totalBlocks;

  memcpy(&oldCfg, &(pRepo->config), sizeof(STsdbCfg));

  pRepo->config.compression = pRepo->save_config.compression;
  pRepo->config.keep = pRepo->save_config.keep;
  pRepo->config.keep1 = pRepo->save_config.keep1;
  pRepo->config.keep2 = pRepo->save_config.keep2;
  pRepo->config.cacheLastRow = pRepo->save_config.cacheLastRow;
  pRepo->config.totalBlocks = pRepo->save_config.totalBlocks;

  pthread_mutex_unlock(&pRepo->save_mutex);

  tsdbInfo("vgId:%d apply new config: compression(%d), keep(%d,%d,%d), totalBlocks(%d), cacheLastRow(%d->%d),totalBlocks(%d->%d)",
    REPO_ID(pRepo),
    pSaveCfg->compression, pSaveCfg->keep,pSaveCfg->keep1, pSaveCfg->keep2,
    pSaveCfg->totalBlocks, oldCfg.cacheLastRow, pSaveCfg->cacheLastRow, oldTotalBlocks, pSaveCfg->totalBlocks);

  int err = tsdbExpandPool(pRepo, oldTotalBlocks);
  if (!TAOS_SUCCEEDED(err)) {
    tsdbError("vgId:%d expand pool from %d to %d fail,reason:%s",
      REPO_ID(pRepo), oldTotalBlocks, pSaveCfg->totalBlocks, tstrerror(err));
  }

  if (oldCfg.cacheLastRow != pRepo->config.cacheLastRow) {
    if (tsdbLockRepo(pRepo) < 0) return;
    // tsdbCacheLastData(pRepo, &oldCfg);
    // lazy load last cache when query or update
    ++pRepo->cacheLastConfigVersion;
    tsdbUnlockRepo(pRepo);
  }

}

static void *tsdbLoopCommit(void *arg) {
  SCommitQueue *pQueue = &tsCommitQueue;
  SListNode *   pNode = NULL;
  STsdbRepo *   pRepo = NULL;
  TSDB_REQ_T    req;

  setThreadName("tsdbCommit");

  while (true) {
    pthread_mutex_lock(&(pQueue->lock));

    while (true) {
      pNode = tdListPopHead(pQueue->queue);
      if (pNode == NULL) {
        if (pQueue->stop && pQueue->refCount <= 0) {
          pthread_mutex_unlock(&(pQueue->lock));
          goto _exit;
        } else {
          pthread_cond_wait(&(pQueue->queueNotEmpty), &(pQueue->lock));
        }
      } else {
        break;
      }
    }

    pthread_mutex_unlock(&(pQueue->lock));

    req = ((SReq *)pNode->data)->req;
    pRepo = ((SReq *)pNode->data)->pRepo;

    if (req == COMMIT_REQ) {
      tsdbCommitData(pRepo);
    } else if (req == COMPACT_REQ) {
      tsdbCompactImpl(pRepo);
    } else if (req == COMMIT_CONFIG_REQ) {        
      ASSERT(pRepo->config_changed);
      tsdbApplyRepoConfig(pRepo);
      tsem_post(&(pRepo->readyToCommit));
    } else {
      ASSERT(0);
    }

    listNodeFree(pNode);
  }

_exit:
  return NULL;
}

void tsdbIncCommitRef(int vgId) {
  int refCount = atomic_add_fetch_32(&tsCommitQueue.refCount, 1);
  tsdbDebug("vgId:%d, inc commit queue ref to %d", vgId, refCount);
}

void tsdbDecCommitRef(int vgId) {
  int refCount = atomic_sub_fetch_32(&tsCommitQueue.refCount, 1);
  pthread_cond_broadcast(&(tsCommitQueue.queueNotEmpty));
  tsdbDebug("vgId:%d, dec commit queue ref to %d", vgId, refCount);
}
