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

#include "os.h"
#include "tlist.h"
#include "tref.h"
#include "tsdbMain.h"

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
  STsdbRepo *pRepo;
} SCommitReq;

static void *tsdbLoopCommit(void *arg);

SCommitQueue tsCommitQueue = {0};

int tsdbInitCommitQueue(int nthreads) {
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

int tsdbScheduleCommit(STsdbRepo *pRepo) {
  SCommitQueue *pQueue = &tsCommitQueue;

  SListNode *pNode = (SListNode *)calloc(1, sizeof(SListNode) + sizeof(SCommitReq));
  if (pNode == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  ((SCommitReq *)pNode->data)->pRepo = pRepo;

  pthread_mutex_lock(&(pQueue->lock));

  // ASSERT(pQueue->stop);

  tdListAppendNode(pQueue->queue, pNode);
  pthread_cond_signal(&(pQueue->queueNotEmpty));

  pthread_mutex_unlock(&(pQueue->lock));
  return 0;
}

static void *tsdbLoopCommit(void *arg) {
  SCommitQueue *pQueue = &tsCommitQueue;
  SListNode *   pNode = NULL;
  STsdbRepo *   pRepo = NULL;

  while (true) {
    pthread_mutex_lock(&(pQueue->lock));

    while (true) {
      pNode = tdListPopHead(pQueue->queue);
      if (pNode == NULL) {
        if (pQueue->stop && pQueue->refCount == 0) {
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

    pRepo = ((SCommitReq *)pNode->data)->pRepo;

    tsdbCommitData(pRepo);
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
  tsdbDebug("vgId:%d, dec commit queue ref to %d", vgId, refCount);
}