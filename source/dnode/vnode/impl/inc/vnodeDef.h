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

#ifndef _TD_VNODE_DEF_H_
#define _TD_VNODE_DEF_H_

#include "mallocator.h"
#include "sync.h"
#include "tcoding.h"
#include "tlist.h"
#include "tlockfree.h"
#include "tmacro.h"
#include "wal.h"
#include "tfs.h"

#include "vnode.h"

#include "vnodeBufferPool.h"
#include "vnodeCfg.h"
#include "vnodeCommit.h"
#include "vnodeFS.h"
#include "vnodeMemAllocator.h"
#include "vnodeQuery.h"
#include "vnodeRequest.h"
#include "vnodeStateMgr.h"
#include "vnodeSync.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVnodeTask {
  TD_DLIST_NODE(SVnodeTask);
  void* arg;
  int (*execute)(void*);
} SVnodeTask;

typedef struct SVnodeMgr {
  td_mode_flag_t vnodeInitFlag;
  // For commit
  bool            stop;
  uint16_t        nthreads;
  pthread_t*      threads;
  pthread_mutex_t mutex;
  pthread_cond_t  hasTask;
  TD_DLIST(SVnodeTask) queue;
  // For vnode Mgmt
} SVnodeMgr;

extern SVnodeMgr vnodeMgr;

struct SVnode {
  int32_t     vgId;
  char*       path;
  SVnodeCfg   config;
  SVState     state;
  SVBufPool*  pBufPool;
  SMeta*      pMeta;
  STsdb*      pTsdb;
  STQ*        pTq;
  SWal*       pWal;
  SVnodeSync* pSync;
  SVnodeFS*   pFs;
  tsem_t      canCommit;
  SQHandle*   pQuery;
};

int vnodeScheduleTask(SVnodeTask* task);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_DEF_H_*/