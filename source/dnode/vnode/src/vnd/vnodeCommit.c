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

#include "vnd.h"

static int  vnodeStartCommit(SVnode *pVnode);
static int  vnodeEndCommit(SVnode *pVnode);
static int  vnodeCommit(void *arg);
static void vnodeWaitCommit(SVnode *pVnode);

int vnodeAsyncCommit(SVnode *pVnode) {
  vnodeWaitCommit(pVnode);

  vnodeBufPoolSwitch(pVnode);
  SVnodeTask *pTask = (SVnodeTask *)taosMemoryMalloc(sizeof(*pTask));

  pTask->execute = vnodeCommit;  // TODO
  pTask->arg = pVnode;           // TODO

  tsdbPrepareCommit(pVnode->pTsdb);
  // metaPrepareCommit(pVnode->pMeta);
  // walPreapareCommit(pVnode->pWal);

  vnodeScheduleTask(pTask);
  return 0;
}

int vnodeSyncCommit(SVnode *pVnode) {
  vnodeAsyncCommit(pVnode);
  vnodeWaitCommit(pVnode);
  tsem_post(&(pVnode->canCommit));
  return 0;
}

static int vnodeCommit(void *arg) {
  SVnode *pVnode = (SVnode *)arg;

  metaCommit(pVnode->pMeta);
  tqCommit(pVnode->pTq);
  tsdbCommit(pVnode->pTsdb);

  vnodeBufPoolRecycle(pVnode);
  tsem_post(&(pVnode->canCommit));
  return 0;
}

static int vnodeStartCommit(SVnode *pVnode) {
  // TODO
  return 0;
}

static int vnodeEndCommit(SVnode *pVnode) {
  // TODO
  return 0;
}

static FORCE_INLINE void vnodeWaitCommit(SVnode *pVnode) { tsem_wait(&pVnode->canCommit); }