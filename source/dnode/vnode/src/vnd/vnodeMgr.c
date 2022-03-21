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

SVnodeMgr vnodeMgr = {.vnodeInitFlag = TD_MOD_UNINITIALIZED};

static void* loop(void* arg);

int vnodeInit(const SVnodeOpt *pOption) {
  if (TD_CHECK_AND_SET_MODE_INIT(&(vnodeMgr.vnodeInitFlag)) == TD_MOD_INITIALIZED) {
    return 0;
  }

  vnodeMgr.stop = false;
  vnodeMgr.putToQueryQFp = pOption->putToQueryQFp;
  vnodeMgr.putToFetchQFp = pOption->putToFetchQFp;
  vnodeMgr.sendReqFp = pOption->sendReqFp;
  vnodeMgr.sendMnodeReqFp = pOption->sendMnodeReqFp;
  vnodeMgr.sendRspFp = pOption->sendRspFp;

  // Start commit handers
  if (pOption->nthreads > 0) {
    vnodeMgr.nthreads = pOption->nthreads;
    vnodeMgr.threads = (TdThread*)calloc(pOption->nthreads, sizeof(TdThread));
    if (vnodeMgr.threads == NULL) {
      return -1;
    }

    taosThreadMutexInit(&(vnodeMgr.mutex), NULL);
    taosThreadCondInit(&(vnodeMgr.hasTask), NULL);
    TD_DLIST_INIT(&(vnodeMgr.queue));

    for (uint16_t i = 0; i < pOption->nthreads; i++) {
      taosThreadCreate(&(vnodeMgr.threads[i]), NULL, loop, NULL);
      // pthread_setname_np(vnodeMgr.threads[i], "VND Commit Thread");
    }
  } else {
    // TODO: if no commit thread is set, then another mechanism should be
    // given. Otherwise, it is a false.
    ASSERT(0);
  }

  if (walInit() < 0) {
    return -1;
  }

  return 0;
}

void vnodeCleanup() {
  if (TD_CHECK_AND_SET_MOD_CLEAR(&(vnodeMgr.vnodeInitFlag)) == TD_MOD_UNINITIALIZED) {
    return;
  }

  // Stop commit handler
  taosThreadMutexLock(&(vnodeMgr.mutex));
  vnodeMgr.stop = true;
  taosThreadCondBroadcast(&(vnodeMgr.hasTask));
  taosThreadMutexUnlock(&(vnodeMgr.mutex));

  for (uint16_t i = 0; i < vnodeMgr.nthreads; i++) {
    taosThreadJoin(vnodeMgr.threads[i], NULL);
  }

  tfree(vnodeMgr.threads);
  taosThreadCondDestroy(&(vnodeMgr.hasTask));
  taosThreadMutexDestroy(&(vnodeMgr.mutex));
}

int vnodeScheduleTask(SVnodeTask* pTask) {
  taosThreadMutexLock(&(vnodeMgr.mutex));

  TD_DLIST_APPEND(&(vnodeMgr.queue), pTask);

  taosThreadCondSignal(&(vnodeMgr.hasTask));

  taosThreadMutexUnlock(&(vnodeMgr.mutex));

  return 0;
}

int32_t vnodePutToVQueryQ(SVnode* pVnode, struct SRpcMsg* pReq) {
  return (*vnodeMgr.putToQueryQFp)(pVnode->pWrapper, pReq);
}

int32_t vnodePutToVFetchQ(SVnode* pVnode, struct SRpcMsg* pReq) {
  return (*vnodeMgr.putToFetchQFp)(pVnode->pWrapper, pReq);
}

int32_t vnodeSendReq(SVnode* pVnode, struct SEpSet* epSet, struct SRpcMsg* pReq) {
  return (*vnodeMgr.sendReqFp)(pVnode->pWrapper, epSet, pReq);
}

int32_t vnodeSendMnodeReq(SVnode* pVnode, struct SRpcMsg* pReq) {
  return (*vnodeMgr.sendMnodeReqFp)(pVnode->pWrapper, pReq);
}

void vnodeSendRsp(SVnode* pVnode, struct SEpSet* epSet, struct SRpcMsg* pRsp) {
  (*vnodeMgr.sendRspFp)(pVnode->pWrapper, pRsp);
}

/* ------------------------ STATIC METHODS ------------------------ */
static void* loop(void* arg) {
  setThreadName("vnode-commit");

  SVnodeTask* pTask;
  for (;;) {
    taosThreadMutexLock(&(vnodeMgr.mutex));
    for (;;) {
      pTask = TD_DLIST_HEAD(&(vnodeMgr.queue));
      if (pTask == NULL) {
        if (vnodeMgr.stop) {
          taosThreadMutexUnlock(&(vnodeMgr.mutex));
          return NULL;
        } else {
          taosThreadCondWait(&(vnodeMgr.hasTask), &(vnodeMgr.mutex));
        }
      } else {
        TD_DLIST_POP(&(vnodeMgr.queue), pTask);
        break;
      }
    }

    taosThreadMutexUnlock(&(vnodeMgr.mutex));

    (*(pTask->execute))(pTask->arg);
    free(pTask);
  }

  return NULL;
}