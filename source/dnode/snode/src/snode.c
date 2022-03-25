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

#include "executor.h"
#include "sndInt.h"
#include "tuuid.h"

SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) {
  SSnode *pSnode = taosMemoryCalloc(1, sizeof(SSnode));
  if (pSnode == NULL) {
    return NULL;
  }
  pSnode->msgCb = pOption->msgCb;
  pSnode->pMeta = sndMetaNew();
  if (pSnode->pMeta == NULL) {
    taosMemoryFree(pSnode);
    return NULL;
  }
  return pSnode;
}

void sndClose(SSnode *pSnode) {
  sndMetaDelete(pSnode->pMeta);
  taosMemoryFree(pSnode);
}

int32_t sndGetLoad(SSnode *pSnode, SSnodeLoad *pLoad) { return 0; }

SStreamMeta *sndMetaNew() {
  SStreamMeta *pMeta = taosMemoryCalloc(1, sizeof(SStreamMeta));
  if (pMeta == NULL) {
    return NULL;
  }
  pMeta->pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMeta->pHash == NULL) {
    taosMemoryFree(pMeta);
    return NULL;
  }
  return pMeta;
}

void sndMetaDelete(SStreamMeta *pMeta) {
  taosHashCleanup(pMeta->pHash);
  taosMemoryFree(pMeta);
}

int32_t sndMetaDeployTask(SStreamMeta *pMeta, SStreamTask *pTask) {
  for (int i = 0; i < pTask->exec.numOfRunners; i++) {
    pTask->exec.runners[i].executor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, NULL);
  }
  return taosHashPut(pMeta->pHash, &pTask->taskId, sizeof(int32_t), pTask, sizeof(void *));
}

SStreamTask *sndMetaGetTask(SStreamMeta *pMeta, int32_t taskId) {
  return taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
}

int32_t sndMetaRemoveTask(SStreamMeta *pMeta, int32_t taskId) {
  SStreamTask *pTask = taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
  if (pTask == NULL) {
    return -1;
  }
  taosMemoryFree(pTask->exec.qmsg);
  // TODO:free executor
  taosMemoryFree(pTask);
  return taosHashRemove(pMeta->pHash, &taskId, sizeof(int32_t));
}

static int32_t sndProcessTaskExecReq(SSnode *pSnode, SRpcMsg *pMsg) {
  /*SStreamExecMsgHead *pHead = pMsg->pCont;*/
  /*int32_t             taskId = pHead->streamTaskId;*/
  /*SStreamTask *pTask = sndMetaGetTask(pSnode->pMeta, taskId);*/
  /*if (pTask == NULL) {*/
  /*return -1;*/
  /*}*/
  return 0;
}

void sndProcessUMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  // stream deploy
  // stream stop/resume
  // operator exec
  if (pMsg->msgType == TDMT_SND_TASK_DEPLOY) {
    void        *msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
    SStreamTask *pTask = taosMemoryMalloc(sizeof(SStreamTask));
    if (pTask == NULL) {
      ASSERT(0);
      return;
    }
    SCoder decoder;
    tCoderInit(&decoder, TD_LITTLE_ENDIAN, msg, pMsg->contLen - sizeof(SMsgHead), TD_DECODER);
    tDecodeSStreamTask(&decoder, pTask);
    tCoderClear(&decoder);

    sndMetaDeployTask(pSnode->pMeta, pTask);
  } else if (pMsg->msgType == TDMT_SND_TASK_EXEC) {
    sndProcessTaskExecReq(pSnode, pMsg);
  } else {
    ASSERT(0);
  }
}

void sndProcessSMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  // operator exec
  if (pMsg->msgType == TDMT_SND_TASK_EXEC) {
    sndProcessTaskExecReq(pSnode, pMsg);
  } else {
    ASSERT(0);
  }
}
