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

#include "sndInt.h"
#include "tuuid.h"

SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) {
  SSnode *pSnode = calloc(1, sizeof(SSnode));
  memcpy(&pSnode->cfg, pOption, sizeof(SSnodeOpt));
  return pSnode;
}

void sndClose(SSnode *pSnode) { free(pSnode); }

int32_t sndGetLoad(SSnode *pSnode, SSnodeLoad *pLoad) { return 0; }

int32_t sndProcessMsg(SSnode *pSnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  *pRsp = NULL;
  return 0;
}

void sndDestroy(const char *path) {}

static int32_t sndDeployTask(SSnode *pSnode, SRpcMsg *pMsg) {
  SStreamTask *task = malloc(sizeof(SStreamTask));
  if (task == NULL) {
    return -1;
  }
  task->meta.taskId = tGenIdPI32();
  taosHashPut(pSnode->pMeta->pHash, &task->meta.taskId, sizeof(int32_t), &task, sizeof(void *));
  return 0;
}

int32_t sndProcessUMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  // stream deployment
  // stream stop/resume
  // operator exec
  return 0;
}

int32_t sndProcessSMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  // operator exec
  return 0;
}
