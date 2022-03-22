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
#include "sync.h"
#include "vnd.h"
// #include "vnodeInt.h"

int32_t vnodeAlter(SVnode *pVnode, const SVnodeCfg *pCfg) { return 0; }

int32_t vnodeCompact(SVnode *pVnode) { return 0; }

int32_t vnodeSync(SVnode *pVnode) { return 0; }

int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad) {
  pLoad->vgId = pVnode->vgId;
  pLoad->role = TAOS_SYNC_STATE_LEADER;
  pLoad->numOfTables = metaGetTbNum(pVnode->pMeta);
  pLoad->numOfTimeSeries = 400;
  pLoad->totalStorage = 300;
  pLoad->compStorage = 200;
  pLoad->pointsWritten = 100;
  pLoad->numOfSelectReqs = 1;
  pLoad->numOfInsertReqs = 3;
  pLoad->numOfInsertSuccessReqs = 2;
  pLoad->numOfBatchInsertReqs = 5;
  pLoad->numOfBatchInsertSuccessReqs = 4;
  return 0;
}

int vnodeProcessSyncReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  /*vInfo("sync message is processed");*/
  return 0;
}
