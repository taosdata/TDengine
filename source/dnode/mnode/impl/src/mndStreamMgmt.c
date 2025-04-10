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

#include "mndStream.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "osMemory.h"
#include "parser.h"
#include "taoserror.h"
#include "tmisce.h"
#include "tname.h"

int32_t msmDeployStream(SStreamObj* pStream) {
  code = mndScheduleStream(pMnode, &streamObj, pCreate);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to schedule since %s", createReq.name, tstrerror(code));
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // add notify info into all stream tasks
  code = addStreamNotifyInfo(pCreate, &streamObj);
  if (code != TSDB_CODE_SUCCESS) {
    mError("stream:%s failed to add stream notify info since %s", pCreate->name, tstrerror(code));
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // add into buffer firstly
  // to make sure when the hb from vnode arrived, the newly created tasks have been in the task map already.
  streamMutexLock(&execInfo.lock);
  mDebug("stream stream:%s start to register tasks into task nodeList and set initial checkpointId", createReq.name);
  saveTaskAndNodeInfoIntoBuf(&streamObj, &execInfo);
  streamMutexUnlock(&execInfo.lock);
}

int32_t msmInitRuntimeInfo() {
  int32_t code = taosThreadMutexInit(&mStreamMgmt.lock, NULL);
  if (code) {
    return code;
  }

  _hash_fn_t fn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR);

  mStreamMgmt.pTaskList = taosArrayInit(4, sizeof(STaskId));
  mStreamMgmt.pTaskMap = taosHashInit(64, fn, true, HASH_NO_LOCK);
  mStreamMgmt.transMgmt.pDBTrans = taosHashInit(32, fn, true, HASH_NO_LOCK);
  mStreamMgmt.pTransferStateStreams = taosHashInit(32, fn, true, HASH_NO_LOCK);
  mStreamMgmt.pChkptStreams = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  mStreamMgmt.pStreamConsensus = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  mStreamMgmt.pNodeList = taosArrayInit(4, sizeof(SNodeEntry));
  mStreamMgmt.pKilledChkptTrans = taosArrayInit(4, sizeof(SStreamTaskResetMsg));

  if (mStreamMgmt.pTaskList == NULL || mStreamMgmt.pTaskMap == NULL || mStreamMgmt.transMgmt.pDBTrans == NULL ||
      mStreamMgmt.pTransferStateStreams == NULL || mStreamMgmt.pChkptStreams == NULL || mStreamMgmt.pStreamConsensus == NULL ||
      mStreamMgmt.pNodeList == NULL || mStreamMgmt.pKilledChkptTrans == NULL) {
    mError("failed to initialize the stream runtime env, code:%s", tstrerror(terrno));
    return terrno;
  }

  mStreamMgmt.role = NODE_ROLE_UNINIT;
  mStreamMgmt.switchFromFollower = false;

  taosHashSetFreeFp(mStreamMgmt.pTransferStateStreams, freeTaskList);
  taosHashSetFreeFp(mStreamMgmt.pChkptStreams, freeTaskList);
  taosHashSetFreeFp(mStreamMgmt.pStreamConsensus, freeTaskList);
  return 0;
}

static int32_t msmLaunchStreamDepolyAction(SRpcMsg *pReq) {
  int64_t streamId = *(int64_t*)pReq->pCont;
  char* streamName = (char*)pReq->pCont + sizeof(streamId);

  
}

static int32_t msmLaunchStreamDropAction(SRpcMsg *pReq) {
  int64_t streamId = *(int64_t*)pReq->pCont;
  char* streamName = (char*)pReq->pCont + sizeof(streamId);
}


