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

void msmDestroyRuntimeInfo() {

}

int32_t msmInitRuntimeInfo(SMnode *pMnode) {
  int32_t code = taosThreadMutexInit(&mStreamMgmt.lock, NULL);
  if (code) {
    return code;
  }

  int32_t vnodeNum = sdbGetSize(pMnode->pSdb, SDB_VGROUP);
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t dnodeNum = sdbGetSize(pMnode->pSdb, SDB_DNODE);

  mStreamMgmt.qNum = ;
  mStreamMgmt.actionQ = taosMemoryCalloc(mStreamMgmt.qNum, sizeof(SStreamActionQ));
  if (mStreamMgmt.actionQ == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime actionQ, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.streamMap = taosHashInit(MND_STREAM_DEFAULT_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.streamMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime streamMap, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.taskMap = taosHashInit(MND_STREAM_DEFAULT_TASK_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.taskMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime taskMap, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.vgroupMap = taosHashInit(vnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.vgroupMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime vgroupMap, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.snodeMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.snodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime snodeMap, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.dnodeMap = taosHashInit(dnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.dnodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime dnodeMap, code:%s", tstrerror(terrno));
    goto _return;
  }

  taosHashSetFreeFp(mStreamMgmt.nodeMap, freeTaskList);
  taosHashSetFreeFp(mStreamMgmt.streamMap, freeTaskList);

_return:

  if (code) {
    msmDestroyRuntimeInfo();
  }

  return code;
}

static int32_t msmBuildStreamTasksFromObj(SStreamTasksInfo* pInfo, SStreamObj* pStream) {
  if (TSDB_NORMAL_TABLE == pStream->pCreate->triggerTblType || TSDB_CHILD_TABLE == pStream->pCreate->triggerTblType) {

  }
}

static int32_t msmLaunchStreamDepolyAction(SMnode* pMnode, SStreamQNode* pQNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pQNode->streamId;
  char* streamName = pQNode->streamName;
  SStreamObj* pStream = NULL;

  SStreamTasksInfo** ppStream = (SStreamTasksInfo**)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (ppStream) {

  }

  SStreamTasksInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamTasksInfo));
  TSDB_CHECK_NULL(pInfo, code, lino, _return, terrno);

  TSDB_CHECK_CODE(mndAcquireStream(pMnode, streamName, &pStream), lino, _return);
  TSDB_CHECK_CODE(msmBuildStreamTasksFromObj(pInfo, pStream), lino, _return);

_return:

  return code;
}

static int32_t msmLaunchStreamDropAction(SRpcMsg *pReq) {
  int64_t streamId = *(int64_t*)pReq->pCont;
  char* streamName = (char*)pReq->pCont + sizeof(streamId);
}

static int32_t msmHandleStreamActions(SMnode* pMnode, SStreamActionQ* pQ) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamQNode* pQNode = NULL;
  while (mndStreamActionDequeue(pQ, &pQNode)) {
    switch (pQNode->action) {
      case STREAM_ACTION_DEPLOY:
        TSDB_CHECK_CODE(msmLaunchStreamDepolyAction(pMnode, pQNode), lino, _return);
        break;
      case STREAM_ACTION_UNDEPLOY:
      default:
        break;
    }
  }

_return:

  return code;
}

int32_t msmHandleGrantExpired(SMnode *pMnode) {

}

int32_t msmHandleStreamHbMsg(SMnode* pMnode, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t qIdx = streamGetTargetQIdx(mStreamMgmt.qNum, pHb->streamGId);
  
  if (atomic_load_64(&mStreamMgmt.actionQ[qIdx].qRemainNum) > 0) {
    TSDB_CHECK_CODE(msmHandleStreamActions(pMnode, mStreamMgmt.actionQ + qIdx), lino, _return);
  }

_return:

  return code;
}


