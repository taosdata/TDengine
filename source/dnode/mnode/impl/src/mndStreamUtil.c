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

#include "mndDb.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "taoserror.h"
#include "tmisce.h"
#include "mndSnode.h"

bool mstWaitLock(SRWLatch* pLock, bool readLock) {
  if (readLock) {
    while (taosRTryLockLatch(pLock)) {
      taosMsleep(1);
    }

    return true;
  }

  taosWWaitLockLatch(pLock);

  return true;
}

void mstDestroySStmVgStreamStatus(void* p) { 
  SStmVgStreamStatus* pStatus = (SStmVgStreamStatus*)p;
  taosArrayDestroy(pStatus->trigReaders); 
  taosArrayDestroy(pStatus->calcReaders); 
}

void mstDestroySStmSnodeStreamStatus(void* p) { 
  SStmSnodeStreamStatus* pStatus = (SStmSnodeStreamStatus*)p;
  for (int32_t i = 0; i < MND_STREAM_RUNNER_DEPLOY_NUM; ++i) {
    taosArrayDestroy(pStatus->runners[i]);
    pStatus->runners[i] = NULL;
  }
}


void mstDestroyVgroupStatus(SStmVgroupStatus* pVgStatus) {
  taosHashCleanup(pVgStatus->streamTasks);
  pVgStatus->streamTasks = NULL;
}

void mstDestroySStmTaskToDeployExt(void* param) {
  SStmTaskToDeployExt* pExt = (SStmTaskToDeployExt*)param;
  if (pExt->deployed) {
    return;
  }
  
  switch (pExt->deploy.task.type) {
    case STREAM_TRIGGER_TASK:
      taosArrayDestroy(pExt->deploy.msg.trigger.readerList);
      pExt->deploy.msg.trigger.readerList = NULL;
      taosArrayDestroy(pExt->deploy.msg.trigger.runnerList);
      pExt->deploy.msg.trigger.runnerList = NULL;
      break;
    case STREAM_RUNNER_TASK:
      taosMemoryFreeClear(pExt->deploy.msg.runner.pPlan);
      break;
    default:  
      break;;
  }
}

void mstDestroyScanAddrList(void* param) {
  if (NULL == param) {
    return;
  }
  SArray* pList = *(SArray**)param;
  taosArrayDestroy(pList);
}

void mstDestroySStmSnodeTasksDeploy(void* param) {
  SStmSnodeTasksDeploy* pSnode = (SStmSnodeTasksDeploy*)param;
  taosArrayDestroyEx(pSnode->triggerList, mstDestroySStmTaskToDeployExt);
  taosArrayDestroyEx(pSnode->runnerList, mstDestroySStmTaskToDeployExt);
}

void mstDestroySStmVgTasksToDeploy(void* param) {
  SStmVgTasksToDeploy* pVg = (SStmVgTasksToDeploy*)param;
  taosArrayDestroyEx(pVg->taskList, mstDestroySStmTaskToDeployExt);
}

void mstDestroySStmSnodeStatus(void* param) {
  SStmSnodeStatus* pSnode = (SStmSnodeStatus*)param;
  taosHashCleanup(pSnode->streamTasks);
}

void mstDestroySStmVgroupStatus(void* param) {
  SStmVgroupStatus* pVg = (SStmVgroupStatus*)param;
  taosHashCleanup(pVg->streamTasks);
}

void mstResetSStmStatus(SStmStatus* pStatus) {
  (void)mstWaitLock(&pStatus->resetLock, false);

  taosArrayDestroy(pStatus->trigReaders);
  pStatus->trigReaders = NULL;
  taosArrayDestroy(pStatus->trigOReaders);
  pStatus->trigOReaders = NULL;
  pStatus->calcReaders = tdListFree(pStatus->calcReaders);
  if (pStatus->triggerTask) {
    (void)mstWaitLock(&pStatus->triggerTask->detailStatusLock, false);
    taosMemoryFreeClear(pStatus->triggerTask->detailStatus);
    taosWUnLockLatch(&pStatus->triggerTask->detailStatusLock);
  }
  taosMemoryFreeClear(pStatus->triggerTask);
  for (int32_t i = 0; i < MND_STREAM_RUNNER_DEPLOY_NUM; ++i) {
    taosArrayDestroy(pStatus->runners[i]);
    pStatus->runners[i] = NULL;
  }

  taosWUnLockLatch(&pStatus->resetLock);
}

void mstDestroySStmStatus(void* param) {
  SStmStatus* pStatus = (SStmStatus*)param;
  taosMemoryFreeClear(pStatus->streamName);

  mstResetSStmStatus(pStatus);

  taosWLockLatch(&pStatus->userRecalcLock);
  taosArrayDestroy(pStatus->userRecalcList);
  taosWUnLockLatch(&pStatus->userRecalcLock);

  tFreeSCMCreateStreamReq(pStatus->pCreate);
  taosMemoryFreeClear(pStatus->pCreate);  
}

void mstDestroySStmAction(void* param) {
  SStmAction* pAction = (SStmAction*)param;

  taosArrayDestroy(pAction->undeploy.taskList);
  taosArrayDestroy(pAction->recalc.recalcList);
}

void mstClearSStmStreamDeploy(SStmStreamDeploy* pDeploy) {
  pDeploy->readerTasks = NULL;
  pDeploy->triggerTask = NULL;
  pDeploy->runnerTasks = NULL;
}

int32_t mstIsStreamDropped(SMnode *pMnode, int64_t streamId, bool* dropped) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  
  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    if (pStream->pCreate->streamId == streamId) {
      *dropped = pStream->userDropped ? true : false;
      sdbRelease(pSdb, pStream);
      sdbCancelFetch(pSdb, pIter);
      mstsDebug("stream found, dropped:%d", *dropped);
      return TSDB_CODE_SUCCESS;
    }
    
    sdbRelease(pSdb, pStream);
  }

  *dropped = true;

  return TSDB_CODE_SUCCESS;
}

typedef struct SStmCheckDbInUseCtx {
  bool* dbStream;
  bool* vtableStream;
  bool  ignoreCurrDb;
} SStmCheckDbInUseCtx;

static bool mstChkSetDbInUse(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SStreamObj *pStream = pObj;
  if (atomic_load_8(&pStream->userDropped)) {
    return true;
  }

  SStmCheckDbInUseCtx* pCtx = (SStmCheckDbInUseCtx*)p2;
  if (pCtx->ignoreCurrDb && 0 == strcmp(pStream->pCreate->streamDB, p1)) {
    return true;
  }
  
  if (pStream->pCreate->triggerDB && 0 == strcmp(pStream->pCreate->triggerDB, p1)) {
    *pCtx->dbStream = true;
    return false;
  }

  int32_t calcDBNum = taosArrayGetSize(pStream->pCreate->calcDB);
  for (int32_t i = 0; i < calcDBNum; ++i) {
    char* calcDB = taosArrayGetP(pStream->pCreate->calcDB, i);
    if (0 == strcmp(calcDB, p1)) {
      *pCtx->dbStream = true;
      return false;
    }
  }

  if (pStream->pCreate->vtableCalc || STREAM_IS_VIRTUAL_TABLE(pStream->pCreate->triggerTblType, pStream->pCreate->flags)) {
    *pCtx->vtableStream = true;
    return true;
  }
  
  return true;
}

void mstCheckDbInUse(SMnode *pMnode, char *dbFName, bool *dbStream, bool *vtableStream, bool ignoreCurrDb) {
  int32_t streamNum = sdbGetSize(pMnode->pSdb, SDB_STREAM);
  if (streamNum <= 0) {
    return;
  }

  SStmCheckDbInUseCtx ctx = {dbStream, vtableStream, ignoreCurrDb};
  sdbTraverse(pMnode->pSdb, SDB_STREAM, mstChkSetDbInUse, dbFName, &ctx, NULL);
}

static void mstShowStreamStatus(char *dst, int8_t status, int32_t bufLen) {
  if (status == STREAM_STATUS_INIT) {
    tstrncpy(dst, "init", bufLen);
  } else if (status == STREAM_STATUS_RUNNING) {
    tstrncpy(dst, "running", bufLen);
  } else if (status == STREAM_STATUS_STOPPED) {
    tstrncpy(dst, "stopped", bufLen);
  } else if (status == STREAM_STATUS_FAILED) {
    tstrncpy(dst, "failed", bufLen);
  }
}

int32_t mstCheckSnodeExists(SMnode *pMnode) {
  SSdb      *pSdb = pMnode->pSdb;
  void      *pIter = NULL;
  SSnodeObj *pObj = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter == NULL) {
      break;
    }

    sdbRelease(pSdb, pObj);
    sdbCancelFetch(pSdb, pIter);
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_SNODE_NOT_DEPLOYED;
}

void mstSetTaskStatusFromMsg(SStmGrpCtx* pCtx, SStmTaskStatus* pTask, SStmTaskStatusMsg* pMsg) {
  pTask->id.taskId = pMsg->taskId;
  pTask->id.deployId = pMsg->deployId;
  pTask->id.seriousId = pMsg->seriousId;
  pTask->id.nodeId = pMsg->nodeId;
  pTask->id.taskIdx = pMsg->taskIdx;

  pTask->type = pMsg->type;
  pTask->flags = pMsg->flags;
  pTask->status = pMsg->status;
  pTask->lastUpTs = pCtx->currTs;
}

bool mndStreamActionDequeue(SStmActionQ* pQueue, SStmQNode **param) {
  while (0 == atomic_load_64(&pQueue->qRemainNum)) {
    return false;
  }

  SStmQNode *orig = pQueue->head;

  SStmQNode *node = pQueue->head->next;
  pQueue->head = pQueue->head->next;

  *param = node;

  taosMemoryFreeClear(orig);

  (void)atomic_sub_fetch_64(&pQueue->qRemainNum, 1);

  return true;
}

void mndStreamActionEnqueue(SStmActionQ* pQueue, SStmQNode* param) {
  taosWLockLatch(&pQueue->lock);
  pQueue->tail->next = param;
  pQueue->tail = param;
  taosWUnLockLatch(&pQueue->lock);

  (void)atomic_add_fetch_64(&pQueue->qRemainNum, 1);
}

char* mstGetStreamActionString(int32_t action) {
  switch (action) {
    case STREAM_ACT_DEPLOY:
      return "DEPLOY";
    case STREAM_ACT_UNDEPLOY:
      return "UNDEPLOY";
    case STREAM_ACT_START:
      return "START";
    case STREAM_ACT_UPDATE_TRIGGER:
      return "UPDATE TRIGGER";
    case STREAM_ACT_RECALC:
      return "USER RECALC";
    default:
      break;
  }

  return "UNKNOWN";
}

void mstPostStreamAction(SStmActionQ*       actionQ, int64_t streamId, char* streamName, void* param, bool userAction, int32_t action) {
  SStmQNode *pNode = taosMemoryMalloc(sizeof(SStmQNode));
  if (NULL == pNode) {
    taosMemoryFreeClear(param);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, __LINE__, tstrerror(terrno));
    return;
  }

  pNode->type = action;
  pNode->streamAct = true;
  pNode->action.stream.streamId = streamId;
  TAOS_STRCPY(pNode->action.stream.streamName, streamName);
  pNode->action.stream.userAction = userAction;
  pNode->action.stream.actionParam = param;
  
  pNode->next = NULL;

  mndStreamActionEnqueue(actionQ, pNode);

  mstsDebug("stream action %s posted enqueue", mstGetStreamActionString(action));
}

void mstPostTaskAction(SStmActionQ*        actionQ, SStmTaskAction* pAction, int32_t action) {
  SStmQNode *pNode = taosMemoryMalloc(sizeof(SStmQNode));
  if (NULL == pNode) {
    int64_t streamId = pAction->streamId;
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, __LINE__, tstrerror(terrno));
    return;
  }

  pNode->type = action;
  pNode->streamAct = false;
  pNode->action.task = *pAction;
  
  pNode->next = NULL;

  mndStreamActionEnqueue(actionQ, pNode);
}

void mstDestroyDbVgroupsHash(SSHashObj *pDbVgs) {
  int32_t iter = 0;
  SDBVgHashInfo* pVg = NULL;
  void* p = NULL;
  while (NULL != (p = tSimpleHashIterate(pDbVgs, p, &iter))) {
    pVg = (SDBVgHashInfo*)p;
    taosArrayDestroy(pVg->vgArray);
  }
  
  tSimpleHashCleanup(pDbVgs);
}


int32_t mstBuildDBVgroupsMap(SMnode* pMnode, SSHashObj** ppRes) {
  void*   pIter = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pTarget = NULL;
  SArray* pNew = NULL;
  SDbObj* pDb = NULL;
  SDBVgHashInfo dbInfo = {0}, *pDbInfo = NULL;
  SVgObj* pVgroup = NULL;

  SSHashObj* pDbVgroup = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  TSDB_CHECK_NULL(pDbVgroup, code, lino, _exit, terrno);

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    pDbInfo = (SDBVgHashInfo*)tSimpleHashGet(pDbVgroup, pVgroup->dbName, strlen(pVgroup->dbName) + 1);
    if (NULL == pDbInfo) {
      pNew = taosArrayInit(20, sizeof(SVGroupHashInfo));
      if (NULL == pNew) {
        sdbRelease(pMnode->pSdb, pVgroup);
        sdbCancelFetch(pMnode->pSdb, pIter);
        pVgroup = NULL;
        TSDB_CHECK_NULL(pNew, code, lino, _exit, terrno);
      }
      
      pDb = mndAcquireDb(pMnode, pVgroup->dbName);
      if (NULL == pDb) {
        sdbRelease(pMnode->pSdb, pVgroup);
        sdbCancelFetch(pMnode->pSdb, pIter);      
        pVgroup = NULL;
        TSDB_CHECK_NULL(pDb, code, lino, _exit, terrno);
      }
      dbInfo.vgSorted = false;
      dbInfo.hashMethod = pDb->cfg.hashMethod;
      dbInfo.hashPrefix = pDb->cfg.hashPrefix;
      dbInfo.hashSuffix = pDb->cfg.hashSuffix;
      dbInfo.vgArray = pNew;
      
      mndReleaseDb(pMnode, pDb);

      pTarget = pNew;
    } else {
      pTarget = pDbInfo->vgArray;
    }

    SVGroupHashInfo vgInfo = {.vgId = pVgroup->vgId, .hashBegin = pVgroup->hashBegin, .hashEnd = pVgroup->hashEnd};
    if (NULL == taosArrayPush(pTarget, &vgInfo)) {
      sdbRelease(pMnode->pSdb, pVgroup);
      sdbCancelFetch(pMnode->pSdb, pIter);      
      pVgroup = NULL;
      TSDB_CHECK_NULL(NULL, code, lino, _exit, terrno);
    }

    if (NULL == pDbInfo) {
      code = tSimpleHashPut(pDbVgroup, pVgroup->dbName, strlen(pVgroup->dbName) + 1, &dbInfo, sizeof(dbInfo));
      if (code) {
        sdbRelease(pMnode->pSdb, pVgroup);
        sdbCancelFetch(pMnode->pSdb, pIter);      
        pVgroup = NULL;
        TAOS_CHECK_EXIT(code);
      }
      pNew = NULL;
    }

    sdbRelease(pMnode->pSdb, pVgroup);
    pVgroup = NULL;
  }

  *ppRes = pDbVgroup;
  
_exit:

  taosArrayDestroy(pNew);

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int mstDbVgInfoComp(const void* lp, const void* rp) {
  SVGroupHashInfo* pLeft = (SVGroupHashInfo*)lp;
  SVGroupHashInfo* pRight = (SVGroupHashInfo*)rp;
  if (pLeft->hashBegin < pRight->hashBegin) {
    return -1;
  } else if (pLeft->hashBegin > pRight->hashBegin) {
    return 1;
  }

  return 0;
}

int32_t mstTableHashValueComp(void const* lp, void const* rp) {
  uint32_t*    key = (uint32_t*)lp;
  SVgroupInfo* pVg = (SVgroupInfo*)rp;

  if (*key < pVg->hashBegin) {
    return -1;
  } else if (*key > pVg->hashEnd) {
    return 1;
  }

  return 0;
}


int32_t mstGetTableVgId(SSHashObj* pDbVgroups, char* dbFName, char *tbName, int32_t* vgId) {
  int32_t code = 0;
  int32_t lino = 0;
  SVgroupInfo* vgInfo = NULL;
  char         tbFullName[TSDB_TABLE_FNAME_LEN];

  SDBVgHashInfo* dbInfo = (SDBVgHashInfo*)tSimpleHashGet(pDbVgroups, dbFName, strlen(dbFName) + 1);
  if (NULL == dbInfo) {
    mstError("db %s does not exist", dbFName);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_DB_NOT_EXIST);
  }
  
  (void)snprintf(tbFullName, sizeof(tbFullName), "%s.%s", dbFName, tbName);
  uint32_t hashValue = taosGetTbHashVal(tbFullName, (uint32_t)strlen(tbFullName), dbInfo->hashMethod,
                                        dbInfo->hashPrefix, dbInfo->hashSuffix);

  if (!dbInfo->vgSorted) {
    taosArraySort(dbInfo->vgArray, mstDbVgInfoComp);
    dbInfo->vgSorted = true;
  }

  vgInfo = taosArraySearch(dbInfo->vgArray, &hashValue, mstTableHashValueComp, TD_EQ);
  if (NULL == vgInfo) {
    mstError("no hash range found for hash value [%u], dbFName:%s, numOfVgId:%d", hashValue, dbFName,
             (int32_t)taosArrayGetSize(dbInfo->vgArray));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  *vgId = vgInfo->vgId;

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


void mstLogSStreamObj(char* tips, SStreamObj* p) {
  if (!(stDebugFlag & DEBUG_DEBUG)) {
    return;
  }
  
  if (NULL == p) {
    mstDebug("%s: stream is NULL", tips);
    return;
  }

  mstDebug("%s: stream obj", tips);
  mstDebug("name:%s mainSnodeId:%d userDropped:%d userStopped:%d createTime:%" PRId64 " updateTime:%" PRId64,
      p->name, p->mainSnodeId, p->userDropped, p->userStopped, p->createTime, p->updateTime);

  SCMCreateStreamReq* q = p->pCreate;
  if (NULL == q) {
    mstDebug("stream pCreate is NULL");
    return;
  }

  int64_t streamId = q->streamId;
  int32_t calcDBNum = taosArrayGetSize(q->calcDB);
  int32_t calcScanNum = taosArrayGetSize(q->calcScanPlanList);
  int32_t notifyUrlNum = taosArrayGetSize(q->pNotifyAddrUrls);
  int32_t outColNum = taosArrayGetSize(q->outCols);
  int32_t outTagNum = taosArrayGetSize(q->outTags);
  int32_t forceOutColNum = taosArrayGetSize(q->forceOutCols);

  mstsDebugL("create_info: name:%s sql:%s streamDB:%s triggerDB:%s outDB:%s calcDBNum:%d triggerTblName:%s outTblName:%s "
      "igExists:%d triggerType:%d igDisorder:%d deleteReCalc:%d deleteOutTbl:%d fillHistory:%d fillHistroyFirst:%d "
      "calcNotifyOnly:%d lowLatencyCalc:%d igNoDataTrigger:%d notifyUrlNum:%d notifyEventTypes:%d addOptions:%d notifyHistory:%d "
      "outColsNum:%d outTagsNum:%d maxDelay:%" PRId64 " fillHistoryStartTs:%" PRId64 " watermark:%" PRId64 " expiredTime:%" PRId64 " "
      "triggerTblType:%d triggerTblUid:%" PRIx64 " triggerTblSuid:%" PRIx64 " vtableCalc:%d outTblType:%d outStbExists:%d outStbUid:%" PRIu64 " outStbSversion:%d "
      "eventTypes:0x%" PRIx64 " flags:0x%" PRIx64 " tsmaId:0x%" PRIx64 " placeHolderBitmap:0x%" PRIx64 " calcTsSlotId:%d triTsSlotId:%d "
      "triggerTblVgId:%d outTblVgId:%d calcScanPlanNum:%d forceOutCols:%d",
      q->name, q->sql, q->streamDB, q->triggerDB, q->outDB, calcDBNum, q->triggerTblName, q->outTblName,
      q->igExists, q->triggerType, q->igDisorder, q->deleteReCalc, q->deleteOutTbl, q->fillHistory, q->fillHistoryFirst,
      q->calcNotifyOnly, q->lowLatencyCalc, q->igNoDataTrigger, notifyUrlNum, q->notifyEventTypes, q->addOptions, q->notifyHistory,
      outColNum, outTagNum, q->maxDelay, q->fillHistoryStartTime, q->watermark, q->expiredTime,
      q->triggerTblType, q->triggerTblUid, q->triggerTblSuid, q->vtableCalc, q->outTblType, q->outStbExists, q->outStbUid, q->outStbSversion,
      q->eventTypes, q->flags, q->tsmaId, q->placeHolderBitmap, q->calcTsSlotId, q->triTsSlotId,
      q->triggerTblVgId, q->outTblVgId, calcScanNum, forceOutColNum);

  switch (q->triggerType) {
    case WINDOW_TYPE_INTERVAL: {
      SSlidingTrigger* t = &q->trigger.sliding;
      mstsDebug("sliding trigger options, intervalUnit:%d, slidingUnit:%d, offsetUnit:%d, soffsetUnit:%d, precision:%d, interval:%" PRId64 ", offset:%" PRId64 ", sliding:%" PRId64 ", soffset:%" PRId64, 
          t->intervalUnit, t->slidingUnit, t->offsetUnit, t->soffsetUnit, t->precision, t->interval, t->offset, t->sliding, t->soffset);
      break;
    }  
    case WINDOW_TYPE_SESSION: {
      SSessionTrigger* t = &q->trigger.session;
      mstsDebug("session trigger options, slotId:%d, sessionVal:%" PRId64, t->slotId, t->sessionVal);
      break;
    }
    case WINDOW_TYPE_STATE: {
      SStateWinTrigger* t = &q->trigger.stateWin;
      mstsDebug("state trigger options, slotId:%d, expr:%s, extend:%d, trueForDuration:%" PRId64, t->slotId, (char *)t->expr, t->extend, t->trueForDuration);
      break;
    }
    case WINDOW_TYPE_EVENT:{
      SEventTrigger* t = &q->trigger.event;
      mstsDebug("event trigger options, startCond:%s, endCond:%s, trueForDuration:%" PRId64, (char*)t->startCond, (char*)t->endCond, t->trueForDuration);
      break;
    }
    case WINDOW_TYPE_COUNT: {
      SCountTrigger* t = &q->trigger.count;
      mstsDebug("count trigger options, countVal:%" PRId64 ", sliding:%" PRId64 ", condCols:%s", t->countVal, t->sliding, (char*)t->condCols);
      break;
    }
    case WINDOW_TYPE_PERIOD: {
      SPeriodTrigger* t = &q->trigger.period;
      mstsDebug("period trigger options, periodUnit:%d, offsetUnit:%d, precision:%d, period:%" PRId64 ", offset:%" PRId64, 
          t->periodUnit, t->offsetUnit, t->precision, t->period, t->offset);
      break;
    }
    default:
      mstsDebug("unknown triggerType:%d", q->triggerType);
      break;
  }

  mstsDebugL("create_info: triggerCols:[%s]", (char*)q->triggerCols);

  mstsDebugL("create_info: partitionCols:[%s]", (char*)q->partitionCols);

  mstsDebugL("create_info: triggerScanPlan:[%s]", (char*)q->triggerScanPlan);

  mstsDebugL("create_info: calcPlan:[%s]", (char*)q->calcPlan);

  mstsDebugL("create_info: subTblNameExpr:[%s]", (char*)q->subTblNameExpr);

  mstsDebugL("create_info: tagValueExpr:[%s]", (char*)q->tagValueExpr);


  for (int32_t i = 0; i < calcDBNum; ++i) {
    char* dbName = taosArrayGetP(q->calcDB, i);
    mstsDebug("create_info: calcDB[%d] - %s", i, dbName);
  }

  for (int32_t i = 0; i < calcScanNum; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(q->calcScanPlanList, i);
    int32_t vgNum = taosArrayGetSize(pScan->vgList);
    mstsDebugL("create_info: calcScanPlan[%d] - readFromCache:%d vgNum:%d scanPlan:[%s]", i, pScan->readFromCache, vgNum, (char*)pScan->scanPlan);
    for (int32_t v = 0; v < vgNum; ++v) {
      mstsDebug("create_info: calcScanPlan[%d] vg[%d] - vgId:%d", i, v, *(int32_t*)taosArrayGet(pScan->vgList, v));
    }
  }

  for (int32_t i = 0; i < notifyUrlNum; ++i) {
    char* url = taosArrayGetP(q->pNotifyAddrUrls, i);
    mstsDebug("create_info: notifyUrl[%d] - %s", i, url);
  }

  for (int32_t i = 0; i < outColNum; ++i) {
    SFieldWithOptions* o = taosArrayGet(q->outCols, i);
    mstsDebug("create_info: outCol[%d] - name:%s type:%d flags:%d bytes:%d compress:%u typeMod:%d", 
        i, o->name, o->type, o->flags, o->bytes, o->compress, o->typeMod);
  }
      
}

void mstLogSStmTaskStatus(char* name, int64_t streamId, SStmTaskStatus* pTask, int32_t idx) {
  mstsDebug("%s[%d]: task %" PRIx64 " deployId:%d SID:%" PRId64 " nodeId:%d tidx:%d type:%s flags:%" PRIx64 " status:%s lastUpTs:%" PRId64, 
      name, idx, pTask->id.taskId, pTask->id.deployId, pTask->id.seriousId, pTask->id.nodeId, pTask->id.taskIdx,
      gStreamTaskTypeStr[pTask->type], pTask->flags, gStreamStatusStr[pTask->status], pTask->lastUpTs);
}

void mstLogSStmStatus(char* tips, int64_t streamId, SStmStatus* p) {
  if (!(stDebugFlag & DEBUG_DEBUG)) {
    return;
  }
  
  if (NULL == p) {
    mstsDebug("%s: stream status is NULL", tips);
    return;
  }

  int32_t trigReaderNum = taosArrayGetSize(p->trigReaders);
  int32_t trigOReaderNum = taosArrayGetSize(p->trigOReaders);
  int32_t calcReaderNum = MST_LIST_SIZE(p->calcReaders);
  int32_t triggerNum = p->triggerTask ? 1 : 0;
  int32_t runnerNum = 0;

  for (int32_t i = 0; i < p->runnerDeploys; ++i) {
    runnerNum += taosArrayGetSize(p->runners[i]);
  }

  mstsDebug("%s: stream status", tips);
  mstsDebug("name:%s runnerNum:%d runnerDeploys:%d runnerReplica:%d lastActionTs:%" PRId64
           " trigReaders:%d trigOReaders:%d calcReaders:%d trigger:%d runners:%d",
      p->streamName, p->runnerNum, p->runnerDeploys, p->runnerReplica, p->lastActionTs,
      trigReaderNum, trigOReaderNum, calcReaderNum, triggerNum, runnerNum);

  SStmTaskStatus* pTask = NULL;
  for (int32_t i = 0; i < trigReaderNum; ++i) {
    pTask = taosArrayGet(p->trigReaders, i);
    mstLogSStmTaskStatus("trigReader task", streamId, pTask, i);
  }

  for (int32_t i = 0; i < trigOReaderNum; ++i) {
    pTask = taosArrayGet(p->trigOReaders, i);
    mstLogSStmTaskStatus("trigOReader task", streamId, pTask, i);
  }

  SListNode* pNode = listHead(p->calcReaders);
  for (int32_t i = 0; i < calcReaderNum; ++i) {
    pTask = (SStmTaskStatus*)pNode->data;
    mstLogSStmTaskStatus("calcReader task", streamId, pTask, i);
    pNode = TD_DLIST_NODE_NEXT(pNode);
  }

  if (triggerNum > 0) {
    mstLogSStmTaskStatus("trigger task", streamId, p->triggerTask, 0);
  }

  for (int32_t i = 0; i < p->runnerDeploys; ++i) {
    int32_t num = taosArrayGetSize(p->runners[i]);
    if (num <= 0) {
      continue;
    }
    
    mstsDebug("the %dth deploy runners status", i);
    for (int32_t m = 0; m < num; ++m) {
      pTask = taosArrayGet(p->runners[i], m);
      mstLogSStmTaskStatus("runner task", streamId, pTask, m);
    }
  }
      
}

bool mstEventPassIsolation(int32_t num, int32_t event) {
  bool ret = ((mStreamMgmt.lastTs[event].ts + num * MST_SHORT_ISOLATION_DURATION) <= mStreamMgmt.hCtx.currentTs);
  if (ret) {
    mstDebug("event %s passed %d isolation, last:%" PRId64 ", curr:%" PRId64, 
        gMndStreamEvent[event], num, mStreamMgmt.lastTs[event].ts, mStreamMgmt.hCtx.currentTs);
  }

  return ret;
}

bool mstEventHandledChkSet(int32_t event) {
  if (0 == atomic_val_compare_exchange_8((int8_t*)&mStreamMgmt.lastTs[event].handled, 0, 1)) {
    mstDebug("event %s set handled", gMndStreamEvent[event]);
    return true;
  }
  return false;
}

int32_t mstGetStreamStatusStr(SStreamObj* pStream, char* status, int32_t statusSize, char* msg, int32_t msgSize) {
  int8_t active = atomic_load_8(&mStreamMgmt.active), state = atomic_load_8(&mStreamMgmt.state);
  if (0 == active || MND_STM_STATE_NORMAL != state) {
    mstDebug("mnode streamMgmt not in active mode, active:%d, state:%d", active, state);
    STR_WITH_MAXSIZE_TO_VARSTR(status, gStreamStatusStr[STREAM_STATUS_UNDEPLOYED], statusSize);
    STR_WITH_MAXSIZE_TO_VARSTR(msg, "Mnode may be unstable, try again later", msgSize);
    return TSDB_CODE_SUCCESS;
  }

  if (atomic_load_8(&pStream->userDropped)) {
    STR_WITH_MAXSIZE_TO_VARSTR(status, gStreamStatusStr[STREAM_STATUS_DROPPING], statusSize);
    STR_WITH_MAXSIZE_TO_VARSTR(msg, "", msgSize);
    return TSDB_CODE_SUCCESS;
  }

  if (atomic_load_8(&pStream->userStopped)) {
    STR_WITH_MAXSIZE_TO_VARSTR(status, gStreamStatusStr[STREAM_STATUS_STOPPED], statusSize);
    STR_WITH_MAXSIZE_TO_VARSTR(msg, "", msgSize);
    return TSDB_CODE_SUCCESS;
  }

  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);
  
  SStmStatus* pStatus = (SStmStatus*)taosHashGet(mStreamMgmt.streamMap, &pStream->pCreate->streamId, sizeof(pStream->pCreate->streamId));
  if (NULL == pStatus) {
    STR_WITH_MAXSIZE_TO_VARSTR(status, gStreamStatusStr[STREAM_STATUS_UNDEPLOYED], statusSize);
    STR_WITH_MAXSIZE_TO_VARSTR(msg, "", msgSize);
    goto _exit;
  }

  char tmpBuf[256];
  int8_t stopped = atomic_load_8(&pStatus->stopped);
  switch (stopped) {
    case 1:
      STR_WITH_MAXSIZE_TO_VARSTR(status, gStreamStatusStr[STREAM_STATUS_FAILED], statusSize);
      snprintf(tmpBuf, sizeof(tmpBuf), "Last error: %s, Failed times: %" PRId64, tstrerror(pStatus->fatalError), pStatus->fatalRetryTimes);
      STR_WITH_MAXSIZE_TO_VARSTR(msg, tmpBuf, msgSize);
      goto _exit;
      break;
    case 4:
      STR_WITH_MAXSIZE_TO_VARSTR(status, gStreamStatusStr[STREAM_STATUS_FAILED], statusSize);
      snprintf(tmpBuf, sizeof(tmpBuf), "Error: %s", tstrerror(TSDB_CODE_GRANT_STREAM_EXPIRED));
      STR_WITH_MAXSIZE_TO_VARSTR(msg, tmpBuf, msgSize);
      goto _exit;
      break;
    default:
      break;
  }

  if (pStatus->triggerTask && STREAM_STATUS_RUNNING == pStatus->triggerTask->status) {
    STR_WITH_MAXSIZE_TO_VARSTR(status, gStreamStatusStr[STREAM_STATUS_RUNNING], statusSize);
    strcpy(tmpBuf, "Running start from: ");
    (void)formatTimestampLocal(&tmpBuf[strlen(tmpBuf)], pStatus->triggerTask->runningStartTs, TSDB_TIME_PRECISION_MILLI);
    STR_WITH_MAXSIZE_TO_VARSTR(msg, tmpBuf, msgSize);
    goto _exit;
  }

  STR_WITH_MAXSIZE_TO_VARSTR(status, gStreamStatusStr[STREAM_STATUS_INIT], statusSize);
  snprintf(tmpBuf, sizeof(tmpBuf), "Current deploy times: %" PRId64, pStatus->deployTimes);
  STR_WITH_MAXSIZE_TO_VARSTR(msg, tmpBuf, msgSize);
  goto _exit;

_exit:
  
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);

  return TSDB_CODE_SUCCESS;
}

int32_t mstSetStreamAttrResBlock(SMnode *pMnode, SStreamObj* pStream, SSDataBlock* pBlock, int32_t numOfRows) {
  int32_t code = 0;
  int32_t cols = 0;
  int32_t lino = 0;

  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetStableStr(pStream->name), sizeof(streamName));
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char*)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // db_name
  char streamDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamDB, mndGetDbStr(pStream->pCreate->streamDB), sizeof(streamDB));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&streamDB, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // create time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&pStream->createTime, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stream id
  char streamId2[19] = {0};
  char streamId[19 + VARSTR_HEADER_SIZE] = {0};
  snprintf(streamId2, sizeof(streamId2), "%" PRIx64, pStream->pCreate->streamId);
  STR_WITH_MAXSIZE_TO_VARSTR(streamId, streamId2, sizeof(streamId));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)streamId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // sql
  char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(sql, pStream->pCreate->sql, sizeof(sql));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)sql, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // status
  char   status[20 + VARSTR_HEADER_SIZE] = {0};
  char msg[TSDB_RESERVE_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};
  code = mstGetStreamStatusStr(pStream, status, sizeof(status), msg, sizeof(msg));
  TSDB_CHECK_CODE(code, lino, _end);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&status, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // snodeLeader
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&pStream->mainSnodeId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // snodeReplica
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  SSnodeObj* pSnode = mndAcquireSnode(pMnode, pStream->mainSnodeId);
  int32_t replicaSnodeId = pSnode ? pSnode->replicaId : -1;
  mndReleaseSnode(pMnode, pSnode);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&replicaSnodeId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // msg
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)msg, false);

_end:
  if (code) {
    mError("error happens when build stream attr result block, lino:%d, code:%s", lino, tstrerror(code));
  }
  return code;
}


int32_t mstGetTaskStatusStr(SStmTaskStatus* pTask, char* status, int32_t statusSize, char* msg, int32_t msgSize) {
  char tmpBuf[256];
  
  STR_WITH_MAXSIZE_TO_VARSTR(status, gStreamStatusStr[pTask->status], statusSize);
  if (STREAM_STATUS_FAILED == pTask->status && pTask->errCode) {
    snprintf(tmpBuf, sizeof(tmpBuf), "Last error: %s", tstrerror(pTask->errCode));
    STR_WITH_MAXSIZE_TO_VARSTR(msg, tmpBuf, msgSize);
    return TSDB_CODE_SUCCESS;
  }

  if (STREAM_TRIGGER_TASK == pTask->type && mstWaitLock(&pTask->detailStatusLock, true)) {
    if (pTask->detailStatus) {
      SSTriggerRuntimeStatus* pTrigger = (SSTriggerRuntimeStatus*)pTask->detailStatus;
      snprintf(tmpBuf, sizeof(tmpBuf), "Current RT/HI/RE session num: %d/%d/%d, histroy progress:%d%%, total AUTO/USER recalc num: %d/%d", 
          pTrigger->realtimeSessionNum, pTrigger->historySessionNum, pTrigger->recalcSessionNum, pTrigger->histroyProgress,
          pTrigger->autoRecalcNum, (int32_t)taosArrayGetSize(pTrigger->userRecalcs));
      taosRUnLockLatch(&pTask->detailStatusLock);
      return TSDB_CODE_SUCCESS;
    }

    taosRUnLockLatch(&pTask->detailStatusLock);    
  }
  
  STR_WITH_MAXSIZE_TO_VARSTR(msg, "", msgSize);
  
  return TSDB_CODE_SUCCESS;
}

int32_t mstGetTaskExtraStr(SStmTaskStatus* pTask, char* extraStr, int32_t extraSize) {
  switch (pTask->type) {
    case STREAM_READER_TASK:
      if (STREAM_IS_TRIGGER_READER(pTask->flags)) {
        STR_WITH_MAXSIZE_TO_VARSTR(extraStr, "trigReader", extraSize);
      } else {
        STR_WITH_MAXSIZE_TO_VARSTR(extraStr, "calcReader", extraSize);
      }
      return TSDB_CODE_SUCCESS;
    case STREAM_RUNNER_TASK:
      if (STREAM_IS_TOP_RUNNER(pTask->flags)) {
        STR_WITH_MAXSIZE_TO_VARSTR(extraStr, "topRunner", extraSize);
        return TSDB_CODE_SUCCESS;
      }
      break;
    default:
      break;
  }

  STR_WITH_MAXSIZE_TO_VARSTR(extraStr, "", extraSize);
  return TSDB_CODE_SUCCESS;
}


int32_t mstSetStreamTaskResBlock(SStreamObj* pStream, SStmTaskStatus* pTask, SSDataBlock* pBlock, int32_t numOfRows) {
  int32_t code = 0;
  int32_t cols = 0;
  int32_t lino = 0;

  // stream_name
  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetStableStr(pStream->name), sizeof(streamName));
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char*)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stream id
  char idstr[19 + VARSTR_HEADER_SIZE] = {0};
  snprintf(&idstr[VARSTR_HEADER_SIZE], sizeof(idstr) - VARSTR_HEADER_SIZE, "%" PRIx64, pStream->pCreate->streamId);
  varDataSetLen(idstr, strlen(&idstr[VARSTR_HEADER_SIZE])); 
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)idstr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // task id
  snprintf(&idstr[VARSTR_HEADER_SIZE], sizeof(idstr) - VARSTR_HEADER_SIZE, "%" PRIx64, pTask->id.taskId);
  varDataSetLen(idstr, strlen(&idstr[VARSTR_HEADER_SIZE]));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)idstr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // type
  char type[20 + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(type, (STREAM_READER_TASK == pTask->type) ? "Reader" : ((STREAM_TRIGGER_TASK == pTask->type) ? "Trigger" : "Runner"), sizeof(type));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)type, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // serious id
  snprintf(&idstr[VARSTR_HEADER_SIZE], sizeof(idstr) - VARSTR_HEADER_SIZE, "%" PRIx64, pTask->id.seriousId);
  varDataSetLen(idstr, strlen(&idstr[VARSTR_HEADER_SIZE]));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)idstr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // deploy id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&pTask->id.deployId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // node_type
  char nodeType[10 + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(nodeType, (STREAM_READER_TASK == pTask->type) ? "vnode" : "snode", sizeof(nodeType));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)nodeType, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // node id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&pTask->id.nodeId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // task idx
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&pTask->id.taskIdx, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // status
  char   status[20 + VARSTR_HEADER_SIZE] = {0};
  char msg[TSDB_RESERVE_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};
  code = mstGetTaskStatusStr(pTask, status, sizeof(status), msg, sizeof(msg));
  TSDB_CHECK_CODE(code, lino, _end);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&status, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  if (pTask->runningStartTs) {
    code = colDataSetVal(pColInfo, numOfRows, (const char*)&pTask->runningStartTs, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, (const char*)&pTask->runningStartTs, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // last update
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  if (pTask->lastUpTs) {
    code = colDataSetVal(pColInfo, numOfRows, (const char*)&pTask->lastUpTs, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, (const char*)&pTask->lastUpTs, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // extra info
  char extra[64 + VARSTR_HEADER_SIZE] = {0};
  code = mstGetTaskExtraStr(pTask, extra, sizeof(extra));
  TSDB_CHECK_CODE(code, lino, _end);
  
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)extra, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // msg
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)msg, false);

_end:
  if (code) {
    mError("error happens when build stream attr result block, lino:%d, code:%s", lino, tstrerror(code));
  }
  return code;
}

int32_t mstGetNumOfStreamTasks(SStmStatus* pStatus) {
  int32_t num = taosArrayGetSize(pStatus->trigReaders) + taosArrayGetSize(pStatus->trigOReaders) + MST_LIST_SIZE(pStatus->calcReaders) + (pStatus->triggerTask ? 1 : 0);
  for (int32_t i = 0; i < MND_STREAM_RUNNER_DEPLOY_NUM; ++i) {
    num += taosArrayGetSize(pStatus->runners[i]);
  }

  return num;
}

int32_t mstSetStreamTasksResBlock(SStreamObj* pStream, SSDataBlock* pBlock, int32_t* numOfRows, int32_t rowsCapacity) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  bool    statusLocked = false;

  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);

  SStmStatus* pStatus = (SStmStatus*)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstsDebug("stream not in streamMap, ignore it, dropped:%d, stopped:%d", atomic_load_8(&pStream->userDropped), atomic_load_8(&pStream->userStopped));
    goto _exit;
  }

  int8_t stopped = atomic_load_8(&pStatus->stopped);
  if (stopped) {
    mstsDebug("stream stopped %d, ignore it", stopped);
    goto _exit;
  }

  (void)mstWaitLock(&pStatus->resetLock, true);
  statusLocked = true;
  
  int32_t count = mstGetNumOfStreamTasks(pStatus);

  if (*numOfRows + count > rowsCapacity) {
    code = blockDataEnsureCapacity(pBlock, *numOfRows + count);
    if (code) {
      mstError("failed to prepare the result block buffer, rows:%d", *numOfRows + count);
      TAOS_CHECK_EXIT(code);
    }
  }

  SStmTaskStatus* pTask = NULL;
  int32_t trigReaderNum = taosArrayGetSize(pStatus->trigReaders);
  for (int32_t i = 0; i < trigReaderNum; ++i) {
    pTask = taosArrayGet(pStatus->trigReaders, i);
  
    code = mstSetStreamTaskResBlock(pStream, pTask, pBlock, *numOfRows);
    if (code == TSDB_CODE_SUCCESS) {
      (*numOfRows)++;
    }
  }

  trigReaderNum = taosArrayGetSize(pStatus->trigOReaders);
  for (int32_t i = 0; i < trigReaderNum; ++i) {
    pTask = taosArrayGet(pStatus->trigOReaders, i);
  
    code = mstSetStreamTaskResBlock(pStream, pTask, pBlock, *numOfRows);
    if (code == TSDB_CODE_SUCCESS) {
      (*numOfRows)++;
    }
  }

  if (pStatus->calcReaders) {
    int32_t calcReaderNum = MST_LIST_SIZE(pStatus->calcReaders);
    SListNode* pNode = listHead(pStatus->calcReaders);
    for (int32_t i = 0; i < calcReaderNum; ++i) {
      pTask = (SStmTaskStatus*)pNode->data;
    
      code = mstSetStreamTaskResBlock(pStream, pTask, pBlock, *numOfRows);
      if (code == TSDB_CODE_SUCCESS) {
        (*numOfRows)++;
      }
      pNode = TD_DLIST_NODE_NEXT(pNode);
    }
  }

  if (pStatus->triggerTask) {
    code = mstSetStreamTaskResBlock(pStream, pStatus->triggerTask, pBlock, *numOfRows);
    if (code == TSDB_CODE_SUCCESS) {
      (*numOfRows)++;
    }
  }

  int32_t runnerNum = 0;
  for (int32_t i = 0; i < MND_STREAM_RUNNER_DEPLOY_NUM; ++i) {
    runnerNum = taosArrayGetSize(pStatus->runners[i]);
    for (int32_t m = 0; m < runnerNum; ++m) {
      pTask = taosArrayGet(pStatus->runners[i], m);
    
      code = mstSetStreamTaskResBlock(pStream, pTask, pBlock, *numOfRows);
      if (code == TSDB_CODE_SUCCESS) {
        (*numOfRows)++;
      }
    }
  }
  
  pBlock->info.rows = *numOfRows;

_exit:

  if (statusLocked) {
    taosRUnLockLatch(&pStatus->resetLock);
  }
  
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);

  if (code) {
    mError("error happens when build stream tasks result block, lino:%d, code:%s", lino, tstrerror(code));
  }
  
  return code;
}


int32_t mstAppendNewRecalcRange(int64_t streamId, SStmStatus *pStream, STimeWindow* pRange) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    locked = false;
  SArray* userRecalcList = NULL;

  SStreamRecalcReq req = {.recalcId = 0, .start = pRange->skey, .end = pRange->ekey};
  TAOS_CHECK_EXIT(taosGetSystemUUIDU64(&req.recalcId));
  
  taosWLockLatch(&pStream->userRecalcLock);
  locked = true;
  
  if (NULL == pStream->userRecalcList) {
    userRecalcList = taosArrayInit(2, sizeof(SStreamRecalcReq));
    if (NULL == userRecalcList) {
      TAOS_CHECK_EXIT(terrno);
    }

    TSDB_CHECK_NULL(taosArrayPush(userRecalcList, &req), code, lino, _exit, terrno);

    atomic_store_ptr(&pStream->userRecalcList, userRecalcList);
    userRecalcList = NULL;    
  } else {
    TSDB_CHECK_NULL(taosArrayPush(pStream->userRecalcList, &req), code, lino, _exit, terrno);
  }
  
  mstsInfo("stream recalc ID:%" PRIx64 " range:%" PRId64 " - %" PRId64 " added", req.recalcId, pRange->skey, pRange->ekey);

_exit:

  taosArrayDestroy(userRecalcList);

  if (locked) {
    taosWUnLockLatch(&pStream->userRecalcLock);
  }
  
  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}



int32_t mstSetStreamRecalculateResBlock(SStreamObj* pStream, SSTriggerRecalcProgress* pProgress, SSDataBlock* pBlock, int32_t numOfRows) {
  int32_t code = 0;
  int32_t cols = 0;
  int32_t lino = 0;

  // stream_name
  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetStableStr(pStream->name), sizeof(streamName));
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char*)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stream id
  char idstr[19 + VARSTR_HEADER_SIZE] = {0};
  snprintf(&idstr[VARSTR_HEADER_SIZE], sizeof(idstr) - VARSTR_HEADER_SIZE, "%" PRIx64, pStream->pCreate->streamId);
  varDataSetLen(idstr, strlen(&idstr[VARSTR_HEADER_SIZE]) + VARSTR_HEADER_SIZE); 
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)idstr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // recalc id
  snprintf(&idstr[VARSTR_HEADER_SIZE], sizeof(idstr) - VARSTR_HEADER_SIZE, "%" PRIx64, pProgress->recalcId);
  varDataSetLen(idstr, strlen(&idstr[VARSTR_HEADER_SIZE]) + VARSTR_HEADER_SIZE);
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)idstr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&pProgress->start, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // end
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)&pProgress->end, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // progress
  char progress[20 + VARSTR_HEADER_SIZE] = {0};
  snprintf(&progress[VARSTR_HEADER_SIZE], sizeof(progress) - VARSTR_HEADER_SIZE, "%d%%", pProgress->progress);
  varDataSetLen(progress, strlen(&progress[VARSTR_HEADER_SIZE]) + VARSTR_HEADER_SIZE);
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char*)progress, false);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code) {
    mError("error happens when build stream attr result block, lino:%d, code:%s", lino, tstrerror(code));
  }
  return code;
}


int32_t mstSetStreamRecalculatesResBlock(SStreamObj* pStream, SSDataBlock* pBlock, int32_t* numOfRows, int32_t rowsCapacity) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);

  SStmStatus* pStatus = (SStmStatus*)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstsDebug("stream not in streamMap, ignore it, dropped:%d, stopped:%d", atomic_load_8(&pStream->userDropped), atomic_load_8(&pStream->userStopped));
    goto _exit;
  }

  int8_t stopped = atomic_load_8(&pStatus->stopped);
  if (stopped) {
    mstsDebug("stream stopped %d, ignore it", stopped);
    goto _exit;
  }

  if (NULL == pStatus->triggerTask) {
    mstsDebug("no trigger task now, deployTimes:%" PRId64 ", ignore it", pStatus->deployTimes);
    goto _exit;
  }

  (void)mstWaitLock(&pStatus->triggerTask->detailStatusLock, true);
  if (NULL == pStatus->triggerTask->detailStatus) {
    mstsDebug("no trigger task now, deployTimes:%" PRId64 ", ignore it", pStatus->deployTimes);
    taosRUnLockLatch(&pStatus->triggerTask->detailStatusLock);
    goto _exit;
  }

  SSTriggerRuntimeStatus* pTrigger = (SSTriggerRuntimeStatus*)pStatus->triggerTask->detailStatus;
  int32_t count = taosArrayGetSize(pTrigger->userRecalcs);

  if (*numOfRows + count > rowsCapacity) {
    code = blockDataEnsureCapacity(pBlock, *numOfRows + count);
    if (code) {
      mstError("failed to prepare the result block buffer, rows:%d", *numOfRows + count);
      taosRUnLockLatch(&pStatus->triggerTask->detailStatusLock);
      TAOS_CHECK_EXIT(code);
    }
  }

  for (int32_t i = 0; i < count; ++i) {
    SSTriggerRecalcProgress* pProgress = taosArrayGet(pTrigger->userRecalcs, i);
  
    code = mstSetStreamRecalculateResBlock(pStream, pProgress, pBlock, *numOfRows);
    if (code == TSDB_CODE_SUCCESS) {
      (*numOfRows)++;
    }
  }

  taosRUnLockLatch(&pStatus->triggerTask->detailStatusLock);
  
  pBlock->info.rows = *numOfRows;

_exit:
  
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);

  if (code) {
    mError("error happens when build stream recalculates result block, lino:%d, code:%s", lino, tstrerror(code));
  }
  
  return code;
}

int32_t mstGetScanUidFromPlan(int64_t streamId, void* scanPlan, int64_t* uid) {
  SSubplan* pSubplan = NULL;
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  
  TAOS_CHECK_EXIT(nodesStringToNode(scanPlan, (SNode**)&pSubplan));

  if (pSubplan->pNode && nodeType(pSubplan->pNode) == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    SScanPhysiNode* pScanNode = (SScanPhysiNode*)pSubplan->pNode;
    *uid = pScanNode->uid;
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  nodesDestroyNode((SNode *)pSubplan);

  return code;
}


