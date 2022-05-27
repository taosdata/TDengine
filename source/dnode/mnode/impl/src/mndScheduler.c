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

#include "mndScheduler.h"
#include "mndConsumer.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndOffset.h"
#include "mndShow.h"
#include "mndSnode.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndSubscribe.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tcompare.h"
#include "tname.h"
#include "tuuid.h"

extern bool tsStreamSchedV;

int32_t mndConvertRSmaTask(const char* ast, int64_t uid, int8_t triggerType, int64_t watermark, char** pStr,
                           int32_t* pLen) {
  SNode*      pAst = NULL;
  SQueryPlan* pPlan = NULL;
  terrno = TSDB_CODE_SUCCESS;

  if (nodesStringToNode(ast, &pAst) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  if (qSetSTableIdForRSma(pAst, uid) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  SPlanContext cxt = {
      .pAstRoot = pAst,
      .topicQuery = false,
      .streamQuery = true,
      .rSmaQuery = true,
      .triggerType = triggerType,
      .watermark = watermark,
  };
  if (qCreateQueryPlan(&cxt, &pPlan, NULL) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
  if (levelNum != 1) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }
  SNodeListNode* inner = nodesListGetNode(pPlan->pSubplans, 0);

  int32_t opNum = LIST_LENGTH(inner->pNodeList);
  if (opNum != 1) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  SSubplan* plan = nodesListGetNode(inner->pNodeList, 0);
  if (qSubPlanToString(plan, pStr, pLen) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

END:
  if (pAst) nodesDestroyNode(pAst);
  if (pPlan) nodesDestroyNode(pPlan);
  return terrno;
}

int32_t mndPersistTaskDeployReq(STrans* pTrans, SStreamTask* pTask, const SEpSet* pEpSet, tmsg_t type, int32_t nodeId) {
  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  tEncodeSStreamTask(&encoder, pTask);
  int32_t size = encoder.pos;
  int32_t tlen = sizeof(SMsgHead) + size;
  tEncoderClear(&encoder);
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  ((SMsgHead*)buf)->vgId = htonl(nodeId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, abuf, size);
  tEncodeSStreamTask(&encoder, pTask);
  tEncoderClear(&encoder);

  STransAction action = {0};
  memcpy(&action.epSet, pEpSet, sizeof(SEpSet));
  action.pCont = buf;
  action.contLen = tlen;
  action.msgType = type;
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(buf);
    return -1;
  }
  return 0;
}

int32_t mndAssignTaskToVg(SMnode* pMnode, STrans* pTrans, SStreamTask* pTask, SSubplan* plan, const SVgObj* pVgroup) {
  int32_t msgLen;
  pTask->nodeId = pVgroup->vgId;
  pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);

  plan->execNode.nodeId = pVgroup->vgId;
  plan->execNode.epSet = pTask->epSet;

  if (qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  mndPersistTaskDeployReq(pTrans, pTask, &plan->execNode.epSet, TDMT_VND_TASK_DEPLOY, pVgroup->vgId);
  return 0;
}

SSnodeObj* mndSchedFetchSnode(SMnode* pMnode) {
  SSnodeObj* pObj = NULL;
  pObj = sdbFetch(pMnode->pSdb, SDB_SNODE, NULL, (void**)&pObj);
  return pObj;
}

int32_t mndAssignTaskToSnode(SMnode* pMnode, STrans* pTrans, SStreamTask* pTask, SSubplan* plan,
                             const SSnodeObj* pSnode) {
  int32_t msgLen;

  pTask->nodeId = 0;
  pTask->epSet = mndAcquireEpFromSnode(pMnode, pSnode);

  plan->execNode.nodeId = 0;
  plan->execNode.epSet = pTask->epSet;

  if (qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  mndPersistTaskDeployReq(pTrans, pTask, &plan->execNode.epSet, TDMT_SND_TASK_DEPLOY, 0);
  return 0;
}

SVgObj* mndSchedFetchOneVg(SMnode* pMnode, int64_t dbUid) {
  void*   pIter = NULL;
  SVgObj* pVgroup = NULL;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != dbUid) {
      sdbRelease(pMnode->pSdb, pVgroup);
      continue;
    }
    return pVgroup;
  }
  return pVgroup;
}

int32_t mndAddShuffledSinkToStream(SMnode* pMnode, STrans* pTrans, SStreamObj* pStream) {
  SSdb*   pSdb = pMnode->pSdb;
  void*   pIter = NULL;
  SArray* tasks = taosArrayGetP(pStream->tasks, 0);

  ASSERT(taosArrayGetSize(pStream->tasks) == 1);

  while (1) {
    SVgObj* pVgroup;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pStream->dbUid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    SStreamTask* pTask = tNewSStreamTask(pStream->uid);
    if (pTask == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    taosArrayPush(tasks, &pTask);

    pTask->nodeId = pVgroup->vgId;
    pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);

    // source
    pTask->sourceType = TASK_SOURCE__MERGE;
    pTask->inputType = TASK_INPUT_TYPE__DATA_BLOCK;

    // exec
    pTask->execType = TASK_EXEC__NONE;

    // sink
    if (pStream->createdBy == STREAM_CREATED_BY__SMA) {
      pTask->sinkType = TASK_SINK__SMA;
      pTask->smaSink.smaId = pStream->smaId;
    } else {
      pTask->sinkType = TASK_SINK__TABLE;
      pTask->tbSink.stbUid = pStream->targetStbUid;
      memcpy(pTask->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
      pTask->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
      ASSERT(pTask->tbSink.pSchemaWrapper);
    }

    // dispatch
    pTask->dispatchType = TASK_DISPATCH__NONE;

    mndPersistTaskDeployReq(pTrans, pTask, &pTask->epSet, TDMT_VND_TASK_DEPLOY, pVgroup->vgId);
  }
  return 0;
}

int32_t mndAddFixedSinkToStream(SMnode* pMnode, STrans* pTrans, SStreamObj* pStream) {
  ASSERT(pStream->fixedSinkVgId != 0);
  SArray*      tasks = taosArrayGetP(pStream->tasks, 0);
  SStreamTask* pTask = tNewSStreamTask(pStream->uid);
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  taosArrayPush(tasks, &pTask);

  pTask->nodeId = pStream->fixedSinkVgId;
#if 0
  SVgObj* pVgroup = mndAcquireVgroup(pMnode, pStream->fixedSinkVgId);
  if (pVgroup == NULL) {
    return -1;
  }
  pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);
#endif
  pTask->epSet = mndGetVgroupEpset(pMnode, &pStream->fixedSinkVg);
  // source
  pTask->sourceType = TASK_SOURCE__MERGE;
  pTask->inputType = TASK_INPUT_TYPE__DATA_BLOCK;

  // exec
  pTask->execType = TASK_EXEC__NONE;

  // sink
  if (pStream->createdBy == STREAM_CREATED_BY__SMA) {
    pTask->sinkType = TASK_SINK__SMA;
    pTask->smaSink.smaId = pStream->smaId;
  } else {
    pTask->sinkType = TASK_SINK__TABLE;
    pTask->tbSink.stbUid = pStream->targetStbUid;
    memcpy(pTask->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    pTask->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
  }

  // dispatch
  pTask->dispatchType = TASK_DISPATCH__NONE;

  /*mndPersistTaskDeployReq(pTrans, pTask, &pTask->epSet, TDMT_VND_TASK_DEPLOY, pVgroup->vgId);*/
  mndPersistTaskDeployReq(pTrans, pTask, &pTask->epSet, TDMT_VND_TASK_DEPLOY, pStream->fixedSinkVg.vgId);

  return 0;
}

int32_t mndScheduleStream(SMnode* pMnode, STrans* pTrans, SStreamObj* pStream) {
  SSdb*       pSdb = pMnode->pSdb;
  SQueryPlan* pPlan = qStringToQueryPlan(pStream->physicalPlan);
  if (pPlan == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  ASSERT(pStream->vgNum == 0);

  int32_t totLevel = LIST_LENGTH(pPlan->pSubplans);
  ASSERT(totLevel <= 2);
  pStream->tasks = taosArrayInit(totLevel, sizeof(void*));

  bool hasExtraSink = false;
  if (totLevel == 2) {
    SArray* taskOneLevel = taosArrayInit(0, sizeof(void*));
    taosArrayPush(pStream->tasks, &taskOneLevel);
    // add extra sink
    hasExtraSink = true;
    if (pStream->fixedSinkVgId == 0) {
      mndAddShuffledSinkToStream(pMnode, pTrans, pStream);
    } else {
      mndAddFixedSinkToStream(pMnode, pTrans, pStream);
    }
  }

  for (int32_t level = 0; level < totLevel; level++) {
    SArray*        taskOneLevel = taosArrayInit(0, sizeof(void*));
    SNodeListNode* inner = nodesListGetNode(pPlan->pSubplans, level);
    ASSERT(LIST_LENGTH(inner->pNodeList) == 1);

    SSubplan* plan = nodesListGetNode(inner->pNodeList, 0);

    // if (level == totLevel - 1 /* or no snode */) {
    if (level == totLevel - 1) {
      // last level, source, must assign to vnode
      // must be scan type
      ASSERT(plan->subplanType == SUBPLAN_TYPE_SCAN);

      // replicate task to each vnode
      void* pIter = NULL;
      while (1) {
        SVgObj* pVgroup;
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
        if (pIter == NULL) break;
        if (pVgroup->dbUid != pStream->dbUid) {
          sdbRelease(pSdb, pVgroup);
          continue;
        }
        SStreamTask* pTask = tNewSStreamTask(pStream->uid);
        // source part
        pTask->sourceType = TASK_SOURCE__SCAN;
        pTask->inputType = TASK_INPUT_TYPE__SUMBIT_BLOCK;

        // sink part
        if (level == 0) {
          // only for inplace
          pTask->sinkType = TASK_SINK__NONE;
          if (!hasExtraSink) {
#if 1
            if (pStream->createdBy == STREAM_CREATED_BY__SMA) {
              pTask->sinkType = TASK_SINK__SMA;
              pTask->smaSink.smaId = pStream->smaId;
            } else {
              pTask->sinkType = TASK_SINK__TABLE;
              pTask->tbSink.stbUid = pStream->targetStbUid;
              memcpy(pTask->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
              pTask->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
            }
#endif
          }
        } else {
          pTask->sinkType = TASK_SINK__NONE;
        }

        // dispatch part
        if (level == 0) {
          pTask->dispatchType = TASK_DISPATCH__NONE;
        } else {
          // add fixed ep dispatcher
          int32_t lastLevel = level - 1;
          ASSERT(lastLevel == 0);
          if (hasExtraSink) lastLevel++;
          SArray* pArray = taosArrayGetP(pStream->tasks, lastLevel);
          // one merge only
          ASSERT(taosArrayGetSize(pArray) == 1);
          SStreamTask* lastLevelTask = taosArrayGetP(pArray, 0);
          /*pTask->dispatchMsgType = TDMT_VND_TASK_MERGE_EXEC;*/
          pTask->dispatchMsgType = TDMT_VND_TASK_DISPATCH;
          pTask->dispatchType = TASK_DISPATCH__FIXED;

          pTask->fixedEpDispatcher.taskId = lastLevelTask->taskId;
          pTask->fixedEpDispatcher.nodeId = lastLevelTask->nodeId;
          pTask->fixedEpDispatcher.epSet = lastLevelTask->epSet;
        }

        // exec part
        pTask->execType = TASK_EXEC__PIPE;
        pTask->exec.parallelizable = 1;
        if (mndAssignTaskToVg(pMnode, pTrans, pTask, plan, pVgroup) < 0) {
          sdbRelease(pSdb, pVgroup);
          qDestroyQueryPlan(pPlan);
          return -1;
        }
        sdbRelease(pSdb, pVgroup);
        taosArrayPush(taskOneLevel, &pTask);
      }
    } else {
      // merge plan

      // TODO if has snode, assign to snode

      // else, assign to vnode
      ASSERT(plan->subplanType == SUBPLAN_TYPE_MERGE);
      SStreamTask* pTask = tNewSStreamTask(pStream->uid);

      // source part, currently only support multi source
      pTask->sourceType = TASK_SOURCE__PIPE;
      pTask->inputType = TASK_INPUT_TYPE__DATA_BLOCK;

      // sink part
      pTask->sinkType = TASK_SINK__NONE;

      // dispatch part
      ASSERT(hasExtraSink);
      /*pTask->dispatchType = TASK_DISPATCH__NONE;*/
#if 1

      if (hasExtraSink) {
        // add dispatcher
        if (pStream->fixedSinkVgId == 0) {
          pTask->dispatchType = TASK_DISPATCH__SHUFFLE;

          /*pTask->dispatchMsgType = TDMT_VND_TASK_WRITE_EXEC;*/
          pTask->dispatchMsgType = TDMT_VND_TASK_DISPATCH;
          SDbObj* pDb = mndAcquireDb(pMnode, pStream->sourceDb);
          ASSERT(pDb);
          if (mndExtractDbInfo(pMnode, pDb, &pTask->shuffleDispatcher.dbInfo, NULL) < 0) {
            sdbRelease(pSdb, pDb);
            qDestroyQueryPlan(pPlan);
            return -1;
          }
          sdbRelease(pSdb, pDb);

          // put taskId to useDbRsp
          // TODO: optimize
          SArray* pVgs = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
          int32_t sz = taosArrayGetSize(pVgs);
          SArray* sinkLv = taosArrayGetP(pStream->tasks, 0);
          int32_t sinkLvSize = taosArrayGetSize(sinkLv);
          for (int32_t i = 0; i < sz; i++) {
            SVgroupInfo* pVgInfo = taosArrayGet(pVgs, i);
            for (int32_t j = 0; j < sinkLvSize; j++) {
              SStreamTask* pLastLevelTask = taosArrayGetP(sinkLv, j);
              /*printf("vgid %d node id %d\n", pVgInfo->vgId, pTask->nodeId);*/
              if (pLastLevelTask->nodeId == pVgInfo->vgId) {
                pVgInfo->taskId = pLastLevelTask->taskId;
                /*printf("taskid %d set to %d\n", pVgInfo->taskId, pTask->taskId);*/
                break;
              }
            }
          }
        } else {
          pTask->dispatchType = TASK_DISPATCH__FIXED;
          /*pTask->dispatchMsgType = TDMT_VND_TASK_WRITE_EXEC;*/
          pTask->dispatchMsgType = TDMT_VND_TASK_DISPATCH;
          SArray* pArray = taosArrayGetP(pStream->tasks, 0);
          // one sink only
          ASSERT(taosArrayGetSize(pArray) == 1);
          SStreamTask* lastLevelTask = taosArrayGetP(pArray, 0);
          pTask->fixedEpDispatcher.taskId = lastLevelTask->taskId;
          pTask->fixedEpDispatcher.nodeId = lastLevelTask->nodeId;
          pTask->fixedEpDispatcher.epSet = lastLevelTask->epSet;
        }
      }
#endif

      // exec part
      pTask->execType = TASK_EXEC__MERGE;
      pTask->exec.parallelizable = 0;
      SVgObj* pVgroup = mndSchedFetchOneVg(pMnode, pStream->dbUid);
      ASSERT(pVgroup);
      if (mndAssignTaskToVg(pMnode, pTrans, pTask, plan, pVgroup) < 0) {
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        return -1;
      }
      sdbRelease(pSdb, pVgroup);
      taosArrayPush(taskOneLevel, &pTask);
    }

    taosArrayPush(pStream->tasks, &taskOneLevel);
  }

  if (totLevel == 2) {
    void* pIter = NULL;
    while (1) {
      SVgObj* pVgroup;
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
      if (pIter == NULL) break;
      if (pVgroup->dbUid != pStream->dbUid) {
        sdbRelease(pSdb, pVgroup);
        continue;
      }
      SStreamTask* pTask = tNewSStreamTask(pStream->uid);

      // source part
      pTask->sourceType = TASK_SOURCE__MERGE;
      pTask->inputType = TASK_INPUT_TYPE__DATA_BLOCK;

      // sink part
      pTask->sinkType = TASK_SINK__NONE;

      // dispatch part
      pTask->dispatchType = TASK_DISPATCH__NONE;

      // exec part
      pTask->execType = TASK_EXEC__NONE;
      pTask->exec.parallelizable = 0;
    }
  }

  // free memory
  qDestroyQueryPlan(pPlan);

  return 0;
}

int32_t mndSchedInitSubEp(SMnode* pMnode, const SMqTopicObj* pTopic, SMqSubscribeObj* pSub) {
  SSdb*       pSdb = pMnode->pSdb;
  SVgObj*     pVgroup = NULL;
  SQueryPlan* pPlan = NULL;
  SSubplan*   plan = NULL;

  if (pTopic->subType == TOPIC_SUB_TYPE__TABLE) {
    pPlan = qStringToQueryPlan(pTopic->physicalPlan);
    if (pPlan == NULL) {
      terrno = TSDB_CODE_QRY_INVALID_INPUT;
      return -1;
    }

    int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
    if (levelNum != 1) {
      qDestroyQueryPlan(pPlan);
      terrno = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
      return -1;
    }

    SNodeListNode* inner = nodesListGetNode(pPlan->pSubplans, 0);

    int32_t opNum = LIST_LENGTH(inner->pNodeList);
    if (opNum != 1) {
      qDestroyQueryPlan(pPlan);
      terrno = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
      return -1;
    }
    plan = nodesListGetNode(inner->pNodeList, 0);
  }

  ASSERT(pSub->unassignedVgs);
  ASSERT(taosHashGetSize(pSub->consumerHash) == 0);

  void* pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pTopic->dbUid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    pSub->vgNum++;

    SMqVgEp* pVgEp = taosMemoryMalloc(sizeof(SMqVgEp));
    pVgEp->epSet = mndGetVgroupEpset(pMnode, pVgroup);
    pVgEp->vgId = pVgroup->vgId;
    taosArrayPush(pSub->unassignedVgs, &pVgEp);

    mDebug("init subscription %s, assign vg: %d", pSub->key, pVgEp->vgId);

    if (pTopic->subType == TOPIC_SUB_TYPE__TABLE) {
      int32_t msgLen;

      plan->execNode.epSet = pVgEp->epSet;
      plan->execNode.nodeId = pVgEp->vgId;

      if (qSubPlanToString(plan, &pVgEp->qmsg, &msgLen) < 0) {
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        terrno = TSDB_CODE_QRY_INVALID_INPUT;
        return -1;
      }
    } else {
      pVgEp->qmsg = strdup("");
    }
  }

  ASSERT(pSub->unassignedVgs->size > 0);

  ASSERT(taosHashGetSize(pSub->consumerHash) == 0);

  qDestroyQueryPlan(pPlan);

  return 0;
}
