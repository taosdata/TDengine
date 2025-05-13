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
      return TSDB_CODE_SUCCESS;
    }
    
    sdbRelease(pSdb, pStream);
  }

  *dropped = true;

  return TSDB_CODE_SUCCESS;
}

int32_t mstGetStreamsNumInDb(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    TAOS_RETURN(TSDB_CODE_MND_DB_NOT_SELECTED);
  }

  int32_t numOfStreams = 0;
  void   *pIter = NULL;
  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

/* STREAMTODO
    if (pStream->sourceDbUid == pDb->uid) {
      numOfStreams++;
    }
*/
    sdbRelease(pSdb, pStream);
  }

  *pNumOfStreams = numOfStreams;
  mndReleaseDb(pMnode, pDb);
  return 0;
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

int32_t mstGenerateResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows) {
  int32_t code = 0;
  int32_t cols = 0;
  int32_t lino = 0;

/* STREAMTODO
  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));
  SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // create time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->createTime, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stream id
  char buf[128] = {0};
  int64ToHexStr(pStream->uid, buf, tListLen(buf));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, buf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // related fill-history stream id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  if (pStream->hTaskUid != 0) {
    int64ToHexStr(pStream->hTaskUid, buf, tListLen(buf));
    code = colDataSetVal(pColInfo, numOfRows, buf, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, buf, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // related fill-history stream id
  char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(sql, pStream->sql, sizeof(sql));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)sql, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char status[20 + VARSTR_HEADER_SIZE] = {0};
  char status2[MND_STREAM_TRIGGER_NAME_SIZE] = {0};
  bool isPaused = false;
  //code = isAllTaskPaused(pStream, &isPaused);
  TSDB_CHECK_CODE(code, lino, _end);

  int8_t streamStatus = atomic_load_8(&pStream->status);
  if (isPaused && pStream->pTaskList != NULL) {
    streamStatus = STREAM_STATUS__PAUSE;
  }
  mndShowStreamStatus(status2, streamStatus);
  STR_WITH_MAXSIZE_TO_VARSTR(status, status2, sizeof(status));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&status, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char sourceDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(sourceDB, mndGetDbStr(pStream->sourceDb), sizeof(sourceDB));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&sourceDB, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char targetDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(targetDB, mndGetDbStr(pStream->targetDb), sizeof(targetDB));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&targetDB, false);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pStream->targetSTbName[0] == 0) {
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

    code = colDataSetVal(pColInfo, numOfRows, NULL, true);
  } else {
    char targetSTB[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(targetSTB, mndGetStbStr(pStream->targetSTbName), sizeof(targetSTB));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

    code = colDataSetVal(pColInfo, numOfRows, (const char *)&targetSTB, false);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->conf.watermark, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char trigger[20 + VARSTR_HEADER_SIZE] = {0};
  char trigger2[MND_STREAM_TRIGGER_NAME_SIZE] = {0};
  mndShowStreamTrigger(trigger2, pStream);
  STR_WITH_MAXSIZE_TO_VARSTR(trigger, trigger2, sizeof(trigger));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&trigger, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // sink_quota
  char sinkQuota[20 + VARSTR_HEADER_SIZE] = {0};
  sinkQuota[0] = '0';
  char dstStr[20] = {0};
  STR_TO_VARSTR(dstStr, sinkQuota)
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);
  TSDB_CHECK_CODE(code, lino, _end);


  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  // checkpoint backup type
  char backup[20 + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(backup, "none")
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)backup, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // history scan idle
  char scanHistoryIdle[20 + VARSTR_HEADER_SIZE] = {0};
  tstrncpy(scanHistoryIdle, "100a", sizeof(scanHistoryIdle));

  memset(dstStr, 0, tListLen(dstStr));
  STR_TO_VARSTR(dstStr, scanHistoryIdle)
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  char msg[TSDB_RESERVE_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};
  if (streamStatus == STREAM_STATUS__FAILED){
    STR_TO_VARSTR(msg, pStream->reserve)
  } else {
    STR_TO_VARSTR(msg, " ")
  }
  code = colDataSetVal(pColInfo, numOfRows, (const char *)msg, false);

_end:
  if (code) {
    mError("error happens when build stream attr result block, lino:%d, code:%s", lino, tstrerror(code));
  }
*/

  return code;
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


bool mndStreamActionDequeue(SStmActionQ* pQueue, SStmQNode **param) {
  while (0 == atomic_load_64(&pQueue->qRemainNum)) {
    return false;
  }

  SStmQNode *orig = pQueue->head;

  SStmQNode *node = pQueue->head->next;
  pQueue->head = pQueue->head->next;

  *param = node;

  atomic_sub_fetch_64(&pQueue->qRemainNum, 1);

  return true;
}

void mndStreamActionEnqueue(SStmActionQ* pQueue, SStmQNode* param) {
  taosWLockLatch(&pQueue->lock);
  pQueue->tail->next = param;
  pQueue->tail = param;
  taosWUnLockLatch(&pQueue->lock);

  atomic_add_fetch_64(&pQueue->qRemainNum, 1);
}


void mndStreamPostAction(SStmActionQ*       actionQ, int64_t streamId, char* streamName, int32_t action) {
  SStmQNode *pNode = taosMemoryMalloc(sizeof(SStmQNode));
  if (NULL == pNode) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, __LINE__, tstrerror(terrno));
    return;
  }

  pNode->type = action;
  pNode->streamAct = true;
  pNode->action.stream.streamId = streamId;
  TAOS_STRCPY(pNode->action.stream.streamName, streamName);
  
  pNode->next = NULL;
  

  mndStreamActionEnqueue(actionQ, pNode);
}

void mndStreamPostTaskAction(SStmActionQ*        actionQ, SStmTaskAction* pAction, int32_t action) {
  SStmQNode *pNode = taosMemoryMalloc(sizeof(SStmQNode));
  if (NULL == pNode) {
    int64_t streamId = pAction->streamId;
    mstError("%s failed at line %d, error:%s", __FUNCTION__, __LINE__, tstrerror(terrno));
    return;
  }

  pNode->type = action;
  pNode->streamAct = false;
  pNode->action.task = *pAction;
  
  pNode->next = NULL;

  mndStreamActionEnqueue(actionQ, pNode);
}


