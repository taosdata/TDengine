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

int32_t doRemoveTasks(SStreamExecInfo *pExecNode, STaskId *pRemovedId);

static int32_t mndAddSnodeInfo(SMnode *pMnode, SArray *pVgroupList) {
  SSnodeObj *pObj = NULL;
  void      *pIter = NULL;
  int32_t    code = 0;

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {.nodeId = SNODE_HANDLE};
    code = addEpIntoEpSet(&entry.epset, pObj->pDnode->fqdn, pObj->pDnode->port);
    if (code) {
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      mError("failed to extract epset for fqdn:%s during task vgroup snapshot", pObj->pDnode->fqdn);
      return code;
    }

    char buf[256] = {0};
    code = epsetToStr(&entry.epset, buf, tListLen(buf));
    if (code != 0) {  // print error and continue
      mError("failed to convert epset to str, code:%s", tstrerror(code));
    }

    void *p = taosArrayPush(pVgroupList, &entry);
    if (p == NULL) {
      code = terrno;
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      mError("failed to put entry in vgroup list, nodeId:%d code:%s", entry.nodeId, tstrerror(code));
      return code;
    } else {
      mDebug("take snode snapshot, nodeId:%d %s", entry.nodeId, buf);
    }

    sdbRelease(pMnode->pSdb, pObj);
  }

  return code;
}

static int32_t mndCheckAndAddVgroupsInfo(SMnode *pMnode, SArray *pVgroupList, bool* allReady, SHashObj* pTermMap) {
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  SVgObj   *pVgroup = NULL;
  int32_t   code = 0;
  SHashObj *pHash = NULL;

  pHash = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pHash == NULL) {
    mError("failed to prepare hashmap during take vgroup snapshot, code:%s", tstrerror(terrno));
    return terrno;
  }

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {.nodeId = pVgroup->vgId, .hbTimestamp = pVgroup->updateTime};
    entry.epset = mndGetVgroupEpset(pMnode, pVgroup);

    int8_t *pReplica = taosHashGet(pHash, &pVgroup->dbUid, sizeof(pVgroup->dbUid));
    if (pReplica == NULL) {  // not exist, add it into hash map
      code = taosHashPut(pHash, &pVgroup->dbUid, sizeof(pVgroup->dbUid), &pVgroup->replica, sizeof(pVgroup->replica));
      if (code) {
        mError("failed to put info into hashmap during task vgroup snapshot, code:%s", tstrerror(code));
        sdbRelease(pSdb, pVgroup);
        sdbCancelFetch(pSdb, pIter);
        goto _end;  // take snapshot failed, and not all ready
      }
    } else {
      if (*pReplica != pVgroup->replica) {
        mInfo("vgId:%d replica:%d inconsistent with other vgroups replica:%d, not ready for stream operations",
              pVgroup->vgId, pVgroup->replica, *pReplica);
        *allReady = false;  // task snap success, but not all ready
      }
    }

    // if not all ready till now, no need to check the remaining vgroups,
    // but still we need to put the info of the existed vgroups into the snapshot list
    if (*allReady) {
      *allReady = checkStatusForEachReplica(pVgroup);
    }

    char buf[256] = {0};
    code = epsetToStr(&entry.epset, buf, tListLen(buf));
    if (code != 0) {  // print error and continue
      mError("failed to convert epset to str, code:%s", tstrerror(code));
    }

    void *p = taosArrayPush(pVgroupList, &entry);
    if (p == NULL) {
      mError("failed to put entry in vgroup list, nodeId:%d code:out of memory", entry.nodeId);
      code = terrno;
      sdbRelease(pSdb, pVgroup);
      sdbCancelFetch(pSdb, pIter);
      goto _end;
    } else {
      mDebug("take node snapshot, nodeId:%d %s", entry.nodeId, buf);
    }

    if (pTermMap != NULL) {
      int64_t term = pVgroup->vnodeGid[0].syncTerm;
      code = taosHashPut(pTermMap, &pVgroup->vgId, sizeof(pVgroup->vgId), &term, sizeof(term));
      if (code) {
        mError("failed to put vnode:%d term into hashMap, code:%s", pVgroup->vgId, tstrerror(code));
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

_end:
  taosHashCleanup(pHash);
  return code;
}

int32_t mndGetStreamObj(SMnode *pMnode, int64_t streamId, SStreamObj **pStream) {
  void *pIter = NULL;
  SSdb *pSdb = pMnode->pSdb;
  *pStream = NULL;

  SStreamObj *p = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&p)) != NULL) {
    if (p->uid == streamId) {
      sdbCancelFetch(pSdb, pIter);
      *pStream = p;
      return TSDB_CODE_SUCCESS;
    }
    sdbRelease(pSdb, p);
  }

  return TSDB_CODE_STREAM_TASK_NOT_EXIST;
}

int32_t mndGetNumOfStreamTasks(const SStreamObj *pStream) {
  int32_t num = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pStream->pTaskList); ++i) {
    SArray *pLevel = taosArrayGetP(pStream->pTaskList, i);
    num += taosArrayGetSize(pLevel);
  }

  return num;
}

int32_t mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams) {
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

    if (pStream->sourceDbUid == pDb->uid) {
      numOfStreams++;
    }

    sdbRelease(pSdb, pStream);
  }

  *pNumOfStreams = numOfStreams;
  mndReleaseDb(pMnode, pDb);
  return 0;
}

static void freeTaskList(void *param) {
  SArray **pList = (SArray **)param;
  taosArrayDestroy(*pList);
}

int32_t doRemoveTasks(SStreamExecInfo *pExecNode, STaskId *pRemovedId) {
  void *p = taosHashGet(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));
  if (p == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = taosHashRemove(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));
  if (code) {
    return code;
  }

  for (int32_t k = 0; k < taosArrayGetSize(pExecNode->pTaskList); ++k) {
    STaskId *pId = taosArrayGet(pExecNode->pTaskList, k);
    if (pId == NULL) {
      continue;
    }

    if (pId->taskId == pRemovedId->taskId && pId->streamId == pRemovedId->streamId) {
      taosArrayRemove(pExecNode->pTaskList, k);

      int32_t num = taosArrayGetSize(pExecNode->pTaskList);
      mInfo("s-task:0x%x removed from buffer, remain:%d in buffer list", (int32_t)pRemovedId->taskId, num);
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void mndShowStreamStatus(char *dst, int8_t status) {
  if (status == STREAM_STATUS__NORMAL) {
    tstrncpy(dst, "ready", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__STOP) {
    tstrncpy(dst, "stop", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__FAILED) {
    tstrncpy(dst, "failed", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__RECOVER) {
    tstrncpy(dst, "recover", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__PAUSE) {
    tstrncpy(dst, "paused", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__INIT) {
    tstrncpy(dst, "init", MND_STREAM_TRIGGER_NAME_SIZE);
  }
}

static void mndShowStreamTrigger(char *dst, SStreamObj *pStream) {
  int8_t trigger = pStream->conf.trigger;
  if (trigger == STREAM_TRIGGER_AT_ONCE) {
    tstrncpy(dst, "at once", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (trigger == STREAM_TRIGGER_WINDOW_CLOSE) {
    tstrncpy(dst, "window close", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (trigger == STREAM_TRIGGER_MAX_DELAY) {
    tstrncpy(dst, "max delay", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE) {
    tstrncpy(dst, "force window close", MND_STREAM_TRIGGER_NAME_SIZE);
  }
}

static void int64ToHexStr(int64_t id, char *pBuf, int32_t bufLen) {
  memset(pBuf, 0, bufLen);
  pBuf[2] = '0';
  pBuf[3] = 'x';

  int32_t len = tintToHex(id, &pBuf[4]);
  varDataSetLen(pBuf, len + 2);
}

int32_t setStreamAttrInResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows) {
  int32_t code = 0;
  int32_t cols = 0;
  int32_t lino = 0;

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
  return code;
}

int32_t mndStreamCheckSnodeExists(SMnode *pMnode) {
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
