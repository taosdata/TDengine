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
#include "mndTrans.h"

typedef struct SKeyInfo {
  void   *pKey;
  int32_t keyLen;
} SKeyInfo;

int32_t mndStreamRegisterTrans(STrans *pTrans, const char *pTransName, int64_t streamId) {
  SStreamTransInfo info = {
      .transId = pTrans->id, .startTime = taosGetTimestampMs(), .name = pTransName, .streamId = streamId};
  return taosHashPut(execInfo.transMgmt.pDBTrans, &streamId, sizeof(streamId), &info, sizeof(SStreamTransInfo));
}

int32_t mndStreamClearFinishedTrans(SMnode *pMnode, int32_t *pNumOfActiveChkpt) {
  size_t  keyLen = 0;
  void   *pIter = NULL;
  SArray *pList = taosArrayInit(4, sizeof(SKeyInfo));
  int32_t num = 0;

  if (pList == NULL) {
    return terrno;
  }

  while ((pIter = taosHashIterate(execInfo.transMgmt.pDBTrans, pIter)) != NULL) {
    SStreamTransInfo *pEntry = (SStreamTransInfo *)pIter;

    // let's clear the finished trans
    STrans *pTrans = mndAcquireTrans(pMnode, pEntry->transId);
    if (pTrans == NULL) {
      void *pKey = taosHashGetKey(pEntry, &keyLen);
      // key is the name of src/dst db name
      SKeyInfo info = {.pKey = pKey, .keyLen = keyLen};
      mDebug("transId:%d %s startTs:%" PRId64 " cleared since finished", pEntry->transId, pEntry->name,
             pEntry->startTime);
      void* p = taosArrayPush(pList, &info);
      if (p == NULL) {
        return terrno;
      }
    } else {
      if (strcmp(pEntry->name, MND_STREAM_CHECKPOINT_NAME) == 0) {
        num++;
      }
      mndReleaseTrans(pMnode, pTrans);
    }
  }

  int32_t size = taosArrayGetSize(pList);
  for (int32_t i = 0; i < size; ++i) {
    SKeyInfo *pKey = taosArrayGet(pList, i);
    if (pKey == NULL) {
      continue;
    }

    int32_t code = taosHashRemove(execInfo.transMgmt.pDBTrans, pKey->pKey, pKey->keyLen);
    if (code != 0) {
      taosArrayDestroy(pList);
      return code;
    }
  }

  mDebug("clear %d finished stream-trans, remained:%d, active checkpoint trans:%d", size,
         taosHashGetSize(execInfo.transMgmt.pDBTrans), num);

  taosArrayDestroy(pList);

  if (pNumOfActiveChkpt != NULL) {
    *pNumOfActiveChkpt = num;
  }

  return 0;
}

// * Transactions of different streams are not related. Here only check the conflict of transaction for a given stream.
// For a given stream:
// 1. checkpoint trans is conflict with any other trans except for the drop and reset trans.
// 2. create/drop/reset/update trans are conflict with any other trans.
int32_t mndStreamTransConflictCheck(SMnode *pMnode, int64_t streamId, const char *pTransName, bool lock) {
  if (lock) {
    streamMutexLock(&execInfo.lock);
  }

  int32_t num = taosHashGetSize(execInfo.transMgmt.pDBTrans);
  if (num <= 0) {
    if (lock) {
      streamMutexUnlock(&execInfo.lock);
    }
    return 0;
  }

  int32_t code = mndStreamClearFinishedTrans(pMnode, NULL);
  if (code) {
    mError("failed to clear finish trans, code:%s", tstrerror(code));
  }

  SStreamTransInfo *pEntry = taosHashGet(execInfo.transMgmt.pDBTrans, &streamId, sizeof(streamId));
  if (pEntry != NULL) {
    SStreamTransInfo tInfo = *pEntry;

    if (lock) {
      streamMutexUnlock(&execInfo.lock);
    }

    if (strcmp(tInfo.name, MND_STREAM_CHECKPOINT_NAME) == 0) {
      if ((strcmp(pTransName, MND_STREAM_DROP_NAME) != 0) && (strcmp(pTransName, MND_STREAM_TASK_RESET_NAME) != 0)) {
        mWarn("conflict with other transId:%d streamUid:0x%" PRIx64 ", trans:%s", tInfo.transId, tInfo.streamId,
              tInfo.name);
        return TSDB_CODE_MND_TRANS_CONFLICT;
      } else {
        mDebug("not conflict with checkpoint trans, name:%s, continue creating trans", pTransName);
      }
    } else if ((strcmp(tInfo.name, MND_STREAM_CREATE_NAME) == 0) || (strcmp(tInfo.name, MND_STREAM_DROP_NAME) == 0) ||
               (strcmp(tInfo.name, MND_STREAM_TASK_RESET_NAME) == 0) ||
               strcmp(tInfo.name, MND_STREAM_TASK_UPDATE_NAME) == 0) {
      mWarn("conflict with other transId:%d streamUid:0x%" PRIx64 ", trans:%s", tInfo.transId, tInfo.streamId,
            tInfo.name);
      return TSDB_CODE_MND_TRANS_CONFLICT;
    }
  } else {
    mDebug("stream:0x%" PRIx64 " no conflict trans existed, continue create trans", streamId);
  }

  if (lock) {
    streamMutexUnlock(&execInfo.lock);
  }

  return 0;
}

int32_t mndStreamGetRelTrans(SMnode *pMnode, int64_t streamId) {
  streamMutexLock(&execInfo.lock);
  int32_t num = taosHashGetSize(execInfo.transMgmt.pDBTrans);
  if (num <= 0) {
    streamMutexUnlock(&execInfo.lock);
    return 0;
  }

  int32_t code = mndStreamClearFinishedTrans(pMnode, NULL);
  if (code) {
    mError("failed to clear finish trans, code:%s", tstrerror(code));
  }

  SStreamTransInfo *pEntry = taosHashGet(execInfo.transMgmt.pDBTrans, &streamId, sizeof(streamId));
  if (pEntry != NULL) {
    SStreamTransInfo tInfo = *pEntry;
    streamMutexUnlock(&execInfo.lock);

    if (strcmp(tInfo.name, MND_STREAM_CHECKPOINT_NAME) == 0 || strcmp(tInfo.name, MND_STREAM_TASK_UPDATE_NAME) == 0 ||
        strcmp(tInfo.name, MND_STREAM_CHKPT_UPDATE_NAME) == 0) {
      return tInfo.transId;
    }
  } else {
    streamMutexUnlock(&execInfo.lock);
  }

  return 0;
}

int32_t doCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name,
                      const char *pMsg, STrans **pTrans1) {
  *pTrans1 = NULL;
  terrno = 0;

  int32_t code = 0;
  STrans *p = mndTransCreate(pMnode, TRN_POLICY_RETRY, conflict, pReq, name);
  if (p == NULL) {
    mError("failed to build trans:%s, reason: %s", name, tstrerror(terrno));
    return terrno;
  }

  mInfo("stream:0x%" PRIx64 " start to build trans %s, transId:%d", pStream->uid, pMsg, p->id);

  mndTransSetDbName(p, pStream->sourceDb, pStream->targetSTbName);
  if ((code = mndTransCheckConflict(pMnode, p)) != 0) {
    mError("failed to build trans:%s for stream:0x%" PRIx64 " code:%s", name, pStream->uid, tstrerror(terrno));
    mndTransDrop(p);
    return code;
  }

  *pTrans1 = p;
  return code;
}

SSdbRaw *mndStreamActionEncode(SStreamObj *pStream) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *buf = NULL;

  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  if ((code = tEncodeSStreamObj(&encoder, pStream)) < 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code, lino, _over);
  }

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  int32_t  size = sizeof(int32_t) + tlen + MND_STREAM_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STREAM, MND_STREAM_VER_NUMBER, size);
  TSDB_CHECK_NULL(pRaw, code, lino, _over, terrno);

  buf = taosMemoryMalloc(tlen);
  TSDB_CHECK_NULL(buf, code, lino, _over, terrno);

  tEncoderInit(&encoder, buf, tlen);
  if ((code = tEncodeSStreamObj(&encoder, pStream)) < 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code, lino, _over);
  }

  tEncoderClear(&encoder);

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _over);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _over);
  SDB_SET_DATALEN(pRaw, dataPos, _over);

_over:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    mError("stream:%s, failed to encode to raw:%p at line:%d since %s", pStream->name, pRaw, lino, tstrerror(code));
    sdbFreeRaw(pRaw);
    terrno = code;
    return NULL;
  }

  terrno = 0;
  mTrace("stream:%s, encode to raw:%p, row:%p, checkpoint:%" PRId64 "", pStream->name, pRaw, pStream,
         pStream->checkpointId);
  return pRaw;
}

int32_t mndPersistTransLog(SStreamObj *pStream, STrans *pTrans, int32_t status) {
  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL) {
    mError("failed to encode stream since %s", terrstr());
    mndTransDrop(pTrans);
    return terrno;
  }

  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("stream trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    return terrno;
  }

  if (sdbSetRawStatus(pCommitRaw, status) != 0) {
    mError("stream trans:%d failed to set raw status:%d since %s", pTrans->id, status, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    return terrno;
  }

  return 0;
}

int32_t setTransAction(STrans *pTrans, void *pCont, int32_t contLen, int32_t msgType, const SEpSet *pEpset,
                       int32_t retryCode, int32_t acceptCode) {
  STransAction action = {.epSet = *pEpset,
                         .contLen = contLen,
                         .pCont = pCont,
                         .msgType = msgType,
                         .retryCode = retryCode,
                         .acceptableCode = acceptCode};
  return mndTransAppendRedoAction(pTrans, &action);
}

static bool identicalName(const char *pDb, const char *pParam, int32_t len) {
  return (strlen(pDb) == len) && (strncmp(pDb, pParam, len) == 0);
}

int32_t doKillCheckpointTrans(SMnode *pMnode, const char *pDBName, size_t len) {
  void *pIter = NULL;

  while ((pIter = taosHashIterate(execInfo.transMgmt.pDBTrans, pIter)) != NULL) {
    SStreamTransInfo *pTransInfo = (SStreamTransInfo *)pIter;
    if (strcmp(pTransInfo->name, MND_STREAM_CHECKPOINT_NAME) != 0) {
      continue;
    }

    SStreamObj *pStream = NULL;
    int32_t code = mndGetStreamObj(pMnode, pTransInfo->streamId, &pStream);
    if (pStream != NULL && code == 0) {
      if (identicalName(pStream->sourceDb, pDBName, len)) {
        mndKillTransImpl(pMnode, pTransInfo->transId, pStream->sourceDb);
      } else if (identicalName(pStream->targetDb, pDBName, len)) {
        mndKillTransImpl(pMnode, pTransInfo->transId, pStream->targetDb);
      }

      mndReleaseStream(pMnode, pStream);
    }
  }

  return TSDB_CODE_SUCCESS;
}

// kill all trans in the dst DB
void killAllCheckpointTrans(SMnode *pMnode, SVgroupChangeInfo *pChangeInfo) {
  mDebug("start to clear checkpoints in all Dbs");

  void *pIter = NULL;
  while ((pIter = taosHashIterate(pChangeInfo->pDBMap, pIter)) != NULL) {
    char *pDb = (char *)pIter;

    size_t len = 0;
    void  *pKey = taosHashGetKey(pDb, &len);
    char  *p = strndup(pKey, len);

    int32_t code = doKillCheckpointTrans(pMnode, pKey, len);
    if (code) {
      mError("failed to kill trans, transId:%p", pKey)
    } else {
      mDebug("clear checkpoint trans in Db:%s", p);
    }
    taosMemoryFree(p);
  }

  mDebug("complete clear checkpoints in all Dbs");
}
