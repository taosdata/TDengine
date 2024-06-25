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
  taosHashPut(execInfo.transMgmt.pDBTrans, &streamId, sizeof(streamId), &info, sizeof(SStreamTransInfo));
  return 0;
}

int32_t mndStreamClearFinishedTrans(SMnode *pMnode, int32_t *pNumOfActiveChkpt) {
  size_t  keyLen = 0;
  void   *pIter = NULL;
  SArray *pList = taosArrayInit(4, sizeof(SKeyInfo));
  int32_t num = 0;

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
      taosArrayPush(pList, &info);
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
    taosHashRemove(execInfo.transMgmt.pDBTrans, pKey->pKey, pKey->keyLen);
  }

  mDebug("clear %d finished stream-trans, remained:%d, active checkpoint trans:%d", size,
         taosHashGetSize(execInfo.transMgmt.pDBTrans), num);

  terrno = TSDB_CODE_SUCCESS;
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
bool mndStreamTransConflictCheck(SMnode *pMnode, int64_t streamId, const char *pTransName, bool lock) {
  if (lock) {
    taosThreadMutexLock(&execInfo.lock);
  }

  int32_t num = taosHashGetSize(execInfo.transMgmt.pDBTrans);
  if (num <= 0) {
    if (lock) {
      taosThreadMutexUnlock(&execInfo.lock);
    }
    return false;
  }

  mndStreamClearFinishedTrans(pMnode, NULL);

  SStreamTransInfo *pEntry = taosHashGet(execInfo.transMgmt.pDBTrans, &streamId, sizeof(streamId));
  if (pEntry != NULL) {
    SStreamTransInfo tInfo = *pEntry;

    if (lock) {
      taosThreadMutexUnlock(&execInfo.lock);
    }

    if (strcmp(tInfo.name, MND_STREAM_CHECKPOINT_NAME) == 0) {
      if ((strcmp(pTransName, MND_STREAM_DROP_NAME) != 0) && (strcmp(pTransName, MND_STREAM_TASK_RESET_NAME) != 0)) {
        mWarn("conflict with other transId:%d streamUid:0x%" PRIx64 ", trans:%s", tInfo.transId, tInfo.streamId,
              tInfo.name);
        terrno = TSDB_CODE_MND_TRANS_CONFLICT;
        return true;
      } else {
        mDebug("not conflict with checkpoint trans, name:%s, continue creating trans", pTransName);
      }
    } else if ((strcmp(tInfo.name, MND_STREAM_CREATE_NAME) == 0) || (strcmp(tInfo.name, MND_STREAM_DROP_NAME) == 0) ||
               (strcmp(tInfo.name, MND_STREAM_TASK_RESET_NAME) == 0) ||
               strcmp(tInfo.name, MND_STREAM_TASK_UPDATE_NAME) == 0) {
      mWarn("conflict with other transId:%d streamUid:0x%" PRIx64 ", trans:%s", tInfo.transId, tInfo.streamId,
            tInfo.name);
      terrno = TSDB_CODE_MND_TRANS_CONFLICT;
      return true;
    }
  } else {
    mDebug("stream:0x%" PRIx64 " no conflict trans existed, continue create trans", streamId);
  }

  if (lock) {
    taosThreadMutexUnlock(&execInfo.lock);
  }

  return false;
}

int32_t mndStreamGetRelTrans(SMnode *pMnode, int64_t streamId) {
  taosThreadMutexLock(&execInfo.lock);
  int32_t num = taosHashGetSize(execInfo.transMgmt.pDBTrans);
  if (num <= 0) {
    taosThreadMutexUnlock(&execInfo.lock);
    return 0;
  }

  mndStreamClearFinishedTrans(pMnode, NULL);
  SStreamTransInfo* pEntry = taosHashGet(execInfo.transMgmt.pDBTrans, &streamId, sizeof(streamId));
  if (pEntry != NULL) {
    SStreamTransInfo tInfo = *pEntry;
    taosThreadMutexUnlock(&execInfo.lock);

    if (strcmp(tInfo.name, MND_STREAM_CHECKPOINT_NAME) == 0 || strcmp(tInfo.name, MND_STREAM_TASK_UPDATE_NAME) == 0 ||
        strcmp(tInfo.name, MND_STREAM_CHKPT_UPDATE_NAME) == 0) {
      return tInfo.transId;
    }
  } else {
    taosThreadMutexUnlock(&execInfo.lock);
  }

  return 0;
}

STrans *doCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name,
                      const char *pMsg) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, conflict, pReq, name);
  if (pTrans == NULL) {
    mError("failed to build trans:%s, reason: %s", name, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  mInfo("stream:0x%" PRIx64 " start to build trans %s, transId:%d", pStream->uid, pMsg, pTrans->id);

  mndTransSetDbName(pTrans, pStream->sourceDb, pStream->targetSTbName);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
    terrno = TSDB_CODE_MND_TRANS_CONFLICT;
    mError("failed to build trans:%s for stream:0x%" PRIx64 " code:%s", name, pStream->uid, tstrerror(terrno));
    mndTransDrop(pTrans);
    return NULL;
  }

  terrno = 0;
  return pTrans;
}

SSdbRaw *mndStreamActionEncode(SStreamObj *pStream) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void *buf = NULL;

  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  if (tEncodeSStreamObj(&encoder, pStream) < 0) {
    tEncoderClear(&encoder);
    goto STREAM_ENCODE_OVER;
  }
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  int32_t  size = sizeof(int32_t) + tlen + MND_STREAM_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STREAM, MND_STREAM_VER_NUMBER, size);
  if (pRaw == NULL) goto STREAM_ENCODE_OVER;

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto STREAM_ENCODE_OVER;

  tEncoderInit(&encoder, buf, tlen);
  if (tEncodeSStreamObj(&encoder, pStream) < 0) {
    tEncoderClear(&encoder);
    goto STREAM_ENCODE_OVER;
  }
  tEncoderClear(&encoder);

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, STREAM_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, STREAM_ENCODE_OVER);
  SDB_SET_DATALEN(pRaw, dataPos, STREAM_ENCODE_OVER);

  terrno = TSDB_CODE_SUCCESS;

STREAM_ENCODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("stream:%s, failed to encode to raw:%p since %s", pStream->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("stream:%s, encode to raw:%p, row:%p, checkpoint:%" PRId64 "", pStream->name, pRaw, pStream,
         pStream->checkpointId);
  return pRaw;
}

int32_t mndPersistTransLog(SStreamObj *pStream, STrans *pTrans, int32_t status) {
  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL) {
    mError("failed to encode stream since %s", terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("stream trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    return -1;
  }

  if (sdbSetRawStatus(pCommitRaw, status) != 0) {
    mError("stream trans:%d failed to set raw status:%d since %s", pTrans->id, status, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    return -1;
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

    SStreamObj *pStream = mndGetStreamObj(pMnode, pTransInfo->streamId);
    if (pStream != NULL) {
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

    mDebug("clear checkpoint trans in Db:%s", p);
    doKillCheckpointTrans(pMnode, pKey, len);
    taosMemoryFree(p);
  }

  mDebug("complete clear checkpoints in Dbs");
}
