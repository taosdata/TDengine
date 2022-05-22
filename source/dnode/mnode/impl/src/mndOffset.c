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
#include "mndOffset.h"
#include "mndAuth.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tname.h"

#define MND_OFFSET_VER_NUMBER   1
#define MND_OFFSET_RESERVE_SIZE 64

static int32_t mndOffsetActionInsert(SSdb *pSdb, SMqOffsetObj *pOffset);
static int32_t mndOffsetActionDelete(SSdb *pSdb, SMqOffsetObj *pOffset);
static int32_t mndOffsetActionUpdate(SSdb *pSdb, SMqOffsetObj *pOffset, SMqOffsetObj *pNewOffset);
static int32_t mndProcessCommitOffsetReq(SRpcMsg *pReq);

int32_t mndInitOffset(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_OFFSET,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndOffsetActionEncode,
                     .decodeFp = (SdbDecodeFp)mndOffsetActionDecode,
                     .insertFp = (SdbInsertFp)mndOffsetActionInsert,
                     .updateFp = (SdbUpdateFp)mndOffsetActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndOffsetActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_MQ_COMMIT_OFFSET, mndProcessCommitOffsetReq);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupOffset(SMnode *pMnode) {}

bool mndOffsetFromTopic(SMqOffsetObj *pOffset, const char *topic) {
  int32_t i = 0;
  while (pOffset->key[i] != ':') i++;
  while (pOffset->key[i] != ':') i++;
  if (strcmp(&pOffset->key[i + 1], topic) == 0) return true;
  return false;
}

bool mndOffsetFromSubKey(SMqOffsetObj *pOffset, const char *subKey) {
  int32_t i = 0;
  while (pOffset->key[i] != ':') i++;
  if (strcmp(&pOffset->key[i + 1], subKey) == 0) return true;
  return false;
}
SSdbRaw *mndOffsetActionEncode(SMqOffsetObj *pOffset) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void   *buf = NULL;
  int32_t tlen = tEncodeSMqOffsetObj(NULL, pOffset);
  int32_t size = sizeof(int32_t) + tlen + MND_OFFSET_RESERVE_SIZE;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_OFFSET, MND_OFFSET_VER_NUMBER, size);
  if (pRaw == NULL) goto OFFSET_ENCODE_OVER;

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto OFFSET_ENCODE_OVER;

  void *abuf = buf;
  tEncodeSMqOffsetObj(&abuf, pOffset);

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, OFFSET_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, OFFSET_ENCODE_OVER);
  SDB_SET_RESERVE(pRaw, dataPos, MND_OFFSET_RESERVE_SIZE, OFFSET_ENCODE_OVER);
  SDB_SET_DATALEN(pRaw, dataPos, OFFSET_ENCODE_OVER);

  terrno = TSDB_CODE_SUCCESS;

OFFSET_ENCODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("offset:%s, failed to encode to raw:%p since %s", pOffset->key, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("offset:%s, encode to raw:%p, row:%p", pOffset->key, pRaw, pOffset);
  return pRaw;
}

SSdbRow *mndOffsetActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto OFFSET_DECODE_OVER;

  if (sver != MND_OFFSET_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto OFFSET_DECODE_OVER;
  }

  int32_t  size = sizeof(SMqOffsetObj);
  SSdbRow *pRow = sdbAllocRow(size);
  if (pRow == NULL) goto OFFSET_DECODE_OVER;

  SMqOffsetObj *pOffset = sdbGetRowObj(pRow);
  if (pOffset == NULL) goto OFFSET_DECODE_OVER;

  int32_t dataPos = 0;
  int32_t tlen;
  SDB_GET_INT32(pRaw, dataPos, &tlen, OFFSET_DECODE_OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) goto OFFSET_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, OFFSET_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_OFFSET_RESERVE_SIZE, OFFSET_DECODE_OVER);

  if (tDecodeSMqOffsetObj(buf, pOffset) == NULL) {
    goto OFFSET_DECODE_OVER;
  }

  terrno = TSDB_CODE_SUCCESS;

OFFSET_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("offset:%s, failed to decode from raw:%p since %s", pOffset->key, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("offset:%s, decode from raw:%p, row:%p", pOffset->key, pRaw, pOffset);
  return pRow;
}

int32_t mndCreateOffsets(STrans *pTrans, const char *cgroup, const char *topicName, const SArray *vgs) {
  int32_t sz = taosArrayGetSize(vgs);
  for (int32_t i = 0; i < sz; i++) {
    int32_t      vgId = *(int32_t *)taosArrayGet(vgs, i);
    SMqOffsetObj offsetObj = {0};
    if (mndMakePartitionKey(offsetObj.key, cgroup, topicName, vgId) < 0) {
      return -1;
    }
    // TODO assign db
    offsetObj.offset = -1;
    SSdbRaw *pOffsetRaw = mndOffsetActionEncode(&offsetObj);
    if (pOffsetRaw == NULL) {
      return -1;
    }
    sdbSetRawStatus(pOffsetRaw, SDB_STATUS_READY);
    // commit log or redo log?
    if (mndTransAppendRedolog(pTrans, pOffsetRaw) < 0) {
      return -1;
    }
  }
  return 0;
}

static int32_t mndProcessCommitOffsetReq(SRpcMsg *pMsg) {
  char key[TSDB_PARTITION_KEY_LEN];

  SMnode              *pMnode = pMsg->info.node;
  char                *msgStr = pMsg->pCont;
  SMqCMCommitOffsetReq commitOffsetReq;
  SDecoder             decoder;
  tDecoderInit(&decoder, msgStr, pMsg->contLen);

  tDecodeSMqCMCommitOffsetReq(&decoder, &commitOffsetReq);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_COMMIT_OFFSET, pMsg);

  for (int32_t i = 0; i < commitOffsetReq.num; i++) {
    SMqOffset *pOffset = &commitOffsetReq.offsets[i];
    if (mndMakePartitionKey(key, pOffset->cgroup, pOffset->topicName, pOffset->vgId) < 0) {
      return -1;
    }
    bool          create = false;
    SMqOffsetObj *pOffsetObj = mndAcquireOffset(pMnode, key);
    if (pOffsetObj == NULL) {
      pOffsetObj = taosMemoryMalloc(sizeof(SMqOffsetObj));
      memcpy(pOffsetObj->key, key, TSDB_PARTITION_KEY_LEN);
      create = true;
    }
    pOffsetObj->offset = pOffset->offset;
    SSdbRaw *pOffsetRaw = mndOffsetActionEncode(pOffsetObj);
    sdbSetRawStatus(pOffsetRaw, SDB_STATUS_READY);
    mndTransAppendCommitlog(pTrans, pOffsetRaw);
    if (create) {
      taosMemoryFree(pOffsetObj);
    } else {
      mndReleaseOffset(pMnode, pOffsetObj);
    }
  }

  tDecoderClear(&decoder);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("mq-commit-offset-trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return TSDB_CODE_ACTION_IN_PROGRESS;
}

static int32_t mndOffsetActionInsert(SSdb *pSdb, SMqOffsetObj *pOffset) {
  mTrace("offset:%s, perform insert action", pOffset->key);
  return 0;
}

static int32_t mndOffsetActionDelete(SSdb *pSdb, SMqOffsetObj *pOffset) {
  mTrace("offset:%s, perform delete action", pOffset->key);
  return 0;
}

static int32_t mndOffsetActionUpdate(SSdb *pSdb, SMqOffsetObj *pOldOffset, SMqOffsetObj *pNewOffset) {
  mTrace("offset:%s, perform update action", pOldOffset->key);
  atomic_store_64(&pOldOffset->offset, pNewOffset->offset);
  return 0;
}

SMqOffsetObj *mndAcquireOffset(SMnode *pMnode, const char *key) {
  SSdb         *pSdb = pMnode->pSdb;
  SMqOffsetObj *pOffset = sdbAcquire(pSdb, SDB_OFFSET, key);
  if (pOffset == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_OFFSET_NOT_EXIST;
  }
  return pOffset;
}

void mndReleaseOffset(SMnode *pMnode, SMqOffsetObj *pOffset) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pOffset);
}

static void mndCancelGetNextOffset(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndSetDropOffsetCommitLogs(SMnode *pMnode, STrans *pTrans, SMqOffsetObj *pOffset) {
  SSdbRaw *pCommitRaw = mndOffsetActionEncode(pOffset);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetDropOffsetRedoLogs(SMnode *pMnode, STrans *pTrans, SMqOffsetObj *pOffset) {
  SSdbRaw *pRedoRaw = mndOffsetActionEncode(pOffset);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

int32_t mndDropOffsetByDB(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;

  void         *pIter = NULL;
  SMqOffsetObj *pOffset = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_OFFSET, pIter, (void **)&pOffset);
    if (pIter == NULL) break;

    if (pOffset->dbUid != pDb->uid) {
      sdbRelease(pSdb, pOffset);
      continue;
    }

    if (mndSetDropOffsetCommitLogs(pMnode, pTrans, pOffset) < 0) {
      sdbRelease(pSdb, pOffset);
      goto END;
    }

    sdbRelease(pSdb, pOffset);
  }

  code = 0;
END:
  return code;
}

int32_t mndDropOffsetByTopic(SMnode *pMnode, STrans *pTrans, const char *topic) {
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;

  void         *pIter = NULL;
  SMqOffsetObj *pOffset = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_OFFSET, pIter, (void **)&pOffset);
    if (pIter == NULL) break;

    if (!mndOffsetFromTopic(pOffset, topic)) {
      sdbRelease(pSdb, pOffset);
      continue;
    }

    if (mndSetDropOffsetCommitLogs(pMnode, pTrans, pOffset) < 0) {
      sdbRelease(pSdb, pOffset);
      goto END;
    }

    sdbRelease(pSdb, pOffset);
  }

  code = 0;
END:
  return code;
}

int32_t mndDropOffsetBySubKey(SMnode *pMnode, STrans *pTrans, const char *subKey) {
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;

  void         *pIter = NULL;
  SMqOffsetObj *pOffset = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_OFFSET, pIter, (void **)&pOffset);
    if (pIter == NULL) break;

    if (!mndOffsetFromSubKey(pOffset, subKey)) {
      sdbRelease(pSdb, pOffset);
      continue;
    }

    if (mndSetDropOffsetCommitLogs(pMnode, pTrans, pOffset) < 0) {
      sdbRelease(pSdb, pOffset);
      goto END;
    }

    sdbRelease(pSdb, pOffset);
  }

  code = 0;
END:
  return code;
}
