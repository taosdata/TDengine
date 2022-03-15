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
static int32_t mndProcessCommitOffsetReq(SNodeMsg *pReq);

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

SSdbRaw *mndOffsetActionEncode(SMqOffsetObj *pOffset) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void   *buf = NULL;
  int32_t tlen = tEncodeSMqOffsetObj(NULL, pOffset);
  int32_t size = sizeof(int32_t) + tlen + MND_OFFSET_RESERVE_SIZE;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_OFFSET, MND_OFFSET_VER_NUMBER, size);
  if (pRaw == NULL) goto OFFSET_ENCODE_OVER;

  buf = malloc(tlen);
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
  tfree(buf);
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
  buf = malloc(tlen + 1);
  if (buf == NULL) goto OFFSET_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, OFFSET_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_OFFSET_RESERVE_SIZE, OFFSET_DECODE_OVER);

  if (tDecodeSMqOffsetObj(buf, pOffset) == NULL) {
    goto OFFSET_DECODE_OVER;
  }

  terrno = TSDB_CODE_SUCCESS;

OFFSET_DECODE_OVER:
  tfree(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("offset:%s, failed to decode from raw:%p since %s", pOffset->key, pRaw, terrstr());
    tfree(pRow);
    return NULL;
  }

  mTrace("offset:%s, decode from raw:%p, row:%p", pOffset->key, pRaw, pOffset);
  return pRow;
}

int32_t mndCreateOffset(STrans *pTrans, const char *cgroup, const char *topicName, const SArray *vgs) {
  int32_t code = 0;
  int32_t sz = taosArrayGetSize(vgs);
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp *pConsumerEp = taosArrayGet(vgs, i);
    SMqOffsetObj   offsetObj;
    if (mndMakePartitionKey(offsetObj.key, cgroup, topicName, pConsumerEp->vgId) < 0) {
      return -1;
    }
    offsetObj.offset = -1;
    SSdbRaw *pOffsetRaw = mndOffsetActionEncode(&offsetObj);
    if (pOffsetRaw == NULL) {
      return -1;
    }
    sdbSetRawStatus(pOffsetRaw, SDB_STATUS_READY);
    if (mndTransAppendRedolog(pTrans, pOffsetRaw) < 0) {
      return -1;
    }
  }
  return 0;
}

static int32_t mndProcessCommitOffsetReq(SNodeMsg *pMsg) {
  char key[TSDB_PARTITION_KEY_LEN];

  SMnode              *pMnode = pMsg->pNode;
  char                *msgStr = pMsg->rpcMsg.pCont;
  SMqCMCommitOffsetReq commitOffsetReq;
  SCoder               decoder;
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, msgStr, pMsg->rpcMsg.contLen, TD_DECODER);

  tDecodeSMqCMCommitOffsetReq(&decoder, &commitOffsetReq);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_COMMIT_OFFSET, &pMsg->rpcMsg);

  for (int32_t i = 0; i < commitOffsetReq.num; i++) {
    SMqOffset *pOffset = &commitOffsetReq.offsets[i];
    if (mndMakePartitionKey(key, pOffset->cgroup, pOffset->topicName, pOffset->vgId) < 0) {
      return -1;
    }
    SMqOffsetObj *pOffsetObj = mndAcquireOffset(pMnode, key);
    ASSERT(pOffsetObj);
    pOffsetObj->offset = pOffset->offset;
    SSdbRaw *pOffsetRaw = mndOffsetActionEncode(pOffsetObj);
    sdbSetRawStatus(pOffsetRaw, SDB_STATUS_READY);
    mndTransAppendRedolog(pTrans, pOffsetRaw);
    mndReleaseOffset(pMnode, pOffsetObj);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("mq-commit-offset-trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
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
