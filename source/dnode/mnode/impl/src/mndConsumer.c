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
#include "mndConsumer.h"
#include "mndAuth.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tcompare.h"
#include "tname.h"

#define MND_CONSUMER_VER_NUMBER 1
#define MND_CONSUMER_RESERVE_SIZE 64

static int32_t mndConsumerActionInsert(SSdb *pSdb, SMqConsumerObj *pConsumer);
static int32_t mndConsumerActionDelete(SSdb *pSdb, SMqConsumerObj *pConsumer);
static int32_t mndConsumerActionUpdate(SSdb *pSdb, SMqConsumerObj *pConsumer, SMqConsumerObj *pNewConsumer);
static int32_t mndProcessConsumerMetaMsg(SNodeMsg *pMsg);
static int32_t mndGetConsumerMeta(SNodeMsg *pMsg, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t mndRetrieveConsumer(SNodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void    mndCancelGetNextConsumer(SMnode *pMnode, void *pIter);

int32_t mndInitConsumer(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_CONSUMER,
                     .keyType = SDB_KEY_INT64,
                     .encodeFp = (SdbEncodeFp)mndConsumerActionEncode,
                     .decodeFp = (SdbDecodeFp)mndConsumerActionDecode,
                     .insertFp = (SdbInsertFp)mndConsumerActionInsert,
                     .updateFp = (SdbUpdateFp)mndConsumerActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndConsumerActionDelete};

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupConsumer(SMnode *pMnode) {}

SMqConsumerObj *mndCreateConsumer(int64_t consumerId, const char *cgroup) {
  SMqConsumerObj *pConsumer = calloc(1, sizeof(SMqConsumerObj));
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pConsumer->recentRemovedTopics = taosArrayInit(1, sizeof(char *));
  pConsumer->epoch = 1;
  pConsumer->consumerId = consumerId;
  atomic_store_32(&pConsumer->status, MQ_CONSUMER_STATUS__INIT);
  strcpy(pConsumer->cgroup, cgroup);
  taosInitRWLatch(&pConsumer->lock);
  return pConsumer;
}

SSdbRaw *mndConsumerActionEncode(SMqConsumerObj *pConsumer) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  void   *buf = NULL;
  int32_t tlen = tEncodeSMqConsumerObj(NULL, pConsumer);
  int32_t size = sizeof(int32_t) + tlen + MND_CONSUMER_RESERVE_SIZE;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_CONSUMER, MND_CONSUMER_VER_NUMBER, size);
  if (pRaw == NULL) goto CM_ENCODE_OVER;

  buf = malloc(tlen);
  if (buf == NULL) goto CM_ENCODE_OVER;

  void *abuf = buf;
  tEncodeSMqConsumerObj(&abuf, pConsumer);

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, CM_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, CM_ENCODE_OVER);
  SDB_SET_RESERVE(pRaw, dataPos, MND_CONSUMER_RESERVE_SIZE, CM_ENCODE_OVER);
  SDB_SET_DATALEN(pRaw, dataPos, CM_ENCODE_OVER);

  terrno = TSDB_CODE_SUCCESS;

CM_ENCODE_OVER:
  tfree(buf);
  if (terrno != 0) {
    mError("consumer:%" PRId64 ", failed to encode to raw:%p since %s", pConsumer->consumerId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("consumer:%" PRId64 ", encode to raw:%p, row:%p", pConsumer->consumerId, pRaw, pConsumer);
  return pRaw;
}

SSdbRow *mndConsumerActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto CM_DECODE_OVER;

  if (sver != MND_CONSUMER_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto CM_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SMqConsumerObj));
  if (pRow == NULL) goto CM_DECODE_OVER;

  SMqConsumerObj *pConsumer = sdbGetRowObj(pRow);
  if (pConsumer == NULL) goto CM_DECODE_OVER;

  int32_t dataPos = 0;
  int32_t len;
  SDB_GET_INT32(pRaw, dataPos, &len, CM_DECODE_OVER);
  buf = malloc(len);
  if (buf == NULL) goto CM_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, len, CM_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_CONSUMER_RESERVE_SIZE, CM_DECODE_OVER);

  if (tDecodeSMqConsumerObj(buf, pConsumer) == NULL) {
    goto CM_DECODE_OVER;
  }

  terrno = TSDB_CODE_SUCCESS;

CM_DECODE_OVER:
  tfree(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("consumer:%" PRId64 ", failed to decode from raw:%p since %s", pConsumer->consumerId, pRaw, terrstr());
    tfree(pRow);
    return NULL;
  }

  return pRow;
}

static int32_t mndConsumerActionInsert(SSdb *pSdb, SMqConsumerObj *pConsumer) {
  mTrace("consumer:%" PRId64 ", perform insert action", pConsumer->consumerId);
  return 0;
}

static int32_t mndConsumerActionDelete(SSdb *pSdb, SMqConsumerObj *pConsumer) {
  mTrace("consumer:%" PRId64 ", perform delete action", pConsumer->consumerId);
  return 0;
}

static int32_t mndConsumerActionUpdate(SSdb *pSdb, SMqConsumerObj *pOldConsumer, SMqConsumerObj *pNewConsumer) {
  mTrace("consumer:%" PRId64 ", perform update action", pOldConsumer->consumerId);

  // TODO handle update
  /*taosWLockLatch(&pOldConsumer->lock);*/
  /*taosWUnLockLatch(&pOldConsumer->lock);*/

  return 0;
}

SMqConsumerObj *mndAcquireConsumer(SMnode *pMnode, int64_t consumerId) {
  SSdb           *pSdb = pMnode->pSdb;
  SMqConsumerObj *pConsumer = sdbAcquire(pSdb, SDB_CONSUMER, &consumerId);
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
  }
  return pConsumer;
}

void mndReleaseConsumer(SMnode *pMnode, SMqConsumerObj *pConsumer) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pConsumer);
}
