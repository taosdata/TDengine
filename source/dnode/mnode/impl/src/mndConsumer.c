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
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tname.h"

#define MND_CONSUMER_VER_NUMBER 1
#define MND_CONSUMER_RESERVE_SIZE 64

static SSdbRaw *mndConsumerActionEncode(SConsumerObj *pConsumer);
static SSdbRow *mndConsumerActionDecode(SSdbRaw *pRaw);
static int32_t  mndConsumerActionInsert(SSdb *pSdb, SConsumerObj *pConsumer);
static int32_t  mndConsumerActionDelete(SSdb *pSdb, SConsumerObj *pConsumer);
static int32_t  mndConsumerActionUpdate(SSdb *pSdb, SConsumerObj *pConsumer, SConsumerObj *pNewConsumer);
static int32_t  mndProcessCreateConsumerMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessDropConsumerMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessDropConsumerInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessConsumerMetaMsg(SMnodeMsg *pMsg);
static int32_t  mndGetConsumerMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveConsumer(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextConsumer(SMnode *pMnode, void *pIter);

static int32_t mndProcessSubscribeReq(SMnodeMsg *pMsg);
static int32_t mndProcessSubscribeRsp(SMnodeMsg *pMsg);
static int32_t mndProcessSubscribeInternalReq(SMnodeMsg *pMsg);
static int32_t mndProcessSubscribeInternalRsp(SMnodeMsg *pMsg);

int32_t mndInitConsumer(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_CONSUMER,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndConsumerActionEncode,
                     .decodeFp = (SdbDecodeFp)mndConsumerActionDecode,
                     .insertFp = (SdbInsertFp)mndConsumerActionInsert,
                     .updateFp = (SdbUpdateFp)mndConsumerActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndConsumerActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_SUBSCRIBE, mndProcessSubscribeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_SUBSCRIBE_RSP, mndProcessSubscribeRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_SUBSCRIBE, mndProcessSubscribeInternalReq);
  mndSetMsgHandle(pMnode, TDMT_VND_SUBSCRIBE_RSP, mndProcessSubscribeInternalRsp);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupConsumer(SMnode *pMnode) {}

static SSdbRaw *mndConsumerActionEncode(SConsumerObj *pConsumer) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t  size = sizeof(SConsumerObj) + MND_CONSUMER_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_CONSUMER, MND_CONSUMER_VER_NUMBER, size);
  if (pRaw == NULL) goto CM_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pConsumer->name, TSDB_TABLE_FNAME_LEN, CM_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pConsumer->db, TSDB_DB_FNAME_LEN, CM_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pConsumer->createTime, CM_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pConsumer->updateTime, CM_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pConsumer->uid, CM_ENCODE_OVER)
  /*SDB_SET_INT64(pRaw, dataPos, pConsumer->dbUid);*/
  SDB_SET_INT32(pRaw, dataPos, pConsumer->version, CM_ENCODE_OVER)

  SDB_SET_RESERVE(pRaw, dataPos, MND_CONSUMER_RESERVE_SIZE, CM_ENCODE_OVER)
  SDB_SET_DATALEN(pRaw, dataPos, CM_ENCODE_OVER)

CM_ENCODE_OVER:
  if (terrno != 0) {
    mError("consumer:%s, failed to encode to raw:%p since %s", pConsumer->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("consumer:%s, encode to raw:%p, row:%p", pConsumer->name, pRaw, pConsumer);
  return pRaw;
}

static SSdbRow *mndConsumerActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto CONSUME_DECODE_OVER;

  if (sver != MND_CONSUMER_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto CONSUME_DECODE_OVER;
  }

  int32_t  size = sizeof(SConsumerObj) + TSDB_MAX_COLUMNS * sizeof(SSchema);
  SSdbRow *pRow = sdbAllocRow(size);
  if (pRow == NULL) goto CONSUME_DECODE_OVER;

  SConsumerObj *pConsumer = sdbGetRowObj(pRow);
  if (pConsumer == NULL) goto CONSUME_DECODE_OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pConsumer->name, TSDB_TABLE_FNAME_LEN, CONSUME_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pConsumer->db, TSDB_DB_FNAME_LEN, CONSUME_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pConsumer->createTime, CONSUME_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pConsumer->updateTime, CONSUME_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pConsumer->uid, CONSUME_DECODE_OVER)
  /*SDB_GET_INT64(pRaw, pRow, dataPos, &pConsumer->dbUid);*/
  SDB_GET_INT32(pRaw, dataPos, &pConsumer->version, CONSUME_DECODE_OVER)
  SDB_GET_RESERVE(pRaw, dataPos, MND_CONSUMER_RESERVE_SIZE, CONSUME_DECODE_OVER)
  terrno = 0;

CONSUME_DECODE_OVER:
  if (terrno != 0) {
    mError("consumer:%s, failed to decode from raw:%p since %s", pConsumer->name, pRaw, terrstr());
    tfree(pRow);
    return NULL;
  }

  mTrace("consumer:%s, decode from raw:%p, row:%p", pConsumer->name, pRaw, pConsumer);
  return pRow;
}

static int32_t mndConsumerActionInsert(SSdb *pSdb, SConsumerObj *pConsumer) {
  mTrace("consumer:%s, perform insert action, row:%p", pConsumer->name, pConsumer);
  return 0;
}

static int32_t mndConsumerActionDelete(SSdb *pSdb, SConsumerObj *pConsumer) {
  mTrace("consumer:%s, perform delete action, row:%p", pConsumer->name, pConsumer);
  return 0;
}

static int32_t mndConsumerActionUpdate(SSdb *pSdb, SConsumerObj *pOldConsumer, SConsumerObj *pNewConsumer) {
  mTrace("consumer:%s, perform update action, old_row:%p new_row:%p", pOldConsumer->name, pOldConsumer, pNewConsumer);
  atomic_exchange_32(&pOldConsumer->updateTime, pNewConsumer->updateTime);
  atomic_exchange_32(&pOldConsumer->version, pNewConsumer->version);

  taosWLockLatch(&pOldConsumer->lock);

  // TODO handle update

  taosWUnLockLatch(&pOldConsumer->lock);
  return 0;
}

SConsumerObj *mndAcquireConsumer(SMnode *pMnode, int32_t consumerId) {
  SSdb         *pSdb = pMnode->pSdb;
  SConsumerObj *pConsumer = sdbAcquire(pSdb, SDB_CONSUMER, &consumerId);
  if (pConsumer == NULL) {
    /*terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;*/
  }
  return pConsumer;
}

void mndReleaseConsumer(SMnode *pMnode, SConsumerObj *pConsumer) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pConsumer);
}

static int32_t mndProcessSubscribeReq(SMnodeMsg *pMsg) {
  SMnode          *pMnode = pMsg->pMnode;
  char            *msgStr = pMsg->rpcMsg.pCont;
  SCMSubscribeReq *pSubscribe;
  tDeserializeSCMSubscribeReq(msgStr, pSubscribe);
  // add consumerGroupId -> list<consumerId> to sdb
  // add consumerId -> list<consumer> to sdb
  // add consumer -> list<consumerId> to sdb
  return 0;
}

static int32_t mndProcessSubscribeRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessSubscribeInternalReq(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessSubscribeInternalRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessDropConsumerInRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndProcessConsumerMetaMsg(SMnodeMsg *pMsg) {
  SMnode        *pMnode = pMsg->pMnode;
  STableInfoMsg *pInfo = pMsg->rpcMsg.pCont;

  mDebug("consumer:%s, start to retrieve meta", pInfo->tableFname);

#if 0
  SDbObj *pDb = mndAcquireDbByConsumer(pMnode, pInfo->tableFname);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    mError("consumer:%s, failed to retrieve meta since %s", pInfo->tableFname, terrstr());
    return -1;
  }

  SConsumerObj *pConsumer = mndAcquireConsumer(pMnode, pInfo->tableFname);
  if (pConsumer == NULL) {
    mndReleaseDb(pMnode, pDb);
    terrno = TSDB_CODE_MND_INVALID_CONSUMER;
    mError("consumer:%s, failed to get meta since %s", pInfo->tableFname, terrstr());
    return -1;
  }

  taosRLockLatch(&pConsumer->lock);
  int32_t totalCols = pConsumer->numOfColumns + pConsumer->numOfTags;
  int32_t contLen = sizeof(STableMetaMsg) + totalCols * sizeof(SSchema);

  STableMetaMsg *pMeta = rpcMallocCont(contLen);
  if (pMeta == NULL) {
    taosRUnLockLatch(&pConsumer->lock);
    mndReleaseDb(pMnode, pDb);
    mndReleaseConsumer(pMnode, pConsumer);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("consumer:%s, failed to get meta since %s", pInfo->tableFname, terrstr());
    return -1;
  }

  memcpy(pMeta->consumerFname, pConsumer->name, TSDB_TABLE_FNAME_LEN);
  pMeta->numOfTags = htonl(pConsumer->numOfTags);
  pMeta->numOfColumns = htonl(pConsumer->numOfColumns);
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = TSDB_SUPER_TABLE;
  pMeta->update = pDb->cfg.update;
  pMeta->sversion = htonl(pConsumer->version);
  pMeta->tuid = htonl(pConsumer->uid);

  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pMeta->pSchema[i];
    SSchema *pSrcSchema = &pConsumer->pSchema[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->colId = htonl(pSrcSchema->colId);
    pSchema->bytes = htonl(pSrcSchema->bytes);
  }
  taosRUnLockLatch(&pConsumer->lock);
  mndReleaseDb(pMnode, pDb);
  mndReleaseConsumer(pMnode, pConsumer);

  pMsg->pCont = pMeta;
  pMsg->contLen = contLen;

  mDebug("consumer:%s, meta is retrieved, cols:%d tags:%d", pInfo->tableFname, pConsumer->numOfColumns, pConsumer->numOfTags);
#endif
  return 0;
}

static int32_t mndGetNumOfConsumers(SMnode *pMnode, char *dbName, int32_t *pNumOfConsumers) {
  SSdb *pSdb = pMnode->pSdb;

  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfConsumers = 0;
  void   *pIter = NULL;
  while (1) {
    SConsumerObj *pConsumer = NULL;
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) break;

    if (strcmp(pConsumer->db, dbName) == 0) {
      numOfConsumers++;
    }

    sdbRelease(pSdb, pConsumer);
  }

  *pNumOfConsumers = numOfConsumers;
  return 0;
}

static int32_t mndGetConsumerMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  if (mndGetNumOfConsumers(pMnode, pShow->db, &pShow->numOfRows) != 0) {
    return -1;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "columns");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "tags");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_CONSUMER);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveConsumer(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode       *pMnode = pMsg->pMnode;
  SSdb         *pSdb = pMnode->pSdb;
  int32_t       numOfRows = 0;
  SConsumerObj *pConsumer = NULL;
  int32_t       cols = 0;
  char         *pWrite;
  char          prefix[64] = {0};

  tstrncpy(prefix, pShow->db, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_CONSUMER, pShow->pIter, (void **)&pConsumer);
    if (pShow->pIter == NULL) break;

    if (strncmp(pConsumer->name, prefix, prefixLen) != 0) {
      sdbRelease(pSdb, pConsumer);
      continue;
    }

    cols = 0;

    char consumerName[TSDB_TABLE_NAME_LEN] = {0};
    tstrncpy(consumerName, pConsumer->name + prefixLen, TSDB_TABLE_NAME_LEN);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, consumerName);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pConsumer->createTime;
    cols++;

    /*pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;*/
    /**(int32_t *)pWrite = pConsumer->numOfColumns;*/
    /*cols++;*/

    /*pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;*/
    /**(int32_t *)pWrite = pConsumer->numOfTags;*/
    /*cols++;*/

    numOfRows++;
    sdbRelease(pSdb, pConsumer);
  }

  pShow->numOfReads += numOfRows;
  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static void mndCancelGetNextConsumer(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
