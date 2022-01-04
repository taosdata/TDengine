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

static SSdbRaw *mndCGroupActionEncode(SCGroupObj *pCGroup) {
  int32_t  size = sizeof(SConsumerObj) + MND_CONSUMER_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_CONSUMER, MND_CONSUMER_VER_NUMBER, size);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pCGroup->name, TSDB_TABLE_FNAME_LEN);
  SDB_SET_INT64(pRaw, dataPos, pCGroup->createTime);
  SDB_SET_INT64(pRaw, dataPos, pCGroup->updateTime);
  SDB_SET_INT64(pRaw, dataPos, pCGroup->uid);
  /*SDB_SET_INT64(pRaw, dataPos, pConsumer->dbUid);*/
  SDB_SET_INT32(pRaw, dataPos, pCGroup->version);

  int32_t sz = listNEles(pCGroup->consumerIds);
  SDB_SET_INT32(pRaw, dataPos, sz);

  SListIter iter;
  tdListInitIter(pCGroup->consumerIds, &iter, TD_LIST_FORWARD);
  SListNode *pn = NULL;
  while ((pn = tdListNext(&iter)) != NULL) {
    int64_t consumerId = *(int64_t *)pn->data;
    SDB_SET_INT64(pRaw, dataPos, consumerId);
  }

  SDB_SET_RESERVE(pRaw, dataPos, MND_CONSUMER_RESERVE_SIZE);
  SDB_SET_DATALEN(pRaw, dataPos);
  return pRaw;
}

static SSdbRow *mndCGroupActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != MND_CONSUMER_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode cgroup since %s", terrstr());
    return NULL;
  }

  // TODO: maximum size is not known
  int32_t     size = sizeof(SCGroupObj) + 128 * sizeof(int64_t);
  SSdbRow    *pRow = sdbAllocRow(size);
  SCGroupObj *pCGroup = sdbGetRowObj(pRow);
  if (pCGroup == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pCGroup->name, TSDB_TABLE_FNAME_LEN);
  SDB_GET_INT64(pRaw, pRow, dataPos, &pCGroup->createTime);
  SDB_GET_INT64(pRaw, pRow, dataPos, &pCGroup->updateTime);
  SDB_GET_INT64(pRaw, pRow, dataPos, &pCGroup->uid);
  /*SDB_GET_INT64(pRaw, pRow, dataPos, &pConsumer->dbUid);*/
  SDB_GET_INT32(pRaw, pRow, dataPos, &pCGroup->version);

  int32_t sz;
  SDB_GET_INT32(pRaw, pRow, dataPos, &sz);
  // TODO: free list when failing
  tdListInit(pCGroup->consumerIds, sizeof(int64_t));
  for (int i = 0; i < sz; i++) {
    int64_t consumerId;
    SDB_GET_INT64(pRaw, pRow, dataPos, &consumerId);
    tdListAppend(pCGroup->consumerIds, &consumerId);
  }

  SDB_GET_RESERVE(pRaw, pRow, dataPos, MND_CONSUMER_RESERVE_SIZE);
  return pRow;
}

static SSdbRaw *mndConsumerActionEncode(SConsumerObj *pConsumer) {
  int32_t  size = sizeof(SConsumerObj) + MND_CONSUMER_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_CONSUMER, MND_CONSUMER_VER_NUMBER, size);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT64(pRaw, dataPos, pConsumer->uid);
  SDB_SET_INT64(pRaw, dataPos, pConsumer->createTime);
  SDB_SET_INT64(pRaw, dataPos, pConsumer->updateTime);
  /*SDB_SET_INT64(pRaw, dataPos, pConsumer->dbUid);*/
  SDB_SET_INT32(pRaw, dataPos, pConsumer->version);

  SDB_SET_RESERVE(pRaw, dataPos, MND_CONSUMER_RESERVE_SIZE);
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndConsumerActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != MND_CONSUMER_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode consumer since %s", terrstr());
    return NULL;
  }

  int32_t       size = sizeof(SConsumerObj) + TSDB_MAX_COLUMNS * sizeof(SSchema);
  SSdbRow      *pRow = sdbAllocRow(size);
  SConsumerObj *pConsumer = sdbGetRowObj(pRow);
  if (pConsumer == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT64(pRaw, pRow, dataPos, &pConsumer->uid);
  SDB_GET_INT64(pRaw, pRow, dataPos, &pConsumer->createTime);
  SDB_GET_INT64(pRaw, pRow, dataPos, &pConsumer->updateTime);
  /*SDB_GET_INT64(pRaw, pRow, dataPos, &pConsumer->dbUid);*/
  SDB_GET_INT32(pRaw, pRow, dataPos, &pConsumer->version);

  SDB_GET_RESERVE(pRaw, pRow, dataPos, MND_CONSUMER_RESERVE_SIZE);

  return pRow;
}

static int32_t mndConsumerActionInsert(SSdb *pSdb, SConsumerObj *pConsumer) {
  mTrace("consumer:%ld, perform insert action", pConsumer->uid);
  return 0;
}

static int32_t mndConsumerActionDelete(SSdb *pSdb, SConsumerObj *pConsumer) {
  mTrace("consumer:%ld, perform delete action", pConsumer->uid);
  return 0;
}

static int32_t mndConsumerActionUpdate(SSdb *pSdb, SConsumerObj *pOldConsumer, SConsumerObj *pNewConsumer) {
  mTrace("consumer:%ld, perform update action", pOldConsumer->uid);
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
  int     topicNum = pSubscribe->topicNum;
  int64_t consumerId = pSubscribe->consumerId;
  char   *consumerGroup = pSubscribe->consumerGroup;
  // get consumer group and add client into it
  SCGroupObj *pCGroup = sdbAcquire(pMnode->pSdb, SDB_CGROUP, consumerGroup);
  if (pCGroup != NULL) {
    // iterate the list until finding the consumer
    // add consumer to cgroup list if not found
    // put new record
  }

  SConsumerObj *pConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pConsumer != NULL) {
    //reset topic list
  }

  for (int i = 0; i < topicNum; i++) {
    char *topicName = pSubscribe->topicName[i];
    STopicObj *pTopic = mndAcquireTopic(pMnode, topicName);
    //get 
    // consumer id
    SList *list = pTopic->consumerIds;
    // add the consumer if not in the list
    //
    SList* topicList = pConsumer->topics;
    //add to topic
  }

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

    numOfConsumers++;

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

static int32_t mndRetrieveCGroup(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode       *pMnode = pMsg->pMnode;
  SSdb         *pSdb = pMnode->pSdb;
  int32_t       numOfRows = 0;
  SCGroupObj   *pCGroup = NULL;
  int32_t       cols = 0;
  char         *pWrite;
  char          prefix[64] = {0};

  tstrncpy(prefix, pShow->db, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_CONSUMER, pShow->pIter, (void **)&pCGroup);
    if (pShow->pIter == NULL) break;

    if (strncmp(pCGroup->name, prefix, prefixLen) != 0) {
      sdbRelease(pSdb, pCGroup);
      continue;
    }

    cols = 0;

    char consumerName[TSDB_TABLE_NAME_LEN] = {0};
    tstrncpy(consumerName, pCGroup->name + prefixLen, TSDB_TABLE_NAME_LEN);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, consumerName);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pCGroup->createTime;
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pCGroup);
  }

  pShow->numOfReads += numOfRows;
  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static void mndCancelGetNextConsumer(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
