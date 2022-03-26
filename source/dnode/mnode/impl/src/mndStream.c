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
#include "parser.h"
#include "mndAuth.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tname.h"

#define MND_STREAM_VER_NUMBER   1
#define MND_STREAM_RESERVE_SIZE 64

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pStream, SStreamObj *pNewStream);
static int32_t mndProcessCreateStreamReq(SNodeMsg *pReq);
static int32_t mndProcessTaskDeployInternalRsp(SNodeMsg *pRsp);
/*static int32_t mndProcessDropStreamReq(SNodeMsg *pReq);*/
/*static int32_t mndProcessDropStreamInRsp(SNodeMsg *pRsp);*/
static int32_t mndProcessStreamMetaReq(SNodeMsg *pReq);
static int32_t mndGetStreamMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t mndRetrieveStream(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void    mndCancelGetNextStream(SMnode *pMnode, void *pIter);

int32_t mndInitStream(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_STREAM,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndStreamActionEncode,
                     .decodeFp = (SdbDecodeFp)mndStreamActionDecode,
                     .insertFp = (SdbInsertFp)mndStreamActionInsert,
                     .updateFp = (SdbUpdateFp)mndStreamActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndStreamActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STREAM, mndProcessCreateStreamReq);
  mndSetMsgHandle(pMnode, TDMT_VND_TASK_DEPLOY_RSP, mndProcessTaskDeployInternalRsp);
  mndSetMsgHandle(pMnode, TDMT_SND_TASK_DEPLOY_RSP, mndProcessTaskDeployInternalRsp);
  /*mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM, mndProcessDropStreamReq);*/
  /*mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM_RSP, mndProcessDropStreamInRsp);*/

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_TP, mndGetStreamMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TP, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TP, mndCancelGetNextStream);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupStream(SMnode *pMnode) {}

SSdbRaw *mndStreamActionEncode(SStreamObj *pStream) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void *buf = NULL;

  SCoder encoder;
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, NULL, 0, TD_ENCODER);
  if (tEncodeSStreamObj(&encoder, pStream) < 0) {
    tCoderClear(&encoder);
    goto STREAM_ENCODE_OVER;
  }
  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);

  int32_t  size = sizeof(int32_t) + tlen + MND_STREAM_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STREAM, MND_STREAM_VER_NUMBER, size);
  if (pRaw == NULL) goto STREAM_ENCODE_OVER;

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto STREAM_ENCODE_OVER;

  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, tlen, TD_ENCODER);
  if (tEncodeSStreamObj(&encoder, pStream) < 0) {
    tCoderClear(&encoder);
    goto STREAM_ENCODE_OVER;
  }
  tCoderClear(&encoder);

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

  mTrace("stream:%s, encode to raw:%p, row:%p", pStream->name, pRaw, pStream);
  return pRaw;
}

SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto STREAM_DECODE_OVER;

  if (sver != MND_STREAM_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto STREAM_DECODE_OVER;
  }

  int32_t  size = sizeof(SStreamObj);
  SSdbRow *pRow = sdbAllocRow(size);
  if (pRow == NULL) goto STREAM_DECODE_OVER;

  SStreamObj *pStream = sdbGetRowObj(pRow);
  if (pStream == NULL) goto STREAM_DECODE_OVER;

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, STREAM_DECODE_OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) goto STREAM_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, STREAM_DECODE_OVER);

  SCoder decoder;
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, tlen + 1, TD_DECODER);
  if (tDecodeSStreamObj(&decoder, pStream) < 0) {
    goto STREAM_DECODE_OVER;
  }

  terrno = TSDB_CODE_SUCCESS;

STREAM_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("stream:%s, failed to decode from raw:%p since %s", pStream->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("stream:%s, decode from raw:%p, row:%p", pStream->name, pRaw, pStream);
  return pRow;
}

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream) {
  mTrace("stream:%s, perform insert action", pStream->name);
  return 0;
}

static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream) {
  mTrace("stream:%s, perform delete action", pStream->name);
  return 0;
}

static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream) {
  mTrace("stream:%s, perform update action", pOldStream->name);
  atomic_exchange_64(&pOldStream->updateTime, pNewStream->updateTime);
  atomic_exchange_32(&pOldStream->version, pNewStream->version);

  taosWLockLatch(&pOldStream->lock);

  // TODO handle update

  taosWUnLockLatch(&pOldStream->lock);
  return 0;
}

SStreamObj *mndAcquireStream(SMnode *pMnode, char *streamName) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = sdbAcquire(pSdb, SDB_STREAM, streamName);
  if (pStream == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
  }
  return pStream;
}

void mndReleaseStream(SMnode *pMnode, SStreamObj *pStream) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pStream);
}

static int32_t mndProcessTaskDeployInternalRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static SDbObj *mndAcquireDbByStream(SMnode *pMnode, char *streamName) {
  SName name = {0};
  tNameFromString(&name, streamName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  char db[TSDB_STREAM_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, db);

  return mndAcquireDb(pMnode, db);
}

static int32_t mndCheckCreateStreamReq(SCMCreateStreamReq *pCreate) {
  if (pCreate->name[0] == 0 || pCreate->sql == NULL || pCreate->sql[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_STREAM_OPTION;
    return -1;
  }
  return 0;
}

static int32_t mndStreamGetPlanString(const char *ast, char **pStr) {
  if (NULL == ast) {
    return TSDB_CODE_SUCCESS;
  }

  SNode  *pAst = NULL;
  int32_t code = nodesStringToNode(ast, &pAst);

  SQueryPlan *pPlan = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    SPlanContext cxt = {
        .pAstRoot = pAst,
        .topicQuery = false,
        .streamQuery = true,
    };
    code = qCreateQueryPlan(&cxt, &pPlan, NULL);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesNodeToString(pPlan, false, pStr, NULL);
  }
  nodesDestroyNode(pAst);
  nodesDestroyNode(pPlan);
  terrno = code;
  return code;
}

int32_t mndAddStreamToTrans(SMnode *pMnode, SStreamObj *pStream, const char *ast, STrans *pTrans) {
  SNode *pAst = NULL;
#if 1 // TODO: remove debug info later
  printf("ast = %s\n", ast); 
#endif
  if (nodesStringToNode(ast, &pAst) < 0) {
    return -1;
  }
#if 1
  SSchemaWrapper sw = {0};
  qExtractResultSchema(pAst, (int32_t*)&sw.nCols, &sw.pSchema);

  printf("|");
  for (int i = 0; i < sw.nCols; i++) {
    printf(" %15s |", (char *)sw.pSchema[i].name);
  }
  printf("\n=======================================================\n");

  pStream->ColAlias = NULL;
#endif

  if (TSDB_CODE_SUCCESS != mndStreamGetPlanString(ast, &pStream->physicalPlan)) {
    mError("topic:%s, failed to get plan since %s", pStream->name, terrstr());
    return -1;
  }

  if (mndScheduleStream(pMnode, pTrans, pStream) < 0) {
    mError("stream:%ld, schedule stream since %s", pStream->uid, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create stream:%s", pTrans->id, pStream->name);

  SSdbRaw *pRedoRaw = mndStreamActionEncode(pStream);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

  return 0;
}

static int32_t mndCreateStream(SMnode *pMnode, SNodeMsg *pReq, SCMCreateStreamReq *pCreate, SDbObj *pDb) {
  mDebug("stream:%s to create", pCreate->name);
  SStreamObj streamObj = {0};
  tstrncpy(streamObj.name, pCreate->name, TSDB_STREAM_FNAME_LEN);
  tstrncpy(streamObj.db, pDb->name, TSDB_DB_FNAME_LEN);
  tstrncpy(streamObj.outputSTbName, pCreate->outputSTbName, TSDB_TABLE_FNAME_LEN);
  streamObj.createTime = taosGetTimestampMs();
  streamObj.updateTime = streamObj.createTime;
  streamObj.uid = mndGenerateUid(pCreate->name, strlen(pCreate->name));
  streamObj.dbUid = pDb->uid;
  streamObj.version = 1;
  streamObj.sql = pCreate->sql;
  /*streamObj.physicalPlan = "";*/
  streamObj.logicalPlan = "not implemented";

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_CREATE_STREAM, &pReq->rpcMsg);
  if (pTrans == NULL) {
    mError("stream:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create stream:%s", pTrans->id, pCreate->name);

  if (mndAddStreamToTrans(pMnode, &streamObj, pCreate->ast, pTrans) != 0) {
    mError("trans:%d, failed to add stream since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateStreamReq(SNodeMsg *pReq) {
  SMnode            *pMnode = pReq->pNode;
  int32_t            code = -1;
  SStreamObj        *pStream = NULL;
  SDbObj            *pDb = NULL;
  SUserObj          *pUser = NULL;
  SCMCreateStreamReq createStreamReq = {0};

  if (tDeserializeSCMCreateStreamReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createStreamReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CREATE_STREAM_OVER;
  }

  mDebug("stream:%s, start to create, sql:%s", createStreamReq.name, createStreamReq.sql);

  if (mndCheckCreateStreamReq(&createStreamReq) != 0) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto CREATE_STREAM_OVER;
  }

  pStream = mndAcquireStream(pMnode, createStreamReq.name);
  if (pStream != NULL) {
    if (createStreamReq.igExists) {
      mDebug("stream:%s, already exist, ignore exist is set", createStreamReq.name);
      code = 0;
      goto CREATE_STREAM_OVER;
    } else {
      terrno = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
      goto CREATE_STREAM_OVER;
    }
  } else if (terrno != TSDB_CODE_MND_STREAM_NOT_EXIST) {
    goto CREATE_STREAM_OVER;
  }

  pDb = mndAcquireDbByStream(pMnode, createStreamReq.name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto CREATE_STREAM_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto CREATE_STREAM_OVER;
  }

  if (mndCheckWriteAuth(pUser, pDb) != 0) {
    goto CREATE_STREAM_OVER;
  }

  code = mndCreateStream(pMnode, pReq, &createStreamReq, pDb);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

CREATE_STREAM_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
  }

  mndReleaseStream(pMnode, pStream);
  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);

  tFreeSCMCreateStreamReq(&createStreamReq);
  return code;
}

static int32_t mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfStreams = 0;
  void   *pIter = NULL;
  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    if (pStream->dbUid == pDb->uid) {
      numOfStreams++;
    }

    sdbRelease(pSdb, pStream);
  }

  *pNumOfStreams = numOfStreams;
  mndReleaseDb(pMnode, pDb);
  return 0;
}

static int32_t mndGetStreamMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;

  if (mndGetNumOfStreams(pMnode, pShow->db, &pShow->numOfRows) != 0) {
    return -1;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchemas;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "sql");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pMeta->numOfColumns = cols;
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_STREAM);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbName, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveStream(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode     *pMnode = pReq->pNode;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;
  int32_t     cols = 0;
  char       *pWrite;
  char        prefix[TSDB_DB_FNAME_LEN] = {0};

  SDbObj *pDb = mndAcquireDb(pMnode, pShow->db);
  if (pDb == NULL) return 0;

  tstrncpy(prefix, pShow->db, TSDB_DB_FNAME_LEN);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) break;

    if (pStream->dbUid != pDb->uid) {
      if (strncmp(pStream->name, prefix, prefixLen) != 0) {
        mError("Inconsistent stream data, name:%s, db:%s, dbUid:%" PRIu64, pStream->name, pDb->name, pDb->uid);
      }

      sdbRelease(pSdb, pStream);
      continue;
    }

    cols = 0;

    char streamName[TSDB_TABLE_NAME_LEN] = {0};
    tstrncpy(streamName, pStream->name + prefixLen, TSDB_TABLE_NAME_LEN);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, streamName);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pStream->createTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pStream->sql, pShow->bytes[cols]);
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pStream);
  }

  mndReleaseDb(pMnode, pDb);
  pShow->numOfReads += numOfRows;
  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static void mndCancelGetNextStream(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
