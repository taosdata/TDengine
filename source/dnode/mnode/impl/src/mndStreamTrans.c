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

#define MAX_CHKPT_EXEC_ELAPSED (600*1000*3)  // 600s

typedef struct SKeyInfo {
  void   *pKey;
  int32_t keyLen;
} SKeyInfo;

static bool identicalName(const char *pDb, const char *pParam, int32_t len) {
  return (strlen(pDb) == len) && (strncmp(pDb, pParam, len) == 0);
}

int32_t mndStreamCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name, STrans **ppTrans) {
  int64_t streamId = pStream->pCreate->streamId;
  int32_t code = 0;

  STrans *p = mndTransCreate(pMnode, TRN_POLICY_RETRY, conflict, pReq, name);
  if (p == NULL) {
    mstsError("failed to build trans:%s, reason: %s", name, tstrerror(terrno));
    return terrno;
  }

  mstsInfo("start to build trans %s, transId:%d", name, p->id);
  p->ableToBeKilled = true;

  mndTransSetDbName(p, pStream->pCreate->streamDB, pStream->pCreate->outTblName);
  if ((code = mndTransCheckConflict(pMnode, p)) != 0) {
    mstsError("failed to build trans:%s for stream, code:%s", name, tstrerror(terrno));
    mndTransDrop(p);
    return code;
  }

  *ppTrans = p;
  return code;
}

SSdbRaw *mndStreamActionEncode(SStreamObj *pStream) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *buf = NULL;
  int64_t streamId = pStream->pCreate->streamId;

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
    mstsError("failed to encode stream %s to raw:%p at line:%d since %s", pStream->pCreate->name, pRaw, lino, tstrerror(code));
    sdbFreeRaw(pRaw);
    terrno = code;
    return NULL;
  }

  mstsTrace("stream %s encoded to raw:%p", pStream->pCreate->name, pRaw);
         
  return pRaw;
}

int32_t mndStreamTransAppend(SStreamObj *pStream, STrans *pTrans, int32_t status) {
  int64_t streamId = pStream->pCreate->streamId;
  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL) {
    mstsError("failed to encode stream since %s", terrstr());
    mndTransDrop(pTrans);
    return terrno;
  }

  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mstsError("stream trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    return terrno;
  }

  if (sdbSetRawStatus(pCommitRaw, status) != 0) {
    mstsError("stream trans:%d failed to set raw status:%d since %s", pTrans->id, status, terrstr());
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
