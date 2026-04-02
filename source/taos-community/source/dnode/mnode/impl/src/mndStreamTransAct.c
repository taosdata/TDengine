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


static int32_t doSetStopAllTasksAction(SMnode* pMnode, STrans* pTrans, SVgObj* pVgObj) {
  void   *pBuf = NULL;
  int32_t len = 0;
  int32_t code = 0;
  SEncoder encoder;

  SStreamTaskStopReq req = {.streamId = -1};
  tEncodeSize(tEncodeStreamTaskStopReq, &req, len, code);
  if (code < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  int32_t tlen = sizeof(SMsgHead) + len;

  pBuf = taosMemoryMalloc(tlen);
  if (pBuf == NULL) {
    return terrno;
  }

  void    *abuf = POINTER_SHIFT(pBuf, sizeof(SMsgHead));
  tEncoderInit(&encoder, abuf, tlen);
  code = tEncodeStreamTaskStopReq(&encoder, &req);
  if (code == -1) {
    tEncoderClear(&encoder);
    taosMemoryFree(pBuf);
    return code;
  }

  SMsgHead *pMsgHead = (SMsgHead *)pBuf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(pVgObj->vgId);

  tEncoderClear(&encoder);

  SEpSet epset = mndGetVgroupEpset(pMnode, pVgObj);
  // mndReleaseVgroup(pMnode, pVgObj);

  code = setTransAction(pTrans, pBuf, tlen, TDMT_VND_STREAM_ALL_STOP, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to create stop all streams trans, code:%s", tstrerror(code));
    taosMemoryFree(pBuf);
  }

  return 0;
}

int32_t mndStreamSetStopStreamTasksActions(SMnode* pMnode, STrans *pTrans, uint64_t dbUid) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if(pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    if (pVgroup->dbUid == dbUid) {
      if ((code = doSetStopAllTasksAction(pMnode, pTrans, pVgroup)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  TAOS_RETURN(code);
  return 0;
}
