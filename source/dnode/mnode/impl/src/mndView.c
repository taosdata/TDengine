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

#include "mndView.h"
#include "mndShow.h"

int32_t mndInitView(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_VIEW, mndProcessCreateViewReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_VIEW, mndProcessDropViewReq);
  mndSetMsgHandle(pMnode, TDMT_MND_VIEW_META, mndProcessGetViewMetaReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VIEWS, mndRetrieveView);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VIEWS, mndCancelGetNextView);

#ifdef TD_ENTERPRISE
  initDynViewVersion();

  SSdbTable table = {
      .sdbType = SDB_VIEW,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndViewActionEncode,
      .decodeFp = (SdbDecodeFp)mndViewActionDecode,
      .insertFp = (SdbInsertFp)mndViewActionInsert,
      .updateFp = (SdbUpdateFp)mndViewActionUpdate,
      .deleteFp = (SdbDeleteFp)mndViewActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
#else
  return TSDB_CODE_SUCCESS;
#endif
}

void mndCleanupView(SMnode *pMnode) {
  mDebug("mnd view cleanup");
}

int32_t mndProcessCreateViewReq(SRpcMsg *pReq) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  SCMCreateViewReq   createViewReq = {0};
  if (tDeserializeSCMCreateViewReq(pReq->pCont, pReq->contLen, &createViewReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  mInfo("start to create view:%s, sql:%s", createViewReq.fullname, createViewReq.sql);

  return mndProcessCreateViewReqImpl(&createViewReq, pReq);
#endif
}

int32_t mndProcessDropViewReq(SRpcMsg *pReq) {
#ifndef TD_ENTERPRISE
    return TSDB_CODE_OPS_NOT_SUPPORT;
#else
    SCMDropViewReq dropViewReq = {0};
    if (tDeserializeSCMDropViewReq(pReq->pCont, pReq->contLen, &dropViewReq) != 0) {
      terrno = TSDB_CODE_INVALID_MSG;
      return -1;
    }
  
    mInfo("start to drop view:%s, sql:%s", dropViewReq.name, dropViewReq.sql);
  
    return mndProcessDropViewReqImpl(&dropViewReq, pReq);
#endif
}

int32_t mndProcessGetViewMetaReq(SRpcMsg *pReq) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  SViewMetaReq  req = {0};

  if (tDeserializeSViewMetaReq(pReq->pCont, pReq->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  return mndProcessViewMetaReqImpl(&req, pReq);
#endif  
}


int32_t mndRetrieveView(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
#ifndef TD_ENTERPRISE
  return 0;
#else
  return mndRetrieveViewImpl(pReq, pShow, pBlock, rows);
#endif
}

void mndCancelGetNextView(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}



