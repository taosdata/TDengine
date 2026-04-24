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

#include "mndExtSource.h"
#include "mndShow.h"

// ============================================================
// Lifecycle
// ============================================================

int32_t mndInitExtSource(SMnode *pMnode) {
  // Register message handlers — both community and enterprise stubs need these
  // so that the mnode can receive and respond to DDL messages properly.
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_EXT_SOURCE,  mndProcessCreateExtSourceReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_EXT_SOURCE,   mndProcessAlterExtSourceReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_EXT_SOURCE,    mndProcessDropExtSourceReq);
  mndSetMsgHandle(pMnode, TDMT_MND_REFRESH_EXT_SOURCE, mndProcessRefreshExtSourceReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_EXT_SOURCE,     mndProcessGetExtSourceReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_EXT_SOURCES, mndRetrieveExtSources);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_EXT_SOURCES, mndCancelGetNextExtSource);

#ifdef TD_ENTERPRISE
  // Enterprise: register SDB table for persistent storage and recovery
  SSdbTable table = {
    .sdbType  = SDB_EXT_SOURCE,
    .keyType  = SDB_KEY_BINARY,
    .encodeFp = (SdbEncodeFp)mndExtSourceActionEncode,
    .decodeFp = (SdbDecodeFp)mndExtSourceActionDecode,
    .insertFp = (SdbInsertFp)mndExtSourceActionInsert,
    .updateFp = (SdbUpdateFp)mndExtSourceActionUpdate,
    .deleteFp = (SdbDeleteFp)mndExtSourceActionDelete,
  };
  return sdbSetTable(pMnode->pSdb, table);
#else
  return TSDB_CODE_SUCCESS;
#endif
}

void mndCleanupExtSource(SMnode *pMnode) {
  mDebug("mnd ext-source cleanup");
}

// ============================================================
// Message handlers — community stub pattern (same as mndView.c)
// ============================================================

int32_t mndProcessCreateExtSourceReq(SRpcMsg *pReq) {
#ifndef TD_ENTERPRISE
  mError("failed to process create ext source req since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  SCreateExtSourceReq createReq = {0};
  if (tDeserializeSCreateExtSourceReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    tFreeSCreateExtSourceReq(&createReq);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }
  mInfo("start to create ext source:%s", createReq.source_name);
  int32_t code = mndProcessCreateExtSourceReqImpl(&createReq, pReq);
  tFreeSCreateExtSourceReq(&createReq);
  return code;
#endif
}

int32_t mndProcessAlterExtSourceReq(SRpcMsg *pReq) {
#ifndef TD_ENTERPRISE
  mError("failed to process alter ext source req since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  SAlterExtSourceReq alterReq = {0};
  if (tDeserializeSAlterExtSourceReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    tFreeSAlterExtSourceReq(&alterReq);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }
  mInfo("start to alter ext source:%s", alterReq.source_name);
  int32_t code = mndProcessAlterExtSourceReqImpl(&alterReq, pReq);
  tFreeSAlterExtSourceReq(&alterReq);
  return code;
#endif
}

int32_t mndProcessDropExtSourceReq(SRpcMsg *pReq) {
#ifndef TD_ENTERPRISE
  mError("failed to process drop ext source req since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  SDropExtSourceReq dropReq = {0};
  if (tDeserializeSDropExtSourceReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    tFreeSDropExtSourceReq(&dropReq);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }
  mInfo("start to drop ext source:%s", dropReq.source_name);
  int32_t code = mndProcessDropExtSourceReqImpl(&dropReq, pReq);
  tFreeSDropExtSourceReq(&dropReq);
  return code;
#endif
}

int32_t mndProcessRefreshExtSourceReq(SRpcMsg *pReq) {
#ifndef TD_ENTERPRISE
  mError("failed to process refresh ext source req since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  SRefreshExtSourceReq refreshReq = {0};
  if (tDeserializeSRefreshExtSourceReq(pReq->pCont, pReq->contLen, &refreshReq) != 0) {
    tFreeSRefreshExtSourceReq(&refreshReq);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }
  mInfo("start to refresh ext source:%s", refreshReq.source_name);
  int32_t code = mndProcessRefreshExtSourceReqImpl(&refreshReq, pReq);
  tFreeSRefreshExtSourceReq(&refreshReq);
  return code;
#endif
}

int32_t mndProcessGetExtSourceReq(SRpcMsg *pReq) {
#ifndef TD_ENTERPRISE
  mError("failed to process get ext source req since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  return mndProcessGetExtSourceReqImpl(pReq);
#endif
}

// ============================================================
// System table retrieve
// ============================================================

int32_t mndRetrieveExtSources(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
#ifndef TD_ENTERPRISE
  return 0;  // community: empty result set
#else
  return mndRetrieveExtSourcesImpl(pReq, pShow, pBlock, rows);
#endif
}

void mndCancelGetNextExtSource(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_EXT_SOURCE);
}
