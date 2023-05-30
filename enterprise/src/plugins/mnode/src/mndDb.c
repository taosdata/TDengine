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
#include "mndPrivilege.h"
#include "mndTrans.h"
#include "mndVgroup.h"

static int32_t mndSetCompactDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, int64_t compactTs) {
  SDbObj dbObj = {0};
  memcpy(&dbObj, pDb, sizeof(SDbObj));
  dbObj.compactStartTime = compactTs;

  SSdbRaw *pCommitRaw = mndDbActionEncode(&dbObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    sdbFreeRaw(pCommitRaw);
    return -1;
  }

  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  return 0;
}

static int32_t mndSetCompactDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, int64_t compactTs,
                                          STimeWindow tw) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (mndVgroupInDb(pVgroup, pDb->uid)) {
      if (mndBuildCompactVgroupAction(pMnode, pTrans, pDb, pVgroup, compactTs, tw) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        return -1;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndCompactDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow tw) {
  int64_t compactTs = taosGetTimestampMs();
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "compact-db");
  if (pTrans == NULL) goto _OVER;

  mInfo("trans:%d, used to compact db:%s", pTrans->id, pDb->name);
  mndTransSetDbName(pTrans, pDb->name, NULL);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) goto _OVER;
  if (mndSetCompactDbCommitLogs(pMnode, pTrans, pDb, compactTs) != 0) goto _OVER;
  if (mndSetCompactDbRedoActions(pMnode, pTrans, pDb, compactTs, tw) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

int32_t mndProcessCompactDbReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SDbObj       *pDb = NULL;
  SCompactDbReq compactReq = {0};

  if (tDeserializeSCompactDbReq(pReq->pCont, pReq->contLen, &compactReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("db:%s, start to compact", compactReq.db);

  pDb = mndAcquireDb(pMnode, compactReq.db);
  if (pDb == NULL) {
    goto _OVER;
  }

  if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_COMPACT_DB, pDb) != 0) {
    goto _OVER;
  }

  code = mndCompactDb(pMnode, pReq, pDb, compactReq.timeRange);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to process compact db req since %s", compactReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  return code;
}

int32_t mndSetCreateDbRedoActionsImpl(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups){
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
      if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid) != 0) {
        return -1;
      }
    }
  }

  return 0;
}

int32_t mndSetCreateDbUndoActionsImpl(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups){
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
      if (mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid, false) != 0) {
        return -1;
      }
    }
  }

  return 0;
}