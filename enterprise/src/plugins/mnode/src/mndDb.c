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

static int32_t mndCompactDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb) {
  int64_t compactTs = taosGetTimestampMs();
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "compact-db");
  if (pTrans == NULL) goto _OVER;

  mInfo("trans:%d, used to compact db:%s", pTrans->id, pDb->name);
  mndTransSetDbName(pTrans, pDb->name, NULL);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) goto _OVER;
  if (mndSetCompactDbCommitLogs(pMnode, pTrans, pDb, compactTs) != 0) goto _OVER;
  if (mndSetCompactDbRedoActions(pMnode, pTrans, pDb, compactTs) != 0) goto _OVER;
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

  code = mndCompactDb(pMnode, pReq, pDb);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to process compact db req since %s", compactReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  return code;
}
