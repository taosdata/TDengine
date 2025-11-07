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

#include "mndDumpMeta.h"
#include "mndDb.h"

static int32_t mndDumpMeta(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow tw, SArray *vgroupIds, bool metaOnly,
                     bool force, ETsdbOpType type, ETriggerType triggerType) {
  int32_t       code = 0;
  SCompactDbRsp compactRsp = {0};

  bool  isExist = false;
  void *pIter = NULL;
  while (1) {
    SCompactObj *pCompact = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT, pIter, (void **)&pCompact);
    if (pIter == NULL) break;

    if (strcmp(pCompact->dbname, pDb->name) == 0) {
      isExist = true;
    }
    sdbRelease(pMnode->pSdb, pCompact);
  }
  if (isExist) {
    mInfo("compact db:%s already exist", pDb->name);

    if (pReq) {
      int32_t rspLen = 0;
      void   *pRsp = NULL;
      compactRsp.compactId = 0;
      compactRsp.bAccepted = false;
      TAOS_CHECK_RETURN(mndBuildCompactDbRsp(&compactRsp, &rspLen, &pRsp, true));

      pReq->info.rsp = pRsp;
      pReq->info.rspLen = rspLen;
    }

    return TSDB_CODE_MND_COMPACT_ALREADY_EXIST;
  }

  int64_t compactTs = taosGetTimestampMs();
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "compact-db");
  if (pTrans == NULL) goto _OVER;

  mInfo("trans:%d, used to compact db:%s", pTrans->id, pDb->name);
  mndTransSetDbName(pTrans, pDb->name, NULL);
  TAOS_CHECK_GOTO(mndTrancCheckConflict(pMnode, pTrans), NULL, _OVER);

  TAOS_CHECK_GOTO(mndSetCompactDbCommitLogs(pMnode, pTrans, pDb, compactTs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCompactDbRedoActions(pMnode, pTrans, pDb, compactTs, tw, vgroupIds, metaOnly, force, type,
                                             triggerType, &compactRsp),
                  NULL, _OVER);

  if (pReq) {
    int32_t rspLen = 0;
    void   *pRsp = NULL;
    compactRsp.bAccepted = true;
    TAOS_CHECK_GOTO(mndBuildCompactDbRsp(&compactRsp, &rspLen, &pRsp, false), NULL, _OVER);
    mndTransSetRpcRsp(pTrans, pRsp, rspLen);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

int32_t mndProcessDumpMetaReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SDbObj       *pDb = NULL;
  SDumpMetaReq request = {0};

  if (tDeserializeSDumpMetaReq(pReq->pCont, pReq->contLen, &request) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("db:%s, start to compact", request.db);

  pDb = mndAcquireDb(pMnode, request.db);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_COMPACT_DB, pDb), NULL, _OVER);

  if (pDb->cfg.isMount) {
    code = TSDB_CODE_MND_MOUNT_OBJ_NOT_SUPPORT;
    goto _OVER;
  }

  code = mndCompactDb(pMnode, pReq, pDb, compactReq.timeRange, compactReq.vgroupIds, compactReq.metaOnly,
                      compactReq.force, TSDB_OPTR_NORMAL, TSDB_TRIGGER_MANUAL);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  SName name = {0};
  (void)tNameFromString(&name, compactReq.db, T_NAME_ACCT | T_NAME_DB);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to process compact db req since %s", compactReq.db, tstrerror(code));
  }

  mndReleaseDb(pMnode, pDb);
  tFreeSCompactDbReq(&compactReq);
  TAOS_RETURN(code);

}