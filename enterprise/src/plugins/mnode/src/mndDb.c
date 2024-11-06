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

#include "mndDnode.h"
#include "mndDb.h"
#include "audit.h"
#include "mndCompact.h"
#include "mndCompactDetail.h"
#include "mndDef.h"
#include "mndPrivilege.h"
#include "mndTrans.h"
#include "mndVgroup.h"

int32_t mndCheckDbDnodeList(SMnode *pMnode, char *db, char *dnodeListStr, SArray *dnodeList) {
  if (dnodeListStr[0] == 0) return 0;

  mInfo("db:%s, dnode list is %s", db, dnodeListStr);

  int32_t len = strlen(dnodeListStr);
  for (int32_t i = 0; i < len; ++i) {
    if ((dnodeListStr[i] < '0' || dnodeListStr[i] > '9') && dnodeListStr[i] != ',') {
      terrno = TSDB_CODE_MND_INVALID_DNODE_LIST_FMT;
      return terrno;
    }
  }

  char *pos = dnodeListStr;
  while (pos != NULL) {
    if (pos[0] < '0' || pos[0] > '9') {
      terrno = TSDB_CODE_MND_INVALID_DNODE_LIST_FMT;
      return terrno;
    }

    int32_t    dnodeId = taosStr2Int32(pos, NULL, 10);
    SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
    if (pDnode != NULL) {
      mndReleaseDnode(pMnode, pDnode);
      if (taosArrayPush(dnodeList, &dnodeId) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return terrno;
      }
    } else {
      mError("db:%s, invalid dnode:%d from pos:%s", db, dnodeId, pos);
      terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
      return terrno;
    }

    pos = strstr(pos, ",");
    if (pos != NULL) {
      pos++;
    }
  }

  int32_t dnodeSize = (int32_t)taosArrayGetSize(dnodeList);
  for (int32_t i = 0; i < dnodeSize; ++i) {
    for (int32_t j = i + 1; j < dnodeSize; ++j) {
      if (((int32_t *)TARRAY_DATA(dnodeList))[i] == ((int32_t *)TARRAY_DATA(dnodeList))[j]) {
        terrno = TSDB_CODE_MND_DNODE_LIST_REPEAT;
        return terrno;
      }
    }
  }

  return 0;
}

static int32_t mndSetCompactDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, int64_t compactTs) {
  int32_t code = 0;
  SDbObj  dbObj = {0};
  memcpy(&dbObj, pDb, sizeof(SDbObj));
  dbObj.compactStartTime = compactTs;

  SSdbRaw *pCommitRaw = mndDbActionEncode(&dbObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    sdbFreeRaw(pCommitRaw);
    TAOS_RETURN(code);
  }

  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  TAOS_RETURN(code);
}

static int32_t mndSetCompactDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, int64_t compactTs,
                                          STimeWindow tw, SCompactDbRsp *pCompactRsp) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  SCompactObj compact;
  if ((code = mndAddCompactToTran(pMnode, pTrans, &compact, pDb, pCompactRsp)) != 0) {
    TAOS_RETURN(code);
  }

  int32_t j = 0;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      if ((code = mndBuildCompactVgroupAction(pMnode, pTrans, pDb, pVgroup, compactTs, tw)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(code);
      }

      for (int32_t i = 0; i < pVgroup->replica; i++) {
        SVnodeGid *gid = &pVgroup->vnodeGid[i];
        if ((code = mndAddCompactDetailToTran(pMnode, pTrans, &compact, pVgroup, gid, j)) != 0) {
          sdbCancelFetch(pSdb, pIter);
          sdbRelease(pSdb, pVgroup);
          TAOS_RETURN(code);
        }
        j++;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  // tFreeCompactObj(&compact);

  TAOS_RETURN(code);
}

static int32_t mndBuildCompactDbRsp(SCompactDbRsp *pCompactRsp, int32_t *pRspLen, void **ppRsp, bool useRpcMalloc) {
  int32_t code = 0;
  int32_t rspLen = tSerializeSCompactDbRsp(NULL, 0, pCompactRsp);
  void   *pRsp = NULL;
  if (useRpcMalloc) {
    pRsp = rpcMallocCont(rspLen);
  } else {
    pRsp = taosMemoryMalloc(rspLen);
  }

  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_RETURN(code);
  }

  (void)tSerializeSCompactDbRsp(pRsp, rspLen, pCompactRsp);
  *pRspLen = rspLen;
  *ppRsp = pRsp;
  TAOS_RETURN(code);
}

static int32_t mndCompactDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow tw) {
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

    int32_t rspLen = 0;
    void   *pRsp = NULL;
    compactRsp.compactId = 0;
    compactRsp.bAccepted = false;
    TAOS_CHECK_RETURN(mndBuildCompactDbRsp(&compactRsp, &rspLen, &pRsp, true));

    pReq->info.rsp = pRsp;
    pReq->info.rspLen = rspLen;

    return TSDB_CODE_MND_COMPACT_ALREADY_EXIST;
  }

  int64_t compactTs = taosGetTimestampMs();
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "compact-db");
  if (pTrans == NULL) goto _OVER;

  mInfo("trans:%d, used to compact db:%s", pTrans->id, pDb->name);
  mndTransSetDbName(pTrans, pDb->name, NULL);
  TAOS_CHECK_GOTO(mndTrancCheckConflict(pMnode, pTrans), NULL, _OVER);

  TAOS_CHECK_GOTO(mndSetCompactDbCommitLogs(pMnode, pTrans, pDb, compactTs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCompactDbRedoActions(pMnode, pTrans, pDb, compactTs, tw, &compactRsp), NULL, _OVER);

  int32_t rspLen = 0;
  void   *pRsp = NULL;
  compactRsp.bAccepted = true;
  TAOS_CHECK_GOTO(mndBuildCompactDbRsp(&compactRsp, &rspLen, &pRsp, false), NULL, _OVER);
  mndTransSetRpcRsp(pTrans, pRsp, rspLen);

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

int32_t mndProcessCompactDbReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SDbObj       *pDb = NULL;
  SCompactDbReq compactReq = {0};

  if (tDeserializeSCompactDbReq(pReq->pCont, pReq->contLen, &compactReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("db:%s, start to compact", compactReq.db);

  pDb = mndAcquireDb(pMnode, compactReq.db);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_COMPACT_DB, pDb), NULL, _OVER);

  code = mndCompactDb(pMnode, pReq, pDb, compactReq.timeRange);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  SName name = {0};
  (void)tNameFromString(&name, compactReq.db, T_NAME_ACCT | T_NAME_DB);

  auditRecord(pReq, pMnode->clusterId, "compactDB", name.dbname, "", compactReq.sql, compactReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to process compact db req since %s", compactReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  tFreeSCompactDbReq(&compactReq);
  TAOS_RETURN(code);
}

int32_t mndSetCreateDbRedoActionsImpl(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  int32_t code = 0;
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
      if ((code = mndAddCreateVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid)) != 0) {
        TAOS_RETURN(code);
      }
    }
  }

  TAOS_RETURN(code);
}

int32_t mndSetCreateDbUndoActionsImpl(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  int32_t code = 0;
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
      if ((code = mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid, false)) != 0) {
        TAOS_RETURN(code);
      }
    }
  }

  TAOS_RETURN(code);
}
#if 0
void mndSetCfgFromCreateReqImpl(SDbCfg *pCfg, SCreateDbReq *pCreate){
  *pCfg = (SDbCfg){
      .numOfVgroups = pCreate->numOfVgroups,
      .numOfStables = pCreate->numOfStables,
      .buffer = pCreate->buffer,
      .pageSize = pCreate->pageSize,
      .pages = pCreate->pages,
      .cacheLastSize = pCreate->cacheLastSize,
      .daysPerFile = pCreate->daysPerFile,
      .daysToKeep0 = pCreate->daysToKeep0,
      .daysToKeep1 = pCreate->daysToKeep1,
      .daysToKeep2 = pCreate->daysToKeep2,
      .minRows = pCreate->minRows,
      .maxRows = pCreate->maxRows,
      .walFsyncPeriod = pCreate->walFsyncPeriod,
      .walLevel = pCreate->walLevel,
      .precision = pCreate->precision,
      .compression = pCreate->compression,
      .replications = pCreate->replications,
      .strict = pCreate->strict,
      .cacheLast = pCreate->cacheLast,
      .hashMethod = 1,
      .schemaless = pCreate->schemaless,
      .walRetentionPeriod = pCreate->walRetentionPeriod,
      .walRetentionSize = pCreate->walRetentionSize,
      .walRollPeriod = pCreate->walRollPeriod,
      .walSegmentSize = pCreate->walSegmentSize,
      .sstTrigger = pCreate->sstTrigger,
      .hashPrefix = pCreate->hashPrefix,
      .hashSuffix = pCreate->hashSuffix,
      .tsdbPageSize = pCreate->tsdbPageSize,
  };
}

int32_t mndSetDbCfgFromAlterDbReqImpl(SDbObj *pDb, SAlterDbReq *pAlter) {
  terrno = TSDB_CODE_MND_DB_OPTION_UNCHANGED;

  if (pAlter->buffer > 0 && pAlter->buffer != pDb->cfg.buffer) {
    pDb->cfg.buffer = pAlter->buffer;
    terrno = 0;
  }

  if (pAlter->pages > 0 && pAlter->pages != pDb->cfg.pages) {
    pDb->cfg.pages = pAlter->pages;
    terrno = 0;
  }

  if (pAlter->pageSize > 0 && pAlter->pageSize != pDb->cfg.pageSize) {
    pDb->cfg.pageSize = pAlter->pageSize;
    terrno = 0;
  }

  if (pAlter->daysPerFile > 0 && pAlter->daysPerFile != pDb->cfg.daysPerFile) {
    pDb->cfg.daysPerFile = pAlter->daysPerFile;
    terrno = 0;
  }

  if (pAlter->daysToKeep0 > 0 && pAlter->daysToKeep0 != pDb->cfg.daysToKeep0) {
    pDb->cfg.daysToKeep0 = pAlter->daysToKeep0;
    terrno = 0;
  }

  if (pAlter->daysToKeep1 > 0 && pAlter->daysToKeep1 != pDb->cfg.daysToKeep1) {
    pDb->cfg.daysToKeep1 = pAlter->daysToKeep1;
    terrno = 0;
  }

  if (pAlter->daysToKeep2 > 0 && pAlter->daysToKeep2 != pDb->cfg.daysToKeep2) {
    pDb->cfg.daysToKeep2 = pAlter->daysToKeep2;
    terrno = 0;
  }

  if (pAlter->walFsyncPeriod >= 0 && pAlter->walFsyncPeriod != pDb->cfg.walFsyncPeriod) {
    pDb->cfg.walFsyncPeriod = pAlter->walFsyncPeriod;
    terrno = 0;
  }

  if (pAlter->walLevel >= 0 && pAlter->walLevel != pDb->cfg.walLevel) {
    pDb->cfg.walLevel = pAlter->walLevel;
    terrno = 0;
  }

  if (pAlter->strict >= 0 && pAlter->strict != pDb->cfg.strict) {
#if 1
    terrno = TSDB_CODE_OPS_NOT_SUPPORT;
#else
    pDb->cfg.strict = pAlter->strict;
    terrno = 0;
#endif
  }

  if (pAlter->cacheLast >= 0 && pAlter->cacheLast != pDb->cfg.cacheLast) {
    pDb->cfg.cacheLast = pAlter->cacheLast;
    terrno = 0;
  }

  if (pAlter->cacheLastSize > 0 && pAlter->cacheLastSize != pDb->cfg.cacheLastSize) {
    pDb->cfg.cacheLastSize = pAlter->cacheLastSize;
    terrno = 0;
  }

  if (pAlter->replications > 0 && pAlter->replications != pDb->cfg.replications) {
    pDb->cfg.replications = pAlter->replications;
    pDb->vgVersion++;
    terrno = 0;
  }

  if (pAlter->sstTrigger > 0 && pAlter->sstTrigger != pDb->cfg.sstTrigger) {
    pDb->cfg.sstTrigger = pAlter->sstTrigger;
    pDb->vgVersion++;
    terrno = 0;
  }

  if (pAlter->minRows > 0 && pAlter->minRows != pDb->cfg.minRows) {
    pDb->cfg.minRows = pAlter->minRows;
    pDb->vgVersion++;
    terrno = 0;
  }

  if (pAlter->walRetentionPeriod > TSDB_DB_MIN_WAL_RETENTION_PERIOD &&
      pAlter->walRetentionPeriod != pDb->cfg.walRetentionPeriod) {
    pDb->cfg.walRetentionPeriod = pAlter->walRetentionPeriod;
    pDb->vgVersion++;
    terrno = 0;
  }

  if (pAlter->walRetentionSize > TSDB_DB_MIN_WAL_RETENTION_SIZE &&
      pAlter->walRetentionSize != pDb->cfg.walRetentionSize) {
    pDb->cfg.walRetentionSize = pAlter->walRetentionSize;
    pDb->vgVersion++;
    terrno = 0;
  }

  return terrno;
}
#endif