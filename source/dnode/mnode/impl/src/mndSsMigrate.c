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
#include "audit.h"
#include "mndSsMigrate.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "tmisce.h"
#include "tmsgcb.h"

#define MND_SSMIGRATE_VER_NUMBER       2

static int32_t mndProcessSsMigrateDbTimer(SRpcMsg *pReq);
static int32_t mndProcessUpdateSsMigrateProgressTimer(SRpcMsg *pReq);
static int32_t mndProcessSsMigrateListFileSetsRsp(SRpcMsg *pMsg);
static int32_t mndProcessSsMigrateFileSetRsp(SRpcMsg *pMsg);
static int32_t mndProcessQuerySsMigrateProgressRsp(SRpcMsg *pReq);
static int32_t mndProcessFollowerSsMigrateRsp(SRpcMsg *pMsg);

int32_t mndInitSsMigrate(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SSMIGRATE, mndRetrieveSsMigrate);
  mndSetMsgHandle(pMnode, TDMT_MND_SSMIGRATE_DB_TIMER, mndProcessSsMigrateDbTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_SSMIGRATE, mndProcessKillSsMigrateReq);
  mndSetMsgHandle(pMnode, TDMT_VND_KILL_SSMIGRATE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_SSMIGRATE_PROGRESS_TIMER, mndProcessUpdateSsMigrateProgressTimer);
  mndSetMsgHandle(pMnode, TDMT_VND_LIST_SSMIGRATE_FILESETS_RSP, mndProcessSsMigrateListFileSetsRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_SSMIGRATE_FILESET_RSP, mndProcessSsMigrateFileSetRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_SSMIGRATE_PROGRESS_RSP, mndProcessQuerySsMigrateProgressRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_FOLLOWER_SSMIGRATE_RSP, mndProcessFollowerSsMigrateRsp);

  SSdbTable table = {
      .sdbType = SDB_SSMIGRATE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndSsMigrateActionEncode,
      .decodeFp = (SdbDecodeFp)mndSsMigrateActionDecode,
      .insertFp = (SdbInsertFp)mndSsMigrateActionInsert,
      .updateFp = (SdbUpdateFp)mndSsMigrateActionUpdate,
      .deleteFp = (SdbDeleteFp)mndSsMigrateActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupSsMigrate(SMnode *pMnode) { mDebug("mnd ssmigrate cleanup"); }

void tFreeSsMigrateObj(SSsMigrateObj *pSsMigrate) {
  taosArrayDestroy(pSsMigrate->vgroups);
  taosArrayDestroy(pSsMigrate->fileSets);
}

int32_t tSerializeSSsMigrateObj(void *buf, int32_t bufLen, const SSsMigrateObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->id));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->dbUid));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->dbname));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->startTime));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->stateUpdateTime));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->vgIdx));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->vgState));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->fsetIdx));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->currFset.nodeId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->currFset.vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->currFset.fid));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->currFset.state));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->currFset.startTime));

  int32_t numVg = pObj->vgroups ? taosArrayGetSize(pObj->vgroups) : 0;
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numVg));
  for (int32_t i = 0; i < numVg; ++i) {
    int32_t *vgId = (int32_t *)taosArrayGet(pObj->vgroups, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, *vgId));
  }

  int32_t numFset = pObj->fileSets ? taosArrayGetSize(pObj->fileSets) : 0;
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numFset));
  for (int32_t i = 0; i < numFset; ++i) {
    int32_t *fsetId = (int32_t *)taosArrayGet(pObj->fileSets, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, *fsetId));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSsMigrateObj(void *buf, int32_t bufLen, SSsMigrateObj *pObj) {
  int32_t  code = TSDB_CODE_SUCCESS, lino;
  SArray *vgroups = NULL, *fileSets = NULL;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->id));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->dbUid));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->dbname));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->startTime));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->stateUpdateTime));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->vgIdx));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->vgState));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->fsetIdx));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->currFset.nodeId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->currFset.vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->currFset.fid));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->currFset.state));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->currFset.startTime));

  int32_t numVg = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numVg));
  vgroups = pObj->vgroups;
  if (vgroups) {
    taosArrayClear(vgroups);
  } else if ((vgroups = taosArrayInit(numVg, sizeof(int32_t))) == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; i < numVg; ++i) {
    int32_t vgId = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &vgId));
    if(taosArrayPush(vgroups, &vgId) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  int32_t numFset = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numFset));
  fileSets = pObj->fileSets;
  if (fileSets) {
    taosArrayClear(fileSets);
  } else if ((fileSets = taosArrayInit(numFset, sizeof(int32_t))) == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; i < numFset; ++i) {
    int32_t fsetId = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &fsetId));
    if(taosArrayPush(fileSets, &fsetId) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  if (code == TSDB_CODE_SUCCESS) {
    pObj->vgroups = vgroups;
  } else if (pObj->vgroups) {
    taosArrayClear(pObj->vgroups);
  } else {
    taosArrayDestroy(vgroups);
  }
  if (code == TSDB_CODE_SUCCESS) {
    pObj->fileSets = fileSets;
  } else if (pObj->fileSets) {
    taosArrayClear(pObj->fileSets);
  } else {
    taosArrayDestroy(fileSets);
  }
  return code;
}

SSdbRaw *mndSsMigrateActionEncode(SSsMigrateObj *pSsMigrate) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSSsMigrateObj(NULL, 0, pSsMigrate);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_SSMIGRATE, MND_SSMIGRATE_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSSsMigrateObj(buf, tlen, pSsMigrate);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, OVER);
  SDB_SET_DATALEN(pRaw, dataPos, OVER);

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("ssmigrate:%" PRId32 ", failed to encode to raw:%p since %s", pSsMigrate->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("ssmigrate:%" PRId32 ", encode to raw:%p, row:%p", pSsMigrate->id, pRaw, pSsMigrate);
  return pRaw;
}

SSdbRow *mndSsMigrateActionDecode(SSdbRaw *pRaw) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSdbRow     *pRow = NULL;
  SSsMigrateObj *pSsMigrate = NULL;
  void        *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_SSMIGRATE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("ssmigrate read invalid ver, data ver: %d, curr ver: %d", sver, MND_SSMIGRATE_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SSsMigrateObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pSsMigrate = sdbGetRowObj(pRow);
  if (pSsMigrate == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, OVER);

  if ((terrno = tDeserializeSSsMigrateObj(buf, tlen, pSsMigrate)) < 0) {
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("ssmigrate:%" PRId32 ", failed to decode from raw:%p since %s", pSsMigrate->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("ssmigrate:%" PRId32 ", decode from raw:%p, row:%p", pSsMigrate->id, pRaw, pSsMigrate);
  return pRow;
}

int32_t mndSsMigrateActionInsert(SSdb *pSdb, SSsMigrateObj *pSsMigrate) {
  mTrace("ssmigrate:%" PRId32 ", perform insert action", pSsMigrate->id);
  return 0;
}

int32_t mndSsMigrateActionDelete(SSdb *pSdb, SSsMigrateObj *pSsMigrate) {
  mTrace("ssmigrate:%" PRId32 ", perform delete action", pSsMigrate->id);
  tFreeSsMigrateObj(pSsMigrate);
  return 0;
}

int32_t mndSsMigrateActionUpdate(SSdb *pSdb, SSsMigrateObj *pOldSsMigrate, SSsMigrateObj *pNewSsMigrate) {
  mTrace("ssmigrate:%" PRId32 ", perform update action, old row:%p new row:%p", pOldSsMigrate->id, pOldSsMigrate,
         pNewSsMigrate);

  return 0;
}

SSsMigrateObj *mndAcquireSsMigrate(SMnode *pMnode, int64_t ssMigrateId) {
  SSdb        *pSdb = pMnode->pSdb;
  SSsMigrateObj *pSsMigrate = sdbAcquire(pSdb, SDB_SSMIGRATE, &ssMigrateId);
  if (pSsMigrate == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pSsMigrate;
}

void mndReleaseSsMigrate(SMnode *pMnode, SSsMigrateObj *pSsMigrate) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pSsMigrate);
  pSsMigrate = NULL;
}

int32_t mndSsMigrateGetDbName(SMnode *pMnode, int32_t ssMigrateId, char *dbname, int32_t len) {
  int32_t      code = 0;
  SSsMigrateObj *pSsMigrate = mndAcquireSsMigrate(pMnode, ssMigrateId);
  if (pSsMigrate == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  tstrncpy(dbname, pSsMigrate->dbname, len);
  mndReleaseSsMigrate(pMnode, pSsMigrate);
  TAOS_RETURN(code);
}

// ssmigrate db
int32_t mndAddSsMigrateToTran(SMnode *pMnode, STrans *pTrans, SSsMigrateObj *pSsMigrate, SDbObj *pDb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  pSsMigrate->dbUid = pDb->uid;
  pSsMigrate->id = tGenIdPI32();
  tstrncpy(pSsMigrate->dbname, pDb->name, sizeof(pSsMigrate->dbname));
  pSsMigrate->stateUpdateTime = taosGetTimestampSec();
  pSsMigrate->vgIdx = 0;
  pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
  pSsMigrate->fsetIdx = 0;

  pSsMigrate->vgroups = taosArrayInit(8, sizeof(int32_t));
  if (pSsMigrate->vgroups == NULL) {
    TAOS_RETURN(terrno);
  }

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->mountVgId || pVgroup->dbUid != pDb->uid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t vgId = pVgroup->vgId;
    sdbRelease(pSdb, pVgroup);
    if (taosArrayPush(pSsMigrate->vgroups, &vgId) == NULL) {
      code = terrno;
      taosArrayDestroy(pSsMigrate->vgroups);
      pSsMigrate->vgroups = NULL;
      TAOS_RETURN(code);
    }
  }

  SSdbRaw *pRaw = mndSsMigrateActionEncode(pSsMigrate);
  code = terrno;
  taosArrayDestroy(pSsMigrate->vgroups);
  pSsMigrate->vgroups = NULL;
  if (pRaw == NULL) {
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    sdbFreeRaw(pRaw);
    TAOS_RETURN(code);
  }

  if ((code = sdbSetRawStatus(pRaw, SDB_STATUS_READY)) != 0) {
    sdbFreeRaw(pRaw);
    TAOS_RETURN(code);
  }

  mInfo("trans:%d, ssmigrate:%d, db:%s, has been added", pTrans->id, pSsMigrate->id, pSsMigrate->dbname);
  return 0;
}

// retrieve ssmigrate
int32_t mndRetrieveSsMigrate(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode      *pMnode = pReq->info.node;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      numOfRows = 0;
  SSsMigrateObj *pSsMigrate = NULL;
  char        *sep = NULL;
  SDbObj      *pDb = NULL;
  int32_t      code = 0;
  int32_t      lino = 0;

  if (strlen(pShow->db) > 0) {
    sep = strchr(pShow->db, '.');
    if (sep &&
        ((0 == strcmp(sep + 1, TSDB_INFORMATION_SCHEMA_DB) || (0 == strcmp(sep + 1, TSDB_PERFORMANCE_SCHEMA_DB))))) {
      sep++;
    } else {
      pDb = mndAcquireDb(pMnode, pShow->db);
      if (pDb == NULL) return terrno;
    }
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_SSMIGRATE, pShow->pIter, (void **)&pSsMigrate);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    // ssmigrate_id
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pSsMigrate->id, false), pSsMigrate, &lino, _OVER);

    // db_name
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pDb != NULL || !IS_SYS_DBNAME(pSsMigrate->dbname)) {
      SName name = {0};
      TAOS_CHECK_GOTO(tNameFromString(&name, pSsMigrate->dbname, T_NAME_ACCT | T_NAME_DB), &lino, _OVER);
      (void)tNameGetDbName(&name, varDataVal(tmpBuf));
    } else {
      tstrncpy(varDataVal(tmpBuf), pSsMigrate->dbname, TSDB_SHOW_SQL_LEN);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pSsMigrate, &lino, _OVER);

    // start_time
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pSsMigrate->startTime, false), pSsMigrate, &lino, _OVER);

    // number_vgroup
    int32_t numVg = taosArrayGetSize(pSsMigrate->vgroups);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&numVg, false), pSsMigrate, &lino, _OVER);

    // migrated_vgroup
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pSsMigrate->vgIdx, false), pSsMigrate, &lino, _OVER);

    if (pSsMigrate->vgIdx < numVg) {
      // vgroup_id
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      int32_t vgId = *(int32_t*)taosArrayGet(pSsMigrate->vgroups, pSsMigrate->vgIdx);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&vgId, false), pSsMigrate, &lino, _OVER);
      
      // number_fileset
      int32_t numFset = taosArrayGetSize(pSsMigrate->fileSets);
      if (pSsMigrate->vgState < SSMIGRATE_VGSTATE_FSET_LIST_RECEIVED) {
        numFset = 0;
      }
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&numFset, false), pSsMigrate, &lino, _OVER);

      // migrated_fileset
      int32_t fsetIdx = pSsMigrate->fsetIdx;
      if (pSsMigrate->vgState < SSMIGRATE_VGSTATE_FSET_LIST_RECEIVED) {
        fsetIdx = 0;
      }
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&fsetIdx, false), pSsMigrate, &lino, _OVER);

      // fileset_id
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (fsetIdx < numFset) {
        int32_t fid = *(int32_t*)taosArrayGet(pSsMigrate->fileSets, fsetIdx);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&fid, false), pSsMigrate, &lino, _OVER);
      } else {
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, NULL, true), pSsMigrate, &lino, _OVER);
      }
    } else {
      // vgroup_id
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, NULL, true), pSsMigrate, &lino, _OVER);

      // number_fileset
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, NULL, true), pSsMigrate, &lino, _OVER);

      // migrated_fileset
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, NULL, true), pSsMigrate, &lino, _OVER);

      // fileset_id
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, NULL, true), pSsMigrate, &lino, _OVER);
    }

    numOfRows++;
    sdbRelease(pSdb, pSsMigrate);
  }

_OVER:
  if (code != 0) mError("failed to retrieve at line:%d, since %s", lino, tstrerror(code));
  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
  return numOfRows;
}


int32_t mndSsMigrateDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb);

static int32_t mndProcessSsMigrateDbTimer(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  void *pIter = NULL;

  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) {
      break;
    }
    int32_t code = mndSsMigrateDb(pMnode, NULL, pDb);
    sdbRelease(pMnode->pSdb, pDb);
    if (code == TSDB_CODE_SUCCESS) {
      mInfo("ssmigrate db:%s, has been triggered by timer", pDb->name);
    } else {
      mError("failed to trigger ssmigrate db:%s, code:%d, %s", pDb->name, code, tstrerror(code));
    }
  }

  TAOS_RETURN(0);
}


static int32_t mndDropSsMigrate(SMnode *pMnode, SSsMigrateObj *pSsMigrate) {
  int32_t code = 0, lino = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "drop-ssmigrate");
  if (pTrans == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  mndTransSetDbName(pTrans, pSsMigrate->dbname, NULL);

  SSdbRaw *pRaw = mndSsMigrateActionEncode(pSsMigrate);
  if (pRaw == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }
  if ((code = sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED)) != 0) {
    sdbFreeRaw(pRaw);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pRaw), &lino, _exit);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), &lino, _exit);

_exit:
  mndTransDrop(pTrans);
  if (code == TSDB_CODE_SUCCESS) {
    mInfo("ssmigrate:%d was dropped successfully", pSsMigrate->id);
  } else {
    mError("ssmigrate:%d, failed to drop at lino %d since %s", pSsMigrate->id, lino, tstrerror(code));
  }
  return code;
}


static void mndUpdateSsMigrate(SMnode *pMnode, SSsMigrateObj *pSsMigrate) {
  int32_t code = 0, lino = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "update-ssmigrate");
  if (pTrans == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }
  mndTransSetDbName(pTrans, pSsMigrate->dbname, NULL);

  SSdbRaw *pRaw = mndSsMigrateActionEncode(pSsMigrate);
  if (pRaw == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }
  if ((code = sdbSetRawStatus(pRaw, SDB_STATUS_READY)) != 0) {
    sdbFreeRaw(pRaw);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pRaw), &lino, _exit);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), &lino, _exit);

_exit:
  mndTransDrop(pTrans);
  if (code == TSDB_CODE_SUCCESS) {
    mTrace("ssmigrate:%d was updated successfully", pSsMigrate->id);
  } else {
    mError("ssmigrate:%d, failed to update at lino %d since %s", pSsMigrate->id, lino, tstrerror(code));
  }
}


static int32_t mndKillSsMigrate(SMnode *pMnode, SRpcMsg *pReq, SSsMigrateObj *pSsMigrate) {
  int32_t vgId = *(int32_t*)taosArrayGet(pSsMigrate->vgroups, pSsMigrate->vgIdx);
  SVgObj *pVgroup = mndAcquireVgroup(pMnode, vgId);
  if (pVgroup == NULL) {
    return mndDropSsMigrate(pMnode, pSsMigrate);
  }

  SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
  mndReleaseVgroup(pMnode, pVgroup);

  SVnodeKillSsMigrateReq req = {.ssMigrateId = pSsMigrate->id};
  int32_t   reqLen = tSerializeSVnodeKillSsMigrateReq(NULL, 0, &req);
  int32_t   contLen = reqLen + sizeof(SMsgHead);
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    return mndDropSsMigrate(pMnode, pSsMigrate);
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  int32_t ret = 0;
  if ((ret = tSerializeSVnodeKillSsMigrateReq((char *)pHead + sizeof(SMsgHead), reqLen, &req)) < 0) {
    return mndDropSsMigrate(pMnode, pSsMigrate);
  }

  SRpcMsg rpcMsg = {.msgType = TDMT_VND_KILL_SSMIGRATE, .pCont = pHead, .contLen = contLen};
  int32_t code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("ssmigrate:%d, vgId:%d, failed to send kill ssmigrate request to vnode since 0x%x", req.ssMigrateId, vgId, code);
  } else {
    mInfo("ssmigrate:%d, vgId:%d, kill ssmigrate request was sent to vnode", req.ssMigrateId, vgId);
  }

  return mndDropSsMigrate(pMnode, pSsMigrate);
}

int32_t mndProcessKillSsMigrateReq(SRpcMsg *pReq) {
  int32_t         code = 0;
  int32_t         lino = 0;
  SKillSsMigrateReq killReq = {0};

  if ((code = tDeserializeSKillSsMigrateReq(pReq->pCont, pReq->contLen, &killReq)) != 0) {
    TAOS_RETURN(code);
  }

  mInfo("start to kill ssmigrate:%" PRId32, killReq.ssMigrateId);

  SMnode      *pMnode = pReq->info.node;
  SSsMigrateObj *pSsMigrate = mndAcquireSsMigrate(pMnode, killReq.ssMigrateId);
  if (pSsMigrate == NULL) {
    code = TSDB_CODE_MND_INVALID_SSMIGRATE_ID;
    tFreeSKillSsMigrateReq(&killReq);
    TAOS_RETURN(code);
  }

  //TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_SSMIGRATE_DB), &lino, _OVER);

  TAOS_CHECK_GOTO(mndKillSsMigrate(pMnode, pReq, pSsMigrate), &lino, _OVER);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to kill ssmigrate %" PRId32 " since %s", killReq.ssMigrateId, tstrerror(code));
  }

  tFreeSKillSsMigrateReq(&killReq);
  mndReleaseSsMigrate(pMnode, pSsMigrate);

  TAOS_RETURN(code);
}


static void mndSendSsMigrateListFileSetsReq(SMnode* pMnode, SSsMigrateObj* pSsMigrate) {
  int32_t vgId = *(int32_t*)taosArrayGet(pSsMigrate->vgroups, pSsMigrate->vgIdx);
  SVgObj *pVgroup = mndAcquireVgroup(pMnode, vgId);
  if (pVgroup == NULL) {
    mError("ssmigrate:%d, vgId:%d, vgroup does not exist in %s", pSsMigrate->id, vgId, __func__);
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
    pSsMigrate->vgIdx++;
    return;
  }

  SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
  mndReleaseVgroup(pMnode, pVgroup);

  SListSsMigrateFileSetsReq req = {.ssMigrateId = pSsMigrate->id};
  int32_t   reqLen = tSerializeSListSsMigrateFileSetsReq(NULL, 0, &req);
  int32_t   contLen = reqLen + sizeof(SMsgHead);
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    return;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  int32_t ret = 0;
  if ((ret = tSerializeSListSsMigrateFileSetsReq((char *)pHead + sizeof(SMsgHead), reqLen, &req)) < 0) {
    return;
  }

  SRpcMsg rpcMsg = {.msgType = TDMT_VND_LIST_SSMIGRATE_FILESETS, .pCont = pHead, .contLen = contLen};
  int32_t code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("ssmigrate:%d, vgId:%d, failed to send list filesets request to vnode since 0x%x", req.ssMigrateId, vgId, code);
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
    pSsMigrate->vgIdx++;
  } else {
    mInfo("ssmigrate:%d, vgId:%d, list filesets request was sent to vnode", req.ssMigrateId, vgId);
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_WAITING_FSET_LIST;
  }

  pSsMigrate->stateUpdateTime = taosGetTimestampSec();
  mndUpdateSsMigrate(pMnode, pSsMigrate);
}


static int32_t mndProcessSsMigrateListFileSetsRsp(SRpcMsg *pMsg) {
  int32_t code = 0, lino = 0;

  if (pMsg->code != 0) {
    mError("received wrong ssmigrate list filesets response, req code is %s", tstrerror(pMsg->code));
    TAOS_RETURN(pMsg->code);
  }

  SSsMigrateObj *pSsMigrate = NULL;
  SListSsMigrateFileSetsRsp rsp = {0};
  code = tDeserializeSListSsMigrateFileSetsRsp(pMsg->pCont, pMsg->contLen, &rsp);
  TAOS_CHECK_GOTO(code, &lino, _exit);

  SMnode *pMnode = pMsg->info.node;
  pSsMigrate = mndAcquireSsMigrate(pMnode, rsp.ssMigrateId);
  if (pSsMigrate == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_RETURN_VALUE_NULL, &lino, _exit);
  }

  int32_t vgId = *(int32_t*)taosArrayGet(pSsMigrate->vgroups, pSsMigrate->vgIdx);
  if (vgId != rsp.vgId) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }

  if (pSsMigrate->vgState != SSMIGRATE_VGSTATE_WAITING_FSET_LIST) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }

  // we need to use the new filesets to update the SSsMigrateObj,
  // swap is only to make the it is easier to free both of them.
  SArray* tmp = pSsMigrate->fileSets;
  pSsMigrate->fileSets = rsp.pFileSets;
  rsp.pFileSets = tmp;

  pSsMigrate->fsetIdx = 0;
  pSsMigrate->stateUpdateTime = taosGetTimestampSec();
  if (taosArrayGetSize(pSsMigrate->fileSets) == 0) {
    mInfo("ssmigrate:%d, vgId:%d, no filesets to migrate.", pSsMigrate->id, rsp.vgId);
    pSsMigrate->vgIdx++;
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
  } else {
    mInfo("ssmigrate:%d, vgId:%d, filesets received.", pSsMigrate->id, rsp.vgId);
    pSsMigrate->currFset.nodeId = 0;
    pSsMigrate->currFset.vgId = vgId;
    pSsMigrate->currFset.fid = *(int32_t*)taosArrayGet(pSsMigrate->fileSets, 0);
    pSsMigrate->currFset.state = SSMIGRATE_FILESET_STATE_IN_PROGRESS;
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_FSET_LIST_RECEIVED;
  }

  mndUpdateSsMigrate(pMnode, pSsMigrate);
  
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mError("%s:%d, error=%s", __func__, lino, tstrerror(code));
  }
  tFreeSListSsMigrateFileSetsRsp(&rsp);
  mndReleaseSsMigrate(pMnode, pSsMigrate);
  return code;
}


static void mndSendSsMigrateFileSetReq(SMnode* pMnode, SSsMigrateObj* pSsMigrate) {
  int32_t vgId = *(int32_t*)taosArrayGet(pSsMigrate->vgroups, pSsMigrate->vgIdx);
  SVgObj *pVgroup = mndAcquireVgroup(pMnode, vgId);
  if (pVgroup == NULL) {
    mError("ssmigrate:%d, vgId:%d, vgroup does not exist in %s", pSsMigrate->id, vgId, __func__);
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
    pSsMigrate->vgIdx++;
    return;
  }

  SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
  mndReleaseVgroup(pMnode, pVgroup);

  SSsMigrateFileSetReq req = { 0 };
  req.ssMigrateId = pSsMigrate->id;
  req.nodeId = 0;
  req.fid = pSsMigrate->currFset.fid;
  req.startTimeSec = taosGetTimestampSec();

  int32_t   reqLen = tSerializeSSsMigrateFileSetReq(NULL, 0, &req);
  int32_t   contLen = reqLen + sizeof(SMsgHead);
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    return;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  int32_t ret = 0;
  if ((ret = tSerializeSSsMigrateFileSetReq((char *)pHead + sizeof(SMsgHead), reqLen, &req)) < 0) {
    return;
  }

  SRpcMsg rpcMsg = {.msgType = TDMT_VND_SSMIGRATE_FILESET, .pCont = pHead, .contLen = contLen};
  int32_t code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("ssmigrate:%d, vgId:%d, fid:%d, failed to send migrate fileset request to vnode since 0x%x", req.ssMigrateId, vgId, req.fid, code);
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
    pSsMigrate->vgIdx++;
  } else {
    mInfo("ssmigrate:%d, vgId:%d, fid:%d, migrate fileset request was sent to vnode", req.ssMigrateId, vgId, req.fid);
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_FSET_STARTING;
    pSsMigrate->currFset.startTime = req.startTimeSec;
  }

  pSsMigrate->stateUpdateTime = taosGetTimestampSec();
  mndUpdateSsMigrate(pMnode, pSsMigrate);
}


static int32_t mndProcessSsMigrateFileSetRsp(SRpcMsg *pMsg) {
  int32_t code = 0, lino = 0;

  if (pMsg->code != 0) {
    mError("received wrong ssmigrate fileset response, error code is %s", tstrerror(pMsg->code));
    TAOS_RETURN(pMsg->code);
  }

  SSsMigrateObj *pSsMigrate = NULL;
  SSsMigrateFileSetRsp rsp = {0};
  code = tDeserializeSSsMigrateFileSetRsp(pMsg->pCont, pMsg->contLen, &rsp);
  TAOS_CHECK_GOTO(code, &lino, _exit);

  SMnode *pMnode = pMsg->info.node;
  pSsMigrate = mndAcquireSsMigrate(pMnode, rsp.ssMigrateId);
  if (pSsMigrate == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_RETURN_VALUE_NULL, &lino, _exit);
  }

  int32_t vgId = *(int32_t*)taosArrayGet(pSsMigrate->vgroups, pSsMigrate->vgIdx);
  if (vgId != rsp.vgId) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }
  if (rsp.fid != pSsMigrate->currFset.fid) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }
  if (pSsMigrate->vgState != SSMIGRATE_VGSTATE_FSET_STARTING) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }
  if (rsp.nodeId <= 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }

  mInfo("ssmigrate:%d, vgId:%d, fid:%d, leader node is %d", rsp.ssMigrateId, vgId, rsp.fid, rsp.nodeId);
  pSsMigrate->currFset.nodeId = rsp.nodeId;
  pSsMigrate->vgState = SSMIGRATE_VGSTATE_FSET_STARTED;
  pSsMigrate->stateUpdateTime = taosGetTimestampSec();
  mndUpdateSsMigrate(pMnode, pSsMigrate);
  
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mError("%s:%d, error=%s", __func__, lino, tstrerror(code));
  }
  mndReleaseSsMigrate(pMnode, pSsMigrate);
  return code;
}



static int32_t mndProcessFollowerSsMigrateRsp(SRpcMsg *pReq) {
  TAOS_RETURN(0);
}


static void mndSendFollowerSsMigrateReq(SMnode* pMnode, SSsMigrateObj *pSsMigrate) {
  SVgObj *pVgroup = mndAcquireVgroup(pMnode, pSsMigrate->currFset.vgId);
  if (pVgroup == NULL) {
    return;
  }

  SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
  mndReleaseVgroup(pMnode, pVgroup);

  SSsMigrateProgress req = {
    .ssMigrateId = pSsMigrate->id,
    .nodeId = pSsMigrate->currFset.nodeId,
    .vgId = pSsMigrate->currFset.vgId,
    .fid = pSsMigrate->currFset.fid,
    .state = pSsMigrate->currFset.state,
  };

  int32_t          reqLen = tSerializeSSsMigrateProgress(NULL, 0, &req);
  int32_t          contLen = reqLen + sizeof(SMsgHead);
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    return;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(req.vgId);
  TAOS_UNUSED(tSerializeSSsMigrateProgress((char *)pHead + sizeof(SMsgHead), reqLen, &req));
  SRpcMsg rpcMsg = {.msgType = TDMT_VND_FOLLOWER_SSMIGRATE, .pCont = pHead, .contLen = contLen};

  int32_t code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("vgId:%d, ssmigrate:%d, fid:%d, failed to send follower-ssmigrate request since 0x%x", req.ssMigrateId, req.vgId, req.fid, code);
  } else {
    mTrace("vgId:%d, ssmigrate:%d, fid:%d, follower-ssmigrate request sent", req.ssMigrateId, req.vgId, req.fid);
  }
}



static int32_t mndProcessQuerySsMigrateProgressRsp(SRpcMsg *pMsg) {
  int32_t code = 0, lino = 0;

  if (pMsg->code != 0) {
    mError("received wrong query ssmigrate progress response, error code is %s", tstrerror(pMsg->code));
    TAOS_RETURN(pMsg->code);
  }

  SSsMigrateObj *pSsMigrate = NULL;
  SSsMigrateProgress rsp = {0};
  code = tDeserializeSSsMigrateProgress(pMsg->pCont, pMsg->contLen, &rsp);
  TAOS_CHECK_GOTO(code, &lino, _exit);

  SMnode *pMnode = pMsg->info.node;
  pSsMigrate = mndAcquireSsMigrate(pMnode, rsp.ssMigrateId);
  if (pSsMigrate == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_RETURN_VALUE_NULL, &lino, _exit);
  }

  if (pSsMigrate->vgState != SSMIGRATE_VGSTATE_FSET_STARTED) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }
  if (rsp.nodeId != pSsMigrate->currFset.nodeId) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }
  if (rsp.vgId != pSsMigrate->currFset.vgId) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }
  if (rsp.fid != pSsMigrate->currFset.fid) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _exit);
  }

  if (rsp.state == pSsMigrate->currFset.state) {
    mTrace("ssmigrate:%d, vgId:%d, fid:%d, state is %d", rsp.ssMigrateId, rsp.vgId, rsp.fid, rsp.state);
  } else {
    mInfo("ssmigrate:%d, vgId:%d, fid:%d, state is %d", rsp.ssMigrateId, rsp.vgId, rsp.fid, rsp.state);
  }
  pSsMigrate->currFset.state = rsp.state;
  pSsMigrate->stateUpdateTime = taosGetTimestampSec();
  mndUpdateSsMigrate(pMnode, pSsMigrate);

  mndSendFollowerSsMigrateReq(pMnode, pSsMigrate);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mError("%s:%d, error=%s", __func__, lino, tstrerror(code));
  }
  mndReleaseSsMigrate(pMnode, pSsMigrate);
  return code;
}



// when query migration progress, we need to send the msg to dnode instead of vgroup,
// because migration may take a long time, and leader may change during the migration process,
// while only the initial leader vnode can handle the migration progress query.
void mndSendQuerySsMigrateProgressReq(SMnode *pMnode, SSsMigrateObj *pSsMigrate) {
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pSsMigrate->currFset.nodeId);
  if (pDnode == NULL) {
    return;
  }

  SEpSet epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  SSsMigrateProgress req = {
    .ssMigrateId = pSsMigrate->id,
    .nodeId = pSsMigrate->currFset.nodeId,
    .vgId = pSsMigrate->currFset.vgId,
    .fid = pSsMigrate->currFset.fid,
    .state = SSMIGRATE_FILESET_STATE_IN_PROGRESS,
  };

  int32_t          reqLen = tSerializeSSsMigrateProgress(NULL, 0, &req);
  int32_t          contLen = reqLen + sizeof(SMsgHead);
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    return;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(req.vgId);
  TAOS_UNUSED(tSerializeSSsMigrateProgress((char *)pHead + sizeof(SMsgHead), reqLen, &req));
  SRpcMsg rpcMsg = {.msgType = TDMT_VND_QUERY_SSMIGRATE_PROGRESS, .pCont = pHead, .contLen = contLen};

  int32_t code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("ssmigrate:%d, vgId:%d, fid:%d, failed to send ssmigrate-query-progress request since 0x%x", req.ssMigrateId, req.vgId, req.fid, code)
  } else {
    mTrace("ssmigrate:%d, vgId:%d, fid:%d, ssmigrate-query-progress request sent", req.ssMigrateId, req.vgId, req.fid);
  }
}



static void mndUpdateSsMigrateProgress(SMnode* pMnode, SSsMigrateObj* pSsMigrate) {
  int32_t numVg = taosArrayGetSize(pSsMigrate->vgroups);
  int32_t numFset = taosArrayGetSize(pSsMigrate->fileSets);

  if (pSsMigrate->vgIdx >= numVg) {
    mInfo("ssmigrate:%d, all vgroups has been processed", pSsMigrate->id);
    TAOS_UNUSED(mndDropSsMigrate(pMnode, pSsMigrate));
    return;
  }

  // vgroup state is init, we need to get the list of its file sets
  if (pSsMigrate->vgState == SSMIGRATE_VGSTATE_INIT) {
    mndSendSsMigrateListFileSetsReq(pMnode, pSsMigrate);
    return;
  }

  int32_t vgId = *(int32_t*)taosArrayGet(pSsMigrate->vgroups, pSsMigrate->vgIdx);

  if (pSsMigrate->vgState == SSMIGRATE_VGSTATE_WAITING_FSET_LIST) {
    if (taosGetTimestampSec() - pSsMigrate->stateUpdateTime > 30) {
      mWarn("ssmigrate:%d, vgId:%d, haven't receive file set list in 30 seconds, skip", pSsMigrate->id, vgId);
      pSsMigrate->vgIdx++;
      pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
      pSsMigrate->stateUpdateTime = taosGetTimestampSec();
      mndUpdateSsMigrate(pMnode, pSsMigrate);
    }
    return;
  }

  if (pSsMigrate->vgState == SSMIGRATE_VGSTATE_FSET_LIST_RECEIVED) {
    mndSendSsMigrateFileSetReq(pMnode, pSsMigrate);
    return;
  }

  if (pSsMigrate->vgState == SSMIGRATE_VGSTATE_FSET_STARTING) {
    // if timeout, we skip the current vgroup instead of the current file set, because timeout
    // of a file set often means the vgroup is not available.
    if (taosGetTimestampSec() - pSsMigrate->stateUpdateTime > 30) {
      mWarn("ssmigrate:%d, vgId:%d, fid:%d, haven't receive response in 30 seconds, skip", pSsMigrate->id, vgId, pSsMigrate->currFset.fid);
      pSsMigrate->vgIdx++;
      pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
      pSsMigrate->stateUpdateTime = taosGetTimestampSec();
      mndUpdateSsMigrate(pMnode, pSsMigrate);
    }
    return;
  }

  // compact need some time, so only reset migration state here and wait the next
  // tick to send the first migration request again.
  if (pSsMigrate->currFset.state == SSMIGRATE_FILESET_STATE_COMPACT) {
    mInfo("ssmigrate:%d, vgId:%d, fid:%d, compacting, will retry later", pSsMigrate->id, vgId, pSsMigrate->currFset.fid);
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_FSET_LIST_RECEIVED;
    pSsMigrate->currFset.nodeId = 0;
    pSsMigrate->stateUpdateTime = taosGetTimestampSec();
    pSsMigrate->currFset.state = SSMIGRATE_FILESET_STATE_IN_PROGRESS;
    mndUpdateSsMigrate(pMnode, pSsMigrate);
    return;
  }

  if (pSsMigrate->currFset.state == SSMIGRATE_FILESET_STATE_IN_PROGRESS) {
    if (taosGetTimestampSec() - pSsMigrate->stateUpdateTime > 30) {
      mWarn("ssmigrate:%d, vgId:%d, fid:%d, haven't receive state in 30 seconds, skip", pSsMigrate->id, vgId, pSsMigrate->currFset.fid);
      pSsMigrate->vgIdx++;
      pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
      pSsMigrate->stateUpdateTime = taosGetTimestampSec();
      mndUpdateSsMigrate(pMnode, pSsMigrate);
    } else {
      mndSendQuerySsMigrateProgressReq(pMnode, pSsMigrate);
    }
    return;
  }

  // wait at least 30 seconds after the leader node has processed the file set, this is to ensure
  // that the follower nodes have enough time to start process the file set, and make the code of
  // tsdb simpler.
  if (taosGetTimestampSec() - pSsMigrate->stateUpdateTime < 30) {
    return;
  }

  // this file set has been processed, move to the next file set
  pSsMigrate->fsetIdx++;
  if (pSsMigrate->fsetIdx >= numFset) {
    pSsMigrate->vgIdx++;
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_INIT;
  } else {
    pSsMigrate->vgState = SSMIGRATE_VGSTATE_FSET_LIST_RECEIVED;
    pSsMigrate->currFset.nodeId = 0;
    pSsMigrate->currFset.vgId = vgId;
    pSsMigrate->currFset.fid = *(int32_t*)taosArrayGet(pSsMigrate->fileSets, pSsMigrate->fsetIdx);
    pSsMigrate->currFset.state = SSMIGRATE_FILESET_STATE_IN_PROGRESS;
  }

  pSsMigrate->stateUpdateTime = taosGetTimestampSec();
  mndUpdateSsMigrate(pMnode, pSsMigrate);
}


static int32_t mndProcessUpdateSsMigrateProgressTimer(SRpcMsg *pReq) {
  mTrace("start to process update ssmigrate progress timer");

  int32_t code = 0;
  SMnode* pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SSsMigrateObj *pSsMigrate = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SSMIGRATE, pIter, (void **)&pSsMigrate);
    if (pIter == NULL) {
      break;
    }
    mndUpdateSsMigrateProgress(pMnode, pSsMigrate);
    sdbRelease(pSdb, pSsMigrate);
  }

  return 0;
}
