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

#include "astGenerator.h"
#include "parserInt.h"
#include "parserUtil.h"

char* buildUserManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen) {
  SCreateUserReq createReq = {0};

  SUserInfo* pUser = &pInfo->pMiscInfo->user;
  strncpy(createReq.user, pUser->user.z, pUser->user.n);
  createReq.createType = pUser->type;
  createReq.superUser = (int8_t)pUser->type;

  if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
    // pMsg->privilege = (char)pCmd->count;
  } else {
    strncpy(createReq.pass, pUser->passwd.z, pUser->passwd.n);
  }

  int32_t tlen = tSerializeSCreateUserReq(NULL, 0, &createReq);
  void*   pReq = malloc(tlen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSCreateUserReq(pReq, tlen, &createReq);
  *outputLen = tlen;
  return pReq;
}

char* buildAcctManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen) {
  SCreateAcctReq createReq = {0};

  SToken* pName = &pInfo->pMiscInfo->user.user;
  SToken* pPwd = &pInfo->pMiscInfo->user.passwd;

  strncpy(createReq.user, pName->z, pName->n);
  strncpy(createReq.pass, pPwd->z, pPwd->n);

  SCreateAcctInfo* pAcctOpt = &pInfo->pMiscInfo->acctOpt;

  createReq.maxUsers = pAcctOpt->maxUsers;
  createReq.maxDbs = pAcctOpt->maxDbs;
  createReq.maxTimeSeries = pAcctOpt->maxTimeSeries;
  createReq.maxStreams = pAcctOpt->maxStreams;
  createReq.maxStorage = pAcctOpt->maxStorage;

  if (pAcctOpt->stat.n == 0) {
    createReq.accessState = -1;
  } else {
    if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
      createReq.accessState = TSDB_VN_READ_ACCCESS;
    } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
      createReq.accessState = TSDB_VN_WRITE_ACCCESS;
    } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
      createReq.accessState = TSDB_VN_ALL_ACCCESS;
    } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
      createReq.accessState = 0;
    }
  }

  int32_t tlen = tSerializeSCreateAcctReq(NULL, 0, &createReq);
  void*   pReq = malloc(tlen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSCreateAcctReq(pReq, tlen, &createReq);
  *outputLen = tlen;
  return pReq;
}

char* buildDropUserMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgBufLen) {
  SDropUserReq dropReq = {0};

  SToken* pName = taosArrayGet(pInfo->pMiscInfo->a, 0);
  if (pName->n >= TSDB_USER_LEN) {
    return NULL;
  }

  strncpy(dropReq.user, pName->z, pName->n);

  int32_t tlen = tSerializeSDropUserReq(NULL, 0, &dropReq);
  void*   pReq = malloc(tlen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSDropUserReq(pReq, tlen, &dropReq);
  *outputLen = tlen;
  return pReq;
}

char* buildShowMsg(SShowInfo* pShowInfo, int32_t* outputLen, SParseContext* pCtx, SMsgBuf* pMsgBuf) {
  SShowReq showReq = {.type = pShowInfo->showType};

  if (pShowInfo->showType != TSDB_MGMT_TABLE_VNODES) {
    SToken* pPattern = &pShowInfo->pattern;
    if (pPattern->type > 0) {  // only show tables support wildcard query
      showReq.payloadLen = pPattern->n;
      showReq.payload = malloc(showReq.payloadLen);
      strncpy(showReq.payload, pPattern->z, pPattern->n);
    }
  } else {
    SToken* pEpAddr = &pShowInfo->prefix;
    assert(pEpAddr->n > 0 && pEpAddr->type > 0);
    showReq.payloadLen = pEpAddr->n;
    showReq.payload = malloc(showReq.payloadLen);
    strncpy(showReq.payload, pEpAddr->z, pEpAddr->n);
  }

  if (pShowInfo->showType == TSDB_MGMT_TABLE_STB || pShowInfo->showType == TSDB_MGMT_TABLE_VGROUP) {
    SName n = {0};

    if (pShowInfo->prefix.n > 0) {
      if (pShowInfo->prefix.n >= TSDB_DB_FNAME_LEN) {
        terrno = buildInvalidOperationMsg(pMsgBuf, "prefix name is too long");
        tFreeSShowReq(&showReq);
        return NULL;
      }
      tNameSetDbName(&n, pCtx->acctId, pShowInfo->prefix.z, pShowInfo->prefix.n);
    } else if (pCtx->db == NULL || strlen(pCtx->db) == 0) {
      terrno = buildInvalidOperationMsg(pMsgBuf, "database is not specified");
      tFreeSShowReq(&showReq);
      return NULL;
    } else {
      tNameSetDbName(&n, pCtx->acctId, pCtx->db, strlen(pCtx->db));
    }

    tNameGetFullDbName(&n, showReq.db);
  }

  int32_t tlen = tSerializeSShowReq(NULL, 0, &showReq);
  void*   pReq = malloc(tlen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSShowReq(pReq, tlen, &showReq);
  tFreeSShowReq(&showReq);
  *outputLen = tlen;
  return pReq;
}

static int32_t setKeepOption(SCreateDbReq* pMsg, const SCreateDbInfo* pCreateDb, SMsgBuf* pMsgBuf) {
  const char* msg1 = "invalid number of keep options";
  const char* msg2 = "invalid keep value";
  const char* msg3 = "invalid keep value, should be keep0 <= keep1 <= keep2";

  pMsg->daysToKeep0 = -1;
  pMsg->daysToKeep1 = -1;
  pMsg->daysToKeep2 = -1;

  SArray* pKeep = pCreateDb->keep;
  if (pKeep != NULL) {
    size_t s = taosArrayGetSize(pKeep);
#ifdef _STORAGE
    if (s >= 4 || s <= 0) {
#else
    if (s != 1) {
#endif
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    //    tListI* p0 = taosArrayGet(pKeep, 0);
    //    tVariantListItem* p1 = (s > 1) ? taosArrayGet(pKeep, 1) : p0;
    //    tVariantListItem* p2 = (s > 2) ? taosArrayGet(pKeep, 2) : p1;
    //
    //    if ((int32_t)p0->pVar.i64 <= 0 || (int32_t)p1->pVar.i64 <= 0 || (int32_t)p2->pVar.i64 <= 0) {
    //      return buildInvalidOperationMsg(pMsgBuf, msg2);
    //    }
    //    if (!(((int32_t)p0->pVar.i64 <= (int32_t)p1->pVar.i64) && ((int32_t)p1->pVar.i64 <= (int32_t)p2->pVar.i64))) {
    //      return buildInvalidOperationMsg(pMsgBuf, msg3);
    //    }
    //
    //    pMsg->daysToKeep0 = htonl((int32_t)p0->pVar.i64);
    //    pMsg->daysToKeep1 = htonl((int32_t)p1->pVar.i64);
    //    pMsg->daysToKeep2 = htonl((int32_t)p2->pVar.i64);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setTimePrecision(SCreateDbReq* pMsg, const SCreateDbInfo* pCreateDbInfo, SMsgBuf* pMsgBuf) {
  const char* msg = "invalid time precision";

  pMsg->precision = TSDB_TIME_PRECISION_MILLI;  // millisecond by default

  SToken* pToken = (SToken*)&pCreateDbInfo->precision;
  if (pToken->n > 0) {
    pToken->n = strdequote(pToken->z);

    if (strncmp(pToken->z, TSDB_TIME_PRECISION_MILLI_STR, pToken->n) == 0 &&
        strlen(TSDB_TIME_PRECISION_MILLI_STR) == pToken->n) {
      // time precision for this db: million second
      pMsg->precision = TSDB_TIME_PRECISION_MILLI;
    } else if (strncmp(pToken->z, TSDB_TIME_PRECISION_MICRO_STR, pToken->n) == 0 &&
               strlen(TSDB_TIME_PRECISION_MICRO_STR) == pToken->n) {
      pMsg->precision = TSDB_TIME_PRECISION_MICRO;
    } else if (strncmp(pToken->z, TSDB_TIME_PRECISION_NANO_STR, pToken->n) == 0 &&
               strlen(TSDB_TIME_PRECISION_NANO_STR) == pToken->n) {
      pMsg->precision = TSDB_TIME_PRECISION_NANO;
    } else {
      return buildInvalidOperationMsg(pMsgBuf, msg);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void doSetDbOptions(SCreateDbReq* pMsg, const SCreateDbInfo* pCreateDb) {
  pMsg->cacheBlockSize = pCreateDb->cacheBlockSize;
  pMsg->totalBlocks = pCreateDb->numOfBlocks;
  pMsg->daysPerFile = pCreateDb->daysPerFile;
  pMsg->commitTime = (int32_t)pCreateDb->commitTime;
  pMsg->minRows = pCreateDb->minRowsPerBlock;
  pMsg->maxRows = pCreateDb->maxRowsPerBlock;
  pMsg->fsyncPeriod = pCreateDb->fsyncPeriod;
  pMsg->compression = (int8_t)pCreateDb->compressionLevel;
  pMsg->walLevel = (char)pCreateDb->walLevel;
  pMsg->replications = pCreateDb->replica;
  pMsg->quorum = pCreateDb->quorum;
  pMsg->ignoreExist = pCreateDb->ignoreExists;
  pMsg->update = pCreateDb->update;
  pMsg->cacheLastRow = pCreateDb->cachelast;
  pMsg->numOfVgroups = pCreateDb->numOfVgroups;
}

int32_t setDbOptions(SCreateDbReq* pCreateDbMsg, const SCreateDbInfo* pCreateDbSql, SMsgBuf* pMsgBuf) {
  doSetDbOptions(pCreateDbMsg, pCreateDbSql);

  if (setKeepOption(pCreateDbMsg, pCreateDbSql, pMsgBuf) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (setTimePrecision(pCreateDbMsg, pCreateDbSql, pMsgBuf) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

// can only perform the parameters based on the macro definitation
static int32_t doCheckDbOptions(SCreateDbReq* pCreate, SMsgBuf* pMsgBuf) {
  char msg[512] = {0};

  if (pCreate->walLevel != -1 && (pCreate->walLevel < TSDB_MIN_WAL_LEVEL || pCreate->walLevel > TSDB_MAX_WAL_LEVEL)) {
    snprintf(msg, tListLen(msg), "invalid db option walLevel: %d, only 1-2 allowed", pCreate->walLevel);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  if (pCreate->replications != -1 &&
      (pCreate->replications < TSDB_MIN_DB_REPLICA_OPTION || pCreate->replications > TSDB_MAX_DB_REPLICA_OPTION)) {
    snprintf(msg, tListLen(msg), "invalid db option replications: %d valid range: [%d, %d]", pCreate->replications,
             TSDB_MIN_DB_REPLICA_OPTION, TSDB_MAX_DB_REPLICA_OPTION);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  int32_t blocks = pCreate->totalBlocks;
  if (blocks != -1 && (blocks < TSDB_MIN_TOTAL_BLOCKS || blocks > TSDB_MAX_TOTAL_BLOCKS)) {
    snprintf(msg, tListLen(msg), "invalid db option totalBlocks: %d valid range: [%d, %d]", blocks,
             TSDB_MIN_TOTAL_BLOCKS, TSDB_MAX_TOTAL_BLOCKS);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  if (pCreate->quorum != -1 &&
      (pCreate->quorum < TSDB_MIN_DB_QUORUM_OPTION || pCreate->quorum > TSDB_MAX_DB_QUORUM_OPTION)) {
    snprintf(msg, tListLen(msg), "invalid db option quorum: %d valid range: [%d, %d]", pCreate->quorum,
             TSDB_MIN_DB_QUORUM_OPTION, TSDB_MAX_DB_QUORUM_OPTION);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  int32_t val = pCreate->daysPerFile;
  if (val != -1 && (val < TSDB_MIN_DAYS_PER_FILE || val > TSDB_MAX_DAYS_PER_FILE)) {
    snprintf(msg, tListLen(msg), "invalid db option daysPerFile: %d valid range: [%d, %d]", val, TSDB_MIN_DAYS_PER_FILE,
             TSDB_MAX_DAYS_PER_FILE);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  val = pCreate->cacheBlockSize;
  if (val != -1 && (val < TSDB_MIN_CACHE_BLOCK_SIZE || val > TSDB_MAX_CACHE_BLOCK_SIZE)) {
    snprintf(msg, tListLen(msg), "invalid db option cacheBlockSize: %d valid range: [%d, %d]", val,
             TSDB_MIN_CACHE_BLOCK_SIZE, TSDB_MAX_CACHE_BLOCK_SIZE);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  if (pCreate->precision != TSDB_TIME_PRECISION_MILLI && pCreate->precision != TSDB_TIME_PRECISION_MICRO &&
      pCreate->precision != TSDB_TIME_PRECISION_NANO) {
    snprintf(msg, tListLen(msg), "invalid db option timePrecision: %d valid value: [%d, %d, %d]", pCreate->precision,
             TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  val = pCreate->commitTime;
  if (val != -1 && (val < TSDB_MIN_COMMIT_TIME || val > TSDB_MAX_COMMIT_TIME)) {
    snprintf(msg, tListLen(msg), "invalid db option commitTime: %d valid range: [%d, %d]", val, TSDB_MIN_COMMIT_TIME,
             TSDB_MAX_COMMIT_TIME);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  val = pCreate->fsyncPeriod;
  if (val != -1 && (val < TSDB_MIN_FSYNC_PERIOD || val > TSDB_MAX_FSYNC_PERIOD)) {
    snprintf(msg, tListLen(msg), "invalid db option fsyncPeriod: %d valid range: [%d, %d]", val, TSDB_MIN_FSYNC_PERIOD,
             TSDB_MAX_FSYNC_PERIOD);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  if (pCreate->compression != -1 &&
      (pCreate->compression < TSDB_MIN_COMP_LEVEL || pCreate->compression > TSDB_MAX_COMP_LEVEL)) {
    snprintf(msg, tListLen(msg), "invalid db option compression: %d valid range: [%d, %d]", pCreate->compression,
             TSDB_MIN_COMP_LEVEL, TSDB_MAX_COMP_LEVEL);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  val = pCreate->numOfVgroups;
  if (val < TSDB_MIN_VNODES_PER_DB || val > TSDB_MAX_VNODES_PER_DB) {
    snprintf(msg, tListLen(msg), "invalid number of vgroups for DB:%d valid range: [%d, %d]", val,
             TSDB_MIN_VNODES_PER_DB, TSDB_MAX_VNODES_PER_DB);
  }

  val = pCreate->maxRows;
  if (val < TSDB_MIN_MAX_ROW_FBLOCK || val > TSDB_MAX_MAX_ROW_FBLOCK) {
    snprintf(msg, tListLen(msg), "invalid number of max rows in file block for DB:%d valid range: [%d, %d]", val,
             TSDB_MIN_MAX_ROW_FBLOCK, TSDB_MAX_MAX_ROW_FBLOCK);
  }

  val = pCreate->minRows;
  if (val < TSDB_MIN_MIN_ROW_FBLOCK || val > TSDB_MAX_MIN_ROW_FBLOCK) {
    snprintf(msg, tListLen(msg), "invalid number of min rows in file block for DB:%d valid range: [%d, %d]", val,
             TSDB_MIN_MIN_ROW_FBLOCK, TSDB_MAX_MIN_ROW_FBLOCK);
  }

  return TSDB_CODE_SUCCESS;
}

char* buildCreateDbMsg(SCreateDbInfo* pCreateDbInfo, int32_t* outputLen, SParseContext* pCtx, SMsgBuf* pMsgBuf) {
  SCreateDbReq createReq = {0};

  if (setDbOptions(&createReq, pCreateDbInfo, pMsgBuf) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TSC_INVALID_OPERATION;
    return NULL;
  }

  SName   name = {0};
  int32_t ret = tNameSetDbName(&name, pCtx->acctId, pCreateDbInfo->dbname.z, pCreateDbInfo->dbname.n);
  if (ret != TSDB_CODE_SUCCESS) {
    terrno = ret;
    return NULL;
  }

  tNameGetFullDbName(&name, createReq.db);

  if (doCheckDbOptions(&createReq, pMsgBuf) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TSC_INVALID_OPERATION;
    return NULL;
  }

  int32_t tlen = tSerializeSCreateDbReq(NULL, 0, &createReq);
  void*   pReq = malloc(tlen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSCreateDbReq(pReq, tlen, &createReq);
  *outputLen = tlen;
  return pReq;
}

char* buildCreateStbReq(SCreateTableSql* pCreateTableSql, int32_t* outputLen, SParseContext* pParseCtx,
                        SMsgBuf* pMsgBuf) {
  SMCreateStbReq createReq = {0};
  createReq.igExists = pCreateTableSql->existCheck ? 1 : 0;
  createReq.pColumns = pCreateTableSql->colInfo.pColumns;
  createReq.pTags = pCreateTableSql->colInfo.pTagColumns;
  createReq.numOfColumns = (int32_t)taosArrayGetSize(pCreateTableSql->colInfo.pColumns);
  createReq.numOfTags = (int32_t)taosArrayGetSize(pCreateTableSql->colInfo.pTagColumns);

  SName n = {0};
  if (createSName(&n, &pCreateTableSql->name, pParseCtx, pMsgBuf) != 0) {
    return NULL;
  }

  if (tNameExtractFullName(&n, createReq.name) != 0) {
    buildInvalidOperationMsg(pMsgBuf, "invalid table name or database not specified");
    return NULL;
  }

  int32_t tlen = tSerializeSMCreateStbReq(NULL, &createReq);
  void*   pReq = malloc(tlen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void* pBuf = pReq;
  tSerializeSMCreateStbReq(&pBuf, &createReq);
  *outputLen = tlen;
  return pReq;
}

char* buildDropStableReq(SSqlInfo* pInfo, int32_t* outputLen, SParseContext* pParseCtx, SMsgBuf* pMsgBuf) {
  SToken* tableName = taosArrayGet(pInfo->pMiscInfo->a, 0);

  SName   name = {0};
  int32_t code = createSName(&name, tableName, pParseCtx, pMsgBuf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = buildInvalidOperationMsg(pMsgBuf, "invalid table name");
    return NULL;
  }

  SMDropStbReq dropReq = {0};
  code = tNameExtractFullName(&name, dropReq.name);

  assert(code == TSDB_CODE_SUCCESS && name.type == TSDB_TABLE_NAME_T);
  dropReq.igNotExists = pInfo->pMiscInfo->existsCheck ? 1 : 0;

  int32_t tlen = tSerializeSMDropStbReq(NULL, &dropReq);
  void*   pReq = malloc(tlen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void* pBuf = pReq;
  tSerializeSMDropStbReq(&pBuf, &dropReq);
  *outputLen = tlen;
  return pReq;
}

char* buildCreateDnodeMsg(SSqlInfo* pInfo, int32_t* outputLen, SMsgBuf* pMsgBuf) {
  const char* msg1 = "invalid host name (name too long, maximum length 128)";
  const char* msg2 = "dnode name can not be string";
  const char* msg3 = "port should be an integer that is less than 65535 and greater than 0";
  const char* msg4 = "failed prepare create dnode message";

  if (taosArrayGetSize(pInfo->pMiscInfo->a) != 2) {
    buildInvalidOperationMsg(pMsgBuf, msg1);
    return NULL;
  }

  SToken* id = taosArrayGet(pInfo->pMiscInfo->a, 0);
  if (id->type != TK_ID && id->type != TK_IPTOKEN) {
    buildInvalidOperationMsg(pMsgBuf, msg2);
    return NULL;
  }

  SToken* port = taosArrayGet(pInfo->pMiscInfo->a, 1);
  if (port->type != TK_INTEGER) {
    buildInvalidOperationMsg(pMsgBuf, msg3);
    return NULL;
  }

  bool    isSign = false;
  int64_t val = 0;

  toInteger(port->z, port->n, 10, &val, &isSign);
  if (val >= UINT16_MAX || val <= 0) {
    buildInvalidOperationMsg(pMsgBuf, msg3);
    return NULL;
  }

  SCreateDnodeReq createReq = {0};

  strncpy(createReq.fqdn, id->z, id->n);
  createReq.port = val;

  int32_t tlen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
  void*   pReq = malloc(tlen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSCreateDnodeReq(pReq, tlen, &createReq);
  *outputLen = tlen;
  return pReq;
}

char* buildDropDnodeMsg(SSqlInfo* pInfo, int32_t* outputLen, SMsgBuf* pMsgBuf) {
  SDropDnodeReq dropReq = {0};

  SToken* pzName = taosArrayGet(pInfo->pMiscInfo->a, 0);

  char* end = NULL;
  dropReq.dnodeId = strtoll(pzName->z, &end, 10);

  if (end - pzName->z != pzName->n) {
    buildInvalidOperationMsg(pMsgBuf, "invalid dnode id");
    return NULL;
  }

  int32_t tlen = tSerializeSDropDnodeReq(NULL, 0, &dropReq);
  void*   pReq = malloc(tlen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSDropDnodeReq(pReq, tlen, &dropReq);
  *outputLen = tlen;
  return pReq;
}