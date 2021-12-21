#include "parserInt.h"
#include "parserUtil.h"

SCreateUserMsg* buildUserManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen) {
  SCreateUserMsg* pMsg = (SCreateUserMsg*)calloc(1, sizeof(SCreateUserMsg));
  if (pMsg == NULL) {
    //    tscError("0x%" PRIx64 " failed to malloc for query msg", id);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  SUserInfo* pUser = &pInfo->pMiscInfo->user;
  strncpy(pMsg->user, pUser->user.z, pUser->user.n);
  pMsg->type = pUser->type;
  pMsg->superUser = (int8_t)pUser->type;

  if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
    //    pMsg->privilege = (char)pCmd->count;
  } else {
    strncpy(pMsg->pass, pUser->passwd.z, pUser->passwd.n);
  }

  *outputLen = sizeof(SUserInfo);
  return pMsg;
}

SCreateAcctMsg* buildAcctManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen) {
  SCreateAcctMsg* pMsg = (SCreateAcctMsg*)calloc(1, sizeof(SCreateAcctMsg));
  if (pMsg == NULL) {
    //    tscError("0x%" PRIx64 " failed to malloc for query msg", id);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  SCreateAcctMsg *pCreateMsg = (SCreateAcctMsg *) calloc(1, sizeof(SCreateAcctMsg));

  SToken *pName = &pInfo->pMiscInfo->user.user;
  SToken *pPwd = &pInfo->pMiscInfo->user.passwd;

  strncpy(pCreateMsg->user, pName->z, pName->n);
  strncpy(pCreateMsg->pass, pPwd->z, pPwd->n);

  SCreateAcctInfo *pAcctOpt = &pInfo->pMiscInfo->acctOpt;

  pCreateMsg->maxUsers = htonl(pAcctOpt->maxUsers);
  pCreateMsg->maxDbs = htonl(pAcctOpt->maxDbs);
  pCreateMsg->maxTimeSeries = htonl(pAcctOpt->maxTimeSeries);
  pCreateMsg->maxStreams = htonl(pAcctOpt->maxStreams);
//  pCreateMsg->maxPointsPerSecond = htonl(pAcctOpt->maxPointsPerSecond);
  pCreateMsg->maxStorage = htobe64(pAcctOpt->maxStorage);
//  pCreateMsg->maxQueryTime = htobe64(pAcctOpt->maxQueryTime);
//  pCreateMsg->maxConnections = htonl(pAcctOpt->maxConnections);

  if (pAcctOpt->stat.n == 0) {
    pCreateMsg->accessState = -1;
  } else {
    if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
      pCreateMsg->accessState = TSDB_VN_READ_ACCCESS;
    } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
      pCreateMsg->accessState = TSDB_VN_WRITE_ACCCESS;
    } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
      pCreateMsg->accessState = TSDB_VN_ALL_ACCCESS;
    } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
      pCreateMsg->accessState = 0;
    }
  }

  *outputLen = sizeof(SCreateAcctMsg);
  return pMsg;
}
SDropUserMsg* buildDropUserMsg(SSqlInfo* pInfo, int32_t *msgLen, int64_t id, char* msgBuf, int32_t msgBufLen) {
  SToken* pName = taosArrayGet(pInfo->pMiscInfo->a, 0);
  if (pName->n >= TSDB_USER_LEN) {
    return NULL;
  }


  SDropUserMsg* pMsg = calloc(1, sizeof(SDropUserMsg));
  if (pMsg == NULL) {
    return NULL;
  }

  strncpy(pMsg->user, pName->z, pName->n);
  *msgLen = sizeof(SDropUserMsg);
  return pMsg;
}

SShowMsg* buildShowMsg(SShowInfo* pShowInfo, int64_t id, char* msgBuf, int32_t msgLen) {
  SShowMsg* pShowMsg = calloc(1, sizeof(SShowMsg));

  pShowMsg->type = pShowInfo->showType;

  if (pShowInfo->showType != TSDB_MGMT_TABLE_VNODES) {
    SToken* pPattern = &pShowInfo->pattern;
    if (pPattern->type > 0) {  // only show tables support wildcard query
      strncpy(pShowMsg->payload, pPattern->z, pPattern->n);
      pShowMsg->payloadLen = htons(pPattern->n);
    }
  } else {
    SToken* pEpAddr = &pShowInfo->prefix;
    assert(pEpAddr->n > 0 && pEpAddr->type > 0);

    strncpy(pShowMsg->payload, pEpAddr->z, pEpAddr->n);
    pShowMsg->payloadLen = htons(pEpAddr->n);
  }

  return pShowMsg;
}

static int32_t setKeepOption(SCreateDbMsg* pMsg, const SCreateDbInfo* pCreateDb, SMsgBuf* pMsgBuf) {
  const char* msg1 = "invalid number of keep options";
  const char* msg2 = "invalid keep value";
  const char* msg3 = "invalid keep value, should be keep0 <= keep1 <= keep2";

  pMsg->daysToKeep0 = htonl(-1);
  pMsg->daysToKeep1 = htonl(-1);
  pMsg->daysToKeep2 = htonl(-1);

  SArray* pKeep = pCreateDb->keep;
  if (pKeep != NULL) {
    size_t s = taosArrayGetSize(pKeep);
#ifdef _STORAGE
    if (s >= 4 ||s <= 0) {
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

static int32_t setTimePrecision(SCreateDbMsg* pMsg, const SCreateDbInfo* pCreateDbInfo, SMsgBuf* pMsgBuf) {
  const char* msg = "invalid time precision";

  pMsg->precision = TSDB_TIME_PRECISION_MILLI;  // millisecond by default

  SToken* pToken = (SToken*) &pCreateDbInfo->precision;
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

static void doSetDbOptions(SCreateDbMsg* pMsg, const SCreateDbInfo* pCreateDb) {
  pMsg->cacheBlockSize = htonl(pCreateDb->cacheBlockSize);
  pMsg->totalBlocks  = htonl(pCreateDb->numOfBlocks);
  pMsg->daysPerFile  = htonl(pCreateDb->daysPerFile);
  pMsg->commitTime   = htonl((int32_t)pCreateDb->commitTime);
  pMsg->minRowsPerFileBlock = htonl(pCreateDb->minRowsPerBlock);
  pMsg->maxRowsPerFileBlock = htonl(pCreateDb->maxRowsPerBlock);
  pMsg->fsyncPeriod  = htonl(pCreateDb->fsyncPeriod);
  pMsg->compression  = pCreateDb->compressionLevel;
  pMsg->walLevel     = (char)pCreateDb->walLevel;
  pMsg->replications = pCreateDb->replica;
  pMsg->quorum       = pCreateDb->quorum;
  pMsg->ignoreExist  = pCreateDb->ignoreExists;
  pMsg->update       = pCreateDb->update;
  pMsg->cacheLastRow = pCreateDb->cachelast;
}

int32_t setDbOptions(SCreateDbMsg* pCreateDbMsg, const SCreateDbInfo* pCreateDbSql, SMsgBuf* pMsgBuf) {
  doSetDbOptions(pCreateDbMsg, pCreateDbSql);

  if (setKeepOption(pCreateDbMsg, pCreateDbSql, pMsgBuf) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (setTimePrecision(pCreateDbMsg, pCreateDbSql, pMsgBuf) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  // todo configurable
  pCreateDbMsg->numOfVgroups = htonl(2);
  return TSDB_CODE_SUCCESS;
}

SCreateDbMsg* buildCreateDbMsg(SCreateDbInfo* pCreateDbInfo, char* msgBuf, int32_t msgLen) {
  SCreateDbMsg* pCreateMsg = calloc(1, sizeof(SCreateDbMsg));

  SMsgBuf msg = {.buf = msgBuf, .len = msgLen};
  if (setDbOptions(pCreateMsg, pCreateDbInfo, &msg) != TSDB_CODE_SUCCESS) {
    tfree(pCreateMsg);
    terrno = TSDB_CODE_TSC_INVALID_OPERATION;

    return NULL;
  }

  return pCreateMsg;
}
