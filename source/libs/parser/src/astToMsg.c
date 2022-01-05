#include "parserInt.h"
#include "astGenerator.h"
#include "parserUtil.h"

SCreateUserReq* buildUserManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen) {
  SCreateUserReq* pMsg = (SCreateUserReq*)calloc(1, sizeof(SCreateUserReq));
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

SCreateAcctReq* buildAcctManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen) {
  SCreateAcctReq *pCreateMsg = (SCreateAcctReq *) calloc(1, sizeof(SCreateAcctReq));
  if (pCreateMsg == NULL) {
    qError("0x%" PRIx64 " failed to malloc for query msg", id);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

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

  *outputLen = sizeof(SCreateAcctReq);
  return pCreateMsg;
}

SDropUserReq* buildDropUserMsg(SSqlInfo* pInfo, int32_t *msgLen, int64_t id, char* msgBuf, int32_t msgBufLen) {
  SToken* pName = taosArrayGet(pInfo->pMiscInfo->a, 0);
  if (pName->n >= TSDB_USER_LEN) {
    return NULL;
  }

  SDropUserReq* pMsg = calloc(1, sizeof(SDropUserReq));
  if (pMsg == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  strncpy(pMsg->user, pName->z, pName->n);
  *msgLen = sizeof(SDropUserReq);
  return pMsg;
}

SShowMsg* buildShowMsg(SShowInfo* pShowInfo, SParseBasicCtx *pCtx, char* msgBuf, int32_t msgLen) {
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

  if (pShowInfo->showType == TSDB_MGMT_TABLE_STB || pShowInfo->showType == TSDB_MGMT_TABLE_VGROUP) {
    SName n = {0};
    tNameSetDbName(&n, pCtx->acctId, pCtx->db, strlen(pCtx->db));
    tNameGetFullDbName(&n, pShowMsg->db);
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
  pMsg->totalBlocks    = htonl(pCreateDb->numOfBlocks);
  pMsg->daysPerFile    = htonl(pCreateDb->daysPerFile);
  pMsg->commitTime     = htonl((int32_t)pCreateDb->commitTime);
  pMsg->minRows        = htonl(pCreateDb->minRowsPerBlock);
  pMsg->maxRows        = htonl(pCreateDb->maxRowsPerBlock);
  pMsg->fsyncPeriod    = htonl(pCreateDb->fsyncPeriod);
  pMsg->compression    = (int8_t) pCreateDb->compressionLevel;
  pMsg->walLevel       = (char)pCreateDb->walLevel;
  pMsg->replications   = pCreateDb->replica;
  pMsg->quorum         = pCreateDb->quorum;
  pMsg->ignoreExist    = pCreateDb->ignoreExists;
  pMsg->update         = pCreateDb->update;
  pMsg->cacheLastRow   = pCreateDb->cachelast;
  pMsg->numOfVgroups   = htonl(pCreateDb->numOfVgroups);
}

int32_t setDbOptions(SCreateDbMsg* pCreateDbMsg, const SCreateDbInfo* pCreateDbSql, SMsgBuf* pMsgBuf) {
  doSetDbOptions(pCreateDbMsg, pCreateDbSql);

  if (setKeepOption(pCreateDbMsg, pCreateDbSql, pMsgBuf) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (setTimePrecision(pCreateDbMsg, pCreateDbSql, pMsgBuf) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

SCreateDbMsg* buildCreateDbMsg(SCreateDbInfo* pCreateDbInfo, SParseBasicCtx *pCtx, SMsgBuf* pMsgBuf) {
  SCreateDbMsg* pCreateMsg = calloc(1, sizeof(SCreateDbMsg));
  if (setDbOptions(pCreateMsg, pCreateDbInfo, pMsgBuf) != TSDB_CODE_SUCCESS) {
    tfree(pCreateMsg);
    terrno = TSDB_CODE_TSC_INVALID_OPERATION;

    return NULL;
  }

  SName   name = {0};
  int32_t ret = tNameSetDbName(&name, pCtx->acctId, pCreateDbInfo->dbname.z, pCreateDbInfo->dbname.n);
  if (ret != TSDB_CODE_SUCCESS) {
    terrno = ret;
    return NULL;
  }

  tNameGetFullDbName(&name, pCreateMsg->db);
  return pCreateMsg;
}

SCreateStbMsg* buildCreateTableMsg(SCreateTableSql* pCreateTableSql, int32_t* len, SParseBasicCtx* pParseCtx, SMsgBuf* pMsgBuf) {
  SSchema* pSchema;

  int32_t numOfTags = 0;
  int32_t numOfCols = (int32_t) taosArrayGetSize(pCreateTableSql->colInfo.pColumns);
  if (pCreateTableSql->colInfo.pTagColumns != NULL) {
    numOfTags = (int32_t) taosArrayGetSize(pCreateTableSql->colInfo.pTagColumns);
  }

  SCreateStbMsg* pCreateTableMsg = (SCreateStbMsg*)calloc(1, sizeof(SCreateStbMsg) + (numOfCols + numOfTags) * sizeof(SSchema));

  char* pMsg = NULL;
  int32_t tableType = pCreateTableSql->type;
  if (tableType != TSQL_CREATE_TABLE && tableType != TSQL_CREATE_STABLE) {  // create by using super table, tags value
#if 0
    SArray* list = pInfo->pCreateTableInfo->childTableInfo;

    int32_t numOfTables = (int32_t)taosArrayGetSize(list);
    pCreateTableMsg->numOfTables = htonl(numOfTables);

    pMsg = (char*)pCreateMsg;
    for (int32_t i = 0; i < numOfTables; ++i) {
      SCreateTableMsg* pCreate = (SCreateTableMsg*)pMsg;

      pCreate->numOfColumns = htons(pCmd->numOfCols);
      pCreate->numOfTags = htons(pCmd->count);
      pMsg += sizeof(SCreateTableMsg);

      SCreatedTableInfo* p = taosArrayGet(list, i);
      strcpy(pCreate->tableName, p->fullname);
      pCreate->igExists = (p->igExist) ? 1 : 0;

      // use dbinfo from table id without modifying current db info
      pMsg = serializeTagData(&p->tagdata, pMsg);

      int32_t len = (int32_t)(pMsg - (char*)pCreate);
      pCreate->len = htonl(len);
    }
#endif
  } else { // create (super) table
    SName n = {0};
    int32_t code = createSName(&n, &pCreateTableSql->name, pParseCtx, pMsgBuf);
    if (code != 0) {
      return NULL;
    }

    code = tNameExtractFullName(&n, pCreateTableMsg->name);
    if (code != 0) {
      buildInvalidOperationMsg(pMsgBuf, "invalid table name or database not specified");
      return NULL;
    }

    pCreateTableMsg->igExists     = pCreateTableSql->existCheck ? 1 : 0;
    pCreateTableMsg->numOfColumns = htonl(numOfCols);
    pCreateTableMsg->numOfTags    = htonl(numOfTags);

    pSchema = (SSchema*) pCreateTableMsg->pSchema;
    for (int i = 0; i < numOfCols; ++i) {
      SField* pField = taosArrayGet(pCreateTableSql->colInfo.pColumns, i);
      pSchema->type  = pField->type;
      pSchema->bytes = htonl(pField->bytes);
      strcpy(pSchema->name, pField->name);

      pSchema++;
    }

    for(int32_t i = 0; i < numOfTags; ++i) {
      SField* pField = taosArrayGet(pCreateTableSql->colInfo.pTagColumns, i);
      pSchema->type  = pField->type;
      pSchema->bytes = htonl(pField->bytes);
      strcpy(pSchema->name, pField->name);

      pSchema++;
    }

    pMsg = (char*)pSchema;
  }

  int32_t msgLen = (int32_t)(pMsg - (char*)pCreateTableMsg);
  *len = msgLen;

  return pCreateTableMsg;
}

SDropStbMsg* buildDropStableMsg(SSqlInfo* pInfo, int32_t* len, SParseBasicCtx* pParseCtx, SMsgBuf* pMsgBuf) {
  SToken* tableName = taosArrayGet(pInfo->pMiscInfo->a, 0);

  SName name = {0};
  int32_t code = createSName(&name, tableName, pParseCtx, pMsgBuf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = buildInvalidOperationMsg(pMsgBuf, "invalid table name");
    return NULL;
  }

  SDropStbMsg *pDropTableMsg = (SDropStbMsg*) calloc(1, sizeof(SDropStbMsg));

  code = tNameExtractFullName(&name, pDropTableMsg->name);
  assert(code == TSDB_CODE_SUCCESS && name.type == TSDB_TABLE_NAME_T);

  pDropTableMsg->igNotExists = pInfo->pMiscInfo->existsCheck ? 1 : 0;
  *len = sizeof(SDropStbMsg);
  return pDropTableMsg;
}

SCreateDnodeMsg *buildCreateDnodeMsg(SSqlInfo* pInfo, int32_t* len, SMsgBuf* pMsgBuf) {
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

  SCreateDnodeMsg *pCreate = (SCreateDnodeMsg *) calloc(1, sizeof(SCreateDnodeMsg));
  if (pCreate == NULL) {
    buildInvalidOperationMsg(pMsgBuf, msg4);
    return NULL;
  }

  strncpy(pCreate->fqdn, id->z, id->n);
  pCreate->port = htonl(val);

  *len = sizeof(SCreateDnodeMsg);
  return pCreate;
}

SDropDnodeMsg *buildDropDnodeMsg(SSqlInfo* pInfo, int32_t* len, SMsgBuf* pMsgBuf) {
  SToken* pzName = taosArrayGet(pInfo->pMiscInfo->a, 0);

  char* end = NULL;
  SDropDnodeMsg * pDrop = (SDropDnodeMsg *)calloc(1, sizeof(SDropDnodeMsg));
  pDrop->dnodeId = strtoll(pzName->z, &end, 10);
  pDrop->dnodeId = htonl(pDrop->dnodeId);
  *len = sizeof(SDropDnodeMsg);

  if (end - pzName->z != pzName->n) {
    buildInvalidOperationMsg(pMsgBuf, "invalid dnode id");
    tfree(pDrop);
    return NULL;
  }

  return pDrop;
}