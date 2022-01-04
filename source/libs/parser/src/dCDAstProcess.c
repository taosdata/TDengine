#include <ttime.h>
#include "astToMsg.h"
#include "parserInt.h"
#include "parserUtil.h"
#include "queryInfoUtil.h"
#include "tglobal.h"

/* is contained in pFieldList or not */
static bool has(SArray* pFieldList, int32_t startIndex, const char* name) {
  size_t numOfCols = taosArrayGetSize(pFieldList);
  for (int32_t j = startIndex; j < numOfCols; ++j) {
    TAOS_FIELD* field = taosArrayGet(pFieldList, j);
    if (strncasecmp(name, field->name, sizeof(field->name) - 1) == 0) return true;
  }

  return false;
}

static int32_t setShowInfo(SShowInfo* pShowInfo, SParseBasicCtx *pCtx, void** output, int32_t* outputLen, SMsgBuf* pMsgBuf) {
  const char* msg1 = "invalid name";
  const char* msg2 = "wildcard string should be less than %d characters";
  const char* msg3 = "database name too long";
  const char* msg4 = "pattern is invalid";
  const char* msg5 = "database name is empty";
  const char* msg6 = "pattern string is empty";

  /*
   * database prefix in pInfo->pMiscInfo->a[0]
   * wildcard in like clause in pInfo->pMiscInfo->a[1]
   */
  int16_t    showType = pShowInfo->showType;
  if (showType == TSDB_MGMT_TABLE_STB || showType == TSDB_MGMT_TABLE_VGROUP) {
    SToken* pDbPrefixToken = &pShowInfo->prefix;
    if (pDbPrefixToken->type != 0) {
      if (pDbPrefixToken->n >= TSDB_DB_NAME_LEN) {  // db name is too long
        return buildInvalidOperationMsg(pMsgBuf, msg3);
      }

      if (pDbPrefixToken->n <= 0) {
        return buildInvalidOperationMsg(pMsgBuf, msg5);
      }

      if (parserValidateIdToken(pDbPrefixToken) != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg1);
      }

      //      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pRequest->pTsc), pDbPrefixToken);
      //      if (ret != TSDB_CODE_SUCCESS) {
      //        return buildInvalidOperationMsg(pMsgBuf, msg1);
      //      }
    }

    // show table/stable like 'xxxx', set the like pattern for show tables
    SToken* pPattern = &pShowInfo->pattern;
    if (pPattern->type != 0) {
      if (pPattern->type == TK_ID && pPattern->z[0] == TS_ESCAPE_CHAR) {
        return buildInvalidOperationMsg(pMsgBuf, msg4);
      }

      pPattern->n = strdequote(pPattern->z);
      if (pPattern->n <= 0) {
        return buildInvalidOperationMsg(pMsgBuf, msg6);
      }

      if (pPattern->n > tsMaxWildCardsLen) {
        char tmp[64] = {0};
        sprintf(tmp, msg2, tsMaxWildCardsLen);
        return buildInvalidOperationMsg(pMsgBuf, tmp);
      }
    }
  } else if (showType == TSDB_MGMT_TABLE_VNODES) {
    if (pShowInfo->prefix.type == 0) {
      return buildInvalidOperationMsg(pMsgBuf, "No specified dnode ep");
    }

    if (pShowInfo->prefix.type == TK_STRING) {
      pShowInfo->prefix.n = strdequote(pShowInfo->prefix.z);
    }
  }

  *output = buildShowMsg(pShowInfo, pCtx, pMsgBuf->buf, pMsgBuf->len);
  *outputLen = sizeof(SShowMsg)/* + htons(pShowMsg->payloadLen)*/;
  return TSDB_CODE_SUCCESS;
}

// can only perform the parameters based on the macro definitation
static int32_t doCheckDbOptions(SCreateDbMsg* pCreate, SMsgBuf* pMsgBuf) {
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

  int32_t blocks = ntohl(pCreate->totalBlocks);
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

  int32_t val = htonl(pCreate->daysPerFile);
  if (val != -1 && (val < TSDB_MIN_DAYS_PER_FILE || val > TSDB_MAX_DAYS_PER_FILE)) {
    snprintf(msg, tListLen(msg), "invalid db option daysPerFile: %d valid range: [%d, %d]", val,
             TSDB_MIN_DAYS_PER_FILE, TSDB_MAX_DAYS_PER_FILE);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  val = htonl(pCreate->cacheBlockSize);
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

  val = htonl(pCreate->commitTime);
  if (val != -1 && (val < TSDB_MIN_COMMIT_TIME || val > TSDB_MAX_COMMIT_TIME)) {
    snprintf(msg, tListLen(msg), "invalid db option commitTime: %d valid range: [%d, %d]", val,
             TSDB_MIN_COMMIT_TIME, TSDB_MAX_COMMIT_TIME);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  val = htonl(pCreate->fsyncPeriod);
  if (val != -1 && (val < TSDB_MIN_FSYNC_PERIOD || val > TSDB_MAX_FSYNC_PERIOD)) {
    snprintf(msg, tListLen(msg), "invalid db option fsyncPeriod: %d valid range: [%d, %d]", val,
             TSDB_MIN_FSYNC_PERIOD, TSDB_MAX_FSYNC_PERIOD);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  if (pCreate->compression != -1 &&
      (pCreate->compression < TSDB_MIN_COMP_LEVEL || pCreate->compression > TSDB_MAX_COMP_LEVEL)) {
    snprintf(msg, tListLen(msg), "invalid db option compression: %d valid range: [%d, %d]", pCreate->compression,
             TSDB_MIN_COMP_LEVEL, TSDB_MAX_COMP_LEVEL);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateTableColumns(SArray* pFieldList, int32_t maxRowLength, int32_t maxColumns, SMsgBuf* pMsgBuf) {
  const char* msg2 = "row length exceeds max length";
  const char* msg3 = "duplicated column names";
  const char* msg4 = "invalid data type";
  const char* msg5 = "invalid binary/nchar column length";
  const char* msg6 = "invalid column name";
  const char* msg7 = "too many columns";
  const char* msg8 = "illegal number of columns";

  size_t numOfCols = taosArrayGetSize(pFieldList);
  if (numOfCols > maxColumns) {
    return buildInvalidOperationMsg(pMsgBuf, msg7);
  }

  int32_t rowLen = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    TAOS_FIELD* pField = taosArrayGet(pFieldList, i);
    if (!isValidDataType(pField->type)) {
      return buildInvalidOperationMsg(pMsgBuf, msg4);
    }

    if (pField->bytes == 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg5);
    }

    if ((pField->type == TSDB_DATA_TYPE_BINARY && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_BINARY_LEN)) ||
        (pField->type == TSDB_DATA_TYPE_NCHAR && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_NCHAR_LEN))) {
      return buildInvalidOperationMsg(pMsgBuf, msg5);
    }

    SToken nameToken = {.z = pField->name, .n = strlen(pField->name), .type = TK_ID};
    if (parserValidateNameToken(&nameToken) != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg6);
    }

    // field name must be unique
    if (has(pFieldList, i + 1, pField->name) == true) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }

    rowLen += pField->bytes;
  }

  // max row length must be less than TSDB_MAX_BYTES_PER_ROW
  if (rowLen > maxRowLength) {
    return buildInvalidOperationMsg(pMsgBuf, msg2);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateTableColumnInfo(SArray* pFieldList, SMsgBuf* pMsgBuf) {
  assert(pFieldList != NULL);

  const char* msg1 = "first column must be timestamp";
  const char* msg2 = "row length exceeds max length";
  const char* msg3 = "duplicated column names";
  const char* msg4 = "invalid data type";
  const char* msg5 = "invalid binary/nchar column length";
  const char* msg6 = "invalid column name";
  const char* msg7 = "too many columns";
  const char* msg8 = "illegal number of columns";

  // first column must be timestamp
  SField* pField = taosArrayGet(pFieldList, 0);
  if (pField->type != TSDB_DATA_TYPE_TIMESTAMP) {
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  // number of fields no less than 2
  size_t numOfCols = taosArrayGetSize(pFieldList);
  if (numOfCols <= 1) {
    return buildInvalidOperationMsg(pMsgBuf, msg8);
  }

  return validateTableColumns(pFieldList, TSDB_MAX_BYTES_PER_ROW, TSDB_MAX_COLUMNS, pMsgBuf);
}

static int32_t validateTagParams(SArray* pTagsList, SArray* pFieldList, SMsgBuf* pMsgBuf) {
  assert(pTagsList != NULL);

  const char* msg1 = "invalid number of tag columns";
  const char* msg3 = "duplicated column names";

  // number of fields at least 1
  size_t numOfTags = taosArrayGetSize(pTagsList);
  if (numOfTags < 1) {
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  // field name must be unique
  for (int32_t i = 0; i < numOfTags; ++i) {
    SField* p = taosArrayGet(pTagsList, i);
    if (has(pFieldList, 0, p->name) == true) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }
  }

  return validateTableColumns(pFieldList, TSDB_MAX_TAGS_LEN, TSDB_MAX_TAGS, pMsgBuf);
}

int32_t doCheckForCreateTable(SSqlInfo* pInfo, SMsgBuf* pMsgBuf) {
  const char* msg1 = "invalid table name";

  SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;

  SArray* pFieldList = pCreateTable->colInfo.pColumns;
  SArray* pTagList = pCreateTable->colInfo.pTagColumns;
  assert(pFieldList != NULL);

  // if sql specifies db, use it, otherwise use default db
  SToken* pzTableName = &(pCreateTable->name);

  if (parserValidateNameToken(pzTableName) != TSDB_CODE_SUCCESS) {
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  if (validateTableColumnInfo(pFieldList, pMsgBuf) != TSDB_CODE_SUCCESS ||
      (pTagList != NULL && validateTagParams(pTagList, pFieldList, pMsgBuf) != TSDB_CODE_SUCCESS)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doCheckForCreateCTable(SSqlInfo* pInfo, SParseBasicCtx *pCtx, SMsgBuf* pMsgBuf) {
  const char* msg1 = "invalid table name";
  const char* msg2 = "tags number not matched";
  const char* msg3 = "tag value too long";
  const char* msg4 = "illegal value or data overflow";

  SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;

  // super table name, create table by using dst
  int32_t numOfTables = (int32_t) taosArrayGetSize(pCreateTable->childTableInfo);
  for(int32_t j = 0; j < numOfTables; ++j) {
    SCreatedTableInfo* pCreateTableInfo = taosArrayGet(pCreateTable->childTableInfo, j);

    SToken* pSTableNameToken = &pCreateTableInfo->stbName;

    char buf[TSDB_TABLE_FNAME_LEN];
    SToken sTblToken;
    sTblToken.z = buf;

    int32_t code = parserValidateNameToken(pSTableNameToken);
    if (code != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    SName name = {0};
    code = createSName(&name, pSTableNameToken, pCtx, pMsgBuf);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    const char* pStableName = tNameGetTableName(&name);
    SArray* pValList = pCreateTableInfo->pTagVals;

    size_t valSize = taosArrayGetSize(pValList);
    STableMeta* pSuperTableMeta = NULL;

    char dbName[TSDB_DB_FNAME_LEN] = {0};
    tNameGetFullDbName(&name, dbName);

    catalogGetTableMeta(pCtx->pCatalog, pCtx->pTransporter, &pCtx->mgmtEpSet, dbName, pStableName, &pSuperTableMeta);

    // too long tag values will return invalid sql, not be truncated automatically
    SSchema  *pTagSchema = getTableTagSchema(pSuperTableMeta);
    STableComInfo tinfo = getTableInfo(pSuperTableMeta);
    STagData *pTag = &pCreateTableInfo->tagdata;

    SKVRowBuilder kvRowBuilder = {0};
    if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    SArray* pNameList = NULL;
    size_t  nameSize = 0;
    int32_t schemaSize = getNumOfTags(pSuperTableMeta);

    if (pCreateTableInfo->pTagNames) {
      pNameList = pCreateTableInfo->pTagNames;
      nameSize = taosArrayGetSize(pNameList);

      if (valSize != nameSize || schemaSize < valSize) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      bool findColumnIndex = false;
      for (int32_t i = 0; i < nameSize; ++i) {
        SToken* sToken = taosArrayGet(pNameList, i);

        char tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0}; // create tmp buf to avoid alter orginal sqlstr
        strncpy(tmpTokenBuf, sToken->z, sToken->n);
        sToken->z = tmpTokenBuf;

//        if (TK_STRING == sToken->type) {
//          tscDequoteAndTrimToken(sToken);
//        }

//        if (TK_ID == sToken->type) {
//          tscRmEscapeAndTrimToken(sToken);
//        }

        SListItem* pItem = taosArrayGet(pValList, i);

        findColumnIndex = false;

        // todo speedup by using hash list
        for (int32_t t = 0; t < schemaSize; ++t) {
          if (strncmp(sToken->z, pTagSchema[t].name, sToken->n) == 0 && strlen(pTagSchema[t].name) == sToken->n) {
            SSchema*          pSchema = &pTagSchema[t];

            char tagVal[TSDB_MAX_TAGS_LEN] = {0};
            if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
              if (pItem->pVar.nLen > pSchema->bytes) {
                tdDestroyKVRowBuilder(&kvRowBuilder);
                return buildInvalidOperationMsg(pMsgBuf, msg3);
              }
            } else if (pSchema->type == TSDB_DATA_TYPE_TIMESTAMP) {
              if (pItem->pVar.nType == TSDB_DATA_TYPE_BINARY) {
//                code = convertTimestampStrToInt64(&(pItem->pVar), tinfo.precision);
//                if (code != TSDB_CODE_SUCCESS) {
//                  return buildInvalidOperationMsg(pMsgBuf, msg4);
//                }
              } else if (pItem->pVar.nType == TSDB_DATA_TYPE_TIMESTAMP) {
                pItem->pVar.i = convertTimePrecision(pItem->pVar.i, TSDB_TIME_PRECISION_NANO, tinfo.precision);
              }
            }

            code = taosVariantDump(&(pItem->pVar), tagVal, pSchema->type, true);

            // check again after the convert since it may be converted from binary to nchar.
            if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
              int16_t len = varDataTLen(tagVal);
              if (len > pSchema->bytes) {
                tdDestroyKVRowBuilder(&kvRowBuilder);
                return buildInvalidOperationMsg(pMsgBuf, msg3);
              }
            }

            if (code != TSDB_CODE_SUCCESS) {
              tdDestroyKVRowBuilder(&kvRowBuilder);
              return buildInvalidOperationMsg(pMsgBuf, msg4);
            }

            tdAddColToKVRow(&kvRowBuilder, pSchema->colId, pSchema->type, tagVal);

            findColumnIndex = true;
            break;
          }
        }

        if (!findColumnIndex) {
          tdDestroyKVRowBuilder(&kvRowBuilder);
//          return buildInvalidOperationMsg(pMsgBuf, "invalid tag name", sToken->z);
        }
      }
    } else {
      if (schemaSize != valSize) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      for (int32_t i = 0; i < valSize; ++i) {
        SSchema   *pSchema = &pTagSchema[i];
        SListItem *pItem = taosArrayGet(pValList, i);

        char tagVal[TSDB_MAX_TAGS_LEN];
        if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
          if (pItem->pVar.nLen > pSchema->bytes) {
            tdDestroyKVRowBuilder(&kvRowBuilder);
            return buildInvalidOperationMsg(pMsgBuf, msg3);
          }
        } else if (pSchema->type == TSDB_DATA_TYPE_TIMESTAMP) {
          if (pItem->pVar.nType == TSDB_DATA_TYPE_BINARY) {
//            code = convertTimestampStrToInt64(&(pItem->pVar), tinfo.precision);
            if (code != TSDB_CODE_SUCCESS) {
              return buildInvalidOperationMsg(pMsgBuf, msg4);
            }
          } else if (pItem->pVar.nType == TSDB_DATA_TYPE_TIMESTAMP) {
            pItem->pVar.i = convertTimePrecision(pItem->pVar.i, TSDB_TIME_PRECISION_NANO, tinfo.precision);
          }
        }

        code = taosVariantDump(&(pItem->pVar), tagVal, pSchema->type, true);

        // check again after the convert since it may be converted from binary to nchar.
        if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
          int16_t len = varDataTLen(tagVal);
          if (len > pSchema->bytes) {
            tdDestroyKVRowBuilder(&kvRowBuilder);
            return buildInvalidOperationMsg(pMsgBuf, msg3);
          }
        }

        if (code != TSDB_CODE_SUCCESS) {
          tdDestroyKVRowBuilder(&kvRowBuilder);
          return buildInvalidOperationMsg(pMsgBuf, msg4);
        }

        tdAddColToKVRow(&kvRowBuilder, pSchema->colId, pSchema->type, tagVal);
      }
    }

    SKVRow row = tdGetKVRowFromBuilder(&kvRowBuilder);
    tdDestroyKVRowBuilder(&kvRowBuilder);
    if (row == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    tdSortKVRowByColIdx(row);
    pTag->dataLen = kvRowLen(row);

    if (pTag->data == NULL) {
      pTag->data = malloc(pTag->dataLen);
    }

    kvRowCpy(pTag->data, row);
    free(row);

    bool dbIncluded2 = false;
    // table name
//    if (tscValidateName(&(pCreateTableInfo->name), true, &dbIncluded2) != TSDB_CODE_SUCCESS) {
//      return buildInvalidOperationMsg(pMsgBuf, msg1);
//    }

//    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, TABLE_INDEX);
//    code = tscSetTableFullName(&pTableMetaInfo->name, &pCreateTableInfo->name, pSql, dbIncluded2);
//    if (code != TSDB_CODE_SUCCESS) {
//      return code;
//    }

//    pCreateTableInfo->fullname = calloc(1, tNameLen(&pTableMetaInfo->name) + 1);
//    code = tNameExtractFullName(&pTableMetaInfo->name, pCreateTableInfo->fullname);
//    if (code != TSDB_CODE_SUCCESS) {
//      return buildInvalidOperationMsg(pMsgBuf, msg1);
//    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qParserValidateDclSqlNode(SSqlInfo* pInfo, SParseBasicCtx* pCtx, SDclStmtInfo* pDcl, char* msgBuf, int32_t msgBufLen) {
  int32_t code = 0;

  SMsgBuf m = {.buf = msgBuf, .len = msgBufLen};
  SMsgBuf *pMsgBuf = &m;

  switch (pInfo->type) {
    case TSDB_SQL_CREATE_USER:
    case TSDB_SQL_ALTER_USER: {
      const char* msg1 = "not support options";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "name too long";
      const char* msg4 = "invalid user rights";

      SUserInfo* pUser = &pInfo->pMiscInfo->user;
      SToken*    pName = &pUser->user;
      SToken*    pPwd = &pUser->passwd;

      if (pName->n >= TSDB_USER_LEN) {
        return buildInvalidOperationMsg(pMsgBuf, msg3);
      }

      if (parserValidateIdToken(pName) != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      if (pInfo->type == TSDB_SQL_CREATE_USER) {
        if (parserValidatePassword(pPwd, pMsgBuf) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      } else {
        if (pUser->type == TSDB_ALTER_USER_PASSWD) {
          if (parserValidatePassword(pPwd, pMsgBuf) != TSDB_CODE_SUCCESS) {
            return TSDB_CODE_TSC_INVALID_OPERATION;
          }
        } else if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
          assert(pPwd->type == TSDB_DATA_TYPE_NULL);

          SToken* pPrivilege = &pUser->privilege;
          if (strncasecmp(pPrivilege->z, "super", 5) == 0 && pPrivilege->n == 5) {
            //            pCmd->count = 1;
          } else if (strncasecmp(pPrivilege->z, "normal", 4) == 0 && pPrivilege->n == 4) {
            //            pCmd->count = 2;
          } else {
            return buildInvalidOperationMsg(pMsgBuf, msg4);
          }
        } else {
          return buildInvalidOperationMsg(pMsgBuf, msg1);
        }
      }

      pDcl->pMsg = (char*)buildUserManipulationMsg(pInfo, &pDcl->msgLen, pCtx->requestId, msgBuf, msgBufLen);
      pDcl->msgType = (pInfo->type == TSDB_SQL_CREATE_USER)? TDMT_MND_CREATE_USER:TDMT_MND_ALTER_USER;
      break;
    }

    case TSDB_SQL_CREATE_ACCT:
    case TSDB_SQL_ALTER_ACCT: {
      const char* msg1 = "invalid state option, available options[no, r, w, all]";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "name too long";

      SToken* pName = &pInfo->pMiscInfo->user.user;
      SToken* pPwd = &pInfo->pMiscInfo->user.passwd;

      if (parserValidatePassword(pPwd, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (pName->n >= TSDB_USER_LEN) {
        return buildInvalidOperationMsg(pMsgBuf, msg3);
      }

      if (parserValidateNameToken(pName) != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      SCreateAcctInfo* pAcctOpt = &pInfo->pMiscInfo->acctOpt;
      if (pAcctOpt->stat.n > 0) {
        if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
        } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
        } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
        } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
        } else {
          return buildInvalidOperationMsg(pMsgBuf, msg1);
        }
      }

      pDcl->pMsg = (char*)buildAcctManipulationMsg(pInfo, &pDcl->msgLen, pCtx->requestId, msgBuf, msgBufLen);
      pDcl->msgType = (pInfo->type == TSDB_SQL_CREATE_ACCT)? TDMT_MND_CREATE_ACCT:TDMT_MND_ALTER_ACCT;
      break;
    }

    case TSDB_SQL_DROP_ACCT:
    case TSDB_SQL_DROP_USER: {
      pDcl->pMsg = (char*)buildDropUserMsg(pInfo, &pDcl->msgLen, pCtx->requestId, msgBuf, msgBufLen);
      pDcl->msgType = (pInfo->type == TSDB_SQL_DROP_ACCT)? TDMT_MND_DROP_ACCT:TDMT_MND_DROP_USER;
      break;
    }

    case TSDB_SQL_SHOW: {
      code = setShowInfo(&pInfo->pMiscInfo->showOpt, pCtx, (void**)&pDcl->pMsg, &pDcl->msgLen, pMsgBuf);
      pDcl->msgType = TDMT_MND_SHOW;
      break;
    }

    case TSDB_SQL_USE_DB: {
      const char* msg = "invalid db name";

      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (parserValidateNameToken(pToken) != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg);
      }

      SName n = {0};
      int32_t ret = tNameSetDbName(&n, pCtx->acctId, pToken->z, pToken->n);
      if (ret != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg);
      }

      SUseDbMsg *pUseDbMsg = (SUseDbMsg *) calloc(1, sizeof(SUseDbMsg));
      tNameExtractFullName(&n, pUseDbMsg->db);

      pDcl->pMsg = (char*)pUseDbMsg;
      pDcl->msgLen = sizeof(SUseDbMsg);
      pDcl->msgType = TDMT_MND_USE_DB;
      break;
    }

    case TSDB_SQL_ALTER_DB:
    case TSDB_SQL_CREATE_DB: {
      const char* msg1 = "invalid db name";
      const char* msg2 = "name too long";

      SCreateDbInfo* pCreateDB = &(pInfo->pMiscInfo->dbOpt);
      if (pCreateDB->dbname.n >= TSDB_DB_NAME_LEN) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      char buf[TSDB_DB_NAME_LEN] = {0};
      SToken token = taosTokenDup(&pCreateDB->dbname, buf, tListLen(buf));

      if (parserValidateNameToken(&token) != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg1);
      }

      SCreateDbMsg* pCreateMsg = buildCreateDbMsg(pCreateDB, pCtx, pMsgBuf);
      if (doCheckDbOptions(pCreateMsg, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      pDcl->pMsg = (char*)pCreateMsg;
      pDcl->msgLen = sizeof(SCreateDbMsg);
      pDcl->msgType = (pInfo->type == TSDB_SQL_CREATE_DB)? TDMT_MND_CREATE_DB:TDMT_MND_ALTER_DB;
      break;
    }

    case TSDB_SQL_DROP_DB: {
      const char* msg1 = "invalid database name";

      assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
      SToken* dbName = taosArrayGet(pInfo->pMiscInfo->a, 0);

      SName name = {0};
      code = tNameSetDbName(&name, pCtx->acctId, dbName->z, dbName->n);
      if (code != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg1);
      }

      SDropDbMsg *pDropDbMsg = (SDropDbMsg*) calloc(1, sizeof(SDropDbMsg));

      code = tNameExtractFullName(&name, pDropDbMsg->db);
      pDropDbMsg->ignoreNotExists = pInfo->pMiscInfo->existsCheck ? 1 : 0;
      assert(code == TSDB_CODE_SUCCESS && name.type == TSDB_DB_NAME_T);

      pDcl->msgType = TDMT_MND_DROP_DB;
      pDcl->msgLen = sizeof(SDropDbMsg);
      pDcl->pMsg = (char*)pDropDbMsg;
      return TSDB_CODE_SUCCESS;
    }

    case TSDB_SQL_CREATE_TABLE: {
      SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;

      if (pCreateTable->type == TSQL_CREATE_TABLE || pCreateTable->type == TSQL_CREATE_STABLE) {
        if ((code = doCheckForCreateTable(pInfo, pMsgBuf)) != TSDB_CODE_SUCCESS) {
          return code;
        }
        pDcl->pMsg = (char*)buildCreateTableMsg(pCreateTable, &pDcl->msgLen, pCtx, pMsgBuf);
        pDcl->msgType = (pCreateTable->type == TSQL_CREATE_TABLE)? TDMT_VND_CREATE_TABLE:TDMT_MND_CREATE_STB;
      }  else if (pCreateTable->type == TSQL_CREATE_CTABLE) {
        if ((code = doCheckForCreateCTable(pInfo, pCtx, pMsgBuf)) != TSDB_CODE_SUCCESS) {
          return code;
        }

      } else if (pCreateTable->type == TSQL_CREATE_STREAM) {
        //        if ((code = doCheckForStream(pSql, pInfo)) != TSDB_CODE_SUCCESS) {
        //          return code;
      }

      break;
    }

    case TSDB_SQL_DROP_TABLE: {
      pDcl->pMsg = (char*)buildDropStableMsg(pInfo, &pDcl->msgLen, pCtx, pMsgBuf);
      if (pDcl->pMsg == NULL) {
        code = terrno;
      }

      pDcl->msgType = TDMT_MND_DROP_STB;
      break;
    }

    case TSDB_SQL_CREATE_DNODE: {
      pDcl->pMsg = (char*) buildCreateDnodeMsg(pInfo, &pDcl->msgLen, pMsgBuf);
      if (pDcl->pMsg == NULL) {
        code = terrno;
      }

      pDcl->msgType = TDMT_MND_CREATE_DNODE;
      break;
    }

    case TSDB_SQL_DROP_DNODE: {
      pDcl->pMsg = (char*) buildDropDnodeMsg(pInfo, &pDcl->msgLen, pMsgBuf);
      if (pDcl->pMsg == NULL) {
        code = terrno;
      }

      pDcl->msgType = TDMT_MND_DROP_DNODE;
      break;
    }

    default:
      break;
  }

  return code;
}

