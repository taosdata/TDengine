#include "tmsg.h"
#include "tglobal.h"
#include "parserInt.h"
#include "ttime.h"
#include "astToMsg.h"
#include "astGenerator.h"
#include "parserUtil.h"
#include "queryInfoUtil.h"

/* is contained in pFieldList or not */
static bool has(SArray* pFieldList, int32_t startIndex, const char* name) {
  size_t numOfCols = taosArrayGetSize(pFieldList);
  for (int32_t j = startIndex; j < numOfCols; ++j) {
    TAOS_FIELD* field = taosArrayGet(pFieldList, j);
    if (strncasecmp(name, field->name, sizeof(field->name) - 1) == 0) return true;
  }

  return false;
}

static int32_t setShowInfo(SShowInfo* pShowInfo, SParseContext* pCtx, void** output, int32_t* outputLen,
                           SEpSet* pEpSet, void** pExtension, SMsgBuf* pMsgBuf) {
  const char* msg1 = "invalid name";
  const char* msg2 = "wildcard string should be less than %d characters";
  const char* msg3 = "database name too long";
  const char* msg4 = "pattern is invalid";
  const char* msg5 = "database name is empty";
  const char* msg6 = "pattern string is empty";
  const char* msg7 = "database not specified";
  /*
   * database prefix in pInfo->pMiscInfo->a[0]
   * wildcard in like clause in pInfo->pMiscInfo->a[1]
   */
  int16_t showType = pShowInfo->showType;
  if (showType == TSDB_MGMT_TABLE_TABLE) {
    SArray* array = NULL;
    SName   name = {0};

    if (pCtx->db == NULL && pShowInfo->prefix.n == 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg7);
    }

    SVShowTablesReq* pShowReq = calloc(1, sizeof(SVShowTablesReq));
    if (pShowInfo->prefix.n > 0) {
      tNameSetDbName(&name, pCtx->acctId, pShowInfo->prefix.z, pShowInfo->prefix.n);
    } else {
      tNameSetDbName(&name, pCtx->acctId, pCtx->db, strlen(pCtx->db));
    }

    char dbFname[TSDB_DB_FNAME_LEN] = {0};
    tNameGetFullDbName(&name, dbFname);

    int32_t code = catalogGetDBVgroup(pCtx->pCatalog, pCtx->pTransporter, &pCtx->mgmtEpSet, dbFname, false, &array);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return code;
    }

    SVgroupInfo* info = taosArrayGet(array, 0);
    pShowReq->head.vgId = htonl(info->vgId);
    *pEpSet = info->epset;

    *outputLen  = sizeof(SVShowTablesReq);
    *output     = pShowReq;
    *pExtension = array;
  } else {
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

    *pEpSet = pCtx->mgmtEpSet;
    *output = buildShowMsg(pShowInfo, pCtx, pMsgBuf);
    if (*output == NULL) {
      return terrno;
    }

    *outputLen = sizeof(SShowReq) /* + htons(pShowMsg->payloadLen)*/;
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
    snprintf(msg, tListLen(msg), "invalid db option daysPerFile: %d valid range: [%d, %d]", val, TSDB_MIN_DAYS_PER_FILE,
             TSDB_MAX_DAYS_PER_FILE);
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
    snprintf(msg, tListLen(msg), "invalid db option commitTime: %d valid range: [%d, %d]", val, TSDB_MIN_COMMIT_TIME,
             TSDB_MAX_COMMIT_TIME);
    return buildInvalidOperationMsg(pMsgBuf, msg);
  }

  val = htonl(pCreate->fsyncPeriod);
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

  val = htonl(pCreate->numOfVgroups);
  if (val < TSDB_MIN_VNODES_PER_DB || val > TSDB_MAX_VNODES_PER_DB) {
    snprintf(msg, tListLen(msg), "invalid number of vgroups for DB:%d valid range: [%d, %d]", val,
             TSDB_MIN_VNODES_PER_DB, TSDB_MAX_VNODES_PER_DB);
  }

  val = htonl(pCreate->maxRows);
  if (val < TSDB_MIN_MAX_ROW_FBLOCK || val > TSDB_MAX_MAX_ROW_FBLOCK) {
    snprintf(msg, tListLen(msg), "invalid number of max rows in file block for DB:%d valid range: [%d, %d]", val,
             TSDB_MIN_MAX_ROW_FBLOCK, TSDB_MAX_MAX_ROW_FBLOCK);
  }

  val = htonl(pCreate->minRows);
  if (val < TSDB_MIN_MIN_ROW_FBLOCK || val > TSDB_MAX_MIN_ROW_FBLOCK) {
    snprintf(msg, tListLen(msg), "invalid number of min rows in file block for DB:%d valid range: [%d, %d]", val,
             TSDB_MIN_MIN_ROW_FBLOCK, TSDB_MAX_MIN_ROW_FBLOCK);
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
  assert(pFieldList != NULL && pMsgBuf != NULL);

  const char* msg1 = "first column must be timestamp";
  const char* msg2 = "illegal number of columns";

  // first column must be timestamp
  SField* pField = taosArrayGet(pFieldList, 0);
  if (pField->type != TSDB_DATA_TYPE_TIMESTAMP) {
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  // number of fields no less than 2
  size_t numOfCols = taosArrayGetSize(pFieldList);
  if (numOfCols <= 1) {
    return buildInvalidOperationMsg(pMsgBuf, msg2);
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

int32_t doCheckForCreateTable(SCreateTableSql* pCreateTable, SMsgBuf* pMsgBuf) {
  const char* msg1 = "invalid table name";

  SArray* pFieldList = pCreateTable->colInfo.pColumns;
  SArray* pTagList = pCreateTable->colInfo.pTagColumns;
  assert(pFieldList != NULL);

  // if sql specifies db, use it, otherwise use default db
  SToken* pNameToken = &(pCreateTable->name);

  if (parserValidateIdToken(pNameToken) != TSDB_CODE_SUCCESS) {
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  if (validateTableColumnInfo(pFieldList, pMsgBuf) != TSDB_CODE_SUCCESS ||
      (pTagList != NULL && validateTagParams(pTagList, pFieldList, pMsgBuf) != TSDB_CODE_SUCCESS)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SVgroupTablesBatch {
  SVCreateTbBatchReq req;
  SVgroupInfo        info;
} SVgroupTablesBatch;

static SArray* doSerializeVgroupCreateTableInfo(SHashObj* pVgroupHashmap);

static int32_t doParseSerializeTagValue(SSchema* pTagSchema, int32_t numOfInputTag, SKVRowBuilder* pKvRowBuilder,
                                        SArray* pTagValList, int32_t tsPrecision, SMsgBuf* pMsgBuf) {
  const char* msg1 = "illegal value or data overflow";
  int32_t     code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < numOfInputTag; ++i) {
    SSchema* pSchema = &pTagSchema[i];

    char* endPtr = NULL;
    char  tmpTokenBuf[TSDB_MAX_TAGS_LEN] = {0};
    SKvParam param = {.builder = pKvRowBuilder, .schema = pSchema};

    SToken* pItem = taosArrayGet(pTagValList, i);
    code = parseValueToken(&endPtr, pItem, pSchema, tsPrecision, tmpTokenBuf, KvRowAppend, &param, pMsgBuf);

    if (code != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  }

  return code;
}

static void addCreateTbReqIntoVgroup(SHashObj* pVgroupHashmap, const SName* pTableName, SKVRow row, uint64_t suid, SVgroupInfo* pVgInfo) {
  struct SVCreateTbReq req = {0};
  req.type        = TD_CHILD_TABLE;
  req.name        = strdup(tNameGetTableName(pTableName));
  req.ctbCfg.suid = suid;
  req.ctbCfg.pTag = row;

  SVgroupTablesBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  if (pTableBatch == NULL) {
    SVgroupTablesBatch tBatch = {0};
    tBatch.info = *pVgInfo;

    tBatch.req.pArray = taosArrayInit(4, sizeof(struct SVCreateTbReq));
    taosArrayPush(tBatch.req.pArray, &req);

    taosHashPut(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &tBatch, sizeof(tBatch));
  } else {  // add to the correct vgroup
    assert(pVgInfo->vgId == pTableBatch->info.vgId);
    taosArrayPush(pTableBatch->req.pArray, &req);
  }
}

static void destroyCreateTbReqBatch(SVgroupTablesBatch* pTbBatch) {
  size_t size = taosArrayGetSize(pTbBatch->req.pArray);
  for(int32_t i = 0; i < size; ++i) {
    SVCreateTbReq* pTableReq = taosArrayGet(pTbBatch->req.pArray, i);
    tfree(pTableReq->name);

    if (pTableReq->type == TSDB_NORMAL_TABLE) {
      tfree(pTableReq->ntbCfg.pSchema);
    } else if (pTableReq->type == TSDB_CHILD_TABLE) {
      tfree(pTableReq->ctbCfg.pTag);
    } else {
      assert(0);
    }
  }

  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t doCheckAndBuildCreateCTableReq(SCreateTableSql* pCreateTable, SParseContext* pCtx, SMsgBuf* pMsgBuf, SArray** pBufArray) {
  const char* msg1 = "invalid table name";
  const char* msg2 = "tags number not matched";
  const char* msg3 = "tag value too long";
  const char* msg4 = "illegal value or data overflow";

  int32_t code = 0;
  STableMeta* pSuperTableMeta = NULL;

  SHashObj* pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  // super table name, create table by using dst
  size_t numOfTables = taosArrayGetSize(pCreateTable->childTableInfo);
  for (int32_t j = 0; j < numOfTables; ++j) {
    SCreatedTableInfo* pCreateTableInfo = taosArrayGet(pCreateTable->childTableInfo, j);

    SToken* pSTableNameToken = &pCreateTableInfo->stbName;
    code = parserValidateNameToken(pSTableNameToken);
    if (code != TSDB_CODE_SUCCESS) {
      code = buildInvalidOperationMsg(pMsgBuf, msg1);
      goto _error;
    }

    SName name = {0};
    code = createSName(&name, pSTableNameToken, pCtx, pMsgBuf);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    SKVRowBuilder kvRowBuilder = {0};
    if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }

    SArray* pValList = pCreateTableInfo->pTagVals;
    size_t  numOfInputTag = taosArrayGetSize(pValList);

    code = catalogGetTableMeta(pCtx->pCatalog, pCtx->pTransporter, &pCtx->mgmtEpSet, &name, &pSuperTableMeta);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    assert(pSuperTableMeta != NULL);

    // too long tag values will return invalid sql, not be truncated automatically
    SSchema*      pTagSchema = getTableTagSchema(pSuperTableMeta);
    STableComInfo tinfo = getTableInfo(pSuperTableMeta);

    SArray* pNameList = NULL;
    size_t  numOfBoundTags = 0;
    int32_t schemaSize = getNumOfTags(pSuperTableMeta);

    if (pCreateTableInfo->pTagNames) {
      pNameList = pCreateTableInfo->pTagNames;
      numOfBoundTags = taosArrayGetSize(pNameList);

      if (numOfInputTag != numOfBoundTags || schemaSize < numOfInputTag) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        code = buildInvalidOperationMsg(pMsgBuf, msg2);
        goto _error;
      }

      bool findColumnIndex = false;
      for (int32_t i = 0; i < numOfBoundTags; ++i) {
        SToken* sToken = taosArrayGet(pNameList, i);

        char tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // create tmp buf to avoid alter orginal sqlstr
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
            SSchema* pSchema = &pTagSchema[t];

            char tagVal[TSDB_MAX_TAGS_LEN] = {0};
            if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
              if (pItem->pVar.nLen > pSchema->bytes) {
                tdDestroyKVRowBuilder(&kvRowBuilder);
                code = buildInvalidOperationMsg(pMsgBuf, msg3);
                goto _error;
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
            if (IS_VAR_DATA_TYPE(pSchema->type)) {
              int16_t len = varDataTLen(tagVal);
              if (len > pSchema->bytes) {
                tdDestroyKVRowBuilder(&kvRowBuilder);
                code = buildInvalidOperationMsg(pMsgBuf, msg3);
                goto _error;
              }
            }

            if (code != TSDB_CODE_SUCCESS) {
              tdDestroyKVRowBuilder(&kvRowBuilder);
              code = buildInvalidOperationMsg(pMsgBuf, msg4);
              goto _error;
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
      if (schemaSize != numOfInputTag) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        code = buildInvalidOperationMsg(pMsgBuf, msg2);
        goto _error;
      }

      code = doParseSerializeTagValue(pTagSchema, numOfInputTag, &kvRowBuilder, pValList, tinfo.precision, pMsgBuf);
      if (code != TSDB_CODE_SUCCESS) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        goto _error;
      }
    }

    SKVRow row = tdGetKVRowFromBuilder(&kvRowBuilder);
    tdDestroyKVRowBuilder(&kvRowBuilder);
    if (row == NULL) {
      code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto _error;
    }

    tdSortKVRowByColIdx(row);

    SName tableName = {0};
    code = createSName(&tableName, &pCreateTableInfo->name, pCtx, pMsgBuf);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    // Find a appropriate vgroup to accommodate this table , according to the table name
    SVgroupInfo info = {0};
    code = catalogGetTableHashVgroup(pCtx->pCatalog, pCtx->pTransporter, &pCtx->mgmtEpSet, &tableName, &info);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    addCreateTbReqIntoVgroup(pVgroupHashmap, &tableName, row, pSuperTableMeta->uid, &info);
    tfree(pSuperTableMeta);
  }

  *pBufArray = doSerializeVgroupCreateTableInfo(pVgroupHashmap);
  if (*pBufArray == NULL) {
    code = terrno;
    goto _error;
  }

  taosHashCleanup(pVgroupHashmap);
  return TSDB_CODE_SUCCESS;

  _error:
  taosHashCleanup(pVgroupHashmap);
  tfree(pSuperTableMeta);
  terrno = code;
  return code;
}

static int32_t serializeVgroupTablesBatchImpl(SVgroupTablesBatch* pTbBatch, SArray* pBufArray) {
  int   tlen = sizeof(SMsgHead) + tSVCreateTbBatchReqSerialize(NULL, &(pTbBatch->req));
  void* buf = malloc(tlen);
  if (buf == NULL) {
    // TODO: handle error
  }

  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);

  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tSVCreateTbBatchReqSerialize(&pBuf, &(pTbBatch->req));

  SVgDataBlocks* pVgData = calloc(1, sizeof(SVgDataBlocks));
  pVgData->vg    = pTbBatch->info;
  pVgData->pData = buf;
  pVgData->size  = tlen;
  pVgData->numOfTables = (int32_t) taosArrayGetSize(pTbBatch->req.pArray);

  taosArrayPush(pBufArray, &pVgData);
}

static int32_t doBuildSingleTableBatchReq(SName* pTableName, SArray* pColumns, SVgroupInfo* pVgroupInfo, SVgroupTablesBatch* pBatch) {
  struct SVCreateTbReq req = {0};
  req.type = TD_NORMAL_TABLE;
  req.name = strdup(tNameGetTableName(pTableName));

  req.ntbCfg.nCols = taosArrayGetSize(pColumns);
  int32_t num = req.ntbCfg.nCols;

  req.ntbCfg.pSchema = calloc(num, sizeof(SSchema));
  for(int32_t i = 0; i < num; ++i) {
    SSchema* pSchema = taosArrayGet(pColumns, i);
    memcpy(&req.ntbCfg.pSchema[i], pSchema, sizeof(SSchema));
  }

  pBatch->info = *pVgroupInfo;
  pBatch->req.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (pBatch->req.pArray == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  taosArrayPush(pBatch->req.pArray, &req);
  return TSDB_CODE_SUCCESS;
}

int32_t doCheckAndBuildCreateTableReq(SCreateTableSql* pCreateTable, SParseContext* pCtx, SMsgBuf* pMsgBuf, char** pOutput, int32_t* len) {
  SArray* pBufArray = NULL;
  int32_t code = 0;

  // it is a sql statement to create a normal table
  if (pCreateTable->childTableInfo == NULL) {
    assert(taosArrayGetSize(pCreateTable->colInfo.pColumns) > 0 && pCreateTable->colInfo.pTagColumns == NULL);
    code = doCheckForCreateTable(pCreateTable, pMsgBuf);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    SName tableName = {0};
    code = createSName(&tableName, &pCreateTable->name, pCtx, pMsgBuf);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    SVgroupInfo info = {0};
    catalogGetTableHashVgroup(pCtx->pCatalog, pCtx->pTransporter, &pCtx->mgmtEpSet, &tableName, &info);

    SVgroupTablesBatch tbatch = {0};
    code = doBuildSingleTableBatchReq(&tableName, pCreateTable->colInfo.pColumns, &info, &tbatch);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pBufArray = taosArrayInit(1, POINTER_BYTES);
    if (pBufArray == NULL) {
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }

    serializeVgroupTablesBatchImpl(&tbatch, pBufArray);
    destroyCreateTbReqBatch(&tbatch);
  } else { // it is a child table, created according to a super table
    code = doCheckAndBuildCreateCTableReq(pCreateTable, pCtx, pMsgBuf, &pBufArray);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  SVnodeModifOpStmtInfo* pStmtInfo = calloc(1, sizeof(SVnodeModifOpStmtInfo));
  pStmtInfo->nodeType    = TSDB_SQL_CREATE_TABLE;
  pStmtInfo->pDataBlocks = pBufArray;

  *pOutput = (char*) pStmtInfo;
  *len     = sizeof(SVnodeModifOpStmtInfo);

  return TSDB_CODE_SUCCESS;
}

SArray* doSerializeVgroupCreateTableInfo(SHashObj* pVgroupHashmap) {
  SArray* pBufArray = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(void*));

  SVgroupTablesBatch* pTbBatch = NULL;
  do {
    pTbBatch = taosHashIterate(pVgroupHashmap, pTbBatch);
    if (pTbBatch == NULL) {
      break;
    }

    /*int32_t code = */serializeVgroupTablesBatchImpl(pTbBatch, pBufArray);
    destroyCreateTbReqBatch(pTbBatch);
  } while (true);

  return pBufArray;
}

SDclStmtInfo* qParserValidateDclSqlNode(SSqlInfo* pInfo, SParseContext* pCtx, char* msgBuf, int32_t msgBufLen) {
  int32_t code = 0;

  SDclStmtInfo* pDcl = calloc(1, sizeof(SDclStmtInfo));

  SMsgBuf  m = {.buf = msgBuf, .len = msgBufLen};
  SMsgBuf* pMsgBuf = &m;

  pDcl->epSet = pCtx->mgmtEpSet;

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
        code = buildInvalidOperationMsg(pMsgBuf, msg3);
        goto _error;
      }

      if (parserValidateIdToken(pName) != TSDB_CODE_SUCCESS) {
        code = buildInvalidOperationMsg(pMsgBuf, msg2);
        goto _error;
      }

      if (pInfo->type == TSDB_SQL_CREATE_USER) {
        if (parserValidatePassword(pPwd, pMsgBuf) != TSDB_CODE_SUCCESS) {
          code = TSDB_CODE_TSC_INVALID_OPERATION;
          goto _error;
        }
      } else {
        if (pUser->type == TSDB_ALTER_USER_PASSWD) {
          if (parserValidatePassword(pPwd, pMsgBuf) != TSDB_CODE_SUCCESS) {
            code = TSDB_CODE_TSC_INVALID_OPERATION;
            goto _error;
          }
        } else if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
          assert(pPwd->type == TSDB_DATA_TYPE_NULL);

          SToken* pPrivilege = &pUser->privilege;
          if (strncasecmp(pPrivilege->z, "super", 5) == 0 && pPrivilege->n == 5) {
            //            pCmd->count = 1;
          } else if (strncasecmp(pPrivilege->z, "normal", 4) == 0 && pPrivilege->n == 4) {
            //            pCmd->count = 2;
          } else {
            code = buildInvalidOperationMsg(pMsgBuf, msg4);
            goto _error;
          }
        } else {
          code = buildInvalidOperationMsg(pMsgBuf, msg1);
          goto _error;
        }
      }

      pDcl->pMsg = (char*)buildUserManipulationMsg(pInfo, &pDcl->msgLen, pCtx->requestId, msgBuf, msgBufLen);
      pDcl->msgType = (pInfo->type == TSDB_SQL_CREATE_USER) ? TDMT_MND_CREATE_USER : TDMT_MND_ALTER_USER;
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
        code = TSDB_CODE_TSC_INVALID_OPERATION;
        goto _error;
      }

      if (pName->n >= TSDB_USER_LEN) {
        code = buildInvalidOperationMsg(pMsgBuf, msg3);
        goto _error;
      }

      if (parserValidateNameToken(pName) != TSDB_CODE_SUCCESS) {
        code = buildInvalidOperationMsg(pMsgBuf, msg2);
        goto _error;
      }

      SCreateAcctInfo* pAcctOpt = &pInfo->pMiscInfo->acctOpt;
      if (pAcctOpt->stat.n > 0) {
        if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
        } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
        } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
        } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
        } else {
          code = buildInvalidOperationMsg(pMsgBuf, msg1);
          goto _error;
        }
      }

      pDcl->pMsg = (char*)buildAcctManipulationMsg(pInfo, &pDcl->msgLen, pCtx->requestId, msgBuf, msgBufLen);
      pDcl->msgType = (pInfo->type == TSDB_SQL_CREATE_ACCT) ? TDMT_MND_CREATE_ACCT : TDMT_MND_ALTER_ACCT;
      break;
    }

    case TSDB_SQL_DROP_ACCT:
    case TSDB_SQL_DROP_USER: {
      pDcl->pMsg = (char*)buildDropUserMsg(pInfo, &pDcl->msgLen, pCtx->requestId, msgBuf, msgBufLen);
      pDcl->msgType = (pInfo->type == TSDB_SQL_DROP_ACCT) ? TDMT_MND_DROP_ACCT : TDMT_MND_DROP_USER;
      break;
    }

    case TSDB_SQL_SHOW: {
      SShowInfo* pShowInfo = &pInfo->pMiscInfo->showOpt;
      code = setShowInfo(pShowInfo, pCtx, (void**)&pDcl->pMsg, &pDcl->msgLen, &pDcl->epSet, &pDcl->pExtension, pMsgBuf);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
      
      pDcl->msgType = (pShowInfo->showType == TSDB_MGMT_TABLE_TABLE) ? TDMT_VND_SHOW_TABLES : TDMT_MND_SHOW;
      break;
    }

    case TSDB_SQL_USE_DB: {
      const char* msg = "invalid db name";

      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (parserValidateNameToken(pToken) != TSDB_CODE_SUCCESS) {
        code = buildInvalidOperationMsg(pMsgBuf, msg);
        goto _error;
      }

      SName   n = {0};
      int32_t ret = tNameSetDbName(&n, pCtx->acctId, pToken->z, pToken->n);
      if (ret != TSDB_CODE_SUCCESS) {
        code = buildInvalidOperationMsg(pMsgBuf, msg);
        goto _error;
      }

      SUseDbReq* pUseDbMsg = (SUseDbReq*)calloc(1, sizeof(SUseDbReq));
      tNameExtractFullName(&n, pUseDbMsg->db);

      pDcl->pMsg = (char*)pUseDbMsg;
      pDcl->msgLen = sizeof(SUseDbReq);
      pDcl->msgType = TDMT_MND_USE_DB;
      break;
    }

    case TSDB_SQL_ALTER_DB:
    case TSDB_SQL_CREATE_DB: {
      const char* msg1 = "invalid db name";
      const char* msg2 = "name too long";

      SCreateDbInfo* pCreateDB = &(pInfo->pMiscInfo->dbOpt);
      if (pCreateDB->dbname.n >= TSDB_DB_NAME_LEN) {
        code = buildInvalidOperationMsg(pMsgBuf, msg2);
        goto _error;
      }

      char   buf[TSDB_DB_NAME_LEN] = {0};
      SToken token = taosTokenDup(&pCreateDB->dbname, buf, tListLen(buf));

      if (parserValidateNameToken(&token) != TSDB_CODE_SUCCESS) {
        code = buildInvalidOperationMsg(pMsgBuf, msg1);
        goto _error;
      }

      SCreateDbReq* pCreateMsg = buildCreateDbMsg(pCreateDB, pCtx, pMsgBuf);
      if (doCheckDbOptions(pCreateMsg, pMsgBuf) != TSDB_CODE_SUCCESS) {
        code = TSDB_CODE_TSC_INVALID_OPERATION;
        goto _error;
      }

      pDcl->pMsg = (char*)pCreateMsg;
      pDcl->msgLen = sizeof(SCreateDbReq);
      pDcl->msgType = (pInfo->type == TSDB_SQL_CREATE_DB) ? TDMT_MND_CREATE_DB : TDMT_MND_ALTER_DB;
      break;
    }

    case TSDB_SQL_DROP_DB: {
      const char* msg1 = "invalid database name";

      assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
      SToken* dbName = taosArrayGet(pInfo->pMiscInfo->a, 0);

      SName name = {0};
      code = tNameSetDbName(&name, pCtx->acctId, dbName->z, dbName->n);
      if (code != TSDB_CODE_SUCCESS) {
        code = buildInvalidOperationMsg(pMsgBuf, msg1);
        goto _error;
      }

      SDropDbReq* pDropDbMsg = (SDropDbReq*)calloc(1, sizeof(SDropDbReq));

      code = tNameExtractFullName(&name, pDropDbMsg->db);
      pDropDbMsg->ignoreNotExists = pInfo->pMiscInfo->existsCheck ? 1 : 0;
      assert(code == TSDB_CODE_SUCCESS && name.type == TSDB_DB_NAME_T);

      pDcl->msgType = TDMT_MND_DROP_DB;
      pDcl->msgLen = sizeof(SDropDbReq);
      pDcl->pMsg = (char*)pDropDbMsg;
      break;
    }

    case TSDB_SQL_CREATE_STABLE: {
      SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;
      if ((code = doCheckForCreateTable(pCreateTable, pMsgBuf)) != TSDB_CODE_SUCCESS) {
        terrno = code;
        goto _error;
      }

      pDcl->pMsg = (char*)buildCreateStbMsg(pCreateTable, &pDcl->msgLen, pCtx, pMsgBuf);
      pDcl->msgType = TDMT_MND_CREATE_STB;
      break;
    }

    case TSDB_SQL_DROP_TABLE: {
      pDcl->pMsg = (char*)buildDropStableMsg(pInfo, &pDcl->msgLen, pCtx, pMsgBuf);
      if (pDcl->pMsg == NULL) {
        goto _error;
      }

      pDcl->msgType = TDMT_MND_DROP_STB;
      break;
    }

    case TSDB_SQL_CREATE_DNODE: {
      pDcl->pMsg = (char*)buildCreateDnodeMsg(pInfo, &pDcl->msgLen, pMsgBuf);
      if (pDcl->pMsg == NULL) {
        goto _error;
      }

      pDcl->msgType = TDMT_MND_CREATE_DNODE;
      break;
    }

    case TSDB_SQL_DROP_DNODE: {
      pDcl->pMsg = (char*)buildDropDnodeMsg(pInfo, &pDcl->msgLen, pMsgBuf);
      if (pDcl->pMsg == NULL) {
        goto _error;
      }

      pDcl->msgType = TDMT_MND_DROP_DNODE;
      break;
    }

    default:
      break;
  }

  return pDcl;

  _error:
    terrno = code;
    tfree(pDcl);
    return NULL;
}

SVnodeModifOpStmtInfo* qParserValidateCreateTbSqlNode(SSqlInfo* pInfo, SParseContext* pCtx, char* msgBuf, int32_t msgBufLen) {
  SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;
  assert(pCreateTable->type == TSDB_SQL_CREATE_TABLE);

  SMsgBuf  m = {.buf = msgBuf, .len = msgBufLen};
  SMsgBuf* pMsgBuf = &m;

  SVnodeModifOpStmtInfo* pModifSqlStmt = NULL;

  int32_t msgLen = 0;
  int32_t code = doCheckAndBuildCreateTableReq(pCreateTable, pCtx, pMsgBuf, (char**) &pModifSqlStmt, &msgLen);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    tfree(pModifSqlStmt);
    return NULL;
  }

  return pModifSqlStmt;
}