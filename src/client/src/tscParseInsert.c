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

#define _DEFAULT_SOURCE /* See feature_test_macros(7) */
#define _GNU_SOURCE

#define _XOPEN_SOURCE

#pragma GCC diagnostic ignored "-Woverflow"
#pragma GCC diagnostic ignored "-Wunused-variable"

#include <stdio.h>
#include <stdlib.h>

#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <assert.h>
#include <math.h>
#include <string.h>
#include <time.h>
#include <wchar.h>

#include "ihash.h"
#include "os.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tsqldef.h"
#include "ttypes.h"

#include "tlog.h"
#include "tstoken.h"
#include "ttime.h"

#define INVALID_SQL_RET_MSG(p, ...) \
  do {                              \
    sprintf(p, __VA_ARGS__);        \
    return TSDB_CODE_INVALID_SQL;   \
  } while (0)

static void setErrMsg(char *msg, char *sql);
static int32_t tscAllocateMemIfNeed(STableDataBlocks *pDataBlock, int32_t rowSize);

// get formation
static int32_t getNumericType(const char *data) {
  if (*data == '-' || *data == '+') {
    data += 1;
  }

  if (data[0] == '0') {
    if (data[1] == 'x' || data[1] == 'X') {
      return TK_HEX;
    } else {
      return TK_OCT;
    }
  } else {
    return TK_INTEGER;
  }
}

static int64_t tscToInteger(char *data, char **endPtr) {
  int32_t numType = getNumericType(data);
  int32_t radix = 10;

  if (numType == TK_HEX) {
    radix = 16;
  } else if (numType == TK_OCT) {
    radix = 8;
  }

  return strtoll(data, endPtr, radix);
}

int tsParseTime(char *value, int32_t valuelen, int64_t *time, char **next, char *error, int16_t timePrec) {
  char *  token;
  int     tokenlen;
  int64_t interval;

  int64_t useconds = 0;

  char *pTokenEnd = *next;
  tscGetToken(pTokenEnd, &token, &tokenlen);
  if (tokenlen == 0 && strlen(value) == 0) {
    INVALID_SQL_RET_MSG(error, "missing time stamp");
  }

  if (strncmp(value, "now", 3) == 0 && valuelen == 3) {
    useconds = taosGetTimestamp(timePrec);
  } else if (strncmp(value, "0", 1) == 0 && valuelen == 1) {
    // do nothing
  } else if (value[4] != '-') {
    for (int32_t i = 0; i < valuelen; ++i) {
      /*
       * filter illegal input.
       * e.g., nw, tt, ff etc.
       */
      if (value[i] < '0' || value[i] > '9') {
        return TSDB_CODE_INVALID_SQL;
      }
    }
    useconds = str2int64(value);
  } else {
    // strptime("2001-11-12 18:31:01", "%Y-%m-%d %H:%M:%S", &tm);
    if (taosParseTime(value, time, valuelen, timePrec) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int k = valuelen; value[k] != '\0'; k++) {
    if (value[k] == ' ' || value[k] == '\t') continue;
    if (value[k] == ',') {
      *next = pTokenEnd;
      *time = useconds;
      return 0;
    }

    break;
  }

  /*
   * time expression:
   * e.g., now+12a, now-5h
   */
  pTokenEnd = tscGetToken(pTokenEnd, &token, &tokenlen);
  if (tokenlen && (*token == '+' || *token == '-')) {
    pTokenEnd = tscGetToken(pTokenEnd, &value, &valuelen);
    if (valuelen < 2) {
      strcpy(error, "value is expected");
      return TSDB_CODE_INVALID_SQL;
    }

    if (getTimestampInUsFromStr(value, valuelen, &interval) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }
    if (timePrec == TSDB_TIME_PRECISION_MILLI) {
      interval /= 1000;
    }

    if (*token == '+') {
      useconds += interval;
    } else {
      useconds = (useconds >= interval) ? useconds - interval : 0;
    }

    *next = pTokenEnd;
  }

  *time = useconds;
  return TSDB_CODE_SUCCESS;
}

int32_t tsParseOneColumnData(SSchema *pSchema, char *value, int valuelen, char *payload, char *msg, char **str,
                             bool primaryKey, int16_t timePrec) {
  int64_t temp;
  int32_t nullInt = *(int32_t *)TSDB_DATA_NULL_STR_L;
  char *  endptr = NULL;
  errno = 0;  // reset global error code

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {  // bool
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        *(uint8_t *)payload = TSDB_DATA_BOOL_NULL;
      } else {
        if (strncmp(value, "true", valuelen) == 0) {
          *(uint8_t *)payload = TSDB_TRUE;
        } else if (strncmp(value, "false", valuelen) == 0) {
          *(uint8_t *)payload = TSDB_FALSE;
        } else {
          int64_t v = strtoll(value, NULL, 10);
          *(uint8_t *)payload = (int8_t)((v == 0) ? TSDB_FALSE : TSDB_TRUE);
        }
      }
      break;
    }
    case TSDB_DATA_TYPE_TINYINT:
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        *((int32_t *)payload) = TSDB_DATA_TINYINT_NULL;
      } else {
        int64_t v = tscToInteger(value, &endptr);
        if (errno == ERANGE || v > INT8_MAX || v < INT8_MIN) {
          INVALID_SQL_RET_MSG(msg, "data is overflow");
        }

        int8_t v8 = (int8_t)v;
        if (isNull((char *)&v8, pSchema->type)) {
          INVALID_SQL_RET_MSG(msg, "data is overflow");
        }

        *((int8_t *)payload) = v8;
      }

      break;

    case TSDB_DATA_TYPE_SMALLINT:
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        *((int32_t *)payload) = TSDB_DATA_SMALLINT_NULL;
      } else {
        int64_t v = tscToInteger(value, &endptr);

        if (errno == ERANGE || v > INT16_MAX || v < INT16_MIN) {
          INVALID_SQL_RET_MSG(msg, "data is overflow");
        }

        int16_t v16 = (int16_t)v;
        if (isNull((char *)&v16, pSchema->type)) {
          INVALID_SQL_RET_MSG(msg, "data is overflow");
        }

        *((int16_t *)payload) = v16;
      }
      break;

    case TSDB_DATA_TYPE_INT:
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        *((int32_t *)payload) = TSDB_DATA_INT_NULL;
      } else {
        int64_t v = tscToInteger(value, &endptr);

        if (errno == ERANGE || v > INT32_MAX || v < INT32_MIN) {
          INVALID_SQL_RET_MSG(msg, "data is overflow");
        }

        int32_t v32 = (int32_t)v;
        if (isNull((char *)&v32, pSchema->type)) {
          INVALID_SQL_RET_MSG(msg, "data is overflow");
        }

        *((int32_t *)payload) = v32;
      }

      break;

    case TSDB_DATA_TYPE_BIGINT:
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        *((int64_t *)payload) = TSDB_DATA_BIGINT_NULL;
      } else {
        int64_t v = tscToInteger(value, &endptr);
        if (isNull((char *)&v, pSchema->type) || errno == ERANGE) {
          INVALID_SQL_RET_MSG(msg, "data is overflow");
        }
        *((int64_t *)payload) = v;
      }
      break;

    case TSDB_DATA_TYPE_FLOAT:
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        *((int32_t *)payload) = TSDB_DATA_FLOAT_NULL;
      } else {
        float v = (float)strtod(value, &endptr);
        if (isNull((char *)&v, pSchema->type) || isinf(v) || isnan(v)) {
          *((int32_t *)payload) = TSDB_DATA_FLOAT_NULL;
        } else {
          *((float *)payload) = v;
        }

        if (str != NULL) {
          // This if statement is just for Fanuc case, when a float point number is quoted by
          // quotes, we need to skip the quote. But this is temporary, it should be changed in the future.
          if (*endptr == '\'' || *endptr == '\"') endptr++;
          *str = endptr;
        }
      }
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        *((int64_t *)payload) = TSDB_DATA_DOUBLE_NULL;
      } else {
        double v = strtod(value, &endptr);
        if (isNull((char *)&v, pSchema->type) || isinf(v) || isnan(v)) {
          *((int32_t *)payload) = TSDB_DATA_FLOAT_NULL;
        } else {
          *((double *)payload) = v;
        }

        if (str != NULL) {
          // This if statement is just for Fanuc case, when a float point number is quoted by
          // quotes, we need to skip the quote. But this is temporary, it should be changed in the future.
          if (*endptr == '\'' || *endptr == '\"') endptr++;
          *str = endptr;
        }
      }
      break;

    case TSDB_DATA_TYPE_BINARY:
      /*
       * binary data cannot be null-terminated char string, otherwise the last char of the string is lost
       */
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        *payload = TSDB_DATA_BINARY_NULL;
      } else {
        /* truncate too long string */
        if (valuelen > pSchema->bytes) valuelen = pSchema->bytes;
        strncpy(payload, value, valuelen);
      }

      break;

    case TSDB_DATA_TYPE_NCHAR:
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        *(uint32_t *)payload = TSDB_DATA_NCHAR_NULL;
      } else {
        if (!taosMbsToUcs4(value, valuelen, payload, pSchema->bytes)) {
          sprintf(msg, "%s", strerror(errno));
          return TSDB_CODE_INVALID_SQL;
        }
      }
      break;

    case TSDB_DATA_TYPE_TIMESTAMP: {
      if (valuelen == 4 && nullInt == *(int32_t *)value) {
        if (primaryKey) {
          *((int64_t *)payload) = 0;
        } else {
          *((int64_t *)payload) = TSDB_DATA_BIGINT_NULL;
        }
      } else {
        if (tsParseTime(value, valuelen, &temp, str, msg, timePrec) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_INVALID_SQL;
        }
        *((int64_t *)payload) = temp;
      }

      break;
    }
  }

  return 0;
}

// todo merge the error msg function with tSQLParser
static void setErrMsg(char *msg, char *sql) {
  const char*   msgFormat = "near \"%s\" syntax error";
  const int32_t BACKWARD_CHAR_STEP = 15;

  // only extract part of sql string,avoid too long sql string cause stack over flow
  char buf[64] = {0};
  strncpy(buf, (sql - BACKWARD_CHAR_STEP), tListLen(buf) - 1);
  sprintf(msg, msgFormat, buf);
}

int tsParseOneRowData(char **str, STableDataBlocks *pDataBlocks, SSchema schema[], SParsedDataColInfo *spd, char *error,
                      int16_t timePrec) {
  char *value = NULL;
  int   valuelen = 0;

  char *payload = pDataBlocks->pData + pDataBlocks->size;

  /* 1. set the parsed value from sql string */
  int32_t rowSize = 0;
  for (int i = 0; i < spd->numOfAssignedCols; ++i) {
    /* the start position in data block buffer of current value in sql */
    char *  start = payload + spd->elems[i].offset;
    int16_t colIndex = spd->elems[i].colIndex;
    rowSize += schema[colIndex].bytes;

    int sign = 0;
  _again:
    *str = tscGetToken(*str, &value, &valuelen);
    if ((valuelen == 0 && value == NULL) || (valuelen == 1 && value[0] == ')')) {
      setErrMsg(error, *str);
      return -1;
    }

    /* support positive/negative integer/float data format */
    if ((*value == '+' || *value == '-') && valuelen == 1) {
      sign = 1;
      goto _again;
    }

    if (sign) {
      value = value - 1;
      /* backward to include the +/- symbol */
      valuelen++;
    }

    int32_t ret = tsParseOneColumnData(&schema[colIndex], value, valuelen, start, error, str,
                                       colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX, timePrec);
    if (ret != 0) {
      return -1;  // NOTE: here 0 mean error!
    }

    // once the data block is disordered, we do NOT keep previous timestamp any more
    if (colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX && pDataBlocks->ordered) {
      TSKEY k = *(TSKEY *)start;
      if (k <= pDataBlocks->prevTS) {
        pDataBlocks->ordered = false;
      }

      pDataBlocks->prevTS = k;
    }
  }

  /*2. set the null value for the rest columns */
  if (spd->numOfAssignedCols < spd->numOfCols) {
    char *ptr = payload;

    for (int32_t i = 0; i < spd->numOfCols; ++i) {
      if (!spd->hasVal[i]) {
        /* current column do not have any value to insert, set it to null */
        setNull(ptr, schema[i].type, schema[i].bytes);
      }

      ptr += schema[i].bytes;
    }

    rowSize = ptr - payload;
  }

  return rowSize;
}

static int32_t rowDataCompar(const void *lhs, const void *rhs) {
  TSKEY left = *(TSKEY *)lhs;
  TSKEY right = *(TSKEY *)rhs;

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

int tsParseValues(char **str, STableDataBlocks *pDataBlock, SMeterMeta *pMeterMeta, int maxRows,
                  SParsedDataColInfo *spd, char *error) {
  char *token;
  int   tokenlen;

  int16_t numOfRows = 0;

  SSchema *pSchema = tsGetSchema(pMeterMeta);
  int32_t  precision = pMeterMeta->precision;

  if (spd->hasVal[0] == false) {
    sprintf(error, "primary timestamp column can not be null");
    return -1;
  }

  while (1) {
    char *tmp = tscGetToken(*str, &token, &tokenlen);
    if (tokenlen == 0 || *token != '(') break;

    *str = tmp;
    if (numOfRows >= maxRows || pDataBlock->size + pMeterMeta->rowSize >= pDataBlock->nAllocSize) {
      maxRows += tscAllocateMemIfNeed(pDataBlock, pMeterMeta->rowSize);
    }

    int32_t len = tsParseOneRowData(str, pDataBlock, pSchema, spd, error, precision);
    if (len <= 0) {
      setErrMsg(error, *str);
      return -1;
    }

    pDataBlock->size += len;

    *str = tscGetToken(*str, &token, &tokenlen);
    if (tokenlen == 0 || *token != ')') {
      setErrMsg(error, *str);
      return -1;
    }

    numOfRows++;
  }

  if (numOfRows <= 0) {
    strcpy(error, "no any data points");
    return -1;
  } else {
    return numOfRows;
  }
}

void tscAppendDataBlock(SDataBlockList *pList, STableDataBlocks *pBlocks) {
  if (pList->nSize >= pList->nAlloc) {
    pList->nAlloc = pList->nAlloc << 1;
    pList->pData = realloc(pList->pData, sizeof(void *) * (size_t)pList->nAlloc);

    // reset allocated memory
    memset(pList->pData + pList->nSize, 0, sizeof(void *) * (pList->nAlloc - pList->nSize));
  }

  pList->pData[pList->nSize++] = pBlocks;
}

static void tscSetAssignedColumnInfo(SParsedDataColInfo *spd, SSchema *pSchema, int32_t numOfCols) {
  spd->numOfCols = numOfCols;
  spd->numOfAssignedCols = numOfCols;

  for (int32_t i = 0; i < numOfCols; ++i) {
    spd->hasVal[i] = true;
    spd->elems[i].colIndex = i;

    if (i > 0) {
      spd->elems[i].offset = spd->elems[i - 1].offset + pSchema[i - 1].bytes;
    }
  }
}

int32_t tscAllocateMemIfNeed(STableDataBlocks *pDataBlock, int32_t rowSize) {
  size_t remain = pDataBlock->nAllocSize - pDataBlock->size;
  const int factor = 5;

  // expand the allocated size
  if (remain < rowSize * factor) {
    while (remain < rowSize * factor) {
      pDataBlock->nAllocSize = (uint32_t) (pDataBlock->nAllocSize * 1.5);
      remain = pDataBlock->nAllocSize - pDataBlock->size;
    }

    char *tmp = realloc(pDataBlock->pData, (size_t)pDataBlock->nAllocSize);
    if (tmp != NULL) {
      pDataBlock->pData = tmp;
      memset(pDataBlock->pData + pDataBlock->size, 0, pDataBlock->nAllocSize - pDataBlock->size);
    } else {
      assert(false);
      // do nothing
    }
  }

  return (int32_t)(pDataBlock->nAllocSize - pDataBlock->size) / rowSize;
}

static void tsSetBlockInfo(SShellSubmitBlock *pBlocks, const SMeterMeta *pMeterMeta, int32_t numOfRows) {
  pBlocks->sid = pMeterMeta->sid;
  pBlocks->uid = pMeterMeta->uid;
  pBlocks->sversion = pMeterMeta->sversion;
  pBlocks->numOfRows += numOfRows;
}

// data block is disordered, sort it in ascending order
void sortRemoveDuplicates(STableDataBlocks *dataBuf) {
  SShellSubmitBlock* pBlocks = (SShellSubmitBlock*)dataBuf->pData;

  // size is less than the total size, since duplicated rows may be removed yet.
  assert(pBlocks->numOfRows * dataBuf->rowSize + sizeof(SShellSubmitBlock) == dataBuf->size);

  if (!dataBuf->ordered) {
    char *pBlockData = pBlocks->payLoad;
    qsort(pBlockData, pBlocks->numOfRows, dataBuf->rowSize, rowDataCompar);

    int32_t i = 0;
    int32_t j = 1;

    while (j < pBlocks->numOfRows) {
      TSKEY ti = *(TSKEY *)(pBlockData + dataBuf->rowSize * i);
      TSKEY tj = *(TSKEY *)(pBlockData + dataBuf->rowSize * j);

      if (ti == tj) {
        ++j;
        continue;
      }

      int32_t nextPos = (++i);
      if (nextPos != j) {
        memmove(pBlockData + dataBuf->rowSize * nextPos, pBlockData + dataBuf->rowSize * j, dataBuf->rowSize);
      }

      ++j;
    }

    dataBuf->ordered = true;

    pBlocks->numOfRows = i + 1;
    dataBuf->size = sizeof(SShellSubmitBlock) + dataBuf->rowSize*pBlocks->numOfRows;
  }
}

static int32_t doParseInsertStatement(SSqlObj *pSql, void *pTableHashList, char **str, SParsedDataColInfo *spd,
                                      int32_t *totalNum) {
  SSqlCmd *   pCmd = &pSql->cmd;
  SMeterMeta *pMeterMeta = pCmd->pMeterMeta;

  STableDataBlocks *dataBuf =
      tscGetDataBlockFromList(pTableHashList, pCmd->pDataBlocks, pMeterMeta->uid, TSDB_DEFAULT_PAYLOAD_SIZE,
                              sizeof(SShellSubmitBlock), pMeterMeta->rowSize, pCmd->name);

  int32_t maxNumOfRows = tscAllocateMemIfNeed(dataBuf, pMeterMeta->rowSize);

  int32_t numOfRows = tsParseValues(str, dataBuf, pMeterMeta, maxNumOfRows, spd, pCmd->payload);
  if (numOfRows <= 0) {
    return TSDB_CODE_INVALID_SQL;
  }

  SShellSubmitBlock *pBlocks = (SShellSubmitBlock *)(dataBuf->pData);
  tsSetBlockInfo(pBlocks, pMeterMeta, numOfRows);

  dataBuf->vgid = pMeterMeta->vgid;
  dataBuf->numOfMeters = 1;

  /*
   * the value of pRes->numOfRows does not affect the true result of AFFECTED ROWS,
   * which is actually returned from server.
   */
  *totalNum += numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t tscParseSqlForCreateTableOnDemand(char **sqlstr, SSqlObj *pSql) {
  char *  id = NULL;
  int32_t idlen = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  SSqlCmd *pCmd = &pSql->cmd;
  char *   sql = *sqlstr;

  sql = tscGetToken(sql, &id, &idlen);

  /* build the token of specified table */
  SSQLToken tableToken = {.z = id, .n = idlen, .type = TK_ID};

  char *cstart = NULL;
  char *cend = NULL;

  /* skip possibly exists column list */
  sql = tscGetToken(sql, &id, &idlen);
  int32_t numOfColList = 0;
  bool   createTable = false;

  if (id[0] == '(' && idlen == 1) {
    cstart = &id[0];
    while (1) {
      sql = tscGetToken(sql, &id, &idlen);
      if (id[0] == ')' && idlen == 1) {
        cend = &id[0];
        break;
      }

      ++numOfColList;
    }

    sql = tscGetToken(sql, &id, &idlen);
  }

  if (numOfColList == 0 && cstart != NULL) {
    return TSDB_CODE_INVALID_SQL;
  }

  if (strncmp(id, "using", idlen) == 0 && idlen == 5) {
    /* create table if not exists */
    sql = tscGetToken(sql, &id, &idlen);
    STagData *pTag = (STagData *)pCmd->payload;
    memset(pTag, 0, sizeof(STagData));

    SSQLToken token1 = {idlen, TK_ID, id};
    setMeterID(pSql, &token1);

    strcpy(pTag->name, pSql->cmd.name);

    code = tscGetMeterMeta(pSql, pTag->name);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (!UTIL_METER_IS_METRIC(pCmd)) {
      const char* msg = "create table only from super table is allowed";
      sprintf(pCmd->payload, "%s", msg);
      return TSDB_CODE_INVALID_SQL;
    }

    char *   tagVal = pTag->data;
    SSchema *pTagSchema = tsGetTagSchema(pCmd->pMeterMeta);

    sql = tscGetToken(sql, &id, &idlen);
    if (!(strncmp(id, "tags", idlen) == 0 && idlen == 4)) {
      setErrMsg(pCmd->payload, sql);
      return TSDB_CODE_INVALID_SQL;
    }

    int32_t numOfTagValues = 0;
    while (1) {
      sql = tscGetToken(sql, &id, &idlen);
      if (idlen == 0) {
        break;
      } else if (idlen == 1) {
        if (id[0] == '(') {
          continue;
        }

        if (id[0] == ')') {
          break;
        }

        if (id[0] == '-' || id[0] == '+') {
          sql = tscGetToken(sql, &id, &idlen);

          id -= 1;
          idlen += 1;
        }
      }

      code = tsParseOneColumnData(&pTagSchema[numOfTagValues], id, idlen, tagVal, pCmd->payload, &sql, false,
                                  pCmd->pMeterMeta->precision);
      if (code != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd->payload, sql);
        return TSDB_CODE_INVALID_SQL;
      }

      tagVal += pTagSchema[numOfTagValues++].bytes;
    }

    if (numOfTagValues != pCmd->pMeterMeta->numOfTags) {
      setErrMsg(pCmd->payload, sql);
      return TSDB_CODE_INVALID_SQL;
    }

    if (tscValidateName(&tableToken) != TSDB_CODE_SUCCESS) {
      setErrMsg(pCmd->payload, sql);
      return TSDB_CODE_INVALID_SQL;
    }

    int32_t ret = setMeterID(pSql, &tableToken);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    createTable = true;
    code = tscGetMeterMetaEx(pSql, pSql->cmd.name, true);
  } else {
    if (cstart != NULL) {
      sql = cstart;
    } else {
      sql = id;
    }
    code = tscGetMeterMeta(pSql, pCmd->name);
  }

  int32_t len = cend - cstart + 1;
  if (cstart != NULL && createTable == true) {
    /* move the column list to start position of the next accessed points */
    memmove(sql - len, cstart, len);
    *sqlstr = sql - len;
  } else {
    *sqlstr = sql;
  }

  return code;
}

int validateTableName(char *tblName, int len) {
  char buf[TSDB_METER_ID_LEN] = {0};
  strncpy(buf, tblName, len);

  SSQLToken token = {.n = len, .type = TK_ID, .z = buf};
  tSQLGetToken(buf, &token.type);

  return tscValidateName(&token);
}

/**
 * usage: insert into table1 values() () table2 values()()
 *
 * @param pCmd
 * @param str
 * @param acct
 * @param db
 * @param pSql
 * @return
 */
int tsParseInsertStatement(SSqlObj *pSql, char *str, char *acct, char *db) {
  SSqlCmd *pCmd = &pSql->cmd;

  pCmd->command = TSDB_SQL_INSERT;
  pCmd->isInsertFromFile = -1;
  pCmd->count = 0;

  pSql->res.numOfRows = 0;
  int32_t totalNum = 0;

  if (!pSql->pTscObj->writeAuth) {
    return TSDB_CODE_NO_RIGHTS;
  }

  char *id;
  int   idlen;
  int   code = TSDB_CODE_INVALID_SQL;

  str = tscGetToken(str, &id, &idlen);
  if (idlen == 0 || (strncmp(id, "into", 4) != 0 || idlen != 4)) {
    INVALID_SQL_RET_MSG(pCmd->payload, "keyword INTO is expected");
  }

  if ((code = tscAllocPayloadWithSize(pCmd, TSDB_PAYLOAD_SIZE)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  void *pTableHashList = taosInitIntHash(128, sizeof(void *), taosHashInt);

  pSql->cmd.pDataBlocks = tscCreateBlockArrayList();
  tscTrace("%p create data block list for submit data, %p", pSql, pSql->cmd.pDataBlocks);

  while (1) {
    tscGetToken(str, &id, &idlen);

    if (idlen == 0) {
      // parse file, do not release the STableDataBlock
      if (pCmd->isInsertFromFile == 1) {
        goto _clean;
      }

      if (totalNum > 0) {
        break;
      } else {  // no data in current sql string, error
        code = TSDB_CODE_INVALID_SQL;
        goto _error_clean;
      }
    }

    // Check if the table name available or not
    if (validateTableName(id, idlen) != TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_INVALID_SQL;
      sprintf(pCmd->payload, "table name is invalid");
      goto _error_clean;
    }

    SSQLToken token = {idlen, TK_ID, id};
    if ((code = setMeterID(pSql, &token)) != TSDB_CODE_SUCCESS) {
      goto _error_clean;
    }

    void *fp = pSql->fp;
    if ((code = tscParseSqlForCreateTableOnDemand(&str, pSql)) != TSDB_CODE_SUCCESS) {
      if (fp != NULL) {
        goto _clean;
      } else {
        /*
         * for async insert, the free data block operations, which is tscDestroyBlockArrayList,
         * must be executed before launch another threads to get metermeta, since the
         * later ops may manipulate SSqlObj through another thread in getMeterMetaCallback function.
         */
        goto _error_clean;
      }
    }

    if (UTIL_METER_IS_METRIC(pCmd)) {
      code = TSDB_CODE_INVALID_SQL;
      sprintf(pCmd->payload, "insert data into metric is not supported");
      goto _error_clean;
    }

    str = tscGetToken(str, &id, &idlen);
    if (idlen == 0) {
      code = TSDB_CODE_INVALID_SQL;
      sprintf(pCmd->payload, "keyword VALUES or FILE are required");
      goto _error_clean;
    }

    if (strncmp(id, "values", 6) == 0 && idlen == 6) {
      SParsedDataColInfo spd = {0};
      SSchema *          pSchema = tsGetSchema(pCmd->pMeterMeta);

      tscSetAssignedColumnInfo(&spd, pSchema, pCmd->pMeterMeta->numOfColumns);

      if (pCmd->isInsertFromFile == -1) {
        pCmd->isInsertFromFile = 0;
      } else {
        if (pCmd->isInsertFromFile == 1) {
          code = TSDB_CODE_INVALID_SQL;
          sprintf(pCmd->payload, "keyword VALUES and FILE are not allowed to mix up");
          goto _error_clean;
        }
      }

      /*
       * app here insert data in different vnodes, so we need to set the following
       * data in another submit procedure using async insert routines
       */
      code = doParseInsertStatement(pSql, pTableHashList, &str, &spd, &totalNum);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error_clean;
      }

    } else if (strncmp(id, "file", 4) == 0 && idlen == 4) {
      if (pCmd->isInsertFromFile == -1) {
        pCmd->isInsertFromFile = 1;
      } else {
        if (pCmd->isInsertFromFile == 0) {
          code = TSDB_CODE_INVALID_SQL;
          sprintf(pCmd->payload, "keyword VALUES and FILE are not allowed to mix up");
          goto _error_clean;
        }
      }

      str = tscGetTokenDelimiter(str, &id, &idlen, " ;");
      if (idlen == 0) {
        code = TSDB_CODE_INVALID_SQL;
        sprintf(pCmd->payload, "filename is required following keyword FILE");
        goto _error_clean;
      }

      char fname[PATH_MAX] = {0};
      strncpy(fname, id, idlen);
      strdequote(fname);

      wordexp_t full_path;
      if (wordexp(fname, &full_path, 0) != 0) {
        code = TSDB_CODE_INVALID_SQL;
        sprintf(pCmd->payload, "invalid filename");
        goto _error_clean;
      }
      strcpy(fname, full_path.we_wordv[0]);
      wordfree(&full_path);

      STableDataBlocks* pDataBlock = tscCreateDataBlockEx(PATH_MAX, pCmd->pMeterMeta->rowSize, sizeof(SShellSubmitBlock),
                                                          pCmd->name);

      tscAppendDataBlock(pCmd->pDataBlocks, pDataBlock);
      strcpy(pDataBlock->filename, fname);
      str = id + idlen;
    } else if (idlen == 1 && id[0] == '(') {
      /* insert into tablename(col1, col2,..., coln) values(v1, v2,... vn); */
      SMeterMeta *pMeterMeta = pCmd->pMeterMeta;
      SSchema *   pSchema = tsGetSchema(pMeterMeta);

      if (pCmd->isInsertFromFile == -1) {
        pCmd->isInsertFromFile = 0;
      } else if (pCmd->isInsertFromFile == 1) {
        code = TSDB_CODE_INVALID_SQL;
        sprintf(pCmd->payload, "keyword VALUES and FILE are not allowed to mix up");
        goto _error_clean;
      }

      SParsedDataColInfo spd = {0};
      spd.numOfCols = pMeterMeta->numOfColumns;

      int16_t offset[TSDB_MAX_COLUMNS] = {0};
      for (int32_t t = 1; t < pMeterMeta->numOfColumns; ++t) {
        offset[t] = offset[t - 1] + pSchema[t - 1].bytes;
      }

      while (1) {
        str = tscGetToken(str, &id, &idlen);
        if (idlen == 1 && id[0] == ')') {
          break;
        }

        bool findColumnIndex = false;

        // todo speedup by using hash list
        for (int32_t t = 0; t < pMeterMeta->numOfColumns; ++t) {
          if (strncmp(id, pSchema[t].name, idlen) == 0 && strlen(pSchema[t].name) == idlen) {
            SParsedColElem *pElem = &spd.elems[spd.numOfAssignedCols++];
            pElem->offset = offset[t];
            pElem->colIndex = t;

            if (spd.hasVal[t] == true) {
              code = TSDB_CODE_INVALID_SQL;
              sprintf(pCmd->payload, "duplicated column name");
              goto _error_clean;
            }

            spd.hasVal[t] = true;
            findColumnIndex = true;
            break;
          }
        }

        if (!findColumnIndex) {  //
          code = TSDB_CODE_INVALID_SQL;
          sprintf(pCmd->payload, "invalid column name");
          goto _error_clean;
        }
      }

      if (spd.numOfAssignedCols == 0 || spd.numOfAssignedCols > pMeterMeta->numOfColumns) {
        code = TSDB_CODE_INVALID_SQL;
        sprintf(pCmd->payload, "column name expected");
        goto _error_clean;
      }

      str = tscGetToken(str, &id, &idlen);
      if (strncmp(id, "values", idlen) != 0 || idlen != 6) {
        code = TSDB_CODE_INVALID_SQL;
        sprintf(pCmd->payload, "keyword VALUES is expected");
        goto _error_clean;
      }

      code = doParseInsertStatement(pSql, pTableHashList, &str, &spd, &totalNum);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error_clean;
      }
    } else {
      code = TSDB_CODE_INVALID_SQL;
      sprintf(pCmd->payload, "keyword VALUES or FILE are required");
      goto _error_clean;
    }
  }

  // submit to more than one vnode
  if (pCmd->pDataBlocks->nSize > 0) {
    // merge according to vgid
    tscMergeTableDataBlocks(pSql, pCmd->pDataBlocks);

    STableDataBlocks *pDataBlock = pCmd->pDataBlocks->pData[0];
    if ((code = tscCopyDataBlockToPayload(pSql, pDataBlock)) != TSDB_CODE_SUCCESS) {
      goto _error_clean;
    }

    pCmd->vnodeIdx = 1;  // set the next sent data vnode index in data block arraylist
  } else {
    pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);
  }

  code = TSDB_CODE_SUCCESS;
  goto _clean;

_error_clean:
  pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);

_clean:
  taosCleanUpIntHash(pTableHashList);
  return code;
}

int tsParseImportStatement(SSqlObj *pSql, char *str, char *acct, char *db) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->order.order = TSQL_SO_ASC;
  return tsParseInsertStatement(pSql, str, acct, db);
}

int tsParseInsertSql(SSqlObj *pSql, char *sql, char *acct, char *db) {
  char *verb;
  int   verblen;
  int   code = TSDB_CODE_INVALID_SQL;

  SSqlCmd *pCmd = &pSql->cmd;
  sql = tscGetToken(sql, &verb, &verblen);

  if (verblen) {
    if (strncmp(verb, "insert", 6) == 0 && verblen == 6) {
      code = tsParseInsertStatement(pSql, sql, acct, db);
    } else if (strncmp(verb, "import", 6) == 0 && verblen == 6) {
      code = tsParseImportStatement(pSql, sql, acct, db);
    } else {
      verb[verblen] = 0;
      sprintf(pCmd->payload, "invalid keyword:%s", verb);
    }
  } else {
    sprintf(pCmd->payload, "no any keywords");
  }

  return code;
}

int tsParseSql(SSqlObj *pSql, char *acct, char *db, bool multiVnodeInsertion) {
  int32_t ret = TSDB_CODE_SUCCESS;
  tscCleanSqlCmd(&pSql->cmd);

  if (tscIsInsertOrImportData(pSql->sqlstr)) {
    /*
     * only for async multi-vnode insertion
     * Set the fp before parse the sql string, in case of getmetermeta failed, in which
     * the error handle callback function can rightfully restore the user defined function (fp)
     */
    if (pSql->fp != NULL && multiVnodeInsertion) {
      assert(pSql->fetchFp == NULL);
      pSql->fetchFp = pSql->fp;

      /* replace user defined callback function with multi-insert proxy function */
      pSql->fp = tscAsyncInsertMultiVnodesProxy;
    }

    ret = tsParseInsertSql(pSql, pSql->sqlstr, acct, db);
  } else {
    SSqlInfo SQLInfo = {0};
    tSQLParse(&SQLInfo, pSql->sqlstr);

    tscAllocPayloadWithSize(&pSql->cmd, TSDB_DEFAULT_PAYLOAD_SIZE);

    ret = tscToSQLCmd(pSql, &SQLInfo);
    SQLInfoDestroy(&SQLInfo);
  }

  /*
   * the pRes->code may be modified or even released by another thread in tscMeterMetaCallBack
   * function, so do NOT use pRes->code to determine if the getMeterMeta/getMetricMeta function
   * invokes new threads to get data from mnode or simply retrieves data from cache.
   *
   * do NOT assign return code to pRes->code for the same reason for it may be released by another thread
   * pRes->code = ret;
   */
  return ret;
}

static int doPackSendDataBlock(SSqlObj* pSql, int32_t numOfRows, STableDataBlocks* pTableDataBlocks) {
  int32_t code = TSDB_CODE_SUCCESS;
  SSqlCmd* pCmd = &pSql->cmd;

  SMeterMeta* pMeterMeta = pCmd->pMeterMeta;

  SShellSubmitBlock *pBlocks = (SShellSubmitBlock *)(pTableDataBlocks->pData);
  tsSetBlockInfo(pBlocks, pMeterMeta, numOfRows);

  tscMergeTableDataBlocks(pSql, pCmd->pDataBlocks);

  // the pDataBlock is different from the pTableDataBlocks
  STableDataBlocks *pDataBlock = pCmd->pDataBlocks->pData[0];
  if ((code = tscCopyDataBlockToPayload(pSql, pDataBlock)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  if ((code = tscProcessSql(pSql)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static int tscInsertDataFromFile(SSqlObj *pSql, FILE *fp) {
  int         readLen = 0;
  char *      line = NULL;
  size_t      n = 0;
  int         len = 0;
  uint32_t    maxRows = 0;
  SSqlCmd *   pCmd = &pSql->cmd;
  SMeterMeta *pMeterMeta = pCmd->pMeterMeta;
  int         numOfRows = 0;
  int32_t     rowSize = pMeterMeta->rowSize;
  int32_t     code = 0;
  int         nrows = 0;

  pCmd->pDataBlocks = tscCreateBlockArrayList();
  STableDataBlocks* pTableDataBlock = tscCreateDataBlockEx(TSDB_PAYLOAD_SIZE, pMeterMeta->rowSize,
                                                           sizeof(SShellSubmitBlock), pCmd->name);

  tscAppendDataBlock(pCmd->pDataBlocks, pTableDataBlock);

  maxRows = tscAllocateMemIfNeed(pTableDataBlock, rowSize);
  if (maxRows < 1) return -1;

  int                count = 0;
  SParsedDataColInfo spd = {.numOfCols = pCmd->pMeterMeta->numOfColumns};
  SSchema *          pSchema = tsGetSchema(pCmd->pMeterMeta);

  tscSetAssignedColumnInfo(&spd, pSchema, pCmd->pMeterMeta->numOfColumns);

  while ((readLen = getline(&line, &n, fp)) != -1) {
    // line[--readLen] = '\0';
    if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) line[--readLen] = 0;
    if (readLen <= 0) continue;

    char *lineptr = line;
    strtolower(line, line);

    len = tsParseOneRowData(&lineptr, pTableDataBlock, pSchema, &spd, pCmd->payload, pMeterMeta->precision);
    if (len <= 0) return -1;

    pTableDataBlock->size += len;

    count++;
    nrows++;
    if (count >= maxRows) {
      if ((code = doPackSendDataBlock(pSql, count, pTableDataBlock)) != TSDB_CODE_SUCCESS) {
        return -code;
      }

      pTableDataBlock = pCmd->pDataBlocks->pData[0];
      pTableDataBlock->size = sizeof(SShellSubmitBlock);
      pTableDataBlock->rowSize = pMeterMeta->rowSize;

      numOfRows += pSql->res.numOfRows;
      pSql->res.numOfRows = 0;
      count = 0;
    }
  }

  if (count > 0) {
    if ((code = doPackSendDataBlock(pSql, count, pTableDataBlock)) != TSDB_CODE_SUCCESS) {
      return -code;
    }

    numOfRows += pSql->res.numOfRows;
    pSql->res.numOfRows = 0;
  }

  if (line) tfree(line);

  return numOfRows;
}

/* multi-vnodes insertion in sync query model
 *
 * modify history
 * 2019.05.10 lihui
 * Remove the code for importing records from files
 */
void tscProcessMultiVnodesInsert(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  if (pCmd->command != TSDB_SQL_INSERT) {
    return;
  }

  STableDataBlocks *pDataBlock = NULL;
  int32_t           affected_rows = 0;
  int32_t           code = TSDB_CODE_SUCCESS;

  /* the first block has been sent to server in processSQL function */
  assert(pCmd->isInsertFromFile != -1 && pCmd->vnodeIdx >= 1 && pCmd->pDataBlocks != NULL);

  if (pCmd->vnodeIdx < pCmd->pDataBlocks->nSize) {
    SDataBlockList *pDataBlocks = pCmd->pDataBlocks;

    for (int32_t i = pCmd->vnodeIdx; i < pDataBlocks->nSize; ++i) {
      pDataBlock = pDataBlocks->pData[i];
      if (pDataBlock == NULL) {
        continue;
      }

      if ((code = tscCopyDataBlockToPayload(pSql, pDataBlock)) != TSDB_CODE_SUCCESS) {
        tscTrace("%p build submit data block failed, vnodeIdx:%d, total:%d", pSql, pCmd->vnodeIdx, pDataBlocks->nSize);
        continue;
      }

      tscProcessSql(pSql);
    }
  }

  // all data have been submit to vnode, release data blocks
  pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);
}

/* multi-vnodes insertion in sync query model */
void tscProcessMultiVnodesInsertForFile(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  if (pCmd->command != TSDB_SQL_INSERT) {
    return;
  }

  STableDataBlocks *pDataBlock = NULL;
  int32_t           affected_rows = 0;

  assert(pCmd->isInsertFromFile == 1 && pCmd->pDataBlocks != NULL);
  SDataBlockList *pDataBlockList = pCmd->pDataBlocks;
  pCmd->pDataBlocks = NULL;

  char            path[PATH_MAX] = {0};

  for (int32_t i = 0; i < pDataBlockList->nSize; ++i) {
    pDataBlock = pDataBlockList->pData[i];
    if (pDataBlock == NULL) {
      continue;
    }

    tscAllocPayloadWithSize(pCmd, TSDB_PAYLOAD_SIZE);
    pCmd->count = 1;

    strncpy(path, pDataBlock->filename, PATH_MAX);

    FILE *fp = fopen(path, "r");
    if (fp == NULL) {
      tscError("%p failed to open file %s to load data from file, reason:%s", pSql, path,
               strerror(errno));
      continue;
    }

    strcpy(pCmd->name, pDataBlock->meterId);
    memset(pDataBlock->pData, 0, pDataBlock->nAllocSize);

    int32_t ret = tscGetMeterMeta(pSql, pCmd->name);
    if (ret != TSDB_CODE_SUCCESS) {
      tscError("%p get meter meta failed, abort", pSql);
      continue;
    }

    int nrows = tscInsertDataFromFile(pSql, fp);
    pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);

    if (nrows < 0) {
      fclose(fp);
      tscTrace("%p no records in file %s", pSql, path);
      continue;
    }

    fclose(fp);
    affected_rows += nrows;

    tscTrace("%p Insert data %d records from file %s", pSql, nrows, path);
  }

  pSql->res.numOfRows = affected_rows;

  // all data have been submit to vnode, release data blocks
  pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);
  tscDestroyBlockArrayList(pDataBlockList);
}
