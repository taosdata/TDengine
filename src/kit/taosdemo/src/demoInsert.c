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

#include "demo.h"
#include "demoData.h"

static char *getTagValueFromTagSample(SSuperTable *stbInfo, int tagUsePos) {
    char *dataBuf = (char *)calloc(TSDB_MAX_SQL_LEN + 1, 1);
    if (NULL == dataBuf) {
        errorPrint("%s() LN%d, calloc failed! size:%d\n", __func__, __LINE__,
                   TSDB_MAX_SQL_LEN + 1);
        return NULL;
    }

    int dataLen = 0;
    dataLen +=
        snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "(%s)",
                 stbInfo->tagDataBuf + stbInfo->lenOfTagOfOneRow * tagUsePos);

    return dataBuf;
}

static char *generateBinaryNCharTagValues(int64_t tableSeq, uint32_t len) {
    char *buf = (char *)calloc(len, 1);
    if (NULL == buf) {
        printf("calloc failed! size:%d\n", len);
        return NULL;
    }

    if (tableSeq % 2) {
        tstrncpy(buf, "beijing", len);
    } else {
        tstrncpy(buf, "shanghai", len);
    }
    // rand_string(buf, stbInfo->tags[i].dataLen);

    return buf;
}

static char *generateTagValuesForStb(SSuperTable *stbInfo, int64_t tableSeq) {
    char *dataBuf = (char *)calloc(TSDB_MAX_SQL_LEN + 1, 1);
    if (NULL == dataBuf) {
        printf("calloc failed! size:%d\n", TSDB_MAX_SQL_LEN + 1);
        return NULL;
    }

    int dataLen = 0;
    dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "(");
    for (int i = 0; i < stbInfo->tagCount; i++) {
        if ((0 == strncasecmp(stbInfo->tags[i].dataType, "binary",
                              strlen("binary"))) ||
            (0 == strncasecmp(stbInfo->tags[i].dataType, "nchar",
                              strlen("nchar")))) {
            if (stbInfo->tags[i].dataLen > TSDB_MAX_BINARY_LEN) {
                printf("binary or nchar length overflow, max size:%u\n",
                       (uint32_t)TSDB_MAX_BINARY_LEN);
                tmfree(dataBuf);
                return NULL;
            }

            int32_t tagBufLen = stbInfo->tags[i].dataLen + 1;
            char *  buf = generateBinaryNCharTagValues(tableSeq, tagBufLen);
            if (NULL == buf) {
                tmfree(dataBuf);
                return NULL;
            }
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "\'%s\',", buf);
            tmfree(buf);
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "int",
                                    strlen("int"))) {
            if ((g_args.demo_mode) && (i == 0)) {
                dataLen +=
                    snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                             "%" PRId64 ",", (tableSeq % 10) + 1);
            } else {
                dataLen +=
                    snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                             "%" PRId64 ",", tableSeq);
            }
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "bigint",
                                    strlen("bigint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%" PRId64 ",", rand_bigint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "float",
                                    strlen("float"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%f,", rand_float());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "double",
                                    strlen("double"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%f,", rand_double());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "smallint",
                                    strlen("smallint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%d,", rand_smallint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "tinyint",
                                    strlen("tinyint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%d,", rand_tinyint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "bool",
                                    strlen("bool"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%d,", rand_bool());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "timestamp",
                                    strlen("timestamp"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%" PRId64 ",", rand_ubigint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "utinyint",
                                    strlen("utinyint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%d,", rand_utinyint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "usmallint",
                                    strlen("usmallint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%d,", rand_usmallint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "uint",
                                    strlen("uint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%d,", rand_uint());
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "ubigint",
                                    strlen("ubigint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                                "%" PRId64 ",", rand_ubigint());
        } else {
            errorPrint("No support data type: %s\n", stbInfo->tags[i].dataType);
            tmfree(dataBuf);
            return NULL;
        }
    }

    dataLen -= 1;
    dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, ")");
    return dataBuf;
}

static int calcRowLen(SSuperTable *superTbls) {
    int colIndex;
    int lenOfOneRow = 0;

    for (colIndex = 0; colIndex < superTbls->columnCount; colIndex++) {
        char *dataType = superTbls->columns[colIndex].dataType;

        switch (superTbls->columns[colIndex].data_type) {
            case TSDB_DATA_TYPE_BINARY:
                lenOfOneRow += superTbls->columns[colIndex].dataLen + 3;
                break;

            case TSDB_DATA_TYPE_NCHAR:
                lenOfOneRow += superTbls->columns[colIndex].dataLen + 3;
                break;

            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                lenOfOneRow += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                lenOfOneRow += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                lenOfOneRow += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                lenOfOneRow += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                lenOfOneRow += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                lenOfOneRow += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                lenOfOneRow += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                lenOfOneRow += TIMESTAMP_BUFF_LEN;
                break;

            default:
                errorPrint("get error data type : %s\n", dataType);
                exit(EXIT_FAILURE);
        }
    }

    superTbls->lenOfOneRow = lenOfOneRow + TIMESTAMP_BUFF_LEN;  // timestamp

    int tagIndex;
    int lenOfTagOfOneRow = 0;
    for (tagIndex = 0; tagIndex < superTbls->tagCount; tagIndex++) {
        char *dataType = superTbls->tags[tagIndex].dataType;
        switch (superTbls->tags[tagIndex].data_type) {
            case TSDB_DATA_TYPE_BINARY:
                lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 3;
                break;
            case TSDB_DATA_TYPE_NCHAR:
                lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 3;
                break;
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                lenOfTagOfOneRow +=
                    superTbls->tags[tagIndex].dataLen + INT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                lenOfTagOfOneRow +=
                    superTbls->tags[tagIndex].dataLen + BIGINT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                lenOfTagOfOneRow +=
                    superTbls->tags[tagIndex].dataLen + SMALLINT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                lenOfTagOfOneRow +=
                    superTbls->tags[tagIndex].dataLen + TINYINT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_BOOL:
                lenOfTagOfOneRow +=
                    superTbls->tags[tagIndex].dataLen + BOOL_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_FLOAT:
                lenOfTagOfOneRow +=
                    superTbls->tags[tagIndex].dataLen + FLOAT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                lenOfTagOfOneRow +=
                    superTbls->tags[tagIndex].dataLen + DOUBLE_BUFF_LEN;
                break;
            default:
                errorPrint("get error tag type : %s\n", dataType);
                exit(EXIT_FAILURE);
        }
    }

    superTbls->lenOfTagOfOneRow = lenOfTagOfOneRow;

    return 0;
}

static int getSuperTableFromServer(TAOS *taos, char *dbName,
                                   SSuperTable *superTbls) {
    char      command[SQL_BUFF_LEN] = "\0";
    TAOS_RES *res;
    TAOS_ROW  row = NULL;
    int       count = 0;

    // get schema use cmd: describe superTblName;
    snprintf(command, SQL_BUFF_LEN, "describe %s.%s", dbName,
             superTbls->stbName);
    res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        printf("failed to run command %s, reason: %s\n", command,
               taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    int         tagIndex = 0;
    int         columnIndex = 0;
    TAOS_FIELD *fields = taos_fetch_fields(res);
    while ((row = taos_fetch_row(res)) != NULL) {
        if (0 == count) {
            count++;
            continue;
        }

        if (strcmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "TAG") == 0) {
            tstrncpy(superTbls->tags[tagIndex].field,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes);
            if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                 "INT", strlen("INT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_INT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TINYINT", strlen("TINYINT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_TINYINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "SMALLINT", strlen("SMALLINT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_SMALLINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BIGINT", strlen("BIGINT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_BIGINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "FLOAT", strlen("FLOAT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_FLOAT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "DOUBLE", strlen("DOUBLE"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_DOUBLE;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BINARY", strlen("BINARY"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_BINARY;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "NCHAR", strlen("NCHAR"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_NCHAR;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BOOL", strlen("BOOL"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_BOOL;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TIMESTAMP", strlen("TIMESTAMP"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_TIMESTAMP;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TINYINT UNSIGNED",
                                   strlen("TINYINT UNSIGNED"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_UTINYINT;
                tstrncpy(superTbls->tags[tagIndex].dataType, "UTINYINT",
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "SMALLINT UNSIGNED",
                                   strlen("SMALLINT UNSIGNED"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_USMALLINT;
                tstrncpy(superTbls->tags[tagIndex].dataType, "USMALLINT",
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "INT UNSIGNED", strlen("INT UNSIGNED"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_UINT;
                tstrncpy(superTbls->tags[tagIndex].dataType, "UINT",
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "BIGINT UNSIGNED", strlen("BIGINT UNSIGNED"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_UBIGINT;
                tstrncpy(superTbls->tags[tagIndex].dataType, "UBIGINT",
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            } else {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_NULL;
            }
            superTbls->tags[tagIndex].dataLen =
                *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            tstrncpy(superTbls->tags[tagIndex].note,
                     (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
                     min(NOTE_BUFF_LEN,
                         fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes) +
                         1);
            if (strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                       "UNSIGNED") == NULL) {
                tstrncpy(superTbls->tags[tagIndex].dataType,
                         (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            }
            tagIndex++;
        } else {
            tstrncpy(superTbls->columns[columnIndex].field,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes);

            if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                 "INT", strlen("INT")) &&
                strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                       "UNSIGNED") == NULL) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_INT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "TINYINT", strlen("TINYINT")) &&
                       strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                              "UNSIGNED") == NULL) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_TINYINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "SMALLINT", strlen("SMALLINT")) &&
                       strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                              "UNSIGNED") == NULL) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_SMALLINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "BIGINT", strlen("BIGINT")) &&
                       strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                              "UNSIGNED") == NULL) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_BIGINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "FLOAT", strlen("FLOAT"))) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_FLOAT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "DOUBLE", strlen("DOUBLE"))) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_DOUBLE;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BINARY", strlen("BINARY"))) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_BINARY;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "NCHAR", strlen("NCHAR"))) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_NCHAR;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BOOL", strlen("BOOL"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_BOOL;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TIMESTAMP", strlen("TIMESTAMP"))) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_TIMESTAMP;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TINYINT UNSIGNED",
                                   strlen("TINYINT UNSIGNED"))) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_UTINYINT;
                tstrncpy(superTbls->columns[columnIndex].dataType, "UTINYINT",
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "SMALLINT UNSIGNED",
                                   strlen("SMALLINT UNSIGNED"))) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_USMALLINT;
                tstrncpy(superTbls->columns[columnIndex].dataType, "USMALLINT",
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "INT UNSIGNED", strlen("INT UNSIGNED"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_UINT;
                tstrncpy(superTbls->columns[columnIndex].dataType, "UINT",
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "BIGINT UNSIGNED", strlen("BIGINT UNSIGNED"))) {
                superTbls->columns[columnIndex].data_type =
                    TSDB_DATA_TYPE_UBIGINT;
                tstrncpy(superTbls->columns[columnIndex].dataType, "UBIGINT",
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            } else {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_NULL;
            }
            superTbls->columns[columnIndex].dataLen =
                *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            tstrncpy(superTbls->columns[columnIndex].note,
                     (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
                     min(NOTE_BUFF_LEN,
                         fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes) +
                         1);

            if (strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                       "UNSIGNED") == NULL) {
                tstrncpy(superTbls->columns[columnIndex].dataType,
                         (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                         min(DATATYPE_BUFF_LEN,
                             fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) +
                             1);
            }

            columnIndex++;
        }
        count++;
    }

    superTbls->columnCount = columnIndex;
    superTbls->tagCount = tagIndex;
    taos_free_result(res);

    calcRowLen(superTbls);

    /*
       if (TBL_ALREADY_EXISTS == superTbls->childTblExists) {
    //get all child table name use cmd: select tbname from superTblName;
    int childTblCount = 10000;
    superTbls->childTblName = (char*)calloc(1, childTblCount *
    TSDB_TABLE_NAME_LEN); if (superTbls->childTblName == NULL) {
    errorPrint("%s() LN%d, alloc memory failed!\n", __func__, __LINE__);
    return -1;
    }
    getAllChildNameOfSuperTable(taos, dbName,
    superTbls->stbName,
    &superTbls->childTblName,
    &superTbls->childTblCount);
    }
    */
    return 0;
}

static int createSuperTable(TAOS *taos, char *dbName, SSuperTable *superTbl) {
    char *command = calloc(1, BUFFER_SIZE);
    assert(command);

    char cols[COL_BUFFER_LEN] = "\0";
    int  len = 0;

    int lenOfOneRow = 0;

    if (superTbl->columnCount == 0) {
        errorPrint("%s() LN%d, super table column count is %d\n", __func__,
                   __LINE__, superTbl->columnCount);
        free(command);
        return -1;
    }

    for (int colIndex = 0; colIndex < superTbl->columnCount; colIndex++) {
        switch (superTbl->columns[colIndex].data_type) {
            case TSDB_DATA_TYPE_BINARY:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s(%d)",
                                colIndex, "BINARY",
                                superTbl->columns[colIndex].dataLen);
                lenOfOneRow += superTbl->columns[colIndex].dataLen + 3;
                break;

            case TSDB_DATA_TYPE_NCHAR:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s(%d)",
                                colIndex, "NCHAR",
                                superTbl->columns[colIndex].dataLen);
                lenOfOneRow += superTbl->columns[colIndex].dataLen + 3;
                break;

            case TSDB_DATA_TYPE_INT:
                if ((g_args.demo_mode) && (colIndex == 1)) {
                    len += snprintf(cols + len, COL_BUFFER_LEN - len,
                                    ", VOLTAGE INT");
                } else {
                    len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                    colIndex, "INT");
                }
                lenOfOneRow += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "BIGINT");
                lenOfOneRow += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "SMALLINT");
                lenOfOneRow += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "TINYINT");
                lenOfOneRow += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "BOOL");
                lenOfOneRow += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                if (g_args.demo_mode) {
                    if (colIndex == 0) {
                        len += snprintf(cols + len, COL_BUFFER_LEN - len,
                                        ", CURRENT FLOAT");
                    } else if (colIndex == 2) {
                        len += snprintf(cols + len, COL_BUFFER_LEN - len,
                                        ", PHASE FLOAT");
                    }
                } else {
                    len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                    colIndex, "FLOAT");
                }

                lenOfOneRow += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "DOUBLE");
                lenOfOneRow += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "TIMESTAMP");
                lenOfOneRow += TIMESTAMP_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "TINYINT UNSIGNED");
                lenOfOneRow += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_USMALLINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "SMALLINT UNSIGNED");
                lenOfOneRow += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_UINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "INT UNSIGNED");
                lenOfOneRow += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_UBIGINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "BIGINT UNSIGNED");
                lenOfOneRow += BIGINT_BUFF_LEN;
                break;

            default:
                taos_close(taos);
                free(command);
                errorPrint("%s() LN%d, config error data type : %s\n", __func__,
                           __LINE__, superTbl->columns[colIndex].dataType);
                exit(EXIT_FAILURE);
        }
    }

    superTbl->lenOfOneRow = lenOfOneRow + TIMESTAMP_BUFF_LEN;  // timestamp

    // save for creating child table
    superTbl->colsOfCreateChildTable =
        (char *)calloc(len + TIMESTAMP_BUFF_LEN, 1);
    if (NULL == superTbl->colsOfCreateChildTable) {
        taos_close(taos);
        free(command);
        errorPrint("%s() LN%d, Failed when calloc, size:%d", __func__, __LINE__,
                   len + 1);
        exit(EXIT_FAILURE);
    }

    snprintf(superTbl->colsOfCreateChildTable, len + TIMESTAMP_BUFF_LEN,
             "(ts timestamp%s)", cols);
    verbosePrint("%s() LN%d: %s\n", __func__, __LINE__,
                 superTbl->colsOfCreateChildTable);

    if (superTbl->tagCount == 0) {
        errorPrint("%s() LN%d, super table tag count is %d\n", __func__,
                   __LINE__, superTbl->tagCount);
        free(command);
        return -1;
    }

    char tags[TSDB_MAX_TAGS_LEN] = "\0";
    int  tagIndex;
    len = 0;

    int lenOfTagOfOneRow = 0;
    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "(");
    for (tagIndex = 0; tagIndex < superTbl->tagCount; tagIndex++) {
        char *dataType = superTbl->tags[tagIndex].dataType;

        if (strcasecmp(dataType, "BINARY") == 0) {
            if ((g_args.demo_mode) && (tagIndex == 1)) {
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                                "location BINARY(%d),",
                                superTbl->tags[tagIndex].dataLen);
            } else {
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                                "T%d %s(%d),", tagIndex, "BINARY",
                                superTbl->tags[tagIndex].dataLen);
            }
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "NCHAR") == 0) {
            len +=
                snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s(%d),",
                         tagIndex, "NCHAR", superTbl->tags[tagIndex].dataLen);
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "INT") == 0) {
            if ((g_args.demo_mode) && (tagIndex == 0)) {
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                                "groupId INT, ");
            } else {
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "INT");
            }
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + INT_BUFF_LEN;
        } else if (strcasecmp(dataType, "BIGINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "BIGINT");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + BIGINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "SMALLINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "SMALLINT");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + SMALLINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "TINYINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "TINYINT");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + TINYINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "BOOL") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "BOOL");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + BOOL_BUFF_LEN;
        } else if (strcasecmp(dataType, "FLOAT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "FLOAT");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + FLOAT_BUFF_LEN;
        } else if (strcasecmp(dataType, "DOUBLE") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "DOUBLE");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + DOUBLE_BUFF_LEN;
        } else if (strcasecmp(dataType, "UTINYINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "TINYINT UNSIGNED");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + TINYINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "USMALLINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "SMALLINT UNSIGNED");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + SMALLINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "UINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "INT UNSIGNED");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + INT_BUFF_LEN;
        } else if (strcasecmp(dataType, "UBIGINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "BIGINT UNSIGNED");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + BIGINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "TIMESTAMP") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                            tagIndex, "TIMESTAMP");
            lenOfTagOfOneRow +=
                superTbl->tags[tagIndex].dataLen + TIMESTAMP_BUFF_LEN;
        } else {
            taos_close(taos);
            free(command);
            errorPrint("%s() LN%d, config error tag type : %s\n", __func__,
                       __LINE__, dataType);
            exit(EXIT_FAILURE);
        }
    }

    len -= 1;
    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, ")");

    superTbl->lenOfTagOfOneRow = lenOfTagOfOneRow;

    snprintf(command, BUFFER_SIZE,
             superTbl->escapeChar
                 ? "CREATE TABLE IF NOT EXISTS %s.`%s` (ts TIMESTAMP%s) TAGS %s"
                 : "CREATE TABLE IF NOT EXISTS %s.%s (ts TIMESTAMP%s) TAGS %s",
             dbName, superTbl->stbName, cols, tags);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        errorPrint("create supertable %s failed!\n\n", superTbl->stbName);
        free(command);
        return -1;
    }

    debugPrint("create supertable %s success!\n\n", superTbl->stbName);
    free(command);
    return 0;
}

int createDatabasesAndStables(char *command) {
    TAOS *taos = NULL;
    int   ret = 0;
    taos =
        taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, NULL, g_Dbs.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine, reason:%s\n",
                   taos_errstr(NULL));
        return -1;
    }

    for (int i = 0; i < g_Dbs.dbCount; i++) {
        if (g_Dbs.db[i].drop) {
            sprintf(command, "drop database if exists %s;", g_Dbs.db[i].dbName);
            if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
                taos_close(taos);
                return -1;
            }

            int dataLen = 0;
            dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                "CREATE DATABASE IF NOT EXISTS %s",
                                g_Dbs.db[i].dbName);

            if (g_Dbs.db[i].dbCfg.blocks > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " BLOCKS %d", g_Dbs.db[i].dbCfg.blocks);
            }
            if (g_Dbs.db[i].dbCfg.cache > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " CACHE %d", g_Dbs.db[i].dbCfg.cache);
            }
            if (g_Dbs.db[i].dbCfg.days > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " DAYS %d", g_Dbs.db[i].dbCfg.days);
            }
            if (g_Dbs.db[i].dbCfg.keep > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " KEEP %d", g_Dbs.db[i].dbCfg.keep);
            }
            if (g_Dbs.db[i].dbCfg.quorum > 1) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " QUORUM %d", g_Dbs.db[i].dbCfg.quorum);
            }
            if (g_Dbs.db[i].dbCfg.replica > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " REPLICA %d", g_Dbs.db[i].dbCfg.replica);
            }
            if (g_Dbs.db[i].dbCfg.update > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " UPDATE %d", g_Dbs.db[i].dbCfg.update);
            }
            // if (g_Dbs.db[i].dbCfg.maxtablesPerVnode > 0) {
            //  dataLen += snprintf(command + dataLen,
            //  BUFFER_SIZE - dataLen, "tables %d ",
            //  g_Dbs.db[i].dbCfg.maxtablesPerVnode);
            //}
            if (g_Dbs.db[i].dbCfg.minRows > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " MINROWS %d", g_Dbs.db[i].dbCfg.minRows);
            }
            if (g_Dbs.db[i].dbCfg.maxRows > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " MAXROWS %d", g_Dbs.db[i].dbCfg.maxRows);
            }
            if (g_Dbs.db[i].dbCfg.comp > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " COMP %d", g_Dbs.db[i].dbCfg.comp);
            }
            if (g_Dbs.db[i].dbCfg.walLevel > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " wal %d", g_Dbs.db[i].dbCfg.walLevel);
            }
            if (g_Dbs.db[i].dbCfg.cacheLast > 0) {
                dataLen +=
                    snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                             " CACHELAST %d", g_Dbs.db[i].dbCfg.cacheLast);
            }
            if (g_Dbs.db[i].dbCfg.fsync > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " FSYNC %d", g_Dbs.db[i].dbCfg.fsync);
            }
            if ((0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ms", 2)) ||
                (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ns", 2)) ||
                (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "us", 2))) {
                dataLen +=
                    snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                             " precision \'%s\';", g_Dbs.db[i].dbCfg.precision);
            }

            if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
                taos_close(taos);
                errorPrint("\ncreate database %s failed!\n\n",
                           g_Dbs.db[i].dbName);
                return -1;
            }
            printf("\ncreate database %s success!\n\n", g_Dbs.db[i].dbName);
        }

        debugPrint("%s() LN%d supertbl count:%" PRIu64 "\n", __func__, __LINE__,
                   g_Dbs.db[i].superTblCount);

        int validStbCount = 0;

        for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {
            if (g_Dbs.db[i].superTbls[j].iface == SML_IFACE) {
                goto skip;
            }

            sprintf(command, "describe %s.%s;", g_Dbs.db[i].dbName,
                    g_Dbs.db[i].superTbls[j].stbName);
            ret = queryDbExec(taos, command, NO_INSERT_TYPE, true);

            if ((ret != 0) || (g_Dbs.db[i].drop)) {
                ret = createSuperTable(taos, g_Dbs.db[i].dbName,
                                       &g_Dbs.db[i].superTbls[j]);

                if (0 != ret) {
                    errorPrint("create super table %" PRIu64 " failed!\n\n", j);
                    continue;
                }
            } else {
                ret = getSuperTableFromServer(taos, g_Dbs.db[i].dbName,
                                              &g_Dbs.db[i].superTbls[j]);
                if (0 != ret) {
                    errorPrint("\nget super table %s.%s info failed!\n\n",
                               g_Dbs.db[i].dbName,
                               g_Dbs.db[i].superTbls[j].stbName);
                    continue;
                }
            }
        skip:
            validStbCount++;
        }
        g_Dbs.db[i].superTblCount = validStbCount;
    }

    taos_close(taos);
    return 0;
}

static void *createTable(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    setThreadName("createTable");

    uint64_t lastPrintTime = taosGetTimestampMs();

    int buff_len = BUFFER_SIZE;

    pThreadInfo->buffer = calloc(buff_len, 1);
    if (pThreadInfo->buffer == NULL) {
        errorPrint("%s() LN%d, Memory allocated failed!\n", __func__, __LINE__);
        exit(EXIT_FAILURE);
    }

    int len = 0;
    int batchNum = 0;

    verbosePrint("%s() LN%d: Creating table from %" PRIu64 " to %" PRIu64 "\n",
                 __func__, __LINE__, pThreadInfo->start_table_from,
                 pThreadInfo->end_table_to);

    for (uint64_t i = pThreadInfo->start_table_from;
         i <= pThreadInfo->end_table_to; i++) {
        if (0 == g_Dbs.use_metric) {
            snprintf(pThreadInfo->buffer, buff_len,
                     g_args.escapeChar
                         ? "CREATE TABLE IF NOT EXISTS %s.`%s%" PRIu64 "` %s;"
                         : "CREATE TABLE IF NOT EXISTS %s.%s%" PRIu64 " %s;",
                     pThreadInfo->db_name, g_args.tb_prefix, i,
                     pThreadInfo->cols);
            batchNum++;
        } else {
            if (stbInfo == NULL) {
                free(pThreadInfo->buffer);
                errorPrint(
                    "%s() LN%d, use metric, but super table info is NULL\n",
                    __func__, __LINE__);
                exit(EXIT_FAILURE);
            } else {
                if (0 == len) {
                    batchNum = 0;
                    memset(pThreadInfo->buffer, 0, buff_len);
                    len += snprintf(pThreadInfo->buffer + len, buff_len - len,
                                    "CREATE TABLE ");
                }

                char *tagsValBuf = NULL;
                if (0 == stbInfo->tagSource) {
                    tagsValBuf = generateTagValuesForStb(stbInfo, i);
                } else {
                    if (0 == stbInfo->tagSampleCount) {
                        free(pThreadInfo->buffer);
                        ERROR_EXIT(
                            "use sample file for tag, but has no content!\n");
                    }
                    tagsValBuf = getTagValueFromTagSample(
                        stbInfo, i % stbInfo->tagSampleCount);
                }

                if (NULL == tagsValBuf) {
                    free(pThreadInfo->buffer);
                    ERROR_EXIT("use metric, but tag buffer is NULL\n");
                }
                len += snprintf(
                    pThreadInfo->buffer + len, buff_len - len,
                    stbInfo->escapeChar ? "if not exists %s.`%s%" PRIu64
                                          "` using %s.`%s` tags %s "
                                        : "if not exists %s.%s%" PRIu64
                                          " using %s.%s tags %s ",
                    pThreadInfo->db_name, stbInfo->childTblPrefix, i,
                    pThreadInfo->db_name, stbInfo->stbName, tagsValBuf);
                free(tagsValBuf);
                batchNum++;
                if ((batchNum < stbInfo->batchCreateTableNum) &&
                    ((buff_len - len) >=
                     (stbInfo->lenOfTagOfOneRow + EXTRA_SQL_LEN))) {
                    continue;
                }
            }
        }

        len = 0;

        if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                             NO_INSERT_TYPE, false)) {
            errorPrint("queryDbExec() failed. buffer:\n%s\n",
                       pThreadInfo->buffer);
            free(pThreadInfo->buffer);
            return NULL;
        }
        pThreadInfo->tables_created += batchNum;
        uint64_t currentPrintTime = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > PRINT_STAT_INTERVAL) {
            printf("thread[%d] already create %" PRIu64 " - %" PRIu64
                   " tables\n",
                   pThreadInfo->threadID, pThreadInfo->start_table_from, i);
            lastPrintTime = currentPrintTime;
        }
    }

    if (0 != len) {
        if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                             NO_INSERT_TYPE, false)) {
            errorPrint("queryDbExec() failed. buffer:\n%s\n",
                       pThreadInfo->buffer);
        }
        pThreadInfo->tables_created += batchNum;
    }
    free(pThreadInfo->buffer);
    return NULL;
}

int startMultiThreadCreateChildTable(char *cols, int threads,
                                     uint64_t tableFrom, int64_t ntables,
                                     char *db_name, SSuperTable *stbInfo) {
    pthread_t * pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));

    assert(pids);
    assert(infos);

    if (threads < 1) {
        threads = 1;
    }

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = ntables;
        a = 1;
    }

    int64_t b = 0;
    b = ntables % threads;

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;
        tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
        pThreadInfo->stbInfo = stbInfo;
        verbosePrint("%s() %d db_name: %s\n", __func__, __LINE__, db_name);
        pThreadInfo->taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password,
                                         db_name, g_Dbs.port);
        if (pThreadInfo->taos == NULL) {
            errorPrint("%s() LN%d, Failed to connect to TDengine, reason:%s\n",
                       __func__, __LINE__, taos_errstr(NULL));
            free(pids);
            free(infos);
            return -1;
        }

        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->use_metric = true;
        pThreadInfo->cols = cols;
        pThreadInfo->minDelay = UINT64_MAX;
        pThreadInfo->tables_created = 0;
        pthread_create(pids + i, NULL, createTable, pThreadInfo);
    }

    for (int i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        taos_close(pThreadInfo->taos);

        g_actualChildTables += pThreadInfo->tables_created;
    }

    free(pids);
    free(infos);

    return 0;
}

int createChildTables() {
    int32_t code = 0;
    fprintf(stderr, "creating %" PRId64 " table(s) with %d thread(s)\n\n",
            g_totalChildTables, g_Dbs.threadCountForCreateTbl);
    if (g_fpOfInsertResult) {
        fprintf(g_fpOfInsertResult,
                "creating %" PRId64 " table(s) with %d thread(s)\n\n",
                g_totalChildTables, g_Dbs.threadCountForCreateTbl);
    }
    double start = taosGetTimestampMs();
    char   tblColsBuf[TSDB_MAX_BYTES_PER_ROW];
    int    len;

    for (int i = 0; i < g_Dbs.dbCount; i++) {
        if (g_Dbs.use_metric) {
            if (g_Dbs.db[i].superTblCount > 0) {
                // with super table
                for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
                    if ((AUTO_CREATE_SUBTBL ==
                         g_Dbs.db[i].superTbls[j].autoCreateTable) ||
                        (TBL_ALREADY_EXISTS ==
                         g_Dbs.db[i].superTbls[j].childTblExists)) {
                        continue;
                    }
                    verbosePrint(
                        "%s() LN%d: %s\n", __func__, __LINE__,
                        g_Dbs.db[i].superTbls[j].colsOfCreateChildTable);
                    uint64_t startFrom = 0;

                    verbosePrint("%s() LN%d: create %" PRId64
                                 " child tables from %" PRIu64 "\n",
                                 __func__, __LINE__, g_totalChildTables,
                                 startFrom);

                    code = startMultiThreadCreateChildTable(
                        g_Dbs.db[i].superTbls[j].colsOfCreateChildTable,
                        g_Dbs.threadCountForCreateTbl, startFrom,
                        g_Dbs.db[i].superTbls[j].childTblCount,
                        g_Dbs.db[i].dbName, &(g_Dbs.db[i].superTbls[j]));
                    if (code) {
                        errorPrint(
                            "%s() LN%d, startMultiThreadCreateChildTable() "
                            "failed for db %d stable %d\n",
                            __func__, __LINE__, i, j);
                        return code;
                    }
                }
            }
        } else {
            // normal table
            len = snprintf(tblColsBuf, TSDB_MAX_BYTES_PER_ROW, "(TS TIMESTAMP");
            for (int j = 0; j < g_args.columnCount; j++) {
                if ((strncasecmp(g_args.dataType[j], "BINARY",
                                 strlen("BINARY")) == 0) ||
                    (strncasecmp(g_args.dataType[j], "NCHAR",
                                 strlen("NCHAR")) == 0)) {
                    snprintf(tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len,
                             ",C%d %s(%d)", j, g_args.dataType[j],
                             g_args.binwidth);
                } else {
                    snprintf(tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len,
                             ",C%d %s", j, g_args.dataType[j]);
                }
                len = strlen(tblColsBuf);
            }

            snprintf(tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len, ")");

            verbosePrint("%s() LN%d: dbName: %s num of tb: %" PRId64
                         " schema: %s\n",
                         __func__, __LINE__, g_Dbs.db[i].dbName, g_args.ntables,
                         tblColsBuf);
            code = startMultiThreadCreateChildTable(
                tblColsBuf, g_Dbs.threadCountForCreateTbl, 0, g_args.ntables,
                g_Dbs.db[i].dbName, NULL);
            if (code) {
                errorPrint(
                    "%s() LN%d, startMultiThreadCreateChildTable() "
                    "failed\n",
                    __func__, __LINE__);
                return code;
            }
        }
    }
    double end = taosGetTimestampMs();
    fprintf(stderr,
            "\nSpent %.4f seconds to create %" PRId64
            " table(s) with %d thread(s), actual %" PRId64
            " table(s) created\n\n",
            (end - start) / 1000.0, g_totalChildTables,
            g_Dbs.threadCountForCreateTbl, g_actualChildTables);
    if (g_fpOfInsertResult) {
        fprintf(g_fpOfInsertResult,
                "\nSpent %.4f seconds to create %" PRId64
                " table(s) with %d thread(s), actual %" PRId64
                " table(s) created\n\n",
                (end - start) / 1000.0, g_totalChildTables,
                g_Dbs.threadCountForCreateTbl, g_actualChildTables);
    }
    return code;
}

/*
   Read 10000 lines at most. If more than 10000 lines, continue to read after
   using
   */
static int readTagFromCsvFileToMem(SSuperTable *stbInfo) {
    size_t  n = 0;
    ssize_t readLen = 0;
    char *  line = NULL;

    FILE *fp = fopen(stbInfo->tagsFile, "r");
    if (fp == NULL) {
        printf("Failed to open tags file: %s, reason:%s\n", stbInfo->tagsFile,
               strerror(errno));
        return -1;
    }

    if (stbInfo->tagDataBuf) {
        free(stbInfo->tagDataBuf);
        stbInfo->tagDataBuf = NULL;
    }

    int   tagCount = MAX_SAMPLES;
    int   count = 0;
    char *tagDataBuf = calloc(1, stbInfo->lenOfTagOfOneRow * tagCount);
    if (tagDataBuf == NULL) {
        printf("Failed to calloc, reason:%s\n", strerror(errno));
        fclose(fp);
        return -1;
    }

    while ((readLen = tgetline(&line, &n, fp)) != -1) {
        if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
            line[--readLen] = 0;
        }

        if (readLen == 0) {
            continue;
        }

        memcpy(tagDataBuf + count * stbInfo->lenOfTagOfOneRow, line, readLen);
        count++;

        if (count >= tagCount - 1) {
            char *tmp = realloc(
                tagDataBuf, (size_t)tagCount * 1.5 * stbInfo->lenOfTagOfOneRow);
            if (tmp != NULL) {
                tagDataBuf = tmp;
                tagCount = (int)(tagCount * 1.5);
                memset(
                    tagDataBuf + count * stbInfo->lenOfTagOfOneRow, 0,
                    (size_t)((tagCount - count) * stbInfo->lenOfTagOfOneRow));
            } else {
                // exit, if allocate more memory failed
                printf("realloc fail for save tag val from %s\n",
                       stbInfo->tagsFile);
                tmfree(tagDataBuf);
                free(line);
                fclose(fp);
                return -1;
            }
        }
    }

    stbInfo->tagDataBuf = tagDataBuf;
    stbInfo->tagSampleCount = count;

    free(line);
    fclose(fp);
    return 0;
}

static void getAndSetRowsFromCsvFile(SSuperTable *stbInfo) {
    FILE *fp = fopen(stbInfo->sampleFile, "r");
    int   line_count = 0;
    if (fp == NULL) {
        errorPrint("Failed to open sample file: %s, reason:%s\n",
                   stbInfo->sampleFile, strerror(errno));
        exit(EXIT_FAILURE);
    }
    char *buf = calloc(1, stbInfo->maxSqlLen);
    while (fgets(buf, stbInfo->maxSqlLen, fp)) {
        line_count++;
    }
    fclose(fp);
    tmfree(buf);
    stbInfo->insertRows = line_count;
}

/*
   Read 10000 lines at most. If more than 10000 lines, continue to read after
   using
   */
static int generateSampleFromCsvForStb(SSuperTable *stbInfo) {
    size_t  n = 0;
    ssize_t readLen = 0;
    char *  line = NULL;
    int     getRows = 0;

    FILE *fp = fopen(stbInfo->sampleFile, "r");
    if (fp == NULL) {
        errorPrint("Failed to open sample file: %s, reason:%s\n",
                   stbInfo->sampleFile, strerror(errno));
        return -1;
    }

    assert(stbInfo->sampleDataBuf);
    memset(stbInfo->sampleDataBuf, 0, MAX_SAMPLES * stbInfo->lenOfOneRow);
    while (1) {
        readLen = tgetline(&line, &n, fp);
        if (-1 == readLen) {
            if (0 != fseek(fp, 0, SEEK_SET)) {
                errorPrint("Failed to fseek file: %s, reason:%s\n",
                           stbInfo->sampleFile, strerror(errno));
                fclose(fp);
                return -1;
            }
            continue;
        }

        if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
            line[--readLen] = 0;
        }

        if (readLen == 0) {
            continue;
        }

        if (readLen > stbInfo->lenOfOneRow) {
            printf("sample row len[%d] overflow define schema len[%" PRIu64
                   "], so discard this row\n",
                   (int32_t)readLen, stbInfo->lenOfOneRow);
            continue;
        }

        memcpy(stbInfo->sampleDataBuf + getRows * stbInfo->lenOfOneRow, line,
               readLen);
        getRows++;

        if (getRows == MAX_SAMPLES) {
            break;
        }
    }

    fclose(fp);
    tmfree(line);
    return 0;
}

static int prepareSampleData() {
    for (int i = 0; i < g_Dbs.dbCount; i++) {
        for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
            if (g_Dbs.db[i].superTbls[j].tagsFile[0] != 0) {
                if (readTagFromCsvFileToMem(&g_Dbs.db[i].superTbls[j]) != 0) {
                    return -1;
                }
            }
        }
    }

    return 0;
}

void postFreeResource() {
    tmfclose(g_fpOfInsertResult);

    for (int i = 0; i < g_Dbs.dbCount; i++) {
        for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {
            if (0 != g_Dbs.db[i].superTbls[j].colsOfCreateChildTable) {
                tmfree(g_Dbs.db[i].superTbls[j].colsOfCreateChildTable);
                g_Dbs.db[i].superTbls[j].colsOfCreateChildTable = NULL;
            }
            if (0 != g_Dbs.db[i].superTbls[j].sampleDataBuf) {
                tmfree(g_Dbs.db[i].superTbls[j].sampleDataBuf);
                g_Dbs.db[i].superTbls[j].sampleDataBuf = NULL;
            }

#if STMT_BIND_PARAM_BATCH == 1
            for (int c = 0; c < g_Dbs.db[i].superTbls[j].columnCount; c++) {
                if (g_Dbs.db[i].superTbls[j].sampleBindBatchArray) {
                    tmfree((char *)((uintptr_t) *
                                    (uintptr_t *)(g_Dbs.db[i]
                                                      .superTbls[j]
                                                      .sampleBindBatchArray +
                                                  sizeof(char *) * c)));
                }
            }
            tmfree(g_Dbs.db[i].superTbls[j].sampleBindBatchArray);
#endif
            if (0 != g_Dbs.db[i].superTbls[j].tagDataBuf) {
                tmfree(g_Dbs.db[i].superTbls[j].tagDataBuf);
                g_Dbs.db[i].superTbls[j].tagDataBuf = NULL;
            }
            if (0 != g_Dbs.db[i].superTbls[j].childTblName) {
                tmfree(g_Dbs.db[i].superTbls[j].childTblName);
                g_Dbs.db[i].superTbls[j].childTblName = NULL;
            }
        }
        tmfree(g_Dbs.db[i].superTbls);
    }
    tmfree(g_Dbs.db);
    tmfree(g_randbool_buff);
    tmfree(g_randint_buff);
    tmfree(g_rand_voltage_buff);
    tmfree(g_randbigint_buff);
    tmfree(g_randsmallint_buff);
    tmfree(g_randtinyint_buff);
    tmfree(g_randfloat_buff);
    tmfree(g_rand_current_buff);
    tmfree(g_rand_phase_buff);

    tmfree(g_sampleDataBuf);

#if STMT_BIND_PARAM_BATCH == 1
    for (int l = 0; l < g_args.columnCount; l++) {
        if (g_sampleBindBatchArray) {
            tmfree((char *)((uintptr_t) * (uintptr_t *)(g_sampleBindBatchArray +
                                                        sizeof(char *) * l)));
        }
    }
    tmfree(g_sampleBindBatchArray);

#endif
}

static int getRowDataFromSample(char *dataBuf, int64_t maxLen,
                                int64_t timestamp, SSuperTable *stbInfo,
                                int64_t *sampleUsePos) {
    if ((*sampleUsePos) == MAX_SAMPLES) {
        *sampleUsePos = 0;
    }

    int dataLen = 0;
    if (stbInfo->useSampleTs) {
        dataLen += snprintf(
            dataBuf + dataLen, maxLen - dataLen, "(%s",
            stbInfo->sampleDataBuf + stbInfo->lenOfOneRow * (*sampleUsePos));
    } else {
        dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen,
                            "(%" PRId64 ", ", timestamp);
        dataLen += snprintf(
            dataBuf + dataLen, maxLen - dataLen, "%s",
            stbInfo->sampleDataBuf + stbInfo->lenOfOneRow * (*sampleUsePos));
    }
    dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, ")");

    (*sampleUsePos)++;

    return dataLen;
}

static int64_t generateStbRowData(SSuperTable *stbInfo, char *recBuf,
                                  int64_t remainderBufLen, int64_t timestamp) {
    int64_t dataLen = 0;
    char *  pstr = recBuf;
    int64_t maxLen = MAX_DATA_SIZE;
    int     tmpLen;

    dataLen +=
        snprintf(pstr + dataLen, maxLen - dataLen, "(%" PRId64 "", timestamp);

    for (int i = 0; i < stbInfo->columnCount; i++) {
        tstrncpy(pstr + dataLen, ",", 2);
        dataLen += 1;

        if ((stbInfo->columns[i].data_type == TSDB_DATA_TYPE_BINARY) ||
            (stbInfo->columns[i].data_type == TSDB_DATA_TYPE_NCHAR)) {
            if (stbInfo->columns[i].dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("binary or nchar length overflow, max size:%u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }

            if ((stbInfo->columns[i].dataLen + 1) >
                /* need count 3 extra chars \', \', and , */
                (remainderBufLen - dataLen - 3)) {
                return 0;
            }
            char *buf = (char *)calloc(stbInfo->columns[i].dataLen + 1, 1);
            if (NULL == buf) {
                errorPrint("calloc failed! size:%d\n",
                           stbInfo->columns[i].dataLen);
                return -1;
            }
            rand_string(buf, stbInfo->columns[i].dataLen);
            dataLen +=
                snprintf(pstr + dataLen, maxLen - dataLen, "\'%s\'", buf);
            tmfree(buf);

        } else {
            char *tmp = NULL;
            switch (stbInfo->columns[i].data_type) {
                case TSDB_DATA_TYPE_INT:
                    if ((g_args.demo_mode) && (i == 1)) {
                        tmp = demo_voltage_int_str();
                    } else {
                        tmp = rand_int_str();
                    }
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, INT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_UINT:
                    tmp = rand_uint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, INT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    tmp = rand_bigint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, BIGINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_UBIGINT:
                    tmp = rand_ubigint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, BIGINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (g_args.demo_mode) {
                        if (i == 0) {
                            tmp = demo_current_float_str();
                        } else {
                            tmp = demo_phase_float_str();
                        }
                    } else {
                        tmp = rand_float_str();
                    }
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, FLOAT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    tmp = rand_double_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, DOUBLE_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    tmp = rand_smallint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, SMALLINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    tmp = rand_usmallint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, SMALLINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    tmp = rand_tinyint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, TINYINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    tmp = rand_utinyint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, TINYINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    tmp = rand_bool_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, BOOL_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    tmp = rand_bigint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp,
                             min(tmpLen + 1, BIGINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_NULL:
                    break;

                default:
                    errorPrint("Not support data type: %s\n",
                               stbInfo->columns[i].dataType);
                    exit(EXIT_FAILURE);
            }
            if (tmp) {
                dataLen += tmpLen;
            }
        }

        if (dataLen > (remainderBufLen - (128))) return 0;
    }

    dataLen += snprintf(pstr + dataLen, 2, ")");

    verbosePrint("%s() LN%d, dataLen:%" PRId64 "\n", __func__, __LINE__,
                 dataLen);
    verbosePrint("%s() LN%d, recBuf:\n\t%s\n", __func__, __LINE__, recBuf);

    return strlen(recBuf);
}

static int64_t generateData(char *recBuf, char *data_type, int64_t timestamp,
                            int lenOfBinary) {
    memset(recBuf, 0, MAX_DATA_SIZE);
    char *pstr = recBuf;
    pstr += sprintf(pstr, "(%" PRId64 "", timestamp);

    int columnCount = g_args.columnCount;

    bool  b;
    char *s;
    for (int i = 0; i < columnCount; i++) {
        switch (data_type[i]) {
            case TSDB_DATA_TYPE_TINYINT:
                pstr += sprintf(pstr, ",%d", rand_tinyint());
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                pstr += sprintf(pstr, ",%d", rand_smallint());
                break;

            case TSDB_DATA_TYPE_INT:
                pstr += sprintf(pstr, ",%d", rand_int());
                break;

            case TSDB_DATA_TYPE_BIGINT:
                pstr += sprintf(pstr, ",%" PRId64 "", rand_bigint());
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                pstr += sprintf(pstr, ",%" PRId64 "", rand_bigint());
                break;

            case TSDB_DATA_TYPE_FLOAT:
                pstr += sprintf(pstr, ",%10.4f", rand_float());
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                pstr += sprintf(pstr, ",%20.8f", rand_double());
                break;

            case TSDB_DATA_TYPE_BOOL:
                b = rand_bool() & 1;
                pstr += sprintf(pstr, ",%s", b ? "true" : "false");
                break;

            case TSDB_DATA_TYPE_BINARY:
                s = malloc(lenOfBinary + 1);
                if (s == NULL) {
                    errorPrint("%s() LN%d, memory allocation %d bytes failed\n",
                               __func__, __LINE__, lenOfBinary + 1);
                    exit(EXIT_FAILURE);
                }
                rand_string(s, lenOfBinary);
                pstr += sprintf(pstr, ",\"%s\"", s);
                free(s);
                break;

            case TSDB_DATA_TYPE_NCHAR:
                s = malloc(lenOfBinary + 1);
                if (s == NULL) {
                    errorPrint("%s() LN%d, memory allocation %d bytes failed\n",
                               __func__, __LINE__, lenOfBinary + 1);
                    exit(EXIT_FAILURE);
                }
                rand_string(s, lenOfBinary);
                pstr += sprintf(pstr, ",\"%s\"", s);
                free(s);
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                pstr += sprintf(pstr, ",%d", rand_utinyint());
                break;

            case TSDB_DATA_TYPE_USMALLINT:
                pstr += sprintf(pstr, ",%d", rand_usmallint());
                break;

            case TSDB_DATA_TYPE_UINT:
                pstr += sprintf(pstr, ",%d", rand_uint());
                break;

            case TSDB_DATA_TYPE_UBIGINT:
                pstr += sprintf(pstr, ",%" PRId64 "", rand_ubigint());
                break;

            case TSDB_DATA_TYPE_NULL:
                break;

            default:
                errorPrint("%s() LN%d, Unknown data type %d\n", __func__,
                           __LINE__, data_type[i]);
                exit(EXIT_FAILURE);
        }

        if (strlen(recBuf) > MAX_DATA_SIZE) {
            ERROR_EXIT("column length too long, abort");
        }
    }

    pstr += sprintf(pstr, ")");

    verbosePrint("%s() LN%d, recBuf:\n\t%s\n", __func__, __LINE__, recBuf);

    return (int32_t)strlen(recBuf);
}

static int generateSampleFromRand(char *sampleDataBuf, uint64_t lenOfOneRow,
                                  int columnCount, StrColumn *columns) {
    char data[MAX_DATA_SIZE];
    memset(data, 0, MAX_DATA_SIZE);

    char *buff = malloc(lenOfOneRow);
    if (NULL == buff) {
        errorPrint("%s() LN%d, memory allocation %" PRIu64 " bytes failed\n",
                   __func__, __LINE__, lenOfOneRow);
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < MAX_SAMPLES; i++) {
        uint64_t pos = 0;
        memset(buff, 0, lenOfOneRow);

        for (int c = 0; c < columnCount; c++) {
            char *tmp = NULL;

            uint32_t dataLen;
            char     data_type =
                (columns) ? (columns[c].data_type) : g_args.data_type[c];

            switch (data_type) {
                case TSDB_DATA_TYPE_BINARY:
                    dataLen = (columns) ? columns[c].dataLen : g_args.binwidth;
                    rand_string(data, dataLen);
                    pos += sprintf(buff + pos, "%s,", data);
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                    dataLen = (columns) ? columns[c].dataLen : g_args.binwidth;
                    rand_string(data, dataLen - 1);
                    pos += sprintf(buff + pos, "%s,", data);
                    break;

                case TSDB_DATA_TYPE_INT:
                    if ((g_args.demo_mode) && (c == 1)) {
                        tmp = demo_voltage_int_str();
                    } else {
                        tmp = rand_int_str();
                    }
                    pos += sprintf(buff + pos, "%s,", tmp);
                    break;

                case TSDB_DATA_TYPE_UINT:
                    pos += sprintf(buff + pos, "%s,", rand_uint_str());
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    pos += sprintf(buff + pos, "%s,", rand_bigint_str());
                    break;

                case TSDB_DATA_TYPE_UBIGINT:
                    pos += sprintf(buff + pos, "%s,", rand_ubigint_str());
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (g_args.demo_mode) {
                        if (c == 0) {
                            tmp = demo_current_float_str();
                        } else {
                            tmp = demo_phase_float_str();
                        }
                    } else {
                        tmp = rand_float_str();
                    }
                    pos += sprintf(buff + pos, "%s,", tmp);
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    pos += sprintf(buff + pos, "%s,", rand_double_str());
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    pos += sprintf(buff + pos, "%s,", rand_smallint_str());
                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    pos += sprintf(buff + pos, "%s,", rand_usmallint_str());
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    pos += sprintf(buff + pos, "%s,", rand_tinyint_str());
                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    pos += sprintf(buff + pos, "%s,", rand_utinyint_str());
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    pos += sprintf(buff + pos, "%s,", rand_bool_str());
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    pos += sprintf(buff + pos, "%s,", rand_bigint_str());
                    break;

                case TSDB_DATA_TYPE_NULL:
                    break;

                default:
                    errorPrint(
                        "%s() LN%d, Unknown data type %s\n", __func__, __LINE__,
                        (columns) ? (columns[c].dataType) : g_args.dataType[c]);
                    exit(EXIT_FAILURE);
            }
        }

        *(buff + pos - 1) = 0;
        memcpy(sampleDataBuf + i * lenOfOneRow, buff, pos);
    }

    free(buff);
    return 0;
}

static int generateSampleFromRandForNtb() {
    return generateSampleFromRand(g_sampleDataBuf, g_args.lenOfOneRow,
                                  g_args.columnCount, NULL);
}

static int generateSampleFromRandForStb(SSuperTable *stbInfo) {
    return generateSampleFromRand(stbInfo->sampleDataBuf, stbInfo->lenOfOneRow,
                                  stbInfo->columnCount, stbInfo->columns);
}

static int prepareSampleForNtb() {
    g_sampleDataBuf = calloc(g_args.lenOfOneRow * MAX_SAMPLES, 1);
    if (NULL == g_sampleDataBuf) {
        errorPrint("%s() LN%d, Failed to calloc %" PRIu64 " Bytes, reason:%s\n",
                   __func__, __LINE__, g_args.lenOfOneRow * MAX_SAMPLES,
                   strerror(errno));
        return -1;
    }

    return generateSampleFromRandForNtb();
}

static int prepareSampleForStb(SSuperTable *stbInfo) {
    stbInfo->sampleDataBuf = calloc(stbInfo->lenOfOneRow * MAX_SAMPLES, 1);
    if (NULL == stbInfo->sampleDataBuf) {
        errorPrint("%s() LN%d, Failed to calloc %" PRIu64 " Bytes, reason:%s\n",
                   __func__, __LINE__, stbInfo->lenOfOneRow * MAX_SAMPLES,
                   strerror(errno));
        return -1;
    }

    int ret;
    if (0 == strncasecmp(stbInfo->dataSource, "sample", strlen("sample"))) {
        if (stbInfo->useSampleTs) {
            getAndSetRowsFromCsvFile(stbInfo);
        }
        ret = generateSampleFromCsvForStb(stbInfo);
    } else {
        ret = generateSampleFromRandForStb(stbInfo);
    }

    if (0 != ret) {
        errorPrint("%s() LN%d, read sample from csv file failed.\n", __func__,
                   __LINE__);
        tmfree(stbInfo->sampleDataBuf);
        stbInfo->sampleDataBuf = NULL;
        return -1;
    }

    return 0;
}

static int32_t execInsert(threadInfo *pThreadInfo, uint32_t k) {
    int32_t      affectedRows;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_RES *   res;
    int32_t      code;
    uint16_t     iface;
    if (stbInfo)
        iface = stbInfo->iface;
    else {
        if (g_args.iface == INTERFACE_BUT)
            iface = TAOSC_IFACE;
        else
            iface = g_args.iface;
    }

    debugPrint("[%d] %s() LN%d %s\n", pThreadInfo->threadID, __func__, __LINE__,
               (iface == TAOSC_IFACE)  ? "taosc"
               : (iface == REST_IFACE) ? "rest"
                                       : "stmt");

    switch (iface) {
        case TAOSC_IFACE:
            verbosePrint("[%d] %s() LN%d %s\n", pThreadInfo->threadID, __func__,
                         __LINE__, pThreadInfo->buffer);

            affectedRows = queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                                       INSERT_TYPE, false);
            break;

        case REST_IFACE:
            verbosePrint("[%d] %s() LN%d %s\n", pThreadInfo->threadID, __func__,
                         __LINE__, pThreadInfo->buffer);

            if (0 != postProceSql(g_Dbs.host, g_Dbs.port, pThreadInfo->buffer,
                                  pThreadInfo)) {
                affectedRows = -1;
                printf("========restful return fail, threadID[%d]\n",
                       pThreadInfo->threadID);
            } else {
                affectedRows = k;
            }
            break;

        case STMT_IFACE:
            debugPrint("%s() LN%d, stmt=%p", __func__, __LINE__,
                       pThreadInfo->stmt);
            if (0 != taos_stmt_execute(pThreadInfo->stmt)) {
                errorPrint(
                    "%s() LN%d, failied to execute insert statement. reason: "
                    "%s\n",
                    __func__, __LINE__, taos_stmt_errstr(pThreadInfo->stmt));

                fprintf(stderr,
                        "\n\033[31m === Please reduce batch number if WAL size "
                        "exceeds limit. ===\033[0m\n\n");
                exit(EXIT_FAILURE);
            }
            affectedRows = k;
            break;
        case SML_IFACE:
            res = taos_schemaless_insert(pThreadInfo->taos, pThreadInfo->lines,
                                         k, 0, pThreadInfo->time_precision);
            code = taos_errno(res);
            affectedRows = taos_affected_rows(res);
            if (code != TSDB_CODE_SUCCESS) {
                errorPrint(
                    "%s() LN%d, failed to execute schemaless insert. reason: "
                    "%s\n",
                    __func__, __LINE__, taos_errstr(res));
                exit(EXIT_FAILURE);
            }
            break;
        default:
            errorPrint("%s() LN%d: unknown insert mode: %d\n", __func__,
                       __LINE__, stbInfo->iface);
            affectedRows = 0;
    }

    return affectedRows;
}

static void getTableName(char *pTblName, threadInfo *pThreadInfo,
                         uint64_t tableSeq) {
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    if (stbInfo) {
        if (AUTO_CREATE_SUBTBL != stbInfo->autoCreateTable) {
            if (stbInfo->childTblLimit > 0) {
                snprintf(pTblName, TSDB_TABLE_NAME_LEN,
                         stbInfo->escapeChar ? "`%s`" : "%s",
                         stbInfo->childTblName +
                             (tableSeq - stbInfo->childTblOffset) *
                                 TSDB_TABLE_NAME_LEN);
            } else {
                verbosePrint("[%d] %s() LN%d: from=%" PRIu64 " count=%" PRId64
                             " seq=%" PRIu64 "\n",
                             pThreadInfo->threadID, __func__, __LINE__,
                             pThreadInfo->start_table_from,
                             pThreadInfo->ntables, tableSeq);
                snprintf(
                    pTblName, TSDB_TABLE_NAME_LEN,
                    stbInfo->escapeChar ? "`%s`" : "%s",
                    stbInfo->childTblName + tableSeq * TSDB_TABLE_NAME_LEN);
            }
        } else {
            snprintf(pTblName, TSDB_TABLE_NAME_LEN,
                     stbInfo->escapeChar ? "`%s%" PRIu64 "`" : "%s%" PRIu64 "",
                     stbInfo->childTblPrefix, tableSeq);
        }
    } else {
        snprintf(pTblName, TSDB_TABLE_NAME_LEN,
                 g_args.escapeChar ? "`%s%" PRIu64 "`" : "%s%" PRIu64 "",
                 g_args.tb_prefix, tableSeq);
    }
}

static int64_t getTSRandTail(int64_t timeStampStep, int32_t seq,
                             int disorderRatio, int disorderRange) {
    int64_t randTail = timeStampStep * seq;
    if (disorderRatio > 0) {
        int rand_num = taosRandom() % 100;
        if (rand_num < disorderRatio) {
            randTail = (randTail + (taosRandom() % disorderRange + 1)) * (-1);
            debugPrint("rand data generated, back %" PRId64 "\n", randTail);
        }
    }

    return randTail;
}

static int32_t generateDataTailWithoutStb(
    uint32_t batch, char *buffer, int64_t remainderBufLen, int64_t insertRows,
    uint64_t recordFrom, int64_t startTime,
    /* int64_t *pSamplePos, */ int64_t *dataLen) {
    uint64_t len = 0;
    char *   pstr = buffer;

    verbosePrint("%s() LN%d batch=%d\n", __func__, __LINE__, batch);

    int32_t k = 0;
    for (k = 0; k < batch;) {
        char *data = pstr;
        memset(data, 0, MAX_DATA_SIZE);

        int64_t retLen = 0;

        char *data_type = g_args.data_type;
        int   lenOfBinary = g_args.binwidth;

        if (g_args.disorderRatio) {
            retLen =
                generateData(data, data_type,
                             startTime + getTSRandTail(g_args.timestamp_step, k,
                                                       g_args.disorderRatio,
                                                       g_args.disorderRange),
                             lenOfBinary);
        } else {
            retLen = generateData(data, data_type,
                                  startTime + g_args.timestamp_step * k,
                                  lenOfBinary);
        }

        if (len > remainderBufLen) break;

        pstr += retLen;
        k++;
        len += retLen;
        remainderBufLen -= retLen;

        verbosePrint("%s() LN%d len=%" PRIu64 " k=%d \nbuffer=%s\n", __func__,
                     __LINE__, len, k, buffer);

        recordFrom++;

        if (recordFrom >= insertRows) {
            break;
        }
    }

    *dataLen = len;
    return k;
}

static int32_t generateStbDataTail(SSuperTable *stbInfo, uint32_t batch,
                                   char *buffer, int64_t remainderBufLen,
                                   int64_t insertRows, uint64_t recordFrom,
                                   int64_t startTime, int64_t *pSamplePos,
                                   int64_t *dataLen) {
    uint64_t len = 0;

    char *pstr = buffer;

    bool tsRand;
    if (0 == strncasecmp(stbInfo->dataSource, "rand", strlen("rand"))) {
        tsRand = true;
    } else {
        tsRand = false;
    }
    verbosePrint("%s() LN%d batch=%u buflen=%" PRId64 "\n", __func__, __LINE__,
                 batch, remainderBufLen);

    int32_t k;
    for (k = 0; k < batch;) {
        char *data = pstr;

        int64_t lenOfRow = 0;

        if (tsRand) {
            if (stbInfo->disorderRatio > 0) {
                lenOfRow = generateStbRowData(
                    stbInfo, data, remainderBufLen,
                    startTime + getTSRandTail(stbInfo->timeStampStep, k,
                                              stbInfo->disorderRatio,
                                              stbInfo->disorderRange));
            } else {
                lenOfRow =
                    generateStbRowData(stbInfo, data, remainderBufLen,
                                       startTime + stbInfo->timeStampStep * k);
            }
        } else {
            lenOfRow = getRowDataFromSample(
                data,
                (remainderBufLen < MAX_DATA_SIZE) ? remainderBufLen
                                                  : MAX_DATA_SIZE,
                startTime + stbInfo->timeStampStep * k, stbInfo, pSamplePos);
        }

        if (lenOfRow == 0) {
            data[0] = '\0';
            break;
        }
        if ((lenOfRow + 1) > remainderBufLen) {
            break;
        }

        pstr += lenOfRow;
        k++;
        len += lenOfRow;
        remainderBufLen -= lenOfRow;

        verbosePrint("%s() LN%d len=%" PRIu64 " k=%u \nbuffer=%s\n", __func__,
                     __LINE__, len, k, buffer);

        recordFrom++;

        if (recordFrom >= insertRows) {
            break;
        }
    }

    *dataLen = len;
    return k;
}

static int generateSQLHeadWithoutStb(char *tableName, char *dbName,
                                     char *buffer, int remainderBufLen) {
    int len;

    char headBuf[HEAD_BUFF_LEN];

    len = snprintf(headBuf, HEAD_BUFF_LEN, "%s.%s values", dbName, tableName);

    if (len > remainderBufLen) return -1;

    tstrncpy(buffer, headBuf, len + 1);

    return len;
}

static int generateStbSQLHead(SSuperTable *stbInfo, char *tableName,
                              int64_t tableSeq, char *dbName, char *buffer,
                              int remainderBufLen) {
    int len;

    char headBuf[HEAD_BUFF_LEN];

    if (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable) {
        char *tagsValBuf = NULL;
        if (0 == stbInfo->tagSource) {
            tagsValBuf = generateTagValuesForStb(stbInfo, tableSeq);
        } else {
            tagsValBuf = getTagValueFromTagSample(
                stbInfo, tableSeq % stbInfo->tagSampleCount);
        }
        if (NULL == tagsValBuf) {
            errorPrint("%s() LN%d, tag buf failed to allocate  memory\n",
                       __func__, __LINE__);
            return -1;
        }

        len =
            snprintf(headBuf, HEAD_BUFF_LEN, "%s.%s using %s.%s TAGS%s values",
                     dbName, tableName, dbName, stbInfo->stbName, tagsValBuf);
        tmfree(tagsValBuf);
    } else if (TBL_ALREADY_EXISTS == stbInfo->childTblExists) {
        len =
            snprintf(headBuf, HEAD_BUFF_LEN, "%s.%s values", dbName, tableName);
    } else {
        len =
            snprintf(headBuf, HEAD_BUFF_LEN, "%s.%s values", dbName, tableName);
    }

    if (len > remainderBufLen) return -1;

    tstrncpy(buffer, headBuf, len + 1);

    return len;
}

static int32_t generateStbInterlaceData(threadInfo *pThreadInfo,
                                        char *tableName, uint32_t batchPerTbl,
                                        uint64_t i, uint32_t batchPerTblTimes,
                                        uint64_t tableSeq, char *buffer,
                                        int64_t insertRows, int64_t startTime,
                                        uint64_t *pRemainderBufLen) {
    assert(buffer);
    char *pstr = buffer;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int          headLen =
        generateStbSQLHead(stbInfo, tableName, tableSeq, pThreadInfo->db_name,
                           pstr, *pRemainderBufLen);

    if (headLen <= 0) {
        return 0;
    }
    // generate data buffer
    verbosePrint("[%d] %s() LN%d i=%" PRIu64 " buffer:\n%s\n",
                 pThreadInfo->threadID, __func__, __LINE__, i, buffer);

    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen = 0;

    verbosePrint("[%d] %s() LN%d i=%" PRIu64
                 " batchPerTblTimes=%u batchPerTbl = %u\n",
                 pThreadInfo->threadID, __func__, __LINE__, i, batchPerTblTimes,
                 batchPerTbl);

    if (0 == strncasecmp(stbInfo->startTimestamp, "now", 3)) {
        startTime = taosGetTimestamp(pThreadInfo->time_precision);
    }

    int32_t k = generateStbDataTail(stbInfo, batchPerTbl, pstr,
                                    *pRemainderBufLen, insertRows, 0, startTime,
                                    &(pThreadInfo->samplePos), &dataLen);

    if (k == batchPerTbl) {
        pstr += dataLen;
        *pRemainderBufLen -= dataLen;
    } else {
        debugPrint(
            "%s() LN%d, generated data tail: %u, not equal batch per table: "
            "%u\n",
            __func__, __LINE__, k, batchPerTbl);
        pstr -= headLen;
        pstr[0] = '\0';
        k = 0;
    }

    return k;
}

static int64_t generateInterlaceDataWithoutStb(char *tableName, uint32_t batch,
                                               uint64_t tableSeq, char *dbName,
                                               char *buffer, int64_t insertRows,
                                               int64_t   startTime,
                                               uint64_t *pRemainderBufLen) {
    assert(buffer);
    char *pstr = buffer;

    int headLen =
        generateSQLHeadWithoutStb(tableName, dbName, pstr, *pRemainderBufLen);

    if (headLen <= 0) {
        return 0;
    }

    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen = 0;

    int32_t k = generateDataTailWithoutStb(batch, pstr, *pRemainderBufLen,
                                           insertRows, 0, startTime, &dataLen);

    if (k == batch) {
        pstr += dataLen;
        *pRemainderBufLen -= dataLen;
    } else {
        debugPrint(
            "%s() LN%d, generated data tail: %d, not equal batch per table: "
            "%u\n",
            __func__, __LINE__, k, batch);
        pstr -= headLen;
        pstr[0] = '\0';
        k = 0;
    }

    return k;
}

static int32_t prepareStmtBindArrayByType(TAOS_BIND *bind, char data_type,
                                          int32_t dataLen, int32_t timePrec,
                                          char *value) {
    int32_t * bind_int;
    uint32_t *bind_uint;
    int64_t * bind_bigint;
    uint64_t *bind_ubigint;
    float *   bind_float;
    double *  bind_double;
    int8_t *  bind_bool;
    int64_t * bind_ts2;
    int16_t * bind_smallint;
    uint16_t *bind_usmallint;
    int8_t *  bind_tinyint;
    uint8_t * bind_utinyint;

    switch (data_type) {
        case TSDB_DATA_TYPE_BINARY:
            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("binary length overflow, max size:%u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_binary;

            bind->buffer_type = TSDB_DATA_TYPE_BINARY;
            if (value) {
                bind_binary = calloc(1, strlen(value) + 1);
                strncpy(bind_binary, value, strlen(value));
                bind->buffer_length = strlen(bind_binary);
            } else {
                bind_binary = calloc(1, dataLen + 1);
                rand_string(bind_binary, dataLen);
                bind->buffer_length = dataLen;
            }

            bind->length = &bind->buffer_length;
            bind->buffer = bind_binary;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_NCHAR:
            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("nchar length overflow, max size:%u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_nchar;

            bind->buffer_type = TSDB_DATA_TYPE_NCHAR;
            if (value) {
                bind_nchar = calloc(1, strlen(value) + 1);
                strncpy(bind_nchar, value, strlen(value));
            } else {
                bind_nchar = calloc(1, dataLen + 1);
                rand_string(bind_nchar, dataLen);
            }

            bind->buffer_length = strlen(bind_nchar);
            bind->buffer = bind_nchar;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_INT:
            bind_int = malloc(sizeof(int32_t));
            assert(bind_int);

            if (value) {
                *bind_int = atoi(value);
            } else {
                *bind_int = rand_int();
            }
            bind->buffer_type = TSDB_DATA_TYPE_INT;
            bind->buffer_length = sizeof(int32_t);
            bind->buffer = bind_int;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_UINT:
            bind_uint = malloc(sizeof(uint32_t));
            assert(bind_uint);

            if (value) {
                *bind_uint = atoi(value);
            } else {
                *bind_uint = rand_int();
            }
            bind->buffer_type = TSDB_DATA_TYPE_UINT;
            bind->buffer_length = sizeof(uint32_t);
            bind->buffer = bind_uint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_BIGINT:
            bind_bigint = malloc(sizeof(int64_t));
            assert(bind_bigint);

            if (value) {
                *bind_bigint = atoll(value);
            } else {
                *bind_bigint = rand_bigint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_BIGINT;
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = bind_bigint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_UBIGINT:
            bind_ubigint = malloc(sizeof(uint64_t));
            assert(bind_ubigint);

            if (value) {
                *bind_ubigint = atoll(value);
            } else {
                *bind_ubigint = rand_bigint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_UBIGINT;
            bind->buffer_length = sizeof(uint64_t);
            bind->buffer = bind_ubigint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_FLOAT:
            bind_float = malloc(sizeof(float));
            assert(bind_float);

            if (value) {
                *bind_float = (float)atof(value);
            } else {
                *bind_float = rand_float();
            }
            bind->buffer_type = TSDB_DATA_TYPE_FLOAT;
            bind->buffer_length = sizeof(float);
            bind->buffer = bind_float;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            bind_double = malloc(sizeof(double));
            assert(bind_double);

            if (value) {
                *bind_double = atof(value);
            } else {
                *bind_double = rand_double();
            }
            bind->buffer_type = TSDB_DATA_TYPE_DOUBLE;
            bind->buffer_length = sizeof(double);
            bind->buffer = bind_double;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            bind_smallint = malloc(sizeof(int16_t));
            assert(bind_smallint);

            if (value) {
                *bind_smallint = (int16_t)atoi(value);
            } else {
                *bind_smallint = rand_smallint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
            bind->buffer_length = sizeof(int16_t);
            bind->buffer = bind_smallint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_USMALLINT:
            bind_usmallint = malloc(sizeof(uint16_t));
            assert(bind_usmallint);

            if (value) {
                *bind_usmallint = (uint16_t)atoi(value);
            } else {
                *bind_usmallint = rand_smallint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
            bind->buffer_length = sizeof(uint16_t);
            bind->buffer = bind_usmallint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_TINYINT:
            bind_tinyint = malloc(sizeof(int8_t));
            assert(bind_tinyint);

            if (value) {
                *bind_tinyint = (int8_t)atoi(value);
            } else {
                *bind_tinyint = rand_tinyint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_TINYINT;
            bind->buffer_length = sizeof(int8_t);
            bind->buffer = bind_tinyint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            bind_utinyint = malloc(sizeof(uint8_t));
            assert(bind_utinyint);

            if (value) {
                *bind_utinyint = (int8_t)atoi(value);
            } else {
                *bind_utinyint = rand_tinyint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_UTINYINT;
            bind->buffer_length = sizeof(uint8_t);
            bind->buffer = bind_utinyint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_BOOL:
            bind_bool = malloc(sizeof(int8_t));
            assert(bind_bool);

            if (value) {
                if (strncasecmp(value, "true", 4)) {
                    *bind_bool = true;
                } else {
                    *bind_bool = false;
                }
            } else {
                *bind_bool = rand_bool();
            }
            bind->buffer_type = TSDB_DATA_TYPE_BOOL;
            bind->buffer_length = sizeof(int8_t);
            bind->buffer = bind_bool;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            bind_ts2 = malloc(sizeof(int64_t));
            assert(bind_ts2);

            if (value) {
                if (strchr(value, ':') && strchr(value, '-')) {
                    int i = 0;
                    while (value[i] != '\0') {
                        if (value[i] == '\"' || value[i] == '\'') {
                            value[i] = ' ';
                        }
                        i++;
                    }
                    int64_t tmpEpoch;
                    if (TSDB_CODE_SUCCESS != taosParseTime(value, &tmpEpoch,
                                                           strlen(value),
                                                           timePrec, 0)) {
                        free(bind_ts2);
                        errorPrint("Input %s, time format error!\n", value);
                        return -1;
                    }
                    *bind_ts2 = tmpEpoch;
                } else {
                    *bind_ts2 = atoll(value);
                }
            } else {
                *bind_ts2 = rand_bigint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = bind_ts2;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_NULL:
            break;

        default:
            errorPrint("Not support data type: %d\n", data_type);
            exit(EXIT_FAILURE);
    }

    return 0;
}

static int32_t prepareStmtBindArrayByTypeForRand(TAOS_BIND *bind,
                                                 char       data_type,
                                                 int32_t    dataLen,
                                                 int32_t timePrec, char **ptr,
                                                 char *value) {
    int32_t * bind_int;
    uint32_t *bind_uint;
    int64_t * bind_bigint;
    uint64_t *bind_ubigint;
    float *   bind_float;
    double *  bind_double;
    int16_t * bind_smallint;
    uint16_t *bind_usmallint;
    int8_t *  bind_tinyint;
    uint8_t * bind_utinyint;
    int8_t *  bind_bool;
    int64_t * bind_ts2;

    switch (data_type) {
        case TSDB_DATA_TYPE_BINARY:

            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("binary length overflow, max size:%u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_binary = (char *)*ptr;

            bind->buffer_type = TSDB_DATA_TYPE_BINARY;
            if (value) {
                strncpy(bind_binary, value, strlen(value));
                bind->buffer_length = strlen(bind_binary);
            } else {
                rand_string(bind_binary, dataLen);
                bind->buffer_length = dataLen;
            }

            bind->length = &bind->buffer_length;
            bind->buffer = bind_binary;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_NCHAR:
            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint("nchar length overflow, max size: %u\n",
                           (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_nchar = (char *)*ptr;

            bind->buffer_type = TSDB_DATA_TYPE_NCHAR;
            if (value) {
                strncpy(bind_nchar, value, strlen(value));
            } else {
                rand_string(bind_nchar, dataLen);
            }

            bind->buffer_length = strlen(bind_nchar);
            bind->buffer = bind_nchar;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_INT:
            bind_int = (int32_t *)*ptr;

            if (value) {
                *bind_int = atoi(value);
            } else {
                *bind_int = rand_int();
            }
            bind->buffer_type = TSDB_DATA_TYPE_INT;
            bind->buffer_length = sizeof(int32_t);
            bind->buffer = bind_int;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_UINT:
            bind_uint = (uint32_t *)*ptr;

            if (value) {
                *bind_uint = atoi(value);
            } else {
                *bind_uint = rand_int();
            }
            bind->buffer_type = TSDB_DATA_TYPE_UINT;
            bind->buffer_length = sizeof(uint32_t);
            bind->buffer = bind_uint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_BIGINT:
            bind_bigint = (int64_t *)*ptr;

            if (value) {
                *bind_bigint = atoll(value);
            } else {
                *bind_bigint = rand_bigint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_BIGINT;
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = bind_bigint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_UBIGINT:
            bind_ubigint = (uint64_t *)*ptr;

            if (value) {
                *bind_ubigint = atoll(value);
            } else {
                *bind_ubigint = rand_bigint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_UBIGINT;
            bind->buffer_length = sizeof(uint64_t);
            bind->buffer = bind_ubigint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_FLOAT:
            bind_float = (float *)*ptr;

            if (value) {
                *bind_float = (float)atof(value);
            } else {
                *bind_float = rand_float();
            }
            bind->buffer_type = TSDB_DATA_TYPE_FLOAT;
            bind->buffer_length = sizeof(float);
            bind->buffer = bind_float;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            bind_double = (double *)*ptr;

            if (value) {
                *bind_double = atof(value);
            } else {
                *bind_double = rand_double();
            }
            bind->buffer_type = TSDB_DATA_TYPE_DOUBLE;
            bind->buffer_length = sizeof(double);
            bind->buffer = bind_double;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            bind_smallint = (int16_t *)*ptr;

            if (value) {
                *bind_smallint = (int16_t)atoi(value);
            } else {
                *bind_smallint = rand_smallint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
            bind->buffer_length = sizeof(int16_t);
            bind->buffer = bind_smallint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_USMALLINT:
            bind_usmallint = (uint16_t *)*ptr;

            if (value) {
                *bind_usmallint = (uint16_t)atoi(value);
            } else {
                *bind_usmallint = rand_smallint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_USMALLINT;
            bind->buffer_length = sizeof(uint16_t);
            bind->buffer = bind_usmallint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_TINYINT:
            bind_tinyint = (int8_t *)*ptr;

            if (value) {
                *bind_tinyint = (int8_t)atoi(value);
            } else {
                *bind_tinyint = rand_tinyint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_TINYINT;
            bind->buffer_length = sizeof(int8_t);
            bind->buffer = bind_tinyint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            bind_utinyint = (uint8_t *)*ptr;

            if (value) {
                *bind_utinyint = (uint8_t)atoi(value);
            } else {
                *bind_utinyint = rand_tinyint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_UTINYINT;
            bind->buffer_length = sizeof(uint8_t);
            bind->buffer = bind_utinyint;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_BOOL:
            bind_bool = (int8_t *)*ptr;

            if (value) {
                if (strncasecmp(value, "true", 4)) {
                    *bind_bool = true;
                } else {
                    *bind_bool = false;
                }
            } else {
                *bind_bool = rand_bool();
            }
            bind->buffer_type = TSDB_DATA_TYPE_BOOL;
            bind->buffer_length = sizeof(int8_t);
            bind->buffer = bind_bool;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            bind_ts2 = (int64_t *)*ptr;

            if (value) {
                if (strchr(value, ':') && strchr(value, '-')) {
                    int i = 0;
                    while (value[i] != '\0') {
                        if (value[i] == '\"' || value[i] == '\'') {
                            value[i] = ' ';
                        }
                        i++;
                    }
                    int64_t tmpEpoch;
                    if (TSDB_CODE_SUCCESS != taosParseTime(value, &tmpEpoch,
                                                           strlen(value),
                                                           timePrec, 0)) {
                        errorPrint("Input %s, time format error!\n", value);
                        return -1;
                    }
                    *bind_ts2 = tmpEpoch;
                } else {
                    *bind_ts2 = atoll(value);
                }
            } else {
                *bind_ts2 = rand_bigint();
            }
            bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = bind_ts2;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            *ptr += bind->buffer_length;
            break;

        default:
            errorPrint("No support data type: %d\n", data_type);
            return -1;
    }

    return 0;
}

static int32_t prepareStmtWithoutStb(threadInfo *pThreadInfo, char *tableName,
                                     uint32_t batch, int64_t insertRows,
                                     int64_t recordFrom, int64_t startTime) {
    TAOS_STMT *stmt = pThreadInfo->stmt;
    int        ret = taos_stmt_set_tbname(stmt, tableName);
    if (ret != 0) {
        errorPrint(
            "failed to execute taos_stmt_set_tbname(%s). return 0x%x. reason: "
            "%s\n",
            tableName, ret, taos_stmt_errstr(stmt));
        return ret;
    }

    char *data_type = g_args.data_type;

    char *bindArray = malloc(sizeof(TAOS_BIND) * (g_args.columnCount + 1));
    if (bindArray == NULL) {
        errorPrint("Failed to allocate %d bind params\n",
                   (g_args.columnCount + 1));
        return -1;
    }

    int32_t k = 0;
    for (k = 0; k < batch;) {
        /* columnCount + 1 (ts) */

        TAOS_BIND *bind = (TAOS_BIND *)(bindArray + 0);

        int64_t *bind_ts = pThreadInfo->bind_ts;

        bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;

        if (g_args.disorderRatio) {
            *bind_ts = startTime + getTSRandTail(g_args.timestamp_step, k,
                                                 g_args.disorderRatio,
                                                 g_args.disorderRange);
        } else {
            *bind_ts = startTime + g_args.timestamp_step * k;
        }
        bind->buffer_length = sizeof(int64_t);
        bind->buffer = bind_ts;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        for (int i = 0; i < g_args.columnCount; i++) {
            bind = (TAOS_BIND *)((char *)bindArray +
                                 (sizeof(TAOS_BIND) * (i + 1)));
            if (-1 ==
                prepareStmtBindArrayByType(bind, data_type[i], g_args.binwidth,
                                           pThreadInfo->time_precision, NULL)) {
                free(bindArray);
                return -1;
            }
        }
        if (0 != taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray)) {
            errorPrint("%s() LN%d, stmt_bind_param() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            break;
        }
        // if msg > 3MB, break
        if (0 != taos_stmt_add_batch(stmt)) {
            errorPrint("%s() LN%d, stmt_add_batch() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            break;
        }

        k++;
        recordFrom++;
        if (recordFrom >= insertRows) {
            break;
        }
    }

    free(bindArray);
    return k;
}

static int32_t prepareStbStmtBindTag(char *bindArray, SSuperTable *stbInfo,
                                     char *tagsVal, int32_t timePrec) {
    TAOS_BIND *tag;

    for (int t = 0; t < stbInfo->tagCount; t++) {
        tag = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * t));
        if (-1 == prepareStmtBindArrayByType(tag, stbInfo->tags[t].data_type,
                                             stbInfo->tags[t].dataLen, timePrec,
                                             NULL)) {
            return -1;
        }
    }

    return 0;
}

static int32_t prepareStbStmtBindRand(int64_t *ts, char *bindArray,
                                      SSuperTable *stbInfo, int64_t startTime,
                                      int32_t recSeq, int32_t timePrec) {
    char data[MAX_DATA_SIZE];
    memset(data, 0, MAX_DATA_SIZE);
    char *ptr = data;

    TAOS_BIND *bind;

    for (int i = 0; i < stbInfo->columnCount + 1; i++) {
        bind = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * i));

        if (i == 0) {
            int64_t *bind_ts = ts;

            bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
            if (stbInfo->disorderRatio) {
                *bind_ts =
                    startTime + getTSRandTail(stbInfo->timeStampStep, recSeq,
                                              stbInfo->disorderRatio,
                                              stbInfo->disorderRange);
            } else {
                *bind_ts = startTime + stbInfo->timeStampStep * recSeq;
            }
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = bind_ts;
            bind->length = &bind->buffer_length;
            bind->is_null = NULL;

            ptr += bind->buffer_length;
        } else if (-1 == prepareStmtBindArrayByTypeForRand(
                             bind, stbInfo->columns[i - 1].data_type,
                             stbInfo->columns[i - 1].dataLen, timePrec, &ptr,
                             NULL)) {
            return -1;
        }
    }

    return 0;
}

UNUSED_FUNC static int32_t prepareStbStmtRand(
    threadInfo *pThreadInfo, char *tableName, int64_t tableSeq, uint32_t batch,
    uint64_t insertRows, uint64_t recordFrom, int64_t startTime) {
    int          ret;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_STMT *  stmt = pThreadInfo->stmt;

    if (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable) {
        char *tagsValBuf = NULL;

        if (0 == stbInfo->tagSource) {
            tagsValBuf = generateTagValuesForStb(stbInfo, tableSeq);
        } else {
            tagsValBuf = getTagValueFromTagSample(
                stbInfo, tableSeq % stbInfo->tagSampleCount);
        }

        if (NULL == tagsValBuf) {
            errorPrint("%s() LN%d, tag buf failed to allocate  memory\n",
                       __func__, __LINE__);
            return -1;
        }

        char *tagsArray = calloc(1, sizeof(TAOS_BIND) * stbInfo->tagCount);
        if (NULL == tagsArray) {
            tmfree(tagsValBuf);
            errorPrint("%s() LN%d, tag buf failed to allocate  memory\n",
                       __func__, __LINE__);
            return -1;
        }

        if (-1 == prepareStbStmtBindTag(tagsArray, stbInfo, tagsValBuf,
                                        pThreadInfo->time_precision
                                        /* is tag */)) {
            tmfree(tagsValBuf);
            tmfree(tagsArray);
            return -1;
        }

        ret =
            taos_stmt_set_tbname_tags(stmt, tableName, (TAOS_BIND *)tagsArray);

        tmfree(tagsValBuf);
        tmfree(tagsArray);

        if (0 != ret) {
            errorPrint("%s() LN%d, stmt_set_tbname_tags() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
    } else {
        ret = taos_stmt_set_tbname(stmt, tableName);
        if (0 != ret) {
            errorPrint("%s() LN%d, stmt_set_tbname() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
    }

    char *bindArray = calloc(1, sizeof(TAOS_BIND) * (stbInfo->columnCount + 1));
    if (bindArray == NULL) {
        errorPrint("%s() LN%d, Failed to allocate %d bind params\n", __func__,
                   __LINE__, (stbInfo->columnCount + 1));
        return -1;
    }

    uint32_t k;
    for (k = 0; k < batch;) {
        /* columnCount + 1 (ts) */
        if (-1 == prepareStbStmtBindRand(pThreadInfo->bind_ts, bindArray,
                                         stbInfo, startTime, k,
                                         pThreadInfo->time_precision
                                         /* is column */)) {
            free(bindArray);
            return -1;
        }
        ret = taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray);
        if (0 != ret) {
            errorPrint("%s() LN%d, stmt_bind_param() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            free(bindArray);
            return -1;
        }
        // if msg > 3MB, break
        ret = taos_stmt_add_batch(stmt);
        if (0 != ret) {
            errorPrint("%s() LN%d, stmt_add_batch() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            free(bindArray);
            return -1;
        }

        k++;
        recordFrom++;

        if (recordFrom >= insertRows) {
            break;
        }
    }

    free(bindArray);
    return k;
}

#if STMT_BIND_PARAM_BATCH == 1
static int execStbBindParamBatch(threadInfo *pThreadInfo, char *tableName,
                                 int64_t tableSeq, uint32_t batch,
                                 uint64_t insertRows, uint64_t recordFrom,
                                 int64_t startTime, int64_t *pSamplePos) {
    int        ret;
    TAOS_STMT *stmt = pThreadInfo->stmt;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    assert(stbInfo);

    uint32_t columnCount = pThreadInfo->stbInfo->columnCount;

    uint32_t thisBatch = MAX_SAMPLES - (*pSamplePos);

    if (thisBatch > batch) {
        thisBatch = batch;
    }
    verbosePrint("%s() LN%d, batch=%d pos=%" PRId64 " thisBatch=%d\n", __func__,
                 __LINE__, batch, *pSamplePos, thisBatch);

    memset(pThreadInfo->bindParams, 0,
           (sizeof(TAOS_MULTI_BIND) * (columnCount + 1)));
    memset(pThreadInfo->is_null, 0, thisBatch);

    for (int c = 0; c < columnCount + 1; c++) {
        TAOS_MULTI_BIND *param =
            (TAOS_MULTI_BIND *)(pThreadInfo->bindParams +
                                sizeof(TAOS_MULTI_BIND) * c);

        char data_type;

        if (c == 0) {
            data_type = TSDB_DATA_TYPE_TIMESTAMP;
            param->buffer_length = sizeof(int64_t);
            param->buffer = pThreadInfo->bind_ts_array;

        } else {
            data_type = stbInfo->columns[c - 1].data_type;

            char *tmpP;

            switch (data_type) {
                case TSDB_DATA_TYPE_BINARY:
                    param->buffer_length = stbInfo->columns[c - 1].dataLen;

                    tmpP =
                        (char *)((uintptr_t) *
                                 (uintptr_t *)(stbInfo->sampleBindBatchArray +
                                               sizeof(char *) * (c - 1)));

                    verbosePrint("%s() LN%d, tmpP=%p pos=%" PRId64
                                 " width=%" PRIxPTR " position=%" PRId64 "\n",
                                 __func__, __LINE__, tmpP, *pSamplePos,
                                 param->buffer_length,
                                 (*pSamplePos) * param->buffer_length);

                    param->buffer =
                        (void *)(tmpP + *pSamplePos * param->buffer_length);
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                    param->buffer_length = stbInfo->columns[c - 1].dataLen;

                    tmpP =
                        (char *)((uintptr_t) *
                                 (uintptr_t *)(stbInfo->sampleBindBatchArray +
                                               sizeof(char *) * (c - 1)));

                    verbosePrint("%s() LN%d, tmpP=%p pos=%" PRId64
                                 " width=%" PRIxPTR " position=%" PRId64 "\n",
                                 __func__, __LINE__, tmpP, *pSamplePos,
                                 param->buffer_length,
                                 (*pSamplePos) * param->buffer_length);

                    param->buffer =
                        (void *)(tmpP + *pSamplePos * param->buffer_length);
                    break;

                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT:
                    param->buffer_length = sizeof(int32_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->columns[c - 1].dataLen *
                                     (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT:
                    param->buffer_length = sizeof(int8_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->columns[c - 1].dataLen *
                                     (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT:
                    param->buffer_length = sizeof(int16_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->columns[c - 1].dataLen *
                                     (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                    param->buffer_length = sizeof(int64_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->columns[c - 1].dataLen *
                                     (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    param->buffer_length = sizeof(int8_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->columns[c - 1].dataLen *
                                     (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    param->buffer_length = sizeof(float);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->columns[c - 1].dataLen *
                                     (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    param->buffer_length = sizeof(double);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->columns[c - 1].dataLen *
                                     (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    param->buffer_length = sizeof(int64_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->columns[c - 1].dataLen *
                                     (*pSamplePos));
                    break;

                default:
                    errorPrint("%s() LN%d, wrong data type: %d\n", __func__,
                               __LINE__, data_type);
                    exit(EXIT_FAILURE);
            }
        }

        param->buffer_type = data_type;
        param->length = malloc(sizeof(int32_t) * thisBatch);
        assert(param->length);

        for (int b = 0; b < thisBatch; b++) {
            if (param->buffer_type == TSDB_DATA_TYPE_NCHAR) {
                param->length[b] = strlen((char *)param->buffer +
                                          b * stbInfo->columns[c].dataLen);
            } else {
                param->length[b] = param->buffer_length;
            }
        }
        param->is_null = pThreadInfo->is_null;
        param->num = thisBatch;
    }

    uint32_t k;
    for (k = 0; k < thisBatch;) {
        /* columnCount + 1 (ts) */
        if (stbInfo->disorderRatio) {
            *(pThreadInfo->bind_ts_array + k) =
                startTime + getTSRandTail(stbInfo->timeStampStep, k,
                                          stbInfo->disorderRatio,
                                          stbInfo->disorderRange);
        } else {
            *(pThreadInfo->bind_ts_array + k) =
                startTime + stbInfo->timeStampStep * k;
        }

        debugPrint("%s() LN%d, k=%d ts=%" PRId64 "\n", __func__, __LINE__, k,
                   *(pThreadInfo->bind_ts_array + k));
        k++;
        recordFrom++;

        (*pSamplePos)++;
        if ((*pSamplePos) == MAX_SAMPLES) {
            *pSamplePos = 0;
        }

        if (recordFrom >= insertRows) {
            break;
        }
    }

    ret = taos_stmt_bind_param_batch(
        stmt, (TAOS_MULTI_BIND *)pThreadInfo->bindParams);
    if (0 != ret) {
        errorPrint("%s() LN%d, stmt_bind_param() failed! reason: %s\n",
                   __func__, __LINE__, taos_stmt_errstr(stmt));
        return -1;
    }

    for (int c = 0; c < stbInfo->columnCount + 1; c++) {
        TAOS_MULTI_BIND *param =
            (TAOS_MULTI_BIND *)(pThreadInfo->bindParams +
                                sizeof(TAOS_MULTI_BIND) * c);
        free(param->length);
    }

    // if msg > 3MB, break
    ret = taos_stmt_add_batch(stmt);
    if (0 != ret) {
        errorPrint("%s() LN%d, stmt_add_batch() failed! reason: %s\n", __func__,
                   __LINE__, taos_stmt_errstr(stmt));
        return -1;
    }
    return k;
}

static int parseSamplefileToStmtBatch(SSuperTable *stbInfo) {
    // char *sampleDataBuf = (stbInfo)?
    //    stbInfo->sampleDataBuf:g_sampleDataBuf;
    int32_t columnCount = (stbInfo) ? stbInfo->columnCount : g_args.columnCount;
    char *  sampleBindBatchArray = NULL;

    if (stbInfo) {
        stbInfo->sampleBindBatchArray =
            calloc(1, sizeof(uintptr_t *) * columnCount);
        sampleBindBatchArray = stbInfo->sampleBindBatchArray;
    } else {
        g_sampleBindBatchArray = calloc(1, sizeof(uintptr_t *) * columnCount);
        sampleBindBatchArray = g_sampleBindBatchArray;
    }
    assert(sampleBindBatchArray);

    for (int c = 0; c < columnCount; c++) {
        char data_type =
            (stbInfo) ? stbInfo->columns[c].data_type : g_args.data_type[c];

        char *tmpP = NULL;

        switch (data_type) {
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                tmpP = calloc(1, sizeof(int) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                tmpP = calloc(1, sizeof(int8_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                tmpP = calloc(1, sizeof(int16_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                tmpP = calloc(1, sizeof(int64_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_BOOL:
                tmpP = calloc(1, sizeof(int8_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                tmpP = calloc(1, sizeof(float) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                tmpP = calloc(1, sizeof(double) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                tmpP = calloc(
                    1, MAX_SAMPLES * (((stbInfo) ? stbInfo->columns[c].dataLen
                                                 : g_args.binwidth)));
                assert(tmpP);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                tmpP = calloc(1, sizeof(int64_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t *)(sampleBindBatchArray + sizeof(uintptr_t *) * c) =
                    (uintptr_t)tmpP;
                break;

            default:
                errorPrint("Unknown data type: %s\n",
                           (stbInfo) ? stbInfo->columns[c].dataType
                                     : g_args.dataType[c]);
                exit(EXIT_FAILURE);
        }
    }

    char *sampleDataBuf = (stbInfo) ? stbInfo->sampleDataBuf : g_sampleDataBuf;
    int64_t lenOfOneRow = (stbInfo) ? stbInfo->lenOfOneRow : g_args.lenOfOneRow;

    for (int i = 0; i < MAX_SAMPLES; i++) {
        int cursor = 0;

        for (int c = 0; c < columnCount; c++) {
            char data_type =
                (stbInfo) ? stbInfo->columns[c].data_type : g_args.data_type[c];
            char *restStr = sampleDataBuf + lenOfOneRow * i + cursor;
            int   lengthOfRest = strlen(restStr);

            int index = 0;
            for (index = 0; index < lengthOfRest; index++) {
                if (restStr[index] == ',') {
                    break;
                }
            }

            char *tmpStr = calloc(1, index + 1);
            if (NULL == tmpStr) {
                errorPrint("%s() LN%d, Failed to allocate %d bind buffer\n",
                           __func__, __LINE__, index + 1);
                return -1;
            }

            strncpy(tmpStr, restStr, index);
            cursor += index + 1;  // skip ',' too
            char *tmpP;

            switch (data_type) {
                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT:
                    *((int32_t *)((uintptr_t) *
                                      (uintptr_t *)(sampleBindBatchArray +
                                                    sizeof(char *) * c) +
                                  sizeof(int32_t) * i)) = atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    *(float *)(((uintptr_t) *
                                    (uintptr_t *)(sampleBindBatchArray +
                                                  sizeof(char *) * c) +
                                sizeof(float) * i)) = (float)atof(tmpStr);
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    *(double *)(((uintptr_t) *
                                     (uintptr_t *)(sampleBindBatchArray +
                                                   sizeof(char *) * c) +
                                 sizeof(double) * i)) = atof(tmpStr);
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT:
                    *((int8_t *)((uintptr_t) *
                                     (uintptr_t *)(sampleBindBatchArray +
                                                   sizeof(char *) * c) +
                                 sizeof(int8_t) * i)) = (int8_t)atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT:
                    *((int16_t *)((uintptr_t) *
                                      (uintptr_t *)(sampleBindBatchArray +
                                                    sizeof(char *) * c) +
                                  sizeof(int16_t) * i)) = (int16_t)atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                    *((int64_t *)((uintptr_t) *
                                      (uintptr_t *)(sampleBindBatchArray +
                                                    sizeof(char *) * c) +
                                  sizeof(int64_t) * i)) = (int64_t)atol(tmpStr);
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    *((int8_t *)((uintptr_t) *
                                     (uintptr_t *)(sampleBindBatchArray +
                                                   sizeof(char *) * c) +
                                 sizeof(int8_t) * i)) = (int8_t)atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    *((int64_t *)((uintptr_t) *
                                      (uintptr_t *)(sampleBindBatchArray +
                                                    sizeof(char *) * c) +
                                  sizeof(int64_t) * i)) = (int64_t)atol(tmpStr);
                    break;

                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR:
                    tmpP = (char *)(*(uintptr_t *)(sampleBindBatchArray +
                                                   sizeof(char *) * c));
                    strcpy(tmpP + i * (((stbInfo) ? stbInfo->columns[c].dataLen
                                                  : g_args.binwidth)),
                           tmpStr);
                    break;

                default:
                    break;
            }

            free(tmpStr);
        }
    }

    return 0;
}

static int parseSampleToStmtBatchForThread(threadInfo * pThreadInfo,
                                           SSuperTable *stbInfo,
                                           uint32_t timePrec, uint32_t batch) {
    uint32_t columnCount =
        (stbInfo) ? stbInfo->columnCount : g_args.columnCount;

    pThreadInfo->bind_ts_array = malloc(sizeof(int64_t) * batch);
    assert(pThreadInfo->bind_ts_array);

    pThreadInfo->bindParams =
        malloc(sizeof(TAOS_MULTI_BIND) * (columnCount + 1));
    assert(pThreadInfo->bindParams);

    pThreadInfo->is_null = malloc(batch);
    assert(pThreadInfo->is_null);

    return 0;
}

static int parseStbSampleToStmtBatchForThread(threadInfo * pThreadInfo,
                                              SSuperTable *stbInfo,
                                              uint32_t     timePrec,
                                              uint32_t     batch) {
    return parseSampleToStmtBatchForThread(pThreadInfo, stbInfo, timePrec,
                                           batch);
}

static int parseNtbSampleToStmtBatchForThread(threadInfo *pThreadInfo,
                                              uint32_t    timePrec,
                                              uint32_t    batch) {
    return parseSampleToStmtBatchForThread(pThreadInfo, NULL, timePrec, batch);
}

#else
static int parseSampleToStmt(threadInfo *pThreadInfo, SSuperTable *stbInfo,
                             uint32_t timePrec) {
    pThreadInfo->sampleBindArray =
        (char *)calloc(1, sizeof(char *) * MAX_SAMPLES);
    if (pThreadInfo->sampleBindArray == NULL) {
        errorPrint("%s() LN%d, Failed to allocate %" PRIu64
                   " bind array buffer\n",
                   __func__, __LINE__, (uint64_t)sizeof(char *) * MAX_SAMPLES);
        return -1;
    }

    int32_t columnCount = (stbInfo) ? stbInfo->columnCount : g_args.columnCount;
    char *sampleDataBuf = (stbInfo) ? stbInfo->sampleDataBuf : g_sampleDataBuf;
    int64_t lenOfOneRow = (stbInfo) ? stbInfo->lenOfOneRow : g_args.lenOfOneRow;

    for (int i = 0; i < MAX_SAMPLES; i++) {
        char *bindArray = calloc(1, sizeof(TAOS_BIND) * (columnCount + 1));
        if (bindArray == NULL) {
            errorPrint("%s() LN%d, Failed to allocate %d bind params\n",
                       __func__, __LINE__, (columnCount + 1));
            return -1;
        }

        TAOS_BIND *bind;
        int        cursor = 0;

        for (int c = 0; c < columnCount + 1; c++) {
            bind = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * c));

            if (c == 0) {
                bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
                bind->buffer_length = sizeof(int64_t);
                bind->buffer = NULL;  // bind_ts;
                bind->length = &bind->buffer_length;
                bind->is_null = NULL;
            } else {
                char data_type = (stbInfo) ? stbInfo->columns[c - 1].data_type
                                           : g_args.data_type[c - 1];
                int32_t dataLen = (stbInfo) ? stbInfo->columns[c - 1].dataLen
                                            : g_args.binwidth;
                char *  restStr = sampleDataBuf + lenOfOneRow * i + cursor;
                int     lengthOfRest = strlen(restStr);

                int index = 0;
                for (index = 0; index < lengthOfRest; index++) {
                    if (restStr[index] == ',') {
                        break;
                    }
                }

                char *bindBuffer = calloc(1, index + 1);
                if (bindBuffer == NULL) {
                    errorPrint("%s() LN%d, Failed to allocate %d bind buffer\n",
                               __func__, __LINE__, index + 1);
                    return -1;
                }

                strncpy(bindBuffer, restStr, index);
                cursor += index + 1;  // skip ',' too

                if (-1 == prepareStmtBindArrayByType(bind, data_type, dataLen,
                                                     timePrec, bindBuffer)) {
                    free(bindBuffer);
                    free(bindArray);
                    return -1;
                }
                free(bindBuffer);
            }
        }
        *((uintptr_t *)(pThreadInfo->sampleBindArray + (sizeof(char *)) * i)) =
            (uintptr_t)bindArray;
    }

    return 0;
}

static int parseStbSampleToStmt(threadInfo *pThreadInfo, SSuperTable *stbInfo,
                                uint32_t timePrec) {
    return parseSampleToStmt(pThreadInfo, stbInfo, timePrec);
}

static int parseNtbSampleToStmt(threadInfo *pThreadInfo, uint32_t timePrec) {
    return parseSampleToStmt(pThreadInfo, NULL, timePrec);
}

static int32_t prepareStbStmtBindStartTime(char *tableName, int64_t *ts,
                                           char *       bindArray,
                                           SSuperTable *stbInfo,
                                           int64_t startTime, int32_t recSeq) {
    TAOS_BIND *bind;

    bind = (TAOS_BIND *)bindArray;

    int64_t *bind_ts = ts;

    bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    if (stbInfo->disorderRatio) {
        *bind_ts = startTime + getTSRandTail(stbInfo->timeStampStep, recSeq,
                                             stbInfo->disorderRatio,
                                             stbInfo->disorderRange);
    } else {
        *bind_ts = startTime + stbInfo->timeStampStep * recSeq;
    }

    verbosePrint("%s() LN%d, tableName: %s, bind_ts=%" PRId64 "\n", __func__,
                 __LINE__, tableName, *bind_ts);

    bind->buffer_length = sizeof(int64_t);
    bind->buffer = bind_ts;
    bind->length = &bind->buffer_length;
    bind->is_null = NULL;

    return 0;
}

static uint32_t execBindParam(threadInfo *pThreadInfo, char *tableName,
                              int64_t tableSeq, uint32_t batch,
                              uint64_t insertRows, uint64_t recordFrom,
                              int64_t startTime, int64_t *pSamplePos) {
    int          ret;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_STMT *  stmt = pThreadInfo->stmt;

    uint32_t k;
    for (k = 0; k < batch;) {
        char *bindArray =
            (char *)(*((uintptr_t *)(pThreadInfo->sampleBindArray +
                                     (sizeof(char *)) * (*pSamplePos))));
        /* columnCount + 1 (ts) */
        if (-1 == prepareStbStmtBindStartTime(tableName, pThreadInfo->bind_ts,
                                              bindArray, stbInfo, startTime, k
                                              /* is column */)) {
            return -1;
        }
        ret = taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray);
        if (0 != ret) {
            errorPrint("%s() LN%d, stmt_bind_param() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
        // if msg > 3MB, break
        ret = taos_stmt_add_batch(stmt);
        if (0 != ret) {
            errorPrint("%s() LN%d, stmt_add_batch() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }

        k++;
        recordFrom++;

        (*pSamplePos)++;
        if ((*pSamplePos) == MAX_SAMPLES) {
            *pSamplePos = 0;
        }

        if (recordFrom >= insertRows) {
            break;
        }
    }

    return k;
}
#endif

int32_t prepareStbStmt(threadInfo *pThreadInfo, char *tableName,
                       int64_t tableSeq, uint32_t batch, uint64_t insertRows,
                       uint64_t recordFrom, int64_t startTime,
                       int64_t *pSamplePos) {
    int          ret;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_STMT *  stmt = pThreadInfo->stmt;

    if (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable) {
        char *tagsValBuf = NULL;

        if (0 == stbInfo->tagSource) {
            tagsValBuf = generateTagValuesForStb(stbInfo, tableSeq);
        } else {
            tagsValBuf = getTagValueFromTagSample(
                stbInfo, tableSeq % stbInfo->tagSampleCount);
        }

        if (NULL == tagsValBuf) {
            errorPrint("%s() LN%d, tag buf failed to allocate  memory\n",
                       __func__, __LINE__);
            return -1;
        }

        char *tagsArray = calloc(1, sizeof(TAOS_BIND) * stbInfo->tagCount);
        if (NULL == tagsArray) {
            tmfree(tagsValBuf);
            errorPrint("%s() LN%d, tag buf failed to allocate  memory\n",
                       __func__, __LINE__);
            return -1;
        }

        if (-1 == prepareStbStmtBindTag(tagsArray, stbInfo, tagsValBuf,
                                        pThreadInfo->time_precision
                                        /* is tag */)) {
            tmfree(tagsValBuf);
            tmfree(tagsArray);
            return -1;
        }

        ret =
            taos_stmt_set_tbname_tags(stmt, tableName, (TAOS_BIND *)tagsArray);

        tmfree(tagsValBuf);
        tmfree(tagsArray);

        if (0 != ret) {
            errorPrint("%s() LN%d, stmt_set_tbname_tags() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
    } else {
        ret = taos_stmt_set_tbname(stmt, tableName);
        if (0 != ret) {
            errorPrint("%s() LN%d, stmt_set_tbname() failed! reason: %s\n",
                       __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
    }

#if STMT_BIND_PARAM_BATCH == 1
    return execStbBindParamBatch(pThreadInfo, tableName, tableSeq, batch,
                                 insertRows, recordFrom, startTime, pSamplePos);
#else
    return execBindParam(pThreadInfo, tableName, tableSeq, batch, insertRows,
                         recordFrom, startTime, pSamplePos);
#endif
}

int32_t generateStbProgressiveData(SSuperTable *stbInfo, char *tableName,
                                   int64_t tableSeq, char *dbName, char *buffer,
                                   int64_t insertRows, uint64_t recordFrom,
                                   int64_t startTime, int64_t *pSamplePos,
                                   int64_t *pRemainderBufLen) {
    assert(buffer != NULL);
    char *pstr = buffer;

    memset(pstr, 0, *pRemainderBufLen);

    int64_t headLen = generateStbSQLHead(stbInfo, tableName, tableSeq, dbName,
                                         buffer, *pRemainderBufLen);

    if (headLen <= 0) {
        return 0;
    }
    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen;

    return generateStbDataTail(stbInfo, g_args.reqPerReq, pstr,
                               *pRemainderBufLen, insertRows, recordFrom,
                               startTime, pSamplePos, &dataLen);
}

int32_t generateProgressiveDataWithoutStb(
    char *tableName,
    /* int64_t tableSeq, */
    threadInfo *pThreadInfo, char *buffer, int64_t insertRows,
    uint64_t recordFrom, int64_t startTime, /*int64_t *pSamplePos, */
    int64_t *pRemainderBufLen) {
    assert(buffer != NULL);
    char *pstr = buffer;

    memset(buffer, 0, *pRemainderBufLen);

    int64_t headLen = generateSQLHeadWithoutStb(tableName, pThreadInfo->db_name,
                                                buffer, *pRemainderBufLen);

    if (headLen <= 0) {
        return 0;
    }
    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen;

    return generateDataTailWithoutStb(g_args.reqPerReq, pstr, *pRemainderBufLen,
                                      insertRows, recordFrom, startTime,
                                      /*pSamplePos, */ &dataLen);
}

#if STMT_BIND_PARAM_BATCH == 1
// stmt sync write interlace data
static void *syncWriteInterlaceStmtBatch(threadInfo *pThreadInfo,
                                         uint32_t    interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### stmt interlace write\n",
               pThreadInfo->threadID, __func__, __LINE__);

    int64_t  insertRows;
    int64_t  timeStampStep;
    uint64_t insert_interval;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    if (stbInfo) {
        insertRows = stbInfo->insertRows;
        timeStampStep = stbInfo->timeStampStep;
        insert_interval = stbInfo->insertInterval;
    } else {
        insertRows = g_args.insertRows;
        timeStampStep = g_args.timestamp_step;
        insert_interval = g_args.insert_interval;
    }

    debugPrint("[%d] %s() LN%d: start_table_from=%" PRIu64 " ntables=%" PRId64
               " insertRows=%" PRIu64 "\n",
               pThreadInfo->threadID, __func__, __LINE__,
               pThreadInfo->start_table_from, pThreadInfo->ntables, insertRows);

    uint64_t timesInterlace = (insertRows / interlaceRows) + 1;
    uint32_t precalcBatch = interlaceRows;

    if (precalcBatch > g_args.reqPerReq) precalcBatch = g_args.reqPerReq;

    if (precalcBatch > MAX_SAMPLES) precalcBatch = MAX_SAMPLES;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    uint64_t st = 0;
    uint64_t et = UINT64_MAX;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    uint64_t tableSeq = pThreadInfo->start_table_from;
    int64_t  startTime;

    bool     flagSleep = true;
    uint64_t sleepTimeTotal = 0;

    int     percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;
    pThreadInfo->samplePos = 0;

    for (int64_t interlace = 0; interlace < timesInterlace; interlace++) {
        if ((flagSleep) && (insert_interval)) {
            st = taosGetTimestampMs();
            flagSleep = false;
        }

        int64_t generated = 0;
        int64_t samplePos;

        for (; tableSeq < pThreadInfo->start_table_from + pThreadInfo->ntables;
             tableSeq++) {
            char tableName[TSDB_TABLE_NAME_LEN];
            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint("[%d] %s() LN%d, getTableName return null\n",
                           pThreadInfo->threadID, __func__, __LINE__);
                return NULL;
            }

            samplePos = pThreadInfo->samplePos;
            startTime = pThreadInfo->start_time +
                        interlace * interlaceRows * timeStampStep;
            uint64_t remainRecPerTbl = insertRows - interlaceRows * interlace;
            uint64_t recPerTbl = 0;

            uint64_t remainPerInterlace;
            if (remainRecPerTbl > interlaceRows) {
                remainPerInterlace = interlaceRows;
            } else {
                remainPerInterlace = remainRecPerTbl;
            }

            while (remainPerInterlace > 0) {
                uint32_t batch;
                if (remainPerInterlace > precalcBatch) {
                    batch = precalcBatch;
                } else {
                    batch = remainPerInterlace;
                }
                debugPrint(
                    "[%d] %s() LN%d, tableName:%s, batch:%d startTime:%" PRId64
                    "\n",
                    pThreadInfo->threadID, __func__, __LINE__, tableName, batch,
                    startTime);

                if (stbInfo) {
                    generated =
                        prepareStbStmt(pThreadInfo, tableName, tableSeq, batch,
                                       insertRows, 0, startTime, &samplePos);
                } else {
                    generated = prepareStmtWithoutStb(
                        pThreadInfo, tableName, batch, insertRows,
                        interlaceRows * interlace + recPerTbl, startTime);
                }

                debugPrint("[%d] %s() LN%d, generated records is %" PRId64 "\n",
                           pThreadInfo->threadID, __func__, __LINE__,
                           generated);
                if (generated < 0) {
                    errorPrint(
                        "[%d] %s() LN%d, generated records is %" PRId64 "\n",
                        pThreadInfo->threadID, __func__, __LINE__, generated);
                    goto free_of_interlace_stmt;
                } else if (generated == 0) {
                    break;
                }

                recPerTbl += generated;
                remainPerInterlace -= generated;
                pThreadInfo->totalInsertRows += generated;

                verbosePrint("[%d] %s() LN%d totalInsertRows=%" PRIu64 "\n",
                             pThreadInfo->threadID, __func__, __LINE__,
                             pThreadInfo->totalInsertRows);

                startTs = taosGetTimestampUs();

                int64_t affectedRows = execInsert(pThreadInfo, generated);

                endTs = taosGetTimestampUs();
                uint64_t delay = endTs - startTs;
                performancePrint(
                    "%s() LN%d, insert execution time is %10.2f ms\n", __func__,
                    __LINE__, delay / 1000.0);
                verbosePrint("[%d] %s() LN%d affectedRows=%" PRId64 "\n",
                             pThreadInfo->threadID, __func__, __LINE__,
                             affectedRows);

                if (delay > pThreadInfo->maxDelay)
                    pThreadInfo->maxDelay = delay;
                if (delay < pThreadInfo->minDelay)
                    pThreadInfo->minDelay = delay;
                pThreadInfo->cntDelay++;
                pThreadInfo->totalDelay += delay;

                if (generated != affectedRows) {
                    errorPrint("[%d] %s() LN%d execInsert() insert %" PRId64
                               ", affected rows: %" PRId64 "\n\n",
                               pThreadInfo->threadID, __func__, __LINE__,
                               generated, affectedRows);
                    goto free_of_interlace_stmt;
                }

                pThreadInfo->totalAffectedRows += affectedRows;

                int currentPercent =
                    pThreadInfo->totalAffectedRows * 100 / totalRows;
                if (currentPercent > percentComplete) {
                    printf("[%d]:%d%%\n", pThreadInfo->threadID,
                           currentPercent);
                    percentComplete = currentPercent;
                }
                int64_t currentPrintTime = taosGetTimestampMs();
                if (currentPrintTime - lastPrintTime > 30 * 1000) {
                    printf("thread[%d] has currently inserted rows: %" PRIu64
                           ", affected rows: %" PRIu64 "\n",
                           pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                           pThreadInfo->totalAffectedRows);
                    lastPrintTime = currentPrintTime;
                }

                startTime += (generated * timeStampStep);
            }
        }
        pThreadInfo->samplePos = samplePos;

        if (tableSeq == pThreadInfo->start_table_from + pThreadInfo->ntables) {
            // turn to first table
            tableSeq = pThreadInfo->start_table_from;

            flagSleep = true;
        }

        if ((insert_interval) && flagSleep) {
            et = taosGetTimestampMs();

            if (insert_interval > (et - st)) {
                uint64_t sleepTime = insert_interval - (et - st);
                performancePrint("%s() LN%d sleep: %" PRId64
                                 " ms for insert interval\n",
                                 __func__, __LINE__, sleepTime);
                taosMsleep(sleepTime);  // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    if (percentComplete < 100)
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);

free_of_interlace_stmt:
    printStatPerThread(pThreadInfo);
    return NULL;
}
#else
// stmt sync write interlace data
static void *syncWriteInterlaceStmt(threadInfo *pThreadInfo,
                                    uint32_t    interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### stmt interlace write\n",
               pThreadInfo->threadID, __func__, __LINE__);

    int64_t  insertRows;
    uint64_t maxSqlLen;
    int64_t  timeStampStep;
    uint64_t insert_interval;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    if (stbInfo) {
        insertRows = stbInfo->insertRows;
        maxSqlLen = stbInfo->maxSqlLen;
        timeStampStep = stbInfo->timeStampStep;
        insert_interval = stbInfo->insertInterval;
    } else {
        insertRows = g_args.insertRows;
        maxSqlLen = g_args.max_sql_len;
        timeStampStep = g_args.timestamp_step;
        insert_interval = g_args.insert_interval;
    }

    debugPrint("[%d] %s() LN%d: start_table_from=%" PRIu64 " ntables=%" PRId64
               " insertRows=%" PRIu64 "\n",
               pThreadInfo->threadID, __func__, __LINE__,
               pThreadInfo->start_table_from, pThreadInfo->ntables, insertRows);

    uint32_t batchPerTbl = interlaceRows;
    uint32_t batchPerTblTimes;

    if (interlaceRows > g_args.reqPerReq) interlaceRows = g_args.reqPerReq;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes = g_args.reqPerReq / interlaceRows;
    } else {
        batchPerTblTimes = 1;
    }

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    uint64_t st = 0;
    uint64_t et = UINT64_MAX;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    uint64_t tableSeq = pThreadInfo->start_table_from;
    int64_t  startTime = pThreadInfo->start_time;

    uint64_t generatedRecPerTbl = 0;
    bool     flagSleep = true;
    uint64_t sleepTimeTotal = 0;

    int     percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;

    while (pThreadInfo->totalInsertRows < pThreadInfo->ntables * insertRows) {
        if ((flagSleep) && (insert_interval)) {
            st = taosGetTimestampMs();
            flagSleep = false;
        }

        uint32_t recOfBatch = 0;

        int32_t generated;
        for (uint64_t i = 0; i < batchPerTblTimes; i++) {
            char tableName[TSDB_TABLE_NAME_LEN];

            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint("[%d] %s() LN%d, getTableName return null\n",
                           pThreadInfo->threadID, __func__, __LINE__);
                return NULL;
            }

            debugPrint(
                "[%d] %s() LN%d, tableName:%s, batch:%d startTime:%" PRId64
                "\n",
                pThreadInfo->threadID, __func__, __LINE__, tableName,
                batchPerTbl, startTime);
            if (stbInfo) {
                generated = prepareStbStmt(
                    pThreadInfo, tableName, tableSeq, batchPerTbl, insertRows,
                    0, startTime, &(pThreadInfo->samplePos));
            } else {
                generated =
                    prepareStmtWithoutStb(pThreadInfo, tableName, batchPerTbl,
                                          insertRows, i, startTime);
            }

            debugPrint("[%d] %s() LN%d, generated records is %d\n",
                       pThreadInfo->threadID, __func__, __LINE__, generated);
            if (generated < 0) {
                errorPrint("[%d] %s() LN%d, generated records is %d\n",
                           pThreadInfo->threadID, __func__, __LINE__,
                           generated);
                goto free_of_interlace_stmt;
            } else if (generated == 0) {
                break;
            }

            tableSeq++;
            recOfBatch += batchPerTbl;

            pThreadInfo->totalInsertRows += batchPerTbl;

            verbosePrint("[%d] %s() LN%d batchPerTbl=%d recOfBatch=%d\n",
                         pThreadInfo->threadID, __func__, __LINE__, batchPerTbl,
                         recOfBatch);

            if (tableSeq ==
                pThreadInfo->start_table_from + pThreadInfo->ntables) {
                // turn to first table
                tableSeq = pThreadInfo->start_table_from;
                generatedRecPerTbl += batchPerTbl;

                startTime = pThreadInfo->start_time +
                            generatedRecPerTbl * timeStampStep;

                flagSleep = true;
                if (generatedRecPerTbl >= insertRows) break;

                int64_t remainRows = insertRows - generatedRecPerTbl;
                if ((remainRows > 0) && (batchPerTbl > remainRows))
                    batchPerTbl = remainRows;

                if (pThreadInfo->ntables * batchPerTbl < g_args.reqPerReq)
                    break;
            }

            verbosePrint("[%d] %s() LN%d generatedRecPerTbl=%" PRId64
                         " insertRows=%" PRId64 "\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         generatedRecPerTbl, insertRows);

            if ((g_args.reqPerReq - recOfBatch) < batchPerTbl) break;
        }

        verbosePrint("[%d] %s() LN%d recOfBatch=%d totalInsertRows=%" PRIu64
                     "\n",
                     pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                     pThreadInfo->totalInsertRows);

        startTs = taosGetTimestampUs();

        if (recOfBatch == 0) {
            errorPrint("[%d] %s() LN%d Failed to insert records of batch %d\n",
                       pThreadInfo->threadID, __func__, __LINE__, batchPerTbl);
            if (batchPerTbl > 0) {
                errorPrint(
                    "\tIf the batch is %d, the length of the SQL to insert a "
                    "row must be less then %" PRId64 "\n",
                    batchPerTbl, maxSqlLen / batchPerTbl);
            }
            goto free_of_interlace_stmt;
        }
        int64_t affectedRows = execInsert(pThreadInfo, recOfBatch);

        endTs = taosGetTimestampUs();
        uint64_t delay = endTs - startTs;
        performancePrint("%s() LN%d, insert execution time is %10.2f ms\n",
                         __func__, __LINE__, delay / 1000.0);
        verbosePrint("[%d] %s() LN%d affectedRows=%" PRId64 "\n",
                     pThreadInfo->threadID, __func__, __LINE__, affectedRows);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        if (recOfBatch != affectedRows) {
            errorPrint(
                "[%d] %s() LN%d execInsert insert %d, affected rows: %" PRId64
                "\n\n",
                pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                affectedRows);
            goto free_of_interlace_stmt;
        }

        pThreadInfo->totalAffectedRows += affectedRows;

        int currentPercent = pThreadInfo->totalAffectedRows * 100 / totalRows;
        if (currentPercent > percentComplete) {
            printf("[%d]:%d%%\n", pThreadInfo->threadID, currentPercent);
            percentComplete = currentPercent;
        }
        int64_t currentPrintTime = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            printf("thread[%d] has currently inserted rows: %" PRIu64
                   ", affected rows: %" PRIu64 "\n",
                   pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                   pThreadInfo->totalAffectedRows);
            lastPrintTime = currentPrintTime;
        }

        if ((insert_interval) && flagSleep) {
            et = taosGetTimestampMs();

            if (insert_interval > (et - st)) {
                uint64_t sleepTime = insert_interval - (et - st);
                performancePrint("%s() LN%d sleep: %" PRId64
                                 " ms for insert interval\n",
                                 __func__, __LINE__, sleepTime);
                taosMsleep(sleepTime);  // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    if (percentComplete < 100)
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);

free_of_interlace_stmt:
    printStatPerThread(pThreadInfo);
    return NULL;
}

#endif

void *syncWriteInterlace(threadInfo *pThreadInfo, uint32_t interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### interlace write\n", pThreadInfo->threadID,
               __func__, __LINE__);

    int64_t  insertRows;
    uint64_t maxSqlLen;
    int64_t  timeStampStep;
    uint64_t insert_interval;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    if (stbInfo) {
        insertRows = stbInfo->insertRows;
        maxSqlLen = stbInfo->maxSqlLen;
        timeStampStep = stbInfo->timeStampStep;
        insert_interval = stbInfo->insertInterval;
    } else {
        insertRows = g_args.insertRows;
        maxSqlLen = g_args.max_sql_len;
        timeStampStep = g_args.timestamp_step;
        insert_interval = g_args.insert_interval;
    }

    debugPrint("[%d] %s() LN%d: start_table_from=%" PRIu64 " ntables=%" PRId64
               " insertRows=%" PRIu64 "\n",
               pThreadInfo->threadID, __func__, __LINE__,
               pThreadInfo->start_table_from, pThreadInfo->ntables, insertRows);
#if 1
    if (interlaceRows > g_args.reqPerReq) interlaceRows = g_args.reqPerReq;

    uint32_t batchPerTbl = interlaceRows;
    uint32_t batchPerTblTimes;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes = g_args.reqPerReq / interlaceRows;
    } else {
        batchPerTblTimes = 1;
    }
#else
    uint32_t batchPerTbl;
    if (interlaceRows > g_args.reqPerReq)
        batchPerTbl = g_args.reqPerReq;
    else
        batchPerTbl = interlaceRows;

    uint32_t batchPerTblTimes;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes = interlaceRows / batchPerTbl;
    } else {
        batchPerTblTimes = 1;
    }
#endif
    pThreadInfo->buffer = calloc(maxSqlLen, 1);
    if (NULL == pThreadInfo->buffer) {
        errorPrint("%s() LN%d, Failed to alloc %" PRIu64 " Bytes, reason:%s\n",
                   __func__, __LINE__, maxSqlLen, strerror(errno));
        return NULL;
    }

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    uint64_t st = 0;
    uint64_t et = UINT64_MAX;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    uint64_t tableSeq = pThreadInfo->start_table_from;
    int64_t  startTime = pThreadInfo->start_time;

    uint64_t generatedRecPerTbl = 0;
    bool     flagSleep = true;
    uint64_t sleepTimeTotal = 0;

    int     percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;

    while (pThreadInfo->totalInsertRows < pThreadInfo->ntables * insertRows) {
        if ((flagSleep) && (insert_interval)) {
            st = taosGetTimestampMs();
            flagSleep = false;
        }

        // generate data
        memset(pThreadInfo->buffer, 0, maxSqlLen);
        uint64_t remainderBufLen = maxSqlLen;

        char *pstr = pThreadInfo->buffer;

        int len =
            snprintf(pstr, strlen(STR_INSERT_INTO) + 1, "%s", STR_INSERT_INTO);
        pstr += len;
        remainderBufLen -= len;

        uint32_t recOfBatch = 0;

        int32_t generated;
        for (uint64_t i = 0; i < batchPerTblTimes; i++) {
            char tableName[TSDB_TABLE_NAME_LEN];

            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint("[%d] %s() LN%d, getTableName return null\n",
                           pThreadInfo->threadID, __func__, __LINE__);
                free(pThreadInfo->buffer);
                return NULL;
            }

            uint64_t oldRemainderLen = remainderBufLen;

            if (stbInfo) {
                generated = generateStbInterlaceData(
                    pThreadInfo, tableName, batchPerTbl, i, batchPerTblTimes,
                    tableSeq, pstr, insertRows, startTime, &remainderBufLen);
            } else {
                generated = generateInterlaceDataWithoutStb(
                    tableName, batchPerTbl, tableSeq, pThreadInfo->db_name,
                    pstr, insertRows, startTime, &remainderBufLen);
            }

            debugPrint("[%d] %s() LN%d, generated records is %d\n",
                       pThreadInfo->threadID, __func__, __LINE__, generated);
            if (generated < 0) {
                errorPrint("[%d] %s() LN%d, generated records is %d\n",
                           pThreadInfo->threadID, __func__, __LINE__,
                           generated);
                goto free_of_interlace;
            } else if (generated == 0) {
                break;
            }

            tableSeq++;
            recOfBatch += batchPerTbl;

            pstr += (oldRemainderLen - remainderBufLen);
            pThreadInfo->totalInsertRows += batchPerTbl;

            verbosePrint("[%d] %s() LN%d batchPerTbl=%d recOfBatch=%d\n",
                         pThreadInfo->threadID, __func__, __LINE__, batchPerTbl,
                         recOfBatch);

            if (tableSeq ==
                pThreadInfo->start_table_from + pThreadInfo->ntables) {
                // turn to first table
                tableSeq = pThreadInfo->start_table_from;
                generatedRecPerTbl += batchPerTbl;

                startTime = pThreadInfo->start_time +
                            generatedRecPerTbl * timeStampStep;

                flagSleep = true;
                if (generatedRecPerTbl >= insertRows) break;

                int64_t remainRows = insertRows - generatedRecPerTbl;
                if ((remainRows > 0) && (batchPerTbl > remainRows))
                    batchPerTbl = remainRows;

                if (pThreadInfo->ntables * batchPerTbl < g_args.reqPerReq)
                    break;
            }

            verbosePrint("[%d] %s() LN%d generatedRecPerTbl=%" PRId64
                         " insertRows=%" PRId64 "\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         generatedRecPerTbl, insertRows);

            if ((g_args.reqPerReq - recOfBatch) < batchPerTbl) break;
        }

        verbosePrint("[%d] %s() LN%d recOfBatch=%d totalInsertRows=%" PRIu64
                     "\n",
                     pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                     pThreadInfo->totalInsertRows);
        verbosePrint("[%d] %s() LN%d, buffer=%s\n", pThreadInfo->threadID,
                     __func__, __LINE__, pThreadInfo->buffer);

        startTs = taosGetTimestampUs();

        if (recOfBatch == 0) {
            errorPrint("[%d] %s() LN%d Failed to insert records of batch %d\n",
                       pThreadInfo->threadID, __func__, __LINE__, batchPerTbl);
            if (batchPerTbl > 0) {
                errorPrint(
                    "\tIf the batch is %d, the length of the SQL to insert a "
                    "row must be less then %" PRId64 "\n",
                    batchPerTbl, maxSqlLen / batchPerTbl);
            }
            errorPrint("\tPlease check if the buffer length(%" PRId64
                       ") or batch(%d) is set with proper value!\n",
                       maxSqlLen, batchPerTbl);
            goto free_of_interlace;
        }
        int64_t affectedRows = execInsert(pThreadInfo, recOfBatch);

        endTs = taosGetTimestampUs();
        uint64_t delay = endTs - startTs;
        performancePrint("%s() LN%d, insert execution time is %10.2f ms\n",
                         __func__, __LINE__, delay / 1000.0);
        verbosePrint("[%d] %s() LN%d affectedRows=%" PRId64 "\n",
                     pThreadInfo->threadID, __func__, __LINE__, affectedRows);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        if (recOfBatch != affectedRows) {
            errorPrint(
                "[%d] %s() LN%d execInsert insert %d, affected rows: %" PRId64
                "\n%s\n",
                pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                affectedRows, pThreadInfo->buffer);
            goto free_of_interlace;
        }

        pThreadInfo->totalAffectedRows += affectedRows;

        int currentPercent = pThreadInfo->totalAffectedRows * 100 / totalRows;
        if (currentPercent > percentComplete) {
            printf("[%d]:%d%%\n", pThreadInfo->threadID, currentPercent);
            percentComplete = currentPercent;
        }
        int64_t currentPrintTime = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            printf("thread[%d] has currently inserted rows: %" PRIu64
                   ", affected rows: %" PRIu64 "\n",
                   pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                   pThreadInfo->totalAffectedRows);
            lastPrintTime = currentPrintTime;
        }

        if ((insert_interval) && flagSleep) {
            et = taosGetTimestampMs();

            if (insert_interval > (et - st)) {
                uint64_t sleepTime = insert_interval - (et - st);
                performancePrint("%s() LN%d sleep: %" PRId64
                                 " ms for insert interval\n",
                                 __func__, __LINE__, sleepTime);
                taosMsleep(sleepTime);  // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    if (percentComplete < 100)
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);

free_of_interlace:
    tmfree(pThreadInfo->buffer);
    printStatPerThread(pThreadInfo);
    return NULL;
}

void *syncWriteProgressiveStmt(threadInfo *pThreadInfo) {
    debugPrint("%s() LN%d: ### stmt progressive write\n", __func__, __LINE__);

    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int64_t      timeStampStep =
        stbInfo ? stbInfo->timeStampStep : g_args.timestamp_step;
    int64_t insertRows = (stbInfo) ? stbInfo->insertRows : g_args.insertRows;
    verbosePrint("%s() LN%d insertRows=%" PRId64 "\n", __func__, __LINE__,
                 insertRows);

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    pThreadInfo->samplePos = 0;

    int     percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;

    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        int64_t start_time = pThreadInfo->start_time;

        for (uint64_t i = 0; i < insertRows;) {
            char tableName[TSDB_TABLE_NAME_LEN];
            getTableName(tableName, pThreadInfo, tableSeq);
            verbosePrint("%s() LN%d: tid=%d seq=%" PRId64 " tableName=%s\n",
                         __func__, __LINE__, pThreadInfo->threadID, tableSeq,
                         tableName);
            if (0 == strlen(tableName)) {
                errorPrint("[%d] %s() LN%d, getTableName return null\n",
                           pThreadInfo->threadID, __func__, __LINE__);
                return NULL;
            }

            // measure prepare + insert
            startTs = taosGetTimestampUs();

            int32_t generated;
            if (stbInfo) {
                generated = prepareStbStmt(
                    pThreadInfo, tableName, tableSeq,
                    (g_args.reqPerReq > stbInfo->insertRows)
                        ? stbInfo->insertRows
                        : g_args.reqPerReq,
                    insertRows, i, start_time, &(pThreadInfo->samplePos));
            } else {
                generated = prepareStmtWithoutStb(pThreadInfo, tableName,
                                                  g_args.reqPerReq, insertRows,
                                                  i, start_time);
            }

            verbosePrint("[%d] %s() LN%d generated=%d\n", pThreadInfo->threadID,
                         __func__, __LINE__, generated);

            if (generated > 0)
                i += generated;
            else
                goto free_of_stmt_progressive;

            start_time += generated * timeStampStep;
            pThreadInfo->totalInsertRows += generated;

            // only measure insert
            // startTs = taosGetTimestampUs();

            int32_t affectedRows = execInsert(pThreadInfo, generated);

            endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;
            performancePrint("%s() LN%d, insert execution time is %10.f ms\n",
                             __func__, __LINE__, delay / 1000.0);
            verbosePrint("[%d] %s() LN%d affectedRows=%d\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         affectedRows);

            if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
            if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
            pThreadInfo->cntDelay++;
            pThreadInfo->totalDelay += delay;

            if (affectedRows < 0) {
                errorPrint("%s() LN%d, affected rows: %d\n", __func__, __LINE__,
                           affectedRows);
                goto free_of_stmt_progressive;
            }

            pThreadInfo->totalAffectedRows += affectedRows;

            int currentPercent =
                pThreadInfo->totalAffectedRows * 100 / totalRows;
            if (currentPercent > percentComplete) {
                printf("[%d]:%d%%\n", pThreadInfo->threadID, currentPercent);
                percentComplete = currentPercent;
            }
            int64_t currentPrintTime = taosGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                printf("thread[%d] has currently inserted rows: %" PRId64
                       ", affected rows: %" PRId64 "\n",
                       pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                       pThreadInfo->totalAffectedRows);
                lastPrintTime = currentPrintTime;
            }

            if (i >= insertRows) break;
        }  // insertRows

        if ((g_args.verbose_print) && (tableSeq == pThreadInfo->ntables - 1) &&
            (stbInfo) &&
            (0 ==
             strncasecmp(stbInfo->dataSource, "sample", strlen("sample")))) {
            verbosePrint("%s() LN%d samplePos=%" PRId64 "\n", __func__,
                         __LINE__, pThreadInfo->samplePos);
        }
    }  // tableSeq

    if (percentComplete < 100) {
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);
    }

free_of_stmt_progressive:
    tmfree(pThreadInfo->buffer);
    printStatPerThread(pThreadInfo);
    return NULL;
}

void *syncWriteProgressive(threadInfo *pThreadInfo) {
    debugPrint("%s() LN%d: ### progressive write\n", __func__, __LINE__);

    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    uint64_t     maxSqlLen = stbInfo ? stbInfo->maxSqlLen : g_args.max_sql_len;
    int64_t      timeStampStep =
        stbInfo ? stbInfo->timeStampStep : g_args.timestamp_step;
    int64_t insertRows = (stbInfo) ? stbInfo->insertRows : g_args.insertRows;
    verbosePrint("%s() LN%d insertRows=%" PRId64 "\n", __func__, __LINE__,
                 insertRows);

    pThreadInfo->buffer = calloc(maxSqlLen, 1);
    if (NULL == pThreadInfo->buffer) {
        errorPrint("Failed to alloc %" PRIu64 " bytes, reason:%s\n", maxSqlLen,
                   strerror(errno));
        return NULL;
    }

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    pThreadInfo->samplePos = 0;

    int     percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;

    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        int64_t start_time = pThreadInfo->start_time;

        for (uint64_t i = 0; i < insertRows;) {
            char tableName[TSDB_TABLE_NAME_LEN];
            getTableName(tableName, pThreadInfo, tableSeq);
            verbosePrint("%s() LN%d: tid=%d seq=%" PRId64 " tableName=%s\n",
                         __func__, __LINE__, pThreadInfo->threadID, tableSeq,
                         tableName);
            if (0 == strlen(tableName)) {
                errorPrint("[%d] %s() LN%d, getTableName return null\n",
                           pThreadInfo->threadID, __func__, __LINE__);
                free(pThreadInfo->buffer);
                return NULL;
            }

            int64_t remainderBufLen = maxSqlLen - 2000;
            char *  pstr = pThreadInfo->buffer;

            int len = snprintf(pstr, strlen(STR_INSERT_INTO) + 1, "%s",
                               STR_INSERT_INTO);

            pstr += len;
            remainderBufLen -= len;

            // measure prepare + insert
            startTs = taosGetTimestampUs();

            int32_t generated;
            if (stbInfo) {
                if (stbInfo->iface == STMT_IFACE) {
                    generated = prepareStbStmt(
                        pThreadInfo, tableName, tableSeq,
                        (g_args.reqPerReq > stbInfo->insertRows)
                            ? stbInfo->insertRows
                            : g_args.reqPerReq,
                        insertRows, i, start_time, &(pThreadInfo->samplePos));
                } else {
                    generated = generateStbProgressiveData(
                        stbInfo, tableName, tableSeq, pThreadInfo->db_name,
                        pstr, insertRows, i, start_time,
                        &(pThreadInfo->samplePos), &remainderBufLen);
                }
            } else {
                if (g_args.iface == STMT_IFACE) {
                    generated = prepareStmtWithoutStb(
                        pThreadInfo, tableName, g_args.reqPerReq, insertRows, i,
                        start_time);
                } else {
                    generated = generateProgressiveDataWithoutStb(
                        tableName,
                        /*  tableSeq, */
                        pThreadInfo, pstr, insertRows, i, start_time,
                        /* &(pThreadInfo->samplePos), */
                        &remainderBufLen);
                }
            }

            verbosePrint("[%d] %s() LN%d generated=%d\n", pThreadInfo->threadID,
                         __func__, __LINE__, generated);

            if (generated > 0)
                i += generated;
            else
                goto free_of_progressive;

            start_time += generated * timeStampStep;
            pThreadInfo->totalInsertRows += generated;

            // only measure insert
            // startTs = taosGetTimestampUs();

            int32_t affectedRows = execInsert(pThreadInfo, generated);

            endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;
            performancePrint("%s() LN%d, insert execution time is %10.f ms\n",
                             __func__, __LINE__, delay / 1000.0);
            verbosePrint("[%d] %s() LN%d affectedRows=%d\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         affectedRows);

            if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
            if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
            pThreadInfo->cntDelay++;
            pThreadInfo->totalDelay += delay;

            if (affectedRows < 0) {
                errorPrint("%s() LN%d, affected rows: %d\n", __func__, __LINE__,
                           affectedRows);
                goto free_of_progressive;
            }

            pThreadInfo->totalAffectedRows += affectedRows;

            int currentPercent =
                pThreadInfo->totalAffectedRows * 100 / totalRows;
            if (currentPercent > percentComplete) {
                printf("[%d]:%d%%\n", pThreadInfo->threadID, currentPercent);
                percentComplete = currentPercent;
            }
            int64_t currentPrintTime = taosGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                printf("thread[%d] has currently inserted rows: %" PRId64
                       ", affected rows: %" PRId64 "\n",
                       pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                       pThreadInfo->totalAffectedRows);
                lastPrintTime = currentPrintTime;
            }

            if (i >= insertRows) break;
        }  // insertRows

        if ((g_args.verbose_print) && (tableSeq == pThreadInfo->ntables - 1) &&
            (stbInfo) &&
            (0 ==
             strncasecmp(stbInfo->dataSource, "sample", strlen("sample")))) {
            verbosePrint("%s() LN%d samplePos=%" PRId64 "\n", __func__,
                         __LINE__, pThreadInfo->samplePos);
        }
    }  // tableSeq

    if (percentComplete < 100) {
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);
    }

free_of_progressive:
    tmfree(pThreadInfo->buffer);
    printStatPerThread(pThreadInfo);
    return NULL;
}

void *syncWrite(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    setThreadName("syncWrite");

    uint32_t interlaceRows = 0;

    if (stbInfo) {
        if (stbInfo->interlaceRows < stbInfo->insertRows)
            interlaceRows = stbInfo->interlaceRows;
    } else {
        if (g_args.interlaceRows < g_args.insertRows)
            interlaceRows = g_args.interlaceRows;
    }

    if (interlaceRows > 0) {
        // interlace mode
        if (stbInfo) {
            if (STMT_IFACE == stbInfo->iface) {
#if STMT_BIND_PARAM_BATCH == 1
                return syncWriteInterlaceStmtBatch(pThreadInfo, interlaceRows);
#else
                return syncWriteInterlaceStmt(pThreadInfo, interlaceRows);
#endif
            } else {
                return syncWriteInterlace(pThreadInfo, interlaceRows);
            }
        }
    } else {
        // progressive mode
        if (((stbInfo) && (STMT_IFACE == stbInfo->iface)) ||
            (STMT_IFACE == g_args.iface)) {
            return syncWriteProgressiveStmt(pThreadInfo);
        } else {
            return syncWriteProgressive(pThreadInfo);
        }
    }

    return NULL;
}

void callBack(void *param, TAOS_RES *res, int code) {
    threadInfo * pThreadInfo = (threadInfo *)param;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    int insert_interval =
        stbInfo ? stbInfo->insertInterval : g_args.insert_interval;
    if (insert_interval) {
        pThreadInfo->et = taosGetTimestampMs();
        if ((pThreadInfo->et - pThreadInfo->st) < insert_interval) {
            taosMsleep(insert_interval -
                       (pThreadInfo->et - pThreadInfo->st));  // ms
        }
    }

    char *buffer = calloc(1, pThreadInfo->stbInfo->maxSqlLen);
    char  data[MAX_DATA_SIZE];
    char *pstr = buffer;
    pstr += sprintf(pstr, "INSERT INTO %s.%s%" PRId64 " VALUES",
                    pThreadInfo->db_name, pThreadInfo->tb_prefix,
                    pThreadInfo->start_table_from);
    //  if (pThreadInfo->counter >= pThreadInfo->stbInfo->insertRows) {
    if (pThreadInfo->counter >= g_args.reqPerReq) {
        pThreadInfo->start_table_from++;
        pThreadInfo->counter = 0;
    }
    if (pThreadInfo->start_table_from > pThreadInfo->end_table_to) {
        tsem_post(&pThreadInfo->lock_sem);
        free(buffer);
        taos_free_result(res);
        return;
    }

    for (int i = 0; i < g_args.reqPerReq; i++) {
        int rand_num = taosRandom() % 100;
        if (0 != pThreadInfo->stbInfo->disorderRatio &&
            rand_num < pThreadInfo->stbInfo->disorderRatio) {
            int64_t d =
                pThreadInfo->lastTs -
                (taosRandom() % pThreadInfo->stbInfo->disorderRange + 1);
            generateStbRowData(pThreadInfo->stbInfo, data, MAX_DATA_SIZE, d);
        } else {
            generateStbRowData(pThreadInfo->stbInfo, data, MAX_DATA_SIZE,
                               pThreadInfo->lastTs += 1000);
        }
        pstr += sprintf(pstr, "%s", data);
        pThreadInfo->counter++;

        if (pThreadInfo->counter >= pThreadInfo->stbInfo->insertRows) {
            break;
        }
    }

    if (insert_interval) {
        pThreadInfo->st = taosGetTimestampMs();
    }
    taos_query_a(pThreadInfo->taos, buffer, callBack, pThreadInfo);
    free(buffer);

    taos_free_result(res);
}

void *asyncWrite(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    setThreadName("asyncWrite");

    pThreadInfo->st = 0;
    pThreadInfo->et = 0;
    pThreadInfo->lastTs = pThreadInfo->start_time;

    int insert_interval =
        stbInfo ? stbInfo->insertInterval : g_args.insert_interval;
    if (insert_interval) {
        pThreadInfo->st = taosGetTimestampMs();
    }
    taos_query_a(pThreadInfo->taos, "show databases", callBack, pThreadInfo);

    tsem_wait(&(pThreadInfo->lock_sem));

    return NULL;
}

int startMultiThreadInsertData(int threads, char *db_name, char *precision,
                               SSuperTable *stbInfo) {
    int32_t timePrec = TSDB_TIME_PRECISION_MILLI;
    if (0 != precision[0]) {
        if (0 == strncasecmp(precision, "ms", 2)) {
            timePrec = TSDB_TIME_PRECISION_MILLI;
        } else if (0 == strncasecmp(precision, "us", 2)) {
            timePrec = TSDB_TIME_PRECISION_MICRO;
        } else if (0 == strncasecmp(precision, "ns", 2)) {
            timePrec = TSDB_TIME_PRECISION_NANO;
        } else {
            errorPrint("Not support precision: %s\n", precision);
            return -1;
        }
    }

    int64_t startTime;
    if (stbInfo) {
        if (0 == strncasecmp(stbInfo->startTimestamp, "now", 3)) {
            startTime = taosGetTimestamp(timePrec);
        } else {
            if (TSDB_CODE_SUCCESS !=
                taosParseTime(stbInfo->startTimestamp, &startTime,
                              strlen(stbInfo->startTimestamp), timePrec, 0)) {
                errorPrint("failed to parse time %s\n",
                           stbInfo->startTimestamp);
                return -1;
            }
        }
    } else {
        startTime = DEFAULT_START_TIME;
    }
    debugPrint("%s() LN%d, startTime= %" PRId64 "\n", __func__, __LINE__,
               startTime);

    // read sample data from file first
    int ret;
    if (stbInfo && stbInfo->iface != SML_IFACE) {
        ret = prepareSampleForStb(stbInfo);
    } else {
        ret = prepareSampleForNtb();
    }

    if (0 != ret) {
        errorPrint("%s() LN%d, prepare sample data for stable failed!\n",
                   __func__, __LINE__);
        return -1;
    }

    TAOS *taos0 = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, db_name,
                               g_Dbs.port);
    if (NULL == taos0) {
        errorPrint("%s() LN%d, connect to server fail , reason: %s\n", __func__,
                   __LINE__, taos_errstr(NULL));
        return -1;
    }

    int64_t  ntables = 0;
    uint64_t tableFrom = 0;

    if (stbInfo) {
        if (stbInfo->iface != SML_IFACE) {
            int64_t  limit;
            uint64_t offset;

            if ((NULL != g_args.sqlFile) &&
                (stbInfo->childTblExists == TBL_NO_EXISTS) &&
                ((stbInfo->childTblOffset != 0) ||
                 (stbInfo->childTblLimit >= 0))) {
                printf(
                    "WARNING: offset and limit will not be used since the "
                    "child tables not exists!\n");
            }

            if (stbInfo->childTblExists == TBL_ALREADY_EXISTS) {
                if ((stbInfo->childTblLimit < 0) ||
                    ((stbInfo->childTblOffset + stbInfo->childTblLimit) >
                     (stbInfo->childTblCount))) {
                    if (stbInfo->childTblCount < stbInfo->childTblOffset) {
                        printf(
                            "WARNING: offset will not be used since the child "
                            "tables count is less then offset!\n");

                        stbInfo->childTblOffset = 0;
                    }
                    stbInfo->childTblLimit =
                        stbInfo->childTblCount - stbInfo->childTblOffset;
                }

                offset = stbInfo->childTblOffset;
                limit = stbInfo->childTblLimit;
            } else {
                limit = stbInfo->childTblCount;
                offset = 0;
            }

            ntables = limit;
            tableFrom = offset;

            if ((stbInfo->childTblExists != TBL_NO_EXISTS) &&
                ((stbInfo->childTblOffset + stbInfo->childTblLimit) >
                 stbInfo->childTblCount)) {
                printf(
                    "WARNING: specified offset + limit > child table count!\n");
                prompt();
            }

            if ((stbInfo->childTblExists != TBL_NO_EXISTS) &&
                (0 == stbInfo->childTblLimit)) {
                printf(
                    "WARNING: specified limit = 0, which cannot find table "
                    "name to insert or query! \n");
                prompt();
            }

            stbInfo->childTblName =
                (char *)calloc(1, limit * TSDB_TABLE_NAME_LEN);
            if (stbInfo->childTblName == NULL) {
                taos_close(taos0);
                errorPrint("%s() LN%d, alloc memory failed!\n", __func__,
                           __LINE__);
                return -1;
            }

            int64_t childTblCount;
            getChildNameOfSuperTableWithLimitAndOffset(
                taos0, db_name, stbInfo->stbName, &stbInfo->childTblName,
                &childTblCount, limit, offset, stbInfo->escapeChar);
            ntables = childTblCount;
        } else {
            ntables = stbInfo->childTblCount;
        }
    } else {
        ntables = g_args.ntables;
        tableFrom = 0;
    }

    taos_close(taos0);

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = ntables;
        a = 1;
    }

    int64_t b = 0;
    if (threads != 0) {
        b = ntables % threads;
    }

    if (g_args.iface == REST_IFACE ||
        ((stbInfo) && (stbInfo->iface == REST_IFACE))) {
        if (convertHostToServAddr(g_Dbs.host, g_Dbs.port, &(g_Dbs.serv_addr)) !=
            0) {
            errorPrint("%s\n", "convert host to server address");
            return -1;
        }
    }

    pthread_t * pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    assert(pids != NULL);
    assert(infos != NULL);

    char *stmtBuffer = calloc(1, BUFFER_SIZE);
    assert(stmtBuffer);

#if STMT_BIND_PARAM_BATCH == 1
    uint32_t interlaceRows = 0;
    uint32_t batch;

    if (stbInfo) {
        if (stbInfo->interlaceRows < stbInfo->insertRows)
            interlaceRows = stbInfo->interlaceRows;
    } else {
        if (g_args.interlaceRows < g_args.insertRows)
            interlaceRows = g_args.interlaceRows;
    }

    if (interlaceRows > 0) {
        batch = interlaceRows;
    } else {
        batch = (g_args.reqPerReq > g_args.insertRows) ? g_args.insertRows
                                                       : g_args.reqPerReq;
    }

#endif

    if ((g_args.iface == STMT_IFACE) ||
        ((stbInfo) && (stbInfo->iface == STMT_IFACE))) {
        char *pstr = stmtBuffer;

        if ((stbInfo) && (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable)) {
            pstr += sprintf(pstr, "INSERT INTO ? USING %s TAGS(?",
                            stbInfo->stbName);
            for (int tag = 0; tag < (stbInfo->tagCount - 1); tag++) {
                pstr += sprintf(pstr, ",?");
            }
            pstr += sprintf(pstr, ") VALUES(?");
        } else {
            pstr += sprintf(pstr, "INSERT INTO ? VALUES(?");
        }

        int columnCount = (stbInfo) ? stbInfo->columnCount : g_args.columnCount;

        for (int col = 0; col < columnCount; col++) {
            pstr += sprintf(pstr, ",?");
        }
        pstr += sprintf(pstr, ")");

        debugPrint("%s() LN%d, stmtBuffer: %s", __func__, __LINE__, stmtBuffer);
#if STMT_BIND_PARAM_BATCH == 1
        parseSamplefileToStmtBatch(stbInfo);
#endif
    }

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;

        tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
        pThreadInfo->time_precision = timePrec;
        pThreadInfo->stbInfo = stbInfo;

        pThreadInfo->start_time = startTime;
        pThreadInfo->minDelay = UINT64_MAX;

        if ((NULL == stbInfo) || (stbInfo->iface != REST_IFACE)) {
            // t_info->taos = taos;
            pThreadInfo->taos = taos_connect(
                g_Dbs.host, g_Dbs.user, g_Dbs.password, db_name, g_Dbs.port);
            if (NULL == pThreadInfo->taos) {
                free(infos);
                errorPrint(
                    "%s() LN%d, connect to server fail from insert sub thread, "
                    "reason: %s\n",
                    __func__, __LINE__, taos_errstr(NULL));
                return -1;
            }

            if ((g_args.iface == STMT_IFACE) ||
                ((stbInfo) && (stbInfo->iface == STMT_IFACE))) {
                pThreadInfo->stmt = taos_stmt_init(pThreadInfo->taos);
                if (NULL == pThreadInfo->stmt) {
                    free(pids);
                    free(infos);
                    errorPrint("%s() LN%d, failed init stmt, reason: %s\n",
                               __func__, __LINE__, taos_errstr(NULL));
                    return -1;
                }

                if (0 != taos_stmt_prepare(pThreadInfo->stmt, stmtBuffer, 0)) {
                    free(pids);
                    free(infos);
                    free(stmtBuffer);
                    errorPrint(
                        "failed to execute taos_stmt_prepare. return 0x%x. "
                        "reason: %s\n",
                        ret, taos_stmt_errstr(pThreadInfo->stmt));
                    return -1;
                }
                pThreadInfo->bind_ts = malloc(sizeof(int64_t));

                if (stbInfo) {
#if STMT_BIND_PARAM_BATCH == 1
                    parseStbSampleToStmtBatchForThread(pThreadInfo, stbInfo,
                                                       timePrec, batch);
#else
                    parseStbSampleToStmt(pThreadInfo, stbInfo, timePrec);
#endif
                } else {
#if STMT_BIND_PARAM_BATCH == 1
                    parseNtbSampleToStmtBatchForThread(pThreadInfo, timePrec,
                                                       batch);
#else
                    parseNtbSampleToStmt(pThreadInfo, timePrec);
#endif
                }
            }
        } else {
            pThreadInfo->taos = NULL;
        }

        /*    if ((NULL == stbInfo)
              || (0 == stbInfo->multiThreadWriteOneTbl)) {
              */
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        /*    } else {
              pThreadInfo->start_table_from = 0;
              pThreadInfo->ntables = stbInfo->childTblCount;
              pThreadInfo->start_time = pThreadInfo->start_time + rand_int() %
           10000 - rand_tinyint();
              }
              */
        if (g_args.iface == REST_IFACE ||
            ((stbInfo) && (stbInfo->iface == REST_IFACE))) {
#ifdef WINDOWS
            WSADATA wsaData;
            WSAStartup(MAKEWORD(2, 2), &wsaData);
            SOCKET sockfd;
#else
            int sockfd;
#endif
            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd < 0) {
#ifdef WINDOWS
                errorPrint("Could not create socket : %d", WSAGetLastError());
#endif
                debugPrint("%s() LN%d, sockfd=%d\n", __func__, __LINE__,
                           sockfd);
                errorPrint("%s\n", "failed to create socket");
                return -1;
            }

            int retConn = connect(sockfd, (struct sockaddr *)&(g_Dbs.serv_addr),
                                  sizeof(struct sockaddr));
            debugPrint("%s() LN%d connect() return %d\n", __func__, __LINE__,
                       retConn);
            if (retConn < 0) {
                errorPrint("%s\n", "failed to connect");
                return -1;
            }
            pThreadInfo->sockfd = sockfd;
        }

        tsem_init(&(pThreadInfo->lock_sem), 0, 0);
        if (ASYNC_MODE == g_Dbs.asyncMode) {
            pthread_create(pids + i, NULL, asyncWrite, pThreadInfo);
        } else {
            pthread_create(pids + i, NULL, syncWrite, pThreadInfo);
        }
    }

    free(stmtBuffer);

    int64_t start = taosGetTimestampUs();

    for (int i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    uint64_t totalDelay = 0;
    uint64_t maxDelay = 0;
    uint64_t minDelay = UINT64_MAX;
    uint64_t cntDelay = 1;
    double   avgDelay = 0;

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;

        tsem_destroy(&(pThreadInfo->lock_sem));
        taos_close(pThreadInfo->taos);

        if (pThreadInfo->stmt) {
            taos_stmt_close(pThreadInfo->stmt);
        }

        tmfree((char *)pThreadInfo->bind_ts);
#if STMT_BIND_PARAM_BATCH == 1
        tmfree((char *)pThreadInfo->bind_ts_array);
        tmfree(pThreadInfo->bindParams);
        tmfree(pThreadInfo->is_null);
        if (g_args.iface == REST_IFACE ||
            ((stbInfo) && (stbInfo->iface == REST_IFACE))) {
#ifdef WINDOWS
            closesocket(pThreadInfo->sockfd);
            WSACleanup();
#else
            close(pThreadInfo->sockfd);
#endif
        }
#else
        if (pThreadInfo->sampleBindArray) {
            for (int k = 0; k < MAX_SAMPLES; k++) {
                uintptr_t *tmp =
                    (uintptr_t *)(*(uintptr_t *)(pThreadInfo->sampleBindArray +
                                                 sizeof(uintptr_t *) * k));
                int columnCount = (pThreadInfo->stbInfo)
                                      ? pThreadInfo->stbInfo->columnCount
                                      : g_args.columnCount;
                for (int c = 1; c < columnCount + 1; c++) {
                    TAOS_BIND *bind =
                        (TAOS_BIND *)((char *)tmp + (sizeof(TAOS_BIND) * c));
                    if (bind) tmfree(bind->buffer);
                }
                tmfree((char *)tmp);
            }
            tmfree(pThreadInfo->sampleBindArray);
        }
#endif

        debugPrint("%s() LN%d, [%d] totalInsert=%" PRIu64
                   " totalAffected=%" PRIu64 "\n",
                   __func__, __LINE__, pThreadInfo->threadID,
                   pThreadInfo->totalInsertRows,
                   pThreadInfo->totalAffectedRows);
        if (stbInfo) {
            stbInfo->totalAffectedRows += pThreadInfo->totalAffectedRows;
            stbInfo->totalInsertRows += pThreadInfo->totalInsertRows;
        } else {
            g_args.totalAffectedRows += pThreadInfo->totalAffectedRows;
            g_args.totalInsertRows += pThreadInfo->totalInsertRows;
        }

        totalDelay += pThreadInfo->totalDelay;
        cntDelay += pThreadInfo->cntDelay;
        if (pThreadInfo->maxDelay > maxDelay) maxDelay = pThreadInfo->maxDelay;
        if (pThreadInfo->minDelay < minDelay) minDelay = pThreadInfo->minDelay;
    }

    if (cntDelay == 0) cntDelay = 1;
    avgDelay = (double)totalDelay / cntDelay;

    int64_t end = taosGetTimestampUs();
    int64_t t = end - start;
    if (0 == t) t = 1;

    double tInMs = (double)t / 1000000.0;

    if (stbInfo) {
        fprintf(stderr,
                "Spent %.4f seconds to insert rows: %" PRIu64
                ", affected rows: %" PRIu64
                " with %d thread(s) into %s.%s. %.2f records/second\n\n",
                tInMs, stbInfo->totalInsertRows, stbInfo->totalAffectedRows,
                threads, db_name, stbInfo->stbName,
                (double)(stbInfo->totalInsertRows / tInMs));

        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult,
                    "Spent %.4f seconds to insert rows: %" PRIu64
                    ", affected rows: %" PRIu64
                    " with %d thread(s) into %s.%s. %.2f records/second\n\n",
                    tInMs, stbInfo->totalInsertRows, stbInfo->totalAffectedRows,
                    threads, db_name, stbInfo->stbName,
                    (double)(stbInfo->totalInsertRows / tInMs));
        }
    } else {
        fprintf(stderr,
                "Spent %.4f seconds to insert rows: %" PRIu64
                ", affected rows: %" PRIu64
                " with %d thread(s) into %s %.2f records/second\n\n",
                tInMs, g_args.totalInsertRows, g_args.totalAffectedRows,
                threads, db_name, (double)(g_args.totalInsertRows / tInMs));
        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult,
                    "Spent %.4f seconds to insert rows: %" PRIu64
                    ", affected rows: %" PRIu64
                    " with %d thread(s) into %s %.2f records/second\n\n",
                    tInMs, g_args.totalInsertRows, g_args.totalAffectedRows,
                    threads, db_name, (double)(g_args.totalInsertRows / tInMs));
        }
    }

    if (minDelay != UINT64_MAX) {
        fprintf(stderr,
                "insert delay, avg: %10.2fms, max: %10.2fms, min: %10.2fms\n\n",
                (double)avgDelay / 1000.0, (double)maxDelay / 1000.0,
                (double)minDelay / 1000.0);

        if (g_fpOfInsertResult) {
            fprintf(
                g_fpOfInsertResult,
                "insert delay, avg:%10.2fms, max: %10.2fms, min: %10.2fms\n\n",
                (double)avgDelay / 1000.0, (double)maxDelay / 1000.0,
                (double)minDelay / 1000.0);
        }
    }

    // taos_close(taos);

    free(pids);
    free(infos);
    return 0;
}

int insertTestProcess() {
    int32_t code = printfInsertMeta();
    if (code) {
        errorPrint("%s() LN%d, printfInsertMeta() failed\n", __func__,
                   __LINE__);
        return code;
    }

    debugPrint("%d result file: %s\n", __LINE__, g_Dbs.resultFile);
    g_fpOfInsertResult = fopen(g_Dbs.resultFile, "a");
    if (NULL == g_fpOfInsertResult) {
        errorPrint("%s() LN%d, failed to open %s for save result\n", __func__,
                   __LINE__, g_Dbs.resultFile);
        return -1;
    }

    if (g_fpOfInsertResult) {
        printfInsertMetaToFile(g_fpOfInsertResult);
    }

    prompt();

    init_rand_data();

    // create database and super tables
    char *cmdBuffer = calloc(1, BUFFER_SIZE);
    assert(cmdBuffer);
    code = createDatabasesAndStables(cmdBuffer);
    if (code) {
        errorPrint("%s() LN%d, createDatabasesAndStables() failed\n", __func__,
                   __LINE__);
        goto end_insert_process;
    }

    // pretreatment
    code = prepareSampleData();
    if (code) {
        errorPrint("%s() LN%d, prepareSampleData() failed\n", __func__,
                   __LINE__);
        goto end_insert_process;
    }

    if (g_args.iface != SML_IFACE && g_totalChildTables > 0) {
        code = createChildTables();
        if (code) {
            errorPrint("%s() LN%d, createChildTables() failed\n", __func__,
                       __LINE__);
            goto end_insert_process;
        }
    }
    // create sub threads for inserting data
    // start = taosGetTimestampMs();
    for (int i = 0; i < g_Dbs.dbCount; i++) {
        if (g_Dbs.use_metric) {
            if (g_Dbs.db[i].superTblCount > 0) {
                for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {
                    SSuperTable *stbInfo = &g_Dbs.db[i].superTbls[j];

                    if (stbInfo && (stbInfo->insertRows > 0)) {
                        code = startMultiThreadInsertData(
                            g_Dbs.threadCount, g_Dbs.db[i].dbName,
                            g_Dbs.db[i].dbCfg.precision, stbInfo);
                        if (code) {
                            errorPrint(
                                "%s() LN%d, startMultiThreadInsertData failed "
                                "for db %d stb %" PRIu64 "\n",
                                __func__, __LINE__, i, j);
                            goto end_insert_process;
                        }
                    }
                }
            }
        } else {
            if (SML_IFACE == g_args.iface) {
                code = -1;
                errorPrint("%s\n", "Schemaless insertion must include stable");
                goto end_insert_process;
            } else {
                code = startMultiThreadInsertData(
                    g_Dbs.threadCount, g_Dbs.db[i].dbName,
                    g_Dbs.db[i].dbCfg.precision, NULL);
                if (code) {
                    errorPrint("%s() LN%d, startMultiThreadInsertData failed\n",
                               __func__, __LINE__);
                    goto end_insert_process;
                }
            }
        }
    }
end_insert_process:
    tmfree(cmdBuffer);
    if (g_fpOfInsertResult) {
        fclose(g_fpOfInsertResult);
    }
    return code;
}