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

#include "cJSON.h"
#include "demo.h"
#include "demoData.h"

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
        if (superTbls->iface == SML_IFACE) {
            lenOfOneRow += SML_LINE_SQL_SYNTAX_OFFSET;
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
        if (superTbls->iface == SML_IFACE) {
            lenOfOneRow += SML_LINE_SQL_SYNTAX_OFFSET;
        }
    }

    if (superTbls->iface == SML_IFACE) {
        lenOfTagOfOneRow +=
            2 * TSDB_TABLE_NAME_LEN * 2 + SML_LINE_SQL_SYNTAX_OFFSET;
        superTbls->lenOfOneRow += lenOfTagOfOneRow;
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
    return 0;
}

static int createSuperTable(TAOS *taos, char *dbName, SSuperTable *superTbl,
                            char *command) {
    char cols[COL_BUFFER_LEN] = "\0";
    int  len = 0;

    int lenOfOneRow = 0;

    if (superTbl->columnCount == 0) {
        errorPrint("super table column count is %d\n", superTbl->columnCount);
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
                errorPrint("config error data type : %s\n",
                           superTbl->columns[colIndex].dataType);
                return -1;
        }
    }

    superTbl->lenOfOneRow = lenOfOneRow + TIMESTAMP_BUFF_LEN;  // timestamp

    // save for creating child table
    superTbl->colsOfCreateChildTable =
        (char *)calloc(len + TIMESTAMP_BUFF_LEN, 1);
    if (NULL == superTbl->colsOfCreateChildTable) {
        taos_close(taos);
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }

    snprintf(superTbl->colsOfCreateChildTable, len + TIMESTAMP_BUFF_LEN,
             "(ts timestamp%s)", cols);
    verbosePrint("%s() LN%d: %s\n", __func__, __LINE__,
                 superTbl->colsOfCreateChildTable);

    if (superTbl->tagCount == 0) {
        errorPrint("super table tag count is %d\n", superTbl->tagCount);
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
            errorPrint("config error tag type : %s\n", dataType);
            return -1;
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
        return -1;
    }

    debugPrint("create supertable %s success!\n\n", superTbl->stbName);
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
                char *cmd = calloc(1, BUFFER_SIZE);
                if (NULL == cmd) {
                    errorPrint("%s", "failed to allocate memory\n");
                    return -1;
                }

                ret = createSuperTable(taos, g_Dbs.db[i].dbName,
                                       &g_Dbs.db[i].superTbls[j], cmd);
                tmfree(cmd);

                if (0 != ret) {
                    tmfree(g_Dbs.db[i].superTbls[j].colsOfCreateChildTable);
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
    int32_t* code = calloc(1, sizeof(int32_t));
    *code = -1;
    setThreadName("createTable");

    uint64_t lastPrintTime = taosGetTimestampMs();

    int buff_len = BUFFER_SIZE;

    pThreadInfo->buffer = calloc(1, buff_len);
    if (NULL == pThreadInfo->buffer) {
        errorPrint("%s", "failed to allocate memory\n");
        goto create_table_end;
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
                errorPrint(
                    "%s() LN%d, use metric, but super table info is NULL\n",
                    __func__, __LINE__);
                goto create_table_end;
            } else {
                if (0 == len) {
                    batchNum = 0;
                    memset(pThreadInfo->buffer, 0, buff_len);
                    len += snprintf(pThreadInfo->buffer + len, buff_len - len,
                                    "CREATE TABLE ");
                }

                char *tagsValBuf = (char *)calloc(TSDB_MAX_SQL_LEN + 1, 1);
                if (NULL == tagsValBuf) {
                    errorPrint("%s", "failed to allocate memory\n");
                    goto create_table_end;
                }

                if (0 == stbInfo->tagSource) {
                    if (generateTagValuesForStb(stbInfo, i, tagsValBuf)) {
                        tmfree(tagsValBuf);
                        goto create_table_end;
                    }
                } else {
                    snprintf(tagsValBuf, TSDB_MAX_SQL_LEN, "(%s)",
                             stbInfo->tagDataBuf +
                                 stbInfo->lenOfTagOfOneRow *
                                     (i % stbInfo->tagSampleCount));
                }
                len += snprintf(
                    pThreadInfo->buffer + len, buff_len - len,
                    stbInfo->escapeChar ? "if not exists %s.`%s%" PRIu64
                                          "` using %s.`%s` tags %s "
                                        : "if not exists %s.%s%" PRIu64
                                          " using %s.%s tags %s ",
                    pThreadInfo->db_name, stbInfo->childTblPrefix, i,
                    pThreadInfo->db_name, stbInfo->stbName, tagsValBuf);
                tmfree(tagsValBuf);
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
            goto create_table_end;
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
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
    }
    *code = 0;
    create_table_end:
    tmfree(pThreadInfo->buffer);
    return code;
}

int startMultiThreadCreateChildTable(char *cols, int threads,
                                     uint64_t tableFrom, int64_t ntables,
                                     char *db_name, SSuperTable *stbInfo) {
    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    if (NULL == pids) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    if (NULL == infos) {
        errorPrint("%s", "failed to allocate memory\n");
        tmfree(pids);
        return -1;
    }

    if (threads < 1) {
        threads = 1;
    }

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = (int)ntables;
        a = 1;
    }

    int64_t b = 0;
    b = ntables % threads;

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = (int)i;
        tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
        pThreadInfo->stbInfo = stbInfo;
        verbosePrint("%s() %d db_name: %s\n", __func__, __LINE__, db_name);
        pThreadInfo->taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password,
                                         db_name, g_Dbs.port);
        if (pThreadInfo->taos == NULL) {
            errorPrint("failed to connect to TDengine, reason:%s\n",
                       taos_errstr(NULL));
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
        void* result;
        pthread_join(pids[i], &result);
        if (*(int32_t*)result) {
            g_fail = true;
        }
        tmfree(result);
    }

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        taos_close(pThreadInfo->taos);

        g_actualChildTables += pThreadInfo->tables_created;
    }

    free(pids);
    free(infos);
    if (g_fail) {
        return -1;
    }

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
    double start = (double)taosGetTimestampMs();
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
                if ((strcasecmp(g_args.dataType[j], "BINARY") == 0) ||
                    (strcasecmp(g_args.dataType[j], "NCHAR") == 0)) {
                    snprintf(tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len,
                             ",C%d %s(%d)", j, g_args.dataType[j],
                             g_args.binwidth);
                } else {
                    snprintf(tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len,
                             ",C%d %s", j, g_args.dataType[j]);
                }
                len = (int)strlen(tblColsBuf);
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
    double end = (double)taosGetTimestampMs();
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

void postFreeResource() {
    tmfclose(g_fpOfInsertResult);
    tmfree(g_dupstr);
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
    tmfree(g_randdouble_buff);
    tmfree(g_randuint_buff);
    tmfree(g_randutinyint_buff);
    tmfree(g_randusmallint_buff);
    tmfree(g_randubigint_buff);
    tmfree(g_randint);
    tmfree(g_randuint);
    tmfree(g_randbigint);
    tmfree(g_randubigint);
    tmfree(g_randfloat);
    tmfree(g_randdouble);
    tmfree(g_sampleDataBuf);

    for (int l = 0; l < g_args.columnCount; l++) {
        if (g_sampleBindBatchArray) {
            tmfree((char *)((uintptr_t) * (uintptr_t *)(g_sampleBindBatchArray +
                                                        sizeof(char *) * l)));
        }
    }
    tmfree(g_sampleBindBatchArray);
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
            res = taos_schemaless_insert(
                pThreadInfo->taos, pThreadInfo->lines,
                stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL ? 0 : k,
                stbInfo->lineProtocol, stbInfo->tsPrecision);
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
            errorPrint("Unknown insert mode: %d\n", stbInfo->iface);
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

static int execStbBindParamBatch(threadInfo *pThreadInfo, char *tableName,
                                 int64_t tableSeq, uint32_t batch,
                                 uint64_t insertRows, uint64_t recordFrom,
                                 int64_t startTime, int64_t *pSamplePos) {
    TAOS_STMT *stmt = pThreadInfo->stmt;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    uint32_t columnCount = pThreadInfo->stbInfo->columnCount;

    uint32_t thisBatch = (uint32_t)(MAX_SAMPLES - (*pSamplePos));

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
                    errorPrint("wrong data type: %d\n", data_type);
                    return -1;
            }
        }

        param->buffer_type = data_type;
        param->length = calloc(1, sizeof(int32_t) * thisBatch);
        if (param->length == NULL) {
            errorPrint("%s", "failed to allocate memory\n");
            return -1;
        }

        for (int b = 0; b < thisBatch; b++) {
            if (param->buffer_type == TSDB_DATA_TYPE_NCHAR) {
                param->length[b] = (int32_t)strlen(
                    (char *)param->buffer + b * stbInfo->columns[c].dataLen);
            } else {
                param->length[b] = (int32_t)param->buffer_length;
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

    if (taos_stmt_bind_param_batch(
            stmt, (TAOS_MULTI_BIND *)pThreadInfo->bindParams)) {
        errorPrint("taos_stmt_bind_param_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return -1;
    }

    for (int c = 0; c < stbInfo->columnCount + 1; c++) {
        TAOS_MULTI_BIND *param =
            (TAOS_MULTI_BIND *)(pThreadInfo->bindParams +
                                sizeof(TAOS_MULTI_BIND) * c);
        free(param->length);
    }

    // if msg > 3MB, break
    if (taos_stmt_add_batch(stmt)) {
        errorPrint("taos_stmt_add_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return -1;
    }
    return k;
}

int32_t prepareStbStmt(threadInfo *pThreadInfo, char *tableName,
                       int64_t tableSeq, uint32_t batch, uint64_t insertRows,
                       uint64_t recordFrom, int64_t startTime,
                       int64_t *pSamplePos) {
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_STMT *  stmt = pThreadInfo->stmt;

    char *tagsArray = calloc(1, sizeof(TAOS_BIND) * stbInfo->tagCount);
    if (NULL == tagsArray) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }
    char *tagsValBuf = (char *)calloc(TSDB_MAX_SQL_LEN + 1, 1);

    if (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable) {
        if (0 == stbInfo->tagSource) {
            if (generateTagValuesForStb(stbInfo, tableSeq, tagsValBuf)) {
                tmfree(tagsValBuf);
                return -1;
            }
        } else {
            snprintf(
                tagsValBuf, TSDB_MAX_SQL_LEN, "(%s)",
                stbInfo->tagDataBuf + stbInfo->lenOfTagOfOneRow *
                                          (tableSeq % stbInfo->tagSampleCount));
        }

        if (prepareStbStmtBindTag(tagsArray, stbInfo, tagsValBuf,
                                  pThreadInfo->time_precision)) {
            tmfree(tagsValBuf);
            tmfree(tagsArray);
            return -1;
        }

        if (taos_stmt_set_tbname_tags(stmt, tableName,
                                      (TAOS_BIND *)tagsArray)) {
            errorPrint("taos_stmt_set_tbname_tags() failed! reason: %s\n",
                       taos_stmt_errstr(stmt));
            return -1;
        }

    } else {
        if (taos_stmt_set_tbname(stmt, tableName)) {
            errorPrint("taos_stmt_set_tbname() failed! reason: %s\n",
                       taos_stmt_errstr(stmt));
            return -1;
        }
    }
    tmfree(tagsValBuf);
    tmfree(tagsArray);
    return execStbBindParamBatch(pThreadInfo, tableName, tableSeq, batch,
                                 insertRows, recordFrom, startTime, pSamplePos);
}

// stmt sync write interlace data
static void *syncWriteInterlaceStmtBatch(threadInfo *pThreadInfo,
                                         uint32_t    interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### stmt interlace write\n",
               pThreadInfo->threadID, __func__, __LINE__);
    int32_t* code = calloc(1, sizeof (int32_t));
    *code = -1;
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
                goto free_of_interlace_stmt;
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
                    batch = (uint32_t)remainPerInterlace;
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

                int64_t affectedRows =
                    execInsert(pThreadInfo, (uint32_t)generated);

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
                    (int)(pThreadInfo->totalAffectedRows * 100 / totalRows);
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
                taosMsleep((int32_t)sleepTime);  // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    if (percentComplete < 100)
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);
    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_interlace_stmt:
    return code;
}

void *syncWriteInterlace(threadInfo *pThreadInfo, uint32_t interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### interlace write\n", pThreadInfo->threadID,
               __func__, __LINE__);
    int32_t* code = calloc(1, sizeof (int32_t));
    *code = -1;
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

    if (interlaceRows > g_args.reqPerReq) interlaceRows = g_args.reqPerReq;

    uint32_t batchPerTbl = interlaceRows;
    uint32_t batchPerTblTimes;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes = g_args.reqPerReq / interlaceRows;
    } else {
        batchPerTblTimes = 1;
    }
    pThreadInfo->buffer = calloc(maxSqlLen, 1);
    if (NULL == pThreadInfo->buffer) {
        errorPrint("%s", "failed to allocate memory\n");
        goto free_of_interlace;
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
                goto free_of_interlace;
            }

            uint64_t oldRemainderLen = remainderBufLen;

            if (stbInfo) {
                generated = generateStbInterlaceData(
                    pThreadInfo, tableName, batchPerTbl, i, batchPerTblTimes,
                    tableSeq, pstr, insertRows, startTime, &remainderBufLen);
            } else {
                generated = (int32_t)generateInterlaceDataWithoutStb(
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
                    batchPerTbl = (uint32_t)remainRows;

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

        int currentPercent =
            (int)(pThreadInfo->totalAffectedRows * 100 / totalRows);
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
                taosMsleep((int32_t)sleepTime);  // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    if (percentComplete < 100)
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);
    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_interlace:
    tmfree(pThreadInfo->buffer);
    return code;
}

static void *syncWriteInterlaceSml(threadInfo *pThreadInfo,
                                   uint32_t    interlaceRows) {
    int32_t* code = calloc(1, sizeof (int32_t));
    *code = -1;
    debugPrint("[%d] %s() LN%d: ### interlace schemaless write\n",
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

    if (interlaceRows > g_args.reqPerReq) interlaceRows = g_args.reqPerReq;

    uint32_t batchPerTbl = interlaceRows;
    uint32_t batchPerTblTimes;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes = g_args.reqPerReq / interlaceRows;
    } else {
        batchPerTblTimes = 1;
    }

    char **smlList;
    cJSON *tagsList;
    cJSON *jsonArray;
    if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL ||
        stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL) {
        smlList = (char **)calloc(pThreadInfo->ntables, sizeof(char *));
        if (NULL == smlList) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_of_interlace_sml;
        }

        for (int t = 0; t < pThreadInfo->ntables; t++) {
            char *sml = (char *)calloc(1, stbInfo->lenOfOneRow);
            if (NULL == sml) {
                errorPrint("%s", "failed to allocate memory\n");
                goto free_smlheadlist_interlace_sml;
            }
            if (generateSmlConstPart(sml, stbInfo, pThreadInfo, t)) {
                goto free_smlheadlist_interlace_sml;
            }
            smlList[t] = sml;
        }

        pThreadInfo->lines = calloc(g_args.reqPerReq, sizeof(char *));
        if (NULL == pThreadInfo->lines) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_smlheadlist_interlace_sml;
        }

        for (int i = 0; i < g_args.reqPerReq; i++) {
            pThreadInfo->lines[i] = calloc(1, stbInfo->lenOfOneRow);
            if (NULL == pThreadInfo->lines[i]) {
                errorPrint("%s", "failed to allocate memory\n");
                goto free_lines_interlace_sml;
            }
        }
    } else {
        jsonArray = cJSON_CreateArray();
        tagsList = cJSON_CreateArray();
        for (int t = 0; t < pThreadInfo->ntables; t++) {
            if (generateSmlJsonTags(tagsList, stbInfo, pThreadInfo, t)) {
                goto free_json_interlace_sml;
            }
        }

        pThreadInfo->lines = (char **)calloc(1, sizeof(char *));
        if (NULL == pThreadInfo->lines) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_json_interlace_sml;
        }
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

        uint32_t recOfBatch = 0;

        for (uint64_t i = 0; i < batchPerTblTimes; i++) {
            int64_t timestamp = startTime;
            for (int j = recOfBatch; j < recOfBatch + batchPerTbl; j++) {
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    cJSON *tag = cJSON_Duplicate(
                        cJSON_GetArrayItem(
                            tagsList,
                            (int)(tableSeq - pThreadInfo->start_table_from)),
                        true);
                    if (generateSmlJsonCols(jsonArray, tag, stbInfo,
                                            pThreadInfo, timestamp)) {
                        goto free_json_interlace_sml;
                    }
                } else {
                    if (generateSmlMutablePart(
                            pThreadInfo->lines[j],
                            smlList[tableSeq - pThreadInfo->start_table_from],
                            stbInfo, pThreadInfo, timestamp)) {
                        goto free_lines_interlace_sml;
                    }
                }

                timestamp += timeStampStep;
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
                if (generatedRecPerTbl >= insertRows) {
                    break;
                }

                int64_t remainRows = insertRows - generatedRecPerTbl;
                if ((remainRows > 0) && (batchPerTbl > remainRows)) {
                    batchPerTbl = (uint32_t)remainRows;
                }

                if (pThreadInfo->ntables * batchPerTbl < g_args.reqPerReq) {
                    break;
                }
            }

            verbosePrint("[%d] %s() LN%d generatedRecPerTbl=%" PRId64
                         " insertRows=%" PRId64 "\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         generatedRecPerTbl, insertRows);

            if ((g_args.reqPerReq - recOfBatch) < batchPerTbl) {
                break;
            }
        }

        verbosePrint("[%d] %s() LN%d recOfBatch=%d totalInsertRows=%" PRIu64
                     "\n",
                     pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                     pThreadInfo->totalInsertRows);
        verbosePrint("[%d] %s() LN%d, buffer=%s\n", pThreadInfo->threadID,
                     __func__, __LINE__, pThreadInfo->buffer);

        if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
            pThreadInfo->lines[0] = cJSON_Print(jsonArray);
        }

        startTs = taosGetTimestampUs();

        if (recOfBatch == 0) {
            errorPrint("Failed to insert records of batch %d\n", batchPerTbl);
            if (batchPerTbl > 0) {
                errorPrint(
                    "\tIf the batch is %d, the length of the SQL to insert a "
                    "row must be less then %" PRId64 "\n",
                    batchPerTbl, maxSqlLen / batchPerTbl);
            }
            errorPrint("\tPlease check if the buffer length(%" PRId64
                       ") or batch(%d) is set with proper value!\n",
                       maxSqlLen, batchPerTbl);
            goto free_lines_interlace_sml;
        }
        int64_t affectedRows = execInsert(pThreadInfo, recOfBatch);

        endTs = taosGetTimestampUs();
        uint64_t delay = endTs - startTs;

        if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
            tmfree(pThreadInfo->lines[0]);
            cJSON_Delete(jsonArray);
            jsonArray = cJSON_CreateArray();
        }

        performancePrint("%s() LN%d, insert execution time is %10.2f ms\n",
                         __func__, __LINE__, delay / 1000.0);
        verbosePrint("[%d] %s() LN%d affectedRows=%" PRId64 "\n",
                     pThreadInfo->threadID, __func__, __LINE__, affectedRows);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        if (recOfBatch != affectedRows) {
            errorPrint("execInsert insert %d, affected rows: %" PRId64 "\n%s\n",
                       recOfBatch, affectedRows, pThreadInfo->buffer);
            goto free_lines_interlace_sml;
        }

        pThreadInfo->totalAffectedRows += affectedRows;

        int currentPercent =
            (int)(pThreadInfo->totalAffectedRows * 100 / totalRows);
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
                taosMsleep((int32_t)sleepTime);  // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    if (percentComplete < 100)
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);

    *code = 0;
    printStatPerThread(pThreadInfo);
    free_of_interlace_sml:
    if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
        tmfree(pThreadInfo->lines);
    free_json_interlace_sml:
        if (jsonArray != NULL) {
            cJSON_Delete(jsonArray);
        }
        if (tagsList != NULL) {
            cJSON_Delete(tagsList);
        }
    } else {
    free_lines_interlace_sml:
        for (int index = 0; index < g_args.reqPerReq; index++) {
            tmfree(pThreadInfo->lines[index]);
        }
        tmfree(pThreadInfo->lines);
    free_smlheadlist_interlace_sml:
        for (int index = 0; index < pThreadInfo->ntables; index++) {
            tmfree(smlList[index]);
        }
        tmfree(smlList);
    }
    return code;
}

void *syncWriteProgressiveStmt(threadInfo *pThreadInfo) {
    debugPrint("%s() LN%d: ### stmt progressive write\n", __func__, __LINE__);
    int32_t* code = calloc(1, sizeof (int32_t));
    *code = -1;
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
                goto free_of_stmt_progressive;
            }

            // measure prepare + insert
            startTs = taosGetTimestampUs();

            int32_t generated;
            if (stbInfo) {
                generated = prepareStbStmt(
                    pThreadInfo, tableName, tableSeq,
                    (uint32_t)((g_args.reqPerReq > stbInfo->insertRows)
                                   ? stbInfo->insertRows
                                   : g_args.reqPerReq),
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
                errorPrint("affected rows: %d\n", affectedRows);
                goto free_of_stmt_progressive;
            }

            pThreadInfo->totalAffectedRows += affectedRows;

            int currentPercent =
                (int)(pThreadInfo->totalAffectedRows * 100 / totalRows);
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
    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_stmt_progressive:
    tmfree(pThreadInfo->buffer);
    return code;
}

void *syncWriteProgressive(threadInfo *pThreadInfo) {
    debugPrint("%s() LN%d: ### progressive write\n", __func__, __LINE__);
    int32_t* code = calloc(1, sizeof (int32_t));
    *code = -1;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    uint64_t     maxSqlLen = stbInfo ? stbInfo->maxSqlLen : g_args.max_sql_len;
    int64_t      timeStampStep =
        stbInfo ? stbInfo->timeStampStep : g_args.timestamp_step;
    int64_t insertRows = (stbInfo) ? stbInfo->insertRows : g_args.insertRows;
    verbosePrint("%s() LN%d insertRows=%" PRId64 "\n", __func__, __LINE__,
                 insertRows);

    pThreadInfo->buffer = calloc(maxSqlLen, 1);
    if (NULL == pThreadInfo->buffer) {
        errorPrint("%s", "failed to allocate memory\n");
        goto free_of_progressive;
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
                goto free_of_progressive;
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
                        (uint32_t)((g_args.reqPerReq > stbInfo->insertRows)
                                       ? stbInfo->insertRows
                                       : g_args.reqPerReq),
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
                errorPrint("affected rows: %d\n", affectedRows);
                goto free_of_progressive;
            }

            pThreadInfo->totalAffectedRows += affectedRows;

            int currentPercent =
                (int)(pThreadInfo->totalAffectedRows * 100 / totalRows);
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
    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_progressive:
    tmfree(pThreadInfo->buffer);
    return code;
}

void *syncWriteProgressiveSml(threadInfo *pThreadInfo) {
    debugPrint("%s() LN%d: ### sml progressive write\n", __func__, __LINE__);
    int32_t * code = calloc(1, sizeof (int32_t));
    *code = -1;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int64_t      timeStampStep = stbInfo->timeStampStep;
    int64_t      insertRows = stbInfo->insertRows;
    verbosePrint("%s() LN%d insertRows=%" PRId64 "\n", __func__, __LINE__,
                 insertRows);

    uint64_t lastPrintTime = taosGetTimestampMs();

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    pThreadInfo->samplePos = 0;

    char **smlList;
    cJSON *tagsList;
    cJSON *jsonArray;

    if (insertRows < g_args.reqPerReq) {
        g_args.reqPerReq = (uint32_t)insertRows;
    }

    if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL ||
        stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL) {
        smlList = (char **)calloc(pThreadInfo->ntables, sizeof(char *));
        if (NULL == smlList) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_of_progressive_sml;
        }
        for (int t = 0; t < pThreadInfo->ntables; t++) {
            char *sml = (char *)calloc(1, stbInfo->lenOfOneRow);
            if (NULL == sml) {
                errorPrint("%s", "failed to allocate memory\n");
                goto free_smlheadlist_progressive_sml;
            }
            if (generateSmlConstPart(sml, stbInfo, pThreadInfo, t)) {
                goto free_smlheadlist_progressive_sml;
            }
            smlList[t] = sml;
        }

        pThreadInfo->lines = (char **)calloc(g_args.reqPerReq, sizeof(char *));
        if (NULL == pThreadInfo->lines) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_smlheadlist_progressive_sml;
        }

        for (int i = 0; i < g_args.reqPerReq; i++) {
            pThreadInfo->lines[i] = (char *)calloc(1, stbInfo->lenOfOneRow);
            if (NULL == pThreadInfo->lines[i]) {
                errorPrint("%s", "failed to allocate memory\n");
                goto free_lines_progressive_sml;
            }
        }
    } else {
        jsonArray = cJSON_CreateArray();
        tagsList = cJSON_CreateArray();
        for (int t = 0; t < pThreadInfo->ntables; t++) {
            if (generateSmlJsonTags(tagsList, stbInfo, pThreadInfo, t)) {
                goto free_json_progressive_sml;
            }
        }

        pThreadInfo->lines = (char **)calloc(1, sizeof(char *));
        if (NULL == pThreadInfo->lines) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_json_progressive_sml;
        }
    }
    int currentPercent = 0;
    int percentComplete = 0;

    for (uint64_t i = 0; i < pThreadInfo->ntables; i++) {
        int64_t timestamp = pThreadInfo->start_time;
        for (uint64_t j = 0; j < insertRows;) {
            for (int k = 0; k < g_args.reqPerReq; k++) {
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    cJSON *tag = cJSON_Duplicate(
                        cJSON_GetArrayItem(tagsList, (int)i), true);
                    if (generateSmlJsonCols(jsonArray, tag, stbInfo,
                                            pThreadInfo, timestamp)) {
                        goto free_json_progressive_sml;
                    }
                } else {
                    if (generateSmlMutablePart(pThreadInfo->lines[k],
                                               smlList[i], stbInfo,
                                               pThreadInfo, timestamp)) {
                        goto free_lines_progressive_sml;
                    }
                }
                timestamp += timeStampStep;
                j++;
                if (j == insertRows) {
                    break;
                }
            }
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = cJSON_Print(jsonArray);
            }
            uint64_t startTs = taosGetTimestampUs();
            int32_t  affectedRows = execInsert(pThreadInfo, g_args.reqPerReq);
            uint64_t endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                tmfree(pThreadInfo->lines[0]);
                cJSON_Delete(jsonArray);
                jsonArray = cJSON_CreateArray();
            }

            performancePrint("%s() LN%d, insert execution time is %10.f ms\n",
                             __func__, __LINE__, delay / 1000.0);
            verbosePrint("[%d] %s() LN%d affectedRows=%d\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         affectedRows);

            if (delay > pThreadInfo->maxDelay) {
                pThreadInfo->maxDelay = delay;
            }
            if (delay < pThreadInfo->minDelay) {
                pThreadInfo->minDelay = delay;
            }
            pThreadInfo->cntDelay++;
            pThreadInfo->totalDelay += delay;

            pThreadInfo->totalAffectedRows += affectedRows;
            pThreadInfo->totalInsertRows += g_args.reqPerReq;
            currentPercent = (int)(pThreadInfo->totalAffectedRows * 100 /
                                   (insertRows * pThreadInfo->ntables));
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

            if (j == insertRows) {
                break;
            }
        }
    }

    *code = 0;
    free_of_progressive_sml:
    if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
        tmfree(pThreadInfo->lines);
    free_json_progressive_sml:
        if (jsonArray != NULL) {
            cJSON_Delete(jsonArray);
        }
        if (tagsList != NULL) {
            cJSON_Delete(tagsList);
        }
    } else {
    free_lines_progressive_sml:
        for (int index = 0; index < g_args.reqPerReq; index++) {
            tmfree(pThreadInfo->lines[index]);
        }
        tmfree(pThreadInfo->lines);
    free_smlheadlist_progressive_sml:
        for (int index = 0; index < pThreadInfo->ntables; index++) {
            tmfree(smlList[index]);
        }
        tmfree(smlList);
    }
    return code;
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
                return syncWriteInterlaceStmtBatch(pThreadInfo, interlaceRows);
            } else if (SML_IFACE == stbInfo->iface) {
                return syncWriteInterlaceSml(pThreadInfo, interlaceRows);
            } else {
                return syncWriteInterlace(pThreadInfo, interlaceRows);
            }
        }
    } else {
        // progressive mode
        if (((stbInfo) && (STMT_IFACE == stbInfo->iface)) ||
            (STMT_IFACE == g_args.iface)) {
            return syncWriteProgressiveStmt(pThreadInfo);
        } else if (((stbInfo) && (SML_IFACE == stbInfo->iface)) ||
                   (SML_IFACE == g_args.iface)) {
            return syncWriteProgressiveSml(pThreadInfo);
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
        (int)(stbInfo ? stbInfo->insertInterval : g_args.insert_interval);
    if (insert_interval) {
        pThreadInfo->et = taosGetTimestampMs();
        if ((pThreadInfo->et - pThreadInfo->st) < insert_interval) {
            taosMsleep(insert_interval -
                       (int32_t)(pThreadInfo->et - pThreadInfo->st));  // ms
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
        (int)(stbInfo ? stbInfo->insertInterval : g_args.insert_interval);
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
    if (stbInfo) {
        stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    }

    if (0 != precision[0]) {
        if (0 == strncasecmp(precision, "ms", 2)) {
            timePrec = TSDB_TIME_PRECISION_MILLI;
            if (stbInfo) {
                stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
            }
        } else if (0 == strncasecmp(precision, "us", 2)) {
            timePrec = TSDB_TIME_PRECISION_MICRO;
            if (stbInfo) {
                stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_MICRO_SECONDS;
            }
        } else if (0 == strncasecmp(precision, "ns", 2)) {
            timePrec = TSDB_TIME_PRECISION_NANO;
            if (stbInfo) {
                stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_NANO_SECONDS;
            }
        } else {
            errorPrint("Not support precision: %s\n", precision);
            return -1;
        }
    }
    if (stbInfo) {
        if (stbInfo->iface == SML_IFACE) {
            if (stbInfo->lineProtocol != TSDB_SML_LINE_PROTOCOL) {
                if (stbInfo->columnCount != 1) {
                    errorPrint(
                        "Schemaless telnet/json protocol can only have 1 "
                        "column "
                        "instead of %d\n",
                        stbInfo->columnCount);
                    return -1;
                }
                stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_NOT_CONFIGURED;
            }
            if (stbInfo->lineProtocol != TSDB_SML_JSON_PROTOCOL) {
                calcRowLen(stbInfo);
            }
        }
    }

    int64_t startTime;
    if (stbInfo) {
        if (0 == strncasecmp(stbInfo->startTimestamp, "now", 3)) {
            startTime = taosGetTimestamp(timePrec);
        } else {
            if (TSDB_CODE_SUCCESS !=
                taosParseTime(stbInfo->startTimestamp, &startTime,
                              (int32_t)strlen(stbInfo->startTimestamp),
                              timePrec, 0)) {
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

    if (ret) {
        errorPrint("%s", "prepare sample data for stable failed!\n");
        return -1;
    }

    TAOS *taos0 = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, db_name,
                               g_Dbs.port);
    if (NULL == taos0) {
        errorPrint("connect to taosd fail , reason: %s\n", taos_errstr(NULL));
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
            if (NULL == stbInfo->childTblName) {
                errorPrint("%s", "failed to allocate memory\n");
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
        threads = (int)ntables;
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

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    if (pids == NULL) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    if (infos == NULL) {
        errorPrint("%s", "failed to allocate memory\n");
        tmfree(pids);
        return -1;
    }

    char *stmtBuffer = calloc(1, BUFFER_SIZE);
    if (stmtBuffer == NULL) {
        errorPrint("%s", "failed to allocate memory\n");
        tmfree(pids);
        tmfree(infos);
        return -1;
    }

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
        batch = (uint32_t)((g_args.reqPerReq > g_args.insertRows)
                               ? g_args.insertRows
                               : g_args.reqPerReq);
    }

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
        parseSamplefileToStmtBatch(stbInfo);
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
                    "connect to server fail from insert sub "
                    "thread,reason:%s\n ",
                    taos_errstr(NULL));
                return -1;
            }

            if ((g_args.iface == STMT_IFACE) ||
                ((stbInfo) && (stbInfo->iface == STMT_IFACE))) {
                pThreadInfo->stmt = taos_stmt_init(pThreadInfo->taos);
                if (NULL == pThreadInfo->stmt) {
                    free(pids);
                    free(infos);
                    errorPrint("taos_stmt_init() failed, reason: %s\n",
                               taos_errstr(NULL));
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
                    parseStbSampleToStmtBatchForThread(pThreadInfo, stbInfo,
                                                       timePrec, batch);

                } else {
                    parseNtbSampleToStmtBatchForThread(pThreadInfo, timePrec,
                                                       batch);
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
        void* result;
        pthread_join(pids[i], &result);
        if (*(int32_t*)result){
            g_fail = true;
        }
        tmfree(result);
    }

    uint64_t totalDelay = 0;
    uint64_t maxDelay = 0;
    uint64_t minDelay = UINT64_MAX;
    uint64_t cntDelay = 0;
    double   avgDelay = 0;

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;

        tsem_destroy(&(pThreadInfo->lock_sem));
        taos_close(pThreadInfo->taos);

        if (pThreadInfo->stmt) {
            taos_stmt_close(pThreadInfo->stmt);
        }

        tmfree((char *)pThreadInfo->bind_ts);

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

    free(pids);
    free(infos);

    if (g_fail){
        return -1;
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

    return 0;
}

int insertTestProcess() {
    int32_t code = -1;
    char *  cmdBuffer = calloc(1, BUFFER_SIZE);
    if (NULL == cmdBuffer) {
        errorPrint("%s", "failed to allocate memory\n");
        goto end_insert_process;
    }

    printfInsertMeta();

    debugPrint("%d result file: %s\n", __LINE__, g_Dbs.resultFile);
    g_fpOfInsertResult = fopen(g_Dbs.resultFile, "a");
    if (NULL == g_fpOfInsertResult) {
        errorPrint("failed to open %s for save result\n", g_Dbs.resultFile);
        goto end_insert_process;
    }

    if (g_fpOfInsertResult) {
        printfInsertMetaToFile(g_fpOfInsertResult);
    }

    prompt();

    if (init_rand_data()) {
        goto end_insert_process;
    }

    // create database and super tables

    if (createDatabasesAndStables(cmdBuffer)) {
        goto end_insert_process;
    }

    // pretreatment
    if (prepareSampleData()) {
        goto end_insert_process;
    }

    if (g_args.iface != SML_IFACE && g_totalChildTables > 0) {
        if (createChildTables()) {
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
                        if (startMultiThreadInsertData(
                                g_Dbs.threadCount, g_Dbs.db[i].dbName,
                                g_Dbs.db[i].dbCfg.precision, stbInfo)) {
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
                if (startMultiThreadInsertData(
                        g_Dbs.threadCount, g_Dbs.db[i].dbName,
                        g_Dbs.db[i].dbCfg.precision, NULL)) {
                    goto end_insert_process;
                }
            }
        }
    }
    code = 0;
end_insert_process:
    tmfree(cmdBuffer);
    return code;
}