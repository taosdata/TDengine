/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#define _GNU_SOURCE

#ifdef WEBSOCKET

#include "dump.h"
#include "dumpUtil.h"

int cleanIfQueryFailedWS(const char *funcname, int lineno, char *command, WS_RES *res) {
    errorPrint("%s() LN%d, failed to run command <%s>. code: 0x%08x, reason: %s\n", funcname, lineno, command,
               ws_errno(res), ws_errstr(res));
    ws_free_result(res);
    free(command);
    return -1;
}

int getTableRecordInfoImplWS(char *dbName, char *table, TableRecordInfo *pTableRecordInfo, bool tryStable) {
    WS_TAOS *ws_taos = NULL;
    WS_RES  *ws_res;
    int32_t  ws_code = -1;

    if (NULL == (ws_taos = wsConnect())) {
        return -1;
    }
    memset(pTableRecordInfo, 0, sizeof(TableRecordInfo));

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, g_args.db_escape_char ? "USE `%s`" : "USE %s", dbName);
    ws_res = wsQuery(&ws_taos, command, &ws_code);
    if (ws_code != 0) {
        errorPrint("Invalid database %s, reason: %s\n", dbName, ws_errstr(ws_res));
        ws_free_result(ws_res);
        ws_res = NULL;
        free(command);
        return 0;
    }
    ws_free_result(ws_res);

    if (3 == g_majorVersionOfClient) {
        if (tryStable) {
            snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                     "SELECT STABLE_NAME FROM information_schema.ins_stables "
                     "WHERE db_name='%s' AND stable_name='%s'",
                     dbName, table);
        } else {
            snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                     "SELECT TABLE_NAME,STABLE_NAME FROM "
                     "information_schema.ins_tables "
                     "WHERE db_name='%s' AND table_name='%s'",
                     dbName, table);
        }
    } else {
        if (tryStable) {
            snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, "SHOW STABLES LIKE \'%s\'", table);
        } else {
            snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, "SHOW TABLES LIKE \'%s\'", table);
        }
    }

    ws_res = wsQuery(&ws_taos, command, &ws_code);

    if (ws_code != 0) {
        cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
        ws_close(ws_taos);
        return -1;
    }

    bool isSet = false;

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);
        if (ws_code) {
            errorPrint("%s() LN%d, ws_fetch_raw_block() error. reason: %s!\n", __func__, __LINE__, ws_errstr(ws_res));
            ws_free_result(ws_res);
            ws_res = NULL;
            ws_close(ws_taos);
            ws_taos = NULL;
            free(command);
            return 0;
        }

        if (0 == rows) {
            break;
        }

        uint8_t  type;
        uint32_t length;
        char     buffer[TSDB_TABLE_NAME_LEN] = {0};

        for (int row = 0; row < rows; row++) {
            const void *value0 = ws_get_value_in_block(ws_res, row, TSDB_SHOW_DB_NAME_INDEX, &type, &length);
            if (NULL == value0) {
                errorPrint(
                    "%s() LN%d, row: %d, col: %d, "
                    "ws_get_value_in_block() error!\n",
                    __func__, __LINE__, row, TSDB_SHOW_DB_NAME_INDEX);
                continue;
            }

            memset(buffer, 0, TSDB_TABLE_NAME_LEN);
            memcpy(buffer, value0, length);

            if (0 == strcmp(buffer, table)) {
                if (tryStable) {
                    pTableRecordInfo->isStb = true;
                    tstrncpy(pTableRecordInfo->tableRecord.stable, buffer, min(TSDB_TABLE_NAME_LEN, length + 1));
                    isSet = true;
                } else {
                    pTableRecordInfo->isStb = false;
                    tstrncpy(pTableRecordInfo->tableRecord.name, buffer, min(TSDB_TABLE_NAME_LEN, length + 1));
                    const void *value1 = NULL;
                    if (3 == g_majorVersionOfClient) {
                        value1 = ws_get_value_in_block(ws_res, row, 1, &type, &length);
                    } else {
                        value1 = ws_get_value_in_block(ws_res, row, TSDB_SHOW_TABLES_METRIC_INDEX, &type, &length);
                    }
                    if (length) {
                        if (NULL == value1) {
                            errorPrint(
                                "%s() LN%d, row: %d, col: %d, "
                                "ws_get_value_in_block() error!\n",
                                __func__, __LINE__, row, TSDB_SHOW_TABLES_METRIC_INDEX);
                            break;
                        }

                        pTableRecordInfo->belongStb = true;
                        memset(buffer, 0, TSDB_TABLE_NAME_LEN);
                        memcpy(buffer, value1, length);
                        tstrncpy(pTableRecordInfo->tableRecord.stable, buffer, min(TSDB_TABLE_NAME_LEN, length + 1));
                    } else {
                        pTableRecordInfo->belongStb = false;
                    }
                    isSet = true;
                    break;
                }
            }
        }

        if (isSet) {
            break;
        }
    }

    ws_free_result(ws_res);
    ws_res = NULL;
    ws_close(ws_taos);
    ws_taos = NULL;

    free(command);

    if (isSet) {
        return 0;
    }
    return -1;
}

int getTableRecordInfoWS(char *dbName, char *table, TableRecordInfo *pTableRecordInfo) {
    if (0 == getTableRecordInfoImplWS(dbName, table, pTableRecordInfo, false)) {
        return 0;
    } else if (0 == getTableRecordInfoImplWS(dbName, table, pTableRecordInfo, true)) {
        return 0;
    }

    errorPrint("Invalid table/stable %s\n", table);
    return -1;
}

int getDbCountWS(WS_RES *ws_res) {
    int     count = 0;
    int32_t code;

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        code = ws_fetch_raw_block(ws_res, &data, &rows);
        if (code) {
            errorPrint("%s() LN%d, ws_fetch_raw_block() error. reason: %s!\n", __func__, __LINE__, ws_errstr(ws_res));
            return 0;
        }

        if (0 == rows) {
            break;
        }

        uint8_t  type;
        uint32_t length;
        char     buffer[VALUE_BUF_LEN] = {0};

        for (int row = 0; row < rows; row++) {
            const void *value = ws_get_value_in_block(ws_res, row, TSDB_SHOW_DB_NAME_INDEX, &type, &length);
            if (NULL == value) {
                errorPrint(
                    "%s() LN%d, row: %d, "
                    "ws_get_value_in_block() error!\n",
                    __func__, __LINE__, row);
                continue;
            }

            memset(buffer, 0, VALUE_BUF_LEN);
            memcpy(buffer, value, length);
            debugPrint("%s() LN%d, dbname: %s\n", __func__, __LINE__, buffer);

            if (isSystemDatabase(buffer)) {
                if (!g_args.allow_sys) {
                    continue;
                }
            } else if (g_args.databases) {  // input multi dbs
                if (inDatabasesSeq(buffer) != 0) {
                    continue;
                }
            } else if (!g_args.all_databases) {  // only input one db
                if (strcmp(g_args.arg_list[0], buffer)) {
                    continue;
                }
            }
            count++;
        }
    }

    return count;
}

int64_t getNtbCountOfStbWS(char *dbName, const char *stbName) {
    WS_TAOS *ws_taos;
    if (NULL == (ws_taos = wsConnect())) {
        return -1;
    }

    int64_t count = 0;
    char   *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }
    if (3 == g_majorVersionOfClient) {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                 g_args.db_escape_char ? "SELECT COUNT(*) FROM (SELECT DISTINCT(TBNAME) "
                                         "FROM `%s`.%s%s%s)"
                                       : "SELECT COUNT(*) FROM (SELECT DISTINCT(TBNAME) "
                                         "FROM %s.%s%s%s)",
                 dbName, g_escapeChar, stbName, g_escapeChar);
    } else {
        snprintf(
            command, TSDB_MAX_ALLOWED_SQL_LEN,
            g_args.db_escape_char ? "SELECT COUNT(TBNAME) FROM `%s`.%s%s%s" : "SELECT COUNT(TBNAME) FROM %s.%s%s%s",
            dbName, g_escapeChar, stbName, g_escapeChar);
    }
    debugPrint("get stable child count %s", command);

    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(&ws_taos, command, &ws_code);
    if (ws_code) {
        return cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
    }
    tfree(command);

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);
        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from ws_fetch_raw_block(), "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, ws_taos, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t  type;
        uint32_t len;

        for (int row = 0; row < rows; row++) {
            const void *value = ws_get_value_in_block(ws_res, row, TSDB_SHOW_TABLES_NAME_INDEX, &type, &len);
            if (0 == len) {
                errorPrint(
                    "%s() LN%d, row: %d, col: %d, "
                    "ws_get_value_in_block() error!\n",
                    __func__, __LINE__, TSDB_DESCRIBE_METRIC_FIELD_INDEX, row);
                continue;
            }
            count = *(int64_t *)value;
        }
        break;
    }
    debugPrint("%s() LN%d, COUNT(TBNAME): %" PRId64 "\n", __func__, __LINE__, count);

    ws_free_result(ws_res);
    ws_close(ws_taos);
    return count;
}

int getTableTagValueWSV3(WS_TAOS **taos_v, const char *dbName, const char *table, TableDes **ppTableDes) {
    TableDes *tableDes = *ppTableDes;
    char     *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
             "SELECT tag_name,tag_value FROM information_schema.ins_tags "
             "WHERE db_name = '%s' AND table_name = '%s'",
             dbName, table);

    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code) {
        return cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
    }

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);

        if (ws_code) {
            errorPrint(
                "%s() LN%d, ws_fetch_raw_block() error, "
                "code: 0x%08x, command: %s, reason: %s\n",
                __func__, __LINE__, ws_code, command, ws_errstr(ws_res));
        }
        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from fetch to run "
                "command <%s>, "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, command, taos_v, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t  type;
        uint32_t len;
        int      index = tableDes->columns;

        for (int row = 0; row < rows; row++) {
            const void *value1 = ws_get_value_in_block(ws_res, row, 1, &type, &len);

            debugPrint("%s() LN%d, len=%d\n", __func__, __LINE__, len);

            if (NULL == value1) {
                strcpy(tableDes->cols[index].value, "NULL");
                strcpy(tableDes->cols[index].note, "NUL");
            } else if (0 != processFieldsValueV3(index, tableDes, value1, len)) {
                errorPrint("%s() LN%d, processFieldsValueV3 tag_value: %p\n", __func__, __LINE__, value1);
                ws_free_result(ws_res);
                free(command);
                return -1;
            }
            index++;
        }
    }

    ws_free_result(ws_res);
    free(command);

    return (tableDes->columns + tableDes->tags);
}

int getTableTagValueWSV2(WS_TAOS **taos_v, const char *dbName, const char *table, TableDes **ppTableDes) {
    TableDes *tableDes = *ppTableDes;
    char     *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }
    char *sqlstr = command;

    sqlstr += snprintf(sqlstr, TSDB_MAX_ALLOWED_SQL_LEN, "SELECT %s%s%s", g_escapeChar,
                       tableDes->cols[tableDes->columns].field, g_escapeChar);
    for (int i = tableDes->columns + 1; i < (tableDes->columns + tableDes->tags); i++) {
        sqlstr += sprintf(sqlstr, ",%s%s%s ", g_escapeChar, tableDes->cols[i].field, g_escapeChar);
    }
    sqlstr += sprintf(sqlstr, g_args.db_escape_char ? " FROM `%s`.%s%s%s LIMIT 1" : " FROM %s.%s%s%s LIMIT 1", dbName,
                      g_escapeChar, table, g_escapeChar);

    int32_t ws_code = -1;
    int32_t retryCount = 0;
    WS_RES  *ws_res = NULL;

 RETRY_QUERY:   
    ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code) {
        return cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
    }

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = wsFetchBlock(ws_res, &data, &rows);

        if (ws_code) {
            // output error
            errorPrint(
                "%s() LN%d, getTableTagValueWSV2-> wsFetchBlock() error, "
                "code: 0x%08x, sqlstr: %s, reason: %s\n",
                __func__, __LINE__, ws_code, sqlstr, ws_errstr(ws_res));

            // check can retry
            if(canRetry(ws_code, RETRY_TYPE_FETCH) && ++retryCount <= g_args.retryCount) {
                infoPrint("wsFetchBlock failed, goto wsQuery to retry %d\n", retryCount);
                ws_free_result(ws_res);
                ws_res = NULL;
                toolsMsleep(g_args.retrySleepMs);
                goto RETRY_QUERY;
            }

            // error break while
            break;
        }
        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from fetch to run "
                "command <%s>, "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, sqlstr, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t  type;
        uint32_t len;
        for (int row = 0; row < rows; row++) {
            for (int j = tableDes->columns; j < (tableDes->columns + tableDes->tags); j++) {
                const void *value = ws_get_value_in_block(ws_res, row, j - tableDes->columns, &type, &len);

                debugPrint("%s() LN%d, len=%d\n", __func__, __LINE__, len);

                if (NULL == value) {
                    strcpy(tableDes->cols[j].value, "NULL");
                    strcpy(tableDes->cols[j].note, "NUL");
                } else if (0 != processFieldsValueV2(j, tableDes, value, len)) {
                    errorPrint("%s() LN%d, processFieldsValueV2 value0: %p\n", __func__, __LINE__, value);
                    ws_free_result(ws_res);
                    free(command);
                    return -1;
                }
            }
        }
    }

    ws_free_result(ws_res);
    free(command);

    return (tableDes->columns + tableDes->tags);
}

int getTableTagValueWS(void  **taos_v, const char *dbName, const char *table, TableDes **ppTableDes) {
    int ret = -1;
    if (3 == g_majorVersionOfClient) {
        // if child-table have tag, V3 using select tag_value
        // from information_schema.ins_tag where table to get tagValue
        ret = getTableTagValueWSV2(taos_v, dbName, table, ppTableDes);
        if (ret < 0) {
            ret = getTableTagValueWSV3(taos_v, dbName, table, ppTableDes);
        }
    } else if (2 == g_majorVersionOfClient) {
        // if child-table have tag,
        // using  select tagName from table to get tagValue
        ret = getTableTagValueWSV2(taos_v, dbName, table, ppTableDes);
    } else {
        errorPrint("%s() LN%d, major version %d is not supported\n", __func__, __LINE__, g_majorVersionOfClient);
    }

    return ret;
}

int getTableDesFromStbWS(WS_TAOS **taos_v, const char *dbName, const TableDes *stbTableDes, const char *table,
                                TableDes **ppTableDes) {
    constructTableDesFromStb(stbTableDes, table, ppTableDes);
    return getTableTagValueWS(taos_v, dbName, table, ppTableDes);
}

int getTableDesWS(WS_TAOS **taos_v, const char *dbName, const char *table, TableDes *tableDes, const bool colOnly) {
    int   colCount = 0;
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, g_args.db_escape_char ? "DESCRIBE `%s`.%s%s%s" : "DESCRIBE %s.%s%s%s",
             dbName, g_escapeChar, table, g_escapeChar);

    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code) {
        return cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
    } else {
        debugPrint("%s() LN%d, run command <%s> success, ws_taos: %p\n", __func__, __LINE__, command, *taos_v);
    }

    tstrncpy(tableDes->name, table, TSDB_TABLE_NAME_LEN);
    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);
        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from ws_fetch_raw_block(), "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t     type;
        uint32_t    len;
        char        buffer[VALUE_BUF_LEN] = {0};
        const void *value = NULL;

        for (int row = 0; row < rows; row++) {
            value = ws_get_value_in_block(ws_res, row, TSDB_DESCRIBE_METRIC_FIELD_INDEX, &type, &len);
            if (NULL == value) {
                errorPrint(
                    "%s() LN%d, row: %d, col: %d, "
                    "ws_get_value_in_block() error!\n",
                    __func__, __LINE__, TSDB_DESCRIBE_METRIC_FIELD_INDEX, row);
                continue;
            }
            memset(buffer, 0, VALUE_BUF_LEN);
            memcpy(buffer, value, len);
            strncpy(tableDes->cols[colCount].field, buffer, len);

            value = ws_get_value_in_block(ws_res, row, TSDB_DESCRIBE_METRIC_TYPE_INDEX, &type, &len);
            if (NULL == value) {
                errorPrint(
                    "%s() LN%d, row: %d, col: %d, "
                    "ws_get_value_in_block() error!\n",
                    __func__, __LINE__, TSDB_DESCRIBE_METRIC_TYPE_INDEX, row);
                continue;
            }
            memset(buffer, 0, VALUE_BUF_LEN);
            memcpy(buffer, value, len);
            tableDes->cols[colCount].type = typeStrToType(buffer);

            value = ws_get_value_in_block(ws_res, row, TSDB_DESCRIBE_METRIC_LENGTH_INDEX, &type, &len);
            if (NULL == value) {
                errorPrint("row: %d, col: %d, ws_get_value_in_block() error!\n", TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
                           row);
                continue;
            }
            tableDes->cols[colCount].length = *((int *)value);

            value = ws_get_value_in_block(ws_res, row, TSDB_DESCRIBE_METRIC_NOTE_INDEX, &type, &len);
            if (NULL == value) {
                errorPrint("row: %d, col: %d, ws_get_value_in_block() error!\n", TSDB_DESCRIBE_METRIC_NOTE_INDEX, row);
                continue;
            }
            memset(buffer, 0, VALUE_BUF_LEN);
            memcpy(buffer, value, len);

            debugPrint("%s() LN%d, buffer: %s\n", __func__, __LINE__, buffer);

            strncpy(tableDes->cols[colCount].note, buffer, len);
            if (strcmp(tableDes->cols[colCount].note, "TAG") != 0) {
                tableDes->columns++;
            } else {
                tableDes->tags++;
            }
            colCount++;
        }
    }

    ws_free_result(ws_res);
    ws_res = NULL;
    free(command);

    if (colOnly) {
        return colCount;
    }

    return getTableTagValueWS(taos_v, dbName, table, &tableDes);
}

int64_t queryDbForDumpOutCountWS(char *command, WS_TAOS **taos_v, const char *dbName, const char *tbName,
                                 const int precision) {
    int64_t count = -1;
    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code != 0) {
        return cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
    }

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);
        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from ws_fetch_raw_block(), "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t  type;
        uint32_t len;

        for (int row = 0; row < rows; row++) {
            const void *value0 = ws_get_value_in_block(ws_res, row, TSDB_SHOW_TABLES_NAME_INDEX, &type, &len);
            if (NULL == value0) {
                if (0 == ws_errno(ws_res)) {
                    count = 0;
                    debugPrint("%s fetch row, count: %" PRId64 "\n", command, count);
                } else {
                    count = -1;
                    errorPrint(
                        "failed run %s to fetch row, ws_taos: %p, "
                        "code: 0x%08x, reason: %s\n",
                        command, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
                }
            } else {
                count = *(int64_t *)value0;
                debugPrint("%s fetch row, count: %" PRId64 "\n", command, count);
                break;
            }
        }
    }

    ws_free_result(ws_res);
    free(command);
    return count;
}

TAOS_RES *queryDbForDumpOutOffsetWS(WS_TAOS **taos_v, char *command) {
    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code) {
        cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
        return NULL;
    }
    free(command);
    return ws_res;
}

int64_t writeResultToAvroWS(const char *avroFilename, const char *dbName, const char *tbName, char *jsonSchema,
                            WS_TAOS **taos_v, int precision, int64_t start_time, int64_t end_time) {
    int64_t queryCount = queryDbForDumpOutCount(taos_v, dbName, tbName, precision);
    if (queryCount <= 0) {
        return 0;
    }

    avro_schema_t      schema;
    RecordSchema      *recordSchema;
    avro_file_writer_t db;

    avro_value_iface_t *wface = prepareAvroWface(avroFilename, jsonSchema, &schema, &recordSchema, &db);

    int64_t success = 0;
    int64_t failed = 0;

    bool printDot = true;

    int currentPercent = 0;
    int percentComplete = 0;

    int64_t limit = g_args.data_batch;
    int64_t offset = 0;

    do {
        if (queryCount > limit) {
            if (limit < (queryCount - offset)) {
                limit = queryCount - offset;
            }
        } else {
            limit = queryCount;
        }

        WS_RES  *ws_res = NULL;
        int numFields = 0;
        void *ws_fields = NULL;
        int32_t countInBatch = 0;
        int32_t retryCount = 0;

RETRY_QUERY:
        countInBatch = 0;
        ws_res = queryDbForDumpOutOffset(taos_v, dbName, tbName, precision, start_time, end_time, limit, offset);
        if (NULL == ws_res) {
            break;
        }

        numFields = ws_field_count(ws_res);
        if (3 == g_majorVersionOfClient) {
            const struct WS_FIELD *ws_fields_v3 = ws_fetch_fields(ws_res);
            ws_fields = (void *)ws_fields_v3;
        } else {
            const struct WS_FIELD_V2 *ws_fields_v2 = ws_fetch_fields_v2(ws_res);
            ws_fields = (void *)ws_fields_v2;
        }

        while (true) {
            int         rows = 0;
            const void *data = NULL;
            int32_t     ws_code = wsFetchBlock(ws_res, &data, &rows);

            if (ws_code) {
                errorPrint(
                    "%s() LN%d, writeResultToAvroWS->wsFetchBlock() error, ws_taos: %p, "
                    "code: 0x%08x, reason: %s\n",
                    __func__, __LINE__, *taos_v, ws_code, ws_errstr(ws_res));

                // check can retry
                if(canRetry(ws_code, RETRY_TYPE_FETCH) && ++retryCount <= g_args.retryCount) {
                    infoPrint("wsFetchBlock failed, goto wsQuery to retry %d limit=%"PRId64" offset=%"PRId64" queryCount=%"PRId64" \n",
                             retryCount, limit, offset, queryCount);
                    // need close old res
                    ws_free_result(ws_res);
                    ws_res = NULL;
                    toolsMsleep(g_args.retrySleepMs);
                    goto RETRY_QUERY;
                }

                // break 
                break;
            }

            if (0 == rows) {
                debugPrint(
                    "%s() LN%d, No more data from wsFetchBlock(), "
                    "ws_taos: %p, code: 0x%08x, reason:%s\n",
                    __func__, __LINE__, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
                break;
            }

            for (int row = 0; row < rows; row++) {
                avro_value_t record;
                avro_generic_value_new(wface, &record);

                avro_value_t avro_value, branch;

                if (!g_args.loose_mode) {
                    if (0 != avro_value_get_by_name(&record, "tbname", &avro_value, NULL)) {
                        errorPrint(
                            "%s() LN%d, avro_value_get_by_name(tbname) "
                            "failed\n",
                            __func__, __LINE__);
                        break;
                    }
                    avro_value_set_branch(&avro_value, 1, &branch);
                    avro_value_set_string(&branch, tbName);
                }

                for (int32_t f = 0; f < numFields; f++) {
                    uint8_t  type;
                    uint32_t len;

                    const void *value = ws_get_value_in_block(ws_res, row, f, &type, &len);

                    if (3 == g_majorVersionOfClient) {
                        struct WS_FIELD *ws_fields_3 = (struct WS_FIELD *)ws_fields;
                        processValueToAvro(f, record, avro_value, branch, ws_fields_3[f].name, ws_fields_3[f].type,
                                           ws_fields_3[f].bytes, value, len);
                    } else {
                        struct WS_FIELD_V2 *ws_fields_2 = (struct WS_FIELD_V2 *)ws_fields;
                        processValueToAvro(f, record, avro_value, branch, ws_fields_2[f].name, ws_fields_2[f].type,
                                           ws_fields_2[f].bytes, value, len);
                    }
                }

                if (0 != avro_file_writer_append_value(db, &record)) {
                    errorPrint(
                        "%s() LN%d, "
                        "Unable to write record to file. Message: %s\n",
                        __func__, __LINE__, avro_strerror());
                    failed--;
                } else {
                    success++;
                }

                countInBatch++;
                avro_value_decref(&record);
            }
        }

        if (countInBatch != limit) {
            errorPrint("%s() LN%d, actual dump out: %d, batch %" PRId64 "\n", __func__, __LINE__, countInBatch, limit);
        }
        ws_free_result(ws_res);
        ws_res = NULL;
        printDotOrX(offset, &printDot);
        offset += limit;

        currentPercent = ((offset) * 100 / queryCount);
        if (currentPercent > percentComplete) {
            // infoPrint("%d%% of %s\n", currentPercent, tbName);
            percentComplete = currentPercent;
        }
    } while (offset < queryCount);

    if (percentComplete < 100) {
        errorPrint("%d%% of %s\n", percentComplete, tbName);
    }

    avro_value_iface_decref(wface);
    freeRecordSchema(recordSchema);
    avro_file_writer_close(db);
    avro_schema_decref(schema);

    return success;
}

int64_t writeResultDebugWS(WS_RES *ws_res, FILE *fp, const char *dbName, const char *tbName) {
    int64_t totalRows = 0;

    int32_t sql_buf_len = g_args.max_sql_len;
    char   *tmpBuffer = (char *)calloc(1, sql_buf_len + 128);
    if (NULL == tmpBuffer) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return 0;
    }

    char *pstr = tmpBuffer;

    int64_t lastRowsPrint = 5000000;
    int     count = 0;

    int fieldCount = ws_field_count(ws_res);
    ASSERT(fieldCount > 0);

    void *ws_fields = NULL;
    if (3 == g_majorVersionOfClient) {
        const struct WS_FIELD *ws_fields_v3 = ws_fetch_fields(ws_res);
        ws_fields = (void *)ws_fields_v3;
    } else {
        const struct WS_FIELD_V2 *ws_fields_v2 = ws_fetch_fields_v2(ws_res);
        ws_fields = (void *)ws_fields_v2;
    }

    int32_t total_sqlstr_len = 0;

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        int32_t     ws_code = ws_fetch_raw_block(ws_res, &data, &rows);

        if (ws_code) {
            errorPrint(
                "%s() LN%d, ws_fetch_raw_block() error!"
                " code: 0x%08x, reason: %s\n",
                __func__, __LINE__, ws_code, ws_errstr(ws_res));
            break;
        }
        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from ws_fetch_raw_block(), "
                "code: 0x%08x, reason:%s\n",
                __func__, __LINE__, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        for (int row = 0; row < rows; row++) {
            int32_t curr_sqlstr_len = 0;

            if (count == 0) {
                total_sqlstr_len = 0;
                curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "INSERT INTO %s.%s VALUES (", dbName, tbName);
            } else {
                curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "(");
            }

            for (int f = 0; f < fieldCount; f++) {
                if (f != 0) {
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ", ");
                }
                uint8_t  type;
                uint32_t len;

                const void *value = ws_get_value_in_block(ws_res, row, f, &type, &len);
                if (NULL == value) {
                    errorPrint("row: %d, ws_get_value_in_block() error!\n", row);
                    continue;
                }

                if (3 == g_majorVersionOfClient) {
                    struct WS_FIELD *ws_fields_3 = (struct WS_FIELD *)ws_fields;
                    curr_sqlstr_len += processResultValue(pstr, curr_sqlstr_len, ws_fields_3[f].type, value, len);
                } else {
                    struct WS_FIELD_V2 *ws_fields_2 = (struct WS_FIELD_V2 *)ws_fields;
                    curr_sqlstr_len += processResultValue(pstr, curr_sqlstr_len, ws_fields_2[f].type, value, len);
                }
            }
            curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ")");

            totalRows++;
            count++;
            fprintf(fp, "%s", tmpBuffer);

            if (totalRows >= lastRowsPrint) {
                infoPrint(" %" PRId64 " rows already be dump-out from %s.%s\n", totalRows, dbName, tbName);
                lastRowsPrint += 5000000;
            }

            total_sqlstr_len += curr_sqlstr_len;

            if ((count >= g_args.data_batch) || (sql_buf_len - total_sqlstr_len < TSDB_MAX_BYTES_PER_ROW)) {
                fprintf(fp, ";\n");
                count = 0;
            }
        }
    }

    debugPrint("total_sqlstr_len: %d\n", total_sqlstr_len);

    fprintf(fp, "\n");
    free(tmpBuffer);

    return totalRows;
}

WS_RES *queryDbForDumpOutWS(WS_TAOS **taos_v, const char *dbName, const char *tbName, const int precision,
                            const int64_t start_time, const int64_t end_time) {
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return NULL;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
             g_args.db_escape_char ? "SELECT * FROM `%s`.%s%s%s WHERE _c0 >= %" PRId64
                                     " "
                                     "AND _c0 <= %" PRId64 " ORDER BY _c0 ASC;"
                                   : "SELECT * FROM %s.%s%s%s WHERE _c0 >= %" PRId64
                                     " "
                                     "AND _c0 <= %" PRId64 " ORDER BY _c0 ASC;",
             dbName, g_escapeChar, tbName, g_escapeChar, start_time, end_time);

    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code != 0) {
        cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
        return NULL;
    }

    free(command);
    return ws_res;
}

int64_t dumpTableDataAvroWS(char *dataFilename, int64_t index, const char *tbName, const bool belongStb,
                            const char *dbName, const int precision, int colCount, TableDes *tableDes,
                            int64_t start_time, int64_t end_time) {
    WS_TAOS *ws_taos;
    if (NULL == (ws_taos = wsConnect())) {
        return -1;
    }

    char *jsonSchema = NULL;
    if (0 != convertTbDesToJsonWrap(dbName, tbName, tableDes, colCount, &jsonSchema)) {
        errorPrint("%s() LN%d, convertTbDesToJsonWrap failed\n", __func__, __LINE__);
        ws_close(ws_taos);
        return -1;
    }

    int64_t totalRows =
        writeResultToAvroWS(dataFilename, dbName, tbName, jsonSchema, &ws_taos, precision, start_time, end_time);

    ws_close(ws_taos);
    ws_taos = NULL;
    tfree(jsonSchema);

    return totalRows;
}

int64_t fillTbNameArrWS(WS_TAOS **taos_v, char *command, char **tbNameArr, const char *stable, const int64_t preCount) {
    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code) {
        return cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
    }

    int currentPercent = 0;
    int percentComplete = 0;

    int64_t ntbCount = 0;
    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);

        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from ws_fetch_raw_block(), "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t  type;
        uint32_t len;

        for (int row = 0; row < rows; row++) {
            const void *value0 = ws_get_value_in_block(ws_res, row, TSDB_SHOW_TABLES_NAME_INDEX, &type, &len);
            if (NULL == value0) {
                errorPrint(
                    "%s() LN%d, ws_get_value_in_blocK() return NULL."
                    " code: 0x%08x, reason: %s!\n",
                    __func__, __LINE__, ws_errno(ws_res), ws_errstr(ws_res));
                continue;
            } else {
                debugPrint("%s() LN%d, ws_get_value_in_blocK() return %s. len: %d\n", __func__, __LINE__,
                           (char *)value0, len);
            }

            tbNameArr[ntbCount] = calloc(len + 1, 1);
            strncpy(tbNameArr[ntbCount], (char *)value0, len);

            debugPrint("%s() LN%d, sub table name: %s %" PRId64 " of stable: %s\n", __func__, __LINE__,
                       tbNameArr[ntbCount], ntbCount, stable);
            ++ntbCount;

            currentPercent = (ntbCount * 100 / preCount);

            if (currentPercent > percentComplete) {
                infoPrint("connection %p fetched %d%% of %s' tbname\n", *taos_v, currentPercent, stable);
                percentComplete = currentPercent;
            }
        }
    }

    if ((preCount > 0) && (percentComplete < 100)) {
        errorPrint("%d%% - total %" PRId64 " sub-table's names of stable: %s fetched\n", percentComplete, ntbCount,
                   stable);
    } else {
        okPrint("total %" PRId64 " sub-table's name of stable: %s fetched\n", ntbCount, stable);
    }

    ws_free_result(ws_res);
    free(command);
    return ntbCount;
}

int readNextTableDesWS(void* ws_res, TableDes* tbDes, int *idx, int *cnt) {
    // tbname, tagName , tagValue
    int index = 0;
    uint8_t type  = 0;
    uint32_t len   = 0;
    while( index < tbDes->tags) {
        // get block
        if(*idx >= *cnt || *cnt == 0) {
            const void *data = NULL;
            int ws_code = ws_fetch_raw_block(ws_res, &data, cnt);
            if (ws_code !=0 ) {
                // read to end
                errorPrint("read next ws_fetch_raw_block failed, err code=%d  idx=%d index=%d\n", ws_code, *idx, index);
                return -1;
            }

            if(*cnt == 0) {
                infoPrint("read schema over. tag columns %d.\n", tbDes->tags);
                break;
            }
            *idx = 0;
        }

        // read first column tbname
        const void *val = ws_get_value_in_block(ws_res, *idx, 0, &type, &len);
        if(val == NULL) {
            errorPrint("read tbname failed, idx=%d cnt=%d \n", *idx, *cnt);
            return -1;
        }

        // tbname changed check
        if(tbDes->name[0] == 0) {
            // first set tbName
            strncpy(tbDes->name, val, len);
        } else {
            // compare tbname change   
            if(!(strncmp(tbDes->name, val, len) == 0 
               && tbDes->name[len] == 0)) {
                // tbname cnanged, break
                break;
            }
        }

        // read third column tagvalue
        val = ws_get_value_in_block(ws_res, *idx, 2, &type, &len);
        // copy tagvalue
        if (NULL == val) {
            strcpy(tbDes->cols[index].value, "NULL");
            strcpy(tbDes->cols[index].note , "NUL");
        } else if (0 != processFieldsValueV3(index, tbDes, val, len)) {
            errorPrint("%s() LN%d, call processFieldsValueV3 tag_value: %p\n",
                    __func__, __LINE__, val);
            return -1;
        }
        
        // move next row
        *idx = *idx + 1;
        // counter ++
        index++;
    }

    // check tags count corrent
    if(*cnt && index != tbDes->tags) {
        errorPrint("child table %s read tags(%d) not equal stable tags (%d).\n", 
                    tbDes->name, index, tbDes->tags);
        return -1;
    }

    return index;
}

// read specail line, col
int32_t readRowWS(void *res, int32_t idx, int32_t col, uint32_t *len, char **data) {
  int32_t  i = 0;
  while (i <= idx) {
    // fetch block
    const void *block = NULL;
    int32_t     cnt = 0;
    int         ws_code = ws_fetch_raw_block(res, &block, &cnt);
    if (ws_code != 0) {
      errorPrint("readRow->ws_fetch_raw_block failed, err code=%d i=%d\n", ws_code, i);
      return -1;
    }

    // cnt check
    if (cnt == 0) {
      infoPrint("ws_fetch_raw_block read cnt zero. i=%d.\n", i);
      return -1;
    }

    // check idx
    if (i + cnt <= idx) {
      // move next block
      i += cnt;
      continue;
    }

    // set
    uint8_t     type = 0;
    const void *val = ws_get_value_in_block(res, idx, col, &type, len);
    if (val == NULL) {
      errorPrint("readRow ws_get_value_in_block failed, cnt=%d idx=%d col=%d \n", cnt, idx, col);
      return -1;
    }
    *data = (char *)val;
    break;
  }

  return 0;
}

void dumpExtraInfoVarWS(void **taos_v, FILE *fp) {
    char  buffer[BUFFER_LEN];
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return;
    }
    strcpy(command, "SHOW VARIABLES");

    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(taos_v, command, &ws_code);

    if (0 != ws_code) {
        warnPrint(
            "%s() LN%d, failed to run command %s, "
            "code: 0x%08x, reason: %s. Will use default settings\n",
            __func__, __LINE__, command, ws_code, ws_errstr(ws_res));
        fprintf(g_fpOfResult,
                "# SHOW VARIABLES failed, "
                "code: 0x%08x, reason:%s\n",
                ws_errno(ws_res), ws_errstr(ws_res));
        snprintf(buffer, BUFFER_LEN, "#!charset: %s\n", "UTF-8");
        size_t len = fwrite(buffer, 1, strlen(buffer), fp);
        if (len != strlen(buffer)) {
            errorPrint(
                "%s() LN%d, write to file. "
                "try to write %zu, actual len %zu, "
                "Errno is %d. Reason is %s.\n",
                __func__, __LINE__, strlen(buffer), len, errno, strerror(errno));
        }
        ws_free_result(ws_res);
        ws_res = NULL;
        free(command);
        return;
    }

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);

        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from ws_fetch_raw_block(), "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t  type;
        uint32_t len;
        char     tmp[BUFFER_LEN - 12] = {0};

        for (int row = 0; row < rows; row++) {
            const void *value0 = ws_get_value_in_block(ws_res, row, 0, &type, &len);
            memset(tmp, 0, BUFFER_LEN - 12);
            memcpy(tmp, value0, len);

            verbosePrint("%s() LN%d, value0: %s\n", __func__, __LINE__, tmp);
            if (0 == strcmp(tmp, "charset")) {
                const void *value1 = ws_get_value_in_block(ws_res, row, 1, &type, &len);
                memset(tmp, 0, BUFFER_LEN - 12);
                memcpy(tmp, value1, min(BUFFER_LEN - 13, len));
                snprintf(buffer, BUFFER_LEN, "#!charset: %s\n", tmp);
                debugPrint("%s() LN%d buffer: %s\n", __func__, __LINE__, buffer);
                size_t w_len = fwrite(buffer, 1, strlen(buffer), fp);
                if (w_len != strlen(buffer)) {
                    errorPrint(
                        "%s() LN%d, write to file. "
                        "try to write %zu, actual len %zu, "
                        "Errno is %d. Reason is %s.\n",
                        __func__, __LINE__, strlen(buffer), w_len, errno, strerror(errno));
                }
            }
        }
    }

    ws_free_result(ws_res);
    ws_res = NULL;
    free(command);
}

int queryDbImplWS(WS_TAOS **taos_v, char *command) {
    int     ret = 0;
    WS_RES *ws_res = NULL;
    int32_t ws_code = -1;

    ws_res = wsQuery(taos_v, command, &ws_code);

    if (ws_code) {
        errorPrint(
            "Failed to run <%s>, ws_taos: %p, "
            "code: 0x%08x, reason: %s\n",
            command, *taos_v, ws_code, ws_errstr(ws_res));
        ret = -1;
        ;
    }

    ws_free_result(ws_res);
    ws_res = NULL;
    return ret;
}

void dumpNormalTablesOfStbWS(threadInfo *pThreadInfo, FILE *fp, char *dumpFilename) {
    for (int64_t i = pThreadInfo->from; i < (pThreadInfo->from + pThreadInfo->count); i++) {
        char *tbName = pThreadInfo->tbNameArr[i];
        debugPrint("%s() LN%d, [%d] sub table %" PRId64 ": name: %s\n", __func__, __LINE__, pThreadInfo->threadIndex, i,
                   tbName);

        int64_t count;
        if (g_args.avro) {
            count = dumpNormalTable(i, &pThreadInfo->taos, pThreadInfo->dbInfo, true, pThreadInfo->stbName,
                                    pThreadInfo->stbDes, tbName, pThreadInfo->precision, dumpFilename, NULL);
        } else {
            count = dumpNormalTable(i, &pThreadInfo->taos, pThreadInfo->dbInfo, true, pThreadInfo->stbName,
                                    pThreadInfo->stbDes, tbName, pThreadInfo->precision, NULL, fp);
        }

        // show progress
        atomic_add_fetch_64(&g_tableDone, 1);
        infoPrint("%s.%s %" PRId64 "/%" PRId64 " %s dump data ok.\n", g_dbName, g_stbName, g_tableDone, g_tableCount,
                  tbName);
        if (count < 0) {
            break;
        } else {
            atomic_add_fetch_64(&g_totalDumpOutRows, count);
        }
    }

    return;
}

int64_t dumpStbAndChildTbOfDbWS(WS_TAOS **taos_v, SDbInfo *dbInfo, FILE *fpDbs) {
    int64_t ret = 0;

    //
    // obtain need dump all stable name
    //
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, g_args.db_escape_char ? "USE `%s`" : "USE %s", dbInfo->name);
    WS_RES *ws_res;
    int32_t ws_code = -1;

    ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code != 0) {
        errorPrint("Invalid database %s, reason: %s\n", dbInfo->name, ws_errstr(ws_res));
        ws_free_result(ws_res);
        free(command);
        return -1;
    }

    if (3 == g_majorVersionOfClient) {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                 "SELECT STABLE_NAME FROM information_schema.ins_stables "
                 "WHERE db_name='%s'",
                 dbInfo->name);
    } else {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, "SHOW STABLES");
    }

    ws_res = wsQuery(taos_v, command, &ws_code);

    if (ws_code) {
        return cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
    }

    // link
    SNode* head = NULL;
    SNode* end  = NULL;

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);

        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from ws_fetch_raw_block(), "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t  type;
        uint32_t len;

        for (int row = 0; row < rows; row++) {
            const void *value0 = ws_get_value_in_block(ws_res, row, TSDB_SHOW_DB_NAME_INDEX, &type, &len);
            if (NULL == value0) {
                errorPrint("row: %d, ws_get_value_in_block() error!\n", row);
                continue;
            }
            
            // put to linked list
            if (head == NULL) {
                head = end = mallocNode(value0, len);
                if(head == NULL) {
                    errorPrint("row: %d, mallocNode head error!\n", row);
                    continue;
                }
            } else {
                end->next = mallocNode(value0, len);
                if(end->next == NULL) {
                    errorPrint("row: %d, mallocNode next error!\n", row);
                    continue;
                }
                end = end->next;
            }
            // check
            debugPrint("%s() LN%d, stable: %s\n", __func__, __LINE__, end->name);
        }
    }

    free(command);

    // check except
    if (head == NULL) {
        infoPrint("%s() LN%d, stable count is zero.\n", __func__, __LINE__ );
        return 0;
    }

    //
    // dump stable data
    //
    SNode * next = head;
    while (next) {
        ret = dumpStbAndChildTb(taos_v, dbInfo, next->name, fpDbs);
        if (ret < 0) {
            errorPrint("%s() LN%d, stable: %s dump out failed\n", __func__, __LINE__, next->name);
            break;
        }
        // move next
        next = next->next;
    }

    // free nodes
    freeNodes(head);
    return ret;
}

int64_t dumpNTablesOfDbWS(WS_TAOS **taos_v, SDbInfo *dbInfo) {
    int64_t ret = 0;
    if (0 == dbInfo->ntables) {
        errorPrint("%s() LN%d, database: %s has 0 tables\n", __func__, __LINE__, dbInfo->name);
        return 0;
    }

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    WS_RES *ws_res;
    int32_t ws_code = -1;

    if (3 == g_majorVersionOfClient) {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                 "SELECT TABLE_NAME,STABLE_NAME FROM "
                 "information_schema.ins_tables WHERE db_name='%s'",
                 dbInfo->name);
    } else {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, g_args.db_escape_char ? "USE `%s`" : "USE %s", dbInfo->name);
        ws_res = wsQuery(taos_v, command, &ws_code);
        if (ws_code) {
            errorPrint("invalid database %s, code: 0x%08x, reason: %s\n", dbInfo->name, ws_code, ws_errstr(ws_res));
            ws_free_result(ws_res);
            ws_res = NULL;
            ws_close(taos_v);
            taos_v = NULL;
            free(command);
            return 0;
        }
        ws_free_result(ws_res);
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, "SHOW TABLES");
    }

    ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code) {
        errorPrint("Failed to show %s\'s tables, code: 0x%08x, reason: %s!\n", dbInfo->name, ws_code,
                   ws_errstr(ws_res));
        ws_free_result(ws_res);
        ws_res = NULL;
        ws_close(taos_v);
        taos_v = NULL;
        free(command);
        return 0;
    }

    // link
    SNode* head = NULL;
    SNode* end  = NULL;

    int64_t count = 0;
    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);

        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from ws_fetch_raw_block(), "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t  type;
        uint32_t len0, len1;

        for (int row = 0; row < rows; row++) {
            const void *value1 = NULL;
            if (3 == g_majorVersionOfClient) {
                value1 = ws_get_value_in_block(ws_res, row, 1, &type, &len1);
            } else {
                value1 = ws_get_value_in_block(ws_res, row, TSDB_SHOW_TABLES_METRIC_INDEX, &type, &len1);
            }

            if (len1) {
                if (g_args.debug_print || g_args.verbose_print) {
                    char buffer[VALUE_BUF_LEN];
                    memset(buffer, 0, VALUE_BUF_LEN);
                    memcpy(buffer, value1, len1);
                    debugPrint("%s() LN%d, get table belong %s\n", __func__, __LINE__, buffer);
                }
                continue;
            } else {
                const void *value0 = ws_get_value_in_block(ws_res, row, 0, &type, &len0);
                if ((NULL == value0) || (0 == len0)) {
                    errorPrint("%s() LN%d, value0: %p, type: %d, len0: %d\n", __func__, __LINE__, value0, type, len0);
                    continue;
                }

                // put to linked list
                if (head == NULL) {
                    head = end = mallocNode(value0, len0);
                    if (head == NULL) {
                        errorPrint("row: %d, mallocNode head error!\n", row);
                        continue;
                    }
                } else {
                    end->next = mallocNode(value0, len0);
                    if (end->next == NULL) {
                        errorPrint("row: %d, mallocNode next error!\n", row);
                        continue;
                    }
                    end = end->next;
                }

                debugPrint("%s() LN%d count: %" PRId64
                           ", table name: %s, "
                           "length: %d\n",
                           __func__, __LINE__, count, end->name, len0);
            }
            count++;
        }
    }

    ws_free_result(ws_res);
    free(command);

    // check except
    if (head == NULL) {
        infoPrint("%s() LN%d, normal table count is zero.\n", __func__, __LINE__ );
        return 0;
    }

    //
    // dump stable data
    //
    SNode * next = head;
    while (next) {
        ret = dumpANormalTableNotBelong(count, taos_v, dbInfo, next->name);
        if (0 == ret) {
            infoPrint("Dumping normal table: %s\n", next->name);
        } else {
            errorPrint("%s() LN%d, dump normal table: %s\n", __func__, __LINE__, next->name);
            break;
        }

        // move next
        next = next->next;
    }

    // free nodes
    freeNodes(head);

    return ret;
}

bool fillDBInfoWithFieldsWS(const int index, const char *name, const int row, const int f, WS_RES *res) {
    uint8_t  type;
    uint32_t len;
    char     tmp[VALUE_BUF_LEN] = {0};

    const void *value = ws_get_value_in_block(res, row, f, &type, &len);
    if (0 == strcmp(name, "name")) {
        if (NULL == value) {
            errorPrint(
                "%s() LN%d, row: %d, field: %d, "
                "ws_get_value_in_block() error!\n",
                __func__, __LINE__, row, f);
            return false;
        } else {
            memset(tmp, 0, VALUE_BUF_LEN);
            memcpy(tmp, value, len);
            strncpy(g_dbInfos[index]->name, tmp, len);
        }
    } else if (0 == strcmp(name, "vgroups")) {
        if (TSDB_DATA_TYPE_INT == type) {
            g_dbInfos[index]->vgroups = *((int32_t *)value);
        } else if (TSDB_DATA_TYPE_SMALLINT == type) {
            g_dbInfos[index]->vgroups = *((int16_t *)value);
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "ntables")) {
        if (TSDB_DATA_TYPE_INT == type) {
            g_dbInfos[index]->ntables = *((int32_t *)value);
        } else if (TSDB_DATA_TYPE_BIGINT == type) {
            g_dbInfos[index]->ntables = *((int64_t *)value);
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "replica")) {
        if (TSDB_DATA_TYPE_TINYINT == type) {
            g_dbInfos[index]->replica = *((int8_t *)value);
        } else if (TSDB_DATA_TYPE_SMALLINT == type) {
            g_dbInfos[index]->replica = *((int16_t *)value);
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "strict")) {
        tstrncpy(g_dbInfos[index]->strict, (char *)value, min(STRICT_LEN, len + 1));
        debugPrint("%s() LN%d: field: %d, strict: %s, length:%d\n", __func__, __LINE__, f, g_dbInfos[index]->strict,
                   len);
    } else if (0 == strcmp(name, "quorum")) {
        g_dbInfos[index]->quorum = *((int16_t *)value);
    } else if (0 == strcmp(name, "days")) {
        g_dbInfos[index]->days = *((int16_t *)value);
    } else if ((0 == strcmp(name, "keep")) || (0 == strcmp(name, "keep0,keep1,keep2"))) {
        tstrncpy(g_dbInfos[index]->keeplist, value, min(KEEPLIST_LEN, len + 1));
        debugPrint("%s() LN%d: field: %d, keep: %s, length:%d\n", __func__, __LINE__, f, g_dbInfos[index]->keeplist,
                   len);
    } else if (0 == strcmp(name, "duration")) {
        tstrncpy(g_dbInfos[index]->duration, value, min(DURATION_LEN, len + 1));
        debugPrint("%s() LN%d: field: %d, tmp: %s, duration: %s, length:%d\n", __func__, __LINE__, f, tmp,
                   g_dbInfos[index]->duration, len);
    } else if ((0 == strcmp(name, "cache")) || (0 == strcmp(name, "cache(MB)"))) {
        g_dbInfos[index]->cache = *((int32_t *)value);
    } else if (0 == strcmp(name, "blocks")) {
        g_dbInfos[index]->blocks = *((int32_t *)value);
    } else if (0 == strcmp(name, "minrows")) {
        if (TSDB_DATA_TYPE_INT == type) {
            g_dbInfos[index]->minrows = *((int32_t *)value);
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "maxrows")) {
        if (TSDB_DATA_TYPE_INT == type) {
            g_dbInfos[index]->maxrows = *((int32_t *)value);
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "wallevel")) {
        g_dbInfos[index]->wallevel = *((int8_t *)value);
    } else if (0 == strcmp(name, "wal")) {
        if (TSDB_DATA_TYPE_TINYINT == type) {
            g_dbInfos[index]->wal = *((int8_t *)value);
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "fsync")) {
        if (TSDB_DATA_TYPE_INT == type) {
            g_dbInfos[index]->fsync = *((int32_t *)value);
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "comp")) {
        if (TSDB_DATA_TYPE_TINYINT == type) {
            g_dbInfos[index]->comp = (int8_t)(*((int8_t *)value));
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "cachelast")) {
        if (TSDB_DATA_TYPE_TINYINT == type) {
            g_dbInfos[index]->cachelast = (int8_t)(*((int8_t *)value));
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "cache_model")) {
        if (TSDB_DATA_TYPE_TINYINT == type) {
            g_dbInfos[index]->cache_model = (int8_t)(*((int8_t *)value));
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "single_stable_model")) {
        if (TSDB_DATA_TYPE_BOOL == type) {
            g_dbInfos[index]->single_stable_model = (bool)(*((bool *)value));
        } else {
            errorPrint("%s() LN%d, unexpected type: %d\n", __func__, __LINE__, type);
            return false;
        }
    } else if (0 == strcmp(name, "precision")) {
        tstrncpy(g_dbInfos[index]->precision, (char *)value, min(DB_PRECISION_LEN, len + 1));
    } else if (0 == strcmp(name, "update")) {
        g_dbInfos[index]->update = *((int8_t *)value);
    }

    return true;
}

int fillDbExtraInfoV3WS(void **taos_v, const char *dbName, const int dbIndex) {
    int   ret = 0;
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }
    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
             "SELECT COUNT(table_name) FROM "
             "information_schema.ins_tables WHERE db_name='%s'",
             dbName);

    infoPrint("Getting table(s) count of db (%s) ...\n", dbName);

    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code) {
        return cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
    } else {
        while (true) {
            int         rows = 0;
            const void *data = NULL;
            ws_code = ws_fetch_raw_block(ws_res, &data, &rows);

            if (0 == rows) {
                debugPrint(
                    "%s() LN%d, No more data from ws_fetch_raw_block(), "
                    "ws_taos: %p, code: 0x%08x, reason:%s\n",
                    __func__, __LINE__, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
                break;
            }

            uint8_t  type;
            uint32_t len;
            for (int row = 0; row < rows; row++) {
                const void *value0 = ws_get_value_in_block(ws_res, row, TSDB_SHOW_DB_NAME_INDEX, &type, &len);
                if (NULL == value0) {
                    errorPrint("row: %d, ws_get_value_in_block() error!\n", row);
                    continue;
                }

                if (TSDB_DATA_TYPE_BIGINT == type) {
                    g_dbInfos[dbIndex]->ntables = *(int64_t *)value0;
                } else {
                    errorPrint("%s() LN%d, type: %d, not converted\n", __func__, __LINE__, type);
                }
            }
        }
    }

    ws_free_result(ws_res);
    free(command);
    return ret;
}

int fillDbInfoWS(void **taos_v) {
    int ret = 0;
    int dbIndex = 0;

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    if (3 == g_majorVersionOfClient) {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, "SELECT * FROM information_schema.ins_databases");
    } else {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, "SHOW DATABASES");
    }

    int32_t ws_code = -1;
    WS_RES *ws_res = wsQuery(taos_v, command, &ws_code);
    if (ws_code != 0) {
        return cleanIfQueryFailedWS(__func__, __LINE__, command, ws_res);
    }

    int   fieldCount = ws_field_count(ws_res);
    void *ws_fields = NULL;
    if (3 == g_majorVersionOfClient) {
        const struct WS_FIELD *ws_fields_v3 = ws_fetch_fields(ws_res);
        ws_fields = (void *)ws_fields_v3;
    } else {
        const struct WS_FIELD_V2 *ws_fields_v2 = ws_fetch_fields_v2(ws_res);
        ws_fields = (void *)ws_fields_v2;
    }

    while (true) {
        int         rows = 0;
        const void *data = NULL;
        ws_code = ws_fetch_raw_block(ws_res, &data, &rows);

        if (0 == rows) {
            debugPrint(
                "%s() LN%d, No more data from ws_fetch_raw_block(), "
                "ws_taos: %p, code: 0x%08x, reason:%s\n",
                __func__, __LINE__, *taos_v, ws_errno(ws_res), ws_errstr(ws_res));
            break;
        }

        uint8_t  type;
        uint32_t len;
        char     buffer[VALUE_BUF_LEN] = {0};

        for (int row = 0; row < rows; row++) {
            const void *value0 = ws_get_value_in_block(ws_res, row, TSDB_SHOW_DB_NAME_INDEX, &type, &len);
            if (NULL == value0) {
                errorPrint("row: %d, ws_get_value_in_block() error!\n", row);
                continue;
            }
            memset(buffer, 0, VALUE_BUF_LEN);
            memcpy(buffer, value0, len);
            debugPrint("%s() LN%d, dbname: %s\n", __func__, __LINE__, buffer);

            if (isSystemDatabase(buffer)) {
                if (!g_args.allow_sys) {
                    continue;
                }
            } else if (g_args.databases) {
                if (inDatabasesSeq(buffer) != 0) {
                    continue;
                }
            } else if (!g_args.all_databases) {
                if (strcmp(g_args.arg_list[0], buffer)) {
                    continue;
                }
            }

            g_dbInfos[dbIndex] = (SDbInfo *)calloc(1, sizeof(SDbInfo));
            if (NULL == g_dbInfos[dbIndex]) {
                errorPrint("%s() LN%d, failed to allocate %" PRIu64 " memory\n", __func__, __LINE__,
                           (uint64_t)sizeof(SDbInfo));
                ret = -1;
                break;
            }

            okPrint("Database: %s exists\n", buffer);
            if (3 == g_majorVersionOfClient) {
                struct WS_FIELD *fields = (struct WS_FIELD *)ws_fields;
                for (int f = 0; f < fieldCount; f++) {
                    if (false == fillDBInfoWithFieldsWS(dbIndex, fields[f].name, row, f, ws_res)) {
                        ret = -1;
                        break;
                    }
                }
            } else {
                struct WS_FIELD_V2 *fields = (struct WS_FIELD_V2 *)ws_fields;
                for (int f = 0; f < fieldCount; f++) {
                    if (false == fillDBInfoWithFieldsWS(dbIndex, fields[f].name, row, f, ws_res)) {
                        ret = -1;
                        break;
                    }
                }
            }

            if (3 == g_majorVersionOfClient) {
                fillDbExtraInfoV3WS(taos_v, g_dbInfos[dbIndex]->name, dbIndex);
            }

            dbIndex++;

            if (g_args.databases) {
                if (dbIndex > g_args.dumpDbCount) break;
            } else if (!g_args.all_databases) {
                if (dbIndex >= 1) break;
            }
        }
    }

    ws_free_result(ws_res);
    ws_res = NULL;
    free(command);

    if (0 != ret) {
        return ret;
    }

    return dbIndex;
}

bool jointCloudDsn() {
    if ((NULL != g_args.host) && strlen(g_args.host)) {
        if (0 == g_args.port) {
            snprintf(g_args.cloudHost, MAX_HOSTNAME_LEN, "ws://%s:6041", g_args.host);
        } else {
            snprintf(g_args.cloudHost, MAX_HOSTNAME_LEN, "ws://%s:%d", g_args.host, g_args.port);
        }
    } else {
        if (0 == g_args.port) {
            snprintf(g_args.cloudHost, MAX_HOSTNAME_LEN, "ws://localhost:6041");
        } else {
            snprintf(g_args.cloudHost, MAX_HOSTNAME_LEN, "ws://localhost:%d", g_args.port);
        }
    }

    g_args.dsn = g_args.cloudHost;
    debugPrint("%s() LN%d, dsn: %s\n", __func__, __LINE__, g_args.dsn);
    return true;
}

bool splitCloudDsn() {
    if (g_args.dsn) {
        char *token = strstr(g_args.dsn, "?token=");
        if (NULL == token) {
            return false;
        } else {
            g_args.cloudToken = token + strlen("?token=");
        }

        char *http = NULL, *https = NULL;
        http = strstr(g_args.dsn, "http://");
        if (NULL == http) {
            https = strstr(g_args.dsn, "https://");
            if (NULL == https) {
                tstrncpy(g_args.cloudHost, g_args.dsn, MAX_HOSTNAME_LEN);
            } else {
                tstrncpy(g_args.cloudHost, https + strlen("https://"), MAX_HOSTNAME_LEN);
            }
        } else {
            tstrncpy(g_args.cloudHost, http + strlen("http://"), MAX_HOSTNAME_LEN);
        }

        char *colon = strstr(g_args.cloudHost, ":");
        if (colon) {
            g_args.cloudHost[strlen(g_args.cloudHost) - strlen(colon)] = '\0';
            g_args.cloudPort = atoi(colon + 1);
        }

        return true;
    }

    return false;
}

int64_t dumpTableDataWS(const int64_t index, FILE *fp, const char *tbName, const char *dbName, const int precision,
                        TableDes *tableDes, const int64_t start_time, const int64_t end_time) {
    WS_TAOS *ws_taos;
    if (NULL == (ws_taos = wsConnect())) {
        return -1;
    }

    WS_RES *ws_res = queryDbForDumpOutWS(&ws_taos, dbName, tbName, precision, start_time, end_time);

    int64_t totalRows = -1;
    if (ws_res) {
        totalRows = writeResultDebugWS(ws_res, fp, dbName, tbName);
    }

    ws_free_result(ws_res);
    ws_res = NULL;
    ws_close(ws_taos);

    return totalRows;
}

#endif  // WEBSOCKET
