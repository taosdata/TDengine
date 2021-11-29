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

int getColumnAndTagTypeFromInsertJsonFile(cJSON *      stbInfo,
                                          SSuperTable *superTbls) {
    int32_t code = -1;

    // columns
    cJSON *columns = cJSON_GetObjectItem(stbInfo, "columns");
    if (columns && columns->type != cJSON_Array) {
        errorPrint("%s", "failed to read json, columns not found\n");
        goto PARSE_OVER;
    } else if (NULL == columns) {
        superTbls->columnCount = 0;
        superTbls->tagCount = 0;
        return 0;
    }

    int columnSize = cJSON_GetArraySize(columns);
    if ((columnSize + 1 /* ts */) > TSDB_MAX_COLUMNS) {
        errorPrint(
            "failed to read json, column size overflow, max column size is "
            "%d\n",
            TSDB_MAX_COLUMNS);
        goto PARSE_OVER;
    }

    int       count = 1;
    int       index = 0;
    StrColumn columnCase;

    // superTbls->columnCount = columnSize;
    for (int k = 0; k < columnSize; ++k) {
        cJSON *column = cJSON_GetArrayItem(columns, k);
        if (column == NULL) continue;

        count = 1;
        cJSON *countObj = cJSON_GetObjectItem(column, "count");
        if (countObj && countObj->type == cJSON_Number) {
            count = (int)countObj->valueint;
        } else if (countObj && countObj->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column count not found\n");
            goto PARSE_OVER;
        } else {
            count = 1;
        }

        // column info
        memset(&columnCase, 0, sizeof(StrColumn));
        cJSON *dataType = cJSON_GetObjectItem(column, "type");
        if (!dataType || dataType->type != cJSON_String ||
            dataType->valuestring == NULL) {
            errorPrint("%s", "failed to read json, column type not found\n");
            goto PARSE_OVER;
        }
        // tstrncpy(superTbls->columns[k].dataType, dataType->valuestring,
        // DATATYPE_BUFF_LEN);
        tstrncpy(columnCase.dataType, dataType->valuestring,
                 min(DATATYPE_BUFF_LEN, strlen(dataType->valuestring) + 1));

        cJSON *dataLen = cJSON_GetObjectItem(column, "len");
        if (dataLen && dataLen->type == cJSON_Number) {
            columnCase.dataLen = (uint32_t)dataLen->valueint;
        } else if (dataLen && dataLen->type != cJSON_Number) {
            debugPrint("%s() LN%d: failed to read json, column len not found\n",
                       __func__, __LINE__);
            goto PARSE_OVER;
        } else {
            columnCase.dataLen = SMALL_BUFF_LEN;
        }

        for (int n = 0; n < count; ++n) {
            tstrncpy(superTbls->columns[index].dataType, columnCase.dataType,
                     min(DATATYPE_BUFF_LEN, strlen(columnCase.dataType) + 1));

            superTbls->columns[index].dataLen = columnCase.dataLen;
            index++;
        }
    }

    if ((index + 1 /* ts */) > MAX_NUM_COLUMNS) {
        errorPrint(
            "failed to read json, column size overflow, allowed max column "
            "size is %d\n",
            MAX_NUM_COLUMNS);
        goto PARSE_OVER;
    }

    superTbls->columnCount = index;

    for (int c = 0; c < superTbls->columnCount; c++) {
        if (0 ==
            strncasecmp(superTbls->columns[c].dataType, "INT", strlen("INT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_INT;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "TINYINT",
                                    strlen("TINYINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_TINYINT;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "SMALLINT",
                                    strlen("SMALLINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_SMALLINT;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "BIGINT",
                                    strlen("BIGINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_BIGINT;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "FLOAT",
                                    strlen("FLOAT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_FLOAT;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "DOUBLE",
                                    strlen("DOUBLE"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_DOUBLE;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "BINARY",
                                    strlen("BINARY"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_BINARY;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "NCHAR",
                                    strlen("NCHAR"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_NCHAR;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "BOOL",
                                    strlen("BOOL"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_BOOL;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "TIMESTAMP",
                                    strlen("TIMESTAMP"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_TIMESTAMP;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "UTINYINT",
                                    strlen("UTINYINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_UTINYINT;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "USMALLINT",
                                    strlen("USMALLINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_USMALLINT;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "UINT",
                                    strlen("UINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_UINT;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType, "UBIGINT",
                                    strlen("UBIGINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_UBIGINT;
        } else {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_NULL;
        }
    }

    count = 1;
    index = 0;
    // tags
    cJSON *tags = cJSON_GetObjectItem(stbInfo, "tags");
    if (!tags || tags->type != cJSON_Array) {
        errorPrint("%s", "failed to read json, tags not found\n");
        goto PARSE_OVER;
    }

    int tagSize = cJSON_GetArraySize(tags);
    if (tagSize > TSDB_MAX_TAGS) {
        errorPrint(
            "failed to read json, tags size overflow, max tag size is %d\n",
            TSDB_MAX_TAGS);
        goto PARSE_OVER;
    }

    // superTbls->tagCount = tagSize;
    for (int k = 0; k < tagSize; ++k) {
        cJSON *tag = cJSON_GetArrayItem(tags, k);
        if (tag == NULL) continue;

        count = 1;
        cJSON *countObj = cJSON_GetObjectItem(tag, "count");
        if (countObj && countObj->type == cJSON_Number) {
            count = (int)countObj->valueint;
        } else if (countObj && countObj->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column count not found\n");
            goto PARSE_OVER;
        } else {
            count = 1;
        }

        // column info
        memset(&columnCase, 0, sizeof(StrColumn));
        cJSON *dataType = cJSON_GetObjectItem(tag, "type");
        if (!dataType || dataType->type != cJSON_String ||
            dataType->valuestring == NULL) {
            errorPrint("%s", "failed to read json, tag type not found\n");
            goto PARSE_OVER;
        }
        tstrncpy(columnCase.dataType, dataType->valuestring,
                 min(DATATYPE_BUFF_LEN, strlen(dataType->valuestring) + 1));

        cJSON *dataLen = cJSON_GetObjectItem(tag, "len");
        if (dataLen && dataLen->type == cJSON_Number) {
            columnCase.dataLen = (uint32_t)dataLen->valueint;
        } else if (dataLen && dataLen->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column len not found\n");
            goto PARSE_OVER;
        } else {
            columnCase.dataLen = 0;
        }

        for (int n = 0; n < count; ++n) {
            tstrncpy(superTbls->tags[index].dataType, columnCase.dataType,
                     min(DATATYPE_BUFF_LEN, strlen(columnCase.dataType) + 1));
            superTbls->tags[index].dataLen = columnCase.dataLen;
            index++;
        }
    }

    if (index > TSDB_MAX_TAGS) {
        errorPrint(
            "failed to read json, tags size overflow, allowed max tag count is "
            "%d\n",
            TSDB_MAX_TAGS);
        goto PARSE_OVER;
    }

    superTbls->tagCount = index;

    for (int t = 0; t < superTbls->tagCount; t++) {
        if (0 ==
            strncasecmp(superTbls->tags[t].dataType, "INT", strlen("INT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_INT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "TINYINT",
                                    strlen("TINYINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_TINYINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "SMALLINT",
                                    strlen("SMALLINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_SMALLINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "BIGINT",
                                    strlen("BIGINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_BIGINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "FLOAT",
                                    strlen("FLOAT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_FLOAT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "DOUBLE",
                                    strlen("DOUBLE"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_DOUBLE;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "BINARY",
                                    strlen("BINARY"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_BINARY;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "NCHAR",
                                    strlen("NCHAR"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_NCHAR;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "BOOL",
                                    strlen("BOOL"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_BOOL;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "TIMESTAMP",
                                    strlen("TIMESTAMP"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_TIMESTAMP;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "UTINYINT",
                                    strlen("UTINYINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_UTINYINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "USMALLINT",
                                    strlen("USMALLINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_USMALLINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "UINT",
                                    strlen("UINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_UINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType, "UBIGINT",
                                    strlen("UBIGINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_UBIGINT;
        } else {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_NULL;
        }
    }

    if ((superTbls->columnCount + superTbls->tagCount + 1 /* ts */) >
        TSDB_MAX_COLUMNS) {
        errorPrint(
            "columns + tags is more than allowed max columns count: %d\n",
            TSDB_MAX_COLUMNS);
        goto PARSE_OVER;
    }
    code = 0;

PARSE_OVER:
    return code;
}

int getMetaFromInsertJsonFile(cJSON *root) {
    int32_t code = -1;

    cJSON *cfgdir = cJSON_GetObjectItem(root, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(g_Dbs.cfgDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON *host = cJSON_GetObjectItem(root, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        tstrncpy(g_Dbs.host, host->valuestring, MAX_HOSTNAME_SIZE);
    } else if (!host) {
        tstrncpy(g_Dbs.host, "127.0.0.1", MAX_HOSTNAME_SIZE);
    } else {
        errorPrint("%s", "failed to read json, host not found\n");
        goto PARSE_OVER;
    }

    cJSON *port = cJSON_GetObjectItem(root, "port");
    if (port && port->type == cJSON_Number) {
        g_Dbs.port = (uint16_t)port->valueint;
    } else if (!port) {
        g_Dbs.port = DEFAULT_PORT;
    }

    cJSON *user = cJSON_GetObjectItem(root, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        tstrncpy(g_Dbs.user, user->valuestring, MAX_USERNAME_SIZE);
    } else if (!user) {
        tstrncpy(g_Dbs.user, TSDB_DEFAULT_USER, MAX_USERNAME_SIZE);
    }

    cJSON *password = cJSON_GetObjectItem(root, "password");
    if (password && password->type == cJSON_String &&
        password->valuestring != NULL) {
        tstrncpy(g_Dbs.password, password->valuestring, SHELL_MAX_PASSWORD_LEN);
    } else if (!password) {
        tstrncpy(g_Dbs.password, TSDB_DEFAULT_PASS, SHELL_MAX_PASSWORD_LEN);
    }

    cJSON *resultfile = cJSON_GetObjectItem(root, "result_file");
    if (resultfile && resultfile->type == cJSON_String &&
        resultfile->valuestring != NULL) {
        tstrncpy(g_Dbs.resultFile, resultfile->valuestring, MAX_FILE_NAME_LEN);
    } else if (!resultfile) {
        tstrncpy(g_Dbs.resultFile, DEFAULT_OUTPUT, MAX_FILE_NAME_LEN);
    }

    cJSON *threads = cJSON_GetObjectItem(root, "thread_count");
    if (threads && threads->type == cJSON_Number) {
        g_Dbs.threadCount = (uint32_t)threads->valueint;
    } else if (!threads) {
        g_Dbs.threadCount = DEFAULT_NTHREADS;
    } else {
        errorPrint("%s", "failed to read json, threads not found\n");
        goto PARSE_OVER;
    }

    cJSON *threads2 = cJSON_GetObjectItem(root, "thread_count_create_tbl");
    if (threads2 && threads2->type == cJSON_Number) {
        g_Dbs.threadCountForCreateTbl = (uint32_t)threads2->valueint;
    } else if (!threads2) {
        g_Dbs.threadCountForCreateTbl = DEFAULT_NTHREADS;
    } else {
        errorPrint("%s", "failed to read json, threads2 not found\n");
        goto PARSE_OVER;
    }

    cJSON *gInsertInterval = cJSON_GetObjectItem(root, "insert_interval");
    if (gInsertInterval && gInsertInterval->type == cJSON_Number) {
        if (gInsertInterval->valueint < 0) {
            errorPrint("%s",
                       "failed to read json, insert interval input mistake\n");
            goto PARSE_OVER;
        }
        g_args.insert_interval = gInsertInterval->valueint;
    } else if (!gInsertInterval) {
        g_args.insert_interval = DEFAULT_INSERT_INTERVAL;
    } else {
        errorPrint("%s",
                   "failed to read json, insert_interval input mistake\n");
        goto PARSE_OVER;
    }

    cJSON *interlaceRows = cJSON_GetObjectItem(root, "interlace_rows");
    if (interlaceRows && interlaceRows->type == cJSON_Number) {
        if (interlaceRows->valueint < 0) {
            errorPrint("%s",
                       "failed to read json, interlaceRows input mistake\n");
            goto PARSE_OVER;
        }
        g_args.interlaceRows = (uint32_t)interlaceRows->valueint;
    } else if (!interlaceRows) {
        g_args.interlaceRows =
            DEFAULT_INTERLACE_ROWS;  // 0 means progressive mode, > 0 mean
                                     // interlace mode. max value is less or equ
                                     // num_of_records_per_req
    } else {
        errorPrint("%s", "failed to read json, interlaceRows input mistake\n");
        goto PARSE_OVER;
    }

    cJSON *maxSqlLen = cJSON_GetObjectItem(root, "max_sql_len");
    if (maxSqlLen && maxSqlLen->type == cJSON_Number) {
        if (maxSqlLen->valueint < 0) {
            errorPrint(
                "%s() LN%d, failed to read json, max_sql_len input mistake\n",
                __func__, __LINE__);
            goto PARSE_OVER;
        }
        g_args.max_sql_len = maxSqlLen->valueint;
    } else if (!maxSqlLen) {
        g_args.max_sql_len = TSDB_MAX_ALLOWED_SQL_LEN;
    } else {
        errorPrint(
            "%s() LN%d, failed to read json, max_sql_len input mistake\n",
            __func__, __LINE__);
        goto PARSE_OVER;
    }

    cJSON *numRecPerReq = cJSON_GetObjectItem(root, "num_of_records_per_req");
    if (numRecPerReq && numRecPerReq->type == cJSON_Number) {
        if (numRecPerReq->valueint <= 0) {
            errorPrint(
                "%s() LN%d, failed to read json, num_of_records_per_req input "
                "mistake\n",
                __func__, __LINE__);
            goto PARSE_OVER;
        } else if (numRecPerReq->valueint > MAX_RECORDS_PER_REQ) {
            printf("NOTICE: number of records per request value %" PRIu64
                   " > %d\n\n",
                   numRecPerReq->valueint, MAX_RECORDS_PER_REQ);
            printf(
                "        number of records per request value will be set to "
                "%d\n\n",
                MAX_RECORDS_PER_REQ);
            prompt();
            numRecPerReq->valueint = MAX_RECORDS_PER_REQ;
        }
        g_args.reqPerReq = (uint32_t)numRecPerReq->valueint;
    } else if (!numRecPerReq) {
        g_args.reqPerReq = MAX_RECORDS_PER_REQ;
    } else {
        errorPrint(
            "%s() LN%d, failed to read json, num_of_records_per_req not "
            "found\n",
            __func__, __LINE__);
        goto PARSE_OVER;
    }

    cJSON *prepareRand = cJSON_GetObjectItem(root, "prepared_rand");
    if (prepareRand && prepareRand->type == cJSON_Number) {
        if (prepareRand->valueint <= 0) {
            errorPrint(
                "%s() LN%d, failed to read json, prepared_rand input mistake\n",
                __func__, __LINE__);
            goto PARSE_OVER;
        }
        g_args.prepared_rand = prepareRand->valueint;
    } else if (!prepareRand) {
        g_args.prepared_rand = DEFAULT_PREPARED_RAND;
    } else {
        errorPrint("%s", "failed to read json, prepared_rand not found\n");
        goto PARSE_OVER;
    }

    cJSON *chineseOpt = cJSON_GetObjectItem(root, "chinese");  // yes, no,
        if (chineseOpt && chineseOpt->type == cJSON_String &&
        chineseOpt->valuestring != NULL) {
            if (0 == strncasecmp(chineseOpt->valuestring, "yes", 3)) {
                g_args.chinese = true;
            } else if (0 == strncasecmp(chineseOpt->valuestring, "no", 2)) {
                g_args.chinese = false;
            } else {
                g_args.chinese = DEFAULT_CHINESE_OPT;
            }
        } else if (!chineseOpt) {
            g_args.chinese = DEFAULT_CHINESE_OPT;
        } else {
            errorPrint(
                    "%s",
                    "failed to read json, chinese input mistake\n");
            goto PARSE_OVER;
        }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(root, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strncasecmp(answerPrompt->valuestring, "yes", 3)) {
            g_args.answer_yes = false;
        } else if (0 == strncasecmp(answerPrompt->valuestring, "no", 2)) {
            g_args.answer_yes = true;
        } else {
            g_args.answer_yes = DEFAULT_ANS_YES;
        }
    } else if (!answerPrompt) {
        g_args.answer_yes = true;  // default is no, mean answer_yes.
    } else {
        errorPrint(
            "%s",
            "failed to read json, confirm_parameter_prompt input mistake\n");
        goto PARSE_OVER;
    }

    // rows per table need be less than insert batch
    if (g_args.interlaceRows > g_args.reqPerReq) {
        printf(
            "NOTICE: interlace rows value %u > num_of_records_per_req %u\n\n",
            g_args.interlaceRows, g_args.reqPerReq);
        printf(
            "        interlace rows value will be set to "
            "num_of_records_per_req %u\n\n",
            g_args.reqPerReq);
        prompt();
        g_args.interlaceRows = g_args.reqPerReq;
    }

    cJSON *dbs = cJSON_GetObjectItem(root, "databases");
    if (!dbs || dbs->type != cJSON_Array) {
        errorPrint("%s", "failed to read json, databases not found\n");
        goto PARSE_OVER;
    }

    int dbSize = cJSON_GetArraySize(dbs);
    if (dbSize > MAX_DB_COUNT) {
        errorPrint(
            "failed to read json, databases size overflow, max database is "
            "%d\n",
            MAX_DB_COUNT);
        goto PARSE_OVER;
    }
    g_Dbs.db = calloc(1, sizeof(SDataBase) * dbSize);
    assert(g_Dbs.db);
    g_Dbs.dbCount = dbSize;
    for (int i = 0; i < dbSize; ++i) {
        cJSON *dbinfos = cJSON_GetArrayItem(dbs, i);
        if (dbinfos == NULL) continue;

        // dbinfo
        cJSON *dbinfo = cJSON_GetObjectItem(dbinfos, "dbinfo");
        if (!dbinfo || dbinfo->type != cJSON_Object) {
            errorPrint("%s", "failed to read json, dbinfo not found\n");
            goto PARSE_OVER;
        }

        cJSON *dbName = cJSON_GetObjectItem(dbinfo, "name");
        if (!dbName || dbName->type != cJSON_String ||
            dbName->valuestring == NULL) {
            errorPrint("%s", "failed to read json, db name not found\n");
            goto PARSE_OVER;
        }
        tstrncpy(g_Dbs.db[i].dbName, dbName->valuestring, TSDB_DB_NAME_LEN);

        cJSON *drop = cJSON_GetObjectItem(dbinfo, "drop");
        if (drop && drop->type == cJSON_String && drop->valuestring != NULL) {
            if (0 == strncasecmp(drop->valuestring, "yes", strlen("yes"))) {
                g_Dbs.db[i].drop = true;
            } else {
                g_Dbs.db[i].drop = false;
            }
        } else if (!drop) {
            g_Dbs.db[i].drop = g_args.drop_database;
        } else {
            errorPrint("%s", "failed to read json, drop input mistake\n");
            goto PARSE_OVER;
        }

        cJSON *precision = cJSON_GetObjectItem(dbinfo, "precision");
        if (precision && precision->type == cJSON_String &&
            precision->valuestring != NULL) {
            tstrncpy(g_Dbs.db[i].dbCfg.precision, precision->valuestring,
                     SMALL_BUFF_LEN);
        } else if (!precision) {
            memset(g_Dbs.db[i].dbCfg.precision, 0, SMALL_BUFF_LEN);
        } else {
            errorPrint("%s", "failed to read json, precision not found\n");
            goto PARSE_OVER;
        }

        cJSON *update = cJSON_GetObjectItem(dbinfo, "update");
        if (update && update->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.update = (int)update->valueint;
        } else if (!update) {
            g_Dbs.db[i].dbCfg.update = -1;
        } else {
            errorPrint("%s", "failed to read json, update not found\n");
            goto PARSE_OVER;
        }

        cJSON *replica = cJSON_GetObjectItem(dbinfo, "replica");
        if (replica && replica->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.replica = (int)replica->valueint;
        } else if (!replica) {
            g_Dbs.db[i].dbCfg.replica = -1;
        } else {
            errorPrint("%s", "failed to read json, replica not found\n");
            goto PARSE_OVER;
        }

        cJSON *keep = cJSON_GetObjectItem(dbinfo, "keep");
        if (keep && keep->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.keep = (int)keep->valueint;
        } else if (!keep) {
            g_Dbs.db[i].dbCfg.keep = -1;
        } else {
            errorPrint("%s", "failed to read json, keep not found\n");
            goto PARSE_OVER;
        }

        cJSON *days = cJSON_GetObjectItem(dbinfo, "days");
        if (days && days->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.days = (int)days->valueint;
        } else if (!days) {
            g_Dbs.db[i].dbCfg.days = -1;
        } else {
            errorPrint("%s", "failed to read json, days not found\n");
            goto PARSE_OVER;
        }

        cJSON *cache = cJSON_GetObjectItem(dbinfo, "cache");
        if (cache && cache->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.cache = (int)cache->valueint;
        } else if (!cache) {
            g_Dbs.db[i].dbCfg.cache = -1;
        } else {
            errorPrint("%s", "failed to read json, cache not found\n");
            goto PARSE_OVER;
        }

        cJSON *blocks = cJSON_GetObjectItem(dbinfo, "blocks");
        if (blocks && blocks->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.blocks = (int)blocks->valueint;
        } else if (!blocks) {
            g_Dbs.db[i].dbCfg.blocks = -1;
        } else {
            errorPrint("%s", "failed to read json, block not found\n");
            goto PARSE_OVER;
        }

        // cJSON* maxtablesPerVnode= cJSON_GetObjectItem(dbinfo,
        // "maxtablesPerVnode"); if (maxtablesPerVnode &&
        // maxtablesPerVnode->type
        // == cJSON_Number) {
        //  g_Dbs.db[i].dbCfg.maxtablesPerVnode = maxtablesPerVnode->valueint;
        //} else if (!maxtablesPerVnode) {
        //  g_Dbs.db[i].dbCfg.maxtablesPerVnode = TSDB_DEFAULT_TABLES;
        //} else {
        // printf("failed to read json, maxtablesPerVnode not found");
        // goto PARSE_OVER;
        //}

        cJSON *minRows = cJSON_GetObjectItem(dbinfo, "minRows");
        if (minRows && minRows->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.minRows = (uint32_t)minRows->valueint;
        } else if (!minRows) {
            g_Dbs.db[i].dbCfg.minRows = 0;  // 0 means default
        } else {
            errorPrint("%s", "failed to read json, minRows not found\n");
            goto PARSE_OVER;
        }

        cJSON *maxRows = cJSON_GetObjectItem(dbinfo, "maxRows");
        if (maxRows && maxRows->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.maxRows = (uint32_t)maxRows->valueint;
        } else if (!maxRows) {
            g_Dbs.db[i].dbCfg.maxRows = 0;  // 0 means default
        } else {
            errorPrint("%s", "failed to read json, maxRows not found\n");
            goto PARSE_OVER;
        }

        cJSON *comp = cJSON_GetObjectItem(dbinfo, "comp");
        if (comp && comp->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.comp = (int)comp->valueint;
        } else if (!comp) {
            g_Dbs.db[i].dbCfg.comp = -1;
        } else {
            errorPrint("%s", "failed to read json, comp not found\n");
            goto PARSE_OVER;
        }

        cJSON *walLevel = cJSON_GetObjectItem(dbinfo, "walLevel");
        if (walLevel && walLevel->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.walLevel = (int)walLevel->valueint;
        } else if (!walLevel) {
            g_Dbs.db[i].dbCfg.walLevel = -1;
        } else {
            errorPrint("%s", "failed to read json, walLevel not found\n");
            goto PARSE_OVER;
        }

        cJSON *cacheLast = cJSON_GetObjectItem(dbinfo, "cachelast");
        if (cacheLast && cacheLast->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.cacheLast = (int)cacheLast->valueint;
        } else if (!cacheLast) {
            g_Dbs.db[i].dbCfg.cacheLast = -1;
        } else {
            errorPrint("%s", "failed to read json, cacheLast not found\n");
            goto PARSE_OVER;
        }

        cJSON *quorum = cJSON_GetObjectItem(dbinfo, "quorum");
        if (quorum && quorum->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.quorum = (int)quorum->valueint;
        } else if (!quorum) {
            g_Dbs.db[i].dbCfg.quorum = 1;
        } else {
            errorPrint("%s", "failed to read json, quorum input mistake");
            goto PARSE_OVER;
        }

        cJSON *fsync = cJSON_GetObjectItem(dbinfo, "fsync");
        if (fsync && fsync->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.fsync = (int)fsync->valueint;
        } else if (!fsync) {
            g_Dbs.db[i].dbCfg.fsync = -1;
        } else {
            errorPrint("%s", "failed to read json, fsync input mistake\n");
            goto PARSE_OVER;
        }

        // super_tables
        cJSON *stables = cJSON_GetObjectItem(dbinfos, "super_tables");
        if (!stables || stables->type != cJSON_Array) {
            errorPrint("%s", "failed to read json, super_tables not found\n");
            goto PARSE_OVER;
        }

        int stbSize = cJSON_GetArraySize(stables);
        if (stbSize > MAX_SUPER_TABLE_COUNT) {
            errorPrint(
                "failed to read json, supertable size overflow, max supertable "
                "is %d\n",
                MAX_SUPER_TABLE_COUNT);
            goto PARSE_OVER;
        }
        g_Dbs.db[i].superTbls = calloc(1, stbSize * sizeof(SSuperTable));
        assert(g_Dbs.db[i].superTbls);
        g_Dbs.db[i].superTblCount = stbSize;
        for (int j = 0; j < stbSize; ++j) {
            cJSON *stbInfo = cJSON_GetArrayItem(stables, j);
            if (stbInfo == NULL) continue;

            // dbinfo
            cJSON *stbName = cJSON_GetObjectItem(stbInfo, "name");
            if (!stbName || stbName->type != cJSON_String ||
                stbName->valuestring == NULL) {
                errorPrint("%s", "failed to read json, stb name not found\n");
                goto PARSE_OVER;
            }
            tstrncpy(g_Dbs.db[i].superTbls[j].stbName, stbName->valuestring,
                     TSDB_TABLE_NAME_LEN);

            cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
            if (!prefix || prefix->type != cJSON_String ||
                prefix->valuestring == NULL) {
                errorPrint(
                    "%s", "failed to read json, childtable_prefix not found\n");
                goto PARSE_OVER;
            }
            tstrncpy(g_Dbs.db[i].superTbls[j].childTblPrefix,
                     prefix->valuestring, TBNAME_PREFIX_LEN);

            cJSON *escapeChar =
                cJSON_GetObjectItem(stbInfo, "escape_character");
            if (escapeChar && escapeChar->type == cJSON_String &&
                escapeChar->valuestring != NULL) {
                if ((0 == strncasecmp(escapeChar->valuestring, "yes", 3))) {
                    g_Dbs.db[i].superTbls[j].escapeChar = true;
                } else if (0 == strncasecmp(escapeChar->valuestring, "no", 2)) {
                    g_Dbs.db[i].superTbls[j].escapeChar = false;
                } else {
                    g_Dbs.db[i].superTbls[j].escapeChar = false;
                }
            } else if (!escapeChar) {
                g_Dbs.db[i].superTbls[j].escapeChar = false;
            } else {
                errorPrint("%s",
                           "failed to read json, escape_character not found\n");
                goto PARSE_OVER;
            }

            cJSON *autoCreateTbl =
                cJSON_GetObjectItem(stbInfo, "auto_create_table");
            if (autoCreateTbl && autoCreateTbl->type == cJSON_String &&
                autoCreateTbl->valuestring != NULL) {
                if ((0 == strncasecmp(autoCreateTbl->valuestring, "yes", 3)) &&
                    (TBL_ALREADY_EXISTS !=
                     g_Dbs.db[i].superTbls[j].childTblExists)) {
                    g_Dbs.db[i].superTbls[j].autoCreateTable =
                        AUTO_CREATE_SUBTBL;
                } else if (0 ==
                           strncasecmp(autoCreateTbl->valuestring, "no", 2)) {
                    g_Dbs.db[i].superTbls[j].autoCreateTable =
                        PRE_CREATE_SUBTBL;
                } else {
                    g_Dbs.db[i].superTbls[j].autoCreateTable =
                        PRE_CREATE_SUBTBL;
                }
            } else if (!autoCreateTbl) {
                g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
            } else {
                errorPrint(
                    "%s", "failed to read json, auto_create_table not found\n");
                goto PARSE_OVER;
            }

            cJSON *batchCreateTbl =
                cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
            if (batchCreateTbl && batchCreateTbl->type == cJSON_Number) {
                g_Dbs.db[i].superTbls[j].batchCreateTableNum =
                    batchCreateTbl->valueint;
            } else if (!batchCreateTbl) {
                g_Dbs.db[i].superTbls[j].batchCreateTableNum =
                    DEFAULT_CREATE_BATCH;
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, batch_create_tbl_num not found\n");
                goto PARSE_OVER;
            }

            cJSON *childTblExists =
                cJSON_GetObjectItem(stbInfo, "child_table_exists");  // yes, no
            if (childTblExists && childTblExists->type == cJSON_String &&
                childTblExists->valuestring != NULL) {
                if ((0 == strncasecmp(childTblExists->valuestring, "yes", 3)) &&
                    (g_Dbs.db[i].drop == false)) {
                    g_Dbs.db[i].superTbls[j].childTblExists =
                        TBL_ALREADY_EXISTS;
                } else if ((0 == strncasecmp(childTblExists->valuestring, "no",
                                             2) ||
                            (g_Dbs.db[i].drop == true))) {
                    g_Dbs.db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
                } else {
                    g_Dbs.db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
                }
            } else if (!childTblExists) {
                g_Dbs.db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, child_table_exists not found\n");
                goto PARSE_OVER;
            }

            if (TBL_ALREADY_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists) {
                g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
            }

            cJSON *count = cJSON_GetObjectItem(stbInfo, "childtable_count");
            if (!count || count->type != cJSON_Number || 0 >= count->valueint) {
                errorPrint(
                    "%s",
                    "failed to read json, childtable_count input mistake\n");
                goto PARSE_OVER;
            }
            g_Dbs.db[i].superTbls[j].childTblCount = count->valueint;
            g_totalChildTables += g_Dbs.db[i].superTbls[j].childTblCount;

            cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
            if (dataSource && dataSource->type == cJSON_String &&
                dataSource->valuestring != NULL) {
                tstrncpy(
                    g_Dbs.db[i].superTbls[j].dataSource,
                    dataSource->valuestring,
                    min(SMALL_BUFF_LEN, strlen(dataSource->valuestring) + 1));
            } else if (!dataSource) {
                tstrncpy(g_Dbs.db[i].superTbls[j].dataSource, "rand",
                         min(SMALL_BUFF_LEN, strlen("rand") + 1));
            } else {
                errorPrint("%s",
                           "failed to read json, data_source not found\n");
                goto PARSE_OVER;
            }

            cJSON *stbIface = cJSON_GetObjectItem(
                stbInfo, "insert_mode");  // taosc , rest, stmt
            if (stbIface && stbIface->type == cJSON_String &&
                stbIface->valuestring != NULL) {
                if (0 == strcasecmp(stbIface->valuestring, "taosc")) {
                    g_Dbs.db[i].superTbls[j].iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "rest")) {
                    g_Dbs.db[i].superTbls[j].iface = REST_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "stmt")) {
                    g_Dbs.db[i].superTbls[j].iface = STMT_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "sml")) {
                    g_Dbs.db[i].superTbls[j].iface = SML_IFACE;
                    g_args.iface = SML_IFACE;
                } else {
                    errorPrint(
                        "failed to read json, insert_mode %s not recognized\n",
                        stbIface->valuestring);
                    goto PARSE_OVER;
                }
            } else if (!stbIface) {
                g_Dbs.db[i].superTbls[j].iface = TAOSC_IFACE;
            } else {
                errorPrint("%s",
                           "failed to read json, insert_mode not found\n");
                goto PARSE_OVER;
            }

            cJSON *stbLineProtocol =
                cJSON_GetObjectItem(stbInfo, "line_protocol");
            if (stbLineProtocol && stbLineProtocol->type == cJSON_String &&
                stbLineProtocol->valuestring != NULL) {
                if (0 == strcasecmp(stbLineProtocol->valuestring, "line")) {
                    g_Dbs.db[i].superTbls[j].lineProtocol =
                        TSDB_SML_LINE_PROTOCOL;
                } else if (0 ==
                           strcasecmp(stbLineProtocol->valuestring, "telnet")) {
                    g_Dbs.db[i].superTbls[j].lineProtocol =
                        TSDB_SML_TELNET_PROTOCOL;
                } else if (0 ==
                           strcasecmp(stbLineProtocol->valuestring, "json")) {
                    g_Dbs.db[i].superTbls[j].lineProtocol =
                        TSDB_SML_JSON_PROTOCOL;
                } else {
                    errorPrint(
                        "failed to read json, line_protocol %s not "
                        "recognized\n",
                        stbLineProtocol->valuestring);
                    goto PARSE_OVER;
                }
            } else if (!stbLineProtocol) {
                g_Dbs.db[i].superTbls[j].lineProtocol = TSDB_SML_LINE_PROTOCOL;
            } else {
                errorPrint("%s",
                           "failed to read json, line_protocol not found\n");
                goto PARSE_OVER;
            }

            cJSON *childTbl_limit =
                cJSON_GetObjectItem(stbInfo, "childtable_limit");
            if ((childTbl_limit) && (g_Dbs.db[i].drop != true) &&
                (g_Dbs.db[i].superTbls[j].childTblExists ==
                 TBL_ALREADY_EXISTS)) {
                if (childTbl_limit->type != cJSON_Number) {
                    errorPrint("%s", "failed to read json, childtable_limit\n");
                    goto PARSE_OVER;
                }
                g_Dbs.db[i].superTbls[j].childTblLimit =
                    childTbl_limit->valueint;
            } else {
                g_Dbs.db[i].superTbls[j].childTblLimit =
                    -1;  // select ... limit -1 means all query result, drop =
                         // yes mean all table need recreate, limit value is
                         // invalid.
            }

            cJSON *childTbl_offset =
                cJSON_GetObjectItem(stbInfo, "childtable_offset");
            if ((childTbl_offset) && (g_Dbs.db[i].drop != true) &&
                (g_Dbs.db[i].superTbls[j].childTblExists ==
                 TBL_ALREADY_EXISTS)) {
                if ((childTbl_offset->type != cJSON_Number) ||
                    (0 > childTbl_offset->valueint)) {
                    errorPrint("%s",
                               "failed to read json, childtable_offset\n");
                    goto PARSE_OVER;
                }
                g_Dbs.db[i].superTbls[j].childTblOffset =
                    childTbl_offset->valueint;
            } else {
                g_Dbs.db[i].superTbls[j].childTblOffset = 0;
            }

            cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
            if (ts && ts->type == cJSON_String && ts->valuestring != NULL) {
                tstrncpy(g_Dbs.db[i].superTbls[j].startTimestamp,
                         ts->valuestring, TSDB_DB_NAME_LEN);
            } else if (!ts) {
                tstrncpy(g_Dbs.db[i].superTbls[j].startTimestamp, "now",
                         TSDB_DB_NAME_LEN);
            } else {
                errorPrint("%s",
                           "failed to read json, start_timestamp not found\n");
                goto PARSE_OVER;
            }

            cJSON *timestampStep =
                cJSON_GetObjectItem(stbInfo, "timestamp_step");
            if (timestampStep && timestampStep->type == cJSON_Number) {
                g_Dbs.db[i].superTbls[j].timeStampStep =
                    timestampStep->valueint;
            } else if (!timestampStep) {
                g_Dbs.db[i].superTbls[j].timeStampStep = g_args.timestamp_step;
            } else {
                errorPrint("%s",
                           "failed to read json, timestamp_step not found\n");
                goto PARSE_OVER;
            }

            cJSON *sampleFormat = cJSON_GetObjectItem(stbInfo, "sample_format");
            if (sampleFormat && sampleFormat->type == cJSON_String &&
                sampleFormat->valuestring != NULL) {
                tstrncpy(
                    g_Dbs.db[i].superTbls[j].sampleFormat,
                    sampleFormat->valuestring,
                    min(SMALL_BUFF_LEN, strlen(sampleFormat->valuestring) + 1));
            } else if (!sampleFormat) {
                tstrncpy(g_Dbs.db[i].superTbls[j].sampleFormat, "csv",
                         SMALL_BUFF_LEN);
            } else {
                errorPrint("%s",
                           "failed to read json, sample_format not found\n");
                goto PARSE_OVER;
            }

            cJSON *sampleFile = cJSON_GetObjectItem(stbInfo, "sample_file");
            if (sampleFile && sampleFile->type == cJSON_String &&
                sampleFile->valuestring != NULL) {
                tstrncpy(g_Dbs.db[i].superTbls[j].sampleFile,
                         sampleFile->valuestring,
                         min(MAX_FILE_NAME_LEN,
                             strlen(sampleFile->valuestring) + 1));
            } else if (!sampleFile) {
                memset(g_Dbs.db[i].superTbls[j].sampleFile, 0,
                       MAX_FILE_NAME_LEN);
            } else {
                errorPrint("%s",
                           "failed to read json, sample_file not found\n");
                goto PARSE_OVER;
            }

            cJSON *useSampleTs = cJSON_GetObjectItem(stbInfo, "use_sample_ts");
            if (useSampleTs && useSampleTs->type == cJSON_String &&
                useSampleTs->valuestring != NULL) {
                if (0 == strncasecmp(useSampleTs->valuestring, "yes", 3)) {
                    g_Dbs.db[i].superTbls[j].useSampleTs = true;
                } else if (0 ==
                           strncasecmp(useSampleTs->valuestring, "no", 2)) {
                    g_Dbs.db[i].superTbls[j].useSampleTs = false;
                } else {
                    g_Dbs.db[i].superTbls[j].useSampleTs = false;
                }
            } else if (!useSampleTs) {
                g_Dbs.db[i].superTbls[j].useSampleTs = false;
            } else {
                errorPrint("%s",
                           "failed to read json, use_sample_ts not found\n");
                goto PARSE_OVER;
            }

            cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
            if ((tagsFile && tagsFile->type == cJSON_String) &&
                (tagsFile->valuestring != NULL)) {
                tstrncpy(g_Dbs.db[i].superTbls[j].tagsFile,
                         tagsFile->valuestring, MAX_FILE_NAME_LEN);
                if (0 == g_Dbs.db[i].superTbls[j].tagsFile[0]) {
                    g_Dbs.db[i].superTbls[j].tagSource = 0;
                } else {
                    g_Dbs.db[i].superTbls[j].tagSource = 1;
                }
            } else if (!tagsFile) {
                memset(g_Dbs.db[i].superTbls[j].tagsFile, 0, MAX_FILE_NAME_LEN);
                g_Dbs.db[i].superTbls[j].tagSource = 0;
            } else {
                errorPrint("%s", "failed to read json, tags_file not found\n");
                goto PARSE_OVER;
            }

            cJSON *stbMaxSqlLen = cJSON_GetObjectItem(stbInfo, "max_sql_len");
            if (stbMaxSqlLen && stbMaxSqlLen->type == cJSON_Number) {
                int32_t len = (int32_t)stbMaxSqlLen->valueint;
                if (len > TSDB_MAX_ALLOWED_SQL_LEN) {
                    len = TSDB_MAX_ALLOWED_SQL_LEN;
                } else if (len < 5) {
                    len = 5;
                }
                g_Dbs.db[i].superTbls[j].maxSqlLen = len;
            } else if (!maxSqlLen) {
                g_Dbs.db[i].superTbls[j].maxSqlLen = g_args.max_sql_len;
            } else {
                errorPrint("%s",
                           "failed to read json, stbMaxSqlLen input mistake\n");
                goto PARSE_OVER;
            }
            /*
               cJSON *multiThreadWriteOneTbl =
               cJSON_GetObjectItem(stbInfo, "multi_thread_write_one_tbl"); // no
               , yes if (multiThreadWriteOneTbl
               && multiThreadWriteOneTbl->type == cJSON_String
               && multiThreadWriteOneTbl->valuestring != NULL) {
               if (0 == strncasecmp(multiThreadWriteOneTbl->valuestring, "yes",
               3)) { g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl = 1; } else
               { g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl = 0;
               }
               } else if (!multiThreadWriteOneTbl) {
               g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl = 0;
               } else {
               errorPrint("%s", "failed to read json, multiThreadWriteOneTbl not
               found\n"); goto PARSE_OVER;
               }
               */
            cJSON *insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
            if (insertRows && insertRows->type == cJSON_Number) {
                if (insertRows->valueint < 0) {
                    errorPrint(
                        "%s",
                        "failed to read json, insert_rows input mistake\n");
                    goto PARSE_OVER;
                }
                g_Dbs.db[i].superTbls[j].insertRows = insertRows->valueint;
            } else if (!insertRows) {
                g_Dbs.db[i].superTbls[j].insertRows = 0x7FFFFFFFFFFFFFFF;
            } else {
                errorPrint("%s",
                           "failed to read json, insert_rows input mistake\n");
                goto PARSE_OVER;
            }

            cJSON *stbInterlaceRows =
                cJSON_GetObjectItem(stbInfo, "interlace_rows");
            if (stbInterlaceRows && stbInterlaceRows->type == cJSON_Number) {
                if (stbInterlaceRows->valueint < 0) {
                    errorPrint(
                        "%s",
                        "failed to read json, interlace rows input mistake\n");
                    goto PARSE_OVER;
                }
                g_Dbs.db[i].superTbls[j].interlaceRows =
                    (uint32_t)stbInterlaceRows->valueint;

                if (g_Dbs.db[i].superTbls[j].interlaceRows >
                    g_Dbs.db[i].superTbls[j].insertRows) {
                    printf(
                        "NOTICE: db[%d].superTbl[%d]'s interlace rows value %u "
                        "> insert_rows %" PRId64 "\n\n",
                        i, j, g_Dbs.db[i].superTbls[j].interlaceRows,
                        g_Dbs.db[i].superTbls[j].insertRows);
                    printf(
                        "        interlace rows value will be set to "
                        "insert_rows %" PRId64 "\n\n",
                        g_Dbs.db[i].superTbls[j].insertRows);
                    prompt();
                    g_Dbs.db[i].superTbls[j].interlaceRows =
                        (uint32_t)g_Dbs.db[i].superTbls[j].insertRows;
                }
            } else if (!stbInterlaceRows) {
                g_Dbs.db[i].superTbls[j].interlaceRows =
                    g_args.interlaceRows;  // 0 means progressive mode, > 0 mean
                                           // interlace mode. max value is less
                                           // or equ num_of_records_per_req
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, interlace rows input mistake\n");
                goto PARSE_OVER;
            }

            cJSON *disorderRatio =
                cJSON_GetObjectItem(stbInfo, "disorder_ratio");
            if (disorderRatio && disorderRatio->type == cJSON_Number) {
                if (disorderRatio->valueint > 50) disorderRatio->valueint = 50;

                if (disorderRatio->valueint < 0) disorderRatio->valueint = 0;

                g_Dbs.db[i].superTbls[j].disorderRatio =
                    (int)disorderRatio->valueint;
            } else if (!disorderRatio) {
                g_Dbs.db[i].superTbls[j].disorderRatio = 0;
            } else {
                errorPrint("%s",
                           "failed to read json, disorderRatio not found\n");
                goto PARSE_OVER;
            }

            cJSON *disorderRange =
                cJSON_GetObjectItem(stbInfo, "disorder_range");
            if (disorderRange && disorderRange->type == cJSON_Number) {
                g_Dbs.db[i].superTbls[j].disorderRange =
                    (int)disorderRange->valueint;
            } else if (!disorderRange) {
                g_Dbs.db[i].superTbls[j].disorderRange = DEFAULT_DISORDER_RANGE;
            } else {
                errorPrint("%s",
                           "failed to read json, disorderRange not found\n");
                goto PARSE_OVER;
            }

            cJSON *insertInterval =
                cJSON_GetObjectItem(stbInfo, "insert_interval");
            if (insertInterval && insertInterval->type == cJSON_Number) {
                g_Dbs.db[i].superTbls[j].insertInterval =
                    insertInterval->valueint;
                if (insertInterval->valueint < 0) {
                    errorPrint(
                        "%s",
                        "failed to read json, insert_interval input mistake\n");
                    goto PARSE_OVER;
                }
            } else if (!insertInterval) {
                verbosePrint(
                    "%s() LN%d: stable insert interval be overrode by global "
                    "%" PRIu64 ".\n",
                    __func__, __LINE__, g_args.insert_interval);
                g_Dbs.db[i].superTbls[j].insertInterval =
                    g_args.insert_interval;
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, insert_interval input mistake\n");
                goto PARSE_OVER;
            }

            if (getColumnAndTagTypeFromInsertJsonFile(
                    stbInfo, &g_Dbs.db[i].superTbls[j])) {
                goto PARSE_OVER;
            }
        }
    }

    code = 0;

PARSE_OVER:
    return code;
}
int getMetaFromQueryJsonFile(cJSON *root) {
    int32_t code = -1;

    cJSON *cfgdir = cJSON_GetObjectItem(root, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(g_queryInfo.cfgDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON *host = cJSON_GetObjectItem(root, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        tstrncpy(g_queryInfo.host, host->valuestring, MAX_HOSTNAME_SIZE);
    } else if (!host) {
        tstrncpy(g_queryInfo.host, DEFAULT_HOST, MAX_HOSTNAME_SIZE);
    } else {
        errorPrint("%s", "failed to read json, host not found\n");
        goto PARSE_OVER;
    }

    cJSON *port = cJSON_GetObjectItem(root, "port");
    if (port && port->type == cJSON_Number) {
        g_queryInfo.port = (uint16_t)port->valueint;
    } else if (!port) {
        g_queryInfo.port = DEFAULT_PORT;
    }

    cJSON *user = cJSON_GetObjectItem(root, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        tstrncpy(g_queryInfo.user, user->valuestring, MAX_USERNAME_SIZE);
    } else if (!user) {
        tstrncpy(g_queryInfo.user, TSDB_DEFAULT_USER, MAX_USERNAME_SIZE);
        ;
    }

    cJSON *password = cJSON_GetObjectItem(root, "password");
    if (password && password->type == cJSON_String &&
        password->valuestring != NULL) {
        tstrncpy(g_queryInfo.password, password->valuestring,
                 SHELL_MAX_PASSWORD_LEN);
    } else if (!password) {
        tstrncpy(g_queryInfo.password, TSDB_DEFAULT_PASS,
                 SHELL_MAX_PASSWORD_LEN);
        ;
    }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(root, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strncasecmp(answerPrompt->valuestring, "yes", 3)) {
            g_args.answer_yes = false;
        } else if (0 == strncasecmp(answerPrompt->valuestring, "no", 2)) {
            g_args.answer_yes = true;
        } else {
            g_args.answer_yes = false;
        }
    } else if (!answerPrompt) {
        g_args.answer_yes = false;
    } else {
        errorPrint("%s",
                   "failed to read json, confirm_parameter_prompt not found\n");
        goto PARSE_OVER;
    }

    cJSON *gQueryTimes = cJSON_GetObjectItem(root, "query_times");
    if (gQueryTimes && gQueryTimes->type == cJSON_Number) {
        if (gQueryTimes->valueint <= 0) {
            errorPrint("%s",
                       "failed to read json, query_times input mistake\n");
            goto PARSE_OVER;
        }
        g_args.query_times = gQueryTimes->valueint;
    } else if (!gQueryTimes) {
        g_args.query_times = DEFAULT_QUERY_TIME;
    } else {
        errorPrint("%s", "failed to read json, query_times input mistake\n");
        goto PARSE_OVER;
    }

    cJSON *dbs = cJSON_GetObjectItem(root, "databases");
    if (dbs && dbs->type == cJSON_String && dbs->valuestring != NULL) {
        tstrncpy(g_queryInfo.dbName, dbs->valuestring, TSDB_DB_NAME_LEN);
    } else if (!dbs) {
        errorPrint("%s", "failed to read json, databases not found\n");
        goto PARSE_OVER;
    }

    cJSON *queryMode = cJSON_GetObjectItem(root, "query_mode");
    if (queryMode && queryMode->type == cJSON_String &&
        queryMode->valuestring != NULL) {
        tstrncpy(g_queryInfo.queryMode, queryMode->valuestring,
                 min(SMALL_BUFF_LEN, strlen(queryMode->valuestring) + 1));
    } else if (!queryMode) {
        tstrncpy(g_queryInfo.queryMode, "taosc",
                 min(SMALL_BUFF_LEN, strlen("taosc") + 1));
    } else {
        errorPrint("%s", "failed to read json, query_mode not found\n");
        goto PARSE_OVER;
    }

    // specified_table_query
    cJSON *specifiedQuery = cJSON_GetObjectItem(root, "specified_table_query");
    if (!specifiedQuery) {
        g_queryInfo.specifiedQueryInfo.concurrent = 1;
        g_queryInfo.specifiedQueryInfo.sqlCount = 0;
    } else if (specifiedQuery->type != cJSON_Object) {
        errorPrint("%s", "failed to read json, super_table_query not found\n");
        goto PARSE_OVER;
    } else {
        cJSON *queryInterval =
            cJSON_GetObjectItem(specifiedQuery, "query_interval");
        if (queryInterval && queryInterval->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.queryInterval =
                queryInterval->valueint;
        } else if (!queryInterval) {
            g_queryInfo.specifiedQueryInfo.queryInterval = 0;
        }

        cJSON *specifiedQueryTimes =
            cJSON_GetObjectItem(specifiedQuery, "query_times");
        if (specifiedQueryTimes && specifiedQueryTimes->type == cJSON_Number) {
            if (specifiedQueryTimes->valueint <= 0) {
                errorPrint("failed to read json, query_times: %" PRId64
                           ", need be a valid (>0) number\n",
                           specifiedQueryTimes->valueint);
                goto PARSE_OVER;
            }
            g_queryInfo.specifiedQueryInfo.queryTimes =
                specifiedQueryTimes->valueint;
        } else if (!specifiedQueryTimes) {
            g_queryInfo.specifiedQueryInfo.queryTimes = g_args.query_times;
        } else {
            errorPrint(
                "%s() LN%d, failed to read json, query_times input mistake\n",
                __func__, __LINE__);
            goto PARSE_OVER;
        }

        cJSON *concurrent = cJSON_GetObjectItem(specifiedQuery, "concurrent");
        if (concurrent && concurrent->type == cJSON_Number) {
            if (concurrent->valueint <= 0) {
                errorPrint(
                    "query sqlCount %d or concurrent %d is not correct.\n",
                    g_queryInfo.specifiedQueryInfo.sqlCount,
                    g_queryInfo.specifiedQueryInfo.concurrent);
                goto PARSE_OVER;
            }
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)concurrent->valueint;
        } else if (!concurrent) {
            g_queryInfo.specifiedQueryInfo.concurrent = 1;
        }

        cJSON *specifiedAsyncMode = cJSON_GetObjectItem(specifiedQuery, "mode");
        if (specifiedAsyncMode && specifiedAsyncMode->type == cJSON_String &&
            specifiedAsyncMode->valuestring != NULL) {
            if (0 == strcmp("sync", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
            } else if (0 == strcmp("async", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                errorPrint("%s",
                           "failed to read json, async mode input error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
        }

        cJSON *interval = cJSON_GetObjectItem(specifiedQuery, "interval");
        if (interval && interval->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                interval->valueint;
        } else if (!interval) {
            // printf("failed to read json, subscribe interval no found\n");
            // goto PARSE_OVER;
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                DEFAULT_SUB_INTERVAL;
        }

        cJSON *restart = cJSON_GetObjectItem(specifiedQuery, "restart");
        if (restart && restart->type == cJSON_String &&
            restart->valuestring != NULL) {
            if (0 == strcmp("yes", restart->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
            } else if (0 == strcmp("no", restart->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = false;
            } else {
                errorPrint("%s",
                           "failed to read json, subscribe restart error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
        }

        cJSON *keepProgress =
            cJSON_GetObjectItem(specifiedQuery, "keepProgress");
        if (keepProgress && keepProgress->type == cJSON_String &&
            keepProgress->valuestring != NULL) {
            if (0 == strcmp("yes", keepProgress->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 1;
            } else if (0 == strcmp("no", keepProgress->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, subscribe keepProgress error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
        }

        // sqls
        cJSON *specifiedSqls = cJSON_GetObjectItem(specifiedQuery, "sqls");
        if (!specifiedSqls) {
            g_queryInfo.specifiedQueryInfo.sqlCount = 0;
        } else if (specifiedSqls->type != cJSON_Array) {
            errorPrint("%s", "failed to read json, super sqls not found\n");
            goto PARSE_OVER;
        } else {
            int superSqlSize = cJSON_GetArraySize(specifiedSqls);
            if (superSqlSize * g_queryInfo.specifiedQueryInfo.concurrent >
                MAX_QUERY_SQL_COUNT) {
                errorPrint(
                    "failed to read json, query sql(%d) * concurrent(%d) "
                    "overflow, max is %d\n",
                    superSqlSize, g_queryInfo.specifiedQueryInfo.concurrent,
                    MAX_QUERY_SQL_COUNT);
                goto PARSE_OVER;
            }

            g_queryInfo.specifiedQueryInfo.sqlCount = superSqlSize;
            for (int j = 0; j < superSqlSize; ++j) {
                cJSON *sql = cJSON_GetArrayItem(specifiedSqls, j);
                if (sql == NULL) continue;

                cJSON *sqlStr = cJSON_GetObjectItem(sql, "sql");
                if (!sqlStr || sqlStr->type != cJSON_String ||
                    sqlStr->valuestring == NULL) {
                    errorPrint("%s", "failed to read json, sql not found\n");
                    goto PARSE_OVER;
                }
                tstrncpy(g_queryInfo.specifiedQueryInfo.sql[j],
                         sqlStr->valuestring, BUFFER_SIZE);

                // default value is -1, which mean infinite loop
                g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;
                cJSON *endAfterConsume =
                    cJSON_GetObjectItem(specifiedQuery, "endAfterConsume");
                if (endAfterConsume && endAfterConsume->type == cJSON_Number) {
                    g_queryInfo.specifiedQueryInfo.endAfterConsume[j] =
                        (int)endAfterConsume->valueint;
                }
                if (g_queryInfo.specifiedQueryInfo.endAfterConsume[j] < -1)
                    g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;

                g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;
                cJSON *resubAfterConsume =
                    cJSON_GetObjectItem(specifiedQuery, "resubAfterConsume");
                if ((resubAfterConsume) &&
                    (resubAfterConsume->type == cJSON_Number) &&
                    (resubAfterConsume->valueint >= 0)) {
                    g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] =
                        (int)resubAfterConsume->valueint;
                }

                if (g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] < -1)
                    g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;

                cJSON *result = cJSON_GetObjectItem(sql, "result");
                if ((NULL != result) && (result->type == cJSON_String) &&
                    (result->valuestring != NULL)) {
                    tstrncpy(g_queryInfo.specifiedQueryInfo.result[j],
                             result->valuestring, MAX_FILE_NAME_LEN);
                } else if (NULL == result) {
                    memset(g_queryInfo.specifiedQueryInfo.result[j], 0,
                           MAX_FILE_NAME_LEN);
                } else {
                    errorPrint("%s",
                               "failed to read json, super query result file "
                               "not found\n");
                    goto PARSE_OVER;
                }
            }
        }
    }

    // super_table_query
    cJSON *superQuery = cJSON_GetObjectItem(root, "super_table_query");
    if (!superQuery) {
        g_queryInfo.superQueryInfo.threadCnt = 1;
        g_queryInfo.superQueryInfo.sqlCount = 0;
    } else if (superQuery->type != cJSON_Object) {
        errorPrint("%s", "failed to read json, sub_table_query not found\n");
        code = 0;
        goto PARSE_OVER;
    } else {
        cJSON *subrate = cJSON_GetObjectItem(superQuery, "query_interval");
        if (subrate && subrate->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.queryInterval = subrate->valueint;
        } else if (!subrate) {
            g_queryInfo.superQueryInfo.queryInterval = 0;
        }

        cJSON *superQueryTimes = cJSON_GetObjectItem(superQuery, "query_times");
        if (superQueryTimes && superQueryTimes->type == cJSON_Number) {
            if (superQueryTimes->valueint <= 0) {
                errorPrint("failed to read json, query_times: %" PRId64
                           ", need be a valid (>0) number\n",
                           superQueryTimes->valueint);
                goto PARSE_OVER;
            }
            g_queryInfo.superQueryInfo.queryTimes = superQueryTimes->valueint;
        } else if (!superQueryTimes) {
            g_queryInfo.superQueryInfo.queryTimes = g_args.query_times;
        } else {
            errorPrint("%s",
                       "failed to read json, query_times input mistake\n");
            goto PARSE_OVER;
        }

        cJSON *threads = cJSON_GetObjectItem(superQuery, "threads");
        if (threads && threads->type == cJSON_Number) {
            if (threads->valueint <= 0) {
                errorPrint("%s",
                           "failed to read json, threads input mistake\n");
                goto PARSE_OVER;
            }
            g_queryInfo.superQueryInfo.threadCnt = (uint32_t)threads->valueint;
        } else if (!threads) {
            g_queryInfo.superQueryInfo.threadCnt = DEFAULT_NTHREADS;
        }

        // cJSON* subTblCnt = cJSON_GetObjectItem(superQuery,
        // "childtable_count"); if (subTblCnt && subTblCnt->type ==
        // cJSON_Number)
        // {
        //  g_queryInfo.superQueryInfo.childTblCount = subTblCnt->valueint;
        //} else if (!subTblCnt) {
        //  g_queryInfo.superQueryInfo.childTblCount = 0;
        //}

        cJSON *stblname = cJSON_GetObjectItem(superQuery, "stblname");
        if (stblname && stblname->type == cJSON_String &&
            stblname->valuestring != NULL) {
            tstrncpy(g_queryInfo.superQueryInfo.stbName, stblname->valuestring,
                     TSDB_TABLE_NAME_LEN);
        } else {
            errorPrint("%s",
                       "failed to read json, super table name input error\n");
            goto PARSE_OVER;
        }

        cJSON *superAsyncMode = cJSON_GetObjectItem(superQuery, "mode");
        if (superAsyncMode && superAsyncMode->type == cJSON_String &&
            superAsyncMode->valuestring != NULL) {
            if (0 == strcmp("sync", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
            } else if (0 == strcmp("async", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                errorPrint("%s",
                           "failed to read json, async mode input error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
        }

        cJSON *superInterval = cJSON_GetObjectItem(superQuery, "interval");
        if (superInterval && superInterval->type == cJSON_Number) {
            if (superInterval->valueint < 0) {
                errorPrint("%s",
                           "failed to read json, interval input mistake\n");
                goto PARSE_OVER;
            }
            g_queryInfo.superQueryInfo.subscribeInterval =
                superInterval->valueint;
        } else if (!superInterval) {
            // printf("failed to read json, subscribe interval no found\n");
            // goto PARSE_OVER;
            g_queryInfo.superQueryInfo.subscribeInterval =
                DEFAULT_QUERY_INTERVAL;
        }

        cJSON *subrestart = cJSON_GetObjectItem(superQuery, "restart");
        if (subrestart && subrestart->type == cJSON_String &&
            subrestart->valuestring != NULL) {
            if (0 == strcmp("yes", subrestart->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeRestart = true;
            } else if (0 == strcmp("no", subrestart->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeRestart = false;
            } else {
                errorPrint("%s",
                           "failed to read json, subscribe restart error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.superQueryInfo.subscribeRestart = true;
        }

        cJSON *superkeepProgress =
            cJSON_GetObjectItem(superQuery, "keepProgress");
        if (superkeepProgress && superkeepProgress->type == cJSON_String &&
            superkeepProgress->valuestring != NULL) {
            if (0 == strcmp("yes", superkeepProgress->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeKeepProgress = 1;
            } else if (0 == strcmp("no", superkeepProgress->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
            } else {
                errorPrint("%s",
                           "failed to read json, subscribe super table "
                           "keepProgress error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
        }

        // default value is -1, which mean do not resub
        g_queryInfo.superQueryInfo.endAfterConsume = -1;
        cJSON *superEndAfterConsume =
            cJSON_GetObjectItem(superQuery, "endAfterConsume");
        if (superEndAfterConsume &&
            superEndAfterConsume->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.endAfterConsume =
                (int)superEndAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.endAfterConsume < -1)
            g_queryInfo.superQueryInfo.endAfterConsume = -1;

        // default value is -1, which mean do not resub
        g_queryInfo.superQueryInfo.resubAfterConsume = -1;
        cJSON *superResubAfterConsume =
            cJSON_GetObjectItem(superQuery, "resubAfterConsume");
        if ((superResubAfterConsume) &&
            (superResubAfterConsume->type == cJSON_Number) &&
            (superResubAfterConsume->valueint >= 0)) {
            g_queryInfo.superQueryInfo.resubAfterConsume =
                (int)superResubAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.resubAfterConsume < -1)
            g_queryInfo.superQueryInfo.resubAfterConsume = -1;

        // supert table sqls
        cJSON *superSqls = cJSON_GetObjectItem(superQuery, "sqls");
        if (!superSqls) {
            g_queryInfo.superQueryInfo.sqlCount = 0;
        } else if (superSqls->type != cJSON_Array) {
            errorPrint("%s", "failed to read json, super sqls not found\n");
            goto PARSE_OVER;
        } else {
            int superSqlSize = cJSON_GetArraySize(superSqls);
            if (superSqlSize > MAX_QUERY_SQL_COUNT) {
                errorPrint(
                    "failed to read json, query sql size overflow, max is %d\n",
                    MAX_QUERY_SQL_COUNT);
                goto PARSE_OVER;
            }

            g_queryInfo.superQueryInfo.sqlCount = superSqlSize;
            for (int j = 0; j < superSqlSize; ++j) {
                cJSON *sql = cJSON_GetArrayItem(superSqls, j);
                if (sql == NULL) continue;

                cJSON *sqlStr = cJSON_GetObjectItem(sql, "sql");
                if (!sqlStr || sqlStr->type != cJSON_String ||
                    sqlStr->valuestring == NULL) {
                    errorPrint("%s", "failed to read json, sql not found\n");
                    goto PARSE_OVER;
                }
                tstrncpy(g_queryInfo.superQueryInfo.sql[j], sqlStr->valuestring,
                         BUFFER_SIZE);

                cJSON *result = cJSON_GetObjectItem(sql, "result");
                if (result != NULL && result->type == cJSON_String &&
                    result->valuestring != NULL) {
                    tstrncpy(g_queryInfo.superQueryInfo.result[j],
                             result->valuestring, MAX_FILE_NAME_LEN);
                } else if (NULL == result) {
                    memset(g_queryInfo.superQueryInfo.result[j], 0,
                           MAX_FILE_NAME_LEN);
                } else {
                    errorPrint("%s",
                               "failed to read json, sub query result file not "
                               "found\n");
                    goto PARSE_OVER;
                }
            }
        }
    }

    code = 0;

PARSE_OVER:
    return code;
}

int getInfoFromJsonFile(char *file) {
    debugPrint("%s %d %s\n", __func__, __LINE__, file);
    int32_t code = -1;
    FILE *  fp = fopen(file, "r");
    if (!fp) {
        errorPrint("failed to read %s, reason:%s\n", file, strerror(errno));
        return code;
    }

    int   maxLen = MAX_JSON_BUFF;
    char *content = calloc(1, maxLen + 1);
    int   len = (int)fread(content, 1, maxLen, fp);
    if (len <= 0) {
        free(content);
        fclose(fp);
        errorPrint("failed to read %s, content is null", file);
        return code;
    }

    content[len] = 0;
    cJSON *root = cJSON_Parse(content);
    if (root == NULL) {
        errorPrint("failed to cjson parse %s, invalid json format\n", file);
        goto PARSE_OVER;
    }

    cJSON *filetype = cJSON_GetObjectItem(root, "filetype");
    if (filetype && filetype->type == cJSON_String &&
        filetype->valuestring != NULL) {
        if (0 == strcasecmp("insert", filetype->valuestring)) {
            g_args.test_mode = INSERT_TEST;
        } else if (0 == strcasecmp("query", filetype->valuestring)) {
            g_args.test_mode = QUERY_TEST;
        } else if (0 == strcasecmp("subscribe", filetype->valuestring)) {
            g_args.test_mode = SUBSCRIBE_TEST;
        } else {
            errorPrint("%s", "failed to read json, filetype not support\n");
            goto PARSE_OVER;
        }
    } else if (!filetype) {
        g_args.test_mode = INSERT_TEST;
    } else {
        errorPrint("%s", "failed to read json, filetype not found\n");
        goto PARSE_OVER;
    }

    if (INSERT_TEST == g_args.test_mode) {
        memset(&g_Dbs, 0, sizeof(SDbs));
        g_Dbs.use_metric = g_args.use_metric;
        code = getMetaFromInsertJsonFile(root);
    } else if ((QUERY_TEST == g_args.test_mode) ||
               (SUBSCRIBE_TEST == g_args.test_mode)) {
        memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));
        code = getMetaFromQueryJsonFile(root);
    } else {
        errorPrint("%s",
                   "input json file type error! please input correct file "
                   "type: insert or query or subscribe\n");
        goto PARSE_OVER;
    }
PARSE_OVER:
    free(content);
    cJSON_Delete(root);
    fclose(fp);
    return code;
}

int testMetaFile() {
    if (INSERT_TEST == g_args.test_mode) {
        if (g_Dbs.cfgDir[0]) {
            taos_options(TSDB_OPTION_CONFIGDIR, g_Dbs.cfgDir);
        }
        return insertTestProcess();

    } else if (QUERY_TEST == g_args.test_mode) {
        if (g_queryInfo.cfgDir[0]) {
            taos_options(TSDB_OPTION_CONFIGDIR, g_queryInfo.cfgDir);
        }
        return queryTestProcess();

    } else if (SUBSCRIBE_TEST == g_args.test_mode) {
        if (g_queryInfo.cfgDir[0]) {
            taos_options(TSDB_OPTION_CONFIGDIR, g_queryInfo.cfgDir);
        }
        return subscribeTestProcess();
    } else {
        errorPrint("unsupport test mode (%d)\n", g_args.test_mode);
        return -1;
    }
    return 0;
}