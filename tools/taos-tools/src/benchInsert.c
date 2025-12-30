/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <bench.h>
#include "benchLog.h"
#include "wrapDb.h"
#include <benchData.h>
#include <benchInsertMix.h>

static int32_t stmt2BindAndSubmit(
        threadInfo *pThreadInfo,
        SChildTable *childTbl,
        int64_t *timestamp, uint64_t i, char *ttl, int32_t *pkCur, int32_t *pkCnt, int64_t *delay1,
        int64_t *delay3, int64_t* startTs, int64_t* endTs, int32_t w);
TAOS_STMT2* initStmt2(TAOS* taos, bool single);

#define FREE_PIDS_INFOS_RETURN_MINUS_1()            \
    do {                                            \
        tmfree(pids);                               \
        tmfree(infos);                              \
        return -1;                                  \
    } while (0)

#define FREE_RESOURCE()                             \
    do {                                            \
        if (pThreadInfo->conn)                      \
            closeBenchConn(pThreadInfo->conn);      \
        benchArrayDestroy(pThreadInfo->delayList);  \
        tmfree(pids);                               \
        tmfree(infos);                              \
    } while (0)                                     \

// REST
static int getSuperTableFromServerRest(
    SDataBase* database, SSuperTable* stbInfo, char *command) {

    // TODO(zero): it will create super table based on this error code.
    return TSDB_CODE_NOT_FOUND;
    // TODO(me): finish full implementation
#if 0
    int sockfd = createSockFd();
    if (sockfd < 0) {
        return -1;
    }

    int code = postProcessSql(command,
                         database->dbName,
                         database->precision,
                         REST_IFACE,
                         0,
                         g_arguments->port,
                         false,
                         sockfd,
                         NULL);

    destroySockFd(sockfd);
#endif   // 0
}

static int getSuperTableFromServerTaosc(
        SDataBase *database, SSuperTable *stbInfo, char *command) {
    TAOS_RES *res;
    TAOS_ROW row = NULL;
    SBenchConn *conn = initBenchConn(database->dbName);
    if (NULL == conn) {
        return TSDB_CODE_FAILED;
    }

    res = taos_query(conn->taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        infoPrint("stable %s does not exist, will create one\n",
                  stbInfo->stbName);
        closeBenchConn(conn);
        return TSDB_CODE_NOT_FOUND;
    }
    infoPrint("find stable<%s>, will get meta data from server\n",
              stbInfo->stbName);

    // Check if the the existing super table matches exactly with the definitions in the JSON file.
    // If a hash table were used, the time complexity would be only O(n).
    // But taosBenchmark does not incorporate a hash table, the time complexity of the loop traversal is O(n^2).
    bool isTitleRow = true;
    uint32_t tag_count = 0;
    uint32_t col_count = 0;

    int fieldsNum = taos_num_fields(res);
    TAOS_FIELD_E* fields = taos_fetch_fields_e(res);

    if (fieldsNum < TSDB_MAX_DESCRIBE_METRIC || !fields) {
        errorPrint("%s", "failed to fetch fields\n");
        taos_free_result(res);
        closeBenchConn(conn);
        return TSDB_CODE_FAILED;
    }

    while ((row = taos_fetch_row(res)) != NULL) {
        if (isTitleRow) {
            isTitleRow = false;
            continue;
        }
        int32_t *lengths = taos_fetch_lengths(res);
        if (lengths == NULL) {
            errorPrint("%s", "failed to execute taos_fetch_length\n");
            taos_free_result(res);
            closeBenchConn(conn);
            return TSDB_CODE_FAILED;
        }
        if (strncasecmp((char *) row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "tag",
                        strlen("tag")) == 0) {
            if (stbInfo->tags == NULL || stbInfo->tags->size == 0 || tag_count >= stbInfo->tags->size) {
                errorPrint("%s", "existing stable tag mismatched with that defined in JSON\n");
                taos_free_result(res);
                closeBenchConn(conn);
                return TSDB_CODE_FAILED;
            }
            uint8_t tagType = convertStringToDatatype(
                    (char *) row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX], &(fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].precision));
            char *tagName = (char *) row[TSDB_DESCRIBE_METRIC_FIELD_INDEX];
            if (!searchBArray(stbInfo->tags, tagName,
                              lengths[TSDB_DESCRIBE_METRIC_FIELD_INDEX], tagType)) {
                errorPrint("existing stable tag:%s not found in JSON tags.\n", tagName);
                taos_free_result(res);
                closeBenchConn(conn);
                return TSDB_CODE_FAILED;
            }
            tag_count += 1;
        } else {
            if (stbInfo->cols == NULL || stbInfo->cols->size == 0 || col_count >= stbInfo->cols->size) {
                errorPrint("%s", "existing stable column mismatched with that defined in JSON\n");
                taos_free_result(res);
                closeBenchConn(conn);
                return TSDB_CODE_FAILED;
            }
            uint8_t colType = convertStringToDatatype(
                    (char *) row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX], &(fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].precision));
            char * colName = (char *) row[TSDB_DESCRIBE_METRIC_FIELD_INDEX];
            if (!searchBArray(stbInfo->cols, colName,
                              lengths[TSDB_DESCRIBE_METRIC_FIELD_INDEX], colType)) {
                errorPrint("existing stable col:%s not found in JSON cols.\n", colName);
                taos_free_result(res);
                closeBenchConn(conn);
                return TSDB_CODE_FAILED;
            }
            col_count += 1;
        }
    }  // end while
    taos_free_result(res);
    closeBenchConn(conn);
    if (tag_count != stbInfo->tags->size) {
        errorPrint("%s", "existing stable tag mismatched with that defined in JSON\n");
        return TSDB_CODE_FAILED;
    }

    if (col_count != stbInfo->cols->size) {
        errorPrint("%s", "existing stable column mismatched with that defined in JSON\n");
        return TSDB_CODE_FAILED;
    }

    return TSDB_CODE_SUCCESS;
}


static int getSuperTableFromServer(SDataBase* database, SSuperTable* stbInfo) {
    int ret = 0;
    char command[SHORT_1K_SQL_BUFF_LEN] = "\0";
    snprintf(command, SHORT_1K_SQL_BUFF_LEN,
             "DESCRIBE `%s`.`%s`", database->dbName,
             stbInfo->stbName);

    if (REST_IFACE == stbInfo->iface) {
        ret = getSuperTableFromServerRest(database, stbInfo, command);
    } else {
        ret = getSuperTableFromServerTaosc(database, stbInfo, command);
    }

    return ret;
}

static int queryDbExec(SDataBase *database,
                       SSuperTable *stbInfo, char *command) {
    int ret = 0;
    if (isRest(stbInfo->iface)) {
        if (0 != convertServAddr(stbInfo->iface, false, 1)) {
            errorPrint("%s", "Failed to convert server address\n");
            return -1;
        }
        int sockfd = createSockFd();
        if (sockfd < 0) {
            ret = -1;
        } else {
            ret = queryDbExecRest(command,
                              database->dbName,
                              database->precision,
                              stbInfo->iface,
                              stbInfo->lineProtocol,
                              stbInfo->tcpTransfer,
                              sockfd);
            destroySockFd(sockfd);
        }
    } else {
        SBenchConn* conn = initBenchConn(database->dbName);
        if (NULL == conn) {
            ret = -1;
        } else {
            ret = queryDbExecCall(conn, command);
            int32_t trying = g_arguments->keep_trying;
            while (ret && trying) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-execute command: %s\n",
                          g_arguments->trying_interval, command);
                toolsMsleep(g_arguments->trying_interval);
                ret = queryDbExecCall(conn, command);
                if (trying != -1) {
                    trying--;
                }
            }
            if (0 != ret) {
                ret = -1;
            }
            closeBenchConn(conn);
        }
    }

    return ret;
}

int getCompressStr(Field* col, char* buf) {
    int pos = 0;
    if(strlen(col->encode) > 0) {
        pos +=sprintf(buf + pos, "encode \'%s\' ", col->encode);
    }
    if(strlen(col->compress) > 0) {
        pos +=sprintf(buf + pos, "compress \'%s\' ", col->compress);
    }
    if(strlen(col->level) > 0) {
        pos +=sprintf(buf + pos, "level \'%s\' ", col->level);
    }

    return pos;
}

static int createSuperTable(SDataBase* database, SSuperTable* stbInfo) {
    if (g_arguments->supplementInsert) {
        return 0;
    }

    uint32_t col_buffer_len = (TSDB_COL_NAME_LEN + 15 + COMP_NAME_LEN*3) * stbInfo->cols->size;
    char         *colsBuf = benchCalloc(1, col_buffer_len, false);
    char*         command = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    int          len = 0;

    for (int colIndex = 0; colIndex < stbInfo->cols->size; colIndex++) {
        Field * col = benchArrayGet(stbInfo->cols, colIndex);
        int n;
        if (col->type == TSDB_DATA_TYPE_BINARY ||
            col->type == TSDB_DATA_TYPE_NCHAR ||
            col->type == TSDB_DATA_TYPE_VARBINARY ||
            col->type == TSDB_DATA_TYPE_GEOMETRY) {
            n = snprintf(colsBuf + len, col_buffer_len - len,
                    ",%s %s(%d)", col->name,
                    convertDatatypeToString(col->type), col->length);
            if (col->type == TSDB_DATA_TYPE_GEOMETRY && col->length < 21) {
                errorPrint("%s() LN%d, geometry filed len must be greater than 21 on %d\n", __func__, __LINE__,
                           colIndex);
                return -1;
            }
        } else if (col->type == TSDB_DATA_TYPE_DECIMAL
                || col->type == TSDB_DATA_TYPE_DECIMAL64) {
            n = snprintf(colsBuf + len, col_buffer_len - len,
                    ",%s %s(%d,%d)", col->name,
                    convertDatatypeToString(col->type), col->precision, col->scale);
        } else {
            n = snprintf(colsBuf + len, col_buffer_len - len,
                    ",%s %s", col->name,
                    convertDatatypeToString(col->type));
        }

        // primary key
        if(stbInfo->primary_key && colIndex == 0) {
            len += n;
            n = snprintf(colsBuf + len, col_buffer_len - len, " %s", PRIMARY_KEY);
        }

        // compress key
        char keys[COMP_NAME_LEN*3] = "";
        if (getCompressStr(col, keys) > 0) {
            len += n;
            n = snprintf(colsBuf + len, col_buffer_len - len, " %s", keys);
        }

        if (n < 0 || n >= col_buffer_len - len) {
            errorPrint("%s() LN%d, snprintf overflow on %d\n",
                       __func__, __LINE__, colIndex);
            break;
        } else {
            len += n;
        }
    }

    // save for creating child table
    stbInfo->colsOfCreateChildTable =
        (char *)benchCalloc(len + TIMESTAMP_BUFF_LEN, 1, true);

    snprintf(stbInfo->colsOfCreateChildTable, len + TIMESTAMP_BUFF_LEN,
             "(%s timestamp%s)", stbInfo->primaryKeyName, colsBuf);

    if (stbInfo->tags->size == 0) {
        free(colsBuf);
        free(command);
        return 0;
    }

    uint32_t tag_buffer_len = (TSDB_COL_NAME_LEN + 15) * stbInfo->tags->size;
    char *tagsBuf = benchCalloc(1, tag_buffer_len, false);
    int  tagIndex;
    len = 0;

    int n;
    n = snprintf(tagsBuf + len, tag_buffer_len - len, "(");
    if (n < 0 || n >= tag_buffer_len - len) {
        errorPrint("%s() LN%d snprintf overflow\n",
                       __func__, __LINE__);
        free(colsBuf);
        free(command);
        tmfree(tagsBuf);
        return -1;
    } else {
        len += n;
    }
    for (tagIndex = 0; tagIndex < stbInfo->tags->size; tagIndex++) {
        Field *tag = benchArrayGet(stbInfo->tags, tagIndex);
        if (tag->type == TSDB_DATA_TYPE_BINARY ||
            tag->type == TSDB_DATA_TYPE_NCHAR ||
            tag->type == TSDB_DATA_TYPE_VARBINARY ||
            tag->type == TSDB_DATA_TYPE_GEOMETRY) {
            n = snprintf(tagsBuf + len, tag_buffer_len - len,
                    "%s %s(%d),", tag->name,
                    convertDatatypeToString(tag->type), tag->length);
            if (tag->type == TSDB_DATA_TYPE_GEOMETRY && tag->length < 21) {
                errorPrint("%s() LN%d, geometry filed len must be greater than 21 on %d\n", __func__, __LINE__,
                           tagIndex);
                return -1;
            }
        } else if (tag->type == TSDB_DATA_TYPE_JSON) {
            n = snprintf(tagsBuf + len, tag_buffer_len - len,
                    "%s json", tag->name);
            if (n < 0 || n >= tag_buffer_len - len) {
                errorPrint("%s() LN%d snprintf overflow on %d\n",
                       __func__, __LINE__, tagIndex);
                break;
            } else {
                len += n;
            }
            goto skip;
        }
        // else if (tag->type == TSDB_DATA_TYPE_DECIMAL
        //         || tag->type == TSDB_DATA_TYPE_DECIMAL64) {
        //     n = snprintf(tagsBuf + len, tag_buffer_len - len,
        //             "%s %s(%d,%d),", tag->name,
        //             convertDatatypeToString(tag->type), tag->precision, tag->scale);
        // }
        else {
            n = snprintf(tagsBuf + len, tag_buffer_len - len,
                    "%s %s,", tag->name,
                    convertDatatypeToString(tag->type));
        }

        if (n < 0 || n >= tag_buffer_len - len) {
            errorPrint("%s() LN%d snprintf overflow on %d\n",
                       __func__, __LINE__, tagIndex);
            break;
        } else {
            len += n;
        }
    }
    len -= 1;
skip:
    snprintf(tagsBuf + len, tag_buffer_len - len, ")");

    int length = snprintf(
        command, TSDB_MAX_ALLOWED_SQL_LEN,
        g_arguments->escape_character
            ? "CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s TIMESTAMP%s) TAGS %s"
            : "CREATE TABLE IF NOT EXISTS %s.%s (%s TIMESTAMP%s) TAGS %s",
        database->dbName, stbInfo->stbName, stbInfo->primaryKeyName, colsBuf, tagsBuf);
    tmfree(colsBuf);
    tmfree(tagsBuf);
    if (stbInfo->comment != NULL) {
        length += snprintf(command + length, TSDB_MAX_ALLOWED_SQL_LEN - length,
                           " COMMENT '%s'", stbInfo->comment);
    }
    if (stbInfo->delay >= 0) {
        length += snprintf(command + length,
                           TSDB_MAX_ALLOWED_SQL_LEN - length, " DELAY %d",
                           stbInfo->delay);
    }
    if (stbInfo->file_factor >= 0) {
        length +=
            snprintf(command + length,
                     TSDB_MAX_ALLOWED_SQL_LEN - length, " FILE_FACTOR %f",
                     (float)stbInfo->file_factor / 100);
    }
    if (stbInfo->rollup != NULL) {
        length += snprintf(command + length,
                           TSDB_MAX_ALLOWED_SQL_LEN - length,
                           " ROLLUP(%s)", stbInfo->rollup);
    }

    if (stbInfo->max_delay != NULL) {
        length += snprintf(command + length,
                           TSDB_MAX_ALLOWED_SQL_LEN - length,
                " MAX_DELAY %s", stbInfo->max_delay);
    }

    if (stbInfo->watermark != NULL) {
        length += snprintf(command + length,
                           TSDB_MAX_ALLOWED_SQL_LEN - length,
                " WATERMARK %s", stbInfo->watermark);
    }

    // not support ttl in super table
    /*
    if (stbInfo->ttl != 0) {
        length += snprintf(command + length,
                           TSDB_MAX_ALLOWED_SQL_LEN - length,
                " TTL %d", stbInfo->ttl);
    }
    */

    bool first_sma = true;
    for (int i = 0; i < stbInfo->cols->size; i++) {
        Field * col = benchArrayGet(stbInfo->cols, i);
        if (col->sma) {
            if (first_sma) {
                n = snprintf(command + length, TSDB_MAX_ALLOWED_SQL_LEN - length, " SMA(%s", col->name);
                first_sma = false;
            } else {
                n = snprintf(command + length, TSDB_MAX_ALLOWED_SQL_LEN - length, ",%s", col->name);
            }

            if (n < 0 || n > TSDB_MAX_ALLOWED_SQL_LEN - length) {
                errorPrint("%s() LN%d snprintf overflow on %d iteral\n", __func__, __LINE__, i);
                break;
            } else {
                length += n;
            }
        }
    }
    if (!first_sma) {
        snprintf(command + length, TSDB_MAX_ALLOWED_SQL_LEN - length, ")");
    }
    debugPrint("create stable: <%s>\n", command);

    int ret = queryDbExec(database, stbInfo, command);
    free(command);
    return ret;
}


int32_t getVgroupsNative(SBenchConn *conn, SDataBase *database) {
    int     vgroups = 0;
    char    cmd[SHORT_1K_SQL_BUFF_LEN] = "\0";
    snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
            g_arguments->escape_character
            ? "SHOW `%s`.VGROUPS"
            : "SHOW %s.VGROUPS",
            database->dbName);

    TAOS_RES* res = taos_query(conn->taos, cmd);
    int code = taos_errno(res);
    if (code) {
        printErrCmdCodeStr(cmd, code, res);
        return -1;
    }

    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(res)) != NULL) {
        vgroups++;
    }
    debugPrint("%s() LN%d, vgroups: %d\n", __func__, __LINE__, vgroups);
    taos_free_result(res);

    database->vgroups = vgroups;
    database->vgArray = benchArrayInit(vgroups, sizeof(SVGroup));
    for (int32_t v = 0; (v < vgroups
            && !g_arguments->terminate); v++) {
        SVGroup *vg = benchCalloc(1, sizeof(SVGroup), true);
        benchArrayPush(database->vgArray, vg);
    }

    res = taos_query(conn->taos, cmd);
    code = taos_errno(res);
    if (code) {
        printErrCmdCodeStr(cmd, code, res);
        return -1;
    }

    int32_t vgItem = 0;
    while (((row = taos_fetch_row(res)) != NULL)
            && !g_arguments->terminate) {
        SVGroup *vg = benchArrayGet(database->vgArray, vgItem);
        vg->vgId = *(int32_t*)row[0];
        vgItem++;
    }
    taos_free_result(res);

    return vgroups;
}

int geneDbCreateCmd(SDataBase *database, char *command, int remainVnodes) {
    int dataLen = 0;
    int n;

    // create database
    n = snprintf(command + dataLen, SHORT_1K_SQL_BUFF_LEN - dataLen,
                g_arguments->escape_character
                    ? "CREATE DATABASE IF NOT EXISTS `%s`"
                    : "CREATE DATABASE IF NOT EXISTS %s",
                    database->dbName);

    if (n < 0 || n >= SHORT_1K_SQL_BUFF_LEN - dataLen) {
        errorPrint("%s() LN%d snprintf overflow\n",
                           __func__, __LINE__);
        return -1;
    } else {
        dataLen += n;
    }

    int vgroups = g_arguments->inputted_vgroups;

    // append config items
    if (database->cfgs) {
        for (int i = 0; i < database->cfgs->size; i++) {
            SDbCfg* cfg = benchArrayGet(database->cfgs, i);

            // check vgroups
            if (trimCaseCmp(cfg->name, "vgroups") == 0) {
                if (vgroups > 0) {
                    // inputted vgroups by commandline
                    infoPrint("ignore config set vgroups %d\n", cfg->valueint);
                } else {
                    vgroups = cfg->valueint;
                }
                continue;
            }

            if (cfg->valuestring) {
                n = snprintf(command + dataLen,
                                        TSDB_MAX_ALLOWED_SQL_LEN - dataLen,
                            " %s %s", cfg->name, cfg->valuestring);
            } else {
                n = snprintf(command + dataLen,
                                        TSDB_MAX_ALLOWED_SQL_LEN - dataLen,
                            " %s %d", cfg->name, cfg->valueint);
            }
            if (n < 0 || n >= TSDB_MAX_ALLOWED_SQL_LEN - dataLen) {
                errorPrint("%s() LN%d snprintf overflow on %d\n",
                           __func__, __LINE__, i);
                break;
            } else {
                dataLen += n;
            }
        }
    }

    // not found vgroups
    if (vgroups > 0) {
        dataLen += snprintf(command + dataLen, TSDB_MAX_ALLOWED_SQL_LEN - dataLen, " VGROUPS %d", vgroups);
    }

    switch (database->precision) {
        case TSDB_TIME_PRECISION_MILLI:
            snprintf(command + dataLen, TSDB_MAX_ALLOWED_SQL_LEN - dataLen,
                                " PRECISION \'ms\';");
            break;
        case TSDB_TIME_PRECISION_MICRO:
            snprintf(command + dataLen, TSDB_MAX_ALLOWED_SQL_LEN - dataLen,
                                " PRECISION \'us\';");
            break;
        case TSDB_TIME_PRECISION_NANO:
            snprintf(command + dataLen, TSDB_MAX_ALLOWED_SQL_LEN - dataLen,
                                " PRECISION \'ns\';");
            break;
    }

    return dataLen;
}

int createDatabaseRest(SDataBase* database) {
    int32_t code = 0;
    char       command[SHORT_1K_SQL_BUFF_LEN] = "\0";

    int sockfd = createSockFd();
    if (sockfd < 0) {
        return -1;
    }

    // drop exist database
    snprintf(command, SHORT_1K_SQL_BUFF_LEN,
            g_arguments->escape_character
                ? "DROP DATABASE IF EXISTS `%s`;"
                : "DROP DATABASE IF EXISTS %s;",
             database->dbName);
    code = postProcessSql(command,
                        database->dbName,
                        database->precision,
                        REST_IFACE,
                        0,
                        g_arguments->port,
                        false,
                        sockfd,
                        NULL);
    if (code != 0) {
        errorPrint("Failed to drop database %s\n", database->dbName);
    }

    // create database
    int remainVnodes = INT_MAX;
    geneDbCreateCmd(database, command, remainVnodes);
    code = postProcessSql(command,
                        database->dbName,
                        database->precision,
                        REST_IFACE,
                        0,
                        g_arguments->port,
                        false,
                        sockfd,
                        NULL);
    int32_t trying = g_arguments->keep_trying;
    while (code && trying) {
        infoPrint("will sleep %"PRIu32" milliseconds then "
                "re-create database %s\n",
                g_arguments->trying_interval, database->dbName);
        toolsMsleep(g_arguments->trying_interval);
        code = postProcessSql(command,
                        database->dbName,
                        database->precision,
                        REST_IFACE,
                        0,
                        g_arguments->port,
                        false,
                        sockfd,
                        NULL);
        if (trying != -1) {
            trying--;
        }
    }

    destroySockFd(sockfd);
    return code;
}

int32_t getRemainVnodes(SBenchConn *conn) {
    int remainVnodes = 0;
    char command[SHORT_1K_SQL_BUFF_LEN] = "SHOW DNODES";

    TAOS_RES *res = taos_query(conn->taos, command);
    int32_t   code = taos_errno(res);
    if (code) {
        printErrCmdCodeStr(command, code, res);
        closeBenchConn(conn);
        return -1;
    }
    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(res)) != NULL) {
        remainVnodes += (*(int16_t*)(row[3]) - *(int16_t*)(row[2]));
    }
    debugPrint("%s() LN%d, remainVnodes: %d\n",
               __func__, __LINE__, remainVnodes);
    taos_free_result(res);
    return remainVnodes;
}

int createDatabaseTaosc(SDataBase* database) {
    char command[SHORT_1K_SQL_BUFF_LEN] = "\0";
    // conn
    SBenchConn* conn = initBenchConn(NULL);
    if (NULL == conn) {
        return -1;
    }

    // drop stream in old database
    for (int i = 0; i < g_arguments->streams->size; i++) {
        SSTREAM* stream = benchArrayGet(g_arguments->streams, i);
        if (stream->drop) {
            snprintf(command, SHORT_1K_SQL_BUFF_LEN,
                        "DROP STREAM IF EXISTS %s;",
                    stream->stream_name);
            if (queryDbExecCall(conn, command)) {
                closeBenchConn(conn);
                return -1;
            }
            infoPrint("%s\n", command);
            memset(command, 0, SHORT_1K_SQL_BUFF_LEN);
        }
    }

    // drop old database
    snprintf(command, SHORT_1K_SQL_BUFF_LEN,
            g_arguments->escape_character
                ? "DROP DATABASE IF EXISTS `%s`;":
            "DROP DATABASE IF EXISTS %s;",
             database->dbName);
    if (0 != queryDbExecCall(conn, command)) {
        if (g_arguments->dsn) {
            // websocket
            warnPrint("%s", "TDengine cloud normal users have no privilege "
                      "to drop database! DROP DATABASE failure is ignored!\n");
        }
        closeBenchConn(conn);
        return -1;
    }

    // get remain vgroups
    int remainVnodes = INT_MAX;
    if (g_arguments->bind_vgroup) {
        remainVnodes = getRemainVnodes(conn);
        if (0 >= remainVnodes) {
            errorPrint("Remain vnodes %d, failed to create database\n",
                       remainVnodes);
            return -1;
        }
    }

    // generate and execute create database sql
    geneDbCreateCmd(database, command, remainVnodes);
    int32_t code = queryDbExecCall(conn, command);
    int32_t trying = g_arguments->keep_trying;
    while (code && trying) {
        infoPrint("will sleep %"PRIu32" milliseconds then "
                  "re-create database %s\n",
                  g_arguments->trying_interval, database->dbName);
        toolsMsleep(g_arguments->trying_interval);
        code = queryDbExecCall(conn, command);
        if (trying != -1) {
            trying--;
        }
    }

    if (code) {
        if (g_arguments->dsn) {
            warnPrint("%s", "TDengine cloud normal users have no privilege "
                      "to create database! CREATE DATABASE "
                      "failure is ignored!\n");
        }

        closeBenchConn(conn);
        errorPrint("\ncreate database %s failed!\n\n", database->dbName);
        return -1;
    }
    infoPrint("command to create database: <%s>\n", command);


    // malloc and get vgroup
    if (g_arguments->bind_vgroup) {
        int32_t vgroups;
        vgroups = getVgroupsNative(conn, database);
        if (vgroups <= 0) {
            closeBenchConn(conn);
            errorPrint("Database %s's vgroups is %d\n",
                        database->dbName, vgroups);
            return -1;
        }
    }

    closeBenchConn(conn);
    return 0;
}

int createDatabase(SDataBase* database) {
    int ret = 0;
    if (isRest(g_arguments->iface)) {
        ret = createDatabaseRest(database);
    } else {
        ret = createDatabaseTaosc(database);
    }
    return ret;
}

static int generateChildTblName(int len, char *buffer, SDataBase *database,
                                SSuperTable *stbInfo, uint64_t tableSeq, char* tagData, int i,
                                char *ttl, char *tableName) {
    if (0 == len) {
        memset(buffer, 0, TSDB_MAX_ALLOWED_SQL_LEN);
        len += snprintf(buffer + len,
                        TSDB_MAX_ALLOWED_SQL_LEN - len, "CREATE TABLE");
    }

    const char *tagStart = tagData + i * stbInfo->lenOfTags;  // start position of current row's TAG
    const char *tagsForSQL = tagStart;  // actual TAG part for SQL
    const char *fmt = g_arguments->escape_character ?
        " IF NOT EXISTS `%s`.`%s` USING `%s`.`%s` TAGS (%s) %s " :
        " IF NOT EXISTS %s.%s USING %s.%s TAGS (%s) %s ";

    if (stbInfo->useTagTableName) {
        char *firstComma = strchr(tagStart, ',');
        if (firstComma == NULL && tagStart >= firstComma) {
            errorPrint("Invalid tag data format: %s\n", tagStart);
            return -1;
        }
        size_t nameLen = firstComma - tagStart;
        strncpy(tableName, tagStart, nameLen);
        tableName[nameLen] = '\0';
        tagsForSQL = firstComma + 1;
    } else {
        // generate table name using prefix + sequence number
        snprintf(tableName, TSDB_TABLE_NAME_LEN, "%s%" PRIu64,
                 stbInfo->childTblPrefix, tableSeq);
        tagsForSQL = tagStart;

    }

    len += snprintf(buffer + len, TSDB_MAX_ALLOWED_SQL_LEN - len, fmt,
                    database->dbName, tableName,
                    database->dbName, stbInfo->stbName,
                    tagsForSQL, ttl);
    debugPrint("create table: <%s> <%s>\n", buffer, tableName);
    return len;
}

static int getBatchOfTblCreating(threadInfo *pThreadInfo,
                                         SSuperTable *stbInfo) {
    BArray *batchArray = stbInfo->batchTblCreatingNumbersArray;
    if (batchArray) {
        int *batch = benchArrayGet(
                batchArray, pThreadInfo->posOfTblCreatingBatch);
        pThreadInfo->posOfTblCreatingBatch++;
        if (pThreadInfo->posOfTblCreatingBatch == batchArray->size) {
            pThreadInfo->posOfTblCreatingBatch = 0;
        }
        return *batch;
    }
    return 0;
}

static int getIntervalOfTblCreating(threadInfo *pThreadInfo,
                                         SSuperTable *stbInfo) {
    BArray *intervalArray = stbInfo->batchTblCreatingIntervalsArray;
    if (intervalArray) {
        int *interval = benchArrayGet(
                intervalArray, pThreadInfo->posOfTblCreatingInterval);
        pThreadInfo->posOfTblCreatingInterval++;
        if (pThreadInfo->posOfTblCreatingInterval == intervalArray->size) {
            pThreadInfo->posOfTblCreatingInterval = 0;
        }
        return *interval;
    }
    return 0;
}

// table create thread
static void *createTable(void *sarg) {
    if (g_arguments->supplementInsert) {
        return NULL;
    }

    threadInfo  *pThreadInfo    = (threadInfo *)sarg;
    SDataBase   *database       = pThreadInfo->dbInfo;
    SSuperTable *stbInfo        = pThreadInfo->stbInfo;
    uint64_t    lastTotalCreate = 0;
    uint64_t    lastPrintTime   = toolsGetTimestampMs();
    int32_t     len             = 0;
    int32_t     batchNum        = 0;
    char ttl[SMALL_BUFF_LEN]    = "";

#ifdef LINUX
    prctl(PR_SET_NAME, "createTable");
#endif
    pThreadInfo->buffer = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    infoPrint(
              "thread[%d] start creating table from %" PRIu64 " to %" PRIu64
              "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);
    if (stbInfo->ttl != 0) {
        snprintf(ttl, SMALL_BUFF_LEN, "TTL %d", stbInfo->ttl);
    }

    // tag read from csv
    FILE *csvFile = openTagCsv(stbInfo, pThreadInfo->start_table_from);
    // malloc
    char* tagData = benchCalloc(TAG_BATCH_COUNT, stbInfo->lenOfTags, false);
    int         w = 0; // record tagData

    int smallBatchCount = 0;
    int index=  pThreadInfo->start_table_from;
    int tableSum = pThreadInfo->end_table_to - pThreadInfo->start_table_from + 1;
    if (stbInfo->useTagTableName) {
        pThreadInfo->childNames = benchCalloc(tableSum, sizeof(char *), false);
        pThreadInfo->childTblCount = tableSum;
    }

    for (uint64_t i = pThreadInfo->start_table_from, j = 0;
                  i <= pThreadInfo->end_table_to && !g_arguments->terminate;
                  i++, j++) {
        if (g_arguments->terminate) {
            goto create_table_end;
        }
        if (!stbInfo->use_metric || stbInfo->tags->size == 0) {
            if (stbInfo->childTblCount == 1) {
                snprintf(pThreadInfo->buffer, TSDB_MAX_ALLOWED_SQL_LEN,
                         g_arguments->escape_character
                         ? "CREATE TABLE IF NOT EXISTS `%s`.`%s` %s;"
                         : "CREATE TABLE IF NOT EXISTS %s.%s %s;",
                         database->dbName, stbInfo->stbName,
                         stbInfo->colsOfCreateChildTable);
            } else {
                snprintf(pThreadInfo->buffer, TSDB_MAX_ALLOWED_SQL_LEN,
                         g_arguments->escape_character
                         ? "CREATE TABLE IF NOT EXISTS `%s`.`%s` %s;"
                         : "CREATE TABLE IF NOT EXISTS %s.%s %s;",
                         database->dbName,
                         stbInfo->childTblArray[i]->name,
                         stbInfo->colsOfCreateChildTable);
            }
            batchNum++;
        } else {
            if (0 == len) {
                batchNum = 0;
            }
            // generator
            if (w == 0) {
                if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile, NULL, index)) {
                    goto create_table_end;
                }
            }
            char tbName[TSDB_TABLE_NAME_LEN] = {0};
            len = generateChildTblName(len, pThreadInfo->buffer,
                                       database, stbInfo, i, tagData, w, ttl, tbName);
            if (stbInfo->useTagTableName) {
                pThreadInfo->childNames[j] = strdup(tbName);
            }
            // move next
            if (++w >= TAG_BATCH_COUNT) {
                // reset for gen again
                w = 0;
                index += TAG_BATCH_COUNT;
            }

            batchNum++;
            smallBatchCount++;

            int smallBatch = getBatchOfTblCreating(pThreadInfo, stbInfo);
            if ((!smallBatch || (smallBatchCount == smallBatch))
                    && (batchNum < stbInfo->batchTblCreatingNum)
                    && ((TSDB_MAX_ALLOWED_SQL_LEN - len) >=
                        (stbInfo->lenOfTags + EXTRA_SQL_LEN))) {
                continue;
            } else {
                smallBatchCount = 0;
            }
        }

        len = 0;

        int ret = 0;
        debugPrint("thread[%d] creating table: %s\n", pThreadInfo->threadID,
                   pThreadInfo->buffer);
        // REST
        if (REST_IFACE == stbInfo->iface) {
            ret = queryDbExecRest(pThreadInfo->buffer,
                                  database->dbName,
                                  database->precision,
                                  stbInfo->iface,
                                  stbInfo->lineProtocol,
                                  stbInfo->tcpTransfer,
                                  pThreadInfo->sockfd);
        } else {
            ret = queryDbExecCall(pThreadInfo->conn, pThreadInfo->buffer);
            int32_t trying = g_arguments->keep_trying;
            while (ret && trying) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-create "
                          "table %s\n",
                          g_arguments->trying_interval, pThreadInfo->buffer);
                toolsMsleep(g_arguments->trying_interval);
                ret = queryDbExecCall(pThreadInfo->conn, pThreadInfo->buffer);
                if (trying != -1) {
                    trying--;
                }
            }
        }

        if (0 != ret) {
            g_fail = true;
            goto create_table_end;
        }
        uint64_t intervalOfTblCreating = getIntervalOfTblCreating(pThreadInfo,
                                                                  stbInfo);
        if (intervalOfTblCreating) {
            debugPrint("will sleep %"PRIu64" milliseconds "
                       "for table creating interval\n", intervalOfTblCreating);
            toolsMsleep(intervalOfTblCreating);
        }

        pThreadInfo->tables_created += batchNum;
        batchNum = 0;
        uint64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > PRINT_STAT_INTERVAL) {
            float speed = (pThreadInfo->tables_created - lastTotalCreate) * 1000 / (currentPrintTime - lastPrintTime);
            infoPrint("thread[%d] already created %" PRId64 " tables, peroid speed: %.0f tables/s\n",
                       pThreadInfo->threadID, pThreadInfo->tables_created, speed);
            lastPrintTime   = currentPrintTime;
            lastTotalCreate = pThreadInfo->tables_created;
        }
    }

    if (0 != len) {
        int ret = 0;
        debugPrint("thread[%d] creating table: %s\n", pThreadInfo->threadID,
                   pThreadInfo->buffer);
        // REST
        if (REST_IFACE == stbInfo->iface) {
            ret = queryDbExecRest(pThreadInfo->buffer,
                                  database->dbName,
                                  database->precision,
                                  stbInfo->iface,
                                  stbInfo->lineProtocol,
                                  stbInfo->tcpTransfer,
                                  pThreadInfo->sockfd);
        } else {
            ret = queryDbExecCall(pThreadInfo->conn, pThreadInfo->buffer);
        }
        if (0 != ret) {
            g_fail = true;
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        debugPrint("thread[%d] already created %" PRId64 " tables\n",
                   pThreadInfo->threadID, pThreadInfo->tables_created);
    }
create_table_end:
    // free
    tmfree(tagData);
    tmfree(pThreadInfo->buffer);
    pThreadInfo->buffer = NULL;
    if(csvFile) {
        fclose(csvFile);
    }
    return NULL;
}

static int startMultiThreadCreateChildTable(SDataBase* database, SSuperTable* stbInfo) {
    int32_t code    = -1;
    int32_t threads = g_arguments->table_threads;
    int64_t ntables;

    // malloc stmtData for tags
    prepareTagsStmt(stbInfo);

    if (stbInfo->childTblTo > 0) {
        ntables = stbInfo->childTblTo - stbInfo->childTblFrom;
    } else if(stbInfo->childTblFrom > 0) {
        ntables = stbInfo->childTblCount - stbInfo->childTblFrom;
    } else {
        ntables = stbInfo->childTblCount;
    }
    pthread_t   *pids = benchCalloc(1, threads * sizeof(pthread_t), false);
    threadInfo  *infos = benchCalloc(1, threads * sizeof(threadInfo), false);
    uint64_t     tableFrom = stbInfo->childTblFrom;
    if (threads < 1) {
        threads = 1;
    }
    if (ntables == 0) {
        code = 0;
        infoPrint("child table is zero, no need create. childTblCount: %"PRId64"\n", ntables);
        goto over;
    }

    int64_t div = ntables / threads;
    if (div < 1) {
        threads = (int)ntables;
        div = 1;
    }
    int64_t mod = ntables % threads;

    int threadCnt = 0;
    for (uint32_t i = 0; (i < threads && !g_arguments->terminate); i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;
        pThreadInfo->stbInfo = stbInfo;
        pThreadInfo->dbInfo = database;
        if (REST_IFACE == stbInfo->iface) {
            int sockfd = createSockFd();
            if (sockfd < 0) {
                FREE_PIDS_INFOS_RETURN_MINUS_1();
            }
            pThreadInfo->sockfd = sockfd;
        } else {
            pThreadInfo->conn = initBenchConn(pThreadInfo->dbInfo->dbName);
            if (NULL == pThreadInfo->conn) {
                goto over;
            }
        }
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables          = i < mod ? div + 1 : div;
        pThreadInfo->end_table_to     = i < mod ? tableFrom + div : tableFrom + div - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->tables_created = 0;
        debugPrint("div table by thread. i=%d from=%"PRId64" to=%"PRId64" ntable=%"PRId64"\n", i, pThreadInfo->start_table_from,
                                        pThreadInfo->end_table_to, pThreadInfo->ntables);
        pthread_create(pids + i, NULL, createTable, pThreadInfo);
        threadCnt ++;
    }

    for (int i = 0; i < threadCnt; i++) {
        pthread_join(pids[i], NULL);
    }

    if (g_arguments->terminate)  toolsMsleep(100);

    int nCount = 0;
    for (int i = 0; i < threadCnt; i++) {
        threadInfo *pThreadInfo = infos + i;
        g_arguments->actualChildTables += pThreadInfo->tables_created;

        if ((REST_IFACE != stbInfo->iface) && pThreadInfo->conn) {
            closeBenchConn(pThreadInfo->conn);
        }

        if (stbInfo->useTagTableName) {
            for (int j = 0; j < pThreadInfo->childTblCount; j++) {
                stbInfo->childTblArray[nCount++]->name = pThreadInfo->childNames[j];
            }
            tmfree(pThreadInfo->childNames);
        }
    }

    if (g_fail) {
        goto over;
    }
    code = 0;
over:
    free(pids);
    free(infos);
    return code;
}

static int createChildTables() {
    int32_t    code;
    infoPrint("start creating %" PRId64 " table(s) with %d thread(s)\n",
              g_arguments->totalChildTables, g_arguments->table_threads);
    if (g_arguments->fpOfInsertResult) {
        infoPrintToFile(
                  "start creating %" PRId64 " table(s) with %d thread(s)\n",
                  g_arguments->totalChildTables, g_arguments->table_threads);
    }
    int64_t start = (double)toolsGetTimestampMs();

    for (int i = 0; (i < g_arguments->databases->size
            && !g_arguments->terminate); i++) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        if (database->superTbls) {
            for (int j = 0; (j < database->superTbls->size
                    && !g_arguments->terminate); j++) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);
                if (stbInfo->autoTblCreating || stbInfo->iface == SML_IFACE
                    || stbInfo->iface == SML_REST_IFACE) {
                    g_arguments->autoCreatedChildTables +=
                            stbInfo->childTblCount;
                    continue;
                }
                if (stbInfo->childTblExists) {
                    g_arguments->existedChildTables +=
                            stbInfo->childTblCount;
                    continue;
                }
                debugPrint("colsOfCreateChildTable: %s\n",
                        stbInfo->colsOfCreateChildTable);

                code = startMultiThreadCreateChildTable(database, stbInfo);
                if (code && !g_arguments->terminate) {
                    return code;
                }
            }
        }
    }

    int64_t end = toolsGetTimestampMs();
    if(end == start) {
        end += 1;
    }
    succPrint(
            "Spent %.4f seconds to create %" PRId64
            " table(s) with %d thread(s) speed: %.0f tables/s, already exist %" PRId64
            " table(s), actual %" PRId64 " table(s) pre created, %" PRId64
            " table(s) will be auto created\n",
            (float)(end - start) / 1000.0,
            g_arguments->totalChildTables,
            g_arguments->table_threads,
            g_arguments->actualChildTables * 1000 / (float)(end - start),
            g_arguments->existedChildTables,
            g_arguments->actualChildTables,
            g_arguments->autoCreatedChildTables);
    return 0;
}

static void freeChildTable(SChildTable *childTbl, int colsSize) {
    if (childTbl->useOwnSample) {
        if (childTbl->childCols) {
            for (int col = 0; col < colsSize; col++) {
                ChildField *childCol =
                    benchArrayGet(childTbl->childCols, col);
                if (childCol) {
                    tmfree(childCol->stmtData.data);
                    childCol->stmtData.data = NULL;
                    tmfree(childCol->stmtData.is_null);
                    childCol->stmtData.is_null = NULL;
                    tmfree(childCol->stmtData.lengths);
                    childCol->stmtData.lengths = NULL;
                }
            }
            benchArrayDestroy(childTbl->childCols);
        }
        tmfree(childTbl->sampleDataBuf);
    }
    tmfree(childTbl);
}

void postFreeResource() {
    infoPrint("%s\n", "free resource and exit ...");
    if (!g_arguments->terminate) {
        tmfclose(g_arguments->fpOfInsertResult);
    }

    for (int i = 0; i < g_arguments->databases->size; i++) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        if (database->cfgs) {
            for (int c = 0; c < database->cfgs->size; c++) {
                SDbCfg *cfg = benchArrayGet(database->cfgs, c);
                if (cfg->valuestring && cfg->free) {
                    tmfree(cfg->valuestring);
                    cfg->valuestring = NULL;
                }
            }
            benchArrayDestroy(database->cfgs);
        }
        if (database->superTbls) {
            for (uint64_t j = 0; j < database->superTbls->size; j++) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);
                tmfree(stbInfo->colsOfCreateChildTable);
                stbInfo->colsOfCreateChildTable = NULL;
                tmfree(stbInfo->sampleDataBuf);
                stbInfo->sampleDataBuf = NULL;
                tmfree(stbInfo->partialColNameBuf);
                stbInfo->partialColNameBuf = NULL;
                benchArrayDestroy(stbInfo->batchTblCreatingNumbersArray);
                benchArrayDestroy(stbInfo->batchTblCreatingIntervalsArray);
                for (int k = 0; k < stbInfo->tags->size; k++) {
                    Field * tag = benchArrayGet(stbInfo->tags, k);
                    tmfree(tag->stmtData.data);
                    tag->stmtData.data = NULL;
                    tmfree(tag->stmtData.is_null);
                    tag->stmtData.is_null = NULL;
                    tmfree(tag->stmtData.lengths);
                    tag->stmtData.lengths = NULL;
                }
                benchArrayDestroy(stbInfo->tags);

                for (int k = 0; k < stbInfo->cols->size; k++) {
                    Field * col = benchArrayGet(stbInfo->cols, k);
                    tmfree(col->stmtData.data);
                    col->stmtData.data = NULL;
                    tmfree(col->stmtData.is_null);
                    col->stmtData.is_null = NULL;
                    tmfree(col->stmtData.lengths);
                    col->stmtData.lengths = NULL;
                }
                if (g_arguments->test_mode == INSERT_TEST) {
                    if (stbInfo->childTblArray) {
                        for (int64_t child = 0; child < stbInfo->childTblCount;
                                child++) {
                            SChildTable *childTbl = stbInfo->childTblArray[child];
                            if (childTbl) {
                                tmfree(childTbl->name);
                                freeChildTable(childTbl, stbInfo->cols->size);
                            }
                        }
                    }
                }
                benchArrayDestroy(stbInfo->cols);
                tmfree(stbInfo->childTblArray);
                stbInfo->childTblArray = NULL;
                benchArrayDestroy(stbInfo->tsmas);

                // free sqls
                if(stbInfo->sqls) {
                    char **sqls = stbInfo->sqls;
                    while (*sqls) {
                        free(*sqls);
                        sqls++;
                    }
                    tmfree(stbInfo->sqls);
                }

                // thread_bind
                if (database->vgArray) {
                    for (int32_t v = 0; v < database->vgroups; v++) {
                        SVGroup *vg = benchArrayGet(database->vgArray, v);
                        tmfree(vg->childTblArray);
                        vg->childTblArray = NULL;
                    }
                    benchArrayDestroy(database->vgArray);
                    database->vgArray = NULL;
                }
            }
            benchArrayDestroy(database->superTbls);
        }
    }
    benchArrayDestroy(g_arguments->databases);
    benchArrayDestroy(g_arguments->streams);
    tools_cJSON_Delete(root);
}

int32_t execInsert(threadInfo *pThreadInfo, uint32_t k, int64_t *delay3) {
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_RES *   res = NULL;
    int32_t      code = 0;
    uint16_t     iface = stbInfo->iface;
    int64_t      start = 0;
    int32_t      affectRows = 0;

    int32_t trying = (stbInfo->keep_trying)?
        stbInfo->keep_trying:g_arguments->keep_trying;
    int32_t trying_interval = stbInfo->trying_interval?
        stbInfo->trying_interval:g_arguments->trying_interval;
    int protocol = stbInfo->lineProtocol;

    switch (iface) {
        case TAOSC_IFACE:
            debugPrint("buffer: %s\n", pThreadInfo->buffer);
            code = queryDbExecCall(pThreadInfo->conn, pThreadInfo->buffer);
            while (code && trying && !g_arguments->terminate) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-insert\n",
                          trying_interval);
                toolsMsleep(trying_interval);
                code = queryDbExecCall(pThreadInfo->conn, pThreadInfo->buffer);
                if (trying != -1) {
                    trying--;
                }
            }
            break;
        // REST
        case REST_IFACE:
            debugPrint("buffer: %s\n", pThreadInfo->buffer);
            code = postProcessSql(pThreadInfo->buffer,
                                database->dbName,
                                database->precision,
                                stbInfo->iface,
                                stbInfo->lineProtocol,
                                g_arguments->port,
                                stbInfo->tcpTransfer,
                                pThreadInfo->sockfd,
                                pThreadInfo->filePath);
            while (code && trying && !g_arguments->terminate) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-insert\n",
                          trying_interval);
                toolsMsleep(trying_interval);
                code = postProcessSql(pThreadInfo->buffer,
                                    database->dbName,
                                    database->precision,
                                    stbInfo->iface,
                                    stbInfo->lineProtocol,
                                    g_arguments->port,
                                    stbInfo->tcpTransfer,
                                    pThreadInfo->sockfd,
                                    pThreadInfo->filePath);
                if (trying != -1) {
                    trying--;
                }
            }
            break;

        case STMT_IFACE:
            // add batch
            if(!stbInfo->autoTblCreating) {
                start = toolsGetTimestampUs();
                if (taos_stmt_add_batch(pThreadInfo->conn->stmt) != 0) {
                    errorPrint("taos_stmt_add_batch() failed! reason: %s\n",
                            taos_stmt_errstr(pThreadInfo->conn->stmt));
                    return -1;
                }
                if(delay3) {
                    *delay3 += toolsGetTimestampUs() - start;
                }
            }

            // execute
            code = taos_stmt_execute(pThreadInfo->conn->stmt);
            if (code) {
                errorPrint(
                           "failed to execute insert statement. reason: %s\n",
                           taos_stmt_errstr(pThreadInfo->conn->stmt));
                code = -1;
            }
            break;

        case STMT2_IFACE:
            // execute
            code = taos_stmt2_exec(pThreadInfo->conn->stmt2, &affectRows);
            if (code) {
                errorPrint( "failed to call taos_stmt2_exec(). reason: %s\n", taos_stmt2_error(pThreadInfo->conn->stmt2));
                code = -1;
            }
            debugPrint( "succ call taos_stmt2_exec() affectRows:%d\n", affectRows);
            break;

        case SML_IFACE:
            res = taos_schemaless_insert(
                pThreadInfo->conn->taos, pThreadInfo->lines,
                (TSDB_SML_JSON_PROTOCOL == protocol
                    || SML_JSON_TAOS_FORMAT == protocol)
                    ? 0 : k,
                (SML_JSON_TAOS_FORMAT == protocol)
                    ? TSDB_SML_JSON_PROTOCOL : protocol,
                (TSDB_SML_LINE_PROTOCOL == protocol)
                    ? database->sml_precision
                    : TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            code = taos_errno(res);
            trying = stbInfo->keep_trying;
            while (code && trying && !g_arguments->terminate) {
                taos_free_result(res);
                infoPrint("will sleep %"PRIu32" milliseconds then re-insert\n",
                          trying_interval);
                toolsMsleep(trying_interval);
                res = taos_schemaless_insert(
                        pThreadInfo->conn->taos, pThreadInfo->lines,
                        (TSDB_SML_JSON_PROTOCOL == protocol
                            || SML_JSON_TAOS_FORMAT == protocol)
                            ? 0 : k,
                        (SML_JSON_TAOS_FORMAT == protocol)
                            ? TSDB_SML_JSON_PROTOCOL : protocol,
                        (TSDB_SML_LINE_PROTOCOL == protocol)
                            ? database->sml_precision
                            : TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
                code = taos_errno(res);
                if (trying != -1) {
                    trying--;
                }
            }

            if (code != TSDB_CODE_SUCCESS && !g_arguments->terminate) {
                debugPrint("Failed to execute "
                           "schemaless insert content: %s\n\n",
                        pThreadInfo->lines?(pThreadInfo->lines[0]?
                            pThreadInfo->lines[0]:""):"");
                errorPrint(
                    "failed to execute schemaless insert. "
                        "code: 0x%08x reason: %s\n\n",
                        code, taos_errstr(res));
            }
            taos_free_result(res);
            break;
        case SML_REST_IFACE: {
            if (TSDB_SML_JSON_PROTOCOL == protocol
                    || SML_JSON_TAOS_FORMAT == protocol) {
                code = postProcessSql(pThreadInfo->lines[0], database->dbName,
                                    database->precision, stbInfo->iface,
                                    protocol, g_arguments->port,
                                    stbInfo->tcpTransfer,
                                    pThreadInfo->sockfd, pThreadInfo->filePath);
            } else {
                int len = 0;
                for (int i = 0; i < k; i++) {
                    if (strlen(pThreadInfo->lines[i]) != 0) {
                        int n;
                        if (TSDB_SML_TELNET_PROTOCOL == protocol
                                && stbInfo->tcpTransfer) {
                            n = snprintf(pThreadInfo->buffer + len,
                                            TSDB_MAX_ALLOWED_SQL_LEN - len,
                                           "put %s\n", pThreadInfo->lines[i]);
                        } else {
                            n = snprintf(pThreadInfo->buffer + len,
                                            TSDB_MAX_ALLOWED_SQL_LEN - len,
                                            "%s\n",
                                           pThreadInfo->lines[i]);
                        }
                        if (n < 0 || n >= TSDB_MAX_ALLOWED_SQL_LEN - len) {
                            errorPrint("%s() LN%d snprintf overflow on %d\n",
                                __func__, __LINE__, i);
                            break;
                        } else {
                            len += n;
                        }
                    } else {
                        break;
                    }
                }
                if (g_arguments->terminate) {
                    break;
                }
                code = postProcessSql(pThreadInfo->buffer, database->dbName,
                        database->precision,
                        stbInfo->iface, protocol,
                        g_arguments->port,
                        stbInfo->tcpTransfer,
                        pThreadInfo->sockfd, pThreadInfo->filePath);
            }
            break;
        }
    }
    return code;
}

static int smartContinueIfFail(threadInfo *pThreadInfo,
                               SChildTable *childTbl,
                               char *tagData,
                               int64_t i,
                               char *ttl) {
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    char *buffer =
        benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    snprintf(
            buffer, TSDB_MAX_ALLOWED_SQL_LEN,
            g_arguments->escape_character ?
                "CREATE TABLE IF NOT EXISTS `%s`.`%s` USING `%s`.`%s` TAGS (%s) %s "
                : "CREATE TABLE IF NOT EXISTS %s.%s USING %s.%s TAGS (%s) %s ",
            database->dbName, childTbl->name, database->dbName,
            stbInfo->stbName,
            tagData + i * stbInfo->lenOfTags, ttl);
    debugPrint("creating table: %s\n", buffer);

    int ret;
    if (REST_IFACE == stbInfo->iface) {
        ret = queryDbExecRest(buffer,
                              database->dbName,
                              database->precision,
                              stbInfo->iface,
                              stbInfo->lineProtocol,
                              stbInfo->tcpTransfer,
                              pThreadInfo->sockfd);
    } else {
        ret = queryDbExecCall(pThreadInfo->conn, buffer);
        int32_t trying = g_arguments->keep_trying;
        while (ret && trying) {
            infoPrint("will sleep %"PRIu32" milliseconds then "
                      "re-create table %s\n",
                      g_arguments->trying_interval, buffer);
            toolsMsleep(g_arguments->trying_interval);
            ret = queryDbExecCall(pThreadInfo->conn, buffer);
            if (trying != -1) {
                trying--;
            }
        }
    }

    tmfree(buffer);

    return ret;
}

static void cleanupAndPrint(threadInfo *pThreadInfo, char *mode) {
    if (pThreadInfo) {
        if (pThreadInfo->json_array) {
            tools_cJSON_Delete(pThreadInfo->json_array);
            pThreadInfo->json_array = NULL;
        }
        if (0 == pThreadInfo->totalDelay) {
            pThreadInfo->totalDelay = 1;
        }
        succPrint(
            "thread[%d] %s mode, completed total inserted rows: %" PRIu64
            ", %.2f records/second\n",
            pThreadInfo->threadID,
            mode,
            pThreadInfo->totalInsertRows,
            (double)(pThreadInfo->totalInsertRows /
            ((double)pThreadInfo->totalDelay / 1E6)));
    }
}

static int64_t getDisorderTs(SSuperTable *stbInfo, int *disorderRange) {
    int64_t disorderTs = 0;
    int64_t startTimestamp = stbInfo->startTimestamp;
    if (stbInfo->disorderRatio > 0) {
        int rand_num = taosRandom() % 100;
        if (rand_num < stbInfo->disorderRatio) {
            (*disorderRange)--;
            if (0 == *disorderRange) {
                *disorderRange = stbInfo->disorderRange;
            }
            disorderTs = startTimestamp - *disorderRange;
            debugPrint("rand_num: %d, < disorderRatio: %d, "
                       "disorderTs: %"PRId64"\n",
                       rand_num, stbInfo->disorderRatio,
                       disorderTs);
        }
    }
    return disorderTs;
}

void loadChildTableInfo(threadInfo* pThreadInfo) {
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    if(!g_arguments->pre_load_tb_meta) {
        return ;
    }
    if(pThreadInfo->conn == NULL) {
        return ;
    }

    char *db    = pThreadInfo->dbInfo->dbName;
    int64_t cnt = pThreadInfo->end_table_to - pThreadInfo->start_table_from;

    // 100k
    int   bufLen = 100 * 1024;
    char *buf    = benchCalloc(1, bufLen, false);
    int   pos    = 0;
    infoPrint("start load child tables(%"PRId64") info...\n", cnt);
    int64_t start = toolsGetTimestampUs();
    for(int64_t i = pThreadInfo->start_table_from; i < pThreadInfo->end_table_to; i++) {
        SChildTable *childTbl = stbInfo->childTblArray[i];
        pos += sprintf(buf + pos, ",%s.%s", db, childTbl->name);

        if(pos >= bufLen - 256 || i + 1 == pThreadInfo->end_table_to) {
            taos_load_table_info(pThreadInfo->conn, buf);
            pos = 0;
        }
    }
    int64_t delay = toolsGetTimestampUs() - start;
    infoPrint("end load child tables info. delay=%.2fs\n", delay/1E6);
    pThreadInfo->totalDelay += delay;

    tmfree(buf);
}

// create conn again
int32_t reCreateConn(threadInfo * pThreadInfo) {
    // single
    bool single = true;
    if (pThreadInfo->dbInfo->superTbls->size > 1) {
        single = false;
    }

    //
    // retry stmt2 init
    //

    // stmt2 close
    if (pThreadInfo->conn->stmt2) {
        taos_stmt2_close(pThreadInfo->conn->stmt2);
        pThreadInfo->conn->stmt2 = NULL;
    }

    // retry stmt2 init , maybe success
    pThreadInfo->conn->stmt2 = initStmt2(pThreadInfo->conn->taos, single);
    if (pThreadInfo->conn->stmt2) {
        succPrint("%s", "reCreateConn first taos_stmt2_init() success and return.\n");
        return 0;
    }

    //
    // close old
    //
    closeBenchConn(pThreadInfo->conn);
    pThreadInfo->conn = NULL;

    //
    // create new
    //

    // conn
    pThreadInfo->conn = initBenchConn(pThreadInfo->dbInfo->dbName);
    if (pThreadInfo->conn == NULL) {
        errorPrint("%s", "reCreateConn initBenchConn failed.");
        return -1;
    }
    // stmt2
    pThreadInfo->conn->stmt2 = initStmt2(pThreadInfo->conn->taos, single);
    if (NULL == pThreadInfo->conn->stmt2) {
        errorPrint("reCreateConn taos_stmt2_init() failed, reason: %s\n", taos_errstr(NULL));
        return -1;
    }

    succPrint("%s", "reCreateConn second taos_stmt2_init() success.\n");
    // select db
    if (taos_select_db(pThreadInfo->conn->taos, pThreadInfo->dbInfo->dbName)) {
        errorPrint("second taos select database(%s) failed\n", pThreadInfo->dbInfo->dbName);
        return -1;
    }

    return 0;
}

// reinit
int32_t reConnectStmt2(threadInfo * pThreadInfo, int32_t w) {
    // re-create connection
    int32_t code = reCreateConn(pThreadInfo);
    if (code != 0) {
        return code;
    }

    // prepare
    code = prepareStmt2(pThreadInfo->conn->stmt2, pThreadInfo->stbInfo, NULL, w, pThreadInfo->dbInfo->dbName);
    if (code != 0) {
        return code;
    }

    return code;
}

int32_t submitStmt2Impl(threadInfo * pThreadInfo, TAOS_STMT2_BINDV *bindv, int64_t *delay1, int64_t *delay3,
                    int64_t* startTs, int64_t* endTs, uint32_t* generated) {
    // call bind
    int64_t start = toolsGetTimestampUs();
    int32_t code = taos_stmt2_bind_param(pThreadInfo->conn->stmt2, bindv, -1);
    if (code != 0) {
        errorPrint("taos_stmt2_bind_param failed, reason: %s\n", taos_stmt2_error(pThreadInfo->conn->stmt2));
        return code;
    }
    debugPrint("interlace taos_stmt2_bind_param() ok.  bindv->count=%d \n", bindv->count);
    *delay1 += toolsGetTimestampUs() - start;

    // execute
    *startTs = toolsGetTimestampUs();
    code = execInsert(pThreadInfo, *generated, delay3);
    *endTs = toolsGetTimestampUs();
    return code;
}

int32_t submitStmt2(threadInfo * pThreadInfo, TAOS_STMT2_BINDV *bindv, int64_t *delay1, int64_t *delay3,
                    int64_t* startTs, int64_t* endTs, uint32_t* generated, int32_t w) {
    // calc loop
    int32_t loop = 1;
    SSuperTable* stbInfo = pThreadInfo->stbInfo;
    if(stbInfo->continueIfFail == YES_IF_FAILED) {
        if(stbInfo->keep_trying > 1) {
            loop = stbInfo->keep_trying;
        } else {
            loop = 3; // default
        }
    }

    // submit stmt2
    int32_t i = 0;
    bool connected = true;
    while (1) {
        int32_t code = -1;
        if(connected) {
            // reinit success to do submit
            code = submitStmt2Impl(pThreadInfo, bindv, delay1, delay3, startTs, endTs, generated);
        }

        // check code
        if ( code == 0) {
            // success
            break;
        } else {
            // failed to try
            if (--loop == 0) {
                // failed finally
                char tip[64] = "";
                if (i > 0) {
                    snprintf(tip, sizeof(tip), " after retry %d", i);
                }
                errorPrint("finally faild execute submitStmt2()%s\n", tip);
                return -1;
            }

            // wait a memont for trying
            toolsMsleep(stbInfo->trying_interval);
            // reinit
            infoPrint("stmt2 start retry submit i=%d  after sleep %d ms...\n", i++, stbInfo->trying_interval);
            code = reConnectStmt2(pThreadInfo, w);
            if (code != 0) {
                // faild and try again
                errorPrint("faild reConnectStmt2 and retry again for next i=%d \n", i);
                connected = false;
            } else {
                // succ
                connected = true;
            }
        }
    }

    // success
    return 0;
}

static void *syncWriteInterlace(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    infoPrint(
              "thread[%d] start interlace inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);

    int64_t insertRows = stbInfo->insertRows;
    int32_t interlaceRows = stbInfo->interlaceRows;
    uint32_t nBatchTable  = g_arguments->reqPerReq / interlaceRows;
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    uint64_t   lastTotalInsertRows = 0;
    int64_t   startTs = toolsGetTimestampUs();
    int64_t   endTs;
    uint64_t   tableSeq = pThreadInfo->start_table_from;
    int disorderRange = stbInfo->disorderRange;
    int32_t i = 0;

    loadChildTableInfo(pThreadInfo);
    // check if filling back mode
    bool fillBack = false;
    if(stbInfo->useNow && stbInfo->startFillbackTime) {
        fillBack = true;
        pThreadInfo->start_time = stbInfo->startFillbackTime;
        infoPrint("start time change to startFillbackTime = %"PRId64" \n", pThreadInfo->start_time);
    }

    FILE* csvFile = NULL;
    char* tagData = NULL;
    int   w       = 0; // record tags position, if w > TAG_BATCH_COUNT , need recreate new tag values
    if (stbInfo->autoTblCreating) {
        csvFile = openTagCsv(stbInfo, pThreadInfo->start_table_from);
        tagData = benchCalloc(TAG_BATCH_COUNT, stbInfo->lenOfTags, false);
    }
    int64_t delay1 = 0;
    int64_t delay2 = 0;
    int64_t delay3 = 0;
    bool    firstInsertTb = true;

    TAOS_STMT2_BINDV *bindv = NULL;

    // create bindv
    if(stbInfo->iface == STMT2_IFACE) {
        int32_t tagCnt = stbInfo->autoTblCreating ? stbInfo->tags->size : 0;
        if (csvFile) {
            tagCnt = 0;
        }
        //int32_t tagCnt = stbInfo->tags->size;
        bindv = createBindV(nBatchTable,  tagCnt, stbInfo->cols->size + 1);
    }

    bool oldInitStmt = stbInfo->autoTblCreating;
    // not auto create table call once
    if(stbInfo->iface == STMT_IFACE && !oldInitStmt) {
        debugPrint("call prepareStmt for stable:%s\n", stbInfo->stbName);
        if (prepareStmt(pThreadInfo->conn->stmt, stbInfo, tagData, w, database->dbName)) {
            g_fail = true;
            goto free_of_interlace;
        }
    }
    else if (stbInfo->iface == STMT2_IFACE) {
        // only prepare once
        if (prepareStmt2(pThreadInfo->conn->stmt2, stbInfo, NULL, w, database->dbName)) {
            g_fail = true;
            goto free_of_interlace;
        }
    }
    int64_t index = tableSeq;
    while (insertRows > 0) {
        int64_t tmp_total_insert_rows = 0;
        uint32_t generated = 0;
        if (insertRows <= interlaceRows) {
            interlaceRows = insertRows;
        }

        // loop each table
        for (i = 0; i < nBatchTable; i++) {
            if (g_arguments->terminate) {
                goto free_of_interlace;
            }
            int64_t pos = pThreadInfo->pos;

            // get childTable
            SChildTable *childTbl;
            if (g_arguments->bind_vgroup) {
                childTbl = pThreadInfo->vg->childTblArray[tableSeq];
            } else {
                childTbl = stbInfo->childTblArray[tableSeq];
            }

            char*  tableName   = childTbl->name;
            char *sampleDataBuf = childTbl->useOwnSample?
                                        childTbl->sampleDataBuf:
                                        stbInfo->sampleDataBuf;
            // init ts
            if(childTbl->ts == 0) {
               childTbl->ts = pThreadInfo->start_time;
            }
            char ttl[SMALL_BUFF_LEN] = "";
            if (stbInfo->ttl != 0) {
                snprintf(ttl, SMALL_BUFF_LEN, "TTL %d", stbInfo->ttl);
            }
            switch (stbInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE: {
                    char escapedTbName[TSDB_TABLE_NAME_LEN+2] = "\0";
                    if (g_arguments->escape_character) {
                        snprintf(escapedTbName, TSDB_TABLE_NAME_LEN+2, "`%s`",
                                tableName);
                    } else {
                        snprintf(escapedTbName, TSDB_TABLE_NAME_LEN+2, "%s",
                                tableName);
                    }
                    if (i == 0) {
                        ds_add_str(&pThreadInfo->buffer, STR_INSERT_INTO);
                    }

                    // generator
                    if (stbInfo->autoTblCreating && w == 0) {
                        if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile, NULL, index)) {
                            goto free_of_interlace;
                        }
                    }

                    // create child table
                    if (stbInfo->partialColNum == stbInfo->cols->size) {
                        if (stbInfo->autoTblCreating) {
                            ds_add_strs(&pThreadInfo->buffer, 8,
                                    escapedTbName,
                                    " USING `",
                                    stbInfo->stbName,
                                    "` TAGS (",
                                    tagData + stbInfo->lenOfTags * w,
                                    ") ", ttl, " VALUES ");
                        } else {
                            ds_add_strs(&pThreadInfo->buffer, 2,
                                    escapedTbName, " VALUES ");
                        }
                    } else {
                        if (stbInfo->autoTblCreating) {
                            ds_add_strs(&pThreadInfo->buffer, 10,
                                        escapedTbName,
                                        " (",
                                        stbInfo->partialColNameBuf,
                                        ") USING `",
                                        stbInfo->stbName,
                                        "` TAGS (",
                                        tagData + stbInfo->lenOfTags * w,
                                        ") ", ttl, " VALUES ");
                        } else {
                            ds_add_strs(&pThreadInfo->buffer, 4,
                                        escapedTbName,
                                        "(",
                                        stbInfo->partialColNameBuf,
                                        ") VALUES ");
                        }
                    }

                    // move next
                    if (stbInfo->autoTblCreating && ++w >= TAG_BATCH_COUNT) {
                        // reset for gen again
                        w = 0;
                        index += TAG_BATCH_COUNT;
                    }

                    // write child data with interlaceRows
                    for (int64_t j = 0; j < interlaceRows; j++) {
                        int64_t disorderTs = getDisorderTs(stbInfo,
                                &disorderRange);

                        // change fillBack mode with condition
                        if(fillBack) {
                            int64_t tsnow = toolsGetTimestamp(database->precision);
                            if(childTbl->ts >= tsnow){
                                fillBack = false;
                                infoPrint("fillBack mode set end. because timestamp(%"PRId64") >= now(%"PRId64")\n", childTbl->ts, tsnow);
                            }
                        }

                        // timestamp
                        char time_string[BIGINT_BUFF_LEN];
                        if(stbInfo->useNow && stbInfo->interlaceRows == 1 && !fillBack) {
                            int64_t now = toolsGetTimestamp(database->precision);
                            snprintf(time_string, BIGINT_BUFF_LEN, "%"PRId64"", now);
                        } else {
                            snprintf(time_string, BIGINT_BUFF_LEN, "%"PRId64"",
                                    disorderTs?disorderTs:childTbl->ts);
                        }

                        // combine rows timestamp | other cols = sampleDataBuf[pos]
                        if(stbInfo->useSampleTs) {
                            ds_add_strs(&pThreadInfo->buffer, 3, "(",
                                        sampleDataBuf + pos * stbInfo->lenOfCols, ") ");
                        } else {
                            ds_add_strs(&pThreadInfo->buffer, 5, "(", time_string, ",",
                                        sampleDataBuf + pos * stbInfo->lenOfCols, ") ");
                        }
                        // check buffer enough
                        if (ds_len(pThreadInfo->buffer)
                                > stbInfo->max_sql_len) {
                            errorPrint("sql buffer length (%"PRIu64") "
                                    "is larger than max sql length "
                                    "(%"PRId64")\n",
                                    ds_len(pThreadInfo->buffer),
                                    stbInfo->max_sql_len);
                            goto free_of_interlace;
                        }

                        // move next
                        generated++;
                        pos++;
                        if (pos >= g_arguments->prepared_rand) {
                            pos = 0;
                        }
                        if(stbInfo->primary_key)
                            debugPrint("add child=%s %"PRId64" pk cur=%d cnt=%d \n", childTbl->name, childTbl->ts, childTbl->pkCur, childTbl->pkCnt);

                        // primary key
                        if (!stbInfo->primary_key || needChangeTs(stbInfo, &childTbl->pkCur, &childTbl->pkCnt)) {
                            childTbl->ts += stbInfo->timestamp_step;
                            if(stbInfo->primary_key)
                                debugPrint("changedTs child=%s %"PRId64" pk cur=%d cnt=%d \n", childTbl->name, childTbl->ts, childTbl->pkCur, childTbl->pkCnt);
                        }

                    }
                    break;
                }
                case STMT_IFACE: {
                    char escapedTbName[TSDB_TABLE_NAME_LEN+2] = "\0";
                    if (g_arguments->escape_character) {
                        snprintf(escapedTbName, TSDB_TABLE_NAME_LEN+2,
                                "`%s`", tableName);
                    } else {
                        snprintf(escapedTbName, TSDB_TABLE_NAME_LEN, "%s",
                                tableName);
                    }

                    // generator
                    if (stbInfo->autoTblCreating && w == 0) {
                        if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile, NULL, index)) {
                            goto free_of_interlace;
                        }
                    }

                    // old must call prepareStmt for each table
                    if (oldInitStmt) {
                        debugPrint("call prepareStmt for stable:%s\n", stbInfo->stbName);
                        if (prepareStmt(pThreadInfo->conn->stmt, stbInfo, tagData, w, database->dbName)) {
                            g_fail = true;
                            goto free_of_interlace;
                        }
                    }

                    int64_t start = toolsGetTimestampUs();
                    if (taos_stmt_set_tbname(pThreadInfo->conn->stmt,
                                             escapedTbName)) {
                        errorPrint(
                            "taos_stmt_set_tbname(%s) failed, reason: %s\n",
                            tableName,
                                taos_stmt_errstr(pThreadInfo->conn->stmt));
                        g_fail = true;
                        goto free_of_interlace;
                    }
                    delay1 += toolsGetTimestampUs() - start;

                    int32_t n = 0;
                    generated += bindParamBatch(pThreadInfo, interlaceRows,
                                       childTbl->ts, pos, childTbl, &childTbl->pkCur, &childTbl->pkCnt, &n, &delay2, &delay3);

                    // move next
                    pos += interlaceRows;
                    if (pos + interlaceRows + 1 >= g_arguments->prepared_rand) {
                        pos = 0;
                    }
                    childTbl->ts += stbInfo->timestamp_step * n;

                    // move next
                    if (stbInfo->autoTblCreating) {
                        w += 1;
                        if (w >= TAG_BATCH_COUNT) {
                            // reset for gen again
                            w = 0;
                            index += TAG_BATCH_COUNT;
                        }
                    }

                    break;
                }
                case STMT2_IFACE: {
                    // tbnames
                    bindv->tbnames[i] = childTbl->name;

                    // tags
                    if (stbInfo->autoTblCreating && firstInsertTb) {
                        // create
                        if (w == 0) {
                            // recreate sample tags
                            if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile, pThreadInfo->tagsStmt, index)) {
                                goto free_of_interlace;
                            }
                        }

                        if (csvFile) {
                            if (prepareStmt2(pThreadInfo->conn->stmt2, stbInfo, tagData, w, database->dbName)) {
                                g_fail = true;
                                goto free_of_interlace;
                            }
                        }

                        bindVTags(bindv, i, w, pThreadInfo->tagsStmt);
                    }

                    // cols
                    int32_t n = 0;
                    generated += bindVColsInterlace(bindv, i, pThreadInfo, interlaceRows, childTbl->ts, pos,
                                                    childTbl, &childTbl->pkCur, &childTbl->pkCnt, &n);
                    // move next
                    pos += interlaceRows;
                    if (pos + interlaceRows + 1 >= g_arguments->prepared_rand) {
                        pos = 0;
                    }
                    childTbl->ts += stbInfo->timestamp_step * n;
                    if (stbInfo->autoTblCreating) {
                        w += 1;
                        if (w >= TAG_BATCH_COUNT) {
                            // reset for gen again
                            w = 0;
                            index += TAG_BATCH_COUNT;
                        }
                    }

                    break;
                }
                case SML_REST_IFACE:
                case SML_IFACE: {
                    int protocol = stbInfo->lineProtocol;
                    for (int64_t j = 0; j < interlaceRows; j++) {
                        int64_t disorderTs = getDisorderTs(stbInfo,
                                &disorderRange);
                        if (TSDB_SML_JSON_PROTOCOL == protocol) {
                            tools_cJSON *tag = tools_cJSON_Duplicate(
                                tools_cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                    true);
                            generateSmlJsonCols(
                                pThreadInfo->json_array, tag, stbInfo,
                                database->sml_precision,
                                    disorderTs?disorderTs:childTbl->ts);
                        } else if (SML_JSON_TAOS_FORMAT == protocol) {
                            tools_cJSON *tag = tools_cJSON_Duplicate(
                                tools_cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                    true);
                            generateSmlTaosJsonCols(
                                pThreadInfo->json_array, tag, stbInfo,
                                database->sml_precision,
                                disorderTs?disorderTs:childTbl->ts);
                        } else if (TSDB_SML_LINE_PROTOCOL == protocol) {
                            snprintf(
                                pThreadInfo->lines[generated],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %s %" PRId64,
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from],
                                    sampleDataBuf + pos * stbInfo->lenOfCols,
                                disorderTs?disorderTs:childTbl->ts);
                        } else {
                            snprintf(
                                pThreadInfo->lines[generated],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %" PRId64 " %s %s", stbInfo->stbName,
                                disorderTs?disorderTs:childTbl->ts,
                                    sampleDataBuf + pos * stbInfo->lenOfCols,
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from]);
                        }
                        generated++;
                        // primary key
                        if (!stbInfo->primary_key || needChangeTs(stbInfo, &childTbl->pkCur, &childTbl->pkCnt)) {
                            childTbl->ts += stbInfo->timestamp_step;
                        }
                    }
                    if (TSDB_SML_JSON_PROTOCOL == protocol
                            || SML_JSON_TAOS_FORMAT == protocol) {
                        pThreadInfo->lines[0] =
                            tools_cJSON_PrintUnformatted(
                                pThreadInfo->json_array);
                    }
                    break;
                }
            }

            // move to next table in one batch
            tableSeq++;
            tmp_total_insert_rows += interlaceRows;
            if (tableSeq > pThreadInfo->end_table_to) {
                // first insert tables loop is end
                firstInsertTb = false;
                // one tables loop timestamp and pos add
                tableSeq = pThreadInfo->start_table_from;
                // save
                pThreadInfo->pos = pos;
                if (!stbInfo->non_stop) {
                    insertRows -= interlaceRows;
                }

                // if fillBack mode , can't sleep
                if (stbInfo->insert_interval > 0 && !fillBack) {
                    debugPrint("%s() LN%d, insert_interval: %"PRIu64"\n",
                          __func__, __LINE__, stbInfo->insert_interval);
                    perfPrint("sleep %" PRIu64 " ms\n",
                                     stbInfo->insert_interval);
                    toolsMsleep((int32_t)stbInfo->insert_interval);
                }

                i++;
                // rectify bind count
                if (bindv && bindv->count != i) {
                    bindv->count = i;
                }
                break;
            }
        }

        // exec
        if(stbInfo->iface == STMT2_IFACE) {
            // exec stmt2
            if(g_arguments->debug_print)
                showBindV(bindv, stbInfo->tags, stbInfo->cols);
            // bind & exec stmt2
            if (submitStmt2(pThreadInfo, bindv, &delay1, &delay3, &startTs, &endTs, &generated, w) != 0) {
                g_fail = true;
                goto free_of_interlace;
            }
        } else {
            // exec other
            startTs = toolsGetTimestampUs();
            if (execInsert(pThreadInfo, generated, &delay3)) {
                g_fail = true;
                goto free_of_interlace;
            }
            endTs = toolsGetTimestampUs();
        }

        debugPrint("execInsert tableIndex=%d left insert rows=%"PRId64" generated=%d\n", i, insertRows, generated);

        // reset count
        if(bindv) {
            bindv->count = 0;
        }

        pThreadInfo->totalInsertRows += tmp_total_insert_rows;

        if (g_arguments->terminate) {
            goto free_of_interlace;
        }

        int protocol = stbInfo->lineProtocol;
        switch (stbInfo->iface) {
            case TAOSC_IFACE:
            case REST_IFACE:
                debugPrint("pThreadInfo->buffer: %s\n",
                           pThreadInfo->buffer);
                free_ds(&pThreadInfo->buffer);
                pThreadInfo->buffer = new_ds(0);
                break;
            case SML_REST_IFACE:
                memset(pThreadInfo->buffer, 0,
                       g_arguments->reqPerReq * (pThreadInfo->max_sql_len + 1));
            case SML_IFACE:
                if (TSDB_SML_JSON_PROTOCOL == protocol
                        || SML_JSON_TAOS_FORMAT == protocol) {
                    debugPrint("pThreadInfo->lines[0]: %s\n",
                               pThreadInfo->lines[0]);
                    if (pThreadInfo->json_array && !g_arguments->terminate) {
                        tools_cJSON_Delete(pThreadInfo->json_array);
                        pThreadInfo->json_array = NULL;
                    }
                    pThreadInfo->json_array = tools_cJSON_CreateArray();
                    if (pThreadInfo->lines && pThreadInfo->lines[0]) {
                        tmfree(pThreadInfo->lines[0]);
                        pThreadInfo->lines[0] = NULL;
                    }
                } else {
                    for (int j = 0; j < generated; j++) {
                        if (pThreadInfo && pThreadInfo->lines
                                && !g_arguments->terminate) {
                            debugPrint("pThreadInfo->lines[%d]: %s\n", j,
                                       pThreadInfo->lines[j]);
                            memset(pThreadInfo->lines[j], 0,
                                   pThreadInfo->max_sql_len);
                        }
                    }
                }
                break;
            case STMT_IFACE:
                break;
        }

        int64_t delay4 = endTs - startTs;
        int64_t delay = delay1 + delay2 + delay3 + delay4;
        if (delay <=0) {
            debugPrint("thread[%d]: startTS: %"PRId64", endTS: %"PRId64"\n",
                       pThreadInfo->threadID, startTs, endTs);
        } else {
            perfPrint("insert execution time is %10.2f ms\n",
                      delay / 1E6);

            int64_t * pdelay = benchCalloc(1, sizeof(int64_t), false);
            *pdelay = delay;
            if (benchArrayPush(pThreadInfo->delayList, pdelay) == NULL) {
                tmfree(pdelay);
            }
            pThreadInfo->totalDelay += delay;
            pThreadInfo->totalDelay1 += delay1;
            pThreadInfo->totalDelay2 += delay2;
            pThreadInfo->totalDelay3 += delay3;
        }
        delay1 = delay2 = delay3 = 0;

        int64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            infoPrint(
                    "thread[%d] has currently inserted rows: %" PRIu64
                    ", peroid insert rate: %.3f rows/s \n",
                    pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                    (double)(pThreadInfo->totalInsertRows - lastTotalInsertRows) * 1000.0/(currentPrintTime - lastPrintTime));
            lastPrintTime = currentPrintTime;
	        lastTotalInsertRows = pThreadInfo->totalInsertRows;
        }
    }

free_of_interlace:
    cleanupAndPrint(pThreadInfo, "interlace");
    if(csvFile) {
        fclose(csvFile);
    }
    tmfree(tagData);
    freeBindV(bindv);
    return NULL;
}

static int32_t prepareProgressDataStmt(
        threadInfo *pThreadInfo,
        SChildTable *childTbl,
        int64_t *timestamp, uint64_t i, char *ttl, int32_t *pkCur, int32_t *pkCnt, int64_t *delay1, int64_t *delay2, int64_t *delay3) {
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    char escapedTbName[TSDB_TABLE_NAME_LEN + 2] = "\0";
    if (g_arguments->escape_character) {
        snprintf(escapedTbName, TSDB_TABLE_NAME_LEN + 2,
                 "`%s`", childTbl->name);
    } else {
        snprintf(escapedTbName, TSDB_TABLE_NAME_LEN, "%s",
                 childTbl->name);
    }
    int64_t start = toolsGetTimestampUs();
    if (taos_stmt_set_tbname(pThreadInfo->conn->stmt,
                             escapedTbName)) {
        errorPrint(
                "taos_stmt_set_tbname(%s) failed,"
                "reason: %s\n", escapedTbName,
                taos_stmt_errstr(pThreadInfo->conn->stmt));
        return -1;
    }
    *delay1 = toolsGetTimestampUs() - start;
    int32_t n = 0;
    int64_t pos = i % g_arguments->prepared_rand;
    if (g_arguments->prepared_rand - pos < g_arguments->reqPerReq) {
        // remain prepare data less than batch, reset pos to zero
        pos = 0;
    }
    int32_t generated = bindParamBatch(
            pThreadInfo,
            (g_arguments->reqPerReq > (stbInfo->insertRows - i))
                ? (stbInfo->insertRows - i)
                : g_arguments->reqPerReq,
            *timestamp, pos, childTbl, pkCur, pkCnt, &n, delay2, delay3);
    *timestamp += n * stbInfo->timestamp_step;
    return generated;
}

static void makeTimestampDisorder(
        int64_t *timestamp, SSuperTable *stbInfo) {
    int64_t startTimestamp = stbInfo->startTimestamp;
    int disorderRange = stbInfo->disorderRange;
    int rand_num = taosRandom() % 100;
    if (rand_num < stbInfo->disorderRatio) {
        disorderRange--;
        if (0 == disorderRange) {
            disorderRange = stbInfo->disorderRange;
        }
        *timestamp = startTimestamp - disorderRange;
        debugPrint("rand_num: %d, < disorderRatio: %d"
                   ", ts: %"PRId64"\n",
                   rand_num,
                   stbInfo->disorderRatio,
                   *timestamp);
    }
}

static int32_t prepareProgressDataSmlJsonText(
    threadInfo *pThreadInfo,
    uint64_t tableSeq,
    int64_t *timestamp, uint64_t i, char *ttl, int32_t *pkCur, int32_t *pkCnt) {
    // prepareProgressDataSmlJsonText
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int32_t generated = 0;

    int len = 0;

    char *line = pThreadInfo->lines[0];
    uint32_t line_buf_len = pThreadInfo->line_buf_len;

    strncat(line + len, "[", 2);
    len += 1;

    int32_t pos = 0;
    for (int j = 0; (j < g_arguments->reqPerReq)
            && !g_arguments->terminate; j++) {
        strncat(line + len, "{", 2);
        len += 1;
        int n;
        n = snprintf(line + len, line_buf_len - len,
                 "\"timestamp\":%"PRId64",", *timestamp);
        if (n < 0 || n >= line_buf_len - len) {
            errorPrint("%s() LN%d snprintf overflow on %d\n",
                       __func__, __LINE__, j);
            return -1;
        } else {
            len += n;
        }

        n = snprintf(line + len, line_buf_len - len, "%s",
                        pThreadInfo->sml_json_value_array[tableSeq]);
        if (n < 0 || n >= line_buf_len - len) {
            errorPrint("%s() LN%d snprintf overflow on %d\n",
                       __func__, __LINE__, j);
            return -1;
        } else {
            len += n;
        }
        n = snprintf(line + len, line_buf_len - len, "\"tags\":%s,",
                       pThreadInfo->sml_tags_json_array[tableSeq]);
        if (n < 0 || n >= line_buf_len - len) {
            errorPrint("%s() LN%d snprintf overflow on %d\n",
                       __func__, __LINE__, j);
            return -1;
        } else {
            len += n;
        }
        n = snprintf(line + len, line_buf_len - len,
                       "\"metric\":\"%s\"}", stbInfo->stbName);
        if (n < 0 || n >= line_buf_len - len) {
            errorPrint("%s() LN%d snprintf overflow on %d\n",
                       __func__, __LINE__, j);
            return -1;
        } else {
            len += n;
        }

        pos++;
        if (pos >= g_arguments->prepared_rand) {
            pos = 0;
        }

        // primay key repeat ts count
        if (!stbInfo->primary_key || needChangeTs(stbInfo, pkCur, pkCnt)) {
            *timestamp += stbInfo->timestamp_step;
        }

        if (stbInfo->disorderRatio > 0) {
            makeTimestampDisorder(timestamp, stbInfo);
        }
        generated++;
        if (i + generated >= stbInfo->insertRows) {
            break;
        }
        if ((j+1) < g_arguments->reqPerReq) {
            strncat(line + len, ",", 2);
            len += 1;
        }
    }
    strncat(line + len, "]", 2);

    debugPrint("%s() LN%d, lines[0]: %s\n",
               __func__, __LINE__, pThreadInfo->lines[0]);
    return generated;
}

static int32_t prepareProgressDataSmlJson(
    threadInfo *pThreadInfo,
    uint64_t tableSeq,
    int64_t *timestamp, uint64_t i, char *ttl, int32_t *pkCur, int32_t *pkCnt) {
    // prepareProgressDataSmlJson
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int32_t generated = 0;

    int32_t pos = 0;
    for (int j = 0; (j < g_arguments->reqPerReq)
            && !g_arguments->terminate; j++) {
        tools_cJSON *tag = tools_cJSON_Duplicate(
                tools_cJSON_GetArrayItem(
                    pThreadInfo->sml_json_tags,
                    (int)tableSeq -
                    pThreadInfo->start_table_from),
                true);
        debugPrintJsonNoTime(tag);
        generateSmlTaosJsonCols(
                pThreadInfo->json_array, tag, stbInfo,
                database->sml_precision, *timestamp);
        pos++;
        if (pos >= g_arguments->prepared_rand) {
            pos = 0;
        }

        // primay key repeat ts count
        if (!stbInfo->primary_key || needChangeTs(stbInfo, pkCur, pkCnt)) {
            *timestamp += stbInfo->timestamp_step;
        }

        if (stbInfo->disorderRatio > 0) {
            makeTimestampDisorder(timestamp, stbInfo);
        }
        generated++;
        if (i + generated >= stbInfo->insertRows) {
            break;
        }
    }

    tmfree(pThreadInfo->lines[0]);
    pThreadInfo->lines[0] = NULL;
    pThreadInfo->lines[0] =
            tools_cJSON_PrintUnformatted(
                pThreadInfo->json_array);
    debugPrint("pThreadInfo->lines[0]: %s\n",
                   pThreadInfo->lines[0]);

    return generated;
}

static int32_t prepareProgressDataSmlLineOrTelnet(
    threadInfo *pThreadInfo, uint64_t tableSeq, char *sampleDataBuf,
    int64_t *timestamp, uint64_t i, char *ttl, int protocol, int32_t *pkCur, int32_t *pkCnt) {
    // prepareProgressDataSmlLine
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int32_t generated = 0;

    int32_t pos = 0;
    for (int j = 0; (j < g_arguments->reqPerReq)
            && !g_arguments->terminate; j++) {
        // table index
        int ti = tableSeq - pThreadInfo->start_table_from;
        if (TSDB_SML_LINE_PROTOCOL == protocol) {
            snprintf(
                    pThreadInfo->lines[j],
                    stbInfo->lenOfCols + stbInfo->lenOfTags,
                    "%s %s %" PRId64,
                    pThreadInfo->sml_tags[ti],
                    sampleDataBuf + pos * stbInfo->lenOfCols,
                    *timestamp);
        } else {
            snprintf(
                    pThreadInfo->lines[j],
                    stbInfo->lenOfCols + stbInfo->lenOfTags,
                    "%s %" PRId64 " %s %s", stbInfo->stbName,
                    *timestamp,
                    sampleDataBuf
                    + pos * stbInfo->lenOfCols,
                    pThreadInfo->sml_tags[ti]);
        }
        //infoPrint("sml prepare j=%d stb=%s sml_tags=%s \n", j, stbInfo->stbName, pThreadInfo->sml_tags[ti]);
        pos++;
        if (pos >= g_arguments->prepared_rand) {
            pos = 0;
        }
        // primay key repeat ts count
        if (!stbInfo->primary_key || needChangeTs(stbInfo, pkCur, pkCnt)) {
            *timestamp += stbInfo->timestamp_step;
        }

        if (stbInfo->disorderRatio > 0) {
            makeTimestampDisorder(timestamp, stbInfo);
        }
        generated++;
        if (i + generated >= stbInfo->insertRows) {
            break;
        }
    }
    return generated;
}

static int32_t prepareProgressDataSml(
    threadInfo *pThreadInfo,
    SChildTable *childTbl,
    uint64_t tableSeq,
    int64_t *timestamp, uint64_t i, char *ttl, int32_t *pkCur, int32_t *pkCnt) {
    // prepareProgressDataSml
    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    char *sampleDataBuf;
    if (childTbl->useOwnSample) {
        sampleDataBuf = childTbl->sampleDataBuf;
    } else {
        sampleDataBuf = stbInfo->sampleDataBuf;
    }
    int protocol = stbInfo->lineProtocol;
    int32_t generated = -1;
    switch (protocol) {
        case TSDB_SML_LINE_PROTOCOL:
        case TSDB_SML_TELNET_PROTOCOL:
            generated = prepareProgressDataSmlLineOrTelnet(
                    pThreadInfo,
                    tableSeq,
                    sampleDataBuf,
                    timestamp, i, ttl, protocol, pkCur, pkCnt);
            break;
        case TSDB_SML_JSON_PROTOCOL:
            generated = prepareProgressDataSmlJsonText(
                    pThreadInfo,
                    tableSeq - pThreadInfo->start_table_from,
                timestamp, i, ttl, pkCur, pkCnt);
            break;
        case SML_JSON_TAOS_FORMAT:
            generated = prepareProgressDataSmlJson(
                    pThreadInfo,
                    tableSeq,
                    timestamp, i, ttl, pkCur, pkCnt);
            break;
        default:
            errorPrint("%s() LN%d: unknown protcolor: %d\n",
                       __func__, __LINE__, protocol);
            break;
    }

    return generated;
}

// if return true, timestmap must add timestap_step, else timestamp no need changed
bool needChangeTs(SSuperTable * stbInfo, int32_t *pkCur, int32_t *pkCnt) {
    // check need generate cnt
    if(*pkCnt == 0) {
        if (stbInfo->repeat_ts_min >= stbInfo->repeat_ts_max) {
            // fixed count value is max
            if (stbInfo->repeat_ts_max == 0){
                return true;
            }

            *pkCnt = stbInfo->repeat_ts_max;
        } else {
            // random range
            *pkCnt = RD(stbInfo->repeat_ts_max + 1);
            if(*pkCnt < stbInfo->repeat_ts_min) {
                *pkCnt = (*pkCnt + stbInfo->repeat_ts_min) % stbInfo->repeat_ts_max;
            }
        }
    }

    // compare with current value
    *pkCur = *pkCur + 1;
    if(*pkCur >= *pkCnt) {
        // reset zero
        *pkCur = 0;
        *pkCnt = 0;
        return true;
    } else {
        // add one
        return false;
    }
}

static int32_t prepareProgressDataSql(
                    threadInfo *pThreadInfo,
                    SChildTable *childTbl,
                    char* tagData,
                    uint64_t tableSeq,
                    char *sampleDataBuf,
                    int64_t *timestamp, uint64_t i, char *ttl,
                    int32_t *pos, uint64_t *len, int32_t* pkCur, int32_t* pkCnt) {
    // prepareProgressDataSql
    int32_t generated = 0;
    SDataBase *database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    char *  pstr = pThreadInfo->buffer;
    int disorderRange = stbInfo->disorderRange;

    if (stbInfo->partialColNum == stbInfo->cols->size) {
        if (stbInfo->autoTblCreating) {
            *len =
                snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN,
                        g_arguments->escape_character
                        ? "%s `%s`.`%s` USING `%s`.`%s` TAGS (%s) %s VALUES "
                        : "%s %s.%s USING %s.%s TAGS (%s) %s VALUES ",
                         STR_INSERT_INTO, database->dbName,
                         childTbl->name, database->dbName,
                         stbInfo->stbName,
                         tagData +
                         stbInfo->lenOfTags * tableSeq, ttl);
        } else {
            *len = snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN,
                    g_arguments->escape_character
                           ? "%s `%s`.`%s` VALUES "
                           : "%s %s.%s VALUES ",
                           STR_INSERT_INTO,
                           database->dbName, childTbl->name);
        }
    } else {
        if (stbInfo->autoTblCreating) {
            *len = snprintf(
                    pstr, TSDB_MAX_ALLOWED_SQL_LEN,
                    g_arguments->escape_character
                    ? "%s `%s`.`%s` (%s) USING `%s`.`%s` TAGS (%s) %s VALUES "
                    : "%s %s.%s (%s) USING %s.%s TAGS (%s) %s VALUES ",
                    STR_INSERT_INTO, database->dbName,
                    childTbl->name,
                    stbInfo->partialColNameBuf,
                    database->dbName, stbInfo->stbName,
                    tagData +
                    stbInfo->lenOfTags * tableSeq, ttl);
        } else {
            *len = snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN,
                    g_arguments->escape_character
                    ? "%s `%s`.`%s` (%s) VALUES "
                    : "%s %s.%s (%s) VALUES ",
                    STR_INSERT_INTO, database->dbName,
                    childTbl->name,
                    stbInfo->partialColNameBuf);
        }
    }

    char *ownSampleDataBuf;
    if (childTbl->useOwnSample) {
        debugPrint("%s is using own sample data\n",
                  childTbl->name);
        ownSampleDataBuf = childTbl->sampleDataBuf;
    } else {
        ownSampleDataBuf = stbInfo->sampleDataBuf;
    }
    for (int j = 0; j < g_arguments->reqPerReq; j++) {
        if (stbInfo->useSampleTs
                && (!stbInfo->random_data_source)) {
            *len +=
                snprintf(pstr + *len,
                         TSDB_MAX_ALLOWED_SQL_LEN - *len, "(%s)",
                         sampleDataBuf +
                         *pos * stbInfo->lenOfCols);
        } else {
            int64_t disorderTs = getDisorderTs(stbInfo, &disorderRange);
            *len += snprintf(pstr + *len,
                            TSDB_MAX_ALLOWED_SQL_LEN - *len,
                            "(%" PRId64 ",%s)",
                            disorderTs?disorderTs:*timestamp,
                            ownSampleDataBuf +
                            *pos * stbInfo->lenOfCols);
        }
        *pos += 1;
        if (*pos >= g_arguments->prepared_rand) {
            *pos = 0;
        }
        // primary key
        if(!stbInfo->primary_key || needChangeTs(stbInfo, pkCur, pkCnt)) {
            *timestamp += stbInfo->timestamp_step;
        }

        generated++;
        if (*len > (TSDB_MAX_ALLOWED_SQL_LEN
            - stbInfo->lenOfCols)) {
            break;
        }
        if (i + generated >= stbInfo->insertRows) {
            break;
        }
    }

    return generated;
}

void *syncWriteProgressive(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    loadChildTableInfo(pThreadInfo);

    // special deal flow for TAOSC_IFACE
    if (insertDataMix(pThreadInfo, database, stbInfo)) {
        // request be dealt by this function , so return
        return NULL;
    }

    infoPrint(
        "thread[%d] start progressive inserting into table from "
        "%" PRIu64 " to %" PRIu64 "\n",
        pThreadInfo->threadID, pThreadInfo->start_table_from,
        pThreadInfo->end_table_to + 1);

    uint64_t  lastPrintTime = toolsGetTimestampMs();
    uint64_t  lastTotalInsertRows = 0;
    int64_t   startTs = toolsGetTimestampUs();
    int64_t   endTs;

    FILE* csvFile = NULL;
    char* tagData = NULL;
    bool  stmt    = (stbInfo->iface == STMT_IFACE || stbInfo->iface == STMT2_IFACE) && stbInfo->autoTblCreating;
    bool  smart   = SMART_IF_FAILED == stbInfo->continueIfFail;
    bool  acreate = (stbInfo->iface == TAOSC_IFACE || stbInfo->iface == REST_IFACE) && stbInfo->autoTblCreating;
    int   w       = 0;
    if (stmt || smart || acreate) {
        csvFile = openTagCsv(stbInfo, pThreadInfo->start_table_from);
        tagData = benchCalloc(TAG_BATCH_COUNT, stbInfo->lenOfTags, false);
    }

    bool oldInitStmt = stbInfo->autoTblCreating;
    // stmt.  not auto table create call on stmt
    if (stbInfo->iface == STMT_IFACE && !oldInitStmt) {
        if (prepareStmt(pThreadInfo->conn->stmt, stbInfo, tagData, w, database->dbName)) {
            g_fail = true;
            goto free_of_progressive;
        }
    }
    else if (stbInfo->iface == STMT2_IFACE && !stbInfo->autoTblCreating) {
        if (prepareStmt2(pThreadInfo->conn->stmt2, stbInfo, tagData, w, database->dbName)) {
            g_fail = true;
            goto free_of_progressive;
        }
    }

    //
    // loop write each child table
    //
    int16_t index = pThreadInfo->start_table_from;
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
            tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        char *sampleDataBuf;
        SChildTable *childTbl;

        if (g_arguments->bind_vgroup) {
            childTbl = pThreadInfo->vg->childTblArray[tableSeq];
        } else {
            childTbl = stbInfo->childTblArray[tableSeq];
        }
        debugPrint("tableSeq=%"PRId64" childTbl->name=%s\n", tableSeq, childTbl->name);

        if (childTbl->useOwnSample) {
            sampleDataBuf = childTbl->sampleDataBuf;
        } else {
            sampleDataBuf = stbInfo->sampleDataBuf;
        }

        int64_t  timestamp = pThreadInfo->start_time;
        uint64_t len = 0;
        int32_t pos = 0;
        int32_t pkCur = 0; // record generate same timestamp current count
        int32_t pkCnt = 0; // record generate same timestamp count
        int64_t delay1 = 0;
        int64_t delay2 = 0;
        int64_t delay3 = 0;

        if(stmt || smart || acreate) {
            // generator
            if (w == 0) {
                if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile, NULL, index)) {
                    g_fail = true;
                    goto free_of_progressive;
                }
            }
        }

        // old init stmt must call for each table
        if (stbInfo->iface == STMT_IFACE && oldInitStmt) {
            if (prepareStmt(pThreadInfo->conn->stmt, stbInfo, tagData, w, database->dbName)) {
                g_fail = true;
                goto free_of_progressive;
            }
        }
        else if (stbInfo->iface == STMT2_IFACE && stbInfo->autoTblCreating) {
            if (prepareStmt2(pThreadInfo->conn->stmt2, stbInfo, tagData, w, database->dbName)) {
                g_fail = true;
                goto free_of_progressive;
            }
        }

        if(stmt || smart || acreate) {
            // move next
            if (++w >= TAG_BATCH_COUNT) {
                // reset for gen again
                w = 0;
                index += TAG_BATCH_COUNT;
            }
        }

        char ttl[SMALL_BUFF_LEN] = "";
        if (stbInfo->ttl != 0) {
            snprintf(ttl, SMALL_BUFF_LEN, "TTL %d", stbInfo->ttl);
        }
        for (uint64_t i = 0; i < stbInfo->insertRows;) {
            if (g_arguments->terminate) {
                goto free_of_progressive;
            }
            int32_t generated = 0;
            switch (stbInfo->iface) {
                case TAOSC_IFACE:
                case REST_IFACE:
                    generated = prepareProgressDataSql(
                            pThreadInfo,
                            childTbl,
                            tagData,
                            w,
                            sampleDataBuf,
                            &timestamp, i, ttl, &pos, &len, &pkCur, &pkCnt);
                    break;
                case STMT_IFACE: {
                    generated = prepareProgressDataStmt(
                            pThreadInfo,
                            childTbl, &timestamp, i, ttl, &pkCur, &pkCnt, &delay1, &delay2, &delay3);
                    break;
                }
                case STMT2_IFACE: {
                    generated = stmt2BindAndSubmit(
                            pThreadInfo,
                            childTbl, &timestamp, i, ttl, &pkCur, &pkCnt, &delay1,
                            &delay3, &startTs, &endTs, w);
                    break;
                }
                case SML_REST_IFACE:
                case SML_IFACE:
                    generated = prepareProgressDataSml(
                            pThreadInfo,
                            childTbl,
                            tableSeq, &timestamp, i, ttl, &pkCur, &pkCnt);
                    break;
                default:
                    break;
            }
            if (generated < 0) {
                g_fail = true;
                goto free_of_progressive;
            }
            if (!stbInfo->non_stop) {
                i += generated;
            }

            // stmt2 execInsert already execute on stmt2BindAndSubmit
            if (stbInfo->iface != STMT2_IFACE) {
                // no stmt2 exec
                startTs = toolsGetTimestampUs();
                int code = execInsert(pThreadInfo, generated, &delay3);
                if (code) {
                    if (NO_IF_FAILED == stbInfo->continueIfFail) {
                        warnPrint("The super table parameter "
                                "continueIfFail: %d, STOP insertion!\n",
                                stbInfo->continueIfFail);
                        g_fail = true;
                        goto free_of_progressive;
                    } else if (YES_IF_FAILED == stbInfo->continueIfFail) {
                        infoPrint("The super table parameter "
                                "continueIfFail: %d, "
                                "will continue to insert ..\n",
                                stbInfo->continueIfFail);
                    } else if (smart) {
                        warnPrint("The super table parameter "
                                "continueIfFail: %d, will create table "
                                "then insert ..\n",
                                stbInfo->continueIfFail);

                        // generator
                        if (w == 0) {
                            if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile, NULL, index)) {
                                g_fail = true;
                                goto free_of_progressive;
                            }
                        }

                        code = smartContinueIfFail(
                                pThreadInfo,
                                childTbl, tagData, w, ttl);
                        if (0 != code) {
                            g_fail = true;
                            goto free_of_progressive;
                        }

                        // move next
                        if (++w >= TAG_BATCH_COUNT) {
                            // reset for gen again
                            w = 0;
                            index += TAG_BATCH_COUNT;
                        }

                        code = execInsert(pThreadInfo, generated, &delay3);
                        if (code) {
                            g_fail = true;
                            goto free_of_progressive;
                        }
                    } else {
                        warnPrint("Unknown super table parameter "
                                "continueIfFail: %d\n",
                                stbInfo->continueIfFail);
                        g_fail = true;
                        goto free_of_progressive;
                    }
                }
                endTs = toolsGetTimestampUs() + 1;
            }

            if (stbInfo->insert_interval > 0) {
                debugPrint("%s() LN%d, insert_interval: %"PRIu64"\n",
                          __func__, __LINE__, stbInfo->insert_interval);
                perfPrint("sleep %" PRIu64 " ms\n",
                              stbInfo->insert_interval);
                toolsMsleep((int32_t)stbInfo->insert_interval);
            }

            // flush
            if (database->flush) {
                char sql[260] = "";
                sprintf(sql, "flush database %s", database->dbName);
                int32_t code = executeSql(pThreadInfo->conn->taos,sql);
                if (code != 0) {
                  perfPrint(" %s failed. error code = 0x%x\n", sql, code);
                } else {
                   perfPrint(" %s ok.\n", sql);
                }
            }

            pThreadInfo->totalInsertRows += generated;

            if (g_arguments->terminate) {
                goto free_of_progressive;
            }
            int protocol = stbInfo->lineProtocol;
            switch (stbInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE:
                    memset(pThreadInfo->buffer, 0, pThreadInfo->max_sql_len);
                    break;
                case SML_REST_IFACE:
                    memset(pThreadInfo->buffer, 0,
                           g_arguments->reqPerReq *
                               (pThreadInfo->max_sql_len + 1));
                case SML_IFACE:
                    if (TSDB_SML_JSON_PROTOCOL == protocol) {
                        memset(pThreadInfo->lines[0], 0,
                           pThreadInfo->line_buf_len);
                    } else if (SML_JSON_TAOS_FORMAT == protocol) {
                        if (pThreadInfo->lines && pThreadInfo->lines[0]) {
                            tmfree(pThreadInfo->lines[0]);
                            pThreadInfo->lines[0] = NULL;
                        }
                        if (pThreadInfo->json_array) {
                            tools_cJSON_Delete(pThreadInfo->json_array);
                            pThreadInfo->json_array = NULL;
                        }
                        pThreadInfo->json_array = tools_cJSON_CreateArray();
                    } else {
                        for (int j = 0; j < generated; j++) {
                            debugPrint("pThreadInfo->lines[%d]: %s\n",
                                       j, pThreadInfo->lines[j]);
                            memset(pThreadInfo->lines[j], 0,
                                   pThreadInfo->max_sql_len);
                        }
                    }
                    break;
                case STMT_IFACE:
                    break;
            }

            int64_t delay4 = endTs - startTs;
            int64_t delay = delay1 + delay2 + delay3 + delay4;
            if (delay <= 0) {
                debugPrint("thread[%d]: startTs: %"PRId64", endTs: %"PRId64"\n",
                        pThreadInfo->threadID, startTs, endTs);
            } else {
                perfPrint("insert execution time is %.6f s\n",
                              delay / 1E6);

                int64_t * pDelay = benchCalloc(1, sizeof(int64_t), false);
                *pDelay = delay;
                if (benchArrayPush(pThreadInfo->delayList, pDelay) == NULL) {
                    tmfree(pDelay);
                }
                pThreadInfo->totalDelay += delay;
                pThreadInfo->totalDelay1 += delay1;
                pThreadInfo->totalDelay2 += delay2;
                pThreadInfo->totalDelay3 += delay3;
            }
            delay1 = delay2 = delay3 = 0;

            int64_t currentPrintTime = toolsGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                infoPrint(
                        "thread[%d] has currently inserted rows: "
                        "%" PRId64 ", peroid insert rate: %.3f rows/s \n",
                        pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                        (double)(pThreadInfo->totalInsertRows - lastTotalInsertRows) * 1000.0/(currentPrintTime - lastPrintTime));
                lastPrintTime = currentPrintTime;
                lastTotalInsertRows = pThreadInfo->totalInsertRows;
            }
            if (i >= stbInfo->insertRows) {
                break;
            }
        }  // insertRows
    }      // tableSeq
free_of_progressive:
    cleanupAndPrint(pThreadInfo, "progressive");
    if(csvFile) {
        fclose(csvFile);
    }
    tmfree(tagData);
    return NULL;
}

uint64_t strToTimestamp(char * tsStr) {
    uint64_t ts = 0;
    // remove double quota mark
    if (tsStr[0] == '\"' || tsStr[0] == '\'') {
        tsStr += 1;
        int32_t last = strlen(tsStr) - 1;
        if (tsStr[last] == '\"' || tsStr[0] == '\'') {
            tsStr[last] = 0;
        }
    }

    if (toolsParseTime(tsStr, (int64_t*)&ts, strlen(tsStr), TSDB_TIME_PRECISION_MILLI, 0)) {
        // not timestamp str format, maybe int64 format
        ts = (int64_t)atol(tsStr);
    }

    return ts;
}

static int initStmtDataValue(SSuperTable *stbInfo, SChildTable *childTbl, uint64_t *bind_ts_array) {
    int32_t columnCount = stbInfo->cols->size;

    char *sampleDataBuf;
    if (childTbl) {
        sampleDataBuf = childTbl->sampleDataBuf;
    } else {
        sampleDataBuf = stbInfo->sampleDataBuf;
    }
    int64_t lenOfOneRow = stbInfo->lenOfCols;

    if (stbInfo->useSampleTs) {
        columnCount += 1;  // for skipping first column
    }
    for (int i=0; i < g_arguments->prepared_rand; i++) {
        int cursor = 0;

        for (int c = 0; c < columnCount; c++) {
            char *restStr = sampleDataBuf
                + lenOfOneRow * i + cursor;
            int lengthOfRest = strlen(restStr);

            int index = 0;
            for (index = 0; index < lengthOfRest; index++) {
                if (restStr[index] == ',') {
                    break;
                }
            }

            cursor += index + 1;  // skip ',' too

            char *tmpStr = calloc(1, index + 1);
            if (NULL == tmpStr) {
                errorPrint("%s() LN%d, Failed to allocate %d bind buffer\n",
                        __func__, __LINE__, index + 1);
                return -1;
            }

            strncpy(tmpStr, restStr, index);
            if ((0 == c) && stbInfo->useSampleTs) {
                // set ts to
                bind_ts_array[i] = strToTimestamp(tmpStr);
                free(tmpStr);
                continue;
            }

            Field *col = benchArrayGet(stbInfo->cols,
                    (stbInfo->useSampleTs?c-1:c));
            char dataType = col->type;

            StmtData *stmtData;
            if (childTbl) {
                ChildField *childCol =
                    benchArrayGet(childTbl->childCols,
                                  (stbInfo->useSampleTs?c-1:c));
                stmtData = &childCol->stmtData;
            } else {
                stmtData = &col->stmtData;
            }

            // set value
            stmtData->is_null[i] = 0;
            stmtData->lengths[i] = col->length;

            if (0 == strcmp(tmpStr, "NULL")) {
                *(stmtData->is_null + i) = true;
            } else {
                switch (dataType) {
                    case TSDB_DATA_TYPE_INT:
                        *((int32_t*)stmtData->data + i) = atoi(tmpStr);
                        break;
                    case TSDB_DATA_TYPE_UINT:
                        *((uint32_t*)stmtData->data + i) = (uint32_t)strtoul(tmpStr, NULL, 10);
                        break;
                    case TSDB_DATA_TYPE_FLOAT:
                        *((float*)stmtData->data +i) = (float)atof(tmpStr);
                        break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        *((double*)stmtData->data + i) = atof(tmpStr);
                        break;
                    case TSDB_DATA_TYPE_TINYINT:
                    case TSDB_DATA_TYPE_UTINYINT:
                        *((int8_t*)stmtData->data + i) = (int8_t)atoi(tmpStr);
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                    case TSDB_DATA_TYPE_USMALLINT:
                        *((int16_t*)stmtData->data + i) = (int16_t)atoi(tmpStr);
                        break;
                    case TSDB_DATA_TYPE_BIGINT:
                        *((int64_t*)stmtData->data + i) = (int64_t)strtoll(tmpStr, NULL, 10);
                        break;
                    case TSDB_DATA_TYPE_UBIGINT:
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        *((uint64_t*)stmtData->data + i) = (uint64_t)strtoull(tmpStr, NULL, 10);
                        break;
                    case TSDB_DATA_TYPE_BOOL:
                        *((int8_t*)stmtData->data + i) = (int8_t)atoi(tmpStr);
                        break;
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_NCHAR:
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:
                        {
                            size_t tmpLen = strlen(tmpStr);
                            debugPrint("%s() LN%d, index: %d, "
                                    "tmpStr len: %"PRIu64", col->length: %d\n",
                                    __func__, __LINE__,
                                    i, (uint64_t)tmpLen, col->length);
                            if (tmpLen-2 > col->length) {
                                errorPrint("data length %"PRIu64" "
                                        "is larger than column length %d\n",
                                        (uint64_t)tmpLen, col->length);
                            }
                            if (tmpLen > 2) {
                                strncpy((char *)stmtData->data
                                            + i * col->length,
                                        tmpStr+1,
                                        min(col->length, tmpLen - 2));
                            } else {
                                strncpy((char *)stmtData->data
                                            + i*col->length,
                                        "", 1);
                            }
                        }
                        break;
                    case TSDB_DATA_TYPE_DECIMAL:
                    case TSDB_DATA_TYPE_DECIMAL64:
                        errorPrint("Not implemented data type in func initStmtDataValue: %s\n",
                                convertDatatypeToString(dataType));
                        exit(EXIT_FAILURE);
                    case TSDB_DATA_TYPE_BLOB: {
                        size_t tmpLen = strlen(tmpStr);
                        debugPrint(
                            "%s() LN%d, index: %d, "
                            "tmpStr len: %" PRIu64 ", col->length: %d\n",
                            __func__, __LINE__, i, (uint64_t)tmpLen, col->length);
                        if (tmpLen - 2 > col->length) {
                                errorPrint("data length %" PRIu64
                                           " "
                                           "is larger than column length %d\n",
                                           (uint64_t)tmpLen, col->length);
                        }
                        if (tmpLen > 2) {
                                strncpy((char *)stmtData->data + i * col->length, tmpStr + 1,
                                        min(col->length, tmpLen - 2));
                        } else {
                                strncpy((char *)stmtData->data + i * col->length, "", 1);
                        }
                        break;
                    }

                    default:
                        break;
                }
            }
            free(tmpStr);
        }
    }
    return 0;
}

static void initStmtData(char dataType, void **data, uint32_t length) {
    char *tmpP = NULL;

    switch (dataType) {
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_UINT:
            tmpP = calloc(1, sizeof(int) * g_arguments->prepared_rand);
            assert(tmpP);
            tmfree(*data);
            *data = (void*)tmpP;
            break;

        case TSDB_DATA_TYPE_TINYINT:
        case TSDB_DATA_TYPE_UTINYINT:
            tmpP = calloc(1, sizeof(int8_t) * g_arguments->prepared_rand);
            assert(tmpP);
            tmfree(*data);
            *data = (void*)tmpP;
            break;

        case TSDB_DATA_TYPE_SMALLINT:
        case TSDB_DATA_TYPE_USMALLINT:
            tmpP = calloc(1, sizeof(int16_t) * g_arguments->prepared_rand);
            assert(tmpP);
            tmfree(*data);
            *data = (void*)tmpP;
            break;

        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_UBIGINT:
            tmpP = calloc(1, sizeof(int64_t) * g_arguments->prepared_rand);
            assert(tmpP);
            tmfree(*data);
            *data = (void*)tmpP;
            break;

        case TSDB_DATA_TYPE_BOOL:
            tmpP = calloc(1, sizeof(int8_t) * g_arguments->prepared_rand);
            assert(tmpP);
            tmfree(*data);
            *data = (void*)tmpP;
            break;

        case TSDB_DATA_TYPE_FLOAT:
            tmpP = calloc(1, sizeof(float) * g_arguments->prepared_rand);
            assert(tmpP);
            tmfree(*data);
            *data = (void*)tmpP;
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            tmpP = calloc(1, sizeof(double) * g_arguments->prepared_rand);
            assert(tmpP);
            tmfree(*data);
            *data = (void*)tmpP;
            break;

        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_GEOMETRY:
            tmpP = calloc(1, g_arguments->prepared_rand * length);
            assert(tmpP);
            tmfree(*data);
            *data = (void*)tmpP;
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            tmpP = calloc(1, sizeof(int64_t) * g_arguments->prepared_rand);
            assert(tmpP);
            tmfree(*data);
            *data = (void*)tmpP;
            break;

        case TSDB_DATA_TYPE_DECIMAL:
        case TSDB_DATA_TYPE_DECIMAL64:
            errorPrint("Not implemented data type in func initStmtData: %s\n",
                       convertDatatypeToString(dataType));
            exit(EXIT_FAILURE);

        case TSDB_DATA_TYPE_BLOB: {
            tmpP = calloc(1, g_arguments->prepared_rand * length);
            assert(tmpP);
            tmfree(*data);
            *data = (void *)tmpP;
            break;
        }
        default:

            errorPrint("Unknown data type on initStmtData: %s\n",
                       convertDatatypeToString(dataType));
            exit(EXIT_FAILURE);
    }
}

static int parseBufferToStmtBatchChildTbl(SSuperTable *stbInfo,
                                          SChildTable* childTbl, uint64_t *bind_ts_array) {
    int32_t columnCount = stbInfo->cols->size;

    for (int c = 0; c < columnCount; c++) {
        Field *col = benchArrayGet(stbInfo->cols, c);
        ChildField *childCol = benchArrayGet(childTbl->childCols, c);
        char dataType = col->type;

        // malloc memory
        tmfree(childCol->stmtData.is_null);
        tmfree(childCol->stmtData.lengths);
        childCol->stmtData.is_null = benchCalloc(sizeof(char),     g_arguments->prepared_rand, true);
        childCol->stmtData.lengths = benchCalloc(sizeof(int32_t),  g_arguments->prepared_rand, true);

        initStmtData(dataType, &(childCol->stmtData.data), col->length);
    }

    return initStmtDataValue(stbInfo, childTbl, bind_ts_array);
}

static int parseBufferToStmtBatch(SSuperTable* stbInfo, uint64_t *bind_ts_array) {
    int32_t columnCount = stbInfo->cols->size;

    for (int c = 0; c < columnCount; c++) {
        Field *col = benchArrayGet(stbInfo->cols, c);

        //remalloc element count is g_arguments->prepared_rand buffer
        tmfree(col->stmtData.is_null);
        col->stmtData.is_null = benchCalloc(sizeof(char), g_arguments->prepared_rand, false);
        tmfree(col->stmtData.lengths);
        col->stmtData.lengths = benchCalloc(sizeof(int32_t), g_arguments->prepared_rand, false);

        initStmtData(col->type, &(col->stmtData.data), col->length);
    }

    return initStmtDataValue(stbInfo, NULL, bind_ts_array);
}

static int64_t fillChildTblNameByCount(SSuperTable *stbInfo) {
    if (stbInfo->useTagTableName) {
        return stbInfo->childTblCount;
    }

    for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
        char childName[TSDB_TABLE_NAME_LEN]={0};
        snprintf(childName,
                 TSDB_TABLE_NAME_LEN,
                 "%s%" PRIu64,
                 stbInfo->childTblPrefix, i);
        stbInfo->childTblArray[i]->name = strdup(childName);
        debugPrint("%s(): %s\n", __func__,
                  stbInfo->childTblArray[i]->name);
    }

    return stbInfo->childTblCount;
}

static int64_t fillChildTblNameByFromTo(SDataBase *database,
        SSuperTable* stbInfo) {
    for (int64_t i = stbInfo->childTblFrom; i <= stbInfo->childTblTo; i++) {
        char childName[TSDB_TABLE_NAME_LEN]={0};
        snprintf(childName,
                TSDB_TABLE_NAME_LEN,
                "%s%" PRIu64,
                stbInfo->childTblPrefix, i);
        stbInfo->childTblArray[i]->name = strdup(childName);
    }

    return (stbInfo->childTblTo-stbInfo->childTblFrom);
}

static int64_t fillChildTblNameByLimitOffset(SDataBase *database,
        SSuperTable* stbInfo) {
    SBenchConn* conn = initBenchConn(database->dbName);
    if (NULL == conn) {
        return -1;
    }
    char cmd[SHORT_1K_SQL_BUFF_LEN] = "\0";
    if (g_arguments->taosc_version == 3) {
        snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
                 "SELECT DISTINCT(TBNAME) FROM %s.`%s` LIMIT %" PRId64
                 " OFFSET %" PRIu64,
                 database->dbName, stbInfo->stbName, stbInfo->childTblLimit,
                 stbInfo->childTblOffset);
    } else {
        snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
                 "SELECT TBNAME FROM %s.`%s` LIMIT %" PRId64
                 " OFFSET %" PRIu64,
                 database->dbName, stbInfo->stbName, stbInfo->childTblLimit,
                 stbInfo->childTblOffset);
    }
    debugPrint("cmd: %s\n", cmd);
    TAOS_RES *res = taos_query(conn->taos, cmd);
    int32_t   code = taos_errno(res);
    int64_t   count = 0;
    if (code) {
        printErrCmdCodeStr(cmd, code, res);
        closeBenchConn(conn);
        return -1;
    }
    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(res)) != NULL) {
        int *lengths = taos_fetch_lengths(res);
        char * childName = benchCalloc(1, lengths[0] + 1, true);
        strncpy(childName, row[0], lengths[0]);
        childName[lengths[0]] = '\0';
        stbInfo->childTblArray[count]->name = childName;
        debugPrint("stbInfo->childTblArray[%" PRId64 "]->name: %s\n",
                   count, stbInfo->childTblArray[count]->name);
        count++;
    }
    taos_free_result(res);
    closeBenchConn(conn);
    return count;
}

static void preProcessArgument(SSuperTable *stbInfo) {
    if (stbInfo->interlaceRows > g_arguments->reqPerReq) {
        infoPrint(
            "interlaceRows(%d) is larger than record per request(%u), which "
            "will be set to %u\n",
            stbInfo->interlaceRows, g_arguments->reqPerReq,
            g_arguments->reqPerReq);
        stbInfo->interlaceRows = g_arguments->reqPerReq;
    }

    if (stbInfo->interlaceRows > stbInfo->insertRows) {
        infoPrint(
                "interlaceRows larger than insertRows %d > %" PRId64 "\n",
                stbInfo->interlaceRows, stbInfo->insertRows);
        infoPrint("%s", "interlaceRows will be set to 0\n");
        stbInfo->interlaceRows = 0;
    }

    if (stbInfo->interlaceRows == 0
            && g_arguments->reqPerReq > stbInfo->insertRows) {
        infoPrint("record per request (%u) is larger than "
                "insert rows (%"PRIu64")"
                " in progressive mode, which will be set to %"PRIu64"\n",
                g_arguments->reqPerReq, stbInfo->insertRows,
                stbInfo->insertRows);
        g_arguments->reqPerReq = stbInfo->insertRows;
    }

    if (stbInfo->interlaceRows > 0 && stbInfo->iface == STMT_IFACE
            && stbInfo->autoTblCreating) {
        errorPrint("%s","stmt not support autocreate table with interlace row , quit programe!\n");
        exit(-1);
    }
}

static int printTotalDelay(SDataBase *database,
                           int64_t totalDelay,
                           int64_t totalDelay1,
                           int64_t totalDelay2,
                           int64_t totalDelay3,
                           BArray *total_delay_list,
                            int threads,
                            int64_t totalInsertRows,
                            int64_t spend) {
    // zero check
    if (total_delay_list->size == 0 || spend == 0 || threads == 0) {
        return -1;
    }

    char subDelay[128] = "";
    if(totalDelay1 + totalDelay2 + totalDelay3 > 0) {
        sprintf(subDelay, " stmt delay1=%.2fs delay2=%.2fs delay3=%.2fs",
                totalDelay1/threads/1E6,
                totalDelay2/threads/1E6,
                totalDelay3/threads/1E6);
    }

    double time_cost = spend / 1E6;
    double real_time_cost = totalDelay/threads/1E6;
    double records_per_second = (double)(totalInsertRows / (spend/1E6));
    double real_records_per_second = (double)(totalInsertRows / (totalDelay/threads/1E6));

    succPrint("Spent %.6f (real %.6f) seconds to insert rows: %" PRIu64
              " with %d thread(s) into %s %.2f (real %.2f) records/second%s\n",
              time_cost, real_time_cost, totalInsertRows, threads,
              database->dbName, records_per_second,
              real_records_per_second, subDelay);

    if (!total_delay_list->size) {
        return -1;
    }

    double minDelay = *(int64_t *)(benchArrayGet(total_delay_list, 0))/1E3;
    double avgDelay = (double)totalDelay/total_delay_list->size/1E3;
    double p90 = *(int64_t *)(benchArrayGet(total_delay_list,
                                         (int32_t)(total_delay_list->size
                                         * 0.9)))/1E3;
    double p95 = *(int64_t *)(benchArrayGet(total_delay_list,
                                         (int32_t)(total_delay_list->size
                                         * 0.95)))/1E3;
    double p99 = *(int64_t *)(benchArrayGet(total_delay_list,
                                         (int32_t)(total_delay_list->size
                                         * 0.99)))/1E3;
    double maxDelay = *(int64_t *)(benchArrayGet(total_delay_list,
                                         (int32_t)(total_delay_list->size
                                         - 1)))/1E3;

    succPrint("insert delay, "
              "min: %.4fms, "
              "avg: %.4fms, "
              "p90: %.4fms, "
              "p95: %.4fms, "
              "p99: %.4fms, "
              "max: %.4fms\n",
            minDelay, avgDelay, p90, p95, p99, maxDelay);

    if (g_arguments->output_json_file) {
        tools_cJSON *root = tools_cJSON_CreateObject();
        if (root) {
            tools_cJSON_AddStringToObject(root, "db_name", database->dbName);
            tools_cJSON_AddNumberToObject(root, "inserted_rows", totalInsertRows);
            tools_cJSON_AddNumberToObject(root, "threads", threads);
            tools_cJSON_AddNumberToObject(root, "time_cost", time_cost);
            tools_cJSON_AddNumberToObject(root, "real_time_cost", real_time_cost);
            tools_cJSON_AddNumberToObject(root, "records_per_second",  records_per_second);
            tools_cJSON_AddNumberToObject(root, "real_records_per_second", real_records_per_second);

            tools_cJSON_AddNumberToObject(root, "avg", avgDelay);
            tools_cJSON_AddNumberToObject(root, "min", minDelay);
            tools_cJSON_AddNumberToObject(root, "max", maxDelay);
            tools_cJSON_AddNumberToObject(root, "p90", p90);
            tools_cJSON_AddNumberToObject(root, "p95", p95);
            tools_cJSON_AddNumberToObject(root, "p99", p99);

            char *jsonStr = tools_cJSON_PrintUnformatted(root);
            if (jsonStr) {
                FILE *fp = fopen(g_arguments->output_json_file, "w");
                if (fp) {
                    fprintf(fp, "%s\n", jsonStr);
                    fclose(fp);
                } else {
                    errorPrint("Failed to open output JSON file, file name %s\n",
                            g_arguments->output_json_file);
                }
                free(jsonStr);
            }
            tools_cJSON_Delete(root);
        }
    }
    return 0;
}

static int64_t fillChildTblNameImp(SDataBase *database, SSuperTable *stbInfo) {
    int64_t ntables;
    if (stbInfo->childTblLimit) {
        ntables = fillChildTblNameByLimitOffset(database, stbInfo);
    } else if (stbInfo->childTblFrom || stbInfo->childTblTo) {
        ntables = fillChildTblNameByFromTo(database, stbInfo);
    } else {
        ntables = fillChildTblNameByCount(stbInfo);
    }
    return ntables;
}

static int64_t fillChildTblName(SDataBase *database, SSuperTable *stbInfo) {
    int64_t ntables = stbInfo->childTblCount;
    stbInfo->childTblArray = benchCalloc(stbInfo->childTblCount,
            sizeof(SChildTable*), true);
    for (int64_t child = 0; child < stbInfo->childTblCount; child++) {
        stbInfo->childTblArray[child] =
            benchCalloc(1, sizeof(SChildTable), true);
    }

    if (stbInfo->childTblCount == 1 && stbInfo->tags->size == 0) {
        // Normal table
        char childName[TSDB_TABLE_NAME_LEN]={0};
        snprintf(childName, TSDB_TABLE_NAME_LEN,
                    "%s", stbInfo->stbName);
        stbInfo->childTblArray[0]->name = strdup(childName);
    } else if ((stbInfo->iface != SML_IFACE
        && stbInfo->iface != SML_REST_IFACE)
            && stbInfo->childTblExists) {
        ntables = fillChildTblNameImp(database, stbInfo);
    } else {
        ntables = fillChildTblNameByCount(stbInfo);
    }

    return ntables;
}

// last ts fill to filllBackTime
static bool fillSTableLastTs(SDataBase *database, SSuperTable *stbInfo) {
    SBenchConn* conn = initBenchConn(database->dbName);
    if (NULL == conn) {
        return false;
    }
    char cmd[SHORT_1K_SQL_BUFF_LEN] = "\0";
    snprintf(cmd, SHORT_1K_SQL_BUFF_LEN, "select last(ts) from %s.`%s`", database->dbName, stbInfo->stbName);

    infoPrint("fillBackTime: %s\n", cmd);
    TAOS_RES *res = taos_query(conn->taos, cmd);
    int32_t   code = taos_errno(res);
    if (code) {
        printErrCmdCodeStr(cmd, code, res);
        closeBenchConn(conn);
        return false;
    }

    TAOS_ROW row = taos_fetch_row(res);
    if(row == NULL) {
        taos_free_result(res);
        closeBenchConn(conn);
        return false;
    }

    char lastTs[128];
    memset(lastTs, 0, sizeof(lastTs));

    stbInfo->startFillbackTime = *(int64_t*)row[0];
    toolsFormatTimestamp(lastTs, stbInfo->startFillbackTime, database->precision);
    infoPrint("fillBackTime: get ok %s.%s last ts=%s \n", database->dbName, stbInfo->stbName, lastTs);

    taos_free_result(res);
    closeBenchConn(conn);

    return true;
}

// calcNow expression fill to timestamp_start
static bool calcExprFromServer(SDataBase *database, SSuperTable *stbInfo) {
    SBenchConn* conn = initBenchConn(database->dbName);
    if (NULL == conn) {
        return false;
    }
    char cmd[SHORT_1K_SQL_BUFF_LEN] = "\0";
    snprintf(cmd, SHORT_1K_SQL_BUFF_LEN, "select %s", stbInfo->calcNow);

    infoPrint("calcExprFromServer: %s\n", cmd);
    TAOS_RES *res = taos_query(conn->taos, cmd);
    int32_t   code = taos_errno(res);
    if (code) {
        printErrCmdCodeStr(cmd, code, res);
        closeBenchConn(conn);
        return false;
    }

    TAOS_ROW row = taos_fetch_row(res);
    if(row == NULL) {
        taos_free_result(res);
        closeBenchConn(conn);
        return false;
    }

    char ts[128];
    memset(ts, 0, sizeof(ts));

    stbInfo->startTimestamp = *(int64_t*)row[0];
    toolsFormatTimestamp(ts, stbInfo->startTimestamp, database->precision);
    infoPrint("calcExprFromServer: get ok.  %s = %s \n", stbInfo->calcNow, ts);

    taos_free_result(res);
    closeBenchConn(conn);

    return true;
}

int64_t obtainTableCount(SDataBase* database, SSuperTable* stbInfo) {
    // ntable calc
    int64_t ntables;
    if (stbInfo->childTblTo > 0) {
        ntables = stbInfo->childTblTo - stbInfo->childTblFrom;
    } else if (stbInfo->childTblLimit > 0 && stbInfo->childTblExists) {
        ntables = stbInfo->childTblLimit;
    } else {
        ntables = stbInfo->childTblCount;
    }

    return ntables;
}

// assign table to thread with vgroups, return assign thread count
int32_t assignTableToThread(SDataBase* database, SSuperTable* stbInfo) {
    int32_t threads = 0;

    // calc table count per vgroup
    for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
        int32_t vgIdx = calcGroupIndex(database->dbName, stbInfo->childTblArray[i]->name, database->vgroups);
        if (vgIdx == -1) {
            continue;
        }
        SVGroup *vg = benchArrayGet(database->vgArray, vgIdx);
        vg->tbCountPerVgId ++;
    }

    // malloc vg->childTblArray memory with table count
    for (int v = 0; v < database->vgroups; v++) {
        SVGroup *vg = benchArrayGet(database->vgArray, v);
        infoPrint("Local hash calc %"PRId64" tables on %s's vgroup %d (id: %d)\n",
                    vg->tbCountPerVgId, database->dbName, v, vg->vgId);
        if (vg->tbCountPerVgId) {
            threads++;
        } else {
            continue;
        }
        vg->childTblArray = benchCalloc(vg->tbCountPerVgId, sizeof(SChildTable*), true);
        vg->tbOffset      = 0;
    }

    // set vg->childTblArray data
    for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
        int32_t vgIdx = calcGroupIndex(database->dbName, stbInfo->childTblArray[i]->name, database->vgroups);
        if (vgIdx == -1) {
            continue;
        }
        SVGroup *vg = benchArrayGet(database->vgArray, vgIdx);
        debugPrint("calc table hash to vgroup %s.%s vgIdx=%d\n",
                    database->dbName,
                    stbInfo->childTblArray[i]->name, vgIdx);
        vg->childTblArray[vg->tbOffset] = stbInfo->childTblArray[i];
        vg->tbOffset++;
    }
    return threads;
}

// init stmt
TAOS_STMT* initStmt(TAOS* taos, bool single) {
    if (!single) {
        infoPrint("initStmt call taos_stmt_init single=%d\n", single);
        return taos_stmt_init(taos);
    }

    TAOS_STMT_OPTIONS op;
    memset(&op, 0, sizeof(op));
    op.singleStbInsert      = single;
    op.singleTableBindOnce  = single;
    infoPrint("initStmt call taos_stmt_init_with_options single=%d\n", single);
    return taos_stmt_init_with_options(taos, &op);
}

// init stmt2
TAOS_STMT2* initStmt2(TAOS* taos, bool single) {
    TAOS_STMT2_OPTION op2;
    memset(&op2, 0, sizeof(op2));
    op2.singleStbInsert      = single;
    op2.singleTableBindOnce  = single;

    TAOS_STMT2* stmt2 = taos_stmt2_init(taos, &op2);
    if (stmt2)
        succPrint("succ  taos_stmt2_init single=%d\n", single);
    else
        errorPrint("failed taos_stmt2_init single=%d\n", single);
    return stmt2;
}

// init insert thread
void initTsArray(uint64_t *bind_ts_array, SSuperTable* stbInfo) {
    parseBufferToStmtBatch(stbInfo, bind_ts_array);
    for (int64_t child = 0; child < stbInfo->childTblCount; child++) {
        SChildTable *childTbl = stbInfo->childTblArray[child];
        if (childTbl->useOwnSample) {
            parseBufferToStmtBatchChildTbl(stbInfo, childTbl, bind_ts_array);
        }
    }

}

void *genInsertTheadInfo(void* arg) {

    if (g_arguments->terminate || g_fail) {
        return NULL;
    }

    threadInfo* pThreadInfo = (threadInfo*)arg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    pThreadInfo->delayList = benchArrayInit(1, sizeof(int64_t));
    switch (stbInfo->iface) {
        // rest
        case REST_IFACE: {
            if (stbInfo->interlaceRows > 0) {
                pThreadInfo->buffer = new_ds(0);
            } else {
                pThreadInfo->buffer = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
            }
            int sockfd = createSockFd();
            if (sockfd < 0) {
                g_fail = true;
                goto END;
            }
            pThreadInfo->sockfd = sockfd;
            break;
        }
        // stmt & stmt2 init
        case STMT_IFACE:
        case STMT2_IFACE: {
            pThreadInfo->conn = initBenchConn(pThreadInfo->dbInfo->dbName);
            if (NULL == pThreadInfo->conn) {
                goto END;
            }
            // single always true for benchmark
            bool single = true;
            if (stbInfo->iface == STMT2_IFACE) {
                // stmt2 init
                if (pThreadInfo->conn->stmt2)
                    taos_stmt2_close(pThreadInfo->conn->stmt2);
                pThreadInfo->conn->stmt2 = initStmt2(pThreadInfo->conn->taos, single);
                if (NULL == pThreadInfo->conn->stmt2) {
                    errorPrint("taos_stmt2_init() failed, reason: %s\n", taos_errstr(NULL));
                    g_fail = true;
                    goto END;
                }
            } else {
                // stmt init
                if (pThreadInfo->conn->stmt)
                    taos_stmt_close(pThreadInfo->conn->stmt);
                pThreadInfo->conn->stmt = initStmt(pThreadInfo->conn->taos, single);
                if (NULL == pThreadInfo->conn->stmt) {
                    errorPrint("taos_stmt_init() failed, reason: %s\n", taos_errstr(NULL));
                    g_fail = true;
                    goto END;
                }
            }

            // select db
            if (taos_select_db(pThreadInfo->conn->taos, pThreadInfo->dbInfo->dbName)) {
                errorPrint("taos select database(%s) failed\n", pThreadInfo->dbInfo->dbName);
                g_fail = true;
                goto END;
            }

            // malloc bind
            int32_t unit = stbInfo->iface == STMT2_IFACE ? sizeof(TAOS_STMT2_BIND) : sizeof(TAOS_MULTI_BIND);
            pThreadInfo->bind_ts       = benchCalloc(1, sizeof(int64_t), true);

            pThreadInfo->bindParams    = benchCalloc(1, unit * (stbInfo->cols->size + 1), true);
            // have ts columns, so size + 1
            pThreadInfo->lengths       = benchCalloc(stbInfo->cols->size + 1, sizeof(int32_t*), true);
            for(int32_t c = 0; c <= stbInfo->cols->size; c++) {
                pThreadInfo->lengths[c] = benchCalloc(g_arguments->reqPerReq, sizeof(int32_t), true);
            }
            // tags data
            pThreadInfo->tagsStmt = copyBArray(stbInfo->tags);
            for(int32_t n = 0; n < pThreadInfo->tagsStmt->size; n ++ ) {
                Field *field = benchArrayGet(pThreadInfo->tagsStmt, n);
                memset(&field->stmtData, 0, sizeof(StmtData));
            }

            break;
        }
        // sml rest
        case SML_REST_IFACE: {
            int sockfd = createSockFd();
            if (sockfd < 0) {
                g_fail = true;
                goto END;
            }
            pThreadInfo->sockfd = sockfd;
        }
        // sml
        case SML_IFACE: {
            if (stbInfo->iface == SML_IFACE) {
                pThreadInfo->conn = initBenchConn(pThreadInfo->dbInfo->dbName);
                if (pThreadInfo->conn == NULL) {
                    errorPrint("%s() init connection failed\n", __func__);
                    g_fail = true;
                    goto END;
                }
                if (taos_select_db(pThreadInfo->conn->taos, pThreadInfo->dbInfo->dbName)) {
                    errorPrint("taos select database(%s) failed\n", pThreadInfo->dbInfo->dbName);
                    g_fail = true;
                    goto END;
                }
            }
            pThreadInfo->max_sql_len = stbInfo->lenOfCols + stbInfo->lenOfTags;
            if (stbInfo->iface == SML_REST_IFACE) {
                pThreadInfo->buffer = benchCalloc(1, g_arguments->reqPerReq * (1 + pThreadInfo->max_sql_len), true);
            }
            int protocol = stbInfo->lineProtocol;
            if (TSDB_SML_JSON_PROTOCOL != protocol && SML_JSON_TAOS_FORMAT != protocol) {
                pThreadInfo->sml_tags = (char **)benchCalloc(pThreadInfo->ntables, sizeof(char *), true);
                for (int t = 0; t < pThreadInfo->ntables; t++) {
                    pThreadInfo->sml_tags[t] = benchCalloc(1, stbInfo->lenOfTags, true);
                }
                int64_t index = pThreadInfo->start_table_from;
                for (int t = 0; t < pThreadInfo->ntables; t++) {
                    if (generateRandData(
                                stbInfo, pThreadInfo->sml_tags[t],
                                stbInfo->lenOfTags,
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                stbInfo->tags, 1, true, NULL, index++)) {
                        g_fail = true;
                        goto END;
                    }
                    debugPrint("pThreadInfo->sml_tags[%d]: %s\n", t,
                               pThreadInfo->sml_tags[t]);
                }
                pThreadInfo->lines = benchCalloc(g_arguments->reqPerReq, sizeof(char *), true);
                for (int j = 0; (j < g_arguments->reqPerReq && !g_arguments->terminate); j++) {
                    pThreadInfo->lines[j] = benchCalloc(1, pThreadInfo->max_sql_len, true);
                }
            } else {
                pThreadInfo->json_array          = tools_cJSON_CreateArray();
                pThreadInfo->sml_json_tags       = tools_cJSON_CreateArray();
                pThreadInfo->sml_tags_json_array = (char **)benchCalloc( pThreadInfo->ntables, sizeof(char *), true);
                for (int t = 0; t < pThreadInfo->ntables; t++) {
                    if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                        generateSmlJsonTags(
                            pThreadInfo->sml_json_tags,
                                pThreadInfo->sml_tags_json_array,
                                stbInfo,
                            pThreadInfo->start_table_from, t);
                    } else {
                        generateSmlTaosJsonTags(
                            pThreadInfo->sml_json_tags, stbInfo,
                            pThreadInfo->start_table_from, t);
                    }
                }
                pThreadInfo->lines = (char **)benchCalloc(1, sizeof(char *), true);
                if (0 == stbInfo->interlaceRows && TSDB_SML_JSON_PROTOCOL == protocol) {
                    pThreadInfo->line_buf_len = g_arguments->reqPerReq * accumulateRowLen(pThreadInfo->stbInfo->tags, pThreadInfo->stbInfo->iface);
                    debugPrint("%s() LN%d, line_buf_len=%d\n", __func__, __LINE__, pThreadInfo->line_buf_len);
                    pThreadInfo->lines[0]             = benchCalloc(1, pThreadInfo->line_buf_len, true);
                    pThreadInfo->sml_json_value_array = (char **)benchCalloc(pThreadInfo->ntables, sizeof(char *), true);
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        generateSmlJsonValues(pThreadInfo->sml_json_value_array, stbInfo, t);
                    }
                }
            }
            break;
        }
        // taos
        case TAOSC_IFACE: {
            pThreadInfo->conn = initBenchConn(pThreadInfo->dbInfo->dbName);
            if (pThreadInfo->conn == NULL) {
                errorPrint("%s() failed to connect\n", __func__);
                g_fail = true;
                goto END;
            }
            char* command = benchCalloc(1, SHORT_1K_SQL_BUFF_LEN, false);
            snprintf(command, SHORT_1K_SQL_BUFF_LEN,
                    g_arguments->escape_character ? "USE `%s`" : "USE %s",
                    pThreadInfo->dbInfo->dbName);
            if (queryDbExecCall(pThreadInfo->conn, command)) {
                errorPrint("taos select database(%s) failed\n", pThreadInfo->dbInfo->dbName);
                g_fail = true;
                goto END;
            }
            tmfree(command);
            command = NULL;

            if (stbInfo->interlaceRows > 0) {
                pThreadInfo->buffer = new_ds(0);
            } else {
                pThreadInfo->buffer = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
                if (g_arguments->check_sql) {
                    pThreadInfo->csql = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
                    memset(pThreadInfo->csql, 0, TSDB_MAX_ALLOWED_SQL_LEN);
                }
            }

            break;
        }
        default:
            break;
    }

END:
    return NULL;
}


// init insert thread
int32_t initInsertThread(SDataBase* database, SSuperTable* stbInfo, int32_t nthreads, threadInfo *infos, int64_t div, int64_t mod) {
    int32_t  ret     = -1;
    uint64_t tbNext  = stbInfo->childTblFrom;
    int32_t  vgNext  = 0;
    pthread_t   *pids  = benchCalloc(1, nthreads * sizeof(pthread_t),  true);
    int threadCnt = 0;
    uint64_t * bind_ts_array = NULL;
    if (STMT2_IFACE == stbInfo->iface || STMT_IFACE == stbInfo->iface) {
        bind_ts_array = benchCalloc(1, sizeof(int64_t)*g_arguments->prepared_rand, true);
        initTsArray(bind_ts_array, stbInfo);
    }


    for (int32_t i = 0; i < nthreads && !g_arguments->terminate; i++) {
        // set table
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID   = i;
        pThreadInfo->dbInfo     = database;
        pThreadInfo->stbInfo    = stbInfo;
        pThreadInfo->start_time = stbInfo->startTimestamp;
        pThreadInfo->pos        = 0;
        pThreadInfo->samplePos  = 0;
        pThreadInfo->totalInsertRows = 0;
        if (STMT2_IFACE == stbInfo->iface || STMT_IFACE == stbInfo->iface) {
            pThreadInfo->bind_ts_array = benchCalloc(1, sizeof(int64_t)*g_arguments->prepared_rand, true);
            memcpy(pThreadInfo->bind_ts_array, bind_ts_array, sizeof(int64_t)*g_arguments->prepared_rand);

        }
        if (g_arguments->bind_vgroup) {
            for (int32_t j = vgNext; j < database->vgroups; j++) {
                SVGroup *vg = benchArrayGet(database->vgArray, j);
                if (0 == vg->tbCountPerVgId) {
                    continue;
                }
                pThreadInfo->vg               = vg;
                pThreadInfo->ntables          = vg->tbCountPerVgId;
                pThreadInfo->start_table_from = 0;
                pThreadInfo->end_table_to     = vg->tbCountPerVgId - 1;
                vgNext                        = j + 1;
                break;
            }
        } else {
            pThreadInfo->start_table_from = tbNext;
            pThreadInfo->ntables          = i < mod ? div + 1 : div;
            pThreadInfo->end_table_to     = i < mod ? tbNext + div : tbNext + div - 1;
            tbNext                        = pThreadInfo->end_table_to + 1;
        }

        // init conn
        pthread_create(pids + i, NULL, genInsertTheadInfo,   pThreadInfo);
        threadCnt ++;
    }

    // wait threads
    for (int i = 0; i < threadCnt; i++) {
        infoPrint("init pthread_join %d ...\n", i);
        pthread_join(pids[i], NULL);
    }

    if (bind_ts_array) {
        tmfree(bind_ts_array);
    }

    tmfree(pids);
    if (g_fail) {
       return -1;
    }
    return 0;
}

#ifdef LINUX
#define EMPTY_SLOT -1
// run with limit thread
int32_t runInsertLimitThread(SDataBase* database, SSuperTable* stbInfo, int32_t nthreads, int32_t limitThread, threadInfo *infos, pthread_t *pids) {
    infoPrint("run with bind vgroups limit thread. limit threads=%d nthread=%d\n", limitThread, nthreads);

    // slots save threadInfo array index
    int32_t* slot = benchCalloc(limitThread, sizeof(int32_t), false);
    int32_t  t    = 0; // thread index
    for (int32_t i = 0; i < limitThread; i++) {
        slot[i] = EMPTY_SLOT;
    }

    while (!g_arguments->terminate) {
        int32_t emptySlot = 0;
        for (int32_t i = 0; i < limitThread; i++) {
            int32_t idx = slot[i];
            // check slot thread end
            if(idx != EMPTY_SLOT) {
                if (pthread_tryjoin_np(pids[idx], NULL) == EBUSY ) {
                    // thread is running
                    toolsMsleep(2000);
                } else {
                    // thread is end , set slot empty
                    infoPrint("slot[%d] finished tidx=%d. completed thread count=%d\n", i, slot[i], t);
                    slot[i] = EMPTY_SLOT;
                }
            }

            if (slot[i] == EMPTY_SLOT && t < nthreads) {
                // slot is empty , set new thread to running
                threadInfo *pThreadInfo = infos + t;
                if (stbInfo->interlaceRows > 0) {
                    pthread_create(pids + t, NULL, syncWriteInterlace,   pThreadInfo);
                } else {
                    pthread_create(pids + t, NULL, syncWriteProgressive, pThreadInfo);
                }

                // save current and move next
                slot[i] = t;
                t++;
                infoPrint("slot[%d] start new thread tidx=%d. \n", i, slot[i]);
            }

            // check slot empty
            if(slot[i] == EMPTY_SLOT) {
                emptySlot++;
            }
        }

        // check all thread end
        if(emptySlot == limitThread) {
            debugPrint("all threads(%d) is run finished.\n", nthreads);
            break;
        } else {
            debugPrint("current thread index=%d all thread=%d\n", t, nthreads);
        }
    }

    tmfree(slot);

    return 0;
}
#endif

// run
int32_t runInsertThread(SDataBase* database, SSuperTable* stbInfo, int32_t nthreads, threadInfo *infos, pthread_t *pids) {
    infoPrint("run insert thread. real nthread=%d\n", nthreads);
    // create threads
    int threadCnt = 0;
    for (int i = 0; i < nthreads && !g_arguments->terminate; i++) {
        threadInfo *pThreadInfo = infos + i;
        if (stbInfo->interlaceRows > 0) {
            pthread_create(pids + i, NULL, syncWriteInterlace,   pThreadInfo);
        } else {
            pthread_create(pids + i, NULL, syncWriteProgressive, pThreadInfo);
        }
        threadCnt ++;
    }

    // wait threads
    for (int i = 0; i < threadCnt; i++) {
        infoPrint("pthread_join %d ...\n", i);
        pthread_join(pids[i], NULL);
    }

    return 0;
}


// exit and free resource
int32_t exitInsertThread(SDataBase* database, SSuperTable* stbInfo, int32_t nthreads, threadInfo *infos, pthread_t *pids, int64_t spend) {

    if (g_arguments->terminate)  toolsMsleep(100);

    BArray *  total_delay_list = benchArrayInit(1, sizeof(int64_t));
    int64_t   totalDelay = 0;
    int64_t   totalDelay1 = 0;
    int64_t   totalDelay2 = 0;
    int64_t   totalDelay3 = 0;
    uint64_t  totalInsertRows = 0;

    // free threads resource
    for (int i = 0; i < nthreads; i++) {
        threadInfo *pThreadInfo = infos + i;
        // free check sql
        if (pThreadInfo->csql) {
            tmfree(pThreadInfo->csql);
            pThreadInfo->csql = NULL;
        }

        // close conn
        int protocol = stbInfo->lineProtocol;
        switch (stbInfo->iface) {
            case REST_IFACE:
                if (g_arguments->terminate)
                    toolsMsleep(100);
                destroySockFd(pThreadInfo->sockfd);
                if (stbInfo->interlaceRows > 0) {
                    free_ds(&pThreadInfo->buffer);
                } else {
                    tmfree(pThreadInfo->buffer);
                    pThreadInfo->buffer = NULL;
                }
                break;
            case SML_REST_IFACE:
                if (g_arguments->terminate)
                    toolsMsleep(100);
                tmfree(pThreadInfo->buffer);
                // on-purpose no break here
            case SML_IFACE:
                if (TSDB_SML_JSON_PROTOCOL != protocol
                        && SML_JSON_TAOS_FORMAT != protocol) {
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        tmfree(pThreadInfo->sml_tags[t]);
                    }
                    for (int j = 0; j < g_arguments->reqPerReq; j++) {
                        tmfree(pThreadInfo->lines[j]);
                    }
                    tmfree(pThreadInfo->sml_tags);
                    pThreadInfo->sml_tags = NULL;
                } else {
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        tmfree(pThreadInfo->sml_tags_json_array[t]);
                    }
                    tmfree(pThreadInfo->sml_tags_json_array);
                    pThreadInfo->sml_tags_json_array = NULL;
                    if (pThreadInfo->sml_json_tags) {
                        tools_cJSON_Delete(pThreadInfo->sml_json_tags);
                        pThreadInfo->sml_json_tags = NULL;
                    }
                    if (pThreadInfo->json_array) {
                        tools_cJSON_Delete(pThreadInfo->json_array);
                        pThreadInfo->json_array = NULL;
                    }
                }
                if (pThreadInfo->lines) {
                    if ((0 == stbInfo->interlaceRows)
                            && (TSDB_SML_JSON_PROTOCOL == protocol)) {
                        tmfree(pThreadInfo->lines[0]);
                        for (int t = 0; t < pThreadInfo->ntables; t++) {
                            tmfree(pThreadInfo->sml_json_value_array[t]);
                        }
                        tmfree(pThreadInfo->sml_json_value_array);
                    }
                    tmfree(pThreadInfo->lines);
                    pThreadInfo->lines = NULL;
                }
                break;

            case STMT_IFACE:
                // close stmt
                if(pThreadInfo->conn && pThreadInfo->conn->stmt) {
                    taos_stmt_close(pThreadInfo->conn->stmt);
                    pThreadInfo->conn->stmt = NULL;
                }
            case STMT2_IFACE:
                // close stmt2
                if (pThreadInfo->conn && pThreadInfo->conn->stmt2) {
                    taos_stmt2_close(pThreadInfo->conn->stmt2);
                    pThreadInfo->conn->stmt2 = NULL;
                }

                tmfree(pThreadInfo->bind_ts);
                tmfree(pThreadInfo->bind_ts_array);
                tmfree(pThreadInfo->bindParams);

                // free tagsStmt
                BArray *tags = pThreadInfo->tagsStmt;
                if(tags) {
                    // free child
                    for (int k = 0; k < tags->size; k++) {
                        Field * tag = benchArrayGet(tags, k);
                        tmfree(tag->stmtData.data);
                        tag->stmtData.data = NULL;
                        tmfree(tag->stmtData.is_null);
                        tag->stmtData.is_null = NULL;
                        tmfree(tag->stmtData.lengths);
                        tag->stmtData.lengths = NULL;
                    }
                    // free parent
                    benchArrayDestroy(tags);
                    pThreadInfo->tagsStmt = NULL;
                }

                // free lengths
                if(pThreadInfo->lengths) {
                    for(int c = 0; c <= stbInfo->cols->size; c++) {
                        tmfree(pThreadInfo->lengths[c]);
                    }
                    free(pThreadInfo->lengths);
                    pThreadInfo->lengths = NULL;
                }
                break;

            case TAOSC_IFACE:
                if (stbInfo->interlaceRows > 0) {
                    free_ds(&pThreadInfo->buffer);
                } else {
                    tmfree(pThreadInfo->buffer);
                    pThreadInfo->buffer = NULL;
                }
                break;

            default:
                break;
        }
        totalInsertRows += pThreadInfo->totalInsertRows;
        totalDelay += pThreadInfo->totalDelay;
        totalDelay1 += pThreadInfo->totalDelay1;
        totalDelay2 += pThreadInfo->totalDelay2;
        totalDelay3 += pThreadInfo->totalDelay3;
        if (pThreadInfo->delayList != NULL) {
            benchArrayAddBatch(total_delay_list, pThreadInfo->delayList->pData,
                    pThreadInfo->delayList->size, true);
            tmfree(pThreadInfo->delayList);
            pThreadInfo->delayList = NULL;
        }
        //  free conn
        if (pThreadInfo->conn) {
            closeBenchConn(pThreadInfo->conn);
            pThreadInfo->conn = NULL;
        }
    }

    // calculate result
    qsort(total_delay_list->pData, total_delay_list->size,
            total_delay_list->elemSize, compare);

    if (g_arguments->terminate)  toolsMsleep(100);

    tmfree(pids);
    tmfree(infos);

    // print result
    int ret = printTotalDelay(database, totalDelay, totalDelay1, totalDelay2, totalDelay3,
                              total_delay_list, nthreads, totalInsertRows, spend);
    benchArrayDestroy(total_delay_list);
    if (g_fail || ret != 0) {
        return -1;
    }
    return 0;
}

static int startMultiThreadInsertData(SDataBase* database, SSuperTable* stbInfo) {
    if ((stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE)
            && !stbInfo->use_metric) {
        errorPrint("%s", "schemaless cannot work without stable\n");
        return -1;
    }

    // check argument valid
    preProcessArgument(stbInfo);

    // ntable
    int64_t ntables = obtainTableCount(database, stbInfo);
    if (ntables == 0) {
        errorPrint("insert table count is zero. %s.%s\n", database->dbName, stbInfo->stbName);
        return -1;
    }

    // assign table to thread
    int32_t  nthreads  = g_arguments->nthreads;
    int64_t  div       = 0;  // ntable / nthread  division
    int64_t  mod       = 0;  // ntable % nthread
    int64_t  spend     = 0;

    if (g_arguments->bind_vgroup) {
        nthreads = assignTableToThread(database, stbInfo);
        if(nthreads == 0) {
            errorPrint("bind vgroup assign theads count is zero. %s.%s\n", database->dbName, stbInfo->stbName);
            return -1;
        }
    } else {
        if(nthreads == 0) {
            errorPrint("argument thread_count can not be zero. %s.%s\n", database->dbName, stbInfo->stbName);
            return -1;
        }
        div = ntables / nthreads;
        if (div < 1) {
            nthreads = (int32_t)ntables;
            div = 1;
        }
        mod = ntables % nthreads;
    }


    // init each thread information
    pthread_t   *pids  = benchCalloc(1, nthreads * sizeof(pthread_t),  true);
    threadInfo  *infos = benchCalloc(1, nthreads * sizeof(threadInfo), true);

    // init
    int32_t ret = initInsertThread(database, stbInfo, nthreads, infos, div, mod);
    if( ret != 0) {
        errorPrint("init insert thread failed. %s.%s\n", database->dbName, stbInfo->stbName);
        tmfree(pids);
        tmfree(infos);
        return ret;
    }

    infoPrint("Estimate memory usage: %.2fMB\n", (double)g_memoryUsage / 1048576);
    prompt(0);


    // run
    int64_t start = toolsGetTimestampUs();
    if(g_arguments->bind_vgroup && g_arguments->nthreads < nthreads ) {
        // need many batch execute all threads
#ifdef LINUX
        ret = runInsertLimitThread(database, stbInfo, nthreads, g_arguments->nthreads, infos, pids);
#else
        ret = runInsertThread(database, stbInfo, nthreads, infos, pids);
#endif
    } else {
        // only one batch execute all threads
        ret = runInsertThread(database, stbInfo, nthreads, infos, pids);
    }

    int64_t end = toolsGetTimestampUs();
    if(end == start) {
        spend = 1;
    } else {
        spend = end - start;
    }

    // exit
    ret = exitInsertThread(database, stbInfo, nthreads, infos, pids, spend);
    return ret;
}

static int getStbInsertedRows(char* dbName, char* stbName, TAOS* taos) {
    int rows = 0;
    char command[SHORT_1K_SQL_BUFF_LEN];
    snprintf(command, SHORT_1K_SQL_BUFF_LEN, "SELECT COUNT(*) FROM %s.%s",
             dbName, stbName);
    TAOS_RES* res = taos_query(taos, command);
    int code = taos_errno(res);
    if (code != 0) {
        printErrCmdCodeStr(command, code, res);
        return -1;
    }
    TAOS_ROW row = taos_fetch_row(res);
    if (row == NULL) {
        rows = 0;
    } else {
        rows = (int)*(int64_t*)row[0];
    }
    taos_free_result(res);
    return rows;
}

static void create_tsma(TSMA* tsma, SBenchConn* conn, char* stbName) {
    char command[SHORT_1K_SQL_BUFF_LEN];
    int len = snprintf(command, SHORT_1K_SQL_BUFF_LEN,
                       "CREATE sma INDEX %s ON %s function(%s) "
                       "INTERVAL (%s) SLIDING (%s)",
                       tsma->name, stbName, tsma->func,
                       tsma->interval, tsma->sliding);
    if (tsma->custom) {
        snprintf(command + len, SHORT_1K_SQL_BUFF_LEN - len,
                 " %s", tsma->custom);
    }
    int code = queryDbExecCall(conn, command);
    if (code == 0) {
        infoPrint("successfully create tsma with command <%s>\n", command);
    }
}

static void* create_tsmas(void* args) {
    tsmaThreadInfo* pThreadInfo = (tsmaThreadInfo*) args;
    int inserted_rows = 0;
    SBenchConn* conn = initBenchConn(pThreadInfo->dbName);
    if (NULL == conn) {
        return NULL;
    }
    int finished = 0;
    if (taos_select_db(conn->taos, pThreadInfo->dbName)) {
        errorPrint("failed to use database (%s)\n", pThreadInfo->dbName);
        closeBenchConn(conn);
        return NULL;
    }
    while (finished < pThreadInfo->tsmas->size && inserted_rows >= 0) {
        inserted_rows = (int)getStbInsertedRows(
                pThreadInfo->dbName, pThreadInfo->stbName, conn->taos);
        for (int i = 0; i < pThreadInfo->tsmas->size; i++) {
            TSMA* tsma = benchArrayGet(pThreadInfo->tsmas, i);
            if (!tsma->done &&  inserted_rows >= tsma->start_when_inserted) {
                create_tsma(tsma, conn, pThreadInfo->stbName);
                tsma->done = true;
                finished++;
                break;
            }
        }
        toolsMsleep(10);
    }
    benchArrayDestroy(pThreadInfo->tsmas);
    closeBenchConn(conn);
    return NULL;
}

void changeGlobalIface() {
    if (g_arguments->databases->size == 1) {
            SDataBase *db = benchArrayGet(g_arguments->databases, 0);
            if (db && db->superTbls->size == 1) {
                SSuperTable *stb = benchArrayGet(db->superTbls, 0);
                if (stb) {
                    if(g_arguments->iface != stb->iface) {
                        infoPrint("only 1 db 1 super table, g_arguments->iface(%d) replace with stb->iface(%d) \n", g_arguments->iface, stb->iface);
                        g_arguments->iface = stb->iface;
                    }
                }
            }
    }
}

int insertTestProcess() {
    prompt(0);

    encodeAuthBase64();
    // if only one stable, global iface same with stable->iface
    changeGlobalIface();

    // move from loop to here
    if (isRest(g_arguments->iface)) {
        if (0 != convertServAddr(g_arguments->iface,
                                 false,
                                 1)) {
            return -1;
        }
    }

    //loop create database
    for (int i = 0; i < g_arguments->databases->size; i++) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);

        if (database->drop && !(g_arguments->supplementInsert)) {
            if (database->superTbls && database->superTbls->size > 0) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
                if (stbInfo && isRest(stbInfo->iface)) {
                    if (0 != convertServAddr(stbInfo->iface,
                                             stbInfo->tcpTransfer,
                                             stbInfo->lineProtocol)) {
                        return -1;
                    }
                }
            }

            if (createDatabase(database)) {
                errorPrint("failed to create database (%s)\n",
                        database->dbName);
                return -1;
            }
            succPrint("created database (%s)\n", database->dbName);
        } else if(g_arguments->bind_vgroup) {
            // database already exist, get vgroups from server
            SBenchConn* conn = initBenchConn(NULL);
            if (conn) {
                int32_t vgroups = getVgroupsNative(conn, database);
                if (vgroups <=0) {
                    closeBenchConn(conn);
                    errorPrint("Database %s's vgroups is zero , db exist case.\n", database->dbName);
                    return -1;
                }
                closeBenchConn(conn);
                succPrint("Database (%s) get vgroups num is %d from server.\n", database->dbName, vgroups);
            }
        }
    }

    // create super table && fill child tables && prepareSampleData
    for (int i = 0; i < g_arguments->databases->size; i++) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        if (database->superTbls) {
            for (int j = 0; j < database->superTbls->size; j++) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);
                if (stbInfo->iface != SML_IFACE
                        && stbInfo->iface != SML_REST_IFACE
                        && !stbInfo->childTblExists) {
                    int code = getSuperTableFromServer(database, stbInfo);
                    if (code == TSDB_CODE_FAILED) {
                        return -1;
                    }

                    // with create table if not exists, so if exist, can not report failed
                    if (createSuperTable(database, stbInfo)) {
                        return -1;
                    }

                }
                // fill last ts from super table
                if(stbInfo->autoFillback && stbInfo->childTblExists) {
                    fillSTableLastTs(database, stbInfo);
                }

                // calc now
                if(stbInfo->calcNow) {
                    calcExprFromServer(database, stbInfo);
                }

                // check fill child table count valid
                if(fillChildTblName(database, stbInfo) <= 0) {
                    infoPrint(" warning fill childs table count is zero, db:%s stb: %s \n", database->dbName, stbInfo->stbName);
                }
                if (0 != prepareSampleData(database, stbInfo)) {
                    return -1;
                }

                // early malloc buffer for auto create table
                if((stbInfo->iface == STMT_IFACE || stbInfo->iface == STMT2_IFACE) && stbInfo->autoTblCreating) {
                    prepareTagsStmt(stbInfo);
                }

                // execute sqls
                if (stbInfo->sqls) {
                    char **sqls = stbInfo->sqls;
                    while (*sqls) {
                        queryDbExec(database, stbInfo, *sqls);
                        sqls++;
                    }
                }
            }
        }
    }

    // tsma
    if (g_arguments->taosc_version == 3) {
        for (int i = 0; i < g_arguments->databases->size; i++) {
            SDataBase* database = benchArrayGet(g_arguments->databases, i);
            if (database->superTbls) {
                for (int j = 0; (j < database->superTbls->size
                        && !g_arguments->terminate); j++) {
                    SSuperTable* stbInfo =
                        benchArrayGet(database->superTbls, j);
                    if (stbInfo->tsmas == NULL) {
                        continue;
                    }
                    if (stbInfo->tsmas->size > 0) {
                        tsmaThreadInfo* pThreadInfo =
                            benchCalloc(1, sizeof(tsmaThreadInfo), true);
                        pthread_t tsmas_pid = {0};
                        pThreadInfo->dbName = database->dbName;
                        pThreadInfo->stbName = stbInfo->stbName;
                        pThreadInfo->tsmas = stbInfo->tsmas;
                        pthread_create(&tsmas_pid, NULL,
                                       create_tsmas, pThreadInfo);
                    }
                }
            }
        }
    }

    if (createChildTables()) return -1;

    // create sub threads for inserting data
    for (int i = 0; i < g_arguments->databases->size; i++) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        if (database->superTbls) {
            for (uint64_t j = 0; j < database->superTbls->size; j++) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);
                if (stbInfo->insertRows == 0) {
                    continue;
                }
                prompt(stbInfo->non_stop);
                if (startMultiThreadInsertData(database, stbInfo)) {
                    return -1;
                }
            }
        }
    }
    return 0;
}

//
//     ------- STMT 2 -----------
//

static int32_t stmt2BindAndSubmit(
        threadInfo *pThreadInfo,
        SChildTable *childTbl,
        int64_t *timestamp, uint64_t i, char *ttl, int32_t *pkCur, int32_t *pkCnt, int64_t *delay1,
        int64_t *delay3, int64_t* startTs, int64_t* endTs, int32_t w) {

    // create bindV
    int32_t count            = 1;
    TAOS_STMT2_BINDV * bindv = createBindV(count, 0, 0);
    TAOS_STMT2 *stmt2        = pThreadInfo->conn->stmt2;
    SSuperTable *stbInfo     = pThreadInfo->stbInfo;

    //
    // bind
    //

    // count
    bindv->count = 1;
    // tbnames
    bindv->tbnames[0] = childTbl->name;
    // tags
    //bindv->tags[0] = NULL; // Progrssive mode tag put on prepare sql, no need put here

    // bind_cols
    uint32_t batch = (g_arguments->reqPerReq > stbInfo->insertRows - i) ? (stbInfo->insertRows - i) : g_arguments->reqPerReq;
    int32_t n = 0;
    int64_t pos = i % g_arguments->prepared_rand;

    // adjust batch about pos
    if(g_arguments->prepared_rand - pos < batch ) {
        debugPrint("prepared_rand(%" PRId64 ") is not a multiple of num_of_records_per(%d), the batch size can be modify. before=%d after=%d\n",
                    (int64_t)g_arguments->prepared_rand, (int32_t)g_arguments->reqPerReq, (int32_t)batch, (int32_t)(g_arguments->prepared_rand - pos));
        batch = g_arguments->prepared_rand - pos;
    }

    if (batch == 0) {
        infoPrint("batch size is zero. pos = %"PRId64"\n", pos);
        return 0;
    }

    uint32_t generated = bindVColsProgressive(bindv, 0, pThreadInfo, batch, *timestamp, pos, childTbl, pkCur, pkCnt, &n);
    if(generated == 0) {
        errorPrint( "get cols data bind information failed. table: %s\n", childTbl->name);
        freeBindV(bindv);
        return -1;
    }
    *timestamp += n * stbInfo->timestamp_step;

    if (g_arguments->debug_print) {
        showBindV(bindv, stbInfo->tags, stbInfo->cols);
    }

    // bind and submit
    int32_t code = submitStmt2(pThreadInfo, bindv, delay1, delay3, startTs, endTs, &generated, w);
    // free
    freeBindV(bindv);

    if(code != 0) {
        errorPrint( "failed submitStmt2() progressive mode, table: %s . engine error: %s\n", childTbl->name, taos_stmt2_error(stmt2));
        return code;
    } else {
        debugPrint("succ submitStmt2 progressive mode. table=%s batch=%d pos=%" PRId64 " ts=%" PRId64 " generated=%d\n",
                childTbl->name, batch, pos, *timestamp, generated);
        return generated;
    }
}
