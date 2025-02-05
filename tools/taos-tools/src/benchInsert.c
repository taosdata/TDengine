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
#include "wrapDb.h"
#include <benchData.h>
#include <benchInsertMix.h>

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

static int getSuperTableFromServerRest(
    SDataBase* database, SSuperTable* stbInfo, char *command) {

    return -1;
    // TODO(me): finish full implementation
#if 0
    int sockfd = createSockFd();
    if (sockfd < 0) {
        return -1;
    }

    int code = postProceSql(command,
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
    SDataBase* database, SSuperTable* stbInfo, char *command) {
#ifdef WEBSOCKET
    if (g_arguments->websocket) {
        return -1;
    }
#endif
    TAOS_RES *   res;
    TAOS_ROW     row = NULL;
    SBenchConn* conn = initBenchConn();
    if (NULL == conn) {
        return -1;
    }

    res = taos_query(conn->taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        infoPrint("stable %s does not exist, will create one\n",
                  stbInfo->stbName);
        closeBenchConn(conn);
        return -1;
    }
    infoPrint("find stable<%s>, will get meta data from server\n",
              stbInfo->stbName);
    benchArrayClear(stbInfo->tags);
    benchArrayClear(stbInfo->cols);
    int count = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (count == 0) {
            count++;
            continue;
        }
        int32_t *lengths = taos_fetch_lengths(res);
        if (lengths == NULL) {
            errorPrint("%s", "failed to execute taos_fetch_length\n");
            taos_free_result(res);
            closeBenchConn(conn);
            return -1;
        }
        if (strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "tag",
                        strlen("tag")) == 0) {
            Field* tag = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(stbInfo->tags, tag);
            tag = benchArrayGet(stbInfo->tags, stbInfo->tags->size - 1);
            tag->type = convertStringToDatatype(
                    (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX]);
            tag->length = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            tag->min = convertDatatypeToDefaultMin(tag->type);
            tag->max = convertDatatypeToDefaultMax(tag->type);
            tstrncpy(tag->name,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     lengths[TSDB_DESCRIBE_METRIC_FIELD_INDEX] + 1);
        } else {
            Field * col = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(stbInfo->cols, col);
            col = benchArrayGet(stbInfo->cols, stbInfo->cols->size - 1);
            col->type = convertStringToDatatype(
                    (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX]);
            col->length = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            col->min = convertDatatypeToDefaultMin(col->type);
            col->max = convertDatatypeToDefaultMax(col->type);
            tstrncpy(col->name,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     lengths[TSDB_DESCRIBE_METRIC_FIELD_INDEX] + 1);
        }
    }
    taos_free_result(res);
    closeBenchConn(conn);
    return 0;
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
    if (REST_IFACE == stbInfo->iface) {
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
        SBenchConn* conn = initBenchConn();
        if (NULL == conn) {
            ret = -1;
        } else {
            ret = queryDbExecCall(conn, command);
            int32_t trying = g_arguments->keep_trying;
            while (ret && trying) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-create "
                          "supertable %s\n",
                          g_arguments->trying_interval, stbInfo->stbName);
                toolsMsleep(g_arguments->trying_interval);
                ret = queryDbExecCall(conn, command);
                if (trying != -1) {
                    trying--;
                }
            }
            if (0 != ret) {
                errorPrint("create supertable %s failed!\n\n",
                       stbInfo->stbName);
                ret = -1;
            }
            closeBenchConn(conn);
        }
    }

    return ret;
}

#ifdef WEBSOCKET
static void dropSuperTable(SDataBase* database, SSuperTable* stbInfo) {
    if (g_arguments->supplementInsert) {
        return;
    }

    char command[SHORT_1K_SQL_BUFF_LEN] = "\0";
    snprintf(command, sizeof(command),
        g_arguments->escape_character
            ? "DROP TABLE `%s`.`%s`"
            : "DROP TABLE %s.%s",
             database->dbName,
             stbInfo->stbName);

    infoPrint("drop stable: <%s>\n", command);
    queryDbExec(database, stbInfo, command);

    return;
}
#endif  // WEBSOCKET

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
                col->type == TSDB_DATA_TYPE_NCHAR) {
            n = snprintf(colsBuf + len, col_buffer_len - len,
                    ",%s %s(%d)", col->name,
                    convertDatatypeToString(col->type), col->length);
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
             "(ts timestamp%s)", colsBuf);

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
                tag->type == TSDB_DATA_TYPE_NCHAR) {
            n = snprintf(tagsBuf + len, tag_buffer_len - len,
                    "%s %s(%d),", tag->name,
                    convertDatatypeToString(tag->type), tag->length);
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
        } else {
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
            ? "CREATE TABLE IF NOT EXISTS `%s`.`%s` (ts TIMESTAMP%s) TAGS %s"
            : "CREATE TABLE IF NOT EXISTS %s.%s (ts TIMESTAMP%s) TAGS %s",
        database->dbName, stbInfo->stbName, colsBuf, tagsBuf);
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
                n = snprintf(command + length,
                                   TSDB_MAX_ALLOWED_SQL_LEN - length,
                        " SMA(%s", col->name);
                first_sma = false;
            } else {
                n = snprintf(command + length,
                                   TSDB_MAX_ALLOWED_SQL_LEN - length,
                        ",%s", col->name);
            }

            if (n < 0 || n > TSDB_MAX_ALLOWED_SQL_LEN - length) {
                errorPrint("%s() LN%d snprintf overflow on %d iteral\n",
                           __func__, __LINE__, i);
                break;
            } else {
                length += n;
            }
        }
    }
    if (!first_sma) {
        snprintf(command + length, TSDB_MAX_ALLOWED_SQL_LEN - length, ")");
    }
    infoPrint("create stable: <%s>\n", command);

    int ret = queryDbExec(database, stbInfo, command);
    free(command);
    return ret;
}

#ifdef TD_VER_COMPATIBLE_3_0_0_0
int32_t getVgroupsOfDb(SBenchConn *conn, SDataBase *database) {
    int     vgroups = 0;
    char    cmd[SHORT_1K_SQL_BUFF_LEN] = "\0";

    snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
            g_arguments->escape_character
            ? "USE `%s`"
            : "USE %s",
            database->dbName);

    int32_t   code;
    TAOS_RES *res = NULL;

    res = taos_query(conn->taos, cmd);
    code = taos_errno(res);
    if (code) {
        printErrCmdCodeStr(cmd, code, res);
        return -1;
    }
    taos_free_result(res);

    snprintf(cmd, SHORT_1K_SQL_BUFF_LEN, "SHOW VGROUPS");
    res = taos_query(conn->taos, cmd);
    code = taos_errno(res);
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
#endif  // TD_VER_COMPATIBLE_3_0_0_0

int32_t toolsGetDefaultVGroups() {
    int32_t cores = toolsGetNumberOfCores();
    if (cores < 3 ) {
        return 1;
    }

    int64_t MemKB = 0;
    benchGetTotalMemory(&MemKB);

    infoPrint("check local machine CPU: %d Memory:%d MB \n", cores, (int32_t)(MemKB/1024));
    if (MemKB <= 2*1024*1024) { // 2G
        return 1;
    } else if (MemKB <= 4*1024*1024) { // 4G
        return 2;
    } else if (MemKB <= 8*1024*1024) { // 8G
        return 3;
    } else if (MemKB <= 16*1024*1024) { // 16G
        return 4;
    } else if (MemKB <= 32*1024*1024) { // 32G
        return 5;
    } else {
        return cores / 2;
    }
}

int geneDbCreateCmd(SDataBase *database, char *command, int remainVnodes) {
    int dataLen = 0;
    int n;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
    if (g_arguments->nthreads_auto || (-1 != g_arguments->inputted_vgroups)) {
        n = snprintf(command + dataLen, SHORT_1K_SQL_BUFF_LEN - dataLen,
                    g_arguments->escape_character
                        ? "CREATE DATABASE IF NOT EXISTS `%s` VGROUPS %d"
                        : "CREATE DATABASE IF NOT EXISTS %s VGROUPS %d",
                            database->dbName,
                            (-1 != g_arguments->inputted_vgroups)?
                            g_arguments->inputted_vgroups:
                            min(remainVnodes, toolsGetNumberOfCores()));
    } else {
        n = snprintf(command + dataLen, SHORT_1K_SQL_BUFF_LEN - dataLen,
                    g_arguments->escape_character
                        ? "CREATE DATABASE IF NOT EXISTS `%s`"
                        : "CREATE DATABASE IF NOT EXISTS %s",
                            database->dbName);
    }
#else
    n = snprintf(command + dataLen, SHORT_1K_SQL_BUFF_LEN - dataLen,
                    g_arguments->escape_character
                        ? "CREATE DATABASE IF NOT EXISTS `%s`"
                        : "CREATE DATABASE IF NOT EXISTS %s", database->dbName);
#endif  // TD_VER_COMPATIBLE_3_0_0_0
    if (n < 0 || n >= SHORT_1K_SQL_BUFF_LEN - dataLen) {
        errorPrint("%s() LN%d snprintf overflow\n",
                           __func__, __LINE__);
        return -1;
    } else {
        dataLen += n;
    }

    if (database->cfgs) {
        for (int i = 0; i < database->cfgs->size; i++) {
            SDbCfg* cfg = benchArrayGet(database->cfgs, i);
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

    snprintf(command, SHORT_1K_SQL_BUFF_LEN,
            g_arguments->escape_character
                ? "DROP DATABASE IF EXISTS `%s`;"
                : "DROP DATABASE IF EXISTS %s;",
             database->dbName);
    code = postProceSql(command,
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
    } else {
        int remainVnodes = INT_MAX;
        geneDbCreateCmd(database, command, remainVnodes);
        code = postProceSql(command,
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
            code = postProceSql(command,
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
    SBenchConn* conn = initBenchConn();
    if (NULL == conn) {
        return -1;
    }
    if (g_arguments->taosc_version == 3) {
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
    }

    snprintf(command, SHORT_1K_SQL_BUFF_LEN,
            g_arguments->escape_character
                ? "DROP DATABASE IF EXISTS `%s`;":
            "DROP DATABASE IF EXISTS %s;",
             database->dbName);
    if (0 != queryDbExecCall(conn, command)) {
#ifdef WEBSOCKET
        if (g_arguments->websocket) {
            warnPrint("%s", "TDengine cloud normal users have no privilege "
                      "to drop database! DROP DATABASE failure is ignored!\n");
        } else {
#endif
            closeBenchConn(conn);
            return -1;
#ifdef WEBSOCKET
        }
#endif
    }

    int remainVnodes = INT_MAX;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
    if (g_arguments->nthreads_auto) {
        remainVnodes = getRemainVnodes(conn);
        if (0 >= remainVnodes) {
            errorPrint("Remain vnodes %d, failed to create database\n",
                       remainVnodes);
            return -1;
        }
    }
#endif
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
#ifdef WEBSOCKET
        if (g_arguments->websocket) {
            warnPrint("%s", "TDengine cloud normal users have no privilege "
                      "to create database! CREATE DATABASE "
                      "failure is ignored!\n");
        } else {
#endif

            closeBenchConn(conn);
            errorPrint("\ncreate database %s failed!\n\n",
               database->dbName);
            return -1;
#ifdef WEBSOCKET
        }
#endif
    }
    infoPrint("command to create database: <%s>\n", command);

#ifdef TD_VER_COMPATIBLE_3_0_0_0
    if (database->superTbls) {
        if (g_arguments->nthreads_auto) {
            int32_t vgroups = getVgroupsOfDb(conn, database);
            if (vgroups <=0) {
                closeBenchConn(conn);
                errorPrint("Database %s's vgroups is %d\n",
                           database->dbName, vgroups);
                return -1;
            }
        }
    }
#endif  // TD_VER_COMPATIBLE_3_0_0_0

    closeBenchConn(conn);
    return 0;
}

int createDatabase(SDataBase* database) {
    int ret = 0;
    if (REST_IFACE == g_arguments->iface) {
        ret = createDatabaseRest(database);
    } else {
        ret = createDatabaseTaosc(database);
    }
#if 0
#ifdef LINUX
    infoPrint("%s() LN%d, ret: %d\n", __func__, __LINE__, ret);
    sleep(10);
    infoPrint("%s() LN%d, ret: %d\n", __func__, __LINE__, ret);
#elif defined(DARWIN)
    sleep(2);
#else
    Sleep(2);
#endif
#endif

    return ret;
}

static int generateChildTblName(int len, char *buffer, SDataBase *database,
                                SSuperTable *stbInfo, uint64_t tableSeq, char* tagData, int i,
                                char *ttl) {
    if (0 == len) {
        memset(buffer, 0, TSDB_MAX_ALLOWED_SQL_LEN);
        len += snprintf(buffer + len,
                        TSDB_MAX_ALLOWED_SQL_LEN - len, "CREATE TABLE IF NOT EXISTS ");
    }

    len += snprintf(
            buffer + len, TSDB_MAX_ALLOWED_SQL_LEN - len,
            g_arguments->escape_character
            ? "`%s`.`%s%" PRIu64 "` USING `%s`.`%s` TAGS (%s) %s "
            : "%s.%s%" PRIu64 " USING %s.%s TAGS (%s) %s ",
            database->dbName, stbInfo->childTblPrefix, tableSeq, database->dbName,
            stbInfo->stbName,
            tagData + i * stbInfo->lenOfTags, ttl);

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

static void *createTable(void *sarg) {
    if (g_arguments->supplementInsert) {
        return NULL;
    }

    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
#ifdef LINUX
    prctl(PR_SET_NAME, "createTable");
#endif
    uint64_t lastPrintTime = toolsGetTimestampMs();
    pThreadInfo->buffer = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    int len = 0;
    int batchNum = 0;
    infoPrint(
              "thread[%d] start creating table from %" PRIu64 " to %" PRIu64
              "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);

    char ttl[SMALL_BUFF_LEN] = "";
    if (stbInfo->ttl != 0) {
        snprintf(ttl, SMALL_BUFF_LEN, "TTL %d", stbInfo->ttl);
    }

    // tag read from csv
    FILE *csvFile = openTagCsv(stbInfo);
    // malloc
    char* tagData = benchCalloc(TAG_BATCH_COUNT, stbInfo->lenOfTags, false);
    int         w = 0; // record tagData

    int smallBatchCount = 0;
    for (uint64_t i = pThreadInfo->start_table_from + stbInfo->childTblFrom;
            (i <= (pThreadInfo->end_table_to + stbInfo->childTblFrom)
             && !g_arguments->terminate); i++) {
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
                if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile)) {
                    goto create_table_end;
                }
            }

            len = generateChildTblName(len, pThreadInfo->buffer,
                                       database, stbInfo, i, tagData, w, ttl);
            // move next
            if (++w >= TAG_BATCH_COUNT) {
                // reset for gen again
                w = 0;
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
        memset(pThreadInfo->buffer, 0, TSDB_MAX_ALLOWED_SQL_LEN);
        uint64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > PRINT_STAT_INTERVAL) {
            infoPrint(
                       "thread[%d] already created %" PRId64 " tables\n",
                       pThreadInfo->threadID, pThreadInfo->tables_created);
            lastPrintTime = currentPrintTime;
        }
    }

    if (0 != len) {
        int ret = 0;
        debugPrint("thread[%d] creating table: %s\n", pThreadInfo->threadID,
                   pThreadInfo->buffer);
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

static int startMultiThreadCreateChildTable(
        SDataBase* database, SSuperTable* stbInfo) {
    int code = -1;
    int          threads = g_arguments->table_threads;
    int64_t      ntables;
    if (stbInfo->childTblTo > 0) {
        ntables = stbInfo->childTblTo - stbInfo->childTblFrom;
    } else {
        ntables = stbInfo->childTblCount;
    }
    pthread_t   *pids = benchCalloc(1, threads * sizeof(pthread_t), false);
    threadInfo  *infos = benchCalloc(1, threads * sizeof(threadInfo), false);
    uint64_t     tableFrom = 0;
    if (threads < 1) {
        threads = 1;
    }

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = (int)ntables;
        a = 1;
    }

    if (ntables == 0) {
        errorPrint("failed to create child table, childTblCount: %"PRId64"\n",
                ntables);
        goto over;
    }
    int64_t b = ntables % threads;

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
            pThreadInfo->conn = initBenchConn();
            if (NULL == pThreadInfo->conn) {
                goto over;
            }
        }
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->tables_created = 0;
        pthread_create(pids + i, NULL, createTable, pThreadInfo);
        threadCnt ++;
    }

    for (int i = 0; i < threadCnt; i++) {
        pthread_join(pids[i], NULL);
    }

    if (g_arguments->terminate)  toolsMsleep(100);

    for (int i = 0; i < threadCnt; i++) {
        threadInfo *pThreadInfo = infos + i;
        g_arguments->actualChildTables += pThreadInfo->tables_created;

        if ((REST_IFACE != stbInfo->iface) && pThreadInfo->conn) {
            closeBenchConn(pThreadInfo->conn);
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
        infoPrintToFile(g_arguments->fpOfInsertResult,
                  "start creating %" PRId64 " table(s) with %d thread(s)\n",
                  g_arguments->totalChildTables, g_arguments->table_threads);
    }
    double start = (double)toolsGetTimestampMs();

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

    double end = (double)toolsGetTimestampMs();
    succPrint(
            "Spent %.4f seconds to create %" PRId64
            " table(s) with %d thread(s), already exist %" PRId64
            " table(s), actual %" PRId64 " table(s) pre created, %" PRId64
            " table(s) will be auto created\n",
            (end - start) / 1000.0, g_arguments->totalChildTables,
            g_arguments->table_threads, g_arguments->existedChildTables,
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
                    tmfree(childCol->stmtData.is_null);
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
                if ((NULL == root) && (0 == strcmp(cfg->name, "replica"))) {
                    tmfree(cfg->name);
                    cfg->name = NULL;
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
                }
                benchArrayDestroy(stbInfo->tags);

                for (int k = 0; k < stbInfo->cols->size; k++) {
                    Field * col = benchArrayGet(stbInfo->cols, k);
                    tmfree(col->stmtData.data);
                    col->stmtData.data = NULL;
                    tmfree(col->stmtData.is_null);
                    col->stmtData.is_null = NULL;
                }
                if (g_arguments->test_mode == INSERT_TEST) {
                    if (stbInfo->childTblArray) {
                        for (int64_t child = 0; child < stbInfo->childTblCount;
                                child++) {
                            SChildTable *childTbl =
                                stbInfo->childTblArray[child];
                            tmfree(childTbl->name);
                            if (childTbl) {
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


#ifdef TD_VER_COMPATIBLE_3_0_0_0
                if ((0 == stbInfo->interlaceRows)
                        && (g_arguments->nthreads_auto)) {
                    for (int32_t v = 0; v < database->vgroups; v++) {
                        SVGroup *vg = benchArrayGet(database->vgArray, v);
                        tmfree(vg->childTblArray);
                        vg->childTblArray = NULL;
                    }
                }
#endif  // TD_VER_COMPATIBLE_3_0_0_0
            }
#ifdef TD_VER_COMPATIBLE_3_0_0_0
            if (database->vgArray)
                benchArrayDestroy(database->vgArray);
#endif  // TD_VER_COMPATIBLE_3_0_0_0
            benchArrayDestroy(database->superTbls);
        }
    }
    benchArrayDestroy(g_arguments->databases);
    benchArrayDestroy(g_arguments->streams);
    tools_cJSON_Delete(root);
}

int32_t execInsert(threadInfo *pThreadInfo, uint32_t k) {
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_RES *   res = NULL;
    int32_t      code = 0;
    uint16_t     iface = stbInfo->iface;

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

        case REST_IFACE:
            debugPrint("buffer: %s\n", pThreadInfo->buffer);
            code = postProceSql(pThreadInfo->buffer,
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
                code = postProceSql(pThreadInfo->buffer,
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
            code = taos_stmt_execute(pThreadInfo->conn->stmt);
            if (code) {
                errorPrint(
                           "failed to execute insert statement. reason: %s\n",
                           taos_stmt_errstr(pThreadInfo->conn->stmt));
                code = -1;
            }
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
                code = postProceSql(pThreadInfo->lines[0], database->dbName,
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
                code = postProceSql(pThreadInfo->buffer, database->dbName,
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
    infoPrint("end load child tables info. delay=%.2fs\n", (toolsGetTimestampUs() - start)/1E6);

    tmfree(buf);
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
    uint32_t batchPerTblTimes = g_arguments->reqPerReq / interlaceRows;
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    uint64_t   lastTotalInsertRows = 0;
    int64_t   startTs = toolsGetTimestampUs();
    int64_t   endTs;
    uint64_t   tableSeq = pThreadInfo->start_table_from;
    int disorderRange = stbInfo->disorderRange;

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
    int   w       = 0;
    if (stbInfo->autoTblCreating) {
        csvFile = openTagCsv(stbInfo);
        tagData = benchCalloc(TAG_BATCH_COUNT, stbInfo->lenOfTags, false);
    }
    int64_t delay1 = 0;
    int64_t delay2 = 0;
    int64_t delay3 = 0;

    while (insertRows > 0) {
        int64_t tmp_total_insert_rows = 0;
        uint32_t generated = 0;
        if (insertRows <= interlaceRows) {
            interlaceRows = insertRows;
        }
        for (int i = 0; i < batchPerTblTimes; i++) {
            if (g_arguments->terminate) {
                goto free_of_interlace;
            }
            int64_t pos       = pThreadInfo->pos;
            SChildTable *childTbl = stbInfo->childTblArray[tableSeq];
            char *  tableName   = childTbl->name;
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
                        if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile)) {
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
                            snprintf(time_string, BIGINT_BUFF_LEN, "now");
                        } else {
                            snprintf(time_string, BIGINT_BUFF_LEN, "%"PRId64"",
                                    disorderTs?disorderTs:childTbl->ts);
                        }

                        // combine rows timestamp | other cols = sampleDataBuf[pos]
                        ds_add_strs(&pThreadInfo->buffer, 5,
                                    "(",
                                    time_string,
                                    ",",
                                    sampleDataBuf + pos * stbInfo->lenOfCols,
                                    ") ");
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
                    generated = bindParamBatch(pThreadInfo, interlaceRows,
                                       childTbl->ts, childTbl, &childTbl->pkCur, &childTbl->pkCnt, &n, &delay2, &delay3);
                    
                    childTbl->ts += stbInfo->timestamp_step * n;
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
                                "%s %s %" PRId64 "",
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
                break;
            }
        }

        startTs = toolsGetTimestampUs();
        if (execInsert(pThreadInfo, generated)) {
            g_fail = true;
            goto free_of_interlace;
        }
        endTs = toolsGetTimestampUs();

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
    int32_t n =0;
    int32_t generated = bindParamBatch(
            pThreadInfo,
            (g_arguments->reqPerReq > (stbInfo->insertRows - i))
                ? (stbInfo->insertRows - i)
                : g_arguments->reqPerReq,
            *timestamp, childTbl, pkCur, pkCnt, &n, delay2, delay3);
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
    int protocol = stbInfo->lineProtocol;
    for (int j = 0; (j < g_arguments->reqPerReq)
            && !g_arguments->terminate; j++) {
        tools_cJSON *tag = tools_cJSON_Duplicate(
                tools_cJSON_GetArrayItem(
                    pThreadInfo->sml_json_tags,
                    (int)tableSeq -
                    pThreadInfo->start_table_from),
                true);
        debugPrintJsonNoTime(tag);
        if (TSDB_SML_JSON_PROTOCOL == protocol) {
            generateSmlJsonCols(
                    pThreadInfo->json_array, tag, stbInfo,
                    database->sml_precision, *timestamp);
        } else {
            generateSmlTaosJsonCols(
                    pThreadInfo->json_array, tag, stbInfo,
                    database->sml_precision, *timestamp);
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
        if (TSDB_SML_LINE_PROTOCOL == protocol) {
            snprintf(
                    pThreadInfo->lines[j],
                    stbInfo->lenOfCols + stbInfo->lenOfTags,
                    "%s %s %" PRId64 "",
                    pThreadInfo->sml_tags[tableSeq
                    - pThreadInfo->start_table_from],
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
                    pThreadInfo->sml_tags[tableSeq
                    -pThreadInfo->start_table_from]);
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

#ifdef TD_VER_COMPATIBLE_3_0_0_0
    if (g_arguments->nthreads_auto) {
        if (0 == pThreadInfo->vg->tbCountPerVgId) {
            return NULL;
        }
    } else {
        infoPrint(
            "thread[%d] start progressive inserting into table from "
            "%" PRIu64 " to %" PRIu64 "\n",
            pThreadInfo->threadID, pThreadInfo->start_table_from,
            pThreadInfo->end_table_to + 1);
    }
#else
    infoPrint(
            "thread[%d] start progressive inserting into table from "
            "%" PRIu64 " to %" PRIu64 "\n",
            pThreadInfo->threadID, pThreadInfo->start_table_from,
            pThreadInfo->end_table_to + 1);
#endif
    uint64_t  lastPrintTime = toolsGetTimestampMs();
    uint64_t  lastTotalInsertRows = 0;
    int64_t   startTs = toolsGetTimestampUs();
    int64_t   endTs;

    FILE* csvFile = NULL;
    char* tagData = NULL;
    bool  stmt    = stbInfo->iface == STMT_IFACE && stbInfo->autoTblCreating;
    bool  smart   = SMART_IF_FAILED == stbInfo->continueIfFail;
    bool  acreate = (stbInfo->iface == TAOSC_IFACE || stbInfo->iface == REST_IFACE) && stbInfo->autoTblCreating;
    int   w       = 0;
    if (stmt || smart || acreate) {
        csvFile = openTagCsv(stbInfo);
        tagData = benchCalloc(TAG_BATCH_COUNT, stbInfo->lenOfTags, false);
    }
    
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
            tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        char *sampleDataBuf;
        SChildTable *childTbl;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
        if (g_arguments->nthreads_auto) {
            childTbl = pThreadInfo->vg->childTblArray[tableSeq];
        } else {
            childTbl = stbInfo->childTblArray[
                stbInfo->childTblExists?
                tableSeq:
                stbInfo->childTblFrom + tableSeq];
        }
#else
        childTbl = stbInfo->childTblArray[
                stbInfo->childTblExists?
                tableSeq:
                stbInfo->childTblFrom + tableSeq];
#endif
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
        if (stmt) {
            taos_stmt_close(pThreadInfo->conn->stmt);
            pThreadInfo->conn->stmt = taos_stmt_init(pThreadInfo->conn->taos);
            if (NULL == pThreadInfo->conn->stmt) {
                errorPrint("taos_stmt_init() failed, reason: %s\n",
                        taos_errstr(NULL));
                g_fail = true;
                goto free_of_progressive;
            }
        }

        if(stmt || smart || acreate) {
            // generator
            if (w == 0) {
                if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile)) {
                    g_fail = true;
                    goto free_of_progressive;
                }
            }
        }   
        
        if (stmt) {
            if (prepareStmt(stbInfo, pThreadInfo->conn->stmt, tagData, w)) {
                g_fail = true;
                goto free_of_progressive;
            }
        }

        if(stmt || smart || acreate) {
            // move next
            if (++w >= TAG_BATCH_COUNT) {
                // reset for gen again
                w = 0;
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
            // only measure insert
            startTs = toolsGetTimestampUs();
            int code = execInsert(pThreadInfo, generated);
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
                        if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile)) {
                            g_fail = true;
                            goto free_of_progressive;
                        }
                    }

                    int ret = smartContinueIfFail(
                            pThreadInfo,
                            childTbl, tagData, w, ttl);
                    if (0 != ret) {
                        g_fail = true;
                        goto free_of_progressive;
                    }

                    // move next
                    if (++w >= TAG_BATCH_COUNT) {
                        // reset for gen again
                        w = 0;
                    }

                    code = execInsert(pThreadInfo, generated);
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
            endTs = toolsGetTimestampUs()+1;

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
                code = executeSql(pThreadInfo->conn->taos,sql);
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

static int initStmtDataValue(SSuperTable *stbInfo, SChildTable *childTbl) {
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
            if ((0 == c) && stbInfo->useSampleTs) {
                continue;
            }

            char *tmpStr = calloc(1, index + 1);
            if (NULL == tmpStr) {
                errorPrint("%s() LN%d, Failed to allocate %d bind buffer\n",
                        __func__, __LINE__, index + 1);
                return -1;
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

            strncpy(tmpStr, restStr, index);

            if (0 == strcmp(tmpStr, "NULL")) {
                *(stmtData->is_null + i) = true;
            } else {
                switch (dataType) {
                    case TSDB_DATA_TYPE_INT:
                    case TSDB_DATA_TYPE_UINT:
                        *((int32_t*)stmtData->data + i) = atoi(tmpStr);
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
                    case TSDB_DATA_TYPE_UBIGINT:
                        *((int64_t*)stmtData->data + i) = (int64_t)atol(tmpStr);
                        break;
                    case TSDB_DATA_TYPE_BOOL:
                        *((int8_t*)stmtData->data + i) = (int8_t)atoi(tmpStr);
                        break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        *((int64_t*)stmtData->data + i) = (int64_t)atol(tmpStr);
                        break;
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_NCHAR:
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

        default:
            errorPrint("Unknown data type: %s\n",
                       convertDatatypeToString(dataType));
            exit(EXIT_FAILURE);
    }
}

static int parseBufferToStmtBatchChildTbl(SSuperTable *stbInfo,
                                          SChildTable* childTbl) {
    int32_t columnCount = stbInfo->cols->size;

    for (int c = 0; c < columnCount; c++) {
        Field *col = benchArrayGet(stbInfo->cols, c);
        ChildField *childCol = benchArrayGet(childTbl->childCols, c);
        char dataType = col->type;

        char *is_null = benchCalloc(
                1, sizeof(char) *g_arguments->prepared_rand, false);

        tmfree(childCol->stmtData.is_null);
        childCol->stmtData.is_null = is_null;

        initStmtData(dataType, &(childCol->stmtData.data), col->length);
    }

    return initStmtDataValue(stbInfo, childTbl);
}

static int parseBufferToStmtBatch(SSuperTable* stbInfo) {
    int32_t columnCount = stbInfo->cols->size;

    for (int c = 0; c < columnCount; c++) {
        Field *col = benchArrayGet(stbInfo->cols, c);
        char dataType = col->type;

        char *is_null = benchCalloc(
                1, sizeof(char) *g_arguments->prepared_rand, false);
        tmfree(col->stmtData.is_null);
        col->stmtData.is_null = is_null;

        initStmtData(dataType, &(col->stmtData.data), col->length);
    }

    return initStmtDataValue(stbInfo, NULL);
}

static int64_t fillChildTblNameByCount(SSuperTable *stbInfo) {
    for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
        char childName[TSDB_TABLE_NAME_LEN]={0};
        snprintf(childName,
                 TSDB_TABLE_NAME_LEN,
                 "%s%" PRIu64 "",
                 stbInfo->childTblPrefix, i);
        stbInfo->childTblArray[i]->name = strdup(childName);
        debugPrint("%s(): %s\n", __func__,
                  stbInfo->childTblArray[i]->name);
    }

    return stbInfo->childTblCount;
}

static int64_t fillChildTblNameByFromTo(SDataBase *database,
        SSuperTable* stbInfo) {
    for (int64_t i = stbInfo->childTblFrom; i < stbInfo->childTblTo; i++) {
        char childName[TSDB_TABLE_NAME_LEN]={0};
        snprintf(childName,
                TSDB_TABLE_NAME_LEN,
                "%s%" PRIu64 "",
                stbInfo->childTblPrefix, i);
        stbInfo->childTblArray[i-stbInfo->childTblFrom]->name = strdup(childName);
    }

    return (stbInfo->childTblTo-stbInfo->childTblFrom);
}

static int64_t fillChildTblNameByLimitOffset(SDataBase *database,
        SSuperTable* stbInfo) {
    SBenchConn* conn = initBenchConn();
    if (NULL == conn) {
        return -1;
    }
    char cmd[SHORT_1K_SQL_BUFF_LEN] = "\0";
    if (g_arguments->taosc_version == 3) {
        snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
                 "SELECT DISTINCT(TBNAME) FROM %s.`%s` LIMIT %" PRId64
                 " OFFSET %" PRIu64 "",
                 database->dbName, stbInfo->stbName, stbInfo->childTblLimit,
                 stbInfo->childTblOffset);
    } else {
        snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
                 "SELECT TBNAME FROM %s.`%s` LIMIT %" PRId64
                 " OFFSET %" PRIu64 "",
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
        infoPrint("%s",
                "not support autocreate table with interlace row in stmt "
                "insertion, will change to progressive mode\n");
        stbInfo->interlaceRows = 0;
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
                            int64_t start, int64_t end) {
    // zero check
    if (total_delay_list->size == 0 || (end - start) == 0 || threads == 0) {
        return -1;
    }

    char subDelay[128] = "";
    if(totalDelay1 + totalDelay2 + totalDelay3 > 0) {
        sprintf(subDelay, " stmt delay1=%.2fs delay2=%.2fs delay3=%.2fs",
                totalDelay1/threads/1E6,
                totalDelay2/threads/1E6,
                totalDelay3/threads/1E6);
    }

    succPrint("Spent %.6f (real %.6f) seconds to insert rows: %" PRIu64
              " with %d thread(s) into %s %.2f (real %.2f) records/second%s\n",
              (end - start)/1E6, totalDelay/threads/1E6, totalInsertRows, threads,
              database->dbName,
              (double)(totalInsertRows / ((end - start)/1E6)),
              (double)(totalInsertRows / (totalDelay/threads/1E6)), subDelay);
    if (!total_delay_list->size) {
        return -1;
    }

    succPrint("insert delay, "
              "min: %.4fms, "
              "avg: %.4fms, "
              "p90: %.4fms, "
              "p95: %.4fms, "
              "p99: %.4fms, "
              "max: %.4fms\n",
              *(int64_t *)(benchArrayGet(total_delay_list, 0))/1E3,
              (double)totalDelay/total_delay_list->size/1E3,
              *(int64_t *)(benchArrayGet(total_delay_list,
                                         (int32_t)(total_delay_list->size
                                         * 0.9)))/1E3,
              *(int64_t *)(benchArrayGet(total_delay_list,
                                         (int32_t)(total_delay_list->size
                                         * 0.95)))/1E3,
              *(int64_t *)(benchArrayGet(total_delay_list,
                                         (int32_t)(total_delay_list->size
                                         * 0.99)))/1E3,
              *(int64_t *)(benchArrayGet(total_delay_list,
                                         (int32_t)(total_delay_list->size
                                         - 1)))/1E3);
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
    SBenchConn* conn = initBenchConn();
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
    SBenchConn* conn = initBenchConn();
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

static int startMultiThreadInsertData(SDataBase* database,
        SSuperTable* stbInfo) {
    if ((stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE)
            && !stbInfo->use_metric) {
        errorPrint("%s", "schemaless cannot work without stable\n");
        return -1;
    }

    preProcessArgument(stbInfo);

    int64_t ntables;
    if (stbInfo->childTblTo > 0) {
        ntables = stbInfo->childTblTo - stbInfo->childTblFrom;
    } else if (stbInfo->childTblLimit > 0 && stbInfo->childTblExists) {
        ntables = stbInfo->childTblLimit;
    } else {
        ntables = stbInfo->childTblCount;
    }
    if (ntables == 0) {
        return 0;
    }

    uint64_t tableFrom = 0;
    int32_t threads = g_arguments->nthreads;
    int64_t a = 0, b = 0;

#ifdef TD_VER_COMPATIBLE_3_0_0_0
    if ((0 == stbInfo->interlaceRows)
            && (g_arguments->nthreads_auto)) {
        SBenchConn* conn = initBenchConn();
        if (NULL == conn) {
            return -1;
        }

        for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
            int vgId;
            int ret = taos_get_table_vgId(
                    conn->taos, database->dbName,
                    stbInfo->childTblArray[i]->name, &vgId);
            if (ret < 0) {
                errorPrint("Failed to get %s db's %s table's vgId\n",
                           database->dbName,
                           stbInfo->childTblArray[i]->name);
                closeBenchConn(conn);
                return -1;
            }
            debugPrint("Db %s\'s table\'s %s vgId is: %d\n",
                       database->dbName,
                       stbInfo->childTblArray[i]->name, vgId);
            for (int32_t v = 0; v < database->vgroups; v++) {
                SVGroup *vg = benchArrayGet(database->vgArray, v);
                if (vgId == vg->vgId) {
                    vg->tbCountPerVgId++;
                }
            }
        }

        threads = 0;
        for (int v = 0; v < database->vgroups; v++) {
            SVGroup *vg = benchArrayGet(database->vgArray, v);
            infoPrint("Total %"PRId64" tables on bb %s's vgroup %d (id: %d)\n",
                      vg->tbCountPerVgId, database->dbName, v, vg->vgId);
            if (vg->tbCountPerVgId) {
                threads++;
            } else {
                continue;
            }
            vg->childTblArray = benchCalloc(
                    vg->tbCountPerVgId, sizeof(SChildTable*), true);
            vg->tbOffset = 0;
        }
        for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
            int vgId;
            int ret = taos_get_table_vgId(
                    conn->taos, database->dbName,
                    stbInfo->childTblArray[i]->name, &vgId);
            if (ret < 0) {
                errorPrint("Failed to get %s db's %s table's vgId\n",
                           database->dbName,
                           stbInfo->childTblArray[i]->name);

                closeBenchConn(conn);
                return -1;
            }
            debugPrint("Db %s\'s table\'s %s vgId is: %d\n",
                       database->dbName,
                       stbInfo->childTblArray[i]->name, vgId);
            for (int32_t v = 0; v < database->vgroups; v++) {
                SVGroup *vg = benchArrayGet(database->vgArray, v);
                if (vgId == vg->vgId) {
                    vg->childTblArray[vg->tbOffset] =
                           stbInfo->childTblArray[i];
                    vg->tbOffset++;
                }
            }
        }
        closeBenchConn(conn);
    } else {
        a = ntables / threads;
        if (a < 1) {
            threads = (int32_t)ntables;
            a = 1;
        }
        b = 0;
        if (threads != 0) {
            b = ntables % threads;
        }
    }

    // valid check
    if(threads <= 0) {
        errorPrint("db: %s threads num is invalid. threads=%d\n",
                    database->dbName,
                    threads);
        return -1;
    }

    int32_t vgFrom = 0;
#else
    a = ntables / threads;
    if (a < 1) {
        threads = (int32_t)ntables;
        a = 1;
    }
    b = 0;
    if (threads != 0) {
        b = ntables % threads;
    }
#endif   // TD_VER_COMPATIBLE_3_0_0_0

    FILE* csvFile = NULL;
    char* tagData = NULL;
    bool  stmtN   = (stbInfo->iface == STMT_IFACE && stbInfo->autoTblCreating == false);
    int   w       = 0;
    if (stmtN) {
        csvFile = openTagCsv(stbInfo);
        tagData = benchCalloc(TAG_BATCH_COUNT, stbInfo->lenOfTags, false);
    }

    pthread_t   *pids = benchCalloc(1, threads * sizeof(pthread_t), true);
    threadInfo  *infos = benchCalloc(1, threads * sizeof(threadInfo), true);

    for (int32_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;
        pThreadInfo->dbInfo = database;
        pThreadInfo->stbInfo = stbInfo;
        pThreadInfo->start_time = stbInfo->startTimestamp;
        pThreadInfo->pos  = 0;
        pThreadInfo->totalInsertRows = 0;
        pThreadInfo->samplePos = 0;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
        if ((0 == stbInfo->interlaceRows)
                && (g_arguments->nthreads_auto)) {
            int32_t j;
            for (j = vgFrom; i < database->vgroups; j++) {
                SVGroup *vg = benchArrayGet(database->vgArray, j);
                if (0 == vg->tbCountPerVgId) {
                    continue;
                }
                pThreadInfo->vg = vg;
                pThreadInfo->start_table_from = 0;
                pThreadInfo->ntables = vg->tbCountPerVgId;
                pThreadInfo->end_table_to = vg->tbCountPerVgId-1;
                break;
            }
            vgFrom = j + 1;
        } else {
            pThreadInfo->start_table_from = tableFrom;
            pThreadInfo->ntables = i < b ? a + 1 : a;
            pThreadInfo->end_table_to = (i < b)?(tableFrom+a):(tableFrom+a-1);
            tableFrom = pThreadInfo->end_table_to + 1;
        }
#else
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = (i < b)?(tableFrom+a):(tableFrom+a-1);
        tableFrom = pThreadInfo->end_table_to + 1;
#endif  // TD_VER_COMPATIBLE_3_0_0_0
        pThreadInfo->delayList = benchArrayInit(1, sizeof(int64_t));
        switch (stbInfo->iface) {
            case REST_IFACE: {
                if (stbInfo->interlaceRows > 0) {
                    pThreadInfo->buffer = new_ds(0);
                } else {
                    pThreadInfo->buffer =
                        benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
                }
                int sockfd = createSockFd();
                if (sockfd < 0) {
                    FREE_PIDS_INFOS_RETURN_MINUS_1();
                }
                pThreadInfo->sockfd = sockfd;
                break;
            }
            case STMT_IFACE: {
                pThreadInfo->conn = initBenchConn();
                if (NULL == pThreadInfo->conn) {
                    FREE_PIDS_INFOS_RETURN_MINUS_1();
                }
                pThreadInfo->conn->stmt =
                    taos_stmt_init(pThreadInfo->conn->taos);
                if (NULL == pThreadInfo->conn->stmt) {
                    errorPrint("taos_stmt_init() failed, reason: %s\n",
                               taos_errstr(NULL));
                    FREE_RESOURCE();
                    return -1;
                }
                if (taos_select_db(pThreadInfo->conn->taos, database->dbName)) {
                    errorPrint("taos select database(%s) failed\n",
                            database->dbName);
                    FREE_RESOURCE();
                    return -1;
                }
                if (stmtN) {
                    // generator
                    if (w == 0) {
                        if(!generateTagData(stbInfo, tagData, TAG_BATCH_COUNT, csvFile)) {
                            if(csvFile){
                                fclose(csvFile);
                            }
                            tmfree(tagData);
                            FREE_RESOURCE();
                            return -1;
                        }
                    }

                    if (prepareStmt(stbInfo, pThreadInfo->conn->stmt, tagData, w)) {
                        if(csvFile){
                            fclose(csvFile);
                        }
                        tmfree(tagData);
                        FREE_RESOURCE();
                        return -1;
                    }

                    // move next
                    if (++w >= TAG_BATCH_COUNT) {
                        // reset for gen again
                        w = 0;
                    } 
                }

                pThreadInfo->bind_ts = benchCalloc(1, sizeof(int64_t), true);
                pThreadInfo->bind_ts_array =
                        benchCalloc(1, sizeof(int64_t)*g_arguments->reqPerReq,
                                    true);
                pThreadInfo->bindParams = benchCalloc(
                        1, sizeof(TAOS_MULTI_BIND)*(stbInfo->cols->size + 1),
                        true);
                pThreadInfo->is_null = benchCalloc(1, g_arguments->reqPerReq,
                                                   true);
                parseBufferToStmtBatch(stbInfo);
                for (int64_t child = 0;
                        child < stbInfo->childTblCount; child++) {
                    SChildTable *childTbl = stbInfo->childTblArray[child];
                    if (childTbl->useOwnSample) {
                        parseBufferToStmtBatchChildTbl(stbInfo, childTbl);
                    }
                }

                break;
            }
            case SML_REST_IFACE: {
                int sockfd = createSockFd();
                if (sockfd < 0) {
                    free(pids);
                    free(infos);
                    return -1;
                }
                pThreadInfo->sockfd = sockfd;
            }
            case SML_IFACE: {
                pThreadInfo->conn = initBenchConn();
                if (pThreadInfo->conn == NULL) {
                    errorPrint("%s() init connection failed\n", __func__);
                    FREE_RESOURCE();
                    return -1;
                }
                if (taos_select_db(pThreadInfo->conn->taos, database->dbName)) {
                    errorPrint("taos select database(%s) failed\n",
                               database->dbName);
                    FREE_RESOURCE();
                    return -1;
                }
                pThreadInfo->max_sql_len =
                    stbInfo->lenOfCols + stbInfo->lenOfTags;
                if (stbInfo->iface == SML_REST_IFACE) {
                    pThreadInfo->buffer =
                            benchCalloc(1, g_arguments->reqPerReq *
                                      (1 + pThreadInfo->max_sql_len), true);
                }
                int protocol = stbInfo->lineProtocol;
                if (TSDB_SML_JSON_PROTOCOL != protocol
                        && SML_JSON_TAOS_FORMAT != protocol) {
                    pThreadInfo->sml_tags =
                        (char **)benchCalloc(pThreadInfo->ntables,
                                             sizeof(char *), true);
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        pThreadInfo->sml_tags[t] =
                                benchCalloc(1, stbInfo->lenOfTags, true);
                    }

                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        if (generateRandData(
                                    stbInfo, pThreadInfo->sml_tags[t],
                                    stbInfo->lenOfTags,
                                    stbInfo->lenOfCols + stbInfo->lenOfTags,
                                    stbInfo->tags, 1, true, NULL)) {
                            return -1;
                        }
                        debugPrint("pThreadInfo->sml_tags[%d]: %s\n", t,
                                   pThreadInfo->sml_tags[t]);
                    }
                    pThreadInfo->lines =
                            benchCalloc(g_arguments->reqPerReq,
                                        sizeof(char *), true);

                    for (int j = 0; (j < g_arguments->reqPerReq
                            && !g_arguments->terminate); j++) {
                        pThreadInfo->lines[j] =
                                benchCalloc(1, pThreadInfo->max_sql_len, true);
                    }
                } else {
                    pThreadInfo->json_array = tools_cJSON_CreateArray();
                    pThreadInfo->sml_json_tags = tools_cJSON_CreateArray();
                    pThreadInfo->sml_tags_json_array = (char **)benchCalloc(
                            pThreadInfo->ntables, sizeof(char *), true);
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
                    pThreadInfo->lines = (char **)benchCalloc(
                            1, sizeof(char *), true);
                    if ((0 == stbInfo->interlaceRows)
                        && (TSDB_SML_JSON_PROTOCOL == protocol)) {
                        pThreadInfo->line_buf_len =
                            g_arguments->reqPerReq *
                            accumulateRowLen(pThreadInfo->stbInfo->tags,
                                             pThreadInfo->stbInfo->iface);
                        debugPrint("%s() LN%d, line_buf_len=%d\n",
                               __func__, __LINE__, pThreadInfo->line_buf_len);
                        pThreadInfo->lines[0] = benchCalloc(
                            1, pThreadInfo->line_buf_len, true);
                        pThreadInfo->sml_json_value_array =
                            (char **)benchCalloc(
                                pThreadInfo->ntables, sizeof(char *), true);
                        for (int t = 0; t < pThreadInfo->ntables; t++) {
                            generateSmlJsonValues(
                                pThreadInfo->sml_json_value_array, stbInfo, t);
                        }
                    }
                }
                break;
            }
            case TAOSC_IFACE: {
                pThreadInfo->conn = initBenchConn();
                if (pThreadInfo->conn == NULL) {
                    errorPrint("%s() failed to connect\n", __func__);
                    FREE_RESOURCE();
                    return -1;
                }
                char* command = benchCalloc(1, SHORT_1K_SQL_BUFF_LEN, false);
                snprintf(command, SHORT_1K_SQL_BUFF_LEN,
                        g_arguments->escape_character
                        ? "USE `%s`"
                        : "USE %s",
                        database->dbName);
                if (queryDbExecCall(pThreadInfo->conn, command)) {
                    errorPrint("taos select database(%s) failed\n",
                               database->dbName);
                    FREE_RESOURCE();
                    tmfree(command);
                    return -1;
                }
                tmfree(command);
                command = NULL;

                if (stbInfo->interlaceRows > 0) {
                    pThreadInfo->buffer = new_ds(0);
                } else {
                    pThreadInfo->buffer =
                        benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
                    if (g_arguments->check_sql) {
                        pThreadInfo->csql =
                            benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
                        memset(pThreadInfo->csql, 0, TSDB_MAX_ALLOWED_SQL_LEN);
                    }
                }

                break;
            }
            default:
                break;
        }
    }

    if (csvFile) {
        fclose(csvFile);
    }
    tmfree(tagData);

    infoPrint("Estimate memory usage: %.2fMB\n",
              (double)g_memoryUsage / 1048576);
    prompt(0);

    // create threads
    int threadCnt = 0;
    for (int i = 0; (i < threads && !g_arguments->terminate); i++) {
        threadInfo *pThreadInfo = infos + i;
        if (stbInfo->interlaceRows > 0) {
            pthread_create(pids + i, NULL,
                           syncWriteInterlace, pThreadInfo);
        } else {
            pthread_create(pids + i, NULL,
                           syncWriteProgressive, pThreadInfo);
        }
        threadCnt ++;
    }

    int64_t start = toolsGetTimestampUs();

    // wait threads
    for (int i = 0; i < threadCnt; i++) {
        infoPrint(" pthread_join %d ...\n", i);
        pthread_join(pids[i], NULL);
    }

    int64_t end = toolsGetTimestampUs()+1;

    if (g_arguments->terminate)  toolsMsleep(100);

    BArray *  total_delay_list = benchArrayInit(1, sizeof(int64_t));
    int64_t   totalDelay = 0;
    int64_t   totalDelay1 = 0;
    int64_t   totalDelay2 = 0;
    int64_t   totalDelay3 = 0;
    uint64_t  totalInsertRows = 0;

    // free threads resource
    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        // free check sql
        if (pThreadInfo->csql) {
            tmfree(pThreadInfo->csql);
            pThreadInfo->csql = NULL;
        }

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
                taos_stmt_close(pThreadInfo->conn->stmt);

                // free length
                for (int c = 0; c < stbInfo->cols->size + 1; c++) {
                    TAOS_MULTI_BIND *param = (TAOS_MULTI_BIND *)(pThreadInfo->bindParams + sizeof(TAOS_MULTI_BIND) * c);
                    tmfree(param->length);
                }
                tmfree(pThreadInfo->bind_ts);
                tmfree(pThreadInfo->bind_ts_array);
                tmfree(pThreadInfo->bindParams);
                tmfree(pThreadInfo->is_null);
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
        benchArrayAddBatch(total_delay_list, pThreadInfo->delayList->pData,
                pThreadInfo->delayList->size);
        tmfree(pThreadInfo->delayList);
        pThreadInfo->delayList = NULL;
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

    free(pids);
    free(infos);

    int ret = printTotalDelay(database, totalDelay, totalDelay1, totalDelay2, totalDelay3,
                              total_delay_list, threads, totalInsertRows, start, end);
    benchArrayDestroy(total_delay_list);
    if (g_fail || ret) {
        return -1;
    }
    return 0;
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
    SBenchConn* conn = initBenchConn();
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

static int32_t createStream(SSTREAM* stream) {
    int32_t code = -1;
    char * command = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN, "DROP STREAM IF EXISTS %s",
             stream->stream_name);
    infoPrint("%s\n", command);
    SBenchConn* conn = initBenchConn();
    if (NULL == conn) {
        goto END_STREAM;
    }

    code = queryDbExecCall(conn, command);
    int32_t trying = g_arguments->keep_trying;
    while (code && trying) {
        infoPrint("will sleep %"PRIu32" milliseconds then re-drop stream %s\n",
                          g_arguments->trying_interval, stream->stream_name);
        toolsMsleep(g_arguments->trying_interval);
        code = queryDbExecCall(conn, command);
        if (trying != -1) {
            trying--;
        }
    }

    if (code) {
        closeBenchConn(conn);
        goto END_STREAM;
    }

    memset(command, 0, TSDB_MAX_ALLOWED_SQL_LEN);
    int pos = snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            "CREATE STREAM IF NOT EXISTS %s ", stream->stream_name);
    if (stream->trigger_mode[0] != '\0') {
        pos += snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
                "TRIGGER %s ", stream->trigger_mode);
    }
    if (stream->watermark[0] != '\0') {
        pos += snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
                "WATERMARK %s ", stream->watermark);
    }
    if (stream->ignore_update[0] != '\0') {
        pos += snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
                "IGNORE UPDATE %s ", stream->ignore_update);
    }
    if (stream->ignore_expired[0] != '\0') {
        pos += snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
                "IGNORE EXPIRED %s ", stream->ignore_expired);
    }
    if (stream->fill_history[0] != '\0') {
        pos += snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
                "FILL_HISTORY %s ", stream->fill_history);
    }
    pos += snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
            "INTO %s ", stream->stream_stb);
    if (stream->stream_stb_field[0] != '\0') {
        pos += snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
                "%s ", stream->stream_stb_field);
    }
    if (stream->stream_tag_field[0] != '\0') {
        pos += snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
                "TAGS%s ", stream->stream_tag_field);
    }
    if (stream->subtable[0] != '\0') {
        pos += snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
                "SUBTABLE%s ", stream->subtable);
    }
    snprintf(command + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos,
            "as %s", stream->source_sql);
    infoPrint("%s\n", command);

    code = queryDbExecCall(conn, command);
    trying = g_arguments->keep_trying;
    while (code && trying) {
        infoPrint("will sleep %"PRIu32" milliseconds "
                  "then re-create stream %s\n",
                  g_arguments->trying_interval, stream->stream_name);
        toolsMsleep(g_arguments->trying_interval);
        code = queryDbExecCall(conn, command);
        if (trying != -1) {
            trying--;
        }
    }

    closeBenchConn(conn);
END_STREAM:
    tmfree(command);
    return code;
}

int insertTestProcess() {
    prompt(0);

    encodeAuthBase64();
    //loop create database 
    for (int i = 0; i < g_arguments->databases->size; i++) {
        if (REST_IFACE == g_arguments->iface) {
            if (0 != convertServAddr(g_arguments->iface,
                                     false,
                                     1)) {
                return -1;
            }
        }
        SDataBase * database = benchArrayGet(g_arguments->databases, i);

        if (database->drop && !(g_arguments->supplementInsert)) {
            if (database->superTbls) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
                if (stbInfo && (REST_IFACE == stbInfo->iface)) {
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
        } else {
            // database already exist, get vgroups from server
            #ifdef TD_VER_COMPATIBLE_3_0_0_0
            if (database->superTbls) {
                SBenchConn* conn = initBenchConn();
                if (conn) {
                    int32_t vgroups = getVgroupsOfDb(conn, database);
                    if (vgroups <=0) {
                        closeBenchConn(conn);
                        errorPrint("Database %s's vgroups is zero.\n", database->dbName);
                        return -1;
                    }
                    closeBenchConn(conn);
                    succPrint("Database (%s) get vgroups num is %d from server.\n", database->dbName, vgroups);
                }
            }
            #endif  // TD_VER_COMPATIBLE_3_0_0_0
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
#ifdef WEBSOCKET
                    if (g_arguments->websocket) {
                        dropSuperTable(database, stbInfo);
                    }
#endif
                    if (getSuperTableFromServer(database, stbInfo) != 0) {
                        if (createSuperTable(database, stbInfo)) {
                            return -1;
                        }
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
                    infoPrint(" warning fill childs table count is zero, please check parameters in json is correct. database:%s stb: %s \n", database->dbName, stbInfo->stbName);
                }
                if (0 != prepareSampleData(database, stbInfo)) {
                    return -1;
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

    // create threads 
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

    if (g_arguments->taosc_version == 3) {
        for (int j = 0; j < g_arguments->streams->size; j++) {
            SSTREAM * stream = benchArrayGet(g_arguments->streams, j);
            if (stream->drop) {
                if (createStream(stream)) {
                    return -1;
                }
            }
        }
    }

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
