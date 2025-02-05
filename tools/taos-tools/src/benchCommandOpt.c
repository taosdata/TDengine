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
#include <toolsdef.h>

extern char      g_configDir[MAX_PATH_LEN];

// get taosBenchmark commit number version
#ifndef TAOSBENCHMARK_COMMIT_SHA1
#define TAOSBENCHMARK_COMMIT_SHA1 "unknown"
#endif

#ifndef TAOSBENCHMARK_TAG
#define TAOSBENCHMARK_TAG "0.1.0"
#endif

#ifndef TAOSBENCHMARK_STATUS
#define TAOSBENCHMARK_STATUS "unknown"
#endif


char *g_aggreFuncDemo[] = {"*",
                           "count(*)",
                           "avg(current)",
                           "sum(current)",
                           "max(current)",
                           "min(current)",
                           "first(current)",
                           "last(current)"};
char *g_aggreFunc[] = {"*",       "count(*)", "avg(C0)",   "sum(C0)",
                       "max(C0)", "min(C0)",  "first(C0)", "last(C0)"};

void printVersion() {
    char taosBenchmark_ver[] = TAOSBENCHMARK_TAG;
    char taosBenchmark_commit[] = TAOSBENCHMARK_COMMIT_SHA1;
    char taosBenchmark_status[] = TAOSBENCHMARK_STATUS;

    // version
    printf("taosBenchmark version: %s\ngit: %s\n", taosBenchmark_ver, taosBenchmark_commit);
    printf("build: %s\n", getBuildInfo());
    if (strlen(taosBenchmark_status) > 0) {
        printf("status: %s\n", taosBenchmark_status);
    } 
}

void parseFieldDatatype(char *dataType, BArray *fields, bool isTag) {
    benchArrayClear(fields);
    if (strstr(dataType, ",") == NULL) {
        Field * field = benchCalloc(1, sizeof(Field), true);
        benchArrayPush(fields, field);
        field = benchArrayGet(fields, 0);
        if (1 == regexMatch(dataType,
                    "^(BINARY|NCHAR|GEOMETRY|VARBINARY|VARCHAR)(\\([1-9][0-9]*\\))$",
                    REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            field->type = convertStringToDatatype(type, 0);
            field->length = atoi(length);
        } else {
            field->type = convertStringToDatatype(dataType, 0);
            field->length = convertTypeToLength(field->type);
        }
        field->min = convertDatatypeToDefaultMin(field->type);
        field->max = convertDatatypeToDefaultMax(field->type);
        tstrncpy(field->name, isTag?"t0":"c0", TSDB_COL_NAME_LEN);
    } else {
        char *dup_str = strdup(dataType);
        char *running = dup_str;
        char *token = strsep(&running, ",");
        int   index = 0;
        while (token) {
            Field * field = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(fields, field);
            field = benchArrayGet(fields, index);
            if (1 == regexMatch(token,
                        "^(BINARY|NCHAR|JSON)(\\([1-9][0-9]*\\))$",
                        REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                field->type = convertStringToDatatype(type, 0);
                field->length = atoi(length);
            } else {
                field->type = convertStringToDatatype(token, 0);
                field->length = convertTypeToLength(field->type);
            }
            field->max = convertDatatypeToDefaultMax(field->type);
            field->min = convertDatatypeToDefaultMin(field->type);
            snprintf(field->name, TSDB_COL_NAME_LEN, isTag?"t%d":"c%d", index);
            index++;
            token = strsep(&running, ",");
        }
        tmfree(dup_str);
    }
}

static void initStable() {
    SDataBase *database = benchArrayGet(g_arguments->databases, 0);
    database->superTbls = benchArrayInit(1, sizeof(SSuperTable));
    SSuperTable * stbInfo = benchCalloc(1, sizeof(SSuperTable), true);
    benchArrayPush(database->superTbls, stbInfo);
    stbInfo = benchArrayGet(database->superTbls, 0);
    stbInfo->iface = TAOSC_IFACE;
    stbInfo->stbName = "meters";
    stbInfo->childTblPrefix = DEFAULT_TB_PREFIX;
    stbInfo->use_metric = 1;
    stbInfo->max_sql_len = TSDB_MAX_ALLOWED_SQL_LEN;
    stbInfo->cols = benchArrayInit(3, sizeof(Field));
    for (int i = 0; i < 3; ++i) {
        Field *col = benchCalloc(1, sizeof(Field), true);
        benchArrayPush(stbInfo->cols, col);
    }
    Field * c1 = benchArrayGet(stbInfo->cols, 0);
    Field * c2 = benchArrayGet(stbInfo->cols, 1);
    Field * c3 = benchArrayGet(stbInfo->cols, 2);

    c1->type = TSDB_DATA_TYPE_FLOAT;
    c2->type = TSDB_DATA_TYPE_INT;
    c3->type = TSDB_DATA_TYPE_FLOAT;

    c1->length = sizeof(float);
    c2->length = sizeof(int32_t);
    c3->length = sizeof(float);

    tstrncpy(c1->name, "current", TSDB_COL_NAME_LEN + 1);
    tstrncpy(c2->name, "voltage", TSDB_COL_NAME_LEN + 1);
    tstrncpy(c3->name, "phase", TSDB_COL_NAME_LEN + 1);

    c1->min = 9;
    c1->max = 10;    
    //fun = "4*sin(x)+10*random(5)+10"
    c1->funType  = FUNTYPE_SIN;
    c1->multiple = 4;
    c1->random   = 5;
    c1->addend   = 10;
    c1->base     = 10;

    c2->min = 110;
    c2->max = 119;
    //fun = "1*square(0,60,50,0)+100*random(20)+120"
    c2->funType  = FUNTYPE_SQUARE;
    c2->multiple = 1;
    c2->random   = 20;
    c2->addend   = 100;
    c2->base     = 120;

    c3->min = 115;
    c3->max = 125;
    // fun = "1*saw(0,40,40,0)+50*random(10)+30"
    c3->funType  = FUNTYPE_SAW;
    c3->multiple = 1;
    c3->random   = 10;
    c3->addend   = 50;
    c3->base     = 30;

    stbInfo->tags = benchArrayInit(2, sizeof(Field));
    for (int i = 0; i < 2; ++i) {
        Field * tag = benchCalloc(1, sizeof(Field), true);
        benchArrayPush(stbInfo->tags, tag);
    }
    Field * t1 = benchArrayGet(stbInfo->tags, 0);
    Field * t2 = benchArrayGet(stbInfo->tags, 1);

    t1->type = TSDB_DATA_TYPE_INT;
    t2->type = TSDB_DATA_TYPE_BINARY;

    t1->length = sizeof(int32_t);
    t2->length = 24;

    tstrncpy(t1->name, "groupid", TSDB_COL_NAME_LEN + 1);
    tstrncpy(t2->name, "location", TSDB_COL_NAME_LEN + 1);

    t1->min = 1;
    t1->max = 100000;


    stbInfo->insert_interval = 0;
    stbInfo->timestamp_step = 1;
    stbInfo->angle_step = 1;
    stbInfo->interlaceRows = 0;
    stbInfo->childTblCount = DEFAULT_CHILDTABLES;
    stbInfo->childTblLimit = 0;
    stbInfo->childTblOffset = 0;
    stbInfo->autoTblCreating = false;
    stbInfo->childTblExists = false;
    stbInfo->random_data_source = true;
    stbInfo->lineProtocol = TSDB_SML_LINE_PROTOCOL;

    stbInfo->insertRows = DEFAULT_INSERT_ROWS;
    stbInfo->disorderRange = DEFAULT_DISORDER_RANGE;
    stbInfo->disorderRatio = 0;
    stbInfo->file_factor = -1;
    stbInfo->delay = -1;
    stbInfo->keep_trying = 0;
    stbInfo->trying_interval = 0;
}

static void initDatabase() {
    g_arguments->databases = benchArrayInit(1, sizeof(SDataBase));
    SDataBase *database = benchCalloc(1, sizeof(SDataBase), true);
    benchArrayPush(g_arguments->databases, database);
    database = benchArrayGet(g_arguments->databases, 0);
    database->dbName = DEFAULT_DATABASE;
    database->drop = true;
    database->precision = TSDB_TIME_PRECISION_MILLI;
    database->sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    database->cfgs = benchArrayInit(1, sizeof(SDbCfg));
}

void initArgument() {
    g_arguments = benchCalloc(1, sizeof(SArguments), true);
    if (taos_get_client_info()[0] == '3') {
        g_arguments->taosc_version = 3;
    } else {
        g_arguments->taosc_version = 2;
    }
    g_arguments->test_mode = INSERT_TEST;
    g_arguments->demo_mode = true;
    g_arguments->host = NULL;
    g_arguments->host_auto = true;
    g_arguments->port = DEFAULT_PORT;
    g_arguments->port_inputted = false;
    g_arguments->port_auto = true;
    g_arguments->telnet_tcp_port = TELNET_TCP_PORT;
    g_arguments->user = TSDB_DEFAULT_USER;
    g_arguments->password = TSDB_DEFAULT_PASS;
    g_arguments->answer_yes = 0;
    g_arguments->debug_print = 0;
    g_arguments->binwidth = DEFAULT_BINWIDTH;
    g_arguments->performance_print = 0;
    g_arguments->output_file = DEFAULT_OUTPUT;
    g_arguments->nthreads = DEFAULT_NTHREADS;
    g_arguments->table_threads = DEFAULT_NTHREADS;
    g_arguments->prepared_rand = DEFAULT_PREPARED_RAND;
    g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
    g_arguments->totalChildTables = DEFAULT_CHILDTABLES;
    g_arguments->actualChildTables = 0;
    g_arguments->autoCreatedChildTables = 0;
    g_arguments->existedChildTables = 0;
    g_arguments->chinese = false;
    g_arguments->aggr_func = 0;
    g_arguments->terminate = false;
#ifdef WEBSOCKET
    g_arguments->timeout = 10;
#endif

    g_arguments->supplementInsert = false;
    g_arguments->startTimestamp = DEFAULT_START_TIME;
    g_arguments->partialColNum = 0;

    g_arguments->keep_trying = 3;
    g_arguments->trying_interval = 0;
    g_arguments->iface = TAOSC_IFACE;
    g_arguments->rest_server_ver_major = -1;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
    g_arguments->inputted_vgroups = -1;
#endif
    g_arguments->mistMode = false;

    initDatabase();
    initStable();
    g_arguments->streams = benchArrayInit(1, sizeof(SSTREAM));
}

void modifyArgument() {
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable *superTable = benchArrayGet(database->superTbls, 0);
#ifdef WEBSOCKET
    if (!g_arguments->websocket) {
#endif
        if (strlen(g_configDir)
                && g_arguments->host_auto
                && g_arguments->port_auto) {
#ifdef LINUX
            wordexp_t full_path;
            if (wordexp(g_configDir, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", g_configDir);
                exit(EXIT_FAILURE);
            }
            taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
            wordfree(&full_path);
#else
            taos_options(TSDB_OPTION_CONFIGDIR, g_configDir);
#endif
            g_arguments->host = DEFAULT_HOST;
            g_arguments->port = 0;
        }
#ifdef WEBSOCKET
    }
#endif

    superTable->startTimestamp = g_arguments->startTimestamp;

    if (0 != g_arguments->partialColNum) {
        superTable->partialColNum = g_arguments->partialColNum;
    }

    for (int i = 0; i < superTable->cols->size; ++i) {
        Field * col = benchArrayGet(superTable->cols, i);
        if (!g_arguments->demo_mode) {
            snprintf(col->name, TSDB_COL_NAME_LEN, "c%d", i);
            col->min = convertDatatypeToDefaultMin(col->type);
            col->max = convertDatatypeToDefaultMax(col->type);
        }
        if (col->length == 0) {
            col->length = g_arguments->binwidth;
        }
    }

    for (int i = 0; i < superTable->tags->size; ++i) {
        Field* tag = benchArrayGet(superTable->tags, i);
        if (!g_arguments->demo_mode) {
            snprintf(tag->name, TSDB_COL_NAME_LEN, "t%d", i);
        }
        if (tag->length == 0) {
            tag->length = g_arguments->binwidth;
        }
    }

    if (g_arguments->intColumnCount > superTable->cols->size) {
        for (int i = superTable->cols->size;
                i < g_arguments->intColumnCount; ++i) {
            Field * col = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(superTable->cols, col);
            col = benchArrayGet(superTable->cols, i);
            col->type = TSDB_DATA_TYPE_INT;
            col->length = sizeof(int32_t);
            snprintf(col->name, TSDB_COL_NAME_LEN, "c%d", i);
            col->min = convertDatatypeToDefaultMin(col->type);
            col->max = convertDatatypeToDefaultMax(col->type);
        }
    }

    if (g_arguments->keep_trying) {
        superTable->keep_trying = g_arguments->keep_trying;
        superTable->trying_interval = g_arguments->trying_interval;
    }
}

static void *queryStableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;

    TAOS *taos = NULL;
    if (REST_IFACE != g_arguments->iface) {
        taos = pThreadInfo->conn->taos;
    }
#ifdef LINUX
    prctl(PR_SET_NAME, "queryStableAggrFunc");
#endif
    char *command = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    FILE *  fp = g_arguments->fpOfInsertResult;
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    int64_t totalData = stbInfo->insertRows * stbInfo->childTblCount;
    char **aggreFunc;
    int    n;

    if (g_arguments->demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]);
    } else {
        aggreFunc = g_aggreFunc;
        n = sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0]);
    }

    infoPrint("total Data: %" PRId64 "\n", totalData);
    if (fp) {
        fprintf(fp, "Querying On %" PRId64 " records:\n", totalData);
    }
    for (int j = 0; j < n; j++) {
        char condition[COND_BUF_LEN] = "\0";
        char tempS[LARGE_BUFF_LEN] = "\0";
        int64_t m = 10 < stbInfo->childTblCount ? 10 : stbInfo->childTblCount;
        for (int64_t i = 1; i <= m; i++) {
            if (i == 1) {
                if (g_arguments->demo_mode) {
                    snprintf(tempS, LARGE_BUFF_LEN,
                             "groupid = %" PRId64 "", i);
                } else {
                    snprintf(tempS, LARGE_BUFF_LEN,
                             "t0 = %" PRId64 "", i);
                }
            } else {
                if (g_arguments->demo_mode) {
                    snprintf(tempS, LARGE_BUFF_LEN,
                             " or groupid = %" PRId64 " ", i);
                } else {
                    snprintf(tempS, LARGE_BUFF_LEN,
                             " or t0 = %" PRId64 " ", i);
                }
            }
            strncat(condition, tempS, COND_BUF_LEN - 1);
            snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                     "SELECT %s FROM %s.meters WHERE %s",
                    aggreFunc[j], database->dbName,
                    condition);
            if (fp) {
                fprintf(fp, "%s\n", command);
            }
            double t = (double)toolsGetTimestampUs();
            int32_t code = -1;
            if (REST_IFACE == g_arguments->iface) {
                code = postProceSql(command, NULL, 0, REST_IFACE,
                                    0, g_arguments->port, 0,
                                    pThreadInfo->sockfd, NULL);
            } else {
                TAOS_RES *res = taos_query(taos, command);
                code = taos_errno(res);
                if (code != 0) {
                    printErrCmdCodeStr(command, code, res);
                    free(command);
                    return NULL;
                }
                int count = 0;
                while (taos_fetch_row(res) != NULL) {
                    count++;
                }
                taos_free_result(res);
            }
            t = toolsGetTimestampUs() - t;
            if (fp) {
                fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n",
                        totalData / (t / 1000), t);
            }
            infoPrint("%s took %.6f second(s)\n\n", command,
                      t / 1000000);
        }
    }
    free(command);
    return NULL;
}

static void *queryNtableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = NULL;
    if (pThreadInfo->conn) {
        taos = pThreadInfo->conn->taos;
    }
#ifdef LINUX
    prctl(PR_SET_NAME, "queryNtableAggrFunc");
#endif
    char *  command = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    FILE *  fp = g_arguments->fpOfInsertResult;
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    int64_t totalData = stbInfo->childTblCount * stbInfo->insertRows;
    char **aggreFunc;
    int    n;

    if (g_arguments->demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]);
    } else {
        aggreFunc = g_aggreFunc;
        n = sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0]);
    }

    infoPrint("totalData: %" PRId64 "\n", totalData);
    if (fp) {
        fprintf(fp,
                "| QFunctions |    QRecords    |   QSpeed(R/s)   |  "
                "QLatency(ms) |\n");
    }

    for (int j = 0; j < n; j++) {
        double   totalT = 0;
        uint64_t count = 0;
        for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
            snprintf(command,
                    TSDB_MAX_ALLOWED_SQL_LEN,
                    g_arguments->escape_character
                    ? "SELECT %s FROM `%s`.`%s%" PRId64 "` WHERE ts>= %" PRIu64 
                    : "SELECT %s FROM %s.%s%" PRId64 " WHERE ts>= %" PRIu64 ,
                    aggreFunc[j],
                    database->dbName,
                    stbInfo->childTblPrefix, i,
                    (uint64_t) DEFAULT_START_TIME);
            double    t = (double)toolsGetTimestampUs();
            int32_t code = -1;
            if (REST_IFACE == g_arguments->iface) {
                code = postProceSql(command, NULL, 0, REST_IFACE,
                                    0, g_arguments->port, 0,
                                    pThreadInfo->sockfd, NULL);
            } else {
                TAOS_RES *res = taos_query(taos, command);
                code = taos_errno(res);
                if (code != 0) {
                    printErrCmdCodeStr(command, code, res);
                    free(command);
                    return NULL;
                }
                while (taos_fetch_row(res) != NULL) {
                    count++;
                }
                taos_free_result(res);
            }

            t = toolsGetTimestampUs() - t;
            totalT += t;
        }
        if (fp) {
            fprintf(fp, "|%10s  |   %" PRId64 "   |  %12.2f   |   %10.2f  |\n",
                    (aggreFunc[j][0] == '*')
                        ?("   *   "):(aggreFunc[j]), totalData,
                    (double)(stbInfo->childTblCount*stbInfo->insertRows)/totalT,
                    totalT / 1000000);
        }
        infoPrint("<%s> took %.6f second(s)\n", command,
                  totalT / 1000000);
    }
    free(command);
    return NULL;
}

void queryAggrFunc() {
    pthread_t   read_id;
    threadInfo *pThreadInfo = benchCalloc(1, sizeof(threadInfo), false);
    if (NULL == pThreadInfo) {
        errorPrint("%s() failed to allocate memory\n", __func__);
        return;
    }
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    if (NULL == database) {
        errorPrint("%s() failed to get database\n", __func__);
        free(pThreadInfo);
        return;
    }
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    if (NULL == stbInfo) {
        errorPrint("%s() failed to get super table\n", __func__);
        free(pThreadInfo);
        return;
    }

    if (REST_IFACE != g_arguments->iface) {
        pThreadInfo->conn = initBenchConn();
        if (pThreadInfo->conn == NULL) {
            errorPrint("%s() failed to init connection\n", __func__);
            free(pThreadInfo);
            return;
        }
    } else {
        pThreadInfo->sockfd = createSockFd();
        if (pThreadInfo->sockfd < 0) {
            free(pThreadInfo);
            return;
        }
    }
    if (stbInfo->use_metric) {
        pthread_create(&read_id, NULL, queryStableAggrFunc, pThreadInfo);
    } else {
        pthread_create(&read_id, NULL, queryNtableAggrFunc, pThreadInfo);
    }
    pthread_join(read_id, NULL);
    if (REST_IFACE != g_arguments->iface) {
        closeBenchConn(pThreadInfo->conn);
    } else {
        if (pThreadInfo->sockfd) {
            destroySockFd(pThreadInfo->sockfd);
        }
    }
    free(pThreadInfo);
}
