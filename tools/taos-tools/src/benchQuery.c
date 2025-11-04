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

// query and get result  record is true to total request
int selectAndGetResult(qThreadInfo *pThreadInfo, char *command, bool record) {
    int ret = 0;

    // user cancel
    if (g_arguments->terminate) {
        return -1;
    }

    // execute sql
    uint32_t threadID = pThreadInfo->threadID;
    char dbName[TSDB_DB_NAME_LEN] = {0};
    TOOLS_STRNCPY(dbName, g_queryInfo.dbName, TSDB_DB_NAME_LEN);

    if (g_queryInfo.iface == REST_IFACE) {
        int retCode = postProcessSql(command, g_queryInfo.dbName, 0, REST_IFACE,
                                   0, g_arguments->port, false,
                                   pThreadInfo->sockfd, pThreadInfo->filePath);
        if (0 != retCode) {
            errorPrint("====restful return fail, threadID[%u]\n",
                       threadID);
            ret = -1;
        }
    } else {
        // query
        TAOS *taos = pThreadInfo->conn->taos;
        int64_t rows  = 0;
        TAOS_RES *res = taos_query(taos, command);
        int code = taos_errno(res);
        if (res == NULL || code) {
            // failed query
            errorPrint("failed to execute sql:%s, "
                        "code: 0x%08x, reason:%s\n",
                        command, code, taos_errstr(res));
            ret = -1;
        } else {
            // succ query
            if (record)
                rows = fetchResult(res, pThreadInfo->filePath);
        }

        // free result
        if (res) {
            taos_free_result(res);
        }
        debugPrint("query sql:%s rows:%"PRId64"\n", command, rows);
    }

    // record count
    if (ret ==0) {
        // succ
        if (record)
            pThreadInfo->nSucc ++;
    } else {
        // fail
        if (record)
            pThreadInfo->nFail ++;

        // continue option
        if (YES_IF_FAILED == g_arguments->continueIfFail) {
            ret = 0; // force continue
        }
    }

    return ret;
}

// interlligent sleep
int32_t autoSleep(uint64_t interval, uint64_t delay ) {
    int32_t msleep = 0;
    if (delay < interval * 1000) {
        msleep = (int32_t)((interval - delay/1000));
        infoPrint("do sleep %dms ...\n", msleep);
        toolsMsleep(msleep);  // ms
        debugPrint("%s\n","do sleep end");
    }
    return msleep;
}

// reset
int32_t resetQueryCache(qThreadInfo* pThreadInfo) {
    // execute sql
    if (selectAndGetResult(pThreadInfo, "RESET QUERY CACHE", false)) {
        errorPrint("%s() LN%d, reset query cache failed\n", __func__, __LINE__);
        return -1;
    }
    // succ
    return 0;
}



//
//  ---------------------------------  second levle funtion for Thread -----------------------------------
//

// show rela qps
int64_t showRealQPS(qThreadInfo* pThreadInfo, int64_t lastPrintTime, int64_t startTs) {
    int64_t now = toolsGetTimestampMs();
    if (now - lastPrintTime > 10 * 1000) {
        // real total
        uint64_t totalQueried = pThreadInfo->nSucc;
        if(g_arguments->continueIfFail == YES_IF_FAILED) {
            totalQueried += pThreadInfo->nFail;
        }
        infoPrint(
            "thread[%d] has currently completed queries: %" PRIu64 ", QPS: %10.3f\n",
            pThreadInfo->threadID, totalQueried,
            (double)(totalQueried / ((now - startTs) / 1000.0)));
        return now;
    } else {
        return lastPrintTime;
    }
}

// spec query mixed thread
static void *specQueryMixThread(void *sarg) {
    qThreadInfo *pThreadInfo = (qThreadInfo*)sarg;
#ifdef LINUX
    prctl(PR_SET_NAME, "specQueryMixThread");
#endif
    // use db
    if (g_queryInfo.dbName) {
        if (pThreadInfo->conn &&
            pThreadInfo->conn->taos &&
            taos_select_db(pThreadInfo->conn->taos, g_queryInfo.dbName)) {
                errorPrint("thread[%d]: failed to select database(%s)\n", pThreadInfo->threadID, g_queryInfo.dbName);
                return NULL;
        }
    }

    int64_t st = 0;
    int64_t et = 0;
    int64_t startTs        = toolsGetTimestampMs();
    int64_t lastPrintTime  = startTs;
    // batchQuery
    bool     batchQuery    = g_queryInfo.specifiedQueryInfo.batchQuery;
    uint64_t queryTimes    = batchQuery ? 1 : g_queryInfo.specifiedQueryInfo.queryTimes;
    uint64_t interval      = batchQuery ? 0 : g_queryInfo.specifiedQueryInfo.queryInterval;

    pThreadInfo->query_delay_list = benchArrayInit(queryTimes, sizeof(int64_t));
    for (int i = pThreadInfo->start_sql; i <= pThreadInfo->end_sql; ++i) {
        SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, i);
        for (int j = 0; j < queryTimes; ++j) {
            // use cancel
            if(g_arguments->terminate) {
                infoPrint("%s\n", "user cancel , so exit testing.");
                break;
            }

            // reset cache
            if (g_queryInfo.reset_query_cache) {
                if (resetQueryCache(pThreadInfo)) {
                    errorPrint("%s() LN%d, reset query cache failed\n", __func__, __LINE__);
                    return NULL;
                }
            }

            // execute sql
            st = toolsGetTimestampUs();
            int ret = selectAndGetResult(pThreadInfo, sql->command, true);
            if (ret) {
                g_fail = true;
                errorPrint("failed call mix selectAndGetResult, i=%d j=%d", i, j);
                return NULL;
            }
            et = toolsGetTimestampUs();

            // sleep
            if (interval > 0) {
                autoSleep(interval, et - st);
            }

            // delay
            if (ret == 0) {
                int64_t* delay = benchCalloc(1, sizeof(int64_t), false);
                *delay = et - st;
                debugPrint("%s() LN%d, delay: %"PRId64"\n", __func__, __LINE__, *delay);

                pThreadInfo->total_delay += *delay;
                if(benchArrayPush(pThreadInfo->query_delay_list, delay) == NULL){
                    tmfree(delay);
                }
            }

            // real show
            lastPrintTime = showRealQPS(pThreadInfo, lastPrintTime, startTs);
        }
    }

    return NULL;
}

// spec query thread
static void *specQueryThread(void *sarg) {
    qThreadInfo *pThreadInfo = (qThreadInfo *)sarg;
#ifdef LINUX
    prctl(PR_SET_NAME, "specQueryThread");
#endif
    uint64_t st = 0;
    uint64_t et = 0;
    int32_t  index = 0;

    // use db
    if (g_queryInfo.dbName) {
        if (pThreadInfo->conn &&
            pThreadInfo->conn->taos &&
            taos_select_db(pThreadInfo->conn->taos, g_queryInfo.dbName)) {
                errorPrint("thread[%d]: failed to select database(%s)\n", pThreadInfo->threadID, g_queryInfo.dbName);
                return NULL;
        }
    }

    uint64_t  queryTimes = g_queryInfo.specifiedQueryInfo.queryTimes;
    uint64_t  interval   = g_queryInfo.specifiedQueryInfo.queryInterval;
    pThreadInfo->query_delay_list = benchArrayInit(queryTimes, sizeof(int64_t));

    uint64_t  startTs       = toolsGetTimestampMs();
    uint64_t  lastPrintTime = startTs;

    SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, pThreadInfo->querySeq);

    if (sql->result[0] != '\0') {
        snprintf(pThreadInfo->filePath, MAX_PATH_LEN, "%s-%d",
                sql->result, pThreadInfo->threadID);
    }

    while (index < queryTimes) {
        // use cancel
        if(g_arguments->terminate) {
            infoPrint("thread[%d] user cancel , so exit testing.\n", pThreadInfo->threadID);
            break;
        }

        // reset cache
        if (g_queryInfo.reset_query_cache) {
            if (resetQueryCache(pThreadInfo)) {
                errorPrint("%s() LN%d, reset query cache failed\n", __func__, __LINE__);
                return NULL;
            }
        }

        // execute sql
        st = toolsGetTimestampUs();
        int ret = selectAndGetResult(pThreadInfo, sql->command, true);
        if (ret) {
            g_fail = true;
            errorPrint("failed call spec selectAndGetResult, index=%d\n", index);
            break;
        }
        et = toolsGetTimestampUs();

        // sleep
        if (interval > 0) {
            autoSleep(interval, et - st);
        }


        uint64_t delay = et - st;
        debugPrint("%s() LN%d, delay: %"PRIu64"\n", __func__, __LINE__, delay);

        if (ret == 0) {
            // only succ add delay list
            benchArrayPushNoFree(pThreadInfo->query_delay_list, &delay);
            pThreadInfo->total_delay += delay;
        }
        index++;

        // real show
        lastPrintTime = showRealQPS(pThreadInfo, lastPrintTime, startTs);
    }

    return NULL;
}

// super table query thread
static void *stbQueryThread(void *sarg) {
    char *sqlstr = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    qThreadInfo *pThreadInfo = (qThreadInfo *)sarg;
#ifdef LINUX
    prctl(PR_SET_NAME, "stbQueryThread");
#endif

    uint64_t st = 0;
    uint64_t et = 0;

    uint64_t queryTimes = g_queryInfo.superQueryInfo.queryTimes;
    uint64_t interval   = g_queryInfo.superQueryInfo.queryInterval;
    pThreadInfo->query_delay_list = benchArrayInit(queryTimes, sizeof(uint64_t));

    uint64_t startTs = toolsGetTimestampMs();
    uint64_t lastPrintTime = startTs;
    while (queryTimes--) {
        // use cancel
        if(g_arguments->terminate) {
            infoPrint("%s\n", "user cancel , so exit testing.");
            break;
        }

        // reset cache
        if (g_queryInfo.reset_query_cache) {
            if (resetQueryCache(pThreadInfo)) {
                errorPrint("%s() LN%d, reset query cache failed\n", __func__, __LINE__);
                return NULL;
            }
        }

        // execute
        st = toolsGetTimestampMs();
        // for each table
        for (int i = (int)pThreadInfo->start_table_from; i <= pThreadInfo->end_table_to; i++) {
            // use cancel
            if(g_arguments->terminate) {
                infoPrint("%s\n", "user cancel , so exit testing.");
                break;
            }

            // for each sql
            for (int j = 0; j < g_queryInfo.superQueryInfo.sqlCount; j++) {
                memset(sqlstr, 0, TSDB_MAX_ALLOWED_SQL_LEN);
                // use cancel
                if(g_arguments->terminate) {
                    infoPrint("%s\n", "user cancel , so exit testing.");
                    break;
                }

                // get real child name sql
                if (replaceChildTblName(g_queryInfo.superQueryInfo.sql[j], sqlstr, i)) {
                    // fault
                    tmfree(sqlstr);
                    return NULL;
                }

                if (g_queryInfo.superQueryInfo.result[j][0] != '\0') {
                    snprintf(pThreadInfo->filePath, MAX_PATH_LEN, "%s-%d",
                            g_queryInfo.superQueryInfo.result[j],
                            pThreadInfo->threadID);
                }

                // execute sql
                uint64_t s = toolsGetTimestampUs();
                int ret = selectAndGetResult(pThreadInfo, sqlstr, true);
                if (ret) {
                    // found error
                    errorPrint("failed call stb selectAndGetResult, i=%d j=%d\n", i, j);
                    g_fail = true;
                    tmfree(sqlstr);
                    return NULL;
                }
                uint64_t delay = toolsGetTimestampUs() - s;
                debugPrint("%s() LN%d, delay: %"PRIu64"\n", __func__, __LINE__, delay);
                if (ret == 0) {
                    // only succ add delay list
                    benchArrayPushNoFree(pThreadInfo->query_delay_list, &delay);
                    pThreadInfo->total_delay += delay;
                }

                // show real QPS
                lastPrintTime = showRealQPS(pThreadInfo, lastPrintTime, startTs);
            }
        }
        et = toolsGetTimestampMs();

        // sleep
        if (interval > 0) {
            autoSleep(interval, et - st);
        }

    }
    tmfree(sqlstr);

    return NULL;
}

//
// ---------------------------------  firse level function ------------------------------
//

void totalChildQuery(qThreadInfo* infos, int threadCnt, int64_t spend, BArray *pDelays) {
    // valid check
    if (infos == NULL || threadCnt == 0) {
        return ;
    }

    // statistic
    BArray * delay_list = benchArrayInit(1, sizeof(int64_t));
    double total_delays = 0;

    // clear
    for (int i = 0; i < threadCnt; ++i) {
        qThreadInfo * pThreadInfo = infos + i;
        if(pThreadInfo->query_delay_list == NULL) {
            continue;;
        }

        // append delay
        benchArrayAddBatch(delay_list, pThreadInfo->query_delay_list->pData,
                pThreadInfo->query_delay_list->size, false);
        total_delays += pThreadInfo->total_delay;

        // free delay
        benchArrayDestroy(pThreadInfo->query_delay_list);
        pThreadInfo->query_delay_list = NULL;

    }

    // succ is zero
    if (delay_list->size == 0) {
        errorPrint("%s", "succ queries count is zero.\n");
        benchArrayDestroy(delay_list);
        return ;
    }


    // sort
    qsort(delay_list->pData, delay_list->size, delay_list->elemSize, compare);

    size_t totalQueried = delay_list->size;
    double time_cost = spend / 1E6;
    double qps = (time_cost > 0) ? (totalQueried / time_cost) : 0.0;
    double avgDelay = (double)total_delays/delay_list->size/1E6;
    double minDelay = *(int64_t *)(benchArrayGet(delay_list, 0))/1E6;
    double maxDelay = *(int64_t *)(benchArrayGet(delay_list, (int32_t)(delay_list->size - 1)))/1E6;
    double p90      = *(int64_t *)(benchArrayGet(delay_list, (int32_t)(delay_list->size * 0.90)))/1E6;
    double p95      = *(int64_t *)(benchArrayGet(delay_list, (int32_t)(delay_list->size * 0.95)))/1E6;
    double p99      = *(int64_t *)(benchArrayGet(delay_list, (int32_t)(delay_list->size * 0.99)))/1E6;

    // show delay min max
    if (delay_list->size) {
        infoPrint(
                "spend %.6fs using "
                "%d threads complete query %zu times,  "
                "min delay: %.6fs, "
                "avg delay: %.6fs, "
                "p90: %.6fs, "
                "p95: %.6fs, "
                "p99: %.6fs, "
                "max: %.6fs\n",
                time_cost,
                threadCnt, totalQueried,
                minDelay,
                avgDelay,
                p90,
                p95,
                p99,
                maxDelay);
    }

    // output json for super table query
    if (g_queryInfo.superQueryInfo.sqlCount > 0) {
        tools_cJSON *root = NULL;
        tools_cJSON *result_array = NULL;
        if (g_arguments->output_json_file) {
            root = tools_cJSON_CreateObject();
            if (root != NULL) {
                result_array = tools_cJSON_CreateArray();
                if (result_array != NULL) {
                    tools_cJSON_AddItemToObject(root, "results", result_array);
                } else {
                    errorPrint("Failed to create result_array JSON object\n");
                    tools_cJSON_Delete(root);
                    root = NULL;
                }
            }
        }

        if (result_array) {
            tools_cJSON *sqlResult = tools_cJSON_CreateObject();
            if (sqlResult) {
                tools_cJSON_AddNumberToObject(sqlResult, "threads", threadCnt);
                tools_cJSON_AddNumberToObject(sqlResult, "total_queries", totalQueried);
                tools_cJSON_AddNumberToObject(sqlResult, "time_cost", time_cost);
                tools_cJSON_AddNumberToObject(sqlResult, "qps", qps);
                tools_cJSON_AddNumberToObject(sqlResult, "avg", avgDelay);
                tools_cJSON_AddNumberToObject(sqlResult, "min", minDelay);
                tools_cJSON_AddNumberToObject(sqlResult, "max", maxDelay);
                tools_cJSON_AddNumberToObject(sqlResult, "p90", p90);
                tools_cJSON_AddNumberToObject(sqlResult, "p95", p95);
                tools_cJSON_AddNumberToObject(sqlResult, "p99", p99);
                tools_cJSON_AddItemToArray(result_array, sqlResult);
            } else {
                errorPrint("Failed to create JSON object for SQL result.\n");
            }
        }

        if (root) {
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
                jsonStr = NULL;
            }
            tools_cJSON_Delete(root);
            root = NULL;
        }
    }

    // copy to another
    if (pDelays) {
        benchArrayAddBatch(pDelays, delay_list->pData, delay_list->size, false);
    }
    benchArrayDestroy(delay_list);
}

//
// super table query
//
static int stbQuery(uint16_t iface, char* dbName) {
    int ret = -1;
    pthread_t * pidsOfSub   = NULL;
    qThreadInfo *threadInfos = NULL;
    g_queryInfo.superQueryInfo.totalQueried = 0;
    g_queryInfo.superQueryInfo.totalFail    = 0;

    // check
    if ((g_queryInfo.superQueryInfo.sqlCount == 0)
        || (g_queryInfo.superQueryInfo.threadCnt == 0)) {
        return 0;
    }

    // malloc
    pidsOfSub = benchCalloc(1, g_queryInfo.superQueryInfo.threadCnt
                            *sizeof(pthread_t),
                            false);
    threadInfos = benchCalloc(1, g_queryInfo.superQueryInfo.threadCnt
                                *sizeof(qThreadInfo), false);

    int64_t ntables = g_queryInfo.superQueryInfo.childTblCount;
    int nConcurrent = g_queryInfo.superQueryInfo.threadCnt;

    int64_t a = ntables / nConcurrent;
    if (a < 1) {
        nConcurrent = (int)ntables;
        a = 1;
    }

    int64_t b = 0;
    if (nConcurrent != 0) {
        b = ntables % nConcurrent;
    }

    uint64_t tableFrom = 0;
    int threadCnt = 0;
    for (int i = 0; i < nConcurrent; i++) {
        qThreadInfo *pThreadInfo = threadInfos + i;
        pThreadInfo->dbName = dbName;
        pThreadInfo->threadID = i;
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to =
                i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        // create conn
        if (initQueryConn(pThreadInfo, iface)){
            break;
        }
        int code = pthread_create(pidsOfSub + i, NULL, stbQueryThread, pThreadInfo);
        if (code != 0) {
            errorPrint("failed stbQueryThread create. error code =%d \n", code);
            break;
        }
        threadCnt ++;
    }

    bool needExit = false;
    // if failed, set termainte flag true like ctrl+c exit
    if (threadCnt != nConcurrent  ) {
        needExit = true;
        g_arguments->terminate = true;
        goto OVER;
    }

    // reset total
    g_queryInfo.superQueryInfo.totalQueried = 0;
    g_queryInfo.superQueryInfo.totalFail    = 0;

    // real thread count
    g_queryInfo.superQueryInfo.threadCnt = threadCnt;
    int64_t start = toolsGetTimestampUs();

    for (int i = 0; i < threadCnt; i++) {
        pthread_join(pidsOfSub[i], NULL);
        qThreadInfo *pThreadInfo = threadInfos + i;
        // add succ
        g_queryInfo.superQueryInfo.totalQueried += pThreadInfo->nSucc;
        if (g_arguments->continueIfFail == YES_IF_FAILED) {
            // "yes" need add fail cnt
            g_queryInfo.superQueryInfo.totalQueried += pThreadInfo->nFail;
            g_queryInfo.superQueryInfo.totalFail    += pThreadInfo->nFail;
        }

        // close conn
        closeQueryConn(pThreadInfo, iface);
    }
    int64_t end = toolsGetTimestampUs();

    if (needExit) {
        goto OVER;
    }

    // total show
    totalChildQuery(threadInfos, threadCnt, end - start, NULL);

    ret = 0;

OVER:
    tmfree((char *)pidsOfSub);
    tmfree((char *)threadInfos);

    for (int64_t i = 0; i < g_queryInfo.superQueryInfo.childTblCount; ++i) {
        tmfree(g_queryInfo.superQueryInfo.childTblName[i]);
    }
    tmfree(g_queryInfo.superQueryInfo.childTblName);
    return ret;
}

//
// specQuery
//
static int specQuery(uint16_t iface, char* dbName) {
    int ret = -1;
    pthread_t    *pids = NULL;
    qThreadInfo *infos = NULL;
    int    nConcurrent = g_queryInfo.specifiedQueryInfo.concurrent;
    uint64_t nSqlCount = g_queryInfo.specifiedQueryInfo.sqls->size;
    g_queryInfo.specifiedQueryInfo.totalQueried = 0;
    g_queryInfo.specifiedQueryInfo.totalFail    = 0;

    // check invaid
    if(nSqlCount == 0 || nConcurrent == 0 ) {
        if(nSqlCount == 0)
           warnPrint("specified table query sql count is %" PRIu64 ".\n", nSqlCount);
        if(nConcurrent == 0)
           warnPrint("nConcurrent is %d , specified_table_query->nConcurrent is zero. \n", nConcurrent);
        return 0;
    }

    // malloc threads memory
    pids  = benchCalloc(1, nConcurrent * sizeof(pthread_t),  false);
    infos = benchCalloc(1, nConcurrent * sizeof(qThreadInfo), false);

    tools_cJSON *root = NULL;
    tools_cJSON *result_array = NULL;
    if (g_arguments->output_json_file) {
       root = tools_cJSON_CreateObject();
       if (root != NULL) {
            result_array = tools_cJSON_CreateArray();
            tools_cJSON_AddItemToObject(root, "results", result_array);
       }
    }

    for (uint64_t i = 0; i < nSqlCount; i++) {
        if( g_arguments->terminate ) {
            break;
        }

        // reset
        memset(pids,  0, nConcurrent * sizeof(pthread_t));
        memset(infos, 0, nConcurrent * sizeof(qThreadInfo));

        // get execute sql
        SSQL *sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, i);

        // create threads
        int threadCnt = 0;
        for (int j = 0; j < nConcurrent; j++) {
           qThreadInfo *pThreadInfo = infos + j;
           pThreadInfo->threadID = i * nConcurrent + j;
           pThreadInfo->querySeq = i;
           pThreadInfo->dbName = dbName;

           // create conn
           if (initQueryConn(pThreadInfo, iface)) {
               break;
           }

           int code = pthread_create(pids + j, NULL, specQueryThread, pThreadInfo);
           if (code != 0) {
               errorPrint("failed specQueryThread create. error code =%d \n", code);
               break;
           }
           threadCnt++;
        }

        bool needExit = false;
        // if failed, set termainte flag true like ctrl+c exit
        if (threadCnt != nConcurrent  ) {
            needExit = true;
            g_arguments->terminate = true;
        }

        int64_t start = toolsGetTimestampUs();
        // wait threads execute finished one by one
        for (int j = 0; j < threadCnt ; j++) {
           pthread_join(pids[j], NULL);
           qThreadInfo *pThreadInfo = infos + j;
           closeQueryConn(pThreadInfo, iface);

           // need exit in loop
           if (needExit) {
                // free BArray
                benchArrayDestroy(pThreadInfo->query_delay_list);
                pThreadInfo->query_delay_list = NULL;
           }
        }
        int64_t spend = toolsGetTimestampUs() - start;
        if(spend == 0) {
            // avoid xx/spend expr throw error
            spend = 1;
        }

        // create
        if (needExit) {
            errorPrint("failed to create thread. expect nConcurrent=%d real threadCnt=%d,  exit testing.\n", nConcurrent, threadCnt);
            goto OVER;
        }

        //
        // show QPS and P90 ...
        //
        uint64_t n = 0;
        double  total_delays = 0.0;
        uint64_t totalQueried = 0;
        uint64_t totalFail    = 0;
        for (int j = 0; j < threadCnt; j++) {
           qThreadInfo *pThreadInfo = infos + j;
           if(pThreadInfo->query_delay_list == NULL) {
                continue;;
           }

           // total one sql
           for (uint64_t k = 0; k < pThreadInfo->query_delay_list->size; k++) {
                int64_t * delay = benchArrayGet(pThreadInfo->query_delay_list, k);
                sql->delay_list[n++] = *delay;
                total_delays += *delay;
           }

           // total queries
           totalQueried += pThreadInfo->nSucc;
           if (g_arguments->continueIfFail == YES_IF_FAILED) {
                totalQueried += pThreadInfo->nFail;
                totalFail    += pThreadInfo->nFail;
           }

           // free BArray query_delay_list
           benchArrayDestroy(pThreadInfo->query_delay_list);
           pThreadInfo->query_delay_list = NULL;
        }

        // appand current sql
        g_queryInfo.specifiedQueryInfo.totalQueried += totalQueried;
        g_queryInfo.specifiedQueryInfo.totalFail    += totalFail;

        // succ is zero
        if(totalQueried == 0 || n == 0) {
            errorPrint("%s", "succ queries count is zero.\n");
            goto OVER;
        }

        double time_cost = spend / 1E6;
        double qps = totalQueried / time_cost;
        double avgDelay = total_delays / n / 1E6;
        double minDelay = sql->delay_list[0] / 1E6;
        double maxDelay = sql->delay_list[n - 1] / 1E6;
        double p90 = sql->delay_list[(uint64_t)(n * 0.90)] / 1E6;
        double p95 = sql->delay_list[(uint64_t)(n * 0.95)] / 1E6;
        double p99 = sql->delay_list[(uint64_t)(n * 0.99)] / 1E6;

        qsort(sql->delay_list, n, sizeof(uint64_t), compare);
        int32_t bufLen = strlen(sql->command) + 512;
        char * buf = benchCalloc(bufLen, sizeof(char), false);
        snprintf(buf , bufLen, "complete query with %d threads and %" PRIu64 " "
                             "sql %"PRIu64" spend %.6fs QPS: %.3f "
                             "query delay "
                             "avg: %.6fs "
                             "min: %.6fs "
                             "max: %.6fs "
                             "p90: %.6fs "
                             "p95: %.6fs "
                             "p99: %.6fs "
                             "SQL command: %s \n",
                             threadCnt, totalQueried,
                             i + 1, time_cost, qps,
                             avgDelay, minDelay, maxDelay, p90, p95, p99,
                             sql->command);

        if (result_array) {
            tools_cJSON *sqlResult = tools_cJSON_CreateObject();
            tools_cJSON_AddNumberToObject(sqlResult, "threads", threadCnt);
            tools_cJSON_AddNumberToObject(sqlResult, "total_queries", totalQueried);
            tools_cJSON_AddNumberToObject(sqlResult, "time_cost", time_cost);
            tools_cJSON_AddNumberToObject(sqlResult, "qps", qps);
            tools_cJSON_AddNumberToObject(sqlResult, "avg", avgDelay);
            tools_cJSON_AddNumberToObject(sqlResult, "min", minDelay);
            tools_cJSON_AddNumberToObject(sqlResult, "max", maxDelay);
            tools_cJSON_AddNumberToObject(sqlResult, "p90", p90);
            tools_cJSON_AddNumberToObject(sqlResult, "p95", p95);
            tools_cJSON_AddNumberToObject(sqlResult, "p99", p99);
            tools_cJSON_AddItemToArray(result_array, sqlResult);
        }

        infoPrintNoTimestamp("%s", buf);
        infoPrintNoTimestampToFile("%s", buf);
        tmfree(buf);
    }

    if (root) {
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

    ret = 0;

OVER:
    tmfree((char *)pids);
    tmfree((char *)infos);

    // free specialQueryInfo
    freeSpecialQueryInfo();
    return ret;
}

//
// specQueryMix
//
static int specQueryMix(uint16_t iface, char* dbName) {
    // init
    int ret            = -1;
    int nConcurrent    = g_queryInfo.specifiedQueryInfo.concurrent;
    pthread_t * pids   = benchCalloc(nConcurrent, sizeof(pthread_t), true);
    qThreadInfo *infos = benchCalloc(nConcurrent, sizeof(qThreadInfo), true);

    // concurent calc
    int total_sql_num = g_queryInfo.specifiedQueryInfo.sqls->size;
    int start_sql     = 0;
    int a             = total_sql_num / nConcurrent;
    if (a < 1) {
        warnPrint("sqls num:%d < concurent:%d, so set concurrent to %d\n", total_sql_num, nConcurrent, nConcurrent);
        nConcurrent = total_sql_num;
        a = 1;
    }
    int b = 0;
    if (nConcurrent != 0) {
        b = total_sql_num % nConcurrent;
    }

    //
    // running
    //
    int threadCnt = 0;
    for (int i = 0; i < nConcurrent; ++i) {
        qThreadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID    = i;
        pThreadInfo->start_sql   = start_sql;
        pThreadInfo->end_sql     = i < b ? start_sql + a : start_sql + a - 1;
        start_sql = pThreadInfo->end_sql + 1;
        pThreadInfo->total_delay = 0;
        pThreadInfo->dbName = dbName;

        // create conn
        if (initQueryConn(pThreadInfo, iface)){
            break;
        }
        // main run
        int code = pthread_create(pids + i, NULL, specQueryMixThread, pThreadInfo);
        if (code != 0) {
            errorPrint("failed specQueryMixThread create. error code =%d \n", code);
            break;
        }

        threadCnt ++;
    }

    bool needExit = false;
    // if failed, set termainte flag true like ctrl+c exit
    if (threadCnt != nConcurrent) {
        needExit = true;
        g_arguments->terminate = true;
    }

    // reset total
    g_queryInfo.specifiedQueryInfo.totalQueried = 0;
    g_queryInfo.specifiedQueryInfo.totalFail    = 0;

    int64_t start = toolsGetTimestampUs();
    for (int i = 0; i < threadCnt; ++i) {
        pthread_join(pids[i], NULL);
        qThreadInfo *pThreadInfo = infos + i;
        closeQueryConn(pThreadInfo, iface);

        // total queries
        g_queryInfo.specifiedQueryInfo.totalQueried += pThreadInfo->nSucc;
        if (g_arguments->continueIfFail == YES_IF_FAILED) {
            // yes need add failed count
            g_queryInfo.specifiedQueryInfo.totalQueried += pThreadInfo->nFail;
            g_queryInfo.specifiedQueryInfo.totalFail    += pThreadInfo->nFail;
        }

        // destory
        if (needExit) {
            benchArrayDestroy(pThreadInfo->query_delay_list);
            pThreadInfo->query_delay_list = NULL;
        }
    }
    int64_t end = toolsGetTimestampUs();

    // create
    if (needExit) {
        errorPrint("failed to create thread. expect nConcurrent=%d real threadCnt=%d,  exit testing.\n", nConcurrent, threadCnt);
        goto OVER;
    }

    // statistic
    totalChildQuery(infos, threadCnt, end - start, NULL);
    ret = 0;

OVER:
    tmfree(pids);
    tmfree(infos);

    // free sqls
    freeSpecialQueryInfo();

    return ret;
}

void totalBatchQuery(int32_t allSleep, BArray *pDelays) {
    // sort
    qsort(pDelays->pData, pDelays->size, pDelays->elemSize, compare);

    // total delays
    double totalDelays = 0;
    for (size_t i = 0; i < pDelays->size; i++) {
        int64_t *delay = benchArrayGet(pDelays, i);
        totalDelays   += *delay;
    }

    printf("\n");
    // show sleep times
    if (allSleep > 0) {
        infoPrint("All sleep spend: %.3fs\n", (float)allSleep/1000);
    }

    // show P90 ...
    if (pDelays->size) {
        infoPrint(
                "Total delay: "
                "min delay: %.6fs, "
                "avg delay: %.6fs, "
                "p90: %.6fs, "
                "p95: %.6fs, "
                "p99: %.6fs, "
                "max: %.6fs\n",
                *(int64_t *)(benchArrayGet(pDelays, 0))/1E6,
                (double)totalDelays/pDelays->size/1E6,
                *(int64_t *)(benchArrayGet(pDelays,
                                    (int32_t)(pDelays->size * 0.9)))/1E6,
                *(int64_t *)(benchArrayGet(pDelays,
                                    (int32_t)(pDelays->size * 0.95)))/1E6,
                *(int64_t *)(benchArrayGet(pDelays,
                                    (int32_t)(pDelays->size * 0.99)))/1E6,
                *(int64_t *)(benchArrayGet(pDelays,
                                    (int32_t)(pDelays->size - 1)))/1E6);
    }
}

//
// specQuery Mix Batch
//
static int specQueryBatch(uint16_t iface, char* dbName) {
    // init
    BArray *pDelays    = NULL;
    int ret            = -1;
    int nConcurrent    = g_queryInfo.specifiedQueryInfo.concurrent;
    uint64_t interval  = g_queryInfo.specifiedQueryInfo.queryInterval;
    pthread_t * pids   = benchCalloc(nConcurrent, sizeof(pthread_t), true);
    qThreadInfo *infos = benchCalloc(nConcurrent, sizeof(qThreadInfo), true);
    infoPrint("start batch query, sleep interval:%" PRIu64 "ms query times:%" PRIu64 " thread:%d \n",
        interval, g_queryInfo.query_times, nConcurrent);

    // concurent calc
    int total_sql_num = g_queryInfo.specifiedQueryInfo.sqls->size;
    int start_sql     = 0;
    int a             = total_sql_num / nConcurrent;
    if (a < 1) {
        warnPrint("sqls num:%d < concurent:%d, set concurrent %d\n", total_sql_num, nConcurrent, nConcurrent);
        nConcurrent = total_sql_num;
        a = 1;
    }
    int b = 0;
    if (nConcurrent != 0) {
        b = total_sql_num % nConcurrent;
    }

    //
    // connect
    //
    int connCnt = 0;
    for (int i = 0; i < nConcurrent; ++i) {
        qThreadInfo *pThreadInfo = infos + i;
        pThreadInfo->dbName = dbName;
        // create conn
        if (initQueryConn(pThreadInfo, iface)){
            ret = -1;
            goto OVER;
        }

        connCnt ++;
    }


    // reset total
    g_queryInfo.specifiedQueryInfo.totalQueried = 0;
    g_queryInfo.specifiedQueryInfo.totalFail    = 0;

    //
    // running
    //
    int threadCnt = 0;
    int allSleep  = 0;
    pDelays       = benchArrayInit(10, sizeof(int64_t));
    for (int m = 0; m < g_queryInfo.query_times; ++m) {
        // reset
        threadCnt = 0;
        start_sql = 0;

        // create thread
        for (int i = 0; i < nConcurrent; ++i) {
            qThreadInfo *pThreadInfo = infos + i;
            pThreadInfo->threadID    = i;
            pThreadInfo->start_sql   = start_sql;
            pThreadInfo->end_sql     = i < b ? start_sql + a : start_sql + a - 1;
            start_sql = pThreadInfo->end_sql + 1;
            pThreadInfo->total_delay = 0;
            // total zero
            pThreadInfo->nSucc = 0;
            pThreadInfo->nFail = 0;

            // main run
            int code = pthread_create(pids + i, NULL, specQueryMixThread, pThreadInfo);
            if (code != 0) {
                errorPrint("failed specQueryBatchThread create. error code =%d \n", code);
                break;
            }

            threadCnt ++;
        }

        bool needExit = false;
        if (threadCnt != nConcurrent) {
            // if failed, set termainte flag true like ctrl+c exit
            needExit = true;
            g_arguments->terminate = true;
        }

        // wait thread finished
        int64_t start = toolsGetTimestampUs();
        for (int i = 0; i < threadCnt; ++i) {
            pthread_join(pids[i], NULL);
            qThreadInfo *pThreadInfo = infos + i;
            // total queries
            g_queryInfo.specifiedQueryInfo.totalQueried += pThreadInfo->nSucc;
            if (g_arguments->continueIfFail == YES_IF_FAILED) {
                // yes need add failed count
                g_queryInfo.specifiedQueryInfo.totalQueried += pThreadInfo->nFail;
                g_queryInfo.specifiedQueryInfo.totalFail    += pThreadInfo->nFail;
            }

            // destory
            if (needExit) {
                benchArrayDestroy(pThreadInfo->query_delay_list);
                pThreadInfo->query_delay_list = NULL;
            }
        }
        int64_t end = toolsGetTimestampUs();

        // create
        if (needExit) {
            errorPrint("failed to create thread. expect nConcurrent=%d real threadCnt=%d,  exit testing.\n", nConcurrent, threadCnt);
            goto OVER;
        }

        // batch total
        printf("\n");
        totalChildQuery(infos, threadCnt, end - start, pDelays);

        // show batch total
        int64_t delay = end - start;
        infoPrint("count:%d execute batch spend: %" PRId64 "ms\n", m + 1, delay/1000);

        // sleep
        if ( g_queryInfo.specifiedQueryInfo.batchQuery && interval > 0) {
            allSleep += autoSleep(interval, delay);
        }

        // check cancel
        if(g_arguments->terminate) {
            break;
        }
    }
    ret = 0;

    // all total
    totalBatchQuery(allSleep, pDelays);

OVER:
    // close conn
    for (int i = 0; i < connCnt; ++i) {
        qThreadInfo *pThreadInfo = infos + i;
        closeQueryConn(pThreadInfo, iface);
    }

    // free threads
    tmfree(pids);
    tmfree(infos);

    // free sqls
    freeSpecialQueryInfo();

    // free delays
    if (pDelays) {
        benchArrayDestroy(pDelays);
    }

    return ret;
}

// total query for end
void totalQuery(int64_t spends) {
    // total QPS
    uint64_t totalQueried = g_queryInfo.specifiedQueryInfo.totalQueried
        + g_queryInfo.superQueryInfo.totalQueried;

    // error rate
    char errRate[128] = "";
    if(g_arguments->continueIfFail == YES_IF_FAILED) {
        uint64_t totalFail = g_queryInfo.specifiedQueryInfo.totalFail + g_queryInfo.superQueryInfo.totalFail;
        if (totalQueried > 0) {
            snprintf(errRate, sizeof(errRate), " ,error %" PRIu64 " (rate:%.3f%%)", totalFail, ((float)totalFail * 100)/totalQueried);
        }
    }

    // show
    double  tInS = (double)spends / 1000;
    char buf[512] = "";
    snprintf(buf, sizeof(buf),
                "Spend %.4f second completed total queries: %" PRIu64
                ", the QPS of all threads: %10.3f%s\n\n",
                tInS, totalQueried, (double)totalQueried / tInS, errRate);
    infoPrint("%s", buf);
    infoPrintToFile("%s", buf);
}

int queryTestProcess() {
    prompt(0);

    // covert addr
    if (g_queryInfo.iface == REST_IFACE) {
        encodeAuthBase64();
        char *host = g_arguments->host          ? g_arguments->host : DEFAULT_HOST;
        int   port = g_arguments->port_inputted ? g_arguments->port : DEFAULT_REST_PORT;
        if (convertHostToServAddr(host,
                    port,
                    &(g_arguments->serv_addr)) != 0) {
            errorPrint("%s", "convert host to server address\n");
            return -1;
        }
    }

    // kill sql for executing seconds over "kill_slow_query_threshold"
    if (g_queryInfo.iface == TAOSC_IFACE && g_queryInfo.killQueryThreshold) {
        int32_t ret = killSlowQuery();
        if (ret != 0) {
            return ret;
        }
    }

    // fetch child name if super table
    if ((g_queryInfo.superQueryInfo.sqlCount > 0) &&
            (g_queryInfo.superQueryInfo.threadCnt > 0)) {
        int32_t ret = fetchChildTableName(g_queryInfo.dbName, g_queryInfo.superQueryInfo.stbName);
        if (ret != 0) {
            errorPrint("fetchChildTableName dbName=%s stb=%s failed.", g_queryInfo.dbName, g_queryInfo.superQueryInfo.stbName);
            return -1;
        }
    }

    //
    // start running
    //

    uint64_t startTs = toolsGetTimestampMs();
    if(g_queryInfo.specifiedQueryInfo.sqls && g_queryInfo.specifiedQueryInfo.sqls->size > 0) {
        // specified table
        if (g_queryInfo.specifiedQueryInfo.mixed_query) {
            // mixed
            if(g_queryInfo.specifiedQueryInfo.batchQuery) {
                if (specQueryBatch(g_queryInfo.iface, g_queryInfo.dbName)) {
                    return -1;
                }
            } else {
                if (specQueryMix(g_queryInfo.iface, g_queryInfo.dbName)) {
                    return -1;
                }
            }
        } else {
            // no mixied
            if (specQuery(g_queryInfo.iface, g_queryInfo.dbName)) {
                return -1;
            }
        }
    } else if(g_queryInfo.superQueryInfo.sqlCount > 0) {
        // super table
        if (stbQuery(g_queryInfo.iface, g_queryInfo.dbName)) {
            return -1;
        }
    } else {
        // nothing
        errorPrint("%s\n", "Both 'specified_table_query' and 'super_table_query' sqls is empty.");
        return -1;
    }

    // total
    totalQuery(toolsGetTimestampMs() - startTs);
    return g_fail ? -1 : 0;
}
