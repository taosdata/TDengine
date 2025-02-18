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
#include <benchCsv.h>
#include <toolsdef.h>

SArguments*    g_arguments;
SQueryMetaInfo g_queryInfo;
STmqMetaInfo   g_tmqInfo;
bool           g_fail = false;
uint64_t       g_memoryUsage = 0;
tools_cJSON*   root;

#define CLIENT_INFO_LEN   20
static char     g_client_info[CLIENT_INFO_LEN] = {0};

int32_t         g_majorVersionOfClient = 0;
// set flag if command passed, see ARG_OPT_ ???
uint64_t        g_argFlag = 0;

#ifdef LINUX
void benchQueryInterruptHandler(int32_t signum, void* sigingo, void* context) {
    infoPrint("%s", "Receive SIGINT or other signal, quit benchmark\n");
    sem_post(&g_arguments->cancelSem);
}

void* benchCancelHandler(void* arg) {
    if (bsem_wait(&g_arguments->cancelSem) != 0) {
        toolsMsleep(10);
    }

    g_arguments->terminate = true;
    toolsMsleep(10);

    return NULL;
}
#endif

void checkArgumentValid() {
     // check prepared_rand valid
    if(g_arguments->prepared_rand < g_arguments->reqPerReq) {
        infoPrint("prepared_rand(%"PRIu64") < num_of_records_per_req(%d), so set num_of_records_per_req = prepared_rand\n", 
                   g_arguments->prepared_rand, g_arguments->reqPerReq);
        g_arguments->reqPerReq = g_arguments->prepared_rand;
    }

    if(g_arguments->host == NULL) {
        g_arguments->host = DEFAULT_HOST;
    }

    if (isRest(g_arguments->iface)) {
        if (0 != convertServAddr(g_arguments->iface,
                                 false,
                                 1)) {
            errorPrint("%s", "Failed to convert server address\n");
            return;
        }
        encodeAuthBase64();
        g_arguments->rest_server_ver_major =
            getServerVersionRest(g_arguments->port);
    }

}

int main(int argc, char* argv[]) {
    int ret = 0;

    // log
    initLog();
    initArgument();
    srand(time(NULL)%1000000);

    snprintf(g_client_info, CLIENT_INFO_LEN, "%s", taos_get_client_info());
    g_majorVersionOfClient = atoi(g_client_info);
    debugPrint("Client info: %s, major version: %d\n",
            g_client_info,
            g_majorVersionOfClient);

#ifdef LINUX
    if (sem_init(&g_arguments->cancelSem, 0, 0) != 0) {
        errorPrint("%s", "failed to create cancel semaphore\n");
        exit(EXIT_FAILURE);
    }
    pthread_t spid = {0};
    pthread_create(&spid, NULL, benchCancelHandler, NULL);

    benchSetSignal(SIGINT, benchQueryInterruptHandler);

#endif
    if (benchParseArgs(argc, argv)) {
        exitLog();
        return -1;
    }
#ifdef WEBSOCKET
    if (g_arguments->debug_print) {
        ws_enable_log("info");
    }

    if (g_arguments->dsn != NULL) {
        g_arguments->websocket = true;
        infoPrint("set websocket true from dsn not empty. dsn=%s\n", g_arguments->dsn);
    } else {
        char * dsn = getenv("TDENGINE_CLOUD_DSN");
        if (dsn != NULL && strlen(dsn) > 3) {
            g_arguments->dsn = dsn;
            g_arguments->websocket = true;
            infoPrint("set websocket true from getenv TDENGINE_CLOUD_DSN=%s\n", g_arguments->dsn);
        } else {
            g_arguments->dsn = false;
        }
    }
#endif
    if (g_arguments->metaFile) {
        g_arguments->totalChildTables = 0;
        if (readJsonConfig(g_arguments->metaFile)) {
            errorPrint("failed to readJsonConfig %s\n", g_arguments->metaFile);
            exitLog();
            return -1;
        }
    } else {
        modifyArgument();
    }

    if(g_arguments->output_file[0] == 0) {
        infoPrint("%s","result_file is empty, ignore output.");
        g_arguments->fpOfInsertResult = NULL;
    } else {
        g_arguments->fpOfInsertResult = fopen(g_arguments->output_file, "a");
        if (NULL == g_arguments->fpOfInsertResult) {
            errorPrint("failed to open %s for save result\n",
                    g_arguments->output_file);
        }
    }

    infoPrint("client version: %s\n", taos_get_client_info());
    checkArgumentValid();

    if (g_arguments->test_mode == INSERT_TEST) {
        if (insertTestProcess()) {
            errorPrint("%s", "insert test process failed\n");
            ret = -1;
        }
    } else if (g_arguments->test_mode == CSVFILE_TEST) {
        if (csvTestProcess()) {
            errorPrint("%s", "query test process failed\n");
            ret = -1;
        }
    } else if (g_arguments->test_mode == QUERY_TEST) {
        if (queryTestProcess()) {
            errorPrint("%s", "query test process failed\n");
            ret = -1;
        }
    } else if (g_arguments->test_mode == SUBSCRIBE_TEST) {
        if (subscribeTestProcess()) {
            errorPrint("%s", "sub test process failed\n");
            ret = -1;
        }
    }

    if ((ret == 0) && g_arguments->aggr_func) {
        queryAggrFunc();
    }
    postFreeResource();

#ifdef LINUX
    pthread_cancel(spid);
    pthread_join(spid, NULL);
#endif

    exitLog();
    return ret;
}
