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
bool           g_stopping = false;
uint64_t       g_memoryUsage = 0;
tools_cJSON*   root;
extern char    g_configDir[MAX_PATH_LEN];

#define CLIENT_INFO_LEN   20
static char     g_client_info[CLIENT_INFO_LEN] = {0};

int32_t         g_majorVersionOfClient = 0;
// set flag if command passed, see ARG_OPT_ ???
uint64_t        g_argFlag = 0;

#ifdef LINUX
void benchQueryInterruptHandler(int32_t signum, void* sigingo, void* context) {
    infoPrint("%s", "Receive SIGINT or other signal, quit benchmark\n");
    if (g_stopping) {
        infoPrint("%s", "Benchmark process forced exit!\n");
        exit(1);
    }
    
    sem_post(&g_arguments->cancelSem);
    g_stopping = true;
}

void* benchCancelHandler(void* arg) {
    if (bsem_wait(&g_arguments->cancelSem) != 0) {
        toolsMsleep(10);
    }

    g_arguments->terminate = true;
    toolsMsleep(5 * 1000);

    exit(1);
}
#endif

int checkArgumentValid() {
     // check prepared_rand valid
    if(g_arguments->prepared_rand < g_arguments->reqPerReq) {
        infoPrint("prepared_rand(%"PRIu64") < num_of_records_per_req(%d), so set num_of_records_per_req = prepared_rand\n", 
                   g_arguments->prepared_rand, g_arguments->reqPerReq);
        g_arguments->reqPerReq = g_arguments->prepared_rand;
    }

    if (isRest(g_arguments->iface)) {
        if (0 != convertServAddr(g_arguments->iface,
                                 false,
                                 1)) {
            errorPrint("%s", "Failed to convert server address\n");
            return -1;
        }
        encodeAuthBase64();
        g_arguments->rest_server_ver_major =
            getServerVersionRest(g_arguments->port);
    }    

    // check batch query
    if (g_arguments->test_mode == QUERY_TEST) {
        if (g_queryInfo.specifiedQueryInfo.batchQuery) {
            // batch_query = yes
            if (!g_queryInfo.specifiedQueryInfo.mixed_query) {
                // mixed_query = no
                errorPrint("%s\n", "batch_query = yes require mixed_query is yes");
                return -1;
            }
        }
    }
    
    if (isRest(g_arguments->iface) && g_arguments->bind_vgroup) {
        errorPrint("rest interface does not support bind vgroup, please use native or websocket mode\n");
        return -1; 
    }
    return 0;
}

// apply cfg
int32_t applyConfigDir(char * cfgDir){
    // set engine config dir 
    int32_t code;
#ifdef LINUX
    wordexp_t full_path;
    if (wordexp(cfgDir, &full_path, 0) != 0) {
        errorPrint("Invalid path %s\n", cfgDir);
        exit(EXIT_FAILURE);
    }
    code = taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
    wordfree(&full_path);
#else
    code = taos_options(TSDB_OPTION_CONFIGDIR, cfgDir);
#endif
    // show error
    if (code) {
        engineError("applyConfigDir", "taos_options(TSDB_OPTION_CONFIGDIR, ...)", code);
    }

    return code;
 }

int main(int argc, char* argv[]) {
    int ret = 0;

    // log
    initLog();
    initArgument();
    srand(time(NULL)%1000000);

    // majorVersion
    snprintf(g_client_info, CLIENT_INFO_LEN, "%s", taos_get_client_info());
    g_majorVersionOfClient = atoi(g_client_info);
    debugPrint("Client info: %s, major version: %d\n",
            g_client_info,
            g_majorVersionOfClient);

    // read command line
    if (benchParseArgs(argc, argv)) {
        exitLog();
        return -1;
    }

    // check valid
    if(g_arguments->connMode == CONN_MODE_NATIVE && g_arguments->dsn) {
        errorPrint("%s", DSN_NATIVE_CONFLICT);
        exitLog();
        return -1;
    }

    // read evn
    if (g_arguments->dsn == NULL) {
        char * dsn = getenv("TDENGINE_CLOUD_DSN");
        if (dsn != NULL && strlen(dsn) > 0) {
            g_arguments->dsn = dsn;
            infoPrint("Get dsn from getenv TDENGINE_CLOUD_DSN=%s\n", g_arguments->dsn);
        } 
    }
    
    // read json config
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

    // open result file
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

    // check argument
    infoPrint("client version: %s\n", taos_get_client_info());
    if (checkArgumentValid()) {
        exitLog();
        return -1;
    }

    // conn mode
    if (setConnMode(g_arguments->connMode, g_arguments->dsn, true) != 0) {
        exitLog();
        return -1;
    }

    // check condition for set config dir
    if (strlen(g_configDir)) {
        // apply
        if(applyConfigDir(g_configDir) != TSDB_CODE_SUCCESS) {
            exitLog();
            return -1;    
        }
        infoPrint("Set engine cfgdir successfully, dir:%s\n", g_configDir);
    }

    // cancel thread
#ifdef LINUX
    if (sem_init(&g_arguments->cancelSem, 0, 0) != 0) {
        errorPrint("%s", "failed to create cancel semaphore\n");
        exit(EXIT_FAILURE);
    }
    pthread_t spid = {0};
    pthread_create(&spid, NULL, benchCancelHandler, NULL);
    benchSetSignal(SIGINT, benchQueryInterruptHandler);
#endif

    // running
    if (g_arguments->test_mode == INSERT_TEST) {
        if (insertTestProcess()) {
            errorPrint("%s", "insert test process failed\n");
            ret = -1;
        }
    } else if (g_arguments->test_mode == CSVFILE_TEST) {
        if (csvTestProcess()) {
            errorPrint("%s", "generate csv process failed\n");
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

    // free and exit
    postFreeResource();

#ifdef LINUX
    pthread_cancel(spid);
    pthread_join(spid, NULL);
#endif

    exitLog();
    return ret;
}
