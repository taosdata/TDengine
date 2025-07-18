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
#include <stdlib.h>
#include <bench.h>
#include "benchLog.h"
#include "cus_name.h"

#ifdef LINUX
#include <argp.h>
#else
#ifndef ARGP_ERR_UNKNOWN
    #define ARGP_ERR_UNKNOWN E2BIG
#endif
#endif



#ifdef WINDOWS
char      g_configDir[MAX_PATH_LEN] = {0};  // "C:\\TDengine\\cfg"};
#else
char      g_configDir[MAX_PATH_LEN] = {0};  // "/etc/taos"};
#endif  // WINDOWS

#ifndef LINUX
void benchPrintHelp() {
    char indent[] = "  ";
    printf("Usage: "CUS_PROMPT"Benchmark [OPTION ...] \r\n\r\n");
    printf("%s%s%s%s\r\n", indent, "-f,", indent, BENCH_FILE);
    printf("%s%s%s%s\r\n", indent, "-a,", indent, BENCH_REPLICA);
    printf("%s%s%s%s\r\n", indent, "-A,", indent, BENCH_TAGS);
    printf("%s%s%s%s\r\n", indent, "-b,", indent, BENCH_COLS);
    printf("%s%s%s%s\r\n", indent, "-B,", indent, BENCH_INTERLACE);
    printf("%s%s%s%s\r\n", indent, "-c,", indent, BENCH_CFG_DIR);
    printf("%s%s%s%s\r\n", indent, "-C,", indent, BENCH_CHINESE);
    printf("%s%s%s%s\r\n", indent, "-d,", indent, BENCH_DATABASE);
    printf("%s%s%s%s\r\n", indent, "-E,", indent, BENCH_ESCAPE);
    printf("%s%s%s%s\r\n", indent, "-F,", indent, BENCH_PREPARE);
    printf("%s%s%s%s\r\n", indent, "-g,", indent, BENCH_DEBUG);
    printf("%s%s%s%s\r\n", indent, "-G,", indent, BENCH_PERFORMANCE);
    printf("%s%s%s%s\r\n", indent, "-h,", indent, BENCH_HOST);
    printf("%s%s%s%s\r\n", indent, "-i,", indent, BENCH_INTERVAL);
    printf("%s%s%s%s\r\n", indent, "-I,", indent, BENCH_MODE);
    printf("%s%s%s%s\r\n", indent, "-l,", indent, BENCH_COLS_NUM);
    printf("%s%s%s%s\r\n", indent, "-L,", indent, BENCH_PARTIAL_COL_NUM);
    printf("%s%s%s%s\r\n", indent, "-m,", indent, BENCH_PREFIX);
    printf("%s%s%s%s\r\n", indent, "-M,", indent, BENCH_RANDOM);
    printf("%s%s%s%s\r\n", indent, "-n,", indent, BENCH_ROWS);
    printf("%s%s%s%s\r\n", indent, "-N,", indent, BENCH_NORMAL);
    printf("%s%s%s%s\r\n", indent, "-k,", indent, BENCH_KEEPTRYING);
    printf("%s%s%s%s\r\n", indent, "-o,", indent, BENCH_OUTPUT);
    printf("%s%s%s%s\r\n", indent, "-O,", indent, BENCH_DISORDER);
    printf("%s%s%s%s\r\n", indent, "-p,", indent, BENCH_PASS);
    printf("%s%s%s%s\r\n", indent, "-P,", indent, BENCH_PORT);
    printf("%s%s%s%s\r\n", indent, "-Q,", indent, BENCH_NODROP);
    printf("%s%s%s%s\r\n", indent, "-r,", indent, BENCH_BATCH);
    printf("%s%s%s%s\r\n", indent, "-R,", indent, BENCH_RANGE);
    printf("%s%s%s%s\r\n", indent, "-S,", indent, BENCH_STEP);
    printf("%s%s%s%s\r\n", indent, "-s,", indent, BENCH_START_TIMESTAMP);
    printf("%s%s%s%s\r\n", indent, "-t,", indent, BENCH_TABLE);
    printf("%s%s%s%s\r\n", indent, "-T,", indent, BENCH_THREAD);
    printf("%s%s%s%s\r\n", indent, "-u,", indent, BENCH_USER);
    printf("%s%s%s%s\r\n", indent, "-U,", indent, BENCH_SUPPLEMENT);
    printf("%s%s%s%s\r\n", indent, "-w,", indent, BENCH_WIDTH);
    printf("%s%s%s%s\r\n", indent, "-x,", indent, BENCH_AGGR);
    printf("%s%s%s%s\r\n", indent, "-y,", indent, BENCH_YES);
    printf("%s%s%s%s\r\n", indent, "-z,", indent, BENCH_TRYING_INTERVAL);
    printf("%s%s%s%s\r\n", indent, "-v,", indent, BENCH_VGROUPS);
    printf("%s%s%s%s\r\n", indent, "-V,", indent, BENCH_VERSION);
    printf("%s%s%s%s\r\n", indent, "-X,", indent, DSN_DESC);
    printf("%s%s%s%s\r\n", indent, "-Z,", indent, DRIVER_DESC);
    printf("\r\n\r\nReport bugs to %s.\r\n", CUS_EMAIL);
}

int32_t benchParseArgsNoArgp(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-V") == 0 || strcmp(argv[i], "--version") == 0) {
            printVersion();
            exit(EXIT_SUCCESS);
        }

        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "--usage") == 0
            || strcmp(argv[i], "-?") == 0) {
            benchPrintHelp();
            exit(EXIT_SUCCESS);
        }

        char* key = argv[i];
        int32_t key_len = strlen(key);
        if (key_len != 2) {
            errorPrint("Invalid option %s\r\n", key);
            return -1;
        }
        if (key[0] != '-') {
            errorPrint("Invalid option %s\r\n", key);
            return -1;
        }

        if (key[1] == 'E' || key[1] == 'C'
            || key[1] == 'N' || key[1] == 'M'
            || key[1] == 'x' || key[1] == 'y'
            || key[1] == 'g' || key[1] == 'G'
            || key[1] == 'V' || key[1] == 'Q') {
            benchParseSingleOpt(key[1], NULL);
        } else {
            // check input value
            if (i + 1 >= argc) {
                errorPrint("option %s requires an argument\r\n", key);
                return -1;
            }
            char* val = argv[i+1];
            if (val[0] == '-') {
                errorPrint("option %s requires an argument\r\n", key);
                return -1;
            }
            
            if (benchParseSingleOpt(key[1], val)) {
                errorPrint("Invalid option %s\r\n", key);
                return -1;
            }
            i++;
        }
    }
    return 0;
}
#else
//const char *              argp_program_version = version;
const char *              argp_program_bug_address = CUS_EMAIL;

static struct argp_option bench_options[] = {
    {"file", 'f', "FILE", 0, BENCH_FILE, 0},
    {"config-dir", 'c', "CONFIG_DIR", 0, BENCH_CFG_DIR, 1},
    {"host", 'h', "HOST", 0, BENCH_HOST},
    {"port", 'P', "PORT", 0, BENCH_PORT},
    {"interface", 'I', "IFACE", 0, BENCH_MODE},
    {"user", 'u', "USER", 0, BENCH_USER},
    {"password", 'p', "PASSWORD", 0, BENCH_PASS},
    {"output", 'o', "FILE", 0, BENCH_OUTPUT},
    {"output-json-file", 'j', "FILE", 0, BENCH_OUTPUT_JSON},
    {"threads", 'T', "NUMBER", 0, BENCH_THREAD},
    {"insert-interval", 'i', "NUMBER", 0, BENCH_INTERVAL},
    {"time-step", 'S', "NUMBER", 0, BENCH_STEP},
    {"angle-step", 'H', "NUMBER", 0, ANGLE_STEP},
    {"start-timestamp", 's', "NUMBER", 0, BENCH_START_TIMESTAMP},
    {"supplement-insert", 'U', 0, 0, BENCH_SUPPLEMENT},
    {"interlace-rows", 'B', "NUMBER", 0, BENCH_INTERLACE},
    {"rec-per-req", 'r', "NUMBER", 0, BENCH_BATCH},
    {"tables", 't', "NUMBER", 0, BENCH_TABLE},
    {"records", 'n', "NUMBER", 0, BENCH_ROWS},
    {"database", 'd', "DATABASE", 0, BENCH_DATABASE},
    {"columns", 'l', "NUMBER", 0, BENCH_COLS_NUM},
    {"partial-col-num", 'L', "NUMBER", 0, BENCH_PARTIAL_COL_NUM},
    {"tag-type", 'A', "TAG_TYPE", 0, BENCH_TAGS},
    {"data-type", 'b', "COL_TYPE", 0, BENCH_COLS},
    {"binwidth", 'w', "NUMBER", 0, BENCH_WIDTH},
    {"table-prefix", 'm', "TABLE_PREFIX", 0, BENCH_PREFIX},
    {"escape-character", 'E', 0, 0, BENCH_ESCAPE},
    {"chinese", 'C', 0, 0, BENCH_CHINESE},
    {"normal-table", 'N', 0, 0, BENCH_NORMAL},
    {"random", 'M', 0, 0, BENCH_RANDOM},
    {"aggr-func", 'x', 0, 0, BENCH_AGGR},
    {"answer-yes", 'y', 0, 0, BENCH_YES},
    {"disorder-range", 'R', "NUMBER", 0, BENCH_RANGE},
    {"disorder", 'O', "NUMBER", 0, BENCH_DISORDER},
    {"replica", 'a', "NUMBER", 0, BENCH_REPLICA},
    {"debug", 'g', 0, 0, BENCH_DEBUG},
    {"performance", 'G', 0, 0, BENCH_PERFORMANCE},
    {"prepared_rand", 'F', "NUMBER", 0, BENCH_PREPARE},
    {"cloud_dsn", 'W', "DSN", 0, OLD_DSN_DESC},
    {"keep-trying", 'k', "NUMBER", 0, BENCH_KEEPTRYING},
    {"trying-interval", 'z', "NUMBER", 0, BENCH_TRYING_INTERVAL},
    {"vgroups", 'v', "NUMBER", 0, BENCH_VGROUPS},
    {"version", 'V', 0, 0, BENCH_VERSION},
    {"nodrop", 'Q', 0, 0, BENCH_NODROP},
    {"dsn", 'X', "DSN", 0, DSN_DESC},
    {DRIVER_OPT, 'Z', "DRIVER", 0, DRIVER_DESC},
    {0}
};

static error_t benchParseOpt(int key, char *arg, struct argp_state *state) {
    return benchParseSingleOpt(key, arg);
}

static struct argp bench_argp = {bench_options, benchParseOpt, "", ""};

void benchParseArgsByArgp(int argc, char *argv[]) {
    argp_parse(&bench_argp, argc, argv, 0, 0, g_arguments);
}
#endif  // LINUX

int32_t benchParseSingleOpt(int32_t key, char* arg) {
    SDataBase *database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    switch (key) {
        case 'F':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "F");
            }

            g_arguments->prepared_rand = atol(arg);
            if (g_arguments->prepared_rand <= 0) {
                errorPrint(
                           "Invalid -F: %s, will auto set to default(10000)\n",
                           arg);
                g_arguments->prepared_rand = DEFAULT_PREPARED_RAND;
            }
            break;

        case 'f':
            g_arguments->demo_mode = false;
            g_arguments->metaFile = arg;
            break;

        case 'h':
            g_arguments->host = arg;
            break;

        case 'P':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "P");
            }
            g_arguments->port = atoi(arg);
            if (g_arguments->port <= 0) {
                errorPrint(
                           "Invalid -P: %s, will auto set to default(6030)\n",
                           arg);
                g_arguments->port = DEFAULT_PORT;
            }
            g_arguments->port_inputted = true;
            break;

        case 'I':
            if (0 == strcasecmp(arg, "taosc")) {
                stbInfo->iface = TAOSC_IFACE;
            } else if (0 == strcasecmp(arg, "stmt")) {
                stbInfo->iface = STMT_IFACE;
            } else if (0 == strcasecmp(arg, "stmt2")) {
                stbInfo->iface = STMT2_IFACE;
            } else if (0 == strcasecmp(arg, "rest")) {
                stbInfo->iface = REST_IFACE;
            } else if (0 == strcasecmp(arg, "sml")
                    || 0 == strcasecmp(arg, "sml-line")) {
                stbInfo->iface = SML_IFACE;
                stbInfo->lineProtocol = TSDB_SML_LINE_PROTOCOL;
            } else if (0 == strcasecmp(arg, "sml-telnet")) {
                stbInfo->iface = SML_IFACE;
                stbInfo->lineProtocol = TSDB_SML_TELNET_PROTOCOL;
            } else if (0 == strcasecmp(arg, "sml-json")) {
                stbInfo->iface = SML_IFACE;
                stbInfo->lineProtocol = TSDB_SML_JSON_PROTOCOL;
            } else if (0 == strcasecmp(arg, "sml-taosjson")) {
                stbInfo->iface = SML_IFACE;
                stbInfo->lineProtocol = SML_JSON_TAOS_FORMAT;
            } else if (0 == strcasecmp(arg, "sml-rest")
                   || (0 == strcasecmp(arg, "sml-rest-line"))) {
                stbInfo->iface        = SML_REST_IFACE;
                stbInfo->lineProtocol = TSDB_SML_LINE_PROTOCOL;
            } else if (0 == strcasecmp(arg, "sml-rest-telnet")) {
                stbInfo->iface        = SML_REST_IFACE;
                stbInfo->lineProtocol = TSDB_SML_TELNET_PROTOCOL;
            } else if (0 == strcasecmp(arg, "sml-rest-json")) {
                stbInfo->iface        = SML_REST_IFACE;
                stbInfo->lineProtocol = TSDB_SML_JSON_PROTOCOL;
            } else if (0 == strcasecmp(arg, "sml-rest-taosjson")) {
                stbInfo->iface        = SML_REST_IFACE;
                stbInfo->lineProtocol = SML_JSON_TAOS_FORMAT;
            } else {
                errorPrint(
                           "Invalid -I: %s, will auto set to default (taosc)\n",
                           arg);
                stbInfo->iface = TAOSC_IFACE;
            }
            g_arguments->iface = stbInfo->iface;
            break;

        case 'p':
            g_arguments->password = arg;
            break;

        case 'u':
            g_arguments->user = arg;
            break;

        case 'c':
            TOOLS_STRNCPY(g_configDir, arg, TSDB_FILENAME_LEN);
            g_arguments->cfg_inputted = true;
            break;

        case 'o':
            g_arguments->output_file = arg;
            break;
        case 'j':
            g_arguments->output_json_file = arg;
            break;
        case 'T':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "T");
            }

            g_arguments->nthreads = atoi(arg);
            if (g_arguments->nthreads <= 0) {
                errorPrint(
                           "Invalid -T: %s, will auto set to default(8)\n",
                           arg);
                g_arguments->nthreads = DEFAULT_NTHREADS;
            } else {
                g_argFlag |= ARG_OPT_THREAD;
            }
            
            break;

        case 'i':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "i");
            }

            stbInfo->insert_interval = atoi(arg);
            if (stbInfo->insert_interval <= 0) {
                errorPrint(
                           "Invalid -i: %s, will auto set to default(0)\n",
                           arg);
                stbInfo->insert_interval = 0;
            }
            break;

        case 'S':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "S");
            }

            stbInfo->timestamp_step = atol(arg);
            if (stbInfo->timestamp_step <= 0) {
                errorPrint(
                           "Invalid -S: %s, will auto set to default(1)\n",
                           arg);
                stbInfo->timestamp_step = 1;
            }
            break;

        // angle step
        case 'H':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "H");
            }

            stbInfo->angle_step = atol(arg);
            if (stbInfo->angle_step <= 0) {
                errorPrint(
                           "Invalid -H: %s, will auto set to default(1)\n",
                           arg);
                stbInfo->angle_step = 1;
            }
            break;            

        case 'B':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "B");
            }

            stbInfo->interlaceRows = atoi(arg);
            if (stbInfo->interlaceRows <= 0) {
                errorPrint(
                           "Invalid -B: %s, will auto set to default(0)\n",
                           arg);
                stbInfo->interlaceRows = 0;
            }
            break;

        case 'r':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "r");
            }

            g_arguments->reqPerReq = atoi(arg);
            if (g_arguments->reqPerReq <= 0 ||
                g_arguments->reqPerReq > MAX_RECORDS_PER_REQ) {
                errorPrint(
                           "Invalid -r: %s, will auto set to default(30000)\n",
                           arg);
                g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
            }
            break;

        case 's':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "s");
            }

            g_arguments->startTimestamp = atol(arg);
            break;

        case 'U':
            g_arguments->supplementInsert = true;
            break;

        case 't':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "t");
            }

            stbInfo->childTblCount = atoi(arg);
            if (stbInfo->childTblCount <= 0) {
                errorPrint(
                           "Invalid -t: %s, will auto set to default(10000)\n",
                           arg);
                stbInfo->childTblCount = DEFAULT_CHILDTABLES;
            }
            g_arguments->totalChildTables = stbInfo->childTblCount;
            break;

        case 'n':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "n");
            }

            stbInfo->insertRows = atol(arg);
            if (stbInfo->insertRows <= 0) {
                errorPrint(
                           "Invalid -n: %s, will auto set to default(10000)\n",
                           arg);
                stbInfo->insertRows = DEFAULT_INSERT_ROWS;
            }
            break;

        case 'd':
            database->dbName = arg;
            break;

        case 'l':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "l");
            }

            g_arguments->demo_mode = false;
            g_arguments->intColumnCount = atoi(arg);
            if (g_arguments->intColumnCount <= 0) {
                errorPrint(
                           "Invalid -l: %s, will auto set to default(0)\n",
                           arg);
                g_arguments->intColumnCount = 0;
            }
            break;

        case 'L':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "L");
            }

            g_arguments->demo_mode = false;
            g_arguments->partialColNum = atoi(arg);
            break;

        case 'A':
            g_arguments->demo_mode = false;
            parseFieldDatatype(arg, stbInfo->tags, true);
            break;

        case 'b':
            g_arguments->demo_mode = false;
            parseFieldDatatype(arg, stbInfo->cols, false);
            break;

        case 'k':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "k");
            }

            g_arguments->keep_trying = atoi(arg);
            debugPrint("keep_trying: %d\n", g_arguments->keep_trying);
            break;

        case 'z':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "z");
            }

            g_arguments->trying_interval = atoi(arg);
            debugPrint("trying_interval: %d\n", g_arguments->trying_interval);
            break;

        case 'w':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "w");
            }

            g_arguments->binwidth = atoi(arg);
            if (g_arguments->binwidth <= 0) {
                errorPrint(
                        "Invalid value for w: %s, "
                        "will auto set to default(%d)\n",
                        arg, DEFAULT_BINWIDTH);
                g_arguments->binwidth = DEFAULT_BINWIDTH;
            } else if (g_arguments->binwidth >
                (TSDB_MAX_BINARY_LEN - sizeof(int64_t) -2)) {
                errorPrint(
                        "-w(%d) > (TSDB_MAX_BINARY_LEN(%u"
                        ")-(TIMESTAMP length(%zu) - extrabytes(2), "
                        "will auto set to default(64)\n",
                        g_arguments->binwidth,
                        TSDB_MAX_BINARY_LEN, sizeof(int64_t));
                g_arguments->binwidth = DEFAULT_BINWIDTH;
            }
            break;

        case 'm':
            stbInfo->childTblPrefix = arg;
            break;

        case 'E':
            g_arguments->escape_character = true;
            break;

        case 'C':
            g_arguments->chinese = true;
            break;

        case 'N':
            g_arguments->demo_mode = false;
            stbInfo->use_metric = false;
            benchArrayClear(stbInfo->tags);
            break;

        case 'M':
            g_arguments->mistMode = true;
            break;

        case 'x':
            g_arguments->aggr_func = true;
            break;

        case 'y':
            g_arguments->answer_yes = true;
            break;

        case 'R':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "R");
            }

            stbInfo->disorderRange = atoi(arg);
            if (stbInfo->disorderRange <= 0) {
                errorPrint(
                           "Invalid value for -R: %s, will auto set to "
                           "default(1000)\n",
                           arg);
                stbInfo->disorderRange =
                        DEFAULT_DISORDER_RANGE;
            }
            break;

        case 'O':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "O");
            }

            stbInfo->disorderRatio = atoi(arg);
            stbInfo->disRatio      = (uint8_t)atoi(arg);
            if (stbInfo->disorderRatio <= 0) {
                errorPrint(
                        "Invalid value for -O: %s, "
                        "will auto set to default(0)\n",
                        arg);
                stbInfo->disorderRatio = 0;
            }
            break;

        case 'a': {
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "a");
            }

            int replica = atoi(arg);
            if (replica <= 0) {
                errorPrint(
                        "Invalid value for -a: %s, "
                        "will auto set to default(%d)\n",
                        arg, DEFAULT_REPLICA);
                replica = DEFAULT_REPLICA;
            }
            SDbCfg* cfg = benchCalloc(1, sizeof(SDbCfg), true);
            cfg->name = "replica";
            cfg->valuestring = NULL;
            cfg->valueint = replica;
            benchArrayPush(database->cfgs, cfg);
            break;
        }
        case 'g':
            g_arguments->debug_print = true;
            break;
        case 'G':
            g_arguments->performance_print = true;
            break;

        case 'W':
        case 'X':
            g_arguments->dsn = arg;
            break;

        case 'v':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"Benchmark", "v");
            }
            g_arguments->inputted_vgroups = atoi(arg);
            break;
        case 'Q':
            database->drop = false;
            g_argFlag |= ARG_OPT_NODROP;
            break;
        case 'V':
            printVersion();
            exit(0);
        case 'Z':
            g_arguments->connMode = getConnMode(arg);
            break;
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

int32_t benchParseArgs(int32_t argc, char* argv[]) {
#ifdef LINUX
    benchParseArgsByArgp(argc, argv);
    return 0;
#else
    return benchParseArgsNoArgp(argc, argv);
#endif
}

