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

#include "demo.h"
#include "demoData.h"

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

int parse_args(int argc, char *argv[]) {
    int32_t code = -1;
    for (int i = 1; i < argc; i++) {
        if ((0 == strncmp(argv[i], "-f", strlen("-f"))) ||
            (0 == strncmp(argv[i], "--file", strlen("--file")))) {
            g_args.demo_mode = false;

            if (2 == strlen(argv[i])) {
                if (i + 1 == argc) {
                    errorPrintReqArg(argv[0], "f");
                    goto end_parse_command;
                }
                g_args.metaFile = argv[++i];
            } else if (0 == strncmp(argv[i], "-f", strlen("-f"))) {
                g_args.metaFile = (char *)(argv[i] + strlen("-f"));
            } else if (strlen("--file") == strlen(argv[i])) {
                if (i + 1 == argc) {
                    errorPrintReqArg3(argv[0], "--file");
                    goto end_parse_command;
                }
                g_args.metaFile = argv[++i];
            } else if (0 == strncmp(argv[i], "--file=", strlen("--file="))) {
                g_args.metaFile = (char *)(argv[i] + strlen("--file="));
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-c", strlen("-c"))) ||
                   (0 ==
                    strncmp(argv[i], "--config-dir", strlen("--config-dir")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "c");
                    goto end_parse_command;
                }
                tstrncpy(configDir, argv[++i], TSDB_FILENAME_LEN);
            } else if (0 == strncmp(argv[i], "-c", strlen("-c"))) {
                tstrncpy(configDir, (char *)(argv[i] + strlen("-c")),
                         TSDB_FILENAME_LEN);
            } else if (strlen("--config-dir") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--config-dir");
                    goto end_parse_command;
                }
                tstrncpy(configDir, argv[++i], TSDB_FILENAME_LEN);
            } else if (0 == strncmp(argv[i],
                                    "--config-dir=", strlen("--config-dir="))) {
                tstrncpy(configDir, (char *)(argv[i] + strlen("--config-dir=")),
                         TSDB_FILENAME_LEN);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-h", strlen("-h"))) ||
                   (0 == strncmp(argv[i], "--host", strlen("--host")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "h");
                    goto end_parse_command;
                }
                g_args.host = argv[++i];
            } else if (0 == strncmp(argv[i], "-h", strlen("-h"))) {
                g_args.host = (char *)(argv[i] + strlen("-h"));
            } else if (strlen("--host") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--host");
                    goto end_parse_command;
                }
                g_args.host = argv[++i];
            } else if (0 == strncmp(argv[i], "--host=", strlen("--host="))) {
                g_args.host = (char *)(argv[i] + strlen("--host="));
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if (strcmp(argv[i], "-PP") == 0) {
            g_args.performance_print = true;
        } else if ((0 == strncmp(argv[i], "-P", strlen("-P"))) ||
                   (0 == strncmp(argv[i], "--port", strlen("--port")))) {
            uint64_t port;
            char     strPort[BIGINT_BUFF_LEN];

            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "P");
                    goto end_parse_command;
                } else if (isStringNumber(argv[i + 1])) {
                    tstrncpy(strPort, argv[++i], BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "P");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "--port=", strlen("--port="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--port=")))) {
                    tstrncpy(strPort, (char *)(argv[i] + strlen("--port=")),
                             BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "--port");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-P", strlen("-P"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-P")))) {
                    tstrncpy(strPort, (char *)(argv[i] + strlen("-P")),
                             BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "--port");
                    goto end_parse_command;
                }
            } else if (strlen("--port") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--port");
                    goto end_parse_command;
                } else if (isStringNumber(argv[i + 1])) {
                    tstrncpy(strPort, argv[++i], BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "--port");
                    goto end_parse_command;
                }
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            port = atoi(strPort);
            if (port > 65535) {
                errorWrongValue("taosdump", "-P or --port", strPort);
                goto end_parse_command;
            }
            g_args.port = (uint16_t)port;

        } else if ((0 == strncmp(argv[i], "-I", strlen("-I"))) ||
                   (0 ==
                    strncmp(argv[i], "--interface", strlen("--interface")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "I");
                    goto end_parse_command;
                }
                if (0 == strcasecmp(argv[i + 1], "taosc")) {
                    g_args.iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "rest")) {
                    g_args.iface = REST_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "stmt")) {
                    g_args.iface = STMT_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "sml")) {
                    g_args.iface = SML_IFACE;
                } else {
                    errorWrongValue(argv[0], "-I", argv[i + 1]);
                    goto end_parse_command;
                }
                i++;
            } else if (0 == strncmp(argv[i],
                                    "--interface=", strlen("--interface="))) {
                if (0 == strcasecmp((char *)(argv[i] + strlen("--interface=")),
                                    "taosc")) {
                    g_args.iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(
                                    (char *)(argv[i] + strlen("--interface=")),
                                    "rest")) {
                    g_args.iface = REST_IFACE;
                } else if (0 == strcasecmp(
                                    (char *)(argv[i] + strlen("--interface=")),
                                    "stmt")) {
                    g_args.iface = STMT_IFACE;
                } else if (0 == strcasecmp(
                                    (char *)(argv[i] + strlen("--interface=")),
                                    "sml")) {
                    g_args.iface = SML_IFACE;
                } else {
                    errorPrintReqArg3(argv[0], "--interface");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-I", strlen("-I"))) {
                if (0 ==
                    strcasecmp((char *)(argv[i] + strlen("-I")), "taosc")) {
                    g_args.iface = TAOSC_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("-I")),
                                           "rest")) {
                    g_args.iface = REST_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("-I")),
                                           "stmt")) {
                    g_args.iface = STMT_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("-I")),
                                           "sml")) {
                    g_args.iface = SML_IFACE;
                } else {
                    errorWrongValue(argv[0], "-I",
                                    (char *)(argv[i] + strlen("-I")));
                    goto end_parse_command;
                }
            } else if (strlen("--interface") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--interface");
                    goto end_parse_command;
                }
                if (0 == strcasecmp(argv[i + 1], "taosc")) {
                    g_args.iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "rest")) {
                    g_args.iface = REST_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "stmt")) {
                    g_args.iface = STMT_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "sml")) {
                    g_args.iface = SML_IFACE;
                } else {
                    errorWrongValue(argv[0], "--interface", argv[i + 1]);
                    goto end_parse_command;
                }
                i++;
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-u", strlen("-u"))) ||
                   (0 == strncmp(argv[i], "--user", strlen("--user")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "u");
                    goto end_parse_command;
                }
                g_args.user = argv[++i];
            } else if (0 == strncmp(argv[i], "-u", strlen("-u"))) {
                g_args.user = (char *)(argv[i++] + strlen("-u"));
            } else if (0 == strncmp(argv[i], "--user=", strlen("--user="))) {
                g_args.user = (char *)(argv[i++] + strlen("--user="));
            } else if (strlen("--user") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--user");
                    goto end_parse_command;
                }
                g_args.user = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-p", strlen("-p"))) ||
                   (0 == strcmp(argv[i], "--password"))) {
            if ((strlen(argv[i]) == 2) ||
                (0 == strcmp(argv[i], "--password"))) {
                printf("Enter password: ");
                taosSetConsoleEcho(false);
                if (scanf("%s", g_args.password) > 1) {
                    fprintf(stderr, "password read error!\n");
                }
                taosSetConsoleEcho(true);
            } else {
                tstrncpy(g_args.password, (char *)(argv[i] + 2),
                         SHELL_MAX_PASSWORD_LEN);
            }
        } else if ((0 == strncmp(argv[i], "-o", strlen("-o"))) ||
                   (0 == strncmp(argv[i], "--output", strlen("--output")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--output");
                    goto end_parse_command;
                }
                g_args.output_file = argv[++i];
            } else if (0 ==
                       strncmp(argv[i], "--output=", strlen("--output="))) {
                g_args.output_file = (char *)(argv[i++] + strlen("--output="));
            } else if (0 == strncmp(argv[i], "-o", strlen("-o"))) {
                g_args.output_file = (char *)(argv[i++] + strlen("-o"));
            } else if (strlen("--output") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--output");
                    goto end_parse_command;
                }
                g_args.output_file = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-s", strlen("-s"))) ||
                   (0 ==
                    strncmp(argv[i], "--sql-file", strlen("--sql-file")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "s");
                    goto end_parse_command;
                }
                g_args.sqlFile = argv[++i];
            } else if (0 ==
                       strncmp(argv[i], "--sql-file=", strlen("--sql-file="))) {
                g_args.sqlFile = (char *)(argv[i++] + strlen("--sql-file="));
            } else if (0 == strncmp(argv[i], "-s", strlen("-s"))) {
                g_args.sqlFile = (char *)(argv[i++] + strlen("-s"));
            } else if (strlen("--sql-file") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--sql-file");
                    goto end_parse_command;
                }
                g_args.sqlFile = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-q", strlen("-q"))) ||
                   (0 ==
                    strncmp(argv[i], "--query-mode", strlen("--query-mode")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "q");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "q");
                    goto end_parse_command;
                }
                g_args.async_mode = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i],
                                    "--query-mode=", strlen("--query-mode="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--query-mode=")))) {
                    g_args.async_mode =
                        atoi((char *)(argv[i] + strlen("--query-mode=")));
                } else {
                    errorPrintReqArg2(argv[0], "--query-mode");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-q", strlen("-q"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-q")))) {
                    g_args.async_mode = atoi((char *)(argv[i] + strlen("-q")));
                } else {
                    errorPrintReqArg2(argv[0], "-q");
                    goto end_parse_command;
                }
            } else if (strlen("--query-mode") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--query-mode");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--query-mode");
                    goto end_parse_command;
                }
                g_args.async_mode = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-T", strlen("-T"))) ||
                   (0 == strncmp(argv[i], "--threads", strlen("--threads")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "T");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "T");
                    goto end_parse_command;
                }
                g_args.nthreads = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--threads=", strlen("--threads="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--threads=")))) {
                    g_args.nthreads =
                        atoi((char *)(argv[i] + strlen("--threads=")));
                } else {
                    errorPrintReqArg2(argv[0], "--threads");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-T", strlen("-T"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-T")))) {
                    g_args.nthreads = atoi((char *)(argv[i] + strlen("-T")));
                } else {
                    errorPrintReqArg2(argv[0], "-T");
                    goto end_parse_command;
                }
            } else if (strlen("--threads") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--threads");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--threads");
                    goto end_parse_command;
                }
                g_args.nthreads = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-i", strlen("-i"))) ||
                   (0 == strncmp(argv[i], "--insert-interval",
                                 strlen("--insert-interval")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "i");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "i");
                    goto end_parse_command;
                }
                g_args.insert_interval = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--insert-interval=",
                                    strlen("--insert-interval="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--insert-interval=")))) {
                    g_args.insert_interval =
                        atoi((char *)(argv[i] + strlen("--insert-interval=")));
                } else {
                    errorPrintReqArg3(argv[0], "--insert-innterval");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-i", strlen("-i"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-i")))) {
                    g_args.insert_interval =
                        atoi((char *)(argv[i] + strlen("-i")));
                } else {
                    errorPrintReqArg3(argv[0], "-i");
                    goto end_parse_command;
                }
            } else if (strlen("--insert-interval") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--insert-interval");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--insert-interval");
                    goto end_parse_command;
                }
                g_args.insert_interval = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-S", strlen("-S"))) ||
                   (0 ==
                    strncmp(argv[i], "--time-step", strlen("--time-step")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "S");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "S");
                    goto end_parse_command;
                }
                g_args.timestamp_step = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i],
                                    "--time-step=", strlen("--time-step="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--time-step=")))) {
                    g_args.timestamp_step =
                        atoi((char *)(argv[i] + strlen("--time-step=")));
                } else {
                    errorPrintReqArg2(argv[0], "--time-step");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-S", strlen("-S"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-S")))) {
                    g_args.timestamp_step =
                        atoi((char *)(argv[i] + strlen("-S")));
                } else {
                    errorPrintReqArg2(argv[0], "-S");
                    goto end_parse_command;
                }
            } else if (strlen("--time-step") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--time-step");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--time-step");
                    goto end_parse_command;
                }
                g_args.timestamp_step = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if (strcmp(argv[i], "-qt") == 0) {
            if ((argc == i + 1) || (!isStringNumber(argv[i + 1]))) {
                printHelp();
                errorPrint("%s", "\n\t-qt need a number following!\n");
                goto end_parse_command;
            }
            g_args.query_times = atoi(argv[++i]);
        } else if ((0 == strncmp(argv[i], "-B", strlen("-B"))) ||
                   (0 == strncmp(argv[i], "--interlace-rows",
                                 strlen("--interlace-rows")))) {
            if (strlen("-B") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "B");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "B");
                    goto end_parse_command;
                }
                g_args.interlaceRows = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--interlace-rows=",
                                    strlen("--interlace-rows="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--interlace-rows=")))) {
                    g_args.interlaceRows =
                        atoi((char *)(argv[i] + strlen("--interlace-rows=")));
                } else {
                    errorPrintReqArg2(argv[0], "--interlace-rows");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-B", strlen("-B"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-B")))) {
                    g_args.interlaceRows =
                        atoi((char *)(argv[i] + strlen("-B")));
                } else {
                    errorPrintReqArg2(argv[0], "-B");
                    goto end_parse_command;
                }
            } else if (strlen("--interlace-rows") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--interlace-rows");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--interlace-rows");
                    goto end_parse_command;
                }
                g_args.interlaceRows = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-r", strlen("-r"))) ||
                   (0 == strncmp(argv[i], "--rec-per-req", 13))) {
            if (strlen("-r") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "r");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "r");
                    goto end_parse_command;
                }
                g_args.reqPerReq = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--rec-per-req=",
                                    strlen("--rec-per-req="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--rec-per-req=")))) {
                    g_args.reqPerReq =
                        atoi((char *)(argv[i] + strlen("--rec-per-req=")));
                } else {
                    errorPrintReqArg2(argv[0], "--rec-per-req");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-r", strlen("-r"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-r")))) {
                    g_args.reqPerReq = atoi((char *)(argv[i] + strlen("-r")));
                } else {
                    errorPrintReqArg2(argv[0], "-r");
                    goto end_parse_command;
                }
            } else if (strlen("--rec-per-req") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--rec-per-req");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--rec-per-req");
                    goto end_parse_command;
                }
                g_args.reqPerReq = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-t", strlen("-t"))) ||
                   (0 == strncmp(argv[i], "--tables", strlen("--tables")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "t");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "t");
                    goto end_parse_command;
                }
                g_args.ntables = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--tables=", strlen("--tables="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--tables=")))) {
                    g_args.ntables =
                        atoi((char *)(argv[i] + strlen("--tables=")));
                } else {
                    errorPrintReqArg2(argv[0], "--tables");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-t", strlen("-t"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-t")))) {
                    g_args.ntables = atoi((char *)(argv[i] + strlen("-t")));
                } else {
                    errorPrintReqArg2(argv[0], "-t");
                    goto end_parse_command;
                }
            } else if (strlen("--tables") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--tables");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--tables");
                    goto end_parse_command;
                }
                g_args.ntables = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            g_totalChildTables = g_args.ntables;
        } else if ((0 == strncmp(argv[i], "-n", strlen("-n"))) ||
                   (0 == strncmp(argv[i], "--records", strlen("--records")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "n");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "n");
                    goto end_parse_command;
                }
                g_args.insertRows = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--records=", strlen("--records="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--records=")))) {
                    g_args.insertRows =
                        atoi((char *)(argv[i] + strlen("--records=")));
                } else {
                    errorPrintReqArg2(argv[0], "--records");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-n", strlen("-n"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-n")))) {
                    g_args.insertRows = atoi((char *)(argv[i] + strlen("-n")));
                } else {
                    errorPrintReqArg2(argv[0], "-n");
                    goto end_parse_command;
                }
            } else if (strlen("--records") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--records");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--records");
                    goto end_parse_command;
                }
                g_args.insertRows = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-d", strlen("-d"))) ||
                   (0 ==
                    strncmp(argv[i], "--database", strlen("--database")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "d");
                    goto end_parse_command;
                }
                g_args.database = argv[++i];
            } else if (0 ==
                       strncmp(argv[i], "--database=", strlen("--database="))) {
                g_args.output_file = (char *)(argv[i] + strlen("--database="));
            } else if (0 == strncmp(argv[i], "-d", strlen("-d"))) {
                g_args.output_file = (char *)(argv[i] + strlen("-d"));
            } else if (strlen("--database") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--database");
                    goto end_parse_command;
                }
                g_args.database = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-l", strlen("-l"))) ||
                   (0 == strncmp(argv[i], "--columns", strlen("--columns")))) {
            g_args.demo_mode = false;
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "l");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "l");
                    goto end_parse_command;
                }
                g_args.columnCount = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--columns=", strlen("--columns="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--columns=")))) {
                    g_args.columnCount =
                        atoi((char *)(argv[i] + strlen("--columns=")));
                } else {
                    errorPrintReqArg2(argv[0], "--columns");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-l", strlen("-l"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-l")))) {
                    g_args.columnCount = atoi((char *)(argv[i] + strlen("-l")));
                } else {
                    errorPrintReqArg2(argv[0], "-l");
                    goto end_parse_command;
                }
            } else if (strlen("--columns") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--columns");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--columns");
                    goto end_parse_command;
                }
                g_args.columnCount = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            if (g_args.columnCount > MAX_NUM_COLUMNS) {
                printf("WARNING: max acceptable columns count is %d\n",
                       MAX_NUM_COLUMNS);
                prompt();
                g_args.columnCount = MAX_NUM_COLUMNS;
            }

            for (int col = DEFAULT_DATATYPE_NUM; col < g_args.columnCount;
                 col++) {
                g_args.dataType[col] = "INT";
                g_args.data_type[col] = TSDB_DATA_TYPE_INT;
            }
            for (int col = g_args.columnCount; col < MAX_NUM_COLUMNS; col++) {
                g_args.dataType[col] = NULL;
                g_args.data_type[col] = TSDB_DATA_TYPE_NULL;
            }
        } else if ((0 == strncmp(argv[i], "-b", strlen("-b"))) ||
                   (0 ==
                    strncmp(argv[i], "--data-type", strlen("--data-type")))) {
            g_args.demo_mode = false;

            char *dataType;
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "b");
                    goto end_parse_command;
                }
                dataType = argv[++i];
            } else if (0 == strncmp(argv[i],
                                    "--data-type=", strlen("--data-type="))) {
                dataType = (char *)(argv[i] + strlen("--data-type="));
            } else if (0 == strncmp(argv[i], "-b", strlen("-b"))) {
                dataType = (char *)(argv[i] + strlen("-b"));
            } else if (strlen("--data-type") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--data-type");
                    goto end_parse_command;
                }
                dataType = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            if (strstr(dataType, ",") == NULL) {
                // only one col
                if (strcasecmp(dataType, "INT") &&
                    strcasecmp(dataType, "FLOAT") &&
                    strcasecmp(dataType, "TINYINT") &&
                    strcasecmp(dataType, "BOOL") &&
                    strcasecmp(dataType, "SMALLINT") &&
                    strcasecmp(dataType, "BIGINT") &&
                    strcasecmp(dataType, "DOUBLE") &&
                    strcasecmp(dataType, "TIMESTAMP") &&
                    !regexMatch(dataType,
                                "^(NCHAR|BINARY)(\\([1-9][0-9]*\\))?$",
                                REG_ICASE | REG_EXTENDED) &&
                    strcasecmp(dataType, "UTINYINT") &&
                    strcasecmp(dataType, "USMALLINT") &&
                    strcasecmp(dataType, "UINT") &&
                    strcasecmp(dataType, "UBIGINT")) {
                    printHelp();
                    errorPrint("%s", "-b: Invalid data_type!\n");
                    goto end_parse_command;
                }
                if (0 == strcasecmp(dataType, "INT")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_INT;
                } else if (0 == strcasecmp(dataType, "TINYINT")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_TINYINT;
                } else if (0 == strcasecmp(dataType, "SMALLINT")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_SMALLINT;
                } else if (0 == strcasecmp(dataType, "BIGINT")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_BIGINT;
                } else if (0 == strcasecmp(dataType, "FLOAT")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_FLOAT;
                } else if (0 == strcasecmp(dataType, "DOUBLE")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_DOUBLE;
                } else if (1 == regexMatch(dataType,
                                           "^BINARY(\\([1-9][0-9]*\\))?$",
                                           REG_ICASE | REG_EXTENDED)) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_BINARY;
                } else if (1 == regexMatch(dataType,
                                           "^NCHAR(\\([1-9][0-9]*\\))?$",
                                           REG_ICASE | REG_EXTENDED)) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_NCHAR;
                } else if (0 == strcasecmp(dataType, "BOOL")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_BOOL;
                } else if (0 == strcasecmp(dataType, "TIMESTAMP")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_TIMESTAMP;
                } else if (0 == strcasecmp(dataType, "UTINYINT")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_UTINYINT;
                } else if (0 == strcasecmp(dataType, "USMALLINT")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_USMALLINT;
                } else if (0 == strcasecmp(dataType, "UINT")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_UINT;
                } else if (0 == strcasecmp(dataType, "UBIGINT")) {
                    g_args.data_type[0] = TSDB_DATA_TYPE_UBIGINT;
                } else {
                    g_args.data_type[0] = TSDB_DATA_TYPE_NULL;
                }
                g_args.dataType[0] = dataType;
                g_args.dataType[1] = NULL;
                g_args.data_type[1] = TSDB_DATA_TYPE_NULL;
            } else {
                // more than one col
                int index = 0;
                g_dupstr = strdup(dataType);
                char *running = g_dupstr;
                char *token = strsep(&running, ",");
                while (token != NULL) {
                    if (strcasecmp(token, "INT") &&
                        strcasecmp(token, "FLOAT") &&
                        strcasecmp(token, "TINYINT") &&
                        strcasecmp(token, "BOOL") &&
                        strcasecmp(token, "SMALLINT") &&
                        strcasecmp(token, "BIGINT") &&
                        strcasecmp(token, "DOUBLE") &&
                        strcasecmp(token, "TIMESTAMP") &&
                        !regexMatch(token,
                                    "^(NCHAR|BINARY)(\\([1-9][0-9]*\\))?$",
                                    REG_ICASE | REG_EXTENDED) &&
                        strcasecmp(token, "UTINYINT") &&
                        strcasecmp(token, "USMALLINT") &&
                        strcasecmp(token, "UINT") &&
                        strcasecmp(token, "UBIGINT")) {
                        printHelp();
                        errorPrint("%s", "-b: Invalid data_type!\n");
                        goto end_parse_command;
                    }

                    if (0 == strcasecmp(token, "INT")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_INT;
                    } else if (0 == strcasecmp(token, "FLOAT")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_FLOAT;
                    } else if (0 == strcasecmp(token, "SMALLINT")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_SMALLINT;
                    } else if (0 == strcasecmp(token, "BIGINT")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_BIGINT;
                    } else if (0 == strcasecmp(token, "DOUBLE")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_DOUBLE;
                    } else if (0 == strcasecmp(token, "TINYINT")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_TINYINT;
                    } else if (1 == regexMatch(token,
                                               "^BINARY(\\([1-9][0-9]*\\))?$",
                                               REG_ICASE | REG_EXTENDED)) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_BINARY;
                    } else if (1 == regexMatch(token,
                                               "^NCHAR(\\([1-9][0-9]*\\))?$",
                                               REG_ICASE | REG_EXTENDED)) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_NCHAR;
                    } else if (0 == strcasecmp(token, "BOOL")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_BOOL;
                    } else if (0 == strcasecmp(token, "TIMESTAMP")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_TIMESTAMP;
                    } else if (0 == strcasecmp(token, "UTINYINT")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_UTINYINT;
                    } else if (0 == strcasecmp(token, "USMALLINT")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_USMALLINT;
                    } else if (0 == strcasecmp(token, "UINT")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_UINT;
                    } else if (0 == strcasecmp(token, "UBIGINT")) {
                        g_args.data_type[index] = TSDB_DATA_TYPE_UBIGINT;
                    } else {
                        g_args.data_type[index] = TSDB_DATA_TYPE_NULL;
                    }
                    g_args.dataType[index] = token;
                    index++;
                    token = strsep(&running, ",");
                    if (index >= MAX_NUM_COLUMNS) break;
                }
                g_args.dataType[index] = NULL;
                g_args.data_type[index] = TSDB_DATA_TYPE_NULL;
            }
        } else if ((0 == strncmp(argv[i], "-w", strlen("-w"))) ||
                   (0 ==
                    strncmp(argv[i], "--binwidth", strlen("--binwidth")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "w");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "w");
                    goto end_parse_command;
                }
                g_args.binwidth = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--binwidth=", strlen("--binwidth="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--binwidth=")))) {
                    g_args.binwidth =
                        atoi((char *)(argv[i] + strlen("--binwidth=")));
                } else {
                    errorPrintReqArg2(argv[0], "--binwidth");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-w", strlen("-w"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-w")))) {
                    g_args.binwidth = atoi((char *)(argv[i] + strlen("-w")));
                } else {
                    errorPrintReqArg2(argv[0], "-w");
                    goto end_parse_command;
                }
            } else if (strlen("--binwidth") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--binwidth");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--binwidth");
                    goto end_parse_command;
                }
                g_args.binwidth = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-m", strlen("-m"))) ||
                   (0 == strncmp(argv[i], "--table-prefix",
                                 strlen("--table-prefix")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "m");
                    goto end_parse_command;
                }
                g_args.tb_prefix = argv[++i];
            } else if (0 == strncmp(argv[i], "--table-prefix=",
                                    strlen("--table-prefix="))) {
                g_args.tb_prefix =
                    (char *)(argv[i] + strlen("--table-prefix="));
            } else if (0 == strncmp(argv[i], "-m", strlen("-m"))) {
                g_args.tb_prefix = (char *)(argv[i] + strlen("-m"));
            } else if (strlen("--table-prefix") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--table-prefix");
                    goto end_parse_command;
                }
                g_args.tb_prefix = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-E", strlen("-E"))) ||
                   (0 == strncmp(argv[i], "--escape-character",
                                 strlen("--escape-character")))) {
            g_args.escapeChar = true;
        } else if ((0 == strncmp(argv[i], "-C", strlen("-C"))) ||
        (0 == strncmp(argv[i], "--chinese",
                      strlen("--chinese")))) {
            g_args.chinese = true;
        } else if ((strcmp(argv[i], "-N") == 0) ||
                   (0 == strcmp(argv[i], "--normal-table"))) {
            g_args.demo_mode = false;
            g_args.use_metric = false;
        } else if ((strcmp(argv[i], "-M") == 0) ||
                   (0 == strcmp(argv[i], "--random"))) {
            g_args.demo_mode = false;
        } else if ((strcmp(argv[i], "-x") == 0) ||
                   (0 == strcmp(argv[i], "--aggr-func"))) {
            g_args.aggr_func = true;
        } else if ((strcmp(argv[i], "-y") == 0) ||
                   (0 == strcmp(argv[i], "--answer-yes"))) {
            g_args.answer_yes = true;
        } else if ((strcmp(argv[i], "-g") == 0) ||
                   (0 == strcmp(argv[i], "--debug"))) {
            g_args.debug_print = true;
        } else if (strcmp(argv[i], "-gg") == 0) {
            g_args.verbose_print = true;
        } else if ((0 == strncmp(argv[i], "-R", strlen("-R"))) ||
                   (0 == strncmp(argv[i], "--disorder-range",
                                 strlen("--disorder-range")))) {
            if (strlen("-R") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "R");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "R");
                    goto end_parse_command;
                }
                g_args.disorderRange = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--disorder-range=",
                                    strlen("--disorder-range="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--disorder-range=")))) {
                    g_args.disorderRange =
                        atoi((char *)(argv[i] + strlen("--disorder-range=")));
                } else {
                    errorPrintReqArg2(argv[0], "--disorder-range");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-R", strlen("-R"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-R")))) {
                    g_args.disorderRange =
                        atoi((char *)(argv[i] + strlen("-R")));
                } else {
                    errorPrintReqArg2(argv[0], "-R");
                    goto end_parse_command;
                }

                if (g_args.disorderRange < 0) {
                    errorPrint("Invalid disorder range %d, will be set to %d\n",
                               g_args.disorderRange, 1000);
                    g_args.disorderRange = 1000;
                }
            } else if (strlen("--disorder-range") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--disorder-range");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--disorder-range");
                    goto end_parse_command;
                }
                g_args.disorderRange = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-O", strlen("-O"))) ||
                   (0 ==
                    strncmp(argv[i], "--disorder", strlen("--disorder")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "O");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "O");
                    goto end_parse_command;
                }
                g_args.disorderRatio = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--disorder=", strlen("--disorder="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--disorder=")))) {
                    g_args.disorderRatio =
                        atoi((char *)(argv[i] + strlen("--disorder=")));
                } else {
                    errorPrintReqArg2(argv[0], "--disorder");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-O", strlen("-O"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-O")))) {
                    g_args.disorderRatio =
                        atoi((char *)(argv[i] + strlen("-O")));
                } else {
                    errorPrintReqArg2(argv[0], "-O");
                    goto end_parse_command;
                }
            } else if (strlen("--disorder") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--disorder");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--disorder");
                    goto end_parse_command;
                }
                g_args.disorderRatio = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            if (g_args.disorderRatio > 50) {
                errorPrint("Invalid disorder ratio %d, will be set to %d\n",
                           g_args.disorderRatio, 50);
                g_args.disorderRatio = 50;
            }
        } else if ((0 == strncmp(argv[i], "-a", strlen("-a"))) ||
                   (0 == strncmp(argv[i], "--replica", strlen("--replica")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "a");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "a");
                    goto end_parse_command;
                }
                g_args.replica = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--replica=", strlen("--replica="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--replica=")))) {
                    g_args.replica =
                        atoi((char *)(argv[i] + strlen("--replica=")));
                } else {
                    errorPrintReqArg2(argv[0], "--replica");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-a", strlen("-a"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-a")))) {
                    g_args.replica = atoi((char *)(argv[i] + strlen("-a")));
                } else {
                    errorPrintReqArg2(argv[0], "-a");
                    goto end_parse_command;
                }
            } else if (strlen("--replica") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--replica");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--replica");
                    goto end_parse_command;
                }
                g_args.replica = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            if (g_args.replica > 3 || g_args.replica < 1) {
                errorPrint("Invalid replica value %d, will be set to %d\n",
                           g_args.replica, 1);
                g_args.replica = 1;
            }
        } else if (strcmp(argv[i], "-D") == 0) {
            g_args.method_of_delete = atoi(argv[++i]);
            if (g_args.method_of_delete > 3) {
                errorPrint("%s",
                           "\n\t-D need a value (0~3) number following!\n");
                goto end_parse_command;
            }
        } else if ((strcmp(argv[i], "--version") == 0) ||
                   (strcmp(argv[i], "-V") == 0)) {
            printVersion();
        } else if ((strcmp(argv[i], "--help") == 0) ||
                   (strcmp(argv[i], "-?") == 0)) {
            printHelp();
        } else if (strcmp(argv[i], "--usage") == 0) {
            printf(
                "    Usage: taosdemo [-f JSONFILE] [-u USER] [-p PASSWORD] [-c CONFIG_DIR]\n\
                    [-h HOST] [-P PORT] [-I INTERFACE] [-d DATABASE] [-a REPLICA]\n\
                    [-m TABLEPREFIX] [-s SQLFILE] [-N] [-o OUTPUTFILE] [-q QUERYMODE]\n\
                    [-b DATATYPES] [-w WIDTH_OF_BINARY] [-l COLUMNS] [-T THREADNUMBER]\n\
                    [-i SLEEPTIME] [-S TIME_STEP] [-B INTERLACE_ROWS] [-t TABLES]\n\
                    [-n RECORDS] [-M] [-x] [-y] [-O ORDERMODE] [-R RANGE] [-a REPLIcA][-g]\n\
                    [--help] [--usage] [--version]\n");
            exit(EXIT_SUCCESS);
        } else {
            // to simulate argp_option output
            if (strlen(argv[i]) > 2) {
                if (0 == strncmp(argv[i], "--", 2)) {
                    fprintf(stderr, "%s: unrecognized options '%s'\n", argv[0],
                            argv[i]);
                } else if (0 == strncmp(argv[i], "-", 1)) {
                    char tmp[2] = {0};
                    tstrncpy(tmp, argv[i] + 1, 2);
                    fprintf(stderr, "%s: invalid options -- '%s'\n", argv[0],
                            tmp);
                } else {
                    fprintf(stderr, "%s: Too many arguments\n", argv[0]);
                }
            } else {
                fprintf(stderr, "%s invalid options -- '%s'\n", argv[0],
                        (char *)((char *)argv[i]) + 1);
            }
            fprintf(stderr,
                    "Try `taosdemo --help' or `taosdemo --usage' for more "
                    "information.\n");
            goto end_parse_command;
        }
    }

    int columnCount;
    for (columnCount = 0; columnCount < MAX_NUM_COLUMNS; columnCount++) {
        if (g_args.dataType[columnCount] == NULL) {
            break;
        }
    }

    if (0 == columnCount) {
        errorPrint("%s", "data type error!\n");
        goto end_parse_command;
    }
    g_args.columnCount = columnCount;

    g_args.lenOfOneRow = TIMESTAMP_BUFF_LEN;  // timestamp
    for (int c = 0; c < g_args.columnCount; c++) {
        switch (g_args.data_type[c]) {
            case TSDB_DATA_TYPE_BINARY:
                g_args.lenOfOneRow += g_args.binwidth + 3;
                break;

            case TSDB_DATA_TYPE_NCHAR:
                g_args.lenOfOneRow += g_args.binwidth + 3;
                break;

            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                g_args.lenOfOneRow += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                g_args.lenOfOneRow += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                g_args.lenOfOneRow += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                g_args.lenOfOneRow += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                g_args.lenOfOneRow += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                g_args.lenOfOneRow += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                g_args.lenOfOneRow += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                g_args.lenOfOneRow += TIMESTAMP_BUFF_LEN;
                break;

            default:
                errorPrint("get error data type : %s\n", g_args.dataType[c]);
                goto end_parse_command;
        }
    }

    if (((g_args.debug_print) && (NULL != g_args.metaFile)) ||
        g_args.verbose_print) {
        printf(
            "##################################################################"
            "#\n");
        printf("# meta file:                         %s\n", g_args.metaFile);
        printf("# Server IP:                         %s:%hu\n",
               g_args.host == NULL ? "localhost" : g_args.host, g_args.port);
        printf("# User:                              %s\n", g_args.user);
        printf("# Password:                          %s\n", g_args.password);
        printf("# Use metric:                        %s\n",
               g_args.use_metric ? "true" : "false");
        if (*(g_args.dataType)) {
            printf("# Specified data type:               ");
            for (int c = 0; c < MAX_NUM_COLUMNS; c++)
                if (g_args.dataType[c])
                    printf("%s,", g_args.dataType[c]);
                else
                    break;
            printf("\n");
        }
        printf("# Insertion interval:                %" PRIu64 "\n",
               g_args.insert_interval);
        printf("# Number of records per req:         %u\n", g_args.reqPerReq);
        printf("# Max SQL length:                    %" PRIu64 "\n",
               g_args.max_sql_len);
        printf("# Length of Binary:                  %d\n", g_args.binwidth);
        printf("# Number of Threads:                 %d\n", g_args.nthreads);
        printf("# Number of Tables:                  %" PRId64 "\n",
               g_args.ntables);
        printf("# Number of Data per Table:          %" PRId64 "\n",
               g_args.insertRows);
        printf("# Database name:                     %s\n", g_args.database);
        printf("# Table prefix:                      %s\n", g_args.tb_prefix);
        if (g_args.disorderRatio) {
            printf("# Data order:                        %d\n",
                   g_args.disorderRatio);
            printf("# Data out of order rate:            %d\n",
                   g_args.disorderRange);
        }
        printf("# Delete method:                     %d\n",
               g_args.method_of_delete);
        printf("# Answer yes when prompt:            %d\n", g_args.answer_yes);
        printf("# Print debug info:                  %d\n", g_args.debug_print);
        printf("# Print verbose info:                %d\n",
               g_args.verbose_print);
        printf(
            "##################################################################"
            "#\n");

        prompt();
    }
    code = 0;
end_parse_command:
    return code;
}
void setParaFromArg() {
    char type[20];
    char length[20];
    if (g_args.host) {
        tstrncpy(g_Dbs.host, g_args.host, MAX_HOSTNAME_SIZE);
    } else {
        tstrncpy(g_Dbs.host, "127.0.0.1", MAX_HOSTNAME_SIZE);
    }

    if (g_args.user) {
        tstrncpy(g_Dbs.user, g_args.user, MAX_USERNAME_SIZE);
    }

    tstrncpy(g_Dbs.password, g_args.password, SHELL_MAX_PASSWORD_LEN);

    if (g_args.port) {
        g_Dbs.port = g_args.port;
    }

    g_Dbs.threadCount = g_args.nthreads;
    g_Dbs.threadCountForCreateTbl = g_args.nthreads;

    g_Dbs.dbCount = 1;
    g_Dbs.db[0].drop = true;

    tstrncpy(g_Dbs.db[0].dbName, g_args.database, TSDB_DB_NAME_LEN);
    g_Dbs.db[0].dbCfg.replica = g_args.replica;
    tstrncpy(g_Dbs.db[0].dbCfg.precision, "ms", SMALL_BUFF_LEN);

    tstrncpy(g_Dbs.resultFile, g_args.output_file, MAX_FILE_NAME_LEN);

    g_Dbs.use_metric = g_args.use_metric;
    g_args.prepared_rand = min(g_args.insertRows, MAX_PREPARED_RAND);
    g_Dbs.aggr_func = g_args.aggr_func;

    char     dataString[TSDB_MAX_BYTES_PER_ROW];
    char *   data_type = g_args.data_type;
    char **  dataType = g_args.dataType;
    int32_t *data_length = g_args.data_length;

    memset(dataString, 0, TSDB_MAX_BYTES_PER_ROW);

    if ((data_type[0] == TSDB_DATA_TYPE_BINARY) ||
        (data_type[0] == TSDB_DATA_TYPE_BOOL) ||
        (data_type[0] == TSDB_DATA_TYPE_NCHAR)) {
        g_Dbs.aggr_func = false;
    }

    if (g_args.use_metric) {
        g_Dbs.db[0].superTblCount = 1;
        tstrncpy(g_Dbs.db[0].superTbls[0].stbName, "meters",
                 TSDB_TABLE_NAME_LEN);
        g_Dbs.db[0].superTbls[0].childTblCount = g_args.ntables;
        g_Dbs.db[0].superTbls[0].escapeChar = g_args.escapeChar;
        g_Dbs.threadCount = g_args.nthreads;
        g_Dbs.threadCountForCreateTbl = g_args.nthreads;
        g_Dbs.asyncMode = g_args.async_mode;

        g_Dbs.db[0].superTbls[0].autoCreateTable = PRE_CREATE_SUBTBL;
        g_Dbs.db[0].superTbls[0].childTblExists = TBL_NO_EXISTS;
        g_Dbs.db[0].superTbls[0].disorderRange = g_args.disorderRange;
        g_Dbs.db[0].superTbls[0].disorderRatio = g_args.disorderRatio;
        tstrncpy(g_Dbs.db[0].superTbls[0].childTblPrefix, g_args.tb_prefix,
                 TBNAME_PREFIX_LEN);
        tstrncpy(g_Dbs.db[0].superTbls[0].dataSource, "rand", SMALL_BUFF_LEN);

        if (g_args.iface == INTERFACE_BUT) {
            g_Dbs.db[0].superTbls[0].iface = TAOSC_IFACE;
        } else {
            g_Dbs.db[0].superTbls[0].iface = g_args.iface;
        }
        g_Dbs.db[0].superTbls[0].lineProtocol = TSDB_SML_LINE_PROTOCOL;
        g_Dbs.db[0].superTbls[0].tsPrecision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
        tstrncpy(g_Dbs.db[0].superTbls[0].startTimestamp,
                 "2017-07-14 10:40:00.000", MAX_TB_NAME_SIZE);
        g_Dbs.db[0].superTbls[0].timeStampStep = g_args.timestamp_step;

        g_Dbs.db[0].superTbls[0].insertRows = g_args.insertRows;
        g_Dbs.db[0].superTbls[0].maxSqlLen = g_args.max_sql_len;

        g_Dbs.db[0].superTbls[0].columnCount = 0;
        for (int i = 0; i < MAX_NUM_COLUMNS; i++) {
            if (data_type[i] == TSDB_DATA_TYPE_NULL) {
                break;
            }

            g_Dbs.db[0].superTbls[0].columns[i].data_type = data_type[i];
            tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType, dataType[i],
                     min(DATATYPE_BUFF_LEN, strlen(dataType[i]) + 1));
            if (1 == regexMatch(dataType[i],
                                "^(NCHAR|BINARY)(\\([1-9][0-9]*\\))$",
                                REG_ICASE | REG_EXTENDED)) {
                sscanf(dataType[i], "%[^(](%[^)]", type, length);
                g_Dbs.db[0].superTbls[0].columns[i].dataLen = atoi(length);
                tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType, type,
                         min(DATATYPE_BUFF_LEN, strlen(type) + 1));
            } else {
                switch (g_Dbs.db[0].superTbls[0].columns[i].data_type) {
                    case TSDB_DATA_TYPE_BOOL:
                    case TSDB_DATA_TYPE_UTINYINT:
                    case TSDB_DATA_TYPE_TINYINT:
                        g_Dbs.db[0].superTbls[0].columns[i].dataLen =
                            sizeof(char);
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                    case TSDB_DATA_TYPE_USMALLINT:
                        g_Dbs.db[0].superTbls[0].columns[i].dataLen =
                            sizeof(int16_t);
                        break;
                    case TSDB_DATA_TYPE_INT:
                    case TSDB_DATA_TYPE_UINT:
                        g_Dbs.db[0].superTbls[0].columns[i].dataLen =
                            sizeof(int32_t);
                        break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                    case TSDB_DATA_TYPE_BIGINT:
                    case TSDB_DATA_TYPE_UBIGINT:
                        g_Dbs.db[0].superTbls[0].columns[i].dataLen =
                            sizeof(int64_t);
                        break;
                    case TSDB_DATA_TYPE_FLOAT:
                        g_Dbs.db[0].superTbls[0].columns[i].dataLen =
                            sizeof(float);
                        break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        g_Dbs.db[0].superTbls[0].columns[i].dataLen =
                            sizeof(double);
                        break;
                    default:
                        g_Dbs.db[0].superTbls[0].columns[i].dataLen =
                            g_args.binwidth;
                        break;
                }
                tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType,
                         dataType[i],
                         min(DATATYPE_BUFF_LEN, strlen(dataType[i]) + 1));
            }
            g_Dbs.db[0].superTbls[0].columnCount++;
        }

        if (g_Dbs.db[0].superTbls[0].columnCount > g_args.columnCount) {
            g_Dbs.db[0].superTbls[0].columnCount = g_args.columnCount;
        } else {
            for (int i = g_Dbs.db[0].superTbls[0].columnCount;
                 i < g_args.columnCount; i++) {
                g_Dbs.db[0].superTbls[0].columns[i].data_type =
                    TSDB_DATA_TYPE_INT;
                tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType, "INT",
                         min(DATATYPE_BUFF_LEN, strlen("INT") + 1));
                g_Dbs.db[0].superTbls[0].columns[i].dataLen = sizeof(int32_t);
                g_Dbs.db[0].superTbls[0].columnCount++;
            }
        }

        tstrncpy(g_Dbs.db[0].superTbls[0].tags[0].dataType, "INT",
                 min(DATATYPE_BUFF_LEN, strlen("INT") + 1));
        g_Dbs.db[0].superTbls[0].tags[0].data_type = TSDB_DATA_TYPE_INT;
        g_Dbs.db[0].superTbls[0].tags[0].dataLen = sizeof(int32_t);

        tstrncpy(g_Dbs.db[0].superTbls[0].tags[1].dataType, "BINARY",
                 min(DATATYPE_BUFF_LEN, strlen("BINARY") + 1));
        g_Dbs.db[0].superTbls[0].tags[1].data_type = TSDB_DATA_TYPE_BINARY;
        g_Dbs.db[0].superTbls[0].tags[1].dataLen = g_args.binwidth;
        g_Dbs.db[0].superTbls[0].tagCount = 2;
    } else {
        g_Dbs.threadCountForCreateTbl = g_args.nthreads;
        g_Dbs.db[0].superTbls[0].tagCount = 0;
        for (int i = 0; i < MAX_NUM_COLUMNS; i++) {
            if (data_type[i] == TSDB_DATA_TYPE_NULL) {
                break;
            }
            if (1 == regexMatch(dataType[i],
                                "^(NCHAR|BINARY)(\\([1-9][0-9]*\\))$",
                                REG_ICASE | REG_EXTENDED)) {
                sscanf(dataType[i], "%[^(](%[^)]", type, length);
                data_length[i] = atoi(length);
            } else {
                switch (data_type[i]) {
                    case TSDB_DATA_TYPE_BOOL:
                    case TSDB_DATA_TYPE_UTINYINT:
                    case TSDB_DATA_TYPE_TINYINT:
                        data_length[i] = sizeof(char);
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                    case TSDB_DATA_TYPE_USMALLINT:
                        data_length[i] = sizeof(int16_t);
                        break;
                    case TSDB_DATA_TYPE_INT:
                    case TSDB_DATA_TYPE_UINT:
                        data_length[i] = sizeof(int32_t);
                        break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                    case TSDB_DATA_TYPE_BIGINT:
                    case TSDB_DATA_TYPE_UBIGINT:
                        data_length[i] = sizeof(int64_t);
                        break;
                    case TSDB_DATA_TYPE_FLOAT:
                        data_length[i] = sizeof(float);
                        break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        data_length[i] = sizeof(double);
                        break;
                    default:
                        data_length[i] = g_args.binwidth;
                        break;
                }
            }
        }
    }
}

void querySqlFile(TAOS *taos, char *sqlFile) {
    FILE *fp = fopen(sqlFile, "r");
    if (fp == NULL) {
        printf("failed to open file %s, reason:%s\n", sqlFile, strerror(errno));
        return;
    }

    int   read_len = 0;
    char *cmd = calloc(1, TSDB_MAX_BYTES_PER_ROW);
    if (cmd == NULL) {
        errorPrint("%s", "failde to allocate memory\n");
        return;
    }
    size_t cmd_len = 0;
    char * line = NULL;
    size_t line_len = 0;

    double t = (double)taosGetTimestampMs();

    while ((read_len = tgetline(&line, &line_len, fp)) != -1) {
        if (read_len >= TSDB_MAX_BYTES_PER_ROW) continue;
        line[--read_len] = '\0';

        if (read_len == 0 || isCommentLine(line)) {  // line starts with #
            continue;
        }

        if (line[read_len - 1] == '\\') {
            line[read_len - 1] = ' ';
            memcpy(cmd + cmd_len, line, read_len);
            cmd_len += read_len;
            continue;
        }

        memcpy(cmd + cmd_len, line, read_len);
        if (0 != queryDbExec(taos, cmd, NO_INSERT_TYPE, false)) {
            errorPrint("queryDbExec %s failed!\n", cmd);
            tmfree(cmd);
            tmfree(line);
            tmfclose(fp);
            return;
        }
        memset(cmd, 0, TSDB_MAX_BYTES_PER_ROW);
        cmd_len = 0;
    }

    t = taosGetTimestampMs() - t;
    printf("run %s took %.6f second(s)\n\n", sqlFile, t / 1000000);

    tmfree(cmd);
    tmfree(line);
    tmfclose(fp);
    return;
}

void *queryStableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->taos;
    setThreadName("queryStableAggrFunc");
    char *command = calloc(1, BUFFER_SIZE);
    if (NULL == command) {
        errorPrint("%s", "failed to allocate memory\n");
        return NULL;
    }

    FILE *fp = fopen(pThreadInfo->filePath, "a");
    if (NULL == fp) {
        errorPrint("fopen %s fail, reason:%s.\n", pThreadInfo->filePath,
                   strerror(errno));
        free(command);
        return NULL;
    }

    int64_t insertRows = pThreadInfo->stbInfo->insertRows;
    int64_t ntables =
        pThreadInfo->ntables;  // pThreadInfo->end_table_to -
                               // pThreadInfo->start_table_from + 1;
    int64_t totalData = insertRows * ntables;
    bool    aggr_func = g_Dbs.aggr_func;

    char **aggreFunc;
    int    n;

    if (g_args.demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = aggr_func ? (sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]))
                      : 2;
    } else {
        aggreFunc = g_aggreFunc;
        n = aggr_func ? (sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0])) : 2;
    }

    if (!aggr_func) {
        printf(
            "\nThe first field is either Binary or Bool. Aggregation functions "
            "are not supported.\n");
    }

    printf("%" PRId64 " records:\n", totalData);
    fprintf(fp, "Querying On %" PRId64 " records:\n", totalData);

    for (int j = 0; j < n; j++) {
        char condition[COND_BUF_LEN] = "\0";
        char tempS[64] = "\0";

        int64_t m = 10 < ntables ? 10 : ntables;

        for (int64_t i = 1; i <= m; i++) {
            if (i == 1) {
                if (g_args.demo_mode) {
                    sprintf(tempS, "groupid = %" PRId64 "", i);
                } else {
                    sprintf(tempS, "t0 = %" PRId64 "", i);
                }
            } else {
                if (g_args.demo_mode) {
                    sprintf(tempS, " or groupid = %" PRId64 " ", i);
                } else {
                    sprintf(tempS, " or t0 = %" PRId64 " ", i);
                }
            }
            strncat(condition, tempS, COND_BUF_LEN - 1);

            sprintf(command, "SELECT %s FROM meters WHERE %s", aggreFunc[j],
                    condition);

            printf("Where condition: %s\n", condition);

            debugPrint("%s() LN%d, sql command: %s\n", __func__, __LINE__,
                       command);
            fprintf(fp, "%s\n", command);

            double t = (double)taosGetTimestampUs();

            TAOS_RES *pSql = taos_query(taos, command);
            int32_t   code = taos_errno(pSql);

            if (code != 0) {
                errorPrint("Failed to query:%s\n", taos_errstr(pSql));
                taos_free_result(pSql);
                fclose(fp);
                free(command);
                return NULL;
            }
            int count = 0;
            while (taos_fetch_row(pSql) != NULL) {
                count++;
            }
            t = taosGetTimestampUs() - t;

            fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n",
                    ntables * insertRows / (t / 1000), t);
            printf("select %10s took %.6f second(s)\n\n", aggreFunc[j],
                   t / 1000000);

            taos_free_result(pSql);
        }
        fprintf(fp, "\n");
    }
    fclose(fp);
    free(command);

    return NULL;
}

void *queryNtableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->taos;
    setThreadName("queryNtableAggrFunc");
    char *command = calloc(1, BUFFER_SIZE);
    if (NULL == command) {
        errorPrint("%s", "failed to allocate memory\n");
        return NULL;
    }

    uint64_t startTime = pThreadInfo->start_time;
    char *   tb_prefix = pThreadInfo->tb_prefix;
    FILE *   fp = fopen(pThreadInfo->filePath, "a");
    if (NULL == fp) {
        errorPrint("fopen %s fail, reason:%s.\n", pThreadInfo->filePath,
                   strerror(errno));
        free(command);
        return NULL;
    }

    int64_t insertRows;
    /*  if (pThreadInfo->stbInfo) {
        insertRows = pThreadInfo->stbInfo->insertRows; //  nrecords_per_table;
        } else {
        */
    insertRows = g_args.insertRows;
    //  }

    int64_t ntables =
        pThreadInfo->ntables;  // pThreadInfo->end_table_to -
                               // pThreadInfo->start_table_from + 1;
    int64_t totalData = insertRows * ntables;
    bool    aggr_func = g_Dbs.aggr_func;

    char **aggreFunc;
    int    n;

    if (g_args.demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = aggr_func ? (sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]))
                      : 2;
    } else {
        aggreFunc = g_aggreFunc;
        n = aggr_func ? (sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0])) : 2;
    }

    if (!aggr_func) {
        printf(
            "\nThe first field is either Binary or Bool. Aggregation functions "
            "are not supported.\n");
    }
    printf("%" PRId64 " records:\n", totalData);
    fprintf(
        fp,
        "| QFunctions |    QRecords    |   QSpeed(R/s)   |  QLatency(ms) |\n");

    for (int j = 0; j < n; j++) {
        double   totalT = 0;
        uint64_t count = 0;
        for (int64_t i = 0; i < ntables; i++) {
            if (g_args.escapeChar) {
                sprintf(command,
                        "SELECT %s FROM `%s%" PRId64 "` WHERE ts>= %" PRIu64,
                        aggreFunc[j], tb_prefix, i, startTime);
            } else {
                sprintf(command,
                        "SELECT %s FROM %s%" PRId64 " WHERE ts>= %" PRIu64,
                        aggreFunc[j], tb_prefix, i, startTime);
            }

            double t = (double)taosGetTimestampUs();
            debugPrint("%s() LN%d, sql command: %s\n", __func__, __LINE__,
                       command);
            TAOS_RES *pSql = taos_query(taos, command);
            int32_t   code = taos_errno(pSql);

            if (code != 0) {
                errorPrint("Failed to query <%s>, reason:%s\n", command,
                           taos_errstr(pSql));
                taos_free_result(pSql);
                fclose(fp);
                free(command);
                return NULL;
            }

            while (taos_fetch_row(pSql) != NULL) {
                count++;
            }

            t = taosGetTimestampUs() - t;
            totalT += t;

            taos_free_result(pSql);
        }

        fprintf(fp, "|%10s  |   %" PRId64 "   |  %12.2f   |   %10.2f  |\n",
                aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j], totalData,
                (double)(ntables * insertRows) / totalT, totalT / 1000000);
        printf("select %10s took %.6f second(s)\n", aggreFunc[j],
               totalT / 1000000);
    }
    fprintf(fp, "\n");
    fclose(fp);
    free(command);
    return NULL;
}

void queryAggrFunc() {
    // query data

    pthread_t   read_id;
    threadInfo *pThreadInfo = calloc(1, sizeof(threadInfo));
    if (pThreadInfo == NULL) {
        errorPrint("%s", "failde to allocate memory\n");
        return;
    }

    pThreadInfo->start_time = DEFAULT_START_TIME;  // 2017-07-14 10:40:00.000
    pThreadInfo->start_table_from = 0;

    if (g_args.use_metric) {
        pThreadInfo->ntables = g_Dbs.db[0].superTbls[0].childTblCount;
        pThreadInfo->end_table_to = g_Dbs.db[0].superTbls[0].childTblCount - 1;
        pThreadInfo->stbInfo = &g_Dbs.db[0].superTbls[0];
        tstrncpy(pThreadInfo->tb_prefix,
                 g_Dbs.db[0].superTbls[0].childTblPrefix, TBNAME_PREFIX_LEN);
    } else {
        pThreadInfo->ntables = g_args.ntables;
        pThreadInfo->end_table_to = g_args.ntables - 1;
        tstrncpy(pThreadInfo->tb_prefix, g_args.tb_prefix, TSDB_TABLE_NAME_LEN);
    }

    pThreadInfo->taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password,
                                     g_Dbs.db[0].dbName, g_Dbs.port);
    if (pThreadInfo->taos == NULL) {
        free(pThreadInfo);
        errorPrint("Failed to connect to TDengine, reason:%s\n",
                   taos_errstr(NULL));
        exit(EXIT_FAILURE);
    }

    tstrncpy(pThreadInfo->filePath, g_Dbs.resultFile, MAX_FILE_NAME_LEN);

    if (!g_Dbs.use_metric) {
        pthread_create(&read_id, NULL, queryNtableAggrFunc, pThreadInfo);
    } else {
        pthread_create(&read_id, NULL, queryStableAggrFunc, pThreadInfo);
    }
    pthread_join(read_id, NULL);
    taos_close(pThreadInfo->taos);
    free(pThreadInfo);
}

void testCmdLine() {
    if (strlen(configDir)) {
        wordexp_t full_path;
        if (wordexp(configDir, &full_path, 0) != 0) {
            errorPrint("Invalid path %s\n", configDir);
            return;
        }
        taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
        wordfree(&full_path);
    }

    g_args.test_mode = INSERT_TEST;
    insertTestProcess();

    if (g_Dbs.aggr_func) {
        queryAggrFunc();
    }
}