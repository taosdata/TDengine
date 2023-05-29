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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// #include <unistd.h>
#include <inttypes.h>
#ifndef WINDOWS
#include <argp.h>
#endif
#include "taos.h"

#define debugPrint(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) {\
      fprintf(stdout, "DEBG: "fmt, __VA_ARGS__); }} while (0)

#define warnPrint(fmt, ...) \
    do { fprintf(stderr, "\033[33m"); \
        fprintf(stderr, "WARN: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while (0)

#define errorPrint(fmt, ...) \
    do { fprintf(stderr, "\033[31m"); \
        fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while (0)

#define okPrint(fmt, ...) \
    do { fprintf(stderr, "\033[32m"); \
        fprintf(stderr, "OK: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while (0)

int64_t g_num_of_tb = 2;
int64_t g_num_of_rec = 3;

#ifndef WINDOWS
static struct argp_option options[] = {
    {"tables", 't', "NUMBER", 0, "Number of child tables, default is 10000."},
    {"records", 'n', "NUMBER", 0,
     "Number of records for each table, default is 10000."},
    {0}};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    switch (key) {
        case 't':
            g_num_of_tb = atoll(arg);
            if (g_num_of_tb < 1) {
                warnPrint("minimal g_num_of_tb is %d\n", 1);
                g_num_of_tb = 1;
            }
            break;

        case 'n':
            g_num_of_rec = atoll(arg);
            if (g_num_of_rec < 2) {
                warnPrint("minimal g_num_of_rec is %d\n", 2);
                g_num_of_rec = 2;
            }
            break;
    }

    return 0;
}

static struct argp argp = {options, parse_opt, "", ""};
#endif
static void prepare_data(TAOS* taos) {
    TAOS_RES *res;
    res = taos_query(taos, "drop database if exists test;");
    taos_free_result(res);

    res = taos_query(taos, "create database test;");
    taos_free_result(res);
    if (taos_select_db(taos, "test")) {
        errorPrint("%s() LN%d: taos_select_db() failed\n",
                __func__, __LINE__);
        return;
    }

    char command[1024] = {0};
    sprintf(command, "%s", "create table meters(ts timestamp, f float, n int, "
            "bin1 binary(20), c nchar(20), bin2 binary(20)) tags(area int, "
            "city binary(20), dist nchar(20), street binary(20));");
    res = taos_query(taos, command);
    if ((res) && (0 == taos_errno(res))) {
        okPrint("%s created\n", "meters");
    } else {
        errorPrint("%s() LN%d: %s\n",
                __func__, __LINE__, taos_errstr(res));
        taos_free_result(res);
        exit(1);
    }
    taos_free_result(res);

    for (int64_t i = 0; i < g_num_of_tb; i ++) {
        // sprintf(command, "create table t%"PRId64" using meters "
        //         "tags(%"PRId64", '%s', '%s', '%s');",
        //         i, i, (i%2)?"beijing":"shanghai",
        //         (i%2)?"朝阳区":"黄浦区",
        //         (i%2)?"长安街":"中山路");
        sprintf(command, "create table t%"PRId64" using meters "
                "tags(%"PRId64", '%s', '%s', '%s');",
                i, i,
                (i%2)?"beijing":"shanghai",
                (i%2)?"chaoyang":"huangpu",
                (i%2?"changan street":"jianguo rd"));
        res = taos_query(taos,  command);
        if ((res) && (0 == taos_errno(res))) {
            okPrint("t%" PRId64 " created\n", i);
        } else {
            errorPrint("%s() LN%d: %s\n",
                    __func__, __LINE__, taos_errstr(res));
        }
        taos_free_result(res);

        int64_t j = 0;
        int64_t total = 0;
        int64_t affected;
        for (; j < g_num_of_rec -2; j ++) {
            sprintf(command, "insert into t%"PRId64" "
                    "values(%" PRId64 ", %f, %"PRId64", "
                    "'%c%d', '%s%c%d', '%c%d')",
                    i, 1650000000000+j, j * 1.0, j,
                    'a'+(int)j%25, rand(),
                    // "涛思", 'z' - (int)j%25, rand(),
                    "TAOS", 'z' - (int)j%25, rand(),
                    'b' - (int)j%25, rand());
            res = taos_query(taos,  command);
            if ((res) && (0 == taos_errno(res))) {
                affected = taos_affected_rows(res);
                total += affected;
            } else {
                errorPrint("%s() LN%d: %s\n",
                        __func__, __LINE__, taos_errstr(res));
            }
            taos_free_result(res);
        }
        sprintf(command, "insert into t%"PRId64" values(%" PRId64 ", "
                "NULL, NULL, NULL, NULL, NULL)",
                i, 1650000000000+j);
        res = taos_query(taos,  command);
        if ((res) && (0 == taos_errno(res))) {
            affected = taos_affected_rows(res);
            total += affected;
        } else {
            errorPrint("%s() LN%d: %s\n",
                    __func__, __LINE__, taos_errstr(res));
        }
        sprintf(command, "insert into t%"PRId64" "
                "values(%" PRId64 ", %f, %"PRId64", "
                "'%c%d', '%s%c%d', '%c%d')",
                i, 1650000000000+j+1, (float)j, j,
                'a'+(int)j%25, rand(),
                "数据", 'z' - (int)j%25, rand(),
                'b' - (int)j%25, rand());
        res = taos_query(taos,  command);
        if ((res) && (0 == taos_errno(res))) {
            affected = taos_affected_rows(res);
            total += affected;
        } else {
            errorPrint("%s() LN%d: %s\n",
                    __func__, __LINE__, taos_errstr(res));
        }
        taos_free_result(res);

        okPrint("insert %"PRId64" records into t%"PRId64", "
                "total affected rows: %"PRId64"\n", j, i, total);
    }
}

static int print_result(char *tbname, TAOS_RES* res, int block) {
    int64_t num_rows = 0;
    TAOS_ROW    row = NULL;
    int         num_fields = taos_num_fields(res);
    TAOS_FIELD* fields = taos_fetch_fields(res);

    for (int f = 0; f < num_fields; f++) {
        printf("fields[%d].name=%s, fields[%d].type=%d, fields[%d].bytes=%d\n",
                f, fields[f].name, f, fields[f].type, f, fields[f].bytes);
    }

    if (block) {
        warnPrint("%s", "call taos_fetch_block(), "
                "don't call taos_fetch_lengths()\n");
        int rows = 0;
        while ((rows = taos_fetch_block(res, &row))) {
            int *lengths = taos_fetch_lengths(res);
            for (int f = 0; f < num_fields; f++) {
                if ((fields[f].type != TSDB_DATA_TYPE_VARCHAR)
                        && (fields[f].type != TSDB_DATA_TYPE_NCHAR)
                        && (fields[f].type != TSDB_DATA_TYPE_JSON)
                        && (fields[f].type != TSDB_DATA_TYPE_GEOMETRY)) {
                    printf("col%d type is %d, no need get offset\n",
                            f, fields[f].type);
                    for (int64_t c = 0; c < rows; c++) {
                        switch (fields[f].type) {
                            case TSDB_DATA_TYPE_TIMESTAMP:
                                if (taos_is_null(res, c, f)) {
                                    printf("col%d, row: %"PRId64" "
                                            "value: NULL\n", f, c);
                                } else {
                                    printf("col%d, row: %"PRId64", "
                                            "value: %"PRId64"\n",
                                            f, c,
                                            *(int64_t*)((char*)(row[f])
                                                +c*sizeof(int64_t)));
                                }
                                break;

                            case TSDB_DATA_TYPE_INT:
                                if (taos_is_null(res, c, f)) {
                                    printf("col%d, row: %"PRId64" "
                                            "value: NULL\n", f, c);
                                } else {
                                    printf("col%d, row: %"PRId64", "
                                            "value: %d\n",
                                            f, c,
                                            *(int32_t*)((char*)(row[f])
                                                +c*sizeof(int32_t)));
                                }
                                break;

                            case TSDB_DATA_TYPE_FLOAT:
                                if (taos_is_null(res, c, f)) {
                                    printf("col%d, row: %"PRId64" "
                                            "value: NULL\n", f, c);
                                } else {
                                    printf("col%d, row: %"PRId64", "
                                            "value: %f\n",
                                            f, c,
                                            *(float*)((char*)(row[f])
                                                +c*sizeof(float)));
                                }
                                break;

                            default:
                                printf("type: %d is not processed\n",
                                        fields[f].type);
                                break;
                        }
                    }
                } else {
                    int *offsets = taos_get_column_data_offset(res, f);
                    if (offsets) {
                        for (int c = 0; c < rows; c++) {
                            if (offsets[c] != -1) {
                                int length = *(int16_t*)((char*)(row[f])
                                        + offsets[c]);
                                char *buf = calloc(1, length + 1);
                                strncpy(buf, (char *)((char*)(row[f])
                                            + offsets[c] + 2), length);
                                printf("row: %d, col: %d, offset: %d, "
                                        "length: %d, content: %s\n",
                                        c, f, offsets[c], length, buf);
                                free(buf);
                            } else {
                                printf("row: %d, col: %d, offset: -1, "
                                        "means content is NULL\n",
                                        c, f);
                            }
                        }
                    } else {
                        errorPrint("%s() LN%d: col%d's offsets is NULL\n",
                                __func__, __LINE__, f);
                    }
                }
            }
            num_rows += rows;
        }
    } else {
        warnPrint("%s", "call taos_fetch_rows()\n");
        while ((row = taos_fetch_row(res))) {
            char temp[256] = {0};
            taos_print_row(temp, row, fields, num_fields);
            puts(temp);

            int* lengths = taos_fetch_lengths(res);
            if (lengths) {
                for (int c = 0; c < num_fields; c++) {
                    printf("row: %"PRId64", col: %d, is_null: %s, "
                            "length of column %d is %d\n",
                            num_rows, c,
                            taos_is_null(res, num_rows, c)?"True":"False",
                            c, lengths[c]);
                }
            } else {
                errorPrint("%s() LN%d: %s's lengths is NULL\n",
                        __func__, __LINE__, tbname);
            }

            num_rows++;
        }
    }

    return num_rows;
}

static void verify_query(TAOS* taos) {
    // TODO: select count(tbname) from stable once stable query work
    //
    char tbname[193] = {0};
    char command[1024] = {0};

    for (int64_t i = 0; i < g_num_of_tb; i++) {
        sprintf(tbname, "t%"PRId64"", i);
        sprintf(command, "select * from %s", tbname);
        TAOS_RES* res = taos_query(taos, command);

        if (res) {
            if (0 == taos_errno(res)) {
                int field_count = taos_field_count(res);
                printf("field_count: %d\n", field_count);
                int64_t rows = print_result(tbname, res, i % 2);
                okPrint("total query %s result rows is: %"PRId64"\n",
                        tbname, rows);
            } else {
                errorPrint("%s() LN%d: %s\n",
                        __func__, __LINE__, taos_errstr(res));
            }
        } else {
            errorPrint("%s() LN%d: %s\n",
                    __func__, __LINE__, taos_errstr(res));
        }
    }
}

int main(int argc, char *argv[]) {
    const char* host = "127.0.0.1";
    const char* user = "root";
    const char* passwd = "taosdata";
#ifndef WINDOWS
    argp_parse(&argp, argc, argv, 0, 0, NULL);
#endif
    TAOS* taos = taos_connect(host, user, passwd, "", 0);
    if (taos == NULL) {
        printf("\033[31mfailed to connect to db, reason:%s\033[0m\n",
                taos_errstr(taos));
        exit(1);
    }

    const char* info = taos_get_server_info(taos);
    printf("server info: %s\n", info);
    info = taos_get_client_info(taos);
    printf("client info: %s\n", info);

    prepare_data(taos);

    verify_query(taos);

    taos_close(taos);
    okPrint("%s", "done\n");

    return 0;
}

