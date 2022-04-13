// C api call sequence demo
// to compile: gcc -o apidemo apidemo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <argp.h>

#include "taos.h"

#define debugPrint(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) \
      fprintf(stdout, "DEBG: "fmt, __VA_ARGS__); } while(0)

#define warnPrint(fmt, ...) \
    do { fprintf(stderr, "\033[33m"); \
        fprintf(stderr, "WARN: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

#define errorPrint(fmt, ...) \
    do { fprintf(stderr, "\033[31m"); \
        fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

#define okPrint(fmt, ...) \
    do { fprintf(stderr, "\033[32m"); \
        fprintf(stderr, "OK: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

int64_t g_num_of_tb = 2;
int64_t g_num_of_rec = 2;

static struct argp_option options[] = {
    {"tables", 't', "NUMBER", 0, "Number of child tables, default is 10000."},
    {"records", 'n', "NUMBER", 0,
     "Number of records for each table, default is 10000."},
    {0}};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    switch (key) {
        case 't':
            g_num_of_tb = atoll(arg);
            break;

        case 'n':
            g_num_of_rec = atoll(arg);
            break;
    }

    return 0;
}

static struct argp argp = {options, parse_opt, "", ""};

static void prepare_data(TAOS* taos) {
    TAOS_RES *res;
    res = taos_query(taos, "drop database if exists test;");
    taos_free_result(res);
    usleep(100000);

    res = taos_query(taos, "create database test;");
    taos_free_result(res);
    usleep(100000);
    taos_select_db(taos, "test");

    res = taos_query(taos, "create table meters(ts timestamp, f float, n int, b binary(20)) tags(area int, localtion binary(20));");
    taos_free_result(res);

    char command[1024] = {0};
    for (int64_t i = 0; i < g_num_of_tb; i ++) {
        sprintf(command, "create table t%"PRId64" using meters tags(%"PRId64", '%s');",
                i, i, (i%2)?"beijing":"shanghai");
        res = taos_query(taos,  command);
        taos_free_result(res);

        int64_t j = 0;
        int64_t total = 0;
        int64_t affected;
        for (; j < g_num_of_rec -1; j ++) {
            sprintf(command, "insert into t%"PRId64" values(%" PRId64 ", %f, %"PRId64", '%c%d')",
                    i, 1650000000000+j, (float)j, j, 'a'+(int)j%10, rand());
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
        sprintf(command, "insert into t%"PRId64" values(%" PRId64 ", NULL, NULL, NULL)",
                i, 1650000000000+j+1);
        res = taos_query(taos,  command);
        if ((res) && (0 == taos_errno(res))) {
            affected = taos_affected_rows(res);
            total += affected;
        } else {
            errorPrint("%s() LN%d: %s\n",
                    __func__, __LINE__, taos_errstr(res));
        }
        taos_free_result(res);

        printf("insert %"PRId64" records into t%"PRId64", total affected rows: %"PRId64"\n", j, i, total);
    }
}

static int print_result(TAOS_RES* res, int block) {
    int64_t num_rows = 0;
    TAOS_ROW    row = NULL;
    int         num_fields = taos_num_fields(res);
    TAOS_FIELD* fields = taos_fetch_fields(res);

    if (block) {
        warnPrint("%s() LN%d, call taos_fetch_block()\n", __func__, __LINE__);
        int rows = 0;
        while ((rows = taos_fetch_block(res, &row))) {
            num_rows += rows;
        }
    } else {
        warnPrint("%s() LN%d, call taos_fetch_rows()\n", __func__, __LINE__);
        while ((row = taos_fetch_row(res))) {
            char temp[256] = {0};
            taos_print_row(temp, row, fields, num_fields);
            puts(temp);
            num_rows ++;
        }
    }

    return num_rows;
}

static void verify_query(TAOS* taos) {
    // TODO: select count(tbname) from stable once stable query work
    char command[1024] = {0};

    for (int64_t i = 0; i < g_num_of_tb; i++) {
        sprintf(command, "select * from t%"PRId64"", i);
        TAOS_RES* res = taos_query(taos, command);

        if (res) {
            if (0 == taos_errno(res)) {
                int field_count = taos_field_count(res);
                printf("field_count: %d\n", field_count);
                int* lengths = taos_fetch_lengths(res);
                if (lengths) {
                for (int c = 0; c < field_count; c++) {
                    printf("length of column %d is %d\n", c, lengths[c]);
                }
                } else {
                    errorPrint("%s() LN%d: t%"PRId64"'s lengths is NULL\n",
                            __func__, __LINE__, i);
                }

               int64_t rows = print_result(res, i % 2);
               printf("rows is: %"PRId64"\n", rows);

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

    argp_parse(&argp, argc, argv, 0, 0, NULL);
    TAOS* taos = taos_connect(host, user, passwd, "", 0);
    if (taos == NULL) {
        printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
        exit(1);
    }

    const char* info = taos_get_server_info(taos);
    printf("server info: %s\n", info);
    info = taos_get_client_info(taos);
    printf("client info: %s\n", info);

    prepare_data(taos);

    verify_query(taos);

    taos_close(taos);
    printf("done\n");

    return 0;
}

