#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include "../../../include/client/taos.h"

#define PRINT_ERROR printf("\033[31m");
#define PRINT_SUCCESS printf("\033[32m");

void execute_simple_sql(void *taos, char *sql) {
    TAOS_RES *result = taos_query(taos, sql);
    if ( result == NULL || taos_errno(result) != 0) {
        PRINT_ERROR
        printf("failed to %s, Reason: %s\n", sql, taos_errstr(result));
        taos_free_result(result);
        exit(EXIT_FAILURE);
    }
    taos_free_result(result);
    PRINT_SUCCESS
    printf("Successfully %s\n", sql);
}

void check_result(TAOS *taos, int id, int expected) {
    char sql[256] = {0};
    sprintf(sql, "select * from t%d", id);
    TAOS_RES *result;
    result = taos_query(taos, sql);
    if ( result == NULL || taos_errno(result) != 0) {
        PRINT_ERROR
        printf("failed to %s, Reason: %s\n", sql, taos_errstr(result));
        exit(EXIT_FAILURE);
    }
    PRINT_SUCCESS
    printf("Successfully execute %s\n", sql);
    int rows = 0;
    TAOS_ROW row;
    while ((row = taos_fetch_row(result))) {
        rows++;
    }
    if (rows == expected) {
        PRINT_SUCCESS
        printf("table t%d's %d rows are fetched as expected\n", id, rows);
    } else {
        PRINT_ERROR
        printf("table t%d's %d rows are fetched but %d expected\n", id, rows, expected);
    }
    taos_free_result(result);
}

int main(int argc, char *argv[]) {
    void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
    if (taos == NULL) {
        PRINT_ERROR
        printf("TDengine error: failed to connect\n");
        exit(EXIT_FAILURE);
    }
    PRINT_SUCCESS
    printf("Successfully connected to TDengine\n");
    
    execute_simple_sql(taos, "drop database if exists test");
    execute_simple_sql(taos, "create database test");
    execute_simple_sql(taos, "use test");
    execute_simple_sql(taos, "create table super(ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(8), c6 smallint, c7 tinyint, c8 bool, c9 nchar(8), c10 timestamp) tags (t1 int, t2 bigint, t3 float, t4 double, t5 binary(8), t6 smallint, t7 tinyint, t8 bool, t9 nchar(8))");

    char *sql = taosMemoryCalloc(1, 1024*1024);
    int sqlLen = 0;
    sqlLen = sprintf(sql, "create table");
    for (int i = 0; i < 10; i++) {
        sqlLen += sprintf(sql + sqlLen, " t%d using super tags (%d, 2147483648, 0.1, 0.000000001, 'abcdefgh', 32767, 127, 1, '一二三四五六七八')", i, i);
    }
    execute_simple_sql(taos, sql);
    

    int code = taos_load_table_info(taos, "t0,t1,t2,t3,t4,t5,t6,t7,t8,t9");
    if (code != 0) {
        PRINT_ERROR
        printf("failed to load table info: 0x%08x\n", code);
        exit(EXIT_FAILURE);
    }
    PRINT_SUCCESS
    printf("Successfully load table info\n");

    TAOS_STMT *stmt = taos_stmt_init(taos);
    if (stmt == NULL) {
        PRINT_ERROR
        printf("TDengine error: failed to init taos_stmt\n");
        exit(EXIT_FAILURE);
    }
    PRINT_SUCCESS
    printf("Successfully init taos_stmt\n");

    uintptr_t c10len = 0;
    struct {
        int64_t c1;
        int32_t c2;
        int64_t c3;
        float c4;
        double c5;
        unsigned char c6[8];
        int16_t c7;
        int8_t c8;
        int8_t c9;
        char c10[32];
    } v = {0};
    TAOS_BIND params[11];
    params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[0].buffer_length = sizeof(v.c1);
    params[0].buffer = &v.c1;
    params[0].length = &params[0].buffer_length;
    params[0].is_null = NULL;

    params[1].buffer_type = TSDB_DATA_TYPE_INT;
    params[1].buffer_length = sizeof(v.c2);
    params[1].buffer = &v.c2;
    params[1].length = &params[1].buffer_length;
    params[1].is_null = NULL;

    params[2].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[2].buffer_length = sizeof(v.c3);
    params[2].buffer = &v.c3;
    params[2].length = &params[2].buffer_length;
    params[2].is_null = NULL;

    params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[3].buffer_length = sizeof(v.c4);
    params[3].buffer = &v.c4;
    params[3].length = &params[3].buffer_length;
    params[3].is_null = NULL;

    params[4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[4].buffer_length = sizeof(v.c5);
    params[4].buffer = &v.c5;
    params[4].length = &params[4].buffer_length;
    params[4].is_null = NULL;

    params[5].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[5].buffer_length = sizeof(v.c6);
    params[5].buffer = &v.c6;
    params[5].length = &params[5].buffer_length;
    params[5].is_null = NULL;

    params[6].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[6].buffer_length = sizeof(v.c7);
    params[6].buffer = &v.c7;
    params[6].length = &params[6].buffer_length;
    params[6].is_null = NULL;

    params[7].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[7].buffer_length = sizeof(v.c8);
    params[7].buffer = &v.c8;
    params[7].length = &params[7].buffer_length;
    params[7].is_null = NULL;

    params[8].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[8].buffer_length = sizeof(v.c9);
    params[8].buffer = &v.c9;
    params[8].length = &params[8].buffer_length;
    params[8].is_null = NULL;

    params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
    params[9].buffer_length = sizeof(v.c10);
    params[9].buffer = &v.c10;
    params[9].length = &c10len;
    params[9].is_null = NULL;

    params[10].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[10].buffer_length = sizeof(v.c1);
    params[10].buffer = &v.c1;
    params[10].length = &params[10].buffer_length;
    params[10].is_null = NULL;

    char *stmt_sql = "insert into ? values (?,?,?,?,?,?,?,?,?,?,?)";
    code = taos_stmt_prepare(stmt, stmt_sql, 0);
    if (code != 0){
        PRINT_ERROR
        printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
        exit(EXIT_FAILURE);
    }
    PRINT_SUCCESS
    printf("Successfully execute taos_stmt_prepare\n");

    for (int i = 0; i < 10; i++) {
        char buf[32];
        sprintf(buf, "t%d", i);
        if (i == 0) {
            code = taos_stmt_set_tbname(stmt, buf);
            if (code != 0) {
                PRINT_ERROR
                printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
                exit(EXIT_FAILURE);
            }
            PRINT_SUCCESS
            printf("Successfully execute taos_stmt_set_tbname\n");
        } else {
            code = taos_stmt_set_sub_tbname(stmt, buf);
            if (code != 0) {
                PRINT_ERROR
                printf("failed to execute taos_stmt_set_sub_tbname. code:0x%x\n", code);
                exit(EXIT_FAILURE);
            }
            PRINT_SUCCESS
            printf("Successfully execute taos_stmt_set_sub_tbname\n");
        }
        
        v.c1 = (int64_t)1591060628000;
        v.c2 = (int32_t)2147483647;
        v.c3 = (int64_t)2147483648;
        v.c4 = (float)0.1;
        v.c5 = (double)0.000000001;
        for (int j = 0; j < sizeof(v.c6); j++) {
            v.c6[j] = (char)('a');
        }
        v.c7 = 32767;
        v.c8 = 127;
        v.c9 = 1;
        strcpy(v.c10, "一二三四五六七八");
        c10len=strlen(v.c10);
        taos_stmt_bind_param(stmt, params);
        taos_stmt_add_batch(stmt);
    }

    if (taos_stmt_execute(stmt) != 0) {
        PRINT_ERROR
        printf("failed to execute insert statement.\n");
        exit(EXIT_FAILURE);
    }
    PRINT_SUCCESS
    printf("Successfully execute insert statement.\n");

    taos_stmt_close(stmt);
    for (int i = 0; i < 10; i++) {
        check_result(taos, i, 1);
    }
    
    return 0;
}
