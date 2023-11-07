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

void check_result(TAOS_RES *result, int expected) {
    int rows = 0;
    TAOS_ROW row;
    while ((row = taos_fetch_row(result))) {
        rows++;
    }
    if (rows == expected) {
        PRINT_SUCCESS
        printf("%d rows are fetched as expected\n", rows);
    } else {
        PRINT_ERROR
        printf("%d rows are fetched but %d expected\n", rows, expected);
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

    char *sql = calloc(1, 1024*1024);
    int sqlLen = 0;
    sqlLen = sprintf(sql, "create table");
    for (int i = 0; i < 10; i++) {
        sqlLen += sprintf(sql + sqlLen, " t%d using super tags (%d, 2147483648, 0.1, 0.000000001, 'abcdefgh', 32767, 127, 1, '一二三四五六七八')", i, i);
    }
    execute_simple_sql(taos, sql);

    strcpy(sql, "insert into t0 (ts, c1) values(now, 1)");
    execute_simple_sql(taos, sql);

    strcpy(sql, "insert into t0 (ts, c1) values(now, 2)");
    execute_simple_sql(taos, sql);

    strcpy(sql, "insert into t0 (ts, c1) values(now, 3)");
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


    char* condBuf = "2 or c1 > 0";
    TAOS_MULTI_BIND params[1];
    params[0].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[0].buffer_length = strlen(condBuf) + 1;
    params[0].buffer = condBuf;
    params[0].length = (int*)&params[0].buffer_length;
    params[0].is_null = NULL;
    params[0].num = 1;

    char *stmt_sql = "select * from super where c1 > ?";
    code = taos_stmt_prepare(stmt, stmt_sql, 0);
    if (code != 0){
        PRINT_ERROR
        printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
        exit(EXIT_FAILURE);
    }
    PRINT_SUCCESS
    printf("Successfully execute taos_stmt_prepare\n");

    taos_stmt_bind_param(stmt, params);
    taos_stmt_add_batch(stmt);

    if (taos_stmt_execute(stmt) != 0) {
        PRINT_ERROR
        printf("failed to execute query statement.\n");
        exit(EXIT_FAILURE);
    }
    PRINT_SUCCESS
    printf("Successfully execute query statement.\n");

    TAOS_RES *result = taos_stmt_use_result(stmt);
    check_result(result, 1);
    

    taos_stmt_close(stmt);
    
    return 0;
}
