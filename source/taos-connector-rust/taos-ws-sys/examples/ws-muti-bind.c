#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosws.h"

/**
 * @brief execute sql only.
 *
 * @param taos
 * @param sql
 */
void execute_sql(WS_TAOS *taos, const char *sql)
{
    WS_RES *res = ws_query(taos, sql);
    int code = ws_errno(res);
    if (code != 0)
    {
        printf("%s\n", ws_errstr(res));
        ws_free_result(res);
        ws_close(taos);
        exit(EXIT_FAILURE);
    }
    ws_free_result(res);
}

/**
 * @brief check return status and exit program when error occur.
 *
 * @param stmt
 * @param code
 * @param msg
 */
void check_error_code(WS_STMT *stmt, int code, const char *msg)
{
    if (code != 0)
    {
        printf("%s. error: %s\n", msg, ws_stmt_errstr(stmt));
        ws_stmt_close(stmt);
        exit(EXIT_FAILURE);
    }
}

typedef struct
{
    int64_t ts;
    float current;
    int voltage;
    float phase;
} Row;

/**
 * @brief insert data using stmt API
 *
 * @param taos
 */
void insert_data(WS_TAOS *taos)
{
    // init
    WS_STMT *stmt = ws_stmt_init(taos);
    // prepare
    const char *sql = "INSERT INTO ? USING meters TAGS(?, ?) values(?, ?, ?, ?, ?, ?)";
    int code = ws_stmt_prepare(stmt, sql, strlen(sql));
    check_error_code(stmt, code, "failed to execute ws_stmt_prepare");
    // bind table name and tags
    WS_MULTI_BIND tags[2];
    char *location = "California.SanFrancisco";
    int groupId = 2;

    const int32_t location_len = (int32_t)strlen(location);
    const int32_t group_id_len = (int32_t)sizeof(groupId);

    tags[0].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[0].buffer_length = strlen(location);
    tags[0].length = &location_len;
    tags[0].buffer = location;
    tags[0].is_null = NULL;

    tags[1].buffer_type = TSDB_DATA_TYPE_INT;
    tags[1].buffer_length = sizeof(int);
    tags[1].length = &group_id_len;
    tags[1].buffer = &groupId;
    tags[1].is_null = NULL;

    code = ws_stmt_set_tbname_tags(stmt, "d1001", tags, 2);
    check_error_code(stmt, code, "failed to execute ws_stmt_set_tbname_tags");

    // insert two rows with multi binds
    WS_MULTI_BIND params[6];
    // values to bind
    int8_t data_count = 10;
    int64_t ts[] = {
        1648432611249,
        1648432611749,
        1648432612249,
        1648432612749,
        1648432613249,
        1648432614249,
        1648432614749,
        1648432615249,
        1648432615749,
        1648432616249};
    float current[] = {
        10.3,
        12.6,
        11.1,
        13.2,
        10.9,
        9.8,
        11.5,
        13.7,
        12.0,
        10.1};
    int voltage[] = {
        219,
        218,
        220,
        222,
        217,
        221,
        215,
        219,
        220,
        222};
    float phase[] = {
        0.31,
        0.33,
        0.29,
        0.32,
        0.30,
        0.28,
        0.34,
        0.32,
        0.29,
        0.30};

    char loc2[] = {'a', 'b', 0, 'c', 'd', 0, 'e', 'f', 0, 'g', 'h', 0, 'i', 'j', 0, 'a', 'b', 0, 'c', 'd', 0, 'e', 'f', 0, 'g', 'h', 0, 'i', 'j', 0};
    char loc3[1000] = {0};
    int nchar_len = strlen("一二三四五六七八");
    for (int i = 0; i < data_count; i++)
    {
        char *p = loc3 + i * (nchar_len + 10);
        strcpy(p, "一二三四五六七八");
    }

    char is_null[data_count];
    memset(is_null, 0, sizeof(is_null));
    // length array
    int32_t int64Len[data_count];
    for (int i = 0; i < data_count; i++)
    {
        int64Len[i] = sizeof(int64_t);
    }
    int32_t floatLen[data_count];
    for (int i = 0; i < data_count; i++)
    {
        floatLen[i] = sizeof(float);
    }

    int32_t intLen[data_count];
    for (int i = 0; i < data_count; i++)
    {
        int64Len[i] = sizeof(int);
    }

    params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[0].buffer_length = sizeof(int64_t);
    params[0].buffer = ts;
    params[0].length = int64Len;
    params[0].is_null = is_null;
    params[0].num = data_count;

    params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[1].buffer_length = sizeof(float);
    params[1].buffer = current;
    params[1].length = floatLen;
    params[1].is_null = is_null;
    params[1].num = data_count;

    params[2].buffer_type = TSDB_DATA_TYPE_INT;
    params[2].buffer_length = sizeof(int);
    params[2].buffer = voltage;
    params[2].length = intLen;
    params[2].is_null = is_null;
    params[2].num = data_count;

    params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[3].buffer_length = sizeof(float);
    params[3].buffer = phase;
    params[3].length = floatLen;
    params[3].is_null = is_null;
    params[3].num = data_count;

    int32_t varcharLen[data_count];
    for (int i = 0; i < data_count; i++)
    {
        varcharLen[i] = 2;
    }

    params[4].buffer_type = TSDB_DATA_TYPE_VARCHAR;
    params[4].buffer_length = 3;
    params[4].buffer = loc2;
    params[4].length = varcharLen;
    params[4].is_null = is_null;
    params[4].num = data_count;

    int32_t ncharLen[data_count];
    for (int i = 0; i < data_count; i++)
    {
        ncharLen[i] = nchar_len;
    }

    params[5].buffer_type = TSDB_DATA_TYPE_NCHAR;
    params[5].buffer_length = nchar_len + 10;
    params[5].buffer = loc3;
    params[5].length = ncharLen;
    params[5].is_null = is_null;
    params[5].num = data_count;

    code = ws_stmt_bind_param_batch(stmt, params, 6); // bind batch
    check_error_code(stmt, code, "failed to execute taos_stmt_bind_param_batch");
    code = ws_stmt_add_batch(stmt); // add batch
    check_error_code(stmt, code, "failed to execute taos_stmt_add_batch");

    // execute
    int32_t affected_rows = 0;
    code = ws_stmt_execute(stmt, &affected_rows);
    check_error_code(stmt, code, "failed to execute ws_stmt_execute");

    // close
    ws_stmt_close(stmt);

    printf("successfully inserted %d rows\n", affected_rows);
}

int main()
{
    WS_TAOS *taos = ws_connect("ws://localhost:6041");
    if (taos == NULL)
    {
        printf("failed to connect to server\n");
        exit(EXIT_FAILURE);
    }
    execute_sql(taos, "DROP DATABASE IF EXISTS power");
    execute_sql(taos, "CREATE DATABASE power");
    execute_sql(taos, "USE power");
    execute_sql(taos,
                "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, loc2 VARCHAR(20), loc3 NCHAR(50)) TAGS (location BINARY(64), "
                "groupId INT)");
    insert_data(taos);
    ws_close(taos);
}
