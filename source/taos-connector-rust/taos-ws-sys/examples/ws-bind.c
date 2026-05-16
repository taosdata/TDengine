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
    const char *sql = "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)";
    int code = ws_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
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

    // insert two rows
    Row rows[2] = {
        {1648432611249, 10.3, 219, 0.31},
        {1648432611749, 12.6, 218, 0.33},
    };

    const int32_t ts_len = (int32_t)sizeof(int64_t);
    const int32_t current_len = (int32_t)sizeof(float);
    const int32_t voltage_len = (int32_t)sizeof(int);
    const int32_t phase_len = (int32_t)sizeof(float);

    WS_MULTI_BIND values[4];
    values[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    values[0].buffer_length = sizeof(int64_t);
    values[0].length = &ts_len;
    values[0].is_null = NULL;
    values[0].num = 1;

    values[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
    values[1].buffer_length = sizeof(float);
    values[1].length = &current_len;
    values[1].is_null = NULL;
    values[1].num = 1;

    values[2].buffer_type = TSDB_DATA_TYPE_INT;
    values[2].buffer_length = sizeof(int);
    values[2].length = &voltage_len;
    values[2].is_null = NULL;
    values[2].num = 1;

    values[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    values[3].buffer_length = sizeof(float);
    values[3].length = &phase_len;
    values[3].is_null = NULL;
    values[3].num = 1;

    for (int i = 0; i < 2; ++i)
    {
        values[0].buffer = &rows[i].ts;
        values[1].buffer = &rows[i].current;
        values[2].buffer = &rows[i].voltage;
        values[3].buffer = &rows[i].phase;

        code = ws_stmt_bind_param_batch(stmt, &values[0], 4); // bind param
        check_error_code(stmt, code, "failed to execute ws_stmt_bind_param_batch");
        code = ws_stmt_add_batch(stmt); // add batch
        check_error_code(stmt, code, "failed to execute ws_stmt_add_batch");
    }
    // execute
    int32_t affected_rows = 0;
    code = ws_stmt_execute(stmt, &affected_rows);
    check_error_code(stmt, code, "failed to execute ws_stmt_execute");

    printf("successfully inserted %d rows\n", affected_rows);
    // close
    ws_stmt_close(stmt);
}

int main()
{
    ws_enable_log("debug");
    WS_TAOS *taos = ws_connect("ws://localhost:6041");
    if (taos == NULL)
    {
        printf("failed to connect to server\n");
        exit(EXIT_FAILURE);
    }
    execute_sql(taos, "DROP DATABASE IF EXISTS power");
    execute_sql(taos, "CREATE DATABASE power");
    execute_sql(taos, "USE power");
    execute_sql(taos, "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
    insert_data(taos);
    ws_close(taos);
}
