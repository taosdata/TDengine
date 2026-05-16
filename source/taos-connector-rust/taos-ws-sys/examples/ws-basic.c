#include "taosws.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

int main()
{
    char *dsn = getenv("TAOS_DSN");
    if (dsn == NULL)
    {
        dsn = "ws://localhost:6041";
    }
    ws_enable_log("debug");
    WS_TAOS *taos = ws_connect(dsn);

    const char *version = ws_get_server_info(taos);
    dprintf(2, "Server version: %s\n", version);

    WS_RES *res1 = ws_query(taos, "drop database basic");
    ws_free_result(res1);
    WS_RES *res2 = ws_query(taos, "create database basic");
    ws_free_result(res2);
    WS_RES *res3 = ws_query(taos, "create table basic.t (ts timestamp, v int, name varchar(20), mark nchar(20))");
    int64_t timing = ws_take_timing(res3);
    dprintf(2, "Query timing: %lldns\n", timing);
    int code = ws_errno(res3);
    if (code != 0)
    {
        const char *errstr = ws_errstr(taos);
        dprintf(2, "Error [%6x]: %s", code, errstr);
        ws_free_result(res3);
        ws_close(taos);
        return 0;
    }

    int precision = ws_result_precision(res3);
    printf("precision: %d\n", precision);
    int cols = ws_field_count(res3);
    printf("cols: %d\n", cols);
    const struct WS_FIELD *fields = ws_fetch_fields(res3);
    for (int col = 0; col < cols; col++)
    {
        const struct WS_FIELD *field = &fields[col];
        dprintf(2, "column %d: name: %s, length: %d, type: %d\n", col, field->name,
                field->bytes, field->type);
    }

    for (int col = 0; col < cols; col++)
    {
        if (col == 0)
        {
            printf("%s", fields[col].name);
        }
        else
        {
            printf(",%s", fields[col].name);
        }
    }
    printf("\n");

    WS_STMT *stmt = ws_stmt_init(taos);
    const char *sql = "insert into basic.t (ts, v, name, mark) values (?, ?, ?, ?)";
    int r = ws_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
    printf("prepare: %d\n", r);
    if (r)
        return -1;

    int insert = 0;
    r = ws_stmt_is_insert(stmt, &insert);
    printf("is insert res: %d\n", r);
    printf("is insert count: %d\n", insert);
    if (r)
        return -1;
    assert(insert == 1);

    int64_t ts = 1648432611249L;
    const int32_t ts_len = (int32_t)sizeof(ts);
    const char ts_is_null = (char)0;

    int v = 100;
    const int32_t v_len = (int32_t)sizeof(v);
    const char v_is_null = (char)0;

    const char *name = "涛思";
    const int32_t name_len = (int32_t)strlen(name);
    const char name_is_null = (char)0;

    const char *mark = "数据";
    const int32_t mark_len = (int32_t)strlen(mark);
    const char mark_is_null = (char)0;

    WS_MULTI_BIND mbs[4] = {0};
    mbs[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    mbs[0].buffer = &ts;
    mbs[0].buffer_length = (uintptr_t)sizeof(ts);
    mbs[0].length = &ts_len;
    mbs[0].is_null = &ts_is_null;
    mbs[0].num = 1;

    mbs[1].buffer_type = TSDB_DATA_TYPE_INT;
    mbs[1].buffer = &v;
    mbs[1].buffer_length = (uintptr_t)sizeof(v);
    mbs[1].length = &v_len;
    mbs[1].is_null = &v_is_null;
    mbs[1].num = 1;

    mbs[2].buffer_type = TSDB_DATA_TYPE_VARCHAR;
    mbs[2].buffer = name;
    mbs[2].buffer_length = (uintptr_t)strlen(name);
    mbs[2].length = &name_len;
    mbs[2].is_null = &name_is_null;
    mbs[2].num = 1;

    mbs[3].buffer_type = TSDB_DATA_TYPE_NCHAR;
    mbs[3].buffer = (void *)mark;
    mbs[3].buffer_length = (uintptr_t)mark_len;
    mbs[3].length = (int32_t *)&mark_len;
    mbs[3].is_null = (char *)&mark_is_null;
    mbs[3].num = 1;

    r = ws_stmt_bind_param_batch(stmt, &mbs[0], sizeof(mbs) / sizeof(mbs[0]));
    printf("bind param batch res: %d\n", r);
    if (r)
        return -1;

    r = ws_stmt_add_batch(stmt);
    if (r)
        return -1;

    int32_t affected_rows = 0;
    r = ws_stmt_execute(stmt, &affected_rows);
    if (r)
        return -1;
    assert(affected_rows == 1);
}
