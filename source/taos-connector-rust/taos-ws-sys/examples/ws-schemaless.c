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
        dsn = "ws://localhost:6041/";
    }
    ws_enable_log("trace");
    WS_TAOS *taos = ws_connect(dsn);
    assert(taos != NULL);

    ws_query(taos, "use schemaless_test");
    char *pdata = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639";
    int total_rows = 0;
    WS_RES *rs = ws_schemaless_insert_raw_ttl_with_reqid(
        taos,
        pdata,
        strlen(pdata),
        &total_rows,
        1,
        4,
        0,
        12345);

    printf("%p\n", rs);
    int code = ws_errno(rs);
    printf("%x\n", code);

    if (code != 0)
    {
        const char *errstr = ws_errstr(rs);
        printf("------------------------%ld------1\n", errstr);
        dprintf(2, "Error [%6x]: %s", code, errstr);
        return code;
    }
    printf("------------------------------2\n");
    assert(rs != NULL);
    ws_free_result(rs);

    pdata = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=220i32,phase=0.31f64 1626006833640";
    rs = ws_schemaless_insert_raw(
        taos,
        pdata,
        strlen(pdata),
        &total_rows,
        1,
        4);
    code = ws_errno(rs);
    if (code != 0)
    {
        const char *errstr = ws_errstr(taos);
        dprintf(2, "Error [%6x]: %s", code, errstr);
        return code;
    }

    ws_free_result(rs);
}
