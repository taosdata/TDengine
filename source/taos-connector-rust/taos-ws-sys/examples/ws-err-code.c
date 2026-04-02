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

    int errno = 0;
    ws_enable_log("debug");
    WS_TAOS *taos = ws_connect(dsn);

    WS_RES *res1 = ws_query(taos, "select * from db_not_exsits.tb1");
    errno = ws_errno(res1);
    assert(errno != 0);
    ws_free_result(res1);

    ws_close(taos);

    WS_TAOS *taos2 = ws_connect(dsn);

    WS_RES *res2 = ws_query(taos2, "select to_iso8601(0) as ts");
    assert(res2 != NULL);
    errno = ws_errno(res2);
    assert(errno == 0);

    ws_free_result(res2);

    ws_close(taos2);
}
