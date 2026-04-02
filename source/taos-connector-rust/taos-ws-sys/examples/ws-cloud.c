#include "taosws.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

int main()
{
    ws_enable_log("debug");

    char *url = getenv("TDENGINE_CLOUD_URL");
    assert(url != NULL);

    char *token = getenv("TDENGINE_CLOUD_TOKEN");
    assert(token != NULL);

    char dsn[1024];
    snprintf(dsn, sizeof(dsn), "%s/rust_test?token=%s", url, token);

    WS_TAOS *taos = ws_connect(dsn);
    const char *version = ws_get_server_info(taos);
    printf("Server version: %s\n", version);

    ws_close(taos);
}
