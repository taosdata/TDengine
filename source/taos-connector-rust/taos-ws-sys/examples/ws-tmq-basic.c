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

    ws_tmq_conf_t *conf = ws_tmq_conf_new();
    assert(conf != NULL);

    enum ws_tmq_conf_res_t r1 = ws_tmq_conf_set(conf, "group.id", "abcd");
    assert(r1 == WS_TMQ_CONF_OK);

    r1 = ws_tmq_conf_set(conf, "client.id", "abc");
    assert(r1 == WS_TMQ_CONF_OK);

    r1 = ws_tmq_conf_set(conf, "auto.offset.reset", "earliest");
    assert(r1 == WS_TMQ_CONF_OK);

    ws_tmq_list_t *list = ws_tmq_list_new();
    int res_code = ws_tmq_list_append(list, "topic_ws_map");
    assert(res_code == 0);

    char err_str[200] = {0};
    ws_tmq_t *consumer = ws_tmq_consumer_new(
        conf,
        "taos://localhost:6041",
        err_str,
        200);
    assert(consumer != NULL);

    res_code = ws_tmq_subscribe(consumer, list);
    assert(res_code == 0);

    int row_number = 0;
    for (int i = 0; i < 10; i++)
    {
        WS_RES *res = ws_tmq_consumer_poll(consumer, 100);
        if (res == NULL)
        {
            continue;
        }

        char buf[1024];
        int32_t rows = 0;

        const char *topicName = ws_tmq_get_topic_name(res);
        const char *dbName = ws_tmq_get_db_name(res);
        int32_t vgroupId = ws_tmq_get_vgroup_id(res);

        printf("topic: %s\n", topicName);
        printf("db: %s\n", dbName);
        printf("vgroup id: %d\n", vgroupId);

        int cols = ws_field_count(res);
        const struct WS_FIELD *fields = ws_fetch_fields(res);
        uint8_t ty;
        uint32_t len;
        char tmp[4096];

        int precision = ws_result_precision(res);

        while (1)
        {
            WS_ROW row_data = ws_fetch_row(res);
            if (row_data == NULL)
            {
                break;
            }

            row_number++;
            ws_print_row(tmp, 4096, row_data, fields, cols);
            printf("%s\n", tmp);
        }
    }

    printf("row_number == %d\n", row_number);
    ws_tmq_conf_destroy(conf);

    printf("ok\n");
}
