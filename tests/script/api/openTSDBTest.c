#include "taoserror.h"
#include "cJSON.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
#include <unistd.h>

void verify_telnet_insert(TAOS* taos) {
  TAOS_RES *result;

  result = taos_query(taos, "drop database if exists db;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database db precision 'ms';");
  taos_free_result(result);
  usleep(100000);

  (void)taos_select_db(taos, "db");
  int32_t code = 0;

  /* metric */
  char* lines0[] = {
      "stb0_0 1626006833639 4i8 host=\"host0\" interface=\"eth0\"",
      "stb0_1 1626006833639 4i8 host=\"host0\" interface=\"eth0\"",
      "stb0_2 1626006833639 4i8 host=\"host0\" interface=\"eth0\"",
  };
  result = taos_schemaless_insert(taos, lines0, 3, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines0 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  /* timestamp */
  char* lines1[] = {
      "stb1 1626006833641 1i8 host=\"host0\"",
      "stb1 1626006832 2i8 host=\"host0\"",
      "stb1 0 3i8 host=\"host0\"",
  };
  result = taos_schemaless_insert(taos, lines1, 3, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines1 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  /* metric value */
  //tinyint
  char* lines2_0[] = {
      "stb2_0 1626006833651 -127i8 host=\"host0\"",
      "stb2_0 1626006833652 127i8 host=\"host0\""
  };
  result = taos_schemaless_insert(taos, lines2_0, 2, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines2_0 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  //smallint
  char* lines2_1[] = {
      "stb2_1 1626006833651 -32767i16 host=\"host0\"",
      "stb2_1 1626006833652 32767i16 host=\"host0\""
  };
  result = taos_schemaless_insert(taos, lines2_1, 2, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines2_1 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  //int
  char* lines2_2[] = {
      "stb2_2 1626006833651 -2147483647i32 host=\"host0\"",
      "stb2_2 1626006833652 2147483647i32 host=\"host0\""
  };
  result = taos_schemaless_insert(taos, lines2_2, 2, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines2_2 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  //bigint
  char* lines2_3[] = {
      "stb2_3 1626006833651 -9223372036854775807i64 host=\"host0\"",
      "stb2_3 1626006833652 9223372036854775807i64 host=\"host0\""
  };
  result = taos_schemaless_insert(taos, lines2_3, 2, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines2_3 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  //float
  char* lines2_4[] = {
      "stb2_4 1626006833610 3f32 host=\"host0\"",
      "stb2_4 1626006833620 -3f32 host=\"host0\"",
      "stb2_4 1626006833630 3.4f32 host=\"host0\"",
      "stb2_4 1626006833640 -3.4f32 host=\"host0\"",
      "stb2_4 1626006833650 3.4E10f32 host=\"host0\"",
      "stb2_4 1626006833660 -3.4e10f32 host=\"host0\"",
      "stb2_4 1626006833670 3.4E+2f32 host=\"host0\"",
      "stb2_4 1626006833680 -3.4e-2f32 host=\"host0\"",
      "stb2_4 1626006833700 3.4E38f32 host=\"host0\"",
      "stb2_4 1626006833710 -3.4E38f32 host=\"host0\""
  };
  result = taos_schemaless_insert(taos, lines2_4, 10, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines2_4 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  //double
  char* lines2_5[] = {
      "stb2_5 1626006833610 3f64 host=\"host0\"",
      "stb2_5 1626006833620 -3f64 host=\"host0\"",
      "stb2_5 1626006833630 3.4f64 host=\"host0\"",
      "stb2_5 1626006833640 -3.4f64 host=\"host0\"",
      "stb2_5 1626006833650 3.4E10f64 host=\"host0\"",
      "stb2_5 1626006833660 -3.4e10f64 host=\"host0\"",
      "stb2_5 1626006833670 3.4E+2f64 host=\"host0\"",
      "stb2_5 1626006833680 -3.4e-2f64 host=\"host0\"",
      "stb2_5 1626006833690 1.7E308f64 host=\"host0\"",
      "stb2_5 1626006833700 -1.7E308f64 host=\"host0\"",
      "stb2_5 1626006833710 3.15 host=\"host0\""
  };
  result = taos_schemaless_insert(taos, lines2_5, 11, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines2_5 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  //bool
  char* lines2_6[] = {
      "stb2_6 1626006833610 t host=\"host0\"",
      "stb2_6 1626006833620 T host=\"host0\"",
      "stb2_6 1626006833630 true host=\"host0\"",
      "stb2_6 1626006833640 True host=\"host0\"",
      "stb2_6 1626006833650 TRUE host=\"host0\"",
      "stb2_6 1626006833660 f host=\"host0\"",
      "stb2_6 1626006833670 F host=\"host0\"",
      "stb2_6 1626006833680 false host=\"host0\"",
      "stb2_6 1626006833690 False host=\"host0\"",
      "stb2_6 1626006833700 FALSE host=\"host0\""
  };
  result = taos_schemaless_insert(taos, lines2_6, 10, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines2_6 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  //binary
  char* lines2_7[] = {
      "stb2_7 1626006833610 \"binary_val.!@#$%^&*\" host=\"host0\"",
      "stb2_7 1626006833620 \"binary_val.:;,./?|+-=\" host=\"host0\"",
      "stb2_7 1626006833630 \"binary_val.()[]{}<>\" host=\"host0\""
  };
  result = taos_schemaless_insert(taos, lines2_7, 3, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines2_7 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  //nchar
  char* lines2_8[] = {
      "stb2_8 1626006833610 L\"nchar_val数值一\" host=\"host0\"",
      "stb2_8 1626006833620 L\"nchar_val数值二\" host=\"host0\""
  };
  result = taos_schemaless_insert(taos, lines2_8, 2, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines2_8 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  /* tags */
  //tag value types
  char* lines3_0[] = {
      "stb3_0 1626006833610 1 t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=3.4E38f32 t6=1.7E308f64 t7=true t8=\"binary_val_1\" t9=L\"标签值1\"",
      "stb3_0 1626006833610 2 t1=-127i8 t2=-32767i16 t3=-2147483647i32 t4=-9223372036854775807i64 t5=-3.4E38f32 t6=-1.7E308f64 t7=false t8=\"binary_val_2\" t9=L\"标签值2\""
  };
  result = taos_schemaless_insert(taos, lines3_0, 2, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines3_0 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  //tag ID as child table name
  char* lines3_1[] = {
      "stb3_1 1626006833610 1 id=child_table1 host=host1",
      "stb3_1 1626006833610 2 host=host2 iD=child_table2",
      "stb3_1 1626006833610 3 ID=child_table3 host=host3"
  };
  result = taos_schemaless_insert(taos, lines3_1, 3, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("lines3_1 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  return;
}

void verify_json_insert(TAOS* taos) {
  TAOS_RES *result;

  result = taos_query(taos, "drop database if exists db;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database db precision 'ms';");
  taos_free_result(result);
  usleep(100000);

  (void)taos_select_db(taos, "db");
  int32_t code = 0;

  char *message[] = {
  "{                                      \
      \"metric\":\"cpu_load_0\",          \
      \"timestamp\": 1626006833610,       \
      \"value\": 55.5,                    \
      \"tags\":                           \
          {                               \
              \"host\": \"ubuntu\",       \
              \"interface1\": \"eth0\",   \
              \"Id\": \"tb0\"             \
          }                               \
  }"};

  result = taos_schemaless_insert(taos, message, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload_0 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  char *message1[] = {
  "[                                       \
    {                                      \
       \"metric\":\"cpu_load_1\",          \
       \"timestamp\": 1626006833610,       \
       \"value\": 55.5,                    \
       \"tags\":                           \
           {                               \
               \"host\": \"ubuntu\",       \
               \"interface\": \"eth1\",    \
               \"Id\": \"tb1\"             \
           }                               \
    },                                     \
    {                                      \
       \"metric\":\"cpu_load_2\",          \
       \"timestamp\": 1626006833610,       \
       \"value\": 55.5,                    \
       \"tags\":                           \
           {                               \
               \"host\": \"ubuntu\",       \
               \"interface\": \"eth2\",    \
               \"Id\": \"tb2\"             \
           }                               \
    }                                      \
   ]"};

  result = taos_schemaless_insert(taos, message1, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload_1 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);

  char *message2[] = {
  "[                                       \
    {                                      \
       \"metric\":\"cpu_load_3\",          \
       \"timestamp\":                      \
           {                               \
             \"value\": 1626006833610,     \
             \"type\": \"ms\"              \
           },                              \
       \"value\":                          \
           {                               \
             \"value\": 55,                \
             \"type\": \"int\"             \
           },                              \
       \"tags\":                           \
           {                               \
               \"host\":                   \
                  {                        \
                    \"value\": \"ubuntu\", \
                    \"type\": \"binary\"   \
                  },                       \
               \"interface\":              \
                  {                        \
                    \"value\": \"eth3\",   \
                    \"type\": \"nchar\"    \
                  },                       \
               \"ID\": \"tb3\",            \
               \"port\":                   \
                  {                        \
                    \"value\": 4040,       \
                    \"type\": \"int\"      \
                  }                        \
           }                               \
    },                                     \
    {                                      \
       \"metric\":\"cpu_load_4\",          \
       \"timestamp\": 1626006833610,       \
       \"value\": 66.6,                    \
       \"tags\":                           \
           {                               \
               \"host\": \"ubuntu\",       \
               \"interface\": \"eth4\",    \
               \"Id\": \"tb4\"             \
           }                               \
    }                                      \
   ]"};
  result = taos_schemaless_insert(taos, message2, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload_2 code: %d, %s.\n", code, tstrerror(code));
  }
  taos_free_result(result);


  cJSON *payload, *tags;
  char *payload_str[1];

  /* Default format */
  //number
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_0");
  cJSON_AddNumberToObject(payload, "timestamp", 1626006833610);
  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload0_0 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //true
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_1");
  cJSON_AddNumberToObject(payload, "timestamp", 1626006833610);
  cJSON_AddTrueToObject(payload, "value");
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload0_1 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //false
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_2");
  cJSON_AddNumberToObject(payload, "timestamp", 1626006833610);
  cJSON_AddFalseToObject(payload, "value");
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload0_2 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //string
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_3");
  cJSON_AddNumberToObject(payload, "timestamp", 1626006833610);
  cJSON_AddStringToObject(payload, "value", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload0_3 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //timestamp 0 -> current time
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_4");
  cJSON_AddNumberToObject(payload, "timestamp", 0);
  cJSON_AddNumberToObject(payload, "value", 123);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload0_4 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  /* Nested format */
  //timestamp
  cJSON *timestamp;
  //seconds
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb1_0");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload1_0 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //milleseconds
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb1_1");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833610);
  cJSON_AddStringToObject(timestamp, "type", "ms");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload1_1 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //microseconds
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb1_2");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833610123);
  cJSON_AddStringToObject(timestamp, "type", "us");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload1_2 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //now
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb1_4");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 0);
  cJSON_AddStringToObject(timestamp, "type", "ns");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload1_4 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //metric value
  cJSON *metric_val;
  //bool
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_0");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddTrueToObject(metric_val, "value");
  cJSON_AddStringToObject(metric_val, "type", "bool");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload2_0 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //tinyint
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_1");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 127);
  cJSON_AddStringToObject(metric_val, "type", "tinyint");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload2_1 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //smallint
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_2");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 32767);
  cJSON_AddStringToObject(metric_val, "type", "smallint");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload2_2 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //int
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_3");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 2147483647);
  cJSON_AddStringToObject(metric_val, "type", "int");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload2_3 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //bigint
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_4");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", (double)9223372036854775807);
  cJSON_AddStringToObject(metric_val, "type", "bigint");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload2_4 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //float
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_5");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 11.12345);
  cJSON_AddStringToObject(metric_val, "type", "float");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload2_5 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //double
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_6");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 22.123456789);
  cJSON_AddStringToObject(metric_val, "type", "double");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload2_6 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //binary
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_7");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddStringToObject(metric_val, "value", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddStringToObject(metric_val, "type", "binary");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload2_7 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //nchar
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_8");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddStringToObject(metric_val, "value", "你好");
  cJSON_AddStringToObject(metric_val, "type", "nchar");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload2_8 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);

  //tag value
  cJSON *tag;

  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb3_0");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddStringToObject(metric_val, "value", "hello");
  cJSON_AddStringToObject(metric_val, "type", "nchar");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();

  tag = cJSON_CreateObject();
  cJSON_AddTrueToObject(tag, "value");
  cJSON_AddStringToObject(tag, "type", "bool");
  cJSON_AddItemToObject(tags, "t1", tag);

  tag = cJSON_CreateObject();
  cJSON_AddFalseToObject(tag, "value");
  cJSON_AddStringToObject(tag, "type", "bool");
  cJSON_AddItemToObject(tags, "t2", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 127);
  cJSON_AddStringToObject(tag, "type", "tinyint");
  cJSON_AddItemToObject(tags, "t3", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 32767);
  cJSON_AddStringToObject(tag, "type", "smallint");
  cJSON_AddItemToObject(tags, "t4", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 2147483647);
  cJSON_AddStringToObject(tag, "type", "int");
  cJSON_AddItemToObject(tags, "t5", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", (double)9223372036854775807);
  cJSON_AddStringToObject(tag, "type", "bigint");
  cJSON_AddItemToObject(tags, "t6", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 11.12345);
  cJSON_AddStringToObject(tag, "type", "float");
  cJSON_AddItemToObject(tags, "t7", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 22.1234567890);
  cJSON_AddStringToObject(tag, "type", "double");
  cJSON_AddItemToObject(tags, "t8", tag);

  tag = cJSON_CreateObject();
  cJSON_AddStringToObject(tag, "value", "binary_val");
  cJSON_AddStringToObject(tag, "type", "binary");
  cJSON_AddItemToObject(tags, "t9", tag);

  tag = cJSON_CreateObject();
  cJSON_AddStringToObject(tag, "value", "你好");
  cJSON_AddStringToObject(tag, "type", "nchar");
  cJSON_AddItemToObject(tags, "t10", tag);

  cJSON_AddItemToObject(payload, "tags", tags);

  *payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  result = taos_schemaless_insert(taos, payload_str, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  code = taos_errno(result);
  if (code) {
    printf("payload3_0 code: %d, %s.\n", code, tstrerror(code));
  }
  free(*payload_str);
  cJSON_Delete(payload);
  taos_free_result(result);
}

int main(int argc, char *argv[]) {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
    exit(1);
  }

  char* info = taos_get_server_info(taos);
  printf("server info: %s\n", info);
  info = taos_get_client_info(taos);
  printf("client info: %s\n", info);

  printf("************  verify telnet-insert  *************\n");
  verify_telnet_insert(taos);

  printf("************  verify json-insert  *************\n");
  verify_json_insert(taos);

  printf("done\n");
  taos_close(taos);
  taos_cleanup();
}
