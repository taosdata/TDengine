// compile with
// gcc connect_example.c -o connect_example -I /usr/local/taos/include -L /usr/local/taos/driver -ltaos
#include <stdio.h>
#include "taos.h"
#include "taoserror.h"

int main() {
  // if don't want to connect to a default db, set it to NULL.
  const char *db = "test";
  TAOS       *taos = taos_connect("localhost", "root", "taosdata", db, 6030);
  printf("Connected\n");
  char *message[] = {
"[                                       \
  {                                      \
     \"metric\":\"cpu_load_1\",          \
     \"timestamp\": 1626006833,    \
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
     \"timestamp\": 1626006833,    \
     \"value\": 55.5,                    \
     \"tags\":                           \
         {                               \
             \"host\": \"ubuntu\",       \
             \"interface\": \"eth2\",    \
             \"Id\": \"tb2\"             \
         }                               \
  }                                      \
 ]",
 "[                                       \
  {                                      \
     \"metric\":\"cpu_load_1\",          \
     \"timestamp\": 1626006834,    \
     \"value\": 56.5,                    \
     \"tags\":                           \
         {                               \
             \"host\": \"ubuntu\",       \
             \"interface\": \"eth1\",    \
             \"Id\": \"tb1\"             \
         }                               \
  },                                     \
  {                                      \
     \"metric\":\"cpu_load_2\",          \
     \"timestamp\": 1626006834,    \
     \"value\": 56.5,                    \
     \"tags\":                           \
         {                               \
             \"host\": \"ubuntu\",       \
             \"interface\": \"eth2\",    \
             \"Id\": \"tb2\"             \
         }                               \
  }                                      \
 ]"
 };
void*  code = taos_schemaless_insert(taos, message, 1, 3, NULL);
if (code) {
  printf("payload_1 code: %d, %s.\n", code, tstrerror(code));
}
  taos_close(taos);
}
