// to compile: gcc -o taos_connect_with_example taos_connect_with_example.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

int main() {
  OPTIONS opt = {0};
  taos_set_option(&opt, "ip", "127.0.0.1");
  taos_set_option(&opt, "user", "root");
  taos_set_option(&opt, "pass", "taosdata");
  taos_set_option(&opt, "port", "6030");
  taos_set_option(&opt, "charset", "UTF-8");
  taos_set_option(&opt, "timezone", "UTC");
  taos_set_option(&opt, "userIp", "127.0.0.1");
  taos_set_option(&opt, "userApp", "user_app");
  taos_set_option(&opt, "connectorInfo", "connector_info");

  TAOS *taos = taos_connect_with(&opt);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to TDengine server: %s\n", taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }
  fprintf(stdout, "Connected to 127.0.0.1:6030 successfully.\n");

  /* put your code here for read and write */

  taos_close(taos);
  taos_cleanup();
}
