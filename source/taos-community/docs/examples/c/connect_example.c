// compile with
// gcc connect_example.c -o connect_example -ltaos
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
#include <unistd.h>  // for sleep()
int main() {
  int32_t     code = 0;
  const char *host = "localhost";
  const char *user = "root";
  const char *passwd = "taosdata";
  const char *db = NULL;      // if don't want to connect to a default db, set it to NULL or ""
  uint16_t    port = 6030;    // 0 means use the default port
  TAOS       *taos = taos_connect(host, user, passwd, db, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
            taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }

  code = taos_options_connection(taos, TSDB_OPTION_CONNECTION_USER_IP, "192.168.1.1");
  if (code != 0) {
    fprintf(stderr, "Failed to set user ip, ErrCode: 0x%x, ErrMessage: %s.\n", taos_errno(taos), taos_errstr(taos));
    taos_close(taos);
    taos_cleanup();
    return -1;
  }

  sleep(5);
  {
    TAOS_RES *res = taos_query(taos, "show connections");
    if (taos == NULL) {
      fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
              taos_errstr(NULL));
      taos_cleanup();
      return -1;
    }

    taos_free_result(res);
  }

  fprintf(stdout, "Connected to %s:%hu successfully.\n", host, port);
  
  /* put your code here for read and write */

  // close & clean
  taos_close(taos);
  taos_cleanup();
}
