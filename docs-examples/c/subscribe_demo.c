// A simple demo for asynchronous subscription.
// compile with:
// gcc -o subscribe_demo subscribe_demo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

int nTotalRows;

/**
 * @brief callback function of subscription.
 *
 * @param tsub
 * @param res
 * @param param. the additional parameter passed to  taos_subscribe
 * @param code. error code
 */
void subscribe_callback(TAOS_SUB* tsub, TAOS_RES* res, void* param, int code) {
  if (code != 0) {
    printf("error: %d\n", code);
    exit(EXIT_FAILURE);
  }

  TAOS_ROW    row = NULL;
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD* fields = taos_fetch_fields(res);
  int         nRows = 0;

  while ((row = taos_fetch_row(res))) {
    char buf[4096] = {0};
    taos_print_row(buf, row, fields, num_fields);
    puts(buf);
    nRows++;
  }

  nTotalRows += nRows;
  printf("%d rows consumed.\n", nRows);
}

int main() {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 6030);
  if (taos == NULL) {
    printf("failed to connect to server\n");
    exit(EXIT_FAILURE);
  }

  int         restart = 1;  // if the topic already exists, where to subscribe from the begin.
  const char* topic = "topic-meter-current-bg-10";
  const char* sql = "select * from power.meters where current > 10";
  void*       param = NULL;     // additional parameter.
  int         interval = 2000;  // consumption interval in microseconds.
  TAOS_SUB*   tsub = taos_subscribe(taos, restart, topic, sql, subscribe_callback, NULL, interval);

  // wait for insert from others process. you can open TDengine CLI to insert some records for test.
  
  getchar();  // press Enter to stop

  printf("total rows consumed: %d\n", nTotalRows);
  int keep = 0;  // whether to keep subscribe process
  taos_unsubscribe(tsub, keep);

  taos_close(taos);
  taos_cleanup();
}
