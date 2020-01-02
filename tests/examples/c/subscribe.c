// sample code for TDengine subscribe/consume API
// to compile: gcc -o subscribe subscribe.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>  // include TDengine header file


void print_result(TAOS_RES* res) {
  TAOS_ROW    row;
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD* fields = taos_fetch_fields(res);

  while ((row = taos_fetch_row(res))) {
    char temp[256];
    taos_print_row(temp, row, fields, num_fields);
    puts(temp);
  }
}

void subscribe_callback(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
  print_result(res);
}


int main(int argc, char *argv[]) {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";
  int async = 1, restart = 0;
  TAOS_SUB* tsub = NULL;

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "-h=", 3) == 0) {
      host = argv[i] + 3;
      continue;
    }
    if (strncmp(argv[i], "-u=", 3) == 0) {
      user = argv[i] + 3;
      continue;
    }
    if (strncmp(argv[i], "-p=", 3) == 0) {
      passwd = argv[i] + 3;
      continue;
    }
    if (strcmp(argv[i], "-sync") == 0) {
      async = 0;
      continue;
    }
    if (strcmp(argv[i], "-restart") == 0) {
      restart = 1;
      continue;
    }
  }

  // init TAOS
  taos_init();

  TAOS* taos = taos_connect(host, user, passwd, "test", 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  if (async) {
    tsub = taos_subscribe("test", restart, taos, "select * from meters;", subscribe_callback, NULL, 1000);
  } else {
    tsub = taos_subscribe("test", restart, taos, "select * from meters;", NULL, NULL, 0);
  }

  if (tsub == NULL) {
    printf("failed to create subscription.\n");
    exit(0);
  } 

  if (async) {
    getchar();
  } else while(1) {
    TAOS_RES* res = taos_consume(tsub);
    print_result(res);
    getchar();
  }

  taos_unsubscribe(tsub);

  return 0;
}

