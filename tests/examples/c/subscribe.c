// sample code for TDengine subscribe/consume API
// to compile: gcc -o subscribe subscribe.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>  // include TDengine header file

int main(int argc, char *argv[]) {
  // init TAOS
  taos_init();

  TAOS* taos = taos_connect(argv[1], "root", "taosdata", "test", 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  TAOS_SUB* tsub = taos_subscribe(taos, "select * from meters;", NULL, NULL, 0);
  if ( tsub == NULL ) {
    printf("failed to create subscription.\n");
    exit(0);
  } 

  for( int i = 0; i < 3; i++ ) {
    TAOS_RES* res = taos_consume(tsub);
    TAOS_ROW    row;
    int         rows = 0;
    int         num_fields = taos_subfields_count(tsub);
    TAOS_FIELD *fields = taos_fetch_fields(res);
    char        temp[256];

    // fetch the records row by row
    while ((row = taos_fetch_row(res))) {
      rows++;
      taos_print_row(temp, row, fields, num_fields);
      printf("%s\n", temp);
    }

    printf("\n");
  }

  taos_unsubscribe(tsub);

  return 0;
}

