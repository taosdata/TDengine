// sample code for TDengine subscribe/consume API
// to compile: gcc -o subscribe subscribe.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>  // include TDengine header file

int main(int argc, char *argv[]) 
{
  TAOS_SUB   *tsub;
  TAOS_ROW    row;
  char        dbname[64], table[64];
  char        temp[256];

  if ( argc == 1 ) {
    printf("usage: %s db-name table-name cfg-path\n", argv[0]);
    exit(0);
  } 

  if ( argc >= 2 ) strcpy(dbname, argv[1]);
  if ( argc >= 3 ) strcpy(table, argv[2]);
  if ( argc >= 4 ) strcpy(configDir, argv[3]);

  tsub = taos_subscribe("192.168.0.1", "root", "taosdata", dbname, table, 0, 1000);
  if ( tsub == NULL ) {
    printf("failed to connet to db:%s\n", dbname);
    exit(1);
  }

  TAOS_FIELD *fields = taos_fetch_subfields(tsub);
  int fcount = taos_subfields_count(tsub);

  printf("start to retrieve data\n");
  while ( 1 ) {
    row = taos_consume(tsub);
    if ( row == NULL ) break;

    taos_print_row(temp, row, fields, fcount);
    printf("%s\n", temp);
  }

  taos_unsubscribe(tsub);

  return 0;
}

