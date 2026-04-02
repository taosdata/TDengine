// sample code for TDengine stream computing API
// to compile: gcc -o stream stream.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"  // include TDengine header file

void streamCallBack(void *param, TAOS_RES *res, TAOS_ROW row)
{
  // in this simple demo, it just print out the result
  char temp[128];

  TAOS_FIELD *fields = taos_fetch_fields(res);
  int         numFields = taos_num_fields(res);

  taos_print_row(temp, row, fields, numFields);

  printf("%s\n", temp);
}

int main(int argc, char *argv[]) 
{
  TAOS       *taos;
  TAOS_ROW    row;
  char        dbname[64], table[64];
  char        temp[256], sql[1024] = {0};

  if ( argc == 1 ) {
    printf("usage: %s db-name cfg-path\n", argv[0]);
    exit(0);
  } 

  if ( argc >= 2 ) strcpy(dbname, argv[1]);
  if ( argc >= 3 ) strcpy(configDir, argv[3]);

  // open connection to database
  taos = taos_connect("192.168.0.1", "root", "taosdata", dbname, 0);
  if ( taos == NULL ) {
    printf("failed to connet to db:%s\n", dbname);
    exit(1);
  }

  printf("please input stream SQL:");
  fgets(sql, sizeof(sql), stdin);
  if ( sql[0] == 0 ) exit(1);
  
  // param is set to NULL in this demo, it shall be set to the pointer to app context 
  TAOS_STREAM *pStream = taos_open_stream(taos, sql, streamCallBack, 0, NULL);
  
  printf("presss any key to exit\n");
  getchar();
 
  return 0;
}

