/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
    printf("usage: %s server-ip db-name table-name \n", argv[0]);
    exit(0);
  } 

  if ( argc >= 2 ) strcpy(dbname, argv[2]);
  if ( argc >= 3 ) strcpy(table, argv[3]);

  tsub = taos_subscribe(argv[1], "root", "taosdata", dbname, table, 0, 1000);
  if ( tsub == NULL ) {
    printf("failed to connet to db:%s\n", dbname);
    exit(1);
  }

  TAOS_FIELD *fields = taos_fetch_subfields(tsub);
  int fcount = taos_subfields_count(tsub);

  printf("start to retrieve data\n");
  printf("please use other taos client, insert rows into %s.%s\n", dbname, table);
  while ( 1 ) {
    row = taos_consume(tsub);
    if ( row == NULL ) break;

    taos_print_row(temp, row, fields, fcount);
    printf("%s\n", temp);
  }

  taos_unsubscribe(tsub);

  return 0;
}

