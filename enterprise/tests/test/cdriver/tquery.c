// gcc query.c -o ../../../build/bin/query -g -I../../inc -L../../../build/lib -ltsclient -lttaos -ltutil -lpthread 

/*******************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies. 
 *  No part of this file may be reproduced, stored, transmitted, 
 *  disclosed or used in any form or by any means other than as 
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/time.h>
#include <string.h>
#include <assert.h>

#include "taos.h"
#include "tsclient.h"
#include "tutil.h"

int main(int argc, char *argv[]) {
  TAOS *con;
  struct timeval systemTime;
  int64_t st, et;
  char qstr[128], db[128] = "test";
  char table[20] = "tm0";
  char fn[128];
  int numOfRows;
  TAOS_RES *result;
  FILE *fp;

  if (argc == 1) {
    printf("usage: %s db table cfg file\n", argv[0]);
    exit(0);
  }

  if (argc >= 2) strcpy(db, argv[1]);
  if (argc >= 3) strcpy(table, argv[2]);
  if (argc >= 4) strcpy(configDir, argv[3]);
  if (argc >= 5) strcpy(fn, argv[4]);

  con = taos_connect(NULL, tsDefaultUser, tsDefaultPass, NULL, 0);
  if (con == NULL) {
    printf("failed to connect, reason:%s\n", taos_errstr(con));
    exit(1);
  }

  sprintf(qstr, "use %s", db);
  taos_query(con, qstr);

  sprintf(qstr, "select * from %s order by ts asc", table);
  if ( taos_query(con, qstr) ) {
    printf("failed to select, reason:%s\n", taos_errstr(con));
    exit(1);
  }

  result = taos_use_result(con);

  TAOS_ROW row;
  int num_fields = taos_field_count(con);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char temp[256];

  fp = fopen(fn, "w+");
  if ( fp == NULL ) {
    printf("failed to open file:%s\n", fn);
    exit(1);
  }

  while ((row = taos_fetch_row(result)))
  {
     taos_print_row(temp, row, fields, num_fields);
     fprintf (fp, "%s\n", temp);
  }

  fclose(fp);
}


