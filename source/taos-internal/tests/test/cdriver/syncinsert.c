// gcc syncinsert.c -o ../../../build/bin/syncinsert -g -I../../inc -L../../../build/lib -ltsclient -lttaos -ltutil -lpthread 

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
#include <sys/time.h>
#include <string.h>

#include "taos.h"
#include "tsclient.h"

void taos_error(TAOS *con)
{
  fprintf(stderr, "TSDB error: %s\n", taos_errstr(con));
  taos_close(con);
}

int main(int argc, char *argv[]) 
{
  TAOS   *con;
  struct  timeval systemTime;
  int64_t st, et, i;
  char    qstr[128];
  int     points = 500;
  char    table[20] = "m2";
  //if ( argc == 1 ) {
  //  printf("usage: %s meterId numOfPoints cfg\n", argv[0]);
  //  exit(0);
  //}

  if (argc >= 2) strcpy(table, argv[1]);
  if (argc >= 3) points = atoi(argv[2]);
  if (argc >= 4) strcpy(configDir, argv[3]);

  taos_init();
  con = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if ( con == NULL) {
    taos_error(con);
    exit(1);
  }

  taos_query(con, "create database demodb");
  taos_query(con, "USE demodb");

  sprintf(qstr, "create table %s(ts timestamp, speed bigint)", table);
  taos_query(con, qstr);

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  for (i=0; i<points; ++i) {
    sprintf(qstr, "insert into %s values (now+%lda, %lld)", table, i, i);
    if ( taos_query(con, qstr) >= 128) {
      taos_error(con);
      exit(1);
    }
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("%.3f mseconds to insert %lld data points\n", (et-st)/1000.0, i);

  taos_close(con);

  return getchar();
}


