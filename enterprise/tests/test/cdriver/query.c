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

void taos_error(TAOS *con)
{
  fprintf(stderr, "TSDB error: %s\n", taos_errstr(con));
}

void query(TAOS* con, char* q) {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  int64_t st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  if (taos_query(con, q) != 0 ) {
    taos_error(con);
    exit(1);
  }

  TAOS_RES* result = taos_use_result(con);

  if (result == NULL)
    taos_error(con);

  TAOS_ROW row;

  int32_t numOfRows = 0;
  int num_fields = taos_field_count(con);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char   temp[256] = {0};

  while ((row = taos_fetch_row(result)))
  {
      for(int i = 0; i < num_fields; i++)
      {
        switch( fields[i].type ) {
          case TSDB_DATA_TYPE_TINYINT:
            sprintf(temp + strlen(temp), "%d ", *((char *)row[i]));
                break;
          case TSDB_DATA_TYPE_SMALLINT:
            sprintf(temp + strlen(temp), "%d ", *((short *)row[i]));
                break;
          case TSDB_DATA_TYPE_INT:
            sprintf(temp + strlen(temp), "%d ", *((int *)row[i]));
                break;
          case TSDB_DATA_TYPE_BIGINT:
            sprintf(temp + strlen(temp), "%lld ", *((int64_t *)row[i]));
                break;
          case TSDB_DATA_TYPE_FLOAT:
            sprintf(temp + strlen(temp), "%f ", *((float *)row[i]));
                break;
          case TSDB_DATA_TYPE_DOUBLE:
            sprintf(temp + strlen(temp), "%lf ", *((double *)row[i]));
                break;
          case TSDB_DATA_TYPE_BINARY:
            sprintf(temp + strlen(temp), "%s ", (char *) row[i]);
                break;
          case TSDB_DATA_TYPE_TIMESTAMP:
            sprintf(temp + strlen(temp), "%lld ", *((time_t *)row[i]));
                break;
          default:
            break;
        }
    }
    fprintf(stdout, "%s\n", temp);
    memset(temp, 0, sizeof(temp)/sizeof(temp[0]));
    numOfRows++;
  }


  gettimeofday(&systemTime, NULL);
  int64_t et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("%.3f mseconds to retrieve %d data points\n", (et-st)/1000.0, numOfRows);
//  assert(numOfRows == 200);
  taos_free_result(result);
}

int main(int argc, char *argv[]) {
  TAOS *con;
  struct timeval systemTime;
  int64_t st, et;
  char qstr[128], db[128] = "test";
  char table[20] = "tm0";
  int order = 0;
  int save = 1;
  int numOfRows;
  TAOS_RES *result;
  FILE *fp;

  if (argc == 1) {
    printf("usage: %s db cfg\n", argv[0]);
    exit(0);
  }

  //t1 db1 0 1 . / cfg
  if (argc >= 2) strcpy(db, argv[1]);
//  if (argc >= 3) strcpy(db, argv[2]);
//  if (argc >= 4) order = atoi(argv[3]);
//  if (argc >= 5) save = atoi(argv[4]);
  if (argc >= 3) strcpy(configDir, argv[2]);

  con = taos_connect(tsMasterIp, tsDefaultUser, "1", NULL, 0);
  if (con == NULL) {
    taos_error(con);
    exit(1);
  }

  sprintf(qstr, "create database %s", db);
  taos_query(con, qstr);

  sprintf(qstr, "use %s", db);
  taos_query(con, qstr);

//  taos_query(con, "drop table tt");

//  taos_query(con, "drop table testmetric");
//  taos_query(con, "create table testmetric(ts timestamp, k int) tags(a int)");
//  taos_query(con, "create table tt using testmetric tags(1)");
//  taos_query(con, "insert into tt values(now, 1)");

  for(int32_t i =0; i<100; ++i) {
    printf("%d-----------------------------------------------------\n\n\n", i);

    //error test case: @@@@@@!!!!
    query(con, "select count(*), avg(k), sum(y), count(y), avg(y) from m1 group by a");
    query(con, "select count(*), first(k), last(k), min(f),max(f),sum(y),avg(y) from m1 group by c");
  }
  taos_close(con);
}


