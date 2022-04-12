// gcc mquery.c -o ../../../build/bin/mquery -g -I../../inc -L../../../build/lib -ltaos -lpthread 

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
#include <pthread.h>
#include <errno.h>
#include <unistd.h>

#include "taos.h"
#include "tsclient.h"

typedef struct {
  int   index;
  int   order;
  int   save;
  char  db[20];
  char  name[128];
  pthread_t thread;
} SInfo;

void taos_error(TAOS *con)
{
  fprintf(stderr, "TSDB error: %s\n", taos_errstr(con));
}

void *queryTest(void *param) 
{
  TAOS   *con;
  SInfo  *pInfo = (SInfo *)param;
  struct  timeval systemTime;
  int64_t st, et;
  char    qstr[128];
  int     numOfRows;
  
  TAOS_RES *result;
  FILE    *fp;

  taos_init();
  con = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, pInfo->db, 0);
  if ( con == NULL) {
    taos_error(con);
    exit(1);
  }

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  if ( pInfo->order == 0 ) 
    sprintf(qstr, "SELECT * FROM %s", pInfo->name);
  else 
    sprintf(qstr, "SELECT * FROM %s desc", pInfo->name);

  if (taos_query(con, qstr) != 0 ) {
    taos_error(con);
    exit(1);
  }

  result = taos_use_result(con);

  if (result == NULL)
    taos_error(con);

  TAOS_ROW row;

  numOfRows = 0;
  int num_fields = taos_field_count(con);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char   temp[256], fn[256];

  sprintf(fn, "%s/%s.%d", logDir, pInfo->name, pInfo->order);
  fp = fopen(fn, "w+"); 
  if ( fp == NULL ) {
    printf("failed to open file:%s", fn);
    exit(1);
  }

  while ((row = taos_fetch_row(result)))
  {
    if ( pInfo->save ) {
      temp[0] = 0;

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
            sprintf(temp + strlen(temp), "%lld ", *((int64_t *)row[i]));
            break;
          default:
            break;
        }

      }

      fprintf(fp, "%s\n", temp);
    }

    numOfRows++;
  }


  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("%.3f mseconds to retrieve %d data points\n", (et-st)/1000.0, numOfRows);

  if ( pInfo->save ) 
    printf("%d rows data are saved into file:%s\n", numOfRows, fn);

  fclose(fp);

  taos_free_result(result);

  taos_close(con);

  return 0;

}

int main(int argc, char *argv[]) 
{
  char    prefix[128];
  char    dbname[128]="demo";
  SInfo  *pInfo;
  int     order = 0;
  int     save = 1;
  int     numOfThreads;
  pthread_attr_t thattr;
  
  if ( argc == 1 ) {
    printf("usage: %s prefix numOfThreads db order cfg save\n", argv[0]);
    exit(0);
  }

  if (argc >=3 ) numOfThreads = atoi(argv[2]);
  if (argc >=4 ) strcpy(dbname, argv[3]);
  if (argc >=5 ) order = atoi(argv[4]);
  if (argc >=6 ) strcpy(configDir, argv[5]);
  if (argc >=7 ) save = atoi(argv[6]);

  strcpy(prefix, argv[1]);
  pInfo = (SInfo *)taosMemoryMalloc(sizeof(SInfo)*numOfThreads);

  taos_init();

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  for (int i=0; i<numOfThreads; ++i) {
    pInfo[i].index = i;
    pInfo[i].order = order;
    pInfo[i].save = save;
    strcpy(pInfo[i].db, dbname);
    sprintf(pInfo[i].name, "%s%d", prefix, i);

    pthread_create(&pInfo->thread, &thattr, queryTest, (void *)(pInfo+i));
  }

  pthread_attr_destroy(&thattr);

  printf("%d threads are spawned to query\n", numOfThreads);

  for (int i=0; i<numOfThreads; ++i) 
    pthread_join(pInfo->thread, NULL);

  printf("threads exit\n");
  getchar();

  return 0;

}


