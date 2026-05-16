// gcc msync.c -o ../../../build/bin/synctest -g -I../../inc -L../../../build/lib -ltsclient -lttaos -ltutil -lpthread 

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
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>

#include "taos.h"
#include "tsclient.h"
#include "tlog.h"
#include "ttimer.h"

void  saveQueryResult(TAOS *con, char *fileName);
void *syncTest(void *param);
 
typedef struct {
  int   points;
  int   index;
  char  db[20];
  char  name[128];
  char  result[128];
  pthread_t thread;
} SInfo;

int main(int argc, char *argv[]) 
{
  int    points = 500;
  int    numOfThreads =1;
  char   prefix[128];
  char   dbname[128] = "demo";
  SInfo *pInfo;
  pthread_attr_t thattr;

  if ( argc == 1 ) {
    printf("usage: %s prefix numOfPoints numOfThreads db cfg\n", argv[0]);
    exit(0);
  }

  if (argc >= 3 ) points = atoi(argv[2]);
  if (argc >= 4 ) numOfThreads = atoi(argv[3]);
  if (argc >= 5 ) strcpy(dbname, argv[4]);
  if (argc >= 6 ) strcpy(configDir, argv[5]);
 
  strcpy(prefix, argv[1]);
  pInfo = (SInfo *)taosMemoryMalloc(sizeof(SInfo)*numOfThreads);

  taos_init();

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  for (int i=0; i<numOfThreads; ++i) {
    pInfo[i].points = points;
    pInfo[i].index = i;
    strcpy(pInfo[i].db, dbname);
    sprintf(pInfo[i].name, "%s%d", prefix, i);
    sprintf(pInfo[i].result, "%s/%s%d", logDir, prefix, i);
    
    pthread_create(&pInfo->thread, &thattr, syncTest, (void *)(pInfo+i));
  }

  pthread_attr_destroy(&thattr);

  printf("%d threads are spawned to insert/select\n", numOfThreads);

/*
  for (int i=0; i<numOfThreads; ++i)
    pthread_join(pInfo->thread, NULL);
*/
  printf("threads exit\n");
  getchar();
  getchar();
  
}

void *syncTest(void *param) 
{
  TAOS   *con;
  SInfo  *pInfo = (SInfo *)param;
  struct  timeval systemTime;
  int64_t st, et, i;
  char    qstr[128];
  char    fileName[128];
  int     points = pInfo->points;
  int     numOfRows;
  TAOS_RES *result;

  taos_init();

  con = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if ( con == NULL) {
    printf("index:%d, failed to connect to DB, reason:%s", pInfo->index, taos_errstr(con));
    exit(1);
  }

  //sprintf(qstr, "create database %s replica 2", pInfo->db);
  sprintf(qstr, "create database %s", pInfo->db);
  taos_query(con, qstr);
  
  sprintf(qstr, "use %s", pInfo->db);
  taos_query(con, qstr);

  sprintf(qstr, "create table %s(ts timestamp, speed bigint, height bigint, pressure double, temp double, rotation bigint, dist double, velocity bigint, acce double, weight double, floor bigint, current double, name int)", pInfo->name);
  taos_query(con, qstr);

  sleep(1);
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  int inserts = 0;
  for (i=0; i<points; ++i) {
    uTrace("index:%d, start to insert row: %lld", pInfo->index, i);
    sprintf(qstr, "insert into %s values (now+%lda, %lld, %lld, %lf, %lf, %lld, %lf, %lld, %lf, %lf, %lld, %lf, %d)", pInfo->name, i, i, i*0.1, i*0.2, i, i*1.0, i, i*0.3, i*3.0, i, i*1.0, i);
    if ( taos_query(con, qstr) ) {
      uError("index:%d, failed to insert row: %lld, reason:%s\n", pInfo->index, i, taos_errstr(con));
    } else {
     int numOfRows = taos_affected_rows(con);
     if ( numOfRows <= 0 ) {
       uError("index:%d, failed to insert %s row: %lld", pInfo->index, pInfo->name, i);
     } else
       inserts += numOfRows;
    }
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("index:%d, %.3f mseconds to insert %d data points\n", pInfo->index, (et-st)/1000.0, inserts);


  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  sprintf(qstr, "SELECT * FROM %s", pInfo->name);

  if (taos_query(con, qstr) ) {
    printf("index:%d, failed to select, reason:%s\n", pInfo->index, taos_errstr(con));
    return NULL;
  }

  result = taos_use_result(con);

  if (result == NULL) {
    printf("failed to get result, reason:%s\n", taos_errstr(con));
    return NULL;
  }

  TAOS_ROW row;
  numOfRows = 0;

  while ((row = taos_fetch_row(result)))
  {
      numOfRows++;
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("index:%d, %.3f mseconds to retrieve %d data points\n", pInfo->index, (et-st)/1000.0, numOfRows);

  taos_free_result(result);

  // save the query result
/*
  sprintf(qstr, "SELECT * FROM %s", pInfo->name);
  if (taos_query(con, qstr) ) {
    printf("failed to select, reason:%s\n", taos_errstr(con));
    return NULL;
  }

  sprintf(fileName, "%s.0", pInfo->result);
  saveQueryResult(con, fileName);
*/
  sprintf(qstr, "SELECT * FROM %s desc", pInfo->name);
  if (taos_query(con, qstr) ) {
    printf("failed to select, reason:%s\n", taos_errstr(con));
    return NULL;
  }

  sprintf(fileName, "%s.1", pInfo->result);
  saveQueryResult(con, fileName);

  return NULL;
}

void saveQueryResult(TAOS *con, char *fileName)
{ 
  TAOS_ROW row;
  TAOS_RES *result;
  int       numOfRows = 0;
  char      temp[256];
  FILE     *fp;

  result = taos_use_result(con);
  if (result == NULL) return;

  fp = fopen (fileName, "w");
  if ( fp == NULL ) {
    printf("failed to open file:%s, reason:%s\n", fileName, strerror(errno));
    return;
  }

  int num_fields = taos_field_count(con);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result)))
  {
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

      numOfRows++;
  }

  printf("%d rows data are saved in file:%s\n", numOfRows, fileName);
  fclose(fp);

}


