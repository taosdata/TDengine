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
  char   dbname[128] = "demodb";
  SInfo *pInfo;
  pthread_attr_t thattr;
  char   table[20] = "m4";

  if ( argc == 1 ) {
    printf("usage: %s prefix numOfPoints numOfThreads db cfg\n", argv[0]);
    exit(0);
  }

  //test 1000 1 testdb . / cfg
  if (argc >= 2) strcpy(table, argv[1]);
  if (argc >= 3) points = atoi(argv[2]);
  if (argc >= 4) numOfThreads = atoi(argv[3]);
  if (argc >= 5) strcpy(dbname, argv[4]);
  if (argc >= 6) strcpy(configDir, argv[5]);
 
  strcpy(prefix, table);
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
  return getchar();
}

void *syncTest(void *param) 
{
  TAOS   *con;
  SInfo  *pInfo = (SInfo *)param;
  struct  timeval systemTime;
  long    st, et, i;
  char    qstr[1024];
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

#ifdef LINUX
  sprintf(qstr, "create database %s replica 2", pInfo->db);
#else
  sprintf(qstr, "create database %s replica 1", pInfo->db);
#endif
//  sprintf(qstr, "create database %s", pInfo->db);
  taos_query(con, qstr);
  
  sprintf(qstr, "use %s", pInfo->db);
  taos_query(con, qstr);

  sprintf(qstr, "create table %s(ts timestamp, tag binary(8), lat int, lon int, fu binary(24), tfu binary(48), tfuu binary(96), tfuuu binary(192) )", pInfo->name);
  taos_query(con, qstr);

#ifdef LINUX
  sleep(3);
#else
  Sleep(1);
#endif
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  int inserts = 0;
  for (i=0; i<points; i=i+4) {
    uTrace("index:%d, start to insert row: %ld", pInfo->index, i);
    sprintf(qstr, "insert into %s values (now+%lda, abc, %ld, 3, abcd, efghi, xyz, xyz123456abc) values (now+%lda, abc, %ld, 3, abcd, efghi, xyz, xyz123456abcd) values (now+%lda, abc, %ld, 3, abcd, efghi, xyz, xyz123456abc) values (now+%lda, abc, %ld, 3, abcd, efghi, xyz, xyz123456abc)", pInfo->name, i, i, i+1, i+1, i+2, i+2, i+3, i+3);
    if ( taos_query(con, qstr) ) {
      uError("index:%d, failed to insert row: %ld, reason:%s\n", pInfo->index, i, taos_errstr(con));
    } else {
     int numOfRows = taos_affected_rows(con);
     if ( numOfRows <= 0 ) {
       uError("index:%d, failed to insert %s row: %ld", pInfo->index, pInfo->name, i);
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

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  sprintf(qstr, "SELECT * FROM %s desc", pInfo->name);

  if (taos_query(con, qstr) ) {
    printf("index:%d, failed to select, reason:%s\n", pInfo->index, taos_errstr(con));
    return NULL;
  }

  result = taos_use_result(con);

  if (result == NULL) {
    printf("failed to get result, reason:%s\n", taos_errstr(con));
    return NULL;
  }

  numOfRows = 0;

  while ((row = taos_fetch_row(result)))
  {
      numOfRows++;
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("desc index:%d, %.3f mseconds to retrieve %d data points\n", pInfo->index, (et-st)/1000.0, numOfRows);

  taos_free_result(result);

  return NULL;
}

void saveQueryResult(TAOS *con, char *fileName)
{ 
  TAOS_ROW row;
  TAOS_RES *result;
  int       numOfRows = 0;
  char      temp[1024];
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
/*
      for(int i = 0; i < num_fields; i++) 
      { 
        int len = strlen(temp);
        if ( len > 512 )
         printf("len:%d is too long\n", len);
 
        switch( fields[i].type ) {
          case TSDB_DATA_TYPE_TINYINT:
            sprintf(temp + len, "%d ", *((char *)row[i])); 
            break;
          case TSDB_DATA_TYPE_SMALLINT:
            sprintf(temp + len, "%d ", *((short *)row[i]));
            break;
          case TSDB_DATA_TYPE_INT:
            sprintf(temp + len, "%d ", *((int *)row[i]));
            break;
          case TSDB_DATA_TYPE_BIGINT:
            sprintf(temp + len, "%ld ", *((long *)row[i]));
            break;
          case TSDB_DATA_TYPE_FLOAT:
            sprintf(temp + len, "%f ", *((float *)row[i]));
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            sprintf(temp + len, "%lf ", *((double *)row[i]));
            break;
          case TSDB_DATA_TYPE_BINARY:
            sprintf(temp + len, "%s ", (char *) row[i]);
            break;
          case TSDB_DATA_TYPE_TIMESTAMP:
            sprintf(temp + len, "%lld ", *((time_t *)row[i]));
            break;
          default:
            break;
        }

      } 

      fprintf(fp, "%s\n", temp);
*/
      numOfRows++;
  }

  printf("%d rows data are saved in file:%s\n", numOfRows, fileName);
  fclose(fp);

}


