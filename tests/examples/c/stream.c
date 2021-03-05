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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <taos.h>  // include TDengine header file

typedef struct {
	char  server_ip[64];
    char  db_name[64];
    char  tbl_name[64];
} param;

int g_thread_exit_flag = 0;
void* insert_rows(void *sarg);

void streamCallBack(void *param, TAOS_RES *res, TAOS_ROW row)
{
  // in this simple demo, it just print out the result
  char temp[128];

  TAOS_FIELD *fields = taos_fetch_fields(res);
  int         numFields = taos_num_fields(res);

  taos_print_row(temp, row, fields, numFields);

  printf("\n%s\n", temp);
}

int main(int argc, char *argv[]) 
{
  TAOS       *taos;
  char        db_name[64];
  char        tbl_name[64];
  char        sql[1024] = { 0 };

  if (argc != 4) {
    printf("usage: %s server-ip dbname tblname\n", argv[0]);
    exit(0);
  } 

  strcpy(db_name, argv[2]);
  strcpy(tbl_name, argv[3]);
  
  // create pthread to insert into row per second for stream calc
  param *t_param = (param *)malloc(sizeof(param));
  if (NULL == t_param)
  {
    printf("failed to malloc\n");
    exit(1);
  }
  memset(t_param, 0, sizeof(param)); 
  strcpy(t_param->server_ip, argv[1]);
  strcpy(t_param->db_name, db_name);
  strcpy(t_param->tbl_name, tbl_name);

  pthread_t pid;
  pthread_create(&pid, NULL, (void * (*)(void *))insert_rows, t_param);

  sleep(3); // waiting for database is created.
  // open connection to database
  taos = taos_connect(argv[1], "root", "taosdata", db_name, 0);
  if (taos == NULL) {
    printf("failed to connet to server:%s\n", argv[1]);
	  free(t_param);
    exit(1);
  }

  // starting stream calc, 
  printf("please input stream SQL:[e.g., select count(*) from tblname interval(5s) sliding(2s);]\n");
  fgets(sql, sizeof(sql), stdin);
  if (sql[0] == 0) {
    printf("input NULL stream SQL, so exit!\n");	
    free(t_param);
    exit(1);
  }

  // param is set to NULL in this demo, it shall be set to the pointer to app context 
  TAOS_STREAM *pStream = taos_open_stream(taos, sql, streamCallBack, 0, NULL, NULL);
  if (NULL == pStream) {
    printf("failed to create stream\n");	
    free(t_param);
    exit(1);
  }
  
  printf("presss any key to exit\n");
  getchar();

  taos_close_stream(pStream);
  
  g_thread_exit_flag = 1;  
  pthread_join(pid, NULL);

  taos_close(taos);
  free(t_param); 

  return 0;
}


void* insert_rows(void *sarg)
{
  TAOS  *taos;
  char    command[1024] = { 0 };
  param  *winfo = (param * )sarg;

  if (NULL == winfo){
  	printf("para is null!\n");
    exit(1);
  }

  taos = taos_connect(winfo->server_ip, "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connet to server:%s\n", winfo->server_ip);
    exit(1);
  }
  
  // drop database
  sprintf(command, "drop database %s;", winfo->db_name);
  if (taos_query(taos, command) != 0) {
    printf("failed to drop database, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  // create database
  sprintf(command, "create database %s;", winfo->db_name);
  if (taos_query(taos, command) != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  // use database
  sprintf(command, "use %s;", winfo->db_name);
  if (taos_query(taos, command) != 0) {
    printf("failed to use database, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  // create table
  sprintf(command, "create table %s (ts timestamp, speed int);", winfo->tbl_name);
  if (taos_query(taos, command) != 0) {
    printf("failed to create table, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  // insert data
  int64_t begin = (int64_t)time(NULL);
  int index = 0;
  while (1) {
    if (g_thread_exit_flag) break;
	
    index++;
    sprintf(command, "insert into %s values (%ld, %d)", winfo->tbl_name, (begin + index) * 1000, index);
    if (taos_query(taos, command)) {
      printf("failed to insert row [%s], reason:%s\n", command, taos_errstr(taos));
    }
    sleep(1);
  }  

  taos_close(taos);
  return 0;
}

