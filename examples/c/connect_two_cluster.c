#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include "taos.h"
int numOfThreads = 1;

void* connectClusterAndDeal(void *arg) {
  int port = *(int *)arg;
  const char *host = "127.0.0.1";  
  const char *user = "root";  
  const char *passwd = "taosdata"; 
  TAOS* taos1 = taos_connect(host, user, passwd, "", port); 
  TAOS* taos2 = taos_connect(host, user, passwd, "", port + 1000); 
  if (NULL == taos1 || NULL == taos2) {
    printf("connect to (%d/%d) failed \n", port, port + 1000); 
    return NULL;
  } 
  TAOS_RES *result = NULL;
  result = taos_query(taos1, "drop database if exists db"); 
  if (0 != taos_errno(result)) {
    printf("failed %s\n", taos_errstr(result));
  }
  taos_free_result(result);

  taos_query(taos2, "drop database if exists db"); 
  if (0 != taos_errno(result)) {
    printf("failed %s\n", taos_errstr(result));
  }
  
  taos_free_result(result);
  // ========= build database
  {
    result = taos_query(taos1, "create database db");
    if (0 != taos_errno(result)) {
      printf("failed %s\n", taos_errstr(result));
    }
    
    taos_free_result(result);
  }
  {
    result = taos_query(taos2, "create database db");
    if (0 != taos_errno(result)) {
      printf("failed %s\n", taos_errstr(result));
    }
    taos_free_result(result);
  }
  
  //======== create table 
  {
    result = taos_query(taos1, "create stable db.stest (ts timestamp, port int) tags(tport int)");
    if (0 != taos_errno(result)) {
      printf("failed %s\n", taos_errstr(result));
    }
    taos_free_result(result);
  }
  {
    result = taos_query(taos2, "create stable db.stest (ts timestamp, port int) tags(tport int)");
    if (0 != taos_errno(result)) {
      printf("failed %s\n", taos_errstr(result));
    }
    taos_free_result(result);

  }
  //======== create table 
  {
    result = taos_query(taos1, "use db");
    if (0 != taos_errno(result)) {
      printf("failed %s\n", taos_errstr(result));
    }
    taos_free_result(result);
  }
  {
    result = taos_query(taos2, "use db");
    if (0 != taos_errno(result)) {
      printf("failed %s\n", taos_errstr(result));
    }
    taos_free_result(result);

  }
  {
    char buf[1024] = {0};
    sprintf(buf, "insert into db.t1 using stest tags(%d) values(now, %d)", port, port);
    for (int i = 0; i < 100000; i++) {
      //printf("error here\t");
      result = taos_query(taos1, buf);
    if (0 != taos_errno(result)) {
      printf("failed %s\n", taos_errstr(result));
    }
      taos_free_result(result);
      //sleep(1); 
    }
  }

  {
    char buf[1024] = {0};
    sprintf(buf, "insert into db.t1 using stest tags(%d) values(now, %d)", port + 1000, port + 1000);
    for (int i = 0; i < 100000; i++) {
      result = taos_query(taos2, buf);
      if (0 != taos_errno(result)) {
        printf("failed %s\n", taos_errstr(result));
      }
      taos_free_result(result);
      //sleep(1); 
    }
  }
  // query result
  {
    result = taos_query(taos1, "select * from stest");
    if (result == NULL || taos_errno(result) != 0) {
      printf("query failed %s\n", taos_errstr(result));
      taos_free_result(result);
    }
    TAOS_ROW row;
    int rows = 0;
    int num_fields = taos_field_count(result);
    TAOS_FIELD *fields = taos_fetch_fields(result);
    while ((row = taos_fetch_row(result))) {
      char temp[1024] = {0};
      rows++;
      taos_print_row(temp, row, fields , num_fields);
      printf("%s\n", temp);
    }
    taos_free_result(result);
  } 

  // query result
  {
    result = taos_query(taos2, "select * from stest");
    if (result == NULL || taos_errno(result) != 0) {
      printf("query failed %s\n", taos_errstr(result));
      taos_free_result(result);
    }
    TAOS_ROW row;
    int rows = 0;
    int num_fields = taos_field_count(result);
    TAOS_FIELD *fields = taos_fetch_fields(result);
    while ((row = taos_fetch_row(result))) {
      char temp[1024] = {0};
      rows++;
      taos_print_row(temp, row, fields , num_fields);
      printf("%s\n", temp);
    }
    taos_free_result(result);
  } 
  taos_close(taos1); 
  taos_close(taos2); 
  return NULL;
}
int main(int argc, char* argv[]) {
  pthread_t *pthreads = malloc(sizeof(pthread_t) * numOfThreads);   
  
  int *port = malloc(sizeof(int) * numOfThreads);
  port[0] = 6030;
  for (int i = 0; i < numOfThreads; i++) {
    pthread_create(&pthreads[i], NULL, connectClusterAndDeal, (void *)&port[i]);
  }
  for (int i = 0; i < numOfThreads; i++) {
    pthread_join(pthreads[i], NULL);
  }
  free(port);
}
