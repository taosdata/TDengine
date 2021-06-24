// sample code for TDengine subscribe/consume API
// to compile: gcc -o subscribe subscribe.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>  // include TDengine header file
#include <unistd.h>


void showme();
float calculate_delta_t(size_t size);
int is_lossless_compressed_data(unsigned char* compressedBytes, size_t cmpSize);

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <taos.h>  // TAOS header file

static void queryDB(TAOS *taos, char *command) {
    
  printf("aaa");
  /*
    int i;
  TAOS_RES *pSql = NULL;
  int32_t   code = -1;

  for (i = 0; i < 5; i++) {
    if (NULL != pSql) {
      taos_free_result(pSql);
      pSql = NULL;
    }
    
    pSql = taos_query(taos, command);
    code = taos_errno(pSql);
    if (0 == code) {
      break;
    }
  }

  if (code != 0) {
    fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(pSql));
    taos_free_result(pSql);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }

  taos_free_result(pSql);
   */
}

void Test(TAOS *taos, char *qstr, int i);

int main(int argc, char *argv[]) {
  //char      qstr[1024];
    
 is_lossless_compressed_data(NULL,0);

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }

  TAOS *taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", "null taos"/*taos_errstr(taos)*/);
    exit(1);
  }
  /*
  for (int i = 0; i < 100; i++) {
    Test(taos, qstr, i);
  }
  taos_close(taos);
  taos_cleanup();
   */
}
void Test(TAOS *taos, char *qstr, int index)  {
    
  printf("==================test at %d\n================================", index);
  
  queryDB(taos, "drop database if exists demo");
  queryDB(taos, "create database demo");
  //TAOS_RES *result;
  queryDB(taos, "use demo");

  queryDB(taos, "create table m1 (ts timestamp, ti tinyint, si smallint, i int, bi bigint, f float, d double, b binary(10))");
  printf("success to create table\n");

     /*
  int i = 0;
  for (i = 0; i < 10; ++i) {
    sprintf(qstr, "insert into m1 values (%" PRId64 ", %d, %d, %d, %d, %f, %lf, '%s')", (uint64_t)(1546300800000 + i * 1000), i, i, i, i*10000000, i*1.0, i*2.0, "hello");
    printf("qstr: %s\n", qstr);
    
    // note: how do you wanna do if taos_query returns non-NULL
    // if (taos_query(taos, qstr)) {
    //   printf("insert row: %i, reason:%s\n", i, taos_errstr(taos));
    // }
    TAOS_RES *result1 = taos_query(taos, qstr);
    if (result1 == NULL || taos_errno(result1) != 0) {
      printf("failed to insert row, reason:%s\n", taos_errstr(result1));
      taos_free_result(result1);
      exit(1);
    } else {
      printf("insert row: %i\n", i);
    }
    taos_free_result(result1);
  }
  printf("success to insert rows, total %d rows\n", i);

  // query the records
  sprintf(qstr, "SELECT * FROM m1");
  result = taos_query(taos, qstr);
  if (result == NULL || taos_errno(result) != 0) {
    printf("failed to select, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    exit(1);
  }

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_field_count(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  printf("num_fields = %d\n", num_fields);
  printf("select * from table, result:\n");
  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[1024] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
  printf("====demo end====\n\n");
      */
}

