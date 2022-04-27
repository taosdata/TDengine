#include "taoserror.h"
#include "cJSON.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
#include <unistd.h>
#include <inttypes.h>

static void prepare_data(TAOS* taos) {
  TAOS_RES* result;
  result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database test precision 'ms';");
  taos_free_result(result);
  usleep(100000);
  taos_select_db(taos, "test");

  result = taos_query(taos, "create table meters(ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 float, c5 double, c6 bool, c7 binary(10), c8 nchar(10)) tags (t0 int, t1 float, t2 double, t3 bool, t4 binary(10), t5 nchar(10));");
  taos_free_result(result);

  result = taos_query(taos, "create table tb0 using meters tags(0, 0.0, 0.0, true, \"tag0\", \"标签0\");");
  taos_free_result(result);
  result = taos_query(taos, "create table tb1 using meters tags(1, 1.0, 1.0, true, \"tag1\", \"标签1\");");
  taos_free_result(result);
  result = taos_query(taos, "create table tb2 using meters tags(2, 2.0, 2.0, true, \"tag2\", \"标签2\");");
  taos_free_result(result);
  result = taos_query(taos, "create table tb3 using meters tags(3, 3.0, 3.0, true, \"tag3\", \"标签3\");");
  taos_free_result(result);
  result = taos_query(taos, "create table tb4 using meters tags(4, 4.0, 4.0, true, \"tag4\", \"标签4\");");
  taos_free_result(result);
  result = taos_query(taos, "create table tb5 using meters tags(5, 5.0, 5.0, true, \"tag5\", \"标签5\");");
  taos_free_result(result);
  result = taos_query(taos, "create table tb6 using meters tags(6, 6.0, 6.0, true, \"tag6\", \"标签6\");");
  taos_free_result(result);
  result = taos_query(taos, "create table tb7 using meters tags(7, 7.0, 7.0, true, \"tag7\", \"标签7\");");
  taos_free_result(result);
  result = taos_query(taos, "create table tb8 using meters tags(8, 8.0, 8.0, true, \"tag8\", \"标签8\");");
  taos_free_result(result);
  result = taos_query(taos, "create table tb9 using meters tags(9, 9.0, 9.0, true, \"tag9\", \"标签9\");");
  taos_free_result(result);

  result = taos_query(taos,
                      "insert into tb0 values('2020-01-01 00:00:00.000', 11, 11, 11, 11, 11.0, 11.0, false, \"col11\", \"值11\")"
                      " ('2020-01-01 00:01:00.000', 12, 12, 12, 12, 12.0, 12.0, false, \"col12\", \"值12\")"
                      " ('2020-01-01 00:02:00.000', 13, 13, 13, 13, 13.0, 13.0, false, \"col13\", \"值13\")"
                      " tb1 values('2020-01-01 00:00:00.000', 21, 21, 21, 21, 21.0, 21.0, false, \"col21\", \"值21\")"
                      " tb2 values('2020-01-01 00:00:00.000', 31, 31, 31, 31, 31.0, 31.0, false, \"col31\", \"值31\")"
                      " tb3 values('2020-01-01 00:01:02.000', 41, 41, 41, 41, 41.0, 41.0, false, \"col41\", \"值41\")"
                      " tb4 values('2020-01-01 00:01:02.000', 51, 51, 51, 51, 51.0, 51.0, false, \"col51\", \"值51\")"
                      " tb5 values('2020-01-01 00:01:02.000', 61, 61, 61, 61, 61.0, 61.0, false, \"col61\", \"值61\")"
                      " tb6 values('2020-01-01 00:01:02.000', 71, 71, 71, 71, 71.0, 71.0, false, \"col71\", \"值71\")"
                      " tb7 values('2020-01-01 00:01:02.000', 81, 81, 81, 81, 81.0, 81.0, false, \"col81\", \"值81\")"
                      " tb8 values('2020-01-01 00:01:02.000', 91, 91, 91, 91, 91.0, 91.0, false, \"col91\", \"值91\")"
                      " tb9 values('2020-01-01 00:01:02.000', 101, 101, 101, 101, 101.0, 101.0, false, \"col101\", \"值101\")");
  int affected = taos_affected_rows(result);
  if (affected != 12) {
    printf("\033[31m%d rows affected by last insert statement, but it should be 12\033[0m\n", affected);
  }
  taos_free_result(result);
  // super tables subscription
  usleep(1000000);
}

static int print_result(TAOS_RES* res, int32_t rows) {
  TAOS_ROW*   block_ptr = NULL;
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD* fields = taos_fetch_fields(res);

  block_ptr = taos_result_block(res);
  TAOS_ROW col = *block_ptr;
  for (int k = 0; k < rows; k++) {
    char str[256] = {0};
    int32_t len = 0;
    for (int i = 0; i < num_fields; ++i) {
      if (i > 0) {
        str[len++] = ' ';
      }
      switch (fields[i].type) {
        case TSDB_DATA_TYPE_TINYINT:
          len += sprintf(str + len, "%d", *(((int8_t *)col[i]) + k));
          break;

        case TSDB_DATA_TYPE_UTINYINT:
          len += sprintf(str + len, "%u", *(((uint8_t *)col[i]) + k));
          break;

        case TSDB_DATA_TYPE_SMALLINT:
          len += sprintf(str + len, "%d", *(((int16_t *)col[i]) + k));
          break;

        case TSDB_DATA_TYPE_USMALLINT:
          len += sprintf(str + len, "%u", *(((uint16_t *)col[i]) + k));
          break;

        case TSDB_DATA_TYPE_INT:
          len += sprintf(str + len, "%d", *(((int32_t *)col[i]) + k));
          break;

        case TSDB_DATA_TYPE_UINT:
          len += sprintf(str + len, "%u", *(((uint32_t *)col[i]) + k));
          break;

        case TSDB_DATA_TYPE_BIGINT:
          len += sprintf(str + len, "%" PRId64, *(((int64_t *)col[i]) + k));
          break;

        case TSDB_DATA_TYPE_UBIGINT:
          len += sprintf(str + len, "%" PRIu64, *(((uint64_t *)col[i]) + k));
          break;

        case TSDB_DATA_TYPE_FLOAT: {
          len += sprintf(str + len, "%f", *(((float *)col[i]) + k));
        } break;

        case TSDB_DATA_TYPE_DOUBLE: {
          len += sprintf(str + len, "%lf", *(((double *)col[i]) + k));
        } break;

        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR: {
          int32_t charLen = *(int16_t *)col[i];
          int32_t charBytes = (fields[i].type == TSDB_DATA_TYPE_BINARY) ? sizeof(char) : sizeof(wchar_t);
          int32_t offset = k * (sizeof(int16_t) + fields[i].bytes * charBytes);
          memcpy(str + len, (char *)col[i] + sizeof(int16_t) + offset, charLen);
          len += charLen;
        } break;

        case TSDB_DATA_TYPE_TIMESTAMP:
          len += sprintf(str + len, "%" PRId64, *(((int64_t *)col[i]) + k));
          break;

        case TSDB_DATA_TYPE_BOOL:
          len += sprintf(str + len, "%d", *(((int8_t *)col[i]) + k));
        default:
          break;
      }
    }
    puts(str);
  }
}

void fetch_cb(void *param, TAOS_RES* tres, int32_t numOfRows) {
  if (tres == NULL) {
    printf("result not available!\n");
    return;
  }

  if (numOfRows > 0) {
    printf("%d rows async retrieved\n", numOfRows);
    print_result(tres, numOfRows);
    taos_fetch_rows_a(tres, fetch_cb, param);
  } else {
    if (numOfRows < 0) {
      printf("\033[31masync retrieve failed, code: %d\033[0m\n", numOfRows);
    } else {
      printf("async retrieve completed\n");
    }
    taos_free_result(tres);
  }
}

void query_cb(void* param, TAOS_RES* tres, int32_t code) {
  if (code == 0 && tres) {
    taos_fetch_rows_a(tres, fetch_cb, param);
  } else {
    printf("\033[31masync query failed, code: %d\033[0m\n", code);
  }
}

int main(int argc, char *argv[]) {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
    exit(1);
  }

  char* info = taos_get_server_info(taos);
  printf("server info: %s\n", info);
  info = taos_get_client_info(taos);
  printf("client info: %s\n", info);

  printf("************  Prepare data *************\n");
  prepare_data(taos);

  printf("************  Async query *************\n");
  taos_query_a(taos, "select * from meters", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb0", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb1", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb2", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb3", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb4", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb5", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb6", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb7", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb8", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select * from tb9", query_cb, NULL);
  usleep(1000000);

  taos_query_a(taos, "select count(*) from meters", query_cb, NULL);
  usleep(1000000);

  printf("done\n");
  taos_close(taos);
  taos_cleanup();
}
