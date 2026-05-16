#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "mysql.h"
#include "perftestcommon.h"

static const char *   host = "192.168.92.1";
static const char *   user = "root";
static const char *   password = "root";
static const uint32_t port = 3306;

typedef struct {
  int     threadid;
  char *  tname;
  void *  mysql;
  int32_t numOfRows;
} MultiThreadInsert;

MYSQL connect_to_db(char *db_name) {
  MYSQL mysql;
  mysql_init(&mysql);

  if (!mysql_real_connect(&mysql, host, user, password, db_name, port, NULL, 0)) {
    perror("failed to connecto db\n");
    exit(-1);
  }

  // disable auto commit
  mysql_autocommit(&mysql, 0);
  return mysql;
}

void check_env(MYSQL *mysql, char *table_name) {
  const char *DROP_PERF_TABLE = "drop table if exists perf";

  char create_table_sql[512] = {0};

  const char *CREATE_PERF_TABLE = "create table if not exists %s(ts int, lat float, lon float)";

  sprintf(create_table_sql, CREATE_PERF_TABLE, table_name);
  int32_t ret = mysql_query(mysql, DROP_PERF_TABLE);

  ret = mysql_query(mysql, create_table_sql);
  if (ret != 0) {
    perror(mysql_error(mysql));  //"create table failed!\n");
  }
}

void close_conn(MYSQL *mysql) {
  if (mysql_commit(mysql)) {
    fprintf(stderr, " failed while commit\n");
    fprintf(stderr, " %s\n", mysql_error(mysql));
    exit(0);
  }

  mysql_close(mysql);
}

/**
 * synchronized insert record into db in one-by-one means
 * @param conn
 * @param tname
 * @param entries
 * @param sample_interval : 0=no sampling
 */
void *simple_insert(void *param) {
  MultiThreadInsert *mti = (MultiThreadInsert *)param;

  char qstr[256] = {0};
  double st = get_ts_in_ms();

  // total sampling count during execution
  // prepare sampling record array
  for (int32_t i = 0; i < mti->numOfRows; ++i) {
    sprintf(qstr, "insert into %s (ts, lat, lon) values('%d', %f, %f)", mti->tname, i, i * 1.1, i * 1.2);
    if (mysql_query(mti->mysql, qstr)) {
      const char *errmsg = mysql_error(mti->mysql);
      fprintf(stderr, "insert error: %s\n", errmsg);
    }
  }

  //  record_sampling_end(sample_recs[sampling_cnt - 1], entries->cur_len % sample_interval, rec_size);

  double ed = get_ts_in_ms();
  printf("total consumed time is: %.4f sec.", ((double)(ed - st)) / 1000);

  //  char        szBuffer[64] = {0};
  //  const char *pFormat = "%Y-%m-%d_%H_%M_%S.txt";

  //  time_t     t = time(NULL);
  //  struct tm *local = localtime(&t);
  //  strftime(szBuffer, 64, pFormat, local);

  //  char full_path[512] = {0};
  //  sprintf(full_path, "/home/lisa/Documents/mysql_simple_%s", szBuffer);

  //  dump_sampling_record_to_file(sample_recs, sampling_cnt, full_path);
  return NULL;
}

/**
 * insert into db
 */
void dynamic_bind_insert_operation(MYSQL *mysql, char *table_name, entry_list *entries, int32_t sample_interval,
                                   int32_t rec_size) {
  MYSQL_BIND bind[5] = {0};

  my_ulonglong affected_rows;

  my_bool is_null = 0;

  MYSQL_STMT *stmt = mysql_stmt_init(mysql);
  if (!stmt) {
    fprintf(stderr, " mysql_stmt_init(), out of memory\n");
    exit(0);
  }

  char  insert_stmt_sql[512] = {0};
  char *INSERT_SAMPLE = "INSERT INTO %s (ts, tag, lat, lon, direction) VALUES(?,?,?,?,?)";
  sprintf(insert_stmt_sql, INSERT_SAMPLE, table_name);

  if (mysql_stmt_prepare(stmt, insert_stmt_sql, strlen(insert_stmt_sql))) {
    fprintf(stderr, " mysql_stmt_prepare(), INSERT failed\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
  fprintf(stdout, " prepare, INSERT successful\n");

  /* Get the parameter count from the statement */
  int32_t param_count = mysql_stmt_param_count(stmt);
  fprintf(stdout, " total parameters in INSERT: %d\n", param_count);

  time_t     t = time(NULL);
  struct tm *tx = localtime(&t);
  MYSQL_TIME ts;

  ts.day = tx->tm_mday;
  ts.year = tx->tm_year + 1900;
  ts.month = tx->tm_mon + 1;
  ts.hour = tx->tm_hour;
  ts.minute = tx->tm_min;
  ts.second = tx->tm_sec;

  char tag[15] = {0};
  strcpy(tag, "B12345");
  unsigned long tag_length = strlen(tag);

  float   lat = 90.0;
  float   lon = 12.0;
  int16_t dir = 1;

  bind[0].buffer_type = MYSQL_TYPE_DATE;
  bind[0].buffer = (char *)&ts;
  bind[0].is_null = 0;
  bind[0].length = 0;

  /* STRING PARAM */
  bind[1].buffer_type = MYSQL_TYPE_STRING;
  bind[1].buffer = tag;
  bind[1].buffer_length = sizeof(tag) / sizeof(tag[0]);
  bind[1].is_null = 0;
  bind[1].length = &tag_length;

  /* FLOAT PARAM */
  bind[2].buffer_type = MYSQL_TYPE_FLOAT;
  bind[2].buffer = (char *)&lat;
  bind[2].is_null = &is_null;
  bind[2].length = 0;

  /* FLOAT PARAM */
  bind[3].buffer_type = MYSQL_TYPE_FLOAT;
  bind[3].buffer = (char *)&lon;
  bind[3].is_null = &is_null;
  bind[3].length = 0;

  /* SMALLINT PARAM */
  bind[4].buffer_type = MYSQL_TYPE_SHORT;
  bind[4].buffer = (char *)&dir;
  bind[4].is_null = &is_null;
  bind[4].length = 0;

  /* Bind the buffers */
  if (mysql_stmt_bind_param(stmt, bind)) {
    fprintf(stderr, " mysql_stmt_bind_param() failed\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

  //  char qstr[256] = {0};

  double st = get_ts_in_ms();

  entry *el = entries->data;

  // total sampling count during execution
  int32_t total_sample_rec_cnt = (entries->cur_len + sample_interval - 1) / sample_interval;
  if (total_sample_rec_cnt > MAX_SAMPLING_CNT) {
    total_sample_rec_cnt = MAX_SAMPLING_CNT;
    sample_interval = entries->cur_len / MAX_SAMPLING_CNT;
  }

  // prepare sampling record array
  sampling_ele **sample_recs = (sampling_ele **)taosMemoryMalloc(sizeof(void *) * total_sample_rec_cnt);
  int32_t        sampling_cnt = 0;

  sampling_ele *r1 = record_sample_start();
  sample_recs[sampling_cnt++] = r1;

  for (int32_t i = 0; i < entries->cur_len; ++i) {
    strcpy(tag, el->tag);
    lat = el->lat;
    lon = el->lon;
    dir = el->direction;
    if (mysql_stmt_execute(stmt)) {
      fprintf(stderr, " mysql_stmt_execute(), 1 failed\n");
      fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
      continue;
    }

    el = el->next;

    if (i % sample_interval == 0 && i != 0) {
      record_sampling_end(sample_recs[sampling_cnt - 1], sample_interval, rec_size);
      // start a new sampling
      sample_recs[sampling_cnt++] = record_sample_start();
    }
  }

  record_sampling_end(sample_recs[sampling_cnt - 1], entries->cur_len % sample_interval, rec_size);

  double ed = get_ts_in_ms();
  printf("total consumed time is: %.4f sec.", ((double)(ed - st)) / 1000);

  char        szBuffer[64] = {0};
  const char *pFormat = "%Y-%m-%d_%H_%M_%S.txt";

  time_t     _t = time(NULL);
  struct tm *local = localtime(&_t);
  strftime(szBuffer, 64, pFormat, local);

  char full_path[512] = {0};
  sprintf(full_path, "/home/lisa/Documents/mysql_bulk_%s", szBuffer);

  dump_sampling_record_to_file(sample_recs, sampling_cnt, full_path);

  if (mysql_stmt_execute(stmt)) {
    fprintf(stderr, " mysql_stmt_execute(), 1 failed\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

  /* Get the number of affected rows */
  affected_rows = mysql_stmt_affected_rows(stmt);
  fprintf(stdout, " total affected rows(insert 1): %lu\n", (unsigned long)affected_rows);

  if (affected_rows != 1) /* validate affected rows */
  {
    fprintf(stderr, " invalid affected rows by MySQL\n");
    exit(0);
  }

  /* Close the statement */
  if (mysql_stmt_close(stmt)) {
    fprintf(stderr, " failed while closing the statement\n");
    fprintf(stderr, " %s\n", mysql_error(mysql));
    exit(0);
  }

  if (mysql_commit(mysql)) {
    fprintf(stderr, " failed while commit\n");
    fprintf(stderr, " %s\n", mysql_error(mysql));
    exit(0);
  }

  mysql_close(mysql);
}

int multiThreadInsert(int32_t numOfThreads, void *mysql) {
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  pthread_t *threadId = taosMemoryMalloc(sizeof(pthread_t) * numOfThreads);

  MultiThreadInsert *params = calloc(1, sizeof(MultiThreadInsert) * numOfThreads);

  for (int i = 0; i < numOfThreads; ++i) {
    params[i].threadid = i;
    params[i].mysql = mysql;
    params[i].tname = strdup("testt");
    params[i].numOfRows = 10000;

    pthread_create(&threadId[i], NULL, simple_insert, &params[i]);
  }

  for (int32_t i = 0; i < numOfThreads; ++i) {
    pthread_join(threadId[i], NULL);
  }

  pthread_attr_destroy(&thattr);
  return 0;
}

/**
 * execution arguments:
 * [operation] = syn, [start to sample delay] = 10sec.,
 * [sample interval] = 25sec., [sample duration] = 5sec.
 *
 * @param argc
 * @param argv
 */
int main(int argc, char **argv) {
  char *table_name = "testt";
  char *db_name = "perf_test";

  MYSQL mysql = connect_to_db(db_name);
  check_env(&mysql, table_name);

  //    entry_list *entries = load_all_data_into_mem(data_path);
  //    check_ts_inc(entries);

  multiThreadInsert(3, &mysql);
  mysql_commit(&mysql);

  //    dynamic_bind_insert_operation(&mysql, table_name, entries, 10000, rec_size(entries->data));
  //      multi_simple_insert(&mysql, table_name, entries, 10000, rec_size(entries->data),
  //                          30240);

  //    dynamic_bind_read_operation(&mysql, table_name);
  //  simple_read_operation(&mysql, table_name);

  // load data from sample files
  //    release_entries(entries);
  close_conn(&mysql);
}

// void multi_simple_insert(MYSQL *mysql, char *tname, entry_list *entries, int32_t sample_interval, int32_t rec_size,
//                         int32_t max_sql_len) {
//  char qstr[256] = {0};
//
//  double st = get_ts_in_ms();
//
//  entry *el = entries->data;
//
//  // total sampling count during execution
//  int32_t total_sample_rec_cnt = (entries->cur_len + sample_interval - 1) / sample_interval;
//  if (total_sample_rec_cnt > MAX_SAMPLING_CNT) {
//    total_sample_rec_cnt = MAX_SAMPLING_CNT;
//    sample_interval = entries->cur_len / MAX_SAMPLING_CNT;
//  }
//
//  // prepare sampling record array
//  sampling_ele **sample_recs = (sampling_ele **)taosMemoryMalloc(sizeof(void*) * total_sample_rec_cnt);
//  int32_t        sampling_cnt = 0;
//
//  sampling_ele *r1 = record_sample_start();
//  sample_recs[sampling_cnt++] = r1;
//
//  char *big_qstr = (char *)taosMemoryMalloc(sizeof(char) * max_sql_len);
//
//  int32_t all_cnt = 1;
//  while (el != NULL) {
//    sprintf(qstr, "insert into %s (ts, tag, lat, lon, direction) values('%s', '%s', %f, %f, %d)", tname, el->ts,
//            el->tag, el->lat, el->lon, el->direction);
//    int32_t len = strlen(qstr);
//    strcpy(big_qstr, qstr);
//    big_qstr += len;
//
//    int32_t tc = concat_obj(entries, &el, big_qstr, max_sql_len - len);
//    all_cnt += tc + 1;
//
//    big_qstr -= len;
//
//    if (mysql_query(mysql, big_qstr)) {
//      const char *errmsg = mysql_error(mysql);
//      fprintf(stderr, "insert error: %s\n", errmsg);
//    }
//
//    memset(big_qstr, 0, max_sql_len);
//  }
//
//  record_sampling_end(sample_recs[sampling_cnt - 1], all_cnt, rec_size);
//
//  double ed = get_ts_in_ms();
//  printf("total consumed time is: %.4f sec.", ((double)(ed - st)) / 1000);
//
//  char        szBuffer[64] = {0};
//  const char *pFormat = "%Y-%m-%d_%H_%M_%S.txt";
//
//  time_t     t = time(NULL);
//  struct tm *local = localtime(&t);
//  strftime(szBuffer, 64, pFormat, local);
//
//  char full_path[512] = {0};
//  sprintf(full_path, "/home/lisa/Documents/mysql_multi_simple_%s", szBuffer);
//
//  dump_sampling_record_to_file(sample_recs, sampling_cnt, full_path);
//}

int32_t concat_obj(entry_list *entries, entry **cur, char *buf, int32_t max_len) {
  char    tt[256] = {0};
  int32_t cnt = 0;

  for (int32_t total = 0; total < max_len && ((*cur) != NULL);) {
    entry *el = *cur;
    sprintf(tt, ",('%s', '%s', %f, %f, %d)", el->ts, el->tag, el->lat, el->lon, el->direction);
    int32_t len = strlen(tt);

    if (total + len > max_len) {  // overflow
      break;
    }

    strcpy(buf, tt);

    buf += len;
    total += len;
    *cur = (*cur)->next;
    cnt++;
  }

  return cnt;
}

void dynamic_bind_read_operation(MYSQL *mysql, char *table_name) {
  const char *tempsql = "SELECT * FROM %s";

  MYSQL_STMT *  stmt;
  MYSQL_BIND    bind[6];
  MYSQL_RES *   prepare_meta_result;
  MYSQL_TIME    ts;
  unsigned long length[6];
  int           column_count, row_count;
  float         lat_data;
  float         lon_data;

  int32_t key_data;
  int16_t int_data;
  char    str_data[256] = {0};
  my_bool is_null[6];

  int64_t st = get_ts_in_ms();

  /* Prepare a SELECT query to fetch data from test_table */
  if ((stmt = mysql_stmt_init(mysql)) == 0) {
    fprintf(stderr, " mysql_stmt_init(), out of memory\n");
    exit(0);
  }

  char sql[256] = {0};
  sprintf(sql, tempsql, table_name);

  if (mysql_stmt_prepare(stmt, sql, strlen(sql))) {
    fprintf(stderr, " mysql_stmt_prepare(), SELECT failed\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
  fprintf(stdout, " prepare, SELECT successful\n");

  /* Get the parameter count from the statement */
  int32_t param_count = mysql_stmt_param_count(stmt);
  fprintf(stdout, " total parameters in SELECT: %d\n", param_count);

  if (param_count != 0) /* validate parameter count */
  {
    fprintf(stderr, " invalid parameter count returned by MySQL\n");
    exit(0);
  }

  /* Fetch result set meta information */
  prepare_meta_result = mysql_stmt_result_metadata(stmt);
  if (!prepare_meta_result) {
    fprintf(stderr, " mysql_stmt_result_metadata(), returned no meta information\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

  /* Get total columns in the query */
  column_count = mysql_num_fields(prepare_meta_result);
  fprintf(stdout, " total columns in SELECT statement: %d\n", column_count);

  if (column_count != 6) /* validate column count */
  {
    fprintf(stderr, " invalid column count returned by MySQL\n");
    exit(0);
  }

  /* Bind the result buffers for all 6 columns before fetching them */
  memset(bind, 0, sizeof(bind));

  /* INTEGER COLUMN */
  bind[0].buffer_type = MYSQL_TYPE_LONG;
  bind[0].buffer = (char *)&key_data;
  bind[0].is_null = &is_null[0];
  bind[0].length = &length[0];

  /* TIMESTAMP COLUMN */
  bind[1].buffer_type = MYSQL_TYPE_TIMESTAMP;
  bind[1].buffer = (char *)&ts;
  bind[1].is_null = &is_null[1];
  bind[1].length = &length[1];

  /* STRING COLUMN */
  bind[2].buffer_type = MYSQL_TYPE_VAR_STRING;
  bind[2].buffer = (char *)str_data;
  bind[2].buffer_length = sizeof(str_data) / sizeof(str_data[0]);
  bind[2].is_null = &is_null[2];
  bind[2].length = &length[2];

  /* SMALLINT COLUMN */
  bind[3].buffer_type = MYSQL_TYPE_FLOAT;
  bind[3].buffer = (char *)&lat_data;
  bind[3].is_null = &is_null[3];
  bind[3].length = &length[3];

  /* TIMESTAMP COLUMN */
  bind[4].buffer_type = MYSQL_TYPE_FLOAT;
  bind[4].buffer = (char *)&lon_data;
  bind[4].is_null = &is_null[4];
  bind[4].length = &length[4];

  /* TIMESTAMP COLUMN */
  bind[5].buffer_type = MYSQL_TYPE_SHORT;
  bind[5].buffer = (char *)&int_data;
  bind[5].is_null = &is_null[5];
  bind[5].length = &length[5];

  /* Bind the result buffers */
  if (mysql_stmt_bind_result(stmt, bind)) {
    fprintf(stderr, " mysql_stmt_bind_result() failed\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

  /* Execute the SELECT query */
  if (mysql_stmt_execute(stmt)) {
    fprintf(stderr, " mysql_stmt_execute(), failed\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

  /* Now buffer all results to client */
  if (mysql_stmt_store_result(stmt)) {
    fprintf(stderr, " mysql_stmt_store_result() failed\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

  /* Fetch all rows */
  row_count = 0;
  fprintf(stdout, "Fetching results ...\n");
  while (!mysql_stmt_fetch(stmt)) {
    row_count++;
    //        fprintf(stdout, "  row %d\n", row_count);
    //
    //        /* column 1 */
    //        fprintf(stdout, "   column1 (integer)  : ");
    //        if (is_null[0])
    //            fprintf(stdout, " NULL\n");
    //        else
    //            fprintf(stdout, " %d(%ld)\n", int_data, length[0]);
    //
    //        /* column 2 */
    //        fprintf(stdout, "   column2 (string)   : ");
    //        if (is_null[1])
    //            fprintf(stdout, " NULL\n");
    //        else
    //            fprintf(stdout, " %s(%ld)\n", str_data, length[1]);
    //
    //        /* column 3 */
    //        fprintf(stdout, "   column3 (smallint) : ");
    //        if (is_null[2])
    //            fprintf(stdout, " NULL\n");
    //        else
    //            fprintf(stdout, " %d(%ld)\n", int_data, length[2]);
    //
    //        /* column 4 */
    //        fprintf(stdout, "   column4 (timestamp): ");
    //        if (is_null[3])
    //            fprintf(stdout, " NULL\n");
    //        else
    //            fprintf(stdout, " %04d-%02d-%02d %02d:%02d:%02d (%ld)\n",
    //                    ts.year, ts.month, ts.day,
    //                    ts.hour, ts.minute, ts.second,
    //                    length[3]);
    //        fprintf(stdout, "\n");
  }

  int64_t et = get_ts_in_ms();
  printf("%.3f mseconds to retrieve %d data points\n", (et - st) / 1000.0, row_count);

  /* Free the prepared result metadata */
  mysql_free_result(prepare_meta_result);

  /* Close the statement */
  if (mysql_stmt_close(stmt)) {
    fprintf(stderr, " failed while closing the statement\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
}

void simple_read_operation(MYSQL *mysql, char *table_name) {
  MYSQL_RES *res;  //
  MYSQL_ROW  row;  //
  int        t;

  const char *query = " select * from %s";
  char        fullsql[256] = {0};
  sprintf(fullsql, query, table_name);

  int64_t st = get_ts_in_ms();

  if (mysql_real_query(mysql, fullsql, strlen(fullsql))) {
    printf("error in executing query: %s", mysql_error(mysql));
  }

  res = mysql_store_result(mysql);
  int32_t numOfRows = 0;
  while ((row = mysql_fetch_row(res)) != NULL) {
    for (t = 0; t < mysql_num_fields(res); t++) {
      printf("%s ", row[t]);
    }
    printf("%s\n", row[1]);
    numOfRows++;
  }

  int64_t et = get_ts_in_ms();
  printf("%.3f mseconds to retrieve %d data points\n", (et - st) / 1000.0, numOfRows);

  mysql_free_result(res);
}
