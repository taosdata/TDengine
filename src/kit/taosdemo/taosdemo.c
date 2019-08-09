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

#define _GNU_SOURCE

#include <argp.h>
#include <assert.h>
#include <error.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>

#include "taos.h"
#pragma GCC diagnostic ignored "-Wmissing-braces"

#define BUFFER_SIZE      65536
#define MAX_DB_NAME_SIZE 64
#define MAX_TB_NAME_SIZE 64
#define MAX_DATA_SIZE    1024
#define MAX_NUM_DATATYPE 8
#define OPT_ABORT        1 /* –abort */

/* The options we understand. */
static struct argp_option options[] = {
  {0, 'h', "host",                     0, "The host to connect to TDEngine. Default is localhost.",                                                           0},
  {0, 'p', "port",                     0, "The TCP/IP port number to use for the connection. Default is 0.",                                                  1},
  {0, 'u', "user",                     0, "The TDEngine user name to use when connecting to the server. Default is 'root'.",                                  2},
  {0, 'a', "password",                 0, "The password to use when connecting to the server. Default is 'taosdata'.",                                        3},
  {0, 'd', "database",                 0, "Destination database. Default is 'test'.",                                                                         3},
  {0, 'm', "table_prefix",             0, "Table prefix name. Default is 't'.",                                                                               3},
  {0, 'M', 0,                          0, "Use metric flag.",                                                                                                 13},
  {0, 'o', "outputfile",               0, "Direct output to the named file. Default is './output.txt'.",                                                      14},
  {0, 'q', "query_mode",               0, "Query mode--0: SYNC, 1: ASYNC. Default is SYNC.",                                                                  6},
  {0, 'b', "type_of_cols",             0, "The data_type of columns: 'INT', 'TINYINT', 'SMALLINT', 'BIGINT', 'FLOAT', 'DOUBLE', 'BINARY'. Default is 'INT'.", 7},
  {0, 'w', "length_of_binary",         0, "The length of data_type 'BINARY'. Only applicable when type of cols is 'BINARY'. Default is 8",                    8},
  {0, 'l', "num_of_cols_per_record",   0, "The number of columns per record. Default is 1.",                                                                  8},
  {0, 'c', "num_of_conns",             0, "The number of connections. Default is 1.",                                                                         9},
  {0, 'r', "num_of_records_per_req",   0, "The number of records per request. Default is 1.",                                                                 10},
  {0, 't', "num_of_tables",            0, "The number of tables. Default is 1.",                                                                              11},
  {0, 'n', "num_of_records_per_table", 0, "The number of records per table. Default is 50000.",                                                               12},
  {0, 'f', "config_directory",         0, "Configuration directory. Default is '/etc/taos/'.",                                                                14},
  {0, 'x', 0,                          0, "Insert only flag.",                                                                                                13},
  {0}};

/* Used by main to communicate with parse_opt. */
struct arguments {
  char  *host;
  int    port;
  char  *user;
  char  *password;
  char  *database;
  char  *tb_prefix;
  bool   use_metric;
  bool   insert_only;
  char  *output_file;
  int    mode;
  char  *datatype[MAX_NUM_DATATYPE];
  int    len_of_binary;
  int    num_of_CPR;
  int    num_of_connections;
  int    num_of_RPR;
  int    num_of_tables;
  int    num_of_DPT;
  int    abort;
  char **arg_list;
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  /* Get the input argument from argp_parse, which we
     know is a pointer to our arguments structure. */
  struct arguments *arguments = state->input;
  wordexp_t full_path;
  char **sptr;
  switch (key) {
    case 'h':
      arguments->host = arg;
      break;
    case 'p':
      arguments->port = atoi(arg);
      break;
    case 'u':
      arguments->user = arg;
      break;
    case 'a':
      arguments->password = arg;
      break;
    case 'o':
      arguments->output_file = arg;
      break;
    case 'q':
      arguments->mode = atoi(arg);
      break;
    case 'c':
      arguments->num_of_connections = atoi(arg);
      break;
    case 'r':
      arguments->num_of_RPR = atoi(arg);
      break;
    case 't':
      arguments->num_of_tables = atoi(arg);
      break;
    case 'n':
      arguments->num_of_DPT = atoi(arg);
      break;
    case 'd':
      arguments->database = arg;
      break;
    case 'l':
      arguments->num_of_CPR = atoi(arg);
      break;
    case 'b':
      sptr = arguments->datatype;
      if (strstr(arg, ",") == NULL) {
        if (strcasecmp(arg, "INT") != 0 && strcasecmp(arg, "FLOAT") != 0 &&
            strcasecmp(arg, "TINYINT") != 0 && strcasecmp(arg, "BOOL") != 0 &&
            strcasecmp(arg, "SMALLINT") != 0 &&
            strcasecmp(arg, "BIGINT") != 0 && strcasecmp(arg, "DOUBLE") != 0 &&
            strcasecmp(arg, "BINARY")) {
          argp_error(state, "Invalid data_type!");
        }
        sptr[0] = arg;
      } else {
        int index = 0;
        char *dupstr = strdup(arg);
        char *running = dupstr;
        char *token = strsep(&running, ",");
        while (token != NULL) {
          if (strcasecmp(token, "INT") != 0 &&
              strcasecmp(token, "FLOAT") != 0 &&
              strcasecmp(token, "TINYINT") != 0 &&
              strcasecmp(token, "BOOL") != 0 &&
              strcasecmp(token, "SMALLINT") != 0 &&
              strcasecmp(token, "BIGINT") != 0 &&
              strcasecmp(token, "DOUBLE") != 0 && strcasecmp(token, "BINARY")) {
            argp_error(state, "Invalid data_type!");
          }
          sptr[index++] = token;
          token = strsep(&running, ", ");
        }
      }
      break;
    case 'w':
      arguments->len_of_binary = atoi(arg);
      break;
    case 'm':
      arguments->tb_prefix = arg;
      break;
    case 'M':
      arguments->use_metric = true;
      break;
    case 'x':
      arguments->insert_only = true;
      break;
    case 'f':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      strcpy(configDir, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case OPT_ABORT:
      arguments->abort = 1;
      break;
    case ARGP_KEY_ARG:
      /*arguments->arg_list = &state->argv[state->next-1];
      state->next = state->argc;*/
      argp_usage(state);
      break;

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

/* ******************************* Structure
 * definition*******************************  */
enum MODE {
  SYNC, ASYNC
};
typedef struct {
  TAOS *taos;
  int threadID;
  char db_name[MAX_DB_NAME_SIZE];
  char fp[4096];
  char **datatype;
  int len_of_binary;
  char tb_prefix[MAX_TB_NAME_SIZE];
  int start_table_id;
  int end_table_id;
  int ncols_per_record;
  int nrecords_per_table;
  int nrecords_per_request;
  long start_time;
  bool do_aggreFunc;

  sem_t mutex_sem;
  int notFinished;
  sem_t lock_sem;
} info;

typedef struct {
  TAOS  *taos;

  char   tb_name[MAX_TB_NAME_SIZE];
  long   timestamp;
  int    target;
  int    counter;
  int    nrecords_per_request;
  int    ncols_per_record;
  char **data_type;
  int    len_of_binary;

  sem_t *mutex_sem;
  int   *notFinished;
  sem_t *lock_sem;
} sTable;

/* ******************************* Global
 * variables*******************************  */
char *aggreFunc[] = {"*", "count(*)", "avg(f1)", "sum(f1)", "max(f1)", "min(f1)", "first(f1)", "last(f1)"};

/* ******************************* Global
 * functions*******************************  */
static struct argp argp = {options, parse_opt, 0, 0};

void queryDB(TAOS *taos, char *command);

void *readTable(void *sarg);

void *readMetric(void *sarg);

void *syncWrite(void *sarg);

void *asyncWrite(void *sarg);

void generateData(char *res, char **data_type, int num_of_cols, long timestamp, int len_of_binary);

void rand_string(char *str, int size);

double getCurrentTime();

void callBack(void *param, TAOS_RES *res, int code);

int main(int argc, char *argv[]) {
  struct arguments arguments = {NULL,
                                0,
                                "root",
                                "taosdata",
                                "test",
                                "t",
                                false,
                                false,
                                "./output.txt",
                                0,
                                "int",
                                "",
                                "",
                                "",
                                "",
                                "",
                                "",
                                "",
                                8,
                                1,
                                1,
                                1,
                                1,
                                50000};

  /* Parse our arguments; every option seen by parse_opt will be
     reflected in arguments. */
  // For demo use, change default values for some parameters;
  arguments.num_of_tables = 10000;
  arguments.num_of_CPR = 3; 
  arguments.num_of_connections = 10;
  arguments.num_of_DPT = 100000;
  arguments.num_of_RPR = 1000;
  arguments.use_metric = true;
  arguments.insert_only = true;
  // end change

  argp_parse(&argp, argc, argv, 0, 0, &arguments);

  if (arguments.abort) error(10, 0, "ABORTED");
  
  enum MODE query_mode = arguments.mode;
  char *ip_addr = arguments.host;
  int port = arguments.port;
  char *user = arguments.user;
  char *pass = arguments.password;
  char *db_name = arguments.database;
  char *tb_prefix = arguments.tb_prefix;
  int len_of_binary = arguments.len_of_binary;
  int ncols_per_record = arguments.num_of_CPR;
  int ntables = arguments.num_of_tables;
  int nconnections = arguments.num_of_connections;
  int nrecords_per_table = arguments.num_of_DPT;
  int nrecords_per_request = arguments.num_of_RPR;
  bool use_metric = arguments.use_metric;
  bool insert_only = arguments.insert_only;
  char **data_type = arguments.datatype;
  int count_data_type = 0;
  char dataString[512];
  bool do_aggreFunc = true;
  if (strcasecmp(data_type[0], "BINARY") == 0 || strcasecmp(data_type[0], "BOOL") == 0) {
    do_aggreFunc = false;
  }
  for (; count_data_type <= MAX_NUM_DATATYPE; count_data_type++) {
    if (strcasecmp(data_type[count_data_type], "") == 0) {
      break;
    }
    strcat(dataString, data_type[count_data_type]);
    strcat(dataString, " ");
  }

  FILE *fp = fopen(arguments.output_file, "a");
  time_t tTime = time(NULL);
  struct tm tm = *localtime(&tTime);

  fprintf(fp, "###################################################################\n");
  fprintf(fp, "# Server IP:                         %s:%d\n", ip_addr == NULL ? "localhost" : ip_addr, port);
  fprintf(fp, "# User:                              %s\n", user);
  fprintf(fp, "# Password:                          %s\n", pass);
  fprintf(fp, "# Use metric:                        %s\n", use_metric ? "true" : "false");
  fprintf(fp, "# Datatype of Columns:               %s\n", dataString);
  fprintf(fp, "# Binary Length(If applicable):      %d\n",
          (strcasestr(dataString, "BINARY") != NULL) ? len_of_binary : -1);
  fprintf(fp, "# Number of Columns per record:      %d\n", ncols_per_record);
  fprintf(fp, "# Number of Connections:             %d\n", nconnections);
  fprintf(fp, "# Number of Tables:                  %d\n", ntables);
  fprintf(fp, "# Number of Data per Table:          %d\n", nrecords_per_table);
  fprintf(fp, "# Records/Request:                   %d\n", nrecords_per_request);
  fprintf(fp, "# Database name:                     %s\n", db_name);
  fprintf(fp, "# Table prefix:                      %s\n", tb_prefix);
  fprintf(fp, "# Test time:                         %d-%02d-%02d %02d:%02d:%02d\n", tm.tm_year + 1900, tm.tm_mon + 1,
          tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
  fprintf(fp, "###################################################################\n\n");
  fprintf(fp, "|  WRecords  | Records/Second | Requests/Second |  WLatency(ms) |\n");

  taos_init();
  TAOS *taos = taos_connect(ip_addr, user, pass, NULL, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to TDengine, reason:%s\n", taos_errstr(taos));
    taos_close(taos);
    return 1;
  }
  char command[BUFFER_SIZE] = "\0";

  sprintf(command, "drop database %s;", db_name);
  taos_query(taos, command);
  sleep(3);

  sprintf(command, "create database %s;", db_name);
  taos_query(taos, command);

  char cols[512] = "\0";
  int colIndex = 0;

  for (; colIndex < ncols_per_record - 1; colIndex++) {
    if (strcasecmp(data_type[colIndex % count_data_type], "BINARY") != 0) {
      sprintf(command, ",f%d %s", colIndex + 1, data_type[colIndex % count_data_type]);
      strcat(cols, command);
    } else {
      sprintf(command, ",f%d %s(%d)", colIndex + 1, data_type[colIndex % count_data_type], len_of_binary);
      strcat(cols, command);
    }
  }

  if (strcasecmp(data_type[colIndex % count_data_type], "BINARY") != 0) {
    sprintf(command, ",f%d %s)", colIndex + 1, data_type[colIndex % count_data_type]);
  } else {
    sprintf(command, ",f%d %s(%d))", colIndex + 1, data_type[colIndex % count_data_type], len_of_binary);
  }

  strcat(cols, command);

  if (!use_metric) {
    /* Create all the tables; */
    printf("Creating %d table(s)......\n", ntables);
    for (int i = 0; i < ntables; i++) {
      sprintf(command, "create table %s.%s%d (ts timestamp%s;", db_name, tb_prefix, i, cols);
      queryDB(taos, command);
    }

    printf("Table(s) created!\n");
    taos_close(taos);

  } else {
    /* Create metric table */
    printf("Creating meters super table...\n");
    sprintf(command, "create table %s.meters (ts timestamp%s tags (areaid int, loc binary(10))", db_name, cols);
    queryDB(taos, command);
    printf("meters created!\n");

    /* Create all the tables; */
    printf("Creating %d table(s)......\n", ntables);
    for (int i = 0; i < ntables; i++) {
      int j;
      if (i % 10 == 0) {
        j = 10;
      } else {
        j = i % 10;
      }
    if (j % 2 == 0) {
       sprintf(command, "create table %s.%s%d using %s.meters tags (%d,\"%s\");", db_name, tb_prefix, i, db_name, j,"shanghai");
      } else {
       sprintf(command, "create table %s.%s%d using %s.meters tags (%d,\"%s\");", db_name, tb_prefix, i, db_name, j,"beijing");
      }
      queryDB(taos, command);
    }

    printf("Table(s) created!\n");
    taos_close(taos);
  }
  /* Wait for table to create  */
  sleep(5);

  /* Insert data */
  double ts = getCurrentTime();
  printf("Inserting data......\n");
  pthread_t *pids = malloc(nconnections * sizeof(pthread_t));
  info *infos = malloc(nconnections * sizeof(info));

  int a = ntables / nconnections;
  if (a < 1) {
    nconnections = ntables;
    a = 1;
  }
  int b = ntables % nconnections;
  int last = 0;
  for (int i = 0; i < nconnections; i++) {
    info *t_info = infos + i;
    t_info->threadID = i;
    strcpy(t_info->db_name, db_name);
    strcpy(t_info->tb_prefix, tb_prefix);
    t_info->datatype = data_type;
    t_info->ncols_per_record = ncols_per_record;
    t_info->nrecords_per_table = nrecords_per_table;
    t_info->start_time = 1500000000000;
    t_info->taos = taos_connect(ip_addr, user, pass, db_name, port);
    t_info->len_of_binary = len_of_binary;
    t_info->nrecords_per_request = nrecords_per_request;
    t_info->start_table_id = last;
    t_info->end_table_id = i < b ? last + a : last + a - 1;
    last = t_info->end_table_id + 1;

    sem_init(&(t_info->mutex_sem), 0, 1);
    t_info->notFinished = t_info->end_table_id - t_info->start_table_id + 1;
    sem_init(&(t_info->lock_sem), 0, 0);

    if (query_mode == SYNC) {
      pthread_create(pids + i, NULL, syncWrite, t_info);
    } else {
      pthread_create(pids + i, NULL, asyncWrite, t_info);
    }
  }
  for (int i = 0; i < nconnections; i++) {
    pthread_join(pids[i], NULL);
  }

  double t = getCurrentTime() - ts;
  if (query_mode == SYNC) {
    printf("SYNC Insert with %d connections:\n", nconnections);
  } else {
    printf("ASYNC Insert with %d connections:\n", nconnections);
  }

  fprintf(fp, "|%10.d  |  %10.2f    |  %10.2f     |  %10.4f   |\n\n",
          ntables * nrecords_per_table, ntables * nrecords_per_table / t,
          (ntables * nrecords_per_table) / (t * nrecords_per_request),
          t * 1000);

  printf("Spent %.4f seconds to insert %d records with %d record(s) per request: %.2f records/second\n",
         t, ntables * nrecords_per_table, nrecords_per_request,
         ntables * nrecords_per_table / t);

  for (int i = 0; i < nconnections; i++) {
    info *t_info = infos + i;
    taos_close(t_info->taos);
    sem_destroy(&(t_info->mutex_sem));
    sem_destroy(&(t_info->lock_sem));
  }

  free(pids);
  free(infos);
  fclose(fp);

  if (!insert_only) {
    // query data
    pthread_t read_id;
    info *rInfo = malloc(sizeof(info));
    rInfo->start_time = 1500000000000;
    rInfo->start_table_id = 0;
    rInfo->end_table_id = ntables - 1;
    rInfo->do_aggreFunc = do_aggreFunc;
    rInfo->nrecords_per_table = nrecords_per_table;
    rInfo->taos = taos_connect(ip_addr, user, pass, db_name, port);
    strcpy(rInfo->tb_prefix, tb_prefix);
    strcpy(rInfo->fp, arguments.output_file);

    if (!use_metric) {
      pthread_create(&read_id, NULL, readTable, rInfo);
    } else {
      pthread_create(&read_id, NULL, readMetric, rInfo);
    }
    pthread_join(read_id, NULL);
    taos_close(rInfo->taos);
  }

  return 0;
}

void *readTable(void *sarg) {
  info *rinfo = (info *)sarg;
  TAOS *taos = rinfo->taos;
  char command[BUFFER_SIZE] = "\0";
  long sTime = rinfo->start_time;
  char *tb_prefix = rinfo->tb_prefix;
  FILE *fp = fopen(rinfo->fp, "a");
  int num_of_DPT = rinfo->nrecords_per_table;
  int num_of_tables = rinfo->end_table_id - rinfo->start_table_id + 1;
  int totalData = num_of_DPT * num_of_tables;
  bool do_aggreFunc = rinfo->do_aggreFunc;

  int n = do_aggreFunc ? (sizeof(aggreFunc) / sizeof(aggreFunc[0])) : 2;
  if (!do_aggreFunc) {
    printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
  }
  printf("%d records:\n", totalData);
  fprintf(fp, "| QFunctions |    QRecords    |   QSpeed(R/s)   |  QLatency(ms) |\n");

  for (int j = 0; j < n; j++) {
    double totalT = 0;
    int count = 0;
    for (int i = 0; i < num_of_tables; i++) {
      sprintf(command, "select %s from %s%d where ts>= %ld", aggreFunc[j], tb_prefix, i, sTime);

      double t = getCurrentTime();
      if (taos_query(taos, command) != 0) {
        fprintf(stderr, "Failed to query\n");
        taos_close(taos);
        exit(EXIT_FAILURE);
      }

      TAOS_RES *result = taos_use_result(taos);
      if (result == NULL) {
        fprintf(stderr, "Failed to retreive results:%s\n", taos_errstr(taos));
        taos_close(taos);
        exit(1);
      }

      while (taos_fetch_row(result) != NULL) {
        count++;
      }

      t = getCurrentTime() - t;
      totalT += t;

      taos_free_result(result);
    }

    fprintf(fp, "|%10s  |   %10d   |  %12.2f   |   %10.2f  |\n",
            aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j], totalData,
            (double)(num_of_tables * num_of_DPT) / totalT, totalT * 1000);
    printf("select %10s took %.6f second(s)\n", aggreFunc[j], totalT);
  }
  fprintf(fp, "\n");

  fclose(fp);
  return NULL;
}

void *readMetric(void *sarg) {
  info *rinfo = (info *)sarg;
  TAOS *taos = rinfo->taos;
  char command[BUFFER_SIZE] = "\0";
  FILE *fp = fopen(rinfo->fp, "a");
  int num_of_DPT = rinfo->nrecords_per_table;
  int num_of_tables = rinfo->end_table_id - rinfo->start_table_id + 1;
  int totalData = num_of_DPT * num_of_tables;
  bool do_aggreFunc = rinfo->do_aggreFunc;

  int n = do_aggreFunc ? (sizeof(aggreFunc) / sizeof(aggreFunc[0])) : 2;
  if (!do_aggreFunc) {
    printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
  }
  printf("%d records:\n", totalData);
  fprintf(fp, "Querying On %d records:\n", totalData);

  for (int j = 0; j < n; j++) {
    char condition[BUFFER_SIZE] = "\0";
    char tempS[BUFFER_SIZE] = "\0";

    int m = 10 < num_of_tables ? 10 : num_of_tables;

    for (int i = 1; i <= m; i++) {
      if (i == 1) {
        sprintf(tempS, "index = %d", i);
      } else {
        sprintf(tempS, " or index = %d ", i);
      }
      strcat(condition, tempS);

      sprintf(command, "select %s from m1 where %s", aggreFunc[j], condition);

      printf("Where condition: %s\n", condition);
      fprintf(fp, "%s\n", command);

      double t = getCurrentTime();
      if (taos_query(taos, command) != 0) {
        fprintf(stderr, "Failed to query\n");
        taos_close(taos);
        exit(EXIT_FAILURE);
      }

      TAOS_RES *result = taos_use_result(taos);
      if (result == NULL) {
        fprintf(stderr, "Failed to retreive results:%s\n", taos_errstr(taos));
        taos_close(taos);
        exit(1);
      }
      int count = 0;
      while (taos_fetch_row(result) != NULL) {
        count++;
      }
      t = getCurrentTime() - t;

      fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n", num_of_tables * num_of_DPT / t, t * 1000);
      printf("select %10s took %.6f second(s)\n\n", aggreFunc[j], t);

      taos_free_result(result);
    }
    fprintf(fp, "\n");
  }

  fclose(fp);
  return NULL;
}

void queryDB(TAOS *taos, char *command) {
  if (taos_query(taos, command) != 0) {
    fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(taos));
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
}

// sync insertion
void *syncWrite(void *sarg) {
  info *winfo = (info *)sarg;
  char buffer[BUFFER_SIZE] = "\0";
  char data[MAX_DATA_SIZE];
  char **data_type = winfo->datatype;
  int len_of_binary = winfo->len_of_binary;
  int ncols_per_record = winfo->ncols_per_record;
  srand(time(NULL));
  long time_counter = winfo->start_time;
  for (int i = 0; i < winfo->nrecords_per_table;) {
    for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++) {
      int inserted = i;
      long tmp_time = time_counter;

      char *pstr = buffer;
      pstr += sprintf(pstr, "insert into %s.%s%d values", winfo->db_name, winfo->tb_prefix, tID);
      int k;
      for (k = 0; k < winfo->nrecords_per_request;) {
        generateData(data, data_type, ncols_per_record, tmp_time++, len_of_binary);
        pstr += sprintf(pstr, " %s", data);
        inserted++;
        k++;

        if (inserted >= winfo->nrecords_per_table) break;
      }

      /* puts(buffer); */
      queryDB(winfo->taos, buffer);

      if (tID == winfo->end_table_id) {
        i = inserted;
        time_counter = tmp_time;
      }
    }
  }
  return NULL;
}

void *asyncWrite(void *sarg) {
  info *winfo = (info *)sarg;

  sTable *tb_infos = (sTable *)malloc(sizeof(sTable) * (winfo->end_table_id - winfo->start_table_id + 1));

  for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++) {
    sTable *tb_info = tb_infos + tID - winfo->start_table_id;
    tb_info->data_type = winfo->datatype;
    tb_info->ncols_per_record = winfo->ncols_per_record;
    tb_info->taos = winfo->taos;
    sprintf(tb_info->tb_name, "%s.%s%d", winfo->db_name, winfo->tb_prefix, tID);
    tb_info->timestamp = winfo->start_time;
    tb_info->counter = 0;
    tb_info->target = winfo->nrecords_per_table;
    tb_info->len_of_binary = winfo->len_of_binary;
    tb_info->nrecords_per_request = winfo->nrecords_per_request;
    tb_info->mutex_sem = &(winfo->mutex_sem);
    tb_info->notFinished = &(winfo->notFinished);
    tb_info->lock_sem = &(winfo->lock_sem);

    /* char buff[BUFFER_SIZE] = "\0"; */
    /* sprintf(buff, "insert into %s values (0, 0)", tb_info->tb_name); */
    /* queryDB(tb_info->taos,buff); */

    taos_query_a(winfo->taos, "show databases", callBack, tb_info);
  }

  sem_wait(&(winfo->lock_sem));
  free(tb_infos);

  return NULL;
}

void callBack(void *param, TAOS_RES *res, int code) {
  sTable *tb_info = (sTable *)param;
  char **datatype = tb_info->data_type;
  int ncols_per_record = tb_info->ncols_per_record;
  int len_of_binary = tb_info->len_of_binary;
  long tmp_time = tb_info->timestamp;

  if (code < 0) {
    fprintf(stderr, "failed to insert data %d:reason; %s\n", code, taos_errstr(tb_info->taos));
    exit(EXIT_FAILURE);
  }

  // If finished;
  if (tb_info->counter >= tb_info->target) {
    sem_wait(tb_info->mutex_sem);
    (*(tb_info->notFinished))--;
    if (*(tb_info->notFinished) == 0) sem_post(tb_info->lock_sem);
    sem_post(tb_info->mutex_sem);
    return;
  }

  char buffer[BUFFER_SIZE] = "\0";
  char data[MAX_DATA_SIZE];
  char *pstr = buffer;
  pstr += sprintf(pstr, "insert into %s values", tb_info->tb_name);

  for (int i = 0; i < tb_info->nrecords_per_request; i++) {
    generateData(data, datatype, ncols_per_record, tmp_time++, len_of_binary);
    pstr += sprintf(pstr, "%s", data);
    tb_info->counter++;

    if (tb_info->counter >= tb_info->target) {
      break;
    }
  }

  taos_query_a(tb_info->taos, buffer, callBack, tb_info);

  taos_free_result(res);
}

double getCurrentTime() {
  struct timeval tv;
  if (gettimeofday(&tv, NULL) != 0) {
    perror("Failed to get current time in ms");
    exit(EXIT_FAILURE);
  }

  return tv.tv_sec + tv.tv_usec / 1E6;
}

void generateData(char *res, char **data_type, int num_of_cols, long timestamp, int len_of_binary) {
  memset(res, 0, MAX_DATA_SIZE);
  char *pstr = res;
  pstr += sprintf(pstr, "(%ld", timestamp);
  int c = 0;

  for (; c < MAX_NUM_DATATYPE; c++) {
    if (strcasecmp(data_type[c], "") == 0) {
      break;
    }
  }

  for (int i = 0; i < num_of_cols; i++) {
    if (strcasecmp(data_type[i % c], "tinyint") == 0) {
      pstr += sprintf(pstr, ", %d", (int)(rand() % 128));
    } else if (strcasecmp(data_type[i % c], "smallint") == 0) {
      pstr += sprintf(pstr, ", %d", (int)(rand() % 32767));
    } else if (strcasecmp(data_type[i % c], "int") == 0) {
      pstr += sprintf(pstr, ", %d", (int)(rand() % 50)); 
    } else if (strcasecmp(data_type[i % c], "bigint") == 0) {
      pstr += sprintf(pstr, ", %ld", rand() % 2147483648);
    } else if (strcasecmp(data_type[i % c], "float") == 0) {
      pstr += sprintf(pstr, ", %10.4f", (float)(rand() / 1000));
    } else if (strcasecmp(data_type[i % c], "double") == 0) {
      double t = (double)(rand() / 1000000);
      pstr += sprintf(pstr, ", %20.8f", t);
    } else if (strcasecmp(data_type[i % c], "bool") == 0) {
      bool b = rand() & 1;
      pstr += sprintf(pstr, ", %s", b ? "true" : "false");
    } else if (strcasecmp(data_type[i % c], "binary") == 0) {
      char s[len_of_binary];
      rand_string(s, len_of_binary);
      pstr += sprintf(pstr, ", %s", s);
    }
  }

  pstr += sprintf(pstr, ")");
}

void rand_string(char *str, int size) {
  memset(str, 0, size);
  const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJK1234567890";
  char *sptr = str;
  if (size) {
    --size;
    for (size_t n = 0; n < size; n++) {
      int key = rand() % (int)(sizeof charset - 1);
      sptr += sprintf(sptr, "%c", charset[key]);
    }
  }
}
