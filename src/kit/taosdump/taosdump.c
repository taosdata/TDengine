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

#include <argp.h>
#include <assert.h>
#include <error.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <wordexp.h>

#include "taos.h"
#include "taosmsg.h"
#include "tglobalcfg.h"
#include "tsclient.h"
#include "tsdb.h"
#include "tutil.h"

#define COMMAND_SIZE 65536
#define DEFAULT_DUMP_FILE "taosdump.sql"

typedef struct {
  short bytes;
  int8_t type;
} SOColInfo;

// -------------------------- SHOW DATABASE INTERFACE-----------------------
enum _show_db_index {
  TSDB_SHOW_DB_NAME_INDEX,
  TSDB_SHOW_DB_CREATED_TIME_INDEX,
  TSDB_SHOW_DB_VGROUPS_INDEX,
  TSDB_SHOW_DB_NTABLES_INDEX,
  TSDB_SHOW_DB_REPLICA_INDEX,
  TSDB_SHOW_DB_DAYS_INDEX,
  TSDB_SHOW_DB_KEEP_INDEX,
  TSDB_SHOW_DB_TABLES_INDEX,
  TSDB_SHOW_DB_ROWS_INDEX,
  TSDB_SHOW_DB_CACHE_INDEX,
  TSDB_SHOW_DB_ABLOCKS_INDEX,
  TSDB_SHOW_DB_TBLOCKS_INDEX,
  TSDB_SHOW_DB_CTIME_INDEX,
  TSDB_SHOW_DB_CLOG_INDEX,
  TSDB_SHOW_DB_COMP_INDEX,
  TSDB_MAX_SHOW_DB
};

// -----------------------------------------SHOW TABLES CONFIGURE
// -------------------------------------
enum _show_tables_index {
  TSDB_SHOW_TABLES_NAME_INDEX,
  TSDB_SHOW_TABLES_CREATED_TIME_INDEX,
  TSDB_SHOW_TABLES_COLUMNS_INDEX,
  TSDB_SHOW_TABLES_METRIC_INDEX,
  TSDB_MAX_SHOW_TABLES
};

// ---------------------------------- DESCRIBE METRIC CONFIGURE
// ------------------------------
enum _describe_table_index {
  TSDB_DESCRIBE_METRIC_FIELD_INDEX,
  TSDB_DESCRIBE_METRIC_TYPE_INDEX,
  TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
  TSDB_DESCRIBE_METRIC_NOTE_INDEX,
  TSDB_MAX_DESCRIBE_METRIC
};

typedef struct {
  char field[TSDB_COL_NAME_LEN + 1];
  char type[16];
  int length;
  char note[8];
} SColDes;

typedef struct {
  char name[TSDB_COL_NAME_LEN + 1];
  SColDes cols[];
} STableDef;

extern char version[];

typedef struct {
  char name[TSDB_DB_NAME_LEN + 1];
  int32_t replica;
  int32_t days;
  int32_t keep;
  int32_t tables;
  int32_t rows;
  int32_t cache;
  int32_t ablocks;
  int32_t tblocks;
  int32_t ctime;
  int32_t clog;
  int32_t comp;
} SDbInfo;

typedef struct {
  char name[TSDB_METER_NAME_LEN + 1];
  char metric[TSDB_METER_NAME_LEN + 1];
} STableRecord;

typedef struct {
  bool isMetric;
  STableRecord tableRecord;
} STableRecordInfo;

SDbInfo **dbInfos = NULL;

const char *argp_program_version = version;
const char *argp_program_bug_address = "<support@taosdata.com>";

/* Program documentation. */
static char doc[] = "";
/* "Argp example #4 -- a program with somewhat more complicated\ */
/*         options\ */
/*         \vThis part of the documentation comes *after* the options;\ */
/*         note that the text is automatically filled, but it's possible\ */
/*         to force a line-break, e.g.\n<-- here."; */

/* A description of the arguments we accept. */
static char args_doc[] = "dbname [tbname ...]\n--databases dbname ...\n--all-databases\n-i input_file";

/* Keys for options without short-options. */
#define OPT_ABORT 1 /* –abort */

/* The options we understand. */
static struct argp_option options[] = {
  // connection option
  {"host",          'h', "HOST",       0, "Server host dumping data from. Default is localhost.",     0},
  {"user",          'u', "USER",       0, "User name used to connect to server. Default is root.",    0},
  {"password",      'p', "PASSWORD",   0, "User password to connect to server. Default is taosdata.", 0},
  {"port",          'P', "PORT",       0, "Port to connect",                                          0},
  // input/output file
  {"output",        'o', "OUTPUT",     0, "Output file name.",                                        1},
  {"input",         'i', "INPUT",      0, "Input file name.",                                         1},
  {"config",        'c', "CONFIG_DIR", 0, "Configure directory. Default is /etc/taos/taos.cfg.",      1},
  // dump unit options
  {"all-databases", 'A', 0,            0, "Dump all databases.",                                      2},
  {"databases",     'B', 0,            0, "Dump assigned databases",                                  2},
  // dump format options
  {"schemaonly",    's', 0,            0, "Only dump schema.",                                        3},
  {"with-property", 'M', 0,            0, "Dump schema with properties.",                             3},
  {"start-time",    'S', "START_TIME", 0, "Start time to dump.",                                      3},
  {"end-time",      'E', "END_TIME",   0, "End time to dump.",                                        3},
  {"data-batch",    'N', "DATA_BATCH", 0, "Number of data point per insert statement. Default is 1.", 3},
  {"allow-sys",     'a', 0,            0, "Allow to dump sys database",                               3},
  {0}};

/* Used by main to communicate with parse_opt. */
struct arguments {
  // connection option
  char *host;
  char *user;
  char *password;
  int port;
  // output file
  char output[TSDB_FILENAME_LEN + 1];
  char input[TSDB_FILENAME_LEN + 1];
  // dump unit option
  bool all_databases;
  bool databases;
  // dump format option
  bool schemaonly;
  bool with_property;
  int64_t start_time;
  int64_t end_time;
  int data_batch;
  bool allow_sys;
  // other options
  int abort;
  char **arg_list;
  int arg_list_len;
  bool isDumpIn;
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  /* Get the input argument from argp_parse, which we
     know is a pointer to our arguments structure. */
  struct arguments *arguments = state->input;
  wordexp_t full_path;

  switch (key) {
    // connection option
    case 'a':
      arguments->allow_sys = true;
      break;
    case 'h':
      arguments->host = arg;
      break;
    case 'u':
      arguments->user = arg;
      break;
    case 'p':
      arguments->password = arg;
      break;
    case 'P':
      arguments->port = atoi(arg);
      break;
      // output file
    case 'o':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      strcpy(arguments->output, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case 'i':
      arguments->isDumpIn = true;
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      strcpy(arguments->input, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case 'c':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      strcpy(configDir, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
      // dump unit option
    case 'A':
      arguments->all_databases = true;
      break;
    case 'B':
      arguments->databases = true;
      break;
      // dump format option
    case 's':
      arguments->schemaonly = true;
      break;
    case 'M':
      arguments->with_property = true;
      break;
    case 'S':
      // parse time here.
      arguments->start_time = atol(arg);
      break;
    case 'E':
      arguments->end_time = atol(arg);
      break;
    case 'N':
      arguments->data_batch = atoi(arg);
      break;
    case OPT_ABORT:
      arguments->abort = 1;
      break;
    case ARGP_KEY_ARG:
      arguments->arg_list = &state->argv[state->next - 1];
      arguments->arg_list_len = state->argc - state->next + 1;
      state->next = state->argc;
      break;

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

/* Our argp parser. */
static struct argp argp = {options, parse_opt, args_doc, doc};

TAOS *taos = NULL;
TAOS_RES *result = NULL;
char *command = NULL;
char *buffer = NULL;

int taosDumpOut(struct arguments *arguments);

int taosDumpIn(struct arguments *arguments);

void taosDumpCreateDbClause(SDbInfo *dbInfo, bool isDumpProperty, FILE *fp);

int taosDumpDb(SDbInfo *dbInfo, struct arguments *arguments, FILE *fp);

void taosDumpCreateTableClause(STableDef *tableDes, int numOfCols, struct arguments *arguments, FILE *fp);

void taosDumpCreateMTableClause(STableDef *tableDes, char *metric, int numOfCols, struct arguments *arguments,
                                FILE *fp);

int32_t taosDumpTable(char *table, char *metric, struct arguments *arguments, FILE *fp);

int32_t taosDumpMetric(char *metric, struct arguments *arguments, FILE *fp);

int taosDumpTableData(FILE *fp, char *tbname, struct arguments *arguments);

int taosCheckParam(struct arguments *arguments);

void taosFreeDbInfos();

int main(int argc, char *argv[]) {
  struct arguments arguments = {
    // connection option
    NULL, "root", "taosdata", 0,
    // output file
    DEFAULT_DUMP_FILE, DEFAULT_DUMP_FILE,
    // dump unit option
    false, false,
    // dump format option
    false, false, 0, INT64_MAX, 1, false,
    // other options
    0, NULL, 0, false};

  /* Parse our arguments; every option seen by parse_opt will be
     reflected in arguments. */
  argp_parse(&argp, argc, argv, 0, 0, &arguments);

  if (arguments.abort) error(10, 0, "ABORTED");

  if (taosCheckParam(&arguments) < 0) {
    exit(EXIT_FAILURE);
  }

  if (arguments.isDumpIn) {
    if (taosDumpIn(&arguments) < 0) return -1;
  } else {
    if (taosDumpOut(&arguments) < 0) return -1;
  }

  return 0;
}

void taosFreeDbInfos() {
  if (dbInfos == NULL) return;
  for (int i = 0; i < tsMaxDbs; i++) tfree(dbInfos[i]);
  tfree(dbInfos);
}

int taosGetTableRecordInfo(char *table, STableRecordInfo *pTableRecordInfo) {
  TAOS_ROW row = NULL;
  bool isSet = false;

  memset(pTableRecordInfo, 0, sizeof(STableRecordInfo));

  sprintf(command, "show tables like %s", table);
  if (taos_query(taos, command) != 0) {
    fprintf(stderr, "failed to run command %s\n", command);
    return -1;
  }

  result = taos_use_result(taos);
  if (result == NULL) {
    fprintf(stderr, "failed to use result\n");
    return -1;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result)) != NULL) {
    isSet = true;
    pTableRecordInfo->isMetric = false;
    strncpy(pTableRecordInfo->tableRecord.name, (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
            fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes);
    strncpy(pTableRecordInfo->tableRecord.metric, (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX],
            fields[TSDB_SHOW_TABLES_METRIC_INDEX].bytes);
    break;
  }

  taos_free_result(result);
  result = NULL;

  if (isSet) return 0;

  sprintf(command, "show stables like %s", table);
  if (taos_query(taos, command) != 0) {
    fprintf(stderr, "failed to run command %s\n", command);
    return -1;
  }

  result = taos_use_result(taos);
  if (result == NULL) {
    fprintf(stderr, "failed to use result\n");
    return -1;
  }

  while ((row = taos_fetch_row(result)) != NULL) {
    isSet = true;
    pTableRecordInfo->isMetric = true;
    strcpy(pTableRecordInfo->tableRecord.metric, table);
    break;
  }

  taos_free_result(result);
  result = NULL;

  if (isSet) return 0;

  fprintf(stderr, "invalid table/metric %s\n", table);
  return -1;
}

int taosDumpOut(struct arguments *arguments) {
  TAOS_ROW row;
  char *temp = NULL;
  FILE *fp = NULL;
  int count = 0;
  STableRecordInfo tableRecordInfo;

  fp = fopen(arguments->output, "w");
  if (fp == NULL) {
    fprintf(stderr, "failed to open file %s\n", arguments->output);
    return -1;
  }

  dbInfos = (SDbInfo **)calloc(tsMaxDbs, sizeof(SDbInfo *));
  if (dbInfos == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    goto _exit_failure;
  }

  temp = (char *)malloc(2 * COMMAND_SIZE);
  if (temp == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    goto _exit_failure;
  }

  command = temp;
  buffer = command + COMMAND_SIZE;

  /* Connect to server */
  taos = taos_connect(arguments->host, arguments->user, arguments->password, NULL, arguments->port);
  if (taos == NULL) {
    fprintf(stderr, "failed to connect to TDEngine server\n");
    goto _exit_failure;
  }

  /* --------------------------------- Main Code
   * -------------------------------- */
  /* if (arguments->databases || arguments->all_databases) { // dump part of
   * databases or all databases */
  /*  */
  sprintf(command, "show databases");
  if (taos_query(taos, command) != 0) {
    fprintf(stderr, "failed to run command: %s, reason: %s\n", command, taos_errstr(taos));
    goto _exit_failure;
  }

  result = taos_use_result(taos);
  if (result == NULL) {
    fprintf(stderr, "failed to use result\n");
    goto _exit_failure;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result)) != NULL) {
    if (strncasecmp(row[TSDB_SHOW_DB_NAME_INDEX], "monitor", fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0 &&
        (!arguments->allow_sys))
      continue;

    if (arguments->databases) {
      for (int i = 0; arguments->arg_list[i]; i++) {
        if (strncasecmp(arguments->arg_list[i], (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0)
          goto _dump_db_point;
      }
      continue;
    } else if (!arguments->all_databases) {
      if (strncasecmp(arguments->arg_list[0], (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                      fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0)
        goto _dump_db_point;
      else
        continue;
    }

    _dump_db_point:

    dbInfos[count] = (SDbInfo *)calloc(1, sizeof(SDbInfo));
    if (dbInfos[count] == NULL) {
      fprintf(stderr, "failed to allocate memory\n");
      goto _exit_failure;
    }

    strncpy(dbInfos[count]->name, (char *)row[TSDB_SHOW_DB_NAME_INDEX], fields[TSDB_SHOW_DB_NAME_INDEX].bytes);
    dbInfos[count]->replica = (int)(*((int16_t *)row[TSDB_SHOW_DB_REPLICA_INDEX]));
    dbInfos[count]->days = (int)(*((int16_t *)row[TSDB_SHOW_DB_DAYS_INDEX]));
    dbInfos[count]->keep = *((int *)row[TSDB_SHOW_DB_KEEP_INDEX]);
    dbInfos[count]->tables = *((int *)row[TSDB_SHOW_DB_TABLES_INDEX]);
    dbInfos[count]->rows = *((int *)row[TSDB_SHOW_DB_ROWS_INDEX]);
    dbInfos[count]->cache = *((int *)row[TSDB_SHOW_DB_CACHE_INDEX]);
    dbInfos[count]->ablocks = *((int *)row[TSDB_SHOW_DB_ABLOCKS_INDEX]);
    dbInfos[count]->tblocks = (int)(*((int16_t *)row[TSDB_SHOW_DB_TBLOCKS_INDEX]));
    dbInfos[count]->ctime = *((int *)row[TSDB_SHOW_DB_CTIME_INDEX]);
    dbInfos[count]->clog = (int)(*((int8_t *)row[TSDB_SHOW_DB_CLOG_INDEX]));
    dbInfos[count]->comp = (int)(*((int8_t *)row[TSDB_SHOW_DB_COMP_INDEX]));

    count++;

    if (arguments->databases) {
      if (count > arguments->arg_list_len) break;

    } else if (!arguments->all_databases) {
      if (count >= 1) break;
    }
  }

  taos_free_result(result);

  if (count == 0) {
    fprintf(stderr, "No databases valid to dump\n");
    goto _exit_failure;
  }

  if (arguments->databases || arguments->all_databases) {
    for (int i = 0; i < count; i++) {
      taosDumpDb(dbInfos[i], arguments, fp);
    }
  } else {
    if (arguments->arg_list_len == 1) {
      taosDumpDb(dbInfos[0], arguments, fp);
    } else {
      taosDumpCreateDbClause(dbInfos[0], arguments->with_property, fp);

      sprintf(command, "use %s", dbInfos[0]->name);
      if (taos_query(taos, command) != 0) {
        fprintf(stderr, "invalid database %s\n", dbInfos[0]->name);
        goto _exit_failure;
      }

      fprintf(fp, "USE %s;\n\n", dbInfos[0]->name);

      for (int i = 1; arguments->arg_list[i]; i++) {
        if (taosGetTableRecordInfo(arguments->arg_list[i], &tableRecordInfo) < 0) {
          fprintf(stderr, "invalide table %s\n", arguments->arg_list[i]);
          continue;
        }

        if (tableRecordInfo.isMetric) {  // dump whole metric
          taosDumpMetric(tableRecordInfo.tableRecord.metric, arguments, fp);
        } else {  // dump MTable and NTable
          taosDumpTable(tableRecordInfo.tableRecord.name, tableRecordInfo.tableRecord.metric, arguments, fp);
        }
      }
    }
  }

  /* Close the handle and return */
  fclose(fp);
  taos_close(taos);
  taos_free_result(result);
  free(temp);
  taosFreeDbInfos();
  return 0;

  _exit_failure:
  fclose(fp);
  taos_close(taos);
  taos_free_result(result);
  free(temp);
  taosFreeDbInfos();
  return -1;
}

void taosDumpCreateDbClause(SDbInfo *dbInfo, bool isDumpProperty, FILE *fp) {
  char *pstr = buffer;

  pstr += sprintf(pstr, "CREATE DATABASE IF NOT EXISTS %s", dbInfo->name);
  if (isDumpProperty) {
    pstr += sprintf(pstr,
                    " REPLICA %d DAYS %d KEEP %d TABLES %d ROWS %d CACHE %d ABLOCKS %d TBLOCKS %d CTIME %d CLOG %d COMP %d",
                    dbInfo->replica, dbInfo->days, dbInfo->keep, dbInfo->tables, dbInfo->rows, dbInfo->cache,
                    dbInfo->ablocks, dbInfo->tblocks, dbInfo->ctime, dbInfo->clog, dbInfo->comp);
  }

  fprintf(fp, "%s\n\n", buffer);
}

int taosDumpDb(SDbInfo *dbInfo, struct arguments *arguments, FILE *fp) {
  TAOS_ROW row;
  int fd = -1;
  STableRecord tableRecord;

  taosDumpCreateDbClause(dbInfo, arguments->with_property, fp);

  sprintf(command, "use %s", dbInfo->name);
  if (taos_query(taos, command) != 0) {
    fprintf(stderr, "invalid database %s\n", dbInfo->name);
    return -1;
  }

  fprintf(fp, "USE %s\n\n", dbInfo->name);

  sprintf(command, "show tables");
  if (taos_query(taos, command) != 0) {
    fprintf(stderr, "failed to run command %s\n", command);
    return -1;
  }

  result = taos_use_result(taos);
  if (result == NULL) {
    fprintf(stderr, "failed to use result\n");
    return -1;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);

  fd = open(".table.tmp", O_RDWR | O_CREAT, 0755);
  if (fd == -1) {
    fprintf(stderr, "failed to open temp file\n");
    taos_free_result(result);
    return -1;
  }

  while ((row = taos_fetch_row(result)) != NULL) {
    memset(&tableRecord, 0, sizeof(STableRecord));
    strncpy(tableRecord.name, (char *)row[TSDB_SHOW_TABLES_NAME_INDEX], fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes);
    strncpy(tableRecord.metric, (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX], fields[TSDB_SHOW_TABLES_METRIC_INDEX].bytes);

    write(fd, &tableRecord, sizeof(STableRecord));
  }

  taos_free_result(result);

  lseek(fd, 0, SEEK_SET);

  while (read(fd, &tableRecord, sizeof(STableRecord)) > 0) {
    taosDumpTable(tableRecord.name, tableRecord.metric, arguments, fp);
  }

  tclose(fd);
  remove(".table.tmp");

  return 0;
}

void taosDumpCreateTableClause(STableDef *tableDes, int numOfCols, struct arguments *arguments, FILE *fp) {
  char *pstr = NULL;
  pstr = buffer;
  int counter = 0;
  int count_temp = 0;

  pstr += sprintf(buffer, "CREATE TABLE IF NOT EXISTS %s", tableDes->name);

  for (; counter < numOfCols; counter++) {
    if (tableDes->cols[counter].note[0] != '\0') break;

    if (counter == 0) {
      pstr += sprintf(pstr, " (%s %s", tableDes->cols[counter].field, tableDes->cols[counter].type);
    } else {
      pstr += sprintf(pstr, ", %s %s", tableDes->cols[counter].field, tableDes->cols[counter].type);
    }

    if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
        strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
      pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length);
    }
  }

  count_temp = counter;

  for (; counter < numOfCols; counter++) {
    if (counter == count_temp) {
      pstr += sprintf(pstr, ") TAGS (%s %s", tableDes->cols[counter].field, tableDes->cols[counter].type);
    } else {
      pstr += sprintf(pstr, ", %s %s", tableDes->cols[counter].field, tableDes->cols[counter].type);
    }

    if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
        strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
      pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length);
    }
  }

  pstr += sprintf(pstr, ")");

  fprintf(fp, "%s\n\n", buffer);
}

void taosDumpCreateMTableClause(STableDef *tableDes, char *metric, int numOfCols, struct arguments *arguments,
                                FILE *fp) {
  char *pstr = NULL;
  pstr = buffer;
  int counter = 0;
  int count_temp = 0;

  pstr += sprintf(buffer, "CREATE TABLE IF NOT EXISTS %s USING %s TAGS (", tableDes->name, metric);

  for (; counter < numOfCols; counter++) {
    if (tableDes->cols[counter].note[0] != '\0') break;
  }

  assert(counter < numOfCols);
  count_temp = counter;

  for (; counter < numOfCols; counter++) {
    if (counter != count_temp) {
      if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
          strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
        pstr += sprintf(pstr, ", \'%s\'", tableDes->cols[counter].note);
      } else {
        pstr += sprintf(pstr, ", %s", tableDes->cols[counter].note);
      }
    } else {
      if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
          strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
        pstr += sprintf(pstr, "\'%s\'", tableDes->cols[counter].note);
      } else {
        pstr += sprintf(pstr, "%s", tableDes->cols[counter].note);
      }
      /* pstr += sprintf(pstr, "%s", tableDes->cols[counter].note); */
    }

    /* if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
     * strcasecmp(tableDes->cols[counter].type, "nchar") == 0) { */
    /*     pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length); */
    /* } */
  }

  pstr += sprintf(pstr, ")");

  fprintf(fp, "%s\n\n", buffer);
}

int taosGetTableDes(char *table, STableDef *tableDes) {
  TAOS_ROW row = NULL;
  int count = 0;

  sprintf(command, "describe %s", table);
  if (taos_query(taos, command) != 0) {
    fprintf(stderr, "failed to run command %s\n", command);
    return -1;
  }

  result = taos_use_result(taos);
  if (result == NULL) {
    fprintf(stderr, "failed to use result\n");
    return -1;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);

  strcpy(tableDes->name, table);

  while ((row = taos_fetch_row(result)) != NULL) {
    strncpy(tableDes->cols[count].field, (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
            fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes);
    strncpy(tableDes->cols[count].type, (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
            fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes);
    tableDes->cols[count].length = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
    strncpy(tableDes->cols[count].note, (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
            fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes);

    count++;
  }

  taos_free_result(result);
  result = NULL;

  return count;
}

int32_t taosDumpTable(char *table, char *metric, struct arguments *arguments, FILE *fp) {
  int count = 0;

  STableDef *tableDes = (STableDef *)calloc(1, sizeof(STableDef) + sizeof(SColDes) * TSDB_MAX_COLUMNS);

  if (metric != NULL && metric[0] != '\0') {  // dump metric definition
    count = taosGetTableDes(metric, tableDes);

    if (count < 0) return -1;

    taosDumpCreateTableClause(tableDes, count, arguments, fp);

    memset(tableDes, 0, sizeof(STableDef) + sizeof(SColDes) * TSDB_MAX_COLUMNS);

    count = taosGetTableDes(table, tableDes);

    if (count < 0) return -1;

    taosDumpCreateMTableClause(tableDes, metric, count, arguments, fp);

  } else {  // dump table definition
    count = taosGetTableDes(table, tableDes);

    if (count < 0) return -1;

    taosDumpCreateTableClause(tableDes, count, arguments, fp);
  }

  return taosDumpTableData(fp, table, arguments);
}

int32_t taosDumpMetric(char *metric, struct arguments *arguments, FILE *fp) {
  TAOS_ROW row = NULL;
  int fd = -1;
  STableRecord tableRecord;

  strcpy(tableRecord.metric, metric);

  sprintf(command, "select tbname from %s", metric);
  if (taos_query(taos, command) != 0) {
    fprintf(stderr, "failed to run command %s\n", command);
    return -1;
  }

  result = taos_use_result(taos);
  if (result == NULL) {
    fprintf(stderr, "failed to use result\n");
    return -1;
  }

  fd = open(".table.tmp", O_RDWR | O_CREAT, 0755);
  if (fd < 0) {
    fprintf(stderr, "failed to open temp file");
    return -1;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result)) != NULL) {
    memset(&tableRecord, 0, sizeof(STableRecord));
    strncpy(tableRecord.name, (char *)row[0], fields[0].bytes);
    strcpy(tableRecord.metric, metric);
    write(fd, &tableRecord, sizeof(STableRecord));
  }

  taos_free_result(result);
  result = NULL;

  lseek(fd, 0, SEEK_SET);

  while (read(fd, &tableRecord, sizeof(STableRecord)) > 0) {
    taosDumpTable(tableRecord.name, tableRecord.metric, arguments, fp);
  }

  tclose(fd);
  remove(".table.tmp");

  return 0;
}

int taosDumpTableData(FILE *fp, char *tbname, struct arguments *arguments) {
  /* char       temp[MAX_COMMAND_SIZE] = "\0"; */
  int count = 0;
  char *pstr = NULL;
  TAOS_ROW row = NULL;
  int numFields = 0;
  int lstr = 0;

  if (arguments->schemaonly) return 0;

  sprintf(command, "select * from %s where _c0 >= %ld and _c0 <= %ld order by _c0 asc", tbname, arguments->start_time,
          arguments->end_time);
  if (taos_query(taos, command) != 0) {
    fprintf(stderr, "failed to run command %s, reason: %s\n", command, taos_errstr(taos));
    return -1;
  }

  result = taos_use_result(taos);
  if (result == NULL) {
    fprintf(stderr, "failed to use result\n");
    return -1;
  }

  numFields = taos_field_count(taos);
  assert(numFields > 0);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  count = 0;
  while ((row = taos_fetch_row(result)) != NULL) {
    pstr = buffer;

    if (count == 0) {
      pstr += sprintf(pstr, "INSERT INTO %s VALUES (", tbname);
    } else {
      pstr += sprintf(pstr, "(");
    }

    for (int col = 0; col < numFields; col++) {
      if (col != 0) pstr += sprintf(pstr, ", ");

      if (row[col] == NULL) {
        pstr += sprintf(pstr, "NULL");
        continue;
      }

      switch (fields[col].type) {
        case TSDB_DATA_TYPE_BOOL:
          pstr += sprintf(pstr, "%d", ((((int)(*((char *)row[col]))) == 1) ? 1 : 0));
          break;
        case TSDB_DATA_TYPE_TINYINT:
          pstr += sprintf(pstr, "%d", (int)(*((char *)row[col])));
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          pstr += sprintf(pstr, "%d", (int)(*((short *)row[col])));
          break;
        case TSDB_DATA_TYPE_INT:
          pstr += sprintf(pstr, "%d", *((int *)row[col]));
          break;
        case TSDB_DATA_TYPE_BIGINT:
          pstr += sprintf(pstr, "%ld", *((int64_t *)row[col]));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          pstr += sprintf(pstr, "%f", *((float *)row[col]));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          pstr += sprintf(pstr, "%f", *((double *)row[col]));
          break;
        case TSDB_DATA_TYPE_BINARY:
          *(pstr++) = '\'';
          for (lstr = 0; lstr < fields[col].bytes; lstr++) {
            if (((char *)row[col])[lstr] == '\0') break;
            *(pstr++) = ((char *)row[col])[lstr];
          }
          *(pstr++) = '\'';
          break;
        case TSDB_DATA_TYPE_NCHAR:
          pstr += sprintf(pstr, "\'%s\'", (char *)row[col]);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          pstr += sprintf(pstr, "%ld", *(int64_t *)row[col]);
          break;
        default:
          break;
      }
    }
    pstr += sprintf(pstr, ")");

    count++;
    fprintf(fp, "%s", buffer);

    if (count >= arguments->data_batch) {
      fprintf(fp, "\n");
      count = 0;
    } else {
      fprintf(fp, "\\\n");
    }
  }

  fprintf(fp, "\n");

  taos_free_result(result);
  result = NULL;
  return 0;
}

int taosCheckParam(struct arguments *arguments) {
  if (arguments->all_databases && arguments->databases) {
    fprintf(stderr, "conflict option --all-databases and --databases\n");
    return -1;
  }

  if (arguments->start_time > arguments->end_time) {
    fprintf(stderr, "start time is larger than end time\n");
    return -1;
  }
  if (arguments->arg_list_len == 0) {
    if ((!arguments->all_databases) && (!arguments->isDumpIn)) {
      fprintf(stderr, "taosdump requirs parameters\n");
      return -1;
    }
  }

  if (arguments->isDumpIn && (strcmp(arguments->output, DEFAULT_DUMP_FILE) != 0)) {
    fprintf(stderr, "duplicate parameter input and output file\n");
    return -1;
  }

  return 0;
}

bool isEmptyCommand(char *cmd) {
  char *pchar = cmd;

  while (*pchar != '\0') {
    if (*pchar != ' ') return false;
    pchar++;
  }

  return true;
}

int taosDumpIn(struct arguments *arguments) {
  assert(arguments->isDumpIn);

  int tsize = 0;
  FILE *fp = NULL;
  char *line = NULL;
  bool isRun = true;
  size_t line_size = 0;
  char *pstr = NULL;

  fp = fopen(arguments->input, "r");
  if (fp == NULL) {
    fprintf(stderr, "failed to open input file %s\n", arguments->input);
    return -1;
  }

  taos = taos_connect(arguments->host, arguments->user, arguments->password, NULL, arguments->port);
  if (taos == NULL) {
    fprintf(stderr, "failed to connect to TDEngine server\n");
    goto _dumpin_exit_failure;
  }

  command = (char *)malloc(COMMAND_SIZE);
  if (command == NULL) {
    fprintf(stderr, "failed to connect to allocate memory\n");
    goto _dumpin_exit_failure;
  }

  pstr = command;
  while (1) {
    ssize_t size = getline(&line, &line_size, fp);
    if (size <= 0) break;
    if (size == 1) {
      if (pstr != command) {
        if (taos_query(taos, command) != 0)
          fprintf(stderr, "failed to run command %s reason:%s \ncontinue...\n", command, taos_errstr(taos));

        pstr = command;
        pstr[0] = '\0';
        tsize = 0;
        isRun = true;
      }

      continue;
    }

    /* if (line[0] == '-' && line[1] == '-') continue; */

    line[size - 1] = 0;

    if (tsize + size - 1 > COMMAND_SIZE) {
      fprintf(stderr, "command is too long\n");
      goto _dumpin_exit_failure;
    }

    if (line[size - 2] == '\\') {
      line[size - 2] = ' ';
      isRun = false;
    } else {
      isRun = true;
    }

    memcpy(pstr, line, size - 1);
    pstr += (size - 1);
    *pstr = '\0';

    if (!isRun) continue;

    if (command != pstr && !isEmptyCommand(command)) {
      if (taos_query(taos, command) != 0)
        fprintf(stderr, "failed to run command %s reason: %s \ncontinue...\n", command, taos_errstr(taos));
    }

    pstr = command;
    *pstr = '\0';
    tsize = 0;
  }

  if (pstr != command) {
    if (taos_query(taos, command) != 0)
      fprintf(stderr, "failed to run command %s reason:%s \ncontinue...\n", command, taos_errstr(taos));
  }

  tfree(line);
  tfree(command);
  taos_close(taos);
  fclose(fp);
  return 0;

  _dumpin_exit_failure:
  tfree(command);
  taos_close(taos);
  fclose(fp);
  return -1;
}
