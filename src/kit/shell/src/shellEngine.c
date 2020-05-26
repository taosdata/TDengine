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

#define _BSD_SOURCE
#define _GNU_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "os.h"
#include "shell.h"
#include "shellCommand.h"
#include "ttime.h"
#include "tutil.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tglobal.h"
#include <regex.h>

/**************** Global variables ****************/
char      CLIENT_VERSION[] = "Welcome to the TDengine shell from %s, Client Version:%s\n"
                             "Copyright (c) 2017 by TAOS Data, Inc. All rights reserved.\n\n";
char      PROMPT_HEADER[] = "taos> ";
char      CONTINUE_PROMPT[] = "   -> ";
int       prompt_size = 6;
TAOS_RES *result = NULL;
SShellHistory   history;

#define DEFAULT_MAX_BINARY_DISPLAY_WIDTH 30
extern int32_t tsMaxBinaryDisplayWidth;

/*
 * FUNCTION: Initialize the shell.
 */
TAOS *shellInit(SShellArguments *args) {
  printf("\n");
  printf(CLIENT_VERSION, tsOsName, taos_get_client_info());
  fflush(stdout);

  // set options before initializing
  if (args->timezone != NULL) {
    taos_options(TSDB_OPTION_TIMEZONE, args->timezone);
  }

  if (args->is_use_passwd) {
    if (args->password == NULL) args->password = getpass("Enter password: ");
  } else {
    args->password = tsDefaultPass;
  }

  if (args->user == NULL) {
    args->user = tsDefaultUser;
  }

  taos_init();
  /*
   * set tsTableMetaKeepTimer = 3000ms
   * means not save cache in shell
   */
  tsTableMetaKeepTimer = 3000;

  // Connect to the database.
  TAOS *con = taos_connect(args->host, args->user, args->password, args->database, args->port);
  if (con == NULL) {
    return con;
  }

  /* Read history TODO : release resources here*/
  read_history();

  // Check if it is temperory run
  if (args->commands != NULL || args->file[0] != 0) {
    if (args->commands != NULL) {
      char *token;
      token = strtok(args->commands, ";");
      while (token != NULL) {
        printf("%s%s\n", PROMPT_HEADER, token);
        shellRunCommand(con, token);
        token = strtok(NULL, ";");
      }
    }

    if (args->file[0] != 0) {
      source_file(con, args->file);
    }
    taos_close(con);

    write_history();
    exit(EXIT_SUCCESS);
  }

#ifndef WINDOWS
  if (args->dir[0] != 0) {
    source_dir(con, args);
    taos_close(con);
    exit(EXIT_SUCCESS);
  }
#endif

  return con;
}

void shellReplaceCtrlChar(char *str) {
  _Bool ctrlOn = false;
  char *pstr = NULL;
  char  quote = 0;

  for (pstr = str; *str != '\0'; ++str) {
    if (ctrlOn) {
      switch (*str) {
        case 'n':
          *pstr = '\n';
          pstr++;
          break;
        case 'r':
          *pstr = '\r';
          pstr++;
          break;
        case 't':
          *pstr = '\t';
          pstr++;
          break;
        case 'G':
          *pstr++ = '\\';
          *pstr++ = *str;
          break;
        case '\\':
          *pstr = '\\';
          pstr++;
          break;
        case '\'':
        case '"':
          if (quote) {
            *pstr++ = '\\';
            *pstr++ = *str;
          }
          break;
        default:
          *pstr = *str;
          pstr++;
          break;
      }
      ctrlOn = false;
    } else {
      if (*str == '\\') {
        ctrlOn = true;
      } else {
        if (quote == *str) {
          quote = 0;
        } else if (*str == '\'' || *str == '"') {
          quote = *str;
        }
        *pstr = *str;
        pstr++;
      }
    }
  }
  *pstr = '\0';
}

int32_t shellRunCommand(TAOS *con, char *command) {
  /* If command is empty just return */
  if (regex_match(command, "^[ \t;]*$", REG_EXTENDED)) {
    return 0;
  }

  /* Update the history vector. */
  if (history.hstart == history.hend ||
      history.hist[(history.hend + MAX_HISTORY_SIZE - 1) % MAX_HISTORY_SIZE] == NULL ||
      strcmp(command, history.hist[(history.hend + MAX_HISTORY_SIZE - 1) % MAX_HISTORY_SIZE]) != 0) {
    if (history.hist[history.hend] != NULL) {
      tfree(history.hist[history.hend]);
    }
    history.hist[history.hend] = strdup(command);

    history.hend = (history.hend + 1) % MAX_HISTORY_SIZE;
    if (history.hend == history.hstart) {
      history.hstart = (history.hstart + 1) % MAX_HISTORY_SIZE;
    }
  }

  shellReplaceCtrlChar(command);

  // Analyse the command.
  if (regex_match(command, "^[ \t]*(quit|q|exit)[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
    taos_close(con);
    write_history();
    return -1;
  } else if (regex_match(command, "^[\t ]*clear[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
    // If clear the screen.
    system("clear");
  } else if (regex_match(command, "^[\t ]*set[ \t]+max_binary_display_width[ \t]+(default|[1-9][0-9]*)[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
    strtok(command, " \t");
    strtok(NULL, " \t");
    char* p = strtok(NULL, " \t");
    if (strcasecmp(p, "default") == 0) {
      tsMaxBinaryDisplayWidth = DEFAULT_MAX_BINARY_DISPLAY_WIDTH;
    } else {
      tsMaxBinaryDisplayWidth = atoi(p);
    }
  } else if (regex_match(command, "^[ \t]*source[\t ]+[^ ]+[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
    /* If source file. */
    char *c_ptr = strtok(command, " ;");
    assert(c_ptr != NULL);
    c_ptr = strtok(NULL, " ;");
    assert(c_ptr != NULL);

    source_file(con, c_ptr);
  } else {
    shellRunCommandOnServer(con, command);
  }
  
  return 0;
}

void shellRunCommandOnServer(TAOS *con, char command[]) {
  int64_t   st, et;
  wordexp_t full_path;
  char *    sptr = NULL;
  char *    cptr = NULL;
  char *    fname = NULL;
  bool      printMode = false;

  if ((sptr = strstr(command, ">>")) != NULL) {
    cptr = strstr(command, ";");
    if (cptr != NULL) {
      *cptr = '\0';
    }

    if (wordexp(sptr + 2, &full_path, 0) != 0) {
      fprintf(stderr, "ERROR: invalid filename: %s\n", sptr + 2);
      return;
    }
    *sptr = '\0';
    fname = full_path.we_wordv[0];
  }

  if ((sptr = strstr(command, "\\G")) != NULL) {
    cptr = strstr(command, ";");
    if (cptr != NULL) {
      *cptr = '\0';
    }

    *sptr = '\0';
    printMode = true;  // When output to a file, the switch does not work.
  }

  st = taosGetTimestampUs();

  if (taos_query(con, command)) {
    taos_error(con);
    return;
  }

  if (regex_match(command, "^\\s*use\\s+[a-zA-Z0-9_]+\\s*;\\s*$", REG_EXTENDED | REG_ICASE)) {
    fprintf(stdout, "Database changed.\n\n");
    fflush(stdout);
    return;
  }

  int num_fields = taos_field_count(con);
  if (num_fields != 0) {  // select and show kinds of commands
    int error_no = 0;
    int numOfRows = shellDumpResult(con, fname, &error_no, printMode);
    if (numOfRows < 0) return;

    et = taosGetTimestampUs();
    if (error_no == 0) {
      printf("Query OK, %d row(s) in set (%.6fs)\n", numOfRows, (et - st) / 1E6);
    } else {
      printf("Query interrupted (%s), %d row(s) in set (%.6fs)\n", taos_errstr(con), numOfRows, (et - st) / 1E6);
    }
  } else {
    int num_rows_affacted = taos_affected_rows(con);
    et = taosGetTimestampUs();
    printf("Query OK, %d row(s) affected (%.6fs)\n", num_rows_affacted, (et - st) / 1E6);
  }

  printf("\n");

  if (fname != NULL) {
    wordfree(&full_path);
  }
}

/* Function to do regular expression check */
int regex_match(const char *s, const char *reg, int cflags) {
  regex_t regex;
  char    msgbuf[100] = {0};

  /* Compile regular expression */
  if (regcomp(&regex, reg, cflags) != 0) {
    fprintf(stderr, "Fail to compile regex");
    exitShell();
  }

  /* Execute regular expression */
  int reti = regexec(&regex, s, 0, NULL, 0);
  if (!reti) {
    regfree(&regex);
    return 1;
  } else if (reti == REG_NOMATCH) {
    regfree(&regex);
    return 0;
  } else {
    regerror(reti, &regex, msgbuf, sizeof(msgbuf));
    fprintf(stderr, "Regex match failed: %s\n", msgbuf);
    regfree(&regex);
    exitShell();
  }

  return 0;
}


static char* formatTimestamp(char* buf, int64_t val, int precision) {
  if (args.is_raw_time) {
    sprintf(buf, "%" PRId64, val);
    return buf;
  }

  time_t tt;
  if (precision == TSDB_TIME_PRECISION_MICRO) {
    tt = (time_t)(val / 1000000);
  } else {
    tt = (time_t)(val / 1000);
  }

  struct tm* ptm = localtime(&tt);
  size_t pos = strftime(buf, 32, "%Y-%m-%d %H:%M:%S", ptm);

  if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", (int)(val % 1000000));
  } else {
    sprintf(buf + pos, ".%03d", (int)(val % 1000));
  }

  return buf;
}


static void dumpFieldToFile(FILE* fp, const char* val, TAOS_FIELD* field, int32_t length, int precision) {
  if (val == NULL) {
    fprintf(fp, "%s", TSDB_DATA_NULL_STR);
    return;
  }

  char buf[TSDB_MAX_BYTES_PER_ROW];
  switch (field->type) {
    case TSDB_DATA_TYPE_BOOL:
      fprintf(fp, "%d", ((((int)(*((char *)val))) == 1) ? 1 : 0));
      break;
    case TSDB_DATA_TYPE_TINYINT:
      fprintf(fp, "%d", (int)(*((char *)val)));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      fprintf(fp, "%d", (int)(*((short *)val)));
      break;
    case TSDB_DATA_TYPE_INT:
      fprintf(fp, "%d", *((int *)val));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      fprintf(fp, "%" PRId64, *((int64_t *)val));
      break;
    case TSDB_DATA_TYPE_FLOAT:
      fprintf(fp, "%.5f", GET_FLOAT_VAL(val));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      fprintf(fp, "%.9f", GET_DOUBLE_VAL(val));
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      memcpy(buf, val, length);
      buf[length] = 0;
      fprintf(fp, "\'%s\'", buf);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      formatTimestamp(buf, *(int64_t*)val, precision);
      fprintf(fp, "'%s'", buf);
      break;
    default:
      break;
  }
}

static int dumpResultToFile(const char* fname, TAOS_RES* result) {
  TAOS_ROW row = taos_fetch_row(result);
  if (row == NULL) {
    return 0;
  }

  wordexp_t full_path;

  if (wordexp(fname, &full_path, 0) != 0) {
    fprintf(stderr, "ERROR: invalid file name: %s\n", fname);
    return -1;
  }

  FILE* fp = fopen(full_path.we_wordv[0], "w");
  if (fp == NULL) {
    fprintf(stderr, "ERROR: failed to open file: %s\n", full_path.we_wordv[0]);
    wordfree(&full_path);
    return -1;
  }

  wordfree(&full_path);

  int num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  int32_t* length = taos_fetch_lengths(result);
  int precision = taos_result_precision(result);

  for (int col = 0; col < num_fields; col++) {
    if (col > 0) {
      fprintf(fp, ",");
    }
    fprintf(fp, "%s", fields[col].name);
  }
  fputc('\n', fp);
  
  int numOfRows = 0;
  do {
    for (int i = 0; i < num_fields; i++) {
      if (i > 0) {
        fputc(',', fp);
      }
      dumpFieldToFile(fp, row[i], fields +i, length[i], precision);
    }
    fputc('\n', fp);

    numOfRows++;
    row = taos_fetch_row(result);
  } while( row != NULL);

  fclose(fp);
  return numOfRows;
}


static void printField(const char* val, TAOS_FIELD* field, int width, int32_t length, int precision) {
  if (val == NULL) {
    int w = width;
    if (field->type < TSDB_DATA_TYPE_TINYINT || field->type > TSDB_DATA_TYPE_DOUBLE) {
      w = 0;
    }
    w = printf("%*s", w, TSDB_DATA_NULL_STR);
    for (; w < width; w++) {
      putchar(' ');
    }
    return;
  }

  char buf[TSDB_MAX_BYTES_PER_ROW];
  switch (field->type) {
    case TSDB_DATA_TYPE_BOOL:
      printf("%*s", width, ((((int)(*((char *)val))) == 1) ? "true" : "false"));
      break;
    case TSDB_DATA_TYPE_TINYINT:
      printf("%*d", width, (int)(*((char *)val)));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      printf("%*d", width, (int)(*((short *)val)));
      break;
    case TSDB_DATA_TYPE_INT:
      printf("%*d", width, *((int *)val));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      printf("%*" PRId64, width, *((int64_t *)val));
      break;
    case TSDB_DATA_TYPE_FLOAT:
      printf("%*.5f", width, GET_FLOAT_VAL(val));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      printf("%*.9f", width, GET_DOUBLE_VAL(val));
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      shellPrintNChar(val, length, width);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      formatTimestamp(buf, *(int64_t*)val, precision);
      printf("%s", buf);
      break;
    default:
      break;
  }
}


static int verticalPrintResult(TAOS_RES* result) {
  TAOS_ROW row = taos_fetch_row(result);
  if (row == NULL) {
    return 0;
  }

  int num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  int32_t* length = taos_fetch_lengths(result);
  int precision = taos_result_precision(result);

  int maxColNameLen = 0;
  for (int col = 0; col < num_fields; col++) {
    int len = strlen(fields[col].name);
    if (len > maxColNameLen) {
      maxColNameLen = len;
    }
  }

  int numOfRows = 0;
  do {
    printf("*************************** %d.row ***************************\n", numOfRows + 1);
    for (int i = 0; i < num_fields; i++) {
      TAOS_FIELD* field = fields + i;

      int padding = (int)(maxColNameLen - strlen(field->name));
      printf("%*.s%s: ", padding, " ", field->name);

      printField(row[i], field, 0, length[i], precision);
      putchar('\n');
    }

    numOfRows++;
    row = taos_fetch_row(result);
  } while(row != NULL);

  return numOfRows;
}


static int calcColWidth(TAOS_FIELD* field, int precision) {
  int width = strlen(field->name);

  switch (field->type) {
    case TSDB_DATA_TYPE_BOOL:
      return MAX(5, width); // 'false'

    case TSDB_DATA_TYPE_TINYINT:
      return MAX(4, width); // '-127'

    case TSDB_DATA_TYPE_SMALLINT:
      return MAX(6, width); // '-32767'

    case TSDB_DATA_TYPE_INT:
      return MAX(11, width); // '-2147483648'

    case TSDB_DATA_TYPE_BIGINT:
      return MAX(21, width); // '-9223372036854775807'

    case TSDB_DATA_TYPE_FLOAT:
      return MAX(20, width);

    case TSDB_DATA_TYPE_DOUBLE:
      return MAX(25, width);

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      if (field->bytes > tsMaxBinaryDisplayWidth) {
        return MAX(tsMaxBinaryDisplayWidth, width);
      } else {
        return MAX(field->bytes, width);
      }

    case TSDB_DATA_TYPE_TIMESTAMP:
      if (args.is_raw_time) {
        return MAX(14, width);
      } else if (precision == TSDB_TIME_PRECISION_MICRO) {
        return MAX(26, width); // '2020-01-01 00:00:00.000000'
      } else {
        return MAX(23, width); // '2020-01-01 00:00:00.000'
      }

    default:
      assert(false);
  }

  return 0;
}


static void printHeader(TAOS_FIELD* fields, int* width, int num_fields) {
  int rowWidth = 0;
  for (int col = 0; col < num_fields; col++) {
    TAOS_FIELD* field = fields + col;
    int padding = (int)(width[col] - strlen(field->name));
    int left = padding / 2;
    printf(" %*.s%s%*.s |", left, " ", field->name, padding - left, " ");
    rowWidth += width[col] + 3;
  }

  putchar('\n');
  for (int i = 0; i < rowWidth; i++) {
    putchar('=');
  }
  putchar('\n');
}


static int horizontalPrintResult(TAOS_RES* result) {
  TAOS_ROW row = taos_fetch_row(result);
  if (row == NULL) {
    return 0;
  }

  int num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  int32_t* length = taos_fetch_lengths(result);
  int precision = taos_result_precision(result);

  int width[TSDB_MAX_COLUMNS];
  for (int col = 0; col < num_fields; col++) {
    width[col] = calcColWidth(fields + col, precision);
  }

  printHeader(fields, width, num_fields);

  int numOfRows = 0;
  do {
    for (int i = 0; i < num_fields; i++) {
      putchar(' ');
      printField(row[i], fields + i, width[i], length[i], precision);
      putchar(' ');
      putchar('|');
    }
    putchar('\n');
    numOfRows++;
    row = taos_fetch_row(result);
  } while(row != NULL);

  return numOfRows;
}


int shellDumpResult(TAOS *con, char *fname, int *error_no, bool vertical) {
  int numOfRows = 0;

  TAOS_RES* result = taos_use_result(con);
  if (result == NULL) {
    taos_error(con);
    return -1;
  }

  if (fname != NULL) {
    numOfRows = dumpResultToFile(fname, result);
  } else if(vertical) {
    numOfRows = verticalPrintResult(result);
  } else {
    numOfRows = horizontalPrintResult(result);
  }

  *error_no = taos_errno(con);
  taos_free_result(result);
  return numOfRows;
}


void read_history() {
  // Initialize history
  memset(history.hist, 0, sizeof(char *) * MAX_HISTORY_SIZE);
  history.hstart = 0;
  history.hend = 0;
  char * line = NULL;
  size_t line_size = 0;
  int    read_size = 0;

  char f_history[TSDB_FILENAME_LEN];
  get_history_path(f_history);

  if (access(f_history, R_OK) == -1) {
    return;
  }

  FILE *f = fopen(f_history, "r");
  if (f == NULL) {
    fprintf(stderr, "Opening file %s\n", f_history);
    return;
  }

  while ((read_size = getline(&line, &line_size, f)) != -1) {
    line[read_size - 1] = '\0';
    history.hist[history.hend] = strdup(line);

    history.hend = (history.hend + 1) % MAX_HISTORY_SIZE;

    if (history.hend == history.hstart) {
      history.hstart = (history.hstart + 1) % MAX_HISTORY_SIZE;
    }
  }

  free(line);
  fclose(f);
}

void write_history() {
  char f_history[128];
  get_history_path(f_history);

  FILE *f = fopen(f_history, "w");
  if (f == NULL) {
    fprintf(stderr, "Opening file %s\n", f_history);
    return;
  }

  for (int i = history.hstart; i != history.hend;) {
    if (history.hist[i] != NULL) {
      fprintf(f, "%s\n", history.hist[i]);
      tfree(history.hist[i]);
    }
    i = (i + 1) % MAX_HISTORY_SIZE;
  }
  fclose(f);
}

void taos_error(TAOS *con) {
  fprintf(stderr, "\nDB error: %s\n", taos_errstr(con));

  /* free local resouce: allocated memory/metric-meta refcnt */
  TAOS_RES *pRes = taos_use_result(con);
  taos_free_result(pRes);
}

int isCommentLine(char *line) {
  if (line == NULL) return 1;

  return regex_match(line, "^\\s*#.*", REG_EXTENDED);
}

void source_file(TAOS *con, char *fptr) {
  wordexp_t full_path;
  int       read_len = 0;
  char *    cmd = calloc(1, MAX_COMMAND_SIZE);
  size_t    cmd_len = 0;
  char *    line = NULL;
  size_t    line_len = 0;

  if (wordexp(fptr, &full_path, 0) != 0) {
    fprintf(stderr, "ERROR: illegal file name\n");
    free(cmd);
    return;
  }

  char *fname = full_path.we_wordv[0];

  if (access(fname, F_OK) != 0) {
    fprintf(stderr, "ERROR: file %s is not exist\n", fptr);
    
    wordfree(&full_path);
    free(cmd);
    return;
  }
  
  if (access(fname, R_OK) != 0) {
    fprintf(stderr, "ERROR: file %s is not readable\n", fptr);
  
    wordfree(&full_path);
    free(cmd);
    return;
  }

  FILE *f = fopen(fname, "r");
  if (f == NULL) {
    fprintf(stderr, "ERROR: failed to open file %s\n", fname);
    wordfree(&full_path);
    free(cmd);
    return;
  }

  while ((read_len = getline(&line, &line_len, f)) != -1) {
    if (read_len >= MAX_COMMAND_SIZE) continue;
    line[--read_len] = '\0';

    if (read_len == 0 || isCommentLine(line)) {  // line starts with #
      continue;
    }

    if (line[read_len - 1] == '\\') {
      line[read_len - 1] = ' ';
      memcpy(cmd + cmd_len, line, read_len);
      cmd_len += read_len;
      continue;
    }

    memcpy(cmd + cmd_len, line, read_len);
    printf("%s%s\n", PROMPT_HEADER, cmd);
    shellRunCommand(con, cmd);
    memset(cmd, 0, MAX_COMMAND_SIZE);
    cmd_len = 0;
  }

  free(cmd);
  if (line) free(line);
  wordfree(&full_path);
  fclose(f);
}

void shellGetGrantInfo(void *con) {
  return;

  char sql[] = "show grants";

  int code = taos_query(con, sql);

  if (code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_OPS_NOT_SUPPORT) {
      fprintf(stdout, "Server is Community Edition, version is %s\n\n", taos_get_server_info(con));
    } else {
      fprintf(stderr, "Failed to check Server Edition, Reason:%d:%s\n\n", taos_errno(con), taos_errstr(con));
    }
    return;
  }

  int num_fields = taos_field_count(con);
  if (num_fields == 0) {
    fprintf(stderr, "\nInvalid grant information.\n");
    exit(0);
  } else {
    result = taos_use_result(con);
    if (result == NULL) {
      fprintf(stderr, "\nGrant information is null.\n");
      exit(0);
    }

    TAOS_FIELD *fields = taos_fetch_fields(result);
    TAOS_ROW row = taos_fetch_row(result);
    if (row == NULL) {
      fprintf(stderr, "\nFailed to get grant information from server. Abort.\n");
      exit(0);
    }

    char serverVersion[32] = {0};
    char expiretime[32] = {0};
    char expired[32] = {0};

    memcpy(serverVersion, row[0], fields[0].bytes);
    memcpy(expiretime, row[1], fields[1].bytes);
    memcpy(expired, row[2], fields[2].bytes);

    if (strcmp(expiretime, "unlimited") == 0) {
      fprintf(stdout, "Server is Enterprise %s Edition, version is %s and will never expire.\n", serverVersion, taos_get_server_info(con));
    } else {
      fprintf(stdout, "Server is Enterprise %s Edition, version is %s and will expire at %s.\n", serverVersion, taos_get_server_info(con), expiretime);
    }

    taos_free_result(result);
    result = NULL;
  }

  fprintf(stdout, "\n");
}
