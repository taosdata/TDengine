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
History   history;

/*
 * FUNCTION: Initialize the shell.
 */
TAOS *shellInit(struct arguments *args) {
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
    return 0;
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

int shellDumpResult(TAOS *con, char *fname, int *error_no, bool printMode) {
  TAOS_ROW   row = NULL;
  int        numOfRows = 0;
  time_t     tt;
  char       buf[25] = "\0";
  struct tm *ptm;
  int        output_bytes = 0;
  FILE *     fp = NULL;
  int        num_fields = taos_field_count(con);
  wordexp_t  full_path;

  assert(num_fields != 0);

  result = taos_use_result(con);
  if (result == NULL) {
    taos_error(con);
    return -1;
  }

  if (fname != NULL) {
    if (wordexp(fname, &full_path, 0) != 0) {
      fprintf(stderr, "ERROR: invalid file name: %s\n", fname);
      return -1;
    }

    fp = fopen(full_path.we_wordv[0], "w");
    if (fp == NULL) {
      fprintf(stderr, "ERROR: failed to open file: %s\n", full_path.we_wordv[0]);
      wordfree(&full_path);
      return -1;
    }

    wordfree(&full_path);
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);

  row = taos_fetch_row(result);
  int32_t* length = taos_fetch_lengths(result);
  
  char t_str[TSDB_MAX_BYTES_PER_ROW] = "\0";
  int  l[TSDB_MAX_COLUMNS] = {0};
  int  maxLenColumnName = 0;

  if (row) {
    // Print the header indicator
    if (fname == NULL) {  // print to standard output
      if (!printMode) {
        for (int col = 0; col < num_fields; col++) {
          switch (fields[col].type) {
            case TSDB_DATA_TYPE_BOOL:
              l[col] = MAX(BOOL_OUTPUT_LENGTH, strlen(fields[col].name));
              break;
            case TSDB_DATA_TYPE_TINYINT:
              l[col] = MAX(TINYINT_OUTPUT_LENGTH, strlen(fields[col].name));
              break;
            case TSDB_DATA_TYPE_SMALLINT:
              l[col] = MAX(SMALLINT_OUTPUT_LENGTH, strlen(fields[col].name));
              break;
            case TSDB_DATA_TYPE_INT:
              l[col] = MAX(INT_OUTPUT_LENGTH, strlen(fields[col].name));
              break;
            case TSDB_DATA_TYPE_BIGINT:
              l[col] = MAX(BIGINT_OUTPUT_LENGTH, strlen(fields[col].name));
              break;
            case TSDB_DATA_TYPE_FLOAT:
              l[col] = MAX(FLOAT_OUTPUT_LENGTH, strlen(fields[col].name));
              break;
            case TSDB_DATA_TYPE_DOUBLE:
              l[col] = MAX(DOUBLE_OUTPUT_LENGTH, strlen(fields[col].name));
              break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
              l[col] = MAX(fields[col].bytes, strlen(fields[col].name));
              /* l[col] = max(BINARY_OUTPUT_LENGTH, strlen(fields[col].name)); */
              break;
            case TSDB_DATA_TYPE_TIMESTAMP: {
              int32_t defaultWidth = TIMESTAMP_OUTPUT_LENGTH;
              if (args.is_raw_time) {
                defaultWidth = 14;
              }
              if (taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO) {
                defaultWidth += 3;
              }
              l[col] = MAX(defaultWidth, strlen(fields[col].name));

              break;
            }
            default:
              break;
          }

          int spaces = (int)(l[col] - strlen(fields[col].name));
          int left_space = spaces / 2;
          int right_space = (spaces % 2 ? left_space + 1 : left_space);
          printf("%*.s%s%*.s|", left_space, " ", fields[col].name, right_space, " ");
          output_bytes += (l[col] + 1);
        }
        printf("\n");
        for (int k = 0; k < output_bytes; k++) printf("=");
        printf("\n");
      } else {
        for (int col = 0; col < num_fields; col++) {
          if (strlen(fields[col].name) > maxLenColumnName) maxLenColumnName = strlen(fields[col].name);
        }
      }

      // print the elements
      do {
        if (!printMode) {
          for (int i = 0; i < num_fields; i++) {
            if (row[i] == NULL) {
              printf("%*s|", l[i], TSDB_DATA_NULL_STR);
              continue;
            }

            switch (fields[i].type) {
              case TSDB_DATA_TYPE_BOOL:
                printf("%*s|", l[i], ((((int)(*((char *)row[i]))) == 1) ? "true" : "false"));
                break;
              case TSDB_DATA_TYPE_TINYINT:
                printf("%*d|", l[i], (int)(*((char *)row[i])));
                break;
              case TSDB_DATA_TYPE_SMALLINT:
                printf("%*d|", l[i], (int)(*((short *)row[i])));
                break;
              case TSDB_DATA_TYPE_INT:
                printf("%*d|", l[i], *((int *)row[i]));
                break;
              case TSDB_DATA_TYPE_BIGINT:
                printf("%*" PRId64 "|", l[i], *((int64_t *)row[i]));
                break;
              case TSDB_DATA_TYPE_FLOAT: {
                float fv = 0;
                fv = GET_FLOAT_VAL(row[i]);
                printf("%*.5f|", l[i], fv);
              }              
                break;
              case TSDB_DATA_TYPE_DOUBLE: {
                double dv = 0;
                dv = GET_DOUBLE_VAL(row[i]);
                printf("%*.9f|", l[i], dv);
              }
                break;
              case TSDB_DATA_TYPE_BINARY:
              case TSDB_DATA_TYPE_NCHAR:
                memset(t_str, 0, TSDB_MAX_BYTES_PER_ROW);
                memcpy(t_str, row[i], length[i]);
                /* printf("%-*s|",max(fields[i].bytes, strlen(fields[i].name)),
                 * t_str); */
                /* printf("%-*s|", l[i], t_str); */
                shellPrintNChar(t_str, l[i], printMode);
                break;
              case TSDB_DATA_TYPE_TIMESTAMP:
                if (args.is_raw_time) {
                  printf(" %" PRId64 "|", *(int64_t *)row[i]);
                } else {
                  if (taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO) {
                    tt = (time_t)((*(int64_t *)row[i]) / 1000000);
                  } else {
                    tt = (time_t)((*(int64_t *)row[i]) / 1000);
                  }

                  ptm = localtime(&tt);
                  strftime(buf, 64, "%y-%m-%d %H:%M:%S", ptm);

                  if (taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO) {
                    printf(" %s.%06d|", buf, (int)(*(int64_t *)row[i] % 1000000));
                  } else {
                    printf(" %s.%03d|", buf, (int)(*(int64_t *)row[i] % 1000));
                  }
                }
                break;
              default:
                break;
            }
          }
          printf("\n");
        } else {
          printf("*************************** %d.row ***************************\n", numOfRows + 1);
          for (int i = 0; i < num_fields; i++) {
            // 1. print column name
            int left_space = (int)(maxLenColumnName - strlen(fields[i].name));
            printf("%*.s%s: ", left_space, " ", fields[i].name);

            // 2. print column value
            if (row[i] == NULL) {
              printf("%s\n", TSDB_DATA_NULL_STR);
              continue;
            }

            switch (fields[i].type) {
              case TSDB_DATA_TYPE_BOOL:
                printf("%s\n", ((((int)(*((char *)row[i]))) == 1) ? "true" : "false"));
                break;
              case TSDB_DATA_TYPE_TINYINT:
                printf("%d\n", (int)(*((char *)row[i])));
                break;
              case TSDB_DATA_TYPE_SMALLINT:
                printf("%d\n", (int)(*((short *)row[i])));
                break;
              case TSDB_DATA_TYPE_INT:
                printf("%d\n", *((int *)row[i]));
                break;
              case TSDB_DATA_TYPE_BIGINT:
                printf("%" PRId64 "\n", *((int64_t *)row[i]));
                break;
              case TSDB_DATA_TYPE_FLOAT: {
                float fv = 0;
                fv = GET_FLOAT_VAL(row[i]);
                printf("%.5f\n", fv);
              }              
                break;
              case TSDB_DATA_TYPE_DOUBLE: {
                double dv = 0;
                dv = GET_DOUBLE_VAL(row[i]);
                printf("%.9f\n", dv);
              }
                break;
              case TSDB_DATA_TYPE_BINARY:
              case TSDB_DATA_TYPE_NCHAR:
                memset(t_str, 0, TSDB_MAX_BYTES_PER_ROW);
                memcpy(t_str, row[i], length[i]);
                
                l[i] = MAX(fields[i].bytes, strlen(fields[i].name));
                shellPrintNChar(t_str, l[i], printMode);
                break;
              case TSDB_DATA_TYPE_TIMESTAMP:
                if (args.is_raw_time) {
                  printf("%" PRId64 "\n", *(int64_t *)row[i]);
                } else {
                  if (taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO) {
                    tt = (time_t)((*(int64_t *)row[i]) / 1000000);
                  } else {
                    tt = (time_t)((*(int64_t *)row[i]) / 1000);
                  }

                  ptm = localtime(&tt);
                  strftime(buf, 64, "%y-%m-%d %H:%M:%S", ptm);

                  if (taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO) {
                    printf("%s.%06d\n", buf, (int)(*(int64_t *)row[i] % 1000000));
                  } else {
                    printf("%s.%03d\n", buf, (int)(*(int64_t *)row[i] % 1000));
                  }
                }
                break;
              default:
                break;
            }
          }
        }

        numOfRows++;
      } while ((row = taos_fetch_row(result)));

    } else {  // dump to file
      // first write column
      for (int col = 0; col < num_fields; col++) {
        fprintf(fp, "%s", fields[col].name);
        if (col < num_fields - 1) {
          fprintf(fp, ",");
        } else {
          fprintf(fp, "\n");
        }
      }
      
      do {
        for (int i = 0; i < num_fields; i++) {
          if (row[i]) {
            switch (fields[i].type) {
              case TSDB_DATA_TYPE_BOOL:
                fprintf(fp, "%d", ((((int)(*((char *)row[i]))) == 1) ? 1 : 0));
                break;
              case TSDB_DATA_TYPE_TINYINT:
                fprintf(fp, "%d", (int)(*((char *)row[i])));
                break;
              case TSDB_DATA_TYPE_SMALLINT:
                fprintf(fp, "%d", (int)(*((short *)row[i])));
                break;
              case TSDB_DATA_TYPE_INT:
                fprintf(fp, "%d", *((int *)row[i]));
                break;
              case TSDB_DATA_TYPE_BIGINT:
                fprintf(fp, "%" PRId64, *((int64_t *)row[i]));
                break;
              case TSDB_DATA_TYPE_FLOAT: {
                float fv = 0;
                fv = GET_FLOAT_VAL(row[i]);
                fprintf(fp, "%.5f", fv);
              }              
                break;
              case TSDB_DATA_TYPE_DOUBLE: {
                double dv = 0;
                dv = GET_DOUBLE_VAL(row[i]);
                fprintf(fp, "%.9f", dv);
              }
                break;
              case TSDB_DATA_TYPE_BINARY:
              case TSDB_DATA_TYPE_NCHAR:
                memset(t_str, 0, TSDB_MAX_BYTES_PER_ROW);
                memcpy(t_str, row[i], length[i]);
                fprintf(fp, "\'%s\'", t_str);
                break;
              case TSDB_DATA_TYPE_TIMESTAMP:
                if (args.is_raw_time) {
                  fprintf(fp, "%" PRId64, *(int64_t *)row[i]);
                } else {
                  if (taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO) {
                    tt = (time_t)((*(int64_t *)row[i]) / 1000000);
                  } else {
                    tt = (time_t)((*(int64_t *)row[i]) / 1000);
                  }

                  ptm = localtime(&tt);
                  strftime(buf, 64, "%Y-%m-%d %H:%M:%S", ptm);

                  if (taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO) {
                    fprintf(fp, "\'%s.%06d\'", buf, (int)(*(int64_t *)row[i] % 1000000));
                  } else {
                    fprintf(fp, "\'%s.%03d\'", buf, (int)(*(int64_t *)row[i] % 1000));
                  }
                }
                break;
              default:
                break;
            }
          } else {
            fprintf(fp, "%s", TSDB_DATA_NULL_STR);
          }
          if (i < num_fields - 1) {
            fprintf(fp, ",");
          } else {
            fprintf(fp, "\n");
          }
        }

        numOfRows++;
      } while ((row = taos_fetch_row(result)));
    }
  }

  *error_no = taos_errno(con);

  taos_free_result(result);
  result = NULL;

  if (fname != NULL) {
    fclose(fp);
  }

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
