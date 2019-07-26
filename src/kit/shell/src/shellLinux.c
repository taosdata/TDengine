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
#include <assert.h>
#include <error.h>
#include <pwd.h>
#include <regex.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>

#define __USE_XOPEN

#include <wchar.h>

#include "shell.h"
#include "shellCommand.h"
#include "tkey.h"

#define OPT_ABORT 1 /* �Cabort */

int indicator = 1;
struct termios oldtio;

const char *argp_program_version = version;
const char *argp_program_bug_address = "<support@taosdata.com>";
static char doc[] = "";
static char args_doc[] = "";
static struct argp_option options[] = {
  {"host",       'h', "HOST",       0,                   "TDEngine server IP address to connect. The default host is localhost."},
  {"password",   'p', "PASSWORD",   OPTION_ARG_OPTIONAL, "The password to use when connecting to the server."},
  {"port",       'P', "PORT",       0,                   "The TCP/IP port number to use for the connection."},
  {"user",       'u', "USER",       0,                   "The TDEngine user name to use when connecting to the server."},
  {"config-dir", 'c', "CONFIG_DIR", 0,                   "Configuration directory."},
  {"commands",   's', "COMMANDS",   0,                   "Commands to run without enter the shell."},
  {"raw-time",   'r', 0,            0,                   "Output time as unsigned long."},
  {"file",       'f', "FILE",       0,                   "Script to run without enter the shell."},
  {"database",   'd', "DATABASE",   0,                   "Database to use when connecting to the server."},
  {"timezone",   't', "TIMEZONE",   0,                   "Time zone of the shell, default is local."},
  {0}};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  /* Get the input argument from argp_parse, which we
  know is a pointer to our arguments structure. */
  struct arguments *arguments = state->input;
  wordexp_t full_path;

  switch (key) {
    case 'h':
      arguments->host = arg;
      break;
    case 'p':
      arguments->is_use_passwd = true;
      if (arg) arguments->password = arg;
      break;
    case 'P':
      tsMgmtShellPort = atoi(arg);
      break;
    case 't':
      arguments->timezone = arg;
      break;
    case 'u':
      arguments->user = arg;
      break;
    case 'c':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      strcpy(configDir, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case 's':
      arguments->commands = arg;
      break;
    case 'r':
      arguments->is_raw_time = true;
      break;
    case 'f':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      strcpy(arguments->file, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case 'd':
      arguments->database = arg;
      break;
    case OPT_ABORT:
      arguments->abort = 1;
      break;
    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

/* Our argp parser. */
static struct argp argp = {options, parse_opt, args_doc, doc};

void shellParseArgument(int argc, char *argv[], struct arguments *arguments) {
  argp_parse(&argp, argc, argv, 0, 0, arguments);
  if (arguments->abort) {
    error(10, 0, "ABORTED");
  }
}

void shellReadCommand(TAOS *con, char *command) {
  unsigned hist_counter = history.hend;
  char utf8_array[10] = "\0";
  Command cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.buffer = (char *)calloc(1, MAX_COMMAND_SIZE);
  cmd.command = (char *)calloc(1, MAX_COMMAND_SIZE);
  showOnScreen(&cmd);

  // Read input.
  char c;
  while (1) {
    c = getchar();

    if (c < 0) {  // For UTF-8
      int count = countPrefixOnes(c);
      utf8_array[0] = c;
      for (int k = 1; k < count; k++) {
        c = getchar();
        utf8_array[k] = c;
      }
      insertChar(&cmd, utf8_array, count);
    } else if (c < '\033') {
      // Ctrl keys.  TODO: Implement ctrl combinations
      switch (c) {
        case 1:  // ctrl A
          positionCursorHome(&cmd);
          break;
        case 3:
          printf("\n");
          resetCommand(&cmd, "");
          kill(0, SIGINT);
          break;
        case 4:  // EOF or Ctrl+D
          printf("\n");
          taos_close(con);
          // write the history
          write_history();
          exitShell();
          break;
        case 5:  // ctrl E
          positionCursorEnd(&cmd);
          break;
        case 8:
          backspaceChar(&cmd);
          break;
        case '\n':
        case '\r':
          printf("\n");
          if (isReadyGo(&cmd)) {
            sprintf(command, "%s%s", cmd.buffer, cmd.command);
            tfree(cmd.buffer);
            tfree(cmd.command);
            return;
          } else {
            updateBuffer(&cmd);
          }
          break;
        case 12:  // Ctrl + L;
          system("clear");
          showOnScreen(&cmd);
          break;
      }
    } else if (c == '\033') {
      c = getchar();
      switch (c) {
        case '[':
          c = getchar();
          switch (c) {
            case 'A':  // Up arrow
              if (hist_counter != history.hstart) {
                hist_counter = (hist_counter + MAX_HISTORY_SIZE - 1) % MAX_HISTORY_SIZE;
                resetCommand(&cmd, (history.hist[hist_counter] == NULL) ? "" : history.hist[hist_counter]);
              }
              break;
            case 'B':  // Down arrow
              if (hist_counter != history.hend) {
                int next_hist = (hist_counter + 1) % MAX_HISTORY_SIZE;

                if (next_hist != history.hend) {
                  resetCommand(&cmd, (history.hist[next_hist] == NULL) ? "" : history.hist[next_hist]);
                } else {
                  resetCommand(&cmd, "");
                }
                hist_counter = next_hist;
              }
              break;
            case 'C':  // Right arrow
              moveCursorRight(&cmd);
              break;
            case 'D':  // Left arrow
              moveCursorLeft(&cmd);
              break;
            case '1':
              if ((c = getchar()) == '~') {
                // Home key
                positionCursorHome(&cmd);
              }
              break;
            case '2':
              if ((c = getchar()) == '~') {
                // Insert key
              }
              break;
            case '3':
              if ((c = getchar()) == '~') {
                // Delete key
                deleteChar(&cmd);
              }
              break;
            case '4':
              if ((c = getchar()) == '~') {
                // End key
                positionCursorEnd(&cmd);
              }
              break;
            case '5':
              if ((c = getchar()) == '~') {
                // Page up key
              }
              break;
            case '6':
              if ((c = getchar()) == '~') {
                // Page down key
              }
              break;
            case 72:
              // Home key
              positionCursorHome(&cmd);
              break;
            case 70:
              // End key
              positionCursorEnd(&cmd);
              break;
          }
          break;
      }
    } else if (c == 0x7f) {
      // press delete key
      backspaceChar(&cmd);
    } else {
      insertChar(&cmd, &c, 1);
    }
  }
}

void *shellLoopQuery(void *arg) {
  if (indicator) {
    get_old_terminal_mode(&oldtio);
    indicator = 0;
  }

  TAOS *con = (TAOS *)arg;

  pthread_cleanup_push(cleanup_handler, NULL);

  char *command = malloc(MAX_COMMAND_SIZE);

  while (1) {
    // Read command from shell.

    memset(command, 0, MAX_COMMAND_SIZE);
    set_terminal_mode();
    shellReadCommand(con, command);
    reset_terminal_mode();

    if (command != NULL) {
      // Run the command
      shellRunCommand(con, command);
    }
  }

  pthread_cleanup_pop(1);

  return NULL;
}

void shellPrintNChar(char *str, int width) {
  int col_left = width;
  wchar_t wc;
  while (col_left > 0) {
    if (*str == '\0') break;
    char *tstr = str;
    int byte_width = mbtowc(&wc, tstr, MB_CUR_MAX);
    int col_width = wcwidth(wc);
    if (col_left < col_width) break;
    printf("%lc", wc);
    str += byte_width;
    col_left -= col_width;
  }

  while (col_left > 0) {
    printf(" ");
    col_left--;
  }
  printf("|");
}

int get_old_terminal_mode(struct termios *tio) {
  /* Make sure stdin is a terminal. */
  if (!isatty(STDIN_FILENO)) {
    return -1;
  }

  // Get the parameter of current terminal
  if (tcgetattr(0, &oldtio) != 0) {
    return -1;
  }

  return 1;
}

void reset_terminal_mode() {
  if (tcsetattr(0, TCSANOW, &oldtio) != 0) {
    fprintf(stderr, "Fail to reset the terminal properties!\n");
    exit(EXIT_FAILURE);
  }
}

void set_terminal_mode() {
  struct termios newtio;

  /* if (atexit(reset_terminal_mode) != 0) { */
  /*     fprintf(stderr, "Error register exit function!\n"); */
  /*     exit(EXIT_FAILURE); */
  /* } */

  memcpy(&newtio, &oldtio, sizeof(oldtio));

  // Set new terminal attributes.
  newtio.c_iflag &= ~(IXON | IXOFF | ICRNL | INLCR | IGNCR | IMAXBEL | ISTRIP);
  newtio.c_iflag |= IGNBRK;

  // newtio.c_oflag &= ~(OPOST|ONLCR|OCRNL|ONLRET);
  newtio.c_oflag |= OPOST;
  newtio.c_oflag |= ONLCR;
  newtio.c_oflag &= ~(OCRNL | ONLRET);

  newtio.c_lflag &= ~(IEXTEN | ICANON | ECHO | ECHOE | ECHONL | ECHOCTL | ECHOPRT | ECHOKE | ISIG);
  newtio.c_cc[VMIN] = 1;
  newtio.c_cc[VTIME] = 0;

  if (tcsetattr(0, TCSANOW, &newtio) != 0) {
    fprintf(stderr, "Fail to set terminal properties!\n");
    exit(EXIT_FAILURE);
  }
}

void get_history_path(char *history) { sprintf(history, "%s/%s", getpwuid(getuid())->pw_dir, HISTORY_FILE); }

void clearScreen(int ecmd_pos, int cursor_pos) {
  struct winsize w;
  ioctl(0, TIOCGWINSZ, &w);

  int cursor_x = cursor_pos / w.ws_col;
  int cursor_y = cursor_pos % w.ws_col;
  int command_x = ecmd_pos / w.ws_col;
  positionCursor(cursor_y, LEFT);
  positionCursor(command_x - cursor_x, DOWN);
  fprintf(stdout, "\033[2K");
  for (int i = 0; i < command_x; i++) {
    positionCursor(1, UP);
    fprintf(stdout, "\033[2K");
  }
  fflush(stdout);
}

void showOnScreen(Command *cmd) {
  struct winsize w;
  if (ioctl(0, TIOCGWINSZ, &w) < 0 || w.ws_col == 0 || w.ws_row == 0) {
    fprintf(stderr, "No stream device\n");
    exit(EXIT_FAILURE);
  }

  wchar_t wc;
  int size = 0;

  // Print out the command.
  char *total_string = malloc(MAX_COMMAND_SIZE);
  memset(total_string, '\0', MAX_COMMAND_SIZE);
  if (strcmp(cmd->buffer, "") == 0) {
    sprintf(total_string, "%s%s", PROMPT_HEADER, cmd->command);
  } else {
    sprintf(total_string, "%s%s", CONTINUE_PROMPT, cmd->command);
  }

  int remain_column = w.ws_col;
  /* size = cmd->commandSize + prompt_size; */
  for (char *str = total_string; size < cmd->commandSize + prompt_size;) {
    int ret = mbtowc(&wc, str, MB_CUR_MAX);
    if (ret < 0) break;
    size += ret;
    /* assert(size >= 0); */
    int width = wcwidth(wc);
    if (remain_column > width) {
      printf("%lc", wc);
      remain_column -= width;
    } else {
      if (remain_column == width) {
        printf("%lc\n\r", wc);
        remain_column = w.ws_col;
      } else {
        printf("\n\r%lc", wc);
        remain_column = w.ws_col - width;
      }
    }

    str = total_string + size;
  }

  free(total_string);
  /* for (int i = 0; i < size; i++){ */
  /*     char c = total_string[i]; */
  /*     if (k % w.ws_col == 0) { */
  /*         printf("%c\n\r", c); */
  /*     } */
  /*     else { */
  /*         printf("%c", c); */
  /*     } */
  /*     k += 1; */
  /* } */

  // Position the cursor
  int cursor_pos = cmd->screenOffset + prompt_size;
  int ecmd_pos = cmd->endOffset + prompt_size;

  int cursor_x = cursor_pos / w.ws_col;
  int cursor_y = cursor_pos % w.ws_col;
  // int cursor_y = cursor % w.ws_col;
  int command_x = ecmd_pos / w.ws_col;
  int command_y = ecmd_pos % w.ws_col;
  // int command_y = (command.size() + prompt_size) % w.ws_col;
  positionCursor(command_y, LEFT);
  positionCursor(command_x, UP);
  positionCursor(cursor_x, DOWN);
  positionCursor(cursor_y, RIGHT);
  fflush(stdout);
}

void cleanup_handler(void *arg) { tcsetattr(0, TCSANOW, &oldtio); }

void exitShell() {
  tcsetattr(0, TCSANOW, &oldtio);
  exit(EXIT_SUCCESS);
}
