/*******************************************************************
*           Copyright (c) 2017 by TAOS Technologies, Inc.
*                     All rights reserved.
*
*  This file is proprietary and confidential to TAOS Technologies.
*  No part of this file may be reproduced, stored, transmitted,
*  disclosed or used in any form or by any means other than as
*  expressly provided by the written permission from Jianhui Tao
*
* ****************************************************************/

#include <assert.h>
#include <regex.h>
#include <stdio.h>
#include "os.h"
#include "shell.h"
#include "taos.h"
#include "shellCommand.h"

extern char configDir[];

void printHelp() {
  char indent[10] = "        ";
  printf("taos shell is used to test the TDengine database\n");

  printf("%s%s\n", indent, "-h");
  printf("%s%s%s\n", indent, indent, "TDengine server IP address to connect. The default host is localhost.");
  printf("%s%s\n", indent, "-p");
  printf("%s%s%s\n", indent, indent, "The password to use when connecting to the server.");
  printf("%s%s\n", indent, "-P");
  printf("%s%s%s\n", indent, indent, "The TCP/IP port number to use for the connection");
  printf("%s%s\n", indent, "-u");
  printf("%s%s%s\n", indent, indent, "The user name to use when connecting to the server.");
  printf("%s%s\n", indent, "-A");
  printf("%s%s%s\n", indent, indent, "The user auth to use when connecting to the server.");
  printf("%s%s\n", indent, "-c");
  printf("%s%s%s\n", indent, indent, "Configuration directory.");
  printf("%s%s\n", indent, "-C");
  printf("%s%s%s\n", indent, indent, "Dump configuration.");
  printf("%s%s\n", indent, "-s");
  printf("%s%s%s\n", indent, indent, "Commands to run without enter the shell.");
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s\n", indent, indent, "Output time as unsigned long..");
  printf("%s%s\n", indent, "-f");
  printf("%s%s%s\n", indent, indent, "Script to run without enter the shell.");
  printf("%s%s\n", indent, "-d");
  printf("%s%s%s\n", indent, indent, "Database to use when connecting to the server.");
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s\n", indent, indent, "Time zone of the shell, default is local.");
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s\n", indent, indent, "Net role when network connectivity test, default is startup, options: client|server|rpc|startup.");
  printf("%s%s\n", indent, "-l");
  printf("%s%s%s\n", indent, indent, "Packet length used for net test, default is 1000 bytes.");

  exit(EXIT_SUCCESS);
}

void shellParseArgument(int argc, char *argv[], SShellArguments *arguments) {
  for (int i = 1; i < argc; i++) {
    // for host
    if (strcmp(argv[i], "-h") == 0) {
      if (i < argc - 1) {
        arguments->host = argv[++i];
      } else {
        fprintf(stderr, "option -h requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // for password
    else if (strcmp(argv[i], "-p") == 0) {
      arguments->is_use_passwd = true;
    }
    // for management port
    else if (strcmp(argv[i], "-P") == 0) {
      if (i < argc - 1) {
        arguments->port = atoi(argv[++i]);
      } else {
        fprintf(stderr, "option -P requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // for user
    else if (strcmp(argv[i], "-u") == 0) {
      if (i < argc - 1) {
        arguments->user = argv[++i];
      } else {
        fprintf(stderr, "option -u requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-A") == 0) {
      if (i < argc - 1) {
        arguments->auth = argv[++i];
      } else {
        fprintf(stderr, "option -A requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {   
        char *tmp = argv[++i];
        if (strlen(tmp) >= TSDB_FILENAME_LEN) {
          fprintf(stderr, "config file path: %s overflow max len %d\n", tmp, TSDB_FILENAME_LEN - 1);
          exit(EXIT_FAILURE);
        }
        strcpy(configDir, tmp);
      } else {
        fprintf(stderr, "Option -c requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-C") == 0) {
      arguments->dump_config = true;
    } else if (strcmp(argv[i], "-s") == 0) {
      if (i < argc - 1) {
        arguments->commands = argv[++i];
      } else {
        fprintf(stderr, "option -s requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-r") == 0) {
      arguments->is_raw_time = true;
    }
    // For temperory batch commands to run TODO
    else if (strcmp(argv[i], "-f") == 0) {
      if (i < argc - 1) {
        strcpy(arguments->file, argv[++i]);
      } else {
        fprintf(stderr, "option -f requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // for default database
    else if (strcmp(argv[i], "-d") == 0) {
      if (i < argc - 1) {
        arguments->database = argv[++i];
      } else {
        fprintf(stderr, "option -d requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // For time zone
    else if (strcmp(argv[i], "-t") == 0) {
      if (i < argc - 1) {
        arguments->timezone = argv[++i];
      } else {
        fprintf(stderr, "option -t requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // For time zone
    else if (strcmp(argv[i], "-n") == 0) {
      if (i < argc - 1) {
        arguments->netTestRole = argv[++i];
      } else {
        fprintf(stderr, "option -n requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // For time zone
    else if (strcmp(argv[i], "-l") == 0) {
      if (i < argc - 1) {
        arguments->pktLen = atoi(argv[++i]);
      } else {
        fprintf(stderr, "option -l requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // For temperory command TODO
    else if (strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(EXIT_FAILURE);
    } else {
      fprintf(stderr, "wrong options\n");
      printHelp();
      exit(EXIT_FAILURE);
    }
  }
}

void shellPrintContinuePrompt() { printf("%s", CONTINUE_PROMPT); }

void shellPrintPrompt() { printf("%s", PROMPT_HEADER); }

void updateBuffer(Command *cmd) {
  if (regex_match(cmd->buffer, "(\\s+$)|(^$)", REG_EXTENDED)) strcat(cmd->command, " ");
  strcat(cmd->buffer, cmd->command);

  memset(cmd->command, 0, MAX_COMMAND_SIZE);
  cmd->cursorOffset = 0;
}

int isReadyGo(Command *cmd) {
  char total[MAX_COMMAND_SIZE];
  memset(total, 0, MAX_COMMAND_SIZE);
  sprintf(total, "%s%s", cmd->buffer, cmd->command);

  char *reg_str =
      "(^.*;\\s*$)|(^\\s*$)|(^\\s*exit\\s*$)|(^\\s*q\\s*$)|(^\\s*quit\\s*$)|(^"
      "\\s*clear\\s*$)";
  if (regex_match(total, reg_str, REG_EXTENDED | REG_ICASE)) return 1;

  return 0;
}

void insertChar(Command *cmd, char c) {
  // TODO: Check if the length enough.
  if (cmd->cursorOffset >= MAX_COMMAND_SIZE) {
    fprintf(stdout, "sql is larger than %d bytes", MAX_COMMAND_SIZE);
    return;
  }

  cmd->command[cmd->cursorOffset++] = c;
}

void shellReadCommand(TAOS *con, char command[]) {
  Command cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.buffer = (char *)calloc(1, MAX_COMMAND_SIZE);
  cmd.command = (char *)calloc(1, MAX_COMMAND_SIZE);

  // Read input.
  char c;
  while (1) {
    c = getchar();

    switch (c) {
      case '\n':
      case '\r':
        if (isReadyGo(&cmd)) {
          sprintf(command, "%s%s", cmd.buffer, cmd.command);
          free(cmd.buffer);
          cmd.buffer = NULL;
          free(cmd.command);
          cmd.command = NULL;
          return;
        } else {
          shellPrintContinuePrompt();
          updateBuffer(&cmd);
        }
        break;
      default:
        insertChar(&cmd, c);
    }
  }
}

void *shellLoopQuery(void *arg) {
  TAOS *con = (TAOS *)arg;
  char *command = malloc(MAX_COMMAND_SIZE);
  if (command == NULL) return NULL;

  do {
    memset(command, 0, MAX_COMMAND_SIZE);
    shellPrintPrompt();

    // Read command from shell.
    shellReadCommand(con, command);
  } while (shellRunCommand(con, command) == 0);

  return NULL;
}

void get_history_path(char *history) { sprintf(history, "C:/TDengine/%s", HISTORY_FILE); }

void exitShell() { exit(EXIT_SUCCESS); }
