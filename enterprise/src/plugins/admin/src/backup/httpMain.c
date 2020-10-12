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

#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "httpSystem.h"
#include "tglobal.h"
#include "tlog.h"

void signal_handler(int signum) {
  httpStopSystem();
  httpCleanUpSystem();
  exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
  // Set global configuration file
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {       
        if (strlen(argv[++i]) >= TSDB_FILENAME_LEN) {
          printf("config file path: %s overflow max len %d\n", argv[i], TSDB_FILENAME_LEN - 1);
          exit(EXIT_FAILURE);
        }
        strcpy(configDir, argv[i]);
      } else {
        printf("'-c' requires a parameter, default:%s\n", configDir);
        exit(EXIT_FAILURE);
      }
    }
  }

#if !(defined(WIN32) || defined(WIN64))
  /* Set termination handler. */
  struct sigaction act;
  act.sa_handler = signal_handler;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGHUP, &act, NULL);
  sigaction(SIGINT, &act, NULL);
  sigaction(SIGABRT, &act, NULL);
#endif

  // Read global configuration.
  taosReadGlobalLogCfg();

  struct stat dirstat;
  if (stat(tsLogDir, &dirstat) < 0) mkdir(tsLogDir, 0755);

  char temp[128] = {0};
  sprintf(temp, "%s/taoslog", tsLogDir);
  if (taosOpenLogFileWithMaxLines(temp, tsNumOfLogLines, 1) < 0) printf("failed to init log file\n");

  tsReadGlobalConfig();
  tsHttpPort = 6041;
  strcpy(tsCharset, "CP936");

  taosPrintGlobalCfg();

  // Initialize the system
  if (httpInitSystem() < 0) {
    exit(EXIT_FAILURE);
  }

  if (httpStartSystem() < 0) {
    exit(EXIT_FAILURE);
  }

  while (1) {
    sleep(1000);
  }
}
