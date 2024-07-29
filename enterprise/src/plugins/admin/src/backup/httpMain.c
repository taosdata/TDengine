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

#include "os.h"
#include "httpSystem.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tlog.h"

void sigintHandler(int signum, void *sigInfo, void *context) {
  (void)httpStopSystem();
  (void)httpCleanUpSystem();
  exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
  // Set global configuration file
  (void)taosSetSignal(SIGTERM, sigintHandler);
  (void)taosSetSignal(SIGHUP, sigintHandler);
  (void)taosSetSignal(SIGINT, sigintHandler);
  (void)taosSetSignal(SIGABRT, sigintHandler);

  (void)taosInitGlobalCfg();
  (void)taosReadGlobalLogCfg();
  (void)taosIgnSIGPIPE();
  (void)taosBlockSIGPIPE();

  char temp[TSDB_FILENAME_LEN];
  sprintf(temp, "%s/httplog", tsLogDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) < 0) {
    printf("failed to init log file\n");
  }

  if (!taosReadGlobalCfg()) {
    (void)taosPrintGlobalCfg();
    printf("TDengine read global config failed");
    return -1;
  }

  printf("start to initialize TDengine");

  // Initialize the system
  if (httpInitSystem() < 0) {
    exit(EXIT_FAILURE);
  }

  if (httpStartSystem() < 0) {
    exit(EXIT_FAILURE);
  }

  while (1) {
    taosMsleep(100);
  }
}
