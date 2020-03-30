/*******************************************************************
 *           Copyright (c) 2001 by TAOS Networks, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Networks, Inc.
 *  No part of this file may be reproduced, stored, transmitted,
 *  disclosed or used in any form or by any means other than as
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/

#include "sim.h"
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

bool simAsyncQuery = false;

void simHandleSignal(int signo) {
  simSystemCleanUp();
  exit(1);
}

int main(int argc, char *argv[]) {
  char scriptFile[MAX_FILE_NAME_LEN] = "sim_main_test.sim";

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0 && i < argc - 1) {
      strncpy(configDir, argv[++i], MAX_FILE_NAME_LEN);
    } else if (strcmp(argv[i], "-f") == 0 && i < argc - 1) {
      strcpy(scriptFile, argv[++i]);
    } else if (strcmp(argv[i], "-a") == 0) {
      simAsyncQuery = true;
    } else {
      printf("usage: %s [options] \n", argv[0]);
      printf("       [-c config]: config directory, default is: %s\n",
             configDir);
      printf("       [-f script]: script filename\n");
      exit(0);
    }
  }

  if (!simSystemInit()) {
    simError("failed to initialize the system");
    simSystemCleanUp();
    exit(1);
  }

  simPrint("simulator is running ...");
  signal(SIGINT, simHandleSignal);

  SScript *script = simParseScript(scriptFile);
  if (script == NULL) {
    simError("parse script file:%s failed", scriptFile);
    exit(-1);
  }

  simScriptList[++simScriptPos] = script;
  simExecuteScript(script);

  return 0;
}