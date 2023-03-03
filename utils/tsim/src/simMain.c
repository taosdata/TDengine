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

#define _DEFAULT_SOURCE
#include "simInt.h"

bool simExecSuccess = false;
bool abortExecution = false;
bool useValgrind = false;

void simHandleSignal(int32_t signo, void *sigInfo, void *context) {
  simSystemCleanUp();
  abortExecution = true;
}

int32_t main(int32_t argc, char *argv[]) {
  char scriptFile[MAX_FILE_NAME_LEN] = "sim_main_test.sim";

  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0 && i < argc - 1) {
      tstrncpy(configDir, argv[++i], 128);
    } else if (strcmp(argv[i], "-f") == 0 && i < argc - 1) {
      tstrncpy(scriptFile, argv[++i], MAX_FILE_NAME_LEN);
    } else if (strcmp(argv[i], "-v") == 0) {
      useValgrind = true;
    } else {
      printf("usage: %s [options] \n", argv[0]);
      printf("       [-c config]: config directory, default is: %s\n", configDir);
      printf("       [-f script]: script filename\n");
      return 0;
    }
  }

  if (!simSystemInit()) {
    simError("failed to initialize the system");
    simSystemCleanUp();
    return -1;
  }

  simInfo("simulator is running ...");
  taosSetSignal(SIGINT, simHandleSignal);

  SScript *script = simParseScript(scriptFile);
  if (script == NULL) {
    simError("parse script file:%s failed", scriptFile);
    return -1;
  }

  if (abortExecution) {
    simError("execute abort");
    return -1;
  }

  simScriptList[++simScriptPos] = script;
  simExecuteScript(script);

  int32_t ret = simExecSuccess ? 0 : -1;
  simInfo("execute result %d", ret);

  return ret;
}
