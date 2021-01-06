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
#include "os.h"
#include "tglobal.h"
#include "sim.h"
#undef TAOS_MEM_CHECK

bool simAsyncQuery = false;

void simHandleSignal(int32_t signo) {
  simSystemCleanUp();
  exit(1);
}

int32_t main(int32_t argc, char *argv[]) {
  char scriptFile[MAX_FILE_NAME_LEN] = "sim_main_test.sim";

  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0 && i < argc - 1) {
      tstrncpy(configDir, argv[++i], MAX_FILE_NAME_LEN);
    } else if (strcmp(argv[i], "-f") == 0 && i < argc - 1) {
      strcpy(scriptFile, argv[++i]);
    } else if (strcmp(argv[i], "-a") == 0) {
      simAsyncQuery = true;
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
  signal(SIGINT, simHandleSignal);

  SScript *script = simParseScript(scriptFile);
  if (script == NULL) {
    simError("parse script file:%s failed", scriptFile);
    return -1;
  }

  simScriptList[++simScriptPos] = script;
  simExecuteScript(script);

  return 0;
}