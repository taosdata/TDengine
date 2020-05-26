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

#include "os.h"
#include "sim.h"
#include "taos.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tutil.h"
#include "tsocket.h"

SScript *simScriptList[MAX_MAIN_SCRIPT_NUM];
SCommand simCmdList[SIM_CMD_END];
int simScriptPos = -1;
int simScriptSucced = 0;
int simDebugFlag = 135;
void simCloseTaosdConnect(SScript *script);
char simHostName[128];

char *simParseArbitratorName(char *varName) {
  static char hostName[140];
  sprintf(hostName, "%s:%d", simHostName, 8000);
  return hostName;
}

char *simParseHostName(char *varName) {
  static char hostName[140];

  int index = atoi(varName + 8);
  int port = 7100;
  switch (index) {
    case 1:
      port = 7100;
      break;
    case 2:
      port = 7200;
      break;
    case 3:
      port = 7300;
      break;
    case 4:
      port = 7400;
      break;
    case 5:
      port = 7500;
      break;
    case 6:
      port = 7600;
      break;
    case 7:
      port = 7700;
      break;
    case 8:
      port = 7800;
      break;
    case 9:
      port = 7900;
      break;
  }
  
  sprintf(hostName, "'%s:%d'", simHostName, port);
  //simPrint("hostName:%s", hostName);
  return hostName;
}

bool simSystemInit() {
  taosGetFqdn(simHostName);
  taos_init();
  simInitsimCmdList();
  memset(simScriptList, 0, sizeof(SScript *) * MAX_MAIN_SCRIPT_NUM);
  return true;
}

void simSystemCleanUp() {}

void simFreeScript(SScript *script) {
  if (script->type == SIM_SCRIPT_TYPE_MAIN) {
    for (int i = 0; i < script->bgScriptLen; ++i) {
      SScript *bgScript = script->bgScripts[i];
      bgScript->killed = true;
    }
  }

  taos_close(script->taos);
  tfree(script->lines);
  tfree(script->optionBuffer);
  tfree(script);
}

SScript *simProcessCallOver(SScript *script) {
  if (script->type == SIM_SCRIPT_TYPE_MAIN) {
    if (script->killed) {
      simPrint("script:" FAILED_PREFIX "%s" FAILED_POSTFIX ", " FAILED_PREFIX
               "failed" FAILED_POSTFIX ", error:%s",
               script->fileName, script->error);
      exit(-1);
    } else {
      simPrint("script:" SUCCESS_PREFIX "%s" SUCCESS_POSTFIX ", " SUCCESS_PREFIX
               "success" SUCCESS_POSTFIX,
               script->fileName);
      simCloseTaosdConnect(script);
      simScriptSucced++;
      simScriptPos--;
      if (simScriptPos == -1) {
        simPrint("----------------------------------------------------------------------");
        simPrint("Simulation Test Done, " SUCCESS_PREFIX "%d" SUCCESS_POSTFIX " Passed:\n", simScriptSucced);
        exit(0);
      }

      simFreeScript(script);
      return simScriptList[simScriptPos];
    }
  } else {
    simPrint("script:%s, is stopped by main script", script->fileName);
    simFreeScript(script);
    return NULL;
  }
}

void *simExecuteScript(void *inputScript) {
  SScript *script = (SScript *)inputScript;

  while (1) {
    if (script->type == SIM_SCRIPT_TYPE_MAIN) {
      script = simScriptList[simScriptPos];
    }

    if (script->killed || script->linePos >= script->numOfLines) {
      script = simProcessCallOver(script);
      if (script == NULL) break;
    } else {
      SCmdLine *line = &script->lines[script->linePos];
      char *option = script->optionBuffer + line->optionOffset;
      simTrace("script:%s, line:%d with option \"%s\"", script->fileName, line->lineNum, option);

      SCommand *cmd = &simCmdList[line->cmdno];
      int ret = (*(cmd->executeCmd))(script, option);
      if (!ret) {
        script->killed = true;
      }
    }
  }

  return NULL;
}
