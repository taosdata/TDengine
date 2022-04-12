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
#include "tlog.h"
#include "tglobal.h"
#include "tutil.h"
#include <dlfcn.h>

typedef int32_t (*ModuleStartFp)();
typedef void (*ModuleStopFp)();

typedef struct {
  ModuleStartFp startFunc;
  ModuleStopFp  stopFunc;
  char          name[1024];
  void*         handle;
} ModuleDesc;

static ModuleDesc* tsModules = NULL;
static int32_t     tsModulesNum = 0;

static bool moduleReadCfg() {
  char *line, *option, *value, *value2, *value3;
  int   olen, vlen, vlen2, vlen3;
  int   moduleNum = 0;
  char  fileName[PATH_MAX] = {0};

  sprintf(fileName, "%s/taos.cfg", configDir);
  FILE* fp = fopen(fileName, "r");
  if (fp == NULL) {
    struct stat s;
    if (stat(configDir, &s) != 0 || (!S_ISREG(s.st_mode) && !S_ISLNK(s.st_mode))) {
      return true;
    }
    fp = fopen(configDir, "r");
    if (fp == NULL) {
      return false;
    }
  }

  size_t len = 1024;
  line = calloc(1, len);

  while (!feof(fp)) {
    memset(line, 0, len);

    option = value = value2 = value3 = NULL;
    olen = vlen = vlen2 = vlen3 = 0;

    tgetline(&line, &len, fp);
    line[len - 1] = 0;

    paGetToken(line, &option, &olen);
    if (olen == 0) continue;
    option[olen] = 0;

    paGetToken(option + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    if (strncmp(option, "module", 6) == 0) {
      moduleNum++;
      uInfo("module:%s, read from config file, moduleNum:%d", value, moduleNum);
      if (moduleNum > tsModulesNum) {
        tsModulesNum = 2 * tsModulesNum + 1;
        tsModules = realloc(tsModules, tsModulesNum * sizeof(ModuleDesc));
      }

      tstrncpy(tsModules[moduleNum - 1].name, value, 1023);
    }
  }

  tsModulesNum = moduleNum;

  fclose(fp);
  taosMemoryFreeClear(line);
  return true;
}

int32_t moduleStart() {
  if (!moduleReadCfg()) {
    uError("failed to read module config from dir:%s", configDir);
    return 0;
  }

  if (tsModulesNum == 0) return 0;
  uInfo("all %d modules will be loaded", tsModulesNum);

  for (int32_t i = 0; i < tsModulesNum; ++i) {
    char* path = tsModules[i].name;
    void* handle = dlopen(path, RTLD_LAZY);
    if (handle == NULL) {
      uError("module:%s, not open since %s", path, strerror(errno));
    } else {
      uInfo("module:%s, open successfully", path);
      ModuleStartFp startFunc = (ModuleStartFp)dlsym(handle, "taosModuleStart");
      ModuleStopFp  stopFunc = (ModuleStopFp)dlsym(handle, "taosModuleStop");
      if (startFunc != NULL && stopFunc != NULL) {
        uInfo("module:%s, taosModuleStart:%p taosModuleStop:%p is loaded", path, startFunc, stopFunc);
        tsModules[i].startFunc = startFunc;
        tsModules[i].stopFunc = stopFunc;
      } else {
        uError("module:%s, failed to load taosModuleStart:%p taosModuleStop:%p", path, startFunc, stopFunc);
      }
    }

    tsModules[i].handle = handle;
  }

  uInfo("all %d modules will be started", tsModulesNum);
  for (int32_t i = 0; i < tsModulesNum; ++i) {
    if (tsModules[i].startFunc) {
      uInfo("module:%s, is about to start, index:%d", tsModules[i].name, i);
      int32_t code = (*(tsModules[i].startFunc))();
      uInfo("module:%s, has been started, ret:%d", tsModules[i].name, code);
    }
  }

  uInfo("all %d modules have been started", tsModulesNum);
  return 0;
}

void moduleStop() {
  if (tsModulesNum == 0) return;
  uInfo("all %d modules will be stopped", tsModulesNum);

  for (int32_t i = 0; i < tsModulesNum; ++i) {
    if (tsModules[i].stopFunc) {
      uInfo("module:%s, is about to stop, index:%d", tsModules[i].name, i);
      (*(tsModules[i].stopFunc))();
      uInfo("module:%s, has been stopped", tsModules[i].name);
    }
  }

  for (int32_t i = 0; i < tsModulesNum; ++i) {
    if (tsModules[i].handle) {
      dlclose(tsModules[i].handle);
      uInfo("module:%s, has been detached", tsModules[i].name);
    }
  }

  taosMemoryFree(tsModules);
}

