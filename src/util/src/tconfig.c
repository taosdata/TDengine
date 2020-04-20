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
#include "taosdef.h"
#include "taoserror.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tkey.h"
#include "tulog.h"
#include "tsocket.h"
#include "tsystem.h"
#include "tutil.h"

SGlobalCfg tsGlobalConfig[TSDB_CFG_MAX_NUM] = {0};
int32_t    tsGlobalConfigNum = 0;

static char *tsGlobalUnit[] = {
  " ", 
  "(%)", 
  "(GB)", 
  "(Mb)", 
  "(byte)", 
  "(s)", 
  "(ms)"
};

char *tsCfgStatusStr[] = {
  "none", 
  "system default", 
  "config file", 
  "taos_options", 
  "program argument list"
};

static void taosReadFloatConfig(SGlobalCfg *cfg, char *input_value) {
  float  value = (float)atof(input_value);
  float *option = (float *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    uError("config option:%s, input value:%s, out of range[%f, %f], use default value:%f",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      *option = value;
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %f", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
    }
  }
}

static void taosReadInt32Config(SGlobalCfg *cfg, char *input_value) {
  int32_t  value = atoi(input_value);
  int32_t *option = (int32_t *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    uError("config option:%s, input value:%s, out of range[%f, %f], use default value:%d",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      *option = value;
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
    }
  }
}

static void taosReadInt16Config(SGlobalCfg *cfg, char *input_value) {
  int32_t  value = atoi(input_value);
  int16_t *option = (int16_t *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    uError("config option:%s, input value:%s, out of range[%f, %f], use default value:%d",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      *option = (int16_t)value;
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
    }
  }
}

static void taosReadDirectoryConfig(SGlobalCfg *cfg, char *input_value) {
  int   length = strlen(input_value);
  char *option = (char *)cfg->ptr;
  if (length <= 0 || length > cfg->ptrLength) {
    uError("config option:%s, input value:%s, length out of range[0, %d], use default value:%s",
           cfg->option, input_value, cfg->ptrLength, option);
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      wordexp_t full_path;
      wordexp(input_value, &full_path, 0);
      if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
        strcpy(option, full_path.we_wordv[0]);
      }
      wordfree(&full_path);

      struct stat dirstat;
      if (stat(option, &dirstat) < 0) {
        int code = mkdir(option, 0755);
        uPrint("config option:%s, input value:%s, directory not exist, create with return code:%d",
               cfg->option, input_value, code);
      }
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], option);
    }
  }
}

static void taosReadIpStrConfig(SGlobalCfg *cfg, char *input_value) {
  uint32_t value = inet_addr(input_value);
  char *   option = (char *)cfg->ptr;
  if (value == INADDR_NONE) {
    uError("config option:%s, input value:%s, is not a valid ip address, use default value:%s",
           cfg->option, input_value, option);
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      strncpy(option, input_value, cfg->ptrLength);
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], option);
    }
  }
}

static void taosReadStringConfig(SGlobalCfg *cfg, char *input_value) {
  int   length = strlen(input_value);
  char *option = (char *)cfg->ptr;
  if (length <= 0 || length > cfg->ptrLength) {
    uError("config option:%s, input value:%s, length out of range[0, %d], use default value:%s",
           cfg->option, input_value, cfg->ptrLength, option);
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      strncpy(option, input_value, cfg->ptrLength);
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], option);
    }
  }
}

static void taosReadLogOption(char *option, char *value) {
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_CONFIG) || !(cfg->cfgType & TSDB_CFG_CTYPE_B_LOG)) continue;
    if (strcasecmp(cfg->option, option) != 0) continue;

    switch (cfg->valType) {
      case TAOS_CFG_VTYPE_INT32:
        taosReadInt32Config(cfg, value);
        if (strcasecmp(cfg->option, "debugFlag") == 0) {
          taosSetAllDebugFlag();
        }
        break;
      case TAOS_CFG_VTYPE_DIRECTORY:
        taosReadDirectoryConfig(cfg, value);
        break;
      default:
        break;
    }
    break;
  }
}

SGlobalCfg *taosGetConfigOption(const char *option) {
  taosInitGlobalCfg();
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (strcasecmp(cfg->option, option) != 0) continue;
    return cfg;
  }
  return NULL;
}

static void taosReadConfigOption(const char *option, char *value) {
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_CONFIG)) continue;
    if (strcasecmp(cfg->option, option) != 0) continue;

    switch (cfg->valType) {
      case TAOS_CFG_VTYPE_INT16:
        taosReadInt16Config(cfg, value);
        break;
      case TAOS_CFG_VTYPE_INT32:
        taosReadInt32Config(cfg, value);
        break;
      case TAOS_CFG_VTYPE_FLOAT:
        taosReadFloatConfig(cfg, value);
        break;
      case TAOS_CFG_VTYPE_STRING:
        taosReadStringConfig(cfg, value);
        break;
      case TAOS_CFG_VTYPE_IPSTR:
        taosReadIpStrConfig(cfg, value);
        break;
      case TAOS_CFG_VTYPE_DIRECTORY:
        taosReadDirectoryConfig(cfg, value);
        break;
      default:
        uError("config option:%s, input value:%s, can't be recognized", option, value);
        break;
    }
    break;
  }
}

void taosInitConfigOption(SGlobalCfg cfg) {
  tsGlobalConfig[tsGlobalConfigNum++] = cfg;
}

void taosReadGlobalLogCfg() {
  FILE * fp;
  char * line, *option, *value;
  int    olen, vlen;
  char   fileName[PATH_MAX] = {0};

  mdebugFlag = 135;
  sdbDebugFlag = 135;

  wordexp_t full_path;
  wordexp(configDir, &full_path, 0);
  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    strcpy(configDir, full_path.we_wordv[0]);
  } else {
    printf("configDir:%s not there, use default value: /etc/taos", configDir);
    strcpy(configDir, "/etc/taos");
  }
  wordfree(&full_path);

  taosReadLogOption("logDir", logDir);
  
  sprintf(fileName, "%s/taos.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
    printf("\nconfig file:%s not found, all variables are set to default\n", fileName);
    return;
  }
  
  size_t len = 1024;
  line = calloc(1, len);
  
  while (!feof(fp)) {
    memset(line, 0, len);
    
    option = value = NULL;
    olen = vlen = 0;

    getline(&line, &len, fp);
    line[len - 1] = 0;
    
    paGetToken(line, &option, &olen);
    if (olen == 0) continue;
    option[olen] = 0;

    paGetToken(option + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    taosReadLogOption(option, value);
  }

  tfree(line);
  fclose(fp);
}

bool taosReadGlobalCfg() {
  char * line, *option, *value, *value1;
  int    olen, vlen, vlen1;
  char   fileName[PATH_MAX] = {0};

  sprintf(fileName, "%s/taos.cfg", configDir);
  FILE* fp = fopen(fileName, "r");
  
  size_t len = 1024;
  line = calloc(1, len);
  
  if (fp != NULL) {
    while (!feof(fp)) {
      memset(line, 0, len);

      option = value = NULL;
      olen = vlen = 0;

      getline(&line, &len, fp);
      line[len - 1] = 0;
      
      paGetToken(line, &option, &olen);
      if (olen == 0) continue;
      option[olen] = 0;

      paGetToken(option + olen + 1, &value, &vlen);
      if (vlen == 0) continue;
      value[vlen] = 0;

      // For dataDir, the format is:
      // dataDir    /mnt/disk1    0
      paGetToken(value + vlen + 1, &value1, &vlen1);
      
      taosReadConfigOption(option, value);
    }

    fclose(fp);
  }

  tfree(line);
  
  return true;
}

void taosPrintGlobalCfg() {
  uPrint("   taos config & system info:");
  uPrint("==================================");

  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (tscEmbedded == 0 && !(cfg->cfgType & TSDB_CFG_CTYPE_B_CLIENT)) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_NOT_PRINT) continue;
    
    int optionLen = (int)strlen(cfg->option);
    int blankLen = TSDB_CFG_PRINT_LEN - optionLen;
    blankLen = blankLen < 0 ? 0 : blankLen;

    char blank[TSDB_CFG_PRINT_LEN];
    memset(blank, ' ', TSDB_CFG_PRINT_LEN);
    blank[blankLen] = 0;

    switch (cfg->valType) {
      case TAOS_CFG_VTYPE_INT16:
        uPrint(" %s:%s%d%s", cfg->option, blank, *((int16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_INT32:
        uPrint(" %s:%s%d%s", cfg->option, blank, *((int32_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_FLOAT:
        uPrint(" %s:%s%f%s", cfg->option, blank, *((float *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_STRING:
      case TAOS_CFG_VTYPE_IPSTR:
      case TAOS_CFG_VTYPE_DIRECTORY:
        uPrint(" %s:%s%s%s", cfg->option, blank, (char *)cfg->ptr, tsGlobalUnit[cfg->unitType]);
        break;
      default:
        break;
    }
  }

  taosPrintOsInfo();
}
