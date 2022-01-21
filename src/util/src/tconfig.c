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
#include "tulog.h"
#include "tsocket.h"
#include "tutil.h"

SGlobalCfg tsGlobalConfig[TSDB_CFG_MAX_NUM] = {{0}};
int32_t    tsGlobalConfigNum = 0;

#define ATOI_JUDGE if ( !value && strcmp(input_value, "0") != 0) { \
                      uError("atoi error, input value:%s",input_value); \
                      return false; \
                    }

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

static bool taosReadFloatConfig(SGlobalCfg *cfg, char *input_value) {
  float  value = (float)atof(input_value);
  ATOI_JUDGE
  float *option = (float *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    uError("config option:%s, input value:%s, out of range[%f, %f], use default value:%f",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
    return false;
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      *option = value;
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %f", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
      return false;
    }
  }
  return true;
}

static bool taosReadDoubleConfig(SGlobalCfg *cfg, char *input_value) {
  double  value = atof(input_value);
  ATOI_JUDGE
  double *option = (double *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    uError("config option:%s, input value:%s, out of range[%f, %f], use default value:%f",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
    return false;
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      *option = value;
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %f", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
      return false;
    }
  }
  return true;
}


static bool taosReadInt32Config(SGlobalCfg *cfg, char *input_value) {
  int32_t  value = atoi(input_value);
  ATOI_JUDGE
  int32_t *option = (int32_t *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    uError("config option:%s, input value:%s, out of range[%f, %f], use default value:%d",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
    return false;
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      *option = value;
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
      return false;
    }
  }
  return true;
}

static bool taosReadInt16Config(SGlobalCfg *cfg, char *input_value) {
  int32_t  value = atoi(input_value);
  ATOI_JUDGE
  int16_t *option = (int16_t *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    uError("config option:%s, input value:%s, out of range[%f, %f], use default value:%d",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
    return false;
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      *option = (int16_t)value;
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
      return false;
    }
  }
  return true;
}

static bool taosReadUInt16Config(SGlobalCfg *cfg, char *input_value) {
  int32_t  value = atoi(input_value);
  ATOI_JUDGE
  uint16_t *option = (uint16_t *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    uError("config option:%s, input value:%s, out of range[%f, %f], use default value:%d",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
    return false;
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      *option = (uint16_t)value;
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
      return false;
    }
  }
  return true;
}

static bool taosReadInt8Config(SGlobalCfg *cfg, char *input_value) {
  int32_t  value = atoi(input_value);
  ATOI_JUDGE
  int8_t *option = (int8_t *)cfg->ptr;
  if (value < cfg->minValue || value > cfg->maxValue) {
    uError("config option:%s, input value:%s, out of range[%f, %f], use default value:%d",
           cfg->option, input_value, cfg->minValue, cfg->maxValue, *option);
    return false;
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      *option = (int8_t)value;
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], *option);
      return false;
    }
  }
  return true;
}

static bool taosReadDirectoryConfig(SGlobalCfg *cfg, char *input_value) {
  int   length = (int)strlen(input_value);
  char *option = (char *)cfg->ptr;
  if (length <= 0 || length > cfg->ptrLength) {
    uError("config option:%s, input value:%s, length out of range[0, %d], use default value:%s", cfg->option,
           input_value, cfg->ptrLength, option);
    return false;
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      wordexp_t full_path;
      if (0 != wordexp(input_value, &full_path, 0)) {
        printf("\nconfig dir: %s wordexp fail! reason:%s\n", input_value, strerror(errno));
        wordfree(&full_path);
        return false;
      }

      if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
        strcpy(option, full_path.we_wordv[0]);
      }

      wordfree(&full_path);

      char tmp[PATH_MAX] = {0};
      if (realpath(option, tmp) != NULL) {
        strcpy(option, tmp);
      }

      int code = taosMkDir(option, 0755);
      if (code != 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        uError("config option:%s, input value:%s, directory not exist, create fail:%s", cfg->option, input_value,
               strerror(errno));
        return false;
      }
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], option);
    }
  }

  return true;
}

static bool taosReadIpStrConfig(SGlobalCfg *cfg, char *input_value) {
  uint32_t value = taosInetAddr(input_value);
  char *   option = (char *)cfg->ptr;
  if (value == INADDR_NONE) {
    uError("config option:%s, input value:%s, is not a valid ip address, use default value:%s",
           cfg->option, input_value, option);
    return false;
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      strncpy(option, input_value, cfg->ptrLength);
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], option);
      return false;
    }
  }
  return true;
}

static bool taosReadStringConfig(SGlobalCfg *cfg, char *input_value) {
  int   length = (int) strlen(input_value);
  char *option = (char *)cfg->ptr;
  if (length <= 0 || length > cfg->ptrLength) {
    uError("config option:%s, input value:%s, length out of range[0, %d], use default value:%s",
           cfg->option, input_value, cfg->ptrLength, option);
    return false;
  } else {
    if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_FILE) {
      strncpy(option, input_value, cfg->ptrLength);
      cfg->cfgStatus = TAOS_CFG_CSTATUS_FILE;
    } else {
      uWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, input_value,
            tsCfgStatusStr[cfg->cfgStatus], option);
      return false;
    }
  }
  return true;
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

bool taosReadConfigOption(const char *option, char *value, char *value2, char *value3,
                          int8_t cfgStatus, int8_t sourceType) {
  bool ret = false;
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_CONFIG)) continue;
    if (sourceType != 0 && !(cfg->cfgType & sourceType)) continue;
    if (strcasecmp(cfg->option, option) != 0) continue;

    switch (cfg->valType) {
      case TAOS_CFG_VTYPE_INT8:
        ret = taosReadInt8Config(cfg, value);
        break;
      case TAOS_CFG_VTYPE_INT16:
        ret = taosReadInt16Config(cfg, value);
        break;
      case TAOS_CFG_VTYPE_INT32:
        ret = taosReadInt32Config(cfg, value);
        break;
      case TAOS_CFG_VTYPE_UINT16:
        ret = taosReadUInt16Config(cfg, value);
        break;
      case TAOS_CFG_VTYPE_FLOAT:
        ret = taosReadFloatConfig(cfg, value);
        break;
      case TAOS_CFG_VTYPE_DOUBLE:
        ret = taosReadDoubleConfig(cfg, value);
        break;
      case TAOS_CFG_VTYPE_STRING:
        ret = taosReadStringConfig(cfg, value);
        break;
      case TAOS_CFG_VTYPE_IPSTR:
        ret = taosReadIpStrConfig(cfg, value);
        break;
      case TAOS_CFG_VTYPE_DIRECTORY:
        ret = taosReadDirectoryConfig(cfg, value);
        break;
      case TAOS_CFG_VTYPE_DATA_DIRCTORY:
        if (taosReadDirectoryConfig(cfg, value)) {
           taosReadDataDirCfg(value, value2, value3);
           ret = true;
        } else {
           ret = false;  
        }
        break;
      default:
        uError("config option:%s, input value:%s, can't be recognized", option, value);
        ret = false;
    }
    if(ret && cfgStatus == TAOS_CFG_CSTATUS_OPTION){
      cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
    }
  }
  return ret;
}

void taosInitConfigOption(SGlobalCfg cfg) {
  tsGlobalConfig[tsGlobalConfigNum++] = cfg;
}

void taosReadGlobalLogCfg() {
  FILE * fp;
  char * line, *option, *value;
  int    olen, vlen;
  char   fileName[PATH_MAX] = {0};

  wordexp_t full_path;
  if ( 0 != wordexp(configDir, &full_path, 0)) {
    printf("\nconfig file: %s wordexp fail! reason:%s\n", configDir, strerror(errno));
    wordfree(&full_path);
    return;
  }
  
  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {    
    if (strlen(full_path.we_wordv[0]) >= TSDB_FILENAME_LEN) {
      printf("\nconfig file: %s path overflow max len %d, all variables are set to default\n", full_path.we_wordv[0], TSDB_FILENAME_LEN - 1);
      wordfree(&full_path);
      return;
    }
    strcpy(configDir, full_path.we_wordv[0]);
  } else {
    printf("configDir:%s not there, use default value: /etc/taos", configDir);
    strcpy(configDir, "/etc/taos");
  }
  wordfree(&full_path);

  taosReadLogOption("logDir", tsLogDir);
  
  sprintf(fileName, "%s/taos.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
    printf("\nconfig file:%s not found, all variables are set to default\n", fileName);
    return;
  }

  ssize_t _bytes = 0;
  size_t len = 1024;
  line = calloc(1, len);
  
  while (!feof(fp)) {
    memset(line, 0, len);
    
    option = value = NULL;
    olen = vlen = 0;

    _bytes = tgetline(&line, &len, fp);
    if (_bytes < 0)
    {
      break;
    }

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
  char * line, *option, *value, *value2, *value3;
  int    olen, vlen, vlen2, vlen3;
  char   fileName[PATH_MAX] = {0};

  sprintf(fileName, "%s/taos.cfg", configDir);
  FILE* fp = fopen(fileName, "r");
  if (fp == NULL) {
    struct stat s;
    if (stat(configDir, &s) != 0 || (!S_ISREG(s.st_mode) && !S_ISLNK(s.st_mode))) {
      //return true to follow behavior before file support
      return true;
    }
    fp = fopen(configDir, "r");
    if (fp == NULL) {
      return false;
    }
  }

  ssize_t _bytes = 0;
  size_t len = 1024;
  line = calloc(1, len);
  
  while (!feof(fp)) {
    memset(line, 0, len);

    option = value = value2 = value3 = NULL;
    olen = vlen = vlen2 = vlen3 = 0;

    _bytes = tgetline(&line, &len, fp);
    if (_bytes < 0)
    {
      break;
    }

    line[len - 1] = 0;
    
    paGetToken(line, &option, &olen);
    if (olen == 0) continue;
    option[olen] = 0;

    paGetToken(option + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    paGetToken(value + vlen + 1, &value2, &vlen2);
    if (vlen2 != 0) {
      value2[vlen2] = 0;
      paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
      if (vlen3 != 0) value3[vlen3] = 0;
    }

    taosReadConfigOption(option, value, value2, value3, TAOS_CFG_CSTATUS_FILE, 0);
  }

  fclose(fp);

  tfree(line);

  if (debugFlag & DEBUG_TRACE || debugFlag & DEBUG_DEBUG || debugFlag & DEBUG_DUMP) {
    taosSetAllDebugFlag();
  }

  return true;
}

void taosPrintGlobalCfg() {
  uInfo("   taos config & system info:");
  uInfo("==================================");

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
      case TAOS_CFG_VTYPE_INT8:
        uInfo(" %s:%s%d%s", cfg->option, blank, *((int8_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_INT16:
        uInfo(" %s:%s%d%s", cfg->option, blank, *((int16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_INT32:
        uInfo(" %s:%s%d%s", cfg->option, blank, *((int32_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_UINT16:
        uInfo(" %s:%s%d%s", cfg->option, blank, *((uint16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_FLOAT:
        uInfo(" %s:%s%f%s", cfg->option, blank, *((float *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_DOUBLE:
        uInfo(" %s:%s%f%s", cfg->option, blank, *((double *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_STRING:
      case TAOS_CFG_VTYPE_IPSTR:
      case TAOS_CFG_VTYPE_DIRECTORY:
        uInfo(" %s:%s%s%s", cfg->option, blank, (char *)cfg->ptr, tsGlobalUnit[cfg->unitType]);
        break;
      default:
        break;
    }
  }

  taosPrintOsInfo();
  taosPrintDataDirCfg();
  uInfo("==================================");
}

static void taosDumpCfg(SGlobalCfg *cfg) {
    int optionLen = (int)strlen(cfg->option);
    int blankLen = TSDB_CFG_PRINT_LEN - optionLen;
    blankLen = blankLen < 0 ? 0 : blankLen;

    char blank[TSDB_CFG_PRINT_LEN];
    memset(blank, ' ', TSDB_CFG_PRINT_LEN);
    blank[blankLen] = 0;

    switch (cfg->valType) {
      case TAOS_CFG_VTYPE_INT8:
        printf(" %s:%s%d%s\n", cfg->option, blank, *((int8_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_INT16:
        printf(" %s:%s%d%s\n", cfg->option, blank, *((int16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_INT32:
        printf(" %s:%s%d%s\n", cfg->option, blank, *((int32_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_UINT16:
        printf(" %s:%s%d%s\n", cfg->option, blank, *((uint16_t *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_FLOAT:
        printf(" %s:%s%f%s\n", cfg->option, blank, *((float *)cfg->ptr), tsGlobalUnit[cfg->unitType]);
        break;
      case TAOS_CFG_VTYPE_STRING:
      case TAOS_CFG_VTYPE_IPSTR:
      case TAOS_CFG_VTYPE_DIRECTORY:
        printf(" %s:%s%s%s\n", cfg->option, blank, (char *)cfg->ptr, tsGlobalUnit[cfg->unitType]);
        break;
      default:
        break;
    }
}

void taosDumpGlobalCfg() {
  printf("     global config:\n");
  printf("==================================\n");
  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (tscEmbedded == 0 && !(cfg->cfgType & TSDB_CFG_CTYPE_B_CLIENT)) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_NOT_PRINT) continue;
    if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_SHOW)) continue;

    taosDumpCfg(cfg);
  }

  printf("\n     local config:\n");
  printf("==================================\n");

  for (int i = 0; i < tsGlobalConfigNum; ++i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (tscEmbedded == 0 && !(cfg->cfgType & TSDB_CFG_CTYPE_B_CLIENT)) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_NOT_PRINT) continue;
    if (cfg->cfgType & TSDB_CFG_CTYPE_B_SHOW) continue;

    taosDumpCfg(cfg);
  }
}
