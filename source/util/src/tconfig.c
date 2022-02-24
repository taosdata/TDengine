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
#include "tconfig.h"
#include "taoserror.h"
#include "thash.h"
#include "tutil.h"
#include "ulog.h"

#define CFG_NAME_PRINT_LEN 24
#define CFG_SRC_PRINT_LEN  12

typedef struct SConfig {
  ECfgSrcType stype;
  SHashObj   *hash;
} SConfig;

int32_t cfgLoadFromCfgFile(SConfig *pConfig, const char *filepath);
int32_t cfgLoadFromEnvFile(SConfig *pConfig, const char *filepath);
int32_t cfgLoadFromEnvVar(SConfig *pConfig);
int32_t cfgLoadFromApollUrl(SConfig *pConfig, const char *url);
int32_t cfgSetItem(SConfig *pConfig, const char *name, const char *value, ECfgSrcType stype);

SConfig *cfgInit() {
  SConfig *pCfg = calloc(1, sizeof(SConfig));
  if (pCfg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pCfg->hash = taosHashInit(16, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pCfg->hash == NULL) {
    free(pCfg);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  return pCfg;
}

int32_t cfgLoad(SConfig *pCfg, ECfgSrcType cfgType, const char *sourceStr) {
  switch (cfgType) {
    case CFG_STYPE_CFG_FILE:
      return cfgLoadFromCfgFile(pCfg, sourceStr);
    case CFG_STYPE_ENV_FILE:
      return cfgLoadFromEnvFile(pCfg, sourceStr);
    case CFG_STYPE_ENV_VAR:
      return cfgLoadFromEnvVar(pCfg);
    case CFG_STYPE_APOLLO_URL:
      return cfgLoadFromApollUrl(pCfg, sourceStr);
    default:
      return -1;
  }
}

void cfgCleanup(SConfig *pCfg) {
  if (pCfg != NULL) {
    if (pCfg->hash != NULL) {
      taosHashCleanup(pCfg->hash);
      pCfg->hash == NULL;
    }
    free(pCfg);
  }
}

int32_t cfgGetSize(SConfig *pCfg) { return taosHashGetSize(pCfg->hash); }

SConfigItem *cfgIterate(SConfig *pCfg, SConfigItem *pIter) { return taosHashIterate(pCfg->hash, pIter); }

void cfgCancelIterate(SConfig *pCfg, SConfigItem *pIter) { return taosHashCancelIterate(pCfg->hash, pIter); }

static int32_t cfgCheckAndSetTimezone(SConfigItem *pItem, const char *timezone) {
  tfree(pItem->str);
  pItem->str = strdup(timezone);
  if (pItem->str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int32_t cfgCheckAndSetCharset(SConfigItem *pItem, const char *charset) {
  tfree(pItem->str);
  pItem->str = strdup(charset);
  if (pItem->str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int32_t cfgCheckAndSetLocale(SConfigItem *pItem, const char *locale) {
  tfree(pItem->str);
  pItem->str = strdup(locale);
  if (pItem->str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int32_t cfgCheckAndSetDir(SConfigItem *pItem, const char *inputDir) {
  char fullDir[PATH_MAX] = {0};
  if (taosExpandDir(inputDir, fullDir, PATH_MAX) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to expand dir:%s since %s", inputDir, terrstr());
    return -1;
  }

  if (taosRealPath(fullDir, PATH_MAX) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to get realpath of dir:%s since %s", inputDir, terrstr());
    return -1;
  }

  if (taosMkDir(fullDir) != 0) {
    uError("failed to create dir:%s realpath:%s since %s", inputDir, fullDir, terrstr());
    return -1;
  }

  tfree(pItem->str);
  pItem->str = strdup(fullDir);
  if (pItem->str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int32_t cfgSetBool(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  bool tmp = false;
  if (strcasecmp(value, "true") == 0) {
    tmp = true;
  }
  if (atoi(value) > 0) {
    tmp = true;
  }

  pItem->bval = tmp;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetInt32(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int32_t ival = (int32_t)atoi(value);
  if (ival < pItem->imin || ival > pItem->imax) {
    uError("cfg:%s, type:%s src:%s value:%d out of range[%" PRId64 ", %" PRId64 "], use last src:%s value:%d",
           pItem->name, cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->imin, pItem->imax,
           cfgStypeStr(pItem->stype), pItem->i32);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  pItem->i32 = ival;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetInt64(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int64_t ival = (int64_t)atoi(value);
  if (ival < pItem->imin || ival > pItem->imax) {
    uError("cfg:%s, type:%s src:%s value:%" PRId64 " out of range[%" PRId64 ", %" PRId64
           "], use last src:%s value:%" PRId64,
           pItem->name, cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->imin, pItem->imax,
           cfgStypeStr(pItem->stype), pItem->i64);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  pItem->i64 = ival;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetFloat(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  float fval = (float)atof(value);
  if (fval < pItem->fmin || fval > pItem->fmax) {
    uError("cfg:%s, type:%s src:%s value:%f out of range[%f, %f], use last src:%s value:%f", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), fval, pItem->fmin, pItem->fmax, cfgStypeStr(pItem->stype),
           pItem->fval);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  pItem->fval = fval;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetString(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = strdup(value);
  if (tmp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s, use last src:%s value:%s", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), value, terrstr(), cfgStypeStr(pItem->stype), pItem->str);
    return -1;
  }

  free(pItem->str);
  pItem->str = tmp;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetDir(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = strdup(value);
  if (tmp == NULL || cfgCheckAndSetDir(pItem, value) != 0) {
    free(tmp);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s, use last src:%s value:%s", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), value, terrstr(), cfgStypeStr(pItem->stype), pItem->str);
    return -1;
  }

  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetLocale(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = strdup(value);
  if (tmp == NULL || cfgCheckAndSetLocale(pItem, value) != 0) {
    free(tmp);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s, use last src:%s value:%s", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), value, terrstr(), cfgStypeStr(pItem->stype), pItem->str);
    return -1;
  }

  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetCharset(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = strdup(value);
  if (tmp == NULL || cfgCheckAndSetCharset(pItem, value) != 0) {
    free(tmp);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s, use last src:%s value:%s", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), value, terrstr(), cfgStypeStr(pItem->stype), pItem->str);
    return -1;
  }

  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetTimezone(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = strdup(value);
  if (tmp == NULL || cfgCheckAndSetTimezone(pItem, value) != 0) {
    free(tmp);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s, use last src:%s value:%s", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), value, terrstr(), cfgStypeStr(pItem->stype), pItem->str);
    return -1;
  }

  pItem->stype = stype;
  return 0;
}

int32_t cfgSetItem(SConfig *pCfg, const char *name, const char *value, ECfgSrcType stype) {
  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (pItem == NULL) {
    return -1;
  }

  switch (pItem->dtype) {
    case CFG_DTYPE_BOOL:
      return cfgSetBool(pItem, value, stype);
    case CFG_DTYPE_INT32:
      return cfgSetInt32(pItem, value, stype);
    case CFG_DTYPE_INT64:
      return cfgSetInt64(pItem, value, stype);
    case CFG_DTYPE_FLOAT:
      return cfgSetFloat(pItem, value, stype);
    case CFG_DTYPE_STRING:
      return cfgSetString(pItem, value, stype);
    case CFG_DTYPE_DIR:
      return cfgSetDir(pItem, value, stype);
    case CFG_DTYPE_TIMEZONE:
      return cfgSetTimezone(pItem, value, stype);
    case CFG_DTYPE_CHARSET:
      return cfgSetCharset(pItem, value, stype);
    case CFG_DTYPE_LOCALE:
      return cfgSetLocale(pItem, value, stype);
    case CFG_DTYPE_NONE:
    default:
      break;
  }

  terrno = TSDB_CODE_INVALID_CFG;
  return -1;
}

SConfigItem *cfgGetItem(SConfig *pCfg, const char *name) {
  char lowcaseName[CFG_NAME_MAX_LEN + 1] = {0};
  memcpy(lowcaseName, name, CFG_NAME_MAX_LEN);
  strntolower(lowcaseName, name, CFG_NAME_MAX_LEN);

  SConfigItem *pItem = taosHashGet(pCfg->hash, lowcaseName, strlen(lowcaseName) + 1);
  if (pItem == NULL) {
    terrno = TSDB_CODE_CFG_NOT_FOUND;
  }

  return pItem;
}

static int32_t cfgAddItem(SConfig *pCfg, SConfigItem *pItem, const char *name) {
  pItem->stype = CFG_STYPE_DEFAULT;
  pItem->name = strdup(name);
  if (pItem->name == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  char lowcaseName[CFG_NAME_MAX_LEN + 1] = {0};
  memcpy(lowcaseName, name, CFG_NAME_MAX_LEN);
  strntolower(lowcaseName, name, CFG_NAME_MAX_LEN);

  if (taosHashPut(pCfg->hash, lowcaseName, strlen(lowcaseName) + 1, pItem, sizeof(SConfigItem)) != 0) {
    if (pItem->dtype == CFG_DTYPE_STRING) {
      free(pItem->str);
    }
    free(pItem->name);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int32_t cfgAddBool(SConfig *pCfg, const char *name, bool defaultVal, bool tsc) {
  SConfigItem item = {.dtype = CFG_DTYPE_BOOL, .bval = defaultVal, .tsc = tsc};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddInt32(SConfig *pCfg, const char *name, int32_t defaultVal, int64_t minval, int64_t maxval, bool tsc) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_INT32, .i32 = defaultVal, .imin = minval, .imax = maxval, .tsc = tsc};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddInt64(SConfig *pCfg, const char *name, int64_t defaultVal, int64_t minval, int64_t maxval, bool tsc) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_INT64, .i64 = defaultVal, .imin = minval, .imax = maxval, .tsc = tsc};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddFloat(SConfig *pCfg, const char *name, float defaultVal, double minval, double maxval, bool tsc) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_FLOAT, .fval = defaultVal, .fmin = minval, .fmax = maxval, .tsc = tsc};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddString(SConfig *pCfg, const char *name, const char *defaultVal, bool tsc) {
  SConfigItem item = {.dtype = CFG_DTYPE_STRING, .tsc = tsc};
  item.str = strdup(defaultVal);
  if (item.str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddDir(SConfig *pCfg, const char *name, const char *defaultVal, bool tsc) {
  SConfigItem item = {.dtype = CFG_DTYPE_DIR, .tsc = tsc};
  if (cfgCheckAndSetDir(&item, defaultVal) != 0) {
    return -1;
  }

  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddLocale(SConfig *pCfg, const char *name, const char *defaultVal) {
  SConfigItem item = {.dtype = CFG_DTYPE_LOCALE, .tsc = 1};
  if (cfgCheckAndSetLocale(&item, defaultVal) != 0) {
    return -1;
  }

  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddCharset(SConfig *pCfg, const char *name, const char *defaultVal) {
  SConfigItem item = {.dtype = CFG_DTYPE_CHARSET, .tsc = 1};
  if (cfgCheckAndSetCharset(&item, defaultVal) != 0) {
    return -1;
  }

  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddTimezone(SConfig *pCfg, const char *name, const char *defaultVal) {
  SConfigItem item = {.dtype = CFG_DTYPE_TIMEZONE, .tsc = 1};
  if (cfgCheckAndSetTimezone(&item, defaultVal) != 0) {
    return -1;
  }

  return cfgAddItem(pCfg, &item, name);
}

const char *cfgStypeStr(ECfgSrcType type) {
  switch (type) {
    case CFG_STYPE_DEFAULT:
      return "default";
    case CFG_STYPE_CFG_FILE:
      return "cfg_file";
    case CFG_STYPE_ENV_FILE:
      return "env_file";
    case CFG_STYPE_ENV_VAR:
      return "env_var";
    case CFG_STYPE_APOLLO_URL:
      return "apollo_url";
    case CFG_STYPE_ARG_LIST:
      return "arg_list";
    case CFG_STYPE_API_OPTION:
      return "api_option";
    default:
      return "invalid";
  }
}

const char *cfgDtypeStr(ECfgDataType type) {
  switch (type) {
    case CFG_DTYPE_NONE:
      return "none";
    case CFG_DTYPE_BOOL:
      return "bool";
    case CFG_DTYPE_INT32:
      return "int32";
    case CFG_DTYPE_INT64:
      return "int64";
    case CFG_DTYPE_FLOAT:
      return "float";
    case CFG_DTYPE_STRING:
      return "string";
    case CFG_DTYPE_DIR:
      return "dir";
    case CFG_DTYPE_LOCALE:
      return "locale";
    case CFG_DTYPE_CHARSET:
      return "charset";
    case CFG_DTYPE_TIMEZONE:
      return "timezone";
    default:
      return "invalid";
  }
}

void cfgDumpCfg(SConfig *pCfg, bool tsc, bool dump) {
  if (dump) {
    printf("   global config");
    printf("\n");
    printf("=================================================================");
    printf("\n");
  } else {
    uInfo("   global config");
    uInfo("=================================================================");
  }

  char src[CFG_SRC_PRINT_LEN + 1] = {0};
  char name[CFG_NAME_PRINT_LEN + 1] = {0};

  SConfigItem *pItem = cfgIterate(pCfg, NULL);
  while (pItem != NULL) {
    if (tsc && !pItem->tsc) continue;
    tstrncpy(src, cfgStypeStr(pItem->stype), CFG_SRC_PRINT_LEN);
    for (int32_t i = 0; i < CFG_SRC_PRINT_LEN; ++i) {
      if (src[i] == 0) src[i] = ' ';
    }

    tstrncpy(name, pItem->name, CFG_NAME_PRINT_LEN);
    for (int32_t i = 0; i < CFG_NAME_PRINT_LEN; ++i) {
      if (name[i] == 0) name[i] = ' ';
    }

    switch (pItem->dtype) {
      case CFG_DTYPE_BOOL:
        if (dump) {
          printf("%s %s %u", src, name, pItem->bval);
          printf("\n");
        } else {
          uInfo("%s %s %u", src, name, pItem->bval);
        }

        break;
      case CFG_DTYPE_INT32:
        if (dump) {
          printf("%s %s %d", src, name, pItem->i32);
          printf("\n");
        } else {
          uInfo("%s %s %d", src, name, pItem->i32);
        }
        break;
      case CFG_DTYPE_INT64:
        if (dump) {
          printf("%s %s %" PRId64, src, name, pItem->i64);
          printf("\n");
        } else {
          uInfo("%s %s %" PRId64, src, name, pItem->i64);
        }
        break;
      case CFG_DTYPE_FLOAT:
        if (dump) {
          printf("%s %s %f", src, name, pItem->fval);
          printf("\n");
        } else {
          uInfo("%s %s %f", src, name, pItem->fval);
        }
        break;
      case CFG_DTYPE_STRING:
      case CFG_DTYPE_DIR:
      case CFG_DTYPE_LOCALE:
      case CFG_DTYPE_CHARSET:
      case CFG_DTYPE_TIMEZONE:
        if (dump) {
          printf("%s %s %s", src, name, pItem->str);
          printf("\n");
        } else {
          uInfo("%s %s %s", src, name, pItem->str);
        }
        break;
    }
    pItem = cfgIterate(pCfg, pItem);
  }

  if (dump) {
    printf("=================================================================");
    printf("\n");
  } else {
    uInfo("=================================================================");
  }
}

int32_t cfgLoadFromEnvVar(SConfig *pConfig) {
  uInfo("load from global env variables");
  return 0;
}

int32_t cfgLoadFromEnvFile(SConfig *pConfig, const char *filepath) {
  uInfo("load from env file %s", filepath);
  return 0;
}

int32_t cfgLoadFromCfgFile(SConfig *pConfig, const char *filepath) {
  char   *line, *name, *value, *value2, *value3;
  int     olen, vlen, vlen2, vlen3;
  ssize_t _bytes = 0;
  size_t  len = 1024;

  FILE *fp = fopen(filepath, "r");
  if (fp == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  line = malloc(len);

  while (!feof(fp)) {
    memset(line, 0, len);

    name = value = value2 = value3 = NULL;
    olen = vlen = vlen2 = vlen3 = 0;

    _bytes = tgetline(&line, &len, fp);
    if (_bytes < 0) {
      break;
    }

    line[len - 1] = 0;

    paGetToken(line, &name, &olen);
    if (olen == 0) continue;
    name[olen] = 0;

    paGetToken(name + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    paGetToken(value + vlen + 1, &value2, &vlen2);
    if (vlen2 != 0) {
      value2[vlen2] = 0;
      paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
      if (vlen3 != 0) value3[vlen3] = 0;
    }

    cfgSetItem(pConfig, name, value, CFG_STYPE_CFG_FILE);
    // taosReadConfigOption(name, value, value2, value3);
  }

  fclose(fp);
  tfree(line);

  uInfo("load from cfg file %s success", filepath);
  return 0;
}

int32_t cfgLoadFromApollUrl(SConfig *pConfig, const char *url) {
  uInfo("load from apoll url %s", url);
  return 0;
}