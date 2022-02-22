
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

#ifndef _TD_CONFIG_H_
#define _TD_CONFIG_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  CFG_STYPE_DEFAULT,
  CFG_STYPE_CFG_FILE,
  CFG_STYPE_ENV_FILE,
  CFG_STYPE_ENV_VAR,
  CFG_STYPE_APOLLO_URL,
  CFG_STYPE_ARG_LIST,
  CFG_STYPE_API_OPTION
} ECfgSrcType;

typedef enum {
  CFG_DTYPE_NONE,
  CFG_DTYPE_BOOL,
  CFG_DTYPE_INT8,
  CFG_DTYPE_UINT16,
  CFG_DTYPE_INT32,
  CFG_DTYPE_INT64,
  CFG_DTYPE_FLOAT,
  CFG_DTYPE_STRING,
  CFG_DTYPE_IPSTR,
  CFG_DTYPE_DIR,
} ECfgDataType;

typedef enum {
  CFG_UTYPE_NONE,
  CFG_UTYPE_PERCENT,
  CFG_UTYPE_GB,
  CFG_UTYPE_MB,
  CFG_UTYPE_BYTE,
  CFG_UTYPE_SECOND,
  CFG_UTYPE_MS
} ECfgUnitType;

typedef struct SConfigItem {
  ECfgSrcType  stype;
  ECfgUnitType utype;
  ECfgDataType dtype;
  char        *name;
  union {
    bool     boolVal;
    int8_t   int8Val;
    uint16_t uint16Val;
    int32_t  int32Val;
    int64_t  int64Val;
    float    floatVal;
    char    *strVal;
    char    *ipstrVal;
    char    *dirVal;
  };
  union {
    int64_t minIntVal;
    double  minFloatVal;
  };
  union {
    int64_t maxIntVal;
    double  maxFloatVal;
  };

} SConfigItem;

typedef struct SConfig SConfig;

SConfig *cfgInit();
int32_t  cfgLoad(SConfig *pConfig, ECfgSrcType cfgType, const char *sourceStr);
void     cfgCleanup(SConfig *pConfig);

int32_t      cfgGetSize(SConfig *pConfig);
SConfigItem *cfgIterate(SConfig *pConfig, SConfigItem *pIter);
void         cfgCancelIterate(SConfig *pConfig, SConfigItem *pIter);
SConfigItem *cfgGetItem(SConfig *pConfig, const char *name);

int32_t cfgAddBool(SConfig *pConfig, const char *name, bool defaultVal, ECfgUnitType utype);
int32_t cfgAddInt8(SConfig *pConfig, const char *name, int8_t defaultVal, int64_t minval, int64_t maxval,
                   ECfgUnitType utype);
int32_t cfgAddUInt16(SConfig *pConfig, const char *name, uint16_t defaultVal, int64_t minval, int64_t maxval,
                     ECfgUnitType utype);
int32_t cfgAddInt32(SConfig *pConfig, const char *name, int32_t defaultVal, int64_t minval, int64_t maxval,
                    ECfgUnitType utype);
int32_t cfgAddInt64(SConfig *pConfig, const char *name, int64_t defaultVal, int64_t minval, int64_t maxval,
                    ECfgUnitType utype);
int32_t cfgAddFloat(SConfig *pConfig, const char *name, float defaultVal, double minval, double maxval,
                    ECfgUnitType utype);
int32_t cfgAddString(SConfig *pConfig, const char *name, const char *defaultVal, ECfgUnitType utype);
int32_t cfgAddIpStr(SConfig *pConfig, const char *name, const char *defaultVal, ECfgUnitType utype);
int32_t cfgAddDir(SConfig *pConfig, const char *name, const char *defaultVal, ECfgUnitType utype);

const char *cfgStypeStr(ECfgSrcType type);
const char *cfgDtypeStr(ECfgDataType type);
const char *cfgUtypeStr(ECfgUnitType type);

#ifdef __cplusplus
}
#endif

#endif /*_TD_CONFIG_H_*/
