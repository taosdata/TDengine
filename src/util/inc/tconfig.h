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

#ifndef TDENGINE_CFG_H
#define TDENGINE_CFG_H

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_CFG_MAX_NUM    110
#define TSDB_CFG_PRINT_LEN  23
#define TSDB_CFG_OPTION_LEN 24
#define TSDB_CFG_VALUE_LEN  41

#define TSDB_CFG_CTYPE_B_CONFIG    1U   // can be configured from file
#define TSDB_CFG_CTYPE_B_SHOW      2U   // can displayed by "show configs" commands
#define TSDB_CFG_CTYPE_B_LOG       4U   // is a log type configuration
#define TSDB_CFG_CTYPE_B_CLIENT    8U   // can be displayed in the client log
#define TSDB_CFG_CTYPE_B_OPTION    16U  // can be configured by taos_options function
#define TSDB_CFG_CTYPE_B_NOT_PRINT 32U  // such as password

enum {
  TAOS_CFG_CSTATUS_NONE,     // not configured
  TAOS_CFG_CSTATUS_DEFAULT,  // use system default value
  TAOS_CFG_CSTATUS_FILE,     // configured from file
  TAOS_CFG_CSTATUS_OPTION,   // configured by taos_options function
  TAOS_CFG_CSTATUS_ARG,      // configured by program argument
};

enum {
  TAOS_CFG_VTYPE_INT16,
  TAOS_CFG_VTYPE_INT32,
  TAOS_CFG_VTYPE_FLOAT,
  TAOS_CFG_VTYPE_STRING,
  TAOS_CFG_VTYPE_IPSTR,
  TAOS_CFG_VTYPE_DIRECTORY,
};

enum {
  TAOS_CFG_UTYPE_NONE,
  TAOS_CFG_UTYPE_PERCENT,
  TAOS_CFG_UTYPE_GB,
  TAOS_CFG_UTYPE_MB,
  TAOS_CFG_UTYPE_BYTE,
  TAOS_CFG_UTYPE_SECOND,
  TAOS_CFG_UTYPE_MS
};

typedef struct {
  char *   option;
  void *   ptr;
  float    minValue;
  float    maxValue;
  int8_t   cfgType;
  int8_t   cfgStatus;
  int8_t   unitType;
  int8_t   valType;
  int32_t  ptrLength;
} SGlobalCfg;

extern SGlobalCfg tsGlobalConfig[];
extern int32_t    tsGlobalConfigNum;
extern char *     tsCfgStatusStr[];

void taosReadGlobalLogCfg();
bool taosReadGlobalCfg();
void taosPrintGlobalCfg();

void taosInitConfigOption(SGlobalCfg cfg);
SGlobalCfg * taosGetConfigOption(const char *option);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TGLOBALCFG_H
