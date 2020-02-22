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
#include "taos.h"
#include "tlog.h"

#include "dnode.h"
#include "dnodeModule.h"
#include "dclusterSystem.h"
#include "dclusterMgmt.h"

int32_t  dnodeCheckConfig();
char *grantGetMachineSerials();

static bool tsClusterExist = false;

bool dclusterIsClusterExist() {
  return tsClusterExist;
}

int dnodeCheckSystemClusterImp() {
  char cfgFile[256];
  sprintf(cfgFile, "%s/taos.cfg", configDir);
  grantActiveSystem(cfgFile);

  /*
   * The cluster may not have a master, so the command may always be in progress
   */
  /*
  if (dnodeCheckConfig() != 0) {
    dError("TDengine initialization failed");
    return -1;
  }
  */

  return 0;
}

void dclusterStartModules() {}

void dclusterParseParameterK() {
  char *key = grantGetMachineSerials();
  if (key != NULL) {
    fprintf(stdout, "machine code: %s \n", key);
  } else {
    fprintf(stderr, "should generate machine code under root authority!\n");
  }
  exit(EXIT_SUCCESS);
}

int32_t dnodeCheckConfig() {
  taos_init();

  if (strcmp(tsMasterIp, tsPrivateIp) == 0 || strcmp(tsMasterIp, tsPublicIp) == 0 ||
      strcmp(tsMasterIp, tsPrivateIp) == 0) {
    return 0;
  }

  TAOS *con = taos_connect(NULL, "monitor", tsInternalPass, "", 0);
  if (con == NULL) {
    dTrace("connect to cluster failed, cluster not exist");
    return 0;
  }

  // dTrace("connect to cluster success, cluster exist");
  // tsClusterExist = true;

  if (taos_query(con, "show configs") != 0) {
    dError("can't read config from cluster");
    taos_close(con);
    return 0;
  }

  TAOS_RES *result = taos_use_result(con);
  if (result == NULL) {
    dError("config query result is null");
    taos_close(con);
    return 0;
  }

  int         num_fields = taos_field_count(con);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  if (num_fields != 2 || fields[0].type != TSDB_DATA_TYPE_BINARY || fields[1].type != TSDB_DATA_TYPE_BINARY) {
    dError("config query fields invalid, num_fields:%d", num_fields);
    taos_close(con);
    return -1;
  }

  int check = 0;

  TAOS_ROW row;
  while ((row = taos_fetch_row(result))) {
    char *configName = (char *)row[0];
    char *configValue = (char *)row[1];

    for (int i = 0; i < tsGlobalConfigNum; ++i) {
      SGlobalConfig *cfg = tsGlobalConfig + i;
      if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_CONFIG)) continue;
      if (strcasecmp(configName, cfg->option) != 0) continue;

      switch (cfg->valType) {
        case TSDB_CFG_VTYPE_SHORT:
          if ((int16_t)atoi(configValue) != *((int16_t *)cfg->ptr)) {
            dError("config:%s from cluster:%d different from cfgfile:%d", cfg->option, (int16_t)atoi(configValue),
                   *((int16_t *)cfg->ptr));
            check = -1;
          }
          break;
        case TSDB_CFG_VTYPE_INT:
          if ((int32_t)atoi(configValue) != *((int32_t *)cfg->ptr)) {
            dError("config:%s from cluster:%d different from cfgfile:%d", cfg->option, (int32_t)atoi(configValue),
                   *((int32_t *)cfg->ptr));
            check = -1;
          }
          break;
        case TSDB_CFG_VTYPE_UINT:
          if ((uint32_t)atoi(configValue) != *((uint32_t *)cfg->ptr)) {
            dError("config:%s from cluster:%d different from cfgfile:%d", cfg->option, (uint32_t)atoi(configValue),
                   *((uint32_t *)cfg->ptr));
            check = -1;
          }
          break;
        case TSDB_CFG_VTYPE_FLOAT:
          if ((atof(configValue) - *((float *)cfg->ptr)) > 0.01) {
            dError("config:%s from cluster:%f different from cfgfile:%f", cfg->option, atof(configValue),
                   *((float *)cfg->ptr));
            check = -1;
          }
          break;
        case TSDB_CFG_VTYPE_STRING:
          if (strcmp(configValue, (char *)cfg->ptr) != 0) {
            dError("config:%s from cluster:%s different from cfgfile:%s", cfg->option, configValue, (char *)cfg->ptr);
            check = -1;
          }
          break;
        default:
          break;
      }
      break;
    }
  }

  taos_free_result(result);
  taos_close(con);

  return check;
}

void dnodeClusterInit() {
  dnodeParseParameterK = dclusterParseParameterK;
  dnodeStartModules = dclusterStartModules;
}
