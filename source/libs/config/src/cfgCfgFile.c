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
#include "cfgInt.h"

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