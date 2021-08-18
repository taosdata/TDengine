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

static const char* expand_like_shell(const char *path) {
  static __thread char buf[TSDB_FILENAME_LEN];
  buf[0] = '\0';
  wordexp_t we;
  if (wordexp(path, &we, 0)) return "/tmp/taosd";
  if (sizeof(buf)<=snprintf(buf, sizeof(buf), "%s", we.we_wordv[0])) return "/tmp/taosd";
  wordfree(&we);
  return buf;
}

void osInit() {
  if (configDir[0] == 0) {
    strcpy(configDir, expand_like_shell("/usr/local/etc/taos"));
  }
  strcpy(tsDataDir,   expand_like_shell("/usr/local/var/lib/taos"));
  strcpy(tsLogDir,    expand_like_shell("/usr/local/var/log/taos"));
  strcpy(tsScriptDir, expand_like_shell("/usr/local/etc/taos"));

  strcpy(tsVnodeDir, "");
  strcpy(tsDnodeDir, "");
  strcpy(tsMnodeDir, "");
  strcpy(tsOsName, "Darwin");
}

