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

#ifndef _TDB_ENV_H_
#define _TDB_ENV_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STEnv {
  char *   rootDir;
  char *   jfname;
  int      jfd;
  SPCache *pCache;
} STEnv;

int tdbEnvOpen(const char *rootDir, int pageSize, int cacheSize, STEnv **ppEnv);
int tdbEnvClose(STEnv *pEnv);

SPager *tdbEnvGetPager(STEnv *pEnv, const char *fname);

#ifdef __cplusplus
}
#endif

#endif /*_TDB_ENV_H_*/