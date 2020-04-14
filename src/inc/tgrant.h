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

#ifndef TDENGINE_GTANT_H
#define TDENGINE_GTANT_H

#ifdef __cplusplus
"C" {
#endif

typedef enum {
  TSDB_GRANT_ALL,
  TSDB_GRANT_TIME,
  TSDB_GRANT_USER,
  TSDB_GRANT_DB,
  TSDB_GRANT_TIMESERIES,
  TSDB_GRANT_DNODE,
  TSDB_GRANT_ACCT,
  TSDB_GRANT_STORAGE,
  TSDB_GRANT_SPEED,
  TSDB_GRANT_QUERY_TIME,
  TSDB_GRANT_CONNS,
  TSDB_GRANT_STREAMS,
  TSDB_GRANT_CPU_CORES,
} EGrantType;

int32_t grantInit();
void    grantCleanUp();
void    grantParseParameter();
int32_t grantCheck(EGrantType grant);
void    grantReset(EGrantType grant, uint64_t value);
void    grantAdd(EGrantType grant, uint64_t value);
void    grantRestore(EGrantType grant, uint64_t value);

#ifdef __cplusplus
}
#endif

#endif
