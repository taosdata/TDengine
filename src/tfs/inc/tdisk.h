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

#ifndef TD_TDISK_H
#define TD_TDISK_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int level;
  int id;
} SDiskID;

typedef struct {
  uint64_t size;
  uint64_t free;
  uint64_t nfiles;
} SDiskMeta;

typedef struct {
  int       level;
  int       id;
  char      dir[TSDB_FILENAME_LEN];
  SDiskMeta dmeta;
} SDisk;

SDisk *tdNewDisk(SDiskID did, char *dir);
void   tdFreeDisk(SDisk *pDisk);

#ifdef __cplusplus
}
#endif

#endif