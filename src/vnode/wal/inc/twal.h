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
#ifndef _TD_WAL_H_
#define _TD_WAL_H_

#ifdef __cplusplus
extern "C" {
#endif

#define TAOS_WAL_NOLOG   0
#define TAOS_WAL_WRITE   1
#define TAOS_WAL_FSYNC   2
 
typedef struct {
  int8_t    msgType;
  int8_t    reserved[3];
  int32_t   len;
  uint64_t  version;
  uint32_t  signature;
  uint32_t  cksum;
  char      cont[];
} SWalHead;

typedef void* twal_h;  // WAL HANDLE

twal_h  walOpen(char *path, int max, int level);
void    walClose(twal_h);
int     walRenew(twal_h);
int     walWrite(twal_h, SWalHead *);
void    walFsync(twal_h);
int     walRestore(twal_h, void *pVnode, int (*writeFp)(void *ahandle, void *pWalHead));
int     walGetWalFile(twal_h, char *name, uint32_t *index);

extern int wDebugFlag;


#ifdef __cplusplus
}
#endif

#endif  // _TD_WAL_H_
