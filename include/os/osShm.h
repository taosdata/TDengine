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

#ifndef _TD_OS_SHM_H_
#define _TD_OS_SHM_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int32_t id;
  int32_t size;
  void*   ptr;
} SShm;

int32_t taosCreateShm(SShm *pShm, int32_t shmsize) ;
void    taosDropShm(SShm *pShm);
int32_t taosAttachShm(SShm *pShm);
void    taosDetachShm(SShm *pShm);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_SHM_H_*/
