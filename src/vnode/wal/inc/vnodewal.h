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
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void walh;  // WAL HANDLE

walh *vnodeOpenWal(int vnode, uint8_t op);
int   vnodeCloseWal(walh *pWal);
int   vnodeRenewWal(walh *pWal);
int   vnodeWriteWal(walh *pWal, void *cont, int contLen);
int   vnodeSyncWal(walh *pWal);

#ifdef __cplusplus
}
#endif

#endif  // _TD_WAL_H_
