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

#ifndef TDENGINE_VNODE_INT_H
#define TDENGINE_VNODE_INT_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum _VN_STATUS {
  VN_STATUS_INIT,
  VN_STATUS_CREATING,
  VN_STATUS_READY,
  VN_STATUS_CLOSING,
  VN_STATUS_DELETING,
} EVnStatus;

typedef struct {
  int32_t      vgId;      // global vnode group ID
  int32_t      refCount;  // reference count
  EVnStatus    status; 
  int          role;   
  int64_t      version;
  void *       wqueue;
  void *       rqueue;
  void *       wal;
  void *       tsdb;
  void *       sync;
  void *       events;
  void *       cq;  // continuous query
} SVnodeObj;

int vnodeWriteToQueue(void *param, SWalHead *pHead, int type);

#ifdef __cplusplus
}
#endif

#endif
