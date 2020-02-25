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

#ifndef TDENGINE_TSCHED_H
#define TDENGINE_TSCHED_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _sched_msg {
  void    *msg;
  int      msgLen;
  int8_t   type;
  int32_t  code 
  void     handle;
} SRpcMsg;

void *rpcInitMsgQueue(int queueSize, void (*fp)(int num, SRpcQueue *),const char *label);
int   rpcPutIntoMsgQueue(void *qhandle, SRpcMsg *pMsg);
void  rpcCleanUpMsgQueue(void *param);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCHED_H
