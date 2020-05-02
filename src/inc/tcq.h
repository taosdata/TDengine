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
#ifndef _TD_CQ_H_
#define _TD_CQ_H_

#ifdef __cplusplus
extern "C" {
#endif


typedef int (*FCqWrite)(void *ahandle, void *pHead, int type);

typedef struct {
  int      vgId;
  char     path[TSDB_FILENAME_LEN];
  char     user[TSDB_USER_LEN];
  char     pass[TSDB_PASSWORD_LEN];
  FCqWrite cqWrite;
} SCqCfg;

void *cqOpen(void *ahandle, const SCqCfg *pCfg);
void  cqClose(void *handle);
void  cqStart(void *handle);
void  cqStop(void *handle);
void  cqCreate(void *handle, int sid, char *sqlStr, SSchema *pSchema, int columns);
void  cqDrop(void *handle, int sid);

extern int cqDebugFlag;


#ifdef __cplusplus
}
#endif

#endif  // _TD_CQ_H_
