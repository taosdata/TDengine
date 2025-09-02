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

#ifndef _TD_VNODE_TSDB_INT_H_
#define _TD_VNODE_TSDB_INT_H_

typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int32_t nodeId; // node id of leader vnode in ss migration
  int64_t now;

  // lastCommit time when the task is scheduled, we will compare it with the
  // fileset last commit time at the start of the task execution, if mismatch,
  // we know there are new commits after the task is scheduled.
  TSKEY   lastCommit;
  int64_t cid;

  STFileSet   *fset;
  TFileOpArray fopArr;
} SRTNer;


typedef struct {
  STsdb  *tsdb;
  int64_t now;
  TSKEY   lastCommit;
  int32_t nodeId; // node id of leader vnode in ss migration
  int32_t fid;
  bool    ssMigrate;
} SRtnArg;


int32_t tsdbDoSsMigrate(SRTNer *rtner);
void tsdbRetentionCancel(void *arg);
int32_t tsdbRetention(void *arg);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_VNODE_TSDB_INT_H_*/
