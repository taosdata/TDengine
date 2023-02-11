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

#include "tsdb.h"


typedef struct {
  int32_t (*startFp)(STsdb *pTsdb, void *arg, int64_t varg);
  int32_t (*execFp)(STsdb *pTsdb, void *arg, int64_t varg);
  int32_t (*finishFp)(STsdb *pTsdb, void *arg);
  int32_t (*abortFp)(STsdb *pTsdb, void *arg);
} STsdbFSWriter;

static STsdbFSWriter fsWriter[VND_TASK_MAX] =  {{NULL, NULL, NULL, NULL},
                                               {NULL, tsdbCompact, NULL, NULL},
                                               {NULL, tsdbMerge, NULL, NULL},
                                               {NULL, tsdbDoRetention, NULL, NULL}};



int32_t tsdbBatchExec(STsdb *pTsdb, void *arg, int64_t varg) {
  int32_t code = 0;
  int32_t lino = 0;
  int8_t  type = *(int8_t *)arg;

  ASSERT(type >= VND_TASK_COMMIT && type < VND_TASK_MAX);

  if (fsWriter[type].startFp) {
    code = (*fsWriter[type].startFp)(pTsdb, arg, varg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (fsWriter[type].execFp) {
    code = (*fsWriter[type].execFp)(pTsdb, arg, varg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (fsWriter[type].finishFp) {
    code = (*fsWriter[type].finishFp)(pTsdb, arg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    if (fsWriter[type].abortFp) {
      code = (*fsWriter[type].abortFp)(pTsdb, arg);
    }
  }

  return 0;
}
