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

#include "vnd.h"
#include "vnodeInt.h"

int32_t vnodePutScheduleTask(SVnode* pVnode, int (*exec)(void*), void* arg, int8_t type) {
  int32_t code = 0;

  switch (type) {
    case VND_TASK_COMMIT:
      code = vnodeScheduleTask(exec, arg);
      break;
    case VND_TASK_COMPACT:
      break;
    case VND_TASK_MERGE:
      break;
    case VND_TASK_MIGRATE:
      break;
    default:
      code = TSDB_CODE_APP_ERROR;
      break;
  }

  return code;
}