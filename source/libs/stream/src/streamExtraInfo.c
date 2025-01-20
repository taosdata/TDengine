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

#include "os.h"

#define UP_TASKS_NOT_SEND_CHKPT_TRIGGER 1
#define DOWN_TASKS_NOT_READY            2
#define DOWN_TASKS_BACKPRESSURE         3
#define DOWN_TASKS_INPUTQ_CLOSED        4
#define TASK_OUTPUTQ_FULL               5
#define TASK_SINK_QUOTA_REACHED         6

typedef struct SStreamTaskExtraInfo {
  int32_t infoId;
  char*   pMsg;
} SStreamTaskExtraInfo;

SStreamTaskExtraInfo extraInfoList[8] = {
    {0},
    {.infoId = UP_TASKS_NOT_SEND_CHKPT_TRIGGER, .pMsg = "%d(us) not send checkpoint-trigger"},
    {.infoId = DOWN_TASKS_NOT_READY, .pMsg = "%d(ds) tasks not ready"},
    {.infoId = DOWN_TASKS_BACKPRESSURE, .pMsg = "0x%x(ds) backpressure"},
    {.infoId = DOWN_TASKS_INPUTQ_CLOSED, .pMsg = "0x%x(ds) inputQ closed"},
    {.infoId = TASK_OUTPUTQ_FULL, .pMsg = "outputQ is full"},
    {.infoId = TASK_SINK_QUOTA_REACHED, .pMsg = "sink quota reached"},
};

