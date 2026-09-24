/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "qst_supervisor.h"

#include <string.h>

static void qstSupervisorCancelStall(SQstQueueStallObserver *observer,
                                     uint64_t epoch, uint64_t now_mono_ms) {
  observer->observed_epoch = epoch;
  observer->observed_mono_ms = now_mono_ms;
  observer->stall_deadline_mono_ms = 0;
  observer->has_observation = 1;
}

EQstCode qst_supervisor_observe_queue(
    const SQstShmMapping *mapping, EQstQueueId queue_id,
    int32_t consumer_ready, int32_t has_active_task, uint64_t now_mono_ms,
    SQstQueueStallObserver *observer, int32_t *out_stalled) {
  SQstQueueSnapshot snapshot = {0};
  int32_t queue_nonempty = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == observer || NULL == out_stalled ||
      now_mono_ms == 0 || (consumer_ready != 0 && consumer_ready != 1) ||
      (has_active_task != 0 && has_active_task != 1)) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_queue_snapshot(mapping, queue_id, &snapshot);
  if (code != QST_OK) {
    return code;
  }
  *out_stalled = 0;
  queue_nonempty = snapshot.enqueue_pos != snapshot.dequeue_pos;
  if (!queue_nonempty || consumer_ready == 0 || has_active_task != 0) {
    qstSupervisorCancelStall(observer, snapshot.progress_epoch, now_mono_ms);
    return QST_OK;
  }
  if (observer->has_observation == 0 ||
      observer->observed_epoch != snapshot.progress_epoch ||
      now_mono_ms < observer->observed_mono_ms) {
    qstSupervisorCancelStall(observer, snapshot.progress_epoch, now_mono_ms);
    return QST_OK;
  }
  if (observer->stall_deadline_mono_ms == 0) {
    if (now_mono_ms > UINT64_MAX - QST_QUEUE_STALL_MILLISECONDS) {
      return QST_E_RESOURCE;
    }
    observer->stall_deadline_mono_ms =
        now_mono_ms + QST_QUEUE_STALL_MILLISECONDS;
    return QST_OK;
  }
  if (now_mono_ms >= observer->stall_deadline_mono_ms) {
    *out_stalled = 1;
  }
  return QST_OK;
}

EQstCode qst_supervisor_consume_restart(
    SQstRestartBudget *budget, uint64_t now_mono_ms,
    uint64_t window_milliseconds, uint32_t restart_limit) {
  if (NULL == budget || now_mono_ms == 0 || window_milliseconds == 0 ||
      restart_limit == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (budget->window_started_mono_ms == 0 ||
      now_mono_ms < budget->window_started_mono_ms ||
      now_mono_ms - budget->window_started_mono_ms >= window_milliseconds) {
    budget->window_started_mono_ms = now_mono_ms;
    budget->restart_count = 0;
  }
  if (budget->restart_count >= restart_limit) {
    return QST_E_RESOURCE;
  }
  ++budget->restart_count;
  return QST_OK;
}

void qst_supervisor_reset_restart(SQstRestartBudget *budget) {
  if (NULL != budget) {
    memset(budget, 0, sizeof(*budget));
  }
}
