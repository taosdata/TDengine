/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_SUPERVISOR_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_SUPERVISOR_H_

#include <stdint.h>

#include "qst_queue.h"

enum {
  QST_FLOW_LEASE_MILLISECONDS = 30000,
  QST_FLOW_CONFIRM_MILLISECONDS = 4000,
  QST_QUEUE_STALL_MILLISECONDS = 120000,
  /* Persistence shutdown may wait for already-submitted async batches after
   * the generator has stopped. Keep this bound separate from the normal
   * per-task/watchdog lease so a busy drain does not look stalled. */
  QST_DRAIN_LEASE_MILLISECONDS = 300000,
  QST_STOP_ACK_MILLISECONDS = 2000,
  /* Main shutdown drains already-generated/persisting tasks before exit. A
   * ten-second grace can kill it mid-drain under the fake native delay or a
   * busy result store, so allow a bounded minute for durable shutdown. */
  QST_TERM_GRACE_MILLISECONDS = 60000,
  QST_KILL_GRACE_MILLISECONDS = 5000,
  QST_MANAGER_RESTART_LIMIT = 3,
  QST_MANAGER_RESTART_WINDOW_MILLISECONDS = 300000,
};

/* Private observer state. It is deliberately not stored in shared memory. */
typedef struct {
  uint64_t observed_epoch;
  uint64_t observed_mono_ms;
  uint64_t stall_deadline_mono_ms;
  uint32_t has_observation;
} SQstQueueStallObserver;

/* Private fixed-window restart budget for one manager role. */
typedef struct {
  uint64_t window_started_mono_ms;
  uint32_t restart_count;
} SQstRestartBudget;

/*
 * Observes one queue without changing it. A stall starts only on the second
 * equal epoch sample while work is pending, a consumer is ready, and no task
 * has a valid absolute deadline.
 */
EQstCode qst_supervisor_observe_queue(
    const SQstShmMapping *mapping, EQstQueueId queue_id,
    int32_t consumer_ready, int32_t has_active_task, uint64_t now_mono_ms,
    SQstQueueStallObserver *observer, int32_t *out_stalled);

/* Consumes one restart allowance; a healthy caller resets this private state. */
EQstCode qst_supervisor_consume_restart(
    SQstRestartBudget *budget, uint64_t now_mono_ms,
    uint64_t window_milliseconds, uint32_t restart_limit);

/* Clears a role's restart count after a verified healthy recovery. */
void qst_supervisor_reset_restart(SQstRestartBudget *budget);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_SUPERVISOR_H_
