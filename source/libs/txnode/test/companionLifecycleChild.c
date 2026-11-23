/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include <stdlib.h>
#include "uv.h"

static void timeout(uv_timer_t *timer) {
  (void)timer;
  exit(2);
}

int main(void) {
  uv_loop_t  loop;
  uv_pipe_t  pipe;
  uv_timer_t timer;
  if (uv_loop_init(&loop) != 0 || uv_pipe_init(&loop, &pipe, 1) != 0 || uv_pipe_open(&pipe, 0) != 0 ||
      uv_timer_init(&loop, &timer) != 0 || uv_timer_start(&timer, timeout, 15000, 0) != 0) {
    return 1;
  }
  (void)uv_run(&loop, UV_RUN_DEFAULT);
  return 0;
}
