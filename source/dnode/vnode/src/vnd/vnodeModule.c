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

#include "cos.h"
#include "monitor.h"
#include "vnd.h"

static volatile int32_t VINIT = 0;

int vnodeInit(int nthreads, StopDnodeFp stopDnodeFp) {
  if (atomic_val_compare_exchange_32(&VINIT, 0, 1)) {
    return 0;
  }

  TAOS_CHECK_RETURN(vnodeAsyncOpen(nthreads));
  TAOS_CHECK_RETURN(walInit(stopDnodeFp));

  monInitVnode();

  return 0;
}

void vnodeCleanup() {
  if (atomic_val_compare_exchange_32(&VINIT, 1, 0) == 0) return;
  (void)vnodeAsyncClose();
  walCleanUp();
  smaCleanUp();
}
