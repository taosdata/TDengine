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

#ifndef TDENGINE_SCH_PHASE_H
#define TDENGINE_SCH_PHASE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Main phase (NONE/PARSE/CATALOG/PLAN/SCHEDULE/EXECUTE/FETCH/DONE) of a phase
 * code; sub-phases (4x/5x/6x) map to their parent. Exposed so the scheduler
 * unit tests can verify the routing without pulling in the whole scheduler. */
int32_t schGetMainPhase(int32_t phase);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SCH_PHASE_H
