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

#ifndef _TD_OS_TIMER_H_
#define _TD_OS_TIMER_H_

#ifdef __cplusplus
extern "C" {
#endif

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define timer_create  TIMER_CREATE_FUNC_TAOS_FORBID
#define timer_settime TIMER_SETTIME_FUNC_TAOS_FORBID
#define timer_delete  TIMER_DELETE_FUNC_TAOS_FORBID
#define timeSetEvent  TIMESETEVENT_SETTIME_FUNC_TAOS_FORBID
#define timeKillEvent TIMEKILLEVENT_SETTIME_FUNC_TAOS_FORBID
#endif

#define MSECONDS_PER_TICK 5

int32_t     taosInitTimer(void (*callback)(int32_t), int32_t ms);
void        taosUninitTimer();
int64_t     taosGetMonotonicMs();
const char *taosMonotonicInit();

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_TIMER_H_*/
