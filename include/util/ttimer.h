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

#ifndef _TD_UTIL_TIMER_H_
#define _TD_UTIL_TIMER_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void *tmr_h;
typedef void (*TAOS_TMR_CALLBACK)(void *, void *);

extern int32_t taosTmrThreads;

#define MSECONDS_PER_TICK 5

void *taosTmrInit(int32_t maxTmr, int32_t resoultion, int32_t longest, const char *label);

void taosTmrCleanUp(void *handle);

tmr_h taosTmrStart(TAOS_TMR_CALLBACK fp, int32_t mseconds, void *param, void *handle);

bool taosTmrStop(tmr_h tmrId);

bool taosTmrStopA(tmr_h *tmrId);

bool taosTmrReset(TAOS_TMR_CALLBACK fp, int32_t mseconds, void *param, void *handle, tmr_h *pTmrId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TIMER_H_*/
