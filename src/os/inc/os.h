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

#ifndef TDENGINE_OS_H
#define TDENGINE_OS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "osInc.h"
#include "osDef.h"
#include "osAtomic.h"
#include "osDir.h"
#include "osFile.h"
#include "osLz4.h"
#include "osMath.h"
#include "osMemory.h"
#include "osRand.h"
#include "osSemaphore.h"
#include "osSignal.h"
#include "osSleep.h"
#include "osSocket.h"
#include "osString.h"
#include "osSysinfo.h"
#include "osTime.h"
#include "osTimer.h"
#include "osSystem.h"

void osInit();

#ifdef __cplusplus
}
#endif

#endif
