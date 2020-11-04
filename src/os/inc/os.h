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

#ifdef _TD_DARWIN_64
#include "osDarwin.h"
#endif

#ifdef _TD_ARM_64
#include "osArm64.h"
#endif

#ifdef _TD_ARM_32
#include "osArm32.h"
#endif

#ifdef _TD_LINUX_64
#include "osLinux64.h"
#endif

#ifdef _TD_LINUX_32
#include "osLinux32.h"
#endif

#ifdef _ALPINE
#include "osAlpine.h"
#endif

#ifdef _TD_NINGSI_60
#include "osNingsi.h"
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#include "osWindows.h"
#endif

#include "osDef.h"
#include "osAlloc.h"
#include "osAtomic.h"
#include "osCommon.h"
#include "osDir.h"
#include "osFile.h"
#include "osLz4.h"
#include "osMath.h"
#include "osMemory.h"
#include "osRand.h"
#include "osSemphone.h"
#include "osSocket.h"
#include "osString.h"
#include "osSysinfo.h"
#include "osTime.h"
#include "osTimer.h"

void osInit();

#ifdef __cplusplus
}
#endif

#endif
