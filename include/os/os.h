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

#ifndef _TD_OS_H_
#define _TD_OS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <ctype.h>
#include <pthread.h>
#include <semaphore.h>

#if !defined(WINDOWS)
#include <unistd.h>
#include <dirent.h>
#include <regex.h>
#include <sched.h>
#include <wordexp.h>
#include <libgen.h>

#include <sys/utsname.h>
#include <sys/param.h>
#include <sys/mman.h>

#if defined(DARWIN)
#else
#include <sys/prctl.h>
#include <argp.h>
#endif

#endif

#include <errno.h>
#include <fcntl.h>
#include <float.h>
#include <inttypes.h>
#include <limits.h>
#include <locale.h>
#include <math.h>
#include <setjmp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <wchar.h>
#include <termios.h>
#include <wctype.h>


#include "osAtomic.h"
#include "osDef.h"
#include "osDir.h"
#include "osEndian.h"
#include "osEnv.h"
#include "osFile.h"
#include "osLocale.h"
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
#include "osSystem.h"
#include "osThread.h"
#include "osTime.h"
#include "osTimer.h"
#include "osTimezone.h"

void osInit();

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_H_*/
