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

#define ALLOW_FORBID_FUNC
#define _DEFAULT_SOURCE
#include "os.h"


#if !(defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32))
#include <unistd.h>
#endif

void taosSsleep(int32_t s) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
   Sleep(1000 * s); 
#else
  sleep(s);
#endif
}

void taosMsleep(int32_t ms) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
   Sleep(ms); 
#else
  usleep(ms * 1000);
#endif
}

void taosUsleep(int32_t us) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
   nanosleep(1000 * us); 
#else
  usleep(us);
#endif
}
